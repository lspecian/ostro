//! Compatibility gate, boot / running fences and the NodeVersion advertiser (13-04;
//! CLUS-06, D-P13-3/5/6/7; 05-20 Phase 13 rules 1, 5 and 6).
//!
//! - **Finalized level:** `ClusterInfo.ext` tag 98 ([`EXT_CLUSTER_VERSION`]). Absent (a
//!   Phase 12 cluster, or no cluster record yet) means [`LEVEL_MIN`]. An undecodable value is
//!   also read as [`LEVEL_MIN`] for the gate (the safe direction: every gated feature is
//!   refused) and logged.
//! - **Gate ([`require`]):** a proposer may emit a gated feature only when the cluster's
//!   finalized level **and** this process's `level_max` both reach the feature's
//!   `introduced_at`. Every Phase 13 emitter calls it before proposing; apply never checks
//!   (rule 5). The refusal is [`FeatureNotEnabled`] (`409 feature_not_enabled`).
//! - **Boot fence ([`boot_fence`], rule 6):** after meta caught up and before `Node::open`,
//!   refuse a finalized level outside `[LEVEL_MIN, level_max]` (downgrade after
//!   finalization, or a skipped minor). A newer META_FORMAT byte found while meta was opened
//!   maps to the same family of errors through [`meta_open_error`].
//! - **Running fence ([`start_running_fence`]):** a node that sees the finalized level rise
//!   above its `level_max` (it was not upgraded but stayed up) fail-stops through `on_fatal`.
//! - **Advertiser ([`start_advertiser`]):** keeps this node's `Member.ext` tag 96
//!   ([`NodeVersion`]) current with a read-modify-write (unknown tags are preserved) and a
//!   CAS, writing only when a field other than `reported_at_ms` changed, or every
//!   [`AdvertiserConfig::refresh`] to refresh the report.
//! - **Level source ([`MetaLevels`]):** `level_max` = the tier-B per-node override when set,
//!   else [`ostro_types::compat::level_max`] (which honours `OSTRO_TEST_LEVEL_MAX` only with
//!   the `level-emulation` feature).
//!
//! The mutant `upgrade::mutant::emit_ungated` ([`MUTANT_EMIT_UNGATED`]) makes [`require`]
//! allow everything; `tests/compat_mutants.rs` shows the gate test fails under it.

use std::sync::Arc;
use std::time::Duration;

use ostro_meta::ext::{
    ClusterVersion, EXT_CLUSTER_VERSION, ExtError, MEMBER_REMOVED, NodeVersion,
    get_cluster_version, get_member_state, get_node_version, put_node_version,
};
use ostro_meta::{
    META_FORMAT, MetaCommand, MetaError, MetaHandle, MetaKey, MetaRecord, MetaResult, MetaState,
    MetaView,
};
use ostro_partition::{Clock, SystemClock};
use ostro_types::compat::{Feature, LEVEL_MIN};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::contracts::LevelSource;

/// The `emit_ungated` mutant (RESERVATIONS §11): [`require`] allows every feature.
pub const MUTANT_EMIT_UNGATED: &str = "upgrade::mutant::emit_ungated";

/// How long [`boot_fence`] waits for meta to catch up before it gives up.
pub const BOOT_FENCE_CATCH_UP: Duration = Duration::from_secs(30);

/// Why a node must not open its partitions (boot fence) or keep running (running fence).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CompatError {
    /// The cluster is finalized above this binary's `level_max`.
    #[error(
        "data finalized by a newer Ostro (level {finalized}, this binary supports up to {level_max}); upgrade this binary"
    )]
    FinalizedAbove { finalized: u32, level_max: u32 },
    /// The cluster is finalized below this binary's `LEVEL_MIN`.
    #[error(
        "data at level {finalized} is older than this binary supports (minimum {level_min}); upgrade through {through} first"
    )]
    FinalizedBelow {
        finalized: u32,
        level_min: u32,
        through: String,
    },
    /// The meta format byte is above this build's maximum.
    #[error("metadata written by a newer Ostro (format {found}); upgrade this binary")]
    NewerMetaFormat { found: u8 },
}

/// A gated feature was asked for before the cluster finalized its level
/// (maps to 409 `feature_not_enabled`).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("feature {feature:?} needs level {needed}; the cluster is finalized at {finalized}")]
pub struct FeatureNotEnabled {
    /// The refused feature.
    pub feature: Feature,
    /// Its `introduced_at` level.
    pub needed: u32,
    /// The cluster's finalized level.
    pub finalized: u32,
}

impl FeatureNotEnabled {
    /// The API error reason (`FAILED_PRECONDITION` / HTTP 409).
    pub const REASON: &'static str = "feature_not_enabled";
    /// The REST status code.
    pub const HTTP_STATUS: u16 = 409;

    /// The operator-facing message.
    pub fn detail(&self) -> String {
        format!(
            "{} needs cluster feature level {} (finalized {}); finish and finalize the upgrade",
            self.feature.name(),
            self.needed,
            self.finalized
        )
    }
}

/// The stored `ClusterVersion` (`ClusterInfo.ext` tag 98). `Ok(None)` without a cluster
/// record or without the tag.
pub fn read_cluster_version(state: &MetaState) -> Result<Option<ClusterVersion>, ExtError> {
    match state.cluster() {
        Some(c) => get_cluster_version(&c.value.ext),
        None => Ok(None),
    }
}

/// The cluster's `ClusterVersion` with the defaults applied: absent → `finalized =
/// LEVEL_MIN`, no hold. An undecodable value reads as the default too (and is logged).
pub fn cluster_version(state: &MetaState) -> ClusterVersion {
    match read_cluster_version(state) {
        Ok(Some(cv)) => cv,
        Ok(None) => default_version(),
        Err(e) => {
            tracing::warn!(error = %e, ext_tag = EXT_CLUSTER_VERSION, "undecodable ClusterVersion; reading it as LEVEL_MIN");
            default_version()
        }
    }
}

fn default_version() -> ClusterVersion {
    ClusterVersion {
        finalized: LEVEL_MIN,
        hold: None,
        finalized_at_ms: 0,
    }
}

/// The cluster's finalized feature level (tag 98; absent = [`LEVEL_MIN`]).
pub fn finalized_level(state: &MetaState) -> u32 {
    cluster_version(state).finalized
}

/// Whether the mutant failpoint `name` is enabled (`fail::cfg(name, "return")`).
#[cfg(feature = "failpoints")]
fn mutant(name: &str) -> bool {
    fail::eval(name, |_| ()).is_some()
}

#[cfg(not(feature = "failpoints"))]
#[inline(always)]
fn mutant(_name: &str) -> bool {
    false
}

/// The pure gate rule: `Ok` iff `finalized` and `level_max` both reach `f.introduced_at()`.
pub fn check(f: Feature, finalized: u32, level_max: u32) -> Result<(), FeatureNotEnabled> {
    if mutant(MUTANT_EMIT_UNGATED) {
        return Ok(());
    }
    let needed = f.introduced_at();
    if finalized >= needed && level_max >= needed {
        Ok(())
    } else {
        Err(FeatureNotEnabled {
            feature: f,
            needed,
            finalized,
        })
    }
}

/// Proposer-side gate for a feature (`finalized >= f.introduced_at()` and
/// `level_max >= f.introduced_at()`), reading the finalized level from the local meta view.
pub fn require(view: &MetaView, f: Feature, level_max: u32) -> Result<(), FeatureNotEnabled> {
    let finalized = view.read(finalized_level);
    check(f, finalized, level_max)
}

/// The pure fence rule: a binary with `level_max` may run data finalized at `finalized`.
pub fn fence(finalized: u32, level_max: u32) -> Result<(), CompatError> {
    if finalized > level_max {
        return Err(CompatError::FinalizedAbove {
            finalized,
            level_max,
        });
    }
    if finalized < LEVEL_MIN {
        return Err(CompatError::FinalizedBelow {
            finalized,
            level_min: LEVEL_MIN,
            through: format!("a release supporting level {finalized}"),
        });
    }
    Ok(())
}

/// The META_FORMAT check: a format byte above this build's [`META_FORMAT`] is
/// [`CompatError::NewerMetaFormat`].
pub fn check_meta_format(found: u8) -> Result<(), CompatError> {
    if found > META_FORMAT {
        Err(CompatError::NewerMetaFormat { found })
    } else {
        Ok(())
    }
}

/// Map an error from opening meta (`MetaHandle::start_with`, `identity::probe_meta`) to the
/// fence's message family: a "unknown metadata format byte N" decode error with `N` above
/// this build's [`META_FORMAT`] is [`CompatError::NewerMetaFormat`]. Anything else is
/// `None` (the caller keeps its own error).
pub fn meta_open_error(err: &str) -> Option<CompatError> {
    const NEEDLE: &str = "unknown metadata format byte ";
    let at = err.find(NEEDLE)? + NEEDLE.len();
    let digits: String = err[at..].chars().take_while(char::is_ascii_digit).collect();
    let found: u8 = digits.parse().ok()?;
    check_meta_format(found).err()
}

/// Boot fence (rule 6): after meta catch-up and before `Node::open`. Waits until some node
/// leads the group and this node applied everything it knows committed (bounded by
/// [`BOOT_FENCE_CATCH_UP`]), then refuses a finalized level outside
/// `[LEVEL_MIN, level_max]`. Touches no partition file.
pub async fn boot_fence(meta: &MetaHandle, level_max: u32) -> Result<(), CompatError> {
    match tokio::time::timeout(BOOT_FENCE_CATCH_UP, meta.wait_ready()).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            if let Some(c) = meta_open_error(&e.to_string()) {
                return Err(c);
            }
            // Not a compatibility verdict: the caller's meta checks report it. Use what
            // this node has applied.
            tracing::warn!(error = %e, "boot fence: meta not ready; checking the local view");
        }
        Err(_) => {
            tracing::warn!("boot fence: meta did not catch up in time; checking the local view");
        }
    }
    let finalized = meta.view().read(finalized_level);
    let r = fence(finalized, level_max);
    match &r {
        Ok(()) => tracing::info!(finalized, level_max, "boot fence passed"),
        Err(e) => tracing::error!(finalized, level_max, "boot fence refused: {e}"),
    }
    r
}

/// Meta-backed [`LevelSource`]: `level_max` = the override when set, else
/// [`ostro_types::compat::level_max`]; `finalized` from the local meta view.
#[derive(Clone)]
pub struct MetaLevels {
    view: MetaView,
    level_max: u32,
}

impl MetaLevels {
    /// `level_max_override` is `ServerOptions.level_max_override` (tier B).
    pub fn new(view: MetaView, level_max_override: Option<u32>) -> Self {
        MetaLevels {
            view,
            level_max: effective_level_max(level_max_override),
        }
    }
}

impl LevelSource for MetaLevels {
    fn level_max(&self) -> u32 {
        self.level_max
    }

    fn finalized(&self) -> u32 {
        self.view.read(finalized_level)
    }
}

/// `level_max_override` if set, else [`ostro_types::compat::level_max`].
pub fn effective_level_max(level_max_override: Option<u32>) -> u32 {
    level_max_override.unwrap_or_else(ostro_types::compat::level_max)
}

/// Advertiser timing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdvertiserConfig {
    /// Re-write an unchanged report this often (`reported_at_ms` refresh).
    pub refresh: Duration,
    /// Retry delay after a failed proposal.
    pub retry: Duration,
}

impl Default for AdvertiserConfig {
    fn default() -> Self {
        AdvertiserConfig {
            refresh: Duration::from_secs(600),
            retry: Duration::from_secs(1),
        }
    }
}

/// Same report apart from `reported_at_ms`.
fn same_report(a: &NodeVersion, b: &NodeVersion) -> bool {
    NodeVersion {
        reported_at_ms: 0,
        ..a.clone()
    } == NodeVersion {
        reported_at_ms: 0,
        ..b.clone()
    }
}

/// What one advertiser step did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdvertiseOutcome {
    /// The record was written.
    Written,
    /// The stored report is current; nothing proposed.
    Unchanged,
    /// No `Member` record for this node yet (the self-upsert creates it), or it is a
    /// `Removed` tombstone.
    NoMember,
}

/// One advertiser step: upsert `version` (with `reported_at_ms = now_ms`) into this node's
/// `Member.ext` tag 96 when it differs from the stored report (ignoring `reported_at_ms`),
/// or when the stored report is older than `refresh_ms`. Read-modify-write with a CAS on
/// the member version; every other ext tag is kept byte for byte.
pub async fn advertise_once(
    meta: &MetaHandle,
    node_id: u64,
    version: &NodeVersion,
    now_ms: u64,
    refresh_ms: u64,
) -> Result<AdvertiseOutcome, MetaError> {
    const ATTEMPTS: usize = 5;
    for _ in 0..ATTEMPTS {
        let Some(cur) = meta.view().read(|s| s.member(node_id).cloned()) else {
            return Ok(AdvertiseOutcome::NoMember);
        };
        if matches!(get_member_state(&cur.value.ext), Ok(Some(st)) if st.state == MEMBER_REMOVED) {
            return Ok(AdvertiseOutcome::NoMember);
        }
        let stored = get_node_version(&cur.value.ext).ok().flatten();
        if let Some(s) = &stored
            && same_report(s, version)
            && now_ms.saturating_sub(s.reported_at_ms) < refresh_ms
        {
            return Ok(AdvertiseOutcome::Unchanged);
        }
        let report = NodeVersion {
            reported_at_ms: now_ms,
            ..version.clone()
        };
        let ext = put_node_version(&cur.value.ext, &report)
            .map_err(|e| MetaError::Rejected(format!("member {node_id} ext: {e}")))?;
        let mut record = cur.value.clone();
        record.ext = ext;
        let cmd = MetaCommand::Put {
            key: MetaKey::Member(node_id),
            expected_version: Some(cur.version),
            record: MetaRecord::Member(record),
        };
        match meta.propose(cmd).await? {
            MetaResult::Ok { .. } => {
                tracing::info!(
                    node = node_id,
                    level_min = report.level_min,
                    level_max = report.level_max,
                    ostro = %report.ostro_version,
                    "node version advertised"
                );
                return Ok(AdvertiseOutcome::Written);
            }
            MetaResult::StaleVersion { .. }
            | MetaResult::Conflict { .. }
            | MetaResult::NotFound => continue,
            MetaResult::Invalid(msg) => return Err(MetaError::Rejected(msg)),
        }
    }
    Err(MetaError::Rejected(format!(
        "member {node_id} record kept changing under the version upsert"
    )))
}

/// Keep this node's `Member` ext 96 (NodeVersion) current, with the system clock and the
/// default [`AdvertiserConfig`].
pub fn start_advertiser(
    meta: Arc<MetaHandle>,
    node_id: u64,
    version: NodeVersion,
    stop: CancellationToken,
) -> JoinHandle<()> {
    start_advertiser_with(
        meta,
        node_id,
        version,
        Arc::new(SystemClock),
        AdvertiserConfig::default(),
        stop,
    )
}

/// [`start_advertiser`] with an injected clock and timing. Waits for meta catch-up, then
/// re-checks on every applied meta change (a member record rewrite that dropped tag 96, a
/// late self-upsert) and every `cfg.refresh`.
pub fn start_advertiser_with(
    meta: Arc<MetaHandle>,
    node_id: u64,
    version: NodeVersion,
    clock: Arc<dyn Clock>,
    cfg: AdvertiserConfig,
    stop: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        tokio::select! {
            _ = stop.cancelled() => return,
            r = meta.wait_ready() => if let Err(e) = r {
                tracing::warn!(error = %e, "advertiser: meta not ready; advertising anyway");
            },
        }
        let refresh_ms = u64::try_from(cfg.refresh.as_millis()).unwrap_or(u64::MAX);
        let mut applied = meta.view().subscribe();
        loop {
            let wait = match advertise_once(&meta, node_id, &version, clock.now_ms(), refresh_ms)
                .await
            {
                Ok(_) => cfg.refresh,
                Err(e) => {
                    tracing::warn!(node = node_id, error = %e, "node version advertisement failed");
                    cfg.retry
                }
            };
            tokio::select! {
                _ = stop.cancelled() => return,
                r = applied.changed() => if r.is_err() { return },
                _ = tokio::time::sleep(wait) => {}
            }
        }
    })
}

/// The running fence's message for a finalized level above `level_max`.
pub fn running_fence_message(finalized: u32, level_max: u32) -> String {
    format!(
        "running fence: {}; stopping",
        CompatError::FinalizedAbove {
            finalized,
            level_max
        }
    )
}

/// Fail-stop when the finalized level rises above `level_max` (`on_fatal` gets the message,
/// once; the task then ends). Checks at start and after every applied meta change.
pub fn start_running_fence(
    meta: Arc<MetaHandle>,
    level_max: u32,
    on_fatal: Arc<dyn Fn(String) + Send + Sync>,
    stop: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut applied = meta.view().subscribe();
        loop {
            let finalized = meta.view().read(finalized_level);
            if finalized > level_max {
                let msg = running_fence_message(finalized, level_max);
                tracing::error!(finalized, level_max, "{msg}");
                on_fatal(msg);
                return;
            }
            tokio::select! {
                _ = stop.cancelled() => return,
                r = applied.changed() => if r.is_err() { return },
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_needs_both_levels() {
        let f = Feature::RepinRun;
        assert_eq!(check(f, 2, 2), Ok(()));
        let e = check(f, 1, 2).unwrap_err();
        assert_eq!((e.needed, e.finalized), (2, 1));
        assert!(
            check(f, 2, 1).is_err(),
            "an emulated level-1 binary never emits"
        );
        assert!(e.detail().contains("repin_run"), "{}", e.detail());
        assert_eq!(FeatureNotEnabled::REASON, "feature_not_enabled");
        assert_eq!(FeatureNotEnabled::HTTP_STATUS, 409);
    }

    #[test]
    fn fence_is_the_closed_range() {
        assert_eq!(fence(1, 2), Ok(()));
        assert_eq!(fence(2, 2), Ok(()));
        let e = fence(2, 1).unwrap_err();
        assert!(
            e.to_string().contains("data finalized by a newer Ostro"),
            "{e}"
        );
        let e = fence(0, 2).unwrap_err();
        assert!(e.to_string().contains("upgrade through"), "{e}");
    }

    #[test]
    fn meta_open_errors_map_newer_formats_only() {
        let newer = format!(
            "starting metadata raft: unknown metadata format byte {} (expected {META_FORMAT})",
            META_FORMAT + 1
        );
        assert_eq!(
            meta_open_error(&newer),
            Some(CompatError::NewerMetaFormat {
                found: META_FORMAT + 1
            })
        );
        assert!(
            meta_open_error(&newer)
                .unwrap()
                .to_string()
                .contains("written by a newer Ostro")
        );
        assert_eq!(
            meta_open_error("unknown metadata format byte 0 (expected 2)"),
            None
        );
        assert_eq!(meta_open_error("disk full"), None);
        assert_eq!(check_meta_format(META_FORMAT), Ok(()));
    }

    #[test]
    fn reports_compare_without_the_timestamp() {
        let a = NodeVersion {
            ostro_version: "0.1.0".into(),
            level_min: 1,
            level_max: 2,
            reported_at_ms: 5,
            ..NodeVersion::default()
        };
        let mut b = a.clone();
        b.reported_at_ms = 99;
        assert!(same_report(&a, &b));
        b.level_max = 1;
        assert!(!same_report(&a, &b));
    }
}
