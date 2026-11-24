//! Finalization, hold / release (13-04; D-P13-4, CockroachDB model).
//!
//! - **Rule ([`finalize_decision`], pure):** the target is the lowest `level_max` reported
//!   by the non-`Removed` members (a member without a `NodeVersion` counts as
//!   [`LEVEL_MIN`]). Finalize to it only when it is above the finalized level (the level
//!   never decreases), every non-`Removed` member reported its version and is live, no
//!   operator hold is set, and auto-finalize is on (the manual call passes `true`).
//! - **Write:** `Put(Cluster)` with `expected_version` = the record version (CAS); only the
//!   `ClusterVersion` ext tag 98 changes, every other tag is kept byte for byte. A
//!   `StaleVersion` re-reads and re-evaluates. All checks run here, at the proposer; apply
//!   never validates the ext value (rule 5).
//! - **Finalizer task ([`start_finalizer`]):** runs on every node and acts only while this
//!   node leads the metadata group (liveness is known only there:
//!   `RaftHandle::sample_live`). A freshly elected leader waits one liveness window before
//!   acting (its first samples still hold follower-era contacts). A single node finalizes
//!   on start once its own version is advertised.
//! - **Operator calls:** [`finalize`] (same checks, regardless of auto-finalize; on the meta
//!   leader only), [`hold`] (`hold = Some(finalized)`) and [`release`] (`hold = None`),
//!   idempotent; hold / release work from any node (the proposal is forwarded).

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use ostro_meta::ext::{
    ClusterVersion, MEMBER_ACTIVE, MEMBER_REMOVED, get_member_state, get_node_version,
    put_cluster_version,
};
use ostro_meta::{MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult, MetaState};
use ostro_partition::Clock;
use ostro_types::compat::LEVEL_MIN;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::compat::{cluster_version, read_cluster_version};
use crate::contracts::{ClusterStatusView, MemberView};

/// What the finalizer should do now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FinalizeDecision {
    /// Nothing to do (already at the highest common level).
    Stay,
    /// Propose `ClusterVersion.finalized = to`.
    Finalize { to: u32 },
    /// An operator hold keeps the level at `at`.
    Held { at: u32 },
    /// Not every member is ready (`reason` names the first blocker).
    Waiting { reason: String },
}

/// Why an operator upgrade call failed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum UpgradeError {
    /// Not every member supports the target level, or a member is not live.
    #[error("cannot finalize: {0}")]
    NotReady(String),
    /// Meta refused or failed.
    #[error("meta: {0}")]
    Meta(String),
    /// Not available before 13-04.
    #[error("Phase 13: implemented in 13-04")]
    NotImplemented,
}

/// Default liveness window of the finalizer (a member is live when the meta leader heard
/// from it within this window).
pub const LIVE_WINDOW: Duration = Duration::from_secs(3);

/// CAS races retried by one finalize / hold / release call.
const CAS_ATTEMPTS: usize = 5;

/// Pure finalization rule (D-P13-4).
pub fn finalize_decision(status: &ClusterStatusView, auto_finalize: bool) -> FinalizeDecision {
    let members: Vec<&MemberView> = status
        .members
        .iter()
        .filter(|m| m.state != MEMBER_REMOVED)
        .collect();
    if members.is_empty() {
        return FinalizeDecision::Waiting {
            reason: "no active members".into(),
        };
    }
    let level_of = |m: &MemberView| m.version.as_ref().map_or(LEVEL_MIN, |v| v.level_max);
    let target = members
        .iter()
        .map(|m| level_of(m))
        .min()
        .unwrap_or(LEVEL_MIN);
    let reported_target = members
        .iter()
        .filter_map(|m| m.version.as_ref().map(|v| v.level_max))
        .min()
        .unwrap_or(LEVEL_MIN);
    if let Some(m) = members.iter().find(|m| m.version.is_none()) {
        // An unreported member counts as LEVEL_MIN: it blocks only when the others could
        // go higher.
        if reported_target > status.finalized {
            return FinalizeDecision::Waiting {
                reason: format!("member {} has not reported its version", m.node_id),
            };
        }
        return FinalizeDecision::Stay;
    }
    if target <= status.finalized {
        return FinalizeDecision::Stay;
    }
    if let Some(at) = status.hold {
        return FinalizeDecision::Held { at };
    }
    if let Some(m) = members.iter().find(|m| !m.live) {
        return FinalizeDecision::Waiting {
            reason: format!("member {} is not live", m.node_id),
        };
    }
    if !auto_finalize {
        return FinalizeDecision::Waiting {
            reason: format!(
                "every member supports level {target}; auto-finalize is off (run `ostro cluster upgrade finalize`)"
            ),
        };
    }
    FinalizeDecision::Finalize { to: target }
}

/// The upgrade-relevant part of a [`ClusterStatusView`] from a meta state and the leader's
/// live set: members (state, version, liveness), finalized level, hold and level range.
/// Voters, learners, policy and reassignments are left empty (13-09 builds the full view).
pub fn upgrade_view(state: &MetaState, live: &BTreeSet<u64>) -> ClusterStatusView {
    let cv = cluster_version(state);
    let members: Vec<MemberView> = state
        .members()
        .map(|m| {
            let v = &m.value;
            MemberView {
                node_id: v.node_id,
                addr: v.addr.clone(),
                cluster_addr: v.cluster_addr.clone(),
                state: get_member_state(&v.ext)
                    .ok()
                    .flatten()
                    .map_or(MEMBER_ACTIVE, |s| s.state),
                version: get_node_version(&v.ext).ok().flatten(),
                live: live.contains(&v.node_id),
            }
        })
        .collect();
    let active = || members.iter().filter(|m| m.state != MEMBER_REMOVED);
    let lo = active()
        .map(|m| m.version.as_ref().map_or(LEVEL_MIN, |v| v.level_min))
        .min()
        .unwrap_or(LEVEL_MIN);
    let hi = active()
        .map(|m| m.version.as_ref().map_or(LEVEL_MIN, |v| v.level_max))
        .min()
        .unwrap_or(LEVEL_MIN);
    ClusterStatusView {
        members,
        finalized: cv.finalized,
        hold: cv.hold,
        level_range: (lo, hi),
        ..ClusterStatusView::default()
    }
}

/// Finalizer timing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FinalizerConfig {
    /// Auto-finalize (`[cluster] auto_finalize`, default true).
    pub auto_finalize: bool,
    /// Evaluation interval (1 s).
    pub interval: Duration,
    /// Liveness window ([`LIVE_WINDOW`]).
    pub live_window: Duration,
}

impl Default for FinalizerConfig {
    fn default() -> Self {
        FinalizerConfig {
            auto_finalize: true,
            interval: Duration::from_secs(1),
            live_window: LIVE_WINDOW,
        }
    }
}

/// The meta leader's auto-finalizer, with the node's injected clock (`finalized_at_ms`) and
/// the default liveness window.
pub fn start_finalizer(
    meta: Arc<MetaHandle>,
    auto_finalize: bool,
    interval: Duration,
    clock: Arc<dyn Clock>,
    stop: CancellationToken,
) -> JoinHandle<()> {
    start_finalizer_with(
        meta,
        FinalizerConfig {
            auto_finalize,
            interval,
            live_window: LIVE_WINDOW,
        },
        clock,
        stop,
    )
}

/// [`start_finalizer`] with an injected clock (`finalized_at_ms`) and timing.
pub fn start_finalizer_with(
    meta: Arc<MetaHandle>,
    cfg: FinalizerConfig,
    clock: Arc<dyn Clock>,
    stop: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut leader_since: Option<tokio::time::Instant> = None;
        let mut last_logged: Option<FinalizeDecision> = None;
        loop {
            let status = meta.raft().status();
            if status.is_leader {
                // clock-exempt: leadership-tenure duration (monotonic), never a timestamp
                let since = *leader_since.get_or_insert_with(tokio::time::Instant::now);
                let alone = status.voters.len() + status.learners.len() <= 1;
                if alone || since.elapsed() >= cfg.live_window {
                    match evaluate_and_finalize(&meta, cfg.auto_finalize, cfg.live_window, &*clock)
                        .await
                    {
                        Ok(d) => {
                            if last_logged.as_ref() != Some(&d) {
                                match &d {
                                    FinalizeDecision::Waiting { reason } => {
                                        tracing::info!(reason = %reason, "finalizer waiting")
                                    }
                                    FinalizeDecision::Held { at } => {
                                        tracing::info!(at, "finalizer held")
                                    }
                                    _ => {}
                                }
                                last_logged = Some(d);
                            }
                        }
                        Err(e) => tracing::warn!(error = %e, "finalizer step failed"),
                    }
                }
            } else {
                leader_since = None;
            }
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = tokio::time::sleep(cfg.interval) => {}
            }
        }
    })
}

/// One evaluation on the meta leader: sample liveness, decide, and on `Finalize` propose the
/// CAS. Returns the decision that held when the step ended (`Finalize` only when written).
async fn evaluate_and_finalize(
    meta: &MetaHandle,
    auto_finalize: bool,
    live_window: Duration,
    clock: &dyn Clock,
) -> Result<FinalizeDecision, UpgradeError> {
    for _ in 0..CAS_ATTEMPTS {
        let Some(live) = meta.raft().sample_live(live_window).await else {
            return Err(UpgradeError::NotReady(
                "liveness is known only on the meta leader".into(),
            ));
        };
        let live: BTreeSet<u64> = live.into_iter().collect();
        let Some((version, current)) = read_cluster(meta)? else {
            return Ok(FinalizeDecision::Waiting {
                reason: "cluster not initialized".into(),
            });
        };
        let view = meta.view().read(|s| upgrade_view(s, &live));
        let d = finalize_decision(&view, auto_finalize);
        let FinalizeDecision::Finalize { to } = d else {
            return Ok(d);
        };
        let next = ClusterVersion {
            finalized: to,
            hold: current.hold,
            finalized_at_ms: clock.now_ms(),
        };
        match put_version(meta, version, &next).await? {
            Some(()) => {
                tracing::info!(from = view.finalized, to, "cluster feature level finalized");
                return Ok(d);
            }
            None => continue,
        }
    }
    Err(UpgradeError::Meta(
        "cluster record kept changing under the finalizer".into(),
    ))
}

/// `(record version, ClusterVersion with defaults)`, `None` without a cluster record. An
/// undecodable tag 98 is an error (never overwritten blindly).
fn read_cluster(meta: &MetaHandle) -> Result<Option<(u64, ClusterVersion)>, UpgradeError> {
    meta.view().read(|s| {
        let Some(c) = s.cluster() else {
            return Ok(None);
        };
        read_cluster_version(s)
            .map_err(|e| UpgradeError::Meta(format!("undecodable ClusterVersion: {e}")))?;
        Ok(Some((c.version, cluster_version(s))))
    })
}

/// `Put(Cluster)` with tag 98 = `next`, CAS on `expected`. `Ok(None)` on a lost race.
async fn put_version(
    meta: &MetaHandle,
    expected: u64,
    next: &ClusterVersion,
) -> Result<Option<()>, UpgradeError> {
    let Some(cur) = meta.view().read(|s| s.cluster().cloned()) else {
        return Err(UpgradeError::NotReady("cluster not initialized".into()));
    };
    if cur.version != expected {
        return Ok(None);
    }
    let mut record = cur.value;
    record.ext = put_cluster_version(&record.ext, next)
        .map_err(|e| UpgradeError::Meta(format!("cluster ext: {e}")))?;
    let cmd = MetaCommand::Put {
        key: MetaKey::Cluster,
        expected_version: Some(expected),
        record: MetaRecord::Cluster(record),
    };
    match meta.propose(cmd).await {
        Ok(MetaResult::Ok { .. }) => Ok(Some(())),
        Ok(MetaResult::StaleVersion { .. } | MetaResult::Conflict { .. }) => Ok(None),
        Ok(MetaResult::NotFound) => Err(UpgradeError::NotReady("cluster not initialized".into())),
        Ok(MetaResult::Invalid(msg)) => Err(UpgradeError::Meta(msg)),
        Err(e) => Err(UpgradeError::Meta(e.to_string())),
    }
}

/// `ostro cluster upgrade finalize`: the [`finalize_decision`] checks regardless of
/// auto-finalize, on the meta leader. Returns the resulting `ClusterVersion` (unchanged when
/// already at the highest common level); `NotReady` names the blocker.
pub async fn finalize(meta: &MetaHandle, now_ms: u64) -> Result<ClusterVersion, UpgradeError> {
    struct Fixed(u64);
    impl Clock for Fixed {
        fn now_ms(&self) -> u64 {
            self.0
        }
    }
    match evaluate_and_finalize(meta, true, LIVE_WINDOW, &Fixed(now_ms)).await? {
        FinalizeDecision::Finalize { .. } | FinalizeDecision::Stay => {
            Ok(meta.view().read(cluster_version))
        }
        FinalizeDecision::Held { at } => Err(UpgradeError::NotReady(format!(
            "an upgrade hold keeps the cluster at level {at}; run `ostro cluster upgrade release` first"
        ))),
        FinalizeDecision::Waiting { reason } => Err(UpgradeError::NotReady(reason)),
    }
}

/// Read-modify-write of tag 98 with CAS retries; `edit` returns `None` for "already so".
async fn edit_version(
    meta: &MetaHandle,
    edit: impl Fn(&ClusterVersion) -> Option<ClusterVersion>,
) -> Result<ClusterVersion, UpgradeError> {
    for _ in 0..CAS_ATTEMPTS {
        let Some((version, current)) = read_cluster(meta)? else {
            return Err(UpgradeError::NotReady("cluster not initialized".into()));
        };
        let Some(next) = edit(&current) else {
            return Ok(current);
        };
        if put_version(meta, version, &next).await?.is_some() {
            return Ok(next);
        }
    }
    Err(UpgradeError::Meta(
        "cluster record kept changing under the upgrade call".into(),
    ))
}

/// `ostro cluster upgrade hold`: keep the cluster at its finalized level (the downgrade
/// window stays open) until [`release`]. Idempotent.
pub async fn hold(meta: &MetaHandle, now_ms: u64) -> Result<ClusterVersion, UpgradeError> {
    let _ = now_ms;
    let r = edit_version(meta, |cv| {
        cv.hold.is_none().then_some(ClusterVersion {
            hold: Some(cv.finalized),
            ..*cv
        })
    })
    .await?;
    tracing::info!(hold = ?r.hold, "upgrade hold set");
    Ok(r)
}

/// `ostro cluster upgrade release`: clear the hold. Idempotent.
pub async fn release(meta: &MetaHandle, now_ms: u64) -> Result<ClusterVersion, UpgradeError> {
    let _ = now_ms;
    let r = edit_version(meta, |cv| {
        cv.hold
            .is_some()
            .then_some(ClusterVersion { hold: None, ..*cv })
    })
    .await?;
    tracing::info!("upgrade hold released");
    Ok(r)
}
