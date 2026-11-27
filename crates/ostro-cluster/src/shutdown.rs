//! Controlled shutdown (13-11, D-P13-13): before a node stops (SIGTERM in cluster mode,
//! drain), hand off every partition it leads to an in-ISR replica, then transfer metadata
//! leadership if it holds it, all bounded by `shutdown_handoff_timeout`. On timeout the
//! shutdown proceeds and whatever is still led here fails over the Phase 12 way.
//!
//! Called first in `ServerHandle::shutdown` through `p13::before_shutdown` (13-02's fixed
//! call site); the existing Phase 12 shutdown runs after it.
//!
//! - **Single-member clusters skip it** (no other metadata member: nothing to hand to).
//! - **No metadata quorum, no handoff:** a metadata read barrier (ReadIndex, bounded by
//!   [`QUORUM_PROBE`]) runs first. A node cut off from the quorum (isolated, or the rest of
//!   the cluster is already down) could not elect anyone; it skips the handoffs at once and
//!   the partitions fail over the Phase 12 way, instead of waiting out the catch-up bound.
//! - **Partitions** (bounded parallelism [`HANDOFF_PARALLELISM`]): for every partition meta
//!   names this node leader of, the targets are the published ISR members in replica order
//!   (the preferred replica first), then any other ISR member; the first that answers
//!   `Moved` wins, an `Aborted` / error tries the next. A partition without another ISR
//!   member is kept (RF=1 or a shrunken ISR; it fails over or stays offline as before).
//!   Every handoff goes through the [`HandoffClient`], i.e. the same `LocalHandoff`
//!   sequence the placement controller uses (its catch-up wait picks up the target's
//!   durable LEO). The fixed signature has no log access, so the target is chosen by
//!   replica order rather than by the highest durable LEO; a lagging target aborts within
//!   `handoff_catchup_timeout` and the next one is tried.
//! - **Self-shrink** (14-33, D-P14-50) after the partitions: for every partition meta names
//!   this node a replica and published-ISR member of but not the leader (the partitions it
//!   only follows, including the ones it just handed off), it leaves the published ISR
//!   itself, so the partition does not stall on it once the process is gone (it used to stay
//!   in the ISR until the controller's liveness shrink, about `session_timeout` later). Only
//!   where `published − self` still holds the leader and `≥ min_insync` members (else
//!   `kept_min_insync`: today's behaviour). It first **stops following** those partitions
//!   ([`ShutdownMeta::stop_following`]): with the fetcher gone, no fetch of this node can
//!   land on the leader after it adopted the removal, so the leader's 14-33 re-add rule (a
//!   caught-up observation newer than the removal) keeps the node out until it exits. Then
//!   it proposes `AlterIsr{partition, epoch, expected_isr_version, isr: published − self}`
//!   (bounded parallelism, the remaining budget; a `StaleVersion` re-reads the published
//!   state once, after waiting up to [`VIEW_CATCH_UP`] for this node's metadata view to show
//!   the publication the CAS lost to). This is the shrink the lag detector would make `replica_lag_max` later:
//!   the HW keeps counting this node until meta committed the removal (D-P12-5), and the
//!   `AlterIsr` apply rules are unchanged. The [`ShutdownReport::self_shrink`] lines carry
//!   each partition's outcome. Skipped with the handoffs when there is no metadata quorum.
//! - **Metadata** after the partitions: if this node is the metadata leader, transfer
//!   leadership to the most caught-up active voter (`MetaHandle::transfer_leader`, which
//!   waits up to 10 s) within what is left of the budget.
//! - A per-partition result table is logged; the [`ShutdownReport`] lists moved and kept
//!   partitions.
//!
//! Timing: tokio time only (no wall-clock read).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use ostro_meta::{MetaCommand, MetaHandle, MetaResult};
use ostro_types::{Epoch, PartitionId};
use tracing::{info, warn};

use crate::contracts::{HandoffClient, HandoffOutcome};

/// Partitions handed off concurrently.
pub const HANDOFF_PARALLELISM: usize = 4;

/// Bound of the metadata quorum probe that gates the handoffs.
pub const QUORUM_PROBE: Duration = Duration::from_secs(1);

/// What a controlled shutdown achieved.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ShutdownReport {
    /// Partitions handed off.
    pub moved: Vec<u16>,
    /// Partitions still led here when the shutdown continued (aborted or timed out).
    pub kept: Vec<u16>,
    /// Meta leadership was transferred away.
    pub meta_transferred: bool,
    /// The self-shrink outcome of every partition this node only followed (14-33), by
    /// partition id.
    pub self_shrink: Vec<(u16, SelfShrink)>,
}

/// The self-shrink outcome of one followed partition (14-33, [`ShutdownReport::self_shrink`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelfShrink {
    /// Meta committed `published − self`; this node stopped following the partition.
    Shrunk,
    /// `published − self` would lose the leader or drop below `min_insync`: not proposed
    /// (Phase 13 behaviour, the node keeps following until it exits).
    KeptMinInsync,
    /// No metadata quorum: skipped with the handoffs.
    NoQuorum,
    /// The published ISR moved twice under the proposal (`StaleVersion` after one re-read).
    VersionRace,
    /// The shutdown budget ran out first (or the outcome is still unknown at the deadline).
    Timeout,
    /// Meta rejected the proposal (`Invalid`, `NotFound`, …) or the outcome is unknown.
    Rejected,
}

impl SelfShrink {
    /// The report / log label.
    pub fn as_str(self) -> &'static str {
        match self {
            SelfShrink::Shrunk => "shrunk",
            SelfShrink::KeptMinInsync => "kept_min_insync",
            SelfShrink::NoQuorum => "no_quorum",
            SelfShrink::VersionRace => "version_race",
            SelfShrink::Timeout => "timeout",
            SelfShrink::Rejected => "rejected",
        }
    }
}

/// One partition this node follows and is a published-ISR member of (14-33).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FollowedPartition {
    /// Partition id.
    pub partition: u16,
    /// Leader epoch.
    pub epoch: u32,
    /// The leader (never this node).
    pub leader: u64,
    /// The published ISR (this node included).
    pub isr: Vec<u64>,
    /// Its CAS token.
    pub isr_version: u64,
    /// The partition's `min_insync`.
    pub min_insync: u8,
}

impl FollowedPartition {
    /// The ISR without `me`, if it still holds the leader and `>= min_insync` members.
    pub fn shrunk_isr(&self, me: u64) -> Option<Vec<u64>> {
        let rest: Vec<u64> = self.isr.iter().copied().filter(|n| *n != me).collect();
        let enough = rest.len() >= usize::from(self.min_insync.max(1));
        (self.leader != me && self.leader != 0 && rest.contains(&self.leader) && enough)
            .then_some(rest)
    }
}

/// Partitions handed to [`ShutdownMeta::stop_following`] / proposed concurrently.
pub const SELF_SHRINK_PARALLELISM: usize = 4;

/// One partition this node leads, with its handoff targets in preference order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LedPartition {
    /// Partition id.
    pub partition: u16,
    /// Leader epoch.
    pub epoch: u32,
    /// In-ISR replicas other than this node, preferred first.
    pub targets: Vec<u64>,
}

/// The metadata view a controlled shutdown needs ([`MetaHandle`] in production).
pub trait ShutdownMeta: Send + Sync {
    /// Metadata members (voters and learners), this node included.
    fn members(&self) -> usize;
    /// Partitions `node` leads, with their targets.
    fn led(&self, node: u64) -> Vec<LedPartition>;
    /// If this node is the metadata leader: the voter to transfer leadership to.
    fn meta_transfer_target(&self) -> Option<u64>;
    /// Transfer metadata leadership to `to` and wait until it leads.
    fn transfer_leader(&self, to: u64) -> BoxFuture<'_, Result<(), String>>;
    /// Whether this node reaches the metadata quorum right now (bounded probe).
    fn quorum_contact(&self) -> BoxFuture<'_, bool>;

    /// Partitions `node` follows and is a published-ISR member of (14-33). Default: none
    /// (no self-shrink).
    fn followed(&self, node: u64) -> Vec<FollowedPartition> {
        let _ = node;
        Vec::new()
    }

    /// Propose `cmd` (a `MetaCommand::AlterIsr`) through meta (14-33). `Err` = outcome
    /// unknown. Default: refused.
    fn alter_isr(&self, cmd: MetaCommand) -> BoxFuture<'_, Result<MetaResult, String>> {
        let _ = cmd;
        Box::pin(async { Err("this ShutdownMeta proposes no AlterIsr".to_owned()) })
    }

    /// Stop this node's replica fetchers for exactly `partitions` and keep them stopped
    /// until the process exits (14-33). Resolves once no fetch of those partitions is in
    /// flight. Default: no-op (the production impl is in `ostro-server` `p13/mod.rs`).
    fn stop_following(&self, partitions: &[u16]) -> BoxFuture<'_, ()> {
        let _ = partitions;
        Box::pin(async {})
    }
}

/// Handoff targets of an assignment: ISR members in replica order, then the rest of the
/// ISR, never `me`.
pub fn targets(me: u64, isr: &[u64], replicas: &[u64]) -> Vec<u64> {
    let mut out: Vec<u64> = replicas
        .iter()
        .copied()
        .filter(|r| *r != me && isr.contains(r))
        .collect();
    for n in isr {
        if *n != me && !out.contains(n) {
            out.push(*n);
        }
    }
    out
}

impl ShutdownMeta for MetaHandle {
    fn members(&self) -> usize {
        let c = self.conf_state();
        let all: BTreeSet<u64> = c
            .voters
            .iter()
            .chain(c.learners.iter())
            .chain(c.voters_outgoing.iter())
            .copied()
            .collect();
        all.len()
    }

    fn led(&self, node: u64) -> Vec<LedPartition> {
        self.view().read(|s| {
            s.partitions()
                .filter(|v| v.value.leader == node && v.value.epoch > 0)
                .map(|v| LedPartition {
                    partition: v.value.partition,
                    epoch: v.value.epoch,
                    targets: targets(node, &v.value.isr, &v.value.replicas),
                })
                .collect()
        })
    }

    fn meta_transfer_target(&self) -> Option<u64> {
        let s = self.raft_detail();
        if !s.is_leader {
            return None;
        }
        let me = self.node_id();
        s.progress
            .iter()
            .filter(|p| p.id != me && p.recent_active && s.voters.contains(&p.id))
            .max_by_key(|p| (p.matched, std::cmp::Reverse(p.id)))
            .map(|p| p.id)
    }

    fn transfer_leader(&self, to: u64) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            MetaHandle::transfer_leader(self, to)
                .await
                .map_err(|e| e.to_string())
        })
    }

    fn quorum_contact(&self) -> BoxFuture<'_, bool> {
        Box::pin(async move {
            matches!(
                tokio::time::timeout(QUORUM_PROBE, self.read_index_barrier()).await,
                Ok(Ok(_))
            )
        })
    }

    fn followed(&self, node: u64) -> Vec<FollowedPartition> {
        self.view().read(|s| {
            s.partitions()
                .filter(|v| {
                    let a = &v.value;
                    a.epoch > 0
                        && a.leader != 0
                        && a.leader != node
                        && a.replicas.contains(&node)
                        && a.isr.contains(&node)
                })
                .map(|v| FollowedPartition {
                    partition: v.value.partition,
                    epoch: v.value.epoch,
                    leader: v.value.leader,
                    isr: v.value.isr.clone(),
                    isr_version: v.value.isr_version,
                    min_insync: v.value.min_insync,
                })
                .collect()
        })
    }

    fn alter_isr(&self, cmd: MetaCommand) -> BoxFuture<'_, Result<MetaResult, String>> {
        Box::pin(async move { self.propose(cmd).await.map_err(|e| e.to_string()) })
    }
}

/// Hand off every partition this node leads and transfer meta leadership, within `timeout`.
pub async fn controlled_shutdown(
    node_id: u64,
    meta: Arc<MetaHandle>,
    handoff: Arc<dyn HandoffClient>,
    timeout: Duration,
) -> ShutdownReport {
    controlled_shutdown_with(node_id, meta.as_ref(), handoff, timeout).await
}

/// [`controlled_shutdown`] over any [`ShutdownMeta`] (tests use fakes).
pub async fn controlled_shutdown_with(
    node_id: u64,
    meta: &dyn ShutdownMeta,
    handoff: Arc<dyn HandoffClient>,
    timeout: Duration,
) -> ShutdownReport {
    if meta.members() <= 1 {
        return ShutdownReport::default();
    }
    let led = meta.led(node_id);
    if !meta.quorum_contact().await {
        let mut kept: Vec<u16> = led.iter().map(|l| l.partition).collect();
        kept.sort_unstable();
        let mut self_shrink: Vec<(u16, SelfShrink)> = meta
            .followed(node_id)
            .iter()
            .map(|f| (f.partition, SelfShrink::NoQuorum))
            .collect();
        self_shrink.sort_unstable_by_key(|(p, _)| *p);
        warn!(
            kept = ?kept,
            "controlled shutdown: no metadata quorum; skipping the handoffs and the ISR self-shrink (Phase 12 failover)"
        );
        return ShutdownReport {
            moved: Vec::new(),
            kept,
            meta_transferred: false,
            self_shrink,
        };
    }
    let budget = tokio::time::sleep(timeout);
    tokio::pin!(budget);

    // Partitions first.
    let moved: Arc<Mutex<Vec<u16>>> = Arc::new(Mutex::new(Vec::new()));
    let table: Arc<Mutex<Vec<(u16, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let work = futures_util::stream::iter(led.clone())
        .map(|lp| {
            let handoff = Arc::clone(&handoff);
            let moved = Arc::clone(&moved);
            let table = Arc::clone(&table);
            async move {
                let line = hand_off_one(node_id, &lp, handoff.as_ref()).await;
                if let Some(e) = line.0 {
                    lock(&moved).push(lp.partition);
                    lock(&table).push((lp.partition, format!("moved (epoch {e})")));
                } else {
                    lock(&table).push((lp.partition, line.1));
                }
            }
        })
        .buffer_unordered(HANDOFF_PARALLELISM)
        .collect::<Vec<()>>();
    let timed_out = tokio::select! {
        _ = work => false,
        () = &mut budget => true,
    };
    let mut moved = lock(&moved).clone();
    moved.sort_unstable();
    let mut kept: Vec<u16> = led
        .iter()
        .map(|l| l.partition)
        .filter(|p| !moved.contains(p))
        .collect();
    kept.sort_unstable();
    let mut table = lock(&table).clone();
    table.sort_unstable();
    for (p, result) in &table {
        info!(partition = p, %result, "controlled shutdown: partition handoff");
    }
    if timed_out {
        warn!(
            ?timeout,
            moved = moved.len(),
            kept = ?kept,
            "controlled shutdown: handoff budget exhausted; the rest fails over"
        );
    }

    // Then leave the ISR of every partition this node only follows (14-33).
    let (self_shrink, timed_out) = if timed_out {
        let mut lines: Vec<(u16, SelfShrink)> = meta
            .followed(node_id)
            .iter()
            .map(|f| (f.partition, SelfShrink::Timeout))
            .collect();
        lines.sort_unstable_by_key(|(p, _)| *p);
        (lines, true)
    } else {
        tokio::select! {
            lines = self_shrink_all(node_id, meta) => (lines, false),
            () = &mut budget => {
                let mut lines: Vec<(u16, SelfShrink)> = meta
                    .followed(node_id)
                    .iter()
                    .map(|f| (f.partition, SelfShrink::Timeout))
                    .collect();
                lines.sort_unstable_by_key(|(p, _)| *p);
                warn!(?timeout, "controlled shutdown: ISR self-shrink cut by the budget");
                (lines, true)
            }
        }
    };
    for (p, outcome) in &self_shrink {
        info!(
            partition = p,
            result = outcome.as_str(),
            "controlled shutdown: ISR self-shrink"
        );
    }

    // Then metadata leadership.
    let mut meta_transferred = false;
    if !timed_out && let Some(to) = meta.meta_transfer_target() {
        tokio::select! {
            r = meta.transfer_leader(to) => match r {
                Ok(()) => {
                    meta_transferred = true;
                    info!(to, "controlled shutdown: metadata leadership transferred");
                }
                Err(e) => warn!(to, error = %e, "controlled shutdown: metadata leadership transfer failed"),
            },
            () = &mut budget => {
                warn!(to, "controlled shutdown: metadata leadership transfer timed out");
            }
        }
    }
    ShutdownReport {
        moved,
        kept,
        meta_transferred,
        self_shrink,
    }
}

/// The self-shrink step (module docs): stop following every eligible partition, then
/// propose each `published − self`. Lines sorted by partition. The caller bounds it.
async fn self_shrink_all(node_id: u64, meta: &dyn ShutdownMeta) -> Vec<(u16, SelfShrink)> {
    let followed = meta.followed(node_id);
    let mut lines: Vec<(u16, SelfShrink)> = Vec::new();
    let mut eligible: Vec<FollowedPartition> = Vec::new();
    for f in followed {
        if f.shrunk_isr(node_id).is_some() {
            eligible.push(f);
        } else {
            lines.push((f.partition, SelfShrink::KeptMinInsync));
        }
    }
    if !eligible.is_empty() {
        let mut parts: Vec<u16> = eligible.iter().map(|f| f.partition).collect();
        parts.sort_unstable();
        // Fetchers first: no fetch of this node may land after the leader adopted the
        // removal (a fresh caught-up observation would re-add it, 14-33 rule).
        meta.stop_following(&parts).await;
        info!(partitions = ?parts, "controlled shutdown: stopped following before the ISR self-shrink");
        let results = futures_util::stream::iter(eligible)
            .map(|f| async move { (f.partition, shrink_one(node_id, meta, f).await) })
            .buffer_unordered(SELF_SHRINK_PARALLELISM)
            .collect::<Vec<_>>()
            .await;
        lines.extend(results);
    }
    lines.sort_unstable_by_key(|(p, _)| *p);
    lines
}

/// Propose `published − self` for `f`; on `StaleVersion` re-read the published state once.
async fn shrink_one(node_id: u64, meta: &dyn ShutdownMeta, f: FollowedPartition) -> SelfShrink {
    let mut cur = f;
    for attempt in 0..2 {
        let Some(isr) = cur.shrunk_isr(node_id) else {
            return SelfShrink::KeptMinInsync;
        };
        let cmd = MetaCommand::AlterIsr {
            partition: cur.partition,
            epoch: cur.epoch,
            expected_isr_version: cur.isr_version,
            isr,
        };
        match meta.alter_isr(cmd).await {
            Ok(MetaResult::Ok { .. }) => return SelfShrink::Shrunk,
            Ok(MetaResult::StaleVersion { current }) if attempt == 0 => {
                info!(
                    partition = cur.partition,
                    expected = cur.isr_version,
                    current,
                    "controlled shutdown: self-shrink lost its CAS; re-reading the published ISR"
                );
                match fresher_view(node_id, meta, &cur).await {
                    Some(g) => cur = g,
                    // Out of the ISR already (someone else removed it) or now the leader /
                    // not a replica: nothing left to shrink.
                    None => return SelfShrink::Shrunk,
                }
            }
            Ok(MetaResult::StaleVersion { .. }) => return SelfShrink::VersionRace,
            Ok(other) => {
                warn!(partition = cur.partition, result = ?other, "controlled shutdown: self-shrink rejected");
                return SelfShrink::Rejected;
            }
            Err(e) => {
                warn!(partition = cur.partition, error = %e, "controlled shutdown: self-shrink outcome unknown");
                return SelfShrink::Rejected;
            }
        }
    }
    SelfShrink::VersionRace
}

/// Bound on waiting for this node's metadata view to show the publication a self-shrink CAS
/// lost to (polled every [`VIEW_POLL`]).
pub const VIEW_CATCH_UP: Duration = Duration::from_secs(1);

/// Poll period of [`VIEW_CATCH_UP`].
pub const VIEW_POLL: Duration = Duration::from_millis(20);

/// After a lost CAS: the partition as this node's metadata view shows it once the view moved
/// past `stale` (another epoch or ISR version), waiting up to [`VIEW_CATCH_UP`] for a view
/// that lags the metadata leader (the CAS ran into a publication this node may not have
/// applied yet; re-reading at once would propose the same stale version again). `None` when
/// the node no longer follows the partition as an ISR member; the last read on timeout.
async fn fresher_view(
    node_id: u64,
    meta: &dyn ShutdownMeta,
    stale: &FollowedPartition,
) -> Option<FollowedPartition> {
    let polls = (VIEW_CATCH_UP.as_millis() / VIEW_POLL.as_millis().max(1)).max(1);
    let mut last = None;
    for i in 0..=polls {
        let now = meta
            .followed(node_id)
            .into_iter()
            .find(|g| g.partition == stale.partition)?;
        if (now.epoch, now.isr_version) != (stale.epoch, stale.isr_version) {
            return Some(now);
        }
        last = Some(now);
        if i < polls {
            tokio::time::sleep(VIEW_POLL).await;
        }
    }
    last
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// Try `lp`'s targets in order: `(Some(new_epoch), _)` when one took it, else `(None, why)`.
async fn hand_off_one(
    node_id: u64,
    lp: &LedPartition,
    handoff: &dyn HandoffClient,
) -> (Option<u32>, String) {
    if lp.targets.is_empty() {
        return (None, "kept (no other ISR member)".into());
    }
    let mut why = Vec::new();
    for &to in &lp.targets {
        match handoff
            .hand_off(node_id, PartitionId(lp.partition), to, Epoch(lp.epoch))
            .await
        {
            Ok(HandoffOutcome::Moved { new_epoch }) => {
                return (Some(new_epoch.0), String::new());
            }
            Ok(HandoffOutcome::Aborted { reason }) => why.push(format!("{to}: {reason}")),
            Err(e) => why.push(format!("{to}: {e}")),
        }
    }
    (None, format!("kept ({})", why.join("; ")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn targets_prefer_replica_order_within_the_isr() {
        assert_eq!(targets(1, &[1, 2, 3], &[1, 2, 3]), vec![2, 3]);
        assert_eq!(targets(2, &[3, 2, 1], &[1, 2, 3]), vec![1, 3]);
        assert_eq!(
            targets(1, &[1, 3], &[1, 2, 3]),
            vec![3],
            "2 is out of the ISR"
        );
        assert_eq!(
            targets(1, &[1, 4], &[1, 2]),
            vec![4],
            "ISR outside replicas last"
        );
        assert!(targets(1, &[1], &[1]).is_empty());
    }
}
