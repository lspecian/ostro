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
use ostro_meta::MetaHandle;
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
}

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
        warn!(
            kept = ?kept,
            "controlled shutdown: no metadata quorum; skipping the handoffs (Phase 12 failover)"
        );
        return ShutdownReport {
            moved: Vec::new(),
            kept,
            meta_transferred: false,
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
    }
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
