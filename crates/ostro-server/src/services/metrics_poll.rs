//! Gauge poller (OBS-02, P8-D15, plan 08-16).
//!
//! Every [`POLL_INTERVAL`] (and once right at start) the service samples read-only state
//! and sets gauges, so no hot path takes a lock or pays for a gauge:
//!
//! | Series | Labels | Value |
//! |---|---|---|
//! | `ostro_partition_apply_lag` | `partition` | HW − applied offset (records not yet applied) |
//! | `ostro_partition_hw_lag` | `partition`, `replica` | leader HW − replica HW (RF=1: one replica, the node itself, lag 0) |
//! | `ostro_isr_size` | `partition` | in-sync replicas (RF=1: 1) |
//! | `ostro_wal_log_start` | `partition` | first retained WAL offset |
//! | `ostro_wal_log_end` | `partition` | WAL log end offset (exclusive) |
//! | `ostro_raft_term` | — | metadata Raft term |
//! | `ostro_raft_commit_index` | — | metadata Raft commit index |
//! | `ostro_raft_applied_index` | — | metadata Raft applied index |
//! | `ostro_raft_is_leader` | — | 1 when this node leads the metadata group |
//!
//! Sources: the machine's `applied` watch, the log's HW watch and its atomic log
//! start / end, and the metadata driver's published status. None of them takes the apply
//! lock or reaches into the apply thread. Failed partition slots publish nothing.
//!
//! Time: the poll cadence is a metric sampling interval, not logic; no clock read feeds a
//! decision (Phase 10 F5).

use std::time::Duration;

use ostro_log::ReplicatedLog;
use ostro_partition::PartitionSlot;
use tokio::task::JoinHandle;

use super::ServiceCtx;

/// Sampling interval.
pub const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Start the poller. It always runs (the Raft gauges exist without partitions).
pub fn spawn(ctx: &ServiceCtx) -> Option<JoinHandle<()>> {
    describe();
    let ctx = ctx.clone();
    Some(tokio::spawn(async move {
        loop {
            poll_once(&ctx);
            tokio::select! {
                biased;
                _ = ctx.shutdown.cancelled() => return,
                // clock-exempt: metric sampling cadence; nothing depends on the timing.
                () = tokio::time::sleep(POLL_INTERVAL) => {}
            }
        }
    }))
}

/// Sample every gauge once (public for tests and tooling).
pub fn poll_once(ctx: &ServiceCtx) {
    let node_label = ctx.config.node.node_id.to_string();
    for slot in ctx.node.slots() {
        let PartitionSlot::Ready(h) = slot else {
            continue;
        };
        let p = h.partition().0.to_string();
        let log = h.log();
        let hw = log.high_watermark().borrow().0;
        let applied = h.machine().applied().borrow().0;
        metrics::gauge!("ostro_partition_apply_lag", "partition" => p.clone())
            .set(hw.saturating_sub(applied) as f64);
        // RF=1: the leader is the only replica and always in sync.
        metrics::gauge!(
            "ostro_partition_hw_lag",
            "partition" => p.clone(),
            "replica" => node_label.clone()
        )
        .set(0.0);
        metrics::gauge!("ostro_isr_size", "partition" => p.clone()).set(1.0);
        metrics::gauge!("ostro_wal_log_start", "partition" => p.clone())
            .set(log.log_start().0 as f64);
        metrics::gauge!("ostro_wal_log_end", "partition" => p).set(log.log_end().0 as f64);
    }
    let r = ctx.meta.raft_status();
    metrics::gauge!("ostro_raft_term").set(r.term as f64);
    metrics::gauge!("ostro_raft_commit_index").set(r.commit_index as f64);
    metrics::gauge!("ostro_raft_applied_index").set(r.applied_index as f64);
    metrics::gauge!("ostro_raft_is_leader").set(if r.is_leader { 1.0 } else { 0.0 });
}

/// Help text of the polled series.
fn describe() {
    metrics::describe_gauge!(
        "ostro_partition_apply_lag",
        "Records committed (below HW) but not yet applied, per partition."
    );
    metrics::describe_gauge!(
        "ostro_partition_hw_lag",
        "Leader high watermark minus the replica's, per partition and replica (RF=1: 0)."
    );
    metrics::describe_gauge!(
        "ostro_isr_size",
        "In-sync replicas per partition (RF=1: 1)."
    );
    metrics::describe_gauge!(
        "ostro_wal_log_start",
        "First retained WAL offset per partition."
    );
    metrics::describe_gauge!(
        "ostro_wal_log_end",
        "WAL log end offset (exclusive) per partition."
    );
    metrics::describe_gauge!("ostro_raft_term", "Metadata Raft term.");
    metrics::describe_gauge!("ostro_raft_commit_index", "Metadata Raft commit index.");
    metrics::describe_gauge!("ostro_raft_applied_index", "Metadata Raft applied index.");
    metrics::describe_gauge!(
        "ostro_raft_is_leader",
        "1 when this node leads the metadata Raft group, else 0."
    );
}
