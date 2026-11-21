//! Phase 7: implemented in 07-16.
//!
//! Worker demand (D-W12): `demand(q) = running_on_workers(q) + admissible_backlog_for_workers(q)`,
//! published every 1 s as `ostro_queue_worker_demand{queue}` (Phase 7 owns the value; 08-16
//! only describes it) and served by `GET /v1/queues/{q}/demand`.

use serde::Serialize;
use tokio::task::JoinHandle;

use crate::p7::{P7Ctx, P7Shared};

/// `GET /v1/queues/{q}/demand` body.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct DemandSnapshot {
    pub queue: String,
    pub demand: u64,
    pub backlog: u64,
    pub running_on_workers: u64,
    /// `embedded` | `workers` | `auto`.
    pub placement: String,
    pub spilling: bool,
    pub workers: u64,
    pub worker_slots_free: u64,
}

/// Start the demand ticker; `None` when nothing runs.
///
/// Phase 7 stub (07-02): `None`.
pub fn start(ctx: &P7Ctx) -> Option<JoinHandle<()>> {
    let _ = ctx;
    None
}

/// The current demand of `queue`.
///
/// Phase 7 stub (07-02): zeros (placement `embedded`).
pub fn snapshot(shared: &P7Shared, queue: &str) -> DemandSnapshot {
    let _ = shared;
    DemandSnapshot {
        queue: queue.to_owned(),
        placement: "embedded".to_owned(),
        ..DemandSnapshot::default()
    }
}
