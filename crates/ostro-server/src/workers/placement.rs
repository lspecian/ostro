//! Phase 7: implemented in 07-12.
//!
//! `NodePlacement`: the node's `ostro_partition::Placement`. Embedded slots first, overflow
//! to worker credit while spilling or for `workers` queues (B6, D-W9); selectors filter
//! sessions; overflow attempts are reported to the spill controller.

use ostro_partition::DispatcherConfig;

use crate::p7::P7Ctx;

/// Install the node's placement into the dispatcher configuration.
///
/// Phase 7 stub (07-02): no-op (the 04-14 slot semaphore stays in charge).
pub fn install(ctx: &P7Ctx, cfg: &mut DispatcherConfig) {
    let _ = (ctx, cfg);
}
