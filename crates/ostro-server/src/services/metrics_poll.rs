//! Phase 8: implemented in 08-16.
//!
//! Service: periodic gauge poller (queues, apply lag, HW lag, ISR, Raft). Wired through [`super::Services::start`]; until 08-16 lands it starts no
//! task.

use tokio::task::JoinHandle;

use super::ServiceCtx;

/// Start the service; `None` when it has nothing to run (disabled, or not implemented yet).
pub fn spawn(_ctx: &ServiceCtx) -> Option<JoinHandle<()>> {
    None
}
