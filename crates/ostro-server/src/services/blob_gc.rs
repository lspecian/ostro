//! Phase 8: implemented in 08-19.
//!
//! Service: local blob garbage collection (mark from every partition, sweep after the grace). Wired through [`super::Services::start`]; until 08-19 lands it starts no
//! task.

use tokio::task::JoinHandle;

use super::ServiceCtx;

/// Start the service; `None` when it has nothing to run (disabled, or not implemented yet).
pub fn spawn(_ctx: &ServiceCtx) -> Option<JoinHandle<()>> {
    None
}
