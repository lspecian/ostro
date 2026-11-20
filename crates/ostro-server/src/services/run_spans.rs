//! Phase 8: implemented in 08-11.
//!
//! Service: run / activation span emission from post-apply actions (live only). Wired through [`super::Services::start`]; until 08-11 lands it starts no
//! task.

use tokio::task::JoinHandle;

use super::ServiceCtx;

/// Start the service; `None` when it has nothing to run (disabled, or not implemented yet).
pub fn spawn(_ctx: &ServiceCtx) -> Option<JoinHandle<()>> {
    None
}
