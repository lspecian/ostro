//! Phase 8: implemented in 08-13.
//!
//! Service: bottomless uploader (consistent cut, chunk seal, manifest, upload floor). Wired through [`super::Services::start`]; until 08-13 lands it starts no
//! task.

use tokio::task::JoinHandle;

use super::ServiceCtx;

/// Start the service; `None` when it has nothing to run (disabled, or not implemented yet).
pub fn spawn(_ctx: &ServiceCtx) -> Option<JoinHandle<()>> {
    None
}
