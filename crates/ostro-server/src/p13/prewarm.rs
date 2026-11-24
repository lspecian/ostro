//! Phase 13: implemented in 13-14 (AOT pre-warm of newly uploaded modules on every node,
//! bounded; D-P13-19).
//!
//! Fixed signature. Stub: no task.

use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::P13Ctx;

/// Start the pre-warm task. Stub: nothing to start.
pub fn start(ctx: &Arc<P13Ctx>, stop: CancellationToken) -> Vec<JoinHandle<()>> {
    let _ = (ctx, stop);
    Vec::new()
}
