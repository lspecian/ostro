//! Phase 14: implemented in 14-19 (sealed-secret reseal after a sealing-key rotation).

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::P14Ctx;

/// Stub: no task.
pub fn start(_ctx: &P14Ctx, _stop: CancellationToken) -> Option<JoinHandle<()>> {
    None
}
