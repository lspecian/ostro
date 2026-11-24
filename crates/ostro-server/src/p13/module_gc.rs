//! Phase 13: implemented in 13-15 (module GC on the meta leader: two scans over every
//! partition's references through `NodeOps.ModuleRefs`, then delete; D-P13-20).
//!
//! Fixed signatures. Stubs: no task; a manual round answers `Unimplemented`.

use std::sync::Arc;

use ostro_api::ApiError;
use ostro_api::phase13::GcRunReport;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{P13Ctx, lands_in};

/// Start the periodic GC (`[modules] gc_interval`). Stub: nothing to start.
pub fn start(ctx: &Arc<P13Ctx>, stop: CancellationToken) -> Vec<JoinHandle<()>> {
    let _ = (ctx, stop);
    Vec::new()
}

/// `POST /v1/modules/gc`: one round now. Stub.
pub async fn run_now(ctx: &P13Ctx) -> Result<GcRunReport, ApiError> {
    let _ = ctx;
    Err(lands_in("run_module_gc", "13-15"))
}
