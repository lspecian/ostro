//! Run-logs controller: `ostro_api::phase8::RunLogsApi` over the guest log sink's
//! per-run ring buffers ([`super::ControlCtx::guest_logs`], P8-D16; non-durable).
//! **Owned by 08-17**; a stub until then (answers `Unavailable`, "not yet implemented
//! (08-17)").

use ostro_api::ApiError;
use ostro_api::phase8::{RunLogsApi, RunLogsRequest, RunLogsResponse};

use super::{ControlCtx, not_yet};

/// The plan that implements this controller.
const OWNER: &str = "08-17";

/// `RunLogsApi` for the server.
#[derive(Debug)]
pub struct RunLogsController {
    ctx: ControlCtx,
}

impl RunLogsController {
    /// Over the shared controller context.
    pub fn new(ctx: ControlCtx) -> Self {
        Self { ctx }
    }

    /// The shared controller context.
    pub fn ctx(&self) -> &ControlCtx {
        &self.ctx
    }
}

#[async_trait::async_trait]
impl RunLogsApi for RunLogsController {
    async fn run_logs(&self, _req: RunLogsRequest) -> Result<RunLogsResponse, ApiError> {
        Err(not_yet(OWNER))
    }
}
