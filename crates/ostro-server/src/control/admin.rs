//! Admin controller: `ostro_api::phase8::AdminApi`, the online backup (`GET
//! /v1/admin/backup`, P8-D7: the bottomless layout written to a LocalFs staging area and
//! streamed as `application/x-tar`, e.g. `axum::body::Body::from_stream` over the tar
//! writer). **Owned by 08-18**; a stub until then (answers `Unavailable`, "not yet
//! implemented (08-18)").

use ostro_api::ApiError;
use ostro_api::phase8::{AdminApi, BackupStream};

use super::{ControlCtx, not_yet};

/// The plan that implements this controller.
const OWNER: &str = "08-18";

/// `AdminApi` for the server.
#[derive(Debug)]
pub struct AdminController {
    ctx: ControlCtx,
}

impl AdminController {
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
impl AdminApi for AdminController {
    async fn backup(&self) -> Result<BackupStream, ApiError> {
        Err(not_yet(OWNER))
    }
}
