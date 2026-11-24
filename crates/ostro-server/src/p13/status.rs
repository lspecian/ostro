//! Phase 13: implemented in 13-09 (`GET /v1/cluster/status` from the meta view after a read
//! barrier).
//!
//! Fixed signature. Stub: `Unimplemented`.

use ostro_api::ApiError;
use ostro_api::phase13::ClusterStatus;

use super::{P13Ctx, lands_in};

/// The cluster status document. Stub.
pub async fn status(ctx: &P13Ctx) -> Result<ClusterStatus, ApiError> {
    let _ = ctx;
    Err(lands_in("status", "13-09"))
}
