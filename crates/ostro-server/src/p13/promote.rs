//! Phase 13: implemented in 13-16 (`POST /v1/cluster/init`: online promotion of a running
//! single server-profile node; D-P13-11).
//!
//! Fixed signature. Stub: `Unimplemented`.

use ostro_api::ApiError;
use ostro_api::phase13::{ClusterStatus, InitRequest};

use super::{P13Ctx, lands_in};

/// Promote this node to a cluster. Stub.
pub async fn init(ctx: &P13Ctx, r: InitRequest) -> Result<ClusterStatus, ApiError> {
    let _ = (ctx, r);
    Err(lands_in("init", "13-16"))
}
