//! Phase 13: implemented in 13-09 (the server side of the join handshake: node join tokens,
//! id allocation / resume, CSR signing with `__ostro/cluster-ca`; D-P13-9, D-P13-10).
//!
//! Fixed signatures (called by [`super::cluster_api`] and `promote::init`). Stubs answer
//! `Unimplemented`; [`ensure_cluster_ca`] stores nothing.

use ostro_api::ApiError;
use ostro_api::phase13::{JoinRequest, JoinResponse, NodeTokenRequest, NodeTokenResponse};

use super::{P13Ctx, lands_in};

/// `POST /v1/cluster/join`: admit a joining node (any member serves it). Stub.
pub async fn handle_join(ctx: &P13Ctx, r: JoinRequest) -> Result<JoinResponse, ApiError> {
    let _ = (ctx, r);
    Err(lands_in("join", "13-09"))
}

/// `POST /v1/cluster/tokens`: a `njt_` node join token (scope `node:join`). Stub.
pub async fn create_node_token(
    ctx: &P13Ctx,
    r: NodeTokenRequest,
) -> Result<NodeTokenResponse, ApiError> {
    let _ = (ctx, r);
    Err(lands_in("create_node_token", "13-09"))
}

/// Store this node's CA as `__ostro/cluster-ca` if absent (create-only, never overwrite).
/// Stub: stores nothing.
pub async fn ensure_cluster_ca(ctx: &P13Ctx) -> Result<(), ApiError> {
    let _ = ctx;
    Ok(())
}
