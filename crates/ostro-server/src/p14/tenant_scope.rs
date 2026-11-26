//! Phase 14: implemented in 14-16 (tenant scoping, D-P14-7/38): per `Method` tenant class,
//! force or reject tenant fields before the call and filter results after it.

use ostro_api::ApiError;
use ostro_api::auth::{Method, Principal};

/// Before the inner call: check/force the request's tenant for `p`. Stub: passthrough.
pub fn before<Req>(_p: &Principal, _m: Method, req: Req) -> Result<Req, ApiError> {
    Ok(req)
}

/// After the inner call: filter/strip the response for `p`. Stub: passthrough.
pub fn after<Resp>(_p: &Principal, _m: Method, resp: Resp) -> Result<Resp, ApiError> {
    Ok(resp)
}
