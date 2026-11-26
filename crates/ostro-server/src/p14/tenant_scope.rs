//! Phase 14: implemented in 14-16 (tenant scoping, D-P14-7/38): per `Method` tenant class,
//! force or reject tenant fields before the call and filter results after it.
//!
//! Called by the guard (`guard.rs`, final after 14-03) for every guarded method:
//! `before(p, m, inner, req)` with the method's request (the single argument; `()` for none;
//! a tuple for several, in parameter order) and the routed control plane for lookups (a
//! run's tenant), and `after(p, m, out)` with the inner call's result.

use ostro_api::auth::{Method, Principal};
use ostro_api::{ApiError, ControlPlane};

/// Before the inner call: check/force the request's tenant for `p`. Stub: passthrough.
pub async fn before<Req: Send>(
    _p: &Principal,
    _m: Method,
    _inner: &dyn ControlPlane,
    req: Req,
) -> Result<Req, ApiError> {
    Ok(req)
}

/// After the inner call: filter/strip the response for `p`. Stub: passthrough.
pub fn after<Resp>(
    _p: &Principal,
    _m: Method,
    out: Result<Resp, ApiError>,
) -> Result<Resp, ApiError> {
    out
}
