//! Phase 14: implemented in 14-03 (the guard decorator, D-P14-6).
//!
//! [`wrap`] wraps the routed control plane and every extension slot for the client-facing
//! routers. For each call it reads `ostro_api::auth::current()`, then runs
//! `authz::require` (14-15), `tenant_scope::before` (14-16), `audit_hooks::reserve` (14-18),
//! the inner call, `tenant_scope::after` and `audit_hooks::after`. Final after 14-03.

use std::sync::Arc;

use ostro_api::{ApiExtensions, ControlPlane};

use super::{Guarded, P14Ctx};

/// Stub: the routed instances themselves (passthrough clones).
pub fn wrap(
    _ctx: &Arc<P14Ctx>,
    routed: Arc<dyn ControlPlane>,
    routed_ext: ApiExtensions,
) -> Guarded {
    Guarded {
        control: routed,
        ext: routed_ext,
    }
}
