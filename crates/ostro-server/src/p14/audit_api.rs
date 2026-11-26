//! Phase 14: implemented in 14-18 (`AuditApi`: the cluster-wide audit query).

use std::sync::Arc;

use ostro_api::ApiExtensions;

use super::P14Ctx;

/// Fill `ApiExtensions.audit`. Stub: leaves it `None`.
pub fn install(_ctx: &Arc<P14Ctx>, _ext: &mut ApiExtensions) {}
