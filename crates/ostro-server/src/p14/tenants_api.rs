//! Phase 14: implemented in 14-17 (`TenantsApi`: tenant admin).

use std::sync::Arc;

use ostro_api::ApiExtensions;

use super::P14Ctx;

/// Fill `ApiExtensions.tenants`. Stub: leaves it `None`.
pub fn install(_ctx: &Arc<P14Ctx>, _ext: &mut ApiExtensions) {}
