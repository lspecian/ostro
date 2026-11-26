//! Phase 14: implemented in 14-19 (`SecurityApi`: module policy, trusted keys, attached
//! signatures).

use std::sync::Arc;

use ostro_api::ApiExtensions;

use super::P14Ctx;

/// Fill `ApiExtensions.security`. Stub: leaves it `None`.
pub fn install(_ctx: &Arc<P14Ctx>, _ext: &mut ApiExtensions) {}
