//! Phase 14: implemented in 14-17 (`ApiTokensApi`: token create / list / revoke / rotate /
//! whoami).

use std::sync::Arc;

use ostro_api::ApiExtensions;

use super::P14Ctx;

/// Fill `ApiExtensions.api_tokens`. Stub: leaves it `None`.
pub fn install(_ctx: &Arc<P14Ctx>, _ext: &mut ApiExtensions) {}
