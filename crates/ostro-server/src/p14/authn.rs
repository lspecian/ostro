//! Phase 14: implemented in 14-03 (the meta-backed `Authenticator`, D-P14-4/5).

use std::sync::Arc;

use ostro_api::auth::{AllowDev, Authenticator};

use super::P14Ctx;

/// The API authenticator: `[auth] mode = "none"` → `AllowDev`; token mode → the meta-backed
/// authenticator. Stub: `AllowDev` in both modes.
pub fn authenticator(_ctx: &Arc<P14Ctx>) -> Arc<dyn Authenticator> {
    Arc::new(AllowDev)
}
