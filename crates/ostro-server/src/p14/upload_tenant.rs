//! Phase 14: implemented in 14-16 (per-tenant module registration of an existing sha,
//! ext 114 `ModuleUploaders`).

use super::{P14Ctx, RegisterDecision};

/// Stub: the Phase 13 behaviour (register the tenant's versions from the stored module).
pub async fn register_existing(
    _ctx: &P14Ctx,
    _sha: [u8; 32],
    _tenant: Option<&str>,
) -> RegisterDecision {
    RegisterDecision::Proceed
}
