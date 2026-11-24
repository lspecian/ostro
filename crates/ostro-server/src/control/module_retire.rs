//! Phase 13: implemented in 13-15 (module retirement, CLUS-07 / D-P13-20): `DELETE
//! /v1/modules/{sha}` marks a module Retired (ext 102) when none of its workflow versions is
//! active; a retired module refuses new starts and activations (`module_retired`).
//!
//! Fixed signatures. The call sites exist since 13-02: the start path
//! ([`super::runs`]) and activation ([`super::modules`]) call [`check_not_retired`]. Stubs:
//! nothing is retired, every check passes, `retire` answers `Unimplemented`.

use ostro_api::ApiError;
use ostro_api::phase13::RetireModuleResponse;
use ostro_meta::MetaView;

use crate::p13::{P13Ctx, lands_in};

/// Retire module `sha` (hex). Stub.
pub async fn retire(ctx: &P13Ctx, sha: String) -> Result<RetireModuleResponse, ApiError> {
    let _ = (ctx, sha);
    Err(lands_in("retire_module", "13-15"))
}

/// Whether module `sha` is retired (its Module record carries ext 102). Stub: false.
pub fn is_retired(view: &MetaView, sha: &[u8; 32]) -> bool {
    let _ = (view, sha);
    false
}

/// `module_retired` (409) when module `sha` is retired. Stub: Ok.
pub fn check_not_retired(view: &MetaView, sha: &[u8; 32]) -> Result<(), ApiError> {
    let _ = (view, sha);
    Ok(())
}
