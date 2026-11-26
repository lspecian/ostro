//! Phase 14: implemented in 14-15 (scope enforcement, D-P14-3).

use ostro_api::ApiError;
use ostro_api::auth::{Method, Principal};

/// Whether `p` may call `m` (scope check against `Method::scope`). Stub: allowed.
pub fn require(_p: &Principal, _m: Method) -> Result<(), ApiError> {
    Ok(())
}
