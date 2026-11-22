//! Indexed run search (Phase 11, D-P11-10): `ListRunsRequest` served from the partition
//! run index instead of the scan in [`super::list`]. Declared by 11-04 (D-P11-23);
//! implemented by **11-11**.

use ostro_api::ApiError;
use ostro_api::domain::{ListRunsRequest, RunPage};

use crate::p11::P11Ctx;

/// `Some(page)` when the index can serve `req`; `None` falls back to the existing scan
/// ([`super::list::list_runs`]). Called first by `ServerControlPlane::list_runs` once
/// the P11 context is set. Stub: always `None` (11-11).
pub async fn list_runs_indexed(
    ctx: &P11Ctx,
    req: &ListRunsRequest,
) -> Option<Result<RunPage, ApiError>> {
    let _ = (ctx, req);
    None
}
