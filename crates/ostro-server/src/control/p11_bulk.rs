//! Bulk job API (Phase 11, D-P11-20): create / list / get / cancel node-local bulk jobs
//! over [`crate::services::bulk::BulkRegistry`] (`P11Ctx::bulk`). Declared by 11-04
//! (D-P11-23); implemented by **11-13**. Called by [`crate::p11::P11Controllers`].

use ostro_api::ApiError;
use ostro_api::phase11::{BulkCreateRequest, BulkCreateResponse, BulkJobView};

use crate::p11::P11Ctx;

fn stub(method: &str) -> ApiError {
    ApiError::unimplemented(method).with_details(serde_json::json!({
        "method": method,
        "lands_in": "11-13",
    }))
}

/// Create a bulk job (or count it on a dry run). Stub (11-13).
pub async fn create_bulk(
    ctx: &P11Ctx,
    req: BulkCreateRequest,
) -> Result<BulkCreateResponse, ApiError> {
    let _ = (ctx, req);
    Err(stub("create_bulk"))
}

/// Every bulk job on this node. Stub (11-13).
pub async fn list_bulk(ctx: &P11Ctx) -> Result<Vec<BulkJobView>, ApiError> {
    let _ = ctx;
    Err(stub("list_bulk"))
}

/// One bulk job. Stub (11-13).
pub async fn get_bulk(ctx: &P11Ctx, job_id: String) -> Result<BulkJobView, ApiError> {
    let _ = (ctx, job_id);
    Err(stub("get_bulk"))
}

/// Cancel a bulk job. Stub (11-13).
pub async fn cancel_bulk(ctx: &P11Ctx, job_id: String) -> Result<BulkJobView, ApiError> {
    let _ = (ctx, job_id);
    Err(stub("cancel_bulk"))
}
