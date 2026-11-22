//! Limits and inventory views (Phase 11, D-P11-13, D-P11-19): `GET /v1/limits`,
//! `/v1/modules`, `/v1/queues`, `/v1/partitions`. Declared by 11-04 (D-P11-23);
//! implemented by **11-11**. Called by [`crate::p11::P11Controllers`].

use ostro_api::ApiError;
use ostro_api::phase11::{LimitsView, ListModulesRequest, ModulePage, PartitionView, QueueView};

use crate::p11::P11Ctx;

fn stub(method: &str) -> ApiError {
    ApiError::unimplemented(method).with_details(serde_json::json!({
        "method": method,
        "lands_in": "11-11",
    }))
}

/// The published limits. Stub (11-11).
pub async fn limits(ctx: &P11Ctx) -> Result<LimitsView, ApiError> {
    let _ = ctx;
    Err(stub("limits"))
}

/// Stored modules. Stub (11-11).
pub async fn list_modules(ctx: &P11Ctx, req: ListModulesRequest) -> Result<ModulePage, ApiError> {
    let _ = (ctx, req);
    Err(stub("list_modules"))
}

/// Queues with counts. Stub (11-11).
pub async fn list_queues(ctx: &P11Ctx) -> Result<Vec<QueueView>, ApiError> {
    let _ = ctx;
    Err(stub("list_queues"))
}

/// Partitions with role and health. Stub (11-11).
pub async fn list_partitions(ctx: &P11Ctx) -> Result<Vec<PartitionView>, ApiError> {
    let _ = ctx;
    Err(stub("list_partitions"))
}
