//! DLQ and failure-policy API (Phase 11, D-P11-9..12, D-P11-29): list / requeue /
//! dismiss DLQ entries and `GET|PUT /v1/{workflows,queues}/{name}/failure-policy`.
//! Declared by 11-04 (D-P11-23); implemented by **11-12**. Called by
//! [`crate::p11::P11Controllers`]; [`is_dlq_member`] is also called by 11-11 (search,
//! bulk `dlq` filter).

use ostro_api::ApiError;
use ostro_api::phase11::{
    DlqDismissRequest, DlqEntry, DlqListRequest, DlqPage, DlqRequeueRequest, DlqRequeueResponse,
    FailurePolicy, PolicyScope,
};
use ostro_partition::store::values::{RunRecord, RunStatus};
use ostro_types::RunId;

use crate::p11::P11Ctx;

fn stub(method: &str) -> ApiError {
    ApiError::unimplemented(method).with_details(serde_json::json!({
        "method": method,
        "lands_in": "11-12",
    }))
}

/// List DLQ entries. Stub (11-12).
pub async fn list_dlq(ctx: &P11Ctx, req: DlqListRequest) -> Result<DlqPage, ApiError> {
    let _ = (ctx, req);
    Err(stub("list_dlq"))
}

/// Requeue a DLQ entry. Stub (11-12).
pub async fn requeue_dlq(
    ctx: &P11Ctx,
    req: DlqRequeueRequest,
) -> Result<DlqRequeueResponse, ApiError> {
    let _ = (ctx, req);
    Err(stub("requeue_dlq"))
}

/// Dismiss a DLQ entry. Stub (11-12).
pub async fn dismiss_dlq(ctx: &P11Ctx, req: DlqDismissRequest) -> Result<DlqEntry, ApiError> {
    let _ = (ctx, req);
    Err(stub("dismiss_dlq"))
}

/// The failure policy of a workflow or queue. Stub (11-12).
pub async fn get_failure_policy(
    ctx: &P11Ctx,
    scope: PolicyScope,
) -> Result<FailurePolicy, ApiError> {
    let _ = (ctx, scope);
    Err(stub("get_failure_policy"))
}

/// Replace the failure policy of a workflow or queue. Stub (11-12).
pub async fn set_failure_policy(
    ctx: &P11Ctx,
    scope: PolicyScope,
    p: FailurePolicy,
) -> Result<FailurePolicy, ApiError> {
    let _ = (ctx, scope, p);
    Err(stub("set_failure_policy"))
}

/// Whether `run` (record `rec`) is a DLQ member. Stub: status `Failed | TimedOut`; 11-12
/// subtracts dismissed / requeued marks and disabled queues.
pub fn is_dlq_member(ctx: &P11Ctx, run: &RunId, rec: &RunRecord) -> Result<bool, ApiError> {
    let _ = (ctx, run);
    Ok(matches!(
        rec.status,
        RunStatus::Failed | RunStatus::TimedOut
    ))
}
