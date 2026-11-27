//! Bulk job API (Phase 11, D-P11-20, plan 11-13): create / list / get / cancel node-local
//! bulk jobs over [`crate::services::bulk::BulkRegistry`] (`P11Ctx::bulk`). Called by
//! [`crate::p11::P11Controllers`]; the adapters (11-09) validate the request first.
//!
//! - `create_bulk`: a dry run counts the matches (snapshot bound applied) and returns up
//!   to 20 samples without creating anything; otherwise a `blk_<uuidv7>` job is created,
//!   its file written and its runner started. More than `[bulk] max_matches` matches is
//!   400 `bulk_too_many_matches` (dry run too); `[bulk] max_jobs` unfinished jobs is 429
//!   `bulk_too_many_jobs` with `retry_after_ms`.
//! - `list_bulk` / `get_bulk`: progress (matched, done, skipped, failed with up to 100
//!   reasons, rate, eta). Unknown job: 404.
//! - `cancel_bulk`: no new op starts; an op in flight finishes. A finished job is
//!   returned unchanged.
//!
//! Phase 14 (14-16): a job belongs to the tenant of the principal that created it
//! (`ostro_api::auth::current()`, the guard's request scope). A tenant principal's job
//! selects only its tenant's runs; tenant principals list only their jobs and get 404 for
//! another tenant's job id (get and cancel, the job untouched). System-scope principals (and
//! calls outside any principal scope, i.e. internal callers) see every job.
//!
//! Every call first waits for the bulk service's reload of `<data>/bulk/`
//! ([`crate::services::bulk::BulkRegistry::wait_loaded`], 11-18), so right after a restart
//! the API never answers without the jobs of the previous run.

use ostro_api::ApiError;
use ostro_api::phase11::{BulkCreateRequest, BulkCreateResponse, BulkJobView};

use crate::p11::P11Ctx;

/// The calling principal's tenant (`None` = system scope or no principal scope).
fn caller_tenant() -> Option<String> {
    ostro_api::auth::current().tenant.filter(|t| !t.is_empty())
}

fn job_not_found(job_id: &str) -> ApiError {
    ApiError::not_found(format!("bulk job {job_id} not found"))
}

/// 404 unless the caller may see job `job_id` (missing jobs are 404 too).
fn check_visible(ctx: &P11Ctx, job_id: &str) -> Result<(), ApiError> {
    match (caller_tenant(), ctx.bulk.tenant_of(job_id)) {
        (_, None) => Err(job_not_found(job_id)),
        (None, Some(_)) => Ok(()),
        (Some(t), Some(owner)) if owner.as_deref() == Some(t.as_str()) => Ok(()),
        (Some(_), Some(_)) => Err(job_not_found(job_id)),
    }
}

/// Create a bulk job (or count it on a dry run).
pub async fn create_bulk(
    ctx: &P11Ctx,
    req: BulkCreateRequest,
) -> Result<BulkCreateResponse, ApiError> {
    ctx.bulk.wait_loaded().await;
    ctx.bulk.create(ctx, req, caller_tenant()).await
}

/// Every bulk job on this node, in creation order.
pub async fn list_bulk(ctx: &P11Ctx) -> Result<Vec<BulkJobView>, ApiError> {
    ctx.bulk.wait_loaded().await;
    Ok(match caller_tenant() {
        None => ctx.bulk.list(),
        Some(t) => ctx.bulk.list_of(&t),
    })
}

/// One bulk job.
pub async fn get_bulk(ctx: &P11Ctx, job_id: String) -> Result<BulkJobView, ApiError> {
    ctx.bulk.wait_loaded().await;
    check_visible(ctx, &job_id)?;
    ctx.bulk.get(&job_id).ok_or_else(|| job_not_found(&job_id))
}

/// Cancel a bulk job.
pub async fn cancel_bulk(ctx: &P11Ctx, job_id: String) -> Result<BulkJobView, ApiError> {
    ctx.bulk.wait_loaded().await;
    check_visible(ctx, &job_id)?;
    ctx.bulk.cancel(&job_id).await
}
