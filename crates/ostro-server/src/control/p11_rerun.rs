//! Rerun API (Phase 11, D-P11-3..8): `POST /v1/runs/{id}/rerun` and
//! `GET /v1/runs/{id}/rerun-cuts`. Declared by 11-04 (D-P11-23); implemented by
//! **11-10** (replay check through `P11Ctx::runtime`). Called by
//! [`crate::p11::P11Controllers`].

use ostro_api::ApiError;
use ostro_api::phase11::{RerunCutsView, RerunRequest, RerunResponse};

use crate::p11::P11Ctx;

fn stub(method: &str) -> ApiError {
    ApiError::unimplemented(method).with_details(serde_json::json!({
        "method": method,
        "lands_in": "11-10",
    }))
}

/// Rerun a terminal run from a cut. Stub (11-10).
pub async fn rerun_run(ctx: &P11Ctx, req: RerunRequest) -> Result<RerunResponse, ApiError> {
    let _ = (ctx, req);
    Err(stub("rerun_run"))
}

/// Per-step safe/unsafe cuts of a run. Stub (11-10).
pub async fn analyze_rerun(ctx: &P11Ctx, run_id: String) -> Result<RerunCutsView, ApiError> {
    let _ = (ctx, run_id);
    Err(stub("analyze_rerun"))
}
