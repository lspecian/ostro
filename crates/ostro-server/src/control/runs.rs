//! Run operations of the real control plane.
//!
//! Plan 04-17 only satisfies the [`ostro_api::ControlPlane`] trait here: every call
//! answers `Unavailable`. Plan 04-18 implements start (API-03), get (API-04/WF-01),
//! history and list and replaces every one of these bodies.

use ostro_api::ApiError;
use ostro_api::domain::{
    HistoryPage, HistoryRequest, ListRunsRequest, RunInfo, RunPage, StartRunRequest, StartRunResult,
};

use super::ServerControlPlane;

const PENDING: &str = "runs API lands in 04-18";

pub(crate) async fn start_run(
    _cp: &ServerControlPlane,
    _req: StartRunRequest,
) -> Result<StartRunResult, ApiError> {
    Err(ApiError::unavailable(PENDING))
}

pub(crate) async fn get_run(
    _cp: &ServerControlPlane,
    _run_id: String,
) -> Result<RunInfo, ApiError> {
    Err(ApiError::unavailable(PENDING))
}

pub(crate) async fn run_history(
    _cp: &ServerControlPlane,
    _req: HistoryRequest,
) -> Result<HistoryPage, ApiError> {
    Err(ApiError::unavailable(PENDING))
}

pub(crate) async fn list_runs(
    _cp: &ServerControlPlane,
    _req: ListRunsRequest,
) -> Result<RunPage, ApiError> {
    Err(ApiError::unavailable(PENDING))
}
