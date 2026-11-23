//! Run pause / resume and the paused-run `/retry` alias (plan 07-14; OPS-01).
//!
//! `POST /v1/runs/{id}/pause` submits `Cmd::PauseRun{reason: operator, detail, token: None,
//! requested_at_ms: now}` (the server clock, `cp.now_ms()`) to the run's partition;
//! `/resume` submits `Cmd::ResumeRun`. The
//! 07-04 apply decides: pausing a paused run (or resuming one that is not paused) is
//! accepted with no change, a terminal run is `RunTerminal` (409 `run_terminal`), an unknown
//! one `RunNotFound` (404). The answer is `{run_id, paused, changed}`; `changed` comes from
//! the run record read just before the submit (the apply outcome stays authoritative for
//! the error cases).
//!
//! # `/retry` on a paused run
//!
//! `POST /v1/runs/{id}/retry` on a **non-terminal, paused** run acts as `/resume` and
//! answers 200 `{mode: "resumed", run_id, dry_run}` (a dry run submits nothing). `/resume`
//! is the canonical route; the alias exists so the familiar "retry" button works on a run
//! that auto-paused after exhausting its retries (07-08). Every other retry goes the D1
//! path in [`super::lifecycle`] unchanged (a live, unpaused run is still 409
//! `run_not_terminal`).
//!
//! # Cancel / terminate while paused
//!
//! `/cancel` on a paused run journals the cancel request (the 07-04 input gate) and leaves
//! the run paused: nothing runs until a resume, after which the guest observes the cancel
//! and takes its cancelled cleanup path. `/terminate` is immediate (terminal apply clears
//! the pause mark, B7).

use ostro_api::ApiError;
use ostro_api::domain::{RetryMode, RetryRequest, RetryResponse};
use ostro_api::phase7::{PauseRunRequest, RunPauseState};
use ostro_log::PartitionLog;
use ostro_partition::{ApplyOutcome, PartitionHandle, RejectReason};
use ostro_types::RunId;
use ostro_types::command::{Command, PAUSE_REASON_OPERATOR, PauseRun, ResumeRun};

use super::ServerControlPlane;
use super::lifecycle::read_record;
use super::runs::{not_found, parse_run_id, partition_error, status};

/// Map a rejected PauseRun / ResumeRun.
async fn rejected(handle: &PartitionHandle<PartitionLog>, run: RunId, r: RejectReason) -> ApiError {
    match r {
        RejectReason::RunNotFound => not_found(&run.to_string()),
        RejectReason::RunTerminal => match read_record(handle, run).await {
            Ok(rec) => ApiError::run_terminal(&run.to_string(), status(rec.status)),
            Err(e) => e,
        },
        RejectReason::StaleEpoch { .. } | RejectReason::WrongPartition => {
            ApiError::unavailable(format!("partition cannot accept the command: {r:?}"))
        }
        other => ApiError::invalid_argument(format!("command rejected: {other:?}")),
    }
}

async fn submit(
    handle: &PartitionHandle<PartitionLog>,
    run: RunId,
    c: Command,
) -> Result<(), ApiError> {
    match handle.submit_one(c).await.map_err(partition_error)? {
        ApplyOutcome::Accepted | ApplyOutcome::Duplicate { .. } => Ok(()),
        ApplyOutcome::Rejected(e) => Err(rejected(handle, run, e).await),
    }
}

pub(crate) async fn pause_run(
    cp: &ServerControlPlane,
    run_id: String,
    detail: Option<String>,
) -> Result<RunPauseState, ApiError> {
    PauseRunRequest {
        detail: detail.clone(),
    }
    .validate()?;
    let run = parse_run_id(cp, &run_id)?;
    let handle = &cp.partition(run.partition())?;
    let rec = read_record(handle, run).await?;
    if rec.status.is_terminal() {
        return Err(ApiError::run_terminal(&run_id, status(rec.status)));
    }
    let changed = rec.pause.is_none();
    submit(
        handle,
        run,
        Command::pause_run(PauseRun {
            run_id: run.as_bytes().to_vec(),
            reason: PAUSE_REASON_OPERATOR,
            detail: detail.unwrap_or_default(),
            token: None,
            requested_at_ms: cp.now_ms(),
        }),
    )
    .await?;
    Ok(RunPauseState {
        run_id,
        paused: true,
        changed,
    })
}

async fn submit_resume(
    cp: &ServerControlPlane,
    handle: &PartitionHandle<PartitionLog>,
    run: RunId,
) -> Result<(), ApiError> {
    submit(
        handle,
        run,
        Command::resume_run(ResumeRun {
            run_id: run.as_bytes().to_vec(),
            requested_at_ms: cp.now_ms(),
        }),
    )
    .await
}

pub(crate) async fn resume_run(
    cp: &ServerControlPlane,
    run_id: String,
) -> Result<RunPauseState, ApiError> {
    let run = parse_run_id(cp, &run_id)?;
    let handle = &cp.partition(run.partition())?;
    let rec = read_record(handle, run).await?;
    if rec.status.is_terminal() {
        return Err(ApiError::run_terminal(&run_id, status(rec.status)));
    }
    let changed = rec.pause.is_some();
    submit_resume(cp, handle, run).await?;
    Ok(RunPauseState {
        run_id,
        paused: false,
        changed,
    })
}

/// The `/retry` alias: `Some(response)` when the run is non-terminal and paused (resumed,
/// or reported as such for a dry run); `None` to continue with the D1 retry. Lookup errors
/// (bad id, unknown run) are left to the D1 path so its error answers stay unchanged.
pub(crate) async fn retry_alias(
    cp: &ServerControlPlane,
    r: &RetryRequest,
) -> Result<Option<RetryResponse>, ApiError> {
    let Ok(run) = parse_run_id(cp, &r.run_id) else {
        return Ok(None);
    };
    let Ok(handle) = cp.partition(run.partition()) else {
        return Ok(None);
    };
    let handle = &handle;
    let Ok(rec) = read_record(handle, run).await else {
        return Ok(None);
    };
    if rec.status.is_terminal() || rec.pause.is_none() {
        return Ok(None);
    }
    if !r.dry_run {
        submit_resume(cp, handle, run).await?;
    }
    Ok(Some(RetryResponse {
        mode: RetryMode::Resumed,
        run_id: r.run_id.clone(),
        dry_run: r.dry_run,
    }))
}
