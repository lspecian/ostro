//! SDK test guest (plan 04-06) for step retries, exercised end to end by the server tests
//! once the host returns `retry-now` / requeues for backoff (plan 04-07).
//!
//! Guest memory resets on eviction, so tests must count attempts from the journal
//! (`StepAttemptFailed` events), never from guest statics. Input:
//! `{ "mode": String, "fail_times": u32 }` (`fail_times` defaults to 0). Output:
//! `{ "mode": String, "before": u32, "attempts": u32 | null }`.
//!
//! Every mode first runs the step `before`, which returns a per-instance side-effect
//! counter: a replayed `before` keeps its recorded value and the journal holds exactly one
//! `StepCompleted` for it, even across restarts. Modes (step name in brackets):
//!
//! - `exhaust` [`flaky`]: retry(max_attempts = 4, initial = 50ms, max = 200ms, backoff 2.0,
//!   non_retryable = ["fatal"]); always `retryable("transient")` -> 3 x StepAttemptFailed
//!   + StepCompleted(Err); the workflow fails with that error.
//! - `recover_hot` [`recover`]: same policy; fails while the in-activation attempt counter
//!   (`static ATTEMPTS`) is < `fail_times`, then succeeds; `attempts` = attempts made.
//! - `non_retryable` [`fatal`]: same policy; `non_retryable("fatal")` -> exactly one
//!   attempt, StepCompleted(Err).
//! - `long_backoff` [`slow`]: retry(max_attempts = 3, initial = max = 3s); fails while
//!   `ctx.now() - t0 < 2s` (t0 captured before the step) -> one Requeue backoff, then
//!   success.
//! - `exhaust_slow` [`flaky_slow`]: retry(max_attempts = 3, initial = 3s); always fails
//!   (restart-between-attempts test: exactly 3 attempts in total).

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime};

use ostro_sdk::prelude::*;

static BEFORE_RUNS: AtomicU32 = AtomicU32::new(0);
static ATTEMPTS: AtomicU32 = AtomicU32::new(0);

/// `sdk-retry` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    pub mode: String,
    #[serde(default)]
    pub fail_times: u32,
}

/// `sdk-retry` output.
#[derive(Debug, Serialize)]
pub struct Output {
    pub mode: String,
    pub before: u32,
    pub attempts: Option<u32>,
}

fn transient() -> StepError {
    StepError::retryable("transient", "simulated transient failure")
}

#[step]
fn before() -> Result<u32, StepError> {
    Ok(BEFORE_RUNS.fetch_add(1, Ordering::SeqCst) + 1)
}

#[step(retry(
    max_attempts = 4,
    initial = "50ms",
    max = "200ms",
    backoff = 2.0,
    non_retryable = ["fatal"]
))]
fn flaky() -> Result<u32, StepError> {
    Err(transient())
}

#[step(retry(
    max_attempts = 4,
    initial = "50ms",
    max = "200ms",
    backoff = 2.0,
    non_retryable = ["fatal"]
))]
fn recover(fail_times: u32) -> Result<u32, StepError> {
    let n = ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
    if n <= fail_times {
        Err(transient())
    } else {
        Ok(n)
    }
}

#[step(retry(
    max_attempts = 4,
    initial = "50ms",
    max = "200ms",
    backoff = 2.0,
    non_retryable = ["fatal"]
))]
fn fatal() -> Result<u32, StepError> {
    Err(StepError::non_retryable(
        "fatal",
        "simulated permanent failure",
    ))
}

#[step(retry(max_attempts = 3, initial = "3s", max = "3s"))]
fn slow(ctx: &Ctx, t0: SystemTime) -> Result<u32, StepError> {
    let elapsed = ctx.now().duration_since(t0).unwrap_or(Duration::ZERO);
    if elapsed < Duration::from_secs(2) {
        Err(transient())
    } else {
        Ok(u32::try_from(elapsed.as_millis()).unwrap_or(u32::MAX))
    }
}

#[step(retry(max_attempts = 3, initial = "3s"))]
fn flaky_slow() -> Result<u32, StepError> {
    Err(transient())
}

#[workflow(name = "sdk-retry", version = 1)]
pub fn sdk_retry(ctx: &Ctx, input: Input) -> Result<Output, WorkflowError> {
    let before = before()?;
    let attempts = match input.mode.as_str() {
        "exhaust" => Some(flaky()?),
        "recover_hot" => Some(recover(input.fail_times)?),
        "non_retryable" => Some(fatal()?),
        "long_backoff" => {
            let t0 = ctx.now();
            slow(ctx, t0)?;
            None
        }
        "exhaust_slow" => Some(flaky_slow()?),
        other => {
            return Err(WorkflowError::non_retryable(
                "bad_mode",
                format!("unknown mode {other:?}"),
            ));
        }
    };
    Ok(Output {
        mode: input.mode,
        before,
        attempts,
    })
}

ostro_sdk::export!(SdkRetryWorkflow);
