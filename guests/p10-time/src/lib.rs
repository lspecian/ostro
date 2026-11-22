//! Time-skipping guest (plan 10-10, SDK-06): long durable waits that a dev clock skip
//! fast-forwards. One component, four workflows (each v1):
//!
//! - `sleep_for` `{ms}`: sleeps `ms`, returns `{"slept": ms}`.
//! - `backoff_step` `{fail_times}`: step `flaky` with retry(max_attempts = 5, initial =
//!   max = 1h, backoff 1.0) fails while less than `fail_times` hours of workflow time have
//!   passed since the step was first scheduled (time, not guest statics: guest memory resets
//!   on every requeue), so with one 1 h skip per failure it fails `fail_times` times, then
//!   returns the elapsed hours.
//! - `wait_forever` (run timeout 48h): waits for signal `never`.
//! - `signal_then_sleep`: waits for signal `go`, sleeps 1 day, returns the payload.

use std::time::{Duration, SystemTime};

use ostro_sdk::prelude::*;

const HOUR: Duration = Duration::from_secs(3600);

type Value = serde_json::Value;

/// `sleep_for` input.
#[derive(Debug, Deserialize)]
pub struct SleepInput {
    pub ms: u64,
}

#[workflow(name = "sleep_for", version = 1)]
pub fn sleep_for(ctx: &Ctx, input: SleepInput) -> Result<Value, WorkflowError> {
    ctx.sleep(Duration::from_millis(input.ms)).wait()?;
    Ok(json!({ "slept": input.ms }))
}

/// `backoff_step` input.
#[derive(Debug, Deserialize)]
pub struct BackoffInput {
    pub fail_times: u32,
}

#[step(retry(max_attempts = 5, initial = "1h", max = "1h", backoff = 1.0))]
fn flaky(ctx: &Ctx, t0: SystemTime, fail_times: u32) -> Result<u32, StepError> {
    let elapsed = ctx.now().duration_since(t0).unwrap_or(Duration::ZERO);
    let hours = u32::try_from(elapsed.as_secs() / HOUR.as_secs()).unwrap_or(u32::MAX);
    if hours < fail_times {
        Err(StepError::retryable("transient", "not yet"))
    } else {
        Ok(hours)
    }
}

#[workflow(name = "backoff_step", version = 1)]
pub fn backoff_step(ctx: &Ctx, input: BackoffInput) -> Result<Value, WorkflowError> {
    let t0 = ctx.now();
    let hours = flaky(ctx, t0, input.fail_times)?;
    Ok(json!({ "hours": hours }))
}

#[workflow(name = "wait_forever", version = 1, run_timeout = "48h")]
pub fn wait_forever(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let v = ctx.wait_signal::<Value>("never", None).wait()?;
    Ok(v)
}

#[workflow(name = "signal_then_sleep", version = 1)]
pub fn signal_then_sleep(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let v = ctx.wait_signal::<Value>("go", None).wait()?;
    ctx.sleep(Duration::from_secs(24 * 3600)).wait()?;
    Ok(v)
}

ostro_sdk::export!(
    SleepForWorkflow,
    BackoffStepWorkflow,
    WaitForeverWorkflow,
    SignalThenSleepWorkflow
);
