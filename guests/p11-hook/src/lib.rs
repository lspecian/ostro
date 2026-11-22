//! DLQ / `on_failure` test guest (plan 11-12, OPS-04), exercised by
//! `crates/ostro-server/tests/dlq_api.rs`.
//!
//! - `w` (v1), input `{fail: bool, msg: string}` (both optional): with `fail`, step `s1`
//!   fails non-retryably with `msg` (default `"boom"`), so the run ends `Failed` with the
//!   failure JSON `{"reason_code":"workflow_failed","kind":"asked","message":<msg>}`;
//!   otherwise it completes with `"ok"`.
//! - `w_slow` (v1, run timeout 1 s): a durable sleep of one hour, so the run ends
//!   `TimedOut` (`terminal_reason = run_timeout`).
//! - `on_fail` (v1): the hook. Records its input as step `record` and returns it.
//! - `always_fail` (v1): fails immediately (non-retryable), whatever the input.

use std::time::Duration;

use ostro_sdk::prelude::*;
use serde_json::Value;

/// `w` input.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Input {
    pub fail: bool,
    pub msg: Option<String>,
}

#[workflow(name = "w", version = 1)]
pub fn w(_ctx: &Ctx, input: Input) -> Result<String, WorkflowError> {
    let msg = input.msg.unwrap_or_else(|| "boom".to_owned());
    let fail = input.fail;
    let r: String = run_step("s1", &StepOptions::default(), || {
        if fail {
            Err(StepError::non_retryable("asked", msg.clone()))
        } else {
            Ok("ok".to_owned())
        }
    })?;
    Ok(r)
}

#[workflow(name = "w_slow", version = 1, run_timeout = "1s")]
pub fn w_slow(ctx: &Ctx, _input: Value) -> Result<String, WorkflowError> {
    ctx.sleep(Duration::from_secs(3600)).wait()?;
    Ok("woke".to_owned())
}

#[workflow(name = "on_fail", version = 1)]
pub fn on_fail(_ctx: &Ctx, input: Value) -> Result<Value, WorkflowError> {
    let seen = input.clone();
    let r: Value = run_step("record", &StepOptions::default(), move || Ok(seen.clone()))?;
    Ok(r)
}

#[workflow(name = "always_fail", version = 1)]
pub fn always_fail(_ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    Err(WorkflowError::non_retryable("always", "always_fail fails"))
}

ostro_sdk::export!(WWorkflow, WSlowWorkflow, OnFailWorkflow, AlwaysFailWorkflow);
