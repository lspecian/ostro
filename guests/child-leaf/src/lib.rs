//! Child-workflow leaf guest (plan 09-06, WF-06): the children the `child-parent`
//! workflows start.
//!
//! - `leaf_echo`: returns `{"echo": <input>}`.
//! - `leaf_fail`: fails non-retryable with kind `boom`.
//! - `leaf_sleep`: sleeps 1 h; when cancelled it runs the shielded step `cleanup` and ends
//!   cancelled. Returns `{"slept": true}` if the timer fires.
//! - `leaf_can`: input `{"n": N, "generation"?: k}`; continues-as-new with `n - 1` (and
//!   `generation + 1`) until `n == 0`, then returns `{"done": true, "generation": k}`.

use std::time::Duration;

use ostro_sdk::prelude::*;
use serde_json::Value;

#[workflow(name = "leaf_echo", version = 1)]
pub fn leaf_echo(_ctx: &Ctx, input: Value) -> Result<Value, WorkflowError> {
    Ok(json!({ "echo": input }))
}

#[workflow(name = "leaf_fail", version = 1)]
pub fn leaf_fail(_ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    Err(WorkflowError::non_retryable(
        "boom",
        "leaf_fail always fails",
    ))
}

#[step]
fn cleanup() -> Result<String, StepError> {
    Ok("cleaned".to_string())
}

#[workflow(name = "leaf_sleep", version = 1)]
pub fn leaf_sleep(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    match ctx.sleep(Duration::from_secs(3600)).wait() {
        Ok(()) => Ok(json!({ "slept": true })),
        Err(e) if e.is_cancelled() => {
            ctx.shielded(cleanup)?;
            Err(e.into())
        }
        Err(e) => Err(e.into()),
    }
}

/// `leaf_can` input.
#[derive(Debug, Serialize, Deserialize)]
pub struct CanInput {
    pub n: u32,
    #[serde(default)]
    pub generation: u32,
}

#[workflow(name = "leaf_can", version = 1)]
pub fn leaf_can(ctx: &Ctx, input: CanInput) -> Result<Value, WorkflowError> {
    if input.n == 0 {
        return Ok(json!({ "done": true, "generation": input.generation }));
    }
    Err(ctx.continue_as_new(&CanInput {
        n: input.n - 1,
        generation: input.generation + 1,
    }))
}

ostro_sdk::export!(
    LeafEchoWorkflow,
    LeafFailWorkflow,
    LeafSleepWorkflow,
    LeafCanWorkflow
);
