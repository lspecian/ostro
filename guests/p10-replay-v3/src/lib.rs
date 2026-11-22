//! `ostro replay` guest v3 (plan 10-11): the v2 reorder behind `ctx.patched("reorder")`.
//! A v1 journal has no marker, so it takes the old order and replays clean; new runs
//! journal the marker and run `b` before `a`.

use std::time::Duration;

use ostro_sdk::prelude::*;

#[step]
fn step_a() -> Result<String, StepError> {
    Ok("a".to_string())
}

#[step]
fn step_b() -> Result<String, StepError> {
    Ok("b".to_string())
}

/// `input.sleep_s` (default 1): a long sleep keeps a run in progress for the tests.
fn sleep_of(input: &serde_json::Value) -> Duration {
    Duration::from_secs(input.get("sleep_s").and_then(serde_json::Value::as_u64).unwrap_or(1))
}

#[workflow(name = "replay_demo", version = 3)]
pub fn replay_demo(ctx: &Ctx, input: serde_json::Value) -> Result<Vec<String>, WorkflowError> {
    let steps = if ctx.patched("reorder") {
        vec![step_b()?, step_a()?]
    } else {
        vec![step_a()?, step_b()?]
    };
    ctx.sleep(sleep_of(&input)).wait()?;
    Ok(steps)
}

ostro_sdk::export!(ReplayDemoWorkflow);
