//! `ostro replay` guest v2 (plan 10-11): `replay_demo` with steps `b` and `a` swapped
//! and no patch: deliberately non-deterministic against a v1 journal (diverges at v1's
//! first step).

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

#[workflow(name = "replay_demo", version = 2)]
pub fn replay_demo(ctx: &Ctx, input: serde_json::Value) -> Result<Vec<String>, WorkflowError> {
    let steps = vec![step_b()?, step_a()?];
    ctx.sleep(sleep_of(&input)).wait()?;
    Ok(steps)
}

ostro_sdk::export!(ReplayDemoWorkflow);
