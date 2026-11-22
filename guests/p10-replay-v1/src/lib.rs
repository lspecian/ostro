//! `ostro replay` guest v1 (plan 10-11, CLI-07 SC3): workflow `replay_demo` runs step
//! `a`, then step `b`, sleeps (`input.sleep_s`, default 1 s) and returns the steps.
//! `p10-replay-v2` swaps the steps (non-deterministic on a v1 journal: replay exit 4);
//! `p10-replay-v3` swaps them behind `ctx.patched("reorder")` (replays clean: exit 0).
//! Each declares its own version (1, 2, 3) because workflow bindings are write-once.

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

#[workflow(name = "replay_demo", version = 1)]
pub fn replay_demo(ctx: &Ctx, input: serde_json::Value) -> Result<Vec<String>, WorkflowError> {
    let steps = vec![step_a()?, step_b()?];
    ctx.sleep(sleep_of(&input)).wait()?;
    Ok(steps)
}

ostro_sdk::export!(ReplayDemoWorkflow);
