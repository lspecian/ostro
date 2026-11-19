//! Patching test guest v1 (plan 05-03, WF-08): workflow `patch-demo` v1 before the
//! `add-c` change. Step A, a 2 s durable sleep, step B; the output is the list of steps
//! that ran. `patch-v2` adds step C behind `ctx.patched("add-c")`, `patch-v3` removes
//! the `patched` call and always runs C. All three export the same (name, version), so
//! a journal recorded by one can be replayed by another (`tests/patching.rs`).

use std::time::Duration;

use ostro_sdk::prelude::*;

#[step]
fn step_a() -> Result<String, StepError> {
    Ok("A".to_string())
}

#[step]
fn step_b() -> Result<String, StepError> {
    Ok("B".to_string())
}

#[workflow(name = "patch-demo", version = 1)]
pub fn patch_demo(ctx: &Ctx, _input: serde_json::Value) -> Result<Vec<String>, WorkflowError> {
    let mut steps = vec![step_a()?];
    ctx.sleep(Duration::from_secs(2)).wait()?;
    steps.push(step_b()?);
    Ok(steps)
}

ostro_sdk::export!(PatchDemoWorkflow);
