//! Patching test guest v3 (plan 05-03, WF-08): `patch-demo` after every run of v1 is
//! gone. The `patched("add-c")` call is removed and step C always runs. A v2 journal
//! replayed here skips its unqueried `PatchMarker` (D6) and completes as [A, C, B].

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

#[step]
fn step_c() -> Result<String, StepError> {
    Ok("C".to_string())
}

#[workflow(name = "patch-demo", version = 1)]
pub fn patch_demo(ctx: &Ctx, _input: serde_json::Value) -> Result<Vec<String>, WorkflowError> {
    let mut steps = vec![step_a()?, step_c()?];
    ctx.sleep(Duration::from_secs(2)).wait()?;
    steps.push(step_b()?);
    Ok(steps)
}

ostro_sdk::export!(PatchDemoWorkflow);
