//! Patching test guest v2 (plan 05-03, WF-08): `patch-demo` v1 with step C added behind
//! `ctx.patched("add-c")`. The change-id is queried twice; the second query must return
//! the memo of the first (one marker per id per run). A v1 journal replayed here takes
//! the old path (no C); a fresh run takes the new path: [A, C, B].

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
    let mut steps = vec![step_a()?];
    let add_c = ctx.patched("add-c");
    if add_c {
        steps.push(step_c()?);
    }
    if ctx.patched("add-c") != add_c {
        return Err(WorkflowError::non_retryable(
            "patch_memo",
            "patched(\"add-c\") changed within one run",
        ));
    }
    ctx.sleep(Duration::from_secs(2)).wait()?;
    steps.push(step_b()?);
    Ok(steps)
}

ostro_sdk::export!(PatchDemoWorkflow);
