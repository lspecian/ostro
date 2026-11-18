use ostro_sdk::prelude::*;

#[step(timeout = "5x")]
fn bad_unit() -> Result<(), StepError> {
    Ok(())
}

#[step(retry(max_attempts = 3, initial = "fast"))]
fn bad_initial() -> Result<(), StepError> {
    Ok(())
}

#[step(retry(attempts = 3))]
fn bad_key() -> Result<(), StepError> {
    Ok(())
}

#[step(retry(max_attempts = 3, backoff = "2"))]
fn bad_backoff() -> Result<(), StepError> {
    Ok(())
}

#[step(fuel = -1)]
fn bad_fuel() -> Result<(), StepError> {
    Ok(())
}

#[workflow(name = "x", version = 1, run_timeout = "10 minutes")]
fn bad_run_timeout(_ctx: &Ctx, input: u32) -> Result<u32, WorkflowError> {
    Ok(input)
}

fn main() {}
