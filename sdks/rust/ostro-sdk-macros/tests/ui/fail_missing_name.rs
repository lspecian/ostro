use ostro_sdk::prelude::*;

#[workflow(version = 1)]
fn no_name(_ctx: &Ctx, input: u32) -> Result<u32, WorkflowError> {
    Ok(input)
}

#[workflow(name = "no-version")]
fn no_version(_ctx: &Ctx, input: u32) -> Result<u32, WorkflowError> {
    Ok(input)
}

#[workflow(name = "x", version = 1, colour = "red")]
fn unknown_option(_ctx: &Ctx, input: u32) -> Result<u32, WorkflowError> {
    Ok(input)
}

#[workflow(name = "x", version = 1)]
fn one_param(input: u32) -> Result<u32, WorkflowError> {
    Ok(input)
}

#[workflow(name = "x", version = 1)]
fn not_result(_ctx: &Ctx, input: u32) -> u32 {
    input
}

fn main() {}
