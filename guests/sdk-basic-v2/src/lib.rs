//! SDK test guest (plan 04-06): `sdk-basic` **version 2** in its own component. A
//! component dispatches `run` by name only, so two versions of one workflow name must live
//! in two modules; the activation/version tests deploy this next to `sdk-basic` v1.
//! Same input as v1; the output adds `"version": 2`.

use ostro_sdk::prelude::*;

/// `sdk-basic` input (unchanged from v1).
#[derive(Debug, Deserialize)]
pub struct Input {
    pub name: String,
    pub n: u32,
}

/// `sdk-basic` v2 output: v1 fields plus `version`.
#[derive(Debug, Serialize)]
pub struct Output {
    pub greeting: String,
    pub sum: u64,
    pub steps: u32,
    pub version: u32,
}

#[step]
fn greet(name: &str) -> Result<String, StepError> {
    Ok(format!("hello, {name}"))
}

#[step]
fn add(n: u32) -> Result<u64, StepError> {
    Ok((1..=u64::from(n)).sum())
}

#[workflow(name = "sdk-basic", version = 2)]
pub fn sdk_basic(ctx: &Ctx, input: Input) -> Result<Output, WorkflowError> {
    let greeting = greet(&input.name)?;
    let sum = add(input.n)?;
    ctx.set_status(&json!({ "stage": "done", "version": 2 }))?;
    Ok(Output {
        greeting,
        sum,
        steps: 2,
        version: 2,
    })
}

ostro_sdk::export!(SdkBasicWorkflow);
