//! SDK test guest (plan 04-06): `#[workflow]` / `#[step]`, typed JSON input/output,
//! set-status, `join!` and `select!` over durable sleeps, and two workflows exported from
//! one component (`sdk-basic` v1 and `sdk-echo` v1).

use std::time::Duration;

use ostro_sdk::prelude::*;

/// `sdk-basic` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    pub name: String,
    pub n: u32,
}

/// `sdk-basic` output.
#[derive(Debug, Serialize)]
pub struct Output {
    pub greeting: String,
    pub sum: u64,
    pub steps: u32,
}

#[step]
fn greet(name: &str) -> Result<String, StepError> {
    Ok(format!("hello, {name}"))
}

#[step(retry(max_attempts = 3, initial = "100ms", max = "1s"), timeout = "5s")]
fn add(n: u32) -> Result<u64, StepError> {
    Ok((1..=u64::from(n)).sum())
}

#[workflow(name = "sdk-basic", version = 1)]
pub fn sdk_basic(ctx: &Ctx, input: Input) -> Result<Output, WorkflowError> {
    let greeting = greet(&input.name)?;
    let sum = add(input.n)?;

    let (a, b) = ostro_sdk::join!(
        ctx.sleep(Duration::from_millis(10)),
        ctx.sleep(Duration::from_millis(20))
    );
    a?;
    b?;

    let short = ctx.sleep(Duration::from_millis(5));
    let long = ctx.sleep(Duration::from_secs(3600));
    let (winner, completion) = ostro_sdk::select!(short, long);
    if winner != 0 {
        return Err(WorkflowError::non_retryable(
            "select_order",
            format!("expected the 5 ms sleep to win, got index {winner}"),
        ));
    }
    short.decode(completion)?;
    ctx.cancel(&long);

    ctx.set_status(&json!({ "stage": "done" }))?;
    Ok(Output {
        greeting,
        sum,
        steps: 2,
    })
}

/// Returns its input unchanged (proves multi-workflow export and dispatch by name).
#[workflow(name = "sdk-echo", version = 1)]
pub fn sdk_echo(_ctx: &Ctx, input: serde_json::Value) -> Result<serde_json::Value, WorkflowError> {
    Ok(input)
}

ostro_sdk::export!(SdkBasicWorkflow, SdkEchoWorkflow);
