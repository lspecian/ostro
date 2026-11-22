//! `hello-offline`: the 10-12 offline build fixture, the `ostro init --lang rust`
//! template's `hello` workflow (step `greet`, a durable 3-day sleep, the `approve` signal).

use std::time::Duration;

use ostro_sdk::prelude::*;

/// The input of `hello`.
#[derive(Debug, Deserialize)]
pub struct HelloInput {
    pub name: String,
}

/// The payload of the `approve` signal.
#[derive(Debug, Deserialize)]
pub struct Approval {
    #[serde(default)]
    pub ok: bool,
}

/// The output of `hello`.
#[derive(Debug, Serialize)]
pub struct HelloOutput {
    pub greeting: String,
    pub approved: bool,
}

#[step]
fn greet(name: &str) -> Result<String, StepError> {
    Ok(format!("hello {name}"))
}

#[workflow(name = "hello", version = 1)]
pub fn hello(ctx: &Ctx, input: HelloInput) -> Result<HelloOutput, WorkflowError> {
    let greeting = greet(&input.name)?;
    ctx.sleep(Duration::from_secs(3 * 24 * 60 * 60)).wait()?;
    let approval: Approval = ctx.wait_signal("approve", None).wait()?;
    Ok(HelloOutput {
        greeting,
        approved: approval.ok,
    })
}

ostro_sdk::export!(HelloWorkflow);
