//! Run-timeout guest (plan 05-10, WF-09; used by the 05-15 run-timeout tests): workflow
//! `timeouts` v1.
//!
//! Input `{"mode": "sleep_forever" | "sleep_ms" | "busy_steps", "n": u64}` (`n` defaults
//! to 0):
//! - `sleep_forever`: sleeps 365 days at a time, forever. Only a run timeout, terminate or
//!   cancel ends it (a cancel surfaces as `Err(cancelled)` and the run ends Cancelled).
//! - `sleep_ms`: one durable sleep of `n` ms, then completes with `"slept"`.
//! - `busy_steps`: `n` trivial durable steps (`b<i>` returning `i`), then completes with
//!   `"busy:<n>"`. With a large `n` the run spends its time in one long activation.

use std::time::Duration;

use ostro_sdk::prelude::*;

/// `timeouts` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    pub mode: String,
    #[serde(default)]
    pub n: u64,
}

#[workflow(name = "timeouts", version = 1)]
pub fn timeouts(ctx: &Ctx, input: Input) -> Result<String, WorkflowError> {
    match input.mode.as_str() {
        "sleep_forever" => loop {
            ctx.sleep(Duration::from_secs(365 * 24 * 3600)).wait()?;
        },
        "sleep_ms" => {
            ctx.sleep(Duration::from_millis(input.n)).wait()?;
            Ok("slept".into())
        }
        "busy_steps" => {
            let opts = StepOptions::default();
            for i in 0..input.n {
                let _: u64 = run_step(&format!("b{i}"), &opts, || Ok(i))?;
            }
            Ok(format!("busy:{}", input.n))
        }
        other => Err(WorkflowError::non_retryable(
            "bad_mode",
            format!("unknown mode {other:?}"),
        )),
    }
}

ostro_sdk::export!(TimeoutsWorkflow);
