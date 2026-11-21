//! Phase 7 test guest (plan 07-07, D-W15e): long journals and large payloads for worker
//! journal paging and append framing (WORK-02).
//!
//! Workflow `long-journal` v1. Input
//! `{"steps": u32, "result_bytes": u64, "wait_input": bool?, "sleep_ms": u64?}`:
//! 1. `steps` small steps (`tick`, each returning its index),
//! 2. one step `big` returning a string of `result_bytes` bytes (skipped when 0),
//! 3. with `sleep_ms`, one durable sleep of that length (a timer the test fires through the
//!    journal; with a long hot wait the activation stays parked meanwhile),
//! 4. with `wait_input = true`, one wait for signal `"input"` whose payload is any JSON
//!    value (up to 3 MiB in the paging tests); its length in bytes is reported.
//!
//! Output: `{"steps": n, "sum": Σ tick results, "big_len": len, "input_len": len | null}`.

use std::time::Duration;

use ostro_sdk::prelude::*;

/// `long-journal` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    pub steps: u32,
    #[serde(default)]
    pub result_bytes: u64,
    #[serde(default)]
    pub wait_input: bool,
    #[serde(default)]
    pub sleep_ms: Option<u64>,
}

/// `long-journal` output.
#[derive(Debug, Serialize)]
pub struct Output {
    pub steps: u32,
    pub sum: u64,
    pub big_len: u64,
    pub input_len: Option<u64>,
}

#[step]
fn tick(i: u32) -> Result<u64, StepError> {
    Ok(u64::from(i))
}

#[step]
fn big(n: u64) -> Result<String, StepError> {
    Ok("x".repeat(usize::try_from(n).unwrap_or(0)))
}

#[workflow(name = "long-journal", version = 1)]
pub fn long_journal(ctx: &Ctx, input: Input) -> Result<Output, WorkflowError> {
    let mut sum = 0u64;
    for i in 0..input.steps {
        sum += tick(i)?;
    }
    let big_len = if input.result_bytes > 0 {
        big(input.result_bytes)?.len() as u64
    } else {
        0
    };
    if let Some(ms) = input.sleep_ms {
        ctx.sleep(Duration::from_millis(ms)).wait()?;
    }
    let input_len = if input.wait_input {
        let v: serde_json::Value = ctx.wait_signal("input", None).wait()?;
        Some(serde_json::to_vec(&v)?.len() as u64)
    } else {
        None
    };
    Ok(Output {
        steps: input.steps,
        sum,
        big_len,
        input_len,
    })
}

ostro_sdk::export!(LongJournalWorkflow);
