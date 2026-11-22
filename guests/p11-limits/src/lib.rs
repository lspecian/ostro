//! Run-limit guest (plan 11-06, OPS-07 / D-P11-13), exercised by
//! `crates/ostro-engine/tests/run_limits.rs` and `crates/ostro-server/tests/limits_run.rs`.
//!
//! Workflows (all take JSON input and run in one activation):
//!
//! - `limits-history` `{"n": u32, "end"?: "ok" | "fail" | "flaky"}`: runs `n` trivial steps
//!   `s<i>`, so the journal is `1 + n` events before the terminal. `end` (default `ok`)
//!   chooses the close: `ok` returns `n` (`RunCompleted`), `fail` returns a non-retryable
//!   workflow error (`RunFailed`), `flaky` first runs step `flaky`, which always fails
//!   retryably with two attempts (one `StepAttemptFailed`, then `StepCompleted(Err)`).
//! - `limits-children` `{"n": u32}`: starts `n` detached `limits-leaf` children without
//!   waiting (n `ChildStarted`), returns `n`.
//! - `limits-big-output` `{"bytes": u32}`: one step whose JSON output (a string) is exactly
//!   `bytes` bytes long; returns `bytes`.
//! - `limits-big-result` `{"bytes": u32}`: returns a JSON string of exactly `bytes` bytes as
//!   the run result.
//! - `limits-leaf`: returns `"leaf"`.

use ostro_sdk::prelude::*;

/// `limits-history` input.
#[derive(Debug, Deserialize)]
pub struct HistoryInput {
    pub n: u32,
    #[serde(default)]
    pub end: Option<String>,
}

/// `limits-children` input.
#[derive(Debug, Deserialize)]
pub struct CountInput {
    pub n: u32,
}

/// `limits-big-output` / `limits-big-result` input.
#[derive(Debug, Deserialize)]
pub struct BytesInput {
    pub bytes: u32,
}

/// A JSON string value whose encoding (with the two quotes) is `bytes` long.
fn blob(bytes: u32) -> String {
    "x".repeat((bytes as usize).saturating_sub(2))
}

#[step(retry(max_attempts = 2, initial = "1ms", max = "1ms"))]
fn flaky() -> Result<u32, StepError> {
    Err(StepError::retryable("transient", "always fails"))
}

#[workflow(name = "limits-history", version = 1)]
pub fn limits_history(_ctx: &Ctx, input: HistoryInput) -> Result<u32, WorkflowError> {
    let opts = StepOptions::default();
    for i in 0..input.n {
        let _: u32 = run_step(&format!("s{i}"), &opts, || Ok(i))?;
    }
    match input.end.as_deref().unwrap_or("ok") {
        "ok" => Ok(input.n),
        "fail" => Err(WorkflowError::non_retryable("asked", "end = fail")),
        "flaky" => Ok(flaky()?),
        other => Err(WorkflowError::non_retryable(
            "bad_end",
            format!("unknown end {other:?}"),
        )),
    }
}

#[workflow(name = "limits-children", version = 1)]
pub fn limits_children(ctx: &Ctx, input: CountInput) -> Result<u32, WorkflowError> {
    for i in 0..input.n {
        let _child = ctx.start_child::<ChildRun, _>(
            "limits-leaf",
            &json!({ "i": i }),
            ChildOptions::new().detached(),
        );
    }
    Ok(input.n)
}

#[workflow(name = "limits-big-output", version = 1)]
pub fn limits_big_output(_ctx: &Ctx, input: BytesInput) -> Result<u32, WorkflowError> {
    let bytes = input.bytes;
    let s: String = run_step("big", &StepOptions::default(), || Ok(blob(bytes)))?;
    Ok(u32::try_from(s.len() + 2).unwrap_or(u32::MAX))
}

#[workflow(name = "limits-big-result", version = 1)]
pub fn limits_big_result(_ctx: &Ctx, input: BytesInput) -> Result<String, WorkflowError> {
    Ok(blob(input.bytes))
}

#[workflow(name = "limits-leaf", version = 1)]
pub fn limits_leaf(_ctx: &Ctx, _input: serde_json::Value) -> Result<String, WorkflowError> {
    Ok("leaf".to_string())
}

ostro_sdk::export!(
    LimitsHistoryWorkflow,
    LimitsChildrenWorkflow,
    LimitsBigOutputWorkflow,
    LimitsBigResultWorkflow,
    LimitsLeafWorkflow,
);
