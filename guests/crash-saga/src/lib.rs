//! Crash-campaign workload (plan 05-10; driven by the 05-19 crash campaign and its
//! checker): workflow `crash-saga` v1.
//!
//! # Input
//!
//! `{"id": String, "steps": 3..=8, "fail_pattern": String, "sleep_ms": u64,
//!   "signal_timeout_ms": u64}` (`fail_pattern` defaults to `""`). Character `i` of
//! `fail_pattern` (missing = `'0'`) configures step `i`:
//! - `'0'..='3'`: the step fails retryably that many times, then succeeds. Retries use
//!   `max_attempts = 10` with a 1..10 ms in-activation backoff. The failure counter lives
//!   in guest memory, so a crash in the middle of a step's retries restarts the count;
//!   the step still succeeds unless the process dies mid-step often enough to exhaust
//!   10 attempts (the checker treats that as the documented `failed` outcome below).
//! - `'x'`: the step fails non-retryably, which triggers compensation.
//!
//! Any other character, or `steps` outside `3..=8`, fails the run with the
//! non-retryable error kind `bad_input` before any step runs.
//!
//! # Flow
//!
//! 1. Steps `step0` .. `step{steps-1}`, each returning `"<id>-<i>"`.
//! 2. `sleep(sleep_ms)` (beyond the hot-wait threshold for some runs).
//! 3. `wait_signal("go", signal_timeout_ms)`.
//! 4. Gather: `join!(sleep(sleep_ms / 2), sleep(sleep_ms), wait_signal("extra",
//!    signal_timeout_ms))`.
//!
//! Cleanup always runs inside `ctx.shielded`, so it completes after a cancel.
//!
//! # Output: a pure function of the input and the accepted signals
//!
//! Let `go` be the payload of the first `"go"` signal matched to the step-3 wait, or
//! `null` if that wait timed out; `extra` likewise for the step-4 wait.
//!
//! - All steps succeed, no cancel: Completed with
//!   `{"id": id, "status": "completed", "results": ["<id>-0", .., "<id>-<steps-1>"],
//!   "go": go, "extra": extra}`.
//! - Step `k` is `'x'`: Completed with
//!   `{"id": id, "status": "compensated", "failed_at": k, "undone": [k-1, .., 0]}`, after
//!   running the shielded compensation steps `undo{k-1}` .. `undo0` (each returns
//!   `"undo-<id>-<j>"`).
//! - Graceful cancel observed at a wait in phase 2..4 (all steps done): the shielded
//!   compensation steps `undo{steps-1}` .. `undo0` run, then the run ends **Cancelled**.
//! - A step exhausting its retries (see above): the run **Failed** with that step's
//!   `transient` error (no compensation).
//!
//! Retry failures and crashes do not change the output: every recorded outcome is
//! replayed from the journal.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use ostro_sdk::RetryPolicy;
use ostro_sdk::prelude::*;

type Value = serde_json::Value;

/// Per-step attempt counters for the current guest instance (reset on every activation).
static ATTEMPTS: [AtomicU32; 8] = [const { AtomicU32::new(0) }; 8];

/// `crash-saga` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    pub id: String,
    pub steps: u32,
    #[serde(default)]
    pub fail_pattern: String,
    pub sleep_ms: u64,
    pub signal_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy)]
enum Fail {
    Times(u32),
    Fatal,
}

fn bad_input(msg: String) -> WorkflowError {
    WorkflowError::non_retryable("bad_input", msg)
}

fn plan(input: &Input) -> Result<Vec<Fail>, WorkflowError> {
    if !(3..=8).contains(&input.steps) {
        return Err(bad_input(format!("steps {} not in 3..=8", input.steps)));
    }
    let chars: Vec<char> = input.fail_pattern.chars().collect();
    (0..input.steps as usize)
        .map(|i| match chars.get(i).copied().unwrap_or('0') {
            c @ '0'..='3' => Ok(Fail::Times(c as u32 - '0' as u32)),
            'x' => Ok(Fail::Fatal),
            c => Err(bad_input(format!("fail_pattern[{i}] = {c:?}"))),
        })
        .collect()
}

fn step_opts() -> StepOptions {
    StepOptions {
        retry: Some(RetryPolicy {
            max_attempts: 10,
            initial_interval: Duration::from_millis(1),
            max_interval: Duration::from_millis(10),
            backoff_coefficient: 2.0,
            non_retryable_kinds: Vec::new(),
        }),
        ..StepOptions::default()
    }
}

fn forward(id: &str, i: usize, fail: Fail) -> Result<String, StepError> {
    run_step(&format!("step{i}"), &step_opts(), || match fail {
        Fail::Fatal => Err(StepError::non_retryable(
            "step_failed",
            format!("step {i} configured to fail"),
        )),
        Fail::Times(n) => {
            if ATTEMPTS[i].fetch_add(1, Ordering::SeqCst) < n {
                Err(StepError::retryable(
                    "transient",
                    format!("step {i} simulated failure"),
                ))
            } else {
                Ok(format!("{id}-{i}"))
            }
        }
    })
}

/// Shielded reverse compensation of steps `0..done`; returns the undone indices.
fn compensate(ctx: &Ctx, id: &str, done: usize) -> Result<Vec<usize>, WorkflowError> {
    ctx.shielded(|| {
        let mut undone = Vec::with_capacity(done);
        for j in (0..done).rev() {
            let _: String = run_step(&format!("undo{j}"), &step_opts(), || {
                Ok(format!("undo-{id}-{j}"))
            })?;
            undone.push(j);
        }
        Ok(undone)
    })
}

/// The signal payload, `None` on timeout; a cancel propagates.
fn accepted(r: Result<Value, StepError>) -> Result<Value, StepError> {
    match r {
        Ok(v) => Ok(v),
        Err(e) if e.is_timeout() => Ok(Value::Null),
        Err(e) => Err(e),
    }
}

/// Phases 2..4; `Err` is a cancel (or an unexpected failure).
fn waits(ctx: &Ctx, input: &Input) -> Result<(Value, Value), StepError> {
    let timeout = Some(Duration::from_millis(input.signal_timeout_ms));
    ctx.sleep(Duration::from_millis(input.sleep_ms)).wait()?;
    let go = accepted(ctx.wait_signal::<Value>("go", timeout).wait())?;
    let (a, b, extra) = ostro_sdk::join!(
        ctx.sleep(Duration::from_millis(input.sleep_ms / 2)),
        ctx.sleep(Duration::from_millis(input.sleep_ms)),
        ctx.wait_signal::<Value>("extra", timeout)
    );
    a?;
    b?;
    Ok((go, accepted(extra)?))
}

#[workflow(name = "crash-saga", version = 1)]
pub fn crash_saga(ctx: &Ctx, input: Input) -> Result<Value, WorkflowError> {
    let fails = plan(&input)?;
    let mut results = Vec::with_capacity(fails.len());
    for (i, fail) in fails.iter().enumerate() {
        match forward(&input.id, i, *fail) {
            Ok(r) => results.push(r),
            Err(e) if !e.retryable && e.kind == "step_failed" => {
                let undone = compensate(ctx, &input.id, i)?;
                return Ok(json!({
                    "id": input.id,
                    "status": "compensated",
                    "failed_at": i,
                    "undone": undone,
                }));
            }
            Err(e) => return Err(e.into()),
        }
    }
    match waits(ctx, &input) {
        Ok((go, extra)) => Ok(json!({
            "id": input.id,
            "status": "completed",
            "results": results,
            "go": go,
            "extra": extra,
        })),
        Err(e) if e.is_cancelled() => {
            compensate(ctx, &input.id, results.len())?;
            Err(e.into())
        }
        Err(e) => Err(e.into()),
    }
}

ostro_sdk::export!(CrashSagaWorkflow);
