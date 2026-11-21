//! Saga guest (plan 09-06, WF-10) for the Rust SDK's [`Saga`] helper. Every booking and
//! compensation is a `#[step]`, so the journal shows the order in which they ran. Step
//! failures are non-retryable, so each step runs once.
//!
//! All workflows take any input (ignored).
//! - `saga_three`: `book_a` and `book_b` succeed and register `cancel_a` / `cancel_b`;
//!   `book_c` fails (`book_c_failed`), so the saga runs `cancel_b` then `cancel_a`. The run
//!   fails with `book_c_failed`, and its details hold
//!   `{"compensated": ["cancel_b", "cancel_a"], "compensation_failures": []}`.
//! - `saga_comp_fails`: like `saga_three`, but `cancel_b` fails (`cancel_b_failed`).
//!   `cancel_a` still runs, and the details list the `cancel_b` failure.
//! - `saga_cancelled`: `book_a`, `book_b`, then a 1 h sleep. A cancel during the sleep
//!   makes the saga compensate (shielded: `cancel_b`, `cancel_a`), and the run ends
//!   cancelled.
//! - `saga_ok`: `book_a`, `book_b` and `book_c` all succeed, so no compensation runs.
//!   Returns `{"booked": ["a", "b", "c"], "pending_compensations": 2}` (the registered
//!   compensations are simply dropped).

use std::time::Duration;

use ostro_sdk::prelude::*;
use serde_json::Value;

#[step]
fn book_a() -> Result<String, StepError> {
    Ok("a".to_string())
}

#[step]
fn book_b() -> Result<String, StepError> {
    Ok("b".to_string())
}

#[step]
fn book_c(fail: bool) -> Result<String, StepError> {
    if fail {
        Err(StepError::non_retryable("book_c_failed", "no rooms left"))
    } else {
        Ok("c".to_string())
    }
}

#[step]
fn cancel_a() -> Result<(), StepError> {
    Ok(())
}

#[step]
fn cancel_b(fail: bool) -> Result<(), StepError> {
    if fail {
        Err(StepError::non_retryable(
            "cancel_b_failed",
            "refund rejected",
        ))
    } else {
        Ok(())
    }
}

/// Book a and b, registering their compensations.
fn book_two<'a>(ctx: &'a Ctx, cancel_b_fails: bool) -> Result<Saga<'a>, WorkflowError> {
    let mut saga = Saga::new(ctx);
    book_a()?;
    saga.add("cancel_a", |_| Ok(cancel_a()?));
    book_b()?;
    saga.add("cancel_b", move |_| Ok(cancel_b(cancel_b_fails)?));
    Ok(saga)
}

fn three(ctx: &Ctx, cancel_b_fails: bool) -> Result<Value, WorkflowError> {
    let saga = book_two(ctx, cancel_b_fails)?;
    match book_c(true) {
        Ok(_) => Err(WorkflowError::non_retryable(
            "unexpected",
            "book_c succeeded",
        )),
        Err(e) => Err(saga.fail_with(e)),
    }
}

#[workflow(name = "saga_three", version = 1)]
pub fn saga_three(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    three(ctx, false)
}

#[workflow(name = "saga_comp_fails", version = 1)]
pub fn saga_comp_fails(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    three(ctx, true)
}

#[workflow(name = "saga_cancelled", version = 1)]
pub fn saga_cancelled(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let saga = book_two(ctx, false)?;
    match ctx.sleep(Duration::from_secs(3600)).wait() {
        Ok(()) => Ok(json!({ "slept": true })),
        Err(e) => Err(saga.fail_with(e)),
    }
}

#[workflow(name = "saga_ok", version = 1)]
pub fn saga_ok(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let mut booked = Vec::new();
    let saga = book_two(ctx, false)?;
    booked.push("a");
    booked.push("b");
    match book_c(false) {
        Ok(_) => booked.push("c"),
        Err(e) => return Err(saga.fail_with(e)),
    }
    Ok(json!({ "booked": booked, "pending_compensations": saga.len() }))
}

ostro_sdk::export!(
    SagaThreeWorkflow,
    SagaCompFailsWorkflow,
    SagaCancelledWorkflow,
    SagaOkWorkflow
);
