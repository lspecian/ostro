//! Child-workflow parent guest (plan 09-06, WF-06). Every workflow names its child in the
//! input, so the same parents drive Rust, Python and TS children in later cross-language
//! chains.
//!
//! Input (all workflows): `{"workflow": "<child>", "input": <any>, "queue"?: str,
//! "version"?: u32, "run_timeout_ms"?: u64}`; the optional fields become [`ChildOptions`].
//!
//! - `parent_attached`: start the child attached and wait; returns `{"child": <output>}`.
//! - `parent_fail_child`: start and wait; returns `{"caught": "<error kind>"}` on a child
//!   error (`boom`, `child_terminated`, `timeout`, `cancelled`, ...) or `{"ok": <output>}`.
//! - `parent_detached`: start detached; returns `{"detached": "<child run id>"}` from the
//!   op's [`ChildRun`].
//! - `parent_cancel_child`: start, `ctx.cancel(&op)`, then wait: returns
//!   `{"caught": "cancelled"}` (D5), or `{"ok": <output>}` if the child finished first.
//! - `parent_close`: start the child and return `{"started": true}` without waiting (the
//!   attached child is cancelled when the parent closes).

use std::time::Duration;

use ostro_sdk::prelude::*;
use serde_json::Value;

/// Input shared by every parent workflow.
#[derive(Debug, Deserialize)]
pub struct ParentInput {
    /// Child workflow name (unqualified).
    pub workflow: String,
    /// Child input.
    #[serde(default)]
    pub input: Value,
    #[serde(default)]
    pub queue: Option<String>,
    #[serde(default)]
    pub version: Option<u32>,
    #[serde(default)]
    pub run_timeout_ms: Option<u64>,
}

impl ParentInput {
    fn options(&self) -> ChildOptions {
        let mut o = ChildOptions::new();
        if let Some(q) = &self.queue {
            o = o.queue(q.clone());
        }
        if let Some(v) = self.version {
            o = o.version(v);
        }
        if let Some(ms) = self.run_timeout_ms {
            o = o.run_timeout(Duration::from_millis(ms));
        }
        o
    }
}

fn caught(r: Result<Value, StepError>) -> Value {
    match r {
        Ok(v) => json!({ "ok": v }),
        Err(e) => json!({ "caught": e.kind }),
    }
}

#[workflow(name = "parent_attached", version = 1)]
pub fn parent_attached(ctx: &Ctx, input: ParentInput) -> Result<Value, WorkflowError> {
    let out: Value = ctx
        .start_child(&input.workflow, &input.input, input.options())
        .wait()?;
    Ok(json!({ "child": out }))
}

#[workflow(name = "parent_fail_child", version = 1)]
pub fn parent_fail_child(ctx: &Ctx, input: ParentInput) -> Result<Value, WorkflowError> {
    let op = ctx.start_child::<Value, _>(&input.workflow, &input.input, input.options());
    Ok(caught(op.wait()))
}

#[workflow(name = "parent_detached", version = 1)]
pub fn parent_detached(ctx: &Ctx, input: ParentInput) -> Result<Value, WorkflowError> {
    let run: ChildRun = ctx
        .start_child(&input.workflow, &input.input, input.options().detached())
        .wait()?;
    Ok(json!({ "detached": run.run_id }))
}

#[workflow(name = "parent_cancel_child", version = 1)]
pub fn parent_cancel_child(ctx: &Ctx, input: ParentInput) -> Result<Value, WorkflowError> {
    let op = ctx.start_child::<Value, _>(&input.workflow, &input.input, input.options());
    ctx.cancel(&op);
    Ok(caught(op.wait()))
}

#[workflow(name = "parent_close", version = 1)]
pub fn parent_close(ctx: &Ctx, input: ParentInput) -> Result<Value, WorkflowError> {
    let _child = ctx.start_child::<Value, _>(&input.workflow, &input.input, input.options());
    Ok(json!({ "started": true }))
}

ostro_sdk::export!(
    ParentAttachedWorkflow,
    ParentFailChildWorkflow,
    ParentDetachedWorkflow,
    ParentCancelChildWorkflow,
    ParentCloseWorkflow,
);
