//! Rust conformance reference workflows, v1 (plan 09-11, SDK-05).
//!
//! Every workflow is exported as `rs_<base>` (D-P9-23); the Python (`py_<base>`, 09-14)
//! and TypeScript (`ts_<base>`, 09-15) modules mirror the base names and behaviour
//! exactly. Specs under `conformance/spec/engine/` name the base; the harness maps it with
//! `ostro_conformance::langs::workflow_name`. `conf-rust-v2` exports the same set with v2
//! bodies for `conf_patch` and `conf_ndet` only.
//!
//! Step names are explicit (`run_step("a", ..)`) so they are language-neutral. "1 h" is
//! 3600 s of workflow time; the engine-leg harness fires timers instead of waiting.
//!
//! | Base name | Input | Behaviour | Output |
//! |-----------|-------|-----------|--------|
//! | `conf_steps` | any | steps `a`, `b`, `c` (each returns its own name) | `{"a":"a","b":"b","c":"c"}` |
//! | `conf_step_fail` | any | step `boom` fails non-retryable `boom`; the workflow catches it | `{"caught":"boom"}` |
//! | `conf_step_rng` | any | step `draw` returns `random_u64()` drawn inside the body (step-local stream, D7) as a decimal string | `{"value":"<u64>"}` |
//! | `conf_step_sibling` | any | step `s` (returns `"s"`); then the sibling work: `random_u64()` from the ctx stream and a 1 s sleep (Python/TS: a sibling task running while the async step body yields) | `{"step":"s","sibling_rand":"<u64>"}` |
//! | `conf_native` | any | native `SystemTime::now()`, std `HashMap` iteration (16 keys) and `RandomState` hash of `"x"`, in the body and inside step `observe` | `{"outside":{..},"inside":{..}}` (per-language values) |
//! | `conf_retry` | `{"succeed_after_ms":N}` | step `flaky`, retry 3 / 1 s / 60 s / 2.0; fails `retryable("transient")` while `ctx.now()` inside the body is less than N ms after the run start (`ctx.now()` before the step). Attempt k observes `max(last ts, retry_at)` (D22): 0 ms, 1000 ms, 3000 ms | `{"elapsed_ms":3000}` for N = 2500 |
//! | `conf_retry_exhaust` | any | step `always`, retry 2 / 1 s; always `retryable("transient")` | run fails `transient` |
//! | `conf_timer` | any | sleep 1 h | `{"slept":true}` |
//! | `conf_timer_vs_signal` | any | select(sleep 1 h, signal `go`); no loser cancel | `{"winner":"signal","payload":<p>}` |
//! | `conf_signals` | any | wait signal `go` twice | `{"got":[<p1>,<p2>]}` |
//! | `conf_signal_timeout` | any | wait signal `never` with a 10 s timeout | `{"timeout":true}` |
//! | `conf_gather` | any | join(sleep 1 s, sleep 2 s, signal `go`) | `{"signal":<p>}` |
//! | `conf_race_cancel` | any | select(sleep 1 h, signal `go`); the loser is cancelled with `cancel` and its wait gives `Err(cancelled)` | `{"winner":"signal","loser_cancelled":true}` |
//! | `conf_cancel_wait` | any | sleep 1 h; a run cancel ends the run cancelled (D11) | cancelled (`{"slept":true}` if the timer fires) |
//! | `conf_cancel_shielded` | any | sleep 1 h; on cancel the shielded step `cleanup` runs, then the run ends cancelled | cancelled |
//! | `conf_cancel_timer` | any | start a 1 h timer, `cancel` it, wait it: `Err(cancelled)` | `{"cancelled_op":true}` |
//! | `conf_patch` | any | v1: step `a`, sleep 1 s, step `b`. v2: `if patched("x") { step a2 } else { step a }`, sleep 1 s, step `b` | `["a","b"]` (v1 / v1 journal on v2), `["a2","b"]` (fresh v2) |
//! | `conf_ndet` | any | v1: step `a`, sleep 1 s, step `b`. v2: step `b`, sleep 1 s, step `a` | `["a","b"]` / `["b","a"]`; a v1 journal on v2 is non-determinism |
//! | `conf_rand` | any | three `random_u64()` (decimal strings), then `uuid_v7()` | `{"u64":[..3],"uuid":".."}` |
//! | `conf_rand_native` | any | `random_bytes(8)` (hex), then `random_f64()` | `{"bytes":"<16 hex>","f64":<f>}` |
//! | `conf_values` | any | in this order: `random_u64()` (decimal), `random_f64()`, `uuid_v7()`, `idempotency_key("x")` | `{"random_u64","random_f64","uuid_v7","idempotency_key"}` (equal across SDKs, C-VAL-01) |
//! | `conf_can` | `{"n":N,"generation"?:k}` | continue-as-new with `n-1`, `generation+1` until `n == 0` | `{"generation":k}` |
//! | `conf_token` | any | create an await token (no timeout) and wait it | `{"token_payload":<p>}` |
//! | `conf_status` | any | `set_status({"stage":"one"})`, `set_status({"stage":"two","n":2})` | `{"done":true}` |
//! | `conf_saga_three` | any | steps `book_a`, `book_b` (registering `cancel_a`, `cancel_b`), step `book_c` fails `book_c_failed`; the saga runs `cancel_b`, `cancel_a` (LIFO, shielded) | run fails `book_c_failed` |
//! | `conf_saga_cancel` | any | steps `book_a`, `book_b`, sleep 1 h; on cancel the saga runs `cancel_b`, `cancel_a` | cancelled |
//!
//! **Server-leg workflows (plan 09-13).** Child workflow names arrive in the input as *full
//! prefixed* names (`rs_conf_leaf_echo`, `py_conf_parent_chain`, ...) so a parent in one
//! language can start a child in another (09-16, 09-18); the server-leg harness builds them
//! with `langs::workflow_name`. Parent input: `{"child": "<name>", "input"?: any,
//! "run_timeout_ms"?: N, "delay_ms"?: N}`; `run_timeout_ms` becomes `ChildOptions::run_timeout`.
//! Child failures are caught as `{"caught": "<kind>"}` (`boom`, `cancelled`,
//! `child_terminated`, `timeout`, ...).
//!
//! | Base name | Input | Behaviour | Output |
//! |-----------|-------|-----------|--------|
//! | `conf_parent_attached` | parent input | start the child attached and wait | `{"child": <output>}` or `{"caught": "<kind>"}` |
//! | `conf_parent_fail_child` | parent input | same as `conf_parent_attached` (kept separate so specs name the intent) | `{"child": <output>}` or `{"caught": "<kind>"}` |
//! | `conf_parent_detached` | parent input | start the child detached and wait the op | `{"run_id": "<child run id>"}` |
//! | `conf_parent_cancel_child` | parent input (`delay_ms` default 1000) | start the child, sleep `delay_ms`, `cancel` the child op, wait it | `{"caught": "cancelled"}` (or `{"child": <output>}` if it finished first) |
//! | `conf_parent_close` | parent input (`delay_ms` default 1000) | start the child, sleep `delay_ms`, return without waiting (the attached child is cancelled with reason `parent_closed`) | `{"started": true}` |
//! | `conf_parent_chain` | `{"next": [<name>, ...]}` | empty `next`: leaf; else start `next[0]` attached with `{"next": next[1..]}` and wait (errors propagate) | `{"leaf": true}` / `{"child": <output>}` |
//! | `conf_parent_shielded_child` | parent input | start the child; wait it inside `shielded`; on `Err(cancelled)` run a shielded cleanup that starts a second child (same name, input `{}`) and sleeps 1 s, then return | `{"first": "cancelled", "cleanup_child": true}` (or `{"first": <output>}`) |
//! | `conf_leaf_echo` | any; `{"delay_ms": N, ..}` first sleeps N ms inside `shielded` | echo the input | `{"echo": <input>}` |
//! | `conf_leaf_fail` | any | fail non-retryable `boom` | fails `boom` |
//! | `conf_leaf_sleep` | `{"ms"?: N}` (default 1 h) | sleep; on cancel the shielded step `cleanup` runs and the run ends cancelled | `{"slept": true}` / cancelled |
//! | `conf_leaf_can` | `{"n", "generation"?}` | continue-as-new `n-1`, `generation+1` until 0 | `{"done": true, "generation": k}` |
//! | `conf_sig_api` | any | wait signal `approve` | `{"approved": <payload>}` |
//! | `conf_token_api` | any | create an await token, `set_status({"token": <token>})`, wait it | `{"token_payload": <payload>}` |
//! | `conf_fx_http` | `{"url": "<mock>/pay"}` | `POST url` (JSON body `{}`) with idempotency key `ctx.idempotency_key("pay")` (sent as the `Idempotency-Key` header) | `{"status": <u16>, "body": <response JSON>}` |
//! | `conf_saga_http` | `{"url": "<mock base>"}` | `Saga`: POST `/pay` with keys `idempotency_key("a")`, `("b")` (registering `refund_a`, `refund_b`), step `book_c` fails `book_c_failed`; the compensations POST `/pay` with keys `("refund-b")`, then `("refund-a")` | fails `book_c_failed` (C-SAGA-03, 09-16) |

mod shared;

use std::time::Duration;

use ostro_sdk::prelude::*;
use serde_json::Value;
pub use shared::*;

#[workflow(name = "rs_conf_patch", version = 1)]
pub fn rs_conf_patch(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let a = named("a", "a")?;
    ctx.sleep(Duration::from_secs(1)).wait()?;
    let b = named("b", "b")?;
    Ok(json!([a, b]))
}

#[workflow(name = "rs_conf_ndet", version = 1)]
pub fn rs_conf_ndet(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let a = named("a", "a")?;
    ctx.sleep(Duration::from_secs(1)).wait()?;
    let b = named("b", "b")?;
    Ok(json!([a, b]))
}

// --- server leg (plan 09-13) --------------------------------------------------------

/// Input of the `conf_parent_*` workflows: the child's full (prefixed) workflow name.
#[derive(Debug, Deserialize)]
pub struct ParentInput {
    pub child: String,
    #[serde(default)]
    pub input: Value,
    #[serde(default)]
    pub run_timeout_ms: Option<u64>,
    #[serde(default)]
    pub delay_ms: Option<u64>,
}

impl ParentInput {
    fn options(&self) -> ChildOptions {
        let mut o = ChildOptions::new();
        if let Some(ms) = self.run_timeout_ms {
            o = o.run_timeout(Duration::from_millis(ms));
        }
        o
    }

    fn start(&self, ctx: &Ctx) -> Op<Value> {
        ctx.start_child(&self.child, &self.input, self.options())
    }

    fn delay(&self) -> Duration {
        Duration::from_millis(self.delay_ms.unwrap_or(1000))
    }
}

fn child_or_caught(r: Result<Value, StepError>) -> Value {
    match r {
        Ok(v) => json!({ "child": v }),
        Err(e) => json!({ "caught": e.kind }),
    }
}

#[workflow(name = "rs_conf_parent_attached", version = 1)]
pub fn rs_conf_parent_attached(ctx: &Ctx, input: ParentInput) -> Result<Value, WorkflowError> {
    Ok(child_or_caught(input.start(ctx).wait()))
}

#[workflow(name = "rs_conf_parent_fail_child", version = 1)]
pub fn rs_conf_parent_fail_child(ctx: &Ctx, input: ParentInput) -> Result<Value, WorkflowError> {
    Ok(child_or_caught(input.start(ctx).wait()))
}

#[workflow(name = "rs_conf_parent_detached", version = 1)]
pub fn rs_conf_parent_detached(ctx: &Ctx, input: ParentInput) -> Result<Value, WorkflowError> {
    let run: ChildRun = ctx
        .start_child(&input.child, &input.input, input.options().detached())
        .wait()?;
    Ok(json!({ "run_id": run.run_id }))
}

#[workflow(name = "rs_conf_parent_cancel_child", version = 1)]
pub fn rs_conf_parent_cancel_child(ctx: &Ctx, input: ParentInput) -> Result<Value, WorkflowError> {
    let op = input.start(ctx);
    ctx.sleep(input.delay()).wait()?;
    ctx.cancel(&op);
    Ok(child_or_caught(op.wait()))
}

#[workflow(name = "rs_conf_parent_close", version = 1)]
pub fn rs_conf_parent_close(ctx: &Ctx, input: ParentInput) -> Result<Value, WorkflowError> {
    let _child = input.start(ctx);
    ctx.sleep(input.delay()).wait()?;
    Ok(json!({ "started": true }))
}

/// `conf_parent_chain` input.
#[derive(Debug, Deserialize)]
pub struct ChainInput {
    #[serde(default)]
    pub next: Vec<String>,
}

#[workflow(name = "rs_conf_parent_chain", version = 1)]
pub fn rs_conf_parent_chain(ctx: &Ctx, input: ChainInput) -> Result<Value, WorkflowError> {
    let Some((first, rest)) = input.next.split_first() else {
        return Ok(json!({ "leaf": true }));
    };
    let out: Value = ctx
        .start_child(first, &json!({ "next": rest }), ChildOptions::new())
        .wait()?;
    Ok(json!({ "child": out }))
}

#[workflow(name = "rs_conf_parent_shielded_child", version = 1)]
pub fn rs_conf_parent_shielded_child(
    ctx: &Ctx,
    input: ParentInput,
) -> Result<Value, WorkflowError> {
    let op = input.start(ctx);
    match ctx.shielded(|| op.wait()) {
        Ok(v) => Ok(json!({ "first": v })),
        Err(e) if e.is_cancelled() => {
            ctx.shielded(|| -> Result<(), WorkflowError> {
                let _second: Op<Value> = ctx.start_child(&input.child, &json!({}), input.options());
                ctx.sleep(Duration::from_secs(1)).wait()?;
                Ok(())
            })?;
            Ok(json!({ "first": "cancelled", "cleanup_child": true }))
        }
        Err(e) => Err(e.into()),
    }
}

#[workflow(name = "rs_conf_leaf_echo", version = 1)]
pub fn rs_conf_leaf_echo(ctx: &Ctx, input: Value) -> Result<Value, WorkflowError> {
    if let Some(ms) = input.get("delay_ms").and_then(Value::as_u64) {
        ctx.shielded(|| ctx.sleep(Duration::from_millis(ms)).wait())?;
    }
    Ok(json!({ "echo": input }))
}

#[workflow(name = "rs_conf_leaf_fail", version = 1)]
pub fn rs_conf_leaf_fail(_ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    Err(WorkflowError::non_retryable(
        "boom",
        "conf_leaf_fail always fails",
    ))
}

#[workflow(name = "rs_conf_leaf_sleep", version = 1)]
pub fn rs_conf_leaf_sleep(ctx: &Ctx, input: Value) -> Result<Value, WorkflowError> {
    let ms = input.get("ms").and_then(Value::as_u64).unwrap_or(3_600_000);
    match ctx.sleep(Duration::from_millis(ms)).wait() {
        Ok(()) => Ok(json!({ "slept": true })),
        Err(e) if e.is_cancelled() => {
            ctx.shielded(|| named("cleanup", "cleaned"))?;
            Err(e.into())
        }
        Err(e) => Err(e.into()),
    }
}

#[workflow(name = "rs_conf_leaf_can", version = 1)]
pub fn rs_conf_leaf_can(ctx: &Ctx, input: CanInput) -> Result<Value, WorkflowError> {
    if input.n == 0 {
        return Ok(json!({ "done": true, "generation": input.generation }));
    }
    Err(ctx.continue_as_new(&CanInput {
        n: input.n - 1,
        generation: input.generation + 1,
    }))
}

#[workflow(name = "rs_conf_sig_api", version = 1)]
pub fn rs_conf_sig_api(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let p: Value = ctx.wait_signal("approve", None).wait()?;
    Ok(json!({ "approved": p }))
}

#[workflow(name = "rs_conf_token_api", version = 1)]
pub fn rs_conf_token_api(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let t = ctx.create_await_token::<Value>(None);
    ctx.set_status(&json!({ "token": t.token() }))?;
    let payload = t.op().wait()?;
    Ok(json!({ "token_payload": payload }))
}

/// `conf_fx_http` / `conf_saga_http` input.
#[derive(Debug, Deserialize)]
pub struct UrlInput {
    pub url: String,
}

/// `POST url` with JSON body `{}` and idempotency key `ctx.idempotency_key(key_name)`.
fn pay(ctx: &Ctx, url: &str, key_name: &str) -> Result<Value, WorkflowError> {
    let resp = ctx
        .http()
        .post(url)
        .idempotency_key(ctx.idempotency_key(key_name))
        .json(&json!({}))?
        .send()
        .wait()?;
    let body: Value = serde_json::from_slice(&resp.body).unwrap_or(Value::Null);
    Ok(json!({ "status": resp.status, "body": body }))
}

#[workflow(name = "rs_conf_fx_http", version = 1)]
pub fn rs_conf_fx_http(ctx: &Ctx, input: UrlInput) -> Result<Value, WorkflowError> {
    pay(ctx, &input.url, "pay")
}

#[workflow(name = "rs_conf_saga_http", version = 1)]
pub fn rs_conf_saga_http(ctx: &Ctx, input: UrlInput) -> Result<Value, WorkflowError> {
    let url = format!("{}/pay", input.url);
    let mut saga = Saga::new(ctx);
    pay(ctx, &url, "a")?;
    let u = url.clone();
    saga.add("refund_a", move |c| pay(c, &u, "refund-a").map(drop));
    pay(ctx, &url, "b")?;
    let u = url.clone();
    saga.add("refund_b", move |c| pay(c, &u, "refund-b").map(drop));
    let r: Result<String, StepError> = run_step("book_c", &StepOptions::default(), || {
        Err(StepError::non_retryable("book_c_failed", "no rooms left"))
    });
    match r {
        Ok(v) => Err(WorkflowError::non_retryable(
            "unexpected",
            format!("book_c succeeded: {v}"),
        )),
        Err(e) => Err(saga.fail_with(e)),
    }
}

ostro_sdk::export!(
    RsConfStepsWorkflow,
    RsConfStepFailWorkflow,
    RsConfStepRngWorkflow,
    RsConfStepSiblingWorkflow,
    RsConfNativeWorkflow,
    RsConfRetryWorkflow,
    RsConfRetryExhaustWorkflow,
    RsConfTimerWorkflow,
    RsConfTimerVsSignalWorkflow,
    RsConfSignalsWorkflow,
    RsConfSignalTimeoutWorkflow,
    RsConfGatherWorkflow,
    RsConfRaceCancelWorkflow,
    RsConfCancelWaitWorkflow,
    RsConfCancelShieldedWorkflow,
    RsConfCancelTimerWorkflow,
    RsConfPatchWorkflow,
    RsConfNdetWorkflow,
    RsConfRandWorkflow,
    RsConfRandNativeWorkflow,
    RsConfValuesWorkflow,
    RsConfCanWorkflow,
    RsConfTokenWorkflow,
    RsConfStatusWorkflow,
    RsConfSagaThreeWorkflow,
    RsConfSagaCancelWorkflow,
    RsConfParentAttachedWorkflow,
    RsConfParentFailChildWorkflow,
    RsConfParentDetachedWorkflow,
    RsConfParentCancelChildWorkflow,
    RsConfParentCloseWorkflow,
    RsConfParentChainWorkflow,
    RsConfParentShieldedChildWorkflow,
    RsConfLeafEchoWorkflow,
    RsConfLeafFailWorkflow,
    RsConfLeafSleepWorkflow,
    RsConfLeafCanWorkflow,
    RsConfSigApiWorkflow,
    RsConfTokenApiWorkflow,
    RsConfFxHttpWorkflow,
    RsConfSagaHttpWorkflow,
);
