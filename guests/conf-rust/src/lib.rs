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
);
