//! Conformance workflows shared by `conf-rust` and `conf-rust-v2` (plan 09-11).
//!
//! `conf-rust-v2` includes this file with `#[path]`, so both modules export identical
//! bodies for every workflow except `conf_patch` and `conf_ndet` (defined per crate).
//! The contract table is in `conf-rust/src/lib.rs`.

use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ostro_sdk::prelude::*;
use ostro_sdk::{RetryPolicy, select};
use serde_json::Value;

/// A step that returns `value` as a JSON string (default options).
pub fn named(name: &str, value: &str) -> Result<String, StepError> {
    run_step(name, &StepOptions::default(), || Ok(value.to_string()))
}

fn unexpected(what: &str, detail: impl std::fmt::Debug) -> WorkflowError {
    WorkflowError::non_retryable("unexpected", format!("{what}: {detail:?}"))
}

fn millis_since(t0: SystemTime, t: SystemTime) -> u64 {
    t.duration_since(t0)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

const HOUR: Duration = Duration::from_secs(3600);

// --- steps -----------------------------------------------------------------------

#[workflow(name = "rs_conf_steps", version = 1)]
pub fn rs_conf_steps(_ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let a = named("a", "a")?;
    let b = named("b", "b")?;
    let c = named("c", "c")?;
    Ok(json!({ "a": a, "b": b, "c": c }))
}

#[workflow(name = "rs_conf_step_fail", version = 1)]
pub fn rs_conf_step_fail(_ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let r: Result<String, StepError> = run_step("boom", &StepOptions::default(), || {
        Err(StepError::non_retryable("boom", "always fails"))
    });
    match r {
        Ok(v) => Err(unexpected("boom succeeded", v)),
        Err(e) => Ok(json!({ "caught": e.kind })),
    }
}

#[workflow(name = "rs_conf_step_rng", version = 1)]
pub fn rs_conf_step_rng(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let v: String = run_step("draw", &StepOptions::default(), || {
        Ok(ctx.random_u64().to_string())
    })?;
    Ok(json!({ "value": v }))
}

#[workflow(name = "rs_conf_step_sibling", version = 1)]
pub fn rs_conf_step_sibling(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    // Python/TS: an async step body yields while a sibling task draws ctx randomness and
    // starts a 1 s sleep. The step is atomic in the journal, so the sibling's commands land
    // after it: Rust, which has no concurrent tasks, runs them in that order directly.
    let step = named("s", "s")?;
    let sibling_rand = ctx.random_u64().to_string();
    ctx.sleep(Duration::from_secs(1)).wait()?;
    Ok(json!({ "step": step, "sibling_rand": sibling_rand }))
}

/// Native time, randomness and map iteration (WF-07) through the deterministic WASI shim.
fn observe_native() -> Value {
    let time_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
    let map: HashMap<String, u32> = (0..16).map(|i| (format!("k{i}"), i)).collect();
    let keys: Vec<String> = map.into_keys().collect();
    let hash = RandomState::new().hash_one("x");
    json!({ "time_nanos": time_nanos, "map_order": keys, "random": hash.to_string() })
}

#[workflow(name = "rs_conf_native", version = 1)]
pub fn rs_conf_native(_ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let outside = observe_native();
    let inside: Value = run_step("observe", &StepOptions::default(), || Ok(observe_native()))?;
    Ok(json!({ "outside": outside, "inside": inside }))
}

// --- retries ---------------------------------------------------------------------

/// `conf_retry` input.
#[derive(Debug, Deserialize)]
pub struct RetryInput {
    pub succeed_after_ms: u64,
}

#[workflow(name = "rs_conf_retry", version = 1)]
pub fn rs_conf_retry(ctx: &Ctx, input: RetryInput) -> Result<Value, WorkflowError> {
    // Run start in logical time; each attempt observes max(last ts, retry_at) (D22).
    let t0 = ctx.now();
    let opts = StepOptions {
        retry: Some(RetryPolicy {
            max_attempts: 3,
            initial_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(60),
            backoff_coefficient: 2.0,
            non_retryable_kinds: Vec::new(),
        }),
        ..StepOptions::default()
    };
    let elapsed: u64 = run_step("flaky", &opts, || {
        let elapsed = millis_since(t0, ctx.now());
        if elapsed < input.succeed_after_ms {
            Err(StepError::retryable(
                "transient",
                format!("{elapsed} ms < {} ms", input.succeed_after_ms),
            ))
        } else {
            Ok(elapsed)
        }
    })?;
    Ok(json!({ "elapsed_ms": elapsed }))
}

#[workflow(name = "rs_conf_retry_exhaust", version = 1)]
pub fn rs_conf_retry_exhaust(_ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let opts = StepOptions {
        retry: Some(RetryPolicy {
            max_attempts: 2,
            initial_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(60),
            backoff_coefficient: 2.0,
            non_retryable_kinds: Vec::new(),
        }),
        ..StepOptions::default()
    };
    let _: String = run_step("always", &opts, || {
        Err(StepError::retryable("transient", "always fails"))
    })?;
    Err(unexpected("always", "succeeded"))
}

// --- timers and signals ----------------------------------------------------------

#[workflow(name = "rs_conf_timer", version = 1)]
pub fn rs_conf_timer(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    ctx.sleep(HOUR).wait()?;
    Ok(json!({ "slept": true }))
}

#[workflow(name = "rs_conf_timer_vs_signal", version = 1)]
pub fn rs_conf_timer_vs_signal(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let timer = ctx.sleep(HOUR);
    let go = ctx.wait_signal::<Value>("go", None);
    let (i, c) = select!(timer, go);
    match i {
        0 => {
            timer.decode(c)?;
            Ok(json!({ "winner": "timer" }))
        }
        1 => Ok(json!({ "winner": "signal", "payload": go.decode(c)? })),
        _ => Err(timer
            .decode(c)
            .err()
            .map_or_else(|| unexpected("select", i), WorkflowError::from)),
    }
}

#[workflow(name = "rs_conf_signals", version = 1)]
pub fn rs_conf_signals(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let first: Value = ctx.wait_signal("go", None).wait()?;
    let second: Value = ctx.wait_signal("go", None).wait()?;
    Ok(json!({ "got": [first, second] }))
}

#[workflow(name = "rs_conf_signal_timeout", version = 1)]
pub fn rs_conf_signal_timeout(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    match ctx
        .wait_signal::<Value>("never", Some(Duration::from_secs(10)))
        .wait()
    {
        Err(e) if e.is_timeout() => Ok(json!({ "timeout": true })),
        Ok(v) => Ok(json!({ "timeout": false, "payload": v })),
        Err(e) => Err(e.into()),
    }
}

#[workflow(name = "rs_conf_gather", version = 1)]
pub fn rs_conf_gather(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let (a, b, go) = ostro_sdk::join!(
        ctx.sleep(Duration::from_secs(1)),
        ctx.sleep(Duration::from_secs(2)),
        ctx.wait_signal::<Value>("go", None)
    );
    a?;
    b?;
    Ok(json!({ "signal": go? }))
}

#[workflow(name = "rs_conf_race_cancel", version = 1)]
pub fn rs_conf_race_cancel(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let timer = ctx.sleep(HOUR);
    let go = ctx.wait_signal::<Value>("go", None);
    let (i, c) = select!(timer, go);
    let winner = match i {
        0 => {
            timer.decode(c)?;
            ctx.cancel(&go);
            match go.wait() {
                Err(e) if e.is_cancelled() => "timer",
                other => return Err(unexpected("loser signal wait", other)),
            }
        }
        1 => {
            go.decode(c)?;
            ctx.cancel(&timer);
            match timer.wait() {
                Err(e) if e.is_cancelled() => "signal",
                other => return Err(unexpected("loser timer wait", other)),
            }
        }
        _ => {
            return Err(timer
                .decode(c)
                .map_or_else(WorkflowError::from, |()| unexpected("select", i)));
        }
    };
    Ok(json!({ "winner": winner, "loser_cancelled": true }))
}

// --- cancellation ----------------------------------------------------------------

#[workflow(name = "rs_conf_cancel_wait", version = 1)]
pub fn rs_conf_cancel_wait(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    ctx.sleep(HOUR).wait()?;
    Ok(json!({ "slept": true }))
}

#[workflow(name = "rs_conf_cancel_shielded", version = 1)]
pub fn rs_conf_cancel_shielded(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    match ctx.sleep(HOUR).wait() {
        Ok(()) => Ok(json!({ "slept": true })),
        Err(e) if e.is_cancelled() => {
            ctx.shielded(|| named("cleanup", "cleaned"))?;
            Err(e.into())
        }
        Err(e) => Err(e.into()),
    }
}

#[workflow(name = "rs_conf_cancel_timer", version = 1)]
pub fn rs_conf_cancel_timer(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let timer = ctx.sleep(HOUR);
    ctx.cancel(&timer);
    match timer.wait() {
        Err(e) if e.is_cancelled() => Ok(json!({ "cancelled_op": true })),
        other => Err(unexpected("cancelled timer wait", other)),
    }
}

// --- randomness and values -------------------------------------------------------

#[workflow(name = "rs_conf_rand", version = 1)]
pub fn rs_conf_rand(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let u: Vec<String> = (0..3).map(|_| ctx.random_u64().to_string()).collect();
    let uuid = ctx.uuid_v7();
    Ok(json!({ "u64": u, "uuid": uuid }))
}

#[workflow(name = "rs_conf_rand_native", version = 1)]
pub fn rs_conf_rand_native(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let bytes = hex(&ctx.random_bytes(8));
    let f = ctx.random_f64();
    Ok(json!({ "bytes": bytes, "f64": f }))
}

#[workflow(name = "rs_conf_values", version = 1)]
pub fn rs_conf_values(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    // The order is part of the contract (PF-W9: random and uuid share or split streams).
    let random_u64 = ctx.random_u64().to_string();
    let random_f64 = ctx.random_f64();
    let uuid_v7 = ctx.uuid_v7();
    let idempotency_key = ctx.idempotency_key("x");
    Ok(json!({
        "random_u64": random_u64,
        "random_f64": random_f64,
        "uuid_v7": uuid_v7,
        "idempotency_key": idempotency_key,
    }))
}

// --- continue-as-new, tokens, status ---------------------------------------------

/// `conf_can` input.
#[derive(Debug, Serialize, Deserialize)]
pub struct CanInput {
    pub n: u32,
    #[serde(default)]
    pub generation: u32,
}

#[workflow(name = "rs_conf_can", version = 1)]
pub fn rs_conf_can(ctx: &Ctx, input: CanInput) -> Result<Value, WorkflowError> {
    if input.n == 0 {
        return Ok(json!({ "generation": input.generation }));
    }
    Err(ctx.continue_as_new(&CanInput {
        n: input.n - 1,
        generation: input.generation + 1,
    }))
}

#[workflow(name = "rs_conf_token", version = 1)]
pub fn rs_conf_token(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let t = ctx.create_await_token::<Value>(None);
    if t.token().is_empty() {
        return Err(unexpected("token", "empty"));
    }
    let payload = t.op().wait()?;
    Ok(json!({ "token_payload": payload }))
}

#[workflow(name = "rs_conf_status", version = 1)]
pub fn rs_conf_status(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    ctx.set_status(&json!({ "stage": "one" }))?;
    ctx.set_status(&json!({ "stage": "two", "n": 2 }))?;
    Ok(json!({ "done": true }))
}

// --- sagas (same shapes as guests/sdk-saga) --------------------------------------

fn book_two(ctx: &Ctx) -> Result<Saga<'_>, WorkflowError> {
    let mut saga = Saga::new(ctx);
    named("book_a", "a")?;
    saga.add("cancel_a", |_| Ok(named("cancel_a", "a").map(drop)?));
    named("book_b", "b")?;
    saga.add("cancel_b", |_| Ok(named("cancel_b", "b").map(drop)?));
    Ok(saga)
}

#[workflow(name = "rs_conf_saga_three", version = 1)]
pub fn rs_conf_saga_three(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let saga = book_two(ctx)?;
    let r: Result<String, StepError> = run_step("book_c", &StepOptions::default(), || {
        Err(StepError::non_retryable("book_c_failed", "no rooms left"))
    });
    match r {
        Ok(v) => Err(unexpected("book_c succeeded", v)),
        Err(e) => Err(saga.fail_with(e)),
    }
}

#[workflow(name = "rs_conf_saga_cancel", version = 1)]
pub fn rs_conf_saga_cancel(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let saga = book_two(ctx)?;
    match ctx.sleep(HOUR).wait() {
        Ok(()) => Ok(json!({ "slept": true })),
        Err(e) => Err(saga.fail_with(e)),
    }
}
