//! ENG-2 (plan 09-07): core-instance counting and weighted admission.
//!
//! - `CoreBudget` unit behaviour (FIFO weighted semaphore, timeout, too-large weight).
//! - The admission weight is exact: the pool must hold `n` core instances to instantiate
//!   one and fails with `n - 1` (Wasmtime is the ground truth). For Rust guests the static
//!   `count_core_instances` already equals it; `Runtime::core_weight` (calibrated) equals
//!   it for every guest. `OSTRO_CORE_COUNT_EXTRA=a.wasm,b.wasm` checks more components the
//!   same way (used for the Python/QuickJS numbers in the 09-07 summary; the polyglot
//!   conformance test checks the built fixtures).
//! - `Runtime::activate` holds the permit across hot-wait parking, a second activation
//!   waits for it, and a budget that stays exhausted until the activation deadline evicts
//!   with `BudgetPressure` (exactly one `done`) instead of failing; `describe` waits and
//!   fails with its ordinary error type.
//! - Plan eng2-mem: `PoolBudget` admits only when every pooled resource (component
//!   instances, core instances, memories, tables) fits, releases partial permits on
//!   timeout, and `Runtime::activate` waits (then evicts with `BudgetPressure`, or
//!   `External` on cancel) when the pool's instance/memory slots, not its core instances,
//!   are the binding limit. The QuickJS memories case runs with the real guest in
//!   `ostro-conformance/tests/polyglot_engine.rs`.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_engine::config::{EngineConfig, Fallback, PoolConfig};
use ostro_engine::core_budget::{
    BudgetError, CoreBudget, PoolBudget, PoolBudgetError, PoolResource, PoolWeights,
};
use ostro_engine::engine::OstroEngine;
use ostro_engine::error::{EngineError, EvictReason};
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::testkit::{DEFAULT_ACCEPTED_AT, FakeClock, FakeSink, RunSpec};
use ostro_engine::validate::count_core_instances;
use ostro_types::journal::{Event, RngSeed};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use wasmtime::Store;
use wasmtime::component::{Component, Linker};

// ---------------------------------------------------------------------------
// CoreBudget
// ---------------------------------------------------------------------------

fn soon(ms: u64) -> Instant {
    Instant::now() + Duration::from_millis(ms)
}

#[tokio::test]
async fn budget_admits_by_weight_and_waits() {
    let b = Arc::new(CoreBudget::new(1000));
    let mut held = Vec::new();
    for _ in 0..26 {
        held.push(b.acquire(38, soon(10)).await.expect("immediate"));
    }
    assert_eq!(b.in_use(), 26 * 38);
    // The 27th does not fit (12 slots left) and times out.
    let t = Instant::now();
    assert_eq!(
        b.acquire(38, soon(100)).await.unwrap_err(),
        BudgetError::Timeout { weight: 38 }
    );
    assert!(t.elapsed() >= Duration::from_millis(90));
    // A waiting 27th gets in once one permit is dropped.
    let waiter = {
        let b = b.clone();
        tokio::spawn(async move { b.acquire(38, soon(5_000)).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiter.is_finished());
    drop(held.pop());
    let p = waiter.await.expect("join").expect("admitted after release");
    assert_eq!(p.weight(), 38);
    drop(p);
    drop(held);
    assert_eq!(b.in_use(), 0);
}

#[tokio::test]
async fn budget_rejects_too_large_and_admits_zero() {
    let b = CoreBudget::new(10);
    assert_eq!(
        b.acquire(11, soon(1_000)).await.unwrap_err(),
        BudgetError::TooLarge {
            weight: 11,
            capacity: 10
        }
    );
    let _all = b.acquire(10, soon(10)).await.expect("whole budget");
    let zero = b.acquire(0, soon(10)).await.expect("zero weight");
    assert_eq!(zero.weight(), 0);
    assert_eq!(b.available(), 0);
}

// ---------------------------------------------------------------------------
// Core-instance count vs Wasmtime's pool limit
// ---------------------------------------------------------------------------

/// Pool with only the core-instance total binding (memories and tables are sized from
/// `total_instances`; QuickJS needs 3 of each per instance, S9).
fn pool_cfg(total_core_instances: u32) -> EngineConfig {
    EngineConfig {
        pool: PoolConfig {
            total_instances: 8,
            total_core_instances: Some(total_core_instances),
            fallback: Fallback::Fail,
            ..PoolConfig::default()
        },
        ..EngineConfig::default()
    }
}

/// Instantiate `wasm` once in a pool holding `core` core instances.
fn instantiates_with(wasm: &[u8], core: u32) -> bool {
    let engine = OstroEngine::new(pool_cfg(core)).expect("engine");
    let component = Component::from_binary(engine.engine(), wasm).expect("component");
    let mut linker: Linker<()> = Linker::new(engine.engine());
    linker
        .define_unknown_imports_as_traps(&component)
        .expect("traps");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio");
    rt.block_on(async {
        let mut store = Store::new(engine.engine(), ());
        store.set_fuel(u64::MAX).expect("fuel");
        store.set_epoch_deadline(u64::MAX);
        match linker.instantiate_async(&mut store, &component).await {
            Ok(_) => true,
            Err(e) => {
                let msg = format!("{e:#}");
                assert!(msg.contains("core instance"), "unexpected error: {msg}");
                false
            }
        }
    })
}

/// Calibrated admission weight of `wasm` on a pooling runtime.
fn calibrated_weight(wasm: &[u8]) -> u32 {
    let rt = Runtime::new(pool_cfg(10_000), RuntimeOptions::default()).expect("runtime");
    let p = rt.prepare(wasm).expect("prepare");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio")
        .block_on(rt.core_weight(&p))
}

/// `n` is exactly the pool minimum of `wasm`.
fn assert_exact(what: &str, wasm: &[u8], n: u32) -> u32 {
    assert!(
        instantiates_with(wasm, n),
        "{what}: {n} core instances not enough"
    );
    assert!(
        !instantiates_with(wasm, n - 1),
        "{what}: instantiated with {} (< count {n})",
        n - 1
    );
    println!("{what}: core_instances={n} (exact)");
    n
}

#[test]
fn rust_guest_count_is_exact() {
    for guest in ["smoke", "det-basic", "exports-shape-ok"] {
        let wasm = common::guest_wasm(guest);
        let n = count_core_instances(&wasm).expect("count");
        assert_eq!(assert_exact(guest, &wasm, n), 3, "{guest}");
        assert_eq!(calibrated_weight(&wasm), n, "{guest}");
    }
}

#[test]
fn extra_components_count_is_exact() {
    let Ok(list) = std::env::var("OSTRO_CORE_COUNT_EXTRA") else {
        return;
    };
    for path in list.split(',').filter(|p| !p.is_empty()) {
        let wasm = std::fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        let n = calibrated_weight(&wasm);
        println!(
            "{path}: static count {}",
            count_core_instances(&wasm).expect("count")
        );
        assert_exact(path, &wasm, n);
    }
}

// ---------------------------------------------------------------------------
// Runtime admission
// ---------------------------------------------------------------------------

/// Runtime whose core budget holds exactly one Rust instance (3 slots).
fn runtime(activation_timeout: Duration) -> Arc<Runtime> {
    Arc::new(
        Runtime::new(
            EngineConfig {
                hot_wait: Duration::from_secs(30),
                activation_timeout,
                pool: PoolConfig {
                    total_instances: 4,
                    total_core_instances: Some(3),
                    ..PoolConfig::default()
                },
                ..EngineConfig::default()
            },
            RuntimeOptions {
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
                token_key: [3; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime"),
    )
}

fn prepare(rt: &Runtime) -> Arc<Prepared> {
    let p = rt
        .prepare(&common::guest_wasm("det-basic"))
        .expect("prepare det-basic");
    assert_eq!(p.core_instances, 3);
    Arc::new(p)
}

struct Running {
    handle: JoinHandle<ActivationOutcome>,
    cancel: CancellationToken,
    run: RunRef,
    _tx: UnboundedSender<Event>,
}

/// Start det-basic (it sleeps 5 s of logical time, so with a 30 s hot wait it parks
/// hot until cancelled; no input is ever delivered).
fn start(rt: &Arc<Runtime>, p: &Arc<Prepared>, sink: &Arc<FakeSink>, id: &str) -> Running {
    let run = RunRef {
        partition: 4,
        run_id: id.to_string(),
    };
    let mut spec = RunSpec::new(run.clone(), "det", b"");
    spec.seed = RngSeed([1; 32]);
    sink.seed(
        run.clone(),
        vec![spec.run_started_event(&p.module_hash_hex)],
        1,
    );
    let (tx, rx) = unbounded_channel();
    let cancel = CancellationToken::new();
    let act = Activate {
        run: run.clone(),
        attempt: 1,
        module_sha: p.sha,
        journal: sink.journal(&run),
        inputs: Some(rx),
        cancel: cancel.clone(),
    };
    let (rt, p, sink) = (rt.clone(), p.clone(), sink.clone());
    let handle = tokio::spawn(async move { rt.activate(&p, act, sink).await });
    Running {
        handle,
        cancel,
        run,
        _tx: tx,
    }
}

async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "timed out: {what}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn dones(sink: &FakeSink, run: &RunRef) -> Vec<ActivationOutcome> {
    sink.outcomes()
        .into_iter()
        .filter(|(r, _, _)| r == run)
        .map(|(_, _, o)| o)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exhausted_budget_evicts_with_budget_pressure() {
    let rt = runtime(Duration::from_millis(400));
    let p = prepare(&rt);
    let sink = Arc::new(FakeSink::new());

    // A parks hot inside run_activation and keeps its permit while parked.
    let a = start(&rt, &p, &sink, "budget-a");
    wait_until("A parked", || rt.parked().parked_count() == 1).await;
    assert_eq!(rt.core_budget().in_use(), 3, "permit held while parked");

    // B cannot get slots before its activation deadline: evicted, never failed.
    let t = Instant::now();
    let b = start(&rt, &p, &sink, "budget-b");
    let outcome = b.handle.await.expect("join");
    assert_eq!(
        outcome,
        ActivationOutcome::Evicted {
            reason: EvictReason::BudgetPressure
        }
    );
    assert!(
        t.elapsed() >= Duration::from_millis(350),
        "{:?}",
        t.elapsed()
    );
    assert_eq!(dones(&sink, &b.run), vec![outcome], "exactly one done");
    // Nothing was journaled for B (run_activation never ran).
    assert_eq!(sink.journal(&b.run).len(), 1);
    assert_eq!(rt.parked().parked_count(), 1, "A still parked");

    // A cancelled: its permit is released when activate returns.
    a.cancel.cancel();
    let out_a = a.handle.await.expect("join");
    assert!(
        matches!(out_a, ActivationOutcome::Evicted { .. }),
        "{out_a:?}"
    );
    assert_eq!(rt.core_budget().in_use(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_activation_waits_for_the_permit() {
    let rt = runtime(Duration::from_secs(60));
    let p = prepare(&rt);
    let sink = Arc::new(FakeSink::new());

    let a = start(&rt, &p, &sink, "wait-a");
    wait_until("A parked", || rt.parked().parked_count() == 1).await;
    let b = start(&rt, &p, &sink, "wait-b");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!b.handle.is_finished(), "B waits for slots");
    assert_eq!(rt.live_activations(), 2);
    assert_eq!(rt.parked().parked_count(), 1, "B has not instantiated");

    // Releasing A's slots admits B, which then parks itself.
    a.cancel.cancel();
    a.handle.await.expect("join");
    wait_until("B parked", || {
        rt.parked().parked_count() == 1 && dones(&sink, &b.run).is_empty()
    })
    .await;
    assert_eq!(rt.core_budget().in_use(), 3);
    b.cancel.cancel();
    let out_b = b.handle.await.expect("join");
    assert!(
        matches!(out_b, ActivationOutcome::Evicted { .. }),
        "{out_b:?}"
    );
    assert_eq!(dones(&sink, &b.run).len(), 1);
    assert_eq!(rt.core_budget().in_use(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn describe_waits_then_fails_with_its_error_type() {
    let rt = runtime(Duration::from_secs(60));
    let p = prepare(&rt);
    let sink = Arc::new(FakeSink::new());
    assert!(!rt.describe(&p).await.expect("describe").is_empty());

    let a = start(&rt, &p, &sink, "describe-a");
    wait_until("A parked", || rt.parked().parked_count() == 1).await;
    let t = Instant::now();
    let err = rt.describe(&p).await.unwrap_err();
    assert!(matches!(err, EngineError::Wasmtime(_)), "{err:?}");
    assert!(err.to_string().contains("core-instance budget"), "{err}");
    assert!(t.elapsed() >= ostro_engine::runtime::BUDGET_PROBE_TIMEOUT);

    a.cancel.cancel();
    a.handle.await.expect("join");
    assert!(!rt.describe(&p).await.expect("describe").is_empty());
}

#[test]
fn oversized_module_rejected_at_validation() {
    let rt = Runtime::new(
        EngineConfig {
            pool: PoolConfig {
                total_instances: 2,
                total_core_instances: Some(2),
                ..PoolConfig::default()
            },
            ..EngineConfig::default()
        },
        RuntimeOptions::default(),
    )
    .expect("runtime");
    let err = rt.prepare(&common::guest_wasm("smoke")).unwrap_err();
    let EngineError::Validation(v) = err else {
        panic!("expected validation error, got {err:?}");
    };
    assert!(
        v.iter()
            .any(|m| m.contains("needs 3 core instances") && m.contains("(2,")),
        "{v:?}"
    );
}

// ---------------------------------------------------------------------------
// PoolBudget: every pooled resource (plan eng2-mem)
// ---------------------------------------------------------------------------

/// QuickJS-shaped weights: 1 instance, 13 core instances, 3 memories, 3 tables.
const QJS: PoolWeights = PoolWeights {
    instances: 1,
    core_instances: 13,
    memories: 3,
    tables: 3,
};

/// The pool `class = mixed, total_instances = 60` sizes.
fn mixed_60() -> PoolWeights {
    PoolWeights {
        instances: 60,
        core_instances: 60 * 38,
        memories: 60,
        tables: 120,
    }
}

#[tokio::test]
async fn pool_budget_admits_only_when_every_resource_fits() {
    let b = Arc::new(PoolBudget::new(mixed_60()));
    let mut held = Vec::new();
    // The core budget alone would admit 175; the memories hold 20.
    for _ in 0..20 {
        held.push(b.acquire(QJS, soon(10)).await.expect("immediate"));
    }
    assert_eq!(
        b.in_use(),
        PoolWeights {
            instances: 20,
            core_instances: 260,
            memories: 60,
            tables: 60,
        }
    );
    // The 21st waits on memories and times out; its partial permits are released.
    assert_eq!(
        b.acquire(QJS, soon(100)).await.unwrap_err(),
        PoolBudgetError {
            resource: PoolResource::Memories,
            source: BudgetError::Timeout { weight: 3 },
        }
    );
    assert_eq!(b.in_use().core_instances, 260, "partial permits released");
    assert_eq!(b.in_use().instances, 20);
    // A waiting 21st is admitted once one instance is released.
    let waiter = {
        let b = b.clone();
        tokio::spawn(async move { b.acquire(QJS, soon(5_000)).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiter.is_finished());
    drop(held.pop());
    let p = waiter.await.expect("join").expect("admitted after release");
    assert_eq!(p.weights(), QJS);
    drop(p);
    drop(held);
    assert_eq!(b.in_use(), PoolWeights::default());
}

#[tokio::test]
async fn pool_budget_rejects_too_large_without_holding_anything() {
    let b = PoolBudget::new(mixed_60());
    let w = PoolWeights {
        memories: 61,
        ..QJS
    };
    assert_eq!(
        b.acquire(w, soon(1_000)).await.unwrap_err(),
        PoolBudgetError {
            resource: PoolResource::Memories,
            source: BudgetError::TooLarge {
                weight: 61,
                capacity: 60
            },
        }
    );
    assert_eq!(b.in_use(), PoolWeights::default());
    let e = b
        .acquire(PoolWeights { tables: 121, ..QJS }, soon(1_000))
        .await
        .unwrap_err();
    assert_eq!(e.resource, PoolResource::Tables);
    assert!(e.to_string().starts_with("table budget: "), "{e}");
}

/// Pooling runtime with room for 30 Rust instances by core instances but only `slots`
/// component instances (and so `slots` memories): the core budget is not the binding
/// limit, the other pool totals are.
fn slot_bound_runtime(slots: u32, activation_timeout: Duration) -> Arc<Runtime> {
    Arc::new(
        Runtime::new(
            EngineConfig {
                hot_wait: Duration::from_secs(30),
                activation_timeout,
                pool: PoolConfig {
                    total_instances: slots,
                    total_core_instances: Some(90),
                    fallback: Fallback::Fail,
                    ..PoolConfig::default()
                },
                ..EngineConfig::default()
            },
            RuntimeOptions {
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
                token_key: [5; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_slots_not_core_instances_bound_admission() {
    let rt = slot_bound_runtime(2, Duration::from_secs(60));
    let p = prepare(&rt);
    assert_eq!((p.memories, p.tables), (1, 2), "det-basic shape");
    assert_eq!(
        rt.admission_weights(&p).await,
        PoolWeights {
            instances: 1,
            core_instances: 3,
            memories: 1,
            tables: 2,
        }
    );
    let sink = Arc::new(FakeSink::new());
    let a = start(&rt, &p, &sink, "slots-a");
    let b = start(&rt, &p, &sink, "slots-b");
    wait_until("A and B parked", || rt.parked().parked_count() == 2).await;
    // Before plan eng2-mem C was admitted (6 of 90 core slots in use) and failed to
    // instantiate with pool exhaustion; now it waits for a slot.
    let c = start(&rt, &p, &sink, "slots-c");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!c.handle.is_finished(), "C waits for a pool slot");
    assert!(sink.outcomes().is_empty(), "{:?}", sink.outcomes());
    assert_eq!(rt.pool_budget().in_use().memories, 2);
    assert_eq!(rt.core_budget().in_use(), 6);

    // Releasing A admits C, which parks.
    a.cancel.cancel();
    a.handle.await.expect("join");
    wait_until("C parked", || {
        rt.parked().parked_count() == 2 && dones(&sink, &c.run).is_empty()
    })
    .await;

    // D waits; cancelled while waiting it evicts External with exactly one done.
    let d = start(&rt, &p, &sink, "slots-d");
    tokio::time::sleep(Duration::from_millis(100)).await;
    d.cancel.cancel();
    let out_d = d.handle.await.expect("join");
    assert_eq!(
        out_d,
        ActivationOutcome::Evicted {
            reason: EvictReason::External
        }
    );
    assert_eq!(dones(&sink, &d.run), vec![out_d]);
    assert_eq!(sink.journal(&d.run).len(), 1, "D never ran");

    for r in [b, c] {
        r.cancel.cancel();
        let out = r.handle.await.expect("join");
        assert!(matches!(out, ActivationOutcome::Evicted { .. }), "{out:?}");
    }
    assert!(
        !sink
            .outcomes()
            .iter()
            .any(|(_, _, o)| matches!(o, ActivationOutcome::AttemptFailed(_))),
        "{:?}",
        sink.outcomes()
    );
    assert_eq!(rt.pool_budget().in_use(), PoolWeights::default());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exhausted_pool_slots_evict_with_budget_pressure() {
    let rt = slot_bound_runtime(1, Duration::from_millis(400));
    let p = prepare(&rt);
    let sink = Arc::new(FakeSink::new());
    let a = start(&rt, &p, &sink, "slot1-a");
    wait_until("A parked", || rt.parked().parked_count() == 1).await;
    let b = start(&rt, &p, &sink, "slot1-b");
    let out_b = b.handle.await.expect("join");
    assert_eq!(
        out_b,
        ActivationOutcome::Evicted {
            reason: EvictReason::BudgetPressure
        }
    );
    assert_eq!(dones(&sink, &b.run), vec![out_b]);
    // describe waits its 5 s and fails with the resource in the message.
    let err = rt.describe(&p).await.unwrap_err();
    assert!(
        err.to_string().contains("component-instance budget"),
        "{err}"
    );
    a.cancel.cancel();
    a.handle.await.expect("join");
    assert!(!rt.describe(&p).await.expect("describe").is_empty());
    assert_eq!(rt.pool_budget().in_use(), PoolWeights::default());
}
