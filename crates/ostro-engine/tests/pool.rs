//! ENG-08 / ENG-05: pooling allocator failure wrapping, fallback, exhaustion,
//! warm-instantiate latency and the epoch ticker.

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ostro_engine::config::{EngineConfig, Fallback, PoolConfig};
use ostro_engine::engine::{OstroEngine, PoolMode};
use ostro_engine::error::EngineError;
use ostro_engine::host::bindings::exports::ostro::runtime::workflows::RunResult;
use ostro_engine::host::bindings::ostro::runtime::types::Payload;
use ostro_engine::host::bindings::{Workflow, WorkflowPre};
use wasmtime::Store;
use wasmtime::component::{Component, Linker};

fn cfg(total_instances: u32, fallback: Fallback) -> EngineConfig {
    EngineConfig {
        pool: PoolConfig {
            total_instances,
            fallback,
            ..PoolConfig::default()
        },
        ..EngineConfig::default()
    }
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

fn new_store(engine: &wasmtime::Engine) -> Store<()> {
    let mut store = Store::new(engine, ());
    store.set_fuel(u64::MAX).expect("fuel");
    store.set_epoch_deadline(u64::MAX);
    store
}

/// Smallest instance count (from 40,000, doubling up to 1,280,000) whose pool
/// reservation fails on this machine.
fn failing_instance_count() -> u32 {
    let mut n = 40_000u32;
    loop {
        match OstroEngine::new(cfg(n, Fallback::Fail)) {
            Err(EngineError::PoolReservation { .. }) => return n,
            Err(other) => panic!("unexpected error at {n}: {other}"),
            Ok(_) => {
                eprintln!("machine reserved a pool of {n} instances; raising the count");
                assert!(n < 1_280_000, "pool reservation never failed up to {n}");
                n *= 2;
            }
        }
    }
}

#[test]
fn pool_reservation_failure_wrapped() {
    let n = failing_instance_count();
    let Err(EngineError::PoolReservation { message }) = OstroEngine::new(cfg(n, Fallback::Fail))
    else {
        panic!("expected PoolReservation");
    };
    eprintln!("{message}");
    assert!(
        message.contains(&format!("engine.pool.total_instances={n}")),
        "{message}"
    );
    let gib = u128::from(n) * ((4u128 << 30) + (32 << 20)) / (1 << 30);
    assert!(message.contains(&format!("~{gib} GiB")), "{message}");
    assert!(message.contains("ulimit -v"), "{message}");
    assert!(message.contains("vm.overcommit_memory"), "{message}");
}

/// Minimal subscriber that records WARN event messages.
struct WarnCapture(Arc<Mutex<Vec<String>>>);

struct MessageVisitor<'a>(&'a mut String);

impl tracing::field::Visit for MessageVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.push_str(&format!("{}={:?} ", field.name(), value));
    }
}

impl tracing::Subscriber for WarnCapture {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if *event.metadata().level() == tracing::Level::WARN {
            let mut s = String::new();
            event.record(&mut MessageVisitor(&mut s));
            self.0.lock().unwrap().push(s);
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[test]
fn pool_reservation_fallback() -> wasmtime::Result<()> {
    let n = failing_instance_count();
    let warns = Arc::new(Mutex::new(Vec::new()));
    let engine = tracing::subscriber::with_default(WarnCapture(warns.clone()), || {
        OstroEngine::new(cfg(n, Fallback::OnDemand))
    })
    .expect("on-demand fallback");
    let PoolMode::OnDemand { reason } = engine.pool_mode() else {
        panic!("expected OnDemand, got {:?}", engine.pool_mode());
    };
    assert!(reason.contains("engine.pool.total_instances="), "{reason}");
    assert!(engine.pooling_metrics().is_none());
    let warns = warns.lock().unwrap().clone();
    assert!(
        warns
            .iter()
            .any(|w| w.contains("on-demand") && w.contains("engine.pool.total_instances")),
        "no fallback warning captured: {warns:?}"
    );

    let wasm = common::guest_wasm("smoke");
    let component = Component::from_binary(engine.engine(), &wasm)?;
    let mut linker: Linker<()> = Linker::new(engine.engine());
    linker.define_unknown_imports_as_traps(&component)?;
    rt().block_on(async {
        let mut store = new_store(engine.engine());
        let wf = Workflow::instantiate_async(&mut store, &component, &linker).await?;
        let input = Payload {
            content_type: "x".into(),
            data: vec![7],
        };
        match wf
            .ostro_runtime_workflows()
            .call_run(&mut store, "smoke", &input)
            .await?
        {
            Ok(RunResult::Completed(out)) => assert_eq!(out.data, vec![7]),
            other => panic!("unexpected: {other:?}"),
        }
        Ok(())
    })
}

/// Child half of `pool_reservation_prlimit` (run under `prlimit --as`).
#[test]
#[ignore = "spawned by pool_reservation_prlimit"]
fn pool_child_default_engine() {
    if std::env::var_os("OSTRO_POOL_CHILD").is_none() {
        return;
    }
    let engine = OstroEngine::new(EngineConfig::default()).expect("engine");
    match engine.pool_mode() {
        PoolMode::Pooling => println!("pool_mode=Pooling"),
        PoolMode::OnDemand { reason } => println!("pool_mode=OnDemand reason={reason}"),
    }
}

#[cfg(target_os = "linux")]
#[test]
fn pool_reservation_prlimit() {
    let prlimit_ok = std::process::Command::new("prlimit")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !prlimit_ok {
        eprintln!("prlimit not found; skipping pool_reservation_prlimit");
        return;
    }
    let exe = std::env::current_exe().expect("current_exe");
    let out = std::process::Command::new("prlimit")
        .arg("--as=8589934592")
        .arg("--")
        .arg(&exe)
        .args([
            "--exact",
            "pool_child_default_engine",
            "--ignored",
            "--nocapture",
        ])
        .env("OSTRO_POOL_CHILD", "1")
        .output()
        .expect("spawn prlimit child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "child failed:\n{stdout}\n{stderr}");
    assert!(
        stdout.contains("pool_mode=OnDemand"),
        "default pool should not fit in 8 GiB of address space:\n{stdout}\n{stderr}"
    );
    assert!(
        stdout.contains("engine.pool.total_instances=1000"),
        "{stdout}"
    );
}

#[test]
fn pool_exhaustion_clear_error() -> wasmtime::Result<()> {
    let engine = OstroEngine::new(cfg(2, Fallback::Fail)).expect("pooling engine");
    assert_eq!(engine.pool_mode(), &PoolMode::Pooling);
    let wasm = common::guest_wasm("smoke");
    let component = Component::from_binary(engine.engine(), &wasm)?;
    let mut linker: Linker<()> = Linker::new(engine.engine());
    linker.define_unknown_imports_as_traps(&component)?;
    rt().block_on(async {
        let mut live = Vec::new();
        for _ in 0..2 {
            let mut store = new_store(engine.engine());
            let wf = Workflow::instantiate_async(&mut store, &component, &linker).await?;
            live.push((store, wf));
        }
        let mut store = new_store(engine.engine());
        let Err(err) = Workflow::instantiate_async(&mut store, &component, &linker).await else {
            panic!("third live instance must exhaust a 2-slot pool");
        };
        let msg = engine.explain_instantiate_error(&err);
        eprintln!("{msg}");
        assert!(
            msg.contains("instance pool exhausted (engine.pool.total_instances=2)"),
            "{msg}"
        );
        drop(live);
        // Slots are reusable once the stores are gone.
        let mut store = new_store(engine.engine());
        Workflow::instantiate_async(&mut store, &component, &linker).await?;
        Ok(())
    })
}

#[test]
fn pool_warm_instantiate_p50() -> wasmtime::Result<()> {
    let engine = OstroEngine::new(EngineConfig::default()).expect("engine");
    if engine.pool_mode() != &PoolMode::Pooling {
        eprintln!("note: pooling unavailable here ({:?})", engine.pool_mode());
    }
    let wasm = common::guest_wasm("smoke");
    let component = Component::from_binary(engine.engine(), &wasm)?;
    let mut linker: Linker<()> = Linker::new(engine.engine());
    linker.define_unknown_imports_as_traps(&component)?;
    let pre = WorkflowPre::new(linker.instantiate_pre(&component)?)?;
    rt().block_on(async {
        // Warm the slot.
        for _ in 0..10 {
            let mut store = new_store(engine.engine());
            pre.instantiate_async(&mut store).await?;
        }
        let mut samples = Vec::with_capacity(1000);
        for _ in 0..1000 {
            let start = Instant::now();
            let mut store = new_store(engine.engine());
            let _wf = pre.instantiate_async(&mut store).await?;
            samples.push(start.elapsed());
            drop(store);
        }
        samples.sort();
        let p50 = samples[499];
        let p99 = samples[989];
        println!("warm instantiate p50={p50:?} p99={p99:?}");
        eprintln!("warm instantiate p50={p50:?} p99={p99:?}");
        assert!(p50 < Duration::from_millis(1), "p50 {p50:?} >= 1 ms");
        Ok(())
    })
}

#[test]
fn epoch_ticker_runs_and_stops() {
    let engine = OstroEngine::new(EngineConfig {
        epoch_tick: Duration::from_millis(10),
        ..cfg(4, Fallback::OnDemand)
    })
    .expect("engine");
    let probe = engine.ticker_probe();
    let before = probe.ticks();
    std::thread::sleep(Duration::from_millis(100));
    let advanced = probe.ticks() - before;
    assert!(advanced >= 5, "only {advanced} ticks in 100 ms");
    assert!(!probe.exited());

    let dropped = Instant::now();
    drop(engine);
    while !probe.exited() {
        assert!(
            dropped.elapsed() < Duration::from_millis(30),
            "ticker still running {:?} after drop",
            dropped.elapsed()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let final_ticks = probe.ticks();
    std::thread::sleep(Duration::from_millis(30));
    assert_eq!(probe.ticks(), final_ticks, "ticker ticked after exit");
}
