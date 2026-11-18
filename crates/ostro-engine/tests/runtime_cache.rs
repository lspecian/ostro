//! Runtime module preparation through the AOT cache (plan 03-11, ENG-07, ENG-08).
//!
//! `Runtime::prepare_cached` = L2 `InstancePre` LRU (by module sha) over the L1
//! authenticated AOT disk cache. Validation runs on every load (cache hits included),
//! and a warm prepared module instantiates with the full production linker in < 1 ms
//! p50 (NFR-2.3).

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_engine::config::EngineConfig;
use ostro_engine::error::EngineError;
use ostro_engine::runtime::{Runtime, RuntimeOptions};
use ostro_engine::sink::{ActivationOutcome, RunRef};
use ostro_engine::testkit::{DEFAULT_ACCEPTED_AT, FakeClock, RunSpec, Script, drive_to_completion};

fn runtime(cache_dir: Option<&Path>, capacity: usize) -> Runtime {
    Runtime::new(
        EngineConfig::default(),
        RuntimeOptions {
            clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
            token_key: [4; 32],
            cache_dir: cache_dir.map(Path::to_path_buf),
            instance_pre_capacity: capacity,
        },
    )
    .expect("runtime")
}

fn run_ref(id: &str) -> RunRef {
    RunRef {
        partition: 2,
        run_id: id.to_string(),
    }
}

async fn det_output(rt: &Runtime, wasm: &[u8], id: &str) -> Vec<u8> {
    let p = rt.prepare_cached(wasm).await.expect("prepare_cached");
    let d = drive_to_completion(rt, &p, RunSpec::new(run_ref(id), "det", b""), Script::new()).await;
    match d.outcome {
        ActivationOutcome::Completed { output } => output.data,
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn runtime_cache_l2_hit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let rt = runtime(Some(dir.path()), 256);
    let wasm = common::guest_wasm("det-basic");
    let a = rt.prepare_cached(&wasm).await.expect("first");
    let b = rt.prepare_cached(&wasm).await.expect("second");
    assert!(Arc::ptr_eq(&a, &b), "L2 returns the same prepared module");
    let s = rt.cache_stats().expect("cache configured");
    assert_eq!(s.compiles, 1);
    assert_eq!(s.misses, 1);
    assert_eq!(s.hits, 0, "the second call never reached L1");
    assert_eq!(rt.instance_pre_len(), 1);
}

#[tokio::test]
async fn runtime_cache_l1_after_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wasm = common::guest_wasm("det-basic");
    let cold = {
        let rt = runtime(Some(dir.path()), 256);
        let out = det_output(&rt, &wasm, "restart").await;
        assert_eq!(rt.cache_stats().expect("stats").compiles, 1);
        out
    };
    // "Restart": a new Runtime (new engine) over the same cache dir.
    let rt = runtime(Some(dir.path()), 256);
    let warm = det_output(&rt, &wasm, "restart").await;
    let s = rt.cache_stats().expect("stats");
    assert_eq!(s.compiles, 0, "loaded from the authenticated artefact");
    assert_eq!(s.hits, 1);
    assert_eq!(
        warm, cold,
        "a run on the cached module completes identically"
    );
}

#[tokio::test]
async fn runtime_cache_rejects_forbidden_even_if_cached() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wasm = common::guest_wasm("wasi-forbidden");
    for round in 0..2 {
        // Round 1 compiles (and caches the artefact), round 2 loads it from L1:
        // validation runs after every load.
        let rt = runtime(Some(dir.path()), 256);
        match rt.prepare_cached(&wasm).await {
            Err(EngineError::Validation(v)) => {
                assert!(
                    v.iter().any(|m| m.contains("wasi:filesystem")),
                    "round {round}: {v:?}"
                );
            }
            other => panic!("round {round}: expected Validation, got {other:?}"),
        }
        assert_eq!(rt.instance_pre_len(), 0, "never enters L2");
        let s = rt.cache_stats().expect("stats");
        assert_eq!(
            (s.compiles, s.hits),
            if round == 0 { (1, 0) } else { (0, 1) }
        );
    }
    // Core modules are rejected before any compile.
    let rt = runtime(Some(dir.path()), 256);
    let core = b"\0asm\x01\0\0\0";
    assert!(matches!(
        rt.prepare_cached(core).await,
        Err(EngineError::Validation(_))
    ));
    assert_eq!(rt.cache_stats().expect("stats").misses, 0);
}

#[tokio::test]
async fn runtime_cache_l2_lru() {
    let dir = tempfile::tempdir().expect("tempdir");
    let rt = runtime(Some(dir.path()), 2);
    let smoke = common::guest_wasm("smoke");
    let det = common::guest_wasm("det-basic");
    let surface = common::guest_wasm("wit-surface");
    let first = rt.prepare_cached(&smoke).await.expect("smoke");
    rt.prepare_cached(&det).await.expect("det");
    rt.prepare_cached(&surface).await.expect("surface");
    assert_eq!(rt.instance_pre_len(), 2);
    let s = rt.cache_stats().expect("stats");
    assert_eq!((s.compiles, s.hits), (3, 0));

    // smoke was least recently used: re-prepared from L1 (hit, no compile).
    let again = rt.prepare_cached(&smoke).await.expect("smoke again");
    assert!(!Arc::ptr_eq(&first, &again), "evicted from L2");
    let s = rt.cache_stats().expect("stats");
    assert_eq!((s.compiles, s.hits), (3, 1));
    assert_eq!(rt.instance_pre_len(), 2);
    // surface (used more recently than det) is still in L2; det was evicted by smoke.
    rt.prepare_cached(&surface).await.expect("surface again");
    assert_eq!(rt.cache_stats().expect("stats").hits, 1);
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i]
}

#[tokio::test]
async fn runtime_warm_instantiate_p50() {
    const N: usize = 1_000;
    let dir = tempfile::tempdir().expect("tempdir");
    // Default config: pooling allocator (falls back to on-demand if it cannot reserve).
    let rt = runtime(Some(dir.path()), 256);
    println!("allocator: {:?}", rt.engine().pool_mode());
    let p = rt
        .prepare_cached(&common::guest_wasm("det-basic"))
        .await
        .expect("prepare");
    let spec = RunSpec::new(run_ref("warm"), "det", b"");
    let journal = vec![spec.run_started_event(&p.module_hash_hex)];
    // Warm up (first touches of the pool slot / lazy init).
    for _ in 0..20 {
        rt.instantiate_probe(&p, spec.run.clone(), journal.clone())
            .await
            .expect("warm-up");
    }
    let mut samples = Vec::with_capacity(N);
    for _ in 0..N {
        let journal = journal.clone();
        let t = Instant::now();
        rt.instantiate_probe(&p, spec.run.clone(), journal)
            .await
            .expect("instantiate");
        samples.push(t.elapsed());
    }
    samples.sort();
    let (p50, p99) = (percentile(&samples, 0.50), percentile(&samples, 0.99));
    println!("warm instantiate (production linker, HostState store): p50 {p50:?} p99 {p99:?}");
    assert!(p50 < Duration::from_millis(1), "p50 {p50:?} >= 1 ms");

    // NFR-2.4 context (print only): a whole activation of a one-step workflow is an
    // upper bound on the time to its first begin-step.
    let surface = rt
        .prepare_cached(&common::guest_wasm("wit-surface"))
        .await
        .expect("prepare surface");
    let mut acts = Vec::new();
    for i in 0..50 {
        let t = Instant::now();
        let d = drive_to_completion(
            &rt,
            &surface,
            RunSpec::new(run_ref(&format!("echo-{i}")), "echo-bytes", b""),
            Script::new(),
        )
        .await;
        acts.push(t.elapsed());
        assert!(matches!(d.outcome, ActivationOutcome::Completed { .. }));
    }
    acts.sort();
    println!(
        "one-step activation (upper bound for time to first begin-step): p50 {:?} p99 {:?}",
        percentile(&acts, 0.50),
        percentile(&acts, 0.99)
    );
}
