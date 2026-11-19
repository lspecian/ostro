//! Plan 05-18 (QA-01): the `aot::write` failpoint. A failed AOT artefact write is not
//! fatal: the engine logs it, uses the in-memory compile, the activation completes, nothing
//! (not even a temp file) is left in the cache directory, and the next load recompiles.
//! Run with `--features failpoints -- --test-threads=1` (process-global fail registry).

mod common;

use std::path::Path;
use std::sync::Arc;

use ostro_engine::config::EngineConfig;
use ostro_engine::runtime::{Runtime, RuntimeOptions};
use ostro_engine::sink::{ActivationOutcome, RunRef};
use ostro_engine::testkit::{DEFAULT_ACCEPTED_AT, FakeClock, RunSpec, Script, drive_to_completion};

fn runtime(cache_dir: &Path) -> Runtime {
    Runtime::new(
        EngineConfig::default(),
        RuntimeOptions {
            clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
            token_key: [4; 32],
            cache_dir: Some(cache_dir.to_path_buf()),
            instance_pre_capacity: 16,
        },
    )
    .expect("runtime")
}

/// Prepare through the AOT cache and run `det` to completion.
async fn det_output(rt: &Runtime, wasm: &[u8]) -> Vec<u8> {
    let p = rt.prepare_cached(wasm).await.expect("prepare_cached");
    let run = RunRef {
        partition: 1,
        run_id: "aot-fp".to_string(),
    };
    let d = drive_to_completion(rt, &p, RunSpec::new(run, "det", b""), Script::new()).await;
    match d.outcome {
        ActivationOutcome::Completed { output } => output.data,
        other => panic!("activation did not complete: {other:?}"),
    }
}

/// Every entry of `<cache>/aot/` (artefacts and temp files).
fn aot_entries(cache: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(cache.join("aot"))
        .expect("aot dir")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}

async fn write_fault_is_not_fatal(action: &str) {
    let _scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().expect("tempdir");
    let wasm = common::guest_wasm("det-basic");

    // Reference output without any fault (separate cache dir).
    let ref_dir = tempfile::tempdir().expect("tempdir");
    let expected = det_output(&runtime(ref_dir.path()), &wasm).await;

    fail::cfg("aot::write", action).unwrap();
    {
        let rt = runtime(dir.path());
        let out = det_output(&rt, &wasm).await;
        assert_eq!(
            out, expected,
            "aot::write={action}: the activation succeeds"
        );
        let s = rt.cache_stats().expect("stats");
        assert_eq!((s.misses, s.compiles, s.hits), (1, 1, 0));
    }
    assert!(
        aot_entries(dir.path()).is_empty(),
        "aot::write={action}: no artefact and no temp file left: {:?}",
        aot_entries(dir.path())
    );

    // Restart while the fault persists: nothing cached, so it recompiles and still runs.
    {
        let rt = runtime(dir.path());
        assert_eq!(det_output(&rt, &wasm).await, expected);
        let s = rt.cache_stats().expect("stats");
        assert_eq!((s.misses, s.compiles, s.hits), (1, 1, 0), "recompiled");
    }
    fail::remove("aot::write");

    // Fault cleared: the next restart compiles and persists the artefact...
    {
        let rt = runtime(dir.path());
        assert_eq!(det_output(&rt, &wasm).await, expected);
        assert_eq!(rt.cache_stats().expect("stats").compiles, 1);
    }
    let entries = aot_entries(dir.path());
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert!(entries[0].ends_with(".cwasm"), "{entries:?}");
    // ...and the one after loads it (an authenticated hit, no compile).
    let rt = runtime(dir.path());
    assert_eq!(det_output(&rt, &wasm).await, expected);
    let s = rt.cache_stats().expect("stats");
    assert_eq!((s.hits, s.compiles, s.tamper_total), (1, 0, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aot_write_eio_recompiles_and_activation_succeeds() {
    write_fault_is_not_fatal("return").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aot_write_enospc_partial_write_leaves_nothing_behind() {
    write_fault_is_not_fatal("return(enospc)").await;
}
