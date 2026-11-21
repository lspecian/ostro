//! Module fetch + AOT cache under `--cache-dir` (plan 07-07, WORK-06): verified module
//! fetch, corrupted chunk → clear failure and nothing cached, second start = cache hit,
//! tampered artefact = HMAC mismatch → recompile.

mod common;

use std::path::Path;
use std::time::Duration;

use common::*;
use ostro_worker::WorkerEvent;
use ostro_worker_proto::WireOutcome;
use serde_json::json;

const T: Duration = Duration::from_secs(60);

/// AOT artefacts (`<cache>/aot/*.cwasm`).
fn artefacts(dir: &Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(dir.join("aot"))
        .map(|rd| {
            rd.filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().is_some_and(|x| x == "cwasm"))
                .collect()
        })
        .unwrap_or_default()
}

/// Run one `long-journal {steps: 1}` activation of run `n` on a fresh worker; returns the
/// worker's AOT cache stats after it.
async fn run_once(h: &Harness, n: u8) -> ostro_engine::cache::CacheStatsSnapshot {
    let id = run_id(n);
    let sha = h.seed("long-journal", "long-journal", &id, json!({"steps": 1}));
    let mut w = spawn_worker(h.cfg());
    w.wait_event(T, "connected", |e| {
        matches!(e, WorkerEvent::Connected { .. })
    })
    .await;
    assert!(h.st().send(activate(&id, 1, &sha, 1)));
    h.st()
        .wait_for(T, "Done", |st| !st.dones(&id).is_empty())
        .await;
    let d = h.st().dones(&id)[0].clone();
    assert!(
        matches!(outcome(&d), WireOutcome::Completed { .. }),
        "{:?}",
        outcome(&d)
    );
    let stats = w.worker.runtime().unwrap().cache_stats().unwrap();
    w.stop().await.unwrap();
    stats
}

#[tokio::test]
async fn second_start_hits_the_aot_cache() {
    let h = Harness::new(FakeConfig::default()).await;
    let first = run_once(&h, 1).await;
    assert_eq!(first.compiles, 1, "{first:?}");
    let cache = h.dir.path().join("cache");
    assert_eq!(artefacts(&cache).len(), 1);
    assert!(cache.join("aot.key").is_file(), "node-local HMAC key");
    let fetches = h
        .st()
        .blob_fetches
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(fetches >= 1);

    let second = run_once(&h, 2).await;
    assert_eq!(second.compiles, 0, "no compile on restart: {second:?}");
    assert!(second.hits >= 1, "{second:?}");
}

#[tokio::test]
async fn corrupted_module_chunk_fails_clearly_and_caches_nothing() {
    let h = Harness::new(FakeConfig {
        corrupt_blobs: true,
        blob_chunk: 64 * 1024,
        ..FakeConfig::default()
    })
    .await;
    let id = run_id(3);
    let sha = h.seed("long-journal", "long-journal", &id, json!({"steps": 1}));
    let mut w = spawn_worker(h.cfg());
    w.wait_event(T, "connected", |e| {
        matches!(e, WorkerEvent::Connected { .. })
    })
    .await;
    assert!(h.st().send(activate(&id, 1, &sha, 1)));
    let ev = w
        .wait_event(T, "activation failed", |e| {
            matches!(e, WorkerEvent::ActivationFailed { .. })
        })
        .await;
    let WorkerEvent::ActivationFailed { error, .. } = ev else {
        unreachable!()
    };
    assert!(error.contains("sha256 mismatch"), "{error}");
    h.st()
        .wait_for(T, "Done", |st| !st.dones(&id).is_empty())
        .await;
    assert!(matches!(
        outcome(&h.st().dones(&id)[0]),
        WireOutcome::Evicted { .. }
    ));
    let rt = w.worker.runtime().unwrap();
    assert_eq!(rt.instance_pre_len(), 0);
    assert_eq!(rt.cache_stats().unwrap().compiles, 0);
    assert!(artefacts(&h.dir.path().join("cache")).is_empty());
    assert_eq!(h.st().journal_len(&id), 1, "nothing appended");
    w.stop().await.unwrap();
}

#[tokio::test]
async fn tampered_artefact_is_recompiled() {
    let h = Harness::new(FakeConfig::default()).await;
    let first = run_once(&h, 4).await;
    assert_eq!(first.compiles, 1);
    let cache = h.dir.path().join("cache");
    for p in artefacts(&cache) {
        let mut b = std::fs::read(&p).unwrap();
        let mid = b.len() / 2;
        b[mid] ^= 0x5a;
        std::fs::write(&p, b).unwrap();
    }
    let second = run_once(&h, 5).await;
    assert!(second.tamper_total >= 1, "{second:?}");
    assert_eq!(second.compiles, 1, "recompiled: {second:?}");
    assert_eq!(artefacts(&cache).len(), 1, "fresh artefact written");
}
