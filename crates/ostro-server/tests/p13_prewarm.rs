//! AOT pre-warm after a restart (13-14; D-P13-19, research Pitfall 9).
//!
//! `prewarm_compiles_referenced_modules_before_first_activation`: a run is started on a
//! queue that can never be dispatched here (`[workers] default_placement = "workers"`, no
//! worker connects), so it stays in the ready queue. After a restart the fresh runtime has
//! an empty `InstancePre` cache; the pre-warm task prepares the run's module from the blob
//! store (an L1 AOT cache hit on the persisted artefact, no recompile) while no activation
//! of the run ever happens. A module that no ready or running run references is not
//! prepared.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, guest_wasm, server_config, tmpfs_dir};
use ostro_server::config::PlacementSetting;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn sha_hex(b: &[u8]) -> String {
    Sha256::digest(b)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}

async fn put_module(s: &TestServer, body: &[u8]) {
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{}?language=rust", sha_hex(body))))
        .header("content-type", "application/wasm")
        .body(body.to_vec())
        .send()
        .await
        .expect("PUT module");
    let st = r.status();
    let v: Value = r.json().await.expect("json");
    assert!(st.is_success(), "upload: {st} {v}");
}

async fn run_status(s: &TestServer, run_id: &str) -> String {
    let r = s
        .http
        .get(s.url(&format!("/v1/runs/{run_id}")))
        .send()
        .await
        .expect("GET run");
    assert_eq!(r.status(), StatusCode::OK);
    let v: Value = r.json().await.expect("json");
    v["status"].as_str().unwrap_or_default().to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prewarm_compiles_referenced_modules_before_first_activation() {
    let dir = tmpfs_dir();
    let mut cfg = server_config(dir.path(), &ServerOpts::default());
    // Nothing can run here: every queue waits for a worker that never connects.
    cfg.workers.default_placement = PlacementSetting::Workers;
    let mut s = TestServer::start_cfg(cfg, Some(dir)).await;
    s.wait_ready(Duration::from_secs(30)).await;

    let smoke = guest_wasm("smoke");
    let unreferenced = guest_wasm("trivial-steps");
    put_module(&s, &smoke).await;
    put_module(&s, &unreferenced).await;
    let r = s
        .http
        .post(s.url("/v1/workflows/smoke/runs"))
        .json(&json!({ "input": {"hello": "prewarm"} }))
        .send()
        .await
        .expect("POST run");
    assert!(r.status().is_success(), "start: {}", r.status());
    let v: Value = r.json().await.expect("json");
    let run_id = v["run_id"].as_str().expect("run_id").to_owned();
    assert!(
        !matches!(
            run_status(&s, &run_id).await.as_str(),
            "completed" | "failed"
        ),
        "the run must stay queued"
    );

    s.restart().await;
    s.wait_ready(Duration::from_secs(30)).await;
    let rt = std::sync::Arc::clone(s.h().runtime());
    let smoke_sha: [u8; 32] = Sha256::digest(&smoke).into();
    let other_sha: [u8; 32] = Sha256::digest(&unreferenced).into();

    // Pre-warmed: the module is in L2 without any activation.
    let t0 = Instant::now();
    while rt.prepared(&smoke_sha).is_none() {
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "the referenced module was not pre-warmed within 30 s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let warmed_in = t0.elapsed();
    let stats = rt.cache_stats().expect("AOT cache configured");
    assert!(stats.hits >= 1, "persisted artefact loaded: {stats:?}");
    assert_eq!(stats.compiles, 0, "no recompile after restart: {stats:?}");
    assert!(
        s.hooks.jobs().iter().all(|j| j.run.to_string() != run_id),
        "no activation of the queued run"
    );
    assert!(
        !matches!(
            run_status(&s, &run_id).await.as_str(),
            "completed" | "failed"
        ),
        "the run is still queued"
    );
    // Only referenced modules are prepared.
    assert!(rt.prepared(&other_sha).is_none());
    eprintln!("pre-warm after restart: module in L2 after {warmed_in:?}, AOT {stats:?}");
    s.shutdown().await;
}
