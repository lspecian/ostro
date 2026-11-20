//! OBS-02 base (plan 08-04): `GET /metrics` serves the Prometheus text exposition of the
//! existing `ostro_*` series, with `_bucket` lines for `*_seconds` histograms and no
//! `run_id` label.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, guest_wasm};
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::sleep;

const LIMIT: Duration = Duration::from_secs(30);

async fn deploy_and_run(s: &TestServer) -> String {
    let wasm = guest_wasm("sdk-basic");
    let sha: String = Sha256::digest(&wasm)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{sha}")))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "deploy: {}", r.status());
    let r = s
        .http
        .post(s.url("/v1/workflows/sdk-basic/runs"))
        .json(&json!({"input": {"name": "m", "n": 2}}))
        .send()
        .await
        .expect("POST run");
    assert_eq!(r.status(), StatusCode::CREATED);
    let body: Value = r.json().await.expect("json");
    let id = body["run_id"].as_str().expect("run_id").to_owned();
    let t0 = Instant::now();
    loop {
        let run: Value = s
            .http
            .get(s.url(&format!("/v1/runs/{id}")))
            .send()
            .await
            .expect("GET run")
            .json()
            .await
            .expect("json");
        if run["status"] == "completed" {
            return id;
        }
        assert!(t0.elapsed() < LIMIT, "run not completed: {run}");
        sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_endpoint_serves_existing_series_with_buckets() {
    let mut s = TestServer::start(ServerOpts::default()).await;
    s.wait_ready(Duration::from_secs(5)).await;
    let run_id = deploy_and_run(&s).await;

    let r = s
        .http
        .get(s.url("/metrics"))
        .send()
        .await
        .expect("GET /metrics");
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(
        r.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("text/plain; version=0.0.4")
    );
    let body = r.text().await.expect("body");
    for needle in ["ostro_wal_", "ostro_apply_", "_bucket{"] {
        assert!(body.contains(needle), "{needle} missing in scrape:\n{body}");
    }
    assert!(
        body.lines()
            .any(|l| l.starts_with("ostro_apply_batch_seconds_bucket{")),
        "the _seconds histogram renders buckets:\n{body}"
    );
    // P8-D15: run ids are never labels (nor values).
    assert!(!body.contains("run_id"), "run_id label in scrape:\n{body}");
    assert!(!body.contains(&run_id), "a run id leaked into the scrape");

    // The recorder is installed once per process; a second handle renders the same data.
    let h1 = ostro_server::telemetry::metrics_handle().expect("recorder installed");
    let h2 = ostro_server::telemetry::metrics_handle().expect("same recorder");
    assert!(h1.render().contains("ostro_apply_"));
    assert!(h2.render().contains("ostro_apply_"));

    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_route_is_not_served_when_disabled() {
    let mut s = TestServer::start_with(ServerOpts::default(), |c| {
        c.telemetry.metrics = false;
    })
    .await;
    s.wait_ready(Duration::from_secs(5)).await;
    let r = s
        .http
        .get(s.url("/metrics"))
        .send()
        .await
        .expect("GET /metrics");
    // Unknown paths fall through to tonic's gRPC fallback (HTTP 200, grpc-status 12
    // UNIMPLEMENTED), never the exposition.
    assert_eq!(
        r.headers().get("grpc-status").and_then(|v| v.to_str().ok()),
        Some("12"),
        "no /metrics route: {:?}",
        r.headers()
    );
    assert_ne!(
        r.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("text/plain; version=0.0.4")
    );
    let body = r.text().await.expect("body");
    assert!(!body.contains("ostro_"), "{body}");
    s.shutdown().await;
}
