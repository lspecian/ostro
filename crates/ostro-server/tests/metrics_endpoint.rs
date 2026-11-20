//! OBS-02 base (plan 08-04): `GET /metrics` serves the Prometheus text exposition of the
//! existing `ostro_*` series, with `_bucket` lines for `*_seconds` histograms and no
//! `run_id` label. OBS-02 coverage (plan 08-16): every required family is scraped after
//! a completed multi-step run and a dead-lettered (nondeterministic) run, and every
//! sample's labels pass a cardinality audit.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, guest_wasm};
use ostro_partition::store::values::RunStatus;
use ostro_types::RunId;
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

/// The families OBS-02 requires (08-16 Task 2): each must have at least one sample line.
const REQUIRED: &[&str] = &[
    "ostro_queue_ready",
    "ostro_queue_lag_seconds_bucket",
    "ostro_partition_apply_lag",
    "ostro_partition_hw_lag",
    "ostro_isr_size",
    "ostro_wal_log_start",
    "ostro_wal_log_end",
    "ostro_runs_started_total",
    "ostro_runs_completed_total",
    "ostro_steps_total",
    "ostro_step_duration_seconds_bucket",
    "ostro_replay_events_total",
    "ostro_replay_duration_seconds_bucket",
    "ostro_fuel_consumed_total",
    "ostro_instance_memory_bytes",
    "ostro_wasm_memory_budget_bytes",
    "ostro_parked_instances",
    "ostro_dlq_total",
    "ostro_raft_term",
    "ostro_raft_commit_index",
    "ostro_raft_applied_index",
    "ostro_raft_is_leader",
];

/// Label names that would make a series unbounded (P8-D15).
const FORBIDDEN_LABELS: &[&str] = &["run_id", "idempotency_key", "key", "request_id"];

/// `(name, labels)` of one exposition sample line; `None` for comments and blanks.
fn parse_sample(line: &str) -> Option<(String, Vec<(String, String)>)> {
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let Some(open) = line.find('{') else {
        let name = line.split_whitespace().next()?.to_owned();
        return Some((name, Vec::new()));
    };
    let name = line[..open].to_owned();
    let mut labels = Vec::new();
    let mut chars = line[open + 1..].chars().peekable();
    loop {
        let mut k = String::new();
        for c in chars.by_ref() {
            if c == '=' {
                break;
            }
            k.push(c);
        }
        assert_eq!(chars.next(), Some('"'), "label value quote in {line}");
        let mut v = String::new();
        while let Some(c) = chars.next() {
            match c {
                '\\' => v.push(chars.next().expect("escape")),
                '"' => break,
                c => v.push(c),
            }
        }
        labels.push((k.trim_start_matches(',').to_owned(), v));
        match chars.next() {
            Some(',') if chars.peek() != Some(&'}') => continue,
            Some(',') | Some('}') => break,
            other => panic!("unexpected {other:?} in {line}"),
        }
    }
    Some((name, labels))
}

async fn scrape(s: &TestServer) -> String {
    let r = s
        .http
        .get(s.url("/metrics"))
        .send()
        .await
        .expect("GET /metrics");
    assert_eq!(r.status(), StatusCode::OK);
    r.text().await.expect("body")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_endpoint_covers_obs02_families_with_bounded_labels() {
    // Short hot wait: sleeps evict, so re-activations replay the journal.
    let mut s = TestServer::start_with(ServerOpts::default(), |c| {
        c.engine.hot_wait = Duration::from_millis(50).into();
    })
    .await;
    s.wait_ready(Duration::from_secs(10)).await;

    // A completed multi-step run (greet, add, and sleeps that evict and replay).
    let ok_run = deploy_and_run(&s).await;

    // A dead-lettered run: patch-v1 journals step A + a 2 s sleep; replaying that journal
    // on patch-v3 (which always runs step C there) is nondeterminism.
    s.deploy_guest("patch-v1", None).await;
    let v3 = s.deploy_guest("patch-v3", Some("beta")).await;
    let bad = s.start_rest("patch-demo", json!({"input": null})).await;
    s.wait_record(&bad, LIMIT, |r| {
        r.next_seq > 0 && r.status == RunStatus::Scheduled
    })
    .await;
    s.hooks
        .module_override
        .set(Some(Arc::new(move |r: &RunId| (*r == bad).then_some(v3))));
    let rec = s.wait_record(&bad, LIMIT, |r| r.status.is_terminal()).await;
    s.hooks.module_override.set(None);
    assert_eq!(rec.status, RunStatus::Failed, "the v3 replay must diverge");
    assert!(rec.resumable, "nondeterminism is an executor-level failure");

    // The run consumers and the poller publish asynchronously: wait for every family.
    let t0 = Instant::now();
    let body = loop {
        let body = scrape(&s).await;
        let missing: Vec<&str> = REQUIRED
            .iter()
            .copied()
            .filter(|f| {
                !body.lines().any(|l| {
                    l.strip_prefix(f)
                        .is_some_and(|rest| rest.starts_with('{') || rest.starts_with(' '))
                })
            })
            .collect();
        if missing.is_empty() {
            break body;
        }
        assert!(
            t0.elapsed() < LIMIT,
            "missing families {missing:?} in scrape:\n{body}"
        );
        sleep(Duration::from_millis(100)).await;
    };

    // Specific values: the dead-lettered run is counted under its reason, the completed
    // run under its workflow and status, and the lone node leads the metadata group.
    assert!(
        body.lines()
            .any(|l| l.starts_with("ostro_dlq_total{reason=\"nondeterminism\"}")),
        "{body}"
    );
    assert!(
        body.lines()
            .any(|l| l.starts_with("ostro_runs_completed_total{")
                && l.contains("workflow=\"sdk-basic\"")
                && l.contains("status=\"completed\"")),
        "{body}"
    );
    assert!(
        body.lines().any(|l| l.starts_with("ostro_steps_total{")
            && l.contains("workflow=\"sdk-basic\"")
            && l.contains("outcome=\"ok\"")),
        "{body}"
    );
    assert!(
        body.lines().any(|l| l == "ostro_raft_is_leader 1"),
        "{body}"
    );
    assert!(
        body.lines()
            .any(|l| l.starts_with("ostro_isr_size{partition=\"0\"} 1")),
        "{body}"
    );

    // Label audit: no unbounded label name, no long value, no run id anywhere.
    let mut samples = 0;
    for line in body.lines() {
        let Some((name, labels)) = parse_sample(line) else {
            continue;
        };
        samples += 1;
        for (k, v) in &labels {
            assert!(
                !FORBIDDEN_LABELS.contains(&k.as_str()),
                "unbounded label {k} on {name}: {line}"
            );
            assert!(
                v.len() <= 128,
                "label value over 128 bytes on {name}: {line}"
            );
        }
    }
    assert!(samples > REQUIRED.len(), "parsed {samples} samples");
    assert!(!body.contains(&ok_run), "a run id leaked into the scrape");
    assert!(
        !body.contains(&bad.to_string()),
        "a run id leaked into the scrape"
    );

    s.shutdown().await;
}

#[test]
fn sample_parser_handles_labels_and_escapes() {
    assert_eq!(parse_sample("# HELP x y"), None);
    assert_eq!(
        parse_sample("ostro_raft_term 3"),
        Some(("ostro_raft_term".into(), vec![]))
    );
    let (n, l) = parse_sample(r#"m{a="1",b="x\"y,z"} 2"#).expect("sample");
    assert_eq!(n, "m");
    assert_eq!(
        l,
        vec![("a".into(), "1".into()), ("b".into(), "x\"y,z".into())]
    );
}
