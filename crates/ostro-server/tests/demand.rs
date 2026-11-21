//! Worker demand (plan 07-16, D-W12, WORK-05): `workers::demand` over an in-process
//! server.
//!
//! - real runs on a `workers` queue: scale from zero (no worker connected), the queue
//!   concurrency cap, a connected 2-slot worker running 2 activations, a paused queue,
//!   the 1 s gauge equal to the snapshot, the REST endpoint;
//! - `auto` (spilling or not) and `embedded` queues over synthetic gauge rows (a partition
//!   id no dispatcher owns), with a forced spill state;
//! - no `ostro_queue_worker_demand` value setter outside `workers/demand.rs`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::path::Path;
use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, guest_wasm};
use ostro_partition::QueueGauge;
use ostro_server::workers::demand;
use ostro_types::PartitionId;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::sleep;

const LIMIT: Duration = Duration::from_secs(60);

async fn deploy(s: &TestServer, guest: &str) {
    let wasm = guest_wasm(guest);
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
    assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
}

async fn send(
    s: &TestServer,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = s.http.request(method, s.url(path));
    if let Some(b) = body {
        req = req.json(&b);
    }
    let r = req.send().await.expect("request");
    let st = r.status();
    let b = r.bytes().await.expect("body");
    let v = if b.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&b).expect("json")
    };
    (st, v)
}

async fn get(s: &TestServer, path: &str) -> Value {
    let (st, v) = send(s, reqwest::Method::GET, path, None).await;
    assert_eq!(st, StatusCode::OK, "GET {path}: {v}");
    v
}

/// Read-modify-PUT of the queue flow.
async fn set_flow(s: &TestServer, q: &str, fields: Value) {
    let cur = get(s, &format!("/v1/queues/{q}/flow")).await;
    let mut body = fields;
    body["expected_version"] = cur["version"].clone();
    let (st, v) = send(
        s,
        reqwest::Method::PUT,
        &format!("/v1/queues/{q}/flow"),
        Some(body),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "PUT flow {q}: {v}");
}

async fn start(s: &TestServer, workflow: &str, body: Value) -> String {
    let (st, b) = send(
        s,
        reqwest::Method::POST,
        &format!("/v1/workflows/{workflow}/runs"),
        Some(body),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{b}");
    b["run_id"].as_str().expect("run_id").to_owned()
}

/// Poll the demand endpoint until `pred` holds.
async fn wait_demand(s: &TestServer, q: &str, what: &str, pred: impl Fn(&Value) -> bool) -> Value {
    let t = Instant::now();
    loop {
        let v = get(s, &format!("/v1/queues/{q}/demand")).await;
        if pred(&v) {
            return v;
        }
        assert!(t.elapsed() < LIMIT, "{what}: last demand {v}");
        sleep(Duration::from_millis(50)).await;
    }
}

/// `name{queue="q"}` from the Prometheus exposition.
async fn scrape(s: &TestServer, name: &str, q: &str) -> Option<f64> {
    let body = s
        .http
        .get(s.url("/metrics"))
        .send()
        .await
        .expect("GET /metrics")
        .text()
        .await
        .expect("metrics body");
    let prefix = format!("{name}{{queue=\"{q}\"}} ");
    body.lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .map(|v| v.trim().parse().expect("sample value"))
}

/// Wait until the published gauge equals `want` (the ticker runs every second).
async fn wait_gauge(s: &TestServer, name: &str, q: &str, want: u64) {
    let t = Instant::now();
    loop {
        let got = scrape(s, name, q).await;
        if got == Some(want as f64) {
            return;
        }
        if t.elapsed() >= Duration::from_secs(10) {
            let d = get(s, &format!("/v1/queues/{q}/demand")).await;
            panic!("{name}{{queue={q}}} = {got:?}, want {want}; endpoint {d}");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

struct InProcWorker {
    shutdown: tokio_util::sync::CancellationToken,
    task: tokio::task::JoinHandle<Result<(), ostro_worker::WorkerError>>,
    _cache: tempfile::TempDir,
}

impl InProcWorker {
    async fn stop(self) {
        self.shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(20), self.task).await;
    }
}

/// An in-process `ostro_worker` joined through the server's REST route.
async fn spawn_worker(
    s: &TestServer,
    id: &str,
    slots: u32,
    labels: &[(&str, &str)],
) -> InProcWorker {
    let (st, created) = send(
        s,
        reqwest::Method::POST,
        "/v1/workers/tokens",
        Some(json!({})),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{created}");
    let ca_hash = created["join_command"]
        .as_str()
        .expect("cmd")
        .split("--ca-hash ")
        .nth(1)
        .expect("ca hash")
        .to_owned();
    let cache = tempfile::tempdir().expect("cache");
    let cfg = ostro_worker::WorkerConfig {
        join: Some(s.url("")),
        token: Some(ostro_worker::JoinToken::new(
            created["token"].as_str().expect("token"),
        )),
        ca_hash: Some(ca_hash),
        labels: labels
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
        slots,
        cache_dir: cache.path().to_path_buf(),
        worker_id: Some(id.into()),
        reconnect_max_ms: 200,
        ..ostro_worker::WorkerConfig::default()
    };
    let worker = std::sync::Arc::new(ostro_worker::Worker::new(cfg).expect("worker config"));
    let mut events = worker.subscribe();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let task = tokio::spawn({
        let w = std::sync::Arc::clone(&worker);
        let sd = shutdown.clone();
        async move { w.run(sd).await }
    });
    tokio::time::timeout(LIMIT, async {
        loop {
            match events.recv().await {
                Ok(ostro_worker::WorkerEvent::Connected { .. }) => return,
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(e) => panic!("worker events closed: {e}"),
            }
        }
    })
    .await
    .expect("worker connected");
    InProcWorker {
        shutdown,
        task,
        _cache: cache,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workers_queue_demand_from_real_runs() {
    let t0 = Instant::now();
    // A sleep shorter than the hot wait (and the 60 s activation timeout) keeps the
    // activation parked hot, i.e. leased, on its worker.
    let mut s = TestServer::start_with(ServerOpts::default(), |c| {
        c.engine.hot_wait = Duration::from_secs(55).into();
    })
    .await;
    s.wait_ready(Duration::from_secs(10)).await;
    deploy(&s, "timeouts").await;
    set_flow(&s, "heavy", json!({"placement": "workers"})).await;

    // Scale from zero: 10 ready runs, no worker -> demand 10.
    for _ in 0..10 {
        start(
            &s,
            "timeouts",
            json!({"input": {"mode": "sleep_ms", "n": 50_000}, "queue": "heavy"}),
        )
        .await;
    }
    let v = wait_demand(&s, "heavy", "scale from zero", |v| v["demand"] == 10).await;
    assert_eq!(v["queue"], "heavy");
    assert_eq!(v["backlog"], 10, "{v}");
    assert_eq!(v["running_on_workers"], 0, "{v}");
    assert_eq!(v["placement"], "workers", "{v}");
    assert_eq!(v["spilling"], false, "{v}");
    assert_eq!(v["workers"], 0, "{v}");
    assert_eq!(v["worker_slots_free"], 0, "{v}");
    // The gauge after a tick equals the snapshot.
    wait_gauge(&s, "ostro_queue_worker_demand", "heavy", 10).await;
    wait_gauge(&s, "ostro_queue_worker_backlog", "heavy", 10).await;
    let snap = demand::snapshot(s.h().p7(), "heavy");
    assert_eq!((snap.demand, snap.backlog), (10, 10), "{snap:?}");

    // Queue concurrency 4 -> demand 4.
    set_flow(
        &s,
        "heavy",
        json!({"placement": "workers", "concurrency": 4}),
    )
    .await;
    wait_demand(&s, "heavy", "concurrency cap", |v| v["demand"] == 4).await;
    wait_gauge(&s, "ostro_queue_worker_demand", "heavy", 4).await;

    // A 2-slot worker runs 2 -> 2 running + min(8 ready, 4 - 2 headroom) = 4.
    let w = spawn_worker(&s, "w-heavy", 2, &[("zone", "a")]).await;
    let v = wait_demand(&s, "heavy", "worker running 2", |v| {
        v["running_on_workers"] == 2 && v["backlog"] == 8
    })
    .await;
    assert_eq!(v["demand"], 4, "{v}");
    assert_eq!(v["workers"], 1, "{v}");
    assert_eq!(v["worker_slots_free"], 0, "{v}");
    wait_gauge(&s, "ostro_queue_worker_demand", "heavy", 4).await;

    // A selector the worker does not match: not counted as a worker of the queue.
    set_flow(
        &s,
        "heavy",
        json!({"placement": "workers", "concurrency": 4, "selector": ["zone=b"]}),
    )
    .await;
    wait_demand(&s, "heavy", "selector mismatch", |v| v["workers"] == 0).await;

    // Paused queue -> running only.
    let (st, v) = send(
        &s,
        reqwest::Method::POST,
        "/v1/queues/heavy/pause",
        Some(json!({})),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let v = wait_demand(&s, "heavy", "paused", |v| v["demand"] == 2).await;
    assert_eq!(v["running_on_workers"], 2, "{v}");
    wait_gauge(&s, "ostro_queue_worker_demand", "heavy", 2).await;

    w.stop().await;
    eprintln!("workers_queue_demand_from_real_runs: {:?}", t0.elapsed());
    s.shutdown().await;
}

fn gauge(ready_unblocked: u64, running_embedded: u64, running_on_workers: u64) -> QueueGauge {
    QueueGauge {
        ready: ready_unblocked,
        ready_unblocked,
        oldest_ready_at_ms: None,
        running_embedded,
        running_on_workers,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_and_embedded_queues() {
    let t0 = Instant::now();
    let mut s = TestServer::start_with(ServerOpts::default(), |c| {
        c.executor.slots = 2;
    })
    .await;
    s.wait_ready(Duration::from_secs(10)).await;
    let p7 = std::sync::Arc::clone(s.h().p7());
    // A partition id no dispatcher owns: these rows are only ours.
    let fake = PartitionId(900);

    // auto, not spilling: embedded only -> 0 (no worker runs yet).
    set_flow(&s, "syn-auto", json!({"placement": "auto"})).await;
    p7.gauges.set(fake, "syn-auto", gauge(10, 2, 0));
    let v = wait_demand(&s, "syn-auto", "auto calm", |v| v["placement"] == "auto").await;
    assert_eq!(
        (v["demand"].as_u64(), v["backlog"].as_u64()),
        (Some(0), Some(0)),
        "{v}"
    );
    assert_eq!(v["spilling"], false);
    wait_gauge(&s, "ostro_queue_worker_demand", "syn-auto", 0).await;

    // Spilling: the backlog counts, minus the free embedded slots (2 idle slots).
    assert!(p7.spill.set("syn-auto", true));
    let v = get(&s, "/v1/queues/syn-auto/demand").await;
    assert_eq!(v["spilling"], true, "{v}");
    assert_eq!(v["backlog"], 10, "{v}");
    assert_eq!(v["demand"], 8, "{v}");
    wait_gauge(&s, "ostro_queue_worker_demand", "syn-auto", 8).await;
    wait_gauge(&s, "ostro_queue_worker_backlog", "syn-auto", 10).await;
    // Runs on workers add to it.
    p7.gauges.set(fake, "syn-auto", gauge(10, 2, 3));
    assert_eq!(demand::snapshot(&p7, "syn-auto").demand, 11);
    // Spill over: the runs still on workers keep demand up until they finish.
    assert!(p7.spill.set("syn-auto", false));
    let d = demand::snapshot(&p7, "syn-auto");
    assert_eq!(
        (d.demand, d.backlog, d.running_on_workers),
        (3, 0, 3),
        "{d:?}"
    );

    // embedded (explicit and the default placement): 0.
    set_flow(&s, "syn-emb", json!({"placement": "embedded"})).await;
    p7.gauges.set(fake, "syn-emb", gauge(10, 2, 0));
    p7.gauges.set(fake, "syn-default", gauge(5, 1, 0));
    let v = wait_demand(&s, "syn-emb", "embedded", |v| v["placement"] == "embedded").await;
    assert_eq!(v["demand"], 0, "{v}");
    let v = get(&s, "/v1/queues/syn-default/demand").await;
    assert_eq!(
        (v["placement"].as_str(), v["demand"].as_u64()),
        (Some("embedded"), Some(0)),
        "{v}"
    );
    wait_gauge(&s, "ostro_queue_worker_demand", "syn-emb", 0).await;

    // A queue whose gauges vanish is published one last time as 0.
    set_flow(&s, "syn-w", json!({"placement": "workers"})).await;
    p7.gauges.set(fake, "syn-w", gauge(6, 0, 0));
    wait_gauge(&s, "ostro_queue_worker_demand", "syn-w", 6).await;
    p7.gauges.remove_partition(fake);
    wait_gauge(&s, "ostro_queue_worker_demand", "syn-w", 0).await;

    // Node totals are exported too.
    let body = s
        .http
        .get(s.url("/metrics"))
        .send()
        .await
        .expect("metrics")
        .text()
        .await
        .expect("body");
    for name in ["ostro_workers_connected", "ostro_worker_slots_free"] {
        assert!(
            body.lines().any(|l| l.starts_with(name)),
            "{name} missing from /metrics"
        );
    }
    eprintln!("auto_and_embedded_queues: {:?}", t0.elapsed());
    s.shutdown().await;
}

fn rs_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for e in std::fs::read_dir(dir).expect("read_dir") {
        let p = e.expect("entry").path();
        if p.is_dir() {
            if p.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            rs_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// Phase 7 owns the demand value: no `gauge!(...ostro_queue_worker_demand...)` setter
/// exists outside `workers/demand.rs` (describing it elsewhere is fine; tests read it).
#[test]
fn no_foreign_demand_setter() {
    let crates = common::repo_root().join("crates");
    let mut files = Vec::new();
    rs_files(&crates, &mut files);
    let mut offenders = Vec::new();
    for f in files {
        let rel = f
            .strip_prefix(&crates)
            .expect("under crates")
            .to_string_lossy()
            .replace('\\', "/");
        if rel.ends_with("workers/demand.rs") || rel.contains("/tests/") {
            continue;
        }
        let text = std::fs::read_to_string(&f).expect("read");
        let lines: Vec<&str> = text.lines().collect();
        for (n, line) in lines.iter().enumerate() {
            // The macro call may wrap: look at the line and the one before it.
            let prev = if n > 0 { lines[n - 1] } else { "" };
            let ctx = format!("{prev} {line}");
            if line.contains("ostro_queue_worker_demand")
                && !ctx.contains("describe_gauge")
                && (ctx.contains("gauge!") || line.contains(".set("))
            {
                offenders.push(format!("{rel}:{}: {line}", n + 1));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "foreign demand setters:\n{}",
        offenders.join("\n")
    );
}
