//! Binary-level worker smoke (plan 07-16, WORK-01/05): a real `ostro server` (self-signed
//! TLS, pinned ports) and a real `ostro worker` process.
//!
//! - `smoke`: the worker joins with a `wkr_` token and the CA pin, runs 3 `sdk-basic` runs
//!   of a `workers` queue end to end, is listed by `GET /v1/workers`, the demand endpoint
//!   answers the D-W12 shape and drops to 0 once the runs complete, and the worker logs
//!   no ERROR line.
//! - `restart_resume` (B5): the server is SIGKILLed mid-run and restarted on the same
//!   data dir and ports; the worker reconnects and every run completes exactly once.
#![cfg(unix)]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p7.rs"]
mod p7;

use std::time::{Duration, Instant};

use common::{events_of, step_completions};
use p7::{P7_T, P7Cluster, P7Opts, WorkerOpts};
use serde_json::json;

/// Spawned servers are heavy: one cluster at a time per test process.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn log_opts() -> P7Opts {
    P7Opts {
        // `remote activate run=..` is logged at info: proof the run went to the worker.
        env: vec![("RUST_LOG".into(), "info".into())],
        ..P7Opts::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn smoke() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let c = P7Cluster::start(log_opts()).await;
    let started = t0.elapsed();
    assert_eq!(c.ready.addr.port(), c.api_port, "pinned api port");
    assert_eq!(c.ready.fingerprint.len(), 64);
    assert!(c.token.starts_with("wkr_"), "join token");
    assert!(c.ca_hash.starts_with("sha256:"), "{}", c.ca_hash);

    let w = c.spawn_worker(WorkerOpts::new("smoke-w1", 2).label("pool", "heavy"));
    let joined = t0.elapsed();
    let ws = c.wait_workers(1).await;
    assert_eq!(ws[0]["worker_id"], "smoke-w1", "{ws:?}");
    assert_eq!(ws[0]["labels"], json!({"pool": "heavy"}));
    assert_eq!(ws[0]["slots"], 2);

    c.api.deploy("sdk-basic").await;
    let flow = c
        .api
        .set_flow(
            "heavy",
            json!({"placement": "workers", "selector": ["pool=heavy"]}),
        )
        .await;
    assert_eq!(flow["placement"], "workers", "{flow}");

    let mut ids = Vec::new();
    for i in 0..3 {
        ids.push(
            c.api
                .start(
                    "sdk-basic",
                    json!({"input": {"name": format!("w{i}"), "n": 3}, "queue": "heavy"}),
                )
                .await,
        );
    }
    let t_runs = Instant::now();
    for id in &ids {
        let r = c.api.wait_terminal(id, P7_T).await;
        assert_eq!(r["status"], "completed", "{r}");
        let h = c.api.history(id).await;
        assert_eq!(events_of(&h, "run_completed").len(), 1);
    }
    let runs = t_runs.elapsed();
    // Every run was activated on the worker (the leader logs each remote activation).
    let server_out = c.server().output();
    for id in &ids {
        assert!(
            server_out.contains(&format!("remote activate run={id}")),
            "run {id} not activated remotely:\n{server_out}"
        );
    }

    // Demand: the D-W12 document, back to 0 once the queue is drained.
    let deadline = Instant::now() + Duration::from_secs(20);
    let d = loop {
        let d = c.api.demand("heavy").await;
        if d["demand"] == 0 && d["running_on_workers"] == 0 {
            break d;
        }
        assert!(Instant::now() < deadline, "demand did not drop to 0: {d}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    for k in [
        "queue",
        "demand",
        "backlog",
        "running_on_workers",
        "placement",
        "spilling",
        "workers",
        "worker_slots_free",
    ] {
        assert!(d.get(k).is_some(), "demand lacks {k}: {d}");
    }
    assert_eq!(d["queue"], "heavy");
    assert_eq!(d["placement"], "workers");
    assert_eq!(d["workers"], 1, "{d}");
    assert_eq!(d["backlog"], 0, "{d}");

    let errors = w.error_lines();
    assert!(
        errors.is_empty(),
        "worker ERROR lines:\n{}",
        errors.join("\n")
    );
    eprintln!(
        "worker_smoke::smoke: server ready {started:?}, worker ready {joined:?}, \
         3 runs {runs:?}, total {:?}, bind retries {}",
        t0.elapsed(),
        c.bind_retries
    );
    drop(w);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_resume() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let mut c = P7Cluster::start(P7Opts {
        lease_ttl: Some(Duration::from_secs(6)),
        ..log_opts()
    })
    .await;
    let (api_port, cluster_port) = (c.api_port, c.cluster_port);
    let mut w = c.spawn_worker(WorkerOpts::new("resume-w1", 2));
    c.wait_workers(1).await;
    c.api.deploy("cpu-burn").await;
    c.api
        .set_flow("heavy", json!({"placement": "workers"}))
        .await;

    let mut ids = Vec::new();
    for _ in 0..4 {
        ids.push(
            c.api
                .start("cpu-burn", json!({"input": {"ms": 1500}, "queue": "heavy"}))
                .await,
        );
    }
    // Kill the server while the worker is executing.
    let deadline = Instant::now() + P7_T;
    loop {
        let d = c.api.demand("heavy").await;
        if d["running_on_workers"].as_u64().unwrap_or(0) > 0 {
            break;
        }
        assert!(Instant::now() < deadline, "nothing ran on the worker: {d}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let t_kill = Instant::now();
    c.kill9_server();
    c.restart_server().await;
    let restarted = t_kill.elapsed();
    assert_eq!(
        (c.api_port, c.cluster_port),
        (api_port, cluster_port),
        "ports reused"
    );
    assert_eq!(c.ready.addr.port(), api_port);

    // The same worker process reconnects (no new process, same endpoints).
    c.wait_workers(1).await;
    let reconnected = t_kill.elapsed();
    assert!(w.try_exit().is_none(), "worker exited:\n{}", w.output());

    for id in &ids {
        let r = c.api.wait_terminal(id, P7_T).await;
        assert_eq!(r["status"], "completed", "{r}");
        let h = c.api.history(id).await;
        assert_eq!(events_of(&h, "run_completed").len(), 1, "{id}");
        assert_eq!(step_completions(&h, "burn"), 1, "{id}: burn completed once");
    }
    // After the restart work still went to the worker.
    let out = c.server().output();
    assert!(
        out.contains("remote activate run="),
        "no remote activation after restart:\n{out}"
    );
    eprintln!(
        "worker_smoke::restart_resume: restart {restarted:?}, worker back {reconnected:?}, \
         total {:?}, bind retries {}",
        t0.elapsed(),
        c.bind_retries
    );
    drop(w);
}
