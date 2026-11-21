//! The Phase 7 control plane end to end (plan 07-14; OPS-01, DISP-03..05, WORK-01/05):
//! the in-process server answering the Phase 7 REST routes and gRPC services.
//!
//! - queue flow: GET/PUT round trip (selector, spill, rate specs) over both transports,
//!   validation 400, stale `expected_version` 412 / FAILED_PRECONDITION, Phase 5 policy
//!   fields preserved;
//! - queue and workflow pause stop new grants of a backlog and a resume dispatches it;
//! - run pause / resume (idempotent, 409 on terminal runs), the paused-run `/retry` alias,
//!   cancel and terminate while paused;
//! - worker administration: tokens (secret shown once), join, list, drain, demand.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, guest_wasm};
use ostro_partition::store::values::RunStatus;
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::queues_client::QueuesClient;
use ostro_proto::api::v1::runs_client::RunsClient;
use ostro_proto::api::v1::workflows_client::WorkflowsClient;
use ostro_types::RunId;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::sleep;

const LIMIT: Duration = Duration::from_secs(60);

async fn server() -> TestServer {
    // A short hot wait so durable sleeps evict (Scheduled) instead of parking hot.
    let s = TestServer::start_with(ServerOpts::default(), |c| {
        c.engine.hot_wait = Duration::from_millis(50).into();
    })
    .await;
    s.wait_ready(Duration::from_secs(10)).await;
    s
}

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

async fn post(s: &TestServer, path: &str, body: Value) -> (StatusCode, Value) {
    send(s, reqwest::Method::POST, path, Some(body)).await
}

async fn put(s: &TestServer, path: &str, body: Value) -> (StatusCode, Value) {
    send(s, reqwest::Method::PUT, path, Some(body)).await
}

async fn get(s: &TestServer, path: &str) -> (StatusCode, Value) {
    send(s, reqwest::Method::GET, path, None).await
}

async fn start(s: &TestServer, workflow: &str, body: Value) -> String {
    let (st, b) = post(s, &format!("/v1/workflows/{workflow}/runs"), body).await;
    assert_eq!(st, StatusCode::CREATED, "{b}");
    b["run_id"].as_str().expect("run_id").to_owned()
}

fn rid(id: &str) -> RunId {
    id.parse().expect("run id")
}

fn granted(s: &TestServer, id: &str) -> bool {
    s.run_record(&rid(id)).current_attempt.is_some()
}

fn paused(s: &TestServer, id: &str) -> bool {
    s.run_record(&rid(id)).pause.is_some()
}

/// Wait until a sleeping run is suspended (journal started, not running).
async fn wait_sleeping(s: &TestServer, id: &str) {
    s.wait_record(&rid(id), LIMIT, |r| {
        r.next_seq > 0 && r.status == RunStatus::Scheduled
    })
    .await;
}

fn reason(v: &Value) -> &str {
    v["error"]["details"]["reason"].as_str().unwrap_or("")
}

fn code(v: &Value) -> &str {
    v["error"]["code"].as_str().unwrap_or("")
}

// ---------------------------------------------------------------------------------
// Queue flow
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flow_round_trip_validation_and_cas_rest_and_grpc() {
    let t0 = Instant::now();
    let mut s = server().await;

    // Absent policy: defaults, version 0.
    let (st, v) = get(&s, "/v1/queues/fq/flow").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["version"], 0);
    assert_eq!(v["queue"], "fq");
    assert_eq!(v["paused"], false);

    // A Phase 5 policy first: its fields survive every Phase 7 write.
    let (st, v) = put(&s, "/v1/queues/fq/policy", json!({"retention": "1h"})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let (_, v) = get(&s, "/v1/queues/fq/flow").await;
    let v1 = v["version"].as_u64().expect("version");
    assert!(v1 >= 1);

    let flow = json!({
        "placement": "auto",
        "selector": ["zone in (a,b)", "!gpu"],
        "spill": {"cpu_pct": 70, "slots_pct": 80, "mem_pct": 90, "lag_ms": 1000,
                  "sustain_ms": 2000, "cooldown_ms": 3000, "hysteresis_pct": 50},
        "concurrency": 4,
        "weight": 3,
        "key_concurrency": 1,
        "throttle": {"limit": 5, "period_ms": 1000, "burst": 2},
        "key_throttle": {"limit": 1, "period_ms": 60000},
        "rate_limit": {"limit": 1000, "period_ms": 1000, "burst": 10},
        "key_rate_limit": {"limit": 100, "period_ms": 500, "burst": 1},
        "on_retries_exhausted": "pause",
    });
    let mut body = flow.clone();
    body["expected_version"] = json!(v1);
    let (st, v) = put(&s, "/v1/queues/fq/flow", body.clone()).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let v2 = v["version"].as_u64().expect("version");
    assert!(v2 > v1);
    for (k, want) in flow.as_object().expect("obj") {
        let got = &v[k];
        if k == "key_throttle" {
            assert_eq!(got["burst"], 0);
            assert_eq!(got["limit"], 1);
            continue;
        }
        assert_eq!(got, want, "field {k}");
    }
    let (_, again) = get(&s, "/v1/queues/fq/flow").await;
    assert_eq!(again, v);
    // The Phase 5 field is preserved.
    let (_, pol) = get(&s, "/v1/queues/fq/policy").await;
    assert!(pol["retention"].is_string(), "{pol}");
    // And the meta record carries it all.
    let rec = s
        .h()
        .meta()
        .view()
        .read(|m| m.queue_policy("fq").map(|v| v.value.clone()))
        .expect("record");
    assert_eq!(rec.retention_ms, Some(3_600_000));
    assert_eq!(rec.placement, Some(ostro_meta::records::PLACEMENT_AUTO));
    assert_eq!(rec.selector, vec!["zone in (a,b)", "!gpu"]);

    // Stale version: 412 with the current version.
    let (st, v) = put(&s, "/v1/queues/fq/flow", body).await;
    assert_eq!(st, StatusCode::PRECONDITION_FAILED, "{v}");
    assert_eq!(v["error"]["details"]["current_version"], v2);

    // Invalid flows: 400, nothing stored.
    for bad in [
        json!({"selector": ["zone in ("]}),
        json!({"spill": {"cpu_pct": 101, "slots_pct": 0, "mem_pct": 0, "lag_ms": 0,
                         "sustain_ms": 0, "cooldown_ms": 0, "hysteresis_pct": 0}}),
        json!({"throttle": {"limit": 1, "period_ms": 0}}),
        json!({"placement": "elsewhere"}),
    ] {
        let (st, v) = put(&s, "/v1/queues/fq/flow", bad.clone()).await;
        assert!(
            st == StatusCode::BAD_REQUEST || st == StatusCode::UNPROCESSABLE_ENTITY,
            "{bad}: {st} {v}"
        );
    }
    let (_, v) = get(&s, "/v1/queues/fq/flow").await;
    assert_eq!(v["version"], v2);

    // gRPC sees the same record; a stale CAS is FAILED_PRECONDITION; a fresh one writes.
    let mut q = QueuesClient::new(s.channel().await);
    let info = q
        .get_queue_flow(pb::GetQueueFlowRequest { name: "fq".into() })
        .await
        .expect("grpc get")
        .into_inner();
    assert_eq!(info.version, v2);
    let gflow = info.flow.expect("flow");
    assert_eq!(gflow.placement, pb::Placement::Auto as i32);
    assert_eq!(gflow.selector, vec!["zone in (a,b)", "!gpu"]);
    assert_eq!(gflow.concurrency, Some(4));
    assert_eq!(gflow.throttle.as_ref().map(|r| r.limit), Some(5));
    assert_eq!(gflow.on_retries_exhausted, pb::ExhaustAction::Pause as i32);
    let e = q
        .put_queue_flow(pb::PutQueueFlowRequest {
            name: "fq".into(),
            flow: Some(gflow.clone()),
            expected_version: Some(v1),
        })
        .await
        .expect_err("stale");
    assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e:?}");
    let mut g2 = gflow.clone();
    g2.concurrency = Some(8);
    g2.selector = vec!["tier=gold".into()];
    let out = q
        .put_queue_flow(pb::PutQueueFlowRequest {
            name: "fq".into(),
            flow: Some(g2),
            expected_version: Some(v2),
        })
        .await
        .expect("grpc put")
        .into_inner();
    assert!(out.version > v2);
    let (_, v) = get(&s, "/v1/queues/fq/flow").await;
    assert_eq!(v["concurrency"], 8);
    assert_eq!(v["selector"], json!(["tier=gold"]));
    let mut bad = gflow;
    bad.selector = vec!["a in (".into()];
    let e = q
        .put_queue_flow(pb::PutQueueFlowRequest {
            name: "fq".into(),
            flow: Some(bad),
            expected_version: None,
        })
        .await
        .expect_err("invalid");
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    let (_, pol) = get(&s, "/v1/queues/fq/policy").await;
    assert!(pol["retention"].is_string(), "{pol}");
    eprintln!("flow_round_trip: {:?}", t0.elapsed());
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// Queue / workflow pause
// ---------------------------------------------------------------------------------

async fn assert_held(s: &TestServer, ids: &[String]) {
    sleep(Duration::from_millis(1000)).await;
    for id in ids {
        assert!(!granted(s, id), "run {id} was granted while paused");
        let (_, v) = get(s, &format!("/v1/runs/{id}")).await;
        assert_eq!(v["status"], "ready", "run {id}: {v}");
    }
}

async fn wait_status(s: &TestServer, id: &str, status: RunStatus) {
    s.wait_status(&rid(id), status, LIMIT).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_pause_holds_backlog_and_resume_dispatches() {
    let t0 = Instant::now();
    let mut s = server().await;
    deploy(&s, "cpu-burn").await;
    let (st, v) = post(&s, "/v1/queues/pq/pause", json!({})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v, json!({"name": "pq", "paused": true, "changed": true}));
    let (_, v) = post(&s, "/v1/queues/pq/pause", json!({})).await;
    assert_eq!(v["changed"], false, "idempotent: {v}");
    let (_, f) = get(&s, "/v1/queues/pq/flow").await;
    assert_eq!(f["paused"], true);

    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(start(&s, "cpu-burn", json!({"input": {"ms": 1}, "queue": "pq"})).await);
    }
    let other = start(&s, "cpu-burn", json!({"input": {"ms": 1}, "queue": "free"})).await;
    wait_status(&s, &other, RunStatus::Completed).await;
    assert_held(&s, &ids).await;

    // Resume over gRPC.
    let mut q = QueuesClient::new(s.channel().await);
    let r = q
        .resume_queue(pb::ResumeQueueRequest { name: "pq".into() })
        .await
        .expect("resume")
        .into_inner();
    assert!(!r.paused && r.changed, "{r:?}");
    for id in &ids {
        wait_status(&s, id, RunStatus::Completed).await;
    }
    let r = q
        .resume_queue(pb::ResumeQueueRequest { name: "pq".into() })
        .await
        .expect("resume again")
        .into_inner();
    assert!(!r.changed);
    let r = q
        .pause_queue(pb::PauseQueueRequest {
            name: String::new(),
        })
        .await
        .expect_err("empty name");
    assert_eq!(r.code(), tonic::Code::InvalidArgument);
    eprintln!("queue_pause: {:?}", t0.elapsed());
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflow_pause_holds_two_queues_and_resume_dispatches() {
    let t0 = Instant::now();
    let mut s = server().await;
    deploy(&s, "cpu-burn").await;
    let mut w = WorkflowsClient::new(s.channel().await);
    let r = w
        .pause_workflow(pb::PauseWorkflowRequest {
            name: "cpu-burn".into(),
        })
        .await
        .expect("pause wf")
        .into_inner();
    assert!(r.paused && r.changed, "{r:?}");
    let e = w
        .pause_workflow(pb::PauseWorkflowRequest {
            name: "no-such-wf".into(),
        })
        .await
        .expect_err("unknown");
    assert_eq!(e.code(), tonic::Code::NotFound);
    let (st, v) = post(&s, "/v1/workflows/no-such-wf/resume", json!({})).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");

    let mut ids = Vec::new();
    for q in ["wqa", "wqb", "wqa", "wqb"] {
        ids.push(start(&s, "cpu-burn", json!({"input": {"ms": 1}, "queue": q})).await);
    }
    assert_held(&s, &ids).await;
    let (_, v) = post(&s, "/v1/workflows/cpu-burn/pause", json!({})).await;
    assert_eq!(v["changed"], false);
    let (st, v) = post(&s, "/v1/workflows/cpu-burn/resume", json!({})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(
        v,
        json!({"name": "cpu-burn", "paused": false, "changed": true})
    );
    for id in &ids {
        wait_status(&s, id, RunStatus::Completed).await;
    }
    // The Phase 5 workflow policy route still works and is untouched by the pause.
    let (_, pol) = get(&s, "/v1/workflows/cpu-burn/policy").await;
    assert!(pol.is_object(), "{pol}");
    eprintln!("workflow_pause: {:?}", t0.elapsed());
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// Run pause / resume
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_pause_resume_idempotent_and_terminal_rest_and_grpc() {
    let t0 = Instant::now();
    let mut s = server().await;
    deploy(&s, "timeouts").await;
    let id = start(
        &s,
        "timeouts",
        json!({"input": {"mode": "sleep_ms", "n": 1500}}),
    )
    .await;
    wait_sleeping(&s, &id).await;

    let path = format!("/v1/runs/{id}/pause");
    let (st, v) = post(&s, &path, json!({"detail": "maintenance"})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v, json!({"run_id": id, "paused": true, "changed": true}));
    let rec = s.run_record(&rid(&id));
    let mark = rec.pause.expect("pause mark");
    assert_eq!(mark.reason, 0);
    assert_eq!(mark.detail, "maintenance");
    // 07-22: the run view shows the pause mark (REST and gRPC).
    let (st, v) = get(&s, &format!("/v1/runs/{id}")).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(
        v["pause"],
        json!({"reason": "operator", "detail": "maintenance", "at_ms": mark.at_ms}),
        "{v}"
    );
    let info = RunsClient::new(s.channel().await)
        .get(pb::GetRunRequest { run_id: id.clone() })
        .await
        .expect("get run")
        .into_inner();
    let p = info.pause.expect("gRPC pause mark");
    assert_eq!((p.reason.as_str(), p.at_ms), ("operator", mark.at_ms));
    // Twice: idempotent.
    let (st, v) = post(&s, &path, json!({})).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["changed"], false);
    // The timer fires while paused; the run makes no progress.
    sleep(Duration::from_millis(2500)).await;
    let rec = s.run_record(&rid(&id));
    assert!(rec.pause.is_some());
    assert!(!rec.status.is_terminal(), "{:?}", rec.status);
    assert!(rec.current_attempt.is_none() || rec.status != RunStatus::Running);
    assert!(s.result_bytes(&rid(&id)).await.is_none());

    // Resume over gRPC: the run completes.
    let mut runs = RunsClient::new(s.channel().await);
    let r = runs
        .resume_run(pb::ResumeRunRequest { run_id: id.clone() })
        .await
        .expect("resume")
        .into_inner();
    assert!(!r.paused && r.changed, "{r:?}");
    wait_status(&s, &id, RunStatus::Completed).await;
    assert!(!paused(&s, &id));
    let (_, v) = get(&s, &format!("/v1/runs/{id}")).await;
    assert!(v["pause"].is_null(), "{v}");

    // Terminal: 409 run_terminal on both transports.
    let (st, v) = post(&s, &path, json!({})).await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(reason(&v), "run_terminal");
    let (st, v) = post(&s, &format!("/v1/runs/{id}/resume"), json!({})).await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    let e = runs
        .pause_run(pb::PauseRunRequest {
            run_id: id.clone(),
            detail: None,
        })
        .await
        .expect_err("terminal");
    // Conflict (409 run_terminal) is ALREADY_EXISTS on gRPC.
    assert_eq!(e.code(), tonic::Code::AlreadyExists, "{e:?}");

    // Unknown run 404; bad id 400; detail too long 400.
    let unknown = RunId::new(rid(&id).partition(), uuid::Uuid::now_v7()).to_string();
    let (st, v) = post(&s, &format!("/v1/runs/{unknown}/pause"), json!({})).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");
    let (st, _) = post(&s, "/v1/runs/not-a-run/pause", json!({})).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let id2 = start(&s, "timeouts", json!({"input": {"mode": "sleep_forever"}})).await;
    let (st, v) = post(
        &s,
        &format!("/v1/runs/{id2}/pause"),
        json!({"detail": "x".repeat(2000)}),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(code(&v), "invalid_argument");

    // gRPC pause of a live run, then resume of a run that is not paused (no change).
    wait_sleeping(&s, &id2).await;
    let r = runs
        .pause_run(pb::PauseRunRequest {
            run_id: id2.clone(),
            detail: Some("grpc".into()),
        })
        .await
        .expect("pause")
        .into_inner();
    assert!(r.paused && r.changed);
    assert!(paused(&s, &id2));
    let (_, v) = post(&s, &format!("/v1/runs/{id2}/resume"), json!({})).await;
    assert_eq!(v["changed"], true);
    let (_, v) = post(&s, &format!("/v1/runs/{id2}/resume"), json!({})).await;
    assert_eq!(v["changed"], false);
    eprintln!("run_pause: {:?}", t0.elapsed());
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_on_paused_run_resumes_it() {
    let t0 = Instant::now();
    let mut s = server().await;
    deploy(&s, "timeouts").await;
    let id = start(
        &s,
        "timeouts",
        json!({"input": {"mode": "sleep_ms", "n": 800}}),
    )
    .await;
    wait_sleeping(&s, &id).await;
    // Live and not paused: the D1 answer is unchanged.
    let (st, v) = post(&s, &format!("/v1/runs/{id}/retry"), json!({})).await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(reason(&v), "run_not_terminal");

    let mut runs = RunsClient::new(s.channel().await);
    runs.pause_run(pb::PauseRunRequest {
        run_id: id.clone(),
        detail: None,
    })
    .await
    .expect("pause");
    sleep(Duration::from_millis(1200)).await;
    assert!(paused(&s, &id));
    // Dry run: reports the resume, changes nothing.
    let (st, v) = post(
        &s,
        &format!("/v1/runs/{id}/retry"),
        json!({"dry_run": true}),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v, json!({"mode": "resumed", "run_id": id, "dry_run": true}));
    assert!(paused(&s, &id));
    // REST retry resumes.
    let (st, v) = post(&s, &format!("/v1/runs/{id}/retry"), json!({})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(
        v,
        json!({"mode": "resumed", "run_id": id, "dry_run": false})
    );
    wait_status(&s, &id, RunStatus::Completed).await;
    assert_eq!(
        s.run_record(&rid(&id)).resumed_count,
        0,
        "a resume is not a D1 retry"
    );

    // gRPC retry on a paused run resumes too.
    let id = start(
        &s,
        "timeouts",
        json!({"input": {"mode": "sleep_ms", "n": 300}}),
    )
    .await;
    wait_sleeping(&s, &id).await;
    runs.pause_run(pb::PauseRunRequest {
        run_id: id.clone(),
        detail: None,
    })
    .await
    .expect("pause");
    let r = runs
        .retry(pb::RetryRunRequest {
            run_id: id.clone(),
            dry_run: false,
        })
        .await
        .expect("grpc retry")
        .into_inner();
    assert_eq!(r.run_id, id);
    wait_status(&s, &id, RunStatus::Completed).await;
    eprintln!("retry_alias: {:?}", t0.elapsed());
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_while_paused_stays_paused_until_resume() {
    let t0 = Instant::now();
    let mut s = server().await;
    deploy(&s, "timeouts").await;
    let id = start(&s, "timeouts", json!({"input": {"mode": "sleep_forever"}})).await;
    wait_sleeping(&s, &id).await;
    let (st, _) = post(&s, &format!("/v1/runs/{id}/pause"), json!({})).await;
    assert_eq!(st, StatusCode::OK);
    let (st, v) = post(
        &s,
        &format!("/v1/runs/{id}/cancel"),
        json!({"reason": "stop"}),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    sleep(Duration::from_millis(1000)).await;
    let rec = s.run_record(&rid(&id));
    assert!(rec.cancel_requested);
    assert!(rec.pause.is_some(), "cancel leaves the run paused");
    assert!(!rec.status.is_terminal(), "{:?}", rec.status);
    let (st, v) = post(&s, &format!("/v1/runs/{id}/resume"), json!({})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    wait_status(&s, &id, RunStatus::Cancelled).await;
    eprintln!("cancel_while_paused: {:?}", t0.elapsed());
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_while_paused_is_immediate() {
    let t0 = Instant::now();
    let mut s = server().await;
    deploy(&s, "timeouts").await;
    let id = start(&s, "timeouts", json!({"input": {"mode": "sleep_forever"}})).await;
    wait_sleeping(&s, &id).await;
    let (st, _) = post(&s, &format!("/v1/runs/{id}/pause"), json!({})).await;
    assert_eq!(st, StatusCode::OK);
    let (st, v) = post(&s, &format!("/v1/runs/{id}/terminate"), json!({})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    wait_status(&s, &id, RunStatus::Terminated).await;
    assert!(!paused(&s, &id), "terminal apply clears the pause mark");
    eprintln!("terminate_while_paused: {:?}", t0.elapsed());
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_pause_of_a_running_activation_stops_progress() {
    let t0 = Instant::now();
    let mut s = server().await;
    deploy(&s, "timeouts").await;
    let n = 20_000;
    let id = start(
        &s,
        "timeouts",
        json!({"input": {"mode": "busy_steps", "n": n}}),
    )
    .await;
    // Mid-activation (the steps are journaled in batches, so the journal may not show it).
    s.wait_record(&rid(&id), LIMIT, |r| {
        r.status == RunStatus::Running && r.current_attempt.is_some()
    })
    .await;
    let (st, v) = post(&s, &format!("/v1/runs/{id}/pause"), json!({})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["changed"], true);
    // The dispatcher revokes the lease and apply rejects further appends (RunPaused):
    // once settled, the journal stops growing.
    sleep(Duration::from_millis(500)).await;
    let a = s.run_record(&rid(&id));
    sleep(Duration::from_millis(1000)).await;
    let b = s.run_record(&rid(&id));
    assert!(b.pause.is_some());
    assert_eq!(
        a.next_seq, b.next_seq,
        "no activation progress while paused"
    );
    assert!(!b.status.is_terminal(), "{:?}", b.status);
    let (st, v) = post(&s, &format!("/v1/runs/{id}/resume"), json!({})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    wait_status(&s, &id, RunStatus::Completed).await;
    let out: Value =
        serde_json::from_slice(&s.result_bytes(&rid(&id)).await.expect("result")).expect("json");
    assert_eq!(out, json!(format!("busy:{n}")));
    eprintln!("run_pause_running: {:?}", t0.elapsed());
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// Workers: tokens, join, list, drain, demand
// ---------------------------------------------------------------------------------

/// An ECDSA P-256 key and a PKCS#10 CSR (PEM).
fn key_and_csr(worker_id: &str) -> (String, String) {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("key");
    let mut p = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
    p.distinguished_name
        .push(rcgen::DnType::CommonName, worker_id);
    let csr = p.serialize_request(&key).expect("csr").pem().expect("pem");
    (key.serialize_pem(), csr)
}

fn identity_of(cert_pem: &str) -> ostro_server::tls::ca::WorkerIdentity {
    let der = x509_parser::pem::parse_x509_pem(cert_pem.as_bytes())
        .expect("pem")
        .1
        .contents;
    ostro_server::tls::ca::WorkerIdentity::from_der(&der).expect("worker identity")
}

async fn create_token(s: &TestServer, ttl_ms: Option<u64>) -> Value {
    let body = match ttl_ms {
        Some(t) => json!({"ttl_ms": t}),
        None => json!({}),
    };
    let r = s
        .http
        .post(s.url("/v1/workers/tokens"))
        .json(&body)
        .send()
        .await
        .expect("POST token");
    assert_eq!(r.status(), StatusCode::CREATED);
    assert_eq!(
        r.headers()
            .get("cache-control")
            .map(|v| v.to_str().expect("ascii")),
        Some("no-store")
    );
    r.json().await.expect("json")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_tokens_join_and_revoke_rest_and_grpc() {
    use ostro_proto::api::v1::workers_client::WorkersClient;
    let t0 = Instant::now();
    let mut s = server().await;

    // Create: the secret is shown once, with a join command.
    let created = create_token(&s, Some(3_600_000)).await;
    let token_id = created["token_id"].as_str().expect("id").to_owned();
    let token = created["token"].as_str().expect("token").to_owned();
    assert!(token_id.starts_with("wkr_"), "{created}");
    assert!(token.starts_with(&format!("{token_id}.")));
    let cmd = created["join_command"].as_str().expect("cmd");
    assert!(
        cmd.starts_with("ostro worker --join http://127.0.0.1:"),
        "{cmd}"
    );
    assert!(cmd.contains(&format!("--token {token}")), "{cmd}");
    assert!(cmd.contains("--ca-hash sha256:"), "{cmd}");
    let secret = token.split_once('.').expect("dot").1.to_owned();

    // List: ids and times, never the secret.
    let (st, v) = get(&s, "/v1/workers/tokens").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let ids: Vec<&str> = v["tokens"]
        .as_array()
        .expect("tokens")
        .iter()
        .map(|t| t["token_id"].as_str().expect("id"))
        .collect();
    assert_eq!(ids, vec![token_id.as_str()]);
    assert!(!v.to_string().contains(&secret), "list leaks the secret");
    let mut w = WorkersClient::new(s.channel().await);
    let listed = w
        .list_worker_tokens(pb::ListWorkerTokensRequest {})
        .await
        .expect("grpc list")
        .into_inner();
    assert_eq!(listed.tokens.len(), 1);
    assert!(!format!("{listed:?}").contains(&secret));

    // Join over REST with a CSR: a client certificate naming the token.
    let (_key, csr) = key_and_csr("w-rest");
    let (st, v) = post(
        &s,
        "/v1/workers/join",
        json!({"token": token, "csr_pem": csr, "worker_id": "w-rest", "labels": {"zone": "a"}}),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let id = identity_of(v["client_cert_pem"].as_str().expect("cert"));
    assert_eq!(id.token_id, token_id);
    assert_eq!(id.worker_id, "w-rest");
    assert!(
        !v["endpoints"].as_array().expect("endpoints").is_empty(),
        "{v}"
    );
    assert!(
        v["ca_cert_pem"]
            .as_str()
            .expect("ca")
            .contains("BEGIN CERTIFICATE")
    );

    // A wrong secret: 400 join_token_invalid, never echoing the token.
    let bad = format!("{token_id}.{}", "A".repeat(secret.len()));
    let (st, v) = post(
        &s,
        "/v1/workers/join",
        json!({"token": bad, "csr_pem": csr, "worker_id": "w-rest"}),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(reason(&v), "join_token_invalid");
    assert!(!v.to_string().contains(&bad));

    // gRPC join works too.
    let (_key, csr2) = key_and_csr("w-grpc");
    let r = w
        .join_worker(pb::JoinWorkerRequest {
            token: token.clone(),
            csr_pem: csr2.clone(),
            worker_id: "w-grpc".into(),
            labels: Default::default(),
        })
        .await
        .expect("grpc join")
        .into_inner();
    assert_eq!(identity_of(&r.client_cert_pem).worker_id, "w-grpc");

    // Revoke (gRPC): REST and gRPC joins are refused; revoking again is 404.
    w.revoke_worker_token(pb::RevokeWorkerTokenRequest {
        token_id: token_id.clone(),
    })
    .await
    .expect("revoke");
    let (st, v) = post(
        &s,
        "/v1/workers/join",
        json!({"token": token, "csr_pem": csr, "worker_id": "w-rest"}),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(reason(&v), "join_token_invalid");
    let e = w
        .join_worker(pb::JoinWorkerRequest {
            token: token.clone(),
            csr_pem: csr2,
            worker_id: "w-grpc".into(),
            labels: Default::default(),
        })
        .await
        .expect_err("revoked");
    assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e:?}");
    let (st, v) = send(
        &s,
        reqwest::Method::DELETE,
        &format!("/v1/workers/tokens/{token_id}"),
        None,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");
    let (_, v) = get(&s, "/v1/workers/tokens").await;
    assert_eq!(v["tokens"], json!([]));

    // REST revoke: 204.
    let t2 = create_token(&s, None).await;
    let id2 = t2["token_id"].as_str().expect("id");
    let (st, _) = send(
        &s,
        reqwest::Method::DELETE,
        &format!("/v1/workers/tokens/{id2}"),
        None,
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    // ttl_ms = 0 is invalid.
    let (st, v) = post(&s, "/v1/workers/tokens", json!({"ttl_ms": 0})).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    eprintln!("worker_tokens: {:?}", t0.elapsed());
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workers_list_drain_and_demand_with_a_connected_worker() {
    use ostro_proto::api::v1::workers_client::WorkersClient;
    let t0 = Instant::now();
    let mut s = server().await;
    // No workers yet.
    let (st, v) = get(&s, "/v1/workers").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["workers"], json!([]));
    let (st, v) = post(&s, "/v1/workers/nobody/drain", json!({})).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");

    // An in-process worker joins through this server's REST join route.
    let created = create_token(&s, None).await;
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
        labels: [("zone".to_owned(), "eu-1".to_owned())]
            .into_iter()
            .collect(),
        slots: 3,
        cache_dir: cache.path().to_path_buf(),
        worker_id: Some("w1".into()),
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

    // Listed with its labels and slots, on this node.
    let t = Instant::now();
    let w1 = loop {
        let (_, v) = get(&s, "/v1/workers").await;
        if let Some(w) = v["workers"].as_array().and_then(|a| a.first()).cloned() {
            break w;
        }
        assert!(t.elapsed() < LIMIT, "worker not listed");
        sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(w1["worker_id"], "w1");
    assert_eq!(w1["labels"], json!({"zone": "eu-1"}));
    assert_eq!(w1["slots"], 3);
    assert_eq!(w1["node_id"], s.cfg.node.node_id);
    assert_eq!(w1["draining"], false);
    let mut grpc = WorkersClient::new(s.channel().await);
    let listed = grpc
        .list_workers(pb::ListWorkersRequest {})
        .await
        .expect("grpc list")
        .into_inner();
    assert_eq!(listed.workers.len(), 1);
    assert_eq!(listed.workers[0].worker_id, "w1");

    // Drain: the registry stops assigning (draining, no free slots); idempotent.
    let (st, v) = post(&s, "/v1/workers/w1/drain", json!({})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v, json!({"worker_id": "w1", "already_draining": false}));
    let r = grpc
        .drain_worker(pb::DrainWorkerRequest {
            worker_id: "w1".into(),
        })
        .await
        .expect("grpc drain")
        .into_inner();
    assert!(r.already_draining);
    let (_, v) = get(&s, "/v1/workers").await;
    assert_eq!(v["workers"][0]["draining"], true, "{v}");
    assert_eq!(v["workers"][0]["free_slots"], 0, "{v}");

    // Demand: the D-W12 shape (zeros until 07-16), over both transports.
    let (st, v) = get(&s, "/v1/queues/default/demand").await;
    assert_eq!(st, StatusCode::OK, "{v}");
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
        assert!(v.get(k).is_some(), "demand lacks {k}: {v}");
    }
    assert_eq!(v["queue"], "default");
    let mut q = QueuesClient::new(s.channel().await);
    let d = q
        .get_queue_demand(pb::GetQueueDemandRequest {
            name: "default".into(),
        })
        .await
        .expect("grpc demand")
        .into_inner();
    assert_eq!(d.queue, "default");

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(20), task).await;
    eprintln!("workers_list_drain: {:?}", t0.elapsed());
    s.shutdown().await;
}
