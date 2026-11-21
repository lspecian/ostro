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
