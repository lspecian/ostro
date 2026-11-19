//! API-03 / API-04 (start + get) / WF-01 against the real server (plan 04-18): starting
//! runs over REST and gRPC with every option, explicit and active versions, start delays,
//! idempotency keys (return_existing / reject, across a restart), blob-offloaded inputs,
//! request size limits, and GET with the typed JSON result and the published status.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, guest_wasm, server_config, tmpfs_dir};
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::runs_client::RunsClient;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::sleep;
use tonic::Code;

fn sha_hex(b: &[u8]) -> String {
    Sha256::digest(b)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}

async fn server() -> TestServer {
    let s = TestServer::start(ServerOpts::default()).await;
    s.wait_ready(Duration::from_secs(5)).await;
    s
}

/// Upload `guest` and return its sha.
async fn deploy(s: &TestServer, guest: &str) -> String {
    let wasm = guest_wasm(guest);
    let sha = sha_hex(&wasm);
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{sha}")))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
    sha
}

async fn activate(s: &TestServer, name: &str, version: u32) {
    let r = s
        .http
        .put(s.url(&format!("/v1/workflows/{name}/active")))
        .json(&json!({ "version": version }))
        .send()
        .await
        .expect("PUT active");
    assert_eq!(r.status(), StatusCode::OK);
}

async fn start(s: &TestServer, workflow: &str, body: Value) -> (StatusCode, Value) {
    let r = s
        .http
        .post(s.url(&format!("/v1/workflows/{workflow}/runs")))
        .json(&body)
        .send()
        .await
        .expect("POST run");
    let st = r.status();
    (st, r.json().await.expect("json body"))
}

async fn get(s: &TestServer, run_id: &str) -> (StatusCode, Value) {
    let r = s
        .http
        .get(s.url(&format!("/v1/runs/{run_id}")))
        .send()
        .await
        .expect("GET run");
    let st = r.status();
    (st, r.json().await.expect("json body"))
}

/// Poll GET until the run is terminal.
async fn wait_terminal(s: &TestServer, run_id: &str, limit: Duration) -> Value {
    let t0 = Instant::now();
    loop {
        let (st, body) = get(s, run_id).await;
        assert_eq!(st, StatusCode::OK, "{body}");
        if matches!(
            body["status"].as_str(),
            Some("completed" | "failed" | "cancelled")
        ) {
            return body;
        }
        assert!(
            t0.elapsed() < limit,
            "run {run_id} not terminal within {limit:?}: {body}"
        );
        sleep(Duration::from_millis(20)).await;
    }
}

fn basic_result(name: &str, n: u64) -> Value {
    json!({ "greeting": format!("hello, {name}"), "sum": n * (n + 1) / 2, "steps": 2 })
}

const LIMIT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------------
// REST
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rest_start_get_typed_result_and_options() {
    let mut s = server().await;
    let sha = deploy(&s, "sdk-basic").await;

    // Minimal start: 201, created, active version, default queue.
    let (st, body) = start(&s, "sdk-basic", json!({"input": {"name": "a", "n": 3}})).await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
    assert_eq!(body["created"], true);
    let id = body["run_id"].as_str().expect("run_id").to_owned();
    let run = wait_terminal(&s, &id, LIMIT).await;
    assert_eq!(run["status"], "completed", "{run}");
    assert_eq!(run["run_id"], id);
    assert_eq!(run["workflow"], "sdk-basic");
    assert_eq!(run["version"], 1);
    assert_eq!(run["module_sha256"], sha);
    assert_eq!(run["queue"], "default");
    // `attempt` is the grant counter of the current epoch: one per dispatch (sdk-basic
    // suspends on its sleeps, so it is dispatched more than once).
    assert!(run["attempt"].as_u64().is_some_and(|n| n >= 1), "{run}");
    assert_eq!(run["result"], basic_result("a", 3));
    assert_eq!(run["published_status"], json!({"stage": "done"}));
    assert_eq!(run["failure"], Value::Null);
    let created = run["created_at_ms"].as_u64().expect("created_at_ms");
    let completed = run["completed_at_ms"].as_u64().expect("completed_at_ms");
    assert!(completed >= created, "{run}");
    assert!(run["created_at"].as_str().expect("rfc3339").ends_with('Z'));

    // Every option is persisted and returned exactly as sent. (The tenant is D23-qualified
    // since 05-24: a tenant run resolves `<tenant>/<workflow>`, which needs a tenant
    // upload and, to complete, 05-11's journal-name stripping; tenancy_api.rs covers it.)
    let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let (st, body) = start(
        &s,
        "sdk-basic",
        json!({
            "input": {"name": "opts", "n": 4},
            "queue": "q-special",
            "priority": 7,
            "tags": ["t1", "team:a"],
            "traceparent": tp,
            "concurrency_key": "ck-1",
            "version": 1,
        }),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
    let id2 = body["run_id"].as_str().expect("run_id").to_owned();
    let run = wait_terminal(&s, &id2, LIMIT).await;
    assert_eq!(run["status"], "completed", "{run}");
    assert_eq!(run["queue"], "q-special");
    assert_eq!(run["priority"], 7);
    assert_eq!(run["tags"], json!(["t1", "team:a"]));
    assert_eq!(run["traceparent"], tp);
    assert_eq!(run["tenant"], Value::Null);
    assert_eq!(run["concurrency_key"], "ck-1");
    assert_eq!(run["result"], basic_result("opts", 4));

    // Unknown workflow / version: 404. Bad run id: 400. Unknown (well-formed) id: 404.
    let (st, body) = start(&s, "nope", json!({"input": {}})).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{body}");
    let (st, body) = start(&s, "sdk-basic", json!({"input": {}, "version": 9})).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{body}");
    let (st, body) = get(&s, "not-a-run-id").await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{body}");
    let mut other: Vec<char> = id.chars().collect();
    let last = other.len() - 1;
    other[last] = if other[last] == '0' { '4' } else { '0' };
    let (st, body) = get(&s, &other.into_iter().collect::<String>()).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{body}");

    // A failing input: the workflow's own failure is rendered, no result.
    let (st, body) = start(&s, "sdk-basic", json!({"input": {"wrong": true}})).await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
    let run = wait_terminal(&s, body["run_id"].as_str().expect("id"), LIMIT).await;
    assert_eq!(run["status"], "failed", "{run}");
    assert_eq!(run["result"], Value::Null);
    let failure: Value =
        serde_json::from_str(run["failure"].as_str().expect("failure text")).expect("json");
    assert!(failure["reason_code"].is_string(), "{failure}");
    assert!(failure["message"].is_string(), "{failure}");

    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rest_explicit_version_is_pinned() {
    let mut s = server().await;
    let v1 = deploy(&s, "sdk-basic").await;
    let v2 = deploy(&s, "sdk-basic-v2").await;

    // Active is still 1: explicit version 2 pins the v2 module.
    let (st, body) = start(
        &s,
        "sdk-basic",
        json!({"input": {"name": "b", "n": 2}, "version": 2}),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
    let pinned = body["run_id"].as_str().expect("id").to_owned();

    // Activate 2, start without a version (v2), then activate 1 again.
    activate(&s, "sdk-basic", 2).await;
    let (_, body) = start(&s, "sdk-basic", json!({"input": {"name": "c", "n": 1}})).await;
    let active2 = body["run_id"].as_str().expect("id").to_owned();
    activate(&s, "sdk-basic", 1).await;
    let (_, body) = start(&s, "sdk-basic", json!({"input": {"name": "d", "n": 1}})).await;
    let active1 = body["run_id"].as_str().expect("id").to_owned();

    for (id, version, sha) in [(&pinned, 2, &v2), (&active2, 2, &v2), (&active1, 1, &v1)] {
        let run = wait_terminal(&s, id, LIMIT).await;
        assert_eq!(run["status"], "completed", "{run}");
        assert_eq!(run["version"], version, "{run}");
        assert_eq!(&run["module_sha256"], sha.as_str(), "{run}");
        if version == 2 {
            assert_eq!(run["result"]["version"], 2, "{run}");
            assert_eq!(
                run["published_status"],
                json!({"stage": "done", "version": 2})
            );
        } else {
            assert_eq!(run["result"].get("version"), None, "{run}");
        }
    }
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rest_delayed_start_stays_scheduled() {
    let mut s = server().await;
    deploy(&s, "sdk-basic").await;
    let t0 = Instant::now();
    let (st, body) = start(
        &s,
        "sdk-basic",
        json!({"input": {"name": "late", "n": 1}, "delay_ms": 1500}),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
    let id = body["run_id"].as_str().expect("id").to_owned();
    sleep(Duration::from_millis(200).saturating_sub(t0.elapsed())).await;
    let (_, run) = get(&s, &id).await;
    assert_eq!(run["status"], "scheduled", "{run}");
    assert_eq!(run["attempt"], Value::Null, "{run}");
    let run = wait_terminal(&s, &id, LIMIT).await;
    assert_eq!(run["status"], "completed", "{run}");
    assert!(
        t0.elapsed() >= Duration::from_millis(1500),
        "completed after {:?}",
        t0.elapsed()
    );
    let created = run["created_at_ms"].as_u64().expect("created");
    let completed = run["completed_at_ms"].as_u64().expect("completed");
    assert!(completed >= created + 1500, "{run}");
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rest_idempotency_keys_survive_restart() {
    let mut s = server().await;
    deploy(&s, "sdk-basic").await; // sdk-basic + sdk-echo
    let body = json!({"input": {"name": "i", "n": 2}, "idempotency_key": "order-42"});

    let (st, first) = start(&s, "sdk-basic", body.clone()).await;
    assert_eq!(st, StatusCode::CREATED, "{first}");
    let id = first["run_id"].as_str().expect("id").to_owned();

    // return_existing (default): 200, created=false, same id.
    let (st, again) = start(&s, "sdk-basic", body.clone()).await;
    assert_eq!(st, StatusCode::OK, "{again}");
    assert_eq!(again["created"], false);
    assert_eq!(again["run_id"], id);

    // reject: 409 with details.existing_run_id.
    let mut rej = body.clone();
    rej["conflict_policy"] = json!("reject");
    let (st, err) = start(&s, "sdk-basic", rej.clone()).await;
    assert_eq!(st, StatusCode::CONFLICT, "{err}");
    assert_eq!(err["error"]["details"]["existing_run_id"], id);

    // The same key on another workflow is an independent run.
    let (st, echo) = start(
        &s,
        "sdk-echo",
        json!({"input": [1, 2], "idempotency_key": "order-42"}),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{echo}");
    assert_ne!(echo["run_id"], id);

    let run = wait_terminal(&s, &id, LIMIT).await;
    assert_eq!(run["status"], "completed", "{run}");

    // Dedupe is durable: after a restart the key still maps to the same run.
    s.restart().await;
    s.wait_ready(Duration::from_secs(5)).await;
    let (st, again) = start(&s, "sdk-basic", body.clone()).await;
    assert_eq!(st, StatusCode::OK, "{again}");
    assert_eq!(again["created"], false);
    assert_eq!(again["run_id"], id);
    let (st, err) = start(&s, "sdk-basic", rej).await;
    assert_eq!(st, StatusCode::CONFLICT, "{err}");
    assert_eq!(err["error"]["details"]["existing_run_id"], id);
    let (st, run) = get(&s, &id).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(run["result"], basic_result("i", 2));
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rest_large_input_is_offloaded_and_request_limit_is_413() {
    let dir = tmpfs_dir();
    let mut cfg = server_config(dir.path(), &ServerOpts::default());
    cfg.api.max_request_bytes = 1024 * 1024;
    let mut s = TestServer::start_cfg(cfg, Some(dir)).await;
    s.wait_ready(Duration::from_secs(5)).await;
    deploy(&s, "sdk-basic").await; // exports sdk-echo too

    // 300 KiB > the 256 KiB inline limit: offloaded to the blob store, echoed back.
    let big = "x".repeat(300 * 1024);
    let blobs_before = blob_count(&s);
    let (st, body) = start(&s, "sdk-echo", json!({"input": {"big": big}})).await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
    let run = wait_terminal(&s, body["run_id"].as_str().expect("id"), LIMIT).await;
    assert_eq!(run["status"], "completed", "{run}");
    assert_eq!(run["result"]["big"].as_str().map(str::len), Some(big.len()));
    assert!(
        blob_count(&s) > blobs_before,
        "the input (and result) should be blobs"
    );

    // Over api.max_request_bytes: 413 from the adapter, nothing started.
    let huge = "y".repeat(1100 * 1024);
    let (st, body) = start(&s, "sdk-echo", json!({"input": huge})).await;
    assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    s.shutdown().await;
}

/// Number of files under `<data>/blobs` (any layout).
fn blob_count(s: &TestServer) -> usize {
    fn walk(p: &std::path::Path) -> usize {
        std::fs::read_dir(p).map_or(0, |rd| {
            rd.flatten()
                .map(|e| {
                    let path = e.path();
                    if path.is_dir() { walk(&path) } else { 1 }
                })
                .sum()
        })
    }
    walk(&s.h().data_dir().join("blobs"))
}

// ---------------------------------------------------------------------------------
// gRPC
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_start_get_and_errors() {
    let mut s = server().await;
    let sha = deploy(&s, "sdk-basic").await;
    let mut runs = RunsClient::new(s.channel().await);

    let res = runs
        .start(pb::StartRunRequest {
            workflow: "sdk-basic".into(),
            input_json: br#"{"name":"g","n":5}"#.to_vec(),
            tags: vec!["grpc".into()],
            priority: Some(-3),
            idempotency_key: Some("g-key".into()),
            ..Default::default()
        })
        .await
        .expect("start")
        .into_inner();
    assert!(res.created);

    // Poll Get until terminal.
    let t0 = Instant::now();
    let info = loop {
        let info = runs
            .get(pb::GetRunRequest {
                run_id: res.run_id.clone(),
            })
            .await
            .expect("get")
            .into_inner();
        if info.status == pb::RunStatus::Completed as i32 {
            break info;
        }
        assert!(t0.elapsed() < LIMIT, "not completed: {info:?}");
        sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(info.workflow, "sdk-basic");
    assert_eq!(info.version, 1);
    assert_eq!(info.module_sha256, sha);
    assert_eq!(info.queue, "default");
    assert!(info.attempt.is_some_and(|n| n >= 1), "{info:?}");
    assert_eq!(info.tags, vec!["grpc".to_owned()]);
    assert_eq!(info.tenant, None);
    assert_eq!(info.priority, -3);
    let result: Value =
        serde_json::from_slice(info.result_json.as_deref().expect("result")).expect("result json");
    assert_eq!(result, basic_result("g", 5));
    let status: Value =
        serde_json::from_slice(info.published_status_json.as_deref().expect("status"))
            .expect("status json");
    assert_eq!(status, json!({"stage": "done"}));

    // REST sees the same run.
    let (_, rest) = get(&s, &res.run_id).await;
    assert_eq!(rest["result"], result);

    // Idempotency over gRPC: return_existing then reject (ALREADY_EXISTS + details).
    let dup = pb::StartRunRequest {
        workflow: "sdk-basic".into(),
        input_json: br#"{"name":"g","n":5}"#.to_vec(),
        idempotency_key: Some("g-key".into()),
        ..Default::default()
    };
    let again = runs.start(dup.clone()).await.expect("dup").into_inner();
    assert!(!again.created);
    assert_eq!(again.run_id, res.run_id);
    let e = runs
        .start(pb::StartRunRequest {
            conflict_policy: pb::ConflictPolicy::Reject as i32,
            ..dup
        })
        .await
        .expect_err("reject");
    assert_eq!(e.code(), Code::AlreadyExists);
    let details = ostro_api::grpc::status_details(&e).expect("details");
    assert_eq!(details["existing_run_id"], res.run_id);

    // Invalid JSON input: InvalidArgument. Unknown workflow: NotFound. Bad id: InvalidArgument.
    let e = runs
        .start(pb::StartRunRequest {
            workflow: "sdk-basic".into(),
            input_json: b"{not json".to_vec(),
            ..Default::default()
        })
        .await
        .expect_err("bad json");
    assert_eq!(e.code(), Code::InvalidArgument);
    let e = runs
        .start(pb::StartRunRequest {
            workflow: "missing".into(),
            ..Default::default()
        })
        .await
        .expect_err("unknown workflow");
    assert_eq!(e.code(), Code::NotFound);
    let e = runs
        .get(pb::GetRunRequest {
            run_id: "zzz".into(),
        })
        .await
        .expect_err("bad id");
    assert_eq!(e.code(), Code::InvalidArgument);

    s.shutdown().await;
}
