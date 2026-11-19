//! API-03 / API-04 (start + get) / WF-01 against the real server (plan 04-18): starting
//! runs over REST and gRPC with every option, explicit and active versions, start delays,
//! idempotency keys (return_existing / reject, across a restart), blob-offloaded inputs,
//! request size limits, and GET with the typed JSON result and the published status.
//! Plan 05-13: the start fields (retention, schedule-to-start, dry_run), D9 policy
//! resolution, workflow/queue policies over both transports (persisted in meta), GET's
//! `attempt_failures` / resolved policy fields, and the O(1) published-status read (F10).
//! Plan 05-25 (D24): an API start journals `RunStarted` at seq 0 inside `StartRun`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, guest_wasm, server_config, tmpfs_dir};
use ostro_partition::Table;
use ostro_partition::store::keys::dedupe_key;
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::runs_client::RunsClient;
use ostro_proto::api::v1::workflows_client::WorkflowsClient;
use ostro_types::journal::EventBody;
use ostro_types::{PartitionCount, RunId, partition_for_key};
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

/// D24 (05-25, amends D3): `StartRun.run_started` is set by the API, so a run that was
/// accepted but never activated already has `RunStarted` at seq 0 (`next_seq == 1`), built
/// from the command (stripped name, module hash, JSON input, seed, accepted time). An input
/// over the inline limit makes both the input and the encoded RunStarted blobs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_start_journals_run_started_at_seq_zero() {
    let mut s = TestServer::start_with(ServerOpts::default(), |c| c.executor.slots = 0).await;
    s.wait_ready(Duration::from_secs(5)).await;
    let sha = deploy(&s, "sdk-basic").await; // exports sdk-echo too
    let big = "x".repeat(300 * 1024);
    for (input, offloaded) in [(json!({"n": 1}), false), (json!({ "big": big }), true)] {
        let blobs_before = blob_count(&s);
        let (st, body) = start(&s, "sdk-echo", json!({ "input": input })).await;
        assert_eq!(st, StatusCode::CREATED, "{body}");
        let run: RunId = body["run_id"]
            .as_str()
            .expect("id")
            .parse()
            .expect("run id");
        let rec = s.run_record(&run);
        assert_eq!(rec.next_seq, 1, "RunStarted journaled by StartRun");
        assert!(rec.current_attempt.is_none(), "never granted");
        let j = s.journal(&run).await;
        assert_eq!(j.len(), 1);
        assert_eq!(
            (j[0].seq, j[0].ts_nanos),
            (0, rec.created_at_ms * 1_000_000)
        );
        let EventBody::RunStarted(rs) = &j[0].body else {
            panic!("seq 0 is not RunStarted: {:?}", j[0].body)
        };
        assert_eq!(rs.workflow, "sdk-echo");
        assert_eq!(rs.tenant, None);
        assert_eq!(rs.module_hash, sha);
        assert_eq!(rs.input.content_type, "application/json");
        assert_eq!(
            serde_json::from_slice::<Value>(&rs.input.data).expect("json input"),
            input
        );
        assert_eq!(rs.rng_seed.0.as_slice(), rec.rng_seed.as_slice());
        assert_eq!(rs.accepted_at_nanos, rec.created_at_ms * 1_000_000);
        assert_eq!(
            (rs.parent_run_id.clone(), rs.continued_from.clone()),
            (None, None)
        );
        if offloaded {
            assert!(
                blob_count(&s) >= blobs_before + 2,
                "the input and the encoded RunStarted are blobs"
            );
        }
    }
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

// ---------------------------------------------------------------------------------
// 05-13: start fields, policies, dry run, GET additions
// ---------------------------------------------------------------------------------

const H: u64 = 3_600_000;

async fn put_policy(s: &TestServer, path: &str, body: Value) -> (StatusCode, Value) {
    let r = s
        .http
        .put(s.url(path))
        .json(&body)
        .send()
        .await
        .expect("PUT policy");
    let st = r.status();
    (st, r.json().await.expect("json body"))
}

async fn get_path(s: &TestServer, path: &str) -> (StatusCode, Value) {
    let r = s.http.get(s.url(path)).send().await.expect("GET");
    let st = r.status();
    (st, r.json().await.expect("json body"))
}

async fn run_count(s: &TestServer) -> usize {
    let (st, body) = get_path(s, "/v1/runs?limit=1000").await;
    assert_eq!(st, StatusCode::OK, "{body}");
    body["runs"].as_array().expect("runs").len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_fields_resolve_policies_over_both_transports() {
    let mut s = server().await;
    deploy(&s, "sdk-basic").await;
    let mut wf = WorkflowsClient::new(s.channel().await);
    let mut runs = RunsClient::new(s.channel().await);

    // No policy: the server default `[runs] retention` (168h), no schedule-to-start.
    let (st, body) = start(&s, "sdk-basic", json!({"input": {"name": "p", "n": 1}})).await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
    let plain = body["run_id"].as_str().expect("id").to_owned();
    let run = wait_terminal(&s, &plain, LIMIT).await;
    assert_eq!(run["retention_ms"], json!(168 * H), "{run}");
    assert_eq!(run["schedule_to_start_ms"], Value::Null, "{run}");
    assert_eq!(run["attempt_failures"], json!(0), "{run}");

    // Policies over REST (workflow) and gRPC (queue).
    let (st, body) = get_path(&s, "/v1/workflows/sdk-basic/policy").await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["retention_ms"], Value::Null);
    let (st, body) = put_policy(
        &s,
        "/v1/workflows/sdk-basic/policy",
        json!({"retention": "48h", "schedule_to_start_timeout": "30s"}),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["retention_ms"], json!(48 * H));
    assert_eq!(body["retention"], "48h");
    assert_eq!(body["schedule_to_start_ms"], json!(30_000));
    let qp = wf
        .set_queue_policy(pb::SetQueuePolicyRequest {
            name: "q2".into(),
            policy: Some(pb::RunPolicy {
                retention_ms: Some(2 * H),
                schedule_to_start_ms: None,
            }),
        })
        .await
        .expect("set queue policy")
        .into_inner();
    assert_eq!(qp.retention_ms, Some(2 * H));
    let got = wf
        .get_policy(pb::GetWorkflowPolicyRequest {
            name: "sdk-basic".into(),
        })
        .await
        .expect("get workflow policy")
        .into_inner();
    assert_eq!(got.retention_ms, Some(48 * H));
    assert_eq!(got.schedule_to_start_ms, Some(30_000));

    // Workflow policy beats queue policy; the request beats both.
    let (_, body) = start(
        &s,
        "sdk-basic",
        json!({"input": {"name": "w", "n": 1}, "queue": "q2"}),
    )
    .await;
    let (_, run) = get(&s, body["run_id"].as_str().expect("id")).await;
    assert_eq!(run["retention_ms"], json!(48 * H), "{run}");
    assert_eq!(run["schedule_to_start_ms"], json!(30_000), "{run}");
    let (_, body) = start(
        &s,
        "sdk-basic",
        json!({"input": {"name": "r", "n": 1}, "retention": "1h", "schedule_to_start_timeout": "0s"}),
    )
    .await;
    let (_, run) = get(&s, body["run_id"].as_str().expect("id")).await;
    assert_eq!(run["retention_ms"], json!(H), "{run}");
    assert_eq!(
        run["schedule_to_start_ms"],
        Value::Null,
        "explicit 0 = none: {run}"
    );
    let res = runs
        .start(pb::StartRunRequest {
            workflow: "sdk-basic".into(),
            input_json: br#"{"name":"g","n":1}"#.to_vec(),
            retention_ms: Some(3 * H),
            schedule_to_start_timeout_ms: Some(5_000),
            ..Default::default()
        })
        .await
        .expect("grpc start")
        .into_inner();
    assert!(res.created && !res.dry_run);
    let info = runs
        .get(pb::GetRunRequest {
            run_id: res.run_id.clone(),
        })
        .await
        .expect("get")
        .into_inner();
    assert_eq!(info.retention_ms, Some(3 * H));
    assert_eq!(info.schedule_to_start_ms, Some(5_000));
    assert_eq!(info.attempt_failures, 0);

    // Clear the workflow policy: the queue policy applies.
    let (st, _) = put_policy(&s, "/v1/workflows/sdk-basic/policy", json!({})).await;
    assert_eq!(st, StatusCode::OK);
    let (_, body) = start(
        &s,
        "sdk-basic",
        json!({"input": {"name": "q", "n": 1}, "queue": "q2"}),
    )
    .await;
    let (_, run) = get(&s, body["run_id"].as_str().expect("id")).await;
    assert_eq!(run["retention_ms"], json!(2 * H), "{run}");
    assert_eq!(run["schedule_to_start_ms"], Value::Null, "{run}");

    // Errors: unknown workflow 404 (both verbs, both transports), bad names / durations 400.
    let (st, body) = put_policy(&s, "/v1/workflows/nope/policy", json!({})).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{body}");
    let (st, body) = get_path(&s, "/v1/workflows/nope/policy").await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{body}");
    let e = wf
        .get_policy(pb::GetWorkflowPolicyRequest {
            name: "nope".into(),
        })
        .await
        .expect_err("unknown");
    assert_eq!(e.code(), Code::NotFound);
    let (st, body) = put_policy(&s, "/v1/queues/x:y/policy", json!({})).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{body}");
    let (st, body) = put_policy(&s, "/v1/queues/q2/policy", json!({"retention": "7d"})).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{body}");
    let (st, body) = start(&s, "sdk-basic", json!({"input": {}, "retention": "7d"})).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["details"]["field"], "retention");
    let e = wf
        .set_queue_policy(pb::SetQueuePolicyRequest {
            name: "__z".into(),
            policy: None,
        })
        .await
        .expect_err("bad queue");
    assert_eq!(e.code(), Code::InvalidArgument);

    // Policies persist in meta across a restart.
    s.restart().await;
    s.wait_ready(Duration::from_secs(5)).await;
    let (st, body) = get_path(&s, "/v1/queues/q2/policy").await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["retention_ms"], json!(2 * H));
    let qp = s
        .h()
        .meta()
        .view()
        .read(|st| st.queue_policy("q2").map(|v| v.value.retention_ms));
    assert_eq!(qp, Some(Some(2 * H)));
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dry_run_start_validates_and_submits_nothing() {
    let mut s = server().await;
    deploy(&s, "sdk-basic").await;
    let mut runs = RunsClient::new(s.channel().await);
    let body = json!({"input": {"name": "d", "n": 1}, "idempotency_key": "dk-1"});
    let (st, first) = start(&s, "sdk-basic", body.clone()).await;
    assert_eq!(st, StatusCode::CREATED, "{first}");
    let existing = first["run_id"].as_str().expect("id").to_owned();
    wait_terminal(&s, &existing, LIMIT).await;
    let runs_before = run_count(&s).await;
    let blobs_before = blob_count(&s);

    // New run: 200 (never 201), dry_run, empty run id.
    let (st, res) = start(
        &s,
        "sdk-basic",
        json!({"input": {"name": "d", "n": 1}, "dry_run": true, "idempotency_key": "dk-new"}),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{res}");
    assert_eq!(res, json!({"run_id": "", "created": true, "dry_run": true}));
    // Existing key: the run it maps to; reject policy: 409 with the existing id.
    let mut dry = body.clone();
    dry["dry_run"] = json!(true);
    let (st, res) = start(&s, "sdk-basic", dry.clone()).await;
    assert_eq!(st, StatusCode::OK, "{res}");
    assert_eq!(
        res,
        json!({"run_id": existing, "created": false, "dry_run": true})
    );
    dry["conflict_policy"] = json!("reject");
    let (st, res) = start(&s, "sdk-basic", dry).await;
    assert_eq!(st, StatusCode::CONFLICT, "{res}");
    assert_eq!(res["error"]["details"]["existing_run_id"], existing);
    // The same 400 / 404 as a real start.
    for (wf, b, want) in [
        ("nope", json!({"dry_run": true}), StatusCode::NOT_FOUND),
        (
            "sdk-basic",
            json!({"dry_run": true, "version": 9}),
            StatusCode::NOT_FOUND,
        ),
        (
            "sdk-basic",
            json!({"dry_run": true, "tenant": "_"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            "sdk-basic",
            json!({"dry_run": true, "queue": "x:y"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            "sdk-basic",
            json!({"dry_run": true, "retention": "7d"}),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let (st, res) = start(&s, wf, b.clone()).await;
        assert_eq!(st, want, "{b}: {res}");
    }
    // A large input is validated but no blob is written.
    let big = "x".repeat(300 * 1024);
    let (st, res) = start(
        &s,
        "sdk-echo",
        json!({"input": {"big": big}, "dry_run": true}),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{res}");

    // gRPC.
    let r = runs
        .start(pb::StartRunRequest {
            workflow: "sdk-basic".into(),
            idempotency_key: Some("dk-1".into()),
            dry_run: true,
            ..Default::default()
        })
        .await
        .expect("grpc dry run")
        .into_inner();
    assert!(r.dry_run && !r.created);
    assert_eq!(r.run_id, existing);
    let e = runs
        .start(pb::StartRunRequest {
            workflow: "sdk-basic".into(),
            input_json: b"{bad".to_vec(),
            dry_run: true,
            ..Default::default()
        })
        .await
        .expect_err("bad json");
    assert_eq!(e.code(), Code::InvalidArgument);

    // Nothing was created: same run list, no blob, no dedupe row for the new key.
    assert_eq!(run_count(&s).await, runs_before);
    assert_eq!(blob_count(&s), blobs_before);
    let count = PartitionCount::new(ServerOpts::default().partitions).expect("count");
    let p = partition_for_key("sdk-basic", b"dk-new", count);
    let row = s
        .h()
        .node()
        .handle(p)
        .expect("partition")
        .reader()
        .get(Table::Dedupe, &dedupe_key("sdk-basic", b"dk-new"))
        .expect("read dedupe");
    assert!(row.is_none(), "a dry run must not write a dedupe row");
    // ... and the key is still free for a real start.
    let (st, res) = start(
        &s,
        "sdk-basic",
        json!({"input": {"name": "d", "n": 1}, "idempotency_key": "dk-new"}),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{res}");
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_run_completes_and_tenant_policy_applies() {
    // 05-24 hand-off: since 05-11 strips the tenant prefix for the journal, a tenant
    // upload + tenant start runs to completion.
    let mut s = server().await;
    let wasm = guest_wasm("sdk-basic");
    let sha = sha_hex(&wasm);
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{sha}?tenant=acme")))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "{}", r.status());
    // `acme/sdk-basic` as one path segment (%2F) addresses the tenant's policy.
    let (st, body) = put_policy(
        &s,
        "/v1/workflows/acme%2Fsdk-basic/policy",
        json!({"retention": "5h"}),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let (st, body) = start(
        &s,
        "sdk-basic",
        json!({"input": {"name": "t", "n": 2}, "tenant": "acme"}),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
    let run = wait_terminal(&s, body["run_id"].as_str().expect("id"), LIMIT).await;
    assert_eq!(run["status"], "completed", "{run}");
    assert_eq!(run["workflow"], "acme/sdk-basic");
    assert_eq!(run["queue"], "acme/default");
    assert_eq!(run["tenant"], "acme");
    assert_eq!(run["result"], basic_result("t", 2));
    assert_eq!(run["retention_ms"], json!(5 * H), "{run}");
    // The default tenant's workflow of the same name has no policy (none uploaded: 404).
    let (st, _) = get_path(&s, "/v1/workflows/sdk-basic/policy").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_reads_published_status_in_one_row() {
    let mut s = server().await;
    deploy(&s, "sdk-basic").await;
    let wasm = guest_wasm("trivial-steps");
    let sha = sha_hex(&wasm);
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{sha}")))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success());
    let reads = || s.h().control().status_rows_read();

    // A run that published a status: exactly one journal row read per GET.
    let (_, body) = start(&s, "sdk-basic", json!({"input": {"name": "s", "n": 1}})).await;
    let id = body["run_id"].as_str().expect("id").to_owned();
    wait_terminal(&s, &id, LIMIT).await;
    let before = reads();
    let (_, run) = get(&s, &id).await;
    assert_eq!(run["published_status"], json!({"stage": "done"}));
    assert_eq!(reads() - before, 1, "GET must read only the status_seq row");

    // A ~10k-event run: GET is not a journal scan (no status = no row read at all).
    let (_, body) = start(&s, "trivial-steps", json!({"input": {"n": 10000}})).await;
    let id = body["run_id"].as_str().expect("id").to_owned();
    let run = wait_terminal(&s, &id, Duration::from_secs(120)).await;
    assert_eq!(run["status"], "completed", "{run}");
    let rid: RunId = id.parse().expect("run id");
    let journal = s.journal(&rid).await;
    assert!(
        journal.len() >= 10_000,
        "journal has {} events",
        journal.len()
    );
    let before = reads();
    let t = Instant::now();
    let (_, run) = get(&s, &id).await;
    assert_eq!(run["published_status"], Value::Null);
    assert_eq!(reads() - before, 0);
    assert!(
        t.elapsed() < Duration::from_secs(1),
        "GET took {:?}",
        t.elapsed()
    );
    s.shutdown().await;
}
