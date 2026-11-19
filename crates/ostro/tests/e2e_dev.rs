//! Phase 4 end to end through the real binary (plan 04-20): `ostro dev` is spawned, a
//! Rust SDK component built with plain `cargo build --target wasm32-wasip2` is uploaded
//! by sha256, and runs are started over HTTP/JSON and gRPC on the same port.
//!
//! - `upload_start_rest_and_grpc_history_list`: `/openapi.json`, upload + no-op
//!   re-upload, one run per transport with typed JSON results, GET parity across
//!   transports, idempotent start (`return_existing` / `reject` over both transports),
//!   history and filtered lists.
//! - `kill9_mid_run_restart_completes_and_metadata_persists`: SIGKILL while a step waits
//!   for its requeued retry, restart on the same data dir; the run completes, completed
//!   steps are replayed (one `step_completed` each) and module/workflow/active metadata
//!   is still there.
//! - `activate_v2_new_runs_use_v2`: a new active version applies to new runs only.
#![cfg(unix)]

mod common;

use std::time::{Duration, Instant};

use common::{
    Api, SERIAL, Spawned, dev, events_of, free_addr, guest_wasm, http, sha_hex, step_completions,
};
use ostro_proto::api::v1 as pb;
use reqwest::StatusCode;
use serde_json::{Value, json};
use tonic::Code;

/// Generous wait for a run to finish (debug build, loaded machine).
const RUN_LIMIT: Duration = Duration::from_secs(30);

fn basic_result(name: &str, n: u64) -> Value {
    json!({ "greeting": format!("hello, {name}"), "sum": n * (n + 1) / 2, "steps": 2 })
}

/// `ostro dev` with the given args, P=4 (fast opens), quiet logs; waits for `/readyz`.
async fn spawn_dev(cwd: &std::path::Path, args: &[&str]) -> Spawned {
    let mut cmd = dev(cwd, args);
    cmd.env("OSTRO_NODE__PARTITIONS", "4")
        .env("OSTRO_LOG__LEVEL", "warn");
    let s = Spawned::spawn(cmd);
    let addr = args
        .iter()
        .position(|a| *a == "--listen")
        .map(|i| args[i + 1].parse().expect("listen addr"))
        .expect("--listen given");
    s.wait_readyz(&http(), addr).await;
    s
}

/// A data dir on tmpfs when available (fast fsync), else the OS temp dir.
fn data_dir() -> tempfile::TempDir {
    let shm = std::path::Path::new("/dev/shm");
    if shm.is_dir()
        && let Ok(d) = tempfile::Builder::new()
            .prefix("ostro-e2e-")
            .tempdir_in(shm)
    {
        return d;
    }
    tempfile::Builder::new()
        .prefix("ostro-e2e-")
        .tempdir()
        .expect("tempdir")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_start_rest_and_grpc_history_list() {
    let wasm = guest_wasm("sdk-basic");
    let sha = sha_hex(&wasm);
    let _serial = SERIAL.lock().await;
    let cwd = tempfile::tempdir().expect("cwd");
    let addr = free_addr().to_string();
    let mut srv = spawn_dev(cwd.path(), &["--ephemeral", "--listen", &addr]).await;
    let api = Api::new(addr.parse().expect("addr"));

    // The served OpenAPI document lists the module upload route.
    let (st, doc) = api.get("/openapi.json").await;
    assert_eq!(st, StatusCode::OK);
    assert!(
        doc["openapi"].as_str().is_some_and(|v| v.starts_with('3')),
        "{doc}"
    );
    assert!(doc["paths"]["/v1/modules/{sha256}"].is_object());
    assert!(doc["paths"]["/v1/workflows/{name}/runs"].is_object());

    // Upload by sha256: 201, then a no-op 200.
    let (st, body) = api.upload(&wasm).await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
    assert_eq!(body["created"], true);
    assert_eq!(body["module"]["sha256"], sha);
    assert_eq!(
        body["module"]["workflows"],
        json!([{"name": "sdk-basic", "version": 1}, {"name": "sdk-echo", "version": 1}])
    );
    let (st, body) = api.upload(&wasm).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["created"], false);

    // Run #1 over HTTP/JSON.
    let t0 = Instant::now();
    let run1 = api
        .start_new(
            "sdk-basic",
            json!({"input": {"name": "http", "n": 10}, "tags": ["http", "e2e"]}),
        )
        .await;
    // Run #2 over gRPC on the same address.
    let res = api
        .start_grpc(pb::StartRunRequest {
            workflow: "sdk-basic".into(),
            input_json: br#"{"name":"grpc","n":4}"#.to_vec(),
            tags: vec!["grpc".into(), "e2e".into()],
            ..Default::default()
        })
        .await
        .expect("grpc start");
    assert!(res.created);
    let run2 = res.run_id;
    assert_ne!(run1, run2);

    let r1 = api.wait_terminal(&run1, RUN_LIMIT).await;
    let r2 = api.wait_terminal(&run2, RUN_LIMIT).await;
    println!("two sdk-basic runs completed in {:?}", t0.elapsed());
    assert_eq!(r1["status"], "completed", "{r1}");
    assert_eq!(r2["status"], "completed", "{r2}");
    assert_eq!(r1["result"], basic_result("http", 10));
    assert_eq!(r2["result"], basic_result("grpc", 4));
    for r in [&r1, &r2] {
        assert_eq!(r["workflow"], "sdk-basic");
        assert_eq!(r["version"], 1);
        assert_eq!(r["module_sha256"], sha);
        assert_eq!(r["published_status"], json!({"stage": "done"}));
        assert_eq!(r["failure"], Value::Null);
    }

    // GET over gRPC of the HTTP-started run equals GET over HTTP.
    let g1 = api.get_grpc(&run1).await;
    assert_eq!(g1.run_id, run1);
    assert_eq!(g1.workflow, r1["workflow"].as_str().expect("workflow"));
    assert_eq!(
        u64::from(g1.version),
        r1["version"].as_u64().expect("version")
    );
    assert_eq!(g1.module_sha256, sha);
    assert_eq!(g1.queue, r1["queue"].as_str().expect("queue"));
    assert_eq!(g1.status, pb::RunStatus::Completed as i32);
    assert_eq!(g1.attempt.map(u64::from), r1["attempt"].as_u64());
    assert_eq!(g1.tags, vec!["http".to_owned(), "e2e".to_owned()]);
    assert_eq!(json!(g1.tags), r1["tags"]);
    assert_eq!(
        i64::from(g1.priority),
        r1["priority"].as_i64().expect("priority")
    );
    assert_eq!(Some(g1.created_at_ms), r1["created_at_ms"].as_u64());
    assert_eq!(g1.completed_at_ms, r1["completed_at_ms"].as_u64());
    let g1_result: Value =
        serde_json::from_slice(g1.result_json.as_deref().expect("result_json")).expect("json");
    assert_eq!(g1_result, r1["result"]);
    let g1_status: Value =
        serde_json::from_slice(g1.published_status_json.as_deref().expect("status")).expect("json");
    assert_eq!(g1_status, r1["published_status"]);
    assert_eq!(g1.failure, None);

    // Idempotency over HTTP: same key -> same run (200, created false); reject -> 409.
    let body = json!({"input": {"name": "idem", "n": 2}, "idempotency_key": "e2e-key"});
    let (st, first) = api.start_http("sdk-basic", body.clone()).await;
    assert_eq!(st, StatusCode::CREATED, "{first}");
    let idem = first["run_id"].as_str().expect("run_id").to_owned();
    let (st, again) = api.start_http("sdk-basic", body.clone()).await;
    assert_eq!(st, StatusCode::OK, "{again}");
    assert_eq!(again["created"], false);
    assert_eq!(again["run_id"], idem);
    let mut reject = body.clone();
    reject["conflict_policy"] = json!("reject");
    let (st, err) = api.start_http("sdk-basic", reject).await;
    assert_eq!(st, StatusCode::CONFLICT, "{err}");
    assert_eq!(err["error"]["details"]["existing_run_id"], idem);
    // ... and over gRPC: return_existing then ALREADY_EXISTS with existing_run_id.
    let dup = pb::StartRunRequest {
        workflow: "sdk-basic".into(),
        input_json: br#"{"name":"idem","n":2}"#.to_vec(),
        idempotency_key: Some("e2e-key".into()),
        ..Default::default()
    };
    let same = api.start_grpc(dup.clone()).await.expect("grpc dup");
    assert!(!same.created);
    assert_eq!(same.run_id, idem);
    let e = api
        .start_grpc(pb::StartRunRequest {
            conflict_policy: pb::ConflictPolicy::Reject as i32,
            ..dup
        })
        .await
        .expect_err("reject");
    assert_eq!(e.code(), Code::AlreadyExists);
    let details = ostro_api::grpc::status_details(&e).expect("details");
    assert_eq!(details["existing_run_id"], idem);
    let ri = api.wait_terminal(&idem, RUN_LIMIT).await;
    assert_eq!(ri["result"], basic_result("idem", 2));

    // History of run #1: run_started first, run_completed last, two steps, over REST
    // and gRPC.
    let h1 = api.history(&run1).await;
    assert_eq!(h1.first().expect("events")["kind"], "run_started");
    assert_eq!(h1[0]["details"]["input"], json!({"name": "http", "n": 10}));
    let last = h1.last().expect("events");
    assert_eq!(last["kind"], "run_completed");
    assert_eq!(last["details"]["output"], basic_result("http", 10));
    assert_eq!(step_completions(&h1, "greet"), 1);
    assert_eq!(step_completions(&h1, "add"), 1);
    assert_eq!(events_of(&h1, "step_completed").len(), 2);
    let gh = api
        .runs()
        .await
        .history(pb::RunHistoryRequest {
            run_id: run1.clone(),
            from_seq: 0,
            limit: 0,
        })
        .await
        .expect("grpc history")
        .into_inner();
    assert_eq!(gh.next_from_seq, None);
    assert_eq!(
        gh.events.iter().map(|e| e.kind.clone()).collect::<Vec<_>>(),
        h1.iter()
            .map(|e| e["kind"].as_str().expect("kind").to_owned())
            .collect::<Vec<_>>()
    );

    // Lists: workflow + status returns every completed run; the tag filter narrows.
    let completed = api.list("workflow=sdk-basic&status=completed").await;
    for id in [&run1, &run2, &idem] {
        assert!(completed.contains(id), "{id} in {completed:?}");
    }
    assert_eq!(completed.len(), 3, "{completed:?}");
    assert_eq!(api.list("tag=http").await, vec![run1.clone()]);
    assert_eq!(api.list("tag=grpc").await, vec![run2.clone()]);
    let e2e = api.list("tag=e2e").await;
    assert_eq!(e2e.len(), 2, "{e2e:?}");
    assert!(api.list("workflow=sdk-echo").await.is_empty());
    let mut g = api.runs().await;
    let page = g
        .list(pb::ListRunsRequest {
            workflow: Some("sdk-basic".into()),
            status: Some(pb::RunStatus::Completed as i32),
            tag: Some("e2e".into()),
            limit: 10,
            ..Default::default()
        })
        .await
        .expect("grpc list")
        .into_inner();
    let mut ids: Vec<String> = page.runs.into_iter().map(|r| r.run_id).collect();
    ids.sort();
    let mut want = vec![run1.clone(), run2.clone()];
    want.sort();
    assert_eq!(ids, want);

    srv.stop_clean("INT");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill9_mid_run_restart_completes_and_metadata_persists() {
    let basic = guest_wasm("sdk-basic");
    let retry = guest_wasm("sdk-retry");
    let (basic_sha, retry_sha) = (sha_hex(&basic), sha_hex(&retry));
    let _serial = SERIAL.lock().await;
    let cwd = tempfile::tempdir().expect("cwd");
    let data = data_dir();
    let dir = data.path().to_str().expect("utf-8 dir").to_owned();

    let addr = free_addr().to_string();
    let mut srv = spawn_dev(cwd.path(), &["--data-dir", &dir, "--listen", &addr]).await;
    let api = Api::new(addr.parse().expect("addr"));
    assert_eq!(api.upload(&basic).await.0, StatusCode::CREATED);
    assert_eq!(api.upload(&retry).await.0, StatusCode::CREATED);
    let done = api
        .start_new(
            "sdk-basic",
            json!({"input": {"name": "before-kill", "n": 3}}),
        )
        .await;
    let done_run = api.wait_terminal(&done, RUN_LIMIT).await;
    assert_eq!(done_run["result"], basic_result("before-kill", 3));

    // long_backoff: `before` completes, `slow` fails once and is requeued for 3 s.
    let run = api
        .start_new("sdk-retry", json!({"input": {"mode": "long_backoff"}}))
        .await;
    let deadline = Instant::now() + RUN_LIMIT;
    loop {
        let h = api.history(&run).await;
        if !events_of(&h, "step_attempt_failed").is_empty() {
            assert_eq!(step_completions(&h, "before"), 1);
            assert_eq!(
                step_completions(&h, "slow"),
                0,
                "still waiting for the retry"
            );
            break;
        }
        assert!(Instant::now() < deadline, "no step_attempt_failed: {h:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // SIGKILL mid-run (no graceful shutdown), then restart on the same data dir.
    srv.signal("KILL");
    let st = srv.wait_exit(common::T);
    assert!(!st.success(), "killed: {st:?}");
    drop(srv);

    let addr2 = free_addr().to_string();
    let mut srv = spawn_dev(cwd.path(), &["--data-dir", &dir, "--listen", &addr2]).await;
    let api = Api::new(addr2.parse().expect("addr"));
    let fin = api.wait_terminal(&run, RUN_LIMIT).await;
    assert_eq!(fin["status"], "completed", "{fin}");
    assert_eq!(
        fin["result"],
        json!({"mode": "long_backoff", "before": 1, "attempts": null})
    );
    assert_eq!(fin["module_sha256"], retry_sha);

    // Completed steps were replayed, not re-executed.
    let h = api.history(&run).await;
    assert_eq!(step_completions(&h, "before"), 1, "{h:?}");
    assert_eq!(step_completions(&h, "slow"), 1, "{h:?}");
    // `slow` measures `ctx.now() - t0`. Logical time inside a step is the timestamp of
    // the last journaled command, so requeued attempt 2 still sees the time of attempt
    // 1's failure (elapsed ~0) and fails again; attempt 3 sees attempt 2's failure time
    // (~3 s later) and succeeds. Two requeued failures, deterministically.
    let saf = events_of(&h, "step_attempt_failed");
    assert_eq!(saf.len(), 2, "{h:?}");
    for (i, e) in saf.iter().enumerate() {
        assert_eq!(e["details"]["name"], "slow");
        assert_eq!(e["details"]["attempt"], i as u64 + 1);
        assert_eq!(e["details"]["backoff"], "requeue");
    }
    assert_eq!(events_of(&h, "run_started").len(), 1);
    assert_eq!(events_of(&h, "run_completed").len(), 1);

    // Metadata survived the kill: modules, workflow versions, active versions.
    let b = api.workflow("sdk-basic").await;
    assert_eq!(b["active_version"], 1, "{b}");
    assert_eq!(b["versions"][0]["module_sha256"], basic_sha);
    assert_eq!(api.workflow("sdk-echo").await["active_version"], 1);
    let r = api.workflow("sdk-retry").await;
    assert_eq!(r["active_version"], 1, "{r}");
    assert_eq!(r["versions"][0]["module_sha256"], retry_sha);
    for sha in [&basic_sha, &retry_sha] {
        let (st, m) = api.get(&format!("/v1/modules/{sha}")).await;
        assert_eq!(st, StatusCode::OK, "{m}");
    }
    // A re-upload is still a no-op, and the earlier run is unchanged.
    let (st, body) = api.upload(&basic).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["created"], false);
    let again = api.run(&done).await;
    assert_eq!(again["status"], "completed");
    assert_eq!(again["result"], done_run["result"]);
    // New runs still start and complete after the restart.
    let fresh = api
        .start_new(
            "sdk-basic",
            json!({"input": {"name": "after-kill", "n": 5}}),
        )
        .await;
    assert_eq!(
        api.wait_terminal(&fresh, RUN_LIMIT).await["result"],
        basic_result("after-kill", 5)
    );

    srv.stop_clean("TERM");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activate_v2_new_runs_use_v2() {
    let v1 = guest_wasm("sdk-basic");
    let v2 = guest_wasm("sdk-basic-v2");
    let (v1_sha, v2_sha) = (sha_hex(&v1), sha_hex(&v2));
    let _serial = SERIAL.lock().await;
    let cwd = tempfile::tempdir().expect("cwd");
    let addr = free_addr().to_string();
    let mut srv = spawn_dev(cwd.path(), &["--ephemeral", "--listen", &addr]).await;
    let api = Api::new(addr.parse().expect("addr"));

    assert_eq!(api.upload(&v1).await.0, StatusCode::CREATED);
    let a = api
        .start_new("sdk-basic", json!({"input": {"name": "one", "n": 3}}))
        .await;
    let ra = api.wait_terminal(&a, RUN_LIMIT).await;
    assert_eq!(ra["result"], basic_result("one", 3));

    // Uploading v2 does not activate it; activation does.
    let (st, body) = api.upload(&v2).await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
    let wf = api.workflow("sdk-basic").await;
    assert_eq!(wf["active_version"], 1, "{wf}");
    assert_eq!(wf["versions"].as_array().map(Vec::len), Some(2), "{wf}");
    assert_eq!(api.activate("sdk-basic", 2).await, StatusCode::OK);
    assert_eq!(api.workflow("sdk-basic").await["active_version"], 2);

    // A new run without a version uses v2.
    let b = api
        .start_new("sdk-basic", json!({"input": {"name": "two", "n": 3}}))
        .await;
    let rb = api.wait_terminal(&b, RUN_LIMIT).await;
    assert_eq!(rb["status"], "completed", "{rb}");
    assert_eq!(rb["version"], 2);
    assert_eq!(rb["module_sha256"], v2_sha);
    assert_eq!(rb["result"]["version"], 2, "{rb}");
    assert_eq!(rb["result"]["greeting"], "hello, two");

    // The earlier run still reports v1 and the v1 module.
    let ra = api.run(&a).await;
    assert_eq!(ra["version"], 1);
    assert_eq!(ra["module_sha256"], v1_sha);
    assert_eq!(ra["result"], basic_result("one", 3));
    // An explicit version still pins v1.
    let c = api
        .start_new(
            "sdk-basic",
            json!({"input": {"name": "pinned", "n": 1}, "version": 1}),
        )
        .await;
    let rc = api.wait_terminal(&c, RUN_LIMIT).await;
    assert_eq!(rc["version"], 1);
    assert_eq!(rc["result"], basic_result("pinned", 1));

    srv.stop_clean("INT");
}
