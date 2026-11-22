//! Server-side replay (plan 10-11, CLI-07, D-P10-7): `POST /v1/modules/{sha}/replay` on
//! the real in-process server with `guests/p10-replay-v{1,2,3}` (`replay_demo`: v1 steps
//! `a`, `b`; v2 swaps them; v3 swaps them behind `patched("reorder")`).
//!
//! - v1 deployed (first version: active), 3 runs completed and 1 left sleeping;
//! - v2 uploaded (not activated): `sample: 10` → every run diverged, each with the seq of
//!   v1's first step; the module stays inactive and no run changes;
//! - v3 uploaded: all `ok` (completed) / `ok_frontier` (the sleeping run);
//! - explicit runs: verdicts in request order; an unknown run id is 404 `not_found`;
//! - a module without `replay_demo` → every run `skipped`;
//! - an unknown module is 404, `sample: 0` and a malformed run id are 400;
//! - the response carries no journal data (only the documented fields).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer};
use ostro_partition::store::values::RunStatus;
use ostro_types::RunId;
use ostro_types::journal::{EventBody, EventKind};
use reqwest::StatusCode;
use serde_json::{Value, json};

const LIMIT: Duration = Duration::from_secs(60);

fn hex(sha: &[u8; 32]) -> String {
    sha.iter().map(|b| format!("{b:02x}")).collect()
}

async fn replay(s: &TestServer, sha: &str, body: Value) -> (StatusCode, Value) {
    s.post_json(&format!("/v1/modules/{sha}/replay"), &body)
        .await
}

fn verdicts(v: &Value) -> &Vec<Value> {
    v["verdicts"]
        .as_array()
        .unwrap_or_else(|| panic!("verdicts: {v}"))
}

/// The seq of the first `StepCompleted` (v1's step `a`) of `run`.
async fn first_step_seq(s: &TestServer, run: &RunId) -> u64 {
    s.journal(run)
        .await
        .iter()
        .find(|e| matches!(e.body, EventBody::StepCompleted(_)))
        .map(|e| e.seq)
        .expect("a StepCompleted")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_endpoint_end_to_end() {
    let s = TestServer::start(ServerOpts::default()).await;
    s.wait_ready(Duration::from_secs(20)).await;

    // v1: the first version of replay_demo auto-activates.
    s.deploy_guest("p10-replay-v1", None).await;
    let mut done = Vec::new();
    for _ in 0..3 {
        let run = s.start_rest("replay_demo", json!({"input": {}})).await;
        done.push(run);
    }
    for run in &done {
        s.wait_status(run, RunStatus::Completed, LIMIT).await;
    }
    // One run left in progress (sleeping 10 min after both steps).
    let sleeping = s
        .start_rest("replay_demo", json!({"input": {"sleep_s": 600}}))
        .await;
    let deadline = Instant::now() + LIMIT;
    while !s
        .history_kinds(&sleeping)
        .await
        .contains(&EventKind::TimerStarted)
    {
        assert!(Instant::now() < deadline, "the sleeping run never slept");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let want_seq = first_step_seq(&s, &done[0]).await;

    // v2 (steps swapped, no patch): every run diverged at v1's first step.
    let v2 = hex(&s.deploy_guest("p10-replay-v2", None).await);
    let (st, v) = replay(&s, &v2, json!({"sample": 10})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["module_sha256"], v2.as_str());
    assert_eq!(verdicts(&v).len(), 4, "{v}");
    assert_eq!(v["summary"]["diverged"], 4, "{v}");
    for r in verdicts(&v) {
        assert_eq!(r["verdict"], "diverged", "{r}");
        assert_eq!(r["workflow"], "replay_demo", "{r}");
        assert_eq!(r["seq"], want_seq, "{r}");
        let d = r["detail"].as_str().expect("detail");
        assert!(d.contains("expected") && d.contains("actual"), "{d}");
        // Only the documented fields: no journal, payload or key material.
        let mut keys: Vec<&str> = r
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort();
        assert_eq!(
            keys,
            ["appended", "detail", "run_id", "seq", "verdict", "workflow"],
            "{r}"
        );
    }
    // Replaying changed nothing: v1 is still active and the sleeping run still runs.
    let (_, wfs) = s.get_json("/v1/workflows").await;
    let wf = wfs["workflows"]
        .as_array()
        .and_then(|ws| ws.iter().find(|w| w["name"] == "replay_demo"))
        .unwrap_or_else(|| panic!("replay_demo in {wfs}"));
    assert_eq!(wf["active_version"], 1, "{wf}");
    assert!(!s.run_record(&sleeping).status.is_terminal());
    assert_eq!(s.run_record(&done[0]).status, RunStatus::Completed);

    // v3 (the swap behind patched("reorder")): ok / ok_frontier.
    let v3 = hex(&s.deploy_guest("p10-replay-v3", None).await);
    let (st, v) = replay(&s, &v3, json!({"sample": 10, "workflow": "replay_demo"})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["summary"]["ok"], 3, "{v}");
    assert_eq!(v["summary"]["ok_frontier"], 1, "{v}");
    assert_eq!(v["summary"]["diverged"], 0, "{v}");
    for r in verdicts(&v) {
        let want = if r["run_id"] == sleeping.to_string().as_str() {
            "ok_frontier"
        } else {
            "ok"
        };
        assert_eq!(r["verdict"], want, "{r}");
        assert!(r.get("seq").is_none_or(Value::is_null), "{r}");
    }
    // status filter: only the completed runs.
    let (st, v) = replay(&s, &v3, json!({"sample": 10, "status": "completed"})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["summary"]["ok"], 3, "{v}");
    assert_eq!(verdicts(&v).len(), 3, "{v}");
    // sample 2: the newest two.
    let (_, v) = replay(&s, &v3, json!({"sample": 2})).await;
    assert_eq!(verdicts(&v).len(), 2, "{v}");

    // Explicit runs: request order kept.
    let ids = vec![
        done[2].to_string(),
        sleeping.to_string(),
        done[0].to_string(),
    ];
    let (st, v) = replay(&s, &v2, json!({"runs": ids})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let got: Vec<&str> = verdicts(&v)
        .iter()
        .map(|r| r["run_id"].as_str().expect("run_id"))
        .collect();
    assert_eq!(got, ids);
    assert_eq!(v["summary"]["diverged"], 3, "{v}");

    // An unknown (well-formed) run id: 404 not_found (10-05 contract).
    let unknown = RunId::new(done[0].partition(), uuid::Uuid::now_v7()).to_string();
    let (st, v) = replay(&s, &v3, json!({"runs": [done[0].to_string(), unknown]})).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");
    assert_eq!(v["error"]["code"], "not_found", "{v}");
    let (st, v) = replay(&s, &v3, json!({"runs": ["not-a-run-id"]})).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");

    // A module that does not export replay_demo: skipped.
    let other = hex(&s.deploy_guest("sdk-basic", None).await);
    let (st, v) = replay(
        &s,
        &other,
        json!({"runs": [done[0].to_string(), done[1].to_string()]}),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["summary"]["skipped"], 2, "{v}");
    for r in verdicts(&v) {
        assert_eq!(r["verdict"], "skipped", "{r}");
        assert_eq!(r["detail"], "workflow not exported by module", "{r}");
    }
    // ... and a sample over its own workflows finds none of the replay_demo runs.
    let (st, v) = replay(&s, &other, json!({"sample": 5})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert!(verdicts(&v).is_empty(), "{v}");

    // Unknown module, sample 0, both selectors.
    let (st, v) = replay(&s, &"a".repeat(64), json!({"sample": 1})).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");
    let (st, v) = replay(&s, &v2, json!({"sample": 0})).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    let (st, v) = replay(&s, &v2, json!({"sample": 1, "runs": [done[0].to_string()]})).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
}
