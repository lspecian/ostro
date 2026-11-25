//! The executor's `module_gone` path (13-17; CLUS-07, D-P13-20/21) on the real single-node
//! server with `guests/p10-replay-v{1,3}` (`replay_demo`):
//!
//! - a sleeping run whose module record is deleted (module GC's deletion, done here
//!   directly through meta to model the residual start-during-GC race) fails on its next
//!   activation with the non-retryable reason `module_gone`, although the blob is still on
//!   disk; a retry that restarts it on the same module fails the same way;
//! - a run on a module whose record exists is unaffected;
//! - a record that (re)appears within `MODULE_GONE_CONFIRM` (a node whose metadata view
//!   trails the leader) does not fail the run.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer};
use ostro_meta::{MetaCommand, MetaKey, MetaRecord, MetaResult};
use ostro_partition::store::values::RunStatus;
use ostro_server::executor::REASON_MODULE_GONE;
use ostro_types::RunId;
use ostro_types::journal::EventKind;
use reqwest::StatusCode;
use serde_json::{Value, json};

const LIMIT: Duration = Duration::from_secs(90);

async fn server() -> TestServer {
    let s = TestServer::start_with(ServerOpts::default(), |c| {
        c.engine.hot_wait = Duration::from_millis(50).into();
    })
    .await;
    s.wait_ready(Duration::from_secs(30)).await;
    s
}

async fn activate(s: &TestServer, name: &str, version: u32) {
    let r = s
        .http
        .put(s.url(&format!("/v1/workflows/{name}/active")))
        .json(&json!({ "version": version }))
        .send()
        .await
        .expect("PUT active");
    assert!(r.status().is_success(), "activate {name} v{version}");
}

/// Delete a meta record at its current version.
async fn delete(s: &TestServer, key: MetaKey) {
    let meta = s.h().meta();
    let v = meta
        .view()
        .read(|st| st.get(&key).map(|r| r.version))
        .expect("record exists");
    let r = meta
        .propose(MetaCommand::Delete {
            key,
            expected_version: v,
        })
        .await
        .expect("propose");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
}

/// What module GC deletes: the workflow version, then the module record.
async fn gc_delete(s: &TestServer, sha: [u8; 32]) {
    delete(s, MetaKey::Workflow("replay_demo".into(), 1)).await;
    delete(s, MetaKey::Module(sha)).await;
}

async fn sleeping(s: &TestServer, version: Option<u32>, sleep_s: u64) -> RunId {
    let mut body = json!({"input": {"sleep_s": sleep_s}});
    if let Some(v) = version {
        body["version"] = json!(v);
    }
    let run = s.start_rest("replay_demo", body).await;
    let deadline = Instant::now() + LIMIT;
    while !s
        .history_kinds(&run)
        .await
        .contains(&EventKind::TimerStarted)
    {
        assert!(Instant::now() < deadline, "never slept");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    s.wait_status(&run, RunStatus::Scheduled, LIMIT).await;
    run
}

async fn failure(s: &TestServer, run: &RunId) -> Value {
    let rec = s.wait_record(run, LIMIT, |r| r.status.is_terminal()).await;
    assert_eq!(rec.status, RunStatus::Failed, "{run}");
    assert!(!rec.resumable, "module_gone is not resumable");
    serde_json::from_slice(&s.result_bytes(run).await.expect("failure payload")).expect("json")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleted_module_record_fails_the_run_with_module_gone() {
    let s = server().await;
    let v1 = s.deploy_guest("p10-replay-v1", None).await;
    let _v3 = s.deploy_guest("p10-replay-v3", None).await;
    let doomed = sleeping(&s, Some(1), 3).await;
    // Control: a run on v3, whose record stays.
    let fine = sleeping(&s, Some(3), 3).await;

    activate(&s, "replay_demo", 3).await;
    gc_delete(&s, v1).await;
    let blob = s
        .h()
        .node()
        .handle(doomed.partition())
        .expect("handle")
        .blobs()
        .path_of(&ostro_blob::BlobRef { sha256: v1, len: 0 });
    assert!(blob.is_file(), "the blob is still on disk");

    let f = failure(&s, &doomed).await;
    assert_eq!(f["reason_code"], REASON_MODULE_GONE, "{f}");
    assert!(
        f["message"].as_str().unwrap_or("").contains("is gone"),
        "{f}"
    );
    let (st, v) = s.get_json(&format!("/v1/runs/{doomed}")).await;
    assert_eq!(st, StatusCode::OK);
    assert!(
        v.to_string().contains(REASON_MODULE_GONE),
        "API shows it: {v}"
    );

    let rec = s.wait_status(&fine, RunStatus::Completed, LIMIT).await;
    assert!(rec.module_sha.is_some());

    // A retry restarts it pinned to the same module (D1 restart for a non-resumable
    // failure): the new run fails module_gone too, never silently runs the stale blob.
    let (st, v) = s
        .post_json(&format!("/v1/runs/{doomed}/retry"), &json!({}))
        .await;
    if st == StatusCode::OK
        && let Some(id) = v["run_id"].as_str().filter(|id| *id != doomed.to_string())
    {
        let again: RunId = id.parse().expect("run id");
        let rec = s.run_record(&again);
        if rec.module_sha == Some(v1) {
            let f = failure(&s, &again).await;
            assert_eq!(f["reason_code"], REASON_MODULE_GONE, "restart: {f}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn record_reappearing_within_the_confirm_window_does_not_fail() {
    let s = server().await;
    let v1 = s.deploy_guest("p10-replay-v1", None).await;
    let meta = s.h().meta();
    let saved = meta
        .view()
        .read(|st| st.module(&v1).map(|m| m.value.clone()))
        .expect("module record");
    let run = sleeping(&s, None, 2).await;
    // Model a trailing metadata view: the record vanishes just before the wake-up and
    // comes back about 0.5 s after it (inside MODULE_GONE_CONFIRM).
    delete(&s, MetaKey::Module(v1)).await;
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let r = meta
        .propose(MetaCommand::Put {
            key: MetaKey::Module(v1),
            expected_version: None,
            record: MetaRecord::Module(saved),
        })
        .await
        .expect("propose");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    let rec = s.wait_record(&run, LIMIT, |r| r.status.is_terminal()).await;
    assert_eq!(
        rec.status,
        RunStatus::Completed,
        "{:?}",
        rec.terminal_reason
    );
}
