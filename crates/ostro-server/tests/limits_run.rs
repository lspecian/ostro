//! Run limits end to end (plan 11-06, OPS-07 / D-P11-13), with the `guests/p11-limits`
//! component.
//!
//! - Embedded: with `[limits] max_history_events = 20`, `limits-history(30)` ends `Failed`,
//!   `resumable`, with the failure message `limit_exceeded: history_events 21 > 20`. After
//!   a restart with the default limits, `POST /v1/runs/{id}/retry` resumes the same run and
//!   it completes.
//! - Worker-executed: the same run on a `workers` queue fails the same way (the worker
//!   adopts the run limits from `Welcome.limits`).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p11.rs"]
mod p11;
#[path = "common/p7_workers.rs"]
mod p7_workers;

use std::sync::Arc;
use std::time::Duration;

use ostro_meta::records::PLACEMENT_WORKERS;
use ostro_meta::{MetaCommand, MetaRecord, MetaResult, QueuePolicy};
use ostro_partition::store::values::{RunRecord, RunStatus};
use ostro_types::RunId;
use ostro_types::journal::EventKind;
use p7_workers::ServerWorkers;
use p11::P11Server;
use reqwest::StatusCode;
use serde_json::{Value, json};

const LIMIT: Duration = Duration::from_secs(90);
const GUEST: &str = "p11-limits";
const REASON: &str = "limit_exceeded: history_events 21 > 20";

async fn small_history_server() -> P11Server {
    let s = P11Server::builder()
        .with_limits(|l| l.max_history_events = 20)
        .start()
        .await;
    s.wait_ready(Duration::from_secs(20)).await;
    s
}

async fn terminal(s: &P11Server, run: &RunId) -> RunRecord {
    s.wait_record(run, LIMIT, |r| r.status.is_terminal()).await
}

async fn failure(s: &P11Server, run: &RunId) -> Value {
    let bytes = s.result_bytes(run).await.expect("failure result");
    serde_json::from_slice(&bytes).expect("failure JSON")
}

async fn assert_limit_failure(s: &P11Server, run: &RunId) {
    let rec = terminal(s, run).await;
    assert_eq!(rec.status, RunStatus::Failed, "{rec:?}");
    assert!(
        rec.resumable,
        "a limit breach fails the run resumably: {rec:?}"
    );
    let f = failure(s, run).await;
    assert_eq!(f["reason_code"], "limit_exceeded", "{f}");
    let msg = f["message"].as_str().expect("message");
    assert!(msg.starts_with(REASON), "{f}");
    let journal = s.journal(run).await;
    assert_eq!(journal.len(), 20, "the breaching event is never journaled");
    assert_eq!(common::count(&journal, EventKind::StepCompleted), 19);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn history_breach_fails_resumably_then_raise_and_retry_completes() {
    let mut s = small_history_server().await;
    s.deploy_guest(GUEST, None).await;
    let run = s
        .start_rest("limits-history", json!({"input": {"n": 30}}))
        .await;
    assert_limit_failure(&s, &run).await;

    // Raise the limit (back to the default) and restart on the same data dir.
    s.cfg.limits.max_history_events = ostro_engine::RunLimits::default().max_history_events;
    s.restart().await;
    s.wait_ready(Duration::from_secs(20)).await;
    let id = run.to_string();
    let (st, v) = s
        .post_json(&format!("/v1/runs/{id}/retry"), &json!({"dry_run": false}))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["mode"], "resumed", "{v}");
    assert_eq!(v["run_id"], id.as_str(), "resumed in place, no new run id");
    let rec = terminal(&s, &run).await;
    assert_eq!(rec.status, RunStatus::Completed, "{rec:?}");
    assert_eq!(rec.resumed_count, 1);
    let out: Value =
        serde_json::from_slice(&s.result_bytes(&run).await.expect("result")).expect("json");
    assert_eq!(out, json!(30));
    assert_eq!(s.journal(&run).await.len(), 32);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_executed_run_fails_the_same_way() {
    let mut s = small_history_server().await;
    let sha = s.deploy_guest(GUEST, None).await;
    let meta = s.h().meta();
    let record = MetaRecord::QueuePolicy(QueuePolicy {
        queue: "wq".into(),
        placement: Some(PLACEMENT_WORKERS),
        ..QueuePolicy::default()
    });
    let key = record.key();
    let expected_version = meta.view().read(|st| st.get(&key).map(|v| v.version));
    let r = meta
        .propose(MetaCommand::Put {
            key,
            expected_version,
            record,
        })
        .await
        .expect("meta put");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    let ws = ServerWorkers::new(Arc::clone(s.h().p7())).await;
    let mut w = ws.spawn("w1", 2, &[]).await;

    let run = s
        .start_raw("limits-history", 1, br#"{"n": 30}"#, &sha, |st| {
            st.queue = "wq".into()
        })
        .await;
    assert_limit_failure(&s, &run).await;
    // It really ran on the worker.
    let id = run.to_string();
    w.wait_event(p7_workers::T, |e| {
        matches!(e, ostro_worker::WorkerEvent::ActivationStarted { run_id, .. } if *run_id == id)
    })
    .await;
    drop(w);
    drop(ws);
    s.shutdown().await;
}
