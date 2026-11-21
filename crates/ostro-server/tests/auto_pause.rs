//! Auto-pause on exhausted retries (plan 07-08, OPS-01): with `on_retries_exhausted = 1`
//! (pause) on the queue or the workflow policy, a run whose retryable attempt failures are
//! exhausted is paused (`PauseRun{reason: retries_exhausted, detail: failure JSON}`,
//! fenced by the attempt token) instead of failing; the default still fails; the workflow
//! policy wins over the queue policy.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, guest_wasm};
use ostro_meta::records::{ON_EXHAUSTED_FAIL, ON_EXHAUSTED_PAUSE};
use ostro_meta::{MetaCommand, MetaRecord, MetaResult, QueuePolicy, WorkflowPolicy};
use ostro_partition::store::values::{RunRecord, RunStatus};
use ostro_types::RunId;
use sha2::{Digest, Sha256};
use tokio::time::sleep;

const LIMIT: Duration = Duration::from_secs(60);

/// Two attempts, 20 ms backoff: the second trap exhausts the run.
async fn server() -> TestServer {
    let s = TestServer::start_with(ServerOpts::default(), |c| {
        c.executor.max_attempt_failures = 2;
        c.executor.retry_backoff = Duration::from_millis(20).into();
    })
    .await;
    s.wait_ready(Duration::from_secs(10)).await;
    s
}

async fn deploy(s: &TestServer, guest: &str) -> [u8; 32] {
    let wasm = guest_wasm(guest);
    let sha: [u8; 32] = Sha256::digest(&wasm).into();
    let hex: String = sha.iter().map(|b| format!("{b:02x}")).collect();
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{hex}")))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
    sha
}

async fn put(s: &TestServer, record: MetaRecord) {
    let meta = s.h().meta();
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
}

async fn exhaust_on(s: &TestServer, queue: &str, action: u8) {
    put(
        s,
        MetaRecord::QueuePolicy(QueuePolicy {
            queue: queue.into(),
            on_retries_exhausted: Some(action),
            ..QueuePolicy::default()
        }),
    )
    .await;
}

/// A `retry-probe` run in `trap` mode (every attempt panics outside any step).
async fn trap(s: &TestServer, sha: &[u8; 32], queue: &str) -> RunId {
    s.start_raw("retry-probe", 1, b"trap", sha, |st| st.queue = queue.into())
        .await
}

async fn wait_paused(s: &TestServer, run: &RunId) -> RunRecord {
    s.wait_record(run, LIMIT, |r| {
        assert!(
            !r.status.is_terminal(),
            "run ended {:?} instead of pausing",
            r.status
        );
        r.pause.is_some()
    })
    .await
}

async fn wait_failed(s: &TestServer, run: &RunId) -> RunRecord {
    let r = s.wait_record(run, LIMIT, |r| r.status.is_terminal()).await;
    assert_eq!(r.status, RunStatus::Failed, "{r:?}");
    assert!(r.pause.is_none());
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exhausted_retries_auto_pause_under_queue_policy() {
    let mut s = server().await;
    let sha = deploy(&s, "retry-probe").await;
    exhaust_on(&s, "aq", ON_EXHAUSTED_PAUSE).await;

    let run = trap(&s, &sha, "aq").await;
    let rec = wait_paused(&s, &run).await;
    let mark = rec.pause.clone().expect("paused");
    assert_eq!(mark.reason, 1, "reason retries_exhausted");
    let detail: serde_json::Value = serde_json::from_str(&mark.detail).expect("failure JSON");
    assert_eq!(detail["reason_code"], "trap", "{detail}");
    assert_eq!(detail["kind"], "trap", "{detail}");
    // Non-terminal, both attempts used, no result.
    assert!(!rec.status.is_terminal(), "{:?}", rec.status);
    assert_eq!(rec.current_attempt.map(|a| a.1), Some(2));
    assert!(s.result_bytes(&run).await.is_none());
    // No further grants while paused.
    let jobs = s.jobs_of(&run).len();
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_millis(800) {
        sleep(Duration::from_millis(50)).await;
    }
    let later = s.run_record(&run);
    assert_eq!(
        later.current_attempt, rec.current_attempt,
        "granted while paused"
    );
    assert!(later.pause.is_some() && !later.status.is_terminal());
    assert_eq!(s.jobs_of(&run).len(), jobs);
    // The REST view shows the run non-terminal (parents keep waiting).
    let body: serde_json::Value = s
        .http
        .get(s.url(&format!("/v1/runs/{run}")))
        .send()
        .await
        .expect("GET run")
        .json()
        .await
        .expect("json");
    assert!(
        !matches!(
            body["status"].as_str(),
            Some("completed" | "failed" | "cancelled" | "terminated" | "timed_out")
        ),
        "{body}"
    );

    // Without a policy the run fails exactly as before.
    let plain = trap(&s, &sha, "fq").await;
    let rec = wait_failed(&s, &plain).await;
    assert_eq!(rec.current_attempt.map(|a| a.1), Some(2));
    // An explicit `fail` on the queue too.
    exhaust_on(&s, "xq", ON_EXHAUSTED_FAIL).await;
    wait_failed(&s, &trap(&s, &sha, "xq").await).await;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflow_policy_overrides_queue_policy() {
    let mut s = server().await;
    let sha = deploy(&s, "retry-probe").await;
    exhaust_on(&s, "aq", ON_EXHAUSTED_PAUSE).await;
    put(
        &s,
        MetaRecord::WorkflowPolicy(WorkflowPolicy {
            workflow: "retry-probe".into(),
            on_retries_exhausted: Some(ON_EXHAUSTED_FAIL),
            ..WorkflowPolicy::default()
        }),
    )
    .await;
    wait_failed(&s, &trap(&s, &sha, "aq").await).await;
    // Workflow `pause` over a queue without a policy.
    put(
        &s,
        MetaRecord::WorkflowPolicy(WorkflowPolicy {
            workflow: "retry-probe".into(),
            on_retries_exhausted: Some(ON_EXHAUSTED_PAUSE),
            ..WorkflowPolicy::default()
        }),
    )
    .await;
    let run = trap(&s, &sha, "fq").await;
    assert_eq!(wait_paused(&s, &run).await.pause.expect("paused").reason, 1);
    s.shutdown().await;
}
