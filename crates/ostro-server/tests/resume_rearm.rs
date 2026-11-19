//! Resume re-arm (plan 05-11, D1/B2) on the in-process `TestNode` harness: a run that timed
//! out while sleeping or waiting is resumed in place with `RetryRun`; its first activation
//! re-arms every unresolved op from the replayed journal (apply re-arms nothing and deleted
//! the wait rows at the terminal transition), so the run completes:
//!
//! - (a) a signal wait: re-registered, then a signal sent after the resume completes it;
//! - (b) a sleep whose due time passed while the run was terminal fires immediately;
//! - (c) an await-token wait: re-registered, `CompleteToken` completes it;
//! - (d) a signal wait with a timeout that passed while terminal: `WaitTimedOut` (D4), and
//!   never a stale `TimerFired` for the signal op.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::{T, TestNode, guest_wasm, now_ms};
use ostro_partition::ApplyOutcome;
use ostro_partition::store::values::RunStatus;
use ostro_types::command::{CompleteToken, ConflictPolicy, RetryRun, SignalRun, StartRun};
use ostro_types::journal::{
    Event, EventBody, Payload, SignalPayload, TokenCompleted, encode_event,
};
use ostro_types::{Command, PartitionId, PayloadRef, RunId, partition_for_new_run};
use tokio::time::{Instant, sleep, timeout};

async fn start_with(
    n: &TestNode,
    workflow: &str,
    input: &[u8],
    sha: &[u8; 32],
    run_timeout_ms: u64,
) -> RunId {
    let uuid = RunId::new_v7(PartitionId(0)).uuid();
    let run = RunId::new(partition_for_new_run(&uuid, n.count()), uuid);
    let now = now_ms();
    let o = submit(
        n,
        &run,
        Command::start_run(StartRun {
            run_id: run.as_bytes().to_vec(),
            workflow: workflow.into(),
            workflow_version: 1,
            queue: "default".into(),
            input: Some(PayloadRef::inline(Bytes::copy_from_slice(input))),
            conflict_policy: ConflictPolicy::ReturnExisting as i32,
            ready_at_ms: now,
            requested_at_ms: now,
            rng_seed: uuid.as_bytes().repeat(2),
            module_sha: sha.to_vec(),
            run_timeout_ms,
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(o, ApplyOutcome::Accepted);
    run
}

async fn submit(n: &TestNode, run: &RunId, c: Command) -> ApplyOutcome {
    timeout(T, n.handle_of(run).submit_one(c))
        .await
        .expect("submit timed out")
        .expect("submit")
}

fn json_payload(v: &serde_json::Value) -> Payload {
    Payload {
        content_type: "application/json".into(),
        data: v.to_string().into_bytes(),
    }
}

/// Submitter-encoded event bytes (D21).
fn encoded(ts_nanos: u64, body: EventBody) -> PayloadRef {
    let (_, bytes) = encode_event(&Event {
        seq: 0,
        ts_nanos,
        body,
    });
    PayloadRef::inline(Bytes::from(bytes))
}

async fn signal(n: &TestNode, run: &RunId, name: &str, v: &serde_json::Value) {
    let now = now_ms();
    let o = submit(
        n,
        run,
        Command::signal_run(SignalRun {
            run_id: run.as_bytes().to_vec(),
            name: name.into(),
            event: Some(encoded(
                now * 1_000_000,
                EventBody::SignalPayload(SignalPayload {
                    name: name.into(),
                    payload: json_payload(v),
                }),
            )),
            request_id: vec![],
            sent_at_ms: now,
        }),
    )
    .await;
    assert_eq!(o, ApplyOutcome::Accepted);
}

async fn retry(n: &TestNode, run: &RunId) {
    let o = submit(
        n,
        run,
        Command::retry_run(RetryRun {
            run_id: run.as_bytes().to_vec(),
            requested_at_ms: now_ms(),
        }),
    )
    .await;
    assert_eq!(o, ApplyOutcome::Accepted);
}

/// Wait for the run timeout, check the hard stop, and resume.
async fn timed_out_then_retry(n: &TestNode, run: &RunId) {
    let rec = n.wait_terminal(run, T).await;
    assert_eq!(rec.status, RunStatus::TimedOut);
    assert!(rec.resumable);
    assert_eq!(
        rec.open_waits, 0,
        "the terminal transition removed the waits"
    );
}

/// After the resume: suspended again on a re-registered wait.
async fn wait_rearmed(n: &TestNode, run: &RunId) {
    let rec = n
        .wait_until(run, T, |r| {
            r.resumed_count == 1 && r.status == RunStatus::Scheduled && r.open_waits == 1
        })
        .await;
    assert_eq!(rec.terminal_reason, "");
}

fn json(b: &[u8]) -> serde_json::Value {
    serde_json::from_slice(b).expect("json")
}

fn has_terminated_marker(j: &[Event]) -> bool {
    j.iter()
        .any(|e| matches!(&e.body, EventBody::RunTerminated(t) if t.reason == "run_timeout"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_wait_is_rearmed_after_a_timeout() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), common::Opts::new()).await;
    let sha = n.install_module(&guest_wasm("signals")).await;
    let run = start_with(&n, "signals", br#"{"mode":"long_wait"}"#, &sha, 300).await;
    timed_out_then_retry(&n, &run).await;
    retry(&n, &run).await;
    wait_rearmed(&n, &run).await;
    signal(&n, &run, "x", &serde_json::json!({"ok": "after-resume"})).await;
    let rec = n.wait_terminal(&run, T).await;
    assert_eq!(rec.status, RunStatus::Completed);
    assert_eq!(rec.resumed_count, 1);
    assert_eq!(
        json(&n.result(&run).await.expect("result")),
        serde_json::json!({"ok": "after-resume"})
    );
    assert!(has_terminated_marker(&n.journal(&run).await));
    n.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_past_due_sleep_fires_immediately_after_resume() {
    let dir = ostro_testkit::fs::tempdir();
    // Short hot wait: the sleep evicts, so the run times out while Scheduled (sleeping).
    let n = TestNode::open(
        dir.path(),
        common::Opts::new().hot_wait(Duration::from_millis(50)),
    )
    .await;
    let sha = n.install_module(&guest_wasm("timeouts")).await;
    let t0 = Instant::now();
    let run = start_with(&n, "timeouts", br#"{"mode":"sleep_ms","n":500}"#, &sha, 200).await;
    timed_out_then_retry(&n, &run).await;
    // Let the original sleep due time pass while the run is terminal.
    let past = Duration::from_millis(800);
    if t0.elapsed() < past {
        sleep(past - t0.elapsed()).await;
    }
    let t1 = Instant::now();
    retry(&n, &run).await;
    let rec = n.wait_terminal(&run, T).await;
    assert_eq!(rec.status, RunStatus::Completed);
    assert_eq!(json(&n.result(&run).await.expect("result")), "slept");
    assert!(
        t1.elapsed() < Duration::from_secs(5),
        "past-due timer fired late: {:?}",
        t1.elapsed()
    );
    let j = n.journal(&run).await;
    assert!(has_terminated_marker(&j));
    let fired = j
        .iter()
        .filter(|e| matches!(e.body, EventBody::TimerFired(_)))
        .count();
    assert_eq!(fired, 1, "one TimerFired for the sleep");
    n.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_token_wait_is_rearmed_after_a_timeout() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), common::Opts::new()).await;
    let sha = n.install_module(&guest_wasm("tokens")).await;
    let run = start_with(&n, "tokens", b"{}", &sha, 300).await;
    timed_out_then_retry(&n, &run).await;
    retry(&n, &run).await;
    wait_rearmed(&n, &run).await;
    let op = n
        .journal(&run)
        .await
        .iter()
        .find(|e| matches!(e.body, EventBody::AwaitTokenCreated(_)))
        .expect("token op")
        .seq;
    let now = now_ms();
    let v = serde_json::json!({"token": "done"});
    let o = submit(
        &n,
        &run,
        Command::complete_token(CompleteToken {
            run_id: run.as_bytes().to_vec(),
            op,
            event: Some(encoded(
                now * 1_000_000,
                EventBody::TokenCompleted(TokenCompleted {
                    op,
                    payload: json_payload(&v),
                }),
            )),
            completed_at_ms: now,
        }),
    )
    .await;
    assert_eq!(o, ApplyOutcome::Accepted);
    let rec = n.wait_terminal(&run, T).await;
    assert_eq!(rec.status, RunStatus::Completed);
    assert_eq!(json(&n.result(&run).await.expect("result")), v);
    n.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_wait_timeout_never_surfaces_as_timer_fired() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), common::Opts::new()).await;
    let sha = n.install_module(&guest_wasm("signals")).await;
    let t0 = Instant::now();
    let run = start_with(
        &n,
        "signals",
        br#"{"mode":"timeout","timeout_ms":600}"#,
        &sha,
        200,
    )
    .await;
    timed_out_then_retry(&n, &run).await;
    // The wait timeout passes while the run is terminal.
    let past = Duration::from_millis(900);
    if t0.elapsed() < past {
        sleep(past - t0.elapsed()).await;
    }
    retry(&n, &run).await;
    let rec = n.wait_terminal(&run, T).await;
    assert_eq!(rec.status, RunStatus::Completed);
    assert_eq!(json(&n.result(&run).await.expect("result")), "timeout");
    let j = n.journal(&run).await;
    let op = j
        .iter()
        .find(|e| matches!(e.body, EventBody::SignalWaitStarted(_)))
        .expect("wait op")
        .seq;
    let timed_out: Vec<u64> = j
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::WaitTimedOut(w) => Some(w.op),
            _ => None,
        })
        .collect();
    assert_eq!(timed_out, vec![op], "exactly one WaitTimedOut for the wait");
    assert!(
        !j.iter()
            .any(|e| matches!(&e.body, EventBody::TimerFired(t) if t.op == op)),
        "no stale TimerFired for the signal op: {j:?}"
    );
    n.stop().await;
}
