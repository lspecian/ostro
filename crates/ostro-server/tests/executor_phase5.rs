//! Executor / dispatcher lifecycle through the server seam (plan 05-11 Task 2), on the
//! in-process `TestNode` harness (node + dispatcher + `EngineExecutor`, no API):
//!
//! - hard stops (terminate, run timeout while sleeping and while running) revoke the lease
//!   and stop a hot activation: `live_activations()` returns to 0, nothing is journaled after
//!   `RunTerminated`;
//! - executor-level failures complete `Failed` with `resumable = true` (exhausted attempts,
//!   nondeterminism), and the retry limit counts the durable `attempt_failures` across a
//!   restart (F9);
//! - cancel requested before the first activation is journaled right after `RunStarted`;
//! - `ExecutorHooks::module_override` (WF-08 patching after a restart);
//! - D23: a qualified workflow runs the module-local workflow with `run-info.tenant`;
//! - continue-as-new chains carry tenant/tags/traceparent (F11) and the parent link (PF-F2),
//!   and only the chain end reports to the parent.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use common::{T, TestNode, guest_wasm, now_ms};
use ostro_engine::error::AttemptFailure;
use ostro_engine::sink::ActivationOutcome;
use ostro_partition::ApplyOutcome;
use ostro_partition::store::values::{RunRecord, RunStatus};
use ostro_types::command::{ConflictPolicy, ParentRef, RequestCancel, StartRun, TerminateRun};
use ostro_types::journal::{CancelRequested, Event, EventBody};
use ostro_types::{Command, PartitionId, PayloadRef, RunId, partition_for_new_run};
use tokio::time::{Instant, sleep, timeout};

// ------------------------------------------------------------------- helpers

/// Start `workflow` v1 with `input` on `sha`, letting `tweak` edit the StartRun.
async fn start_with(
    n: &TestNode,
    workflow: &str,
    input: &[u8],
    sha: &[u8; 32],
    tweak: impl FnOnce(&mut StartRun),
) -> RunId {
    let uuid = RunId::new_v7(PartitionId(0)).uuid();
    let run = RunId::new(partition_for_new_run(&uuid, n.count()), uuid);
    let now = now_ms();
    let mut s = StartRun {
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
        ..Default::default()
    };
    tweak(&mut s);
    let o = submit(n, &run, Command::start_run(s)).await;
    assert_eq!(o, ApplyOutcome::Accepted);
    run
}

async fn submit(n: &TestNode, run: &RunId, c: Command) -> ApplyOutcome {
    timeout(T, n.handle_of(run).submit_one(c))
        .await
        .expect("submit timed out")
        .expect("submit")
}

async fn terminate(n: &TestNode, run: &RunId) {
    let o = submit(
        n,
        run,
        Command::terminate_run(TerminateRun {
            run_id: run.as_bytes().to_vec(),
            reason: "operator".into(),
            terminated_at_ms: now_ms(),
        }),
    )
    .await;
    assert_eq!(o, ApplyOutcome::Accepted);
}

async fn wait_live_zero(n: &TestNode, limit: Duration) {
    let deadline = Instant::now() + limit;
    while n.runtime.live_activations() != 0 {
        assert!(
            Instant::now() < deadline,
            "{} live activations after {limit:?}",
            n.runtime.live_activations()
        );
        sleep(Duration::from_millis(10)).await;
    }
}

fn last_body(j: &[Event]) -> &EventBody {
    &j.last().expect("non-empty journal").body
}

/// Journal is final: the last event is `RunTerminated`, and nothing is appended later.
async fn assert_terminated_last(n: &TestNode, run: &RunId, reason: &str) {
    let j = n.journal(run).await;
    match last_body(&j) {
        EventBody::RunTerminated(t) => assert_eq!(t.reason, reason),
        other => panic!("last event is not RunTerminated: {other:?}"),
    }
    sleep(Duration::from_millis(300)).await;
    assert_eq!(n.journal(run).await.len(), j.len(), "events after the stop");
}

fn json(b: &[u8]) -> serde_json::Value {
    serde_json::from_slice(b).expect("json")
}

// ------------------------------------------------------------------- hard stops

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_stops_a_hot_activation() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), common::Opts::new()).await;
    let sha = n.install_module(&guest_wasm("timeouts")).await;
    let run = start_with(
        &n,
        "timeouts",
        br#"{"mode":"busy_steps","n":100000000}"#,
        &sha,
        |_| {},
    )
    .await;
    // Steps are flushed only at a wait or the end: the activation runs hot with nothing
    // journaled after RunStarted.
    n.wait_until(&run, T, |r| r.status == RunStatus::Running)
        .await;
    let deadline = Instant::now() + T;
    while n.runtime.live_activations() == 0 {
        assert!(Instant::now() < deadline, "activation never started");
        sleep(Duration::from_millis(10)).await;
    }
    sleep(Duration::from_millis(200)).await;
    assert!(n.runtime.live_activations() >= 1, "the activation is hot");
    let t0 = Instant::now();
    terminate(&n, &run).await;
    wait_live_zero(&n, Duration::from_secs(1)).await;
    assert!(t0.elapsed() < Duration::from_secs(1));
    let rec = n.record(&run);
    assert_eq!(rec.status, RunStatus::Terminated);
    assert!(rec.resumable);
    assert_terminated_last(&n, &run, "operator").await;
    // The job ended Released after the revoke (no Fenced re-dispatch, no new attempt).
    let jobs = n.jobs_of(&run);
    assert!(jobs.iter().all(|j| j.revoked), "{jobs:?}");
    assert_eq!(n.record(&run).current_attempt.map(|a| a.1), Some(1));
    n.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_timeout_while_sleeping_and_while_running() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), common::Opts::new()).await;
    let sha = n.install_module(&guest_wasm("timeouts")).await;
    let sleeping = start_with(&n, "timeouts", br#"{"mode":"sleep_forever"}"#, &sha, |s| {
        s.run_timeout_ms = 300;
    })
    .await;
    let busy = start_with(
        &n,
        "timeouts",
        br#"{"mode":"busy_steps","n":100000000}"#,
        &sha,
        |s| s.run_timeout_ms = 300,
    )
    .await;
    for run in [&sleeping, &busy] {
        let rec = n.wait_terminal(run, T).await;
        assert_eq!(rec.status, RunStatus::TimedOut, "{run}");
        assert_eq!(rec.terminal_reason, "run_timeout");
        assert!(rec.resumable);
    }
    wait_live_zero(&n, Duration::from_secs(1)).await;
    for run in [&sleeping, &busy] {
        assert_terminated_last(&n, run, "run_timeout").await;
    }
    // The sleeping run was not activated again by its pending sleep timer.
    assert!(n.jobs_of(&sleeping).len() <= 2);
    n.stop().await;
}

// ------------------------------------------------------------------- resumable failures

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exhausted_attempts_fail_resumable() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), common::Opts::new()).await;
    let sha = n.install_module(&guest_wasm("retry-probe")).await;
    let run = n.start("retry-probe", 1, b"trap", &sha).await;
    let rec = n.wait_terminal(&run, T).await;
    assert_eq!(rec.status, RunStatus::Failed);
    assert!(rec.resumable, "executor-level failure (D1)");
    // F9: two re-grants after a failure were counted durably.
    assert_eq!(rec.attempt_failures, 2);
    assert_eq!(
        json(&n.result(&run).await.expect("result"))["reason_code"],
        "trap"
    );
    n.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn attempt_failure_limit_survives_a_restart_f9() {
    let dir = ostro_testkit::fs::tempdir();
    let mut opts = common::Opts::new();
    opts.exec.max_attempt_failures = 3;
    // Delays 1 s then 2 s: the restart lands inside the second backoff.
    opts.exec.retry_backoff = Duration::from_secs(1);
    let n = TestNode::open(dir.path(), opts).await;
    let sha = n.install_module(&guest_wasm("retry-probe")).await;
    let run = n.start("retry-probe", 1, b"trap", &sha).await;
    // Second failure seen: its re-grant (after_failure) is already durable.
    let rec = n.wait_until(&run, T, |r| r.attempt_failures == 2).await;
    assert!(!rec.status.is_terminal());
    let (rt, hooks, opts) = n.stop().await;
    let n = TestNode::open_with(dir.path(), opts, rt, hooks).await;
    assert_eq!(n.record(&run).attempt_failures, 2, "survives the restart");
    let rec = n.wait_terminal(&run, T).await;
    assert_eq!(rec.status, RunStatus::Failed);
    assert!(rec.resumable);
    assert_eq!(rec.attempt_failures, 2);
    let traps = n
        .jobs_of(&run)
        .into_iter()
        .filter(|j| {
            matches!(
                j.activation,
                Some(ActivationOutcome::AttemptFailed(AttemptFailure::Trap(_)))
            )
        })
        .count();
    assert_eq!(traps, 3, "exactly 3 attempt failures in total, not 3 more");
    n.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nondeterminism_fails_resumable() {
    let dir = ostro_testkit::fs::tempdir();
    // Short hot wait: patch-demo's 2 s sleep evicts (Scheduled) instead of parking.
    let opts = common::Opts::new().hot_wait(Duration::from_millis(50));
    let n = TestNode::open(dir.path(), opts).await;
    let v1 = n.install_module(&guest_wasm("patch-v1")).await;
    let v3 = n.put_blob(&guest_wasm("patch-v3"));
    let run = n.start("patch-demo", 1, b"{}", &v1).await;
    // Step A + the 2 s sleep journaled; the run waits for its timer.
    n.wait_until(&run, T, |r| r.status == RunStatus::Scheduled)
        .await;
    // v3 always runs step C where the v1 journal has the sleep: nondeterminism on replay.
    n.hooks
        .module_override
        .set(Some(Arc::new(move |r: &RunId| (*r == run).then_some(v3))));
    let rec = n.wait_terminal(&run, T).await;
    n.hooks.module_override.set(None);
    assert_eq!(rec.status, RunStatus::Failed);
    assert!(rec.resumable, "nondeterminism is resumed in place (D1)");
    let v = json(&n.result(&run).await.expect("result"));
    assert_eq!(v["reason_code"], "nondeterminism", "{v}");
    // Not retryable: no re-grant after the failure.
    assert_eq!(rec.attempt_failures, 0);
    n.stop().await;
}

// ------------------------------------------------------------------- cancel before start

async fn cancel_before_start(
    guest: &str,
    workflow: &str,
    input: &[u8],
) -> (RunRecord, Vec<Event>, Option<Vec<u8>>) {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), common::Opts::new()).await;
    let sha = n.install_module(&guest_wasm(guest)).await;
    let run = start_with(&n, workflow, input, &sha, |s| {
        s.ready_at_ms = s.requested_at_ms + 1_000;
    })
    .await;
    let o = submit(
        &n,
        &run,
        Command::request_cancel(RequestCancel {
            run_id: run.as_bytes().to_vec(),
            reason: "user".into(),
            requested_at_ms: now_ms(),
        }),
    )
    .await;
    assert_eq!(o, ApplyOutcome::Accepted);
    let rec = n.record(&run);
    assert!(rec.cancel_requested);
    assert_eq!(
        rec.next_seq, 0,
        "nothing journaled before RunStarted (05-08)"
    );
    let rec = n.wait_terminal(&run, T).await;
    let j = n.journal(&run).await;
    let result = n.result(&run).await;
    n.stop().await;
    (rec, j, result)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_before_start_is_journaled_after_run_started() {
    let (rec, j, _) =
        cancel_before_start("timeouts", "timeouts", br#"{"mode":"sleep_forever"}"#).await;
    assert!(matches!(j[0].body, EventBody::RunStarted(_)));
    assert_eq!(
        j[1].body,
        EventBody::CancelRequested(CancelRequested {
            reason: "user".into()
        })
    );
    assert_eq!(j[1].seq, 1);
    // The guest saw the cancellation and the run ends Cancelled.
    assert_eq!(rec.status, RunStatus::Cancelled);
    assert!(!rec.resumable, "a guest terminal is not resumed in place");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_before_start_runs_shielded_cleanup() {
    let (rec, j, result) =
        cancel_before_start("sdk-cancel", "sdk-cancel", br#"{"mode":"shielded_sleep"}"#).await;
    assert!(matches!(j[1].body, EventBody::CancelRequested(_)));
    assert_eq!(rec.status, RunStatus::Completed);
    assert_eq!(json(&result.expect("result")), "cleaned");
}

// ------------------------------------------------------------------- module override

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn module_override_swaps_the_module_after_restart() {
    let dir = ostro_testkit::fs::tempdir();
    let opts = common::Opts::new().hot_wait(Duration::from_millis(50));
    let n = TestNode::open(dir.path(), opts).await;
    let v1 = n.install_module(&guest_wasm("patch-v1")).await;
    let run = n.start("patch-demo", 1, b"{}", &v1).await;
    n.wait_until(&run, T, |r| r.status == RunStatus::Scheduled)
        .await;
    let (rt, hooks, opts) = n.stop().await;
    let n = TestNode::open_with(dir.path(), opts, rt, hooks).await;
    let v2 = n.put_blob(&guest_wasm("patch-v2"));
    assert!(n.runtime.prepared(&v2).is_none());
    let calls = Arc::new(AtomicU64::new(0));
    let c = Arc::clone(&calls);
    n.hooks.module_override.set(Some(Arc::new(move |r: &RunId| {
        c.fetch_add(1, Ordering::Relaxed);
        (*r == run).then_some(v2)
    })));
    let rec = n.wait_terminal(&run, T).await;
    n.hooks.module_override.set(None);
    assert_eq!(rec.status, RunStatus::Completed);
    // The v1 journal replayed on v2 takes the old path: no step C.
    assert_eq!(
        json(&n.result(&run).await.expect("result")),
        serde_json::json!(["A", "B"])
    );
    assert!(calls.load(Ordering::Relaxed) >= 1);
    assert!(n.runtime.prepared(&v2).is_some(), "the run executed on v2");
    assert_eq!(rec.module_sha, Some(v1), "the pin itself is unchanged");
    n.stop().await;
}

// ------------------------------------------------------------------- D23 journal form

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn qualified_workflow_runs_the_local_name_with_tenant_d23() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), common::Opts::new()).await;
    let sha = n.install_module(&guest_wasm("wit-surface")).await;
    let run = start_with(&n, "acme/info", b"{}", &sha, |s| {
        s.tenant = "acme".into();
    })
    .await;
    let rec = n.wait_terminal(&run, T).await;
    assert_eq!(rec.status, RunStatus::Completed, "{rec:?}");
    assert_eq!(
        rec.workflow, "acme/info",
        "storage keeps the qualified name"
    );
    let out = String::from_utf8(n.result(&run).await.expect("result")).expect("utf8");
    assert!(out.contains("workflow=info "), "{out}");
    assert!(out.contains("tenant=Some(\"acme\")"), "{out}");
    let j = n.journal(&run).await;
    let EventBody::RunStarted(rs) = &j[0].body else {
        panic!("RunStarted")
    };
    assert_eq!(
        (rs.workflow.as_str(), rs.tenant.as_deref()),
        ("info", Some("acme"))
    );
    n.stop().await;
}

// ------------------------------------------------------------------- continue-as-new

/// Follow `continued_to` from `first` until a run that did not continue; returns every
/// record of the chain in order.
async fn chain(n: &TestNode, first: &RunId) -> Vec<(RunId, RunRecord)> {
    let mut out = Vec::new();
    let mut cur = *first;
    loop {
        let deadline = Instant::now() + T;
        let rec = loop {
            if let Ok(Some(r)) = n.handle_of(&cur).read_run(&cur)
                && r.status.is_terminal()
            {
                break r;
            }
            assert!(Instant::now() < deadline, "chain run {cur} never ended");
            sleep(Duration::from_millis(20)).await;
        };
        let next = rec.continued_to;
        out.push((cur, rec));
        match next {
            Some(nx) => cur = nx,
            None => return out,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn can_chain_copies_tenant_tags_traceparent_f11() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), common::Opts::new()).await;
    let sha = n.install_module(&guest_wasm("can-chain")).await;
    let first = start_with(&n, "acme/can-chain", br#"{"n":0,"max":3}"#, &sha, |s| {
        s.tenant = "acme".into();
        s.tags = vec!["red".into(), "blue".into()];
        s.traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into();
        s.retention_ms = 86_400_000;
    })
    .await;
    let runs = chain(&n, &first).await;
    assert_eq!(runs.len(), 4, "3 continue-as-new runs + the completing one");
    for (i, (id, r)) in runs.iter().enumerate() {
        let last = i + 1 == runs.len();
        let want = if last {
            RunStatus::Completed
        } else {
            RunStatus::ContinuedAsNew
        };
        assert_eq!(r.status, want, "run {i}");
        assert_eq!(r.workflow, "acme/can-chain");
        assert_eq!(r.tenant.as_deref(), Some("acme"));
        assert_eq!(r.tags, vec!["red".to_owned(), "blue".to_owned()]);
        assert_eq!(
            r.traceparent.as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
        assert_eq!(r.retention_ms, 86_400_000);
        if i > 0 {
            assert_eq!(r.continued_from, Some(runs[i - 1].0));
            let j = n.journal(id).await;
            let EventBody::RunStarted(rs) = &j[0].body else {
                panic!("RunStarted")
            };
            assert_eq!(rs.continued_from, Some(runs[i - 1].0.to_string()));
            assert_eq!(rs.workflow, "can-chain");
        }
        if !last {
            let v = json(&n.result(id).await.expect("result"));
            assert_eq!(v["next_run_id"], runs[i + 1].0.to_string());
        }
    }
    let (last, _) = runs.last().expect("last");
    assert_eq!(json(&n.result(last).await.expect("result")), 3);
    n.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn can_chain_keeps_the_parent_and_reports_once_pf_f2() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), common::Opts::new()).await;
    // A live parent: waits on an await token forever (no timeout).
    let tokens = n.install_module(&guest_wasm("tokens")).await;
    let parent = start_with(&n, "tokens", b"{}", &tokens, |_| {}).await;
    n.wait_until(&parent, T, |r| {
        r.open_waits == 1 && r.status == RunStatus::Scheduled
    })
    .await;
    assert_eq!(n.record(&parent).inbox_next, 0);
    let sha = n.install_module(&guest_wasm("can-chain")).await;
    let first = start_with(&n, "can-chain", br#"{"n":0,"max":3}"#, &sha, |s| {
        s.parent = Some(ParentRef {
            run_id: parent.as_bytes().to_vec(),
            op: 7,
        });
    })
    .await;
    let runs = chain(&n, &first).await;
    assert_eq!(runs.len(), 4);
    for (i, (_, r)) in runs.iter().enumerate() {
        assert_eq!(
            r.parent,
            Some((*parent.as_bytes(), 7)),
            "run {i} keeps the parent"
        );
    }
    // Exactly one ChildCompleted reached the parent: from the chain end, not per hop.
    n.wait_until(&parent, T, |r| r.inbox_next >= 1).await;
    sleep(Duration::from_millis(300)).await;
    assert_eq!(n.record(&parent).inbox_next, 1);
    n.stop().await;
}
