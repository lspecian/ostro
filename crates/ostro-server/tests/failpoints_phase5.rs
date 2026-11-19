//! Plan 05-18 (QA-01): the executor failpoints `executor::before_append` and
//! `executor::after_append_before_done` on the real `EngineExecutor` (in-process node +
//! dispatcher + engine). Run with `--features failpoints -- --test-threads=1`: the fail
//! registry is process-global.
//!
//! Each test runs `sdk-basic` (RunStarted, two steps, completion) with one fault and checks
//! the run still completes with the right result and a journal that holds every event
//! exactly once, in contiguous seqs: a refused append is redone, an accepted append whose
//! result was lost is replayed (never appended twice), and a panicked job is retried.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::Duration;

use common::{Opts, T, TestNode, count, evicted_external, guest_wasm};
use ostro_partition::store::values::RunStatus;
use ostro_types::RunId;
use ostro_types::journal::{EventBody, EventKind};
use tempfile::TempDir;
use tokio::time::Instant;

const INPUT: &[u8] = br#"{"name":"ostro","n":10}"#;
const OUTPUT: &[u8] = br#"{"greeting":"hello, ostro","sum":55,"steps":2}"#;

fn json(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).expect("json")
}

/// Completed with the expected result; RunStarted / 2 x StepCompleted / RunCompleted exactly
/// once each, seqs 0..len.
async fn assert_clean_completion(n: &TestNode, run: &RunId, what: &str) {
    let rec = n.wait_terminal(run, T).await;
    assert_eq!(
        rec.status,
        RunStatus::Completed,
        "{what}: jobs {:?}",
        n.jobs_of(run)
    );
    let out = n.result(run).await.expect("result stored");
    assert_eq!(json(&out), json(OUTPUT), "{what}");
    let j = n.journal(run).await;
    for (i, e) in j.iter().enumerate() {
        assert_eq!(e.seq, i as u64, "{what}: contiguous seqs");
    }
    assert!(matches!(j[0].body, EventBody::RunStarted(_)), "{what}");
    assert_eq!(
        count(&j, EventKind::RunStarted),
        1,
        "{what}: no duplicate RunStarted"
    );
    assert_eq!(
        count(&j, EventKind::StepCompleted),
        2,
        "{what}: no duplicate steps"
    );
    assert_eq!(count(&j, EventKind::RunCompleted), 1, "{what}");
    assert_eq!(rec.next_seq, j.len() as u64, "{what}");
}

/// Start `sdk-basic` with `site` configured as `action`, wait for completion, clear it.
/// The returned guard owns the data dir (keep it alive while the node runs).
async fn faulted_run(site: &str, action: &str) -> (TempDir, TestNode, RunId, Duration) {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), Opts::new()).await;
    let sha = n.install_module(&guest_wasm("sdk-basic")).await;
    fail::cfg(site, action).unwrap();
    let t0 = Instant::now();
    let run = n.start("sdk-basic", 1, INPUT, &sha).await;
    let _ = n.wait_terminal(&run, T).await;
    let took = t0.elapsed();
    fail::remove(site);
    (dir, n, run, took)
}

/// `return` on the first engine append (the second executor append; the first is
/// RunStarted): nothing is appended, the activation is evicted and the run re-activated
/// with the same token; the redone work is journaled once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn before_append_return_redispatches_without_duplicates() {
    let _scenario = fail::FailScenario::setup();
    for action in ["1*off->1*return", "1*off->1*return(enospc)"] {
        let (_dir, n, run, _) = faulted_run("executor::before_append", action).await;
        assert_clean_completion(&n, &run, action).await;
        let jobs = n.jobs_of(&run);
        let i = jobs
            .iter()
            .position(|j| evicted_external(&j.activation))
            .unwrap_or_else(|| panic!("{action}: no evicted activation: {jobs:?}"));
        assert_eq!(
            jobs[i].outcome,
            ostro_partition::ExecOutcome::Reactivate,
            "{action}"
        );
        assert!(
            jobs.get(i + 1)
                .is_some_and(|next| next.token == jobs[i].token),
            "{action}: re-activated with the same token: {jobs:?}"
        );
        n.stop().await;
    }
}

/// `return` on the RunStarted append: nothing appended, re-activated, one RunStarted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn before_append_return_on_run_started_reactivates() {
    let _scenario = fail::FailScenario::setup();
    let (_dir, n, run, _) = faulted_run("executor::before_append", "1*return(enospc)").await;
    assert_clean_completion(&n, &run, "RunStarted refused").await;
    let jobs = n.jobs_of(&run);
    assert!(
        jobs.iter().any(
            |j| j.activation.is_none() && j.outcome == ostro_partition::ExecOutcome::Reactivate
        ),
        "{jobs:?}"
    );
    n.stop().await;
}

/// `return` after an accepted append: the events are durable but the engine is told the
/// append failed; the re-activation replays them from the journal instead of appending them
/// again. Covers the RunStarted append (`1*return`) and the first engine append.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_append_return_replays_instead_of_duplicating() {
    let _scenario = fail::FailScenario::setup();
    for action in ["1*return", "1*off->1*return", "1*off->1*return(enospc)"] {
        let (_dir, n, run, _) = faulted_run("executor::after_append_before_done", action).await;
        assert_clean_completion(&n, &run, action).await;
        assert!(
            n.jobs_of(&run).len() >= 2,
            "{action}: {:?}",
            n.jobs_of(&run)
        );
        n.stop().await;
    }
}

/// `sleep` widens the window after an accepted append; nothing else changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_append_sleep_only_delays() {
    let _scenario = fail::FailScenario::setup();
    let (_dir, n, run, took) =
        faulted_run("executor::after_append_before_done", "1*sleep(300)").await;
    assert_clean_completion(&n, &run, "sleep").await;
    assert!(took >= Duration::from_millis(300), "delayed: {took:?}");
    assert!(
        n.jobs_of(&run)
            .iter()
            .all(|j| !evicted_external(&j.activation)),
        "a sleep is not a failure: {:?}",
        n.jobs_of(&run)
    );
    n.stop().await;
}

/// `panic` before the RunStarted append panics the job task: the dispatcher drops the lease
/// and retries the run with the next token; the run completes once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn before_append_panic_is_retried_by_the_dispatcher() {
    let _scenario = fail::FailScenario::setup();
    let (_dir, n, run, _) = faulted_run("executor::before_append", "1*panic").await;
    assert_clean_completion(&n, &run, "panic").await;
    // The panicked job (token n = 1) never finished, so it left no job record: the first
    // recorded job is the dispatcher's retry with a higher token.
    let jobs = n.jobs_of(&run);
    assert!(
        jobs.first().is_some_and(|j| j.token.n >= 2),
        "re-granted after the panic: {jobs:?}"
    );
    n.stop().await;
}
