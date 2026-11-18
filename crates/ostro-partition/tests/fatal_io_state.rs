//! STOR-07: a fjall commit/persist error or a log I/O failure makes the partition Failed;
//! in-flight and later submits return errors, never success. After a restart without the
//! fault, every command acknowledged before the failure is present and the state equals a
//! rebuild from the log.
//!
//! Requires `--features failpoints`; run with `--test-threads=1` (the `fail` registry is
//! process-global).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::path::Path;
use std::time::Duration;

use common::{T, cfg, new_run, open_single, open_with, rebuild_digest, start_cmd};
use fail::FailScenario;
use ostro_partition::{ApplyOutcome, MachineConfig, PartitionHealth};
use ostro_types::RunId;
use tokio::time::timeout;

type TP = common::TestPartition;

async fn submit_ok(tp: &TP, oks: &mut Vec<RunId>, n: usize) {
    for _ in 0..n {
        let run = new_run();
        let o = timeout(T, tp.handle.submit_one(start_cmd(&run)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(o, ApplyOutcome::Accepted);
        oks.push(run);
    }
}

/// Submit while the fault is active; returns the run if it was (legitimately) acknowledged.
async fn submit_during_fault(tp: &TP) -> (RunId, bool) {
    let run = new_run();
    let r = timeout(T, tp.handle.submit_one(start_cmd(&run)))
        .await
        .expect("submit hung after a storage failure");
    (run, r.is_ok())
}

async fn wait_failed(tp: &TP) {
    let mut h = tp.handle.machine().health();
    timeout(
        T,
        h.wait_for(|h| matches!(h, PartitionHealth::Failed { .. })),
    )
    .await
    .expect("machine health did not become Failed")
    .unwrap();
}

async fn later_submits_fail(tp: &TP) {
    for _ in 0..3 {
        let r = timeout(T, tp.handle.submit_one(start_cmd(&new_run())))
            .await
            .expect("submit hung on a failed partition");
        assert!(r.is_err(), "submit on a failed partition returned {r:?}");
    }
}

/// Reopen without failpoints: open succeeds, every acknowledged run is present, and the
/// digest equals a rebuild from the log.
async fn reopen_and_check(dir: &Path, oks: &[RunId]) {
    let tp = open_with(dir, cfg(), false, |l| l).await;
    assert_eq!(
        *tp.handle.machine().health().borrow(),
        PartitionHealth::Healthy
    );
    for run in oks {
        assert!(
            tp.handle.read_run(run).unwrap().is_some(),
            "acknowledged run {run:?} missing after restart"
        );
    }
    assert_eq!(
        tp.handle.store().digest().unwrap(),
        rebuild_digest(&*tp.rf1).await
    );
    tp.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn state_commit_failure_fails_the_partition() {
    let scenario = FailScenario::setup();
    let tmp = ostro_testkit::fs::tempdir();
    let tp = open_single(tmp.path(), cfg()).await;
    let mut oks = Vec::new();
    submit_ok(&tp, &mut oks, 3).await;

    fail::cfg("state::commit", "return").unwrap();
    let (_, ok) = submit_during_fault(&tp).await;
    assert!(!ok, "a submit whose commit failed returned Ok");
    wait_failed(&tp).await;
    later_submits_fail(&tp).await;
    fail::remove("state::commit");
    later_submits_fail(&tp).await; // still failed after the fault clears: no retry

    let _ = tp.close().await;
    reopen_and_check(tmp.path(), &oks).await;
    scenario.teardown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn state_persist_failure_fails_the_partition() {
    let scenario = FailScenario::setup();
    let tmp = ostro_testkit::fs::tempdir();
    let c = MachineConfig {
        checkpoint_batches: 1,
        checkpoint_every: Duration::from_secs(3600),
        ..cfg()
    };
    let tp = open_single(tmp.path(), c).await;
    let mut oks = Vec::new();
    submit_ok(&tp, &mut oks, 2).await;

    fail::cfg("state::persist", "return").unwrap();
    // The chunk commits (so its ack may legitimately be Ok), then its checkpoint fails.
    let (run, ok) = submit_during_fault(&tp).await;
    if ok {
        oks.push(run);
    }
    wait_failed(&tp).await;
    later_submits_fail(&tp).await;
    assert!(tp.handle.machine().force_checkpoint().await.is_err());
    fail::remove("state::persist");

    let _ = tp.close().await;
    reopen_and_check(tmp.path(), &oks).await;
    scenario.teardown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wal_fsync_failure_fails_the_partition() {
    let scenario = FailScenario::setup();
    let tmp = ostro_testkit::fs::tempdir();
    let tp = open_single(tmp.path(), cfg()).await;
    let mut oks = Vec::new();
    submit_ok(&tp, &mut oks, 3).await;

    fail::cfg("wal::fsync", "return").unwrap();
    let (_, ok) = submit_during_fault(&tp).await;
    assert!(!ok, "a submit whose fsync failed returned Ok");
    wait_failed(&tp).await;
    match &*tp.handle.machine().health().borrow() {
        PartitionHealth::Failed { reason } => {
            assert!(reason.contains("log"), "unexpected reason {reason}")
        }
        h => panic!("{h:?}"),
    }
    later_submits_fail(&tp).await;
    fail::remove("wal::fsync");

    let _ = tp.close().await;
    reopen_and_check(tmp.path(), &oks).await;
    scenario.teardown();
}
