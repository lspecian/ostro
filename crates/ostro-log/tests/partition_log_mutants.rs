//! "Mutant bites" for the two `repl::mutant::*` failpoints placed in the partition log
//! (12-08, D-P12-19): `rf1_bypass` (HW straight from the WAL callback, skipping the
//! tracker) and the append-gate half of `ignore_min_insync`. Each test checks the property
//! holds, enables the mutant with `fail::cfg(name, "return")`, shows the property violated,
//! and removes it again.
//!
//! Requires `--features failpoints`; run single-threaded (`--test-threads=1`): the `fail`
//! registry is process-global (`FailScenario` also serialises the tests).
#![cfg(feature = "failpoints")]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fail::FailScenario;
use ostro_log::{
    LogError, LogNode, LogNodeConfig, PartitionLog, ReplicaSet, ReplicatedLog, mutants,
};
use ostro_types::{Epoch, Offset, PartitionCount};
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);

async fn open(dir: &Path) -> (LogNode, Arc<PartitionLog>) {
    let cfg = LogNodeConfig::new(dir.join("wal"), PartitionCount::new(1).unwrap(), 1);
    let (node, mut logs) = LogNode::open(cfg, &[Offset::ZERO]).await.unwrap();
    (node, logs.remove(0).unwrap())
}

fn cmd(tag: &str) -> Vec<Bytes> {
    vec![Bytes::from(tag.to_string())]
}

/// One committed append moves the HW by one batch and costs exactly one tracker
/// publication (D-P12-15: the RF=1 HW goes through the tracker like any other).
async fn one_append_is_one_publication(log: &PartitionLog, tag: &str) -> bool {
    let before = log.hw_publishes();
    let r = log.append(Epoch(1), cmd(tag)).await.unwrap();
    timeout(T, log.wait_committed(r.last))
        .await
        .unwrap()
        .unwrap();
    *log.high_watermark().borrow() == r.last.next() && log.hw_publishes() == before + 1
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rf1_bypass_publishes_without_the_tracker() {
    let _s = FailScenario::setup();
    let tmp = ostro_testkit::fs::tempdir();
    let (node, log) = open(tmp.path()).await;
    log.become_leader(Epoch(1), ReplicaSet::solo(1)).unwrap();

    assert!(one_append_is_one_publication(&log, "a").await);
    fail::cfg(mutants::RF1_BYPASS, "return").unwrap();
    // The HW still advances, but not through the tracker: hw_publishes stays put.
    let before = log.hw_publishes();
    assert!(!one_append_is_one_publication(&log, "b").await);
    assert_eq!(
        log.hw_publishes(),
        before,
        "the bypass publishes nothing via the tracker"
    );
    fail::remove(mutants::RF1_BYPASS);
    assert!(one_append_is_one_publication(&log, "c").await);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignore_min_insync_accepts_appends_below_min() {
    let _s = FailScenario::setup();
    let tmp = ostro_testkit::fs::tempdir();
    let (node, log) = open(tmp.path()).await;
    let shrunk = ReplicaSet {
        replicas: vec![1, 2, 3],
        isr: vec![1],
        isr_version: 2,
        min_insync: 2,
    };
    log.become_leader(Epoch(1), shrunk).unwrap();
    let refused =
        |r: Result<_, LogError>| matches!(r, Err(LogError::NotEnoughReplicas { isr: 1, min: 2 }));

    assert!(refused(log.append(Epoch(1), cmd("a")).await));
    fail::cfg(mutants::IGNORE_MIN_INSYNC, "return").unwrap();
    let r = log.append(Epoch(1), cmd("b")).await;
    assert!(
        !refused(r),
        "with the mutant the append is accepted below min_insync"
    );
    fail::remove(mutants::IGNORE_MIN_INSYNC);
    assert!(refused(log.append(Epoch(1), cmd("c")).await));
    node.shutdown().await;
}
