//! STOR-07: a WAL write/fsync failure fails the partition and is never reported as success.
//!
//! Requires `--features failpoints`; run single-threaded (`--test-threads=1`): the `fail`
//! registry is process-global. The failpoints fire for every partition while enabled, so the
//! "other partitions keep appending" leg runs after the failpoint is removed.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fail::FailScenario;
use ostro_log::{LogError, LogHealth, LogNode, LogNodeConfig, ReplicatedLog, Rf1Log, Role};
use ostro_types::{Epoch, Offset, PartitionCount};
use tokio::time::timeout;

const T: Duration = Duration::from_secs(5);

fn cfg(dir: &Path) -> LogNodeConfig {
    LogNodeConfig::new(dir.join("wal"), PartitionCount::new(2).unwrap(), 1)
}

async fn open(dir: &Path, floors: &[Offset]) -> (LogNode, Vec<Arc<Rf1Log>>) {
    let (node, logs) = LogNode::open(cfg(dir), floors).await.unwrap();
    (node, logs.into_iter().map(Result::unwrap).collect())
}

fn cmd(p: usize, o: u64) -> Bytes {
    Bytes::from(format!("p{p}-cmd-{o:05}-{}", "z".repeat((o % 13) as usize)))
}

async fn append_one(log: &Rf1Log, p: usize) -> Result<ostro_log::AppendReceipt, LogError> {
    let o = log.log_end().0;
    log.append(Epoch(1), vec![cmd(p, o), cmd(p, o + 1)]).await
}

/// Commit 10 batches, inject `failpoint = action`, and check that nothing after the failure is
/// ever reported as success; then that other partitions keep working and a reopen recovers
/// every committed offset with identical bytes.
async fn failure_is_never_success(failpoint: &str, action: &str) {
    let tmp = ostro_testkit::fs::tempdir();
    let (node, logs) = open(tmp.path(), &[Offset::ZERO; 2]).await;
    let (log, other) = (&logs[0], &logs[1]);
    log.set_role(Role::Leader { epoch: Epoch(1) });

    for _ in 0..10 {
        let r = append_one(log, 0).await.unwrap();
        timeout(T, log.wait_committed(r.last))
            .await
            .unwrap()
            .unwrap();
    }
    let hw_before = *log.high_watermark().borrow();
    assert_eq!(hw_before, Offset(20));
    let committed = log.read(Offset::ZERO, hw_before, usize::MAX).await.unwrap();

    fail::cfg(failpoint, action).unwrap();

    // The append may be enqueued (receipt) but its commit must fail.
    let mut oks = 0;
    if let Ok(r) = append_one(log, 0).await {
        match timeout(T, log.wait_committed(r.last)).await {
            Ok(Ok(())) => oks += 1,
            Ok(Err(_)) => {}
            Err(_) => panic!("wait_committed hung after a WAL failure"),
        }
    }
    // Further appends while failed (some may race the poisoning) are never committed.
    for _ in 0..5 {
        if let Ok(r) = append_one(log, 0).await
            && let Ok(Ok(())) = timeout(T, log.wait_committed(r.last)).await
        {
            oks += 1;
        }
    }
    assert_eq!(
        oks, 0,
        "an append was reported committed after the WAL failed"
    );

    // Health Failed within 1 s, role Offline, HW unchanged.
    let mut health = log.health();
    timeout(
        Duration::from_secs(1),
        health.wait_for(|h| matches!(h, LogHealth::Failed { .. })),
    )
    .await
    .expect("health did not become Failed within 1 s")
    .unwrap();
    assert_eq!(*log.role().borrow(), Role::Offline);
    assert_eq!(*log.high_watermark().borrow(), hw_before);

    fail::remove(failpoint);

    // Still failed after the fault clears: no reopen, no retry.
    assert!(matches!(append_one(log, 0).await, Err(LogError::Failed(_))));
    assert!(matches!(
        timeout(T, log.wait_committed(hw_before)).await.unwrap(),
        Err(LogError::Failed(_))
    ));
    log.set_role(Role::Leader { epoch: Epoch(2) });
    assert_eq!(*log.role().borrow(), Role::Offline);
    assert_eq!(*log.high_watermark().borrow(), hw_before);

    // Other partitions on the same node keep appending.
    for _ in 0..5 {
        let r = append_one(other, 1).await.unwrap();
        timeout(T, other.wait_committed(r.last))
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(*other.health().borrow(), LogHealth::Healthy);
    assert_eq!(*other.high_watermark().borrow(), Offset(10));

    node.shutdown().await;
    drop(logs);
    drop(node);

    // Reopen without the failpoint: floor = pre-failure HW (what the state machine could have
    // applied). Recovery succeeds and every committed offset reads back identically.
    let (node, logs) = open(tmp.path(), &[hw_before, Offset::ZERO]).await;
    let log = &logs[0];
    assert!(log.log_end() >= hw_before);
    assert_eq!(*log.health().borrow(), LogHealth::Healthy);
    let again = log.read(Offset::ZERO, hw_before, usize::MAX).await.unwrap();
    assert_eq!(again.len(), committed.len());
    for (a, b) in again.iter().zip(&committed) {
        assert_eq!(a.base, b.base);
        assert_eq!(a.epoch, b.epoch);
        assert_eq!(a.commands, b.commands);
    }
    // And the partition works again after the restart.
    let r = append_one(log, 0).await.unwrap();
    timeout(T, log.wait_committed(r.last))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(logs[1].log_end(), Offset(10));
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_failure_is_never_reported_as_success() {
    let scenario = FailScenario::setup();
    failure_is_never_success("wal::fsync", "return").await;
    scenario.teardown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enospc_partial_write_is_never_reported_as_success() {
    let scenario = FailScenario::setup();
    failure_is_never_success("wal::write", "return(enospc)").await;
    scenario.teardown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eio_write_is_never_reported_as_success() {
    let scenario = FailScenario::setup();
    failure_is_never_success("wal::write", "return").await;
    scenario.teardown();
}
