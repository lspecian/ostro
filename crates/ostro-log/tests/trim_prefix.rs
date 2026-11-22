//! STOR-08: trim deletes only whole segments below a barrier-minted floor; truncate_to drops a
//! suffix (RF=1: tests and repair only).

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use ostro_log::recovery::{list_segments, segment_dir};
use ostro_log::{
    LogError, LogNode, LogNodeConfig, ReadyBarrier, ReplicaSet, ReplicatedLog, Rf1Log,
};
use ostro_types::{Epoch, Offset, PartitionCount, PartitionId};

const PID: PartitionId = PartitionId(0);

fn cfg(dir: &Path) -> LogNodeConfig {
    LogNodeConfig::new(dir.join("wal"), PartitionCount::new(1).unwrap(), 1)
}

async fn open(dir: &Path) -> (LogNode, Arc<Rf1Log>) {
    let (node, mut logs) = LogNode::open(cfg(dir), &[Offset::ZERO]).await.unwrap();
    let log = logs.remove(0).unwrap();
    lead(&log);
    (node, log)
}

/// Lead the way `Node` does at RF=1: epoch above the logged history, ISR {self}.
fn lead(log: &Rf1Log) {
    let e = log.latest_epoch().map_or(1, |e| e.0 + 1);
    log.become_leader(Epoch(e), ReplicaSet::solo(1)).unwrap();
}

fn cmd(o: u64) -> Bytes {
    Bytes::from(format!("cmd-{o:05}"))
}

/// Append `batches` batches of `per` commands each and wait until committed.
async fn append_batches(log: &Rf1Log, batches: u64, per: u64) {
    let mut last = None;
    for _ in 0..batches {
        let base = log.log_end();
        let cmds = (0..per).map(|i| cmd(base.0 + i)).collect();
        let epoch = log.replica_status().epoch;
        last = Some(log.append(epoch, cmds).await.unwrap().last);
    }
    log.wait_committed(last.unwrap()).await.unwrap();
}

/// Non-empty segment files of the partition.
fn non_empty_segments(dir: &Path) -> Vec<std::path::PathBuf> {
    let wal = dir.join("wal");
    list_segments(&segment_dir(&wal, PID), PID)
        .unwrap()
        .into_iter()
        .map(|(_, p)| p)
        .filter(|p| std::fs::metadata(p).unwrap().len() > 0)
        .collect()
}

/// Five segments of 6 offsets each: segment k holds [6k, 6k + 6). Each LogNode session opens
/// a new segment on its first append (research W4).
async fn five_segments(dir: &Path) {
    for _ in 0..5 {
        let (node, log) = open(dir).await;
        append_batches(&log, 3, 2).await;
        node.shutdown().await;
        drop(log);
        drop(node);
    }
    assert_eq!(non_empty_segments(dir).len(), 5);
}

async fn check_read_all(log: &Rf1Log) {
    let start = log.log_start();
    let hw = *log.high_watermark().borrow();
    let got = log.read(start, hw, usize::MAX).await.unwrap();
    let mut next = start;
    for b in &got {
        assert_eq!(b.base, next);
        for (o, c) in b.commands_with_offsets() {
            assert_eq!(c, &cmd(o.0));
        }
        next = b.end();
    }
    assert_eq!(next, hw);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trim_deletes_only_whole_segments_below_floor() {
    let tmp = ostro_testkit::fs::tempdir();
    five_segments(tmp.path()).await;
    let (node, log) = open(tmp.path()).await;
    assert_eq!(log.log_end(), Offset(30));
    let before = non_empty_segments(tmp.path());

    let barrier = ReadyBarrier::new();
    assert!(matches!(barrier.mint(Offset(14)), Err(LogError::NotReady)));
    barrier.open();

    // Floor in the middle of segment 3 ([12, 18)): segments 1-2 go, segment 3 stays.
    let new_start = log
        .trim_prefix(barrier.mint(Offset(14)).unwrap())
        .await
        .unwrap();
    assert_eq!(new_start, Offset(12));
    assert_eq!(log.log_start(), Offset(12));
    let after = non_empty_segments(tmp.path());
    assert_eq!(after, before[2..].to_vec());
    assert!(matches!(
        log.read(Offset(0), Offset(30), usize::MAX).await,
        Err(LogError::OutOfRange(Offset(0)))
    ));
    assert!(matches!(
        log.read(Offset(11), Offset(30), usize::MAX).await,
        Err(LogError::OutOfRange(_))
    ));
    check_read_all(&log).await;

    // Idempotent: same floor again trims nothing.
    let again = log
        .trim_prefix(barrier.mint(Offset(14)).unwrap())
        .await
        .unwrap();
    assert_eq!(again, Offset(12));
    assert_eq!(non_empty_segments(tmp.path()), after);

    // The active segment survives even when the floor is at/above LEO.
    append_batches(&log, 1, 2).await; // opens the active segment 6 at offset 30
    let leo = log.log_end();
    let s = log
        .trim_prefix(barrier.mint(Offset(leo.0 + 1000)).unwrap())
        .await
        .unwrap();
    assert_eq!(s, Offset(30));
    let left = non_empty_segments(tmp.path());
    assert_eq!(left.len(), 1, "only the active segment remains: {left:?}");
    check_read_all(&log).await;
    // Every session leads a new term: five writing sessions (epochs 1-5), then this one (6).
    assert_eq!(log.epochs().0, vec![(Epoch(6), Offset(30))]);

    // Appends continue; a reopen agrees with the trimmed log.
    append_batches(&log, 2, 1).await;
    let leo = log.log_end();
    node.shutdown().await;
    drop(log);
    drop(node);
    let (node, log) = open(tmp.path()).await;
    assert_eq!(log.log_start(), Offset(30));
    assert_eq!(log.log_end(), leo);
    check_read_all(&log).await;
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trim_with_floor_zero_is_inert() {
    let tmp = ostro_testkit::fs::tempdir();
    five_segments(tmp.path()).await;
    let (node, log) = open(tmp.path()).await;
    let barrier = ReadyBarrier::new();
    barrier.open();
    let s = log
        .trim_prefix(barrier.mint(Offset::ZERO).unwrap())
        .await
        .unwrap();
    assert_eq!(s, Offset::ZERO);
    assert_eq!(non_empty_segments(tmp.path()).len(), 5);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_to_drops_the_suffix() {
    let tmp = ostro_testkit::fs::tempdir();
    let (node, log) = open(tmp.path()).await;
    append_batches(&log, 10, 1).await;
    assert_eq!(*log.high_watermark().borrow(), Offset(10));

    let new = log.truncate_to(Offset(6)).await.unwrap();
    assert_eq!(new, Offset(6));
    assert_eq!(log.log_end(), Offset(6));
    assert_eq!(*log.high_watermark().borrow(), Offset(6));
    // A no-op truncation above LEO.
    assert_eq!(log.truncate_to(Offset(50)).await.unwrap(), Offset(6));

    let r = log.append(Epoch(1), vec![cmd(6)]).await.unwrap();
    assert_eq!(r.base, Offset(6));
    log.wait_committed(r.last).await.unwrap();
    assert_eq!(*log.high_watermark().borrow(), Offset(7));
    check_read_all(&log).await;

    // Truncating inside a batch cuts back to the batch start.
    append_batches(&log, 1, 4).await; // [7, 11)
    let new = log.truncate_to(Offset(9)).await.unwrap();
    assert_eq!(new, Offset(7));
    assert_eq!(log.log_end(), Offset(7));
    check_read_all(&log).await;

    node.shutdown().await;
    drop(log);
    drop(node);
    let (node, log) = open(tmp.path()).await;
    assert_eq!(log.log_end(), Offset(7));
    check_read_all(&log).await;
    node.shutdown().await;
}
