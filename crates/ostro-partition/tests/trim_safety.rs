//! Retention / trim safety (plan 02-14, STOR-08): segments are deleted only below the durable
//! checkpoint, only after the readiness barrier opened, and never with the Phase 2 default
//! `snapshot_floor = 0`.
#![allow(clippy::disallowed_methods)]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{T, run_on, start_run};
use ostro_log::recovery::{list_segments, segment_dir};
use ostro_log::{LogError, ReplicatedLog};
use ostro_partition::leader::retention::{RetentionConfig, RetentionStats};
use ostro_partition::{ApplyOutcome, Node, NodeConfig, PartitionSlot, StartMode};
use ostro_types::{Command, Offset, PartitionCount, PartitionId};
use tokio::time::{sleep, timeout};

const P0: PartitionId = PartitionId(0);
/// Workload commands per session (plus the session's `LeaderAnnounced`).
const PER_SESSION: usize = 10;

async fn open(dir: &Path, mode: StartMode, retention: RetentionConfig) -> Node {
    let mut cfg = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(1).expect("non-zero"));
    cfg.mode = mode;
    cfg.leader.retention = retention;
    timeout(T, Node::open(cfg))
        .await
        .expect("node open timed out")
        .expect("node open")
}

fn retention(interval_ms: u64, snapshot_floor: Offset) -> (RetentionConfig, Arc<RetentionStats>) {
    let stats = Arc::new(RetentionStats::default());
    (
        RetentionConfig {
            interval: Duration::from_millis(interval_ms),
            snapshot_floor,
            retention_floor: None,
            stats: Some(Arc::clone(&stats)),
        },
        stats,
    )
}

/// Non-empty WAL segment files of partition 0.
fn segments(dir: &Path) -> Vec<PathBuf> {
    list_segments(&segment_dir(&dir.join("wal"), P0), P0)
        .expect("list segments")
        .into_iter()
        .map(|(_, p)| p)
        .filter(|p| std::fs::metadata(p).map(|m| m.len() > 0).unwrap_or(false))
        .collect()
}

async fn workload(node: &Node) {
    let h = node.handle(P0).expect("ready");
    for _ in 0..PER_SESSION {
        let run = run_on(0);
        let o = timeout(T, h.submit_one(Command::start_run(start_run(&run))))
            .await
            .expect("submit timed out")
            .expect("submit");
        assert_eq!(o, ApplyOutcome::Accepted);
    }
}

/// `sessions` node sessions with a workload each (default, inert retention). Each session
/// writes its own segment; returns the log end after every session (segment k holds
/// `[ends[k-1], ends[k])`).
async fn build(dir: &Path, sessions: usize) -> Vec<Offset> {
    let mut ends = Vec::new();
    for _ in 0..sessions {
        let node = open(dir, StartMode::Leader, RetentionConfig::default()).await;
        workload(&node).await;
        ends.push(node.handle(P0).expect("ready").log().log_end());
        node.shutdown().await.expect("shutdown");
    }
    assert_eq!(segments(dir).len(), sessions, "one segment per session");
    ends
}

async fn wait_passes(stats: &RetentionStats, n: u64) {
    timeout(T, async {
        while stats.passes.load(Ordering::Relaxed) < n {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("retention passes timed out");
}

/// Every offset in `[log_start, HW)` reads back contiguously.
async fn check_readable(node: &Node) {
    let log = node.handle(P0).expect("ready").log();
    let start = log.log_start();
    let hw = *log.high_watermark().borrow();
    let got = log.read(start, hw, usize::MAX).await.expect("read");
    let mut next = start;
    for b in &got {
        assert_eq!(b.base, next, "gap in the log after trimming");
        next = b.end();
    }
    assert_eq!(next, hw);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_snapshot_floor_is_inert() {
    let tmp = ostro_testkit::fs::tempdir();
    build(tmp.path(), 4).await;
    let (cfg, stats) = retention(100, RetentionConfig::default().snapshot_floor);
    assert_eq!(cfg.snapshot_floor, Offset(0));
    let node = open(tmp.path(), StartMode::Leader, cfg).await;
    wait_passes(&stats, 3).await;
    let h = node.handle(P0).expect("ready");
    assert_eq!(h.log().log_start(), Offset(0));
    assert_eq!(stats.last_floor.load(Ordering::Relaxed), 0);
    assert_eq!(stats.last_log_start.load(Ordering::Relaxed), 0);
    assert_eq!(segments(tmp.path()).len(), 5, "no segment deleted");
    check_readable(&node).await;
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn injected_snapshot_floor_trims_whole_segments_below_it() {
    let tmp = ostro_testkit::fs::tempdir();
    let ends = build(tmp.path(), 5).await;
    // Middle of the third segment [ends[1], ends[2]).
    let floor = Offset((ends[1].0 + ends[2].0) / 2);
    assert!(floor > ends[1] && floor < ends[2]);

    let (cfg, stats) = retention(500, floor);
    let node = open(tmp.path(), StartMode::Leader, cfg).await;
    let h = node.handle(P0).expect("ready");
    let digest_before = h.store().digest().expect("digest");
    wait_passes(&stats, 1).await;

    // Segments 1 and 2 lay wholly below the floor; segment 3 contains it and stays.
    assert_eq!(h.log().log_start(), ends[1]);
    assert_eq!(stats.last_log_start.load(Ordering::Relaxed), ends[1].0);
    assert_eq!(segments(tmp.path()).len(), 6 - 2);
    // Checkpoint first: the durable checkpoint covered the floor at trim time.
    let ckpt = stats.last_checkpoint.load(Ordering::Relaxed);
    assert_eq!(stats.last_floor.load(Ordering::Relaxed), floor.0);
    assert!(ckpt >= floor.0, "checkpoint {ckpt} below floor {floor}");
    assert!(*h.machine().durable_checkpoint().borrow() >= floor);
    check_readable(&node).await;
    assert_eq!(h.store().digest().expect("digest"), digest_before);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn floor_never_exceeds_the_durable_checkpoint() {
    let tmp = ostro_testkit::fs::tempdir();
    let ends = build(tmp.path(), 4).await;
    let (cfg, stats) = retention(200, Offset(u64::MAX));
    let node = open(tmp.path(), StartMode::Leader, cfg).await;
    wait_passes(&stats, 2).await;
    let h = node.handle(P0).expect("ready");
    let floor = stats.last_floor.load(Ordering::Relaxed);
    let ckpt = stats.last_checkpoint.load(Ordering::Relaxed);
    assert!(floor <= ckpt, "floor {floor} above checkpoint {ckpt}");
    assert_eq!(floor, ckpt, "floor = min(ckpt, MAX) = ckpt");
    let start = h.log().log_start();
    assert!(start.0 <= floor);
    // Everything before this session's segment was below the checkpoint.
    assert_eq!(start, ends[3]);
    assert_eq!(segments(tmp.path()).len(), 1);
    check_readable(&node).await;

    // The trimmed log can no longer rebuild a state store: why trimming stays inert until
    // Phase 8 snapshots.
    node.shutdown().await.expect("shutdown");
    std::fs::remove_dir_all(tmp.path().join("state").join("p0000")).expect("rm state");
    let node = open(tmp.path(), StartMode::Leader, RetentionConfig::default()).await;
    match &node.slots()[0] {
        PartitionSlot::Failed { reason, .. } => {
            assert!(reason.contains("snapshot required"), "{reason}");
        }
        PartitionSlot::Ready(_) => panic!("rebuild from a trimmed log must fail"),
    }
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_retention_before_the_barrier() {
    let tmp = ostro_testkit::fs::tempdir();
    build(tmp.path(), 3).await;
    let (cfg, stats) = retention(50, Offset(u64::MAX));
    let node = open(tmp.path(), StartMode::Inspect, cfg).await;
    sleep(Duration::from_millis(500)).await;
    assert_eq!(stats.passes.load(Ordering::Relaxed), 0);
    assert_eq!(stats.not_ready.load(Ordering::Relaxed), 0);
    let barrier = node.barrier(P0).expect("barrier");
    assert!(matches!(barrier.mint(Offset(1)), Err(LogError::NotReady)));
    assert_eq!(node.handle(P0).expect("ready").log().log_start(), Offset(0));
    assert_eq!(segments(tmp.path()).len(), 3, "no segment deleted");
    node.shutdown().await.expect("shutdown");
}
