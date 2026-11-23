//! Retention / trim safety (plan 02-14, STOR-08; plan 08-09, BACK-04 SC4): segments are deleted
//! only below the durable checkpoint, only after the readiness barrier opened, never at or
//! above the oldest retained local snapshot nor the bottomless upload floor, and never while
//! no snapshot exists. Restart from snapshot + tail equals the pre-crash state.
//!
//! The SC4 state machine (`sc4_random_snapshot_trim_kill_sequences`) applies random sequences
//! of {apply N, snapshot, retention pass, kill + reopen, kill + delete state + reopen, corrupt
//! newest snapshot}. "Kill" is an in-process stop of the node (services, machines, WAL) that
//! keeps every file exactly as the interrupted operation left it; real `kill -9` runs are the
//! crash_loop suite of ostro-crashtest (`crash_loop_snapshots`).
//!
//! Failpoint cases (`--features failpoints -- --test-threads=1`): an injected crash at
//! `snapshot::before_rename`, `snapshot::before_dir_fsync` and `trim::after_delete` is
//! recoverable and the rebuilt digest is unchanged.
#![allow(clippy::disallowed_methods)]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{OPEN_T, T, run_on, start_run};
use ostro_log::recovery::{list_segments, segment_dir};
use ostro_log::{LogError, ReplicatedLog};
use ostro_partition::leader::retention::{RetentionConfig, RetentionStats};
use ostro_partition::store::snapshot::verify_snapshot;
use ostro_partition::{ApplyOutcome, Node, NodeConfig, PartitionSlot, StartMode};
use ostro_types::{Command, Offset, PartitionCount, PartitionId};
use tokio::time::{sleep, timeout};

const P0: PartitionId = PartitionId(0);
/// Workload commands per session (plus the session's `LeaderAnnounced`).
const PER_SESSION: usize = 10;
/// Effectively never (ticks and passes are forced by the tests).
const NEVER: Duration = Duration::from_secs(3600);

fn base_cfg(dir: &Path, mode: StartMode, retention: RetentionConfig) -> NodeConfig {
    let mut cfg = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(1).expect("non-zero"));
    cfg.mode = mode;
    cfg.leader.retention = retention;
    // The cadence never fires on its own here: snapshots are forced.
    cfg.snapshots.interval = NEVER;
    cfg.snapshots.min_interval = NEVER;
    cfg
}

async fn open_cfg(cfg: NodeConfig) -> Node {
    timeout(OPEN_T, Node::open(cfg))
        .await
        .expect("node open timed out")
        .expect("node open")
}

async fn open(dir: &Path, mode: StartMode, retention: RetentionConfig) -> Node {
    open_cfg(base_cfg(dir, mode, retention)).await
}

fn retention(interval_ms: u64) -> (RetentionConfig, Arc<RetentionStats>) {
    let stats = Arc::new(RetentionStats::default());
    (
        RetentionConfig {
            interval: Duration::from_millis(interval_ms),
            retention_floor: None,
            stats: Some(Arc::clone(&stats)),
        },
        stats,
    )
}

fn inert() -> RetentionConfig {
    RetentionConfig {
        interval: NEVER,
        ..RetentionConfig::default()
    }
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

/// `.osnap` files of partition 0 by name offset, ascending.
fn snapshot_files(dir: &Path) -> Vec<(u64, PathBuf)> {
    let d = dir.join("snapshots").join("p0000");
    let mut v: Vec<(u64, PathBuf)> = std::fs::read_dir(&d)
        .map(|it| {
            it.filter_map(|e| e.ok())
                .filter_map(|e| {
                    let name = e.file_name().to_string_lossy().into_owned();
                    let stem = name.strip_suffix(".osnap")?;
                    stem.parse::<u64>().ok().map(|o| (o, e.path()))
                })
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

async fn submit_n(node: &Node, n: usize) {
    let h = node.handle(P0).expect("ready");
    let cmds: Vec<Command> = (0..n)
        .map(|_| Command::start_run(start_run(&run_on(0))))
        .collect();
    let out = timeout(T, h.submit(cmds))
        .await
        .expect("submit timed out")
        .expect("submit");
    assert!(out.iter().all(|o| *o == ApplyOutcome::Accepted));
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

/// `sessions` node sessions with a workload each (inert retention). Each session writes its
/// own segment; returns the log end after every session (segment k holds
/// `[ends[k-1], ends[k])`). With `snapshot_last`, the last session ends with a snapshot.
async fn build(dir: &Path, sessions: usize, snapshot_last: bool) -> Vec<Offset> {
    let mut ends = Vec::new();
    for i in 0..sessions {
        let node = open(dir, StartMode::Leader, inert()).await;
        workload(&node).await;
        ends.push(node.handle(P0).expect("ready").log().log_end());
        if snapshot_last && i + 1 == sessions {
            node.snapshot_now(P0)
                .await
                .expect("snapshot")
                .expect("new snapshot");
        }
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
    let h = node.handle(P0).expect("ready");
    let log = h.log();
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
async fn no_snapshot_means_no_trim() {
    let tmp = ostro_testkit::fs::tempdir();
    build(tmp.path(), 4, false).await;
    let (cfg, stats) = retention(100);
    let node = open(tmp.path(), StartMode::Leader, cfg).await;
    assert_eq!(node.local_floor(P0), Some(Offset(0)));
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
async fn upload_floor_trims_whole_segments_below_it() {
    let tmp = ostro_testkit::fs::tempdir();
    let ends = build(tmp.path(), 5, true).await;
    // Middle of the third segment [ends[1], ends[2]).
    let floor = Offset((ends[1].0 + ends[2].0) / 2);
    assert!(floor > ends[1] && floor < ends[2]);

    let (rcfg, stats) = retention(200);
    let mut cfg = base_cfg(tmp.path(), StartMode::Leader, rcfg);
    cfg.backup_enabled = true;
    let node = open_cfg(cfg).await;
    let h = node.handle(P0).expect("ready");
    assert_eq!(
        node.local_floor(P0),
        Some(ends[4]),
        "the snapshot covers 5 sessions"
    );
    let up = node.floor_handles(P0).expect("handles").upload;
    assert_eq!(
        *up.borrow(),
        Some(Offset(0)),
        "B4: upload floor starts at log_start"
    );
    wait_passes(&stats, 1).await;
    assert_eq!(
        h.log().log_start(),
        Offset(0),
        "nothing trimmed before an upload"
    );
    let digest_before = h.store().digest().expect("digest");

    up.send_replace(Some(floor));
    let n = stats.passes.load(Ordering::Relaxed);
    wait_passes(&stats, n + 2).await;
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
async fn floor_is_bounded_by_checkpoint_and_snapshot() {
    let tmp = ostro_testkit::fs::tempdir();
    let ends = build(tmp.path(), 4, true).await;
    let (cfg, stats) = retention(200);
    let node = open(tmp.path(), StartMode::Leader, cfg).await;
    wait_passes(&stats, 2).await;
    let h = node.handle(P0).expect("ready");
    let floor = stats.last_floor.load(Ordering::Relaxed);
    let ckpt = stats.last_checkpoint.load(Ordering::Relaxed);
    assert!(floor <= ckpt, "floor {floor} above checkpoint {ckpt}");
    assert_eq!(floor, ckpt.min(ends[3].0), "floor = min(ckpt, local)");
    let start = h.log().log_start();
    assert!(start.0 <= floor);
    // Every earlier session's segment lay wholly below the snapshot at ends[3].
    assert_eq!(start, ends[3]);
    assert_eq!(segments(tmp.path()).len(), 1);
    check_readable(&node).await;
    let live = h.store().digest().expect("digest");
    let applied = *h.machine().applied().borrow();
    drop(h); // an owned handle keeps the store open
    node.shutdown().await.expect("shutdown");

    // The trimmed log alone cannot rebuild the state, but snapshot + tail can.
    std::fs::remove_dir_all(tmp.path().join("state").join("p0000")).expect("rm state");
    let node = open(tmp.path(), StartMode::Inspect, inert()).await;
    assert_eq!(node.snapshot_installed(P0), Some(ends[3]));
    let h = node.handle(P0).expect("rebuilt from snapshot + tail");
    assert_eq!(*h.machine().applied().borrow(), applied);
    assert_eq!(h.store().digest().expect("digest"), live);
    drop(h);
    node.shutdown().await.expect("shutdown");

    // Without the snapshot: "snapshot required".
    std::fs::remove_dir_all(tmp.path().join("state").join("p0000")).expect("rm state");
    std::fs::remove_dir_all(tmp.path().join("snapshots")).expect("rm snapshots");
    let node = open(tmp.path(), StartMode::Leader, inert()).await;
    match &node.slots()[0] {
        PartitionSlot::Failed { reason, .. } => {
            assert!(
                reason.contains("snapshot required: run 'ostro restore'"),
                "{reason}"
            );
        }
        PartitionSlot::Ready(_) => panic!("rebuild from a trimmed log must fail"),
    }
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_retention_before_the_barrier() {
    let tmp = ostro_testkit::fs::tempdir();
    build(tmp.path(), 3, true).await;
    let (cfg, stats) = retention(50);
    let node = open(tmp.path(), StartMode::Inspect, cfg).await;
    sleep(Duration::from_millis(500)).await;
    assert_eq!(stats.passes.load(Ordering::Relaxed), 0);
    assert_eq!(stats.not_ready.load(Ordering::Relaxed), 0);
    let barrier = node.barrier(P0).expect("barrier");
    assert!(matches!(barrier.mint(Offset(1)), Err(LogError::NotReady)));
    assert!(
        node.retention_pass(P0).await.is_err(),
        "no services in Inspect"
    );
    assert_eq!(node.handle(P0).expect("ready").log().log_start(), Offset(0));
    assert_eq!(segments(tmp.path()).len(), 3, "no segment deleted");
    node.shutdown().await.expect("shutdown");
}

// ------------------------------------------------------------------- SC4 state machine

/// Small WAL segments so retention has something to delete.
fn sc4_cfg(dir: &Path, mode: StartMode) -> NodeConfig {
    let mut cfg = base_cfg(dir, mode, inert());
    cfg.wal.enable_rotation = true;
    cfg.wal.rotation_size_bytes = 2 * 1024;
    cfg
}

/// The live `(applied, digest)` once every command was applied.
async fn model(node: &Node) -> (Offset, [u8; 32]) {
    let h = node.handle(P0).expect("ready");
    let hw = *h.log().high_watermark().borrow();
    if hw > Offset(0) {
        timeout(T, h.machine().wait_applied(Offset(hw.0 - 1)))
            .await
            .expect("apply timed out")
            .expect("applied");
    }
    (
        *h.machine().applied().borrow(),
        h.store().digest().expect("digest"),
    )
}

/// SC4: `log_start <= oldest retained snapshot`, and no trimming without a snapshot.
fn check_sc4(node: &Node, dir: &Path, ctx: &str) {
    let start = node.handle(P0).expect("ready").log().log_start();
    match snapshot_files(dir).first() {
        Some((oldest, _)) => assert!(
            start.0 <= *oldest,
            "{ctx}: log_start {start} above the oldest retained snapshot {oldest}"
        ),
        None => assert_eq!(start, Offset(0), "{ctx}: trimmed without a snapshot"),
    }
}

/// Some retained snapshot with `log_start <= S <= LEO` verifies completely.
fn usable_snapshot_exists(dir: &Path, start: Offset, leo: Offset) -> bool {
    snapshot_files(dir).iter().any(|(o, p)| {
        *o >= start.0
            && *o <= leo.0
            && std::fs::File::open(p)
                .ok()
                .and_then(|f| verify_snapshot(std::io::BufReader::new(f)).ok())
                .is_some()
    })
}

/// Kill, optionally delete the state, check the reopened state against the model in Inspect
/// mode, then reopen in Leader mode. `None` = the partition (rightly) needs `ostro restore`.
async fn kill_and_reopen(node: Node, dir: &Path, delete_state: bool, step: usize) -> Option<Node> {
    let (applied, digest) = model(&node).await;
    let (start, leo) = {
        let h = node.handle(P0).expect("ready");
        let log = h.log();
        (log.log_start(), log.log_end())
    };
    node.shutdown().await.expect("kill");
    if !delete_state {
        // The state survived: reopen directly (the digest check below is for rebuilds).
        let node = open_cfg(sc4_cfg(dir, StartMode::Leader)).await;
        let h = node.handle(P0).expect("reopened");
        assert!(*h.machine().applied().borrow() > applied, "step {step}");
        check_sc4(&node, dir, &format!("step {step} (reopened)"));
        return Some(node);
    }
    std::fs::remove_dir_all(dir.join("state").join("p0000")).expect("rm state");
    let inspect = open_cfg(sc4_cfg(dir, StartMode::Inspect)).await;
    match &inspect.slots()[0] {
        PartitionSlot::Failed { reason, .. } => {
            assert!(
                reason.contains("snapshot required"),
                "step {step}: {reason}"
            );
            assert!(
                !usable_snapshot_exists(dir, start, leo),
                "step {step}: 'snapshot required' while a usable snapshot exists ({reason})"
            );
            inspect.shutdown().await.expect("shutdown");
            return None;
        }
        PartitionSlot::Ready(h) => {
            assert_eq!(*h.machine().applied().borrow(), applied, "step {step}");
            assert_eq!(
                h.store().digest().expect("digest"),
                digest,
                "step {step}: recovered digest differs (delete_state {delete_state})"
            );
        }
    }
    check_sc4(&inspect, dir, &format!("step {step} (reopened)"));
    inspect.shutdown().await.expect("shutdown");
    let node = open_cfg(sc4_cfg(dir, StartMode::Leader)).await;
    assert!(matches!(node.slots()[0], PartitionSlot::Ready(_)));
    Some(node)
}

fn corrupt_newest(dir: &Path) {
    if let Some((_, p)) = snapshot_files(dir).last() {
        let mut b = std::fs::read(p).expect("read snapshot");
        let i = b.len() / 2;
        b[i] ^= 0x5a;
        std::fs::write(p, b).expect("write snapshot");
    }
}

// The random sequences run in the default build; the failpoint run (`--test-threads=1`)
// covers the failpoint cases at the end of this file.
#[cfg(not(feature = "failpoints"))]
mod sc4_random {
    use super::*;
    use proptest::prelude::*;

    #[derive(Clone, Debug)]
    enum Op {
        Apply(usize),
        Snapshot,
        Retention,
        Kill,
        KillDeleteState,
        CorruptNewest,
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            5 => (1usize..30).prop_map(Op::Apply),
            3 => Just(Op::Snapshot),
            3 => Just(Op::Retention),
            1 => Just(Op::Kill),
            1 => Just(Op::KillDeleteState),
            1 => Just(Op::CorruptNewest),
        ]
    }

    async fn run_ops(ops: Vec<Op>) {
        let tmp = ostro_testkit::fs::tempdir();
        let dir = tmp.path();
        let mut node = open_cfg(sc4_cfg(dir, StartMode::Leader)).await;
        for (step, op) in ops.into_iter().enumerate() {
            match op {
                Op::Apply(n) => submit_n(&node, n).await,
                Op::Snapshot => {
                    node.snapshot_now(P0).await.expect("snapshot");
                }
                Op::Retention => {
                    node.retention_pass(P0).await.expect("retention pass");
                }
                Op::CorruptNewest => corrupt_newest(dir),
                Op::Kill | Op::KillDeleteState => {
                    let del = matches!(op, Op::KillDeleteState);
                    match kill_and_reopen(node, dir, del, step).await {
                        Some(n) => node = n,
                        None => return,
                    }
                }
            }
            check_sc4(&node, dir, &format!("step {step} ({op:?})"));
        }
        // Final: rebuild from snapshot + tail.
        if let Some(n) = kill_and_reopen(node, dir, true, usize::MAX).await {
            n.shutdown().await.expect("shutdown");
        }
    }

    fn sc4_cases() -> u32 {
        std::env::var("OSTRO_SC4_CASES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(64)
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: sc4_cases(), max_shrink_iters: 16, ..ProptestConfig::default() })]

        #[test]
        fn sc4_random_snapshot_trim_kill_sequences(ops in prop::collection::vec(op(), 30)) {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(run_ops(ops));
        }
    }
}

/// A deterministic sequence that always trims and rebuilds (guards the generator).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sc4_trims_and_rebuilds() {
    let tmp = ostro_testkit::fs::tempdir();
    let dir = tmp.path();
    let node = open_cfg(sc4_cfg(dir, StartMode::Leader)).await;
    for _ in 0..3 {
        submit_n(&node, 60).await;
        node.snapshot_now(P0).await.expect("snapshot").expect("new");
    }
    submit_n(&node, 60).await;
    let start = node.retention_pass(P0).await.expect("pass");
    let snaps = snapshot_files(dir);
    assert_eq!(snaps.len(), 2, "keep = 2");
    assert!(start > Offset(0), "something was trimmed");
    assert!(start.0 <= snaps[0].0);
    let node = kill_and_reopen(node, dir, true, 0).await.expect("reopen");
    assert_eq!(
        node.snapshot_installed(P0),
        None,
        "Leader reopen used the installed state"
    );
    corrupt_newest(dir);
    let node = kill_and_reopen(node, dir, true, 1)
        .await
        .expect("falls back to the older snapshot");
    node.shutdown().await.expect("shutdown");
}

// ------------------------------------------------------------------------- failpoints

#[cfg(feature = "failpoints")]
mod failpoints {
    use super::*;

    /// Two snapshots' worth of state, then an injected crash at `site` during the next
    /// snapshot (or retention pass), then kill + rebuild: recoverable, digest unchanged.
    async fn crash_at(site: &str) {
        let _scenario = fail::FailScenario::setup();
        let tmp = ostro_testkit::fs::tempdir();
        let dir = tmp.path();
        let node = open_cfg(sc4_cfg(dir, StartMode::Leader)).await;
        submit_n(&node, 60).await;
        let s1 = node.snapshot_now(P0).await.expect("snap").expect("new");
        submit_n(&node, 60).await;
        fail::cfg(site, "return").expect("cfg");
        if site.starts_with("snapshot::") {
            let e = node.snapshot_now(P0).await.expect_err("injected");
            assert!(e.contains(site), "{e}");
            assert_eq!(
                node.local_floor(P0),
                Some(s1.applied_offset),
                "the floor did not move"
            );
            fail::remove(site);
            let start = node.retention_pass(P0).await.expect("pass");
            assert!(start <= s1.applied_offset);
        } else {
            node.snapshot_now(P0).await.expect("snap").expect("new");
            submit_n(&node, 10).await;
            let e = node.retention_pass(P0).await.expect_err("injected");
            assert!(e.contains(site), "{e}");
            fail::remove(site);
        }
        check_sc4(&node, dir, site);
        let node = kill_and_reopen(node, dir, false, 0)
            .await
            .expect("recoverable");
        // No leftovers from the interrupted write.
        let names: Vec<String> = std::fs::read_dir(dir.join("snapshots").join("p0000"))
            .expect("dir")
            .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
            .collect();
        assert!(names.iter().all(|n| !n.starts_with(".tmp-")), "{names:?}");
        let node = kill_and_reopen(node, dir, true, 1)
            .await
            .expect("rebuild from snapshot + tail");
        // And the service keeps working.
        submit_n(&node, 5).await;
        node.snapshot_now(P0).await.expect("snap").expect("new");
        check_sc4(&node, dir, "after recovery");
        node.shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crash_before_rename() {
        crash_at("snapshot::before_rename").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crash_before_dir_fsync() {
        crash_at("snapshot::before_dir_fsync").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crash_after_trim_delete() {
        crash_at("trim::after_delete").await;
    }
}
