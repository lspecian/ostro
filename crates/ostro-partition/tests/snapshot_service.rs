//! Local snapshot service (plan 08-09, P8-D2): cadence, keep = 2, durable floor publication,
//! trimming bounded by the oldest retained snapshot and by the optional upload floor (B4),
//! the startup scan, and the Node handles for 08-13.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::{OPEN_T, T, run_on, start_run};
use ostro_log::ReplicatedLog;
use ostro_partition::leader::retention::RetentionConfig;
use ostro_partition::leader::snapshot::{BLOBREFS_EXT, snapshot_file_name};
use ostro_partition::store::snapshot::verify_snapshot;
use ostro_partition::{ApplyOutcome, Node, NodeConfig};
use ostro_types::{Command, Offset, PartitionCount, PartitionId};
use sha2::{Digest as _, Sha256};
use tokio::time::{sleep, timeout};

const P0: PartitionId = PartitionId(0);
const NEVER: Duration = Duration::from_secs(3600);

/// Small segments; cadence and retention as given.
fn cfg(dir: &Path, interval: Duration, bytes: u64, retention_ms: u64) -> NodeConfig {
    let mut cfg = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(1).expect("non-zero"));
    cfg.wal.enable_rotation = true;
    cfg.wal.rotation_size_bytes = 2 * 1024;
    cfg.snapshots.interval = interval;
    cfg.snapshots.min_interval = Duration::ZERO;
    cfg.snapshots.bytes_threshold = bytes;
    cfg.snapshots.keep = 2;
    cfg.leader.retention = RetentionConfig {
        interval: Duration::from_millis(retention_ms),
        ..RetentionConfig::default()
    };
    cfg
}

async fn open(cfg: NodeConfig) -> Node {
    timeout(OPEN_T, Node::open(cfg))
        .await
        .expect("node open timed out")
        .expect("node open")
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

/// `.osnap` offsets on disk, ascending (read-only: `scan_dir` would delete `.tmp-*` files of
/// a snapshot in progress).
fn osnaps(dir: &Path) -> Vec<u64> {
    let mut v: Vec<u64> = std::fs::read_dir(dir.join("snapshots").join("p0000"))
        .map(|it| {
            it.filter_map(|e| e.ok())
                .filter_map(|e| {
                    let n = e.file_name().to_string_lossy().into_owned();
                    n.strip_suffix(".osnap")?.parse().ok()
                })
                .collect()
        })
        .unwrap_or_default();
    v.sort_unstable();
    v
}

/// Watches `log_start <= oldest .osnap on disk` (or no trim without one) while the test runs.
fn spawn_sc4_monitor(
    node: &Node,
    dir: &Path,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<u64> {
    let log = Arc::clone(node.handle(P0).expect("ready").log());
    let dir = dir.to_path_buf();
    tokio::spawn(async move {
        let mut checks = 0u64;
        while !stop.load(Ordering::Relaxed) {
            // Read the floor first: a snapshot deleted after this read was retained before it.
            let snaps = osnaps(&dir);
            let start = log.log_start();
            // Snapshots only get newer, so re-read after log_start to avoid a false alarm from
            // a delete that raced the first scan.
            let after = osnaps(&dir);
            let oldest = snaps
                .first()
                .copied()
                .into_iter()
                .chain(after.first().copied())
                .max();
            match oldest {
                Some(o) => assert!(start.0 <= o, "log_start {start} above oldest snapshot {o}"),
                None => assert_eq!(start, Offset(0), "trimmed without a snapshot"),
            }
            checks += 1;
            sleep(Duration::from_millis(5)).await;
        }
        checks
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cadence_keep_two_and_floor() {
    let tmp = ostro_testkit::fs::tempdir();
    let dir = tmp.path();
    let node = open(cfg(dir, Duration::from_millis(100), 1, 50)).await;
    let mut events = node.snapshot_events();
    let stop = Arc::new(AtomicBool::new(false));
    let mon = spawn_sc4_monitor(&node, dir, Arc::clone(&stop));
    let mut taken = Vec::new();
    for _ in 0..40 {
        submit_n(&node, 20).await;
        sleep(Duration::from_millis(40)).await;
        while let Ok(ev) = events.try_recv() {
            taken.push(ev);
        }
    }
    let deadline = Instant::now() + T;
    while taken.len() < 4 && Instant::now() < deadline {
        if let Ok(Ok(ev)) = timeout(Duration::from_millis(200), events.recv()).await {
            taken.push(ev);
        }
    }
    // Let a retention pass run after the last snapshot.
    sleep(Duration::from_millis(300)).await;
    stop.store(true, Ordering::Relaxed);
    let checks = mon.await.expect("monitor");
    assert!(checks > 10, "monitor ran {checks} times");
    assert!(taken.len() >= 3, "only {} snapshots taken", taken.len());
    let h = node.handle(P0).expect("ready");
    let start = h.log().log_start();
    let on_disk = osnaps(dir);
    assert!(on_disk.len() <= 2, "keep = 2: {on_disk:?}");
    assert!(start > Offset(0), "log_start advanced");
    assert!(start.0 <= on_disk[0], "log_start {start} <= {on_disk:?}");
    assert_eq!(node.local_floor(P0), Some(Offset(on_disk[0])));
    assert_eq!(node.local_snapshots(P0).await.len(), on_disk.len());
    // Every event is ascending, names its file and hash; retained files verify completely
    // and carry a blob-ref sidecar.
    for w in taken.windows(2) {
        assert!(w[0].applied_offset < w[1].applied_offset);
    }
    for ev in &taken {
        assert_eq!(ev.partition, P0);
        assert_eq!(
            ev.path.file_name().expect("name").to_string_lossy(),
            snapshot_file_name(ev.applied_offset)
        );
        if on_disk.contains(&ev.applied_offset.0) {
            let bytes = std::fs::read(&ev.path).expect("read");
            let sha: [u8; 32] = Sha256::digest(&bytes).into();
            assert_eq!(sha, ev.sha256);
            let info = verify_snapshot(bytes.as_slice()).expect("verify");
            assert_eq!(info.applied_offset, ev.applied_offset.0);
            assert_eq!(info.logical_digest, ev.info.logical_digest);
            let side = std::fs::read(ev.path.with_extension(BLOBREFS_EXT)).expect("sidecar");
            assert_eq!(side.len(), 4 + 32 * ev.info.blob_refs.len());
        } else {
            assert!(!ev.path.exists(), "deleted beyond keep");
        }
    }
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_floor_caps_trimming() {
    let tmp = ostro_testkit::fs::tempdir();
    let dir = tmp.path();
    let mut c = cfg(dir, Duration::from_millis(100), 1, 50);
    c.backup_enabled = true;
    let node = open(c).await;
    let up = node.floor_handles(P0).expect("handles").upload;
    assert_eq!(*up.borrow(), Some(Offset(0)));
    let h = node.handle(P0).expect("ready");
    for _ in 0..20 {
        submit_n(&node, 20).await;
        sleep(Duration::from_millis(40)).await;
    }
    sleep(Duration::from_millis(300)).await;
    assert!(
        node.local_floor(P0).expect("floor") > Offset(0),
        "snapshots taken"
    );
    assert_eq!(
        h.log().log_start(),
        Offset(0),
        "upload floor holds trimming"
    );

    let leo = h.log().log_end();
    let x = Offset(leo.0 / 2);
    up.send_replace(Some(x));
    let deadline = Instant::now() + T;
    while h.log().log_start() == Offset(0) && Instant::now() < deadline {
        sleep(Duration::from_millis(20)).await;
    }
    let start = h.log().log_start();
    assert!(start > Offset(0), "trimming resumed");
    assert!(start <= x, "log_start {start} above the upload floor {x}");
    // More load and snapshots never pass x.
    for _ in 0..10 {
        submit_n(&node, 20).await;
        sleep(Duration::from_millis(40)).await;
    }
    sleep(Duration::from_millis(200)).await;
    assert!(h.log().log_start() <= x);
    node.shutdown().await.expect("shutdown");
}

/// B4: with backup on, the upload floor is Some(log_start) from `Node::open` on, so a
/// retention pass before anyone touches `floor_handles` trims nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b4_retention_before_first_upload_trims_nothing() {
    let tmp = ostro_testkit::fs::tempdir();
    let dir = tmp.path();
    // Session 1: snapshots present (several segments below them).
    {
        let node = open(cfg(dir, NEVER, u64::MAX, 3_600_000)).await;
        for _ in 0..3 {
            submit_n(&node, 60).await;
            node.snapshot_now(P0).await.expect("snap").expect("new");
        }
        node.shutdown().await.expect("shutdown");
    }
    assert_eq!(osnaps(dir).len(), 2);
    // Session 2: backup on; force a pass immediately.
    let mut c = cfg(dir, NEVER, u64::MAX, 3_600_000);
    c.backup_enabled = true;
    let node = open(c).await;
    let start0 = node.handle(P0).expect("ready").log().log_start();
    assert_eq!(start0, Offset(0));
    let start = node.retention_pass(P0).await.expect("pass");
    assert_eq!(
        start, start0,
        "log_start did not move before the first upload"
    );
    // The uploader publishes x: trimming up to x.
    let snaps = osnaps(dir);
    let x = Offset(snaps[0]);
    node.floor_handles(P0)
        .expect("handles")
        .upload
        .send_replace(Some(x));
    let start = node.retention_pass(P0).await.expect("pass");
    assert!(start > Offset(0) && start <= x, "log_start {start}, x {x}");
    node.shutdown().await.expect("shutdown");

    // Control: without backup the same pass trims right away.
    let tmp2 = ostro_testkit::fs::tempdir();
    let d2 = tmp2.path();
    let node = open(cfg(d2, NEVER, u64::MAX, 3_600_000)).await;
    for _ in 0..3 {
        submit_n(&node, 60).await;
        node.snapshot_now(P0).await.expect("snap").expect("new");
    }
    assert!(
        node.floor_handles(P0)
            .expect("handles")
            .upload
            .borrow()
            .is_none()
    );
    let start = node.retention_pass(P0).await.expect("pass");
    assert!(start > Offset(0), "no backup: trimmed to the local floor");
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_scan_removes_tmp_and_sets_floor() {
    let tmp = ostro_testkit::fs::tempdir();
    let dir = tmp.path();
    let node = open(cfg(dir, NEVER, u64::MAX, 3_600_000)).await;
    assert_eq!(
        node.local_floor(P0),
        Some(Offset(0)),
        "no snapshot: floor 0"
    );
    assert_eq!(node.snapshot_dir(P0), dir.join("snapshots").join("p0000"));
    assert!(node.snapshot_now(P0).await.expect("snap").is_some());
    assert!(
        node.snapshot_now(P0).await.expect("snap").is_none(),
        "nothing applied since"
    );
    submit_n(&node, 10).await;
    let s2 = node.snapshot_now(P0).await.expect("snap").expect("new");
    submit_n(&node, 10).await;
    let s3 = node.snapshot_now(P0).await.expect("snap").expect("new");
    assert_eq!(node.local_floor(P0), Some(s2.applied_offset));
    let sdir = node.snapshot_dir(P0);
    node.shutdown().await.expect("shutdown");

    std::fs::write(sdir.join(".tmp-00000000000000000099"), b"partial").expect("tmp");
    std::fs::write(sdir.join("00000000000000000077.blobrefs"), b"orphan").expect("orphan");
    let node = open(cfg(dir, NEVER, u64::MAX, 3_600_000)).await;
    assert_eq!(node.local_floor(P0), Some(s2.applied_offset));
    let left: Vec<String> = std::fs::read_dir(&sdir)
        .expect("dir")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    assert!(!left.iter().any(|n| n.starts_with(".tmp-")), "{left:?}");
    assert!(
        !left.iter().any(|n| n.starts_with("00000000000000000077")),
        "{left:?}"
    );
    assert_eq!(osnaps(dir), vec![s2.applied_offset.0, s3.applied_offset.0]);
    node.shutdown().await.expect("shutdown");

    // Disabled: floor fixed at 0 even with snapshots on disk, and no ticker.
    let mut c = cfg(dir, Duration::from_millis(20), 1, 50);
    c.snapshots.enabled = false;
    let node = open(c).await;
    assert_eq!(node.local_floor(P0), Some(Offset(0)));
    submit_n(&node, 50).await;
    sleep(Duration::from_millis(300)).await;
    assert_eq!(osnaps(dir).len(), 2);
    assert_eq!(node.handle(P0).expect("ready").log().log_start(), Offset(0));
    node.shutdown().await.expect("shutdown");
}

/// Apply throughput with and without a concurrent export (research Pitfall 6; reported only).
/// `cargo test --release -p ostro-partition --test snapshot_service -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn apply_throughput_during_export() {
    let tmp = ostro_testkit::fs::tempdir();
    let dir = tmp.path();
    let node = open(cfg(dir, NEVER, u64::MAX, 3_600_000)).await;
    // A sizeable state first.
    for _ in 0..200 {
        submit_n(&node, 500).await;
    }
    let rate = |n: u64, d: Duration| n as f64 / d.as_secs_f64();
    let measure = |secs: u64| {
        let node = &node;
        async move {
            let n = AtomicU64::new(0);
            let t = Instant::now();
            while t.elapsed() < Duration::from_secs(secs) {
                submit_n(node, 500).await;
                n.fetch_add(500, Ordering::Relaxed);
            }
            (n.load(Ordering::Relaxed), t.elapsed())
        }
    };
    let (n0, d0) = measure(5).await;
    let mut snaps = Vec::new();
    let (n1, d1) = {
        let fut = measure(5);
        let snap = async {
            let t = Instant::now();
            while t.elapsed() < Duration::from_secs(5) {
                let s0 = Instant::now();
                if let Ok(Some(ev)) = node.snapshot_now(P0).await {
                    snaps.push((ev.info.bytes, ev.info.total_rows, s0.elapsed()));
                }
            }
        };
        let (r, ()) = tokio::join!(fut, snap);
        r
    };
    println!(
        "apply alone: {:.0} cmds/s; during back-to-back exports: {:.0} cmds/s",
        rate(n0, d0),
        rate(n1, d1)
    );
    for (bytes, rows, d) in &snaps {
        println!("snapshot: {bytes} bytes, {rows} rows, {} ms", d.as_millis());
    }
    node.shutdown().await.expect("shutdown");
}
