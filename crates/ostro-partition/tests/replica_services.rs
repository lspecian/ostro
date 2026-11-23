//! Replica services and the install / roles contracts (12-13): followers take snapshots and
//! trim by their snapshot floor only (the upload floor binds leaders), leaders keep both
//! floors, a quiesced follower accepts an externally installed state and resumes applying
//! after the reopen (with `x_run_index` rebuilt, W13), a quiesce never closes a store others
//! still use, and `PartitionRoles::led` follows promotion and demotion.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p12_roles.rs"]
mod p12_roles;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ostro_log::{EpochHistory, ReplicatedLog};
use ostro_partition::leader::retention::{RetentionConfig, RetentionStats};
use ostro_partition::store::derived::IndexState;
use ostro_partition::store::snapshot::install_snapshot;
use ostro_partition::{
    ApplyOutcome, Node, NodeConfig, PartitionRole, PartitionSlot, PartitionStore,
};
use ostro_types::{Command, Epoch, Offset, PartitionCount, PartitionId};
use p12_roles::{
    FakeAssignments, FakeFollower, FetcherEvent, ROLE_T, eventually, open, roles_cfg, solo, view,
    wait_leader, wait_role,
};
use sha2::{Digest as _, Sha256};
use tokio::time::timeout;

const P0: PartitionId = PartitionId(0);
const NEVER: Duration = Duration::from_secs(3600);

/// Small WAL segments, manual snapshots/retention unless `cadence`.
fn tune(cfg: &mut NodeConfig, cadence: bool) {
    cfg.wal.enable_rotation = true;
    cfg.wal.rotation_size_bytes = 2 * 1024;
    cfg.snapshots.enabled = true;
    cfg.snapshots.interval = if cadence {
        Duration::from_millis(100)
    } else {
        NEVER
    };
    cfg.snapshots.min_interval = Duration::ZERO;
    cfg.snapshots.bytes_threshold = 1;
    cfg.snapshots.keep = 2;
    cfg.leader.retention = RetentionConfig {
        interval: if cadence {
            Duration::from_millis(50)
        } else {
            NEVER
        },
        ..RetentionConfig::default()
    };
}

async fn submit_n(node: &Node, n: usize) {
    let h = node.handle(P0).expect("ready");
    for chunk in 0..n.div_ceil(10) {
        let k = 10.min(n - chunk * 10);
        let cmds: Vec<Command> = (0..k)
            .map(|_| Command::start_run(common::start_run(&common::run_on(0))))
            .collect();
        let out = timeout(common::T, h.submit(cmds))
            .await
            .expect("submit timed out")
            .expect("submit");
        assert!(out.iter().all(|o| *o == ApplyOutcome::Accepted));
    }
}

fn log_start(node: &Node) -> Offset {
    node.handle(P0).expect("ready").log().log_start()
}

/// Retention / snapshot state of P0 for a timeout message: the service's counters, the
/// floors, the retained snapshots, the log bounds, the WAL segment files and one manual pass.
async fn retention_diag(node: &Node, stats: &RetentionStats, dir: &Path) -> String {
    use std::sync::atomic::Ordering::Relaxed;
    let snaps: Vec<u64> = node
        .local_snapshots(P0)
        .await
        .iter()
        .map(|s| s.applied_offset.0)
        .collect();
    let (start, end, hw) = {
        let h = node.handle(P0).expect("ready");
        let log = h.log();
        (
            log.log_start(),
            log.log_end(),
            *log.high_watermark().borrow(),
        )
    };
    let mut segs = Vec::new();
    let mut stack = vec![dir.join("wal")];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let path = e.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                let len = e.metadata().map_or(0, |m| m.len());
                segs.push(format!("{}={len}", e.file_name().to_string_lossy()));
            }
        }
    }
    segs.sort();
    let manual = node.retention_pass(P0).await;
    format!(
        "retention passes {} not_ready {} last_checkpoint {} last_floor {} last_log_start {}; \
         local_floor {:?} upload_floor {:?}; snapshots {snaps:?}; applied {:?}; \
         log [{}, {}) hw {}; wal files {segs:?}; manual pass {manual:?}",
        stats.passes.load(Relaxed),
        stats.not_ready.load(Relaxed),
        stats.last_checkpoint.load(Relaxed),
        stats.last_floor.load(Relaxed),
        stats.last_log_start.load(Relaxed),
        node.local_floor(P0),
        node.floor_handles(P0).map(|f| *f.upload.borrow()),
        node.applied_state(P0).map(|a| a.0),
        start,
        end,
        hw,
    )
}

/// sha256 over every `x_run_index` row (marker excluded) once the index is ready.
fn run_index_digest(store: &PartitionStore) -> ([u8; 32], usize) {
    store.join_run_index_rebuild();
    assert_eq!(
        store.run_index_state(),
        IndexState::Ready,
        "x_run_index ready"
    );
    let rows = store
        .reader()
        .run_index_rows(&[], None, usize::MAX)
        .expect("index rows");
    let mut h = Sha256::new();
    let mut n = 0;
    for (k, v) in rows.iter().filter(|(k, _)| k[0] != 0x00) {
        h.update((k.len() as u32).to_le_bytes());
        h.update(k);
        h.update((v.len() as u32).to_le_bytes());
        h.update(v);
        n += 1;
    }
    (h.finalize().into(), n)
}

/// Session 1: this node (1) leads at epoch 1 and writes `n` commands.
async fn seed_as_leader(dir: &Path, n: usize, backup: bool) {
    let asg = FakeAssignments::new(1, vec![solo(0, 1, 1)]);
    let fol = FakeFollower::new();
    let mut cfg = roles_cfg(dir, 1, 1, &asg, &fol);
    tune(&mut cfg, false);
    cfg.backup_enabled = backup;
    let node = open(cfg).await;
    wait_leader(&node, 0, 1).await;
    submit_n(&node, n).await;
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_snapshots_and_trims_by_its_snapshot_floor_only() {
    let tmp = ostro_testkit::fs::tempdir();
    seed_as_leader(tmp.path(), 200, true).await;

    // Session 2: a follower of node 2, backup on, snapshot + retention cadence on.
    let asg = FakeAssignments::new(1, vec![view(0, 2, 1, &[1, 2], &[2], 2, 1)]);
    let fol = FakeFollower::new();
    let mut cfg = roles_cfg(tmp.path(), 1, 1, &asg, &fol);
    tune(&mut cfg, true);
    cfg.backup_enabled = true;
    let node = open(cfg).await;
    wait_role(&node, 0, "Follower{1, 2}", |r| {
        matches!(r, PartitionRole::Follower { .. })
    })
    .await;
    let upload = node.floor_handles(P0).expect("floor handles").upload;
    assert_eq!(
        *upload.borrow(),
        Some(Offset(0)),
        "B4 upload floor at log_start"
    );
    // The follower's own services: a snapshot on cadence, then trimming below it, although
    // the uploader never published a floor (it runs on leaders only).
    eventually("follower snapshot", || {
        node.local_floor(P0).is_some_and(|f| f > Offset(0))
    })
    .await;
    eventually("follower trimmed", || log_start(&node) > Offset(0)).await;
    let floor = node.local_floor(P0).unwrap();
    assert!(log_start(&node) <= floor, "never past the oldest snapshot");
    assert_eq!(*upload.borrow(), Some(Offset(0)), "upload floor untouched");
    node.shutdown().await.expect("shutdown");

    // Session 3: the same data dir leads again (epoch 2), backup on: the upload floor binds.
    let asg = FakeAssignments::new(1, vec![solo(0, 1, 2)]);
    let mut cfg = roles_cfg(tmp.path(), 1, 1, &asg, &fol);
    tune(&mut cfg, false);
    cfg.backup_enabled = true;
    let node = open(cfg).await;
    wait_leader(&node, 0, 2).await;
    submit_n(&node, 100).await;
    let start = log_start(&node);
    let taken = node
        .snapshot_now(P0)
        .await
        .expect("snapshot")
        .expect("taken");
    assert!(taken.applied_offset > start);
    let after = node.retention_pass(P0).await.expect("pass");
    assert_eq!(
        after, start,
        "a leader keeps the upload floor (Some(log_start))"
    );
    // Once the uploader raises its floor, the leader trims up to it.
    node.floor_handles(P0)
        .unwrap()
        .upload
        .send_replace(Some(taken.applied_offset));
    // The forwarder publishes asynchronously; retry passes until one sees it.
    let mut trimmed = start;
    for _ in 0..200 {
        trimmed = node.retention_pass(P0).await.expect("pass");
        if trimmed > start {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(trimmed > start && trimmed <= taken.applied_offset);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_snapshots_and_trims_in_replica_services() {
    let tmp = ostro_testkit::fs::tempdir();
    let asg = FakeAssignments::new(1, vec![solo(0, 1, 1)]);
    let fol = FakeFollower::new();
    let mut cfg = roles_cfg(tmp.path(), 1, 1, &asg, &fol);
    tune(&mut cfg, true);
    let stats = Arc::new(RetentionStats::default());
    cfg.leader.retention.stats = Some(Arc::clone(&stats));
    let node = open(cfg).await;
    wait_leader(&node, 0, 1).await;
    submit_n(&node, 200).await;
    eventually("leader snapshot", || {
        node.local_floor(P0).is_some_and(|f| f > Offset(0))
    })
    .await;
    // The floor is the older of the newest `keep` = 2 snapshots. When the cadence happened to
    // snapshot once inside the first WAL segment and once at the end of the writes, that floor
    // stays inside the first segment and nothing can be trimmed until two snapshots lie above
    // it, which needs more writes (CI linux-aarch64-musl timed out here). Keep writing.
    let deadline = std::time::Instant::now() + ROLE_T;
    while log_start(&node) == Offset(0) {
        if std::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for leader trimmed: {}",
                retention_diag(&node, &stats, tmp.path()).await
            );
        }
        submit_n(&node, 10).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(log_start(&node) <= node.local_floor(P0).unwrap());
    // Demotion keeps the replica services (and their snapshots) running.
    asg.set(view(0, 2, 2, &[1, 2], &[2], 2, 1));
    wait_role(&node, 0, "Follower{2, 2}", |r| {
        matches!(r, PartitionRole::Follower { .. })
    })
    .await;
    assert!(node.snapshot_now(P0).await.is_ok());
    assert!(node.retention_pass(P0).await.is_ok());
    node.shutdown().await.expect("shutdown");
}

/// Why `leader_snapshots_and_trims_in_replica_services` keeps writing (CI linux-aarch64-musl
/// timed out there): with `keep` = 2 the floor is the older of the two newest snapshots, so an
/// early snapshot inside the first WAL segment followed directly by one at the end of the
/// writes pins the floor inside that segment; retention runs healthily and trims nothing
/// until the next snapshot lifts the floor. Cadence off: snapshots are taken by hand.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn early_snapshot_in_the_first_segment_pins_the_floor_until_the_next_snapshot() {
    let tmp = ostro_testkit::fs::tempdir();
    let asg = FakeAssignments::new(1, vec![solo(0, 1, 1)]);
    let fol = FakeFollower::new();
    let mut cfg = roles_cfg(tmp.path(), 1, 1, &asg, &fol);
    tune(&mut cfg, false);
    let stats = Arc::new(RetentionStats::default());
    cfg.leader.retention.stats = Some(Arc::clone(&stats));
    let node = open(cfg).await;
    wait_leader(&node, 0, 1).await;
    submit_n(&node, 10).await;
    let early = node
        .snapshot_now(P0)
        .await
        .expect("snapshot")
        .expect("taken");
    submit_n(&node, 190).await;
    let late = node
        .snapshot_now(P0)
        .await
        .expect("snapshot")
        .expect("taken");
    assert!(early.applied_offset < late.applied_offset);
    assert_eq!(node.local_floor(P0), Some(early.applied_offset));
    // ~90 bytes per command and 2 KiB segments: the early snapshot is inside segment 0.
    let pinned = node.retention_pass(P0).await.expect("pass");
    assert_eq!(
        pinned,
        Offset(0),
        "floor {} is inside the first segment: {}",
        early.applied_offset.0,
        retention_diag(&node, &stats, tmp.path()).await
    );
    submit_n(&node, 10).await;
    node.snapshot_now(P0)
        .await
        .expect("snapshot")
        .expect("taken");
    assert_eq!(node.local_floor(P0), Some(late.applied_offset));
    let trimmed = node.retention_pass(P0).await.expect("pass");
    assert!(
        trimmed > Offset(0) && trimmed <= late.applied_offset,
        "trimmed to {trimmed:?}: {}",
        retention_diag(&node, &stats, tmp.path()).await
    );
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quiesce_and_reopen_installs_an_external_state() {
    let tmp_l = ostro_testkit::fs::tempdir();
    let tmp_f = ostro_testkit::fs::tempdir();
    let count = PartitionCount::new(1).unwrap();

    // The "leader" (node 2): 30 commands, a snapshot, 20 more.
    let asg_l = FakeAssignments::new(2, vec![view(0, 2, 1, &[1, 2], &[2], 1, 1)]);
    let fol_l = FakeFollower::new();
    let mut cfg = roles_cfg(tmp_l.path(), 2, 1, &asg_l, &fol_l);
    tune(&mut cfg, false);
    let leader = open(cfg).await;
    wait_leader(&leader, 0, 1).await;
    submit_n(&leader, 30).await;
    let snap = leader
        .snapshot_now(P0)
        .await
        .expect("snapshot")
        .expect("taken");
    let s = snap.applied_offset;
    submit_n(&leader, 20).await;
    // A leader refuses an install.
    assert!(leader.control().quiesce_for_install(P0).await.is_err());

    // The follower (node 1): empty, following node 2 at epoch 1.
    let asg_f = FakeAssignments::new(1, vec![view(0, 2, 1, &[1, 2], &[2], 1, 1)]);
    let fol_f = FakeFollower::new();
    let mut cfg = roles_cfg(tmp_f.path(), 1, 1, &asg_f, &fol_f);
    tune(&mut cfg, false);
    cfg.roles.quiesce_timeout = Duration::from_millis(300);
    let follower = open(cfg).await;
    wait_role(&follower, 0, "Follower{1, 2}", |r| {
        matches!(r, PartitionRole::Follower { .. })
    })
    .await;
    assert_eq!(follower.applied_state(P0).unwrap().0, Offset(0));

    // A cached handle keeps the store open: the quiesce is refused and the partition keeps
    // running.
    let cached = follower.handle(P0).expect("ready");
    let r = follower.control().quiesce_for_install(P0).await;
    assert!(
        r.as_ref()
            .is_err_and(|e| e.to_string().contains("still in use")),
        "{r:?}"
    );
    drop(cached);
    assert!(
        follower.handle(P0).is_some(),
        "restarted after the refused quiesce"
    );

    // Quiesce, install the leader's snapshot (12-12's order: reset the log, install), reopen.
    let guard = follower
        .control()
        .quiesce_for_install(P0)
        .await
        .expect("quiesce");
    assert!(follower.handle(P0).is_none(), "quiesced: no handle");
    assert!(matches!(follower.slots()[0], PartitionSlot::Failed { .. }));
    guard
        .log
        .reset_to(s, EpochHistory(vec![(Epoch(1), Offset(0))]))
        .await
        .expect("reset_to");
    let (root, cfg_store) = (guard.state_root.clone(), guard.store_cfg.clone());
    let path = snap.path.clone();
    tokio::task::spawn_blocking(move || {
        let f = std::fs::File::open(&path).expect("snapshot file");
        install_snapshot(&root, P0, count, &cfg_store, std::io::BufReader::new(f))
    })
    .await
    .expect("install task")
    .expect("install");
    timeout(ROLE_T, guard.reopen())
        .await
        .expect("reopen timed out")
        .expect("reopen");

    let h = follower.handle(P0).expect("reopened");
    assert_eq!(follower.applied_state(P0).unwrap().0, s, "state at S");
    assert_eq!(h.log().log_start(), s);
    assert_eq!(
        *follower.role(P0).unwrap().borrow(),
        PartitionRole::Follower {
            epoch: Epoch(1),
            leader: 2
        }
    );
    assert_eq!(
        fol_f.events(),
        vec![FetcherEvent::Start {
            p: 0,
            epoch: 1,
            leader: 2
        }],
        "the fetcher survives the install"
    );

    // The follower resumes applying: the leader's tail [S, HW) is replicated by hand.
    let lh = leader.handle(P0).expect("leader");
    let batches = lh.log().read_replica(s, usize::MAX).await.expect("read");
    h.log()
        .append_replica(batches)
        .await
        .expect("append_replica");
    let leader_hw = *lh.log().high_watermark().borrow();
    eventually("follower durable", || h.log().durable_end() >= leader_hw).await;
    h.log().set_follower_hw(leader_hw);
    timeout(ROLE_T, h.machine().wait_applied(Offset(leader_hw.0 - 1)))
        .await
        .expect("apply timed out")
        .expect("applied");
    let (la, lhash) = leader.applied_state(P0).unwrap();
    let (fa, fhash) = follower.applied_state(P0).unwrap();
    assert_eq!(
        (fa, fhash),
        (la, lhash),
        "follower state equals the leader's"
    );
    // W13: derived x_run_index rebuilt on reopen equals the leader's incrementally kept one.
    let (fd, fn_) = run_index_digest(h.store());
    let (ld, ln) = run_index_digest(lh.store());
    assert_eq!(fn_, ln);
    assert!(fn_ >= 50, "every run indexed ({fn_})");
    assert_eq!(fd, ld, "x_run_index digest equals the leader's");
    drop((h, lh));
    follower.shutdown().await.expect("shutdown");
    leader.shutdown().await.expect("shutdown");

    // Reopen from disk at S + tail.
    let asg_f = FakeAssignments::new(1, vec![view(0, 2, 1, &[1, 2], &[2], 1, 1)]);
    let node = open(roles_cfg(tmp_f.path(), 1, 1, &asg_f, &fol_f)).await;
    assert_eq!(node.applied_state(P0).unwrap(), (la, lhash));
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn led_follows_promotion_and_demotion() {
    let tmp = ostro_testkit::fs::tempdir();
    let asg = FakeAssignments::new(1, vec![view(0, 2, 1, &[1, 2], &[2], 1, 1)]);
    let fol = FakeFollower::new();
    let node = open(roles_cfg(tmp.path(), 1, 1, &asg, &fol)).await;
    let roles = node.roles();
    let mut led = roles.led();
    wait_role(&node, 0, "Follower", |r| {
        matches!(r, PartitionRole::Follower { .. })
    })
    .await;
    assert!(led.borrow_and_update().is_empty());

    asg.set(view(0, 1, 2, &[1, 2], &[1], 2, 1));
    timeout(ROLE_T, led.wait_for(|v| **v == vec![(P0, Epoch(2))]))
        .await
        .expect("led after promotion")
        .expect("watch");
    // Node implements PartitionRoles itself too.
    assert_eq!(
        *ostro_partition::PartitionRoles::led(&node).borrow(),
        Arc::new(vec![(P0, Epoch(2))])
    );
    asg.set(view(0, 2, 3, &[1, 2], &[2], 3, 1));
    timeout(ROLE_T, led.wait_for(|v| v.is_empty()))
        .await
        .expect("led after demotion")
        .expect("watch");
    node.shutdown().await.expect("shutdown");
}
