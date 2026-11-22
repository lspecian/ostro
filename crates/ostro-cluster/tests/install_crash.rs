//! Crash at every step of a follower snapshot install (12-12 Task 2, D-P12-8).
//!
//! For each of `install::{after_log_reset, before_rename, after_rename}`, 20 iterations with
//! a follower holding an old state `A` (a prefix of the leader's history) and randomized
//! frame-level stream cuts: the failpoint panics the install task (a crash: nothing after it
//! runs, everything before it was fsynced), the follower's processes are dropped, and the
//! partition is reopened from disk with the **normal open path** (`Node::open`, Inspect).
//! It must open (never "state ahead of log"):
//!
//! - after `after_log_reset` / `before_rename`: the old state at `A`, log empty and anchored
//!   at `A`;
//! - after `after_rename`: the new state at `S`, log empty and anchored at `S`.
//!
//! A second install attempt then completes and the state digest equals the leader's
//! snapshot digest.
//!
//! The in-process variant (`return` instead of `panic`) checks the rollback: the partition
//! is reopened at once, on the old state with the log re-anchored at `A` (or on the new
//! state at `S` after the rename). Phase 8's own `snapshot::install_after_old_rename` point
//! (state directory renamed aside) is covered the same way.
#![cfg(feature = "failpoints")]

mod support_install;
mod support_meta;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use ostro_cluster::install::{InstallCtx, InstallError, InstallOutcome, install_from_leader};
use ostro_cluster::snapshot::{Exporter, SnapshotServeConfig};
use ostro_log::ReplicatedLog;
use ostro_partition::store::snapshot::OsnpReader;
use ostro_partition::{Node, NodeConfig, PartitionSlot, StartMode, SystemClock};
use ostro_types::{Epoch, Offset};
use support_install::*;
use support_meta::TestCa;
use tempfile::TempDir;

const ITERATIONS: u64 = 20;
const AFTER_LOG_RESET: &str = "install::after_log_reset";
const BEFORE_RENAME: &str = "install::before_rename";
const AFTER_RENAME: &str = "install::after_rename";
const P8_AFTER_OLD_RENAME: &str = "snapshot::install_after_old_rename";

struct Setup {
    ca: TestCa,
    leader: LeaderFx,
    srv: ReplServer,
    frames: u64,
    digest: [u8; 32],
    _a: Assignments,
}

impl Setup {
    async fn new() -> Setup {
        let leader = LeaderFx::new(50, 96).await;
        leader.write_local_snapshot();
        let bytes = leader.export_bytes();
        let mut r = OsnpReader::new(bytes.as_slice()).unwrap();
        let mut frames = 1;
        while r.next_raw().unwrap().is_some() {
            frames += 1;
        }
        let a = assignments(&[LEADER, FOLLOWER]);
        let snapshot = Arc::new(leader.snapshot_ctx(
            a.subscribe(),
            Exporter::None,
            Arc::new(SystemClock),
            SnapshotServeConfig::default(),
        ));
        let ca = test_ca();
        let srv = ReplServer::start(
            &ca,
            TestReplication {
                fetch: None,
                snapshot,
                faults: Default::default(),
            },
        )
        .await;
        let digest = leader.store.digest().unwrap();
        Setup {
            ca,
            leader,
            srv,
            frames,
            digest,
            _a: a,
        }
    }

    fn ctx(&self, f: &FollowerFx) -> InstallCtx {
        InstallCtx::new(
            FOLLOWER,
            f.data_dir().to_path_buf(),
            count(),
            node_transport(&self.ca, FOLLOWER, self.srv.addr),
            Arc::clone(&f.control) as _,
        )
    }

    /// Queue 0-2 random frame-level cuts for the next install.
    fn random_cuts(&self, rng: &mut Lcg) {
        let n = rng.below(3);
        let cuts = (0..n).map(|_| 1 + rng.below(self.frames - 1)).collect();
        self.srv.svc.faults.lock().unwrap().cuts = cuts;
    }

    async fn shutdown(self) {
        self.srv.stop().await;
        self.leader.shutdown().await;
    }
}

/// Reopen the follower's data dir with `Node::open` (Inspect): `(applied, log_start,
/// log_end, digest)`; panics when the partition failed to start.
async fn node_open(dir: &TempDir) -> (Offset, Offset, Offset, [u8; 32]) {
    let mut cfg = NodeConfig::new(dir.path().to_path_buf(), count());
    cfg.node_id = FOLLOWER;
    cfg.mode = StartMode::Inspect;
    let node = Node::open(cfg).await.expect("node open");
    let r = match &node.slots()[0] {
        PartitionSlot::Ready(h) => {
            let applied = h.store().read_meta().unwrap().applied;
            (
                applied,
                h.log().log_start(),
                h.log().log_end(),
                h.store().digest().unwrap(),
            )
        }
        PartitionSlot::Failed { reason, .. } => panic!("partition failed to reopen: {reason}"),
    };
    node.shutdown().await.unwrap();
    r
}

async fn crash_loop(fp: &'static str, seed: u64) {
    let st = Setup::new().await;
    let s = st.leader.s;
    let mut rng = Lcg(seed);
    #[allow(clippy::disallowed_methods)] // wall clock: per-iteration timing for the SUMMARY
    let t0 = std::time::Instant::now();
    for it in 0..ITERATIONS {
        let a = 1 + rng.below(s.0 - 2);
        let l = a + rng.below(s.0 - a);
        let f = FollowerFx::with_old_state(a, l, 96).await;
        let old = f.control.store().digest().unwrap();
        st.random_cuts(&mut rng);
        fail::cfg(fp, "panic").unwrap();
        let e = install_from_leader(&st.ctx(&f), P, LEADER)
            .await
            .unwrap_err();
        fail::remove(fp);
        assert!(matches!(e, InstallError::Aborted(_)), "{fp} #{it}: {e}");
        assert_eq!(
            f.control.reopens.load(Ordering::SeqCst),
            0,
            "crashed: no reopen"
        );
        st.srv.svc.faults.lock().unwrap().cuts.clear();

        // The crash: drop the follower's processes, reopen from disk the normal way.
        let dir = f.close().await;
        let (applied, start, end, digest) = node_open(&dir).await;
        if fp == AFTER_RENAME {
            assert_eq!(
                (applied, start, end),
                (s, s, s),
                "{fp} #{it}: new state at S"
            );
            assert_eq!(digest, st.digest, "{fp} #{it}");
        } else {
            let a = Offset(a);
            assert_eq!(
                (applied, start, end),
                (a, a, a),
                "{fp} #{it}: old state at A"
            );
            assert_eq!(digest, old, "{fp} #{it}: old state intact");
        }

        // The next attempt completes.
        let f = FollowerFx::reopen(dir).await;
        st.random_cuts(&mut rng);
        let out = install_from_leader(&st.ctx(&f), P, LEADER).await.unwrap();
        let InstallOutcome::Installed {
            applied,
            latest_epoch,
            logical_digest,
            ..
        } = out
        else {
            panic!("{out:?}");
        };
        assert_eq!((applied, latest_epoch), (s, Epoch(1)));
        assert_eq!(logical_digest, st.digest);
        assert_eq!(f.control.store().digest().unwrap(), st.digest, "{fp} #{it}");
        assert_eq!((f.log.log_start(), f.log.log_end()), (s, s));
        assert!(f.incoming_files().is_empty());
        st.srv.svc.faults.lock().unwrap().cuts.clear();
        f.close().await;
    }
    println!(
        "{fp}: {ITERATIONS} crash iterations passed in {:.1}s",
        t0.elapsed().as_secs_f64()
    );
    st.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_log_reset() {
    crash_loop(AFTER_LOG_RESET, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_before_rename() {
    crash_loop(BEFORE_RENAME, 2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_rename() {
    crash_loop(AFTER_RENAME, 3).await;
}

/// `return` instead of `panic`: the install fails in-process and rolls back at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_failure_rolls_back_and_reopens() {
    let st = Setup::new().await;
    let s = st.leader.s;
    for fp in [
        AFTER_LOG_RESET,
        BEFORE_RENAME,
        P8_AFTER_OLD_RENAME,
        AFTER_RENAME,
    ] {
        let f = FollowerFx::with_old_state(7, 12, 96).await;
        let old = f.control.store().digest().unwrap();
        let old_epochs = {
            let m = f.control.store().read_meta().unwrap();
            m.cursor.epoch_history.clone()
        };
        fail::cfg(fp, "return").unwrap();
        let e = install_from_leader(&st.ctx(&f), P, LEADER)
            .await
            .unwrap_err();
        fail::remove(fp);
        match fp {
            AFTER_RENAME => assert!(matches!(e, InstallError::Injected(_)), "{fp}: {e}"),
            P8_AFTER_OLD_RENAME => assert!(matches!(e, InstallError::State(_)), "{fp}: {e}"),
            _ => assert!(matches!(e, InstallError::Injected(_)), "{fp}: {e}"),
        }
        assert_eq!(
            f.control.reopens.load(Ordering::SeqCst),
            1,
            "{fp}: reopened"
        );
        let store = f.control.store();
        let applied = store.read_meta().unwrap().applied;
        if fp == AFTER_RENAME {
            assert_eq!(applied, s, "{fp}");
            assert_eq!(store.digest().unwrap(), st.digest, "{fp}");
            assert_eq!((f.log.log_start(), f.log.log_end()), (s, s), "{fp}");
        } else {
            assert_eq!(applied, Offset(7), "{fp}: old state");
            assert_eq!(store.digest().unwrap(), old, "{fp}: old state intact");
            assert_eq!(
                (f.log.log_start(), f.log.log_end()),
                (Offset(7), Offset(7)),
                "{fp}: log re-anchored at A"
            );
            let epochs: Vec<(u32, u64)> =
                f.log.epochs().0.iter().map(|(e, o)| (e.0, o.0)).collect();
            assert_eq!(epochs, old_epochs, "{fp}: epochs from the old cursor");
        }
        drop(store);
        // Consistent on disk too.
        let dir = f.close().await;
        let (applied2, start, end, _) = node_open(&dir).await;
        assert_eq!((start, end), (applied2, applied2), "{fp}");
        assert_eq!(applied2, applied, "{fp}");
        // And the retry completes.
        let f = FollowerFx::reopen(dir).await;
        install_from_leader(&st.ctx(&f), P, LEADER).await.unwrap();
        assert_eq!(f.control.store().digest().unwrap(), st.digest, "{fp}");
        f.close().await;
    }
    st.shutdown().await;
}
