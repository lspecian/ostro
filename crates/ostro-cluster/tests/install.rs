//! Follower snapshot install (12-12 Task 2): `install_from_leader` over loopback mTLS into a
//! follower driven by a fake `PartitionControl` over a real `PartitionLog` + state store.
//! An empty follower bootstraps through the real fetcher and then replicates from `S`; a
//! follower with an old log behind `log_start` installs (old segments gone); a corrupt or
//! foreign snapshot changes nothing; a broken stream resumes; derived keyspaces match.

mod support_install;
mod support_meta;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ostro_cluster::fetcher::{FetcherConfig, FetcherDriver};
use ostro_cluster::install::{InstallCtx, InstallError, InstallOutcome, install_from_leader};
use ostro_cluster::snapshot::{Exporter, SnapshotServeConfig};
use ostro_log::ReplicatedLog;
use ostro_log::recovery::{list_segments, segment_dir};
use ostro_partition::SystemClock;
use ostro_partition::leader::snapshot::{snapshot_dir, snapshot_file_name};
use ostro_partition::roles::FollowerDriver;
use ostro_types::{Epoch, Offset, PartitionCount};
use support_install::*;
use support_meta::TestCa;

struct Setup {
    ca: TestCa,
    leader: LeaderFx,
    srv: ReplServer,
    _a: Assignments,
}

impl Setup {
    /// Leader with `runs` runs, a local snapshot at `S`, `extra` log records after `S`.
    async fn new(runs: u64, payload: usize, extra: u64) -> Setup {
        let leader = LeaderFx::new(runs, payload).await;
        leader.write_local_snapshot();
        if extra > 0 {
            leader.append(extra).await;
        }
        let a = assignments(&[LEADER, FOLLOWER]);
        let snapshot = Arc::new(leader.snapshot_ctx(
            a.subscribe(),
            Exporter::Store(Arc::clone(&leader.store)),
            Arc::new(SystemClock),
            SnapshotServeConfig::default(),
        ));
        let fetch = Some(Arc::new(leader.fetch_ctx(a.subscribe())));
        let ca = test_ca();
        let srv = ReplServer::start(
            &ca,
            TestReplication {
                fetch,
                snapshot,
                faults: Default::default(),
            },
        )
        .await;
        Setup {
            ca,
            leader,
            srv,
            _a: a,
        }
    }

    fn ctx(&self, f: &FollowerFx) -> InstallCtx {
        self.ctx_with_count(f, count())
    }

    fn ctx_with_count(&self, f: &FollowerFx, n: PartitionCount) -> InstallCtx {
        InstallCtx::new(
            FOLLOWER,
            f.data_dir().to_path_buf(),
            n,
            node_transport(&self.ca, FOLLOWER, self.srv.addr),
            Arc::clone(&f.control) as _,
        )
    }

    async fn shutdown(self) {
        self.srv.stop().await;
        self.leader.shutdown().await;
    }
}

fn digest(f: &FollowerFx) -> [u8; 32] {
    f.control.store().digest().unwrap()
}

/// The follower's derived keyspace equals the leader's at the same applied offset (W13).
fn assert_derived_equal(leader: &LeaderFx, f: &FollowerFx) {
    let fs = f.control.store();
    assert_eq!(
        fs.read_meta().unwrap().applied,
        leader.store.read_meta().unwrap().applied
    );
    leader.store.start_run_index_rebuild();
    let (ld, ln) = run_index_digest(&leader.store);
    let (fd, fnr) = run_index_digest(&fs);
    assert!(ln > 0, "the leader has x_run_index rows");
    assert_eq!(
        (fd, fnr),
        (ld, ln),
        "x_run_index digest equal after install"
    );
}

#[tokio::test]
async fn empty_follower_bootstraps_through_the_fetcher_then_appends_from_s() {
    let st = Setup::new(60, 128, 25).await;
    let s = st.leader.s;
    let f = FollowerFx::empty().await;
    assert_eq!(f.log.log_start(), Offset(0));
    let driver = FetcherDriver::new(
        FOLLOWER,
        node_transport(&st.ca, FOLLOWER, st.srv.addr),
        Arc::new(ostro_blob::BlobStore::open(&f.data_dir().join("blobs")).unwrap()),
        Arc::new(NoSuspects),
        Arc::new(st.ctx(&f)),
        FetcherConfig {
            max_wait: Duration::from_millis(100),
            ..FetcherConfig::default()
        },
    );
    let stats = driver.stats();
    let task = driver.start(P, Epoch(EPOCH), LEADER, Arc::clone(&f.log));
    let (l, fl) = (Arc::clone(&st.leader.log), Arc::clone(&f.log));
    wait_until(
        "follower installed and caught up",
        Duration::from_secs(30),
        || fl.log_end() == l.log_end() && fl.durable_end() == l.log_end(),
    )
    .await;
    task.stop().await;
    assert_eq!(stats.installs.load(Ordering::Relaxed), 1, "one install");
    assert_eq!(f.log.log_start(), s, "log anchored at S");
    assert_eq!(f.log.epochs().0, l.epochs().0, "epoch history equal");
    assert_eq!(
        envelopes(&f.log, s).await,
        envelopes(&l, s).await,
        "byte-identical log from S"
    );
    assert_eq!(digest(&f), st.leader.store.digest().unwrap());
    assert_derived_equal(&st.leader, &f);
    assert_eq!(f.control.quiesces.load(Ordering::SeqCst), 1);
    assert_eq!(f.control.reopens.load(Ordering::SeqCst), 1);
    assert!(f.incoming_files().is_empty());
    let kept = snapshot_dir(f.data_dir(), P).join(snapshot_file_name(s));
    assert!(kept.exists(), "the installed snapshot is a local snapshot");
    assert!(kept.with_extension("blobrefs").exists());
    // The kept snapshot opens exactly the way Node::open reopens it.
    let dir = f.close().await;
    let f = FollowerFx::reopen(dir).await;
    assert_eq!(f.log.log_start(), s);
    assert_eq!(f.log.log_end(), st.leader.log.log_end());
    f.close().await;
    st.shutdown().await;
}

struct NoSuspects;
impl ostro_cluster::liveness::SuspectSink for NoSuspects {
    fn report(&self, _reporter: u64, _suspect: u64, _partition: Option<u16>) {}
}

#[tokio::test]
async fn follower_with_an_old_log_behind_log_start_installs() {
    let st = Setup::new(80, 64, 0).await;
    let s = st.leader.s;
    // State at A = 10, log [0, 30) at epoch 1, both prefixes of the leader's history.
    let f = FollowerFx::with_old_state(10, 30, 64).await;
    assert_eq!(f.log.log_end(), Offset(30));
    let wal_dir = f.data_dir().join("wal");
    let before = list_segments(&segment_dir(&wal_dir, P), P).unwrap();
    assert!(!before.is_empty());
    let out = install_from_leader(&st.ctx(&f), P, LEADER).await.unwrap();
    let InstallOutcome::Installed {
        applied,
        latest_epoch,
        logical_digest,
        ..
    } = out
    else {
        panic!("unexpected outcome {out:?}");
    };
    assert_eq!(applied, s);
    assert_eq!(latest_epoch, Epoch(1));
    assert_eq!(logical_digest, st.leader.store.digest().unwrap());
    assert_eq!(
        (f.log.log_start(), f.log.log_end()),
        (s, s),
        "anchored at S"
    );
    assert_eq!(
        f.log.latest_epoch(),
        Some(Epoch(1)),
        "seeded from the cursor"
    );
    assert!(envelopes(&f.log, s).await.is_empty(), "old records gone");
    let after = list_segments(&segment_dir(&wal_dir, P), P).unwrap();
    // Only the WAL's open segment may survive (it holds no records, see above).
    assert!(
        after.len() <= 1,
        "at most the open segment survives: {after:?}"
    );
    for (id, path) in &before {
        if !after.iter().any(|(a, _)| a == id) {
            assert!(!path.exists(), "old segment {} deleted", path.display());
        }
    }
    assert_eq!(digest(&f), st.leader.store.digest().unwrap());
    assert_derived_equal(&st.leader, &f);
    // Reopen from disk: anchored at S with the epochs from the cursor.
    let dir = f.close().await;
    let f = FollowerFx::reopen(dir).await;
    assert_eq!((f.log.log_start(), f.log.log_end()), (s, s));
    assert_eq!(f.log.latest_epoch(), Some(Epoch(1)));
    f.close().await;
    st.shutdown().await;
}

async fn assert_unchanged(f: &FollowerFx, d: [u8; 32], start: Offset, end: Offset) {
    assert_eq!(digest(f), d, "state unchanged");
    assert_eq!(
        (f.log.log_start(), f.log.log_end()),
        (start, end),
        "log unchanged"
    );
    assert_eq!(
        f.control.quiesces.load(Ordering::SeqCst),
        0,
        "never quiesced"
    );
    assert!(f.incoming_files().is_empty(), "staged file removed");
}

#[tokio::test]
async fn a_digest_mismatch_changes_nothing() {
    let st = Setup::new(30, 64, 0).await;
    let f = FollowerFx::with_old_state(5, 8, 64).await;
    let (d, start, end) = (digest(&f), f.log.log_start(), f.log.log_end());
    // A rows frame with a valid CRC but a flipped byte: only the trailer digest sees it.
    let rows_frame = {
        let bytes = st.leader.export_bytes();
        let mut r = ostro_partition::store::snapshot::OsnpReader::new(bytes.as_slice()).unwrap();
        let mut i = 0u64;
        loop {
            i += 1;
            let raw = r.next_raw().unwrap().unwrap();
            if raw.kind == ostro_partition::store::snapshot::kind::ROWS {
                break i;
            }
        }
    };
    st.srv.svc.faults.lock().unwrap().mangle = Some((rows_frame, mangle_payload_keep_crc));
    let e = install_from_leader(&st.ctx(&f), P, LEADER)
        .await
        .unwrap_err();
    assert!(matches!(e, InstallError::Corrupt(_)), "{e}");
    assert_unchanged(&f, d, start, end).await;
    // A frame CRC mismatch is caught as it arrives.
    st.srv.svc.faults.lock().unwrap().mangle = Some((rows_frame, mangle_payload_break_crc));
    let e = install_from_leader(&st.ctx(&f), P, LEADER)
        .await
        .unwrap_err();
    assert!(
        matches!(&e, InstallError::Corrupt(m) if m.contains("CRC")),
        "{e}"
    );
    assert_unchanged(&f, d, start, end).await;
    // Without the fault it installs.
    st.srv.svc.faults.lock().unwrap().mangle = None;
    install_from_leader(&st.ctx(&f), P, LEADER).await.unwrap();
    assert_eq!(digest(&f), st.leader.store.digest().unwrap());
    f.close().await;
    st.shutdown().await;
}

#[tokio::test]
async fn a_partition_mismatch_is_refused() {
    let st = Setup::new(10, 16, 0).await;
    let f = FollowerFx::with_old_state(3, 3, 16).await;
    let (d, start, end) = (digest(&f), f.log.log_start(), f.log.log_end());
    // This follower believes the cluster has 2 partitions: the snapshot (of 1) is foreign.
    let ctx = st.ctx_with_count(&f, PartitionCount::new(2).unwrap());
    let e = install_from_leader(&ctx, P, LEADER).await.unwrap_err();
    assert!(matches!(e, InstallError::Mismatch(_)), "{e}");
    assert_unchanged(&f, d, start, end).await;
    f.close().await;
    st.shutdown().await;
}

#[tokio::test]
async fn a_broken_stream_resumes_from_the_last_complete_frame() {
    let st = Setup::new(40, 32, 0).await;
    let f = FollowerFx::empty().await;
    st.srv.svc.faults.lock().unwrap().cuts = [3, 5, 1].into_iter().collect();
    let out = install_from_leader(&st.ctx(&f), P, LEADER).await.unwrap();
    assert!(matches!(out, InstallOutcome::Installed { applied, .. } if applied == st.leader.s));
    assert_eq!(
        st.srv.svc.snapshot.stats().resumes.load(Ordering::Relaxed),
        3,
        "three resumes"
    );
    assert_eq!(digest(&f), st.leader.store.digest().unwrap());
    // Resumes beyond the limit fail the attempt without touching anything.
    let g = FollowerFx::with_old_state(2, 2, 32).await;
    let (d, start, end) = (digest(&g), g.log.log_start(), g.log.log_end());
    st.srv.svc.faults.lock().unwrap().cuts = std::iter::repeat_n(1, 10).collect();
    let e = install_from_leader(&st.ctx(&g), P, LEADER)
        .await
        .unwrap_err();
    assert!(matches!(e, InstallError::Stream { .. }), "{e}");
    assert_unchanged(&g, d, start, end).await;
    st.srv.svc.faults.lock().unwrap().cuts.clear();
    f.close().await;
    g.close().await;
    st.shutdown().await;
}

#[tokio::test]
async fn stale_incoming_files_are_removed_by_the_next_attempt() {
    let st = Setup::new(10, 16, 0).await;
    let f = FollowerFx::empty().await;
    let dir = snapshot_dir(f.data_dir(), P);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(".incoming-00000000000000000003"), b"half").unwrap();
    install_from_leader(&st.ctx(&f), P, LEADER).await.unwrap();
    assert!(f.incoming_files().is_empty());
    f.close().await;
    st.shutdown().await;
}
