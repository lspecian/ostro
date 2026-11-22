//! 12-08 Task 1: the one log implementation as a leader. The HW is published only through
//! the replica tracker (RF=1 included), starts at the applied offset (F4), freezes below
//! `min_insync`, and the role gates hold (D5, D-P12-5, D-P12-15, B2).

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ostro_log::{
    FollowerProgress, LogError, LogNode, LogNodeConfig, PartitionLog, ReplicaSet, ReplicatedLog,
    Role,
};
use ostro_types::{Epoch, Offset, PartitionCount};
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);
const ME: u64 = 1;

async fn open(dir: &Path, applied: Offset) -> (LogNode, Arc<PartitionLog>) {
    let cfg = LogNodeConfig::new(dir.join("wal"), PartitionCount::new(1).unwrap(), ME);
    let (node, mut logs) = LogNode::open(cfg, &[applied]).await.unwrap();
    (node, logs.remove(0).unwrap())
}

fn cmds(tag: &str, n: usize) -> Vec<Bytes> {
    (0..n).map(|i| Bytes::from(format!("{tag}-{i}"))).collect()
}

fn rf3(isr: &[u64], version: u64, min_insync: u8) -> ReplicaSet {
    ReplicaSet {
        replicas: vec![ME, 2, 3],
        isr: isr.to_vec(),
        isr_version: version,
        min_insync,
    }
}

fn progress(replica: u64, durable: u64, fetch: u64, leader_end: u64) -> FollowerProgress {
    FollowerProgress {
        replica,
        durable_leo: Offset(durable),
        fetch_offset: Offset(fetch),
        now_ms: 1,
        leader_durable_end: Offset(leader_end),
    }
}

fn hw(log: &PartitionLog) -> Offset {
    *log.high_watermark().borrow()
}

async fn wait_durable(log: &PartitionLog, end: Offset) {
    let mut rx = log.durable_end_watch();
    timeout(T, rx.wait_for(|d| *d >= end))
        .await
        .expect("durable end never reached")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf1_append_commits_with_one_publication_per_hw_advance() {
    let tmp = ostro_testkit::fs::tempdir();
    let (node, log) = open(tmp.path(), Offset::ZERO).await;
    log.become_leader(Epoch(1), ReplicaSet::solo(ME)).unwrap();
    assert_eq!(*log.role().borrow(), Role::Leader { epoch: Epoch(1) });
    // An empty log: nothing to publish.
    assert_eq!(log.hw_publishes(), 0);

    let mut seen = vec![hw(&log)];
    for i in 0..5 {
        let r = log
            .append(Epoch(1), cmds(&format!("b{i}"), 3))
            .await
            .unwrap();
        timeout(T, log.wait_committed(r.last))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hw(&log), r.last.next());
        seen.push(hw(&log));
    }
    // Sequential appends: one group commit each, so one HW advance each, and every advance
    // is exactly one publication through the tracker.
    let advances = seen.windows(2).filter(|w| w[1] > w[0]).count() as u64;
    assert_eq!(advances, 5);
    assert_eq!(log.hw_publishes(), advances);
    let st = log.replica_status();
    assert_eq!(st.hw_publishes, 5);
    assert_eq!((st.hw, st.durable_end), (Offset(15), Offset(15)));
    assert_eq!(st.published_isr, vec![ME]);
    assert_eq!(st.min_insync, 1);
    assert!(st.followers.is_empty());
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hw_at_open_is_the_applied_offset_and_leading_raises_it() {
    let tmp = ostro_testkit::fs::tempdir();
    {
        let (node, log) = open(tmp.path(), Offset::ZERO).await;
        log.become_leader(Epoch(1), ReplicaSet::solo(ME)).unwrap();
        let r = log.append(Epoch(1), cmds("a", 10)).await.unwrap();
        log.wait_committed(r.last).await.unwrap();
        node.shutdown().await;
    }
    let (node, log) = open(tmp.path(), Offset(4)).await;
    // F4: HW = applied, not the recovered LEO; the durable end is the LEO.
    assert_eq!(hw(&log), Offset(4));
    assert_eq!(log.durable_end(), Offset(10));
    assert_eq!(log.log_end(), Offset(10));
    assert_eq!(*log.role().borrow(), Role::Offline);
    assert_eq!(log.hw_publishes(), 0);
    // Unassigned: no appends, and reads stop at the HW.
    assert!(matches!(
        log.append(Epoch(2), cmds("x", 1)).await,
        Err(LogError::NotLeader { leader: None })
    ));
    let got = log
        .read(Offset::ZERO, Offset(100), usize::MAX)
        .await
        .unwrap();
    assert!(got.is_empty(), "the single batch [0, 10) is above HW 4");
    // Leading with ISR {self} raises the HW to the durable LEO at once.
    log.become_leader(Epoch(2), ReplicaSet::solo(ME)).unwrap();
    assert_eq!(hw(&log), Offset(10));
    assert_eq!(log.hw_publishes(), 1);
    node.shutdown().await;

    // Applied above the log end clamps to the LEO; the unreadable sentinel too.
    for applied in [Offset(50), Offset(u64::MAX)] {
        let (node, log) = open(tmp.path(), applied).await;
        assert_eq!(hw(&log), Offset(10), "{applied}");
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn become_leader_needs_an_epoch_above_the_log() {
    let tmp = ostro_testkit::fs::tempdir();
    {
        let (node, log) = open(tmp.path(), Offset::ZERO).await;
        log.become_leader(Epoch(3), ReplicaSet::solo(ME)).unwrap();
        // Same epoch again: refused (not a new term).
        assert!(matches!(
            log.become_leader(Epoch(3), ReplicaSet::solo(ME)),
            Err(LogError::StaleEpoch { current: Epoch(3) })
        ));
        let r = log.append(Epoch(3), cmds("e3", 2)).await.unwrap();
        log.wait_committed(r.last).await.unwrap();
        node.shutdown().await;
    }
    let (node, log) = open(tmp.path(), Offset(2)).await;
    assert_eq!(log.latest_epoch(), Some(Epoch(3)));
    for stale in [1, 2, 3] {
        assert!(matches!(
            log.become_leader(Epoch(stale), ReplicaSet::solo(ME)),
            Err(LogError::StaleEpoch { current: Epoch(3) })
        ));
    }
    assert_eq!(*log.role().borrow(), Role::Offline);
    log.become_leader(Epoch(4), ReplicaSet::solo(ME)).unwrap();
    assert_eq!(*log.role().borrow(), Role::Leader { epoch: Epoch(4) });
    // A leader must be in its own published ISR.
    assert!(matches!(
        log.become_leader(Epoch(5), rf3(&[2, 3], 1, 1)),
        Err(LogError::Failed(_))
    ));
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf3_hw_is_the_minimum_durable_leo_over_the_isr() {
    let tmp = ostro_testkit::fs::tempdir();
    let (node, log) = open(tmp.path(), Offset::ZERO).await;
    log.become_leader(Epoch(1), rf3(&[ME, 2, 3], 1, 2)).unwrap();
    let mut last = Offset::ZERO;
    for i in 0..5 {
        last = log
            .append(Epoch(1), cmds(&format!("r{i}"), 2))
            .await
            .unwrap()
            .last;
    }
    wait_durable(&log, Offset(10)).await;
    // No follower progress: the HW stays where it was.
    assert_eq!(hw(&log), Offset::ZERO);
    assert_eq!(log.hw_publishes(), 0);

    log.record_progress(progress(2, 6, 6, 10)).unwrap();
    assert_eq!(hw(&log), Offset::ZERO, "replica 3 holds nothing yet");
    // A durable LEO counts only up to the fetch offset.
    log.record_progress(progress(3, 9, 4, 10)).unwrap();
    assert_eq!(hw(&log), Offset(4));
    log.record_progress(progress(3, 10, 10, 10)).unwrap();
    assert_eq!(hw(&log), Offset(6));
    assert!(
        timeout(Duration::from_millis(100), log.wait_committed(last))
            .await
            .is_err(),
        "not committed while replica 2 lags"
    );
    log.record_progress(progress(2, 10, 10, 10)).unwrap();
    assert_eq!(hw(&log), Offset(10));
    timeout(T, log.wait_committed(last)).await.unwrap().unwrap();
    assert_eq!(log.hw_publishes(), 3);

    let st = log.replica_status();
    assert_eq!(st.published_isr, vec![ME, 2, 3]);
    assert_eq!((st.isr_version, st.min_insync), (1, 2));
    let rows: Vec<(u64, Offset)> = st
        .followers
        .iter()
        .map(|f| (f.replica, f.durable_leo))
        .collect();
    assert_eq!(rows, vec![(2, Offset(10)), (3, Offset(10))]);
    assert_eq!(log.isr().0, vec![ME, 2, 3]);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn below_min_insync_appends_are_refused_and_the_hw_freezes() {
    let tmp = ostro_testkit::fs::tempdir();
    let (node, log) = open(tmp.path(), Offset::ZERO).await;
    log.become_leader(Epoch(1), rf3(&[ME, 2, 3], 1, 2)).unwrap();
    let r = log.append(Epoch(1), cmds("a", 10)).await.unwrap();
    wait_durable(&log, Offset(10)).await;
    log.record_progress(progress(2, 10, 10, 10)).unwrap();
    log.record_progress(progress(3, 10, 10, 10)).unwrap();
    log.wait_committed(r.last).await.unwrap();
    assert_eq!(hw(&log), Offset(10));

    // A batch appended before the shrink, durable on the leader only.
    let b = log.append(Epoch(1), cmds("b", 5)).await.unwrap();
    wait_durable(&log, Offset(15)).await;
    // Meta shrinks the ISR to {self} while min_insync is 2.
    log.set_published_isr(Epoch(1), rf3(&[ME], 2, 2)).unwrap();
    assert!(matches!(
        log.append(Epoch(1), cmds("c", 1)).await,
        Err(LogError::NotEnoughReplicas { isr: 1, min: 2 })
    ));
    assert_eq!(
        log.log_end(),
        Offset(15),
        "a refused append touches nothing"
    );
    // Frozen even though every replica now reports the batch.
    log.record_progress(progress(2, 15, 15, 15)).unwrap();
    log.record_progress(progress(3, 15, 15, 15)).unwrap();
    assert_eq!(hw(&log), Offset(10));
    assert!(
        timeout(Duration::from_millis(100), log.wait_committed(b.last))
            .await
            .is_err()
    );
    // A stale publication (older version) changes nothing.
    log.set_published_isr(Epoch(1), rf3(&[ME, 2, 3], 1, 2))
        .unwrap();
    assert_eq!(hw(&log), Offset(10));
    // Back to min_insync members: the HW resumes.
    log.set_published_isr(Epoch(1), rf3(&[ME, 2], 3, 2))
        .unwrap();
    assert_eq!(hw(&log), Offset(15));
    timeout(T, log.wait_committed(b.last))
        .await
        .unwrap()
        .unwrap();
    let c = log.append(Epoch(1), cmds("c", 1)).await.unwrap();
    assert_eq!(c.base, Offset(15));
    // Wrong epochs are refused.
    assert!(matches!(
        log.set_published_isr(Epoch(0), rf3(&[ME], 4, 1)),
        Err(LogError::StaleEpoch { current: Epoch(1) })
    ));
    assert!(matches!(
        log.set_published_isr(Epoch(2), rf3(&[ME], 4, 1)),
        Err(LogError::NotLeader { .. })
    ));
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_refuses_leader_operations() {
    let tmp = ostro_testkit::fs::tempdir();
    let (node, log) = open(tmp.path(), Offset::ZERO).await;
    log.become_follower(Epoch(2), 2).unwrap();
    assert_eq!(*log.role().borrow(), Role::Follower { epoch: Epoch(2) });
    assert!(matches!(
        log.append(Epoch(2), cmds("x", 1)).await,
        Err(LogError::NotLeader { leader: Some(2) })
    ));
    assert!(matches!(
        log.record_progress(progress(3, 0, 0, 0)),
        Err(LogError::NotLeader { leader: Some(2) })
    ));
    assert!(matches!(
        log.set_published_isr(Epoch(2), rf3(&[2], 1, 1)),
        Err(LogError::NotLeader { leader: Some(2) })
    ));
    // Following an older epoch, or itself, is refused.
    assert!(matches!(
        log.become_follower(Epoch(1), 3),
        Err(LogError::StaleEpoch { current: Epoch(2) })
    ));
    assert!(log.become_follower(Epoch(3), ME).is_err());
    let st = log.replica_status();
    assert_eq!(st.epoch, Epoch(2));
    assert!(st.published_isr.is_empty() && st.followers.is_empty());
    // A follower elected leader at a newer epoch leads.
    log.become_leader(Epoch(3), ReplicaSet::solo(ME)).unwrap();
    let r = log.append(Epoch(3), cmds("y", 1)).await.unwrap();
    log.wait_committed(r.last).await.unwrap();
    node.shutdown().await;
}
