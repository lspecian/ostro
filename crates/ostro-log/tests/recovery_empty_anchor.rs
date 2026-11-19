//! Empty-log anchoring (Phase 12 F3, plan 05-21): a partition log with no records recovers
//! at the floor (`log_start = LEO = floor`), the sequencer starts there, and the epoch
//! history can be seeded from the state's cursor. Non-empty logs are unaffected.

mod common;

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use common::{write_batches, write_log};
use ostro_log::recovery::{recover_partition, segment_dir};
use ostro_log::{EpochHistory, LogError, LogNode, LogNodeConfig, ReplicatedLog, Rf1Log};
use ostro_testkit::corrupt;
use ostro_testkit::fs::tempdir;
use ostro_types::{Epoch, Offset, PartitionCount, PartitionId};

const P: PartitionId = PartitionId(0);

async fn open(dir: &Path, floor: Offset) -> (LogNode, Arc<Rf1Log>) {
    let cfg = LogNodeConfig::new(dir.to_path_buf(), PartitionCount::new(1).unwrap(), 1);
    let (node, mut logs) = LogNode::open(cfg, &[floor]).await.unwrap();
    (node, logs.remove(0).unwrap())
}

fn cmds(n: u64) -> Vec<Bytes> {
    (0..n)
        .map(|i| Bytes::from(format!("anchored-{i}")))
        .collect()
}

#[test]
fn empty_dir_anchors_at_the_floor() {
    let d = tempdir();
    let r = recover_partition(d.path(), P, Offset(500)).unwrap();
    assert_eq!(r.log_start, Offset(500));
    assert_eq!(r.leo, Offset(500));
    assert!(r.epochs.is_empty());
    assert_eq!(r.records, 0);
    assert!(r.torn.is_none());
}

#[test]
fn empty_segment_files_anchor_at_the_floor() {
    let d = tempdir();
    let dir = segment_dir(d.path(), P);
    std::fs::create_dir_all(&dir).unwrap();
    for seg in 0..3 {
        std::fs::File::create(dir.join(format!("wal_0_{seg}.log"))).unwrap();
    }
    let r = recover_partition(d.path(), P, Offset(77)).unwrap();
    assert_eq!((r.log_start, r.leo), (Offset(77), Offset(77)));
    assert_eq!(r.segments.len(), 3);
}

#[test]
fn floor_zero_is_unchanged() {
    let d = tempdir();
    let r = recover_partition(d.path(), P, Offset::ZERO).unwrap();
    assert_eq!((r.log_start, r.leo), (Offset::ZERO, Offset::ZERO));
}

#[test]
fn unreadable_state_sentinel_anchors_at_zero() {
    // Node::open passes Offset(u64::MAX) for a partition whose state could not be read.
    let d = tempdir();
    let r = recover_partition(d.path(), P, Offset(u64::MAX)).unwrap();
    assert_eq!((r.log_start, r.leo), (Offset::ZERO, Offset::ZERO));
    let r = recover_partition(d.path(), P, Offset(i64::MAX as u64 + 1)).unwrap();
    assert_eq!((r.log_start, r.leo), (Offset::ZERO, Offset::ZERO));
}

#[tokio::test(flavor = "multi_thread")]
async fn non_empty_log_ignores_the_floor_for_start_and_end() {
    let d = tempdir();
    let w = write_log(d.path(), P, &[&[(1, 3), (1, 2)], &[(2, 4)]]).await;
    let total: u64 = w.iter().map(|b| b.count).sum();
    for floor in [
        Offset(0),
        Offset(total),
        Offset(total + 1_000),
        Offset(u64::MAX),
    ] {
        let r = recover_partition(d.path(), P, floor).unwrap();
        assert_eq!(r.log_start, Offset(0), "floor {floor}");
        assert_eq!(r.leo, Offset(total), "floor {floor}");
    }
    // a log that itself starts above 0 (trimmed, or anchored earlier) keeps its own start
    let d2 = tempdir();
    write_batches(d2.path(), P, Offset(500), &[(1, 2), (1, 3)]).await;
    for floor in [Offset(0), Offset(500), Offset(9_000)] {
        let r = recover_partition(d2.path(), P, floor).unwrap();
        assert_eq!((r.log_start, r.leo), (Offset(500), Offset(505)), "{floor}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn anchored_log_appends_at_the_floor_and_reads_back() {
    let d = tempdir();
    let (node, log) = open(d.path(), Offset(500)).await;
    assert_eq!(log.log_start(), Offset(500));
    assert_eq!(log.log_end(), Offset(500));
    assert_eq!(*log.high_watermark().borrow(), Offset(500));
    assert_eq!(log.epochs(), EpochHistory::default());
    // reads at the anchor are empty, below it out of range
    assert!(
        log.read(Offset(500), Offset(600), 1 << 20)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        log.read(Offset(499), Offset(600), 1 << 20).await,
        Err(LogError::OutOfRange(_))
    ));

    let a = log.append(Epoch(1), cmds(2)).await.unwrap();
    assert_eq!((a.base, a.last), (Offset(500), Offset(501)));
    let b = log.append(Epoch(1), cmds(3)).await.unwrap();
    assert_eq!((b.base, b.last), (Offset(502), Offset(504)));
    log.wait_committed(b.last).await.unwrap();
    assert_eq!(*log.high_watermark().borrow(), Offset(505));
    let got = log.read(Offset(500), Offset(505), 1 << 20).await.unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].base, Offset(500));
    assert_eq!(got[1].base, Offset(502));
    assert_eq!(got[1].end(), Offset(505));
    assert_eq!(log.epochs().0, vec![(Epoch(1), Offset(500))]);
    node.shutdown().await;
    drop(log);
    drop(node);

    // reopen: a non-empty log that starts at the anchor, whatever the floor
    for floor in [Offset(505), Offset(500)] {
        let r = recover_partition(d.path(), P, floor).unwrap();
        assert_eq!((r.log_start, r.leo), (Offset(500), Offset(505)));
        assert_eq!(r.epochs, vec![(Epoch(1), Offset(500))]);
    }
    let (node, log) = open(d.path(), Offset(505)).await;
    let got = log.read(Offset(500), Offset(505), 1 << 20).await.unwrap();
    assert_eq!(
        got.iter().map(|b| b.base).collect::<Vec<_>>(),
        [Offset(500), Offset(502)]
    );
    let c = log.append(Epoch(2), cmds(1)).await.unwrap();
    assert_eq!(c.base, Offset(505));
    log.wait_committed(c.last).await.unwrap();
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn anchored_log_truncates_back_to_the_anchor() {
    let d = tempdir();
    let (node, log) = open(d.path(), Offset(500)).await;
    let a = log.append(Epoch(1), cmds(4)).await.unwrap();
    log.wait_committed(a.last).await.unwrap();
    assert_eq!(log.truncate_to(Offset(500)).await.unwrap(), Offset(500));
    assert_eq!(log.log_end(), Offset(500));
    assert_eq!(log.log_start(), Offset(500));
    let b = log.append(Epoch(1), cmds(1)).await.unwrap();
    assert_eq!(b.base, Offset(500));
    log.wait_committed(b.last).await.unwrap();
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn seeded_epochs_answer_and_fence_older_epochs() {
    let d = tempdir();
    let (node, log) = open(d.path(), Offset(500)).await;
    let history = [(Epoch(1), Offset(0)), (Epoch(3), Offset(200))];
    log.seed_epochs(&history).await.unwrap();
    assert_eq!(log.epochs().0, history.to_vec());
    assert_eq!(log.epochs().latest(), Some(Epoch(3)));
    // epoch 1 ended where epoch 3 began
    assert_eq!(
        log.epochs().end_offset_for_epoch(Epoch(1)),
        Some(Offset(200))
    );
    // older than the seeded latest: refused, nothing appended
    assert!(matches!(
        log.append(Epoch(2), cmds(1)).await,
        Err(LogError::StaleEpoch { current: Epoch(3) })
    ));
    assert_eq!(log.log_end(), Offset(500));
    let r = log.append(Epoch(4), cmds(1)).await.unwrap();
    assert_eq!(r.base, Offset(500));
    log.wait_committed(r.last).await.unwrap();
    assert_eq!(
        log.epochs().0,
        vec![
            (Epoch(1), Offset(0)),
            (Epoch(3), Offset(200)),
            (Epoch(4), Offset(500))
        ]
    );
    // the log now has records: seeding again is refused
    assert!(matches!(
        log.seed_epochs(&history).await,
        Err(LogError::Failed(_))
    ));
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn seeding_is_refused_on_a_recovered_non_empty_log_and_for_bad_history() {
    let d = tempdir();
    write_batches(d.path(), P, Offset(0), &[(1, 2)]).await;
    let (node, log) = open(d.path(), Offset(2)).await;
    assert!(matches!(
        log.seed_epochs(&[(Epoch(1), Offset(0))]).await,
        Err(LogError::Failed(_))
    ));
    // empty history is a no-op even here
    log.seed_epochs(&[]).await.unwrap();
    assert_eq!(log.epochs().0, vec![(Epoch(1), Offset(0))]);
    node.shutdown().await;

    let d = tempdir();
    let (node, log) = open(d.path(), Offset(10)).await;
    for bad in [
        vec![(Epoch(2), Offset(0)), (Epoch(2), Offset(5))],
        vec![(Epoch(2), Offset(5)), (Epoch(1), Offset(6))],
        vec![(Epoch(1), Offset(5)), (Epoch(2), Offset(4))],
    ] {
        assert!(
            matches!(log.seed_epochs(&bad).await, Err(LogError::Failed(_))),
            "{bad:?}"
        );
    }
    assert!(log.epochs().0.is_empty());
    node.shutdown().await;
}

/// Pins the conservative torn-tail rule for an anchored log (documented on
/// `recover_partition`): with no verified record before it, a torn first record counts as
/// above the floor only when `floor == 0`.
#[tokio::test(flavor = "multi_thread")]
async fn torn_first_record_of_anchored_log() {
    let d = tempdir();
    let w = write_batches(d.path(), P, Offset(500), &[(1, 3)]).await;
    corrupt::truncate_at(&w[0].segment, w[0].byte_pos + w[0].len / 2).unwrap();
    // floor > 0: the first record's offsets are unknown, so it is not provably above the
    // floor; the open fails rather than cutting
    assert!(matches!(
        recover_partition(d.path(), P, Offset(500)),
        Err(LogError::Corrupt { .. })
    ));
    // floor 0 (rebuilding state): cut, and the empty remainder anchors at 0
    let r = recover_partition(d.path(), P, Offset::ZERO).unwrap();
    assert!(r.torn.is_some());
    assert_eq!((r.log_start, r.leo), (Offset::ZERO, Offset::ZERO));
}
