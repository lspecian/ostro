//! 08-18: `write_partition_log` imports raw envelope batches into a partition WAL that
//! `LogNode::open` reads like any other log, stopping exactly at the cut.

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use ostro_log::{
    ImportMode, LogError, LogNode, LogNodeConfig, RawBatch, ReplicaSet, ReplicatedLog, Rf1Log,
    write_partition_log,
};
use ostro_types::{Epoch, Offset, PartitionCount, PartitionId, envelope};

const P: PartitionId = PartitionId(0);

async fn open(dir: &Path, floor: Offset) -> (LogNode, Arc<Rf1Log>) {
    let cfg = LogNodeConfig::new(dir.join("wal"), PartitionCount::new(1).unwrap(), 1);
    let (node, mut logs) = LogNode::open(cfg, &[floor]).await.unwrap();
    let log = logs.remove(0).unwrap();
    lead(&log);
    (node, log)
}

/// Lead the way `Node` does at RF=1: epoch above the logged history, ISR {self}.
fn lead(log: &Rf1Log) {
    let e = log.latest_epoch().map_or(1, |e| e.0 + 1);
    log.become_leader(Epoch(e), ReplicaSet::solo(1)).unwrap();
}

fn cmds(base: u64, n: u64) -> Vec<Bytes> {
    (0..n)
        .map(|i| Bytes::from(format!("cmd-{}-{}", base + i, "q".repeat(i as usize * 3))))
        .collect()
}

/// `(epoch, n_cmds)` per envelope from `start`, grouped per epoch run like `read_envelopes`.
fn batches(start: u64, layout: &[(u32, u64)]) -> Vec<RawBatch> {
    let mut out: Vec<RawBatch> = Vec::new();
    let mut at = start;
    for &(e, n) in layout {
        let env = Bytes::from(envelope::encode(P, Epoch(e), Offset(at), &cmds(at, n)));
        match out.last_mut() {
            Some(b) if b.epoch == e => {
                b.envelopes.push(env);
                b.end = Offset(at + n);
            }
            _ => out.push(RawBatch {
                base: Offset(at),
                end: Offset(at + n),
                epoch: e,
                envelopes: vec![env],
            }),
        }
        at += n;
    }
    out
}

fn flat(raw: &[RawBatch]) -> Vec<Bytes> {
    raw.iter().flat_map(|b| b.envelopes.clone()).collect()
}

fn seg(dir: &Path) -> Vec<u8> {
    std::fs::read(dir.join("wal/p/0/wal_0_0.log")).unwrap()
}

const LAYOUT: [(u32, u64); 6] = [(1, 3), (1, 1), (1, 4), (2, 2), (2, 5), (4, 1)];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_trip_against_a_real_log() {
    // Source: a real log written through Rf1Log.
    let src = tempfile::tempdir().unwrap();
    let (node, log) = open(src.path(), Offset::ZERO).await;
    let mut last = Offset::ZERO;
    for (i, &(e, n)) in LAYOUT.iter().enumerate() {
        last = log
            .append(Epoch(e), cmds(i as u64 * 100, n))
            .await
            .unwrap()
            .last;
    }
    log.wait_committed(last).await.unwrap();
    let leo = last.next();
    let raw = log.read_envelopes(Offset::ZERO, usize::MAX).await.unwrap();
    let epochs = log.epochs();
    node.shutdown().await;

    let dst = tempfile::tempdir().unwrap();
    let rep = write_partition_log(
        &dst.path().join("wal"),
        P,
        raw.clone(),
        leo,
        ImportMode::Create,
    )
    .unwrap();
    assert_eq!(rep.records, LAYOUT.len() as u64);
    assert_eq!((rep.log_start, rep.leo), (Some(Offset::ZERO), Some(leo)));

    let (node, log) = open(dst.path(), Offset::ZERO).await;
    assert_eq!(log.log_start(), Offset::ZERO);
    assert_eq!(log.log_end(), leo);
    assert_eq!(
        log.epochs(),
        epochs,
        "epoch history rebuilt from batch epochs"
    );
    let back = log.read_envelopes(Offset::ZERO, usize::MAX).await.unwrap();
    assert_eq!(flat(&back), flat(&raw), "read_envelopes round trip");
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stops_exactly_at_the_cut_and_rejects_a_mid_batch_cut() {
    let b = batches(0, &LAYOUT); // envelopes [0,3) [3,4) [4,8) [8,10) [10,15) [15,16)
    let dir = tempfile::tempdir().unwrap();
    let rep = write_partition_log(
        &dir.path().join("wal"),
        P,
        b.clone(),
        Offset(10),
        ImportMode::Create,
    )
    .unwrap();
    assert_eq!(rep.records, 4);
    let (node, log) = open(dir.path(), Offset::ZERO).await;
    assert_eq!(log.log_end(), Offset(10));
    assert_eq!(
        log.epochs().0,
        vec![(Epoch(1), Offset(0)), (Epoch(2), Offset(8))],
        "epoch change across batches"
    );
    node.shutdown().await;

    let dir = tempfile::tempdir().unwrap();
    let e = write_partition_log(
        &dir.path().join("wal"),
        P,
        b.clone(),
        Offset(9),
        ImportMode::Create,
    )
    .unwrap_err();
    assert!(e.to_string().contains("inside the batch"), "{e}");
    assert!(
        !dir.path().join("wal/p/0/wal_0_0.log").exists(),
        "nothing written"
    );

    // Batches that end before the cut.
    let e = write_partition_log(
        &dir.path().join("wal"),
        P,
        b,
        Offset(99),
        ImportMode::Create,
    )
    .unwrap_err();
    assert!(e.to_string().contains("before the cut"), "{e}");
}

#[tokio::test]
async fn create_then_append_equals_create_to_the_end() {
    let b = batches(0, &LAYOUT);
    let whole = tempfile::tempdir().unwrap();
    write_partition_log(
        &whole.path().join("wal"),
        P,
        b.clone(),
        Offset(16),
        ImportMode::Create,
    )
    .unwrap();

    let two = tempfile::tempdir().unwrap();
    let wal = two.path().join("wal");
    write_partition_log(&wal, P, b.clone(), Offset(8), ImportMode::Create).unwrap();
    // The second step gets the envelopes from 8 on (as restore reads them from chunks).
    let tail: Vec<RawBatch> = batches(0, &LAYOUT)
        .into_iter()
        .filter_map(|mut rb| {
            rb.envelopes
                .retain(|e| envelope::peek_header(e).unwrap().base >= Offset(8));
            let first = envelope::peek_header(rb.envelopes.first()?).unwrap().base;
            rb.base = first;
            Some(rb)
        })
        .collect();
    let rep = write_partition_log(&wal, P, tail, Offset(16), ImportMode::Append).unwrap();
    assert_eq!(
        (rep.log_start, rep.leo),
        (Some(Offset(0)), Some(Offset(16)))
    );
    assert_eq!(seg(two.path()), seg(whole.path()), "byte for byte");

    // Create refuses the existing log.
    let e = write_partition_log(&wal, P, Vec::new(), Offset(0), ImportMode::Create).unwrap_err();
    assert!(e.to_string().contains("already exists"), "{e}");
}

#[tokio::test]
async fn append_with_a_gap_or_without_a_log_fails() {
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal");
    let e = write_partition_log(&wal, P, batches(0, &LAYOUT), Offset(4), ImportMode::Append)
        .unwrap_err();
    assert!(e.to_string().contains("no imported log"), "{e}");

    write_partition_log(&wal, P, batches(0, &LAYOUT), Offset(4), ImportMode::Create).unwrap();
    let before = seg(dir.path());
    // LEO is 4; a batch at 8 leaves a gap.
    let gap = batches(8, &[(2, 2), (2, 5)]);
    let e = write_partition_log(&wal, P, gap, Offset(15), ImportMode::Append).unwrap_err();
    assert!(matches!(&e, LogError::Wal(m) if m.contains("gap")), "{e}");
    assert_eq!(seg(dir.path()), before, "nothing appended");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_partition_empty_log_anchors_at_the_applied_offset() {
    // A partition with no writes since its snapshot at 40: zero batches, empty segment.
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal");
    let rep = write_partition_log(&wal, P, Vec::new(), Offset(40), ImportMode::Create).unwrap();
    assert_eq!((rep.records, rep.log_start, rep.leo), (0, None, None));
    assert_eq!(seg(dir.path()).len(), 0);
    // Appending later batches to the empty imported log starts it at their base.
    let rep = write_partition_log(
        &wal,
        P,
        batches(40, &[(3, 2)]),
        Offset(42),
        ImportMode::Append,
    )
    .unwrap();
    assert_eq!(
        (rep.log_start, rep.leo),
        (Some(Offset(40)), Some(Offset(42)))
    );

    let idle = tempfile::tempdir().unwrap();
    write_partition_log(
        &idle.path().join("wal"),
        P,
        Vec::new(),
        Offset(40),
        ImportMode::Create,
    )
    .unwrap();
    let (node, log) = open(idle.path(), Offset(40)).await;
    assert_eq!((log.log_start(), log.log_end()), (Offset(40), Offset(40)));
    node.shutdown().await;

    // A log that starts at a snapshot offset (not 0).
    let (node, log) = open(dir.path(), Offset(40)).await;
    assert_eq!((log.log_start(), log.log_end()), (Offset(40), Offset(42)));
    node.shutdown().await;
}
