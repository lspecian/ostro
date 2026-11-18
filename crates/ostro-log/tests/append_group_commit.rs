//! STOR-01: concurrent appends are sequenced (contiguous offsets, one envelope per WAL record)
//! and share fsyncs through the group-commit WAL.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use ostro_log::{LogNode, LogNodeConfig, ReplicatedLog, Rf1Log, recover_partition};
use ostro_types::{Epoch, Offset, PartitionCount, PartitionId};

fn cfg(dir: &Path, p: u16) -> LogNodeConfig {
    LogNodeConfig::new(dir.join("wal"), PartitionCount::new(p).unwrap(), 1)
}

async fn open(dir: &Path, p: u16) -> (LogNode, Vec<Arc<Rf1Log>>) {
    let floors = vec![Offset::ZERO; p as usize];
    let (node, logs) = LogNode::open(cfg(dir, p), &floors).await.unwrap();
    (node, logs.into_iter().map(Result::unwrap).collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_appends_are_contiguous_and_share_fsyncs() {
    let tmp = ostro_testkit::fs::tempdir();
    let (node, logs) = open(tmp.path(), 1).await;
    let log = logs[0].clone();
    let epoch = Epoch(2);
    let before = node.commit_batches();

    const TASKS: u64 = 16;
    const TOTAL: u64 = 200;
    let mut handles = Vec::new();
    for t in 0..TASKS {
        let log = log.clone();
        handles.push(tokio::spawn(async move {
            let mut out = Vec::new();
            let mut i = t;
            while i < TOTAL {
                let n = 1 + (i % 4);
                let cmds: Vec<Bytes> = (0..n)
                    .map(|j| Bytes::from(format!("t{t}-a{i}-c{j}")))
                    .collect();
                let r = log.append(epoch, cmds.clone()).await.unwrap();
                out.push((r, cmds));
                i += TASKS;
            }
            for (r, _) in &out {
                log.wait_committed(r.last).await.unwrap();
            }
            out
        }));
    }
    let mut all = Vec::new();
    for h in handles {
        all.extend(h.await.unwrap());
    }
    assert_eq!(all.len() as u64, TOTAL);
    all.sort_by_key(|(r, _)| r.base);

    // Contiguous, no gaps or duplicates.
    let mut next = Offset::ZERO;
    let mut model = BTreeMap::new();
    for (r, cmds) in &all {
        assert_eq!(r.base, next, "gap or duplicate at {next}");
        assert_eq!(r.last, r.base + (cmds.len() as u64 - 1));
        assert_eq!(r.epoch, epoch);
        for (j, c) in cmds.iter().enumerate() {
            model.insert(r.base + j as u64, c.clone());
        }
        next = r.last.next();
    }
    assert_eq!(log.log_end(), next);
    assert_eq!(*log.high_watermark().borrow(), next);

    // fsyncs were shared.
    let batches = node.commit_batches() - before;
    assert!(batches > 0);
    assert!(
        batches < TOTAL,
        "expected group commit to share fsyncs: {batches} WAL batches for {TOTAL} appends"
    );

    // Read-back matches.
    let got = log.read(Offset::ZERO, next, usize::MAX).await.unwrap();
    assert_eq!(got.len() as u64, TOTAL, "one envelope per append");
    for b in &got {
        for (o, c) in b.commands_with_offsets() {
            assert_eq!(model.get(&o), Some(c));
        }
    }

    node.shutdown().await;
    drop(log);
    drop(logs);
    drop(node);

    // Every record verifies at open (frame CRC, envelope CRC/codec, trailer), LEO matches.
    let wal_dir = tmp.path().join("wal");
    let rep = recover_partition(&wal_dir, PartitionId(0), Offset::ZERO).unwrap();
    assert_eq!(rep.records, TOTAL);
    assert_eq!(rep.leo, next);
    assert_eq!(rep.log_start, Offset::ZERO);
    assert_eq!(rep.epochs, vec![(epoch, Offset::ZERO)]);
    assert!(rep.torn.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitions_on_one_node_are_independent() {
    let tmp = ostro_testkit::fs::tempdir();
    let (node, logs) = open(tmp.path(), 4).await;
    let mut handles = Vec::new();
    for (i, log) in logs.iter().enumerate() {
        let log = log.clone();
        handles.push(tokio::spawn(async move {
            let n = (i + 1) * 5;
            let mut last = None;
            for k in 0..n {
                let r = log
                    .append(Epoch(1), vec![Bytes::from(format!("p{i}-{k}"))])
                    .await
                    .unwrap();
                last = Some(r.last);
            }
            log.wait_committed(last.unwrap()).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    for (i, log) in logs.iter().enumerate() {
        let expect = Offset(((i + 1) * 5) as u64);
        assert_eq!(log.partition(), PartitionId(i as u16));
        assert_eq!(log.log_end(), expect);
        assert_eq!(*log.high_watermark().borrow(), expect, "partition {i} HW");
        let got = log.read(Offset::ZERO, expect, usize::MAX).await.unwrap();
        assert_eq!(got.len(), (i + 1) * 5);
        assert_eq!(got[0].commands[0], Bytes::from(format!("p{i}-0")));
    }
    node.shutdown().await;
}
