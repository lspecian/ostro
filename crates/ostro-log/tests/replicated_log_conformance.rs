//! Generic `ReplicatedLog` conformance checks (Phase 12 reuses the `check_*` functions for the
//! cluster implementation), instantiated here for the RF=1 `Rf1Log`.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use ostro_log::{
    LogError, LogNode, LogNodeConfig, ReadyBarrier, ReplicaProgress, ReplicatedLog, Rf1Log, Role,
    compute_hw,
};
use ostro_types::{Epoch, Offset, PartitionCount, PartitionId};

const NODE: u64 = 7;

/// Model of what a log holds: offset → command bytes.
type Model = BTreeMap<Offset, Bytes>;

/// Deterministic pseudo-random sequence (no rand dependency).
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }
}

fn cmd(tag: &str, i: u64) -> Bytes {
    Bytes::from(format!("{tag}-{i:06}-{}", "y".repeat((i % 23) as usize)))
}

// ---------------------------------------------------------------------------------------------
// Generic checks
// ---------------------------------------------------------------------------------------------

/// `log_start <= HW <= LEO`.
pub fn check_invariant<L: ReplicatedLog>(log: &L) {
    let start = log.log_start();
    let hw = *log.high_watermark().borrow();
    let leo = log.log_end();
    assert!(
        start <= hw && hw <= leo,
        "invariant violated: log_start {start} hw {hw} leo {leo}"
    );
}

/// Random append sequence; invariant after every operation; everything committed at the end.
pub async fn check_random_appends<L: ReplicatedLog>(
    log: &L,
    epoch: Epoch,
    seed: u64,
    n: usize,
    model: &mut Model,
) {
    let mut rng = Lcg(seed);
    let mut last = None;
    for i in 0..n {
        let k = rng.range(1, 5);
        let cmds: Vec<Bytes> = (0..k).map(|j| cmd("r", (i as u64) * 10 + j)).collect();
        let before = log.log_end();
        let r = log.append(epoch, cmds.clone()).await.unwrap();
        assert_eq!(r.base, before, "offsets contiguous");
        assert_eq!(r.last, r.base + (k - 1));
        assert_eq!(r.epoch, epoch);
        for (j, c) in cmds.into_iter().enumerate() {
            model.insert(r.base + j as u64, c);
        }
        check_invariant(log);
        if rng.range(0, 3) == 0 {
            log.wait_committed(r.last).await.unwrap();
            check_invariant(log);
        }
        last = Some(r.last);
    }
    if let Some(l) = last {
        log.wait_committed(l).await.unwrap();
    }
    check_invariant(log);
}

/// Stale epochs are rejected; a newer epoch raises the current one and is recorded.
pub async fn check_epochs<L: ReplicatedLog>(log: &L, model: &mut Model) {
    let cur = log.epochs().latest().unwrap_or(Epoch(0));
    let newer = Epoch(cur.0 + 2);
    let r = log.append(newer, vec![cmd("e", 1)]).await.unwrap();
    model.insert(r.base, cmd("e", 1));
    assert_eq!(log.epochs().latest(), Some(newer));
    assert!(log.epochs().0.contains(&(newer, r.base)));
    match log.append(Epoch(newer.0 - 1), vec![cmd("e", 2)]).await {
        Err(LogError::StaleEpoch { current }) => assert_eq!(current, newer),
        other => panic!("expected StaleEpoch, got {other:?}"),
    }
    // A rejected append leaves no gap.
    assert_eq!(log.log_end(), r.last.next());
    let r2 = log.append(newer, vec![cmd("e", 3)]).await.unwrap();
    model.insert(r2.base, cmd("e", 3));
    assert_eq!(r2.base, r.last.next());
    log.wait_committed(r2.last).await.unwrap();
}

/// `wait_committed(last)` resolves only once HW > last.
pub async fn check_wait_committed<L: ReplicatedLog>(log: &L, epoch: Epoch, model: &mut Model) {
    for i in 0..10 {
        let cmds = vec![cmd("w", i), cmd("w", i + 100)];
        let r = log.append(epoch, cmds.clone()).await.unwrap();
        model.insert(r.base, cmds[0].clone());
        model.insert(r.last, cmds[1].clone());
        log.wait_committed(r.last).await.unwrap();
        assert!(*log.high_watermark().borrow() > r.last);
        check_invariant(log);
    }
}

/// `read(from, HW)` covers exactly `[from, HW)` with contiguous verified batches matching the
/// model; reads never return a batch ending at or beyond `upto`.
pub async fn check_reads<L: ReplicatedLog>(log: &L, model: &Model) {
    let hw = *log.high_watermark().borrow();
    let from = log.log_start();
    let got = log.read(from, hw, usize::MAX).await.unwrap();
    let mut next = from;
    for b in &got {
        assert_eq!(b.base, next, "contiguous bases");
        for (o, c) in b.commands_with_offsets() {
            assert_eq!(model.get(&o), Some(c), "bytes at {o}");
        }
        next = b.end();
    }
    assert_eq!(next, hw, "read covers exactly [from, HW)");

    // Every batch boundary as a start, every offset as an upper bound (sampled).
    let bases: Vec<Offset> = got.iter().map(|b| b.base).collect();
    for (i, &start) in bases.iter().enumerate().step_by(3) {
        for upto in (start.0..=hw.0).step_by(5).map(Offset) {
            let part = log.read(start, upto, usize::MAX).await.unwrap();
            let mut n = start;
            for b in &part {
                assert_eq!(b.base, n);
                assert!(b.end() <= upto, "batch ends beyond upto");
                n = b.end();
            }
            // Maximal: the next batch (if any) would cross upto.
            if let Some(nb) = got.get(i + part.len()) {
                assert!(nb.end() > upto);
            }
        }
    }

    // Small byte budget: still at least one batch, still contiguous.
    let small = log.read(from, hw, 1).await.unwrap();
    if hw > from {
        assert_eq!(small.len(), 1);
        assert_eq!(small[0].base, from);
    }

    // upto beyond HW is capped at HW.
    let capped = log
        .read(from, Offset(hw.0 + 1000), usize::MAX)
        .await
        .unwrap();
    assert_eq!(capped.last().map(|b| b.end()), got.last().map(|b| b.end()));
}

/// ISR is exactly `{node}`.
pub fn check_isr<L: ReplicatedLog>(log: &L, node: u64) {
    assert_eq!(log.isr().0, vec![node]);
}

// ---------------------------------------------------------------------------------------------
// Rf1Log instantiation
// ---------------------------------------------------------------------------------------------

fn cfg(dir: &Path, p: u16) -> LogNodeConfig {
    LogNodeConfig::new(dir.join("wal"), PartitionCount::new(p).unwrap(), NODE)
}

async fn open(dir: &Path, p: u16) -> (LogNode, Vec<Arc<Rf1Log>>) {
    let floors = vec![Offset::ZERO; p as usize];
    let (node, logs) = LogNode::open(cfg(dir, p), &floors).await.unwrap();
    (node, logs.into_iter().map(Result::unwrap).collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf1_conformance_suite() {
    let tmp = ostro_testkit::fs::tempdir();
    let (node, logs) = open(tmp.path(), 1).await;
    let log = &logs[0];
    assert_eq!(log.partition(), PartitionId(0));
    check_invariant(&**log);
    check_isr(&**log, NODE);
    assert_eq!(log.log_end(), Offset::ZERO);

    let mut model = Model::new();
    check_random_appends(&**log, Epoch(1), 0xfeed, 60, &mut model).await;
    check_epochs(&**log, &mut model).await;
    let e = log.epochs().latest().unwrap();
    check_wait_committed(&**log, e, &mut model).await;
    check_reads(&**log, &model).await;
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf1_role_follows_set_role() {
    let tmp = ostro_testkit::fs::tempdir();
    let (node, logs) = open(tmp.path(), 1).await;
    let log = &logs[0];
    let mut role = log.role();
    log.set_role(Role::Leader { epoch: Epoch(4) });
    role.changed().await.unwrap();
    assert_eq!(*role.borrow(), Role::Leader { epoch: Epoch(4) });
    assert_eq!(*log.role().borrow(), Role::Leader { epoch: Epoch(4) });
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf1_reopen_preserves_leo_and_epochs() {
    let tmp = ostro_testkit::fs::tempdir();
    let mut model = Model::new();
    let (leo, epochs) = {
        let (node, logs) = open(tmp.path(), 1).await;
        let log = &logs[0];
        check_random_appends(&**log, Epoch(1), 7, 20, &mut model).await;
        check_random_appends(&**log, Epoch(3), 8, 15, &mut model).await;
        let out = (log.log_end(), log.epochs());
        node.shutdown().await;
        drop(logs);
        drop(node);
        out
    };
    let (node, logs) = open(tmp.path(), 1).await;
    let log = &logs[0];
    assert_eq!(log.log_end(), leo);
    assert_eq!(log.epochs(), epochs);
    assert_eq!(
        *log.high_watermark().borrow(),
        leo,
        "HW == LEO after reopen"
    );
    assert_eq!(log.recovery().leo, leo);
    check_invariant(&**log);
    check_reads(&**log, &model).await;
    // Appends continue contiguously after the reopen, and the stale epoch is still fenced.
    assert!(matches!(
        log.append(Epoch(2), vec![cmd("x", 0)]).await,
        Err(LogError::StaleEpoch { current: Epoch(3) })
    ));
    let r = log.append(Epoch(3), vec![cmd("x", 1)]).await.unwrap();
    assert_eq!(r.base, leo);
    log.wait_committed(r.last).await.unwrap();
    node.shutdown().await;
}

#[test]
fn compute_hw_min_over_isr() {
    let p = [
        ReplicaProgress {
            node: 1,
            leo: Offset(9),
            in_isr: true,
        },
        ReplicaProgress {
            node: 2,
            leo: Offset(4),
            in_isr: true,
        },
        ReplicaProgress {
            node: 3,
            leo: Offset(1),
            in_isr: false,
        },
    ];
    assert_eq!(compute_hw(&p), Offset(4));
    assert_eq!(compute_hw(&p[..1]), Offset(9));
}

#[test]
fn barrier_mints_only_when_open() {
    let b = ReadyBarrier::new();
    assert!(matches!(b.mint(Offset(3)), Err(LogError::NotReady)));
    b.open();
    assert_eq!(b.mint(Offset(3)).unwrap().floor(), Offset(3));
}

#[test]
fn epoch_cache_kafka_semantics() {
    use ostro_log::epoch_cache::EpochCache;
    let c = EpochCache::from_pairs(&[(Epoch(1), Offset(0)), (Epoch(4), Offset(100))]);
    // latest: ends at LEO
    assert_eq!(
        c.end_offset_for_epoch(Epoch(4), Offset(250)),
        Some((Epoch(4), Offset(250)))
    );
    // known earlier: ends where the next began
    assert_eq!(
        c.end_offset_for_epoch(Epoch(1), Offset(250)),
        Some((Epoch(1), Offset(100)))
    );
    // unknown future: UNDEFINED
    assert_eq!(c.end_offset_for_epoch(Epoch(9), Offset(250)), None);
}
