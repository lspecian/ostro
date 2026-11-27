//! 14-29 Task 2 (D-P14-45): the consistent-cut search and the replay observer.
//!
//! - Pure model: generated causal histories of 2-6 partitions (sends, in-order deliveries,
//!   truncations after delivery) with random per-partition upload lag (and, in half the cases,
//!   random restorable floors). [`cluster_cut::search`] must return a cut that satisfies
//!   Pitfall 1 for every pair, stays within `[floor, end]`, is maximal (raising any one
//!   partition breaks consistency), is the greatest consistent cut (brute force on small
//!   boxes), equals the heads with zero lag, and fails only when no consistent cut exists.
//! - Unit cases for each lowering rule and for `NoConsistentCut`.
//! - Observer: a recorded cross-partition signal workload is replayed in Inspect mode with
//!   `CursorRecorders`; the step functions end at the store's cursors, the observed replay
//!   leaves the same digest / state hash as an unobserved one, and the cut search over the
//!   real replay returns the heads of the settled node.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use common::{node_cfg, open, run_on, shutdown, signals_cmd, start_and_grant, submit_ok};
use ostro_backup::cluster_cut::{
    self, CursorSample, CursorSteps, CutError, Lowering, StepFn, steps_from_replay,
};
use ostro_partition::replay_observer::CursorRecorders;
use ostro_partition::{Node, StartMode};
use ostro_types::{AttemptToken, PartitionId, RunId};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;

// ------------------------------------------------------------------ model

/// One event of a partition's log.
#[derive(Clone, Copy, Debug)]
enum Op {
    /// `p` sends one message to `q`.
    Send(usize, usize),
    /// `p` applies the next undelivered message from `q` (in order).
    Deliver(usize, usize),
    /// `p` truncates its outbox to `q` up to what `q` has seen.
    Truncate(usize, usize),
    /// Some unrelated command on `p`.
    Noop(usize),
}

#[derive(Clone, Debug)]
struct Case {
    n: usize,
    ops: Vec<Op>,
    /// Upload lag per partition, as a per-mille of its log length.
    lag: Vec<u16>,
    /// Restorable floor per partition, as a per-mille of its end (`None` = floor 0).
    floor: Option<Vec<u16>>,
}

fn case() -> impl Strategy<Value = Case> {
    (2usize..=6).prop_flat_map(|n| {
        let op = (0u8..10, 0..n, 0..n).prop_map(move |(k, p, q)| {
            let q = if q == p { (p + 1) % n } else { q };
            match k {
                0..=2 => Op::Send(p, q),
                3..=5 => Op::Deliver(p, q),
                6..=7 => Op::Truncate(p, q),
                _ => Op::Noop(p),
            }
        });
        (
            prop::collection::vec(op, 0..48),
            prop::collection::vec(prop_oneof![2 => Just(0u16), 5 => 0u16..=1000], n),
            prop::option::of(prop::collection::vec(
                prop_oneof![3 => Just(0u16), 2 => 0u16..=1000],
                n,
            )),
        )
            .prop_map(move |(ops, lag, floor)| Case { n, ops, lag, floor })
    })
}

/// Simulate the case: per partition, the cursor samples (start at 0, one per event).
fn simulate(c: &Case) -> Vec<Vec<CursorSample>> {
    let n = c.n;
    let mut next = vec![vec![1u64; n]; n];
    let mut trunc = vec![vec![0u64; n]; n];
    let mut seen = vec![vec![0u64; n]; n];
    let mut logs: Vec<Vec<CursorSample>> = (0..n).map(|_| vec![CursorSample::default()]).collect();
    for op in &c.ops {
        let p = match *op {
            Op::Send(p, q) => {
                next[p][q] += 1;
                p
            }
            Op::Deliver(p, q) => {
                if seen[p][q] + 1 < next[q][p] {
                    seen[p][q] += 1;
                }
                p
            }
            Op::Truncate(p, q) => {
                trunc[p][q] = trunc[p][q].max(seen[q][p]);
                p
            }
            Op::Noop(p) => p,
        };
        let map = |v: &[u64], d: u64| -> BTreeMap<u16, u64> {
            v.iter()
                .enumerate()
                .filter(|(q, x)| *q != p && **x != d)
                .map(|(q, x)| (q as u16, *x))
                .collect()
        };
        let at = logs[p].len() as u64;
        logs[p].push(CursorSample {
            at,
            next: map(&next[p], 1),
            truncated: map(&trunc[p], 0),
            seen: map(&seen[p], 0),
        });
    }
    logs
}

fn parts_of(c: &Case) -> BTreeMap<u16, CursorSteps> {
    simulate(c)
        .iter()
        .enumerate()
        .map(|(p, samples)| {
            let len = (samples.len() - 1) as u64;
            let end = len - len * u64::from(c.lag[p]) / 1000;
            let floor = c.floor.as_ref().map_or(0, |f| end * u64::from(f[p]) / 1000);
            (p as u16, steps_from_replay(samples, floor, end))
        })
        .collect()
}

fn ends(parts: &BTreeMap<u16, CursorSteps>) -> BTreeMap<u16, u64> {
    parts.iter().map(|(p, s)| (*p, s.end)).collect()
}

/// Every cut in the box `[floor, end]` when it has at most `limit` points.
fn all_cuts(parts: &BTreeMap<u16, CursorSteps>, limit: u64) -> Option<Vec<BTreeMap<u16, u64>>> {
    let size = parts
        .values()
        .try_fold(1u64, |acc, s| acc.checked_mul(s.end - s.floor + 1))?;
    if size > limit {
        return None;
    }
    let mut out = vec![BTreeMap::new()];
    for (p, s) in parts {
        let mut grown = Vec::new();
        for base in &out {
            for c in s.floor..=s.end {
                let mut m = base.clone();
                m.insert(*p, c);
                grown.push(m);
            }
        }
        out = grown;
    }
    Some(out)
}

#[derive(Default, Debug)]
struct Stats {
    cases: u32,
    lowered_cases: u32,
    lowerings: usize,
    no_cut: u32,
    brute_forced: u32,
    partitions: usize,
}

/// The properties; returns what happened for the statistics.
fn check_case(c: &Case, st: &mut Stats) -> Result<(), TestCaseError> {
    let parts = parts_of(c);
    st.cases += 1;
    st.partitions += parts.len();
    let brute = all_cuts(&parts, 40_000);
    if brute.is_some() {
        st.brute_forced += 1;
    }
    match cluster_cut::search(&parts) {
        Ok(cut) => {
            if !cut.lowered.is_empty() {
                st.lowered_cases += 1;
                st.lowerings += cut.lowered.len();
            }
            // Consistent.
            prop_assert!(
                cluster_cut::check(&parts, &cut.offsets).is_ok(),
                "{:?}",
                cluster_cut::check(&parts, &cut.offsets)
            );
            // Bounded.
            for (p, s) in &parts {
                let x = cut.offsets[p];
                prop_assert!(
                    s.floor <= x && x <= s.end,
                    "p{p}: {x} outside [{}, {}]",
                    s.floor,
                    s.end
                );
            }
            // Maximal: raising any single partition by one, or to its next step, breaks it.
            for (p, s) in &parts {
                let x = cut.offsets[p];
                let mut ups = vec![x + 1];
                ups.extend(s.step_offsets().range(x + 1..).next().copied());
                for up in ups {
                    if up > s.end {
                        continue;
                    }
                    let mut raised = cut.offsets.clone();
                    raised.insert(*p, up);
                    prop_assert!(
                        cluster_cut::check(&parts, &raised).is_err(),
                        "p{p} could rise from {x} to {up}"
                    );
                }
            }
            // Greatest: dominates every consistent cut of the box.
            if let Some(all) = &brute {
                for d in all {
                    if cluster_cut::check(&parts, d).is_ok() {
                        for (p, x) in d {
                            prop_assert!(
                                *x <= cut.offsets[p],
                                "consistent {d:?} above {:?}",
                                cut.offsets
                            );
                        }
                    }
                }
            }
            // Zero lag, floor 0: the heads themselves.
            if c.lag.iter().all(|l| *l == 0) {
                prop_assert_eq!(&cut.offsets, &ends(&parts));
                prop_assert!(cut.lowered.is_empty());
            }
        }
        Err(CutError::NoConsistentCut { partitions }) => {
            st.no_cut += 1;
            prop_assert!(c.floor.is_some(), "floor 0 always has the all-zero cut");
            prop_assert!(!partitions.is_empty());
            if let Some(all) = &brute {
                prop_assert!(
                    all.iter().all(|d| cluster_cut::check(&parts, d).is_err()),
                    "search failed but a consistent cut exists"
                );
            }
        }
    }
    Ok(())
}

proptest! {
    // PROPTEST_CASES overrides (the plan verify runs 256).
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    #[test]
    fn search_is_consistent_maximal_and_bounded(c in case()) {
        check_case(&c, &mut Stats::default())?;
    }
}

/// The same properties over 512 deterministic cases, with the statistics printed (and a
/// floor under the interesting outcomes, so the generator cannot go vacuous).
#[test]
fn search_statistics() {
    let mut runner = TestRunner::deterministic();
    let strat = case();
    let mut st = Stats::default();
    for _ in 0..512 {
        let c = strat.new_tree(&mut runner).expect("tree").current();
        check_case(&c, &mut st).expect("properties");
    }
    eprintln!("cluster_cut statistics: {st:?}");
    assert!(st.lowered_cases >= 50, "{st:?}");
    assert!(st.no_cut >= 3, "{st:?}");
    assert!(st.brute_forced >= 100, "{st:?}");
}

// ------------------------------------------------------------------ unit cases

fn steps(floor: u64, end: u64) -> CursorSteps {
    CursorSteps {
        floor,
        end,
        ..CursorSteps::default()
    }
}

#[test]
fn step_fn_values_and_searches() {
    let s = StepFn(vec![(3, 2), (7, 5)]);
    assert_eq!(s.at(0, 1), 1);
    assert_eq!(s.at(3, 1), 2);
    assert_eq!(s.at(6, 1), 2);
    assert_eq!(s.at(100, 1), 5);
    assert_eq!(s.largest_where(0, 10, 1, |v| v < 5), Some(6));
    assert_eq!(s.largest_where(0, 10, 1, |v| v < 2), Some(2));
    assert_eq!(s.largest_where(3, 10, 1, |v| v < 2), None);
    assert_eq!(s.largest_where(0, 10, 1, |v| v <= 5), Some(10));
    assert_eq!(s.next_step_after(3), Some(7));
    assert_eq!(s.next_step_after(7), None);
}

#[test]
fn rule_one_lowers_the_receiver_below_an_unsent_delivery() {
    // A (0) sent seq 1 at offset 4 (next 1 -> 2) but its head ends at 3; B (1) applied it at 6.
    let mut a = steps(0, 3);
    a.next.insert(1, StepFn(vec![(4, 2)]));
    let mut b = steps(0, 9);
    b.seen.insert(0, StepFn(vec![(6, 1)]));
    let parts = BTreeMap::from([(0, a), (1, b)]);
    assert!(cluster_cut::check(&parts, &ends(&parts)).is_err());
    let cut = cluster_cut::search(&parts).expect("cut");
    assert_eq!(cut.offsets, BTreeMap::from([(0, 3), (1, 5)]));
    assert_eq!(cut.lowered, vec![(1, 9, 5, Lowering::Unsent { from: 0 })]);
}

#[test]
fn rule_two_lowers_the_sender_below_an_unseen_truncation() {
    // A sent 1 at 1, B saw it at 8 but B's head ends at 5; A truncated it at 4.
    let mut a = steps(0, 6);
    a.next.insert(1, StepFn(vec![(1, 2)]));
    a.truncated.insert(1, StepFn(vec![(4, 1)]));
    let mut b = steps(0, 5);
    b.seen.insert(0, StepFn(vec![(8, 1)]));
    let parts = BTreeMap::from([(0, a), (1, b)]);
    let cut = cluster_cut::search(&parts).expect("cut");
    assert_eq!(cut.offsets, BTreeMap::from([(0, 3), (1, 5)]));
    assert_eq!(
        cut.lowered,
        vec![(0, 6, 3, Lowering::Untruncated { to: 1 })]
    );
    assert!(cluster_cut::check(&parts, &cut.offsets).is_ok());
}

#[test]
fn lowerings_cascade_to_the_greatest_fixpoint() {
    // C (2) truncated its outbox to B at 5 after B saw the message (B offset 6), but B's head
    // ends at 4: C drops below the truncation (rule 2), which also drops C's later send to A
    // (offset 8), so A drops below its delivery of it (rule 1).
    let mut a = steps(0, 10);
    a.seen.insert(2, StepFn(vec![(9, 1)]));
    let mut b = steps(0, 4);
    b.seen.insert(2, StepFn(vec![(6, 1)]));
    let mut c = steps(0, 10);
    c.next.insert(1, StepFn(vec![(2, 2)]));
    c.truncated.insert(1, StepFn(vec![(5, 1)]));
    c.next.insert(0, StepFn(vec![(8, 2)]));
    let parts = BTreeMap::from([(0, a), (1, b), (2, c)]);
    let cut = cluster_cut::search(&parts).expect("cut");
    assert!(cluster_cut::check(&parts, &cut.offsets).is_ok());
    assert_eq!(cut.offsets, BTreeMap::from([(0, 8), (1, 4), (2, 4)]));
    assert_eq!(
        cut.lowered,
        vec![
            (2, 10, 4, Lowering::Untruncated { to: 1 }),
            (0, 10, 8, Lowering::Unsent { from: 2 }),
        ]
    );
}

#[test]
fn a_floor_in_the_way_is_no_consistent_cut() {
    let mut a = steps(0, 3);
    a.next.insert(1, StepFn(vec![(4, 2)]));
    // B's oldest reachable snapshot (floor 7) already holds the delivery.
    let mut b = steps(7, 9);
    b.seen.insert(0, StepFn(vec![(7, 1)]));
    let parts = BTreeMap::from([(0, a), (1, b)]);
    assert_eq!(
        cluster_cut::search(&parts),
        Err(CutError::NoConsistentCut {
            partitions: vec![1]
        })
    );
    // A floor above the end is refused up front.
    let parts = BTreeMap::from([(0, steps(5, 4)), (1, steps(0, 1))]);
    assert_eq!(
        cluster_cut::search(&parts),
        Err(CutError::NoConsistentCut {
            partitions: vec![0]
        })
    );
}

#[test]
fn steps_from_replay_keeps_only_changes() {
    let s = |at, next: &[(u16, u64)], seen: &[(u16, u64)]| CursorSample {
        at,
        next: next.iter().copied().collect(),
        truncated: BTreeMap::new(),
        seen: seen.iter().copied().collect(),
    };
    let samples = vec![
        s(10, &[(1, 3)], &[]),
        s(11, &[(1, 3)], &[(2, 1)]),
        s(12, &[(1, 4)], &[(2, 1)]),
        s(15, &[(1, 4)], &[(2, 1)]),
    ];
    let st = steps_from_replay(&samples, 10, 20);
    assert_eq!(st.next[&1], StepFn(vec![(10, 3), (12, 4)]));
    assert_eq!(st.seen[&2], StepFn(vec![(11, 1)]));
    assert!(st.truncated.is_empty());
    assert_eq!((st.floor, st.end), (10, 20));
}

// ------------------------------------------------------------------ observer on a real replay

const P: u16 = 3;

async fn workload(node: &Node) {
    let mut runs: Vec<(RunId, AttemptToken)> = Vec::new();
    for i in 0..6u16 {
        let r = run_on(i % P);
        let t = start_and_grant(node, &r).await;
        runs.push((r, t));
    }
    let mut n = 0u64;
    for round in 0..4 {
        for (i, (r, t)) in runs.iter().enumerate() {
            let targets: Vec<(RunId, String)> = runs
                .iter()
                .enumerate()
                .filter(|(j, (x, _))| *j != i && x.partition() != r.partition())
                .map(|(_, (x, _))| {
                    n += 1;
                    (*x, format!("s{round}-{n}"))
                })
                .collect();
            submit_ok(node, r.partition(), vec![signals_cmd(r, *t, &targets)]).await;
        }
    }
    // Every outbox delivered and truncated by the live shuffler.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let drained = (0..P).all(|p| {
            let m = node
                .handle(PartitionId(p))
                .expect("ready")
                .store()
                .read_meta()
                .expect("meta");
            m.cursor.outbox_next_seq.iter().all(|(q, next)| {
                m.cursor.outbox_truncated.get(q).copied().unwrap_or(0) + 1 == *next
            })
        });
        if drained {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "outboxes never drained"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Let the shuffler's last appends settle.
    tokio::time::sleep(Duration::from_millis(300)).await;
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("mkdir");
    for e in std::fs::read_dir(src).expect("read_dir") {
        let e = e.expect("entry");
        let to = dst.join(e.file_name());
        if e.file_type().expect("file type").is_dir() {
            copy_dir(&e.path(), &to);
        } else {
            std::fs::copy(e.path(), &to).expect("copy");
        }
    }
}

fn drop_states(dir: &Path) {
    for p in 0..P {
        std::fs::remove_dir_all(ostro_partition::store::config::state_dir(
            dir,
            PartitionId(p),
        ))
        .expect("remove state");
    }
}

struct Final {
    digest: [u8; 32],
    hash: [u8; 32],
    applied: u64,
    next: BTreeMap<u16, u64>,
    truncated: BTreeMap<u16, u64>,
    seen: BTreeMap<u16, u64>,
}

fn finals(node: &Node) -> Vec<Final> {
    (0..P)
        .map(|p| {
            let h = node.handle(PartitionId(p)).expect("ready");
            let m = h.store().read_meta().expect("meta");
            Final {
                digest: h.store().digest().expect("digest"),
                hash: m.state_hash,
                applied: m.applied.0,
                next: m.cursor.outbox_next_seq,
                truncated: m.cursor.outbox_truncated,
                seen: m.inbound_seen,
            }
        })
        .collect()
}

fn inspect_cfg(dir: &Path) -> ostro_partition::NodeConfig {
    let mut cfg = node_cfg(dir, P);
    cfg.mode = StartMode::Inspect;
    cfg.backup_enabled = false;
    cfg
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observed_replay_matches_the_store_and_leaves_the_digest_unchanged() {
    let root = tempfile::tempdir().expect("root");
    let (a, plain, observed) = (
        root.path().join("a"),
        root.path().join("plain"),
        root.path().join("observed"),
    );
    let node = open(node_cfg(&a, P)).await;
    workload(&node).await;
    let live = finals(&node);
    shutdown(node).await;
    copy_dir(&a, &plain);
    copy_dir(&a, &observed);
    drop_states(&plain);
    drop_states(&observed);

    // Unobserved Inspect replay.
    let n1 = open(inspect_cfg(&plain)).await;
    let f1 = finals(&n1);
    shutdown(n1).await;
    // Observed Inspect replay.
    let rec = CursorRecorders::new();
    let mut cfg = inspect_cfg(&observed);
    cfg.replay_observer = Some(rec.factory());
    let n2 = open(cfg).await;
    let f2 = finals(&n2);
    shutdown(n2).await;

    let mut parts = BTreeMap::new();
    for p in 0..P {
        let i = usize::from(p);
        assert_eq!(
            f1[i].digest, f2[i].digest,
            "p{p}: digest with/without observer"
        );
        assert_eq!(f1[i].hash, f2[i].hash, "p{p}: state hash");
        assert_eq!(f1[i].applied, f2[i].applied, "p{p}: applied");
        assert_eq!(
            f2[i].digest, live[i].digest,
            "p{p}: rebuild equals the live state"
        );
        assert_eq!(f2[i].applied, live[i].applied);
        let samples = rec.samples(PartitionId(p));
        assert!(samples.len() > 2, "p{p}: {} samples", samples.len());
        assert_eq!(samples[0].at, 0, "replay starts at the (deleted) state's 0");
        assert!(samples.windows(2).all(|w| w[0].at < w[1].at));
        let steps = steps_from_replay(&samples, 0, f2[i].applied);
        let end = f2[i].applied;
        for (q, v) in &f2[i].next {
            assert_eq!(steps.next_at(*q, end), *v, "p{p} next[{q}]");
        }
        for (q, v) in &f2[i].truncated {
            assert_eq!(steps.truncated_at(*q, end), *v, "p{p} truncated[{q}]");
        }
        for (q, v) in &f2[i].seen {
            assert_eq!(steps.seen_at(*q, end), *v, "p{p} seen[{q}]");
        }
        assert!(
            !f2[i].seen.is_empty(),
            "p{p} received cross-partition signals"
        );
        parts.insert(p, steps);
    }
    // The settled node's heads are a consistent cut: the search keeps them.
    let cut = cluster_cut::search(&parts).expect("cut");
    assert_eq!(cut.offsets, ends(&parts));
    // Cutting one partition early forces the others down, and the result still holds.
    let mut lagged = parts.clone();
    let p0 = lagged.get_mut(&0).expect("p0");
    p0.end = p0.step_offsets().iter().nth(3).copied().unwrap_or(p0.end);
    let cut = cluster_cut::search(&lagged).expect("cut");
    assert!(cluster_cut::check(&lagged, &cut.offsets).is_ok());
    assert!(!cut.lowered.is_empty(), "{cut:?}");
}
