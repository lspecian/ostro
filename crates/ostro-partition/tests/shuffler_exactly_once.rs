//! Randomised exactly-once property of the outbox shuffler (plan 02-13; STOR-12, roadmap
//! success criterion 4) over P=16 partitions with lost requests, lost acks (duplicate
//! deliveries) and graceful node restarts.
//!
//! Design choice: a proptest-generated op schedule driven by a plain async loop (instead of
//! `proptest-state-machine`), because the system under test is an async `Node` that is
//! consumed by `shutdown` and reopened on `Restart`, which does not fit the state-machine
//! crate's synchronous `&mut` SUT API well. The reference model is the same: per-target-run
//! multiset of expected signal ids plus the set of expected cancelled runs.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::flaky_router::FlakyControl;
use common::{
    cancel_count, cancel_kind, fast_shuffler, grant, handle_of, open_node_cfg, run_on, send_cmd,
    signal_kind, signal_names, start_and_grant, submit_ok, wait_drained,
};
use ostro_partition::Node;
use ostro_types::{AttemptToken, RunId};
use proptest::prelude::*;

const P: u16 = 16;
/// Runs in the model: two per partition.
const RUNS: usize = 32;
const MAX_OPS: usize = 40;
const MAX_RESTARTS: usize = 3;
const DRAIN: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
enum Op {
    /// `src` run sends a uniquely named signal to `target` run.
    Send {
        src: usize,
        target: usize,
    },
    /// `from` run sends `CancelRun(run)`.
    Cancel {
        from: usize,
        run: usize,
    },
    /// Graceful shutdown of the whole node and reopen.
    Restart,
    SetFlakiness {
        p_lost_ack: f64,
        p_lost_req: f64,
    },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        8 => (0..RUNS, 0..RUNS).prop_map(|(src, target)| Op::Send { src, target }),
        2 => (0..RUNS, 0..RUNS).prop_map(|(from, run)| Op::Cancel { from, run }),
        1 => Just(Op::Restart),
        2 => (0.0..=0.5f64, 0.0..=0.5f64)
            .prop_map(|(p_lost_ack, p_lost_req)| Op::SetFlakiness { p_lost_ack, p_lost_req }),
    ]
}

/// At most [`MAX_RESTARTS`] restarts per schedule (later ones are dropped).
fn schedule() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(op(), 1..=MAX_OPS).prop_map(|ops| {
        let mut restarts = 0;
        ops.into_iter()
            .filter(|o| {
                if matches!(o, Op::Restart) {
                    restarts += 1;
                    restarts <= MAX_RESTARTS
                } else {
                    true
                }
            })
            .collect()
    })
}

/// Reference model.
#[derive(Default)]
struct Model {
    signals: BTreeMap<usize, Vec<String>>,
    cancelled: BTreeSet<usize>,
}

struct Sut {
    node: Node,
    runs: Vec<RunId>,
    tokens: Vec<AttemptToken>,
}

async fn open(dir: &Path, ctl: &Arc<FlakyControl>) -> Node {
    let wrap = ctl.wrapper();
    open_node_cfg(dir, P, move |c| {
        fast_shuffler(c);
        c.router_wrapper = Some(wrap);
    })
    .await
}

async fn regrant(node: &Node, runs: &[RunId]) -> Vec<AttemptToken> {
    let mut t = Vec::with_capacity(runs.len());
    for r in runs {
        t.push(grant(node, r).await);
    }
    t
}

/// Execute `ops` against a fresh P=16 node and compare the final state with the model.
/// Returns the injected `(lost_acks, lost_reqs)`.
async fn check(ops: Vec<Op>, seed: u64) -> (usize, usize) {
    let dir = ostro_testkit::fs::tempdir();
    let ctl = FlakyControl::new(seed);
    let node = open(dir.path(), &ctl).await;
    let runs: Vec<RunId> = (0..RUNS)
        .map(|i| run_on((i % usize::from(P)) as u16))
        .collect();
    let mut tokens = Vec::with_capacity(RUNS);
    for r in &runs {
        tokens.push(start_and_grant(&node, r).await);
    }
    let mut sut = Sut { node, runs, tokens };
    let mut model = Model::default();

    for (i, op) in ops.into_iter().enumerate() {
        match op {
            Op::Send { src, target } => {
                let name = format!("{src}/{i}");
                let (s, t) = (sut.runs[src], sut.runs[target]);
                let cmd = send_cmd(&s, sut.tokens[src], vec![signal_kind(&t, &name)]);
                submit_ok(&sut.node, s.partition(), vec![cmd]).await;
                model.signals.entry(target).or_default().push(name);
            }
            Op::Cancel { from, run } => {
                let (f, r) = (sut.runs[from], sut.runs[run]);
                let cmd = send_cmd(&f, sut.tokens[from], vec![cancel_kind(&r)]);
                submit_ok(&sut.node, f.partition(), vec![cmd]).await;
                model.cancelled.insert(run);
            }
            Op::Restart => {
                let Sut { node, runs, .. } = sut;
                node.shutdown().await.expect("shutdown");
                let node = open(dir.path(), &ctl).await;
                let tokens = regrant(&node, &runs).await;
                sut = Sut { node, runs, tokens };
            }
            Op::SetFlakiness {
                p_lost_ack,
                p_lost_req,
            } => ctl.set(p_lost_ack, p_lost_req),
        }
    }

    ctl.set(0.0, 0.0);
    wait_drained(&sut.node, DRAIN).await;
    for (i, r) in sut.runs.iter().enumerate() {
        let got = signal_names(&sut.node, r);
        let mut g = got.clone();
        g.sort();
        let mut e = model.signals.get(&i).cloned().unwrap_or_default();
        e.sort();
        assert_eq!(g, e, "signals of run {i} (no loss, no duplicate)");
        // signals from one sender arrive in send order
        let mut last: BTreeMap<&str, usize> = BTreeMap::new();
        for name in &got {
            let (src, op) = name.split_once('/').expect("name");
            let op: usize = op.parse().expect("op index");
            if let Some(prev) = last.insert(src, op) {
                assert!(prev < op, "run {i}: {src}'s signals out of order");
            }
        }
        let h = handle_of(&sut.node, r.partition());
        let rec = h.read_run(r).expect("read").expect("run exists");
        let want = model.cancelled.contains(&i);
        assert_eq!(rec.cancel_requested, want, "cancel flag of run {i}");
        assert_eq!(
            cancel_count(&sut.node, r),
            usize::from(want),
            "cancel applied once for run {i}"
        );
    }
    sut.node.shutdown().await.expect("shutdown");
    (
        ctl.lost_acks.load(Ordering::Relaxed),
        ctl.lost_reqs.load(Ordering::Relaxed),
    )
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime")
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: ostro_testkit::proptest_cases(16),
        max_shrink_iters: 64,
        ..ProptestConfig::default()
    })]

    #[test]
    fn exactly_once_under_flaky_acks_and_restarts(ops in schedule(), seed in any::<u64>()) {
        rt().block_on(check(ops, seed));
    }
}

/// A fixed heavy schedule that is guaranteed to exercise lost requests, lost acks and
/// restarts (the random schedules may draw low flakiness).
#[test]
fn exactly_once_heavy_faults() {
    let mut ops = vec![Op::SetFlakiness {
        p_lost_ack: 0.4,
        p_lost_req: 0.3,
    }];
    for round in 0..3 {
        for i in 0..60 {
            ops.push(Op::Send {
                src: (i * 7 + round) % RUNS,
                target: (i * 13 + 5 * round) % RUNS,
            });
        }
        ops.push(Op::Cancel {
            from: round,
            run: 10 + round,
        });
        ops.push(Op::Cancel {
            from: round + 1,
            run: 10 + round,
        });
        ops.push(Op::Restart);
    }
    let (lost_acks, lost_reqs) = rt().block_on(check(ops, 42));
    assert!(lost_acks > 0, "no lost acks injected");
    assert!(lost_reqs > 0, "no lost requests injected");
}

/// The fault injector really injects: with high flakiness both fault kinds occur and the
/// outbox still drains with every signal delivered once, in order.
#[test]
fn flaky_router_injects_both_faults() {
    rt().block_on(async {
        let dir = ostro_testkit::fs::tempdir();
        let ctl = FlakyControl::new(7);
        ctl.set(0.4, 0.4);
        let node = open(dir.path(), &ctl).await;
        let src = run_on(3);
        let dst = run_on(9);
        let t = start_and_grant(&node, &src).await;
        start_and_grant(&node, &dst).await;
        let mut want = Vec::new();
        // Keep traffic flowing until both fault kinds were observed.
        for i in 0..2_000 {
            let name = format!("s/{i}");
            submit_ok(
                &node,
                src.partition(),
                vec![send_cmd(&src, t, vec![signal_kind(&dst, &name)])],
            )
            .await;
            want.push(name);
            if ctl.lost_acks.load(Ordering::Relaxed) > 0
                && ctl.lost_reqs.load(Ordering::Relaxed) > 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(ctl.lost_acks.load(Ordering::Relaxed) > 0, "no lost ack");
        assert!(ctl.lost_reqs.load(Ordering::Relaxed) > 0, "no lost request");
        ctl.set(0.0, 0.0);
        wait_drained(&node, DRAIN).await;
        assert_eq!(signal_names(&node, &dst), want);
        node.shutdown().await.expect("shutdown");
    });
}
