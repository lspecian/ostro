//! Outbox shuffler at P=16 (plan 02-13; STOR-12): every cross-partition message kind is
//! delivered exactly once through the outbox, outboxes drain, restarts only cause harmless
//! re-sends, and there is never more than one delivery in flight per (source, target).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use common::{
    T, cancel_count, cancel_kind, fast_shuffler, grant, handle_of, inbox, open_node_cfg,
    outbox_rows, run_on, send_cmd, signal_kind, signal_names, start_and_grant, start_run,
    submit_ok, wait_drained, wait_until,
};
use ostro_partition::node::RouterWrapper;
use ostro_partition::store::values::{InboxMsg, RunStatus};
use ostro_partition::{InProcessRouter, Node, PartitionRouter, RouteFuture, Table};
use ostro_types::command::{
    CompleteRun, CronStart, Derived, EventsAppended, InboundBatch, ParentRef, StartChild,
    TerminalStatus, derived, outbox_msg,
};
use ostro_types::{AttemptToken, Command, PartitionId, PayloadRef, RunId};

const P: u16 = 16;
const DRAIN: Duration = Duration::from_secs(10);

/// Asserts at most one in-flight `deliver` per (src, target); optionally slows deliveries.
struct ProbeRouter {
    inner: Arc<InProcessRouter>,
    active: Mutex<BTreeSet<(u32, u16)>>,
    violations: AtomicUsize,
    calls: AtomicUsize,
    max_concurrent: AtomicUsize,
    delay: Duration,
}

struct ActiveGuard<'a> {
    probe: &'a ProbeRouter,
    key: (u32, u16),
}

impl Drop for ActiveGuard<'_> {
    fn drop(&mut self) {
        self.probe.active.lock().unwrap().remove(&self.key);
    }
}

impl ProbeRouter {
    fn new(inner: Arc<InProcessRouter>, delay: Duration) -> Self {
        Self {
            inner,
            active: Mutex::new(BTreeSet::new()),
            violations: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
            max_concurrent: AtomicUsize::new(0),
            delay,
        }
    }
}

impl PartitionRouter for ProbeRouter {
    fn deliver(&self, target: PartitionId, batch: InboundBatch) -> RouteFuture<'_> {
        Box::pin(async move {
            let key = (batch.src, target.0);
            self.calls.fetch_add(1, Ordering::Relaxed);
            {
                let mut a = self.active.lock().unwrap();
                if !a.insert(key) {
                    self.violations.fetch_add(1, Ordering::Relaxed);
                }
                self.max_concurrent.fetch_max(a.len(), Ordering::Relaxed);
            }
            let _g = ActiveGuard { probe: self, key };
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.inner.deliver(target, batch).await
        })
    }
}

/// A node whose leader services route through a [`ProbeRouter`] (returned for inspection).
async fn open_probed(dir: &std::path::Path, delay: Duration) -> (Node, Arc<ProbeRouter>) {
    let slot: Arc<Mutex<Option<Arc<ProbeRouter>>>> = Arc::new(Mutex::new(None));
    let s2 = Arc::clone(&slot);
    let wrap: RouterWrapper = Arc::new(move |inner| {
        let p = Arc::new(ProbeRouter::new(inner, delay));
        *s2.lock().unwrap() = Some(Arc::clone(&p));
        p as Arc<dyn PartitionRouter>
    });
    let node = open_node_cfg(dir, P, |c| {
        fast_shuffler(c);
        c.router_wrapper = Some(wrap);
    })
    .await;
    let probe = slot.lock().unwrap().clone().expect("wrapper called");
    (node, probe)
}

/// splitmix64: deterministic test randomness.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        ((z ^ (z >> 31)) % n as u64) as usize
    }
}

/// One granted sender run per partition plus `per` target runs per partition.
async fn setup_runs(node: &Node, per: usize) -> (Vec<(RunId, AttemptToken)>, Vec<RunId>) {
    let mut senders = Vec::new();
    let mut targets = Vec::new();
    for p in 0..P {
        let s = run_on(p);
        let t = start_and_grant(node, &s).await;
        senders.push((s, t));
        for _ in 0..per {
            let r = run_on(p);
            submit_ok(node, r.partition(), vec![Command::start_run(start_run(&r))]).await;
            targets.push(r);
        }
    }
    (senders, targets)
}

/// Send `n` uniquely named signals from random senders to random targets, grouped into
/// commands of up to 8 signals and envelopes of up to 16 commands. Returns the expected
/// signal names per target.
async fn send_signals(
    node: &Node,
    senders: &[(RunId, AttemptToken)],
    targets: &[RunId],
    n: usize,
    seed: u8,
    tag: &str,
) -> BTreeMap<RunId, Vec<String>> {
    let mut rng = Rng(u64::from(seed));
    let mut expected: BTreeMap<RunId, Vec<String>> = BTreeMap::new();
    // per sender: list of (target, name)
    let mut per_sender: Vec<Vec<(RunId, String)>> = vec![Vec::new(); senders.len()];
    for i in 0..n {
        let s = rng.below(senders.len());
        let t = rng.below(targets.len());
        let name = format!("{tag}/{s}/{i}");
        expected.entry(targets[t]).or_default().push(name.clone());
        per_sender[s].push((targets[t], name));
    }
    let mut sends = Vec::new();
    for (si, msgs) in per_sender.into_iter().enumerate() {
        let (run, tok) = senders[si];
        let cmds: Vec<Command> = msgs
            .chunks(8)
            .map(|c| {
                send_cmd(
                    &run,
                    tok,
                    c.iter().map(|(t, name)| signal_kind(t, name)).collect(),
                )
            })
            .collect();
        let h = handle_of(node, run.partition()).clone();
        sends.push(tokio::spawn(async move {
            for env in cmds.chunks(16) {
                let out = tokio::time::timeout(T, h.submit(env.to_vec()))
                    .await
                    .expect("submit timed out")
                    .expect("submit");
                assert!(
                    out.iter()
                        .all(|o| *o == ostro_partition::ApplyOutcome::Accepted)
                );
            }
        }));
    }
    for s in sends {
        s.await.unwrap();
    }
    expected
}

/// Every target's inbox holds exactly the expected signals (as a multiset), and signals
/// from one sender arrive in send order.
fn assert_signals(node: &Node, targets: &[RunId], expected: &BTreeMap<RunId, Vec<String>>) {
    for t in targets {
        let got = signal_names(node, t);
        let mut g = got.clone();
        g.sort();
        let mut e = expected.get(t).cloned().unwrap_or_default();
        e.sort();
        assert_eq!(g, e, "signals of {t:?}");
        // per-sender order: indices ascending
        let mut last: BTreeMap<String, u64> = BTreeMap::new();
        for name in got {
            let mut parts = name.rsplitn(2, '/');
            let i: u64 = parts.next().unwrap().parse().unwrap();
            let key = parts.next().unwrap().to_string();
            if let Some(prev) = last.insert(key, i) {
                assert!(prev < i, "out of order delivery to {t:?}: {prev} then {i}");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signals_exactly_once_single_in_flight() {
    let dir = ostro_testkit::fs::tempdir();
    let (node, probe) = open_probed(dir.path(), Duration::ZERO).await;
    let (senders, targets) = setup_runs(&node, 4).await;
    let expected = send_signals(&node, &senders, &targets, 1000, 1, "a").await;
    wait_drained(&node, DRAIN).await;
    assert_signals(&node, &targets, &expected);
    assert_eq!(
        probe.violations.load(Ordering::Relaxed),
        0,
        "concurrent deliver per pair"
    );
    assert!(probe.calls.load(Ordering::Relaxed) > 0);
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_in_flight_with_slow_router() {
    // Slow deliveries make overlapping rounds likely if the rule were broken.
    let dir = ostro_testkit::fs::tempdir();
    let (node, probe) = open_probed(dir.path(), Duration::from_millis(5)).await;
    let (senders, targets) = setup_runs(&node, 2).await;
    let expected = send_signals(&node, &senders, &targets, 400, 2, "b").await;
    wait_drained(&node, DRAIN).await;
    assert_signals(&node, &targets, &expected);
    assert_eq!(
        probe.violations.load(Ordering::Relaxed),
        0,
        "concurrent deliver per pair"
    );
    assert!(
        probe.max_concurrent.load(Ordering::Relaxed) > 1,
        "deliveries to different pairs should overlap"
    );
    node.shutdown().await.unwrap();
}

fn child_start_kind(parent: &RunId, child: &RunId, op: u64) -> outbox_msg::Kind {
    let mut s = start_run(child);
    s.parent = Some(ParentRef {
        run_id: parent.as_bytes().to_vec(),
        op,
    });
    outbox_msg::Kind::StartChild(StartChild { start: Some(s) })
}

fn complete_cmd(run: &RunId, t: AttemptToken) -> Command {
    Command::events_appended(EventsAppended {
        run_id: run.as_bytes().to_vec(),
        token: Some(ostro_types::command::AttemptTokenMsg {
            epoch: t.epoch.0,
            n: t.n,
        }),
        from_seq: 0,
        events: vec![],
        derived: vec![Derived {
            intent: Some(derived::Intent::CompleteRun(CompleteRun {
                status: TerminalStatus::Completed as i32,
                result: Some(PayloadRef::inline(Bytes::from_static(b"out"))),
                completed_at_ms: 9,
                resumable: false,
                parent_event: None,
            })),
        }],
    })
}

async fn wait_run(node: &Node, run: &RunId) {
    let h = handle_of(node, run.partition()).clone();
    let r = *run;
    wait_until(DRAIN, "run to exist", move || {
        h.read_run(&r).expect("read").is_some()
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn child_start_and_completion() {
    let dir = ostro_testkit::fs::tempdir();
    let node = open_node_cfg(dir.path(), P, fast_shuffler).await;
    let parent = run_on(1);
    let pt = start_and_grant(&node, &parent).await;
    let child = run_on(7);
    submit_ok(
        &node,
        parent.partition(),
        vec![send_cmd(
            &parent,
            pt,
            vec![child_start_kind(&parent, &child, 3)],
        )],
    )
    .await;
    wait_run(&node, &child).await;
    let c = handle_of(&node, child.partition())
        .read_run(&child)
        .unwrap()
        .unwrap();
    assert_eq!(c.parent, Some((*parent.as_bytes(), 3)));

    let ct = grant(&node, &child).await;
    submit_ok(&node, child.partition(), vec![complete_cmd(&child, ct)]).await;
    wait_drained(&node, DRAIN).await;
    let pr = handle_of(&node, parent.partition())
        .read_run(&parent)
        .unwrap()
        .unwrap();
    assert_eq!(pr.children, 0);
    let completions: Vec<InboxMsg> = inbox(&node, &parent)
        .into_iter()
        .filter(|m| matches!(m, InboxMsg::ChildCompleted { .. }))
        .collect();
    assert_eq!(completions.len(), 1, "{completions:?}");
    match &completions[0] {
        InboxMsg::ChildCompleted {
            child: c,
            op,
            status,
            ..
        } => {
            assert_eq!(c, child.as_bytes());
            assert_eq!(*op, 3);
            assert_eq!(*status, RunStatus::Completed);
        }
        _ => unreachable!(),
    }
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_cascade_across_partitions() {
    let dir = ostro_testkit::fs::tempdir();
    let node = open_node_cfg(dir.path(), P, fast_shuffler).await;
    let parent = run_on(0);
    let pt = start_and_grant(&node, &parent).await;
    let (c3, c12, g5) = (run_on(3), run_on(12), run_on(5));
    submit_ok(
        &node,
        parent.partition(),
        vec![send_cmd(
            &parent,
            pt,
            vec![
                child_start_kind(&parent, &c3, 1),
                child_start_kind(&parent, &c12, 2),
            ],
        )],
    )
    .await;
    wait_run(&node, &c3).await;
    wait_run(&node, &c12).await;
    let t3 = grant(&node, &c3).await;
    submit_ok(
        &node,
        c3.partition(),
        vec![send_cmd(&c3, t3, vec![child_start_kind(&c3, &g5, 1)])],
    )
    .await;
    wait_run(&node, &g5).await;

    // Some run on p9 cancels the parent.
    let canceller = run_on(9);
    let kt = start_and_grant(&node, &canceller).await;
    submit_ok(
        &node,
        canceller.partition(),
        vec![send_cmd(&canceller, kt, vec![cancel_kind(&parent)])],
    )
    .await;
    let all = [parent, c3, c12, g5];
    wait_until(DRAIN, "cascade", || {
        all.iter().all(|r| {
            handle_of(&node, r.partition())
                .read_run(r)
                .unwrap()
                .is_some_and(|x| x.cancel_requested)
        })
    })
    .await;
    wait_drained(&node, DRAIN).await;
    for r in &all {
        assert_eq!(cancel_count(&node, r), 1, "cancel of {r:?}");
    }
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cron_start_once_from_two_sources() {
    let dir = ostro_testkit::fs::tempdir();
    let node = open_node_cfg(dir.path(), P, fast_shuffler).await;
    let (a, b) = (run_on(6), run_on(6));
    let cron = |id: &RunId| {
        outbox_msg::Kind::CronStart(CronStart {
            schedule_id: "nightly".into(),
            fire_time_ms: 1_000,
            start: Some(start_run(id)),
        })
    };
    let s2 = run_on(2);
    let s11 = run_on(11);
    let t2 = start_and_grant(&node, &s2).await;
    let t11 = start_and_grant(&node, &s11).await;
    let (ra, rb) = tokio::join!(
        submit_ok(
            &node,
            s2.partition(),
            vec![send_cmd(&s2, t2, vec![cron(&a)])]
        ),
        submit_ok(
            &node,
            s11.partition(),
            vec![send_cmd(&s11, t11, vec![cron(&b)])]
        ),
    );
    let ((), ()) = (ra, rb);
    wait_drained(&node, DRAIN).await;
    let h6 = handle_of(&node, PartitionId(6));
    let started = [a, b]
        .iter()
        .filter(|r| h6.read_run(r).unwrap().is_some())
        .count();
    assert_eq!(started, 1, "same (schedule, fire_time) starts exactly once");
    // p6 holds exactly one run (the cron run).
    let runs = h6.reader().prefix(Table::Runs, &[], usize::MAX).unwrap();
    assert_eq!(runs.len(), 1);
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_mid_flight_delivers_exactly_once() {
    let dir = ostro_testkit::fs::tempdir();
    // First incarnation: slow deliveries, so rows are still pending at shutdown.
    let (node, _probe) = open_probed(dir.path(), Duration::from_millis(50)).await;
    let (senders, targets) = setup_runs(&node, 2).await;
    let expected = send_signals(&node, &senders, &targets, 300, 3, "r").await;
    let pending = outbox_rows(&node);
    assert!(pending > 0, "shutdown should happen mid-flight");
    node.shutdown().await.unwrap();

    let node = open_node_cfg(dir.path(), P, fast_shuffler).await;
    wait_drained(&node, DRAIN).await;
    assert_signals(&node, &targets, &expected);
    node.shutdown().await.unwrap();
}
