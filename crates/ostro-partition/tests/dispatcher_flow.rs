//! Dispatcher placement / flow-control seams (plan 07-05: DISP-03/04/05, WORK-03, OPS-01),
//! driven by fake `FlowControl` / `Placement` implementations with scripted answers and
//! call counters, and a fake `Executor`:
//!
//! - admission and reservation happen before the grant, so a blocked entry never burns an
//!   attempt number (the eventual token is n = 1);
//! - queue caps, key caps with `until`, `Paused` queues (resumed on `changed()`), a paused
//!   workflow on a shared queue holds back only its own entries (fix-0705), weighted
//!   fairness (3:1 over 400 dispatches) and strict priority bands;
//! - a placement-blocked queue does not starve another queue (B2);
//! - throttles see `first_activation` only from F7: re-grants (executor requeue, Retry,
//!   lease expiry) are `first_activation == false` and are admitted (B1);
//! - a throttled first activation ahead of a re-activation on the same partition, queue
//!   and key holds back only first activations: the re-activation is granted as soon as
//!   it is due (07-23, gap G1; queue- and key-scoped throttles);
//! - the lease owns the flow permit and the reservation (both released on expiry);
//!   `ExecOutcome::Abandoned` re-grants n + 1 without counting a failure;
//!   `Action::RunPaused` revokes a running job and drops a queued entry;
//! - per-queue gauges.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{OPEN_T, T, run_on, start_run};
use ostro_partition::{
    Admit, ApplyOutcome, Clock, DispatcherConfig, DispatcherStats, EntryView, ExecOutcome,
    Executor, FlowControl, FlowPermit, Job, Node, NodeConfig, PartitionHandle, PauseScope,
    Placement, QueueGauges, Reservation, ReservationKind, Reserve, SystemClock,
};
use ostro_types::command::{
    AttemptTokenMsg, Derived, EnqueueReady, EventsAppended, PauseRun, ResumeRun, StartRun, derived,
};
use ostro_types::{AttemptToken, Command, PartitionCount, PartitionId, RunId};
use tokio::sync::{Semaphore, watch};
use tokio::time::{Instant, sleep, timeout};

const P0: PartitionId = PartitionId(0);

type Handle = PartitionHandle<ostro_log::PartitionLog>;
type ExecFuture = Pin<Box<dyn Future<Output = ExecOutcome> + Send + 'static>>;
type Script = dyn Fn(Job, usize) -> ExecFuture + Send + Sync;

// ------------------------------------------------------------------ fake executor

#[derive(Clone, Debug)]
struct Seen {
    run: RunId,
    token: AttemptToken,
    failures: u32,
    queue: String,
    kind: ReservationKind,
}

struct Fake {
    seen: Mutex<Vec<Seen>>,
    calls: Mutex<BTreeMap<RunId, usize>>,
    script: Box<Script>,
}

impl Fake {
    fn new(script: impl Fn(Job, usize) -> ExecFuture + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(Vec::new()),
            calls: Mutex::new(BTreeMap::new()),
            script: Box::new(script),
        })
    }

    fn releasing() -> Arc<Self> {
        Self::new(|_, _| boxed(async { ExecOutcome::Released }))
    }

    /// Holds every job for `ms` (or until revoked), then releases it.
    fn holding_for(ms: u64) -> Arc<Self> {
        Self::new(move |mut job, _| {
            boxed(async move {
                let _ = timeout(Duration::from_millis(ms), job.revoked.wait_for(|v| *v)).await;
                ExecOutcome::Released
            })
        })
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("lock").clone()
    }

    fn seen_runs(&self) -> Vec<RunId> {
        self.seen().iter().map(|s| s.run).collect()
    }

    async fn wait_jobs(&self, n: usize, limit: Duration) -> Vec<Seen> {
        let deadline = Instant::now() + limit;
        loop {
            let s = self.seen();
            if s.len() >= n {
                return s;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {n} jobs; saw {}",
                s.len()
            );
            sleep(Duration::from_millis(10)).await;
        }
    }
}

impl Executor for Fake {
    fn execute(&self, job: Job) -> ExecFuture {
        self.seen.lock().expect("lock").push(Seen {
            run: job.run,
            token: job.token,
            failures: job.failures,
            queue: job.record.queue.clone(),
            kind: job.reservation.kind(),
        });
        let idx = {
            let mut c = self.calls.lock().expect("lock");
            let e = c.entry(job.run).or_default();
            *e += 1;
            *e - 1
        };
        (self.script)(job, idx)
    }
}

fn boxed(f: impl Future<Output = ExecOutcome> + Send + 'static) -> ExecFuture {
    Box::pin(f)
}

// ------------------------------------------------------------------ fake flow control

/// What the fake flow control saw of an entry.
#[derive(Clone, Debug)]
struct View {
    queue: String,
    workflow: String,
    key: String,
    priority: i32,
    first: bool,
}

impl View {
    fn of(e: &EntryView<'_>) -> Self {
        Self {
            queue: e.queue.into(),
            workflow: e.workflow.into(),
            key: e.concurrency_key.into(),
            priority: e.priority,
            first: e.first_activation,
        }
    }
}

/// Scripted answer of [`FakeFlow`].
#[derive(Clone, Copy, Debug)]
enum Decision {
    Yes,
    QueueBlocked(Option<u64>),
    KeyBlocked(Option<u64>),
    /// Queue-scoped pause.
    Paused,
    /// Workflow-scoped pause.
    PausedWorkflow,
}

type FlowScript = dyn Fn(&View, u64, &FakeFlow) -> Decision + Send + Sync;

/// Counts admissions, rollbacks and releases; `inflight` = committed permits not yet
/// released plus tentative ones.
struct FakeFlow {
    script: Box<FlowScript>,
    weights: BTreeMap<String, u32>,
    changed: watch::Sender<u64>,
    calls: Mutex<Vec<(View, &'static str)>>,
    admitted: AtomicUsize,
    inflight: Arc<AtomicUsize>,
    rolled_back: Arc<AtomicUsize>,
    released: Arc<AtomicUsize>,
}

impl FakeFlow {
    fn new(script: impl Fn(&View, u64, &FakeFlow) -> Decision + Send + Sync + 'static) -> Self {
        Self {
            script: Box::new(script),
            weights: BTreeMap::new(),
            changed: watch::Sender::new(0),
            calls: Mutex::new(Vec::new()),
            admitted: AtomicUsize::new(0),
            inflight: Arc::new(AtomicUsize::new(0)),
            rolled_back: Arc::new(AtomicUsize::new(0)),
            released: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn with_weight(mut self, q: &str, w: u32) -> Self {
        self.weights.insert(q.into(), w);
        self
    }

    fn bump(&self) {
        self.changed.send_modify(|v| *v += 1);
    }

    fn calls(&self) -> Vec<(View, &'static str)> {
        self.calls.lock().expect("lock").clone()
    }
}

impl FlowControl for FakeFlow {
    fn admit(&self, e: &EntryView<'_>, now_ms: u64) -> Admit {
        let v = View::of(e);
        let d = (self.script)(&v, now_ms, self);
        let tag = match d {
            Decision::Yes => "yes",
            Decision::QueueBlocked(_) => "queue",
            Decision::KeyBlocked(_) => "key",
            Decision::Paused => "paused",
            Decision::PausedWorkflow => "paused_workflow",
        };
        self.calls.lock().expect("lock").push((v, tag));
        match d {
            Decision::Yes => {
                self.admitted.fetch_add(1, Ordering::SeqCst);
                self.inflight.fetch_add(1, Ordering::SeqCst);
                let (i1, rb) = (Arc::clone(&self.inflight), Arc::clone(&self.rolled_back));
                let (i2, rel) = (Arc::clone(&self.inflight), Arc::clone(&self.released));
                let tx = self.changed.clone();
                Admit::Yes(
                    FlowPermit::new(move || {
                        i1.fetch_sub(1, Ordering::SeqCst);
                        rb.fetch_add(1, Ordering::SeqCst);
                    })
                    .with_release(move || {
                        i2.fetch_sub(1, Ordering::SeqCst);
                        rel.fetch_add(1, Ordering::SeqCst);
                        tx.send_modify(|v| *v += 1);
                    }),
                )
            }
            Decision::QueueBlocked(until_ms) => Admit::QueueBlocked { until_ms },
            Decision::KeyBlocked(until_ms) => Admit::KeyBlocked { until_ms },
            Decision::Paused => Admit::Paused {
                scope: PauseScope::Queue,
            },
            Decision::PausedWorkflow => Admit::Paused {
                scope: PauseScope::Workflow,
            },
        }
    }

    fn queue_weight(&self, queue: &str) -> u32 {
        self.weights.get(queue).copied().unwrap_or(1)
    }

    fn changed(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
}

// ------------------------------------------------------------------ fake placement

/// The fake's reservation guard: counts drops and bumps `changed` (like a real placement).
struct Guard {
    live: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
    changed: watch::Sender<u64>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
        self.dropped.fetch_add(1, Ordering::SeqCst);
        self.changed.send_modify(|v| *v += 1);
    }
}

/// Grants while `live < cap` and `allow(queue)`; queues starting with `w` are `Worker`.
struct FakePlacement {
    cap: AtomicUsize,
    blocked_queues: Mutex<Vec<String>>,
    reserves: AtomicUsize,
    live: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
    changed: watch::Sender<u64>,
}

impl FakePlacement {
    fn new(cap: usize) -> Self {
        Self {
            cap: AtomicUsize::new(cap),
            blocked_queues: Mutex::new(Vec::new()),
            reserves: AtomicUsize::new(0),
            live: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicUsize::new(0)),
            changed: watch::Sender::new(0),
        }
    }

    fn set_cap(&self, cap: usize) {
        self.cap.store(cap, Ordering::SeqCst);
        self.bump();
    }

    fn block_queue(&self, q: &str) {
        self.blocked_queues.lock().expect("lock").push(q.into());
    }

    fn unblock_all(&self) {
        self.blocked_queues.lock().expect("lock").clear();
        self.bump();
    }

    fn bump(&self) {
        self.changed.send_modify(|v| *v += 1);
    }
}

impl Placement for FakePlacement {
    fn reserve(&self, e: &EntryView<'_>) -> Reserve {
        self.reserves.fetch_add(1, Ordering::SeqCst);
        if self
            .blocked_queues
            .lock()
            .expect("lock")
            .iter()
            .any(|q| q == e.queue)
        {
            return Reserve::Blocked;
        }
        let cap = self.cap.load(Ordering::SeqCst);
        let ok = self
            .live
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |l| {
                (l < cap).then_some(l + 1)
            })
            .is_ok();
        if !ok {
            return Reserve::Blocked;
        }
        let kind = if e.queue.starts_with('w') {
            ReservationKind::Worker
        } else {
            ReservationKind::Embedded
        };
        Reserve::Granted(Reservation::new(
            kind,
            Guard {
                live: Arc::clone(&self.live),
                dropped: Arc::clone(&self.dropped),
                changed: self.changed.clone(),
            },
        ))
    }

    fn changed(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
}

// ------------------------------------------------------------------ harness

struct Opts {
    fake: Arc<Fake>,
    lease_ttl: Duration,
    stats: Arc<DispatcherStats>,
    flow: Option<Arc<FakeFlow>>,
    placement: Option<Arc<FakePlacement>>,
    gauges: Option<Arc<QueueGauges>>,
}

impl Opts {
    fn new(fake: &Arc<Fake>) -> Self {
        Self {
            fake: Arc::clone(fake),
            lease_ttl: Duration::from_secs(30),
            stats: Arc::new(DispatcherStats::default()),
            flow: None,
            placement: None,
            gauges: None,
        }
    }
}

async fn open(dir: &Path, o: &Opts) -> Node {
    let mut cfg = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(1).expect("non-zero"));
    let executor: Arc<dyn Executor> = o.fake.clone();
    let mut d = DispatcherConfig::new(executor);
    d.lease_ttl = o.lease_ttl;
    d.slots = Arc::new(Semaphore::new(64));
    d.stats = Some(Arc::clone(&o.stats));
    d.flow = o.flow.clone().map(|f| f as Arc<dyn FlowControl>);
    d.placement = o.placement.clone().map(|p| p as Arc<dyn Placement>);
    d.gauges = o.gauges.clone();
    cfg.leader.dispatcher = Some(d);
    timeout(OPEN_T, Node::open(cfg))
        .await
        .expect("node open timed out")
        .expect("node open")
}

fn h(node: &Node) -> &Handle {
    node.handle(P0).expect("partition ready")
}

async fn submit_all(h: &Handle, cmds: Vec<Command>) {
    let out = timeout(T, h.submit(cmds))
        .await
        .expect("submit timed out")
        .expect("submit");
    assert!(
        out.iter().all(|o| *o == ApplyOutcome::Accepted),
        "not all accepted: {out:?}"
    );
}

fn start_in(run: &RunId, queue: &str, priority: i32, key: &str) -> Command {
    Command::start_run(StartRun {
        queue: queue.into(),
        priority,
        concurrency_key: key.into(),
        ..start_run(run)
    })
}

fn now_ms() -> u64 {
    SystemClock.now_ms()
}

async fn wait_until(limit: Duration, what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + limit;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(5)).await;
    }
}

/// Requeue `job.run` through the executor's own `EnqueueReady` intent (a re-activation of
/// an already-started run: apply sets `first_activation = false`).
async fn requeue(job: &Job) {
    let cmd = Command::events_appended(EventsAppended {
        run_id: job.run.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg {
            epoch: job.token.epoch.0,
            n: job.token.n,
        }),
        from_seq: job.record.next_seq,
        events: vec![],
        derived: vec![Derived::new(derived::Intent::EnqueueReady(EnqueueReady {
            ready_at_ms: 0,
        }))],
    });
    let o = job.handle.submit_one(cmd).await.expect("requeue submit");
    assert_eq!(o, ApplyOutcome::Accepted);
}

/// Like [`requeue`], but the re-activation is due at `ready_at_ms` (a durable timer).
async fn requeue_at(job: &Job, ready_at_ms: u64) {
    let cmd = Command::events_appended(EventsAppended {
        run_id: job.run.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg {
            epoch: job.token.epoch.0,
            n: job.token.n,
        }),
        from_seq: job.record.next_seq,
        events: vec![],
        derived: vec![Derived::new(derived::Intent::EnqueueReady(EnqueueReady {
            ready_at_ms,
        }))],
    });
    let o = job.handle.submit_one(cmd).await.expect("requeue submit");
    assert_eq!(o, ApplyOutcome::Accepted);
}

fn attempt(h: &Handle, run: &RunId) -> Option<(u32, u32)> {
    h.read_run(run).expect("read").expect("run").current_attempt
}

// ------------------------------------------------------------------ tests

/// A queue cap of 2: never more than 2 leases at once; every token is n = 1 (blocked
/// entries never burned an attempt).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_cap_bounds_leases_and_burns_no_attempt() {
    let tmp = ostro_testkit::fs::tempdir();
    let inflight = Arc::new(AtomicUsize::new(0));
    let max = Arc::new(AtomicUsize::new(0));
    let fake = {
        let (inflight, max) = (Arc::clone(&inflight), Arc::clone(&max));
        Fake::new(move |_, _| {
            let (inflight, max) = (Arc::clone(&inflight), Arc::clone(&max));
            boxed(async move {
                let n = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                max.fetch_max(n, Ordering::SeqCst);
                sleep(Duration::from_millis(40)).await;
                inflight.fetch_sub(1, Ordering::SeqCst);
                ExecOutcome::Released
            })
        })
    };
    let flow = Arc::new(FakeFlow::new(|_, _, f| {
        if f.inflight.load(Ordering::SeqCst) >= 2 {
            Decision::QueueBlocked(None)
        } else {
            Decision::Yes
        }
    }));
    let mut o = Opts::new(&fake);
    o.flow = Some(Arc::clone(&flow));
    let node = open(tmp.path(), &o).await;
    let h = h(&node);
    let runs: Vec<RunId> = (0..6).map(|_| run_on(0)).collect();
    submit_all(h, runs.iter().map(|r| start_in(r, "q", 0, "")).collect()).await;
    let seen = fake.wait_jobs(6, T).await;
    assert!(
        max.load(Ordering::SeqCst) <= 2,
        "max {}",
        max.load(Ordering::SeqCst)
    );
    assert!(
        flow.calls().iter().any(|(_, t)| *t == "queue"),
        "the cap was hit"
    );
    let epoch = h.epoch();
    for s in &seen {
        assert_eq!(s.token, AttemptToken { epoch, n: 1 }, "no attempt burned");
    }
    for r in &runs {
        // A pipelined grant is applied shortly after its job started.
        wait_until(T, "grant applied", || attempt(h, r) == Some((epoch.0, 1))).await;
    }
    wait_until(T, "permits released", || {
        flow.released.load(Ordering::SeqCst) == 6
    })
    .await;
    node.shutdown().await.expect("shutdown");
}

/// A key blocked with `until`: other keys proceed, the blocked key is retried at `until`
/// (the dispatcher's own timer wakes it; no other event happens).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_cap_with_until_retries_at_until_and_others_proceed() {
    let tmp = ostro_testkit::fs::tempdir();
    let fake = Fake::releasing();
    let blocked_until = Arc::new(Mutex::new(None::<u64>));
    let flow = {
        let bu = Arc::clone(&blocked_until);
        Arc::new(FakeFlow::new(move |v, now, _| {
            if v.key != "hot" {
                return Decision::Yes;
            }
            let mut g = bu.lock().expect("lock");
            match *g {
                None => {
                    let until = now + 300;
                    *g = Some(until);
                    Decision::KeyBlocked(Some(until))
                }
                Some(u) if now < u => Decision::KeyBlocked(Some(u)),
                Some(_) => Decision::Yes,
            }
        }))
    };
    let mut o = Opts::new(&fake);
    o.flow = Some(Arc::clone(&flow));
    let node = open(tmp.path(), &o).await;
    let h = h(&node);
    let hot = run_on(0);
    let others: Vec<RunId> = (0..3).map(|_| run_on(0)).collect();
    let mut cmds = vec![start_in(&hot, "q", 5, "hot")];
    cmds.extend(
        others
            .iter()
            .enumerate()
            .map(|(i, r)| start_in(r, "q", 0, &format!("k{i}"))),
    );
    submit_all(h, cmds).await;
    let seen = fake.wait_jobs(3, T).await;
    assert!(seen.iter().all(|s| s.run != hot), "hot key held back");
    let seen = fake.wait_jobs(4, T).await;
    assert_eq!(seen[3].run, hot);
    let until = blocked_until.lock().expect("lock").expect("blocked once");
    assert!(now_ms() >= until, "dispatched before until");
    assert_eq!(seen[3].token.n, 1, "the block burned no attempt");
    node.shutdown().await.expect("shutdown");
}

/// Weights 3:1: of the first 400 dispatches with both queues backlogged, queue `a` gets
/// 300 (± 5 %).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn weights_three_to_one_share() {
    let tmp = ostro_testkit::fs::tempdir();
    let fake = Fake::releasing();
    let flow = Arc::new(FakeFlow::new(|_, _, _| Decision::Yes).with_weight("a", 3));
    let place = Arc::new(FakePlacement::new(0));
    let gauges = Arc::new(QueueGauges::new());
    let mut o = Opts::new(&fake);
    o.flow = Some(Arc::clone(&flow));
    o.placement = Some(Arc::clone(&place));
    o.gauges = Some(Arc::clone(&gauges));
    let node = open(tmp.path(), &o).await;
    let h = h(&node);
    for q in ["a", "b"] {
        for _ in 0..8 {
            let cmds: Vec<Command> = (0..40).map(|_| start_in(&run_on(0), q, 0, "")).collect();
            submit_all(h, cmds).await;
        }
    }
    wait_until(T, "640 ready", || {
        gauges.snapshot().values().map(|g| g.ready).sum::<u64>() == 640
    })
    .await;
    place.set_cap(1);
    let seen = fake.wait_jobs(400, Duration::from_secs(60)).await;
    let a = seen[..400].iter().filter(|s| s.queue == "a").count();
    assert!(
        (280..=320).contains(&a),
        "queue a got {a} of 400 dispatches (want 300 +- 5 %)"
    );
    node.shutdown().await.expect("shutdown");
}

/// Strict priority bands within a queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn priority_bands_are_served_highest_first() {
    let tmp = ostro_testkit::fs::tempdir();
    let fake = Fake::releasing();
    let place = Arc::new(FakePlacement::new(0));
    let gauges = Arc::new(QueueGauges::new());
    let mut o = Opts::new(&fake);
    o.placement = Some(Arc::clone(&place));
    o.gauges = Some(Arc::clone(&gauges));
    let node = open(tmp.path(), &o).await;
    let h = h(&node);
    let (lo, hi, mid) = (run_on(0), run_on(0), run_on(0));
    submit_all(
        h,
        vec![
            start_in(&lo, "q", 0, "a"),
            start_in(&hi, "q", 9, "b"),
            start_in(&mid, "q", 5, "c"),
        ],
    )
    .await;
    wait_until(T, "3 ready", || {
        gauges.snapshot().get("q").is_some_and(|g| g.ready == 3)
    })
    .await;
    place.set_cap(1);
    fake.wait_jobs(3, T).await;
    assert_eq!(fake.seen_runs(), vec![hi, mid, lo]);
    node.shutdown().await.expect("shutdown");
}

/// A `Paused` queue is never granted while other queues proceed, and dispatches after the
/// flow control's `changed()` bump.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_queue_is_held_and_resumes_on_changed() {
    let tmp = ostro_testkit::fs::tempdir();
    let fake = Fake::releasing();
    let paused = Arc::new(AtomicBool::new(true));
    let flow = {
        let paused = Arc::clone(&paused);
        Arc::new(FakeFlow::new(move |v, _, _| {
            if v.queue == "p" && paused.load(Ordering::SeqCst) {
                Decision::Paused
            } else {
                Decision::Yes
            }
        }))
    };
    let mut o = Opts::new(&fake);
    o.flow = Some(Arc::clone(&flow));
    let node = open(tmp.path(), &o).await;
    let h = h(&node);
    let held: Vec<RunId> = (0..2).map(|_| run_on(0)).collect();
    let free: Vec<RunId> = (0..3).map(|_| run_on(0)).collect();
    let mut cmds: Vec<Command> = held.iter().map(|r| start_in(r, "p", 0, "")).collect();
    cmds.extend(free.iter().map(|r| start_in(r, "q", 0, "")));
    submit_all(h, cmds).await;
    fake.wait_jobs(3, T).await;
    sleep(Duration::from_millis(300)).await;
    assert!(
        fake.seen().iter().all(|s| s.queue == "q"),
        "paused queue granted"
    );
    assert!(flow.calls().iter().any(|(_, t)| *t == "paused"));
    assert_eq!(attempt(h, &held[0]), None, "no attempt while paused");
    paused.store(false, Ordering::SeqCst);
    flow.bump();
    let seen = fake.wait_jobs(5, T).await;
    assert_eq!(seen.iter().filter(|s| s.queue == "p").count(), 2);
    for s in &seen {
        assert_eq!(s.token.n, 1);
    }
    node.shutdown().await.expect("shutdown");
}

/// A paused workflow on a shared queue holds back only its own entries (fix-0705): its
/// entries sit at the head of the queue (older `ready_at`, the unkeyed FIFO and a shared
/// concurrency key), yet the other workflow of the same queue keeps being granted. A held
/// entry is not probed again until a `changed()` bump (no spin while everything left is
/// paused), burns no attempt, and is granted n = 1 after the resume bump.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_workflow_on_shared_queue_does_not_block_others() {
    let tmp = ostro_testkit::fs::tempdir();
    let fake = Fake::releasing();
    let paused = Arc::new(AtomicBool::new(true));
    let flow = {
        let paused = Arc::clone(&paused);
        Arc::new(FakeFlow::new(move |v, _, _| {
            if v.workflow == "wp" && paused.load(Ordering::SeqCst) {
                Decision::PausedWorkflow
            } else {
                Decision::Yes
            }
        }))
    };
    let mut o = Opts::new(&fake);
    o.flow = Some(Arc::clone(&flow));
    let node = open(tmp.path(), &o).await;
    let h = h(&node);
    let start = |run: &RunId, wf: &str, key: &str, at: u64| {
        Command::start_run(StartRun {
            workflow: wf.into(),
            ready_at_ms: at,
            concurrency_key: key.into(),
            ..start_run(run)
        })
    };
    // Paused workflow first (older ready_at): the head of the unkeyed FIFO and of key "k".
    let held: Vec<(RunId, &str)> = vec![(run_on(0), ""), (run_on(0), ""), (run_on(0), "k")];
    submit_all(h, held.iter().map(|(r, k)| start(r, "wp", k, 50)).collect()).await;
    let free: Vec<(RunId, &str)> = vec![
        (run_on(0), ""),
        (run_on(0), ""),
        (run_on(0), ""),
        (run_on(0), "k"),
    ];
    submit_all(
        h,
        free.iter().map(|(r, k)| start(r, "wf", k, 100)).collect(),
    )
    .await;
    let seen = fake.wait_jobs(free.len(), T).await;
    let held_runs: Vec<RunId> = held.iter().map(|(r, _)| *r).collect();
    assert!(
        seen.iter()
            .all(|s| !held_runs.contains(&s.run) && s.queue == "q"),
        "paused workflow granted: {seen:?}"
    );
    let mut got: Vec<RunId> = seen.iter().map(|s| s.run).collect();
    got.sort();
    let mut want: Vec<RunId> = free.iter().map(|(r, _)| *r).collect();
    want.sort();
    assert_eq!(got, want, "every entry of the other workflow granted");
    for (r, _) in &held {
        assert_eq!(attempt(h, r), None, "no attempt while paused");
    }
    // No spin: once only held entries are left, nothing probes them until a bump.
    sleep(Duration::from_millis(300)).await;
    let probes = |f: &FakeFlow| {
        f.calls()
            .iter()
            .filter(|(_, t)| *t == "paused_workflow")
            .count()
    };
    let before = probes(&flow);
    assert!(before >= held.len(), "every held entry probed: {before}");
    sleep(Duration::from_millis(300)).await;
    assert_eq!(
        probes(&flow),
        before,
        "held entries re-probed without a bump"
    );
    assert_eq!(fake.seen().len(), free.len());
    // Resume: one bump releases them.
    paused.store(false, Ordering::SeqCst);
    flow.bump();
    let seen = fake.wait_jobs(free.len() + held.len(), T).await;
    for (r, _) in &held {
        assert!(
            seen.iter().any(|s| s.run == *r),
            "held run granted after resume"
        );
    }
    for s in &seen {
        assert_eq!(s.token.n, 1, "no attempt burned");
    }
    node.shutdown().await.expect("shutdown");
}

/// A queue blocked by flow control for a while (with `until`) burns no attempt: the token
/// finally granted is n = 1 and only one grant was logged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_entry_burns_no_attempt_number() {
    let tmp = ostro_testkit::fs::tempdir();
    let fake = Fake::releasing();
    let first_call = Arc::new(Mutex::new(None::<u64>));
    let flow = {
        let fc = Arc::clone(&first_call);
        Arc::new(FakeFlow::new(move |_, now, _| {
            let t0 = *fc.lock().expect("lock").get_or_insert(now);
            if now < t0 + 200 {
                Decision::QueueBlocked(Some(t0 + 200))
            } else {
                Decision::Yes
            }
        }))
    };
    let mut o = Opts::new(&fake);
    o.flow = Some(Arc::clone(&flow));
    let place = Arc::new(FakePlacement::new(8));
    o.placement = Some(Arc::clone(&place));
    let node = open(tmp.path(), &o).await;
    let h = h(&node);
    let run = run_on(0);
    submit_all(h, vec![start_in(&run, "q", 0, "")]).await;
    let seen = fake.wait_jobs(1, T).await;
    assert_eq!(seen[0].token.n, 1);
    assert!(flow.calls().iter().filter(|(_, t)| *t == "queue").count() >= 1);
    assert_eq!(
        place.reserves.load(Ordering::SeqCst),
        1,
        "placement asked only after admission"
    );
    sleep(Duration::from_millis(100)).await;
    assert_eq!(attempt(h, &run), Some((h.epoch().0, 1)));
    node.shutdown().await.expect("shutdown");
}

/// B2: placement blocks every entry of queue `w` (no worker credit); queue `e` keeps
/// dispatching all of its entries; `w` dispatches after `changed()`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn placement_blocked_queue_does_not_starve_embedded_queue() {
    let tmp = ostro_testkit::fs::tempdir();
    let fake = Fake::releasing();
    let place = Arc::new(FakePlacement::new(64));
    place.block_queue("w");
    let flow = Arc::new(FakeFlow::new(|_, _, _| Decision::Yes));
    let mut o = Opts::new(&fake);
    o.placement = Some(Arc::clone(&place));
    o.flow = Some(Arc::clone(&flow));
    let node = open(tmp.path(), &o).await;
    let h = h(&node);
    let ws: Vec<RunId> = (0..2).map(|_| run_on(0)).collect();
    let es: Vec<RunId> = (0..5).map(|_| run_on(0)).collect();
    // `w` sorts after `e`; interleave so both are always backlogged in the same rounds.
    let mut cmds = Vec::new();
    for i in 0..5 {
        cmds.push(start_in(&es[i], "e", 0, ""));
        if i < 2 {
            cmds.push(start_in(&ws[i], "w", 0, ""));
        }
    }
    submit_all(h, cmds).await;
    fake.wait_jobs(5, T).await;
    sleep(Duration::from_millis(200)).await;
    let seen = fake.seen();
    assert_eq!(seen.len(), 5);
    assert!(seen.iter().all(|s| s.queue == "e"));
    assert_eq!(
        flow.rolled_back.load(Ordering::SeqCst),
        flow.admitted.load(Ordering::SeqCst) - 5,
        "every admission of a placement-blocked entry was rolled back"
    );
    place.unblock_all();
    let seen = fake.wait_jobs(7, T).await;
    let w: Vec<&Seen> = seen.iter().filter(|s| s.queue == "w").collect();
    assert_eq!(w.len(), 2);
    assert!(
        w.iter()
            .all(|s| s.kind == ReservationKind::Worker && s.token.n == 1)
    );
    node.shutdown().await.expect("shutdown");
}

/// B1: a fake flow control that blocks every first activation still admits the executor's
/// requeue (EnqueueReady of a started run), the Retry re-grant and the lease-expiry
/// re-grant (all `first_activation == false` from F7).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reactivations_are_not_throttled() {
    let tmp = ostro_testkit::fs::tempdir();
    let block_first = Arc::new(AtomicBool::new(false));
    let fake = {
        let bf = Arc::clone(&block_first);
        Fake::new(move |mut job, idx| {
            let bf = Arc::clone(&bf);
            boxed(async move {
                match idx {
                    0 => {
                        bf.store(true, Ordering::SeqCst);
                        requeue(&job).await;
                        let _ = timeout(T, job.revoked.wait_for(|v| *v)).await;
                        ExecOutcome::Released
                    }
                    1 => ExecOutcome::Retry { after_ms: 50 },
                    2 => {
                        // Stall without renewing: the lease expires.
                        let _ = timeout(T, job.revoked.wait_for(|v| *v)).await;
                        ExecOutcome::Released
                    }
                    _ => ExecOutcome::Released,
                }
            })
        })
    };
    let flow = {
        let bf = Arc::clone(&block_first);
        Arc::new(FakeFlow::new(move |v, _, _| {
            if v.first && bf.load(Ordering::SeqCst) {
                Decision::KeyBlocked(None)
            } else {
                Decision::Yes
            }
        }))
    };
    let mut o = Opts::new(&fake);
    o.lease_ttl = Duration::from_millis(300);
    o.flow = Some(Arc::clone(&flow));
    let node = open(tmp.path(), &o).await;
    let h = h(&node);
    let old = run_on(0);
    submit_all(h, vec![start_in(&old, "q", 0, "old")]).await;
    fake.wait_jobs(1, T).await;
    let new = run_on(0);
    submit_all(h, vec![start_in(&new, "q", 0, "new")]).await;
    let seen = fake.wait_jobs(4, T).await;
    let old_tokens: Vec<u32> = seen
        .iter()
        .filter(|s| s.run == old)
        .map(|s| s.token.n)
        .collect();
    assert_eq!(old_tokens, vec![1, 2, 3, 4]);
    sleep(Duration::from_millis(200)).await;
    assert!(
        fake.seen().iter().all(|s| s.run == old),
        "first activation throttled"
    );
    let calls = flow.calls();
    let old_views: Vec<bool> = calls
        .iter()
        .filter(|(v, _)| v.key == "old")
        .map(|(v, _)| v.first)
        .collect();
    // Admission saw: the first activation, the requeue, the expiry re-grant (the Retry
    // re-grant keeps its lease and is not re-admitted).
    assert_eq!(old_views, vec![true, false, false]);
    assert!(
        calls
            .iter()
            .filter(|(v, _)| v.key == "new")
            .all(|(v, t)| v.first && *t == "key" && v.workflow == "wf" && v.priority == 0)
    );
    assert_eq!(o.stats.lease_expired.load(Ordering::Relaxed), 1);
    node.shutdown().await.expect("shutdown");
}

/// G1 (07-23): a throttled first activation sits ahead of a re-activation on the same
/// partition, queue and concurrency key (FIFO by `ready_at`); `key_scoped` uses a key
/// throttle (`KeyBlocked{Some}`) instead of a queue throttle (`QueueBlocked{Some}`). The
/// throttle's `until` is 10 minutes away. The re-activation must be granted as soon as it
/// is due, the first activation is never granted, and with no event the gated first
/// activation is not re-probed (no spin).
async fn throttled_first_activation_ahead_of_reactivation(key_scoped: bool) {
    const DELAY_MS: u64 = 500;
    let tmp = ostro_testkit::fs::tempdir();
    let throttle = Arc::new(AtomicBool::new(false));
    let due_at = Arc::new(Mutex::new(None::<u64>));
    let fake = {
        let (th, due) = (Arc::clone(&throttle), Arc::clone(&due_at));
        Fake::new(move |mut job, idx| {
            let (th, due) = (Arc::clone(&th), Arc::clone(&due));
            boxed(async move {
                if idx == 0 && job.record.workflow == "old" {
                    // The throttle starts now; the run sleeps on a durable timer.
                    th.store(true, Ordering::SeqCst);
                    let at = now_ms() + DELAY_MS;
                    *due.lock().expect("lock") = Some(at);
                    requeue_at(&job, at).await;
                    let _ = timeout(T, job.revoked.wait_for(|v| *v)).await;
                }
                ExecOutcome::Released
            })
        })
    };
    let flow = {
        let th = Arc::clone(&throttle);
        Arc::new(FakeFlow::new(move |v, now, _| {
            if v.first && th.load(Ordering::SeqCst) {
                let until = Some(now + 600_000);
                if key_scoped {
                    Decision::KeyBlocked(until)
                } else {
                    Decision::QueueBlocked(until)
                }
            } else {
                Decision::Yes
            }
        }))
    };
    let mut o = Opts::new(&fake);
    o.flow = Some(Arc::clone(&flow));
    let node = open(tmp.path(), &o).await;
    let h = h(&node);
    let start = |run: &RunId, wf: &str| {
        Command::start_run(StartRun {
            workflow: wf.into(),
            concurrency_key: "k".into(),
            ..start_run(run)
        })
    };
    let old = run_on(0);
    submit_all(h, vec![start(&old, "old")]).await;
    fake.wait_jobs(1, T).await;
    wait_until(T, "re-activation scheduled", || {
        due_at.lock().expect("lock").is_some()
    })
    .await;
    // A first activation ahead of the sleeper (ready_at 100 < its timer), same key.
    let new = run_on(0);
    submit_all(h, vec![start(&new, "new")]).await;
    let blocked = if key_scoped { "key" } else { "queue" };
    wait_until(T, "first activation throttled", || {
        flow.calls()
            .iter()
            .any(|(v, t)| v.workflow == "new" && *t == blocked)
    })
    .await;
    let due = due_at.lock().expect("lock").expect("due");
    assert!(
        now_ms() < due,
        "the first activation was throttled before the timer fired"
    );
    // The re-activation is granted once due, not at the throttle's `until`.
    let seen = fake.wait_jobs(2, Duration::from_secs(5)).await;
    let granted_at = now_ms();
    assert_eq!(seen[1].run, old, "re-activation granted: {seen:?}");
    assert_eq!(seen[1].token.n, 2);
    assert!(
        granted_at.saturating_sub(due) < 3_000,
        "re-activation granted {} ms after due",
        granted_at.saturating_sub(due)
    );
    // Admission saw the re-activation as such and admitted it.
    assert!(
        flow.calls()
            .iter()
            .any(|(v, t)| v.workflow == "old" && !v.first && *t == "yes")
    );
    // Quiet: the gated first activation is not re-probed without an event, never granted
    // and burns no attempt.
    sleep(Duration::from_millis(300)).await;
    let probes = |f: &FakeFlow| {
        f.calls()
            .iter()
            .filter(|(v, _)| v.workflow == "new")
            .count()
    };
    let before = probes(&flow);
    sleep(Duration::from_millis(400)).await;
    assert_eq!(probes(&flow), before, "gated first activation re-probed");
    assert!(before <= 4, "first activation probed {before} times");
    assert_eq!(fake.seen().len(), 2);
    assert_eq!(
        attempt(h, &new),
        None,
        "throttled first activation burned an attempt"
    );
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_throttle_on_first_activation_does_not_hold_reactivations() {
    throttled_first_activation_ahead_of_reactivation(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_throttle_on_first_activation_does_not_hold_reactivations() {
    throttled_first_activation_ahead_of_reactivation(true).await;
}

/// Lease expiry revokes the lease and releases both the flow permit and the reservation
/// before the re-grant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expiry_releases_permit_and_reservation() {
    let tmp = ostro_testkit::fs::tempdir();
    let place = Arc::new(FakePlacement::new(8));
    let flow = Arc::new(FakeFlow::new(|_, _, _| Decision::Yes));
    let at_second = Arc::new(Mutex::new(None::<(usize, usize, usize)>));
    let views = Arc::new(Mutex::new(Vec::<(usize, bool)>::new()));
    let fake = {
        let (place, flow, at, views) = (
            Arc::clone(&place),
            Arc::clone(&flow),
            Arc::clone(&at_second),
            Arc::clone(&views),
        );
        Fake::new(move |mut job, idx| {
            let (place, flow, at, views) = (
                Arc::clone(&place),
                Arc::clone(&flow),
                Arc::clone(&at),
                Arc::clone(&views),
            );
            boxed(async move {
                if idx == 0 {
                    let _ = timeout(T, job.revoked.wait_for(|v| *v)).await;
                    // The zombie's view no longer reaches the reservation.
                    let live = job.with_reservation::<Guard, ()>(|_| ()).is_some();
                    views.lock().expect("lock").push((idx, live));
                    sleep(Duration::from_millis(200)).await;
                } else {
                    let live = job.with_reservation::<Guard, ()>(|_| ()).is_some();
                    views.lock().expect("lock").push((idx, live));
                    *at.lock().expect("lock") = Some((
                        place.dropped.load(Ordering::SeqCst),
                        flow.released.load(Ordering::SeqCst),
                        place.live.load(Ordering::SeqCst),
                    ));
                }
                ExecOutcome::Released
            })
        })
    };
    let mut o = Opts::new(&fake);
    o.lease_ttl = Duration::from_millis(300);
    o.placement = Some(Arc::clone(&place));
    o.flow = Some(Arc::clone(&flow));
    let node = open(tmp.path(), &o).await;
    let h = h(&node);
    let run = run_on(0);
    submit_all(h, vec![start_in(&run, "q", 0, "")]).await;
    let seen = fake.wait_jobs(2, T).await;
    assert_eq!(seen[1].token.n, 2);
    wait_until(T, "second job ran", || {
        at_second.lock().expect("lock").is_some()
    })
    .await;
    let (dropped, released, live) = at_second.lock().expect("lock").expect("set");
    assert_eq!(
        (dropped, released, live),
        (1, 1, 1),
        "revoked zombie holds nothing"
    );
    wait_until(T, "zombie returned", || {
        views.lock().expect("lock").len() == 2
    })
    .await;
    let mut v = views.lock().expect("lock").clone();
    v.sort_unstable();
    assert_eq!(
        v,
        vec![(0, false), (1, true)],
        "reservation view of zombie / successor"
    );
    wait_until(T, "all released", || {
        place.dropped.load(Ordering::SeqCst) == 2 && flow.released.load(Ordering::SeqCst) == 2
    })
    .await;
    assert_eq!(place.live.load(Ordering::SeqCst), 0);
    assert_eq!(flow.inflight.load(Ordering::SeqCst), 0);
    node.shutdown().await.expect("shutdown");
}

/// `ExecOutcome::Abandoned` (worker lost) re-grants n + 1 at once and counts no failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoned_regrants_without_counting_a_failure() {
    let tmp = ostro_testkit::fs::tempdir();
    let fake = Fake::new(|_, idx| {
        boxed(async move {
            if idx == 0 {
                ExecOutcome::Abandoned
            } else {
                ExecOutcome::Released
            }
        })
    });
    let place = Arc::new(FakePlacement::new(8));
    let mut o = Opts::new(&fake);
    o.placement = Some(Arc::clone(&place));
    let node = open(tmp.path(), &o).await;
    let h = h(&node);
    let run = run_on(0);
    submit_all(h, vec![start_in(&run, "q", 0, "")]).await;
    let seen = fake.wait_jobs(2, T).await;
    let got: Vec<(u32, u32)> = seen.iter().map(|s| (s.token.n, s.failures)).collect();
    assert_eq!(got, vec![(1, 0), (2, 0)]);
    sleep(Duration::from_millis(100)).await;
    let rec = h.read_run(&run).expect("read").expect("run");
    assert_eq!(rec.attempt_failures, 0);
    assert_eq!(rec.current_attempt, Some((h.epoch().0, 2)));
    assert_eq!(fake.seen().len(), 2);
    wait_until(T, "reservations released", || {
        place.dropped.load(Ordering::SeqCst) == 2
    })
    .await;
    node.shutdown().await.expect("shutdown");
}

fn pause(run: &RunId) -> Command {
    Command::pause_run(PauseRun {
        run_id: run.as_bytes().to_vec(),
        requested_at_ms: now_ms(),
        ..Default::default()
    })
}

fn resume(run: &RunId) -> Command {
    Command::resume_run(ResumeRun {
        run_id: run.as_bytes().to_vec(),
        requested_at_ms: now_ms(),
    })
}

/// `Action::RunPaused` revokes a running job and drops a queued entry; resume queues the
/// run again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_paused_revokes_running_and_drops_queued() {
    let tmp = ostro_testkit::fs::tempdir();
    let revoked = Arc::new(AtomicUsize::new(0));
    let fake = {
        let revoked = Arc::clone(&revoked);
        Fake::new(move |mut job, _| {
            let revoked = Arc::clone(&revoked);
            boxed(async move {
                if timeout(T, job.revoked.wait_for(|v| *v)).await.is_ok() {
                    revoked.fetch_add(1, Ordering::SeqCst);
                }
                ExecOutcome::Released
            })
        })
    };
    let place = Arc::new(FakePlacement::new(1));
    let mut o = Opts::new(&fake);
    o.placement = Some(Arc::clone(&place));
    let node = open(tmp.path(), &o).await;
    let h = h(&node);
    let (a, b) = (run_on(0), run_on(0));
    submit_all(h, vec![start_in(&a, "q", 5, "")]).await;
    fake.wait_jobs(1, T).await;
    submit_all(h, vec![start_in(&b, "q", 0, "")]).await;
    sleep(Duration::from_millis(50)).await;
    // a holds the only reservation; b is queued.
    submit_all(h, vec![pause(&a), pause(&b)]).await;
    wait_until(T, "a revoked", || revoked.load(Ordering::SeqCst) == 1).await;
    sleep(Duration::from_millis(300)).await;
    assert_eq!(fake.seen_runs(), vec![a], "paused b never dispatched");
    assert_eq!(place.live.load(Ordering::SeqCst), 0);
    submit_all(h, vec![resume(&b)]).await;
    let seen = fake.wait_jobs(2, T).await;
    assert_eq!(seen[1].run, b);
    assert_eq!(seen[1].token.n, 1);
    node.shutdown().await.expect("shutdown");
}

/// Gauges: ready / ready_unblocked / oldest while placement holds everything, then running
/// embedded and on workers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gauges_reflect_ready_and_running_counts() {
    let tmp = ostro_testkit::fs::tempdir();
    let fake = Fake::holding_for(3_000);
    let place = Arc::new(FakePlacement::new(0));
    let flow = Arc::new(FakeFlow::new(|v, _, _| {
        if v.key == "capped" {
            Decision::KeyBlocked(None)
        } else {
            Decision::Yes
        }
    }));
    let gauges = Arc::new(QueueGauges::new());
    let mut o = Opts::new(&fake);
    o.placement = Some(Arc::clone(&place));
    o.flow = Some(Arc::clone(&flow));
    o.gauges = Some(Arc::clone(&gauges));
    let node = open(tmp.path(), &o).await;
    let h = h(&node);
    let mut cmds: Vec<Command> = (0..3).map(|_| start_in(&run_on(0), "e", 0, "")).collect();
    // Higher band: admission sees (and parks) the capped key before placement parks `e`.
    cmds.push(start_in(&run_on(0), "e", 5, "capped"));
    cmds.extend((0..2).map(|_| start_in(&run_on(0), "w1", 0, "")));
    submit_all(h, cmds).await;
    wait_until(T, "ready gauges", || {
        let s = gauges.snapshot();
        s.get("e").is_some_and(|g| g.ready == 4) && s.get("w1").is_some_and(|g| g.ready == 2)
    })
    .await;
    let s = gauges.snapshot();
    assert_eq!(s["e"].ready_unblocked, 3, "the capped key is blocked");
    assert_eq!(
        s["w1"].ready_unblocked, 2,
        "placement-blocked work is admissible"
    );
    assert_eq!(s["e"].oldest_ready_at_ms, Some(100));
    assert_eq!((s["e"].running_embedded, s["e"].running_on_workers), (0, 0));
    place.set_cap(4);
    fake.wait_jobs(4, T).await;
    wait_until(T, "running gauges", || {
        let s = gauges.snapshot();
        let e = s.get("e").copied().unwrap_or_default();
        let w = s.get("w1").copied().unwrap_or_default();
        e.running_embedded + w.running_on_workers == 4
            && e.ready + w.ready == 2
            && w.running_embedded == 0
            && e.running_on_workers == 0
    })
    .await;
    node.shutdown().await.expect("shutdown");
    assert!(gauges.snapshot().is_empty(), "rows removed on shutdown");
}
