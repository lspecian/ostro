//! Dispatcher leader service (plan 04-14: DISP-01, DISP-02, DISP-07) driven by a fake
//! `Executor`: rebuild on leadership, action-driven wake-ups (no polling), concurrency key
//! carried end to end, slot bound, outcome handling, soft leases (renew, expiry -> revoke +
//! re-grant, stale outcomes ignored) and shutdown revocation. Plan 05-11: hard stops
//! (Terminated / TimedOut) revoke the lease of a running job, retries are counted durably
//! (`AttemptGranted{after_failure}` at once, job after the delay), and a zero-permit slot
//! semaphore holds runs un-granted. Plan 05-16: pipelined grants (the job starts before the
//! grant's ack) never overlap for one run, a rejected grant (injected) is fenced by apply and
//! re-granted with a higher token, and every executor append lands after its grant.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{OPEN_T, T, run_on, start_run};
use ostro_log::ReplicatedLog;
use ostro_partition::store::values::RunStatus;
use ostro_partition::{
    ApplyOutcome, Clock, DispatcherConfig, DispatcherStats, ExecOutcome, Executor, Job, Node,
    NodeConfig, PartitionHandle, SystemClock,
};
use ostro_types::command::{
    AttemptTokenMsg, Cmd, CompleteRun, Derived, EnqueueReady, EventsAppended, JournalEvent,
    StartRun, TerminalStatus, TerminateRun, decode_command, derived,
};
use ostro_types::{AttemptToken, Command, PartitionCount, PartitionId, RunId};
use tokio::sync::Semaphore;
use tokio::time::{Instant, sleep, timeout};

const P0: PartitionId = PartitionId(0);

type Handle = PartitionHandle<ostro_log::Rf1Log>;
type ExecFuture = Pin<Box<dyn Future<Output = ExecOutcome> + Send + 'static>>;
type Script = dyn Fn(Job, usize) -> ExecFuture + Send + Sync;

/// What the fake executor saw of one job.
#[derive(Clone, Debug)]
struct Seen {
    run: RunId,
    token: AttemptToken,
    failures: u32,
    concurrency_key: String,
    at: Instant,
}

/// Records every job and runs `script(job, call index of this run)`.
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

    /// An executor that releases every job at once.
    fn releasing() -> Arc<Self> {
        Self::new(|_, _| boxed(async { ExecOutcome::Released }))
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("lock").clone()
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
            concurrency_key: job.record.concurrency_key.clone(),
            at: Instant::now(),
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

fn now_ms() -> u64 {
    SystemClock.now_ms()
}

struct Opts {
    fake: Arc<Fake>,
    lease_ttl: Duration,
    slots: usize,
    /// Explicit slot semaphore (overrides `slots`; the test keeps a clone).
    sem: Option<Arc<Semaphore>>,
    stats: Arc<DispatcherStats>,
}

impl Opts {
    fn new(fake: &Arc<Fake>) -> Self {
        Self {
            fake: Arc::clone(fake),
            lease_ttl: Duration::from_secs(30),
            slots: 64,
            sem: None,
            stats: Arc::new(DispatcherStats::default()),
        }
    }
}

/// Open a single-partition node; with `opts`, the dispatcher runs.
async fn open(dir: &Path, opts: Option<&Opts>) -> Node {
    let mut cfg = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(1).expect("non-zero"));
    if let Some(o) = opts {
        let executor: Arc<dyn Executor> = o.fake.clone();
        cfg.leader.dispatcher = Some(DispatcherConfig {
            executor,
            lease_ttl: o.lease_ttl,
            slots: o
                .sem
                .clone()
                .unwrap_or_else(|| Arc::new(Semaphore::new(o.slots))),
            stats: Some(Arc::clone(&o.stats)),
        });
    }
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

fn start(run: &RunId, priority: i32, ck: &str) -> Command {
    Command::start_run(StartRun {
        priority,
        concurrency_key: ck.into(),
        ..start_run(run)
    })
}

/// Requeue `job.run` (EventsAppended with an `EnqueueReady` intent) at `ready_at_ms`.
async fn requeue(job: &Job, ready_at_ms: u64) {
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

/// Poll until the run's `current_attempt` is `want` (a pipelined grant is applied shortly
/// after its job started, plan 05-16).
async fn wait_attempt(h: &Handle, run: &RunId, want: (u32, u32)) {
    let deadline = Instant::now() + T;
    while attempt(h, run) != Some(want) {
        assert!(
            Instant::now() < deadline,
            "attempt {:?}, want {want:?}",
            attempt(h, run)
        );
        sleep(Duration::from_millis(5)).await;
    }
}

fn status(h: &Handle, run: &RunId) -> RunStatus {
    h.read_run(run).expect("read").expect("run").status
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatches_by_priority_with_first_token_and_concurrency_key() {
    let tmp = ostro_testkit::fs::tempdir();
    let fake = Fake::releasing();
    let mut o = Opts::new(&fake);
    o.slots = 1;
    let node = open(tmp.path(), Some(&o)).await;
    let h = h(&node);
    let (a, b, c) = (run_on(0), run_on(0), run_on(0));
    // One envelope: the three runs arrive in one action batch.
    submit_all(
        h,
        vec![start(&a, 0, "ck-a"), start(&b, 5, "ck-b"), start(&c, 0, "")],
    )
    .await;
    let seen = fake.wait_jobs(3, T).await;
    let mut same_priority = [a, c];
    same_priority.sort();
    let order: Vec<RunId> = seen.iter().map(|s| s.run).collect();
    assert_eq!(order, vec![b, same_priority[0], same_priority[1]]);
    let epoch = h.epoch();
    for s in &seen {
        assert_eq!(s.token, AttemptToken { epoch, n: 1 });
        assert_eq!(s.failures, 0);
    }
    let ck: BTreeMap<RunId, String> = seen
        .iter()
        .map(|s| (s.run, s.concurrency_key.clone()))
        .collect();
    assert_eq!(ck[&a], "ck-a");
    assert_eq!(ck[&b], "ck-b");
    assert_eq!(ck[&c], "");
    assert_eq!(o.stats.dispatched.load(Ordering::Relaxed), 3);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn future_ready_at_is_not_dispatched_early() {
    let tmp = ostro_testkit::fs::tempdir();
    let fake = Fake::releasing();
    let o = Opts::new(&fake);
    let node = open(tmp.path(), Some(&o)).await;
    let h = h(&node);
    let run = run_on(0);
    let now = now_ms();
    let ready_at = now + 400;
    submit_all(
        h,
        vec![Command::start_run(StartRun {
            requested_at_ms: now,
            ready_at_ms: ready_at,
            ..start_run(&run)
        })],
    )
    .await;
    sleep(Duration::from_millis(200)).await;
    assert!(fake.seen().is_empty(), "dispatched before ready_at");
    fake.wait_jobs(1, T).await;
    assert!(now_ms() >= ready_at);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn executor_requeue_with_future_ready_at_waits_in_the_delay_set() {
    let tmp = ostro_testkit::fs::tempdir();
    let fake = Fake::new(|job, idx| {
        boxed(async move {
            if idx == 0 {
                requeue(&job, now_ms() + 300).await;
            }
            ExecOutcome::Released
        })
    });
    let o = Opts::new(&fake);
    let node = open(tmp.path(), Some(&o)).await;
    let h = h(&node);
    let run = run_on(0);
    submit_all(h, vec![start(&run, 0, "")]).await;
    let seen = fake.wait_jobs(2, T).await;
    let gap = seen[1].at.duration_since(seen[0].at);
    assert!(
        gap >= Duration::from_millis(280),
        "re-dispatched after {gap:?}"
    );
    assert_eq!(seen[1].token.n, 2);
    sleep(Duration::from_millis(200)).await;
    assert_eq!(fake.seen().len(), 2);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebuild_after_restart_regrants_running_under_the_new_epoch_without_polling() {
    let tmp = ostro_testkit::fs::tempdir();
    let ready: Vec<RunId> = (0..2).map(|_| run_on(0)).collect();
    let running: Vec<RunId> = (0..2).map(|_| run_on(0)).collect();
    let old_epoch = {
        let node = open(tmp.path(), None).await;
        let h = h(&node);
        let mut cmds = Vec::new();
        for (i, r) in ready.iter().chain(&running).enumerate() {
            cmds.push(start(r, 0, &format!("ck-{i}")));
        }
        submit_all(h, cmds).await;
        for r in &running {
            h.grant_attempt(*r).await.expect("grant");
        }
        let e = h.epoch();
        node.shutdown().await.expect("shutdown");
        e
    };

    let fake = Fake::releasing();
    let o = Opts::new(&fake);
    let node = open(tmp.path(), Some(&o)).await;
    let h = h(&node);
    let epoch = h.epoch();
    assert!(epoch > old_epoch);
    let seen = fake.wait_jobs(4, T).await;
    let by_run: BTreeMap<RunId, Seen> = seen.iter().map(|s| (s.run, s.clone())).collect();
    for (i, r) in ready.iter().chain(&running).enumerate() {
        let s = by_run.get(r).expect("dispatched after restart");
        assert_eq!(s.token, AttemptToken { epoch, n: 1 }, "run {i}");
        assert_eq!(s.concurrency_key, format!("ck-{i}"), "run {i}");
        wait_attempt(h, r, (epoch.0, 1)).await;
    }

    // Idle: nothing scans on an interval.
    sleep(Duration::from_millis(500)).await;
    assert_eq!(o.stats.rescans.load(Ordering::Relaxed), 0);
    assert_eq!(o.stats.dispatched.load(Ordering::Relaxed), 4);
    assert_eq!(fake.seen().len(), 4);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slots_bound_concurrent_jobs() {
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
                sleep(Duration::from_millis(50)).await;
                inflight.fetch_sub(1, Ordering::SeqCst);
                ExecOutcome::Released
            })
        })
    };
    let mut o = Opts::new(&fake);
    o.slots = 1;
    let node = open(tmp.path(), Some(&o)).await;
    let h = h(&node);
    let runs: Vec<RunId> = (0..4).map(|_| run_on(0)).collect();
    submit_all(h, runs.iter().map(|r| start(r, 0, "")).collect()).await;
    fake.wait_jobs(4, T).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(max.load(Ordering::SeqCst), 1);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reactivate_reuses_the_token() {
    let tmp = ostro_testkit::fs::tempdir();
    let fake = Fake::new(|_, idx| {
        boxed(async move {
            if idx == 0 {
                ExecOutcome::Reactivate
            } else {
                ExecOutcome::Released
            }
        })
    });
    let o = Opts::new(&fake);
    let node = open(tmp.path(), Some(&o)).await;
    let h = h(&node);
    let run = run_on(0);
    submit_all(h, vec![start(&run, 0, "")]).await;
    let seen = fake.wait_jobs(2, T).await;
    assert_eq!(seen[0].token, seen[1].token);
    assert_eq!(seen[1].token.n, 1);
    sleep(Duration::from_millis(200)).await;
    assert_eq!(fake.seen().len(), 2);
    assert_eq!(o.stats.reactivated.load(Ordering::Relaxed), 1);
    assert_eq!(o.stats.dispatched.load(Ordering::Relaxed), 1);
    assert_eq!(attempt(h, &run), Some((h.epoch().0, 1)));
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_grants_a_new_attempt_after_the_delay() {
    let tmp = ostro_testkit::fs::tempdir();
    let fake = Fake::new(|_, idx| {
        boxed(async move {
            match idx {
                0 | 1 => ExecOutcome::Retry { after_ms: 200 },
                _ => ExecOutcome::Released,
            }
        })
    });
    let o = Opts::new(&fake);
    let node = open(tmp.path(), Some(&o)).await;
    let h = h(&node);
    let run = run_on(0);
    submit_all(h, vec![start(&run, 0, "")]).await;
    // F9: the re-grant is logged (and counted) right after the failure, before the delay.
    fake.wait_jobs(1, T).await;
    let deadline = Instant::now() + T;
    loop {
        let rec = h.read_run(&run).expect("read").expect("run");
        if rec.attempt_failures == 1 {
            assert_eq!(rec.current_attempt, Some((h.epoch().0, 2)));
            break;
        }
        assert!(Instant::now() < deadline, "after_failure grant not seen");
        sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(fake.seen().len(), 1, "the job waits for the retry delay");
    let seen = fake.wait_jobs(3, T).await;
    let ns: Vec<(u32, u32)> = seen.iter().map(|s| (s.token.n, s.failures)).collect();
    assert_eq!(ns, vec![(1, 0), (2, 1), (3, 2)]);
    for w in seen.windows(2) {
        let gap = w[1].at.duration_since(w[0].at);
        assert!(gap >= Duration::from_millis(190), "retried after {gap:?}");
    }
    assert_eq!(attempt(h, &run), Some((h.epoch().0, 3)));
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fenced_drops_the_lease() {
    let tmp = ostro_testkit::fs::tempdir();
    let fake = Fake::new(|_, _| boxed(async { ExecOutcome::Fenced }));
    let o = Opts::new(&fake);
    let node = open(tmp.path(), Some(&o)).await;
    let h = h(&node);
    let run = run_on(0);
    submit_all(h, vec![start(&run, 0, "")]).await;
    fake.wait_jobs(1, T).await;
    sleep(Duration::from_millis(400)).await;
    assert_eq!(fake.seen().len(), 1);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enqueue_ready_for_a_leased_run_drops_the_lease_and_requeues() {
    let tmp = ostro_testkit::fs::tempdir();
    let first_revoked = Arc::new(AtomicBool::new(false));
    let fake = {
        let flag = Arc::clone(&first_revoked);
        Fake::new(move |mut job, idx| {
            let flag = Arc::clone(&flag);
            boxed(async move {
                if idx == 0 {
                    requeue(&job, 0).await;
                    // Keep running: the dispatcher must revoke us.
                    let revoked =
                        matches!(timeout(T, job.revoked.wait_for(|v| *v)).await, Ok(Ok(_)));
                    flag.store(revoked, Ordering::SeqCst);
                }
                ExecOutcome::Released
            })
        })
    };
    let o = Opts::new(&fake);
    let node = open(tmp.path(), Some(&o)).await;
    let h = h(&node);
    let run = run_on(0);
    submit_all(h, vec![start(&run, 0, "")]).await;
    let seen = fake.wait_jobs(2, T).await;
    assert_eq!(seen[1].token.n, 2);
    sleep(Duration::from_millis(100)).await;
    assert!(first_revoked.load(Ordering::SeqCst));
    assert_eq!(fake.seen().len(), 2);
    assert_eq!(status(h, &run), RunStatus::Running);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn renewing_executor_keeps_its_lease() {
    let tmp = ostro_testkit::fs::tempdir();
    let renew_ok = Arc::new(AtomicBool::new(true));
    let fake = {
        let ok = Arc::clone(&renew_ok);
        Fake::new(move |job, _| {
            let ok = Arc::clone(&ok);
            boxed(async move {
                // 3 x lease_ttl, renewing every lease_ttl / 3.
                for _ in 0..9 {
                    sleep(Duration::from_millis(100)).await;
                    if !job.lease.renew() || *job.revoked.borrow() {
                        ok.store(false, Ordering::SeqCst);
                    }
                }
                ExecOutcome::Released
            })
        })
    };
    let mut o = Opts::new(&fake);
    o.lease_ttl = Duration::from_millis(300);
    let node = open(tmp.path(), Some(&o)).await;
    let h = h(&node);
    let run = run_on(0);
    submit_all(h, vec![start(&run, 0, "")]).await;
    fake.wait_jobs(1, T).await;
    sleep(Duration::from_millis(1_200)).await;
    assert!(renew_ok.load(Ordering::SeqCst), "lease lost while renewing");
    assert_eq!(o.stats.lease_expired.load(Ordering::Relaxed), 0);
    assert_eq!(fake.seen().len(), 1);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_executor_is_revoked_regranted_and_its_late_outcome_ignored() {
    let tmp = ostro_testkit::fs::tempdir();
    let revoked_after_ms = Arc::new(AtomicU64::new(0));
    let renew_after_revoke = Arc::new(AtomicBool::new(true));
    let second_ok = Arc::new(AtomicBool::new(false));
    let fake = {
        let (after, renew, ok) = (
            Arc::clone(&revoked_after_ms),
            Arc::clone(&renew_after_revoke),
            Arc::clone(&second_ok),
        );
        Fake::new(move |mut job, idx| {
            let (after, renew, ok) = (Arc::clone(&after), Arc::clone(&renew), Arc::clone(&ok));
            boxed(async move {
                if idx == 0 {
                    // Stall: never renew; wait for the revocation, then return late.
                    let t0 = Instant::now();
                    let revoked =
                        matches!(timeout(T, job.revoked.wait_for(|v| *v)).await, Ok(Ok(_)));
                    assert!(revoked, "never revoked");
                    after.store(t0.elapsed().as_millis() as u64, Ordering::SeqCst);
                    renew.store(job.lease.renew(), Ordering::SeqCst);
                    sleep(Duration::from_millis(300)).await;
                    ExecOutcome::Released // stale: must be ignored
                } else {
                    // The successor renews through the zombie's late return.
                    let mut good = true;
                    for _ in 0..8 {
                        sleep(Duration::from_millis(100)).await;
                        good &= job.lease.renew() && !*job.revoked.borrow();
                    }
                    ok.store(good, Ordering::SeqCst);
                    ExecOutcome::Released
                }
            })
        })
    };
    let mut o = Opts::new(&fake);
    o.lease_ttl = Duration::from_millis(300);
    let node = open(tmp.path(), Some(&o)).await;
    let h = h(&node);
    let run = run_on(0);
    submit_all(h, vec![start(&run, 0, "")]).await;
    let seen = fake.wait_jobs(2, T).await;
    assert_eq!(seen[0].token.n, 1);
    assert_eq!(seen[1].token.n, 2);
    // Logged AttemptGranted n = 2.
    wait_attempt(h, &run, (h.epoch().0, 2)).await;
    let after = revoked_after_ms.load(Ordering::SeqCst);
    assert!(
        (250..2_000).contains(&after),
        "revoked after {after} ms (ttl 300)"
    );
    assert!(!renew_after_revoke.load(Ordering::SeqCst));
    assert_eq!(o.stats.lease_expired.load(Ordering::Relaxed), 1);

    // Let the zombie return (≈ +300 ms) and the successor finish (≈ +800 ms).
    sleep(Duration::from_millis(1_200)).await;
    assert!(
        second_ok.load(Ordering::SeqCst),
        "successor's lease affected"
    );
    assert_eq!(o.stats.lease_expired.load(Ordering::Relaxed), 1);
    assert_eq!(fake.seen().len(), 2);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_revokes_every_lease_and_waits_for_jobs() {
    let tmp = ostro_testkit::fs::tempdir();
    let stopped = Arc::new(AtomicUsize::new(0));
    let fake = {
        let stopped = Arc::clone(&stopped);
        Fake::new(move |mut job, _| {
            let stopped = Arc::clone(&stopped);
            boxed(async move {
                if timeout(T, job.revoked.wait_for(|v| *v)).await.is_ok() {
                    sleep(Duration::from_millis(50)).await;
                    stopped.fetch_add(1, Ordering::SeqCst);
                }
                ExecOutcome::Released
            })
        })
    };
    let o = Opts::new(&fake);
    let node = open(tmp.path(), Some(&o)).await;
    let h = h(&node);
    let runs: Vec<RunId> = (0..3).map(|_| run_on(0)).collect();
    submit_all(h, runs.iter().map(|r| start(r, 0, "")).collect()).await;
    fake.wait_jobs(3, T).await;
    let t0 = Instant::now();
    timeout(Duration::from_secs(4), node.shutdown())
        .await
        .expect("shutdown timed out")
        .expect("shutdown");
    assert!(t0.elapsed() < Duration::from_secs(4));
    assert_eq!(
        stopped.load(Ordering::SeqCst),
        3,
        "jobs not revoked/awaited"
    );
}

/// An executor that holds every job until its lease is revoked, recording when.
fn holding(revoked_at: Arc<Mutex<Vec<(RunId, Instant)>>>) -> Arc<Fake> {
    Fake::new(move |job, _| {
        let revoked_at = Arc::clone(&revoked_at);
        boxed(async move {
            let mut r = job.revoked.clone();
            let _ = r.wait_for(|v| *v).await;
            revoked_at
                .lock()
                .expect("lock")
                .push((job.run, Instant::now()));
            ExecOutcome::Released
        })
    })
}

async fn wait_revoked(revoked_at: &Mutex<Vec<(RunId, Instant)>>, run: &RunId) -> Instant {
    let deadline = Instant::now() + T;
    loop {
        if let Some((_, at)) = revoked_at
            .lock()
            .expect("lock")
            .iter()
            .find(|(r, _)| r == run)
        {
            return *at;
        }
        assert!(Instant::now() < deadline, "lease of {run} never revoked");
        sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_revokes_the_running_jobs_lease() {
    let tmp = ostro_testkit::fs::tempdir();
    let revoked_at = Arc::new(Mutex::new(Vec::new()));
    let fake = holding(Arc::clone(&revoked_at));
    let o = Opts::new(&fake);
    let node = open(tmp.path(), Some(&o)).await;
    let h = h(&node);
    let run = run_on(0);
    submit_all(h, vec![start(&run, 0, "")]).await;
    fake.wait_jobs(1, T).await;
    let t0 = Instant::now();
    submit_all(
        h,
        vec![Command::terminate_run(TerminateRun {
            run_id: run.as_bytes().to_vec(),
            reason: "operator".into(),
            terminated_at_ms: now_ms(),
        })],
    )
    .await;
    let at = wait_revoked(&revoked_at, &run).await;
    assert!(at.duration_since(t0) < Duration::from_secs(1));
    assert_eq!(status(h, &run), RunStatus::Terminated);
    sleep(Duration::from_millis(200)).await;
    assert_eq!(fake.seen().len(), 1, "a terminated run is not re-granted");
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_timeout_revokes_the_running_jobs_lease() {
    let tmp = ostro_testkit::fs::tempdir();
    let revoked_at = Arc::new(Mutex::new(Vec::new()));
    let fake = holding(Arc::clone(&revoked_at));
    let o = Opts::new(&fake);
    let node = open(tmp.path(), Some(&o)).await;
    let h = h(&node);
    let run = run_on(0);
    let now = now_ms();
    submit_all(
        h,
        vec![Command::start_run(StartRun {
            ready_at_ms: now,
            requested_at_ms: now,
            run_timeout_ms: 300,
            ..start_run(&run)
        })],
    )
    .await;
    fake.wait_jobs(1, T).await;
    let at = wait_revoked(&revoked_at, &run).await;
    let rec = h.read_run(&run).expect("read").expect("run");
    assert_eq!(rec.status, RunStatus::TimedOut);
    assert_eq!(rec.terminal_reason, "run_timeout");
    assert!(at.elapsed() < Duration::from_secs(5));
    sleep(Duration::from_millis(200)).await;
    assert_eq!(fake.seen().len(), 1, "a timed-out run is not re-granted");
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_slots_hold_ready_runs_ungranted() {
    let tmp = ostro_testkit::fs::tempdir();
    let fake = Fake::releasing();
    let mut o = Opts::new(&fake);
    let sem = Arc::new(Semaphore::new(0));
    o.sem = Some(Arc::clone(&sem));
    let node = open(tmp.path(), Some(&o)).await;
    let h = h(&node);
    let run = run_on(0);
    submit_all(h, vec![start(&run, 0, "")]).await;
    sleep(Duration::from_millis(300)).await;
    assert!(fake.seen().is_empty(), "no slot: nothing is dispatched");
    assert_eq!(status(h, &run), RunStatus::Ready);
    assert_eq!(attempt(h, &run), None, "never granted");
    // Capacity arrives: the run is dispatched without any other wake-up.
    sem.add_permits(1);
    let seen = fake.wait_jobs(1, T).await;
    assert_eq!(seen[0].token.n, 1);
    node.shutdown().await.expect("shutdown");
}

// ---------------------------------------------------------------------------------
// Plan 05-16: pipelined grants
// ---------------------------------------------------------------------------------

/// One event plus `CompleteRun{Completed}` at the record's journal end, with the job's token.
fn complete_cmd(job: &Job) -> Command {
    let seq = job.record.next_seq;
    Command::events_appended(EventsAppended {
        run_id: job.run.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg {
            epoch: job.token.epoch.0,
            n: job.token.n,
        }),
        from_seq: seq,
        events: vec![JournalEvent {
            seq,
            kind: 16,
            payload: Some(ostro_types::PayloadRef::inline(bytes::Bytes::from_static(
                b"e",
            ))),
        }],
        derived: vec![Derived::new(derived::Intent::CompleteRun(CompleteRun {
            status: TerminalStatus::Completed as i32,
            completed_at_ms: 1,
            ..Default::default()
        }))],
    })
}

/// `(epoch, n)` of a token as logged.
type Tok = (u32, u32);
/// Per run: `(offset, token)` of every logged AttemptGranted.
type GrantLog = BTreeMap<RunId, Vec<(u64, Tok)>>;
/// What a job saw: `(record.current_attempt, record.status, job token)`.
type JobView = (Option<Tok>, RunStatus, AttemptToken);

/// Every command in the partition log: `(offset, command)`.
async fn log_commands(h: &Handle) -> Vec<(u64, Command)> {
    let log = h.log();
    let end = log.log_end();
    let mut from = log.log_start();
    let mut out = Vec::new();
    while from < end {
        let batches = log.read(from, end, 4 << 20).await.expect("log read");
        for b in &batches {
            for (i, c) in b.commands.iter().enumerate() {
                out.push((b.base.0 + i as u64, decode_command(c).expect("decode")));
            }
            from = b.end();
        }
    }
    out
}

/// 1,000 runs through pipelined grants with ~1/3 of first grants rejected by apply (stale
/// epoch, test hook): every run completes exactly once, no token is granted twice for a run,
/// tokens only grow, and each token's first executor append is at a higher offset than its
/// grant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipelined_grants_never_overlap_and_rejections_are_fenced() {
    const RUNS: usize = 1_000;
    let tmp = ostro_testkit::fs::tempdir();
    let completed: Arc<Mutex<BTreeMap<RunId, usize>>> = Arc::default();
    let live: Arc<Mutex<BTreeMap<(RunId, AttemptToken), usize>>> = Arc::default();
    let overlaps = Arc::new(AtomicUsize::new(0));
    let (c2, l2, o2) = (
        Arc::clone(&completed),
        Arc::clone(&live),
        Arc::clone(&overlaps),
    );
    let fake = Fake::new(move |job, _| {
        let (completed, live, overlaps) = (Arc::clone(&c2), Arc::clone(&l2), Arc::clone(&o2));
        boxed(async move {
            let key = (job.run, job.token);
            {
                let mut l = live.lock().expect("lock");
                let e = l.entry(key).or_default();
                *e += 1;
                if *e > 1 {
                    overlaps.fetch_add(1, Ordering::Relaxed);
                }
            }
            // Give a rejected grant's ack time to arrive while the job is live.
            sleep(Duration::from_millis(u64::from(job.run.as_bytes()[17] % 3))).await;
            let out = job.handle.submit_one(complete_cmd(&job)).await;
            *live.lock().expect("lock").get_mut(&key).expect("live") -= 1;
            match out {
                Ok(ApplyOutcome::Accepted) => {
                    *completed.lock().expect("lock").entry(job.run).or_default() += 1;
                    ExecOutcome::Released
                }
                Ok(ApplyOutcome::Rejected(ostro_partition::RejectReason::RunTerminal)) => {
                    ExecOutcome::Released
                }
                Ok(ApplyOutcome::Rejected(
                    ostro_partition::RejectReason::Fenced { .. }
                    | ostro_partition::RejectReason::StaleEpoch { .. },
                )) => ExecOutcome::Fenced,
                Ok(ApplyOutcome::Rejected(ostro_partition::RejectReason::SeqMismatch {
                    ..
                })) => ExecOutcome::Reactivate,
                other => panic!("unexpected append outcome {other:?}"),
            }
        })
    });
    let o = Opts::new(&fake);
    // Reject the first grant of about a third of the runs.
    let faulted: Arc<Mutex<std::collections::BTreeSet<RunId>>> = Arc::default();
    let f2 = Arc::clone(&faulted);
    o.stats.grant_fault.set(Some(Arc::new(move |r: &RunId| {
        let h = r
            .as_bytes()
            .iter()
            .fold(0u32, |a, b| a.wrapping_mul(31) ^ u32::from(*b));
        h % 3 == 0 && f2.lock().expect("lock").insert(*r)
    })));
    let node = open(tmp.path(), Some(&o)).await;
    let h = h(&node);
    let runs: Vec<RunId> = (0..RUNS).map(|_| run_on(0)).collect();
    for chunk in runs.chunks(100) {
        submit_all(h, chunk.iter().map(|r| start(r, 0, "")).collect()).await;
    }
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let done = runs
            .iter()
            .filter(|r| status(h, r) == RunStatus::Completed)
            .count();
        if done == RUNS {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "only {done}/{RUNS} runs completed"
        );
        sleep(Duration::from_millis(50)).await;
    }
    let n_faulted = faulted.lock().expect("lock").len();
    assert!(n_faulted > 100, "the hook rejected {n_faulted} grants");
    assert!(
        o.stats.grants_rejected.load(Ordering::Relaxed) >= n_faulted as u64,
        "every injected rejection is seen by the ack check"
    );
    let completed = completed.lock().expect("lock").clone();
    assert_eq!(completed.len(), RUNS);
    assert!(
        completed.values().all(|n| *n == 1),
        "every run completes exactly once"
    );
    assert_eq!(
        overlaps.load(Ordering::Relaxed),
        0,
        "one live job per token"
    );

    // The log: per run, granted tokens strictly increase (no token twice), and the first
    // append of every token is after a grant of that token.
    let epoch = h.epoch().0;
    let mut grants: GrantLog = BTreeMap::new();
    let mut first_append: BTreeMap<(RunId, (u32, u32)), u64> = BTreeMap::new();
    for (off, c) in log_commands(h).await {
        match c.cmd {
            Some(Cmd::AttemptGranted(g)) => {
                let r = RunId::from_bytes(&g.run_id).expect("run id");
                let t = g.token.expect("token");
                grants.entry(r).or_default().push((off, (t.epoch, t.n)));
            }
            Some(Cmd::EventsAppended(e)) => {
                let r = RunId::from_bytes(&e.run_id).expect("run id");
                let t = e.token.expect("token");
                first_append.entry((r, (t.epoch, t.n))).or_insert(off);
            }
            _ => {}
        }
    }
    let mut regranted = 0;
    for r in &runs {
        let g = &grants[r];
        // A rejected (stale-epoch) grant carries epoch - 1; its job ran with the real epoch
        // and the same n, so the next grant must use a higher n.
        let ns: Vec<u32> = g.iter().map(|(_, (_, n))| *n).collect();
        assert!(
            ns.windows(2).all(|w| w[0] < w[1]),
            "{r}: grant numbers must strictly increase: {g:?}"
        );
        if g.len() > 1 {
            regranted += 1;
        }
        for (&(run, (e, n)), &off) in first_append.range((*r, (0, 0))..=(*r, (u32::MAX, u32::MAX)))
        {
            assert_eq!(run, *r);
            let grant_off = g
                .iter()
                .find(|(_, (ge, gn))| *gn == n && (*ge == e || *ge == e.wrapping_sub(1)))
                .map(|(o, _)| *o)
                .unwrap_or_else(|| panic!("{r}: append with token ({e},{n}) without a grant"));
            assert!(
                grant_off < off,
                "{r}: grant {grant_off} must precede append {off}"
            );
        }
        let last = g.last().expect("granted").1;
        assert_eq!(
            last.0, epoch,
            "{r}: the accepted grant is of the current epoch"
        );
    }
    assert!(regranted >= n_faulted, "rejected grants are re-granted");
    // The lease gauge is back to zero once every run finished.
    let deadline = Instant::now() + T;
    while o.stats.leases.load(Ordering::Relaxed) != 0 {
        assert!(Instant::now() < deadline, "leases still held");
        sleep(Duration::from_millis(10)).await;
    }
    node.shutdown().await.expect("shutdown");
}

/// Pipelined grant: the job starts before the grant is applied, yet its first append is
/// accepted (the log orders it after the grant) and the record it gets already carries the
/// new token and status `Running`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn job_record_carries_the_pipelined_token() {
    let tmp = ostro_testkit::fs::tempdir();
    let seen_rec: Arc<Mutex<Vec<JobView>>> = Arc::default();
    let s2 = Arc::clone(&seen_rec);
    let fake = Fake::new(move |job, _| {
        s2.lock()
            .expect("lock")
            .push((job.record.current_attempt, job.record.status, job.token));
        boxed(async move {
            let o = job.handle.submit_one(complete_cmd(&job)).await;
            assert_eq!(o.expect("append"), ApplyOutcome::Accepted);
            ExecOutcome::Released
        })
    });
    let o = Opts::new(&fake);
    let node = open(tmp.path(), Some(&o)).await;
    let h = h(&node);
    let run = run_on(0);
    submit_all(h, vec![start(&run, 0, "")]).await;
    let deadline = Instant::now() + T;
    while status(h, &run) != RunStatus::Completed {
        assert!(Instant::now() < deadline, "run did not complete");
        sleep(Duration::from_millis(10)).await;
    }
    let seen = seen_rec.lock().expect("lock").clone();
    assert_eq!(seen.len(), 1);
    let (cur, st, tok) = seen[0];
    assert_eq!(cur, Some((tok.epoch.0, tok.n)));
    assert_eq!(st, RunStatus::Running);
    assert_eq!(o.stats.grants_rejected.load(Ordering::Relaxed), 0);
    node.shutdown().await.expect("shutdown");
}
