//! Dispatcher leader service (plan 04-14: DISP-01, DISP-02, DISP-07) driven by a fake
//! `Executor`: rebuild on leadership, action-driven wake-ups (no polling), concurrency key
//! carried end to end, slot bound, outcome handling, soft leases (renew, expiry -> revoke +
//! re-grant, stale outcomes ignored) and shutdown revocation. Plan 05-11: hard stops
//! (Terminated / TimedOut) revoke the lease of a running job, retries are counted durably
//! (`AttemptGranted{after_failure}` at once, job after the delay), and a zero-permit slot
//! semaphore holds runs un-granted.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{T, run_on, start_run};
use ostro_partition::store::values::RunStatus;
use ostro_partition::{
    ApplyOutcome, Clock, DispatcherConfig, DispatcherStats, ExecOutcome, Executor, Job, Node,
    NodeConfig, PartitionHandle, SystemClock,
};
use ostro_types::command::{
    AttemptTokenMsg, Derived, EnqueueReady, EventsAppended, StartRun, TerminateRun, derived,
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
    timeout(T, Node::open(cfg))
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
        assert_eq!(attempt(h, r), Some((epoch.0, 1)));
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
    assert_eq!(attempt(h, &run), Some((h.epoch().0, 2)));
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
