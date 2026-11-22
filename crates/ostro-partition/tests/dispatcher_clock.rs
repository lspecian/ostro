//! Dispatcher and the dev clock hooks (plan 10-10, SDK-06, Pitfall 8 / W1):
//!
//! - a clock with `subscribe_changes` wakes the dispatcher when it jumps: a run requeued
//!   with a 1 h `ready_at` (a step backoff) is dispatched right after the clock advances
//!   1 h, not after an hour of real time;
//! - a clock with `skip_barrier`: while the write guard is held (a skip in progress) no
//!   grant is appended; once it is released the grant goes through, and the `granting`
//!   gauge is back at 0 when the grant task ends.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{OPEN_T, T, run_on, start_run};
use ostro_partition::{
    ApplyOutcome, Clock, DispatcherConfig, DispatcherStats, ExecOutcome, Executor, Job,
    ManualClock, Node, NodeConfig, PartitionHandle, SystemClock,
};
use ostro_types::command::{AttemptTokenMsg, Derived, EnqueueReady, EventsAppended, derived};
use ostro_types::{Command, PartitionCount, PartitionId};
use tokio::sync::{RwLock, Semaphore, watch};
use tokio::time::{Instant, sleep, timeout};

const P0: PartitionId = PartitionId(0);
const HOUR_MS: u64 = 3_600_000;

type Handle = PartitionHandle<ostro_log::PartitionLog>;
type ExecFuture = Pin<Box<dyn Future<Output = ExecOutcome> + Send + 'static>>;

/// A manual clock that exposes the dev hooks (what the server's DevClock does).
struct SkipClock {
    inner: ManualClock,
    barrier: Arc<RwLock<()>>,
}

impl Clock for SkipClock {
    fn now_ms(&self) -> u64 {
        self.inner.now_ms()
    }
    fn subscribe_changes(&self) -> Option<watch::Receiver<u64>> {
        Some(self.inner.changed())
    }
    fn skip_barrier(&self) -> Option<Arc<RwLock<()>>> {
        Some(Arc::clone(&self.barrier))
    }
}

type Script = dyn Fn(Job, usize) -> ExecFuture + Send + Sync;

/// Records job start times and runs `script(job, index)`.
struct Fake {
    seen: Mutex<Vec<Instant>>,
    script: Box<Script>,
}

impl Fake {
    fn new(script: impl Fn(Job, usize) -> ExecFuture + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(Vec::new()),
            script: Box::new(script),
        })
    }
    fn seen(&self) -> Vec<Instant> {
        self.seen.lock().expect("lock").clone()
    }
    async fn wait_jobs(&self, n: usize, limit: Duration) -> Vec<Instant> {
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
            sleep(Duration::from_millis(5)).await;
        }
    }
}

impl Executor for Fake {
    fn execute(&self, job: Job) -> ExecFuture {
        let idx = {
            let mut s = self.seen.lock().expect("lock");
            s.push(Instant::now());
            s.len() - 1
        };
        (self.script)(job, idx)
    }
}

async fn open(
    dir: &Path,
    fake: &Arc<Fake>,
    clock: Arc<dyn Clock>,
    stats: &Arc<DispatcherStats>,
) -> Node {
    let mut cfg = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(1).expect("non-zero"));
    cfg.clock = clock;
    let executor: Arc<dyn Executor> = fake.clone();
    cfg.leader.dispatcher = Some(DispatcherConfig {
        executor,
        lease_ttl: Duration::from_secs(30),
        slots: Arc::new(Semaphore::new(8)),
        stats: Some(Arc::clone(stats)),
        placement: None,
        flow: None,
        gauges: None,
    });
    timeout(OPEN_T, Node::open(cfg))
        .await
        .expect("node open timed out")
        .expect("node open")
}

fn h(node: &Node) -> &Handle {
    node.handle(P0).expect("partition ready")
}

/// Requeue `job.run` at `ready_at_ms` (an `EnqueueReady` intent, like a step backoff).
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clock_jump_wakes_the_dispatcher_for_a_one_hour_backoff() {
    let tmp = ostro_testkit::fs::tempdir();
    let manual = ManualClock::new(SystemClock.now_ms());
    let clock = Arc::new(SkipClock {
        inner: manual.clone(),
        barrier: Arc::new(RwLock::new(())),
    });
    let c2 = manual.clone();
    let fake = Fake::new(move |job, idx| {
        let c = c2.clone();
        Box::pin(async move {
            if idx == 0 {
                requeue(&job, c.now_ms() + HOUR_MS).await;
            }
            ExecOutcome::Released
        })
    });
    let stats = Arc::new(DispatcherStats::default());
    let node = open(tmp.path(), &fake, clock, &stats).await;
    let run = run_on(0);
    let now = manual.now_ms();
    let out = timeout(
        T,
        h(&node).submit(vec![Command::start_run(ostro_types::command::StartRun {
            ready_at_ms: now,
            requested_at_ms: now,
            ..start_run(&run)
        })]),
    )
    .await
    .expect("submit timed out")
    .expect("submit");
    assert!(out.iter().all(|o| *o == ApplyOutcome::Accepted), "{out:?}");
    fake.wait_jobs(1, T).await;
    // The 1 h backoff holds the run (real time does not move the manual clock).
    sleep(Duration::from_millis(300)).await;
    assert_eq!(
        fake.seen().len(),
        1,
        "dispatched before the backoff elapsed"
    );
    let jumped = Instant::now();
    manual.advance(HOUR_MS);
    let seen = fake.wait_jobs(2, Duration::from_secs(1)).await;
    let woke = seen[1].duration_since(jumped);
    eprintln!("dispatcher woke {woke:?} after the 1 h clock jump");
    assert!(woke < Duration::from_secs(1), "woke after {woke:?}");
    assert_eq!(stats.granting.load(Ordering::SeqCst), 0);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_skip_barrier_blocks_grants_until_released() {
    let tmp = ostro_testkit::fs::tempdir();
    let manual = ManualClock::new(SystemClock.now_ms());
    let barrier = Arc::new(RwLock::new(()));
    let clock = Arc::new(SkipClock {
        inner: manual.clone(),
        barrier: Arc::clone(&barrier),
    });
    let fake = Fake::new(|_, _| Box::pin(async { ExecOutcome::Released }));
    let stats = Arc::new(DispatcherStats::default());
    let node = open(tmp.path(), &fake, clock, &stats).await;
    let h = h(&node).clone();

    // A skip in progress: the write guard is held.
    let skip = barrier.write().await;
    let run = run_on(0);
    let now = manual.now_ms();
    let out = timeout(
        T,
        h.submit(vec![Command::start_run(ostro_types::command::StartRun {
            ready_at_ms: now,
            requested_at_ms: now,
            ..start_run(&run)
        })]),
    )
    .await
    .expect("submit timed out")
    .expect("submit");
    assert!(out.iter().all(|o| *o == ApplyOutcome::Accepted), "{out:?}");
    sleep(Duration::from_millis(300)).await;
    assert!(fake.seen().is_empty(), "a grant interleaved with the skip");
    assert_eq!(
        stats.granting.load(Ordering::SeqCst),
        0,
        "the grant is counted only inside the read guard"
    );
    let rec = h.read_run(&run).expect("read").expect("run");
    assert!(
        rec.current_attempt.is_none(),
        "AttemptGranted appended during the skip"
    );
    assert!(
        stats.leases.load(Ordering::SeqCst) >= 1,
        "the lease waits in Granting"
    );

    drop(skip);
    fake.wait_jobs(1, T).await;
    // The grant task ends right after its append: the gauge returns to 0.
    let deadline = Instant::now() + T;
    while stats.granting.load(Ordering::SeqCst) != 0 {
        assert!(Instant::now() < deadline, "granting gauge stuck");
        sleep(Duration::from_millis(5)).await;
    }
    node.shutdown().await.expect("shutdown");
}
