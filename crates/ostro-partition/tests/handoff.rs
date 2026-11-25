//! Graceful handoff quiescing (13-11, D-P13-13) on a single-partition node with the
//! dispatcher driven by a fake executor: admission off blocks new leases (ready runs stay
//! ready), an in-flight activation that finishes within the grace leaves 0 leased, one that
//! does not is reported, an activation evicted at a flush/suspend point (`Reactivate`, the
//! executor's hot-wait eviction) gives its lease back and is re-queued, the write fence
//! (W11) refuses submissions with the retryable `LeaderChanged`, and abort (or dropping the
//! guard) resumes everything. No run is cancelled or failed.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{OPEN_T, T, start_run};
use ostro_partition::store::values::RunStatus;
use ostro_partition::{
    ApplyOutcome, DispatcherConfig, DispatcherStats, ExecOutcome, Executor, HandoffControl, Job,
    Node, NodeConfig, PartitionError, PartitionHandle,
};
use ostro_types::command::{
    AttemptTokenMsg, CompleteRun, Derived, EventsAppended, JournalEvent, TerminalStatus, derived,
};
use ostro_types::{AttemptToken, Command, PartitionCount, PartitionId, RunId};
use tokio::sync::{Semaphore, watch};
use tokio::time::{Instant, sleep, timeout};

const P0: PartitionId = PartitionId(0);

type Handle = PartitionHandle<ostro_log::PartitionLog>;
type ExecFuture = Pin<Box<dyn Future<Output = ExecOutcome> + Send + 'static>>;
type Script = dyn Fn(Job, usize) -> ExecFuture + Send + Sync;

/// Records every job's token and runs `script(job, job index)`.
struct Fake {
    seen: Mutex<Vec<(RunId, AttemptToken)>>,
    n: AtomicUsize,
    script: Box<Script>,
}

impl Fake {
    fn new(script: impl Fn(Job, usize) -> ExecFuture + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(Vec::new()),
            n: AtomicUsize::new(0),
            script: Box::new(script),
        })
    }

    fn seen(&self) -> Vec<(RunId, AttemptToken)> {
        self.seen.lock().expect("lock").clone()
    }

    async fn wait_jobs(&self, n: usize) -> Vec<(RunId, AttemptToken)> {
        let deadline = Instant::now() + T;
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
        self.seen.lock().expect("lock").push((job.run, job.token));
        let i = self.n.fetch_add(1, Ordering::SeqCst);
        (self.script)(job, i)
    }
}

fn boxed(f: impl Future<Output = ExecOutcome> + Send + 'static) -> ExecFuture {
    Box::pin(f)
}

/// One event plus `CompleteRun{Completed}` at the record's journal end, with the job's
/// token, then release (`Reactivate` if the append was refused).
async fn complete(job: Job) -> ExecOutcome {
    let seq = job.record.next_seq;
    let cmd = Command::events_appended(EventsAppended {
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
            ..Default::default()
        }],
        derived: vec![Derived::new(derived::Intent::CompleteRun(CompleteRun {
            status: TerminalStatus::Completed as i32,
            completed_at_ms: 1,
            ..Default::default()
        }))],
    });
    match job.handle.submit_one(cmd).await {
        Ok(_) => ExecOutcome::Released,
        Err(_) => ExecOutcome::Reactivate,
    }
}

async fn open(dir: &Path, fake: &Arc<Fake>) -> Node {
    let mut cfg = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(1).expect("non-zero"));
    let executor: Arc<dyn Executor> = fake.clone();
    cfg.leader.dispatcher = Some(DispatcherConfig {
        executor,
        lease_ttl: Duration::from_secs(30),
        slots: Arc::new(Semaphore::new(64)),
        stats: Some(Arc::new(DispatcherStats::default())),
        placement: None,
        flow: None,
        gauges: None,
    });
    timeout(OPEN_T, Node::open(cfg))
        .await
        .expect("node open timed out")
        .expect("node open")
}

fn h(node: &Node) -> Handle {
    node.handle(P0).expect("partition ready")
}

async fn start(h: &Handle) -> RunId {
    let run = RunId::new_v7(P0);
    let out = timeout(T, h.submit_one(Command::start_run(start_run(&run))))
        .await
        .expect("submit timed out")
        .expect("submit");
    assert_eq!(out, ApplyOutcome::Accepted);
    run
}

fn status(h: &Handle, run: &RunId) -> RunStatus {
    h.read_run(run).expect("read").expect("run exists").status
}

async fn wait_status(h: &Handle, run: &RunId, want: RunStatus) {
    let deadline = Instant::now() + T;
    while status(h, run) != want {
        assert!(Instant::now() < deadline, "run never reached {want:?}");
        sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admission_off_blocks_new_leases_and_abort_resumes() {
    let dir = ostro_testkit::fs::tempdir();
    let fake = Fake::new(|job, _| boxed(complete(job)));
    let node = open(dir.path(), &fake).await;
    let h = h(&node);
    assert!(!node.in_handoff(P0));

    let guard = timeout(T, node.prepare_handoff(P0, Duration::from_millis(200)))
        .await
        .expect("prepare timed out")
        .expect("prepare");
    assert!(node.in_handoff(P0), "in_handoff from prepare until release");
    assert_eq!(guard.partition(), P0);
    assert_eq!(guard.report().leased_left, 0);
    assert_eq!(guard.report().epoch, h.epoch());

    // Admission off: the run is accepted (not frozen yet) but stays Ready, never leased.
    let run = start(&h).await;
    sleep(Duration::from_millis(300)).await;
    assert!(fake.seen().is_empty(), "no lease while admission is off");
    assert_eq!(status(&h, &run), RunStatus::Ready);
    assert_eq!(h.handoff().leased(), 0);

    // A second prepare of the same term is refused.
    let again = node.prepare_handoff(P0, Duration::from_millis(10)).await;
    assert!(again.is_err(), "one handoff per partition at a time");

    guard.abort();
    assert!(!node.in_handoff(P0));
    fake.wait_jobs(1).await;
    wait_status(&h, &run, RunStatus::Completed).await;
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inflight_within_grace_leaves_zero_leased() {
    let dir = ostro_testkit::fs::tempdir();
    let fake = Fake::new(|job, _| {
        boxed(async move {
            sleep(Duration::from_millis(300)).await;
            complete(job).await
        })
    });
    let node = open(dir.path(), &fake).await;
    let h = h(&node);
    let run = start(&h).await;
    fake.wait_jobs(1).await;

    let t0 = Instant::now();
    let guard = timeout(T, node.prepare_handoff(P0, Duration::from_secs(5)))
        .await
        .expect("prepare timed out")
        .expect("prepare");
    let took = t0.elapsed();
    assert_eq!(guard.report().leased_left, 0, "the activation finished");
    assert!(
        took < Duration::from_secs(4),
        "returned at 0 leases, not at the grace: {took:?}"
    );
    wait_status(&h, &run, RunStatus::Completed).await;
    drop(guard);
    assert!(!node.in_handoff(P0), "dropping the guard aborts");
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inflight_past_grace_is_reported_not_cancelled() {
    let dir = ostro_testkit::fs::tempdir();
    let (go_tx, go_rx) = watch::channel(false);
    let fake = Fake::new(move |job, _| {
        let mut go = go_rx.clone();
        boxed(async move {
            let _ = go.wait_for(|v| *v).await;
            complete(job).await
        })
    });
    let node = open(dir.path(), &fake).await;
    let h = h(&node);
    let run = start(&h).await;
    fake.wait_jobs(1).await;

    let guard = timeout(T, node.prepare_handoff(P0, Duration::from_millis(200)))
        .await
        .expect("prepare timed out")
        .expect("prepare");
    assert_eq!(
        guard.report().leased_left,
        1,
        "the activation outlived the grace"
    );
    assert_eq!(
        status(&h, &run),
        RunStatus::Running,
        "not cancelled, not failed"
    );

    // It still completes on this leader (before the write fence).
    go_tx.send_replace(true);
    wait_status(&h, &run, RunStatus::Completed).await;
    guard.abort();
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evicted_activation_gives_lease_back_and_replays_after_abort() {
    let dir = ostro_testkit::fs::tempdir();
    // Job 0 parks "hot" until the partition quiesces, then reports the hot-wait eviction
    // (`Reactivate`), as the executor does when its input bridge closes. Later jobs complete.
    let fake = Fake::new(|job, i| {
        boxed(async move {
            if i == 0 {
                let mut m = job.handle.handoff().subscribe();
                let epoch = job.handle.epoch();
                let _ = m
                    .wait_for(|m| ostro_partition::HandoffCell::quiescing_at(m, epoch))
                    .await;
                return ExecOutcome::Reactivate;
            }
            complete(job).await
        })
    });
    let node = open(dir.path(), &fake).await;
    let h = h(&node);
    let run = start(&h).await;
    fake.wait_jobs(1).await;

    let guard = timeout(T, node.prepare_handoff(P0, Duration::from_secs(5)))
        .await
        .expect("prepare timed out")
        .expect("prepare");
    let r = guard.report();
    assert_eq!(
        r.leased_left, 0,
        "the evicted activation released its lease"
    );
    assert_eq!(r.evicted, 1);
    sleep(Duration::from_millis(200)).await;
    assert_eq!(fake.seen().len(), 1, "not re-run while quiescing");
    assert_eq!(status(&h, &run), RunStatus::Running, "the run is untouched");

    // Abort: re-granted (n + 1, not an attempt failure) and completed here.
    guard.abort();
    let seen = fake.wait_jobs(2).await;
    assert_eq!(seen[1].0, run);
    assert!(
        seen[1].1.n > seen[0].1.n,
        "a new attempt above the evicted one"
    );
    wait_status(&h, &run, RunStatus::Completed).await;
    let rec = h.read_run(&run).expect("read").expect("run");
    assert_eq!(
        rec.attempt_failures, 0,
        "the eviction is not an attempt failure"
    );
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_fence_rejects_retryably_and_abort_lifts() {
    let dir = ostro_testkit::fs::tempdir();
    let fake = Fake::new(|job, _| boxed(complete(job)));
    let node = open(dir.path(), &fake).await;
    let h = h(&node);
    let end_before = ostro_log::ReplicatedLog::log_end(h.log().as_ref());

    let guard = timeout(T, node.prepare_handoff(P0, Duration::from_millis(100)))
        .await
        .expect("prepare timed out")
        .expect("prepare");
    guard.freeze_appends();
    guard.freeze_appends(); // idempotent
    let run = RunId::new_v7(P0);
    let r = h.submit_one(Command::start_run(start_run(&run))).await;
    assert!(
        matches!(r, Err(PartitionError::LeaderChanged)),
        "frozen: retryable leader_changed, got {r:?}"
    );
    assert_eq!(
        ostro_log::ReplicatedLog::log_end(h.log().as_ref()),
        end_before,
        "the log end stops moving"
    );
    assert!(h.read_run(&run).expect("read").is_none());

    guard.abort();
    let out = timeout(T, h.submit_one(Command::start_run(start_run(&run))))
        .await
        .expect("submit timed out")
        .expect("accepted after the abort");
    assert_eq!(out, ApplyOutcome::Accepted);
    fake.wait_jobs(1).await;
    wait_status(&h, &run, RunStatus::Completed).await;
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn complete_keeps_the_fence_for_the_term() {
    let dir = ostro_testkit::fs::tempdir();
    let fake = Fake::new(|job, _| boxed(complete(job)));
    let node = open(dir.path(), &fake).await;
    let h = h(&node);
    let guard = timeout(T, node.prepare_handoff(P0, Duration::from_millis(100)))
        .await
        .expect("prepare timed out")
        .expect("prepare");
    guard.freeze_appends();
    guard.complete();
    // Leadership "moved": the fence stays until the role driver demotes this node (the
    // handle's epoch drops, which lifts it; a later term starts unfenced).
    assert!(node.in_handoff(P0));
    let run = RunId::new_v7(P0);
    let r = h.submit_one(Command::start_run(start_run(&run))).await;
    assert!(matches!(r, Err(PartitionError::LeaderChanged)), "{r:?}");
    let epoch = h.epoch();
    h.set_epoch(ostro_types::Epoch(0));
    assert!(!node.in_handoff(P0), "demotion lifts it");
    assert!(h.admission_open());
    h.set_epoch(ostro_types::Epoch(epoch.0 + 1));
    assert!(h.admission_open(), "a later term is not fenced");
    assert!(!h.handoff().frozen(h.epoch()));
    h.set_epoch(epoch);
    node.shutdown().await.expect("shutdown");
}
