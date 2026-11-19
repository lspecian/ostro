//! Plan 05-18 (QA-01): the dispatcher failpoints `dispatcher::before_grant` and
//! `dispatcher::before_ack`, with a fake executor that completes each run in one append.
//! Run with `--features failpoints -- --test-threads=1` (process-global fail registry).
//!
//! - A delayed grant (`sleep`) still yields exactly one attempt.
//! - A failed grant (`return`, EIO / ENOSPC) logs nothing and is retried: still one
//!   `AttemptGranted` and one attempt.
//! - A delayed ack lets the job run before its grant is acknowledged: one attempt.
//! - A failed ack (outcome unknown) revokes the job and re-grants a higher token: the run
//!   completes exactly once, with the second token.
//!
//! Every test ends with the lease gauge back at 0.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{OPEN_T, T, run_on, start_run};
use ostro_log::ReplicatedLog;
use ostro_partition::store::values::RunStatus;
use ostro_partition::{
    ApplyOutcome, DispatcherConfig, DispatcherStats, ExecOutcome, Executor, Job, Node, NodeConfig,
    PartitionHandle,
};
use ostro_types::command::{
    AttemptTokenMsg, Cmd, CompleteRun, Derived, EventsAppended, JournalEvent, TerminalStatus,
    decode_command, derived,
};
use ostro_types::{AttemptToken, Command, PartitionCount, PartitionId, RunId};
use tokio::sync::Semaphore;
use tokio::time::{Instant, sleep, timeout};

const P0: PartitionId = PartitionId(0);

type Handle = PartitionHandle<ostro_log::Rf1Log>;
type ExecFuture = Pin<Box<dyn Future<Output = ExecOutcome> + Send + 'static>>;

/// One job as the fake executor saw it.
#[derive(Clone, Debug)]
struct Seen {
    token: AttemptToken,
    /// The job's lease was revoked while it waited.
    revoked: bool,
    /// Outcome of its completion append (`None` = it did not append).
    completed: Option<ApplyOutcome>,
}

/// Completes the run on every job, except that a job whose call index is in `revoke_wait`
/// first waits (up to `T`) for its lease to be revoked and then returns without appending.
struct Fake {
    seen: Arc<Mutex<Vec<Seen>>>,
    revoke_wait: Vec<usize>,
}

impl Executor for Fake {
    fn execute(&self, mut job: Job) -> ExecFuture {
        let seen = Arc::clone(&self.seen);
        let idx = seen.lock().expect("lock").len();
        seen.lock().expect("lock").push(Seen {
            token: job.token,
            revoked: false,
            completed: None,
        });
        let wait_revoke = self.revoke_wait.contains(&idx);
        Box::pin(async move {
            if wait_revoke {
                let revoked = timeout(T, job.revoked.wait_for(|v| *v)).await.is_ok();
                seen.lock().expect("lock")[idx].revoked = revoked;
                return ExecOutcome::Released;
            }
            let out = job
                .handle
                .submit_one(complete_cmd(&job))
                .await
                .expect("complete submit");
            seen.lock().expect("lock")[idx].completed = Some(out.clone());
            match out {
                ApplyOutcome::Accepted => ExecOutcome::Released,
                _ => ExecOutcome::Fenced,
            }
        })
    }
}

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

async fn open(dir: &Path, fake: Arc<Fake>, stats: &Arc<DispatcherStats>) -> Node {
    let mut cfg = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(1).expect("non-zero"));
    let executor: Arc<dyn Executor> = fake;
    cfg.leader.dispatcher = Some(DispatcherConfig {
        executor,
        lease_ttl: Duration::from_secs(30),
        slots: Arc::new(Semaphore::new(16)),
        stats: Some(Arc::clone(stats)),
    });
    timeout(OPEN_T, Node::open(cfg))
        .await
        .expect("node open timed out")
        .expect("node open")
}

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

/// `(offset, (epoch, n))` of every `AttemptGranted` of `run`, and the offsets of its
/// `EventsAppended` with their tokens.
async fn grants_and_appends(
    h: &Handle,
    run: &RunId,
) -> (Vec<(u64, (u32, u32))>, Vec<(u64, (u32, u32))>) {
    let (mut grants, mut appends) = (Vec::new(), Vec::new());
    for (off, c) in log_commands(h).await {
        match c.cmd {
            Some(Cmd::AttemptGranted(g)) if g.run_id == run.as_bytes() => {
                let t = g.token.expect("token");
                grants.push((off, (t.epoch, t.n)));
            }
            Some(Cmd::EventsAppended(e)) if e.run_id == run.as_bytes() => {
                let t = e.token.expect("token");
                appends.push((off, (t.epoch, t.n)));
            }
            _ => {}
        }
    }
    (grants, appends)
}

async fn wait_completed(h: &Handle, run: &RunId) {
    let deadline = Instant::now() + T;
    loop {
        let st = h.read_run(run).expect("read").expect("run").status;
        if st == RunStatus::Completed {
            return;
        }
        assert!(Instant::now() < deadline, "run not completed: {st:?}");
        sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_no_leases(stats: &DispatcherStats) {
    let deadline = Instant::now() + T;
    while stats.leases.load(Ordering::Relaxed) != 0 {
        assert!(
            Instant::now() < deadline,
            "lease gauge stuck at {}",
            stats.leases.load(Ordering::Relaxed)
        );
        sleep(Duration::from_millis(10)).await;
    }
}

/// Start one run with `site` configured as `action`; returns what the executor saw and the
/// run's grants/appends in the log.
struct Outcome {
    seen: Vec<Seen>,
    grants: Vec<(u64, (u32, u32))>,
    appends: Vec<(u64, (u32, u32))>,
    epoch: u32,
    granting_ms: u128,
}

async fn one_run(site: &str, action: &str, revoke_wait: Vec<usize>) -> Outcome {
    let _scenario = fail::FailScenario::setup();
    let tmp = ostro_testkit::fs::tempdir();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let fake = Arc::new(Fake {
        seen: Arc::clone(&seen),
        revoke_wait,
    });
    let stats = Arc::new(DispatcherStats::default());
    let node = open(tmp.path(), fake, &stats).await;
    let h = node.handle(P0).expect("partition ready").clone();
    let run = run_on(0);
    fail::cfg(site, action).unwrap();
    let t0 = Instant::now();
    let out = timeout(T, h.submit_one(Command::start_run(start_run(&run))))
        .await
        .expect("start timed out")
        .expect("start");
    assert_eq!(out, ApplyOutcome::Accepted);
    wait_completed(&h, &run).await;
    let granting_ms = t0.elapsed().as_millis();
    fail::remove(site);
    wait_no_leases(&stats).await;
    let (grants, appends) = grants_and_appends(&h, &run).await;
    let epoch = h.epoch().0;
    let seen = seen.lock().expect("lock").clone();
    timeout(T, node.shutdown())
        .await
        .expect("shutdown timed out")
        .expect("shutdown");
    Outcome {
        seen,
        grants,
        appends,
        epoch,
        granting_ms,
    }
}

/// Exactly one attempt: one grant (n = 1), one job, one accepted completion after it.
fn assert_one_attempt(o: &Outcome, what: &str) {
    assert_eq!(o.grants.len(), 1, "{what}: grants {:?}", o.grants);
    assert_eq!(o.grants[0].1, (o.epoch, 1), "{what}");
    assert_eq!(o.seen.len(), 1, "{what}: jobs {:?}", o.seen);
    assert_eq!(o.seen[0].token.n, 1, "{what}");
    assert_eq!(o.seen[0].completed, Some(ApplyOutcome::Accepted), "{what}");
    assert_eq!(o.appends.len(), 1, "{what}: appends {:?}", o.appends);
    assert!(
        o.appends[0].0 > o.grants[0].0,
        "{what}: append after its grant"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_grant_still_yields_exactly_one_attempt() {
    let o = one_run("dispatcher::before_grant", "1*sleep(300)", vec![]).await;
    assert_one_attempt(&o, "before_grant=sleep");
    assert!(
        o.granting_ms >= 300,
        "the grant was delayed ({} ms)",
        o.granting_ms
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_grant_logs_nothing_and_is_retried_once() {
    for action in ["1*return", "1*return(enospc)"] {
        let o = one_run("dispatcher::before_grant", action, vec![]).await;
        assert_one_attempt(&o, action);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_ack_job_runs_before_the_ack_one_attempt() {
    let o = one_run("dispatcher::before_ack", "1*sleep(300)", vec![]).await;
    assert_one_attempt(&o, "before_ack=sleep");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_ack_revokes_and_regrants_a_higher_token_once() {
    for action in ["1*return", "1*return(enospc)"] {
        // The first job waits for its revocation (the failed ack drops its lease).
        let o = one_run("dispatcher::before_ack", action, vec![0]).await;
        let toks: Vec<(u32, u32)> = o.grants.iter().map(|g| g.1).collect();
        assert_eq!(toks, vec![(o.epoch, 1), (o.epoch, 2)], "{action}");
        assert_eq!(o.seen.len(), 2, "{action}: jobs {:?}", o.seen);
        assert!(o.seen[0].revoked, "{action}: the first job was revoked");
        assert_eq!(o.seen[0].completed, None);
        assert_eq!(o.seen[1].token.n, 2);
        assert_eq!(o.seen[1].completed, Some(ApplyOutcome::Accepted));
        // One completion, by the second token, after its grant.
        assert_eq!(o.appends.len(), 1, "{action}: appends {:?}", o.appends);
        assert_eq!(o.appends[0].1, (o.epoch, 2));
        assert!(o.appends[0].0 > o.grants[1].0);
    }
}
