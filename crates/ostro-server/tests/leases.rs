//! DISP-07 in process (plan 04-16): heartbeat renewals keep long activations leased; a
//! stalled executor loses its lease, is cancelled (self-fence / revoke) and the run is
//! re-dispatched as attempt n + 1; a zombie append with the old token is rejected Fenced
//! and leaves no trace; exactly one RunCompleted; self-fence before the dispatcher's
//! expiry; a restart fences every pre-restart token (new epoch).
//!
//! Timings are generous: the assertions are about ordering and counts.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{Opts, T, TestNode, count, evicted_external, guest_wasm, rref};
use ostro_engine::sink::{ActivationSink, AppendError};
use ostro_partition::store::values::RunStatus;
use ostro_partition::{ApplyOutcome, RejectReason};
use ostro_server::sink::PartitionSink;
use ostro_types::command::{AttemptTokenMsg, EventsAppended};
use ostro_types::journal::{
    CommandHash, CommandMeta, Event, EventBody, EventKind, RecordedResult, StepCompleted,
};
use ostro_types::{AttemptToken, Command, Epoch, RunId};
use tokio::sync::Semaphore;
use tokio::time::{Instant, sleep};

const TTL: Duration = Duration::from_millis(400);

fn token_of(a: Option<(u32, u32)>) -> AttemptToken {
    let (e, n) = a.expect("granted");
    AttemptToken { epoch: Epoch(e), n }
}

fn step_names(journal: &[Event]) -> Vec<String> {
    journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::StepCompleted(s) => Some(s.name.clone()),
            _ => None,
        })
        .collect()
}

/// What a zombie would flush: a step named `zombie` at `seq`.
fn zombie_event(seq: u64) -> Event {
    Event {
        seq,
        ts_nanos: 1_700_000_000_000_000_000,
        body: EventBody::StepCompleted(StepCompleted {
            meta: CommandMeta {
                hash_ver: 1,
                hash: CommandHash([0xee; 16]),
            },
            name: "zombie".into(),
            outcome: RecordedResult {
                codec_ver: 1,
                bytes: Vec::new(),
            },
            fuel_used: 0,
        }),
    }
}

/// The raw apply verdict for an empty append with `token` at `from_seq`.
async fn raw_verdict(
    n: &TestNode,
    run: &RunId,
    token: AttemptToken,
    from_seq: u64,
) -> ApplyOutcome {
    let cmd = Command::events_appended(EventsAppended {
        run_id: run.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg::from(token)),
        from_seq,
        events: vec![],
        derived: vec![],
    });
    n.handle_of(run).submit_one(cmd).await.expect("submit")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeat_keeps_long_activation_leased() {
    let dir = ostro_testkit::fs::tempdir();
    // lease 400 ms, heartbeat 100 ms; the guest parks hot for 1.6 s (> 3 x lease_ttl).
    let n = TestNode::open(dir.path(), Opts::new().lease(TTL)).await;
    let sha = n.install_module(&guest_wasm("retry-probe")).await;
    let started = Instant::now();
    let run = n.start("retry-probe", 1, b"sleep:1600", &sha).await;
    let rec = n.wait_terminal(&run, T).await;
    assert!(started.elapsed() > TTL * 3);
    assert_eq!(
        rec.status,
        RunStatus::Completed,
        "jobs {:?}",
        n.jobs_of(&run)
    );
    assert_eq!(rec.current_attempt.map(|a| a.1), Some(1), "no re-dispatch");
    assert_eq!(n.stats.lease_expired.load(Ordering::Relaxed), 0);
    assert_eq!(n.hooks.self_fences.load(Ordering::Relaxed), 0);
    assert_eq!(n.activations(), 1);
    n.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_attempt_is_fenced_redispatched_and_zombie_rejected() {
    let dir = ostro_testkit::fs::tempdir();
    let mut opts = Opts::new().lease(TTL).hot_wait(Duration::from_secs(2));
    // One slot: attempt 2 runs only because the revoked zombie holds no slot.
    opts.slots = 1;
    let n = TestNode::open(dir.path(), opts).await;
    // Attempt 1 stalls: no heartbeat, every engine append blocked.
    let gate = Arc::new(Semaphore::new(0));
    n.hooks.only_attempt.store(1, Ordering::Release);
    n.hooks.suppress_heartbeat.store(true, Ordering::Release);
    *n.hooks.pause_before_append.lock().expect("lock") = Some(Arc::clone(&gate));
    let sha = n.install_module(&guest_wasm("retry-probe")).await;
    let run = n.start("retry-probe", 1, b"sleep:1500", &sha).await;

    // Attempt 1's activation is cancelled (self-fence and/or revoke), not completed.
    let zombie = n.wait_job(&run, T, |j| j.token.n == 1).await;
    assert!(evicted_external(&zombie.activation), "{zombie:?}");
    assert!(zombie.revoked || zombie.self_fenced, "{zombie:?}");
    assert!(n.stats.lease_expired.load(Ordering::Relaxed) >= 1);

    // Attempt 2 owns the run and is parked on its timer.
    let rec = n
        .wait_until(&run, T, |r| r.current_attempt.map(|a| a.1) == Some(2))
        .await;
    assert!(!rec.status.is_terminal());
    let old = zombie.token;
    assert_eq!(old.n, 1);

    // A late zombie flush with the old token: Fenced, nothing journaled.
    let zsink = PartitionSink::new(n.handle_of(&run), run, old, None, None);
    let end = zsink.next_seq();
    let r = zsink
        .append(&rref(&run), old.n, end, vec![zombie_event(end)])
        .await;
    assert_eq!(r, Err(AppendError::Fenced));
    if !n.record(&run).status.is_terminal() {
        // Rejected by the token check itself (the run is still live).
        match raw_verdict(&n, &run, old, end).await {
            ApplyOutcome::Rejected(RejectReason::Fenced { .. }) => {}
            ApplyOutcome::Rejected(RejectReason::RunTerminal) => {}
            other => panic!("zombie verdict {other:?}"),
        }
    }

    let rec = n.wait_terminal(&run, T).await;
    assert_eq!(
        rec.status,
        RunStatus::Completed,
        "jobs {:?}",
        n.jobs_of(&run)
    );
    assert_eq!(rec.current_attempt.map(|a| a.1), Some(2));
    // After completion the zombie is still rejected.
    let r = zsink
        .append(
            &rref(&run),
            old.n,
            rec.next_seq,
            vec![zombie_event(rec.next_seq)],
        )
        .await;
    assert_eq!(r, Err(AppendError::Fenced));
    gate.add_permits(1_000);

    let j = n.journal(&run).await;
    assert_eq!(step_names(&j), ["before", "after"], "no zombie events");
    assert_eq!(count(&j, EventKind::RunCompleted), 1);
    assert_eq!(count(&j, EventKind::RunStarted), 1);
    assert_eq!(n.result(&run).await.as_deref(), Some(&b"slept:1500"[..]));
    n.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn executor_self_fences_before_dispatcher_expiry() {
    let dir = ostro_testkit::fs::tempdir();
    // Dispatcher lease 3 s; the executor's own view 400 ms (deadline 350 ms after the last
    // renewal), heartbeat suppressed for attempt 1 only.
    let mut opts = Opts::new().lease(TTL).hot_wait(Duration::from_secs(2));
    opts.dispatcher_lease_ttl = Duration::from_secs(3);
    let n = TestNode::open(dir.path(), opts).await;
    n.hooks.only_attempt.store(1, Ordering::Release);
    n.hooks.suppress_heartbeat.store(true, Ordering::Release);
    let sha = n.install_module(&guest_wasm("retry-probe")).await;
    let run = n.start("retry-probe", 1, b"sleep:1500", &sha).await;

    // The executor cancels itself while the dispatcher still holds the lease.
    let deadline = Instant::now() + T;
    while n.hooks.self_fences.load(Ordering::Acquire) == 0 {
        assert!(Instant::now() < deadline, "no self-fence");
        sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        n.stats.lease_expired.load(Ordering::Relaxed),
        0,
        "self-fence first"
    );
    assert_eq!(n.record(&run).current_attempt.map(|a| a.1), Some(1));

    let rec = n.wait_terminal(&run, T).await;
    assert_eq!(
        rec.status,
        RunStatus::Completed,
        "jobs {:?}",
        n.jobs_of(&run)
    );
    let first = n.wait_job(&run, T, |j| j.token.n == 1).await;
    assert!(first.self_fenced, "{first:?}");
    assert!(evicted_external(&first.activation), "{first:?}");
    // Then the dispatcher expired the unrenewed lease and re-granted attempt 2.
    assert!(n.stats.lease_expired.load(Ordering::Relaxed) >= 1);
    assert_eq!(rec.current_attempt.map(|a| a.1), Some(2));
    let j = n.journal(&run).await;
    assert_eq!(step_names(&j), ["before", "after"]);
    assert_eq!(count(&j, EventKind::RunCompleted), 1);
    n.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_fences_pre_restart_tokens() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), Opts::new().hot_wait(Duration::from_millis(100))).await;
    let sha = n.install_module(&guest_wasm("retry-probe")).await;
    // Suspends on a 60 s timer (Scheduled, not re-dispatched on restart).
    let run = n.start("retry-probe", 1, b"sleep:60000", &sha).await;
    let rec = n
        .wait_until(&run, T, |r| r.status == RunStatus::Scheduled)
        .await;
    let old = token_of(rec.current_attempt);
    let (rt, hooks, opts) = n.stop().await;

    let n = TestNode::open_with(dir.path(), opts, rt, hooks).await;
    let rec = n.record(&run);
    assert_eq!(rec.status, RunStatus::Scheduled);
    let zsink = PartitionSink::new(n.handle_of(&run), run, old, None, None);
    let r = zsink
        .append(
            &rref(&run),
            old.n,
            rec.next_seq,
            vec![zombie_event(rec.next_seq)],
        )
        .await;
    assert_eq!(r, Err(AppendError::Fenced));
    match raw_verdict(&n, &run, old, rec.next_seq).await {
        ApplyOutcome::Rejected(RejectReason::StaleEpoch { .. }) => {}
        other => panic!("pre-restart token verdict {other:?}"),
    }
    assert_eq!(n.record(&run).next_seq, rec.next_seq, "nothing journaled");
    n.stop().await;
}
