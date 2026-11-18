//! Step `timeout-ms` and per-step fuel budgets (plan 04-10, WF-02) over
//! `guests/retry-probe`.
//!
//! - A live step whose body runs past `timeout-ms` is interrupted by the epoch-deadline
//!   callback (which sees the open step's deadline through the store data). The open
//!   attempt is recorded as a `Timeout` failure: `StepAttemptFailed(Requeue)` + evict
//!   `StepBackoff` while attempts remain, else `StepCompleted(Err(Timeout))` + evict
//!   `StepInterrupted` (re-activate immediately; the run continues from replay).
//! - A step that finishes in time is unaffected.
//! - A per-step fuel breach is recorded as `NonRetryable { kind: "fuel_exhausted" }` for
//!   that step instead of failing the attempt.
//! - Code outside steps is still bounded by `engine.activation_timeout`, which also wins
//!   when it is earlier than the step deadline.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ostro_engine::config::EngineConfig;
use ostro_engine::env::Clock;
use ostro_engine::error::{AttemptFailure, EvictReason};
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::testkit::{DEFAULT_ACCEPTED_AT, FakeSink, RunSpec, replay_journal};
use ostro_types::journal::{
    AttemptBackoff, Event, EventBody, Failure, Payload, StepAttemptFailed, StepCompleted,
    decode_result,
};
use tokio_util::sync::CancellationToken;

const MS: u64 = 1_000_000;
const T0: u64 = DEFAULT_ACCEPTED_AT;

/// A clock that stays where the test puts it.
#[derive(Debug)]
struct TestClock(AtomicU64);

impl TestClock {
    fn at(nanos: u64) -> Arc<Self> {
        Arc::new(Self(AtomicU64::new(nanos)))
    }

    fn set(&self, nanos: u64) {
        self.0.store(nanos, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now_unix_nanos(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn runtime(clock: Arc<TestClock>, activation_timeout: Duration) -> Arc<Runtime> {
    Arc::new(
        Runtime::new(
            EngineConfig {
                hot_wait: Duration::from_secs(2),
                activation_timeout,
                ..EngineConfig::default()
            },
            RuntimeOptions {
                clock,
                token_key: [7; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime"),
    )
}

fn prepare(rt: &Runtime) -> Arc<Prepared> {
    Arc::new(
        rt.prepare(&common::guest_wasm("retry-probe"))
            .expect("prepare retry-probe"),
    )
}

fn spec(id: &str, mode: &str) -> RunSpec {
    RunSpec::new(
        RunRef {
            partition: 5,
            run_id: id.to_string(),
        },
        "retry-probe",
        mode.as_bytes(),
    )
}

fn seeded(p: &Prepared, s: &RunSpec) -> Arc<FakeSink> {
    let sink = Arc::new(FakeSink::new());
    sink.seed(
        s.run.clone(),
        vec![s.run_started_event(&p.module_hash_hex)],
        1,
    );
    sink
}

async fn activate(
    rt: &Runtime,
    p: &Prepared,
    sink: &Arc<FakeSink>,
    run: &RunRef,
) -> ActivationOutcome {
    rt.activate(
        p,
        Activate {
            run: run.clone(),
            attempt: 1,
            module_sha: p.sha,
            journal: sink.journal(run),
            inputs: None,
            cancel: CancellationToken::new(),
        },
        sink.clone(),
    )
    .await
}

fn safs(journal: &[Event]) -> Vec<(u64, StepAttemptFailed)> {
    journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::StepAttemptFailed(f) => Some((e.ts_nanos, f.clone())),
            _ => None,
        })
        .collect()
}

fn step_completed(journal: &[Event]) -> StepCompleted {
    let done: Vec<_> = journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::StepCompleted(sc) => Some(sc.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(done.len(), 1, "exactly one StepCompleted: {journal:#?}");
    done.into_iter().next().unwrap()
}

fn outcome_of(sc: &StepCompleted) -> Result<Payload, Failure> {
    decode_result(&sc.outcome).expect("decode step outcome")
}

fn completed_text(o: &ActivationOutcome) -> String {
    match o {
        ActivationOutcome::Completed { output } => String::from_utf8(output.data.clone()).unwrap(),
        other => panic!("not completed: {other:?}"),
    }
}

const INTERRUPTED: ActivationOutcome = ActivationOutcome::Evicted {
    reason: EvictReason::StepInterrupted,
};

#[tokio::test]
async fn hung_step_times_out_retries_then_records_timeout() {
    let clock = TestClock::at(T0);
    let rt = runtime(clock.clone(), Duration::from_secs(60));
    let p = prepare(&rt);
    let s = spec("hang", "hang:200");
    let sink = seeded(&p, &s);
    let total = Instant::now();

    // Attempt 1: interrupted at ~200 ms, the failure is journaled as a requeued SAF.
    let started = Instant::now();
    let out = activate(&rt, &p, &sink, &s.run).await;
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(200) && took < Duration::from_secs(5),
        "attempt 1 interrupted near the step timeout, took {took:?}"
    );
    let j = sink.journal(&s.run);
    let f = safs(&j);
    assert_eq!(f.len(), 1, "{j:#?}");
    let (ts, saf) = &f[0];
    assert_eq!(saf.attempt, 1);
    assert_eq!(saf.name, "probe");
    assert_eq!(saf.failure, Failure::Timeout);
    assert_eq!(saf.backoff, AttemptBackoff::Requeue);
    assert_eq!(saf.retry_at_nanos, ts + 10 * MS);
    assert_eq!(
        out,
        ActivationOutcome::Evicted {
            reason: EvictReason::StepBackoff {
                retry_at_nanos: saf.retry_at_nanos
            }
        }
    );
    assert_eq!(j.len(), 2, "RunStarted + SAF");

    // Attempt 2 at retry_at: interrupted again, attempts exhausted -> Err(Timeout).
    clock.set(saf.retry_at_nanos);
    let out = activate(&rt, &p, &sink, &s.run).await;
    assert_eq!(out, INTERRUPTED);
    let j = sink.journal(&s.run);
    assert_eq!(safs(&j).len(), 1);
    assert_eq!(outcome_of(&step_completed(&j)), Err(Failure::Timeout));
    assert_eq!(j.len(), 3, "RunStarted + SAF + StepCompleted");

    // Re-activation continues from replay and finishes per guest code.
    let out = activate(&rt, &p, &sink, &s.run).await;
    assert_eq!(completed_text(&out), "err:timeout");
    let total = total.elapsed();
    assert!(
        total < Duration::from_secs(10),
        "2 x 200 ms + backoff, far below activation_timeout: {total:?}"
    );
    assert!(
        sink.outcomes()
            .iter()
            .all(|(_, _, o)| !matches!(o, ActivationOutcome::AttemptFailed(_)))
    );

    // Replay of the final journal appends nothing and never waits on the timeout.
    let j = sink.journal(&s.run);
    for (i, e) in j.iter().enumerate() {
        assert_eq!(e.seq, i as u64);
    }
    let started = Instant::now();
    let (replayed, appended) = replay_journal(&rt, &p, s.run.clone(), j).await;
    assert_eq!(completed_text(&replayed), "err:timeout");
    assert_eq!(appended, 0);
    assert!(started.elapsed() < Duration::from_millis(150));
}

#[tokio::test]
async fn step_finishing_in_time_is_unaffected() {
    let rt = runtime(TestClock::at(T0), Duration::from_secs(60));
    let p = prepare(&rt);
    let s = spec("quick", "quick");
    let sink = seeded(&p, &s);
    let out = activate(&rt, &p, &sink, &s.run).await;
    assert_eq!(completed_text(&out), "ok:quick");
    let j = sink.journal(&s.run);
    assert!(safs(&j).is_empty());
    assert_eq!(outcome_of(&step_completed(&j)).unwrap().data, b"quick");
    assert_eq!(sink.outcomes().len(), 1);
}

#[tokio::test]
async fn step_fuel_breach_fails_the_step_not_the_attempt() {
    let rt = runtime(TestClock::at(T0), Duration::from_secs(60));
    let p = prepare(&rt);
    let s = spec("burn", "burn");
    let sink = seeded(&p, &s);

    let out = activate(&rt, &p, &sink, &s.run).await;
    assert_eq!(out, INTERRUPTED, "not AttemptFailed(FuelExhausted)");
    let j = sink.journal(&s.run);
    assert!(safs(&j).is_empty(), "fuel breach is non-retryable: {j:#?}");
    let sc = step_completed(&j);
    assert_eq!(sc.fuel_used, 1_000_000, "the whole step budget was used");
    match outcome_of(&sc) {
        Err(Failure::NonRetryable(e)) => {
            assert_eq!(e.kind, "fuel_exhausted");
            assert!(e.message.contains("1000000"), "{}", e.message);
        }
        other => panic!("expected non-retryable fuel_exhausted, got {other:?}"),
    }

    let out = activate(&rt, &p, &sink, &s.run).await;
    assert_eq!(completed_text(&out), "err:non-retryable:fuel_exhausted");
}

#[tokio::test]
async fn busy_loop_outside_steps_hits_activation_timeout() {
    let rt = runtime(TestClock::at(T0), Duration::from_millis(300));
    let p = prepare(&rt);
    let s = spec("spin", "spin");
    let sink = seeded(&p, &s);
    let started = Instant::now();
    let out = activate(&rt, &p, &sink, &s.run).await;
    assert_eq!(
        out,
        ActivationOutcome::AttemptFailed(AttemptFailure::Timeout)
    );
    assert!(started.elapsed() >= Duration::from_millis(300));
    assert_eq!(sink.journal(&s.run).len(), 1, "nothing journaled");
}

#[tokio::test]
async fn earlier_activation_deadline_wins_over_step_timeout() {
    let rt = runtime(TestClock::at(T0), Duration::from_millis(300));
    let p = prepare(&rt);
    let s = spec("hang-long", "hang:5000");
    let sink = seeded(&p, &s);
    let started = Instant::now();
    let out = activate(&rt, &p, &sink, &s.run).await;
    let took = started.elapsed();
    assert_eq!(
        out,
        ActivationOutcome::AttemptFailed(AttemptFailure::Timeout)
    );
    assert!(took < Duration::from_secs(3), "{took:?}");
    assert_eq!(
        sink.journal(&s.run).len(),
        1,
        "the open step journals nothing"
    );
}
