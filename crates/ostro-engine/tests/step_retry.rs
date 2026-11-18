//! Host-evaluated step retry (plan 04-07, WF-02) over `guests/retry-probe`.
//!
//! - Short backoff (`delay <= engine.hot_wait`): `StepAttemptFailed(InActivation)` is
//!   journaled and flushed, the host sleeps and `complete` returns `retry-now`; the
//!   guest re-runs the body in the same activation.
//! - Long backoff: `StepAttemptFailed(Requeue)` is flushed and the activation ends
//!   `Evicted { StepBackoff { retry_at_nanos } }`; re-activating before `retry_at`
//!   re-evicts without running the body or appending anything.
//! - Attempt accounting lives in the journal: total attempts equal `max_attempts`, also
//!   across re-activations and runtime restarts.
//! - `Runtime::prepared(sha)` is a pure L2 lookup.
//!
//! All runtimes use a [`TestClock`] that only moves when the test sets it, so `ts` and
//! `retry_at` values are exact.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ostro_engine::config::EngineConfig;
use ostro_engine::env::Clock;
use ostro_engine::error::EvictReason;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::testkit::{DEFAULT_ACCEPTED_AT, FakeSink, RunSpec, replay_journal};
use ostro_types::journal::{
    AttemptBackoff, Event, EventBody, Failure, Payload, StepAttemptFailed, decode_result,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

const MS: u64 = 1_000_000;
const SEC: u64 = 1_000 * MS;
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

fn runtime_with(clock: Arc<TestClock>, cache_dir: Option<std::path::PathBuf>) -> Arc<Runtime> {
    Arc::new(
        Runtime::new(
            EngineConfig {
                hot_wait: Duration::from_secs(2),
                ..EngineConfig::default()
            },
            RuntimeOptions {
                clock,
                token_key: [7; 32],
                cache_dir,
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime"),
    )
}

fn runtime(clock: Arc<TestClock>) -> Arc<Runtime> {
    runtime_with(clock, None)
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
            partition: 3,
            run_id: id.to_string(),
        },
        "retry-probe",
        mode.as_bytes(),
    )
}

/// Seed a fresh sink with the run's `RunStarted`.
fn seeded(p: &Prepared, s: &RunSpec) -> Arc<FakeSink> {
    let sink = Arc::new(FakeSink::new());
    sink.seed(
        s.run.clone(),
        vec![s.run_started_event(&p.module_hash_hex)],
        1,
    );
    sink
}

/// One activation over the sink's committed journal (no hot-wait input channel).
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

/// The decoded outcome of the single `StepCompleted` (panics unless exactly one).
fn step_outcome(journal: &[Event]) -> Result<Payload, Failure> {
    let done: Vec<_> = journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::StepCompleted(sc) => Some(sc),
            _ => None,
        })
        .collect();
    assert_eq!(done.len(), 1, "exactly one StepCompleted: {journal:#?}");
    decode_result(&done[0].outcome).expect("decode step outcome")
}

fn completed_text(o: &ActivationOutcome) -> String {
    match o {
        ActivationOutcome::Completed { output } => String::from_utf8(output.data.clone()).unwrap(),
        other => panic!("not completed: {other:?}"),
    }
}

fn backoff_evicted(at: u64) -> ActivationOutcome {
    ActivationOutcome::Evicted {
        reason: EvictReason::StepBackoff { retry_at_nanos: at },
    }
}

fn kind_of(f: &Failure) -> &str {
    match f {
        Failure::Retryable(e) | Failure::NonRetryable(e) => &e.kind,
        Failure::Timeout => "timeout",
        Failure::Cancelled => "cancelled",
    }
}

#[tokio::test]
async fn exhaust_retries_in_activation_until_max_attempts() {
    let rt = runtime(TestClock::at(T0));
    let p = prepare(&rt);
    let s = spec("exhaust", "exhaust");
    let sink = seeded(&p, &s);

    let started = Instant::now();
    let out = activate(&rt, &p, &sink, &s.run).await;
    let took = started.elapsed();
    assert_eq!(completed_text(&out), "err:retryable:transient");
    assert_eq!(sink.outcomes().len(), 1, "one activation");
    assert!(
        took >= Duration::from_millis(70),
        "slept 10+20+40 ms, took {took:?}"
    );

    let j = sink.journal(&s.run);
    let f = safs(&j);
    assert_eq!(f.len(), 3, "{j:#?}");
    for (i, ((ts, saf), delay)) in f.iter().zip([10 * MS, 20 * MS, 40 * MS]).enumerate() {
        assert_eq!(saf.attempt, i as u32 + 1);
        assert_eq!(saf.name, "probe");
        assert_eq!(saf.backoff, AttemptBackoff::InActivation);
        assert_eq!(saf.retry_at_nanos, ts + delay, "attempt {}", saf.attempt);
        assert_eq!(kind_of(&saf.failure), "transient");
    }
    let outcome = step_outcome(&j);
    assert!(matches!(&outcome, Err(Failure::Retryable(e)) if e.kind == "transient"));
    // RunStarted, SAF x3, StepCompleted, RunCompleted with contiguous seqs.
    assert_eq!(j.len(), 6);
    for (i, e) in j.iter().enumerate() {
        assert_eq!(e.seq, i as u64);
    }
    // Every SAF was flushed before its sleep: 3 SAF batches + the final batch.
    assert_eq!(sink.batches().len(), 4, "{:?}", sink.batches());

    // Replay consumes the attempt chain without sleeping or appending.
    let started = Instant::now();
    let (replayed, appended) = replay_journal(&rt, &p, s.run.clone(), j).await;
    assert_eq!(completed_text(&replayed), "err:retryable:transient");
    assert_eq!(appended, 0);
    assert!(
        started.elapsed() < Duration::from_millis(60),
        "replay must not sleep: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn recover_after_two_failures() {
    let rt = runtime(TestClock::at(T0));
    let p = prepare(&rt);
    let s = spec("recover", "recover:2");
    let sink = seeded(&p, &s);
    let out = activate(&rt, &p, &sink, &s.run).await;
    assert_eq!(completed_text(&out), "ok:recovered after 2");
    let j = sink.journal(&s.run);
    let f = safs(&j);
    assert_eq!(f.iter().map(|(_, s)| s.attempt).collect::<Vec<_>>(), [1, 2]);
    let outcome = step_outcome(&j).expect("step succeeded");
    assert_eq!(outcome.data, b"recovered after 2");
    assert_eq!(sink.outcomes().len(), 1);
}

#[tokio::test]
async fn non_retryable_failures_record_immediately() {
    let rt = runtime(TestClock::at(T0));
    let p = prepare(&rt);
    for (mode, want) in [
        ("fatal", "err:retryable:fatal"),
        ("nonretryable", "err:non-retryable:permanent"),
        ("nopolicy", "err:retryable:transient"),
    ] {
        let s = spec(mode, mode);
        let sink = seeded(&p, &s);
        let out = activate(&rt, &p, &sink, &s.run).await;
        assert_eq!(completed_text(&out), want, "{mode}");
        let j = sink.journal(&s.run);
        assert!(safs(&j).is_empty(), "{mode}: no attempt failures: {j:#?}");
        assert!(step_outcome(&j).is_err(), "{mode}");
        assert!(
            matches!(j[1].body, EventBody::StepCompleted(_)),
            "{mode}: completion right after RunStarted"
        );
    }
}

#[tokio::test]
async fn requeue_evicts_with_step_backoff_and_resumes_after_retry_at() {
    let clock = TestClock::at(T0);
    let rt = runtime(clock.clone());
    let p = prepare(&rt);
    let s = spec("requeue", "requeue");
    let sink = seeded(&p, &s);

    // Attempt 1 fails; 5 s > hot_wait (2 s) -> flushed SAF(Requeue) + StepBackoff.
    let out = activate(&rt, &p, &sink, &s.run).await;
    assert_eq!(out, backoff_evicted(T0 + 5 * SEC));
    let j = sink.journal(&s.run);
    assert_eq!(j.len(), 2, "{j:#?}");
    let f = safs(&j);
    assert_eq!(f.len(), 1);
    assert_eq!(f[0].1.attempt, 1);
    assert_eq!(f[0].1.backoff, AttemptBackoff::Requeue);
    assert_eq!(f[0].1.retry_at_nanos, f[0].0 + 5 * SEC);
    assert_eq!(f[0].0, T0);

    // Re-activation before retry_at: re-evicts without running the body.
    clock.set(T0 + 4 * SEC);
    let before = sink.appended_events(&s.run);
    let out = activate(&rt, &p, &sink, &s.run).await;
    assert_eq!(out, backoff_evicted(T0 + 5 * SEC));
    assert_eq!(sink.appended_events(&s.run), before, "no new events");
    assert_eq!(sink.journal(&s.run), j);

    // At retry_at: attempt 2 runs live and fails again.
    clock.set(T0 + 5 * SEC);
    let out = activate(&rt, &p, &sink, &s.run).await;
    assert_eq!(out, backoff_evicted(T0 + 10 * SEC));
    let f = safs(&sink.journal(&s.run));
    assert_eq!(f.len(), 2);
    assert_eq!(f[1].1.attempt, 2);
    assert_eq!(f[1].0, T0 + 5 * SEC);

    // Attempt 3 == max_attempts: the failure is recorded.
    clock.set(T0 + 10 * SEC);
    let out = activate(&rt, &p, &sink, &s.run).await;
    assert_eq!(completed_text(&out), "err:retryable:transient");
    let j = sink.journal(&s.run);
    assert_eq!(safs(&j).len(), 2, "3 attempts total");
    assert!(step_outcome(&j).is_err());
}

#[tokio::test]
async fn attempt_numbering_survives_runtime_restart() {
    let clock = TestClock::at(T0);
    let s = spec("restart", "requeue");
    let journal = {
        let rt = runtime(clock.clone());
        let p = prepare(&rt);
        let sink = seeded(&p, &s);
        let out = activate(&rt, &p, &sink, &s.run).await;
        assert_eq!(out, backoff_evicted(T0 + 5 * SEC));
        sink.journal(&s.run)
    };

    // A new runtime (as after a node restart) re-activates from the journal alone.
    let clock = TestClock::at(T0 + 6 * SEC);
    let rt = runtime(clock.clone());
    let p = prepare(&rt);
    let sink = Arc::new(FakeSink::new());
    sink.seed(s.run.clone(), journal, 1);
    let out = activate(&rt, &p, &sink, &s.run).await;
    assert_eq!(out, backoff_evicted(T0 + 11 * SEC));
    clock.set(T0 + 11 * SEC);
    let out = activate(&rt, &p, &sink, &s.run).await;
    assert_eq!(completed_text(&out), "err:retryable:transient");

    let j = sink.journal(&s.run);
    let attempts: Vec<u32> = safs(&j).iter().map(|(_, f)| f.attempt).collect();
    assert_eq!(attempts, [1, 2], "numbering continued at 2");
    // 2 failed attempts + the recorded third == max_attempts (3).
    assert_eq!(attempts.len() + 1, 3);
    assert!(step_outcome(&j).is_err());
}

#[tokio::test]
async fn prepared_by_sha_is_a_pure_l2_lookup() {
    let dir = tempfile::tempdir().expect("tempdir");
    let rt = runtime_with(TestClock::at(T0), Some(dir.path().to_path_buf()));
    let wasm = common::guest_wasm("retry-probe");
    let sha: [u8; 32] = Sha256::digest(&wasm).into();

    assert!(rt.prepared(&sha).is_none(), "nothing prepared yet");
    let p = rt.prepare_cached(&wasm).await.expect("prepare_cached");
    assert_eq!(p.sha, sha);
    let stats = rt.cache_stats().expect("cache stats");
    let again = rt.prepared(&sha).expect("cached after prepare_cached");
    assert!(Arc::ptr_eq(&p, &again), "same Arc");
    assert_eq!(
        rt.cache_stats().expect("cache stats"),
        stats,
        "L1 untouched"
    );
    assert!(rt.prepared(&[0; 32]).is_none());
}
