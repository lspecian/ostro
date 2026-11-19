//! D22 (plan 05-23): the retry logical clock.
//!
//! A step attempt that re-runs after a `StepAttemptFailed` observes logical time
//! `max(last journaled ts, StepAttemptFailed.retry_at_nanos)`. The rule reads only the
//! journal (kind 29 already carries `retry_at_nanos`), so live and replay agree, and the
//! clock never moves backwards: the next journaled event is stamped at least that value.
//!
//! - Engine level, over `guests/sdk-retry` with a [`TestClock`] that only moves when the
//!   test sets it: `long_backoff` (3 s requeue) fails exactly once and succeeds on attempt
//!   2 because the step sees `now - t0 >= 3 s`; `recover_hot` (in-activation retries)
//!   stamps each later attempt at its `retry_at`. Replays append nothing.
//! - Replay-core level ([`ReplayState`]): identical `now()` observations live and on
//!   replay, a `retry_at` earlier than the last ts keeps the last ts, and the hot path.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ostro_engine::config::EngineConfig;
use ostro_engine::env::Clock;
use ostro_engine::error::EvictReason;
use ostro_engine::replay::{ReplayState, StepBegin};
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::testkit::{DEFAULT_ACCEPTED_AT, FakeSink, RunSpec, replay_journal};
use ostro_types::journal::{
    AttemptBackoff, CommandHash, CommandMeta, ErrorInfo, Event, EventBody, Failure, HASH_VER,
    Payload, RESULT_CODEC_VER, RecordedResult, RngSeed, RunStarted, StepAttemptFailed,
    StepCompleted, decode_result,
};
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

fn runtime(clock: Arc<TestClock>) -> Arc<Runtime> {
    Arc::new(
        Runtime::new(
            EngineConfig {
                hot_wait: Duration::from_secs(2),
                ..EngineConfig::default()
            },
            RuntimeOptions {
                clock,
                token_key: [9; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime"),
    )
}

fn prepare(rt: &Runtime) -> Arc<Prepared> {
    Arc::new(
        rt.prepare(&common::guest_wasm("sdk-retry"))
            .expect("prepare sdk-retry"),
    )
}

fn spec(id: &str, input: &str) -> RunSpec {
    RunSpec::new(
        RunRef {
            partition: 5,
            run_id: id.to_string(),
        },
        "sdk-retry",
        input.as_bytes(),
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

/// `(ts, StepAttemptFailed)` for every failed attempt of step `name`.
fn safs(journal: &[Event], name: &str) -> Vec<(u64, StepAttemptFailed)> {
    journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::StepAttemptFailed(f) if f.name == name => Some((e.ts_nanos, f.clone())),
            _ => None,
        })
        .collect()
}

/// `(ts, decoded outcome)` of the single `StepCompleted` of step `name`.
fn completion(journal: &[Event], name: &str) -> (u64, Result<Payload, Failure>) {
    let done: Vec<_> = journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::StepCompleted(sc) if sc.name == name => Some((e.ts_nanos, sc)),
            _ => None,
        })
        .collect();
    assert_eq!(
        done.len(),
        1,
        "exactly one StepCompleted for {name}: {journal:#?}"
    );
    let (ts, sc) = done[0];
    (ts, decode_result(&sc.outcome).expect("decode step outcome"))
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

const LONG_BACKOFF_RESULT: &str = r#"{"mode":"long_backoff","before":1,"attempts":null}"#;

#[tokio::test]
async fn long_backoff_fails_once_and_sees_retry_at() {
    let clock = TestClock::at(T0);
    let rt = runtime(clock.clone());
    let p = prepare(&rt);
    let s = spec("long-backoff", r#"{"mode":"long_backoff"}"#);
    let sink = seeded(&p, &s);

    // Attempt 1 at T0: `slow` sees now - t0 = 0 < 2 s and fails; 3 s > hot_wait -> requeue.
    let out = activate(&rt, &p, &sink, &s.run).await;
    assert_eq!(out, backoff_evicted(T0 + 3 * SEC));
    let f = safs(&sink.journal(&s.run), "slow");
    assert_eq!(f.len(), 1);
    assert_eq!((f[0].0, f[0].1.attempt), (T0, 1));
    assert_eq!(f[0].1.backoff, AttemptBackoff::Requeue);
    assert_eq!(f[0].1.retry_at_nanos, T0 + 3 * SEC);

    // Not due yet: the raised clock must not hide the backoff; re-evicts, appends nothing.
    clock.set(T0 + SEC);
    let before = sink.appended_events(&s.run);
    let out = activate(&rt, &p, &sink, &s.run).await;
    assert_eq!(out, backoff_evicted(T0 + 3 * SEC));
    assert_eq!(sink.appended_events(&s.run), before, "no new events");

    // Due: attempt 2 sees max(last ts = T0, retry_at = T0 + 3 s) -> elapsed 3 s -> success.
    clock.set(T0 + 3 * SEC);
    let out = activate(&rt, &p, &sink, &s.run).await;
    assert_eq!(completed_text(&out), LONG_BACKOFF_RESULT);
    let j = sink.journal(&s.run);
    assert_eq!(
        safs(&j, "slow").len(),
        1,
        "exactly one failed attempt: {j:#?}"
    );
    let (ts, outcome) = completion(&j, "slow");
    assert_eq!(outcome.expect("slow succeeded").data, b"3000");
    assert!(
        ts >= T0 + 3 * SEC,
        "monotonic: completion stamped at or after retry_at"
    );
    for w in j.windows(2) {
        assert!(w[0].ts_nanos <= w[1].ts_nanos, "journal ts never decrease");
    }

    // Replay of the recorded journal (even with the clock far ahead) appends nothing and
    // yields the identical result.
    clock.set(T0 + 3_600 * SEC);
    let (replayed, appended) = replay_journal(&rt, &p, s.run.clone(), j.clone()).await;
    assert_eq!(completed_text(&replayed), LONG_BACKOFF_RESULT);
    assert_eq!(appended, 0);

    // A fresh runtime (as after a restart) replays it identically too.
    let rt2 = runtime(TestClock::at(T0));
    let p2 = prepare(&rt2);
    let (replayed, appended) = replay_journal(&rt2, &p2, s.run.clone(), j).await;
    assert_eq!(completed_text(&replayed), LONG_BACKOFF_RESULT);
    assert_eq!(appended, 0);
}

#[tokio::test]
async fn hot_retries_are_stamped_at_retry_at() {
    // The clock never moves: without D22 every attempt would be stamped T0.
    let rt = runtime(TestClock::at(T0));
    let p = prepare(&rt);
    let s = spec("recover-hot", r#"{"mode":"recover_hot","fail_times":2}"#);
    let sink = seeded(&p, &s);
    let out = activate(&rt, &p, &sink, &s.run).await;
    assert_eq!(
        completed_text(&out),
        r#"{"mode":"recover_hot","before":1,"attempts":3}"#
    );
    let j = sink.journal(&s.run);
    let f = safs(&j, "recover");
    assert_eq!(f.len(), 2, "{j:#?}");
    assert_eq!(f[0].0, T0);
    assert_eq!(f[0].1.retry_at_nanos, T0 + 50 * MS);
    assert!(
        f.iter()
            .all(|(_, s)| s.backoff == AttemptBackoff::InActivation)
    );
    // Attempt 2 ran at max(T0, retry_at 1); its own retry_at is 100 ms after that.
    assert_eq!(f[1].0, T0 + 50 * MS);
    assert_eq!(f[1].1.retry_at_nanos, T0 + 150 * MS);
    let (ts, outcome) = completion(&j, "recover");
    assert_eq!(outcome.expect("recovered").data, b"3");
    assert_eq!(ts, T0 + 150 * MS, "attempt 3 stamped at retry_at 2");

    let (replayed, appended) = replay_journal(&rt, &p, s.run.clone(), j).await;
    assert_eq!(
        completed_text(&replayed),
        r#"{"mode":"recover_hot","before":1,"attempts":3}"#
    );
    assert_eq!(appended, 0);
}

// ---------------------------------------------------------------------------
// Replay core
// ---------------------------------------------------------------------------

const ACCEPTED: u64 = 1_000;

fn fixed(nanos: u64) -> Arc<dyn Clock> {
    TestClock::at(nanos)
}

fn hash() -> CommandHash {
    CommandHash([3; 16])
}

fn meta() -> CommandMeta {
    CommandMeta {
        hash_ver: HASH_VER,
        hash: hash(),
    }
}

fn run_started() -> Event {
    Event {
        seq: 0,
        ts_nanos: ACCEPTED,
        body: EventBody::RunStarted(RunStarted {
            workflow: "wf".into(),
            workflow_version: 1,
            input: Payload {
                content_type: "application/json".into(),
                data: b"1".to_vec(),
            },
            rng_seed: RngSeed([1; 32]),
            accepted_at_nanos: ACCEPTED,
            deadline_nanos: None,
            module_hash: "m".into(),
            tenant: None,
            parent_run_id: None,
            continued_from: None,
        }),
    }
}

fn saf(attempt: u32, retry_at_nanos: u64, backoff: AttemptBackoff) -> EventBody {
    EventBody::StepAttemptFailed(StepAttemptFailed {
        meta: meta(),
        name: "s".into(),
        attempt,
        failure: Failure::Retryable(ErrorInfo {
            kind: "transient".into(),
            message: "x".into(),
            details: None,
        }),
        retry_at_nanos,
        backoff,
        fuel_used: 0,
    })
}

fn step_done() -> EventBody {
    EventBody::StepCompleted(StepCompleted {
        meta: meta(),
        name: "s".into(),
        outcome: RecordedResult {
            codec_ver: RESULT_CODEC_VER,
            bytes: vec![1],
        },
        fuel_used: 0,
    })
}

fn begin(st: &mut ReplayState) -> StepBegin<'_> {
    st.begin_step(hash(), &|| "step \"s\"".to_string())
        .expect("begin step")
}

fn live_seq(st: &mut ReplayState) -> u64 {
    match begin(st) {
        StepBegin::Live { seq, .. } => seq,
        StepBegin::Replayed(ev) => panic!("expected live, got {ev:?}"),
    }
}

/// Live: a requeued attempt resumed after `retry_at` sees `retry_at`; replay of the
/// finished journal observes the same `now()` at every guest-visible point.
#[test]
fn requeued_attempt_now_is_retry_at_and_replay_matches() {
    let retry_at = ACCEPTED + 3 * SEC;
    let journal = vec![
        run_started(),
        Event {
            seq: 1,
            ts_nanos: ACCEPTED + 10,
            body: saf(1, retry_at, AttemptBackoff::Requeue),
        },
    ];
    // The node clock is past retry_at; the attempt is due.
    let mut live = ReplayState::new(journal, fixed(retry_at + 7)).expect("journal");
    let mut seen_live = vec![live.logical_now_nanos()];
    let seq = live_seq(&mut live);
    seen_live.push(live.logical_now_nanos());
    assert_eq!(live.logical_now_nanos(), retry_at, "max(last ts, retry_at)");
    live.complete_step(seq, step_done()).expect("complete");
    seen_live.push(live.logical_now_nanos());
    assert!(live.logical_now_nanos() >= retry_at, "never backwards");
    let recorded = live.journal().to_vec();

    // Replay with a different (earlier) node clock: identical observations, no appends.
    let mut rep = ReplayState::new(recorded, fixed(ACCEPTED)).expect("journal");
    let mut seen_rep = vec![rep.logical_now_nanos()];
    match begin(&mut rep) {
        StepBegin::Replayed(ev) => assert_eq!(ev.seq, seq),
        StepBegin::Live { .. } => panic!("expected replay"),
    }
    seen_rep.push(rep.logical_now_nanos());
    assert!(rep.finish().is_ok());
    assert_eq!(rep.pending_len(), 0);
    // Inside the attempt only the live run observes time (the body is skipped on
    // replay); what both observe before and after the step is identical.
    assert_eq!(seen_live[0], seen_rep[0]);
    assert_eq!(seen_live[2], seen_rep[1]);
}

/// A `retry_at` earlier than the last journaled ts keeps the last ts.
#[test]
fn retry_at_before_last_ts_keeps_last_ts() {
    let last = ACCEPTED + 9 * SEC;
    let journal = vec![
        run_started(),
        Event {
            seq: 1,
            ts_nanos: last,
            body: saf(1, ACCEPTED + 5 * SEC, AttemptBackoff::Requeue),
        },
    ];
    let mut st = ReplayState::new(journal, fixed(last)).expect("journal");
    live_seq(&mut st);
    assert_eq!(st.logical_now_nanos(), last);
}

/// Not due yet: the clock is not raised, so the host still sees the pending backoff.
#[test]
fn attempt_not_due_does_not_raise_the_clock() {
    let retry_at = ACCEPTED + 3 * SEC;
    let journal = vec![
        run_started(),
        Event {
            seq: 1,
            ts_nanos: ACCEPTED + 10,
            body: saf(1, retry_at, AttemptBackoff::Requeue),
        },
    ];
    let mut st = ReplayState::new(journal, fixed(ACCEPTED + SEC)).expect("journal");
    match begin(&mut st) {
        StepBegin::Live {
            seq,
            last_retry_at_nanos,
            ..
        } => {
            assert_eq!(last_retry_at_nanos, Some(retry_at));
            assert!(retry_at > st.live_now_nanos(), "backoff still visible");
            st.abandon_step(seq);
        }
        StepBegin::Replayed(_) => panic!("expected live"),
    }
    assert_eq!(st.logical_now_nanos(), ACCEPTED + 10);
}

/// In-activation retry: after the failed attempt is recorded, the next attempt (run
/// after the host's sleep) sees its `retry_at`, and the completion is stamped at least
/// that, even with a node clock that lags.
#[test]
fn hot_retry_now_is_retry_at() {
    let mut st = ReplayState::new(vec![run_started()], fixed(ACCEPTED + 100)).expect("journal");
    let seq = live_seq(&mut st);
    assert_eq!(st.logical_now_nanos(), ACCEPTED);
    let retry_at = ACCEPTED + 100 + 50 * MS;
    let next = st
        .record_attempt_failed(seq, saf(1, retry_at, AttemptBackoff::InActivation))
        .expect("attempt failed");
    assert_eq!(st.logical_now_nanos(), retry_at);
    st.complete_step(next, step_done()).expect("complete");
    let j = st.journal().to_vec();
    assert_eq!(j[1].ts_nanos, ACCEPTED + 100, "SAF stamped at the clock");
    assert_eq!(j[2].ts_nanos, retry_at, "attempt 2 stamped at retry_at");
    assert_eq!(st.logical_now_nanos(), retry_at);

    // Replay consumes the chain and observes the recorded completion ts.
    let mut rep = ReplayState::new(j, fixed(ACCEPTED)).expect("journal");
    assert!(matches!(begin(&mut rep), StepBegin::Replayed(_)));
    assert_eq!(rep.logical_now_nanos(), retry_at);
    assert!(rep.finish().is_ok());
}
