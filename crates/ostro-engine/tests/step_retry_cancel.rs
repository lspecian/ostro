//! D25 (plan 05-27): the backoff between step retries is a cancellation point, proven on
//! the engine with `guests/sdk-cancel` (`retry_step`, `shielded_retry`).
//!
//! - In-activation: a `CancelRequested` that arrives during the backoff sleep is delivered
//!   to the replay core, the step resolves `StepCompleted(Err(cancelled))` (no further
//!   attempt) and the activation evicts `StepInterrupted`; the re-activation replays the
//!   cancelled step and the run ends cancelled.
//! - A cancel already journaled before a failed attempt stops the first retry.
//! - Requeued: a resumed attempt with a pending cancel resolves as cancelled at
//!   `begin-step`, before its `retry_at`, in the same activation.
//! - Once the guest has observed the cancel (D11), a shielded cleanup step still retries.
//!
//! Every journal replays with zero appends and the same outcome (replay identity): the
//! decision comes only from the journal order of `CancelRequested` and the attempts.

mod common;

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use ostro_engine::config::EngineConfig;
use ostro_engine::error::EvictReason;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::testkit::{DEFAULT_ACCEPTED_AT, FakeClock, FakeSink, RunSpec, replay_journal};
use ostro_types::journal::{
    AttemptBackoff, CancelRequested, Event, EventBody, EventKind, Failure, decode_result,
};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tokio_util::sync::CancellationToken;

fn rt() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        Runtime::new(
            EngineConfig {
                hot_wait: Duration::from_secs(2),
                ..EngineConfig::default()
            },
            RuntimeOptions {
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
                token_key: [5; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime")
    })
}

async fn guest() -> Arc<Prepared> {
    static P: tokio::sync::OnceCell<Arc<Prepared>> = tokio::sync::OnceCell::const_new();
    P.get_or_init(|| async {
        rt().prepare_cached(&common::guest_wasm("sdk-cancel"))
            .await
            .expect("prepare sdk-cancel")
    })
    .await
    .clone()
}

fn run(id: &str) -> RunRef {
    RunRef {
        partition: 2,
        run_id: id.to_string(),
    }
}

fn seeded(p: &Prepared, r: &RunRef, input: &str) -> Arc<FakeSink> {
    let s = RunSpec::new(r.clone(), "sdk-cancel", input.as_bytes());
    let sink = Arc::new(FakeSink::new());
    sink.seed(r.clone(), vec![s.run_started_event(&p.module_hash_hex)], 1);
    sink
}

async fn activate(
    p: &Prepared,
    sink: &Arc<FakeSink>,
    r: &RunRef,
    inputs: Option<UnboundedReceiver<Event>>,
) -> ActivationOutcome {
    rt().activate(
        p,
        Activate {
            run: r.clone(),
            attempt: 1,
            module_sha: p.sha,
            journal: sink.journal(r),
            inputs,
            cancel: CancellationToken::new(),
        },
        sink.clone(),
    )
    .await
}

fn cancel_event(seq: u64, ts_nanos: u64) -> Event {
    Event {
        seq,
        ts_nanos,
        body: EventBody::CancelRequested(CancelRequested {
            reason: "test cancel".to_string(),
        }),
    }
}

/// Commit a `CancelRequested` at the journal end (as apply would); returns it.
fn commit_cancel(sink: &FakeSink, r: &RunRef) -> Event {
    let j = sink.journal(r);
    let ev = cancel_event(j.len() as u64, j.last().map_or(0, |e| e.ts_nanos));
    sink.commit_inputs(r, vec![ev.clone()]);
    ev
}

fn kinds(j: &[Event]) -> Vec<EventKind> {
    j.iter().map(|e| e.body.kind()).collect()
}

fn attempts(j: &[Event], step: &str) -> Vec<(u32, AttemptBackoff)> {
    j.iter()
        .filter_map(|e| match &e.body {
            EventBody::StepAttemptFailed(f) if f.name == step => Some((f.attempt, f.backoff)),
            _ => None,
        })
        .collect()
}

/// The decoded outcomes of every `StepCompleted` named `step`.
fn step_outcomes(j: &[Event], step: &str) -> Vec<Result<Vec<u8>, Failure>> {
    j.iter()
        .filter_map(|e| match &e.body {
            EventBody::StepCompleted(sc) if sc.name == step => Some(
                decode_result::<Result<ostro_types::journal::Payload, Failure>>(&sc.outcome)
                    .expect("decode step outcome")
                    .map(|p| p.data),
            ),
            _ => None,
        })
        .collect()
}

fn interrupted() -> ActivationOutcome {
    ActivationOutcome::Evicted {
        reason: EvictReason::StepInterrupted,
    }
}

fn cancelled_run() -> ActivationOutcome {
    ActivationOutcome::Failed {
        failure: Failure::Cancelled,
    }
}

/// Replay `journal` on a fresh sink: same outcome, nothing appended.
async fn assert_replays(p: &Prepared, r: &RunRef, journal: Vec<Event>, want: &ActivationOutcome) {
    let started = Instant::now();
    let (out, appended) = replay_journal(rt(), p, r.clone(), journal.clone()).await;
    assert_eq!(&out, want, "replay outcome (journal {journal:#?})");
    assert_eq!(appended, 0, "replay appended events (journal {journal:#?})");
    assert!(
        started.elapsed() < Duration::from_millis(150),
        "replay must not sleep a backoff: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn cancel_during_in_activation_backoff_stops_retries_and_run_ends_cancelled() {
    let p = guest().await;
    let r = run("d25-in-activation");
    // 400 ms fixed backoff: in-activation (hot_wait 2 s), long enough to cancel inside it.
    let sink = seeded(&p, &r, r#"{"mode":"retry_step","backoff_ms":400}"#);
    let (tx, rx) = unbounded_channel();
    let task = {
        let (p, sink, r) = (p.clone(), sink.clone(), r.clone());
        tokio::spawn(async move { activate(&p, &sink, &r, Some(rx)).await })
    };
    // Wait for attempt 1's flushed failure, then cancel while the host sleeps the backoff.
    let deadline = Instant::now() + Duration::from_secs(20);
    while attempts(&sink.journal(&r), "flaky").is_empty() {
        assert!(Instant::now() < deadline, "attempt 1 never failed");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let cancel = commit_cancel(&sink, &r);
    tx.send(cancel.clone()).expect("send cancel input");
    let out = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the backoff ends at the cancel")
        .expect("join");
    assert_eq!(out, interrupted());

    let j = sink.journal(&r);
    assert_eq!(
        kinds(&j),
        [
            EventKind::RunStarted,
            EventKind::StepAttemptFailed,
            EventKind::CancelRequested,
            EventKind::StepCompleted,
        ],
        "{j:#?}"
    );
    assert_eq!(attempts(&j, "flaky"), [(1, AttemptBackoff::InActivation)]);
    assert_eq!(step_outcomes(&j, "flaky"), [Err(Failure::Cancelled)]);
    assert_eq!(j[2], cancel);

    // Re-activation: the cancelled step is replayed, the run ends cancelled, and no
    // further attempt ever runs.
    let out = activate(&p, &sink, &r, None).await;
    assert_eq!(out, cancelled_run());
    let j = sink.journal(&r);
    assert_eq!(
        attempts(&j, "flaky").len(),
        1,
        "no attempt after the cancel"
    );
    assert_eq!(j.last().map(|e| e.body.kind()), Some(EventKind::RunFailed));
    assert_replays(&p, &r, j, &cancelled_run()).await;
}

#[tokio::test]
async fn cancel_journaled_before_a_failed_attempt_stops_its_retry() {
    let p = guest().await;
    let r = run("d25-pending");
    let sink = seeded(&p, &r, r#"{"mode":"retry_step","backoff_ms":50}"#);
    commit_cancel(&sink, &r);
    // Attempt 1 still runs (only a retry is a cancellation point); its retry does not.
    let out = activate(&p, &sink, &r, None).await;
    assert_eq!(out, interrupted());
    let j = sink.journal(&r);
    assert_eq!(
        kinds(&j),
        [
            EventKind::RunStarted,
            EventKind::CancelRequested,
            EventKind::StepAttemptFailed,
            EventKind::StepCompleted,
        ],
        "{j:#?}"
    );
    assert_eq!(step_outcomes(&j, "flaky"), [Err(Failure::Cancelled)]);
    // Re-activation: the recorded cancelled step is replayed and the run ends cancelled.
    let out = activate(&p, &sink, &r, None).await;
    assert_eq!(out, cancelled_run());
    let j = sink.journal(&r);
    assert_eq!(attempts(&j, "flaky").len(), 1);
    assert_replays(&p, &r, j, &cancelled_run()).await;
}

#[tokio::test]
async fn requeued_retry_with_pending_cancel_resolves_cancelled_before_retry_at() {
    let p = guest().await;
    let r = run("d25-requeue");
    // 5 s > hot_wait (2 s): the retry is requeued.
    let sink = seeded(&p, &r, r#"{"mode":"retry_step","backoff_ms":5000}"#);
    let out = activate(&p, &sink, &r, None).await;
    let ActivationOutcome::Evicted {
        reason: EvictReason::StepBackoff { .. },
    } = out
    else {
        panic!("expected StepBackoff, got {out:?}");
    };
    assert_eq!(
        attempts(&sink.journal(&r), "flaky"),
        [(1, AttemptBackoff::Requeue)]
    );

    // Cancel during the requeued backoff; the run is re-activated (the clock is still
    // before retry_at). The resumed attempt does not run: the step resolves as cancelled
    // at begin-step and the run ends cancelled in this same activation.
    commit_cancel(&sink, &r);
    let out = activate(&p, &sink, &r, None).await;
    assert_eq!(out, cancelled_run());
    let j = sink.journal(&r);
    assert_eq!(
        kinds(&j),
        [
            EventKind::RunStarted,
            EventKind::StepAttemptFailed,
            EventKind::CancelRequested,
            EventKind::StepCompleted,
            EventKind::RunFailed,
        ],
        "{j:#?}"
    );
    assert_eq!(attempts(&j, "flaky").len(), 1);
    assert_eq!(step_outcomes(&j, "flaky"), [Err(Failure::Cancelled)]);
    assert_replays(&p, &r, j, &cancelled_run()).await;
}

#[tokio::test]
async fn retries_without_a_cancel_are_unchanged() {
    // Control: the same guest without a cancel runs all 10 attempts (9 in-activation
    // retries) and fails with the step's own error.
    let p = guest().await;
    let r = run("d25-control");
    let sink = seeded(&p, &r, r#"{"mode":"retry_step","backoff_ms":1}"#);
    let out = activate(&p, &sink, &r, None).await;
    let ActivationOutcome::Failed {
        failure: Failure::Retryable(e),
    } = &out
    else {
        panic!("expected the transient step failure, got {out:?}");
    };
    assert_eq!(e.kind, "transient");
    let j = sink.journal(&r);
    assert_eq!(attempts(&j, "flaky").len(), 9);
    assert_replays(&p, &r, j, &out).await;
}

#[tokio::test]
async fn shielded_cleanup_step_still_retries_after_the_cancel_was_observed() {
    let p = guest().await;
    let r = run("d25-shielded");
    let sink = seeded(&p, &r, r#"{"mode":"shielded_retry"}"#);
    // Activation 1 parks on the 10 s sleep (no input channel: evicts).
    let out = activate(&p, &sink, &r, None).await;
    assert_eq!(
        out,
        ActivationOutcome::Evicted {
            reason: EvictReason::WaitUnsatisfied
        }
    );
    commit_cancel(&sink, &r);
    // Activation 2: the wait observes the cancel (D11), then the shielded cleanup step
    // fails twice and is retried in-activation to completion.
    let out = activate(&p, &sink, &r, None).await;
    let want = ActivationOutcome::Completed {
        output: ostro_types::journal::Payload {
            content_type: "application/json".to_string(),
            data: br#""cleaned:3""#.to_vec(),
        },
    };
    assert_eq!(out, want);
    let j = sink.journal(&r);
    assert_eq!(
        attempts(&j, "cleanup"),
        [
            (1, AttemptBackoff::InActivation),
            (2, AttemptBackoff::InActivation)
        ],
        "{j:#?}"
    );
    assert_eq!(step_outcomes(&j, "cleanup"), [Ok(b"3".to_vec())]);
    assert_replays(&p, &r, j, &want).await;
}
