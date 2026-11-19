//! D11 cancellation surfacing in the Rust SDK, proven on the engine (plan 05-06) with the
//! `guests/sdk-cancel` component.
//!
//! The SDK's `Op::wait` issues `wait([op, CANCELLATION], any)`, so the first completion in
//! journal order decides: a `CancelRequested` committed before the op's completion gives
//! `Err(cancelled)`, the op's completion first gives `Ok`. These tests commit the inputs in
//! an explicit order (both in one batch where it matters) instead of relying on the
//! testkit's default input scripting. Code inside `ctx.shielded` waits on the plain op list
//! and runs to completion after a cancel. Every produced journal replays with zero appends.
//!
//! The same guest also covers the rest of the SDK's 05-06 surface at engine level: await
//! tokens (completed, and cancelled while waiting) and continue-as-new.

mod common;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

use ostro_engine::config::EngineConfig;
use ostro_engine::error::EvictReason;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::testkit::{
    DEFAULT_ACCEPTED_AT, FakeClock, FakeSink, Rule, RunSpec, Script, drive_to_completion,
    replay_journal,
};
use ostro_types::journal::{
    CancelRequested, Event, EventBody, EventKind, Failure, OpId, Payload, TimerFired,
};
use tokio_util::sync::CancellationToken;

const PARTITION: u16 = 5;
const RUN_ID: &str = "run-sdk-cancel-1";
/// The WIT-frozen cancellation op id (`effects.cancellation()`).
const CANCELLATION: OpId = u64::MAX;

fn rt() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        Runtime::new(
            EngineConfig::default(),
            RuntimeOptions {
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
                token_key: [6; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime")
    })
}

async fn guest() -> Arc<Prepared> {
    static CACHE: OnceLock<Mutex<HashMap<&'static str, Arc<Prepared>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some(p) = cache.lock().expect("lock").get("sdk-cancel") {
        return p.clone();
    }
    let p = rt()
        .prepare_cached(&common::guest_wasm("sdk-cancel"))
        .await
        .expect("prepare sdk-cancel");
    cache.lock().expect("lock").insert("sdk-cancel", p.clone());
    p
}

fn run() -> RunRef {
    RunRef {
        partition: PARTITION,
        run_id: RUN_ID.to_string(),
    }
}

fn spec(mode: &str) -> RunSpec {
    RunSpec::new(
        run(),
        "sdk-cancel",
        format!(r#"{{"mode":"{mode}"}}"#).as_bytes(),
    )
}

fn output(outcome: &ActivationOutcome, journal: &[Event]) -> String {
    match outcome {
        ActivationOutcome::Completed { output } => {
            String::from_utf8(output.data.clone()).expect("utf8 output")
        }
        other => panic!("expected Completed, got {other:?} (journal: {journal:?})"),
    }
}

/// One activation over `journal`; returns the outcome and the full journal afterwards.
async fn activate(p: &Prepared, journal: Vec<Event>) -> (ActivationOutcome, Vec<Event>) {
    let sink = Arc::new(FakeSink::new());
    sink.seed(run(), journal.clone(), 1);
    let outcome = rt()
        .activate(
            p,
            Activate {
                run: run(),
                attempt: 1,
                module_sha: p.sha,
                journal,
                inputs: None,
                cancel: CancellationToken::new(),
            },
            sink.clone(),
        )
        .await;
    (outcome, sink.journal(&run()))
}

/// Append input `bodies` to `journal` in the given order (one committed batch).
fn commit(journal: &mut Vec<Event>, bodies: Vec<EventBody>) {
    let ts = journal.last().map_or(0, |e| e.ts_nanos);
    for body in bodies {
        journal.push(Event {
            seq: journal.len() as u64,
            ts_nanos: ts,
            body,
        });
    }
}

fn cancel_requested() -> EventBody {
    EventBody::CancelRequested(CancelRequested {
        reason: "test cancel".to_string(),
    })
}

/// Ops of every `TimerStarted` in `journal` that no event completes yet.
fn open_timers(journal: &[Event]) -> Vec<OpId> {
    let done: HashSet<OpId> = journal
        .iter()
        .filter_map(|e| e.body.completes_op())
        .collect();
    journal
        .iter()
        .filter(|e| e.body.kind() == EventKind::TimerStarted && !done.contains(&e.seq))
        .map(|e| e.seq)
        .collect()
}

fn fire(op: OpId) -> EventBody {
    EventBody::TimerFired(TimerFired { op })
}

fn seq_of(journal: &[Event], kind: EventKind) -> Vec<u64> {
    journal
        .iter()
        .filter(|e| e.body.kind() == kind)
        .map(|e| e.seq)
        .collect()
}

fn resolved_lists(journal: &[Event]) -> Vec<Vec<OpId>> {
    journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::WaitResolved(w) => Some(w.completed.clone()),
            _ => None,
        })
        .collect()
}

fn assert_evicted(outcome: &ActivationOutcome, journal: &[Event]) {
    assert_eq!(
        *outcome,
        ActivationOutcome::Evicted {
            reason: EvictReason::WaitUnsatisfied
        },
        "journal: {journal:?}"
    );
}

/// Replaying `journal` gives `outcome` again and appends nothing.
async fn assert_replays(p: &Prepared, outcome: &ActivationOutcome, journal: &[Event]) {
    let (again, appended) = replay_journal(rt(), p, run(), journal.to_vec()).await;
    assert_eq!(&again, outcome, "replay outcome must be identical");
    assert_eq!(appended, 0, "replay must append nothing");
}

/// `wait_sleep` to its first eviction: `[RunStarted, TimerStarted]`, parked on
/// `wait([timer, CANCELLATION], any)`.
async fn wait_sleep_parked(p: &Prepared) -> (Vec<Event>, OpId) {
    let (outcome, journal) = activate(
        p,
        vec![spec("wait_sleep").run_started_event(&p.module_hash_hex)],
    )
    .await;
    assert_evicted(&outcome, &journal);
    let timers = open_timers(&journal);
    assert_eq!(timers.len(), 1, "{journal:?}");
    (journal, timers[0])
}

#[tokio::test]
async fn cancel_before_timer_fired_is_cancelled() {
    let p = guest().await;
    let (mut journal, timer) = wait_sleep_parked(&p).await;
    // Both inputs in one batch; the cancel is first in journal order.
    commit(&mut journal, vec![cancel_requested(), fire(timer)]);
    let (outcome, journal) = activate(&p, journal).await;
    assert_eq!(output(&outcome, &journal), r#""cancelled""#);
    // The wait resolved with the completions in journal order, cancellation first.
    assert_eq!(resolved_lists(&journal), vec![vec![CANCELLATION, timer]]);
    assert_replays(&p, &outcome, &journal).await;
}

#[tokio::test]
async fn timer_fired_before_cancel_is_slept() {
    let p = guest().await;
    let (mut journal, timer) = wait_sleep_parked(&p).await;
    commit(&mut journal, vec![fire(timer), cancel_requested()]);
    let (outcome, journal) = activate(&p, journal).await;
    assert_eq!(output(&outcome, &journal), r#""slept""#);
    assert_eq!(resolved_lists(&journal), vec![vec![timer, CANCELLATION]]);
    assert_replays(&p, &outcome, &journal).await;
}

#[tokio::test]
async fn timer_alone_is_slept() {
    let p = guest().await;
    let (mut journal, timer) = wait_sleep_parked(&p).await;
    commit(&mut journal, vec![fire(timer)]);
    let (outcome, journal) = activate(&p, journal).await;
    assert_eq!(output(&outcome, &journal), r#""slept""#);
    assert_eq!(resolved_lists(&journal), vec![vec![timer]]);
    assert_replays(&p, &outcome, &journal).await;
}

#[tokio::test]
async fn shielded_cleanup_runs_after_cancel() {
    let p = guest().await;
    let (outcome, mut journal) = activate(
        &p,
        vec![spec("shielded_sleep").run_started_event(&p.module_hash_hex)],
    )
    .await;
    assert_evicted(&outcome, &journal);
    let work = open_timers(&journal);
    assert_eq!(work.len(), 1);

    commit(&mut journal, vec![cancel_requested()]);
    let (outcome, mut journal) = activate(&p, journal).await;
    // Work wait -> cancelled; the unshielded probe wait resolves at once with the
    // cancellation; the shielded cleanup wait parks on its timer alone.
    assert_evicted(&outcome, &journal);
    let cancel_seq = seq_of(&journal, EventKind::CancelRequested)[0];
    let timers = seq_of(&journal, EventKind::TimerStarted);
    assert_eq!(timers.len(), 3, "work, probe, cleanup: {journal:?}");
    let (probe, cleanup) = (timers[1], timers[2]);
    assert!(probe > cancel_seq && cleanup > cancel_seq, "{journal:?}");
    assert_eq!(
        resolved_lists(&journal),
        vec![vec![CANCELLATION], vec![CANCELLATION]],
        "work and probe waits both resolve on the cancellation"
    );

    let open = open_timers(&journal);
    assert_eq!(open, vec![work[0], probe, cleanup]);
    commit(&mut journal, vec![fire(cleanup)]);
    let (outcome, journal) = activate(&p, journal).await;
    assert_eq!(output(&outcome, &journal), r#""cleaned""#);
    // The shielded wait resolved on the cleanup timer alone (no cancellation op waited).
    assert_eq!(resolved_lists(&journal).last(), Some(&vec![cleanup]));
    assert_replays(&p, &outcome, &journal).await;
}

#[tokio::test]
async fn select_loser_cancel_completes_with_cancelled() {
    let p = guest().await;
    let (outcome, mut journal) = activate(
        &p,
        vec![spec("select_loser").run_started_event(&p.module_hash_hex)],
    )
    .await;
    assert_evicted(&outcome, &journal);
    let timers = open_timers(&journal);
    assert_eq!(timers.len(), 2);
    let (short, long) = (timers[0], timers[1]);

    commit(&mut journal, vec![fire(short)]);
    let (outcome, journal) = activate(&p, journal).await;
    assert_eq!(output(&outcome, &journal), r#""loser_cancelled""#);
    // The loser was completed by the guest's cancel-op, not by its timer.
    let cancelled: Vec<OpId> = journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::OpCancelled(c) => Some(c.op),
            _ => None,
        })
        .collect();
    assert_eq!(cancelled, vec![long]);
    assert_eq!(resolved_lists(&journal), vec![vec![short], vec![long]]);
    assert_replays(&p, &outcome, &journal).await;
}

#[tokio::test]
async fn await_token_completes_with_json_payload() {
    let p = guest().await;
    let payload = Payload {
        content_type: "application/json".to_string(),
        data: br#"{"ok":true}"#.to_vec(),
    };
    let d = drive_to_completion(
        rt(),
        &p,
        spec("token"),
        Script::new().rule(Rule::CompleteTokens(payload)),
    )
    .await;
    assert_eq!(output(&d.outcome, &d.journal), r#""token:{\"ok\":true}""#);
    assert_eq!(seq_of(&d.journal, EventKind::AwaitTokenCreated).len(), 1);
    assert_replays(&p, &d.outcome, &d.journal).await;
}

#[tokio::test]
async fn await_token_wait_observes_cancellation() {
    let p = guest().await;
    // No token completion; a cancel arrives after the first activation.
    let d = drive_to_completion(
        rt(),
        &p,
        spec("token"),
        Script::new().rule(Rule::CancelAfterActivations(1)),
    )
    .await;
    assert_eq!(
        d.outcome,
        ActivationOutcome::Failed {
            failure: Failure::Cancelled
        },
        "journal: {:?}",
        d.journal
    );
    assert_replays(&p, &d.outcome, &d.journal).await;
}

#[tokio::test]
async fn continue_as_new_returns_the_continuation() {
    let p = guest().await;
    let d = drive_to_completion(rt(), &p, spec("continue"), Script::new()).await;
    match &d.outcome {
        ActivationOutcome::ContinuedAsNew { continuation } => {
            assert_eq!(continuation.input.content_type, "application/json");
            assert_eq!(continuation.input.data, br#"{"mode":"wait_sleep"}"#);
            assert_eq!(
                (&continuation.workflow, continuation.version),
                (&None, None)
            );
        }
        other => panic!("expected ContinuedAsNew, got {other:?}"),
    }
    assert_eq!(seq_of(&d.journal, EventKind::RunContinuedAsNew).len(), 1);
    assert_replays(&p, &d.outcome, &d.journal).await;

    let d = drive_to_completion(rt(), &p, spec("continue_as"), Script::new()).await;
    match &d.outcome {
        ActivationOutcome::ContinuedAsNew { continuation } => {
            assert_eq!(continuation.input.data, br#"{"mode":"token"}"#);
            assert_eq!(
                (continuation.workflow.as_deref(), continuation.version),
                (Some("sdk-cancel"), Some(1))
            );
        }
        other => panic!("expected ContinuedAsNew, got {other:?}"),
    }
    assert_replays(&p, &d.outcome, &d.journal).await;
}
