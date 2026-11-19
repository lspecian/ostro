//! WF-08 at engine level (plan 05-03, D6): `patched(change-id)` versioning over three
//! builds of workflow `patch-demo` v1 (`guests/patch-v{1,2,3}`).
//!
//! - v1: step A, sleep 2 s, step B.
//! - v2: v1 plus step C behind `ctx.patched("add-c")`, queried twice (memo).
//! - v3: v2 with the `patched` call removed; C always runs.
//!
//! Proven here: a v1 journal (complete, or parked mid-sleep) replayed by v2 takes the
//! old path without a marker; a fresh v2 run journals exactly one `PatchMarker` and
//! takes the new path; a v2 journal replays on v2 with zero new events and on v3 with
//! its unqueried marker skipped. A v1 journal on v3 is a real divergence.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use ostro_engine::config::EngineConfig;
use ostro_engine::error::{AttemptFailure, EvictReason};
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::testkit::{
    DEFAULT_ACCEPTED_AT, Drive, FakeClock, FakeSink, RunSpec, Script, drive_to_completion,
};
use ostro_types::journal::{Event, EventBody, EventKind, TimerFired};
use tokio_util::sync::CancellationToken;

const PARTITION: u16 = 2;
const RUN_ID: &str = "run-patch-1";
const OLD_PATH: &str = r#"["A","B"]"#;
const NEW_PATH: &str = r#"["A","C","B"]"#;

fn rt() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        Runtime::new(
            EngineConfig::default(),
            RuntimeOptions {
                // Frozen clock: journals depend only on inputs, not activation count.
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
                token_key: [5; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime")
    })
}

async fn guest(name: &str) -> Arc<Prepared> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<Prepared>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some(p) = cache.lock().expect("lock").get(name) {
        return p.clone();
    }
    let p = rt()
        .prepare_cached(&common::guest_wasm(name))
        .await
        .unwrap_or_else(|e| panic!("prepare {name}: {e}"));
    cache
        .lock()
        .expect("lock")
        .insert(name.to_string(), p.clone());
    p
}

fn run() -> RunRef {
    RunRef {
        partition: PARTITION,
        run_id: RUN_ID.to_string(),
    }
}

fn spec() -> RunSpec {
    RunSpec::new(run(), "patch-demo", b"{}")
}

fn output(outcome: &ActivationOutcome, journal: &[Event]) -> String {
    match outcome {
        ActivationOutcome::Completed { output } => {
            String::from_utf8(output.data.clone()).expect("utf8 output")
        }
        other => panic!("expected Completed, got {other:?} (journal: {journal:?})"),
    }
}

fn markers(events: &[Event]) -> usize {
    events
        .iter()
        .filter(|e| e.body.kind() == EventKind::PatchMarker)
        .count()
}

/// One activation of `p` over `journal`; returns the outcome and the appended events.
async fn activate(p: &Prepared, journal: Vec<Event>) -> (ActivationOutcome, Vec<Event>) {
    let sink = Arc::new(FakeSink::new());
    let start = journal.len();
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
    let appended = sink
        .journal(&run())
        .get(start..)
        .unwrap_or_default()
        .to_vec();
    (outcome, appended)
}

/// v1 run to completion (timers fire on eviction).
async fn v1_complete() -> Drive {
    let d = drive_to_completion(rt(), &*guest("patch-v1").await, spec(), Script::new()).await;
    assert_eq!(output(&d.outcome, &d.journal), OLD_PATH);
    d
}

/// A run of `module` parked mid-sleep: `[RunStarted, A, TimerStarted]` (no fire).
async fn parked_mid_sleep(module: &str) -> Vec<Event> {
    let d = drive_to_completion(
        rt(),
        &*guest(module).await,
        spec(),
        Script::new().fire_timers(false),
    )
    .await;
    assert_eq!(
        d.outcome,
        ActivationOutcome::Evicted {
            reason: EvictReason::WaitUnsatisfied
        }
    );
    let last = d.journal.last().expect("journal");
    assert_eq!(last.body.kind(), EventKind::TimerStarted, "{:?}", d.journal);
    d.journal
}

/// `journal` with the `TimerFired` for its last `TimerStarted` appended.
fn with_timer_fired(mut journal: Vec<Event>) -> Vec<Event> {
    let (op, fire_at) = journal
        .iter()
        .rev()
        .find_map(|e| match &e.body {
            EventBody::TimerStarted(t) => Some((e.seq, t.fire_at_nanos)),
            _ => None,
        })
        .expect("timer started");
    journal.push(Event {
        seq: journal.len() as u64,
        ts_nanos: fire_at,
        body: EventBody::TimerFired(TimerFired { op }),
    });
    journal
}

#[tokio::test]
async fn v1_journal_replayed_by_v2_takes_old_path() {
    let v2 = guest("patch-v2").await;

    // Complete v1 journal: same outcome, nothing appended, no nondeterminism.
    let d = v1_complete().await;
    assert_eq!(markers(&d.journal), 0);
    let (outcome, appended) = activate(&v2, d.journal.clone()).await;
    assert_eq!(outcome, d.outcome, "v2 replays the v1 journal exactly");
    assert!(appended.is_empty(), "appended {appended:?}");

    // v1 parked mid-sleep, resumed by v2 after the timer fired: old path, no marker.
    let parked = parked_mid_sleep("patch-v1").await;
    let (outcome, appended) = activate(&v2, with_timer_fired(parked.clone())).await;
    assert_eq!(output(&outcome, &appended), OLD_PATH);
    assert_eq!(markers(&appended), 0, "appended {appended:?}");
    assert_eq!(
        appended.iter().map(|e| e.body.kind()).collect::<Vec<_>>(),
        [
            EventKind::WaitResolved,
            EventKind::StepCompleted,
            EventKind::RunCompleted
        ]
    );

    // Still parked (no fire): v2 waits again without appending anything.
    let (outcome, appended) = activate(&v2, parked).await;
    assert_eq!(
        outcome,
        ActivationOutcome::Evicted {
            reason: EvictReason::WaitUnsatisfied
        }
    );
    assert!(appended.is_empty(), "appended {appended:?}");
}

#[tokio::test]
async fn fresh_v2_takes_new_path_with_one_marker() {
    let v2 = guest("patch-v2").await;
    let d = drive_to_completion(rt(), &v2, spec(), Script::new()).await;
    assert_eq!(output(&d.outcome, &d.journal), NEW_PATH);
    assert_eq!(markers(&d.journal), 1, "memo: one marker for two queries");
    let marker = d
        .journal
        .iter()
        .find_map(|e| match &e.body {
            EventBody::PatchMarker(m) => Some(m.change_id.clone()),
            _ => None,
        })
        .expect("marker");
    assert_eq!(marker, "add-c");

    // v2 journal replayed by v2: zero new events.
    let (outcome, appended) = activate(&v2, d.journal.clone()).await;
    assert_eq!(outcome, d.outcome);
    assert!(appended.is_empty(), "appended {appended:?}");
}

#[tokio::test]
async fn v2_journal_replayed_by_v3_skips_unqueried_marker() {
    let v2 = guest("patch-v2").await;
    let v3 = guest("patch-v3").await;

    // Complete v2 journal on v3.
    let d = drive_to_completion(rt(), &v2, spec(), Script::new()).await;
    assert_eq!(output(&d.outcome, &d.journal), NEW_PATH);
    let (outcome, appended) = activate(&v3, d.journal.clone()).await;
    assert_eq!(outcome, d.outcome, "no divergence: the marker is skipped");
    assert!(appended.is_empty(), "appended {appended:?}");

    // v2 parked mid-sleep, resumed by v3.
    let parked = parked_mid_sleep("patch-v2").await;
    assert_eq!(markers(&parked), 1);
    let (outcome, appended) = activate(&v3, with_timer_fired(parked)).await;
    assert_eq!(output(&outcome, &appended), NEW_PATH);
    assert_eq!(markers(&appended), 0);
}

/// Control: v3 dropped the old path, so a v1 journal is a genuine divergence (the
/// skip rule only forgives markers, never missing or extra commands).
#[tokio::test]
async fn v1_journal_on_v3_is_nondeterminism() {
    let v3 = guest("patch-v3").await;
    let d = v1_complete().await;
    let (outcome, appended) = activate(&v3, d.journal).await;
    assert!(
        matches!(
            outcome,
            ActivationOutcome::AttemptFailed(AttemptFailure::NonDeterminism(_))
        ),
        "{outcome:?}"
    );
    assert!(appended.is_empty(), "appended {appended:?}");
}
