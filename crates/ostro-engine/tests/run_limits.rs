//! Run limits on the engine (plan 11-06, OPS-07 / D-P11-13), with the `guests/p11-limits`
//! component.
//!
//! Limits are checked only on live appends: a live append past `max_history_events`,
//! `max_children_per_run` or `max_payload_bytes` ends the attempt with
//! `AttemptFailure::LimitExceeded` (non-retryable) and appends nothing, while a journal
//! recorded under the default limits replays green under lowered limits with 0 appended
//! events. Terminal and failure records are exempt from the history limit (W8).

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use ostro_engine::RunLimits;
use ostro_engine::config::EngineConfig;
use ostro_engine::error::{AttemptFailure, EvictReason};
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::testkit::{DEFAULT_ACCEPTED_AT, FakeClock, FakeSink, RunSpec, replay_journal};
use ostro_types::journal::{Event, EventBody, EventKind};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

const PARTITION: u16 = 11;
const RUN_ID: &str = "run-p11-limits-1";
const GUEST: &str = "p11-limits";
const MIB: u64 = 1024 * 1024;

fn small_limits() -> RunLimits {
    RunLimits {
        max_history_events: 20,
        max_children_per_run: 3,
        max_payload_bytes: MIB,
    }
}

fn new_rt(limits: RunLimits) -> Runtime {
    Runtime::new(
        EngineConfig {
            run_limits: limits,
            ..EngineConfig::default()
        },
        RuntimeOptions {
            clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 1_000)),
            token_key: [11; 32],
            ..RuntimeOptions::default()
        },
    )
    .expect("runtime")
}

/// Runtime with the default limits.
fn default_rt() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| new_rt(RunLimits::default()))
}

/// Runtime with history 20, children 3, payload 1 MiB.
fn small_rt() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| new_rt(small_limits()))
}

async fn guest(rt: &'static Runtime) -> Arc<Prepared> {
    static CACHE: OnceLock<Mutex<HashMap<usize, Arc<Prepared>>>> = OnceLock::new();
    let key = rt as *const Runtime as usize;
    let cache = CACHE.get_or_init(Default::default);
    if let Some(p) = cache.lock().expect("lock").get(&key) {
        return p.clone();
    }
    let p = rt
        .prepare_cached(&common::guest_wasm(GUEST))
        .await
        .unwrap_or_else(|e| panic!("prepare {GUEST}: {e:?}"));
    cache.lock().expect("lock").insert(key, p.clone());
    p
}

fn run() -> RunRef {
    RunRef {
        partition: PARTITION,
        run_id: RUN_ID.to_string(),
    }
}

fn start(p: &Prepared, workflow: &str, input: &Value) -> Vec<Event> {
    let spec = RunSpec::new(run(), workflow, input.to_string().as_bytes());
    vec![spec.run_started_event(&p.module_hash_hex)]
}

/// Activate over `journal` until the attempt ends (re-activating after step-retry
/// evictions); returns the outcome and the full journal.
async fn drive(rt: &'static Runtime, mut journal: Vec<Event>) -> (ActivationOutcome, Vec<Event>) {
    let p = guest(rt).await;
    for _ in 0..8 {
        let sink = Arc::new(FakeSink::new());
        sink.seed(run(), journal.clone(), 1);
        let outcome = rt
            .activate(
                &p,
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
        journal = sink.journal(&run());
        match outcome {
            ActivationOutcome::Evicted {
                reason: EvictReason::StepBackoff { .. } | EvictReason::StepInterrupted,
            } => continue,
            other => return (other, journal),
        }
    }
    panic!("run did not settle: {journal:#?}");
}

async fn run_on(
    rt: &'static Runtime,
    workflow: &str,
    input: Value,
) -> (ActivationOutcome, Vec<Event>) {
    let p = guest(rt).await;
    drive(rt, start(&p, workflow, &input)).await
}

fn breach(limit: &'static str, max: u64, actual: u64) -> ActivationOutcome {
    ActivationOutcome::AttemptFailed(AttemptFailure::LimitExceeded { limit, max, actual })
}

fn count(journal: &[Event], kind: EventKind) -> usize {
    journal.iter().filter(|e| e.body.kind() == kind).count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_breach_at_first_live_append_past_the_limit() {
    let (outcome, journal) = run_on(small_rt(), "limits-history", json!({"n": 30})).await;
    assert_eq!(outcome, breach("history_events", 20, 21), "{journal:#?}");
    assert_eq!(journal.len(), 20, "the breaching event is never appended");
    match &outcome {
        ActivationOutcome::AttemptFailed(f) => {
            assert!(!f.retryable());
            assert_eq!(f.reason_code(), "limit_exceeded");
            assert_eq!(f.dlq_reason(), "limit_exceeded: history_events 21 > 20");
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_and_failure_records_are_exempt_w8() {
    // 1 + 19 steps = 20 events; the terminal is the 21st.
    let (outcome, journal) = run_on(small_rt(), "limits-history", json!({"n": 19})).await;
    assert!(
        matches!(outcome, ActivationOutcome::Completed { .. }),
        "{outcome:?}"
    );
    assert_eq!(journal.len(), 21);
    assert_eq!(journal[20].body.kind(), EventKind::RunCompleted);

    let (outcome, journal) = run_on(
        small_rt(),
        "limits-history",
        json!({"n": 19, "end": "fail"}),
    )
    .await;
    assert!(
        matches!(outcome, ActivationOutcome::Failed { .. }),
        "{outcome:?}"
    );
    assert_eq!(journal.len(), 21);
    assert_eq!(journal[20].body.kind(), EventKind::RunFailed);

    // StepAttemptFailed at seq 20 is exempt; the step's closing StepCompleted is not.
    let (outcome, journal) = run_on(
        small_rt(),
        "limits-history",
        json!({"n": 19, "end": "flaky"}),
    )
    .await;
    assert_eq!(outcome, breach("history_events", 20, 22), "{journal:#?}");
    assert_eq!(journal.len(), 21);
    assert_eq!(journal[20].body.kind(), EventKind::StepAttemptFailed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn children_breach() {
    let (outcome, journal) = run_on(small_rt(), "limits-children", json!({"n": 5})).await;
    assert_eq!(outcome, breach("children_per_run", 3, 4), "{journal:#?}");
    assert_eq!(count(&journal, EventKind::ChildStarted), 3);
    let (outcome, _) = run_on(small_rt(), "limits-children", json!({"n": 3})).await;
    assert!(
        matches!(outcome, ActivationOutcome::Completed { .. }),
        "{outcome:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn payload_breaches_step_output_and_run_result() {
    let two = 2 * MIB;
    let (outcome, journal) = run_on(small_rt(), "limits-big-output", json!({"bytes": two})).await;
    assert_eq!(outcome, breach("payload_bytes", MIB, two));
    assert_eq!(count(&journal, EventKind::StepCompleted), 0);

    let (outcome, journal) = run_on(small_rt(), "limits-big-result", json!({"bytes": two})).await;
    assert_eq!(outcome, breach("payload_bytes", MIB, two));
    assert_eq!(journal.len(), 1);

    // Exactly at the limit is allowed.
    let (outcome, _) = run_on(small_rt(), "limits-big-output", json!({"bytes": MIB})).await;
    assert!(
        matches!(outcome, ActivationOutcome::Completed { .. }),
        "{outcome:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_limit_journals_replay_under_lowered_limits() {
    let small = guest(small_rt()).await;
    for (workflow, input) in [
        ("limits-history", json!({"n": 30})),
        ("limits-children", json!({"n": 5})),
        ("limits-big-output", json!({"bytes": 2 * MIB})),
        ("limits-big-result", json!({"bytes": 2 * MIB})),
    ] {
        let (outcome, journal) = run_on(default_rt(), workflow, input).await;
        assert!(
            matches!(outcome, ActivationOutcome::Completed { .. }),
            "{workflow}: {outcome:?}"
        );
        let (again, appended) = replay_journal(small_rt(), &small, run(), journal.clone()).await;
        assert_eq!(
            again, outcome,
            "{workflow}: replay outcome must be identical"
        );
        assert_eq!(appended, 0, "{workflow}: replay must append nothing");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replayed_journal_at_the_limit_fails_at_its_next_live_append() {
    let (outcome, journal) = run_on(default_rt(), "limits-history", json!({"n": 30})).await;
    assert!(matches!(outcome, ActivationOutcome::Completed { .. }));
    // 25 events: RunStarted + s0..s23 replay; s24 is the first live append (seq 25).
    let prefix: Vec<Event> = journal[..25].to_vec();
    assert!(matches!(prefix[24].body, EventBody::StepCompleted(_)));
    let (outcome, after) = drive(small_rt(), prefix).await;
    assert_eq!(outcome, breach("history_events", 20, 26));
    assert_eq!(after.len(), 25, "nothing appended");

    // Raising the limit again resumes the same journal to completion.
    let (outcome, after) = drive(default_rt(), journal[..25].to_vec()).await;
    assert!(
        matches!(outcome, ActivationOutcome::Completed { .. }),
        "{outcome:?}"
    );
    assert_eq!(after.len(), journal.len());
}
