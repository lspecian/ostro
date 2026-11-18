//! Activation driver + FakeSink testkit (plan 03-08): runs complete through
//! evict/replay cycles, replay appends nothing, and fencing, conflicts,
//! nondeterminism, guest failure, panic and exit produce the documented outcomes
//! (ENG-03, ENG-05, ENG-09, WIT-05, WIT-06).

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use ostro_engine::config::EngineConfig;
use ostro_engine::error::{AttemptFailure, EvictReason};
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::testkit::{
    DEFAULT_ACCEPTED_AT, Drive, FakeClock, FakeSink, Rule, RunSpec, Script, drive_to_completion,
    replay_journal,
};
use ostro_types::journal::{EventBody, EventKind, Failure};
use tokio_util::sync::CancellationToken;

fn runtime_with_step(step_nanos: u64) -> Runtime {
    Runtime::new(
        EngineConfig::default(),
        RuntimeOptions {
            clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, step_nanos)),
            token_key: [9; 32],
        },
    )
    .expect("runtime")
}

struct Shared {
    rt: Runtime,
    prepared: Mutex<HashMap<String, Arc<Prepared>>>,
}

/// Runtime shared by tests that do not depend on clock call counts or the budget.
fn shared() -> &'static Shared {
    static S: OnceLock<Shared> = OnceLock::new();
    S.get_or_init(|| Shared {
        rt: runtime_with_step(1_000_000),
        prepared: Mutex::new(HashMap::new()),
    })
}

fn prepared(guest: &str) -> Arc<Prepared> {
    let s = shared();
    let mut map = s.prepared.lock().expect("lock");
    map.entry(guest.to_string())
        .or_insert_with(|| {
            Arc::new(
                s.rt.prepare(&common::guest_wasm(guest))
                    .unwrap_or_else(|e| panic!("prepare {guest}: {e}")),
            )
        })
        .clone()
}

fn run_ref(id: &str) -> RunRef {
    RunRef {
        partition: 3,
        run_id: id.to_string(),
    }
}

fn kinds(d: &Drive) -> Vec<EventKind> {
    d.journal.iter().map(|e| e.body.kind()).collect()
}

async fn drive_det(rt: &Runtime, p: &Prepared, id: &str, script: Script) -> Drive {
    drive_to_completion(rt, p, RunSpec::new(run_ref(id), "det", b""), script).await
}

#[tokio::test]
async fn activation_det_basic_completes() {
    let p = prepared("det-basic");
    let d = drive_det(&shared().rt, &p, "det-1", Script::new()).await;
    assert!(
        matches!(d.outcome, ActivationOutcome::Completed { .. }),
        "{:?}",
        d.outcome
    );
    assert_eq!(
        d.activations, 2,
        "evicted at the sleep, completed after the fire"
    );
    assert_eq!(
        d.outcomes[0],
        ActivationOutcome::Evicted {
            reason: EvictReason::WaitUnsatisfied
        }
    );
    assert_eq!(
        d.journal.last().map(|e| e.body.kind()),
        Some(EventKind::RunCompleted)
    );
    assert!(kinds(&d).contains(&EventKind::TimerFired));
    assert_eq!(d.latest_status.as_deref(), Some("{\"phase\":\"done\"}"));
    assert_eq!(d.appended_batches, 2);
}

#[tokio::test]
async fn activation_replay_zero_appends() {
    let s = shared();
    let p = prepared("det-basic");
    let d = drive_det(&s.rt, &p, "det-replay", Script::new()).await;
    let ActivationOutcome::Completed { output } = &d.outcome else {
        panic!("{:?}", d.outcome);
    };
    let (outcome, appended) = replay_journal(&s.rt, &p, run_ref("det-replay"), d.journal).await;
    assert_eq!(
        outcome,
        ActivationOutcome::Completed {
            output: output.clone()
        }
    );
    assert_eq!(appended, 0);
}

#[tokio::test]
async fn activation_evict_store_dropped_budget_released() {
    // Own runtime: the node memory budget is engine-wide.
    let rt = runtime_with_step(1_000_000);
    let p = rt
        .prepare(&common::guest_wasm("det-basic"))
        .expect("prepare");
    let budget = rt.engine().budget().clone();
    let run = run_ref("det-budget");
    let spec = RunSpec::new(run.clone(), "det", b"");
    let sink = Arc::new(FakeSink::new());
    sink.seed(
        run.clone(),
        vec![spec.run_started_event(&p.module_hash_hex)],
        1,
    );
    for i in 0..2 {
        let before = budget.used();
        let outcome = rt
            .activate(
                &p,
                Activate {
                    run: run.clone(),
                    attempt: 1,
                    module_sha: p.sha,
                    journal: sink.journal(&run),
                    inputs: None,
                    cancel: CancellationToken::new(),
                },
                sink.clone(),
            )
            .await;
        assert_eq!(budget.used(), before, "activation {i}: budget released");
        match outcome {
            ActivationOutcome::Evicted { .. } => {
                // Fire the timer like the drive loop would.
                let j = sink.journal(&run);
                let timer = j
                    .iter()
                    .find(|e| e.body.kind() == EventKind::TimerStarted)
                    .expect("timer");
                let EventBody::TimerStarted(t) = &timer.body else {
                    unreachable!()
                };
                sink.commit_inputs(
                    &run,
                    vec![ostro_types::journal::Event {
                        seq: j.len() as u64,
                        ts_nanos: t.fire_at_nanos,
                        body: EventBody::TimerFired(ostro_types::journal::TimerFired {
                            op: timer.seq,
                        }),
                    }],
                );
            }
            ActivationOutcome::Completed { .. } => break,
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(budget.used(), 0);
}

#[tokio::test]
async fn activation_fenced() {
    let p = prepared("det-basic");
    let d = drive_det(
        &shared().rt,
        &p,
        "det-fenced",
        Script::new().rule(Rule::FenceOnAppend(1)),
    )
    .await;
    assert_eq!(d.outcome, ActivationOutcome::Fenced);
    assert_eq!(
        d.outcomes,
        vec![ActivationOutcome::Fenced],
        "sink.done recorded it"
    );
    assert_eq!(d.journal.len(), 1, "nothing appended");
}

#[tokio::test]
async fn activation_conflict_evicts_then_replays() {
    // Frozen clock: live timestamps depend only on the journal, so the extra
    // activation does not change the output.
    let rt = runtime_with_step(0);
    let p = rt
        .prepare(&common::guest_wasm("det-basic"))
        .expect("prepare");
    let clean = drive_det(&rt, &p, "det-clean", Script::new()).await;
    let d = drive_det(
        &rt,
        &p,
        "det-conflict",
        Script::new().rule(Rule::ConflictOnAppend(1)),
    )
    .await;
    assert_eq!(
        d.outcomes[0],
        ActivationOutcome::Evicted {
            reason: EvictReason::AppendConflict
        }
    );
    assert_eq!(d.activations, 3);
    assert!(matches!(clean.outcome, ActivationOutcome::Completed { .. }));
    assert_eq!(d.outcome, clean.outcome, "same output as the clean run");
}

#[tokio::test]
async fn activation_nondeterminism_outcome() {
    let s = shared();
    let p = prepared("det-basic");
    let d = drive_det(&s.rt, &p, "det-nd", Script::new()).await;
    let mut journal = d.journal;
    let s1 = journal
        .iter_mut()
        .find(|e| matches!(&e.body, EventBody::StepCompleted(sc) if sc.name == "s1"))
        .expect("s1");
    let s1_seq = s1.seq;
    let EventBody::StepCompleted(sc) = &mut s1.body else {
        unreachable!()
    };
    sc.meta.hash.0[0] ^= 0xff;
    let (outcome, appended) = replay_journal(&s.rt, &p, run_ref("det-nd"), journal).await;
    let ActivationOutcome::AttemptFailed(f @ AttemptFailure::NonDeterminism(nd)) = &outcome else {
        panic!("{outcome:?}");
    };
    assert_eq!(nd.seq, s1_seq);
    let reason = f.dlq_reason();
    assert!(reason.starts_with("nondeterminism: seq="), "{reason}");
    assert!(nd.expected.contains("s1"), "{reason}");
    assert!(
        reason.contains(&nd.expected) && reason.contains(&nd.actual),
        "{reason}"
    );
    assert_eq!(appended, 0);
}

async fn drive_surface(name: &str) -> Drive {
    let p = prepared("wit-surface");
    drive_to_completion(
        &shared().rt,
        &p,
        RunSpec::new(run_ref(&format!("surface-{name}")), name, b""),
        Script::new(),
    )
    .await
}

#[tokio::test]
async fn activation_guest_failure_non_retryable() {
    let d = drive_surface("non-retryable").await;
    match &d.outcome {
        ActivationOutcome::Failed {
            failure: Failure::NonRetryable(info),
        } => assert_eq!(info.kind, "bad-input"),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        d.journal.last().map(|e| e.body.kind()),
        Some(EventKind::RunFailed)
    );
}

#[tokio::test]
async fn activation_panic_reason() {
    let d = drive_surface("panic").await;
    match &d.outcome {
        ActivationOutcome::AttemptFailed(AttemptFailure::Trap(msg)) => {
            assert!(msg.contains("boom"), "{msg}");
        }
        other => panic!("{other:?}"),
    }
    assert!(
        d.journal
            .iter()
            .any(|e| matches!(&e.body, EventBody::StepCompleted(sc) if sc.name == "before-panic")),
        "the completed step was flushed"
    );
}

#[tokio::test]
async fn activation_guest_exit() {
    let d = drive_surface("exit").await;
    assert_eq!(
        d.outcome,
        ActivationOutcome::AttemptFailed(AttemptFailure::GuestExit(7))
    );
}

#[tokio::test]
async fn activation_describe_run_timeout() {
    let s = shared();
    let p = prepared("wit-surface");
    let metas = s.rt.describe(&p).await.expect("describe");
    let surface = metas
        .iter()
        .find(|m| m.name == "surface")
        .expect("surface workflow");
    assert_eq!(surface.run_timeout_ms, Some(30_000));
    assert!(metas.iter().any(|m| m.name == "panic"));
}

#[tokio::test]
async fn activation_external_cancel_evicts() {
    let s = shared();
    let p = prepared("det-basic");
    let run = run_ref("det-cancel");
    let spec = RunSpec::new(run.clone(), "det", b"");
    let sink = Arc::new(FakeSink::new());
    sink.seed(
        run.clone(),
        vec![spec.run_started_event(&p.module_hash_hex)],
        1,
    );
    let cancel = CancellationToken::new();
    cancel.cancel();
    let outcome =
        s.rt.activate(
            &p,
            Activate {
                run: run.clone(),
                attempt: 1,
                module_sha: p.sha,
                journal: sink.journal(&run),
                inputs: None,
                cancel,
            },
            sink.clone(),
        )
        .await;
    assert_eq!(
        outcome,
        ActivationOutcome::Evicted {
            reason: EvictReason::External
        }
    );
    assert_eq!(sink.outcomes().len(), 1);
    assert_eq!(sink.appended_events(&run), 0);
}
