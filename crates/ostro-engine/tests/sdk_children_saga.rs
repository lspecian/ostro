//! Rust SDK child workflows (WF-06) and saga helper (WF-10) on the engine (plan 09-06),
//! with the `guests/child-parent`, `guests/child-leaf` and `guests/sdk-saga` components.
//!
//! Children: the parent's `ChildStarted` is journaled with the requested workflow and
//! options, and the activation evicts on the unsatisfied wait. The test then commits the
//! `ChildCompleted` input the partition would deliver (PF-W7 outcome contract) and
//! re-activates. Saga: compensations journal as steps in reverse registration order, keep
//! going past a failing compensation, and still run (shielded) after a cancel. Every
//! produced journal replays with zero appends.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use ostro_engine::config::EngineConfig;
use ostro_engine::error::EvictReason;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::testkit::{DEFAULT_ACCEPTED_AT, FakeClock, FakeSink, RunSpec, replay_journal};
use ostro_types::journal::{
    CancelRequested, ChildCompleted, ChildOptions, ChildStarted, ErrorInfo, Event, EventBody,
    EventKind, Failure, OpId, OpOutput, Payload, TimerFired, encode_result,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

const PARTITION: u16 = 9;
const RUN_ID: &str = "run-sdk-children-saga-1";

fn rt() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        Runtime::new(
            EngineConfig::default(),
            RuntimeOptions {
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
                token_key: [9; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime")
    })
}

async fn guest(name: &'static str) -> Arc<Prepared> {
    static CACHE: OnceLock<Mutex<HashMap<&'static str, Arc<Prepared>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some(p) = cache.lock().expect("lock").get(name) {
        return p.clone();
    }
    let p = rt()
        .prepare_cached(&common::guest_wasm(name))
        .await
        .unwrap_or_else(|e| panic!("prepare {name}: {e:?}"));
    cache.lock().expect("lock").insert(name, p.clone());
    p
}

fn run() -> RunRef {
    RunRef {
        partition: PARTITION,
        run_id: RUN_ID.to_string(),
    }
}

/// `[RunStarted]` for `workflow` with JSON `input`.
fn start(p: &Prepared, workflow: &str, input: &Value) -> Vec<Event> {
    let spec = RunSpec::new(run(), workflow, input.to_string().as_bytes());
    vec![spec.run_started_event(&p.module_hash_hex)]
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

/// Append input `bodies` to `journal` (one committed batch).
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

fn json_payload(v: &Value) -> Payload {
    Payload {
        content_type: "application/json".to_string(),
        data: serde_json::to_vec(v).expect("json"),
    }
}

fn output_json(outcome: &ActivationOutcome, journal: &[Event]) -> Value {
    match outcome {
        ActivationOutcome::Completed { output } => {
            serde_json::from_slice(&output.data).expect("JSON output")
        }
        other => panic!("expected Completed, got {other:?} (journal: {journal:#?})"),
    }
}

fn assert_evicted(outcome: &ActivationOutcome, journal: &[Event]) {
    assert_eq!(
        *outcome,
        ActivationOutcome::Evicted {
            reason: EvictReason::WaitUnsatisfied
        },
        "journal: {journal:#?}"
    );
}

/// Replaying `journal` gives `outcome` again and appends nothing.
async fn assert_replays(p: &Prepared, outcome: &ActivationOutcome, journal: &[Event]) {
    let (again, appended) = replay_journal(rt(), p, run(), journal.to_vec()).await;
    assert_eq!(&again, outcome, "replay outcome must be identical");
    assert_eq!(appended, 0, "replay must append nothing");
}

fn non_retryable(kind: &str) -> Failure {
    Failure::NonRetryable(ErrorInfo {
        kind: kind.to_string(),
        message: format!("{kind} (test)"),
        details: None,
    })
}

fn cancel_requested() -> EventBody {
    EventBody::CancelRequested(CancelRequested {
        reason: "test cancel".to_string(),
    })
}

/// Step names in journal order (`StepCompleted` events).
fn step_names(journal: &[Event]) -> Vec<String> {
    journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::StepCompleted(s) => Some(s.name.clone()),
            _ => None,
        })
        .collect()
}

mod children {
    use super::*;

    const PARENT: &str = "child-parent";
    const LEAF: &str = "child-leaf";

    fn child_started(journal: &[Event]) -> Vec<(OpId, ChildStarted)> {
        journal
            .iter()
            .filter_map(|e| match &e.body {
                EventBody::ChildStarted(c) => Some((e.seq, c.clone())),
                _ => None,
            })
            .collect()
    }

    fn child_completed(op: OpId, outcome: Result<OpOutput, Failure>) -> EventBody {
        EventBody::ChildCompleted(ChildCompleted {
            op,
            outcome: encode_result(&outcome),
        })
    }

    fn default_options() -> ChildOptions {
        ChildOptions {
            queue: None,
            version: None,
            detached: false,
            run_timeout_ms: None,
        }
    }

    /// Run parent `workflow` to its first eviction; returns the journal and the child op.
    async fn parked(
        p: &Prepared,
        workflow: &str,
        input: Value,
    ) -> (Vec<Event>, ChildStarted, OpId) {
        let (outcome, journal) = activate(p, start(p, workflow, &input)).await;
        assert_evicted(&outcome, &journal);
        let started = child_started(&journal);
        assert_eq!(started.len(), 1, "{journal:#?}");
        let (op, c) = started.into_iter().next().expect("one child");
        (journal, c, op)
    }

    /// Feed the child op's `outcome` and run the parent to completion.
    async fn feed(
        p: &Prepared,
        mut journal: Vec<Event>,
        op: OpId,
        outcome: Result<OpOutput, Failure>,
    ) -> (ActivationOutcome, Vec<Event>) {
        commit(&mut journal, vec![child_completed(op, outcome)]);
        let (outcome, journal) = activate(p, journal).await;
        assert_replays(p, &outcome, &journal).await;
        (outcome, journal)
    }

    #[tokio::test]
    async fn parent_attached_journals_child_started_and_completes_with_child_output() {
        let p = guest(PARENT).await;
        let (journal, c, op) = parked(
            &p,
            "parent_attached",
            json!({"workflow": "leaf_echo", "input": 1}),
        )
        .await;
        assert_eq!(c.workflow, "leaf_echo");
        assert_eq!(c.options, default_options());
        assert_eq!(c.input, json_payload(&json!(1)));
        let (outcome, journal) = feed(
            &p,
            journal,
            op,
            Ok(OpOutput::Child(json_payload(&json!({"echo": 1})))),
        )
        .await;
        assert_eq!(
            output_json(&outcome, &journal),
            json!({"child": {"echo": 1}})
        );
    }

    #[tokio::test]
    async fn parent_attached_maps_child_options() {
        let p = guest(PARENT).await;
        let input = json!({
            "workflow": "leaf_echo", "input": {"k": "v"},
            "queue": "q-children", "version": 2, "run_timeout_ms": 5000
        });
        let (journal, c, op) = parked(&p, "parent_attached", input).await;
        assert_eq!(
            c.options,
            ChildOptions {
                queue: Some("q-children".to_string()),
                version: Some(2),
                detached: false,
                run_timeout_ms: Some(5000),
            }
        );
        assert_eq!(c.input, json_payload(&json!({"k": "v"})));
        // Empty child output decodes as JSON null.
        let empty = Payload {
            content_type: "application/json".to_string(),
            data: Vec::new(),
        };
        let (outcome, journal) = feed(&p, journal, op, Ok(OpOutput::Child(empty))).await;
        assert_eq!(output_json(&outcome, &journal), json!({"child": null}));
    }

    #[tokio::test]
    async fn parent_fail_child_surfaces_child_failures_by_kind() {
        let p = guest(PARENT).await;
        let cases = [
            (non_retryable("boom"), "boom"),
            (non_retryable("child_terminated"), "child_terminated"),
            (Failure::Timeout, "timeout"),
            (Failure::Cancelled, "cancelled"),
        ];
        for (failure, kind) in cases {
            let (journal, c, op) =
                parked(&p, "parent_fail_child", json!({"workflow": "leaf_fail"})).await;
            assert_eq!(c.workflow, "leaf_fail");
            assert_eq!(c.input, json_payload(&Value::Null));
            let (outcome, journal) = feed(&p, journal, op, Err(failure)).await;
            assert_eq!(output_json(&outcome, &journal), json!({"caught": kind}));
        }
    }

    #[tokio::test]
    async fn parent_attached_propagates_child_failure_as_run_failure() {
        let p = guest(PARENT).await;
        let (journal, _, op) =
            parked(&p, "parent_attached", json!({"workflow": "leaf_fail"})).await;
        let (outcome, _) = feed(&p, journal, op, Err(non_retryable("child_terminated"))).await;
        match outcome {
            ActivationOutcome::Failed {
                failure: Failure::NonRetryable(i),
            } => assert_eq!(i.kind, "child_terminated"),
            other => panic!("expected non-retryable failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn parent_detached_resolves_to_child_run() {
        let p = guest(PARENT).await;
        let (journal, c, op) =
            parked(&p, "parent_detached", json!({"workflow": "leaf_sleep"})).await;
        assert!(c.options.detached, "{c:?}");
        assert_eq!(
            c.options,
            ChildOptions {
                detached: true,
                ..default_options()
            }
        );
        let (outcome, journal) = feed(
            &p,
            journal,
            op,
            Ok(OpOutput::Child(json_payload(&json!({"run_id": "r1"})))),
        )
        .await;
        assert_eq!(output_json(&outcome, &journal), json!({"detached": "r1"}));
    }

    #[tokio::test]
    async fn parent_cancel_child_journals_op_cancelled_and_waits_cancelled() {
        let p = guest(PARENT).await;
        let (outcome, journal) = activate(
            &p,
            start(
                &p,
                "parent_cancel_child",
                &json!({"workflow": "leaf_sleep"}),
            ),
        )
        .await;
        assert_eq!(
            output_json(&outcome, &journal),
            json!({"caught": "cancelled"})
        );
        let started = child_started(&journal);
        assert_eq!(started.len(), 1);
        let op = started[0].0;
        let cancelled: Vec<OpId> = journal
            .iter()
            .filter_map(|e| match &e.body {
                EventBody::OpCancelled(c) => Some(c.op),
                _ => None,
            })
            .collect();
        assert_eq!(cancelled, vec![op], "{journal:#?}");
        assert_replays(&p, &outcome, &journal).await;
    }

    #[tokio::test]
    async fn parent_close_returns_without_waiting() {
        let p = guest(PARENT).await;
        let (outcome, journal) = activate(
            &p,
            start(&p, "parent_close", &json!({"workflow": "leaf_sleep"})),
        )
        .await;
        assert_eq!(output_json(&outcome, &journal), json!({"started": true}));
        let started = child_started(&journal);
        assert_eq!(started.len(), 1);
        assert_eq!(started[0].1.workflow, "leaf_sleep");
        assert!(
            !journal
                .iter()
                .any(|e| e.body.kind() == EventKind::WaitResolved),
            "no wait: {journal:#?}"
        );
        assert_replays(&p, &outcome, &journal).await;
    }

    #[tokio::test]
    async fn leaf_echo_and_leaf_fail() {
        let p = guest(LEAF).await;
        let (outcome, journal) = activate(&p, start(&p, "leaf_echo", &json!({"n": 1}))).await;
        assert_eq!(output_json(&outcome, &journal), json!({"echo": {"n": 1}}));
        assert_replays(&p, &outcome, &journal).await;

        let (outcome, journal) = activate(&p, start(&p, "leaf_fail", &Value::Null)).await;
        match &outcome {
            ActivationOutcome::Failed {
                failure: Failure::NonRetryable(i),
            } => assert_eq!(i.kind, "boom"),
            other => panic!("expected boom, got {other:?}"),
        }
        assert_replays(&p, &outcome, &journal).await;
    }

    #[tokio::test]
    async fn leaf_sleep_cleans_up_shielded_after_cancel() {
        let p = guest(LEAF).await;
        let (outcome, mut journal) = activate(&p, start(&p, "leaf_sleep", &Value::Null)).await;
        assert_evicted(&outcome, &journal);
        commit(&mut journal, vec![cancel_requested()]);
        let (outcome, journal) = activate(&p, journal).await;
        assert_eq!(
            outcome,
            ActivationOutcome::Failed {
                failure: Failure::Cancelled
            },
            "{journal:#?}"
        );
        assert_eq!(step_names(&journal), vec!["cleanup".to_string()]);
        assert_replays(&p, &outcome, &journal).await;

        // The timer firing first: the leaf finishes normally.
        let (_, mut journal) = activate(&p, start(&p, "leaf_sleep", &Value::Null)).await;
        let timer = journal
            .iter()
            .find(|e| e.body.kind() == EventKind::TimerStarted)
            .map(|e| e.seq)
            .expect("timer");
        commit(
            &mut journal,
            vec![EventBody::TimerFired(TimerFired { op: timer })],
        );
        let (outcome, journal) = activate(&p, journal).await;
        assert_eq!(output_json(&outcome, &journal), json!({"slept": true}));
    }

    #[tokio::test]
    async fn leaf_can_continues_as_new_until_zero() {
        let p = guest(LEAF).await;
        let (outcome, journal) = activate(&p, start(&p, "leaf_can", &json!({"n": 2}))).await;
        match &outcome {
            ActivationOutcome::ContinuedAsNew { continuation } => {
                let next: Value = serde_json::from_slice(&continuation.input.data).expect("json");
                assert_eq!(next, json!({"n": 1, "generation": 1}));
                assert_eq!(
                    (continuation.workflow.as_deref(), continuation.version),
                    (None, None)
                );
            }
            other => panic!("expected continue-as-new, got {other:?}"),
        }
        assert_replays(&p, &outcome, &journal).await;
        let (outcome, journal) =
            activate(&p, start(&p, "leaf_can", &json!({"n": 0, "generation": 2}))).await;
        assert_eq!(
            output_json(&outcome, &journal),
            json!({"done": true, "generation": 2})
        );
    }
}
