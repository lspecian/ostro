//! Phase 5 journal -> Derived mappings (plan 05-11 Task 1): waits, tokens, cancel-op, the
//! Cancelled status, continue-as-new successors (D23, F11, PF-F2), resume re-arm (B2), the
//! remaining unsupported completions, and D21 blob-backed journal rows read back.
//!
//! The pure tables run [`derive::derive`] / [`derive::rearm_unresolved`] /
//! [`derive::successor_start`] with fakes; the sink cases drive a real single-partition node
//! so apply accepts (or rejects) exactly what derive produced.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ostro_engine::sink::{ActivationSink, AppendError, RunRef};
use ostro_log::Rf1Log;
use ostro_partition::store::values::{RunRecord, RunStatus};
use ostro_partition::{ApplyOutcome, Node, NodeConfig, PartitionHandle, query};
use ostro_server::derive::{
    self, DeriveCtx, ModuleResolver, PolicyResolver, ResolvedWorkflow, SuccessorError,
    SuccessorParts,
};
use ostro_server::journal_io::read_journal;
use ostro_server::run_started::{build_run_started, input_payload};
use ostro_server::sink::{EFFECTS_UNSUPPORTED, PartitionSink};
use ostro_types::command::{
    CancelTimer, CompleteRun, ConflictPolicy, Derived, OutboxMsg, ParentRef, ScheduleTimer,
    SendMessage, SignalRun, StartRun, TerminalStatus, WaitKind, derived::Intent, outbox_msg,
};
use ostro_types::journal::{
    AwaitTokenCreated, CancelRequested, ChildCompleted, ChildOptions, ChildStarted, CommandHash,
    CommandMeta, Continuation, EffectCompleted, EffectRequest, EffectScheduled, ErrorInfo, Event,
    EventBody, Failure, HttpRequest, OpCancelled, Payload, RunCompleted, RunContinuedAsNew,
    RunFailed, SignalMatched, SignalPayload, SignalWaitStarted, StepCompleted, TimerFired,
    TimerStarted, TokenCompleted, WaitTimedOut, encode_event,
};
use ostro_types::{AttemptToken, Command, PartitionCount, PartitionId, PayloadRef, RunId};
use tokio::time::{Instant, sleep, timeout};

const T: Duration = Duration::from_secs(30);
const P0: PartitionId = PartitionId(0);
const MS: u64 = 1_000_000;

// ------------------------------------------------------------------- builders

fn meta() -> CommandMeta {
    CommandMeta {
        hash_ver: 1,
        hash: CommandHash([0; 16]),
    }
}

fn ev(seq: u64, ts: u64, body: EventBody) -> Event {
    Event {
        seq,
        ts_nanos: ts,
        body,
    }
}

fn payload(data: &[u8]) -> Payload {
    Payload {
        content_type: "application/json".into(),
        data: data.to_vec(),
    }
}

fn timer(fire_at: u64) -> EventBody {
    EventBody::TimerStarted(TimerStarted {
        meta: meta(),
        fire_at_nanos: fire_at,
    })
}

fn signal_wait(name: &str, timeout_ms: Option<u64>) -> EventBody {
    EventBody::SignalWaitStarted(SignalWaitStarted {
        meta: meta(),
        name: name.into(),
        timeout_ms,
    })
}

fn token_wait(timeout_ms: Option<u64>) -> EventBody {
    EventBody::AwaitTokenCreated(AwaitTokenCreated {
        meta: meta(),
        token: "tok".into(),
        timeout_ms,
    })
}

fn cancel(op: u64) -> EventBody {
    EventBody::OpCancelled(OpCancelled { meta: meta(), op })
}

fn step() -> EventBody {
    EventBody::StepCompleted(StepCompleted {
        meta: meta(),
        name: "s".into(),
        outcome: ostro_types::journal::RecordedResult {
            codec_ver: 1,
            bytes: vec![],
        },
        fuel_used: 0,
    })
}

fn effect() -> EventBody {
    EventBody::EffectScheduled(EffectScheduled {
        meta: meta(),
        request: EffectRequest::Http(HttpRequest {
            method: "GET".into(),
            url: "http://x".into(),
            headers: vec![],
            body: None,
            timeout_ms: None,
            idempotency_key: None,
            redact: vec![],
        }),
    })
}

fn child() -> EventBody {
    EventBody::ChildStarted(ChildStarted {
        meta: meta(),
        workflow: "c".into(),
        input: payload(b"{}"),
        options: ChildOptions {
            queue: None,
            version: None,
            detached: false,
            run_timeout_ms: None,
        },
    })
}

fn failed(f: Failure) -> EventBody {
    EventBody::RunFailed(RunFailed {
        meta: meta(),
        failure: f,
    })
}

fn completed(out: &[u8]) -> EventBody {
    EventBody::RunCompleted(RunCompleted {
        meta: meta(),
        output: payload(out),
    })
}

fn can(workflow: Option<&str>, version: Option<u32>) -> EventBody {
    EventBody::RunContinuedAsNew(RunContinuedAsNew {
        meta: meta(),
        continuation: Continuation {
            input: payload(b"{\"n\":1}"),
            workflow: workflow.map(str::to_owned),
            version,
        },
    })
}

fn pref(b: &'static [u8]) -> PayloadRef {
    PayloadRef::inline(Bytes::from_static(b))
}

fn none(_: u64) -> Option<EventBody> {
    None
}

fn run_derive(
    events: &[Event],
    prior: &dyn Fn(u64) -> Option<EventBody>,
    result: Option<PayloadRef>,
    successor: Option<StartRun>,
) -> Vec<Derived> {
    derive::derive(
        events,
        DeriveCtx {
            prior,
            result,
            successor,
        },
    )
    .expect("derive")
}

fn complete(status: TerminalStatus, result: PayloadRef, at_ms: u64) -> Derived {
    Derived::new(Intent::CompleteRun(CompleteRun {
        status: status as i32,
        result: Some(result),
        completed_at_ms: at_ms,
        resumable: false,
        parent_event: None,
    }))
}

// ------------------------------------------------------------------- pure mapping table

#[test]
fn wait_token_cancel_and_status_mappings() {
    // ts 2.5 ms: at_ms rounds up to 3; a 100 ms timeout is due at ceil(102.5) = 103.
    let t = 2_500_000;
    let signal_prior = |op: u64| (op == 4).then(|| signal_wait("x", None));
    let token_prior = |op: u64| (op == 6).then(|| token_wait(Some(5)));
    let timer_prior = |op: u64| (op == 2).then(|| timer(9 * MS + 1));
    type Case<'a> = (
        &'a str,
        Vec<Event>,
        &'a dyn Fn(u64) -> Option<EventBody>,
        Option<PayloadRef>,
        Vec<Derived>,
    );
    let cases: Vec<Case<'_>> = vec![
        (
            "signal wait with timeout",
            vec![ev(4, t, signal_wait("approve", Some(100)))],
            &none,
            None,
            vec![Derived::register_wait(
                4,
                WaitKind::Signal,
                "approve",
                103,
                3,
            )],
        ),
        (
            "signal wait without timeout",
            vec![ev(4, t, signal_wait("x", None))],
            &none,
            None,
            vec![Derived::register_wait(4, WaitKind::Signal, "x", 0, 3)],
        ),
        (
            "token wait",
            vec![ev(6, t, token_wait(Some(100)))],
            &none,
            None,
            vec![Derived::register_wait(6, WaitKind::Token, "", 103, 3)],
        ),
        (
            "cancel of a signal op from history",
            vec![ev(9, t, cancel(4))],
            &signal_prior,
            None,
            vec![Derived::cancel_wait(4)],
        ),
        (
            "cancel of a token op from history",
            vec![ev(9, t, cancel(6))],
            &token_prior,
            None,
            vec![Derived::cancel_wait(6)],
        ),
        (
            "cancel of a signal op in the same batch",
            vec![ev(4, t, signal_wait("x", None)), ev(5, t, cancel(4))],
            &none,
            None,
            vec![
                Derived::register_wait(4, WaitKind::Signal, "x", 0, 3),
                Derived::cancel_wait(4),
            ],
        ),
        (
            "cancel of a timer keeps CancelTimer",
            vec![ev(9, t, cancel(2))],
            &timer_prior,
            None,
            vec![Derived::new(Intent::CancelTimer(CancelTimer {
                op: 2,
                due_at_ms: 10,
            }))],
        ),
        (
            // 06-14: an effect registers its in-flight row and its cancel deletes it.
            "effect registers, its cancel cancels the effect",
            vec![ev(3, t, effect()), ev(4, t, cancel(3))],
            &none,
            None,
            vec![Derived::register_effect(3, 0, 3), Derived::cancel_effect(3)],
        ),
        (
            "RunFailed{Cancelled} -> Cancelled",
            vec![ev(7, t, failed(Failure::Cancelled))],
            &none,
            Some(pref(b"c")),
            vec![complete(TerminalStatus::Cancelled, pref(b"c"), 3)],
        ),
        (
            "RunFailed{NonRetryable} -> Failed",
            vec![ev(
                7,
                t,
                failed(Failure::NonRetryable(ErrorInfo {
                    kind: "k".into(),
                    message: "m".into(),
                    details: None,
                })),
            )],
            &none,
            Some(pref(b"f")),
            vec![complete(TerminalStatus::Failed, pref(b"f"), 3)],
        ),
        (
            "RunFailed{Timeout} -> Failed",
            vec![ev(7, t, failed(Failure::Timeout))],
            &none,
            Some(pref(b"f")),
            vec![complete(TerminalStatus::Failed, pref(b"f"), 3)],
        ),
        (
            "RunCompleted -> Completed",
            vec![ev(7, t, completed(b"1"))],
            &none,
            Some(pref(b"1")),
            vec![complete(TerminalStatus::Completed, pref(b"1"), 3)],
        ),
        (
            "terminal batch: waits are not registered, timers are",
            vec![
                ev(4, t, signal_wait("x", Some(1))),
                ev(5, t, timer(7 * MS)),
                ev(6, t, cancel(4)),
                ev(7, t, completed(b"1")),
            ],
            &none,
            Some(pref(b"1")),
            vec![
                Derived::new(Intent::ScheduleTimer(ScheduleTimer {
                    op: 5,
                    due_at_ms: 7,
                })),
                complete(TerminalStatus::Completed, pref(b"1"), 3),
            ],
        ),
    ];
    for (name, events, prior, result, want) in cases {
        assert_eq!(run_derive(&events, prior, result, None), want, "{name}");
    }
}

#[test]
fn guest_terminals_are_never_resumable() {
    for body in [
        completed(b"1"),
        failed(Failure::Cancelled),
        failed(Failure::Timeout),
    ] {
        let d = run_derive(&[ev(1, 1, body)], &none, Some(pref(b"r")), None);
        let Some(Intent::CompleteRun(c)) = &d[0].intent else {
            panic!("CompleteRun")
        };
        assert!(!c.resumable);
    }
}

#[test]
fn continue_as_new_sends_the_successor_then_completes() {
    let succ = RunId::new_v7(P0);
    let start = StartRun {
        run_id: succ.as_bytes().to_vec(),
        workflow: "acme/w".into(),
        ..Default::default()
    };
    let result = PayloadRef::inline(derive::continued_as_new_json(&succ).into());
    let events = [ev(3, 1, timer(5 * MS)), ev(4, 2 * MS, can(None, None))];
    let d = run_derive(&events, &none, Some(result.clone()), Some(start.clone()));
    assert_eq!(
        d,
        vec![
            Derived::new(Intent::ScheduleTimer(ScheduleTimer {
                op: 3,
                due_at_ms: 5
            })),
            Derived::new(Intent::SendMessage(SendMessage {
                msg: Some(OutboxMsg::start_run(start.clone())),
            })),
            complete(TerminalStatus::ContinuedAsNew, result.clone(), 2),
        ]
    );
    let v: serde_json::Value =
        serde_json::from_slice(&derive::continued_as_new_json(&succ)).expect("json");
    assert_eq!(v["next_run_id"], succ.to_string());
    // A successor for a batch that does not continue as new is a caller bug.
    assert_eq!(
        derive::derive(
            &[ev(1, 1, step())],
            DeriveCtx {
                prior: &none,
                result: None,
                successor: Some(start),
            },
        ),
        Err(derive::DeriveError::UnexpectedSuccessor)
    );
}

// ------------------------------------------------------------------- re-arm table

#[test]
fn rearm_unresolved_table() {
    let t = 2_500_000;
    let fired = |op| EventBody::TimerFired(TimerFired { op });
    let matched = |op, payload_seq| EventBody::SignalMatched(SignalMatched { op, payload_seq });
    let timed_out = |op| EventBody::WaitTimedOut(WaitTimedOut { op });
    let token_done = |op| {
        EventBody::TokenCompleted(TokenCompleted {
            op,
            payload: payload(b"1"),
        })
    };
    let cancel_req = || EventBody::CancelRequested(CancelRequested { reason: "r".into() });
    let sched = |op, due| Derived::new(Intent::ScheduleTimer(ScheduleTimer { op, due_at_ms: due }));
    let cases: Vec<(&str, Vec<EventBody>, Vec<Derived>)> = vec![
        ("empty", vec![], vec![]),
        (
            "open timer, signal and token",
            vec![
                timer(500 * MS + 1),
                signal_wait("x", Some(100)),
                token_wait(None),
            ],
            vec![
                sched(1, 501),
                Derived::register_wait(2, WaitKind::Signal, "x", 103, 3),
                Derived::register_wait(3, WaitKind::Token, "", 0, 3),
            ],
        ),
        (
            "resolved ops are skipped",
            vec![
                timer(5 * MS),             // 1: fired
                signal_wait("x", None),    // 2: matched
                signal_wait("y", Some(1)), // 3: timed out
                token_wait(None),          // 4: completed
                signal_wait("z", None),    // 5: guest-cancelled
                timer(9 * MS),             // 6: guest-cancelled
                fired(1),
                EventBody::SignalPayload(SignalPayload {
                    name: "x".into(),
                    payload: payload(b"1"),
                }),
                matched(2, 8),
                timed_out(3),
                token_done(4),
                cancel(5),
                cancel(6),
                signal_wait("w", None), // 14: open
            ],
            vec![Derived::register_wait(14, WaitKind::Signal, "w", 0, 3)],
        ),
        (
            "effects, children and the cancellation op are not re-armed",
            vec![effect(), child(), cancel_req(), timer(7 * MS)],
            vec![sched(4, 7)],
        ),
        (
            "an effect completion does not close a later op",
            vec![
                effect(),
                EventBody::EffectCompleted(EffectCompleted {
                    op: 1,
                    outcome: ostro_types::journal::RecordedResult {
                        codec_ver: 1,
                        bytes: vec![],
                    },
                }),
                signal_wait("x", None),
            ],
            vec![Derived::register_wait(3, WaitKind::Signal, "x", 0, 3)],
        ),
        (
            "a child completion closes only its op",
            vec![
                child(),
                timer(8 * MS),
                EventBody::ChildCompleted(ChildCompleted {
                    op: 1,
                    outcome: ostro_types::journal::RecordedResult {
                        codec_ver: 1,
                        bytes: vec![],
                    },
                }),
            ],
            vec![sched(2, 8)],
        ),
        (
            "a terminal journal re-arms nothing",
            vec![timer(8 * MS), completed(b"1")],
            vec![],
        ),
    ];
    for (name, bodies, want) in cases {
        // seq 0 is RunStarted's place; bodies start at seq 1, all at ts 2.5 ms.
        let journal: Vec<Event> = bodies
            .into_iter()
            .enumerate()
            .map(|(i, b)| ev(i as u64 + 1, t, b))
            .collect();
        assert_eq!(derive::rearm_unresolved(&journal), want, "{name}");
    }
}

#[test]
fn rearm_equals_the_original_derive() {
    // The re-armed rows are byte-identical to the ones the original flush derived.
    let batch = vec![
        ev(1, 7_000_001, timer(900 * MS + 3)),
        ev(2, 7_000_001, signal_wait("x", Some(250))),
        ev(3, 7_000_001, token_wait(Some(1))),
    ];
    assert_eq!(
        derive::rearm_unresolved(&batch),
        run_derive(&batch, &none, None, None)
    );
}

// ------------------------------------------------------------------- successor_start

struct Modules(
    BTreeMap<(String, u32), ResolvedWorkflow>,
    BTreeMap<String, u32>,
);

impl ModuleResolver for Modules {
    fn resolve(&self, workflow: &str, version: Option<u32>) -> Option<ResolvedWorkflow> {
        let v = version.or_else(|| self.1.get(workflow).copied())?;
        self.0.get(&(workflow.to_owned(), v)).cloned()
    }
}

struct Policy;

impl PolicyResolver for Policy {
    fn resolve(&self, workflow: &str, queue: &str) -> (u64, u64) {
        match (workflow, queue) {
            ("acme/other", _) => (77, 5),
            _ => (0, 0),
        }
    }
}

fn modules() -> Modules {
    let rw = |v, sha: u8, t| ResolvedWorkflow {
        version: v,
        module_sha: [sha; 32],
        run_timeout_ms: t,
    };
    let mut m = BTreeMap::new();
    m.insert(("acme/w".to_owned(), 1), rw(1, 0x11, Some(300)));
    m.insert(("acme/w".to_owned(), 2), rw(2, 0x22, None));
    m.insert(("acme/other".to_owned(), 4), rw(4, 0x44, Some(9)));
    let mut active = BTreeMap::new();
    active.insert("acme/other".to_owned(), 4);
    Modules(m, active)
}

fn pred_record() -> RunRecord {
    RunRecord {
        workflow: "acme/w".into(),
        workflow_version: 1,
        queue: "acme/q".into(),
        status: RunStatus::Running,
        parent: Some(([7; 18], 12)),
        concurrency_key: "ck".into(),
        priority: 3,
        module_sha: Some([0x99; 32]),
        tags: vec!["a".into(), "b".into()],
        tenant: Some("acme".into()),
        traceparent: Some("00-trace-01".into()),
        run_timeout_ms: Some(1_000),
        retention_ms: 4_242,
        idempotency_key: b"key".to_vec(),
        ..Default::default()
    }
}

fn parts(pred: &RunRecord) -> SuccessorParts<'_> {
    SuccessorParts {
        pred_id: RunId::new_v7(P0),
        pred,
        run_id: RunId::new_v7(PartitionId(1)),
        rng_seed: [5; 32],
        input: pref(b"{\"n\":1}"),
        at_ms: 1_234,
    }
}

#[test]
fn successor_copies_lineage_and_resolves_its_own_deadlines() {
    let pred = pred_record();
    let p = parts(&pred);
    let (pred_id, run_id) = (p.pred_id, p.run_id);
    let s = derive::successor_start(&can_cont(None, None), p, &modules(), &Policy).unwrap();
    // Same workflow and version: the version's module and run timeout from meta.
    assert_eq!(s.workflow, "acme/w");
    assert_eq!(s.workflow_version, 1);
    assert_eq!(s.module_sha, vec![0x11; 32]);
    assert_eq!(
        s.run_timeout_ms, 300,
        "a fresh run timeout from the version"
    );
    // F11 + PF-F2: copied.
    assert_eq!(s.tenant, "acme");
    assert_eq!(s.tags, vec!["a".to_owned(), "b".to_owned()]);
    assert_eq!(s.traceparent, "00-trace-01");
    assert_eq!(s.queue, "acme/q");
    assert_eq!((s.priority, s.concurrency_key.as_str()), (3, "ck"));
    assert_eq!(
        s.parent,
        Some(ParentRef {
            run_id: vec![7; 18],
            op: 12
        })
    );
    // Lineage and fresh identity.
    assert_eq!(s.continued_from, pred_id.as_bytes().to_vec());
    assert_eq!(s.run_id, run_id.as_bytes().to_vec());
    assert_eq!(s.rng_seed, vec![5; 32]);
    assert!(s.idempotency_key.is_empty() && s.retry_of.is_empty());
    assert_eq!((s.requested_at_ms, s.ready_at_ms), (1_234, 1_234));
    // No policy: the predecessor's retention is kept, no schedule-to-start.
    assert_eq!((s.retention_ms, s.schedule_to_start_ms), (4_242, 0));
    assert_eq!(s.input, Some(pref(b"{\"n\":1}")));
}

fn can_cont(workflow: Option<&str>, version: Option<u32>) -> Continuation {
    let EventBody::RunContinuedAsNew(c) = can(workflow, version) else {
        unreachable!()
    };
    c.continuation
}

#[test]
fn successor_workflow_is_qualified_with_the_tenant_d23() {
    let pred = pred_record();
    // Another workflow by its module-local name, no version: the active version.
    let s = derive::successor_start(
        &can_cont(Some("other"), None),
        parts(&pred),
        &modules(),
        &Policy,
    )
    .unwrap();
    assert_eq!(s.workflow, "acme/other");
    assert_eq!(s.workflow_version, 4);
    assert_eq!(s.module_sha, vec![0x44; 32]);
    assert_eq!(s.run_timeout_ms, 9);
    // Its policy applies.
    assert_eq!((s.retention_ms, s.schedule_to_start_ms), (77, 5));
    // An explicit version of the same workflow.
    let s = derive::successor_start(&can_cont(None, Some(2)), parts(&pred), &modules(), &Policy)
        .unwrap();
    assert_eq!((s.workflow.as_str(), s.workflow_version), ("acme/w", 2));
    assert_eq!((s.module_sha, s.run_timeout_ms), (vec![0x22; 32], 0));
    // Default tenant: names stay bare.
    let mut bare = pred_record();
    bare.workflow = "w".into();
    bare.tenant = None;
    let s = derive::successor_start(
        &can_cont(Some("w2"), Some(1)),
        parts(&bare),
        &derive::NoModules,
        &Policy,
    );
    assert_eq!(
        s,
        Err(SuccessorError::Unresolved {
            workflow: "w2".into(),
            version: Some(1)
        })
    );
}

#[test]
fn successor_without_meta_reuses_the_pinned_module_only_for_its_own_version() {
    let pred = pred_record();
    let s = derive::successor_start(
        &can_cont(None, None),
        parts(&pred),
        &derive::NoModules,
        &derive::InheritPolicy,
    )
    .unwrap();
    assert_eq!(s.module_sha, vec![0x99; 32]);
    assert_eq!(s.run_timeout_ms, 1_000);
    assert_eq!(s.retention_ms, 4_242);
    assert!(matches!(
        derive::successor_start(
            &can_cont(None, Some(2)),
            parts(&pred),
            &derive::NoModules,
            &derive::InheritPolicy,
        ),
        Err(SuccessorError::Unresolved { .. })
    ));
}

// ------------------------------------------------------------------- through a real sink

async fn open(dir: &Path) -> Node {
    let cfg = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(1).expect("non-zero"));
    timeout(common::OPEN_T, Node::open(cfg))
        .await
        .expect("node open timed out")
        .expect("node open")
}

fn rref(run: &RunId) -> RunRef {
    RunRef {
        partition: run.partition().0,
        run_id: run.to_string(),
    }
}

struct Fx {
    _dir: tempfile::TempDir,
    _node: Node,
    handle: PartitionHandle<Rf1Log>,
    run: RunId,
    token: AttemptToken,
    sink: PartitionSink,
}

impl Fx {
    /// A started + granted run with RunStarted appended; `tweak` edits the StartRun.
    async fn new(tweak: impl FnOnce(&mut StartRun)) -> Self {
        let dir = ostro_testkit::fs::tempdir();
        let node = open(dir.path()).await;
        let handle = node.handle(P0).expect("ready").clone();
        let run = RunId::new_v7(P0);
        let mut s = StartRun {
            run_id: run.as_bytes().to_vec(),
            workflow: "wf".into(),
            workflow_version: 1,
            queue: "default".into(),
            input: Some(pref(b"{}")),
            conflict_policy: ConflictPolicy::ReturnExisting as i32,
            // Far future: no run timeout fires during the test.
            requested_at_ms: 4_102_444_800_000,
            ready_at_ms: 4_102_444_800_000,
            rng_seed: vec![0x5a; 32],
            module_sha: vec![0xab; 32],
            ..Default::default()
        };
        tweak(&mut s);
        let o = handle
            .submit_one(Command::start_run(s))
            .await
            .expect("start");
        assert_eq!(o, ApplyOutcome::Accepted);
        let token = handle.grant_attempt(run).await.expect("grant");
        let sink = PartitionSink::new(handle.clone(), run, token, None, None);
        let rec = handle.read_run(&run).expect("read").expect("run");
        let rs = build_run_started(&rec, input_payload(b"{}".to_vec()), "ab").expect("rs");
        let f = Self {
            _dir: dir,
            _node: node,
            handle,
            run,
            token,
            sink,
        };
        assert_eq!(f.append(0, vec![rs]).await, Ok(1));
        f
    }

    async fn append(&self, from: u64, events: Vec<Event>) -> Result<u64, AppendError> {
        self.sink
            .append(&rref(&self.run), self.token.n, from, events)
            .await
    }

    fn record(&self) -> RunRecord {
        self.handle.read_run(&self.run).expect("read").expect("run")
    }

    async fn result_json(&self) -> serde_json::Value {
        let r = query::run_payload(&self.handle.reader(), &self.run, query::PAYLOAD_RESULT)
            .expect("read")
            .expect("result");
        let b = self.handle.fetch_payload(&r).await.expect("payload");
        serde_json::from_slice(&b).expect("json")
    }
}

#[tokio::test]
async fn signal_and_token_waits_register_and_cancel_through_apply() {
    let f = Fx::new(|_| {}).await;
    let far = 4_102_444_800_000 * MS;
    assert_eq!(
        f.append(
            1,
            vec![
                ev(1, far, signal_wait("x", Some(60_000))),
                ev(2, far, token_wait(None)),
            ]
        )
        .await,
        Ok(3)
    );
    assert_eq!(f.record().open_waits, 2);
    // Cancel the signal op from history (the sink reads seq 1 back for the lookup).
    assert_eq!(f.append(3, vec![ev(3, far, cancel(1))]).await, Ok(4));
    assert_eq!(f.record().open_waits, 1);
    assert!(!f.sink.is_terminal(), "tokens no longer fail the run");
}

#[tokio::test]
async fn guest_cancel_completes_the_run_cancelled() {
    let f = Fx::new(|_| {}).await;
    let far = 4_102_444_800_000 * MS;
    assert_eq!(
        f.append(1, vec![ev(1, far, signal_wait("x", None))]).await,
        Ok(2)
    );
    assert_eq!(
        f.append(2, vec![ev(2, far, failed(Failure::Cancelled))])
            .await,
        Ok(3)
    );
    let rec = f.record();
    assert_eq!(rec.status, RunStatus::Cancelled);
    assert!(!rec.resumable);
    assert_eq!(
        rec.open_waits, 0,
        "the terminal transition removed the wait"
    );
    assert_eq!(f.result_json().await["reason_code"], "cancelled");
}

/// 09-05: effects (no bridge) still complete `unsupported`; an unresolvable child
/// (NoModules) completes `unknown_workflow` through the same post-flush path.
#[tokio::test]
async fn effects_unsupported_and_unresolvable_children_complete() {
    let f = Fx::new(|_| {}).await;
    let far = 4_102_444_800_000 * MS;
    assert_eq!(
        f.append(1, vec![ev(1, far, effect()), ev(2, far, child())])
            .await,
        Ok(3),
        "the engine batch's end; the completions are journaled after it"
    );
    let j = read_journal(&f.handle, &f.run).await.expect("journal");
    assert_eq!(j.len(), 5);
    let msgs: Vec<String> = j[3..]
        .iter()
        .map(|e| match &e.body {
            EventBody::EffectCompleted(c) => decode(&c.outcome, "unsupported"),
            EventBody::ChildCompleted(c) => decode(&c.outcome, "unknown_workflow"),
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(msgs[0], EFFECTS_UNSUPPORTED);
    assert!(msgs[1].contains("not registered"), "{}", msgs[1]);
    assert!(!f.sink.is_terminal());
}

fn decode(r: &ostro_types::journal::RecordedResult, kind: &str) -> String {
    let out: Result<ostro_types::journal::OpOutput, Failure> =
        ostro_types::journal::decode_result(r).expect("decode");
    match out {
        Err(Failure::NonRetryable(i)) => {
            assert_eq!(i.kind, kind);
            i.message
        }
        other => panic!("not {kind}: {other:?}"),
    }
}

/// Wait until `run` exists on the handle's partition.
async fn wait_record(h: &PartitionHandle<Rf1Log>, run: &RunId) -> RunRecord {
    let deadline = Instant::now() + T;
    loop {
        if let Some(r) = h.read_run(run).expect("read") {
            return r;
        }
        assert!(Instant::now() < deadline, "successor {run} never started");
        sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn continue_as_new_through_apply_starts_the_successor_with_lineage() {
    let f = Fx::new(|s| {
        s.workflow = "acme/wf".into();
        s.tenant = "acme".into();
        s.tags = vec!["t1".into()];
        s.traceparent = "00-abc-01".into();
        s.retention_ms = 9_999;
        s.run_timeout_ms = 600_000;
    })
    .await;
    let far = 4_102_444_800_000 * MS;
    assert_eq!(f.append(1, vec![ev(1, far, can(None, None))]).await, Ok(2));
    assert!(f.sink.is_terminal());
    let rec = f.record();
    assert_eq!(rec.status, RunStatus::ContinuedAsNew);
    assert!(!rec.resumable);
    let succ = rec.continued_to.expect("continued_to");
    assert_eq!(
        f.result_json().await["next_run_id"],
        succ.to_string(),
        "result names the successor"
    );
    let s = wait_record(&f.handle, &succ).await;
    assert_eq!(s.workflow, "acme/wf");
    assert_eq!(s.tenant.as_deref(), Some("acme"));
    assert_eq!(s.tags, vec!["t1".to_owned()]);
    assert_eq!(s.traceparent.as_deref(), Some("00-abc-01"));
    assert_eq!(s.continued_from, Some(f.run));
    assert_eq!(s.module_sha, Some([0xab; 32]));
    assert_eq!(s.run_timeout_ms, Some(600_000));
    assert_eq!(s.retention_ms, 9_999);
    assert_eq!(s.next_seq, 0);
    // The successor's input is the continuation input.
    let input = query::run_payload(&f.handle.reader(), &succ, query::PAYLOAD_INPUT)
        .expect("read")
        .expect("input");
    assert_eq!(
        &f.handle.fetch_payload(&input).await.expect("bytes")[..],
        b"{\"n\":1}"
    );
}

#[tokio::test]
async fn unresolvable_continue_as_new_fails_the_run() {
    let f = Fx::new(|_| {}).await;
    let far = 4_102_444_800_000 * MS;
    assert_eq!(
        f.append(1, vec![ev(1, far, can(Some("nowhere"), None))])
            .await,
        Ok(2)
    );
    let rec = f.record();
    assert_eq!(rec.status, RunStatus::Failed);
    assert_eq!(rec.continued_to, None);
    assert_eq!(
        f.result_json().await["reason_code"],
        derive::REASON_CONTINUE_AS_NEW_FAILED
    );
}

#[tokio::test]
async fn continue_as_new_uses_the_wired_resolvers() {
    struct Fixed;
    impl ModuleResolver for Fixed {
        fn resolve(&self, workflow: &str, version: Option<u32>) -> Option<ResolvedWorkflow> {
            (workflow == "next" && version.is_none()).then_some(ResolvedWorkflow {
                version: 8,
                module_sha: [0xcd; 32],
                run_timeout_ms: Some(42_000),
            })
        }
    }
    let mut f = Fx::new(|_| {}).await;
    f.sink = PartitionSink::new(f.handle.clone(), f.run, f.token, None, None)
        .with_resolvers(Arc::new(Fixed), Arc::new(Policy));
    let far = 4_102_444_800_000 * MS;
    assert_eq!(
        f.append(1, vec![ev(1, far, can(Some("next"), None))]).await,
        Ok(2)
    );
    let succ = f.record().continued_to.expect("continued_to");
    let s = wait_record(&f.handle, &succ).await;
    assert_eq!((s.workflow.as_str(), s.workflow_version), ("next", 8));
    assert_eq!(s.module_sha, Some([0xcd; 32]));
    assert_eq!(s.run_timeout_ms, Some(42_000));
}

#[tokio::test]
async fn blob_backed_signal_payload_rows_are_read_back_d21() {
    let f = Fx::new(|_| {}).await;
    // A 1 MiB SignalPayload, encoded by the submitter and journaled verbatim by apply.
    let big = vec![b'7'; 1 << 20];
    let event = Event {
        seq: 0,
        ts_nanos: 5 * MS,
        body: EventBody::SignalPayload(SignalPayload {
            name: "x".into(),
            payload: payload(&big),
        }),
    };
    let (_, bytes) = encode_event(&event);
    let pref = f
        .handle
        .prepare_payload(Bytes::from(bytes))
        .await
        .expect("blob");
    assert!(
        matches!(
            pref.kind,
            Some(ostro_types::payload::payload_ref::Kind::Blob(_))
        ),
        "over 256 KiB goes to the blob store"
    );
    let o = f
        .handle
        .submit_one(Command::signal_run(SignalRun {
            run_id: f.run.as_bytes().to_vec(),
            name: "x".into(),
            event: Some(pref),
            request_id: vec![],
            sent_at_ms: 5,
        }))
        .await
        .expect("signal");
    assert_eq!(o, ApplyOutcome::Accepted);
    let j = read_journal(&f.handle, &f.run).await.expect("journal");
    assert_eq!(j.len(), 2);
    match &j[1].body {
        EventBody::SignalPayload(p) => {
            assert_eq!(p.name, "x");
            assert_eq!(p.payload.data.len(), 1 << 20);
            assert!(p.payload.data == big);
        }
        other => panic!("not SignalPayload: {other:?}"),
    }
    assert_eq!((j[1].seq, j[1].ts_nanos), (1, 5 * MS));
}

#[test]
fn outbox_start_run_kind_is_the_successor_message() {
    let m = OutboxMsg::start_run(StartRun::default());
    assert!(matches!(m.kind, Some(outbox_msg::Kind::StartRun(_))));
}
