//! Children server bridge, part 1 (plan 09-05, WF-06): deterministic child ids, derived
//! child starts, `unknown_workflow` for unresolvable targets, the child's `parent_event`
//! (PF-F1) and the journal-load repair rule (W1). Synthetic engine journals against a real
//! single-partition Node (its shuffler delivers outbox messages to the same partition); no
//! guests.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use ostro_engine::sink::{ActivationSink, AppendError, RunRef};
use ostro_log::Rf1Log;
use ostro_partition::store::keys::children_key;
use ostro_partition::store::values::RunRecord;
use ostro_partition::{ApplyOutcome, Node, NodeConfig, PartitionHandle, Table, query};
use ostro_server::children::{
    UNKNOWN_WORKFLOW_KIND, child_run_id, parent_event_bytes, parent_outcome,
};
use ostro_server::derive::{
    self, ChildCtx, DeriveCtx, DeriveError, InheritPolicy, ModuleResolver, ResolvedWorkflow,
};
use ostro_server::journal_io::read_journal;
use ostro_server::sink::PartitionSink;
use ostro_types::command::{
    AttemptTokenMsg, CHILD_ROW_JOURNAL, ConflictPolicy, Derived, EventsAppended, JournalEvent,
    OutboxMsg, ParentRef, SendMessage, StartChild, StartRun, derived::Intent, outbox_msg,
};
use ostro_types::journal::{
    ChildOptions, ChildStarted, CommandHash, CommandMeta, Continuation, ErrorInfo, Event,
    EventBody, Failure, OpOutput, Payload, RunCompleted, RunContinuedAsNew, RunFailed,
    decode_event, decode_result, encode_event,
};
use ostro_types::payload::payload_ref::Kind;
use ostro_types::{AttemptToken, Command, PartitionCount, PartitionId, PayloadRef, RunId};
use tokio::time::timeout;

const T: Duration = Duration::from_secs(20);
const OPEN_T: Duration = Duration::from_secs(120);
const P0: PartitionId = PartitionId(0);
const PARENT_SHA: [u8; 32] = [0xab; 32];
const LEAF_SHA: [u8; 32] = [0xcd; 32];
/// Far in the future, so no timer (run timeout, start delay) ever fires in a test.
const FAR_MS: u64 = 4_102_444_800_000;
/// Event times far in the future too (child run timeouts are armed from them).
const TS0: u64 = FAR_MS * 1_000_000;

// ------------------------------------------------------------------ fake resolver

/// One version: (module sha, default queue, run timeout).
type Version = ([u8; 32], Option<String>, Option<u64>);

/// Workflows by name: `(versions, active version)`.
#[derive(Default)]
struct Fake(BTreeMap<String, (BTreeMap<u32, Version>, u32)>);

impl Fake {
    fn leaf() -> Self {
        let mut f = Fake::default();
        // Keyed by the qualified name (D23): the parents run in tenant `acme`.
        f.add("acme/leaf", 3, LEAF_SHA, Some("leafq"), Some(5_000));
        f.add("acme/leaf", 2, [0x22; 32], None, None);
        f.activate("acme/leaf", 3);
        f
    }

    fn add(&mut self, wf: &str, v: u32, sha: [u8; 32], q: Option<&str>, t: Option<u64>) {
        let e = self.0.entry(wf.into()).or_default();
        e.0.insert(v, (sha, q.map(Into::into), t));
    }

    fn activate(&mut self, wf: &str, v: u32) {
        self.0.get_mut(wf).expect("wf").1 = v;
    }
}

impl ModuleResolver for Fake {
    fn resolve(&self, workflow: &str, version: Option<u32>) -> Option<ResolvedWorkflow> {
        let (vs, active) = self.0.get(workflow)?;
        let v = version.unwrap_or(*active);
        vs.get(&v).map(|(sha, _, t)| ResolvedWorkflow {
            version: v,
            module_sha: *sha,
            run_timeout_ms: *t,
        })
    }

    fn default_queue(&self, workflow: &str, version: u32) -> Option<String> {
        self.0.get(workflow)?.0.get(&version)?.1.clone()
    }
}

// ------------------------------------------------------------------ fixture

async fn open(dir: &Path) -> Node {
    let mut cfg = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(1).expect("non-zero"));
    cfg.leader.shuffler.idle_poll = Duration::from_millis(20);
    timeout(OPEN_T, Node::open(cfg))
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

fn base_start(run: &RunId, parent: Option<ParentRef>) -> StartRun {
    StartRun {
        run_id: run.as_bytes().to_vec(),
        workflow: "acme/wf".into(),
        workflow_version: 1,
        queue: "acme/default".into(),
        input: Some(PayloadRef::inline(Bytes::from_static(b"{}"))),
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        requested_at_ms: FAR_MS,
        ready_at_ms: FAR_MS,
        rng_seed: vec![0x5a; 32],
        module_sha: PARENT_SHA.to_vec(),
        run_timeout_ms: 60_000,
        priority: 4,
        tenant: "acme".into(),
        traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into(),
        parent,
        ..Default::default()
    }
}

struct Fx {
    _dir: tempfile::TempDir,
    _node: Node,
    handle: PartitionHandle<Rf1Log>,
}

impl Fx {
    async fn new() -> Self {
        let dir = ostro_testkit::fs::tempdir();
        let node = open(dir.path()).await;
        let handle = node.handle(P0).expect("partition ready").clone();
        Self {
            _dir: dir,
            _node: node,
            handle,
        }
    }

    /// Starts `s`, grants attempt 1 and appends RunStarted (seq 0) raw.
    async fn start(&self, s: StartRun) -> (RunId, AttemptToken) {
        let run = RunId::from_bytes(&s.run_id).expect("id");
        let o = self
            .handle
            .submit_one(Command::start_run(s))
            .await
            .expect("start");
        assert_eq!(o, ApplyOutcome::Accepted);
        let token = self.handle.grant_attempt(run).await.expect("grant");
        self.raw(&run, token, 0, &[ev(0, step_body())]).await;
        (run, token)
    }

    fn sink(&self, run: RunId, token: AttemptToken, modules: Fake) -> PartitionSink {
        PartitionSink::new(self.handle.clone(), run, token, None, None)
            .with_resolvers(Arc::new(modules), Arc::new(InheritPolicy))
    }

    /// A raw EventsAppended without intents (a flush whose post-flush step was lost).
    async fn raw(&self, run: &RunId, token: AttemptToken, from: u64, events: &[Event]) {
        let jevs = events
            .iter()
            .map(|e| {
                let (kind, bytes) = encode_event(e);
                JournalEvent {
                    seq: e.seq,
                    kind,
                    payload: Some(PayloadRef::inline(bytes.into())),
                }
            })
            .collect();
        let o = self
            .handle
            .submit_one(Command::events_appended(EventsAppended {
                run_id: run.as_bytes().to_vec(),
                token: Some(AttemptTokenMsg::from(token)),
                from_seq: from,
                events: jevs,
                derived: vec![],
            }))
            .await
            .expect("submit");
        assert_eq!(o, ApplyOutcome::Accepted);
    }

    fn record(&self, run: &RunId) -> Option<RunRecord> {
        query::run_record(&self.handle.reader(), run).expect("read")
    }

    async fn journal(&self, run: &RunId) -> Vec<Event> {
        read_journal(&self.handle, run).await.expect("journal")
    }

    fn row(&self, parent: &RunId, child: &RunId) -> Option<Vec<u8>> {
        self.handle
            .reader()
            .get(Table::Children, &children_key(parent, child))
            .expect("read")
    }

    fn runs(&self) -> usize {
        self.handle
            .reader()
            .prefix(Table::Runs, &[], usize::MAX)
            .expect("runs")
            .len()
    }

    async fn wait(&self, what: &str, mut f: impl FnMut(&Self) -> bool) {
        let t0 = Instant::now();
        while !f(self) {
            assert!(t0.elapsed() < T, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_record(&self, run: &RunId) -> RunRecord {
        self.wait("run record", |f| f.record(run).is_some()).await;
        self.record(run).expect("record")
    }

    async fn wait_journal_len(&self, run: &RunId, n: usize) -> Vec<Event> {
        let t0 = Instant::now();
        loop {
            let j = self.journal(run).await;
            if j.len() >= n {
                return j;
            }
            assert!(t0.elapsed() < T, "journal of {run} stuck at {}", j.len());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

fn meta() -> CommandMeta {
    CommandMeta {
        hash_ver: 1,
        hash: CommandHash([7; 16]),
    }
}

fn ev(seq: u64, body: EventBody) -> Event {
    Event {
        seq,
        ts_nanos: TS0 + seq * 1_000_000,
        body,
    }
}

fn step_body() -> EventBody {
    EventBody::PatchMarker(ostro_types::journal::PatchMarker {
        meta: meta(),
        change_id: "x".into(),
    })
}

fn payload(data: &[u8]) -> Payload {
    Payload {
        content_type: "application/json".into(),
        data: data.to_vec(),
    }
}

fn child_started(seq: u64, workflow: &str, opts: ChildOptions) -> Event {
    ev(
        seq,
        EventBody::ChildStarted(ChildStarted {
            meta: meta(),
            workflow: workflow.into(),
            input: payload(b"{\"n\":7}"),
            options: opts,
        }),
    )
}

fn attached() -> ChildOptions {
    ChildOptions {
        queue: None,
        version: None,
        detached: false,
        run_timeout_ms: None,
    }
}

fn completion_of(j: &[Event], op: u64) -> Option<Result<OpOutput, Failure>> {
    j.iter().find_map(|e| match &e.body {
        EventBody::ChildCompleted(c) if c.op == op => {
            Some(decode_result(&c.outcome).expect("outcome"))
        }
        _ => None,
    })
}

fn assert_unknown_workflow(r: Option<Result<OpOutput, Failure>>) {
    match r {
        Some(Err(Failure::NonRetryable(i))) => {
            assert_eq!(i.kind, UNKNOWN_WORKFLOW_KIND);
            assert!(i.message.contains("not registered"), "{}", i.message);
        }
        other => panic!("expected unknown_workflow, got {other:?}"),
    }
}

// ------------------------------------------------------------------ child_run_id

#[test]
fn child_run_id_golden_vectors() {
    let mut b = [0x11u8; 18];
    b[..2].copy_from_slice(&3u16.to_be_bytes());
    let parent = RunId::from_bytes(&b).expect("id");
    let id = child_run_id(&parent, 7, 16);
    assert_eq!(id.to_string(), GOLDEN_OP7_P16);
    assert_eq!(child_run_id(&parent, 7, 16), id, "deterministic");
    assert_eq!(child_run_id(&parent, 8, 16).to_string(), GOLDEN_OP8_P16);
    assert_ne!(child_run_id(&parent, 8, 16), id);
    assert_eq!(child_run_id(&parent, 7, 1).to_string(), GOLDEN_OP7_P1);
    for (op, p) in [(7u64, 16u16), (8, 16), (0, 1), (u64::MAX, 65_535)] {
        let id = child_run_id(&parent, op, p);
        let u = &id.as_bytes()[2..];
        assert_eq!(u[6] >> 4, 8, "uuid version 8");
        assert_eq!(u[8] >> 6, 0b10, "RFC 9562 variant");
        assert!(id.partition().0 < p);
    }
}

const GOLDEN_OP7_P16: &str = "0071t7sbx8jw72bzhfyq2k5jaj6zw";
const GOLDEN_OP8_P16: &str = "002yhpcepx40309vk1cdaptr0q16j";
const GOLDEN_OP7_P1: &str = "0001t7sbx8jw72bzhfyq2k5jaj6zw";

// ------------------------------------------------------------------ derive (pure)

fn start_child_of(d: &[Derived]) -> Vec<&StartRun> {
    d.iter()
        .filter_map(|d| match &d.intent {
            Some(Intent::SendMessage(SendMessage {
                msg:
                    Some(OutboxMsg {
                        kind: Some(outbox_msg::Kind::StartChild(StartChild { start: Some(s) })),
                    }),
            })) => Some(s),
            _ => None,
        })
        .collect()
}

#[test]
fn derive_maps_prepared_child_starts_and_the_marker() {
    let parent = RunId::new_v7(P0);
    let child = child_run_id(&parent, 1, 1);
    let start = StartRun {
        run_id: child.as_bytes().to_vec(),
        parent: Some(ParentRef {
            run_id: parent.as_bytes().to_vec(),
            op: 1,
        }),
        ..Default::default()
    };
    let detached = StartRun {
        run_id: child_run_id(&parent, 2, 1).as_bytes().to_vec(),
        ..Default::default()
    };
    let events = [
        child_started(1, "leaf", attached()),
        child_started(2, "leaf", attached()),
        child_started(3, "nope", attached()),
    ];
    let starts = |op: u64| match op {
        1 => Some(start.clone()),
        2 => Some(detached.clone()),
        _ => None,
    };
    let d = derive::derive_with_children(
        &events,
        DeriveCtx {
            prior: &|_| None,
            result: None,
            successor: None,
        },
        ChildCtx {
            starts: &starts,
            parent_event: None,
            child_run: &|_| None,
        },
    )
    .expect("derive");
    assert_eq!(d.len(), 3, "{d:?}");
    assert_eq!(start_child_of(&d), vec![&start, &detached]);
    assert_eq!(
        d[1],
        Derived::child_journal(&child),
        "marker right after the attached start"
    );
    // Without children (Phase 4 entry points) a ChildStarted maps to nothing.
    assert!(
        derive::intents(&events, &|_| None, None)
            .expect("derive")
            .is_empty()
    );
}

#[test]
fn derive_sets_parent_event_on_guest_terminals_only() {
    let result = PayloadRef::inline(Bytes::from_static(b"r"));
    let pe = PayloadRef::inline(Bytes::from_static(b"pe"));
    let done = ev(
        4,
        EventBody::RunCompleted(RunCompleted {
            meta: meta(),
            output: payload(b"1"),
        }),
    );
    let d = derive::derive_with_children(
        std::slice::from_ref(&done),
        DeriveCtx {
            prior: &|_| None,
            result: Some(result.clone()),
            successor: None,
        },
        ChildCtx {
            starts: &|_| None,
            parent_event: Some(pe.clone()),
            child_run: &|_| None,
        },
    )
    .expect("derive");
    let Some(Intent::CompleteRun(c)) = &d.last().expect("intent").intent else {
        panic!("{d:?}")
    };
    assert_eq!(c.parent_event.as_ref(), Some(&pe));
    // A continue-as-new never carries one (PF-F2): the caller passing one is a bug.
    let can = ev(
        4,
        EventBody::RunContinuedAsNew(RunContinuedAsNew {
            meta: meta(),
            continuation: Continuation {
                workflow: None,
                version: None,
                input: payload(b"{}"),
            },
        }),
    );
    let e = derive::derive_with_children(
        &[can],
        DeriveCtx {
            prior: &|_| None,
            result: Some(result),
            successor: None,
        },
        ChildCtx {
            starts: &|_| None,
            parent_event: Some(pe),
            child_run: &|_| None,
        },
    );
    assert_eq!(e, Err(DeriveError::UnexpectedParentEvent));
    // parent_outcome mapping.
    assert_eq!(
        parent_outcome(&done),
        Some(Ok(OpOutput::Child(payload(b"1"))))
    );
    let cancelled = ev(
        5,
        EventBody::RunFailed(RunFailed {
            meta: meta(),
            failure: Failure::Cancelled,
        }),
    );
    assert_eq!(parent_outcome(&cancelled), Some(Err(Failure::Cancelled)));
}

// ------------------------------------------------------------------ sink: child starts

#[tokio::test]
async fn attached_child_start_is_derived_and_delivered_once() {
    let fx = Fx::new().await;
    let (run, token) = fx.start(base_start(&RunId::new_v7(P0), None)).await;
    let sink = fx.sink(run, token, Fake::leaf());
    let opts = ChildOptions {
        run_timeout_ms: Some(9_000),
        ..attached()
    };
    assert_eq!(
        sink.append(
            &rref(&run),
            token.n,
            1,
            vec![child_started(1, "leaf", opts)]
        )
        .await,
        Ok(2)
    );
    let child = child_run_id(&run, 1, 1);
    assert_eq!(fx.row(&run, &child).as_deref(), Some(CHILD_ROW_JOURNAL));
    let rec = fx.wait_record(&child).await;
    assert_eq!(
        rec.workflow, "acme/leaf",
        "qualified with the parent's tenant"
    );
    assert_eq!(rec.workflow_version, 3, "active version");
    assert_eq!(rec.module_sha, Some(LEAF_SHA));
    assert_eq!(rec.queue, "acme/leafq", "the version's default queue");
    assert_eq!(rec.parent, Some((*run.as_bytes(), 1)));
    assert_eq!(rec.tenant.as_deref(), Some("acme"));
    assert_eq!(
        rec.traceparent.as_deref(),
        Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
    );
    assert_eq!(rec.priority, 4);
    assert_eq!(rec.rng_seed.len(), 32);
    assert_ne!(rec.rng_seed, vec![0x5a; 32], "fresh seed");
    assert_eq!(rec.run_timeout_ms, Some(9_000), "options win");
    let input = query::run_payload(&fx.handle.reader(), &child, query::PAYLOAD_INPUT)
        .expect("read")
        .expect("input");
    assert_eq!(
        fx.handle
            .fetch_payload(&input)
            .await
            .expect("bytes")
            .as_ref(),
        b"{\"n\":7}"
    );
    // No completion journaled for the op (no more `unsupported`).
    assert_eq!(fx.journal(&run).await.len(), 2);

    // Crash-style re-derive: a fresh sink re-appends the same flush -> the journal already
    // holds seq 1 (Conflict), and a re-delivered start for the same id is a duplicate.
    let sink2 = fx.sink(run, token, Fake::leaf());
    sink2
        .repair_unsupported(&fx.journal(&run).await)
        .await
        .expect("repair");
    let r = sink2
        .append(
            &rref(&run),
            token.n,
            1,
            vec![child_started(1, "leaf", attached())],
        )
        .await;
    assert_eq!(r, Err(AppendError::Conflict { expected: 2 }));
    let mut again = base_start(&child, None);
    again.rng_seed = vec![1; 32];
    let o = fx
        .handle
        .submit_one(Command::start_run(again))
        .await
        .expect("submit");
    assert!(matches!(o, ApplyOutcome::Duplicate { .. }), "{o:?}");
    assert_eq!(fx.runs(), 2, "parent + exactly one child");
    assert_eq!(fx.record(&child).expect("child").rng_seed, rec.rng_seed);
    assert_eq!(
        fx.journal(&run).await.len(),
        2,
        "repair saw the row and did nothing"
    );
}

#[tokio::test]
async fn explicit_version_and_queue_and_detached() {
    let fx = Fx::new().await;
    let (run, token) = fx.start(base_start(&RunId::new_v7(P0), None)).await;
    let sink = fx.sink(run, token, Fake::leaf());
    let opts = ChildOptions {
        queue: Some("fast".into()),
        version: Some(2),
        detached: true,
        run_timeout_ms: None,
    };
    assert_eq!(
        sink.append(
            &rref(&run),
            token.n,
            1,
            vec![child_started(1, "leaf", opts)]
        )
        .await,
        Ok(2)
    );
    let child = child_run_id(&run, 1, 1);
    let rec = fx.wait_record(&child).await;
    assert_eq!(rec.parent, None, "detached: no ParentRef");
    assert_eq!(rec.workflow_version, 2);
    assert_eq!(rec.module_sha, Some([0x22; 32]));
    assert_eq!(rec.queue, "acme/fast");
    assert_eq!(rec.run_timeout_ms, None);
    assert_ne!(
        fx.row(&run, &child).as_deref(),
        Some(CHILD_ROW_JOURNAL),
        "no journaling marker for a detached child"
    );
}

#[tokio::test]
async fn unknown_target_journals_unknown_workflow_after_the_flush() {
    let fx = Fx::new().await;
    let (run, token) = fx.start(base_start(&RunId::new_v7(P0), None)).await;
    let sink = fx.sink(run, token, Fake::leaf());
    let v9 = ChildOptions {
        version: Some(9),
        ..attached()
    };
    assert_eq!(
        sink.append(
            &rref(&run),
            token.n,
            1,
            vec![
                child_started(1, "nope", attached()),
                child_started(2, "leaf", v9)
            ]
        )
        .await,
        Ok(3)
    );
    let j = fx.journal(&run).await;
    assert_eq!(j.len(), 5, "two post-flush completions");
    assert_unknown_workflow(completion_of(&j, 1));
    assert_unknown_workflow(completion_of(&j, 2));
    assert_eq!(fx.runs(), 1, "no child run");
    assert!(fx.row(&run, &child_run_id(&run, 1, 1)).is_none());
    // Repair is a no-op once completed.
    sink.repair_unsupported(&j).await.expect("repair");
    assert_eq!(fx.journal(&run).await.len(), 5);
}

// ------------------------------------------------------------------ sink: parent_event

/// A parent with child `leaf` started at op 1; returns (parent, child, child token).
async fn parent_and_child(fx: &Fx) -> (RunId, RunId, AttemptToken) {
    let (run, token) = fx.start(base_start(&RunId::new_v7(P0), None)).await;
    let sink = fx.sink(run, token, Fake::leaf());
    assert_eq!(
        sink.append(
            &rref(&run),
            token.n,
            1,
            vec![child_started(1, "leaf", attached())]
        )
        .await,
        Ok(2)
    );
    let child = child_run_id(&run, 1, 1);
    fx.wait_record(&child).await;
    let ct = fx.handle.grant_attempt(child).await.expect("grant child");
    fx.raw(&child, ct, 0, &[ev(0, step_body())]).await;
    (run, child, ct)
}

async fn child_ends(fx: &Fx, child: RunId, ct: AttemptToken, body: EventBody) {
    let sink = fx.sink(child, ct, Fake::leaf());
    assert_eq!(
        sink.append(&rref(&child), ct.n, 1, vec![ev(1, body)]).await,
        Ok(2)
    );
    assert!(sink.is_terminal());
}

#[tokio::test]
async fn child_completed_reaches_the_parent_as_ok_child() {
    let fx = Fx::new().await;
    let (run, child, ct) = parent_and_child(&fx).await;
    child_ends(
        &fx,
        child,
        ct,
        EventBody::RunCompleted(RunCompleted {
            meta: meta(),
            output: payload(b"{\"ok\":1}"),
        }),
    )
    .await;
    let j = fx.wait_journal_len(&run, 3).await;
    assert_eq!(
        completion_of(&j, 1),
        Some(Ok(OpOutput::Child(payload(b"{\"ok\":1}"))))
    );
    assert_eq!(j[2].ts_nanos, TS0 + 1_000_000, "the child's terminal ts");
    assert!(fx.row(&run, &child).is_none(), "row removed");
    assert_eq!(fx.record(&run).expect("parent").children, 0);
}

#[tokio::test]
async fn child_failed_and_cancelled_map_to_err() {
    let fx = Fx::new().await;
    let (run, child, ct) = parent_and_child(&fx).await;
    let f = Failure::NonRetryable(ErrorInfo {
        kind: "boom".into(),
        message: "bad".into(),
        details: None,
    });
    child_ends(
        &fx,
        child,
        ct,
        EventBody::RunFailed(RunFailed {
            meta: meta(),
            failure: f.clone(),
        }),
    )
    .await;
    let j = fx.wait_journal_len(&run, 3).await;
    assert_eq!(completion_of(&j, 1), Some(Err(f)));

    let fx = Fx::new().await;
    let (run, child, ct) = parent_and_child(&fx).await;
    child_ends(
        &fx,
        child,
        ct,
        EventBody::RunFailed(RunFailed {
            meta: meta(),
            failure: Failure::Cancelled,
        }),
    )
    .await;
    let j = fx.wait_journal_len(&run, 3).await;
    assert_eq!(completion_of(&j, 1), Some(Err(Failure::Cancelled)));
}

#[tokio::test]
async fn large_child_output_rides_a_blob_parent_event() {
    let fx = Fx::new().await;
    let (run, child, ct) = parent_and_child(&fx).await;
    let big = vec![b'x'; 300 * 1024];
    let mut data = b"\"".to_vec();
    data.extend_from_slice(&big);
    data.push(b'"');
    child_ends(
        &fx,
        child,
        ct,
        EventBody::RunCompleted(RunCompleted {
            meta: meta(),
            output: payload(&data),
        }),
    )
    .await;
    fx.wait_journal_len(&run, 3).await;
    let rows = query::journal_rows(&fx.handle.reader(), &run, 2, 1).expect("rows");
    let (seq, kind, pref) = &rows[0];
    assert_eq!(*seq, 2);
    assert!(matches!(pref.kind, Some(Kind::Blob(_))), "blob PayloadRef");
    let bytes = fx.handle.fetch_payload(pref).await.expect("blob");
    let e = decode_event(*seq, *kind, &bytes).expect("decode");
    let EventBody::ChildCompleted(c) = e.body else {
        panic!("{e:?}")
    };
    let out: Result<OpOutput, Failure> = decode_result(&c.outcome).expect("outcome");
    assert_eq!(out, Ok(OpOutput::Child(payload(&data))));
    // The bytes are exactly what the child's derive encodes.
    assert_eq!(
        bytes.as_ref(),
        parent_event_bytes(1, Ok(OpOutput::Child(payload(&data))), TS0 + 1_000_000).as_slice()
    );
}

#[tokio::test]
async fn continued_as_new_child_sets_no_parent_event() {
    let fx = Fx::new().await;
    let (run, child, ct) = parent_and_child(&fx).await;
    let before = fx.journal(&run).await.len();
    // Same workflow and version: the successor reuses the pinned module.
    child_ends(
        &fx,
        child,
        ct,
        EventBody::RunContinuedAsNew(RunContinuedAsNew {
            meta: meta(),
            continuation: Continuation {
                workflow: None,
                version: None,
                input: payload(b"{}"),
            },
        }),
    )
    .await;
    let rec = fx.record(&child).expect("child");
    let succ = rec.continued_to.expect("successor id");
    let srec = fx.wait_record(&succ).await;
    assert_eq!(
        srec.parent,
        Some((*run.as_bytes(), 1)),
        "the successor reports"
    );
    // The successor start and any ChildCompleted share the same outbox stream (ordered), so
    // a parent event would have been delivered by now.
    assert_eq!(fx.journal(&run).await.len(), before, "no ChildCompleted");
    assert!(fx.row(&run, &child).is_some(), "row kept for the chain");
}

// ------------------------------------------------------------------ repair (W1)

#[tokio::test]
async fn repair_resends_a_start_that_was_never_delivered() {
    let fx = Fx::new().await;
    let (run, token) = fx.start(base_start(&RunId::new_v7(P0), None)).await;
    // The flush landed without its intents (as if the target was deployed only later).
    fx.raw(&run, token, 1, &[child_started(1, "leaf", attached())])
        .await;
    let child = child_run_id(&run, 1, 1);
    assert!(fx.row(&run, &child).is_none());
    let sink = fx.sink(run, token, Fake::leaf());
    sink.repair_unsupported(&fx.journal(&run).await)
        .await
        .expect("repair");
    // Re-sent through an intents-only EventsAppended (events = []): the journal end is
    // unchanged and the row is back, marked.
    assert_eq!(sink.next_seq(), 2);
    assert_eq!(fx.record(&run).expect("parent").next_seq, 2);
    assert_eq!(fx.row(&run, &child).as_deref(), Some(CHILD_ROW_JOURNAL));
    let rec = fx.wait_record(&child).await;
    assert_eq!(rec.parent, Some((*run.as_bytes(), 1)));
    // A second load sees the row: nothing more.
    let sink2 = fx.sink(run, token, Fake::leaf());
    sink2
        .repair_unsupported(&fx.journal(&run).await)
        .await
        .expect("repair");
    assert_eq!(fx.record(&run).expect("parent").children, 1);
    assert_eq!(fx.journal(&run).await.len(), 2);
    assert_eq!(fx.runs(), 2);
}

#[tokio::test]
async fn repair_journals_unknown_workflow_once_when_still_unresolvable() {
    let fx = Fx::new().await;
    let (run, token) = fx.start(base_start(&RunId::new_v7(P0), None)).await;
    fx.raw(
        &run,
        token,
        1,
        &[
            child_started(1, "nope", attached()),
            // 09-08: an unresolvable detached child gets unknown_workflow too.
            child_started(
                2,
                "nope",
                ChildOptions {
                    detached: true,
                    ..attached()
                },
            ),
        ],
    )
    .await;
    let sink = fx.sink(run, token, Fake::leaf());
    sink.repair_unsupported(&fx.journal(&run).await)
        .await
        .expect("repair");
    let j = fx.journal(&run).await;
    assert_eq!(j.len(), 5);
    assert_unknown_workflow(completion_of(&j, 1));
    assert_unknown_workflow(completion_of(&j, 2));
    sink.repair_unsupported(&j).await.expect("repair");
    assert_eq!(fx.journal(&run).await.len(), 5, "once");
    assert_eq!(fx.runs(), 1);
}
