//! PartitionSink against a real single-partition Node (plan 04-15): fencing map, conflict,
//! blob offload, derived intents, unsupported completions, journal read-back.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use ostro_engine::sink::{ActivationOutcome, ActivationSink, AppendError, RunRef};
use ostro_log::Rf1Log;
use ostro_partition::store::values::RunStatus;
use ostro_partition::{ApplyOutcome, Node, NodeConfig, PartitionHandle, Table, query};
use ostro_server::derive;
use ostro_server::journal_io::{JournalReadError, read_journal};
use ostro_server::run_started::{build_run_started, input_payload};
use ostro_server::sink::{CHILDREN_UNSUPPORTED, EFFECTS_UNSUPPORTED, PartitionSink, RenewFn};
use ostro_types::command::{
    AttemptTokenMsg, ConflictPolicy, Derived, EventsAppended, JournalEvent, StartRun,
};
use ostro_types::journal::{
    AwaitTokenCreated, ChildOptions, ChildStarted, CommandHash, CommandMeta, EffectRequest,
    EffectScheduled, Event, EventBody, Failure, HttpRequest, OpCancelled, OpOutput, Payload,
    RunCompleted, StepCompleted, TimerStarted, decode_result, encode_result,
};
use ostro_types::payload::payload_ref::Kind;
use ostro_types::{AttemptToken, Command, PartitionCount, PartitionId, PayloadRef, RunId};
use tokio::sync::mpsc;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);
const P0: PartitionId = PartitionId(0);
const MODULE_SHA: [u8; 32] = [0xab; 32];
/// Far in the future, so the timer service never fires test timers.
const FAR_NANOS: u64 = 4_000_000_000_000_000_000;

async fn open(dir: &Path) -> Node {
    let cfg = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(1).expect("non-zero"));
    timeout(T, Node::open(cfg))
        .await
        .expect("node open timed out")
        .expect("node open")
}

fn h(node: &Node) -> PartitionHandle<Rf1Log> {
    node.handle(P0).expect("partition ready").clone()
}

/// Start a run pinned to MODULE_SHA and grant attempt 1.
async fn start(handle: &PartitionHandle<Rf1Log>) -> (RunId, AttemptToken) {
    let run = RunId::new_v7(P0);
    let o = handle
        .submit_one(Command::start_run(StartRun {
            run_id: run.as_bytes().to_vec(),
            workflow: "wf".into(),
            workflow_version: 1,
            queue: "default".into(),
            input: Some(PayloadRef::inline(Bytes::from_static(b"{\"n\":1}"))),
            conflict_policy: ConflictPolicy::ReturnExisting as i32,
            requested_at_ms: 1_700_000_000_000,
            rng_seed: vec![0x5a; 32],
            module_sha: MODULE_SHA.to_vec(),
            run_timeout_ms: 60_000,
            ..Default::default()
        }))
        .await
        .expect("start");
    assert_eq!(o, ApplyOutcome::Accepted);
    let token = handle.grant_attempt(run).await.expect("grant");
    (run, token)
}

fn rref(run: &RunId) -> RunRef {
    RunRef {
        partition: run.partition().0,
        run_id: run.to_string(),
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    node: Node,
    handle: PartitionHandle<Rf1Log>,
    run: RunId,
    token: AttemptToken,
    sink: Arc<PartitionSink>,
    renewals: Arc<AtomicUsize>,
    inputs: mpsc::UnboundedReceiver<Event>,
}

impl Fixture {
    /// Node + started/granted run + sink, with RunStarted (seq 0) already appended.
    async fn new() -> Self {
        let dir = ostro_testkit::fs::tempdir();
        let node = open(dir.path()).await;
        let handle = h(&node);
        let (run, token) = start(&handle).await;
        let renewals = Arc::new(AtomicUsize::new(0));
        let r = Arc::clone(&renewals);
        let on_renew: RenewFn = Arc::new(move || {
            r.fetch_add(1, Ordering::SeqCst);
        });
        let (tx, rx) = mpsc::unbounded_channel();
        let sink = Arc::new(PartitionSink::new(
            handle.clone(),
            run,
            token,
            Some(tx),
            Some(on_renew),
        ));
        assert_eq!(sink.next_seq(), 0);
        let f = Self {
            _dir: dir,
            node,
            handle,
            run,
            token,
            sink,
            renewals,
            inputs: rx,
        };
        let rs = f.run_started().await;
        assert_eq!(f.append(0, vec![rs]).await, Ok(1));
        f
    }

    async fn run_started(&self) -> Event {
        let rec = self.record();
        let input = self
            .handle
            .fetch_payload(
                &query::run_payload(&self.handle.reader(), &self.run, query::PAYLOAD_INPUT)
                    .expect("read")
                    .expect("input"),
            )
            .await
            .expect("input bytes");
        build_run_started(
            &rec,
            input_payload(input.to_vec()),
            &ostro_blob::hex(&MODULE_SHA),
        )
        .expect("run started")
    }

    async fn append(&self, from: u64, events: Vec<Event>) -> Result<u64, AppendError> {
        timeout(
            T,
            self.sink
                .append(&rref(&self.run), self.token.n, from, events),
        )
        .await
        .expect("append timed out")
    }

    fn record(&self) -> ostro_partition::store::values::RunRecord {
        query::run_record(&self.handle.reader(), &self.run)
            .expect("read")
            .expect("record")
    }

    async fn journal(&self) -> Vec<Event> {
        read_journal(&self.handle, &self.run)
            .await
            .expect("journal")
    }

    async fn result_bytes(&self) -> Vec<u8> {
        let p = query::run_payload(&self.handle.reader(), &self.run, query::PAYLOAD_RESULT)
            .expect("read")
            .expect("result payload");
        self.handle.fetch_payload(&p).await.expect("bytes").to_vec()
    }

    fn timers(&self) -> usize {
        self.handle
            .reader()
            .prefix(Table::Timers, &[], usize::MAX)
            .expect("timers")
            .len()
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
        ts_nanos: 1_700_000_001_000_000_000 + seq,
        body,
    }
}

fn step(seq: u64, data: Vec<u8>) -> Event {
    let out: Result<Payload, Failure> = Ok(Payload {
        content_type: "application/json".into(),
        data,
    });
    ev(
        seq,
        EventBody::StepCompleted(StepCompleted {
            meta: meta(),
            name: format!("s{seq}"),
            outcome: encode_result(&out),
            fuel_used: 1,
        }),
    )
}

fn timer(seq: u64) -> Event {
    ev(
        seq,
        EventBody::TimerStarted(TimerStarted {
            meta: meta(),
            fire_at_nanos: FAR_NANOS,
        }),
    )
}

fn effect(seq: u64) -> Event {
    ev(
        seq,
        EventBody::EffectScheduled(EffectScheduled {
            meta: meta(),
            request: EffectRequest::Http(HttpRequest {
                method: "GET".into(),
                url: "http://example.invalid/".into(),
                headers: vec![],
                body: None,
                timeout_ms: None,
                idempotency_key: None,
                redact: vec![],
            }),
        }),
    )
}

fn assert_unsupported(outcome: &ostro_types::journal::RecordedResult, message: &str) {
    let r: Result<OpOutput, Failure> = decode_result(outcome).expect("decode outcome");
    match r {
        Err(Failure::NonRetryable(i)) => {
            assert_eq!(i.kind, "unsupported");
            assert_eq!(i.message, message);
        }
        other => panic!("expected NonRetryable unsupported, got {other:?}"),
    }
}

#[tokio::test]
async fn accepted_append_round_trips_and_renews() {
    let f = Fixture::new().await;
    let batch = vec![step(1, b"{\"a\":1}".to_vec()), timer(2)];
    assert_eq!(f.append(1, batch.clone()).await, Ok(3));
    assert_eq!(f.sink.next_seq(), 3);
    assert_eq!(f.renewals.load(Ordering::SeqCst), 2);
    let j = f.journal().await;
    assert_eq!(j.len(), 3);
    assert_eq!(j[0], f.run_started().await);
    assert_eq!(&j[1..], &batch[..]);
    // RunStarted carries the pinned module and the deadline from run_timeout_ms.
    let EventBody::RunStarted(rs) = &j[0].body else {
        panic!("seq 0 is not RunStarted")
    };
    assert_eq!(rs.module_hash, ostro_blob::hex(&MODULE_SHA));
    assert_eq!(
        rs.deadline_nanos,
        Some(rs.accepted_at_nanos + 60_000 * 1_000_000)
    );
    // TimerStarted -> ScheduleTimer.
    assert_eq!(f.timers(), 1);
    let rec = f.record();
    assert_eq!(rec.next_seq, 3);
    assert_eq!(rec.status, RunStatus::Running);
}

#[tokio::test]
async fn op_cancelled_timer_from_journal_cancels_timer() {
    let f = Fixture::new().await;
    assert_eq!(f.append(1, vec![timer(1)]).await, Ok(2));
    assert_eq!(f.timers(), 1);
    let cancel = ev(
        2,
        EventBody::OpCancelled(OpCancelled {
            meta: meta(),
            op: 1,
        }),
    );
    assert_eq!(f.append(2, vec![cancel]).await, Ok(3));
    assert_eq!(
        f.timers(),
        0,
        "CancelTimer derived from the journaled TimerStarted"
    );
}

#[tokio::test]
async fn newer_attempt_fences_old_sink() {
    let f = Fixture::new().await;
    let t2 = f.handle.grant_attempt(f.run).await.expect("grant 2");
    assert_eq!(t2.n, f.token.n + 1);
    assert_eq!(
        f.append(1, vec![step(1, b"1".to_vec())]).await,
        Err(AppendError::Fenced)
    );
    assert_eq!(
        f.journal().await.len(),
        1,
        "fenced events are not journaled"
    );
    assert_eq!(f.renewals.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn wrong_attempt_number_is_fenced_without_submit() {
    let f = Fixture::new().await;
    let applied = *f.handle.machine().applied().borrow();
    let r = f
        .sink
        .append(
            &rref(&f.run),
            f.token.n + 1,
            1,
            vec![step(1, b"1".to_vec())],
        )
        .await;
    assert_eq!(r, Err(AppendError::Fenced));
    assert_eq!(
        *f.handle.machine().applied().borrow(),
        applied,
        "nothing submitted"
    );
}

#[tokio::test]
async fn stale_from_seq_is_conflict() {
    let f = Fixture::new().await;
    assert_eq!(f.append(1, vec![step(1, b"1".to_vec())]).await, Ok(2));
    assert_eq!(
        f.append(1, vec![step(1, b"2".to_vec())]).await,
        Err(AppendError::Conflict { expected: 2 })
    );
    assert_eq!(f.sink.next_seq(), 2);
}

#[tokio::test]
async fn shutdown_node_is_unavailable() {
    let f = Fixture::new().await;
    let Fixture {
        _dir,
        node,
        sink,
        run,
        token,
        ..
    } = f;
    timeout(T, node.shutdown())
        .await
        .expect("shutdown")
        .expect("shutdown ok");
    let r = timeout(
        T,
        sink.append(&rref(&run), token.n, 1, vec![step(1, b"1".to_vec())]),
    )
    .await
    .expect("append timed out");
    assert!(matches!(r, Err(AppendError::Unavailable(_))), "got {r:?}");
}

#[tokio::test]
async fn large_payload_goes_to_blob_store() {
    let f = Fixture::new().await;
    let big: Vec<u8> = (0..300 * 1024).map(|i| (i % 251) as u8).collect();
    let e = step(1, big);
    assert_eq!(f.append(1, vec![e.clone()]).await, Ok(2));
    let rows = query::journal_rows(&f.handle.reader(), &f.run, 1, 10).expect("rows");
    assert_eq!(rows.len(), 1);
    let Some(Kind::Blob(b)) = &rows[0].2.kind else {
        panic!("300 KiB event stored inline")
    };
    let sha: [u8; 32] = b.sha256.as_slice().try_into().expect("sha");
    assert!(f.handle.blobs().contains(&ostro_blob::BlobRef {
        sha256: sha,
        len: b.len
    }));
    assert_eq!(f.journal().await[1], e);
}

#[tokio::test]
async fn run_completed_completes_run_with_output() {
    let f = Fixture::new().await;
    let done = ev(
        2,
        EventBody::RunCompleted(RunCompleted {
            meta: meta(),
            output: Payload {
                content_type: "application/json".into(),
                data: b"{\"ok\":true}".to_vec(),
            },
        }),
    );
    let ts = done.ts_nanos;
    assert_eq!(f.append(1, vec![step(1, b"1".to_vec()), done]).await, Ok(3));
    assert!(f.sink.is_terminal());
    let rec = f.record();
    assert_eq!(rec.status, RunStatus::Completed);
    assert_eq!(rec.completed_at_ms, Some(derive::ceil_ms(ts)));
    assert_eq!(f.result_bytes().await, b"{\"ok\":true}");
    // Later appends hit a terminal run: Fenced.
    assert_eq!(
        f.append(3, vec![step(3, b"1".to_vec())]).await,
        Err(AppendError::Fenced)
    );
}

#[tokio::test]
async fn effect_gets_unsupported_completion_input() {
    let mut f = Fixture::new().await;
    assert_eq!(f.append(1, vec![effect(1)]).await, Ok(2));
    assert_eq!(f.sink.next_seq(), 3, "completion journaled at the next seq");
    let j = f.journal().await;
    assert_eq!(j.len(), 3);
    let EventBody::EffectCompleted(c) = &j[2].body else {
        panic!("seq 2 is {:?}", j[2].body)
    };
    assert_eq!(c.op, 1);
    assert_unsupported(&c.outcome, EFFECTS_UNSUPPORTED);
    let sent = f.inputs.try_recv().expect("input forwarded");
    assert_eq!(sent, j[2]);
    // The engine's next append at its own view (2) conflicts: re-activation replays it.
    assert_eq!(
        f.append(2, vec![step(2, b"1".to_vec())]).await,
        Err(AppendError::Conflict { expected: 3 })
    );
}

#[tokio::test]
async fn child_gets_unsupported_completion_input() {
    let mut f = Fixture::new().await;
    let child = ev(
        1,
        EventBody::ChildStarted(ChildStarted {
            meta: meta(),
            workflow: "kid".into(),
            input: Payload {
                content_type: "application/json".into(),
                data: b"{}".to_vec(),
            },
            options: ChildOptions {
                queue: None,
                version: None,
                detached: false,
                run_timeout_ms: None,
            },
        }),
    );
    assert_eq!(f.append(1, vec![child, effect(2)]).await, Ok(3));
    let j = f.journal().await;
    assert_eq!(j.len(), 5);
    let EventBody::ChildCompleted(c) = &j[3].body else {
        panic!("seq 3 is {:?}", j[3].body)
    };
    assert_eq!(c.op, 1);
    assert_unsupported(&c.outcome, CHILDREN_UNSUPPORTED);
    let EventBody::EffectCompleted(e) = &j[4].body else {
        panic!("seq 4 is {:?}", j[4].body)
    };
    assert_eq!(e.op, 2);
    assert_eq!(f.inputs.try_recv().expect("child input"), j[3]);
    assert_eq!(f.inputs.try_recv().expect("effect input"), j[4]);
    // Repair is a no-op once every op has a completion.
    f.sink.repair_unsupported(&j).await.expect("repair");
    assert_eq!(f.journal().await.len(), 5);
}

#[tokio::test]
async fn repair_completes_effects_left_without_completion() {
    let f = Fixture::new().await;
    // Simulate a lost second append: journal an EffectScheduled with a raw command.
    let e = effect(1);
    let (kind, bytes) = ostro_types::journal::encode_event(&e);
    let o = f
        .handle
        .submit_one(Command::events_appended(EventsAppended {
            run_id: f.run.as_bytes().to_vec(),
            token: Some(AttemptTokenMsg::from(f.token)),
            from_seq: 1,
            events: vec![JournalEvent {
                seq: 1,
                kind,
                payload: Some(PayloadRef::inline(bytes.into())),
            }],
            derived: vec![],
        }))
        .await
        .expect("submit");
    assert_eq!(o, ApplyOutcome::Accepted);
    let sink = PartitionSink::new(f.handle.clone(), f.run, f.token, None, None);
    assert_eq!(
        sink.next_seq(),
        2,
        "next_seq starts at the record's journal end"
    );
    sink.repair_unsupported(&f.journal().await)
        .await
        .expect("repair");
    let j = f.journal().await;
    assert_eq!(j.len(), 3);
    assert!(matches!(&j[2].body, EventBody::EffectCompleted(c) if c.op == 1));
}

#[tokio::test]
async fn await_token_fails_run_unsupported() {
    let f = Fixture::new().await;
    let tok = ev(
        1,
        EventBody::AwaitTokenCreated(AwaitTokenCreated {
            meta: meta(),
            token: "tok".into(),
            timeout_ms: None,
        }),
    );
    assert_eq!(f.append(1, vec![tok]).await, Ok(2));
    assert!(f.sink.is_terminal());
    let rec = f.record();
    assert_eq!(rec.status, RunStatus::Failed);
    let v: serde_json::Value = serde_json::from_slice(&f.result_bytes().await).expect("json");
    assert_eq!(v["reason_code"], "unsupported");
    assert_eq!(
        f.append(2, vec![step(2, b"1".to_vec())]).await,
        Err(AppendError::Fenced)
    );
}

#[tokio::test]
async fn suspend_via_submit_intents_and_not_running_is_fenced() {
    let f = Fixture::new().await;
    assert_eq!(f.sink.submit_intents(vec![Derived::suspend()]).await, Ok(1));
    assert_eq!(f.record().status, RunStatus::Scheduled);
    // Not Running any more: Invalid(NotRunning) maps to Fenced.
    assert_eq!(
        f.sink.submit_intents(vec![Derived::suspend()]).await,
        Err(AppendError::Fenced)
    );
}

#[tokio::test]
async fn done_stores_outcome_and_fetch_blob_reads_blobs() {
    let f = Fixture::new().await;
    assert_eq!(f.sink.outcome(), None);
    f.sink
        .done(&rref(&f.run), f.token.n, ActivationOutcome::Fenced)
        .await;
    assert_eq!(f.sink.outcome(), Some(ActivationOutcome::Fenced));
    assert_eq!(f.sink.take_outcome(), Some(ActivationOutcome::Fenced));
    assert_eq!(f.sink.take_outcome(), None);

    let bytes = b"component bytes".to_vec();
    let r = f.handle.blobs().put(&bytes).expect("put");
    assert_eq!(f.sink.fetch_blob(&r.sha256).await, Ok(bytes));
    assert!(matches!(
        f.sink.fetch_blob(&[9; 32]).await,
        Err(AppendError::Unavailable(_))
    ));
}

#[tokio::test]
async fn read_journal_decode_failure_names_run_and_seq() {
    let f = Fixture::new().await;
    let o = f
        .handle
        .submit_one(Command::events_appended(EventsAppended {
            run_id: f.run.as_bytes().to_vec(),
            token: Some(AttemptTokenMsg::from(f.token)),
            from_seq: 1,
            events: vec![JournalEvent {
                seq: 1,
                kind: 999,
                payload: Some(PayloadRef::inline(Bytes::from_static(b"\x01junk"))),
            }],
            derived: vec![],
        }))
        .await
        .expect("submit");
    assert_eq!(o, ApplyOutcome::Accepted);
    let err = read_journal(&f.handle, &f.run)
        .await
        .expect_err("decode error");
    assert!(
        matches!(err, JournalReadError::Decode { seq: 1, .. }),
        "{err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains(&f.run.to_string()) && msg.contains("seq 1"),
        "{msg}"
    );
}
