//! Pure deterministic apply core (plan 02-09; STOR-05, STOR-10, STOR-11, STOR-14):
//! epoch and attempt fencing, idempotent starts, fenced appends with Derived
//! intents, payload limits, outbox emission, read budget and the rolling hash.

use bytes::Bytes;
use ostro_log::VerifiedBatch;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, START_DELAY_OP, apply_chunk,
    apply_command, fold_hash,
};
use ostro_partition::store::keys::{
    children_key, dedupe_key, event_key, outbox_key, payload_key, ready_key, run_key, timer_key,
};
use ostro_partition::store::values::{self, ReadySlot, ReadyValue, RunRecord, RunStatus};
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, ChildCompleted, CompleteRun, ConflictPolicy, Derived,
    EnqueueReady, EventsAppended, JournalEvent, OutboxMsg, ParentRef, ScheduleTimer, SendMessage,
    Signal, StartRun, TerminalStatus, derived, encode_command, outbox_msg,
};
use ostro_types::{
    AttemptToken, Command, Epoch, INLINE_PAYLOAD_LIMIT, Offset, PartitionCount, PartitionId,
    PayloadRef, RunId,
};
use prost::Message;

// ---------------------------------------------------------------- helpers

struct H {
    dir: tempfile::TempDir,
    store: PartitionStore,
    cur: Cursor,
}

impl H {
    fn new(p: u16) -> Self {
        let dir = ostro_testkit::fs::tempdir();
        let (store, meta) = PartitionStore::open(
            dir.path(),
            PartitionId(p),
            PartitionCount::new(16).expect("non-zero"),
            &StoreConfig::default(),
        )
        .expect("open store");
        let cur = Cursor::from_meta(&meta);
        H { dir, store, cur }
    }

    /// Applies one raw command at the next offset in its own batch and
    /// commits. Returns the outcome, the actions and the keyspace reads.
    fn apply_raw(&mut self, env: u32, bytes: &[u8]) -> (ApplyOutcome, Vec<Action>, u64) {
        let mut b = self.store.batch();
        let mut actions = Vec::new();
        let off = self.cur.next;
        let before = b.reads();
        let out = apply_command(&mut b, &mut self.cur, off, Epoch(env), bytes, &mut actions)
            .expect("apply");
        let reads = b.reads() - before;
        let mu = self.cur.meta_update();
        b.commit(&mu).expect("commit");
        (out, actions, reads)
    }

    fn apply(&mut self, env: u32, c: &Command) -> ApplyOutcome {
        self.apply_raw(env, &encode_command(c)).0
    }

    fn apply_act(&mut self, env: u32, c: &Command) -> (ApplyOutcome, Vec<Action>) {
        let (o, a, _) = self.apply_raw(env, &encode_command(c));
        (o, a)
    }

    fn get(&self, t: Table, k: &[u8]) -> Option<Vec<u8>> {
        self.store.reader().get(t, k).expect("read")
    }

    fn run(&self, id: &RunId) -> Option<RunRecord> {
        self.get(Table::Runs, &run_key(id))
            .map(|v| values::decode(&v).expect("decode run"))
    }
}

fn rid(p: u16, n: u8) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&p.to_be_bytes());
    b[2] = n.wrapping_mul(7);
    b[17] = n;
    RunId::from_bytes(&b).expect("18 bytes")
}

fn inline(b: &'static [u8]) -> Option<PayloadRef> {
    Some(PayloadRef::inline(Bytes::from_static(b)))
}

fn inline_len(n: usize) -> Option<PayloadRef> {
    Some(PayloadRef::inline(Bytes::from(vec![0xab; n])))
}

fn start(id: &RunId) -> StartRun {
    StartRun {
        run_id: id.as_bytes().to_vec(),
        workflow: "wf".into(),
        workflow_version: 1,
        queue: "q".into(),
        input: inline(b"in"),
        idempotency_key: vec![],
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        concurrency_key: "ck".into(),
        priority: 5,
        ready_at_ms: 100,
        requested_at_ms: 100,
        parent: None,
        rng_seed: vec![7; 32],
        ..Default::default()
    }
}

fn start_cmd(id: &RunId) -> Command {
    Command::start_run(start(id))
}

fn la(e: u32) -> Command {
    Command::leader_announced(e, 1, 0)
}

fn grant(id: &RunId, e: u32, n: u32) -> Command {
    Command::attempt_granted(AttemptGranted {
        run_id: id.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: e, n }),
        after_failure: false,
    })
}

fn ev(seq: u64) -> JournalEvent {
    JournalEvent {
        seq,
        kind: 16,
        payload: inline(b"e"),
    }
}

fn events(
    id: &RunId,
    tok: (u32, u32),
    from: u64,
    evs: Vec<JournalEvent>,
    derived: Vec<Derived>,
) -> Command {
    Command::events_appended(EventsAppended {
        run_id: id.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg {
            epoch: tok.0,
            n: tok.1,
        }),
        from_seq: from,
        events: evs,
        derived,
    })
}

fn intent(i: derived::Intent) -> Derived {
    Derived { intent: Some(i) }
}

fn tok(e: u32, n: u32) -> AttemptToken {
    AttemptToken { epoch: Epoch(e), n }
}

fn rej(r: RejectReason) -> ApplyOutcome {
    ApplyOutcome::Rejected(r)
}

/// Epoch 1 announced, run `r` started and granted `{1,1}`.
fn running(p: u16, n: u8) -> (H, RunId) {
    let mut h = H::new(p);
    let r = rid(p, n);
    assert_eq!(h.apply(1, &la(1)), ApplyOutcome::Accepted);
    assert_eq!(h.apply(1, &start_cmd(&r)), ApplyOutcome::Accepted);
    assert_eq!(h.apply(1, &grant(&r, 1, 1)), ApplyOutcome::Accepted);
    (h, r)
}

// ---------------------------------------------------------------- fencing

#[test]
fn fencing_leader_announced() {
    let mut h = H::new(0);
    assert_eq!(h.apply(1, &la(1)), ApplyOutcome::Accepted);
    assert_eq!(h.cur.epoch(), Epoch(1));
    assert_eq!(h.cur.meta.epoch_history, vec![(1, 0)]);

    let stale = rej(RejectReason::StaleEpoch { current: Epoch(1) });
    assert_eq!(h.apply(1, &la(1)), stale);
    assert_eq!(h.apply(2, &la(3)), stale);
    assert_eq!(h.apply(0, &start_cmd(&rid(0, 1))), stale);
    assert!(h.run(&rid(0, 1)).is_none());
    assert_eq!(h.cur.epoch(), Epoch(1));

    // Persisted with the batch.
    let m = h.store.read_meta().expect("meta");
    assert_eq!(m.cursor.epoch, 1);
    assert_eq!(m.cursor.epoch_history, vec![(1, 0)]);
    assert_eq!(m.applied.0, 4);
}

#[test]
fn fencing_attempts() {
    let mut h = H::new(0);
    let r = rid(0, 1);
    assert_eq!(h.apply(1, &la(1)), ApplyOutcome::Accepted);
    assert_eq!(h.apply(1, &start_cmd(&r)), ApplyOutcome::Accepted);
    assert_eq!(h.apply(1, &grant(&r, 1, 1)), ApplyOutcome::Accepted);
    assert_eq!(
        h.apply(1, &grant(&r, 1, 1)),
        rej(RejectReason::Fenced {
            current: Some(tok(1, 1))
        })
    );
    assert_eq!(h.apply(1, &grant(&r, 1, 2)), ApplyOutcome::Accepted);
    assert_eq!(h.apply(2, &la(2)), ApplyOutcome::Accepted);
    assert_eq!(
        h.apply(2, &grant(&r, 1, 3)),
        rej(RejectReason::StaleEpoch { current: Epoch(2) })
    );
    assert_eq!(h.apply(2, &grant(&r, 2, 1)), ApplyOutcome::Accepted);
    let run = h.run(&r).expect("run");
    assert_eq!(run.current_attempt, Some((2, 1)));
    assert_eq!(run.status, RunStatus::Running);

    // Unknown run, missing token.
    assert_eq!(
        h.apply(2, &grant(&rid(0, 9), 2, 5)),
        rej(RejectReason::RunNotFound)
    );
    let no_tok = Command::attempt_granted(AttemptGranted {
        run_id: r.as_bytes().to_vec(),
        token: None,
        after_failure: false,
    });
    assert_eq!(
        h.apply(2, &no_tok),
        rej(RejectReason::Invalid(InvalidCode::MissingToken))
    );
}

#[test]
fn fencing_zombie() {
    let (mut h, r) = running(0, 1);
    assert_eq!(h.apply(1, &grant(&r, 1, 2)), ApplyOutcome::Accepted);
    // The zombie holding {1,1} appends first: fenced.
    assert_eq!(
        h.apply(1, &events(&r, (1, 1), 0, vec![ev(0)], vec![])),
        rej(RejectReason::Fenced {
            current: Some(tok(1, 2))
        })
    );
    assert!(h.get(Table::Events, &event_key(&r, 0)).is_none());
    assert_eq!(
        h.apply(1, &events(&r, (1, 2), 0, vec![ev(0)], vec![])),
        ApplyOutcome::Accepted
    );
    assert!(h.get(Table::Events, &event_key(&r, 0)).is_some());
}

// ---------------------------------------------------------------- events

#[test]
fn events_seq() {
    let (mut h, r) = running(0, 1);
    assert_eq!(
        h.apply(1, &events(&r, (1, 1), 0, vec![ev(0), ev(1)], vec![])),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.run(&r).expect("run").next_seq, 2);
    let stored = h.get(Table::Events, &event_key(&r, 1)).expect("event 1");
    assert_eq!(JournalEvent::decode(&*stored).expect("prost"), ev(1));

    assert_eq!(
        h.apply(1, &events(&r, (1, 1), 0, vec![ev(0), ev(1)], vec![])),
        rej(RejectReason::SeqMismatch { expected: 2 })
    );
    assert_eq!(
        h.apply(1, &events(&r, (1, 1), 2, vec![ev(5), ev(7)], vec![])),
        rej(RejectReason::Invalid(InvalidCode::EventsNotContiguous))
    );
    assert!(h.get(Table::Events, &event_key(&r, 5)).is_none());

    // All-or-nothing: event 3 is oversize, so neither event 2 nor the timer
    // intent may be written.
    let mut big = ev(3);
    big.payload = inline_len(INLINE_PAYLOAD_LIMIT + 1);
    let sched = intent(derived::Intent::ScheduleTimer(ScheduleTimer {
        op: 4,
        due_at_ms: 999,
    }));
    assert_eq!(
        h.apply(1, &events(&r, (1, 1), 2, vec![ev(2), big], vec![sched])),
        rej(RejectReason::PayloadTooLarge)
    );
    assert!(h.get(Table::Events, &event_key(&r, 2)).is_none());
    assert!(h.get(Table::Timers, &timer_key(999, &r, 4)).is_none());
    assert_eq!(h.run(&r).expect("run").next_seq, 2);

    let no_tok = Command::events_appended(EventsAppended {
        run_id: r.as_bytes().to_vec(),
        token: None,
        from_seq: 2,
        events: vec![ev(2)],
        derived: vec![],
    });
    assert_eq!(
        h.apply(1, &no_tok),
        rej(RejectReason::Invalid(InvalidCode::MissingToken))
    );
    assert_eq!(
        h.apply(1, &events(&rid(0, 9), (1, 1), 0, vec![ev(0)], vec![])),
        rej(RejectReason::RunNotFound)
    );
}

// ---------------------------------------------------------------- start run

#[test]
fn start_run_dedupe() {
    let mut h = H::new(0);
    let (r1, r2, r3, r4) = (rid(0, 1), rid(0, 2), rid(0, 3), rid(0, 4));
    let keyed = |id: &RunId, policy: ConflictPolicy| {
        let mut s = start(id);
        s.idempotency_key = b"k".to_vec();
        s.conflict_policy = policy as i32;
        Command::start_run(s)
    };
    assert_eq!(
        h.apply(0, &keyed(&r1, ConflictPolicy::ReturnExisting)),
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.get(Table::Dedupe, &dedupe_key("wf", b"k")),
        Some(r1.as_bytes().to_vec())
    );
    assert_eq!(
        h.apply(0, &keyed(&r2, ConflictPolicy::ReturnExisting)),
        ApplyOutcome::Duplicate { existing: Some(r1) }
    );
    assert!(h.run(&r2).is_none());
    assert_eq!(
        h.apply(0, &keyed(&r3, ConflictPolicy::Reject)),
        rej(RejectReason::IdempotencyConflict { existing: r1 })
    );
    assert!(h.run(&r3).is_none());

    assert_eq!(h.apply(0, &start_cmd(&r4)), ApplyOutcome::Accepted);
    assert_eq!(
        h.apply(0, &start_cmd(&r4)),
        ApplyOutcome::Duplicate { existing: Some(r4) }
    );

    assert_eq!(
        h.apply(0, &start_cmd(&rid(3, 5))),
        rej(RejectReason::WrongPartition)
    );
    let mut bad = start(&r4);
    bad.run_id = vec![1, 2, 3];
    assert_eq!(
        h.apply(0, &Command::start_run(bad)),
        rej(RejectReason::Invalid(InvalidCode::BadRunId))
    );
}

#[test]
fn start_run_delayed() {
    let mut h = H::new(0);
    let r1 = rid(0, 1);
    let mut s = start(&r1);
    s.ready_at_ms = 500;
    let (o, a) = h.apply_act(0, &Command::start_run(s));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert_eq!(h.run(&r1).expect("run").status, RunStatus::Scheduled);
    assert!(
        h.get(Table::Timers, &timer_key(500, &r1, START_DELAY_OP))
            .is_some()
    );
    assert_eq!(
        a,
        vec![Action::ScheduleTimer {
            due_at_ms: 500,
            run: r1,
            op: START_DELAY_OP
        }]
    );

    let r2 = rid(0, 2);
    let (o, a) = h.apply_act(0, &start_cmd(&r2));
    assert_eq!(o, ApplyOutcome::Accepted);
    let run = h.run(&r2).expect("run");
    assert_eq!(run.status, RunStatus::Ready);
    assert_eq!(
        run.ready,
        Some(ReadySlot {
            priority: 5,
            ready_at_ms: 100
        })
    );
    assert_eq!(run.rng_seed, vec![7; 32]);
    assert_eq!(run.workflow, "wf");
    let rk = ready_key("q", 5, 100, &r2);
    let rv: ReadyValue = values::decode(&h.get(Table::Ready, &rk).expect("ready row")).unwrap();
    assert_eq!(rv.concurrency_key, "ck");
    assert_eq!(
        a,
        vec![Action::EnqueueReady {
            run: r2,
            queue: "q".into(),
            priority: 5,
            ready_at_ms: 100,
            concurrency_key: "ck".into()
        }]
    );
    let input = h.get(Table::Payloads, &payload_key(&r2, 0)).expect("input");
    assert_eq!(PayloadRef::decode(&*input).unwrap(), inline(b"in").unwrap());

    assert_eq!(h.apply(0, &grant(&r2, 0, 1)), ApplyOutcome::Accepted);
    assert!(h.get(Table::Ready, &rk).is_none());
    let run = h.run(&r2).expect("run");
    assert_eq!(run.status, RunStatus::Running);
    assert_eq!(run.ready, None);
}

// ---------------------------------------------------------------- payloads

#[test]
fn payload_limits() {
    let mut h = H::new(0);
    let mut s = start(&rid(0, 1));
    s.input = inline_len(INLINE_PAYLOAD_LIMIT + 1);
    assert_eq!(
        h.apply(0, &Command::start_run(s)),
        rej(RejectReason::PayloadTooLarge)
    );
    assert!(h.run(&rid(0, 1)).is_none());

    let mut s = start(&rid(0, 2));
    s.input = inline_len(INLINE_PAYLOAD_LIMIT);
    assert_eq!(h.apply(0, &Command::start_run(s)), ApplyOutcome::Accepted);

    let mut s = start(&rid(0, 3));
    s.input = Some(PayloadRef::blob([9; 32], 10 << 20));
    assert_eq!(h.apply(0, &Command::start_run(s)), ApplyOutcome::Accepted);

    let (mut h, r) = running(0, 4);
    let mut big = ev(0);
    big.payload = inline_len(INLINE_PAYLOAD_LIMIT + 1);
    assert_eq!(
        h.apply(1, &events(&r, (1, 1), 0, vec![big], vec![])),
        rej(RejectReason::PayloadTooLarge)
    );
    let big_result = intent(derived::Intent::CompleteRun(CompleteRun {
        status: TerminalStatus::Completed as i32,
        result: inline_len(INLINE_PAYLOAD_LIMIT + 1),
        completed_at_ms: 1,
        resumable: false,
        parent_event: None,
    }));
    assert_eq!(
        h.apply(1, &events(&r, (1, 1), 0, vec![ev(0)], vec![big_result])),
        rej(RejectReason::PayloadTooLarge)
    );
    assert_eq!(h.run(&r).expect("run").status, RunStatus::Running);
}

// ---------------------------------------------------------------- completion

#[test]
fn complete_run() {
    let (mut h, r) = running(0, 1);
    let done = intent(derived::Intent::CompleteRun(CompleteRun {
        status: TerminalStatus::Completed as i32,
        result: inline(b"out"),
        completed_at_ms: 9,
        resumable: false,
        parent_event: None,
    }));
    let (o, a) = h.apply_act(1, &events(&r, (1, 1), 0, vec![ev(0)], vec![done]));
    assert_eq!(o, ApplyOutcome::Accepted);
    let run = h.run(&r).expect("run");
    assert_eq!(run.status, RunStatus::Completed);
    assert_eq!(run.next_seq, 1);
    let res = h.get(Table::Payloads, &payload_key(&r, 1)).expect("result");
    assert_eq!(PayloadRef::decode(&*res).unwrap(), inline(b"out").unwrap());
    assert_eq!(
        a,
        vec![Action::RunCompleted {
            run: r,
            status: RunStatus::Completed
        }]
    );

    assert_eq!(
        h.apply(1, &events(&r, (1, 1), 1, vec![ev(1)], vec![])),
        rej(RejectReason::RunTerminal)
    );
    assert_eq!(h.apply(1, &grant(&r, 1, 2)), rej(RejectReason::RunTerminal));
}

#[test]
fn complete_run_then_more_intents_is_invalid() {
    let (mut h, r) = running(0, 1);
    let done = intent(derived::Intent::CompleteRun(CompleteRun {
        status: TerminalStatus::Failed as i32,
        result: None,
        completed_at_ms: 9,
        resumable: false,
        parent_event: None,
    }));
    let again = intent(derived::Intent::EnqueueReady(EnqueueReady {
        ready_at_ms: 5,
    }));
    assert_eq!(
        h.apply(1, &events(&r, (1, 1), 0, vec![], vec![done, again])),
        rej(RejectReason::Invalid(InvalidCode::BadIntent))
    );
    assert_eq!(h.run(&r).expect("run").status, RunStatus::Running);
}

#[test]
fn enqueue_ready_intent() {
    let (mut h, r) = running(0, 1);
    let rdy = intent(derived::Intent::EnqueueReady(EnqueueReady {
        ready_at_ms: 77,
    }));
    let (o, a) = h.apply_act(1, &events(&r, (1, 1), 0, vec![ev(0)], vec![rdy]));
    assert_eq!(o, ApplyOutcome::Accepted);
    let run = h.run(&r).expect("run");
    assert_eq!(run.status, RunStatus::Ready);
    assert!(h.get(Table::Ready, &ready_key("q", 5, 77, &r)).is_some());
    assert_eq!(
        a,
        vec![Action::EnqueueReady {
            run: r,
            queue: "q".into(),
            priority: 5,
            ready_at_ms: 77,
            concurrency_key: "ck".into()
        }]
    );
    // A new grant takes it again and removes the ready row.
    assert_eq!(h.apply(1, &grant(&r, 1, 2)), ApplyOutcome::Accepted);
    assert!(h.get(Table::Ready, &ready_key("q", 5, 77, &r)).is_none());
}

// ---------------------------------------------------------------- malformed

#[test]
fn malformed() {
    let mut h = H::new(0);
    let (o, a, _) = h.apply_raw(0, &[0xff, 0xff, 0xff]);
    assert_eq!(o, rej(RejectReason::Invalid(InvalidCode::Decode)));
    assert!(a.is_empty());
    assert_eq!(h.cur.next.0, 1);
    let (o, _, _) = h.apply_raw(0, &[]);
    assert_eq!(o, rej(RejectReason::Invalid(InvalidCode::Empty)));
    assert_eq!(h.cur.next.0, 2);
    // dir handle kept alive for the store's lifetime
    assert!(h.dir.path().exists());
}

// ---------------------------------------------------------------- rolling hash

/// fold_hash([0; 32], Offset(0), 0, b"x"). Frozen: changing it changes every stored state_hash.
const FOLD_GOLDEN: &str = "2044af1ee71a81a4a6c36183d045adb1459b0d08fe1b1f911510794ae431f75e";

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[test]
fn rolling_hash_golden() {
    let h0 = [0u8; 32];
    let got = fold_hash(&h0, Offset(0), 0, b"x");
    // First principles: blake3(prev ‖ off_le ‖ code ‖ bytes).
    let mut input = Vec::new();
    input.extend_from_slice(&h0);
    input.extend_from_slice(&0u64.to_le_bytes());
    input.push(0);
    input.extend_from_slice(b"x");
    assert_eq!(got, *blake3::hash(&input).as_bytes());
    assert_eq!(hex(&got), FOLD_GOLDEN);
    // Every input byte matters.
    assert_ne!(fold_hash(&h0, Offset(1), 0, b"x"), got);
    assert_ne!(fold_hash(&h0, Offset(0), 1, b"x"), got);
    assert_ne!(fold_hash(&h0, Offset(0), 0, b"y"), got);
}

#[test]
fn rolling_hash_outcome_codes_frozen() {
    use RejectReason::*;
    let r = rid(0, 1);
    let cases: Vec<(ApplyOutcome, u8)> = vec![
        (ApplyOutcome::Accepted, 0),
        (ApplyOutcome::Duplicate { existing: None }, 1),
        (rej(StaleEpoch { current: Epoch(0) }), 16),
        (rej(Fenced { current: None }), 17),
        (rej(SeqMismatch { expected: 0 }), 18),
        (
            rej(SeqGap {
                expected: 0,
                found: 1,
            }),
            19,
        ),
        (rej(RunNotFound), 20),
        (rej(RunTerminal), 21),
        (rej(RunExists), 22),
        (rej(IdempotencyConflict { existing: r }), 23),
        (rej(PayloadTooLarge), 24),
        (rej(WrongPartition), 25),
        (rej(Invalid(InvalidCode::Decode)), 26),
        (rej(Invalid(InvalidCode::FieldTooLong)), 26),
    ];
    for (o, code) in cases {
        assert_eq!(o.code(), code, "{o:?}");
        assert_eq!(o.is_accepted(), code == 0);
    }
}

fn signal_to(target: &RunId) -> Derived {
    intent(derived::Intent::SendMessage(SendMessage {
        msg: Some(OutboxMsg {
            kind: Some(outbox_msg::Kind::Signal(Signal {
                target_run: target.as_bytes().to_vec(),
                name: "s".into(),
                payload: inline(b"p"),
            })),
        }),
    }))
}

fn complete(status: TerminalStatus) -> Derived {
    intent(derived::Intent::CompleteRun(CompleteRun {
        status: status as i32,
        result: inline(b"out"),
        completed_at_ms: 9,
        resumable: false,
        parent_event: None,
    }))
}

/// A 200-command mix of accepted, duplicate and rejected commands.
fn workload() -> Vec<(u32, Bytes)> {
    let enc = |c: &Command| encode_command(c);
    let mut v = vec![(1, enc(&la(1)))];
    let mut i = 0u8;
    while v.len() < 200 {
        i += 1;
        let r = rid(0, i);
        let mut s = start(&r);
        if i.is_multiple_of(3) {
            s.ready_at_ms = 1_000 + u64::from(i);
        }
        v.push((1, enc(&Command::start_run(s))));
        v.push((1, enc(&grant(&r, 1, 1))));
        let sched = intent(derived::Intent::ScheduleTimer(ScheduleTimer {
            op: 1,
            due_at_ms: 5_000 + u64::from(i),
        }));
        v.push((
            1,
            enc(&events(
                &r,
                (1, 1),
                0,
                vec![ev(0), ev(1)],
                vec![sched, signal_to(&rid(5, i))],
            )),
        ));
        v.push((0, enc(&start_cmd(&rid(0, 200 + i)))));
        v.push((1, enc(&grant(&r, 1, 1))));
        let mut keyed = start(&rid(0, 100 + i));
        keyed.idempotency_key = b"k".to_vec();
        v.push((1, enc(&Command::start_run(keyed))));
        v.push((
            1,
            enc(&events(
                &r,
                (1, 1),
                2,
                vec![ev(2)],
                vec![complete(TerminalStatus::Completed)],
            )),
        ));
        v.push((1, Bytes::from_static(&[0xff, 0xff, 0xff])));
    }
    v.truncate(200);
    v
}

type ChunkResult = ([u8; 32], [u8; 32], Vec<(Offset, ApplyOutcome)>);

fn apply_in_chunks(k: usize) -> ChunkResult {
    let mut h = H::new(0);
    let mut outs = Vec::new();
    for chunk in workload().chunks(k) {
        let mut batches: Vec<VerifiedBatch> = Vec::new();
        let mut off = h.cur.next;
        for (env, bytes) in chunk {
            match batches.last_mut() {
                Some(vb) if vb.epoch == Epoch(*env) => vb.commands.push(bytes.clone()),
                _ => batches.push(VerifiedBatch {
                    epoch: Epoch(*env),
                    base: off,
                    commands: vec![bytes.clone()],
                }),
            }
            off = off.next();
        }
        let mut b = h.store.batch();
        let mut acts = Vec::new();
        outs.extend(apply_chunk(&mut b, &mut h.cur, &batches, &mut acts).expect("chunk"));
        let mu = h.cur.meta_update();
        b.commit(&mu).expect("commit");
    }
    (h.cur.hash, h.store.digest().expect("digest"), outs)
}

#[test]
fn rolling_hash_chunking_invariant() {
    let one = apply_in_chunks(1);
    let seven = apply_in_chunks(7);
    let all = apply_in_chunks(200);
    assert_eq!(one.2.len(), 200);
    for (i, (off, _)) in one.2.iter().enumerate() {
        assert_eq!(off.0, i as u64);
    }
    let codes: std::collections::BTreeSet<u8> = one.2.iter().map(|(_, o)| o.code()).collect();
    for c in [0, 1, 16, 17, 26] {
        assert!(
            codes.contains(&c),
            "workload lacks outcome code {c}: {codes:?}"
        );
    }
    assert_eq!(one.2, seven.2);
    assert_eq!(one.2, all.2);
    assert_eq!(one.0, seven.0);
    assert_eq!(one.0, all.0);
    assert_eq!(one.1, seven.1);
    assert_eq!(one.1, all.1);
    assert_ne!(one.0, [0u8; 32]);
}

#[test]
fn rolling_hash_rejections() {
    let mut h = H::new(0);
    assert_eq!(h.apply(1, &la(1)), ApplyOutcome::Accepted);
    let prev = h.cur.hash;
    let off = h.cur.next;
    let bytes = encode_command(&start_cmd(&rid(0, 1)));
    let (o, _, _) = h.apply_raw(0, &bytes);
    assert_eq!(o, rej(RejectReason::StaleEpoch { current: Epoch(1) }));
    assert_ne!(h.cur.hash, prev);
    assert_eq!(h.cur.hash, fold_hash(&prev, off, 16, &bytes));
    assert_eq!(h.cur.next, off.next());
    let m = h.store.read_meta().expect("meta");
    assert_eq!(m.state_hash, h.cur.hash);
    assert_eq!(m.applied, h.cur.next);
}

// ---------------------------------------------------------------- read budget

#[test]
fn reads_budget() {
    let mut h = H::new(0);
    assert_eq!(h.apply(1, &la(1)), ApplyOutcome::Accepted);

    let mut keyed = start(&rid(0, 1));
    keyed.idempotency_key = b"k".to_vec();
    let (o, _, reads) = h.apply_raw(1, &encode_command(&Command::start_run(keyed.clone())));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(reads <= 1, "keyed StartRun did {reads} reads");
    let (o, _, reads) = h.apply_raw(1, &encode_command(&Command::start_run(keyed)));
    assert!(matches!(o, ApplyOutcome::Duplicate { .. }));
    assert!(reads <= 1, "duplicate keyed StartRun did {reads} reads");

    // A child run (parent in partition 3) so completion emits ChildCompleted.
    let r = rid(0, 2);
    let mut s = start(&r);
    s.parent = Some(ParentRef {
        run_id: rid(3, 1).as_bytes().to_vec(),
        op: 4,
    });
    let (o, _, reads) = h.apply_raw(1, &encode_command(&Command::start_run(s)));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(reads <= 1, "StartRun did {reads} reads");

    let (o, _, reads) = h.apply_raw(1, &encode_command(&grant(&r, 1, 1)));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(reads <= 1, "AttemptGranted did {reads} reads");

    let sched = intent(derived::Intent::ScheduleTimer(ScheduleTimer {
        op: 1,
        due_at_ms: 50,
    }));
    let rdy = intent(derived::Intent::EnqueueReady(EnqueueReady {
        ready_at_ms: 60,
    }));
    let cmd = events(
        &r,
        (1, 1),
        0,
        vec![ev(0), ev(1), ev(2)],
        vec![
            sched,
            rdy,
            signal_to(&rid(5, 1)),
            complete(TerminalStatus::Completed),
        ],
    );
    let (o, a, reads) = h.apply_raw(1, &encode_command(&cmd));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(reads <= 1, "EventsAppended did {reads} reads");
    assert!(a.contains(&Action::OutboxAppended {
        target: PartitionId(3),
        seq: 1
    }));
}

// ---------------------------------------------------------------- outbox

fn outbox_row(h: &H, target: u16, seq: u64) -> Option<OutboxMsg> {
    h.get(Table::Outbox, &outbox_key(target, seq))
        .map(|v| OutboxMsg::decode(&*v).expect("prost outbox"))
}

#[test]
fn outbox_emit() {
    let (mut h, r) = running(0, 1);
    let (o, a) = h.apply_act(
        1,
        &events(&r, (1, 1), 0, vec![], vec![signal_to(&rid(5, 1))]),
    );
    assert_eq!(o, ApplyOutcome::Accepted);
    assert_eq!(
        a,
        vec![Action::OutboxAppended {
            target: PartitionId(5),
            seq: 1
        }]
    );
    let msg = outbox_row(&h, 5, 1).expect("row (5,1)");
    assert_eq!(msg.target_partition().unwrap(), PartitionId(5));

    assert_eq!(
        h.apply(
            1,
            &events(&r, (1, 1), 0, vec![], vec![signal_to(&rid(5, 2))])
        ),
        ApplyOutcome::Accepted
    );
    assert!(outbox_row(&h, 5, 2).is_some());
    let (_, a) = h.apply_act(
        1,
        &events(&r, (1, 1), 0, vec![], vec![signal_to(&rid(9, 1))]),
    );
    assert_eq!(
        a,
        vec![Action::OutboxAppended {
            target: PartitionId(9),
            seq: 1
        }]
    );
    assert!(outbox_row(&h, 9, 1).is_some());
    // Same-partition targets go through the outbox too.
    assert_eq!(
        h.apply(
            1,
            &events(&r, (1, 1), 0, vec![], vec![signal_to(&rid(0, 1))])
        ),
        ApplyOutcome::Accepted
    );
    assert!(outbox_row(&h, 0, 1).is_some());
    let want = std::collections::BTreeMap::from([(0u16, 2u64), (5, 3), (9, 2)]);
    assert_eq!(h.cur.meta.outbox_next_seq, want);

    // Target outside the partition count.
    assert_eq!(
        h.apply(
            1,
            &events(&r, (1, 1), 0, vec![], vec![signal_to(&rid(20, 1))])
        ),
        rej(RejectReason::Invalid(InvalidCode::BadRunId))
    );

    // Persisted and reloaded on reopen.
    let H { dir, store, cur } = h;
    drop(store);
    let (_store, meta) = PartitionStore::open(
        dir.path(),
        PartitionId(0),
        PartitionCount::new(16).unwrap(),
        &StoreConfig::default(),
    )
    .expect("reopen");
    assert_eq!(meta.cursor.outbox_next_seq, want);
    assert_eq!(Cursor::from_meta(&meta).hash, cur.hash);
    assert_eq!(meta.applied, cur.next);
}

#[test]
fn outbox_child() {
    let (mut h, r) = running(0, 1);
    let c = rid(4, 7);
    let mut cs = start(&c);
    cs.parent = Some(ParentRef {
        run_id: r.as_bytes().to_vec(),
        op: 3,
    });
    let send = intent(derived::Intent::SendMessage(SendMessage {
        msg: Some(OutboxMsg::start_child(cs)),
    }));
    let (o, a) = h.apply_act(1, &events(&r, (1, 1), 0, vec![ev(0)], vec![send]));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert_eq!(
        a,
        vec![Action::OutboxAppended {
            target: PartitionId(4),
            seq: 1
        }]
    );
    assert!(outbox_row(&h, 4, 1).is_some());
    assert!(h.get(Table::Children, &children_key(&r, &c)).is_some());
    assert_eq!(h.run(&r).expect("run").children, 1);

    // The child, applied on its own partition, reports back on completion.
    let mut ch = H::new(2);
    let child = rid(2, 1);
    let mut s = start(&child);
    s.parent = Some(ParentRef {
        run_id: rid(0, 1).as_bytes().to_vec(),
        op: 3,
    });
    assert_eq!(ch.apply(0, &Command::start_run(s)), ApplyOutcome::Accepted);
    assert_eq!(
        ch.run(&child).expect("child").parent,
        Some((*rid(0, 1).as_bytes(), 3))
    );
    assert_eq!(ch.apply(0, &grant(&child, 0, 1)), ApplyOutcome::Accepted);
    let (o, a) = ch.apply_act(
        0,
        &events(
            &child,
            (0, 1),
            0,
            vec![],
            vec![complete(TerminalStatus::Failed)],
        ),
    );
    assert_eq!(o, ApplyOutcome::Accepted);
    assert_eq!(
        a,
        vec![
            Action::RunCompleted {
                run: child,
                status: RunStatus::Failed
            },
            Action::OutboxAppended {
                target: PartitionId(0),
                seq: 1
            }
        ]
    );
    let msg = outbox_row(&ch, 0, 1).expect("ChildCompleted row");
    assert_eq!(
        msg.kind,
        Some(outbox_msg::Kind::ChildCompleted(ChildCompleted {
            parent_run: rid(0, 1).as_bytes().to_vec(),
            child_run: child.as_bytes().to_vec(),
            op: 3,
            status: TerminalStatus::Failed as i32,
            result: inline(b"out"),
            event: None,
        }))
    );
}

// ---------------------------------------------------------------- apply_chunk

#[test]
fn apply_chunk_multi_batch() {
    let mut h = H::new(0);
    let enc = |c: &Command| encode_command(c);
    let batches = vec![
        VerifiedBatch {
            epoch: Epoch(0),
            base: Offset(0),
            commands: vec![enc(&start_cmd(&rid(0, 1)))],
        },
        VerifiedBatch {
            epoch: Epoch(1),
            base: Offset(1),
            commands: vec![enc(&la(1)), enc(&start_cmd(&rid(0, 2)))],
        },
        VerifiedBatch {
            epoch: Epoch(0),
            base: Offset(3),
            commands: vec![enc(&start_cmd(&rid(0, 3)))],
        },
    ];
    let mut b = h.store.batch();
    let mut acts = Vec::new();
    let outs = apply_chunk(&mut b, &mut h.cur, &batches, &mut acts).expect("chunk");
    let mu = h.cur.meta_update();
    b.commit(&mu).expect("commit");
    assert_eq!(
        outs,
        vec![
            (Offset(0), ApplyOutcome::Accepted),
            (Offset(1), ApplyOutcome::Accepted),
            (Offset(2), ApplyOutcome::Accepted),
            (
                Offset(3),
                rej(RejectReason::StaleEpoch { current: Epoch(1) })
            ),
        ]
    );
    assert_eq!(acts.len(), 2);
    assert_eq!(h.cur.next, Offset(4));
    assert!(h.run(&rid(0, 3)).is_none());
}
