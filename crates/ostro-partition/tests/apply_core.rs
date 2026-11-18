//! Pure deterministic apply core (plan 02-09; STOR-05, STOR-10, STOR-11, STOR-14):
//! epoch and attempt fencing, idempotent starts, fenced appends with Derived
//! intents, payload limits, outbox emission, read budget and the rolling hash.

use bytes::Bytes;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, START_DELAY_OP, apply_command,
};
use ostro_partition::store::keys::{
    dedupe_key, event_key, payload_key, ready_key, run_key, timer_key,
};
use ostro_partition::store::values::{self, ReadySlot, ReadyValue, RunRecord, RunStatus};
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, CompleteRun, ConflictPolicy, Derived, EnqueueReady,
    EventsAppended, JournalEvent, ScheduleTimer, StartRun, TerminalStatus, derived,
    encode_command,
};
use ostro_types::{
    AttemptToken, Command, Epoch, INLINE_PAYLOAD_LIMIT, PartitionCount, PartitionId, PayloadRef,
    RunId,
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
    let input = h
        .get(Table::Payloads, &payload_key(&r2, 0))
        .expect("input");
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
    }));
    let again = intent(derived::Intent::EnqueueReady(EnqueueReady { ready_at_ms: 5 }));
    assert_eq!(
        h.apply(1, &events(&r, (1, 1), 0, vec![], vec![done, again])),
        rej(RejectReason::Invalid(InvalidCode::BadIntent))
    );
    assert_eq!(h.run(&r).expect("run").status, RunStatus::Running);
}

#[test]
fn enqueue_ready_intent() {
    let (mut h, r) = running(0, 1);
    let rdy = intent(derived::Intent::EnqueueReady(EnqueueReady { ready_at_ms: 77 }));
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
