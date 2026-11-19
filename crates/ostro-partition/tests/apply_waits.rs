//! Signals, wait registry, wait timeouts, await tokens and graceful cancel (plan 05-08;
//! WF-05, API-05 apply half, API-07).
//!
//! Every test drives `apply_command` against an in-memory partition store (one committed
//! batch per command, so the `ApplyBatch` read counter is per command). Submitter-encoded
//! events (`SignalPayload`, `TokenCompleted`) are opaque byte strings here: apply journals
//! them verbatim and never decodes them (D21). Apply has no blob store access at all (its only
//! I/O is the `ApplyBatch` overlay over the partition's fjall keyspaces), so a blob-backed
//! 1 MiB event can never be fetched by apply; the tests check the journal row holds exactly
//! the command's `PayloadRef`.

use bytes::Bytes;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, apply_command,
};
use ostro_partition::query::journal_rows;
use ostro_partition::store::keys::{
    decode_signal_inbox_key, outbox_prefix, run_key, run_prefix, timer_key, wait_key,
    wait_name_key, wait_prefixes,
};
use ostro_partition::store::values::{self, RunRecord, RunStatus, SignalSlot, WaitKind, WaitRow};
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, CancelRun, CompleteRun, CompleteToken, ConflictPolicy,
    Derived, EventsAppended, InboundBatch, InboundMsg, JournalEvent, OutboxMsg, ParentRef,
    RequestCancel, SendMessage, Signal, SignalRun, StartChild, StartRun, TerminalStatus,
    TimerFired, WaitKind as CmdWaitKind, derived, encode_command, outbox_msg,
};
use ostro_types::journal::{EventBody, EventKind, decode_event};
use ostro_types::payload::INLINE_PAYLOAD_LIMIT;
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};
use prost::Message;

const P: u16 = 2;

// ---------------------------------------------------------------- harness

struct H {
    _dir: tempfile::TempDir,
    store: PartitionStore,
    cur: Cursor,
    /// Every applied command, for the rebuild check.
    log: Vec<Vec<u8>>,
}

fn open_store() -> (tempfile::TempDir, PartitionStore, Cursor) {
    let dir = ostro_testkit::fs::tempdir();
    let (store, meta) = PartitionStore::open(
        dir.path(),
        PartitionId(P),
        PartitionCount::new(16).expect("non-zero"),
        &StoreConfig::default(),
    )
    .expect("open store");
    let cur = Cursor::from_meta(&meta);
    (dir, store, cur)
}

impl H {
    fn new() -> Self {
        let (dir, store, cur) = open_store();
        let mut h = H {
            _dir: dir,
            store,
            cur,
            log: Vec::new(),
        };
        assert_eq!(
            h.apply(&Command::leader_announced(1, 1, 0)),
            ApplyOutcome::Accepted
        );
        h
    }

    /// Applies one command in its own committed batch: (outcome, actions, keyspace reads).
    fn apply_full(&mut self, c: &Command) -> (ApplyOutcome, Vec<Action>, u64) {
        let bytes = encode_command(c);
        self.log.push(bytes.to_vec());
        let mut b = self.store.batch();
        let mut actions = Vec::new();
        let off = self.cur.next;
        let out = apply_command(&mut b, &mut self.cur, off, Epoch(1), &bytes, &mut actions)
            .expect("apply");
        let reads = b.reads();
        let mu = self.cur.meta_update();
        b.commit(&mu).expect("commit");
        (out, actions, reads)
    }

    fn apply(&mut self, c: &Command) -> ApplyOutcome {
        self.apply_full(c).0
    }

    /// Writes raw rows outside apply (test setup for states only 05-09 can reach). Such a
    /// harness is excluded from the rebuild check.
    fn put(&mut self, rows: Vec<(Table, Vec<u8>, Vec<u8>)>) {
        let mut b = self.store.batch();
        for (t, k, v) in rows {
            b.insert(t, k, v);
        }
        let mu = self.cur.meta_update();
        b.commit(&mu).expect("commit");
        self.log.clear();
    }

    fn get(&self, t: Table, k: &[u8]) -> Option<Vec<u8>> {
        self.store.reader().get(t, k).expect("read")
    }

    fn rows(&self, t: Table, prefix: &[u8]) -> ostro_partition::store::Rows {
        self.store
            .reader()
            .prefix(t, prefix, usize::MAX)
            .expect("prefix")
    }

    fn run(&self, id: &RunId) -> RunRecord {
        values::decode(&self.get(Table::Runs, &run_key(id)).expect("run row")).expect("decode")
    }

    /// The journal of `r`: (seq, kind, payload) rows.
    fn journal(&self, r: &RunId) -> Vec<(u64, u32, PayloadRef)> {
        journal_rows(&self.store.reader(), r, 0, usize::MAX).expect("journal")
    }

    /// Decoded apply-built event at `seq` (inline payload).
    fn event(&self, r: &RunId, seq: u64) -> EventBody {
        let (s, kind, p) = self
            .journal(r)
            .into_iter()
            .find(|(s, ..)| *s == seq)
            .expect("event");
        let bytes = match p.kind {
            Some(ostro_types::payload::payload_ref::Kind::Inline(b)) => b,
            _ => panic!("apply-built events are inline"),
        };
        decode_event(s, kind, &bytes).expect("decode").body
    }

    /// `(name, slot)` of the run's signal inbox rows, in arrival order.
    fn slots(&self, r: &RunId) -> Vec<(String, SignalSlot)> {
        let mut rows: Vec<(u64, String, SignalSlot)> = self
            .rows(Table::Inbox, &run_prefix(r))
            .into_iter()
            .filter_map(|(k, v)| {
                decode_signal_inbox_key(&k)
                    .map(|(_, name, n)| (n, name, values::decode(&v).expect("slot")))
            })
            .collect();
        rows.sort_by_key(|(n, _, _)| *n);
        rows.into_iter().map(|(_, n, s)| (n, s)).collect()
    }

    /// Live wait rows (shape 0x01) of `r`.
    fn wait_rows(&self, r: &RunId) -> usize {
        self.rows(Table::Waits, &wait_prefixes(r)[0]).len()
    }

    /// FIFO index rows (shape 0x02) of `r`.
    fn index_rows(&self, r: &RunId) -> usize {
        self.rows(Table::Waits, &wait_prefixes(r)[1]).len()
    }

    fn timers(&self) -> usize {
        self.rows(Table::Timers, &[]).len()
    }

    /// `RunRecord.open_waits` equals the live wait-row count (every path).
    fn assert_open_waits(&self, r: &RunId) {
        let rec = self.run(r);
        assert_eq!(
            rec.open_waits as usize,
            self.wait_rows(r),
            "open_waits must equal the live wait rows"
        );
    }

    /// Apply-built completion inputs (WaitTimedOut, SignalMatched) journaled for `op`.
    fn completions_of(&self, r: &RunId, op: u64) -> usize {
        self.journal(r)
            .into_iter()
            .filter(|(_, kind, _)| {
                *kind == EventKind::WaitTimedOut.code() || *kind == EventKind::SignalMatched.code()
            })
            .filter(|(seq, ..)| self.event(r, *seq).completes_op() == Some(op))
            .count()
    }

    fn digest(&self) -> [u8; 32] {
        self.store.digest().expect("digest")
    }

    /// Rebuild check: every logged command applied again to a fresh store in ONE batch must
    /// reproduce the live digest (no state outside the log, no batch-boundary leakage).
    fn assert_rebuild_equal(&self) {
        assert!(
            !self.log.is_empty(),
            "harness has raw puts; no rebuild check"
        );
        let (_dir, store, mut cur) = open_store();
        let mut b = store.batch();
        let mut actions = Vec::new();
        for bytes in &self.log {
            let off = cur.next;
            apply_command(&mut b, &mut cur, off, Epoch(1), bytes, &mut actions).expect("apply");
        }
        let mu = cur.meta_update();
        b.commit(&mu).expect("commit");
        assert_eq!(store.digest().expect("digest"), self.digest(), "rebuild");
    }
}

fn rid(n: u8) -> RunId {
    rid_on(P, n)
}

fn rid_on(p: u16, n: u8) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&p.to_be_bytes());
    b[17] = n;
    RunId::from_bytes(&b).expect("18 bytes")
}

fn inline(b: &'static [u8]) -> PayloadRef {
    PayloadRef::inline(Bytes::from_static(b))
}

fn start(id: &RunId) -> StartRun {
    StartRun {
        run_id: id.as_bytes().to_vec(),
        workflow: "wf".into(),
        workflow_version: 1,
        queue: "q".into(),
        input: Some(inline(b"in")),
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        ready_at_ms: 100,
        requested_at_ms: 100,
        rng_seed: vec![7; 32],
        ..Default::default()
    }
}

fn grant(r: &RunId) -> Command {
    Command::attempt_granted(AttemptGranted {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n: 1 }),
        after_failure: false,
    })
}

/// Run `n` started and granted `{1,1}`, journal empty.
fn granted(h: &mut H, n: u8) -> RunId {
    let r = rid(n);
    assert_eq!(
        h.apply(&Command::start_run(start(&r))),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.apply(&grant(&r)), ApplyOutcome::Accepted);
    r
}

/// Run `n` started, granted and with `events` journaled (seqs 0..events).
fn running(h: &mut H, n: u8, events: u64) -> RunId {
    let r = granted(h, n);
    if events > 0 {
        assert_eq!(
            h.apply(&append(h, &r, events, vec![])),
            ApplyOutcome::Accepted
        );
    }
    r
}

/// `EventsAppended` at the run's live `next_seq` with `events` opaque events.
fn append(h: &H, r: &RunId, events: u64, derived: Vec<Derived>) -> Command {
    let from = h.run(r).next_seq;
    Command::events_appended(EventsAppended {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n: 1 }),
        from_seq: from,
        events: (from..from + events)
            .map(|seq| JournalEvent {
                seq,
                kind: 16,
                payload: Some(inline(b"e")),
            })
            .collect(),
        derived,
    })
}

/// One event (the wait's own `SignalWaitStarted`/`AwaitTokenCreated`, opaque here) plus a
/// `RegisterWait` for its seq. Returns (command, op).
fn wait_cmd(h: &H, r: &RunId, kind: CmdWaitKind, name: &str, due: u64) -> (Command, u64) {
    let op = h.run(r).next_seq;
    (
        append(
            h,
            r,
            1,
            vec![Derived::register_wait(op, kind, name, due, 1_000 + op)],
        ),
        op,
    )
}

fn register(h: &mut H, r: &RunId, kind: CmdWaitKind, name: &str, due: u64) -> u64 {
    let (c, op) = wait_cmd(h, r, kind, name, due);
    assert_eq!(h.apply(&c), ApplyOutcome::Accepted);
    op
}

fn signal(r: &RunId, name: &str, event: PayloadRef, request_id: &[u8]) -> Command {
    Command::signal_run(SignalRun {
        run_id: r.as_bytes().to_vec(),
        name: name.into(),
        event: Some(event),
        request_id: request_id.to_vec(),
        sent_at_ms: 777,
    })
}

fn token(r: &RunId, op: u64, event: Option<PayloadRef>) -> Command {
    Command::complete_token(CompleteToken {
        run_id: r.as_bytes().to_vec(),
        op,
        event,
        completed_at_ms: 888,
    })
}

fn cancel_req(r: &RunId, reason: &str) -> Command {
    Command::request_cancel(RequestCancel {
        run_id: r.as_bytes().to_vec(),
        reason: reason.into(),
        requested_at_ms: 999,
    })
}

fn fire(r: &RunId, op: u64, due: u64) -> Command {
    Command::timer_fired(TimerFired {
        run_id: r.as_bytes().to_vec(),
        op,
        due_at_ms: due,
        fired_at_ms: due + 1,
    })
}

fn inbound(src: u16, msgs: Vec<(u64, outbox_msg::Kind)>) -> Command {
    Command::inbound_batch(InboundBatch {
        src: u32::from(src),
        msgs: msgs
            .into_iter()
            .map(|(seq, k)| InboundMsg {
                seq,
                msg: Some(OutboxMsg { kind: Some(k) }),
            })
            .collect(),
    })
}

fn inbound_signal_kind(r: &RunId, name: &str, payload: PayloadRef) -> outbox_msg::Kind {
    outbox_msg::Kind::Signal(Signal {
        target_run: r.as_bytes().to_vec(),
        name: name.into(),
        payload: Some(payload),
    })
}

fn cancel_kind(r: &RunId, reason: &str) -> outbox_msg::Kind {
    outbox_msg::Kind::CancelRun(CancelRun {
        target_run: r.as_bytes().to_vec(),
        reason: reason.into(),
    })
}

fn complete() -> Derived {
    Derived::new(derived::Intent::CompleteRun(CompleteRun {
        status: TerminalStatus::Completed as i32,
        result: None,
        completed_at_ms: 900,
        resumable: false,
        parent_event: None,
    }))
}

fn rej(r: RejectReason) -> ApplyOutcome {
    ApplyOutcome::Rejected(r)
}

fn invalid(c: InvalidCode) -> ApplyOutcome {
    rej(RejectReason::Invalid(c))
}

fn dup() -> ApplyOutcome {
    ApplyOutcome::Duplicate { existing: None }
}

fn row_at(h: &H, r: &RunId, seq: u64) -> (u32, PayloadRef) {
    h.journal(r)
        .into_iter()
        .find(|(s, ..)| *s == seq)
        .map(|(_, k, p)| (k, p))
        .expect("journal row")
}

fn matched(op: u64, payload_seq: u64) -> EventBody {
    EventBody::SignalMatched(ostro_types::journal::SignalMatched { op, payload_seq })
}

fn big_inline() -> PayloadRef {
    PayloadRef::inline(Bytes::from(vec![9u8; INLINE_PAYLOAD_LIMIT + 1]))
}

fn mib_blob(tag: u8) -> PayloadRef {
    PayloadRef::blob([tag; 32], 1 << 20)
}

// ---------------------------------------------------------------- RegisterWait

#[test]
fn register_wait_inserts_rows_and_timer_and_is_idempotent() {
    let mut h = H::new();
    let r = running(&mut h, 1, 2);
    let (c, op) = wait_cmd(&h, &r, CmdWaitKind::Signal, "x", 5_000);
    let (out, actions, reads) = h.apply_full(&c);
    assert_eq!(out, ApplyOutcome::Accepted);
    // run record + inbox-by-name prefix (no open wait: the wait-row read is skipped).
    assert_eq!(reads, 2, "RegisterWait(signal) read budget");
    assert!(actions.contains(&Action::ScheduleTimer {
        due_at_ms: 5_000,
        run: r,
        op
    }));
    assert_eq!(
        values::decode::<WaitRow>(&h.get(Table::Waits, &wait_key(&r, op)).expect("row"))
            .expect("decode"),
        WaitRow {
            kind: WaitKind::Signal,
            name: "x".into(),
            timeout_due_ms: 5_000
        }
    );
    assert!(h.get(Table::Waits, &wait_name_key(&r, "x", op)).is_some());
    assert!(h.get(Table::Timers, &timer_key(5_000, &r, op)).is_some());
    assert_eq!(h.run(&r).open_waits, 1);

    // The same RegisterWait again (05-11 re-arm after resume): no second timer, no count.
    let again = append(
        &h,
        &r,
        1,
        vec![Derived::register_wait(
            op,
            CmdWaitKind::Signal,
            "x",
            5_000,
            9,
        )],
    );
    let (out, actions, reads) = h.apply_full(&again);
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(
        reads, 2,
        "existing row: run record + wait-row point read only"
    );
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::ScheduleTimer { .. }))
    );
    assert_eq!((h.wait_rows(&r), h.index_rows(&r), h.timers()), (1, 1, 1));
    assert_eq!(h.run(&r).open_waits, 1);

    // Twice in one batch: the overlay serves the second check.
    let op2 = h.run(&r).next_seq;
    let twice = append(
        &h,
        &r,
        1,
        vec![
            Derived::register_wait(op2, CmdWaitKind::Token, "", 6_000, 9),
            Derived::register_wait(op2, CmdWaitKind::Token, "", 6_000, 9),
        ],
    );
    let (out, _, reads) = h.apply_full(&twice);
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(reads, 2, "token wait: run record + one wait-row read");
    assert_eq!((h.wait_rows(&r), h.index_rows(&r), h.timers()), (2, 1, 2));
    h.assert_open_waits(&r);
    h.assert_rebuild_equal();
}

#[test]
fn register_wait_validation() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let long = "n".repeat(ostro_types::command::MAX_SIGNAL_NAME_LEN + 1);
    for (d, want) in [
        (
            Derived::register_wait(0, CmdWaitKind::Signal, long.as_str(), 0, 1),
            invalid(InvalidCode::FieldTooLong),
        ),
        (
            Derived::register_wait(u64::MAX - 1, CmdWaitKind::Signal, "x", 0, 1),
            invalid(InvalidCode::BadIntent),
        ),
        (
            Derived::cancel_wait(u64::MAX),
            invalid(InvalidCode::BadIntent),
        ),
        (
            Derived::new(derived::Intent::RegisterWait(
                ostro_types::command::RegisterWait {
                    op: 0,
                    kind: 7,
                    name: "x".into(),
                    timeout_due_ms: 0,
                    at_ms: 1,
                },
            )),
            invalid(InvalidCode::BadIntent),
        ),
    ] {
        assert_eq!(h.apply(&append(&h, &r, 1, vec![d])), want);
    }
    assert_eq!(h.run(&r).next_seq, 1, "rejected batches write nothing");
    assert_eq!(h.wait_rows(&r), 0);
}

#[test]
fn register_wait_for_an_unjournaled_op_is_dropped() {
    // The write-loop seam cannot reject; an op at or past the journal end is a deterministic
    // no-op inside the accepted batch (documented on `waits::register_wait`).
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let c = append(
        &h,
        &r,
        1,
        vec![Derived::register_wait(2, CmdWaitKind::Signal, "x", 500, 1)],
    );
    assert_eq!(h.apply(&c), ApplyOutcome::Accepted);
    assert_eq!((h.wait_rows(&r), h.timers()), (0, 0));
    h.assert_open_waits(&r);
}

#[test]
fn register_wait_with_past_due_times_out_once() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    // Due in the past: the timer row is still inserted; the timer service fires it.
    let op = register(&mut h, &r, CmdWaitKind::Signal, "x", 5);
    assert!(h.get(Table::Timers, &timer_key(5, &r, op)).is_some());
    let (out, actions, reads) = h.apply_full(&fire(&r, op, 5));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(reads, 3, "timer row + run record + wait row");
    let seq = h.run(&r).next_seq - 1;
    assert_eq!(
        h.event(&r, seq),
        EventBody::WaitTimedOut(ostro_types::journal::WaitTimedOut { op })
    );
    assert_eq!(actions, vec![Action::InputAppended { run: r, seq }]);
    assert_eq!((h.wait_rows(&r), h.index_rows(&r), h.timers()), (0, 0, 0));
    // A second fire: the timer row is gone → Duplicate, nothing journaled.
    assert_eq!(h.apply(&fire(&r, op, 5)), dup());
    assert_eq!(h.completions_of(&r, op), 1);
    h.assert_open_waits(&r);
    h.assert_rebuild_equal();
}

#[test]
fn register_wait_for_a_satisfied_op_reinserts_documented_contract() {
    // Apply cannot tell a satisfied op (no row, op < next_seq) from a never-registered one.
    // The submitter (05-11 re-arm) must only send ops without a completion in the journal.
    // This test pins today's behaviour so a change is deliberate.
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let op = register(&mut h, &r, CmdWaitKind::Token, "", 0);
    assert_eq!(
        h.apply(&token(&r, op, Some(inline(b"\x01tok")))),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.wait_rows(&r), 0);
    let again = append(
        &h,
        &r,
        1,
        vec![Derived::register_wait(op, CmdWaitKind::Token, "", 0, 1)],
    );
    assert_eq!(h.apply(&again), ApplyOutcome::Accepted);
    assert_eq!(h.wait_rows(&r), 1, "re-inserted: the submitter's contract");
    h.assert_open_waits(&r);
}

#[test]
fn sleep_timer_with_an_open_wait_still_journals_timer_fired() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let tok = register(&mut h, &r, CmdWaitKind::Token, "", 0);
    // A sleep op (ordinary guest timer) on the same run.
    let sleep = h.run(&r).next_seq;
    let c = append(
        &h,
        &r,
        1,
        vec![Derived::new(derived::Intent::ScheduleTimer(
            ostro_types::command::ScheduleTimer {
                op: sleep,
                due_at_ms: 300,
            },
        ))],
    );
    assert_eq!(h.apply(&c), ApplyOutcome::Accepted);
    let (out, _, reads) = h.apply_full(&fire(&r, sleep, 300));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(
        reads, 3,
        "timer row + run record + wait-row read (open_waits > 0)"
    );
    let seq = h.run(&r).next_seq - 1;
    assert_eq!(
        h.event(&r, seq),
        EventBody::TimerFired(ostro_types::journal::TimerFired { op: sleep })
    );
    assert!(
        h.get(Table::Waits, &wait_key(&r, tok)).is_some(),
        "token wait untouched"
    );
    h.assert_open_waits(&r);
    h.assert_rebuild_equal();
}

// ---------------------------------------------------------------- signals

#[test]
fn signal_matches_the_oldest_wait_fifo() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let a = register(&mut h, &r, CmdWaitKind::Signal, "x", 4_000);
    let b = register(&mut h, &r, CmdWaitKind::Signal, "x", 0);
    let other = register(&mut h, &r, CmdWaitKind::Signal, "y", 0);
    assert!(a < b);
    let ev = inline(b"\x01payload-1");
    let (out, actions, reads) = h.apply_full(&signal(&r, "x", ev.clone(), b""));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(
        reads, 3,
        "run record + waits-by-name prefix + matched wait row"
    );
    let payload_seq = h.run(&r).next_seq - 2;
    assert_eq!(
        row_at(&h, &r, payload_seq),
        (EventKind::SignalPayload.code(), ev),
        "SignalPayload verbatim"
    );
    assert_eq!(h.event(&r, payload_seq + 1), matched(a, payload_seq));
    assert_eq!(
        actions,
        vec![
            Action::InputAppended {
                run: r,
                seq: payload_seq
            },
            Action::CancelTimer {
                due_at_ms: 4_000,
                run: r,
                op: a
            },
            Action::InputAppended {
                run: r,
                seq: payload_seq + 1
            },
        ]
    );
    // b and y remain; a's rows and timer are gone.
    assert!(h.get(Table::Waits, &wait_key(&r, a)).is_none());
    assert!(h.get(Table::Waits, &wait_key(&r, b)).is_some());
    assert!(h.get(Table::Waits, &wait_key(&r, other)).is_some());
    assert_eq!((h.index_rows(&r), h.timers()), (2, 0));
    assert!(h.slots(&r).is_empty());
    h.assert_open_waits(&r);

    // The next signal on x completes b.
    assert_eq!(
        h.apply(&signal(&r, "x", inline(b"\x01p2"), b"")),
        ApplyOutcome::Accepted
    );
    let seq = h.run(&r).next_seq;
    assert_eq!(h.event(&r, seq - 1), matched(b, seq - 2));
    assert_eq!(h.wait_rows(&r), 1);
    h.assert_open_waits(&r);
    h.assert_rebuild_equal();
}

#[test]
fn signals_before_waits_are_consumed_in_send_order() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let evs = [inline(b"\x01s1"), inline(b"\x01s2"), inline(b"\x01s3")];
    let mut payload_seqs = Vec::new();
    for ev in &evs {
        let (out, actions, reads) = h.apply_full(&signal(&r, "x", ev.clone(), b""));
        assert_eq!(out, ApplyOutcome::Accepted);
        assert_eq!(
            reads, 1,
            "run record only (open_waits == 0 skips the waits prefix)"
        );
        let seq = h.run(&r).next_seq - 1;
        // Running run: InputAppended for the SignalPayload seq only.
        assert_eq!(actions, vec![Action::InputAppended { run: r, seq }]);
        payload_seqs.push(seq);
    }
    assert_eq!(
        h.slots(&r)
            .into_iter()
            .map(|(n, s)| match s {
                SignalSlot::Journaled {
                    payload_seq,
                    sent_at_ms,
                } => (n, payload_seq, sent_at_ms),
                SignalSlot::Pending { .. } => panic!("journal started: Journaled"),
            })
            .collect::<Vec<_>>(),
        payload_seqs
            .iter()
            .map(|s| ("x".to_string(), *s, 777))
            .collect::<Vec<_>>()
    );
    for want in &payload_seqs {
        let (c, op) = wait_cmd(&h, &r, CmdWaitKind::Signal, "x", 9_000);
        let (out, actions, reads) = h.apply_full(&c);
        assert_eq!(out, ApplyOutcome::Accepted);
        assert_eq!(
            reads, 2,
            "run record + inbox-by-name prefix (open_waits == 0)"
        );
        let seq = h.run(&r).next_seq - 1;
        assert_eq!(h.event(&r, seq), matched(op, *want));
        // Matched at registration: ts is RegisterWait.at_ms; no wait row, no timer.
        assert_eq!(actions, vec![Action::InputAppended { run: r, seq }]);
        let ts = decode_ts(&h, &r, seq);
        assert_eq!(ts, (1_000 + op) * 1_000_000);
    }
    assert!(h.slots(&r).is_empty());
    assert_eq!((h.wait_rows(&r), h.timers()), (0, 0));
    h.assert_open_waits(&r);
    h.assert_rebuild_equal();
}

fn decode_ts(h: &H, r: &RunId, seq: u64) -> u64 {
    let (kind, p) = row_at(h, r, seq);
    let bytes = match p.kind {
        Some(ostro_types::payload::payload_ref::Kind::Inline(b)) => b,
        _ => panic!("inline"),
    };
    decode_event(seq, kind, &bytes).expect("decode").ts_nanos
}

#[test]
fn signal_after_timeout_goes_to_the_inbox() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let op = register(&mut h, &r, CmdWaitKind::Signal, "x", 500);
    assert_eq!(h.apply(&fire(&r, op, 500)), ApplyOutcome::Accepted);
    assert_eq!(h.completions_of(&r, op), 1);
    h.assert_open_waits(&r);
    assert_eq!(
        h.apply(&signal(&r, "x", inline(b"\x01late"), b"")),
        ApplyOutcome::Accepted
    );
    let seq = h.run(&r).next_seq - 1;
    assert_eq!(
        h.slots(&r),
        vec![(
            "x".to_string(),
            SignalSlot::Journaled {
                payload_seq: seq,
                sent_at_ms: 777
            }
        )]
    );
    assert_eq!(h.completions_of(&r, op), 1, "never two completions");
    h.assert_rebuild_equal();
}

#[test]
fn timeout_after_signal_is_a_no_op() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let op = register(&mut h, &r, CmdWaitKind::Signal, "x", 500);
    assert_eq!(
        h.apply(&signal(&r, "x", inline(b"\x01first"), b"")),
        ApplyOutcome::Accepted
    );
    let before = h.run(&r).next_seq;
    // The signal took the timer row: the late fire is Duplicate, nothing journaled.
    assert_eq!(h.apply(&fire(&r, op, 500)), dup());
    assert_eq!(h.run(&r).next_seq, before);
    assert_eq!(h.completions_of(&r, op), 1);
    h.assert_open_waits(&r);
    h.assert_rebuild_equal();
}

#[test]
fn signal_run_validation_and_dedupe() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    assert_eq!(
        h.apply(&signal(&rid(50), "x", inline(b"e"), b"")),
        rej(RejectReason::RunNotFound)
    );
    let mut no_event = signal(&r, "x", inline(b"e"), b"");
    if let Some(ostro_types::command::Cmd::SignalRun(s)) = &mut no_event.cmd {
        s.event = None;
    }
    assert_eq!(h.apply(&no_event), invalid(InvalidCode::MissingField));
    let long = "n".repeat(ostro_types::command::MAX_SIGNAL_NAME_LEN + 1);
    assert_eq!(
        h.apply(&signal(&r, &long, inline(b"e"), b"")),
        invalid(InvalidCode::FieldTooLong)
    );
    assert_eq!(
        h.apply(&signal(&r, "x", inline(b"e"), &[1u8; 65])),
        invalid(InvalidCode::FieldTooLong)
    );
    assert_eq!(
        h.apply(&signal(&rid_on(3, 1), "x", inline(b"e"), b"")),
        rej(RejectReason::WrongPartition)
    );

    // request_id dedupe (D13): the second send is Duplicate and changes nothing.
    let (out, _, reads) = h.apply_full(&signal(&r, "x", inline(b"\x01a"), b"req-1"));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(
        reads, 2,
        "run record + signal_seen point read (no open wait)"
    );
    let next = h.run(&r).next_seq;
    let (out, actions, _) = h.apply_full(&signal(&r, "x", inline(b"\x01a"), b"req-1"));
    assert_eq!(out, dup());
    assert!(actions.is_empty());
    assert_eq!(h.run(&r).next_seq, next);
    assert_eq!(h.slots(&r).len(), 1);

    // With a request id and a match: 4 reads.
    let _op = register(&mut h, &r, CmdWaitKind::Signal, "y", 0);
    let (out, _, reads) = h.apply_full(&signal(&r, "y", inline(b"\x01b"), b"req-2"));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(
        reads, 4,
        "run + signal_seen + waits-by-name prefix + wait row"
    );

    // Terminal run: RunTerminal (even for a known request id).
    assert_eq!(
        h.apply(&append(&h, &r, 0, vec![complete()])),
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.apply(&signal(&r, "x", inline(b"e"), b"req-1")),
        rej(RejectReason::RunTerminal)
    );
    h.assert_rebuild_equal();
}

#[test]
fn signal_wakes_a_suspended_run_only_when_matched() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let op = register(&mut h, &r, CmdWaitKind::Signal, "x", 0);
    // Suspend (separate batch at the journal end, as the executor does).
    assert_eq!(
        h.apply(&append(&h, &r, 0, vec![Derived::suspend()])),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.run(&r).status, RunStatus::Scheduled);

    // A signal nobody waits for: journaled and buffered, the run stays asleep.
    let (out, actions, _) = h.apply_full(&signal(&r, "other", inline(b"\x01o"), b""));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert!(actions.is_empty(), "{actions:?}");
    assert_eq!(h.run(&r).status, RunStatus::Scheduled);

    // The awaited signal: matched, the run becomes Ready at sent_at_ms.
    let (out, actions, _) = h.apply_full(&signal(&r, "x", inline(b"\x01x"), b""));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(h.run(&r).status, RunStatus::Ready);
    assert!(matches!(
        actions.as_slice(),
        [Action::EnqueueReady { run, ready_at_ms: 777, .. }] if *run == r
    ));
    let seq = h.run(&r).next_seq - 1;
    assert_eq!(h.event(&r, seq), matched(op, seq - 1));
    h.assert_open_waits(&r);
    h.assert_rebuild_equal();
}

// ---------------------------------------------------------------- CancelWait

#[test]
fn cancel_wait_removes_rows_and_timer() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let s = register(&mut h, &r, CmdWaitKind::Signal, "x", 3_000);
    let t = register(&mut h, &r, CmdWaitKind::Token, "", 4_000);
    assert_eq!(h.run(&r).open_waits, 2);
    let (out, actions, reads) = h.apply_full(&append(
        &h,
        &r,
        1,
        vec![Derived::cancel_wait(s), Derived::cancel_wait(t)],
    ));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(reads, 3, "run record + one wait-row read per CancelWait");
    assert!(actions.contains(&Action::CancelTimer {
        due_at_ms: 3_000,
        run: r,
        op: s
    }));
    assert!(actions.contains(&Action::CancelTimer {
        due_at_ms: 4_000,
        run: r,
        op: t
    }));
    assert_eq!((h.wait_rows(&r), h.index_rows(&r), h.timers()), (0, 0, 0));
    h.assert_open_waits(&r);
    // Absent: a no-op inside the accepted batch.
    assert_eq!(
        h.apply(&append(&h, &r, 1, vec![Derived::cancel_wait(s)])),
        ApplyOutcome::Accepted
    );
    h.assert_open_waits(&r);
    // A signal for the cancelled op's name now goes to the inbox.
    assert_eq!(
        h.apply(&signal(&r, "x", inline(b"\x01s"), b"")),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.slots(&r).len(), 1);
    h.assert_rebuild_equal();
}

// ---------------------------------------------------------------- B1

#[test]
fn signal_before_first_activation_is_pending_then_journaled_after_seq0_events() {
    let mut h = H::new();
    let r = granted(&mut h, 1);
    let (e1, e2) = (inline(b"\x01early-1"), inline(b"\x01early-2"));
    for ev in [&e1, &e2] {
        let (out, actions, reads) = h.apply_full(&signal(&r, "x", ev.clone(), b""));
        assert_eq!(out, ApplyOutcome::Accepted);
        assert!(actions.is_empty(), "no wake, nothing journaled");
        assert_eq!(reads, 1, "run record only (no wait can exist at seq 0)");
    }
    assert_eq!(h.run(&r).next_seq, 0);
    assert_eq!(
        h.slots(&r),
        vec![
            (
                "x".to_string(),
                SignalSlot::Pending {
                    event: e1.clone(),
                    sent_at_ms: 777
                }
            ),
            (
                "x".to_string(),
                SignalSlot::Pending {
                    event: e2.clone(),
                    sent_at_ms: 777
                }
            ),
        ]
    );
    assert!(h.journal(&r).is_empty());

    // The seq-0 batch: RunStarted + SignalWaitStarted (opaque) and RegisterWait for op 1.
    let c = append(
        &h,
        &r,
        2,
        vec![Derived::register_wait(1, CmdWaitKind::Signal, "x", 0, 50)],
    );
    let (out, actions, reads) = h.apply_full(&c);
    assert_eq!(out, ApplyOutcome::Accepted);
    // run record + inbox run prefix (on_first_events) + inbox-by-name prefix (no open wait,
    // so no wait-row read).
    assert_eq!(reads, 3);
    let j = h.journal(&r);
    assert_eq!(
        j.iter().map(|(s, k, _)| (*s, *k)).collect::<Vec<_>>(),
        vec![
            (0, 16),
            (1, 16),
            (2, EventKind::SignalPayload.code()),
            (3, EventKind::SignalPayload.code()),
            (4, EventKind::SignalMatched.code()),
        ]
    );
    assert_eq!(j[2].2, e1, "verbatim, arrival order");
    assert_eq!(j[3].2, e2);
    assert_eq!(h.event(&r, 4), matched(1, 2), "oldest pending signal");
    assert_eq!(
        actions,
        vec![
            Action::InputAppended { run: r, seq: 2 },
            Action::InputAppended { run: r, seq: 3 },
            Action::InputAppended { run: r, seq: 4 },
        ]
    );
    assert_eq!(
        h.slots(&r),
        vec![(
            "x".to_string(),
            SignalSlot::Journaled {
                payload_seq: 3,
                sent_at_ms: 777
            }
        )]
    );
    assert_eq!(h.wait_rows(&r), 0);
    h.assert_open_waits(&r);
    h.assert_rebuild_equal();
}

#[test]
fn inbound_signal_before_start_behaves_the_same() {
    let mut h = H::new();
    let r = granted(&mut h, 1);
    let ev = inline(b"\x01remote");
    let (out, actions, reads) = h.apply_full(&inbound(
        7,
        vec![(1, inbound_signal_kind(&r, "x", ev.clone()))],
    ));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert!(actions.is_empty());
    assert_eq!(reads, 1);
    assert_eq!(
        h.slots(&r),
        vec![(
            "x".to_string(),
            SignalSlot::Pending {
                event: ev.clone(),
                sent_at_ms: 0
            }
        )]
    );
    let c = append(
        &h,
        &r,
        2,
        vec![Derived::register_wait(1, CmdWaitKind::Signal, "x", 0, 50)],
    );
    assert_eq!(h.apply(&c), ApplyOutcome::Accepted);
    assert_eq!(row_at(&h, &r, 2), (EventKind::SignalPayload.code(), ev));
    assert_eq!(h.event(&r, 3), matched(1, 2));
    assert!(h.slots(&r).is_empty());
    h.assert_open_waits(&r);
    h.assert_rebuild_equal();
}

#[test]
fn inbound_signal_matches_a_wait_like_signal_run() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let op = register(&mut h, &r, CmdWaitKind::Signal, "x", 700);
    let ev = mib_blob(0x33);
    let (out, actions, reads) = h.apply_full(&inbound(
        7,
        vec![(1, inbound_signal_kind(&r, "x", ev.clone()))],
    ));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(reads, 3, "run record + waits-by-name prefix + wait row");
    let seq = h.run(&r).next_seq - 1;
    assert_eq!(
        row_at(&h, &r, seq - 1),
        (EventKind::SignalPayload.code(), ev)
    );
    assert_eq!(h.event(&r, seq), matched(op, seq - 1));
    // No send time on the outbox message: stamped with the run's created_at_ms.
    assert_eq!(decode_ts(&h, &r, seq), h.run(&r).created_at_ms * 1_000_000);
    assert!(actions.contains(&Action::CancelTimer {
        due_at_ms: 700,
        run: r,
        op
    }));
    assert_eq!(h.timers(), 0);
    h.assert_open_waits(&r);
    h.assert_rebuild_equal();
}

// ---------------------------------------------------------------- W5 and D21 (1 MiB)

#[test]
fn oversize_inline_events_are_rejected_and_blob_refs_accepted() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let op = register(&mut h, &r, CmdWaitKind::Token, "", 0);
    let before = h.run(&r);
    assert_eq!(
        h.apply(&signal(&r, "x", big_inline(), b"")),
        rej(RejectReason::PayloadTooLarge)
    );
    assert_eq!(
        h.apply(&token(&r, op, Some(big_inline()))),
        rej(RejectReason::PayloadTooLarge)
    );
    assert_eq!(
        h.apply(&inbound(
            7,
            vec![(1, inbound_signal_kind(&r, "x", big_inline()))]
        )),
        rej(RejectReason::PayloadTooLarge)
    );
    assert_eq!(h.run(&r), before, "nothing written");
    assert_eq!(h.cur.inbound_seen.get(&7), None, "whole batch rejected");

    // Exactly the inline limit is fine; blob refs of any size are fine.
    let at_limit = PayloadRef::inline(Bytes::from(vec![1u8; INLINE_PAYLOAD_LIMIT]));
    assert_eq!(
        h.apply(&signal(&r, "x", at_limit, b"")),
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.apply(&signal(&r, "x", mib_blob(1), b"")),
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.apply(&inbound(
            7,
            vec![(1, inbound_signal_kind(&r, "x", mib_blob(2)))]
        )),
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.apply(&token(&r, op, Some(mib_blob(3)))),
        ApplyOutcome::Accepted
    );
    h.assert_rebuild_equal();
}

#[test]
fn one_mib_blob_signal_matched_immediately_is_journaled_verbatim() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let op = register(&mut h, &r, CmdWaitKind::Signal, "big", 0);
    let ev = mib_blob(0xa1);
    assert_eq!(
        h.apply(&signal(&r, "big", ev.clone(), b"")),
        ApplyOutcome::Accepted
    );
    let seq = h.run(&r).next_seq - 1;
    // The row holds exactly the command's PayloadRef (prost-equal), never fetched.
    assert_eq!(
        row_at(&h, &r, seq - 1),
        (EventKind::SignalPayload.code(), ev)
    );
    assert_eq!(h.event(&r, seq), matched(op, seq - 1));
    h.assert_rebuild_equal();
}

#[test]
fn one_mib_blob_signal_buffered_then_matched_at_register_wait() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let ev = mib_blob(0xb2);
    assert_eq!(
        h.apply(&signal(&r, "big", ev.clone(), b"")),
        ApplyOutcome::Accepted
    );
    let payload_seq = h.run(&r).next_seq - 1;
    let op = register(&mut h, &r, CmdWaitKind::Signal, "big", 0);
    let seq = h.run(&r).next_seq - 1;
    assert_eq!(
        row_at(&h, &r, payload_seq),
        (EventKind::SignalPayload.code(), ev)
    );
    assert_eq!(h.event(&r, seq), matched(op, payload_seq));
    h.assert_rebuild_equal();
}

#[test]
fn one_mib_blob_token_completion_is_journaled_verbatim() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let op = register(&mut h, &r, CmdWaitKind::Token, "", 0);
    let ev = mib_blob(0xc3);
    assert_eq!(
        h.apply(&token(&r, op, Some(ev.clone()))),
        ApplyOutcome::Accepted
    );
    let seq = h.run(&r).next_seq - 1;
    assert_eq!(row_at(&h, &r, seq), (EventKind::TokenCompleted.code(), ev));
    h.assert_rebuild_equal();
}

// ---------------------------------------------------------------- CompleteToken

#[test]
fn complete_token_once() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let op = register(&mut h, &r, CmdWaitKind::Token, "", 6_000);
    let ev = inline(b"\x01token-done");
    let (out, actions, reads) = h.apply_full(&token(&r, op, Some(ev.clone())));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(reads, 2, "run record + wait row");
    let seq = h.run(&r).next_seq - 1;
    assert_eq!(row_at(&h, &r, seq), (EventKind::TokenCompleted.code(), ev));
    assert_eq!(
        actions,
        vec![
            Action::CancelTimer {
                due_at_ms: 6_000,
                run: r,
                op
            },
            Action::InputAppended { run: r, seq },
        ]
    );
    assert_eq!((h.wait_rows(&r), h.timers()), (0, 0));
    h.assert_open_waits(&r);

    // Second completion: Duplicate, nothing journaled.
    assert_eq!(h.apply(&token(&r, op, Some(inline(b"\x01again")))), dup());
    assert_eq!(h.run(&r).next_seq, seq + 1);
    // No such op yet / not a token wait / missing event.
    assert_eq!(
        h.apply(&token(&r, seq + 5, Some(inline(b"e")))),
        invalid(InvalidCode::BadIntent)
    );
    let s = register(&mut h, &r, CmdWaitKind::Signal, "x", 0);
    assert_eq!(
        h.apply(&token(&r, s, Some(inline(b"e")))),
        invalid(InvalidCode::BadIntent)
    );
    assert_eq!(
        h.apply(&token(&r, s, None)),
        invalid(InvalidCode::MissingField)
    );
    assert_eq!(
        h.apply(&token(&rid(60), 1, Some(inline(b"e")))),
        rej(RejectReason::RunNotFound)
    );
    h.assert_open_waits(&r);
    h.assert_rebuild_equal();
}

#[test]
fn complete_token_after_timeout_is_duplicate_and_terminal_is_rejected() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let op = register(&mut h, &r, CmdWaitKind::Token, "", 400);
    assert_eq!(h.apply(&fire(&r, op, 400)), ApplyOutcome::Accepted);
    let seq = h.run(&r).next_seq - 1;
    assert_eq!(
        h.event(&r, seq),
        EventBody::WaitTimedOut(ostro_types::journal::WaitTimedOut { op })
    );
    assert_eq!(h.apply(&token(&r, op, Some(inline(b"e")))), dup());
    h.assert_open_waits(&r);

    let op2 = register(&mut h, &r, CmdWaitKind::Token, "", 0);
    assert_eq!(
        h.apply(&append(&h, &r, 0, vec![complete()])),
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.apply(&token(&r, op2, Some(inline(b"e")))),
        rej(RejectReason::RunTerminal)
    );
    assert_eq!(h.run(&r).open_waits, 0, "terminal hook cleared the wait");
    h.assert_open_waits(&r);
    h.assert_rebuild_equal();
}

// ---------------------------------------------------------------- RequestCancel

fn child_start(parent: &RunId, child: &RunId, op: u64) -> Derived {
    let mut s = start(child);
    s.parent = Some(ParentRef {
        run_id: parent.as_bytes().to_vec(),
        op,
    });
    Derived::new(derived::Intent::SendMessage(SendMessage {
        msg: Some(OutboxMsg {
            kind: Some(outbox_msg::Kind::StartChild(StartChild { start: Some(s) })),
        }),
    }))
}

/// Outbox messages of this partition for `target`, in seq order.
fn outbox(h: &H, target: u16) -> Vec<(u64, outbox_msg::Kind)> {
    h.rows(Table::Outbox, &outbox_prefix(target))
        .into_iter()
        .map(|(k, v)| {
            (
                u64::from_be_bytes(k[2..].try_into().expect("seq")),
                OutboxMsg::decode(&*v).expect("prost").kind.expect("kind"),
            )
        })
        .collect()
}

fn cancel_events(h: &H, r: &RunId) -> Vec<u64> {
    h.journal(r)
        .into_iter()
        .filter(|(_, k, _)| *k == EventKind::CancelRequested.code())
        .map(|(s, ..)| s)
        .collect()
}

#[test]
fn request_cancel_journals_once_wakes_and_cascades() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let (c5, c9) = (rid_on(5, 1), rid_on(9, 1));
    assert_eq!(
        h.apply(&append(
            &h,
            &r,
            1,
            vec![child_start(&r, &c5, 1), child_start(&r, &c9, 1)]
        )),
        ApplyOutcome::Accepted
    );
    let (out, actions, reads) = h.apply_full(&cancel_req(&r, "user"));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(reads, 2, "run record + children prefix");
    let rec = h.run(&r);
    assert!(rec.cancel_requested);
    assert_eq!(rec.cancel_reason, "user");
    let seq = rec.next_seq - 1;
    assert_eq!(cancel_events(&h, &r), vec![seq]);
    assert_eq!(
        h.event(&r, seq),
        EventBody::CancelRequested(ostro_types::journal::CancelRequested {
            reason: "user".into()
        })
    );
    assert_eq!(decode_ts(&h, &r, seq), 999 * 1_000_000);
    assert!(actions.contains(&Action::InputAppended { run: r, seq }));
    assert!(actions.contains(&Action::CancelRequested { run: r }));
    for (p, c) in [(5, c5), (9, c9)] {
        let msgs = outbox(&h, p);
        assert_eq!(msgs.len(), 2, "StartChild + CancelRun");
        assert_eq!(msgs[1].1, cancel_kind(&c, "user"));
    }

    // Repeat: Duplicate, no second event, no second cascade.
    let (out, actions, _) = h.apply_full(&cancel_req(&r, "again"));
    assert_eq!(out, dup());
    assert!(actions.is_empty());
    assert_eq!(cancel_events(&h, &r).len(), 1);
    assert_eq!(outbox(&h, 5).len(), 2);

    // Terminal: RunTerminal. Unknown: RunNotFound. Over-long reason: FieldTooLong.
    assert_eq!(
        h.apply(&append(&h, &r, 0, vec![complete()])),
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.apply(&cancel_req(&r, "x")),
        rej(RejectReason::RunTerminal)
    );
    assert_eq!(
        h.apply(&cancel_req(&rid(77), "x")),
        rej(RejectReason::RunNotFound)
    );
    let long = "r".repeat(ostro_types::command::MAX_REASON_LEN + 1);
    assert_eq!(
        h.apply(&cancel_req(&rid(77), &long)),
        invalid(InvalidCode::FieldTooLong)
    );
    h.assert_rebuild_equal();
}

#[test]
fn request_cancel_before_first_activation_sets_the_flag_only() {
    let mut h = H::new();
    let r = granted(&mut h, 1);
    let (out, actions, reads) = h.apply_full(&cancel_req(&r, "early"));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(reads, 1, "no children: run record only");
    let rec = h.run(&r);
    assert!(rec.cancel_requested);
    assert_eq!(rec.cancel_reason, "early");
    assert_eq!(rec.next_seq, 0, "nothing journaled before RunStarted");
    assert!(h.journal(&r).is_empty(), "no event at seq 0 by apply");
    assert_eq!(actions, vec![Action::CancelRequested { run: r }]);
    assert_eq!(h.apply(&cancel_req(&r, "early")), dup());

    // A delayed (Scheduled) run at seq 0 is woken so it observes the request.
    let d = rid(2);
    let mut s = start(&d);
    s.ready_at_ms = 50_000;
    assert_eq!(h.apply(&Command::start_run(s)), ApplyOutcome::Accepted);
    assert_eq!(h.run(&d).status, RunStatus::Scheduled);
    let (out, actions, _) = h.apply_full(&cancel_req(&d, "early"));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(h.run(&d).status, RunStatus::Ready);
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::EnqueueReady { run, .. } if *run == d))
    );
    assert!(h.journal(&d).is_empty());
    h.assert_rebuild_equal();
}

#[test]
fn inbound_cancel_takes_the_same_path() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1);
    let (out, actions) = {
        let (o, a, _) = h.apply_full(&inbound(3, vec![(1, cancel_kind(&r, "cascade"))]));
        (o, a)
    };
    assert_eq!(out, ApplyOutcome::Accepted);
    let rec = h.run(&r);
    assert!(rec.cancel_requested);
    assert_eq!(rec.cancel_reason, "cascade");
    let seq = rec.next_seq - 1;
    assert_eq!(cancel_events(&h, &r), vec![seq]);
    // No time on outbox messages: stamped with created_at_ms.
    assert_eq!(decode_ts(&h, &r, seq), rec.created_at_ms * 1_000_000);
    assert!(actions.contains(&Action::CancelRequested { run: r }));
    // No legacy InboxMsg::CancelRequested row any more.
    assert!(
        h.rows(Table::Inbox, &run_prefix(&r))
            .iter()
            .all(|(k, _)| decode_signal_inbox_key(k).is_some())
    );
    // Repeat: consumed, no effect.
    let (out, actions, _) = h.apply_full(&inbound(3, vec![(2, cancel_kind(&r, "cascade"))]));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert!(actions.is_empty());
    assert_eq!(cancel_events(&h, &r).len(), 1);
    assert_eq!(h.cur.inbound_seen.get(&3), Some(&2));
    h.assert_rebuild_equal();
}

#[test]
fn cancel_is_forwarded_along_a_continue_as_new_chain() {
    // PF-F2: A -> B -> C (A and B ContinuedAsNew, C live). Terminal ContinuedAsNew records
    // are written directly (05-09 owns the continue-as-new transition).
    let mut h = H::new();
    let (a, b) = (rid(1), rid(2));
    let c = running(&mut h, 3, 1);
    for (id, next) in [(a, b), (b, c)] {
        let rec = RunRecord {
            workflow: "wf".into(),
            queue: "q".into(),
            status: RunStatus::ContinuedAsNew,
            next_seq: 4,
            created_at_ms: 100,
            continued_to: Some(next),
            ..Default::default()
        };
        h.put(vec![(Table::Runs, run_key(&id), values::encode(&rec))]);
    }

    // RequestCancel on A: forwarded to B through the (self-addressed) outbox.
    let (out, actions, _) = h.apply_full(&cancel_req(&a, "stop chain"));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert!(
        actions
            .iter()
            .any(|x| matches!(x, Action::OutboxAppended { target, .. } if target.0 == P))
    );
    assert!(!h.run(&a).cancel_requested, "terminal A is not modified");
    let hop1 = outbox(&h, P);
    assert_eq!(hop1, vec![(1, cancel_kind(&b, "stop chain"))]);

    // Shuffler stand-in: deliver hop 1 (B forwards to C), then hop 2 (C cancels).
    assert_eq!(h.apply(&inbound(P, hop1)), ApplyOutcome::Accepted);
    let hop2: Vec<_> = outbox(&h, P).into_iter().filter(|(s, _)| *s > 1).collect();
    assert_eq!(hop2, vec![(2, cancel_kind(&c, "stop chain"))]);
    assert_eq!(h.apply(&inbound(P, hop2)), ApplyOutcome::Accepted);
    let rec = h.run(&c);
    assert!(rec.cancel_requested);
    assert_eq!(rec.cancel_reason, "stop chain");
    assert_eq!(cancel_events(&h, &c).len(), 1);
    assert_eq!(
        outbox(&h, P).len(),
        2,
        "C has no children: no further message"
    );

    // A terminal run without a successor: RunTerminal / dropped.
    let done = running(&mut h, 4, 1);
    assert_eq!(
        h.apply(&append(&h, &done, 0, vec![complete()])),
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.apply(&cancel_req(&done, "x")),
        rej(RejectReason::RunTerminal)
    );
    assert_eq!(
        h.apply(&inbound(9, vec![(1, cancel_kind(&done, "x"))])),
        ApplyOutcome::Accepted
    );
    assert_eq!(outbox(&h, P).len(), 2);
}
