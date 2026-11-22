//! Parent-side child completion apply (plan 09-05, WF-06; D-P9-9, D21).
//!
//! Child-completion journaling is **opt-in** (09-05 decision, storage format v0.1 frozen):
//! the parent's `SendMessage(StartChild)` is followed by the `ChildJournal` marker (Derived
//! tag 19, P9 fallback slot), which marks the `children` row. An inbound `ChildCompleted`
//! for a marked row journals the kind-67 event at `next_seq` (verbatim `event`, or an
//! apply-encoded no-user-data failure) and wakes the parent; an unmarked row keeps the v0.1
//! legacy inbox row, so pre-Phase-9 logs apply exactly as before.

use bytes::Bytes;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, apply_command,
};
use ostro_partition::query::journal_rows;
use ostro_partition::store::keys::{children_key, run_key};
use ostro_partition::store::values::{self, InboxMsg, RunRecord, RunStatus};
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, CHILD_ROW_JOURNAL, ChildCompleted, ChildJournal, CompleteRun,
    ConflictPolicy, Derived, EventsAppended, InboundBatch, InboundMsg, JournalEvent, OutboxMsg,
    ParentRef, SendMessage, StartRun, TerminalStatus, TerminateRun, derived, encode_command,
    outbox_msg,
};
use ostro_types::journal::{
    self, ChildCompleted as ChildCompletedEv, EventBody, EventKind, Failure, OpOutput, Payload,
    decode_event, decode_result, encode_event, encode_result,
};
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};
use prost::Message;

// ---------------------------------------------------------------- harness

const P: u16 = 2;
const SRC: u32 = 5;

struct H {
    _dir: tempfile::TempDir,
    store: PartitionStore,
    cur: Cursor,
    /// Next inbound seq from `SRC`.
    seq: u64,
}

impl H {
    fn new() -> Self {
        let dir = ostro_testkit::fs::tempdir();
        let (store, meta) = PartitionStore::open(
            dir.path(),
            PartitionId(P),
            PartitionCount::new(16).expect("non-zero"),
            &StoreConfig::default(),
        )
        .expect("open store");
        let cur = Cursor::from_meta(&meta);
        let mut h = H {
            _dir: dir,
            store,
            cur,
            seq: 0,
        };
        assert_eq!(
            h.apply(&Command::leader_announced(1, 1, 0)),
            ApplyOutcome::Accepted
        );
        h
    }

    /// (outcome, actions, keyspace reads) of one command in its own batch.
    fn apply_full(&mut self, c: &Command) -> (ApplyOutcome, Vec<Action>, u64) {
        let bytes = encode_command(c);
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

    fn ok(&mut self, c: &Command) -> Vec<Action> {
        let (out, actions, _) = self.apply_full(c);
        assert_eq!(out, ApplyOutcome::Accepted, "{}", c.kind_name());
        actions
    }

    fn get(&self, t: Table, k: &[u8]) -> Option<Vec<u8>> {
        self.store.reader().get(t, k).expect("read")
    }

    fn run(&self, id: &RunId) -> RunRecord {
        values::decode(&self.get(Table::Runs, &run_key(id)).expect("run row")).expect("decode")
    }

    fn rows(&self, t: Table) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.store
            .reader()
            .prefix(t, &[], usize::MAX)
            .expect("rows")
    }

    fn journal(&self, run: &RunId) -> Vec<(u64, u32, PayloadRef)> {
        journal_rows(&self.store.reader(), run, 0, usize::MAX).expect("journal")
    }

    /// Delivers `msg` from `SRC` at the next seq.
    fn deliver(&mut self, kind: outbox_msg::Kind) -> (ApplyOutcome, Vec<Action>) {
        self.seq += 1;
        let (o, a, _) = self.apply_full(&inbound(self.seq, kind));
        (o, a)
    }
}

fn inbound(seq: u64, kind: outbox_msg::Kind) -> Command {
    Command::inbound_batch(InboundBatch {
        src: SRC,
        msgs: vec![InboundMsg {
            seq,
            msg: Some(OutboxMsg { kind: Some(kind) }),
        }],
    })
}

fn rid(n: u8) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&P.to_be_bytes());
    b[17] = n;
    RunId::from_bytes(&b).expect("18 bytes")
}

fn start(id: &RunId) -> StartRun {
    StartRun {
        run_id: id.as_bytes().to_vec(),
        workflow: "wf".into(),
        workflow_version: 1,
        queue: "q".into(),
        input: Some(PayloadRef::inline(Bytes::from_static(b"in"))),
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        ready_at_ms: 100,
        requested_at_ms: 100,
        rng_seed: vec![7; 32],
        ..Default::default()
    }
}

fn grant(r: &RunId, n: u32) -> Command {
    Command::attempt_granted(AttemptGranted {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n }),
        after_failure: false,
    })
}

fn ev(seq: u64) -> JournalEvent {
    JournalEvent {
        seq,
        kind: 16,
        payload: Some(PayloadRef::inline(Bytes::from_static(b"e"))),
        ..Default::default()
    }
}

fn append(r: &RunId, from: u64, count: u64, d: Vec<Derived>) -> Command {
    Command::events_appended(EventsAppended {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n: 1 }),
        from_seq: from,
        events: (from..from + count).map(ev).collect(),
        derived: d,
    })
}

fn start_child_intent(parent: &RunId, op: u64, child: &RunId) -> Derived {
    let mut s = start(child);
    s.parent = Some(ParentRef {
        run_id: parent.as_bytes().to_vec(),
        op,
    });
    Derived::new(derived::Intent::SendMessage(SendMessage {
        msg: Some(OutboxMsg {
            kind: Some(outbox_msg::Kind::StartChild(
                ostro_types::command::StartChild { start: Some(s) },
            )),
        }),
    }))
}

/// A running parent `p` with journal `0..3` whose seq-2 flush started child `c` (op 2),
/// marked for journaling iff `marked`.
fn parent_with_child(h: &mut H, p: &RunId, c: &RunId, marked: bool) {
    h.ok(&Command::start_run(start(p)));
    h.ok(&grant(p, 1));
    let mut d = vec![start_child_intent(p, 2, c)];
    if marked {
        d.push(Derived::child_journal(c));
    }
    h.ok(&append(p, 0, 3, d));
    let rec = h.run(p);
    assert_eq!(rec.children, 1);
    assert_eq!(rec.next_seq, 3);
}

fn completed(
    p: &RunId,
    c: &RunId,
    status: TerminalStatus,
    event: Option<PayloadRef>,
) -> outbox_msg::Kind {
    outbox_msg::Kind::ChildCompleted(ChildCompleted {
        parent_run: p.as_bytes().to_vec(),
        child_run: c.as_bytes().to_vec(),
        op: 2,
        status: status as i32,
        result: Some(PayloadRef::inline(Bytes::from_static(b"\"r\""))),
        event,
    })
}

/// Submitter-encoded parent event (what the child's derive builds, 09-05 Task 2).
fn child_event(op: u64, out: Result<OpOutput, Failure>) -> Vec<u8> {
    encode_event(&journal::Event {
        seq: 0,
        ts_nanos: 1_234_000_000,
        body: EventBody::ChildCompleted(ChildCompletedEv {
            op,
            outcome: encode_result(&out),
        }),
    })
    .1
}

fn inline_bytes(p: &PayloadRef) -> &[u8] {
    match &p.kind {
        Some(ostro_types::payload::payload_ref::Kind::Inline(b)) => b,
        other => panic!("not inline: {other:?}"),
    }
}

fn inbox_rows(h: &H) -> Vec<InboxMsg> {
    h.rows(Table::Inbox)
        .into_iter()
        .filter_map(|(_, v)| values::decode::<InboxMsg>(&v).ok())
        .collect()
}

/// Decodes the parent's journal row at `seq` as a kind-67 event and returns its outcome.
fn journaled_outcome(h: &H, p: &RunId, seq: u64) -> Result<OpOutput, Failure> {
    let rows = h.journal(p);
    let (s, kind, pref) = rows.iter().find(|r| r.0 == seq).expect("journal row");
    assert_eq!(*kind, EventKind::ChildCompleted.code());
    let bytes = inline_bytes(pref);
    let e = decode_event(*s, *kind, bytes).expect("decode event");
    let EventBody::ChildCompleted(c) = e.body else {
        panic!("not ChildCompleted: {e:?}")
    };
    assert_eq!(c.op, 2);
    decode_result(&c.outcome).expect("decode outcome")
}

// ---------------------------------------------------------------- marker

#[test]
fn child_journal_intent_tag_is_19() {
    let hex: String = Derived::new(derived::Intent::ChildJournal(ChildJournal::default()))
        .encode_to_vec()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(hex, "9a0100");
    assert_eq!(CHILD_ROW_JOURNAL, &[1u8][..]);
}

#[test]
fn marker_must_follow_its_start_child() {
    let mut h = H::new();
    let (p, c, other) = (rid(1), rid(2), rid(3));
    h.ok(&Command::start_run(start(&p)));
    h.ok(&grant(&p, 1));
    let bad = rejected(RejectReason::Invalid(InvalidCode::BadIntent));
    // Alone.
    assert_eq!(
        h.apply(&append(&p, 0, 1, vec![Derived::child_journal(&c)])),
        bad
    );
    // For another child.
    assert_eq!(
        h.apply(&append(
            &p,
            0,
            1,
            vec![
                start_child_intent(&p, 0, &c),
                Derived::child_journal(&other)
            ]
        )),
        bad
    );
    // Not directly after it.
    assert_eq!(
        h.apply(&append(
            &p,
            0,
            1,
            vec![
                start_child_intent(&p, 0, &c),
                Derived::new(derived::Intent::EnqueueReady(Default::default())),
                Derived::child_journal(&c)
            ]
        )),
        bad
    );
    assert!(h.rows(Table::Children).is_empty());
    // Directly after it: the row is marked, counted once.
    h.ok(&append(
        &p,
        0,
        1,
        vec![start_child_intent(&p, 0, &c), Derived::child_journal(&c)],
    ));
    assert_eq!(
        h.get(Table::Children, &children_key(&p, &c)).as_deref(),
        Some(CHILD_ROW_JOURNAL)
    );
    assert_eq!(h.run(&p).children, 1);
}

fn rejected(r: RejectReason) -> ApplyOutcome {
    ApplyOutcome::Rejected(r)
}

// ---------------------------------------------------------------- journaling

#[test]
fn running_parent_journals_event_verbatim_and_gets_input_appended() {
    let mut h = H::new();
    let (p, c) = (rid(1), rid(2));
    parent_with_child(&mut h, &p, &c, true);
    let bytes = child_event(
        2,
        Ok(OpOutput::Child(Payload {
            content_type: "application/json".into(),
            data: b"42".to_vec(),
        })),
    );
    let event = PayloadRef::inline(Bytes::from(bytes.clone()));
    let (o, actions) = h.deliver(completed(
        &p,
        &c,
        TerminalStatus::Completed,
        Some(event.clone()),
    ));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(actions.contains(&Action::InputAppended { run: p, seq: 3 }));
    let rows = h.journal(&p);
    let last = rows.last().expect("row");
    assert_eq!((last.0, last.1), (3, EventKind::ChildCompleted.code()));
    assert_eq!(last.2, event, "verbatim PayloadRef");
    let rec = h.run(&p);
    assert_eq!(rec.next_seq, 4);
    assert_eq!(rec.children, 0);
    assert_eq!(rec.status, RunStatus::Running);
    assert!(h.get(Table::Children, &children_key(&p, &c)).is_none());
    assert!(inbox_rows(&h).is_empty(), "no legacy inbox row");
    assert_eq!(
        journaled_outcome(&h, &p, 3),
        Ok(OpOutput::Child(Payload {
            content_type: "application/json".into(),
            data: b"42".to_vec(),
        }))
    );
}

#[test]
fn scheduled_parent_becomes_ready() {
    let mut h = H::new();
    let (p, c) = (rid(1), rid(2));
    parent_with_child(&mut h, &p, &c, true);
    h.ok(&append(&p, 3, 0, vec![Derived::suspend()]));
    assert_eq!(h.run(&p).status, RunStatus::Scheduled);
    let event = PayloadRef::inline(Bytes::from(child_event(2, Err(Failure::Timeout))));
    let (o, actions) = h.deliver(completed(&p, &c, TerminalStatus::Failed, Some(event)));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert_eq!(h.run(&p).status, RunStatus::Ready);
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::EnqueueReady { run, .. } if *run == p)),
        "{actions:?}"
    );
    assert_eq!(journaled_outcome(&h, &p, 3), Err(Failure::Timeout));
}

#[test]
fn ready_parent_gets_the_event_only() {
    let mut h = H::new();
    let (p, c) = (rid(1), rid(2));
    parent_with_child(&mut h, &p, &c, true);
    h.ok(&append(&p, 3, 0, vec![Derived::suspend()]));
    // A first marked child completion wakes it (Scheduled -> Ready) ...
    let c2 = rid(3);
    h.ok(&grant(&p, 2));
    h.ok(&Command::events_appended(EventsAppended {
        run_id: p.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n: 2 }),
        from_seq: 3,
        events: vec![ev(3)],
        derived: vec![
            start_child_intent(&p, 3, &c2),
            Derived::child_journal(&c2),
            Derived::suspend(),
        ],
    }));
    assert_eq!(h.run(&p).status, RunStatus::Scheduled);
    h.deliver(completed(&p, &c, TerminalStatus::Cancelled, None));
    assert_eq!(h.run(&p).status, RunStatus::Ready);
    // ... the second only journals.
    let (o, actions) = h.deliver(completed(&p, &c2, TerminalStatus::Cancelled, None));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(
        !actions.iter().any(|a| matches!(
            a,
            Action::EnqueueReady { .. } | Action::InputAppended { .. }
        )),
        "{actions:?}"
    );
    let rec = h.run(&p);
    assert_eq!(rec.status, RunStatus::Ready);
    assert_eq!(rec.next_seq, 6);
    assert_eq!(rec.children, 0);
}

#[test]
fn blob_backed_event_is_journaled_by_reference() {
    let mut h = H::new();
    let (p, c) = (rid(1), rid(2));
    parent_with_child(&mut h, &p, &c, true);
    // A 1 MiB event lives in the blob store; the harness has none, so any fetch would fail.
    let event = PayloadRef::blob([0xAB; 32], 1 << 20);
    let (o, _) = h.deliver(completed(
        &p,
        &c,
        TerminalStatus::Completed,
        Some(event.clone()),
    ));
    assert_eq!(o, ApplyOutcome::Accepted);
    let rows = h.journal(&p);
    let last = rows.last().expect("row");
    assert_eq!(last.0, 3);
    assert_eq!(
        last.2.encode_to_vec(),
        event.encode_to_vec(),
        "byte-for-byte"
    );
}

#[test]
fn event_less_completions_map_to_no_user_data_failures() {
    let terminated = |m: &str| {
        Err(Failure::NonRetryable(journal::ErrorInfo {
            kind: "child_terminated".into(),
            message: m.into(),
            details: None,
        }))
    };
    let cases = [
        (
            TerminalStatus::Terminated,
            terminated("child run was terminated"),
        ),
        (TerminalStatus::TimedOut, Err(Failure::Timeout)),
        (TerminalStatus::Cancelled, Err(Failure::Cancelled)),
        (
            TerminalStatus::Completed,
            terminated("child completed while the parent was terminal"),
        ),
        (
            TerminalStatus::Failed,
            terminated("child completed while the parent was terminal"),
        ),
    ];
    for (status, want) in cases {
        let mut h = H::new();
        let (p, c) = (rid(1), rid(2));
        parent_with_child(&mut h, &p, &c, true);
        let (o, actions) = h.deliver(completed(&p, &c, status, None));
        assert_eq!(o, ApplyOutcome::Accepted, "{status:?}");
        assert!(actions.contains(&Action::InputAppended { run: p, seq: 3 }));
        assert_eq!(journaled_outcome(&h, &p, 3), want, "{status:?}");
        // ts = the parent's created_at_ms (the inbound message has no time).
        let (s, k, pref) = h.journal(&p).pop().expect("row");
        let e = decode_event(s, k, inline_bytes(&pref)).expect("decode");
        assert_eq!(e.ts_nanos, 100 * 1_000_000);
    }
}

// ---------------------------------------------------------------- v0.1 legacy and drops

#[test]
fn unmarked_row_keeps_the_legacy_inbox_row() {
    let mut h = H::new();
    let (p, c) = (rid(1), rid(2));
    parent_with_child(&mut h, &p, &c, false);
    assert_eq!(
        h.get(Table::Children, &children_key(&p, &c)).as_deref(),
        Some(&[][..])
    );
    let event = PayloadRef::inline(Bytes::from(child_event(2, Err(Failure::Cancelled))));
    let (o, _) = h.deliver(completed(&p, &c, TerminalStatus::Completed, Some(event)));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert_eq!(h.journal(&p).len(), 3, "nothing journaled");
    let inbox = inbox_rows(&h);
    assert_eq!(inbox.len(), 1);
    assert!(matches!(inbox[0], InboxMsg::ChildCompleted { op: 2, .. }));
    let rec = h.run(&p);
    assert_eq!(rec.next_seq, 3);
    assert_eq!(rec.children, 0);
}

#[test]
fn terminal_parent_drops_and_consumes_the_seq_duplicates_are_duplicate() {
    let mut h = H::new();
    let (p, c) = (rid(1), rid(2));
    parent_with_child(&mut h, &p, &c, true);
    h.ok(&Command::terminate_run(TerminateRun {
        run_id: p.as_bytes().to_vec(),
        reason: "stop".into(),
        terminated_at_ms: 500,
    }));
    let before = h.journal(&p).len();
    let (o, _) = h.deliver(completed(&p, &c, TerminalStatus::Completed, None));
    assert_eq!(o, ApplyOutcome::Accepted, "per-message drop");
    assert_eq!(h.journal(&p).len(), before);
    assert_eq!(h.cur.inbound_seen.get(&(SRC as u16)), Some(&1));
    // Unknown parent: dropped too.
    let (o, _) = h.deliver(completed(&rid(9), &c, TerminalStatus::Completed, None));
    assert_eq!(o, ApplyOutcome::Accepted);
    // Redelivery of seq 2.
    let o = h.apply(&inbound(
        2,
        completed(&p, &c, TerminalStatus::Completed, None),
    ));
    assert_eq!(o, ApplyOutcome::Duplicate { existing: None });
}

/// End to end on one partition: the child's CompleteRun carries `parent_event`, apply
/// forwards it in the outbox ChildCompleted, and the parent journals it verbatim.
#[test]
fn child_complete_run_parent_event_reaches_the_parent_journal() {
    let mut h = H::new();
    let (p, c) = (rid(1), rid(2));
    parent_with_child(&mut h, &p, &c, true);
    // The StartChild sits in the outbox for partition 2; start the child from it.
    let start_msg = h
        .rows(Table::Outbox)
        .into_iter()
        .map(|(_, v)| OutboxMsg::decode(v.as_slice()).expect("msg"))
        .find_map(|m| match m.kind {
            Some(outbox_msg::Kind::StartChild(s)) => s.start,
            _ => None,
        })
        .expect("StartChild in outbox");
    h.ok(&Command::start_run(start_msg));
    h.ok(&grant(&c, 1));
    let bytes = child_event(
        2,
        Ok(OpOutput::Child(Payload {
            content_type: "application/json".into(),
            data: b"\"done\"".to_vec(),
        })),
    );
    let event = PayloadRef::inline(Bytes::from(bytes));
    h.ok(&append(
        &c,
        0,
        1,
        vec![Derived::new(derived::Intent::CompleteRun(CompleteRun {
            status: TerminalStatus::Completed as i32,
            result: Some(PayloadRef::inline(Bytes::from_static(b"\"done\""))),
            completed_at_ms: 700,
            resumable: false,
            parent_event: Some(event.clone()),
        }))],
    ));
    let done = h
        .rows(Table::Outbox)
        .into_iter()
        .map(|(_, v)| OutboxMsg::decode(v.as_slice()).expect("msg"))
        .find_map(|m| match m.kind {
            Some(k @ outbox_msg::Kind::ChildCompleted(_)) => Some(k),
            _ => None,
        })
        .expect("ChildCompleted in outbox");
    let (o, _) = h.deliver(done);
    assert_eq!(o, ApplyOutcome::Accepted);
    let last = h.journal(&p).pop().expect("row");
    assert_eq!((last.0, last.2), (3, event));
    assert_eq!(
        journaled_outcome(&h, &p, 3),
        Ok(OpOutput::Child(Payload {
            content_type: "application/json".into(),
            data: b"\"done\"".to_vec(),
        }))
    );
}
