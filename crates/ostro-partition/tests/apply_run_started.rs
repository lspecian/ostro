//! `StartRun.run_started` (tag 23, plan 05-16): when a StartRun carries a pre-encoded
//! `RunStarted`, apply writes it verbatim at seq 0 (opaque, like EventsAppended events),
//! sets `next_seq = 1` and runs the B1 first-events hook. Without the field nothing changes
//! (the executor appends RunStarted at the first activation).
//!
//! Pinned here:
//! - the event is journaled verbatim at seq 0 and `next_seq == 1`, with one point read;
//! - a signal sent before the first grant is journaled directly at seq >= 1 (no Pending slot);
//! - a cancel requested before the first grant is journaled directly as `CancelRequested`;
//! - W6 re-assert: `ReadyValue.first` / `EnqueueReady.first_activation` stay `true` for a fresh
//!   run (`current_attempt.is_none()`, not `next_seq == 0`) and turn `false` after the first
//!   grant;
//! - malformed fields are rejected before any write; a duplicate start writes nothing;
//! - rebuild digest over StartRuns with and without the field.

use bytes::Bytes;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, apply_command,
};
use ostro_partition::query::journal_rows;
use ostro_partition::store::keys::{decode_signal_inbox_key, run_key, run_prefix};
use ostro_partition::store::values::{self, ReadyValue, RunRecord, RunStatus, SignalSlot};
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, ConflictPolicy, Derived, EnqueueReady, EventsAppended,
    JournalEvent, RequestCancel, SignalRun, StartRun, derived, encode_command,
};
use ostro_types::journal::{EventBody, EventKind, decode_event};
use ostro_types::payload::INLINE_PAYLOAD_LIMIT;
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};

const P: u16 = 3;

struct H {
    _dir: tempfile::TempDir,
    store: PartitionStore,
    cur: Cursor,
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

    /// One command in its own committed batch: (outcome, actions, keyspace reads).
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

    fn run(&self, id: &RunId) -> Option<RunRecord> {
        self.store
            .reader()
            .get(Table::Runs, &run_key(id))
            .expect("read")
            .map(|v| values::decode(&v).expect("decode"))
    }

    fn journal(&self, r: &RunId) -> Vec<(u64, u32, PayloadRef)> {
        journal_rows(&self.store.reader(), r, 0, usize::MAX).expect("journal")
    }

    /// The single ready row's value (panics unless exactly one exists).
    fn ready_value(&self) -> ReadyValue {
        let rows = self
            .store
            .reader()
            .prefix(Table::Ready, &[], usize::MAX)
            .expect("ready");
        assert_eq!(rows.len(), 1, "one ready row");
        values::decode(&rows[0].1).expect("ready value")
    }

    fn slots(&self, r: &RunId) -> Vec<SignalSlot> {
        self.store
            .reader()
            .prefix(Table::Inbox, &run_prefix(r), usize::MAX)
            .expect("inbox")
            .into_iter()
            .filter(|(k, _)| decode_signal_inbox_key(k).is_some())
            .map(|(_, v)| values::decode(&v).expect("slot"))
            .collect()
    }

    fn digest(&self) -> [u8; 32] {
        self.store.digest().expect("digest")
    }

    /// Rows of the run-data keyspaces (runs, events, ready, payloads, inbox): the digest also
    /// folds in the cursor and outcome hash, which move for every applied command.
    fn data_rows(&self) -> Vec<(Table, Vec<u8>, Vec<u8>)> {
        let r = self.store.reader();
        let mut out = Vec::new();
        for t in [
            Table::Runs,
            Table::Events,
            Table::Ready,
            Table::Payloads,
            Table::Inbox,
        ] {
            for (k, v) in r.prefix(t, &[], usize::MAX).expect("rows") {
                out.push((t, k, v));
            }
        }
        out
    }

    /// Every logged command applied again to a fresh store in one batch reproduces the
    /// live digest.
    fn assert_rebuild_equal(&self) {
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
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&P.to_be_bytes());
    b[17] = n;
    RunId::from_bytes(&b).expect("18 bytes")
}

fn inline(b: &'static [u8]) -> PayloadRef {
    PayloadRef::inline(Bytes::from_static(b))
}

/// Opaque stand-in for the API's encoded RunStarted bytes (apply never decodes them).
fn started_event() -> JournalEvent {
    JournalEvent {
        seq: 0,
        kind: EventKind::RunStarted.code(),
        payload: Some(inline(b"\x01opaque-run-started")),
        ..Default::default()
    }
}

fn start(id: &RunId, run_started: Option<JournalEvent>) -> Command {
    Command::start_run(StartRun {
        run_id: id.as_bytes().to_vec(),
        workflow: "wf".into(),
        workflow_version: 1,
        queue: "q".into(),
        input: Some(inline(b"in")),
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        ready_at_ms: 100,
        requested_at_ms: 100,
        rng_seed: vec![7; 32],
        run_started,
        ..Default::default()
    })
}

fn grant(r: &RunId, n: u32) -> Command {
    Command::attempt_granted(AttemptGranted {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n }),
        after_failure: false,
    })
}

fn first_flag(actions: &[Action]) -> Option<bool> {
    actions.iter().find_map(|a| match a {
        Action::EnqueueReady {
            first_activation, ..
        } => Some(*first_activation),
        _ => None,
    })
}

#[test]
fn run_started_is_journaled_verbatim_at_seq0_and_next_seq_is_1() {
    let mut h = H::new();
    let r = rid(1);
    let (out, actions, reads) = h.apply_full(&start(&r, Some(started_event())));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(
        reads, 1,
        "the run-id point read only (inbox_next == 0: B1 hook is free)"
    );
    let j = h.journal(&r);
    assert_eq!(
        j,
        vec![(
            0,
            EventKind::RunStarted.code(),
            started_event().payload.expect("payload")
        )],
        "verbatim at seq 0"
    );
    let rec = h.run(&r).expect("run");
    assert_eq!(rec.next_seq, 1);
    assert_eq!(rec.status, RunStatus::Ready);
    assert!(rec.current_attempt.is_none());
    // W6: first activation is `current_attempt.is_none()`, not `next_seq == 0`.
    assert_eq!(first_flag(&actions), Some(true));
    assert!(h.ready_value().first);
    h.assert_rebuild_equal();
}

#[test]
fn without_run_started_nothing_is_journaled() {
    let mut h = H::new();
    let r = rid(2);
    let (out, actions, _) = h.apply_full(&start(&r, None));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert!(h.journal(&r).is_empty());
    assert_eq!(h.run(&r).expect("run").next_seq, 0);
    assert_eq!(first_flag(&actions), Some(true));
    assert!(h.ready_value().first);
}

#[test]
fn first_activation_flag_turns_false_after_the_first_grant() {
    let mut h = H::new();
    let r = rid(3);
    assert_eq!(
        h.apply(&start(&r, Some(started_event()))),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.apply(&grant(&r, 1)), ApplyOutcome::Accepted);
    // The executor requeues the run (EnqueueReady intent at the journal end, seq 1).
    let requeue = Command::events_appended(EventsAppended {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n: 1 }),
        from_seq: 1,
        events: vec![],
        derived: vec![Derived::new(derived::Intent::EnqueueReady(EnqueueReady {
            ready_at_ms: 0,
        }))],
    });
    let (out, actions, _) = h.apply_full(&requeue);
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(first_flag(&actions), Some(false));
    assert!(!h.ready_value().first);
    // An executor batch at from_seq 0 is now a seq mismatch: seq 0 is taken.
    assert_eq!(h.apply(&grant(&r, 2)), ApplyOutcome::Accepted);
    let at0 = Command::events_appended(EventsAppended {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n: 2 }),
        from_seq: 0,
        events: vec![JournalEvent {
            seq: 0,
            kind: EventKind::RunStarted.code(),
            payload: Some(inline(b"dup")),
            ..Default::default()
        }],
        derived: vec![],
    });
    assert_eq!(
        h.apply(&at0),
        ApplyOutcome::Rejected(RejectReason::SeqMismatch { expected: 1 })
    );
    h.assert_rebuild_equal();
}

#[test]
fn signal_before_first_grant_is_journaled_directly_without_a_pending_slot() {
    let mut h = H::new();
    let r = rid(4);
    assert_eq!(
        h.apply(&start(&r, Some(started_event()))),
        ApplyOutcome::Accepted
    );
    let ev = inline(b"\x01early");
    let (out, actions, _) = h.apply_full(&Command::signal_run(SignalRun {
        run_id: r.as_bytes().to_vec(),
        name: "x".into(),
        event: Some(ev.clone()),
        request_id: vec![],
        sent_at_ms: 777,
    }));
    assert_eq!(out, ApplyOutcome::Accepted);
    let j = h.journal(&r);
    assert_eq!(j.len(), 2);
    assert_eq!(
        (j[1].0, j[1].1, j[1].2.clone()),
        (1, EventKind::SignalPayload.code(), ev),
        "journaled at seq 1, verbatim"
    );
    assert_eq!(h.run(&r).expect("run").next_seq, 2);
    assert_eq!(
        h.slots(&r),
        vec![SignalSlot::Journaled {
            payload_seq: 1,
            sent_at_ms: 777
        }],
        "no Pending slot"
    );
    // A Ready run has no live activation: no wake-up; its first activation loads seq 1.
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::InputAppended { .. })),
        "{actions:?}"
    );
    h.assert_rebuild_equal();
}

#[test]
fn cancel_before_first_grant_is_journaled_directly() {
    let mut h = H::new();
    let r = rid(5);
    assert_eq!(
        h.apply(&start(&r, Some(started_event()))),
        ApplyOutcome::Accepted
    );
    let out = h.apply(&Command::request_cancel(RequestCancel {
        run_id: r.as_bytes().to_vec(),
        reason: "user".into(),
        requested_at_ms: 200,
    }));
    assert_eq!(out, ApplyOutcome::Accepted);
    let j = h.journal(&r);
    assert_eq!(j.len(), 2);
    assert_eq!((j[1].0, j[1].1), (1, EventKind::CancelRequested.code()));
    let bytes = match &j[1].2.kind {
        Some(ostro_types::payload::payload_ref::Kind::Inline(b)) => b.clone(),
        _ => panic!("apply-built events are inline"),
    };
    match decode_event(1, j[1].1, &bytes).expect("decode").body {
        EventBody::CancelRequested(c) => assert_eq!(c.reason, "user"),
        other => panic!("unexpected {other:?}"),
    }
    let rec = h.run(&r).expect("run");
    assert!(rec.cancel_requested);
    assert_eq!(rec.next_seq, 2);
    h.assert_rebuild_equal();
}

#[test]
fn malformed_run_started_is_rejected_before_any_write() {
    let mut h = H::new();
    let big = PayloadRef::inline(Bytes::from(vec![0u8; INLINE_PAYLOAD_LIMIT + 1]));
    let cases = [
        (
            JournalEvent {
                seq: 1,
                ..started_event()
            },
            RejectReason::Invalid(InvalidCode::EventsNotContiguous),
        ),
        (
            JournalEvent {
                kind: EventKind::StepCompleted.code(),
                ..started_event()
            },
            RejectReason::Invalid(InvalidCode::BadIntent),
        ),
        (
            JournalEvent {
                payload: None,
                ..started_event()
            },
            RejectReason::Invalid(InvalidCode::MissingField),
        ),
        (
            JournalEvent {
                payload: Some(big),
                ..started_event()
            },
            RejectReason::PayloadTooLarge,
        ),
    ];
    for (i, (ev, want)) in cases.into_iter().enumerate() {
        let r = rid(10 + u8::try_from(i).expect("small"));
        let before = h.data_rows();
        assert_eq!(
            h.apply(&start(&r, Some(ev))),
            ApplyOutcome::Rejected(want),
            "case {i}"
        );
        assert!(h.run(&r).is_none(), "case {i}: no run row");
        assert!(h.journal(&r).is_empty(), "case {i}: no journal row");
        assert!(h.data_rows() == before, "case {i}: nothing written");
    }
}

#[test]
fn duplicate_start_with_run_started_changes_nothing() {
    let mut h = H::new();
    let r = rid(6);
    assert_eq!(
        h.apply(&start(&r, Some(started_event()))),
        ApplyOutcome::Accepted
    );
    let before = h.data_rows();
    let other = JournalEvent {
        payload: Some(inline(b"other")),
        ..started_event()
    };
    assert_eq!(
        h.apply(&start(&r, Some(other))),
        ApplyOutcome::Duplicate { existing: Some(r) }
    );
    assert!(h.data_rows() == before, "a duplicate start writes nothing");
    assert_eq!(h.journal(&r).len(), 1);
}

#[test]
fn rebuild_digest_with_and_without_the_field() {
    let mut with = H::new();
    let mut without = H::new();
    for n in 1..=8u8 {
        let r = rid(n);
        let ev = (n % 2 == 0).then(started_event);
        assert_eq!(with.apply(&start(&r, ev)), ApplyOutcome::Accepted);
        assert_eq!(without.apply(&start(&r, None)), ApplyOutcome::Accepted);
        assert_eq!(with.apply(&grant(&r, 1)), ApplyOutcome::Accepted);
        assert_eq!(without.apply(&grant(&r, 1)), ApplyOutcome::Accepted);
    }
    with.assert_rebuild_equal();
    without.assert_rebuild_equal();
    assert_ne!(
        with.digest(),
        without.digest(),
        "the seq-0 rows and next_seq are state"
    );
}
