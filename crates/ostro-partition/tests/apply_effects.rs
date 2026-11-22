//! In-flight effects: `effects` rows, `RegisterEffect`/`CancelEffect` intents, the
//! attempt-independent `CompleteEffect`, terminal/retention cleanup (P-8) and the
//! leader-start wake (P-9) (plan 06-03; FX-01, FX-03).
//!
//! Every test drives `apply_command` against an on-disk partition store, one committed batch
//! per command, so the `ApplyBatch` read counter is per command. `CompleteEffect.outcome` is
//! built with `complete_effect_payload` (06-02, B2); apply stores it verbatim, and only the
//! tests decode it.

use bytes::Bytes;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, apply_command,
};
use ostro_partition::query::{effect_row, journal_rows, open_effects};
use ostro_partition::store::keys::{effect_key, ready_key, run_key};
use ostro_partition::store::values::{self, EffectRow, PauseMark, RunRecord, RunStatus};
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, CompleteEffect, CompleteRun, ConflictPolicy, Derived,
    EXPIRE_OP, EventsAppended, JournalEvent, RetryRun, StartRun, TerminalStatus, TerminateRun,
    TimerFired, complete_effect_payload, derived,
};
use ostro_types::journal::{EffectCompleted, EventBody, EventKind, RecordedResult, decode_event};
use ostro_types::payload::INLINE_PAYLOAD_LIMIT;
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};

const P: u16 = 2;

// ---------------------------------------------------------------- harness

struct H {
    _dir: tempfile::TempDir,
    store: PartitionStore,
    cur: Cursor,
    /// Envelope epoch of the next command (follows accepted `LeaderAnnounced`s).
    epoch: u32,
    /// Every applied command with its envelope epoch, for the rebuild check.
    log: Vec<(u32, Vec<u8>)>,
}

/// One applied command: outcome, actions, keyspace reads, overlay entries (writes).
struct Applied {
    out: ApplyOutcome,
    actions: Vec<Action>,
    reads: u64,
    writes: usize,
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
            epoch: 1,
            log: Vec::new(),
        };
        assert_eq!(h.announce(1).out, ApplyOutcome::Accepted);
        h
    }

    fn apply_at(&mut self, epoch: u32, c: &Command) -> Applied {
        let bytes = ostro_types::command::encode_command(c);
        self.log.push((epoch, bytes.to_vec()));
        let mut b = self.store.batch();
        let mut actions = Vec::new();
        let off = self.cur.next;
        let out = apply_command(
            &mut b,
            &mut self.cur,
            off,
            Epoch(epoch),
            &bytes,
            &mut actions,
        )
        .expect("apply");
        let reads = b.reads();
        let writes = b.len();
        let mu = self.cur.meta_update();
        b.commit(&mu).expect("commit");
        Applied {
            out,
            actions,
            reads,
            writes,
        }
    }

    fn apply_full(&mut self, c: &Command) -> Applied {
        let e = self.epoch;
        self.apply_at(e, c)
    }

    fn apply(&mut self, c: &Command) -> ApplyOutcome {
        self.apply_full(c).out
    }

    /// `LeaderAnnounced{epoch}` in an envelope of that epoch; on acceptance later commands
    /// use it.
    fn announce(&mut self, epoch: u32) -> Applied {
        let a = self.apply_at(epoch, &Command::leader_announced(epoch, 1, 0));
        if a.out == ApplyOutcome::Accepted {
            self.epoch = epoch;
        }
        a
    }

    /// Raw rows outside apply (states apply cannot reach yet); disables the rebuild check.
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

    fn run(&self, id: &RunId) -> RunRecord {
        values::decode(&self.get(Table::Runs, &run_key(id)).expect("run row")).expect("decode")
    }

    fn journal(&self, r: &RunId) -> Vec<(u64, u32, PayloadRef)> {
        journal_rows(&self.store.reader(), r, 0, usize::MAX).expect("journal")
    }

    fn row_at(&self, r: &RunId, seq: u64) -> (u32, PayloadRef) {
        self.journal(r)
            .into_iter()
            .find(|(s, ..)| *s == seq)
            .map(|(_, k, p)| (k, p))
            .expect("journal row")
    }

    fn effect(&self, r: &RunId, op: u64) -> Option<EffectRow> {
        effect_row(&self.store.reader(), r, op).expect("effect row")
    }

    fn effects(&self, r: &RunId) -> Vec<(u64, EffectRow)> {
        open_effects(&self.store.reader(), r).expect("open effects")
    }

    fn all_effects(&self) -> usize {
        self.store
            .reader()
            .prefix(Table::Effects, &[], usize::MAX)
            .expect("prefix")
            .len()
    }

    fn digest(&self) -> [u8; 32] {
        self.store.digest().expect("digest")
    }

    /// Replays the log into a fresh store in ONE batch; returns (digest, rolling hash).
    fn replay(&self) -> ([u8; 32], [u8; 32]) {
        let (_dir, store, mut cur) = open_store();
        let mut b = store.batch();
        let mut actions = Vec::new();
        for (e, bytes) in &self.log {
            let off = cur.next;
            apply_command(&mut b, &mut cur, off, Epoch(*e), bytes, &mut actions).expect("apply");
        }
        let mu = cur.meta_update();
        b.commit(&mu).expect("commit");
        (store.digest().expect("digest"), cur.hash)
    }

    /// Rebuild check (no state outside the log, no batch-boundary leakage).
    fn assert_rebuild_equal(&self) {
        assert!(!self.log.is_empty(), "harness has raw puts");
        let (d, hash) = self.replay();
        assert_eq!(d, self.digest(), "rebuild digest");
        assert_eq!(hash, self.cur.hash, "rebuild rolling hash");
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

fn grant(h: &H, r: &RunId, n: u32) -> Command {
    Command::attempt_granted(AttemptGranted {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: h.epoch, n }),
        after_failure: false,
    })
}

/// `EventsAppended` at the live `next_seq` with `events` opaque events and the run's current
/// token.
fn append(h: &H, r: &RunId, events: u64, derived: Vec<Derived>) -> Command {
    let rec = h.run(r);
    let (epoch, n) = rec.current_attempt.expect("granted");
    let from = rec.next_seq;
    Command::events_appended(EventsAppended {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch, n }),
        from_seq: from,
        events: (from..from + events)
            .map(|seq| JournalEvent {
                seq,
                kind: 16,
                payload: Some(inline(b"e")),
                ..Default::default()
            })
            .collect(),
        derived,
    })
}

/// Run `n` started, granted and with `events` journaled (seqs `0..events`).
fn running(h: &mut H, n: u8, events: u64, start_run: StartRun) -> RunId {
    let r = rid(n);
    assert_eq!(
        h.apply(&Command::start_run(start_run)),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.apply(&grant(h, &r, 1)), ApplyOutcome::Accepted);
    if events > 0 {
        assert_eq!(
            h.apply(&append(h, &r, events, vec![])),
            ApplyOutcome::Accepted
        );
    }
    r
}

/// One opaque `EffectScheduled` event plus `RegisterEffect` for its seq. Returns the op.
fn schedule(h: &mut H, r: &RunId, kind: u32) -> u64 {
    let op = h.run(r).next_seq;
    let c = append(
        h,
        r,
        1,
        vec![Derived::register_effect(op, kind, 1_000 + op)],
    );
    assert_eq!(h.apply(&c), ApplyOutcome::Accepted);
    op
}

fn suspend(h: &mut H, r: &RunId) {
    let c = append(h, r, 0, vec![Derived::suspend()]);
    assert_eq!(h.apply(&c), ApplyOutcome::Accepted);
    assert_eq!(h.run(r).status, RunStatus::Scheduled);
}

fn result(tag: u8) -> RecordedResult {
    RecordedResult {
        codec_ver: 1,
        bytes: vec![tag, 2, 3],
    }
}

fn outcome(op: u64, tag: u8) -> PayloadRef {
    PayloadRef::inline(Bytes::from(complete_effect_payload(
        op,
        &result(tag),
        9_000_000_000,
    )))
}

fn complete_effect(r: &RunId, op: u64, outcome: Option<PayloadRef>) -> Command {
    Command::complete_effect(CompleteEffect {
        run_id: r.as_bytes().to_vec(),
        op,
        outcome,
        completed_at_ms: 9_000,
    })
}

fn complete_run(status: TerminalStatus, resumable: bool) -> Derived {
    Derived::new(derived::Intent::CompleteRun(CompleteRun {
        status: status as i32,
        result: None,
        completed_at_ms: 900,
        resumable,
        parent_event: None,
    }))
}

fn terminate(r: &RunId) -> Command {
    Command::terminate_run(TerminateRun {
        run_id: r.as_bytes().to_vec(),
        reason: "ops".into(),
        terminated_at_ms: 500,
    })
}

fn retry(r: &RunId) -> Command {
    Command::retry_run(RetryRun {
        run_id: r.as_bytes().to_vec(),
        requested_at_ms: 600,
    })
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

fn decoded(seq: u64, kind: u32, bytes: &[u8]) -> EventBody {
    decode_event(seq, kind, bytes).expect("decode").body
}

fn inline_bytes(p: &PayloadRef) -> Bytes {
    match &p.kind {
        Some(ostro_types::payload::payload_ref::Kind::Inline(b)) => b.clone(),
        _ => panic!("inline payload expected"),
    }
}

// ---------------------------------------------------------------- RegisterEffect / CancelEffect

#[test]
fn register_effect_writes_row_idempotently_with_no_extra_reads() {
    let mut h = H::new();
    let r = running(&mut h, 1, 2, start(&rid(1)));

    // Baseline: a plain EventsAppended reads only the run record.
    let base = h.apply_full(&append(&h, &r, 1, vec![]));
    assert_eq!((base.out, base.reads), (ApplyOutcome::Accepted, 1));

    let op = h.run(&r).next_seq;
    let a = h.apply_full(&append(
        &h,
        &r,
        1,
        vec![Derived::register_effect(op, 1, 4_242)],
    ));
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert_eq!(a.reads, 1, "RegisterEffect adds no reads");
    assert_eq!(
        h.effect(&r, op),
        Some(EffectRow {
            kind: 1,
            scheduled_at_ms: 4_242
        })
    );
    let before = h.get(Table::Effects, &effect_key(&r, op)).expect("row");

    // Repeat for the same op (e.g. a re-arm): byte-identical row.
    let a = h.apply_full(&append(
        &h,
        &r,
        0,
        vec![Derived::register_effect(op, 1, 4_242)],
    ));
    assert_eq!((a.out, a.reads), (ApplyOutcome::Accepted, 1));
    assert_eq!(h.get(Table::Effects, &effect_key(&r, op)), Some(before));
    assert_eq!(h.effects(&r).len(), 1);

    // An op that is not journaled yet is dropped deterministically (like RegisterWait).
    let ahead = h.run(&r).next_seq + 5;
    let c = append(&h, &r, 0, vec![Derived::register_effect(ahead, 0, 1)]);
    assert_eq!(h.apply(&c), ApplyOutcome::Accepted);
    assert_eq!(h.effect(&r, ahead), None);
    h.assert_rebuild_equal();
}

#[test]
fn register_effect_bad_kind_is_bad_intent_without_writes() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1, start(&rid(1)));
    let next = h.run(&r).next_seq;
    let c = append(&h, &r, 1, vec![Derived::register_effect(next, 3, 1)]);
    assert_eq!(h.apply(&c), invalid(InvalidCode::BadIntent));
    assert_eq!(h.run(&r).next_seq, next, "nothing written");
    assert_eq!(h.all_effects(), 0);
    // Reserved ops are apply-owned.
    let c = append(&h, &r, 1, vec![Derived::register_effect(EXPIRE_OP, 0, 1)]);
    assert_eq!(h.apply(&c), invalid(InvalidCode::BadIntent));
    h.assert_rebuild_equal();
}

#[test]
fn cancel_effect_is_a_blind_delete_and_completion_after_it_is_duplicate() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1, start(&rid(1)));
    let op = schedule(&mut h, &r, 0);
    assert!(h.effect(&r, op).is_some());

    let a = h.apply_full(&append(&h, &r, 0, vec![Derived::cancel_effect(op)]));
    assert_eq!(
        (a.out, a.reads),
        (ApplyOutcome::Accepted, 1),
        "no extra read"
    );
    assert_eq!(h.effect(&r, op), None);
    // Cancelling a missing row is fine.
    let c = append(
        &h,
        &r,
        0,
        vec![Derived::cancel_effect(op), Derived::cancel_effect(77)],
    );
    assert_eq!(h.apply(&c), ApplyOutcome::Accepted);

    let next = h.run(&r).next_seq;
    let a = h.apply_full(&complete_effect(&r, op, Some(outcome(op, 1))));
    assert_eq!((a.out, a.reads), (dup(), 2));
    assert_eq!(h.run(&r).next_seq, next, "no journal write");
    h.assert_rebuild_equal();
}

// ---------------------------------------------------------------- CompleteEffect

#[test]
fn complete_effect_journals_verbatim_once_then_duplicate() {
    let mut h = H::new();
    let r = running(&mut h, 1, 3, start(&rid(1)));
    let op = schedule(&mut h, &r, 0);
    assert_eq!(op, 3);
    let seq = h.run(&r).next_seq;
    let out = outcome(op, 7);

    let a = h.apply_full(&complete_effect(&r, op, Some(out.clone())));
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert_eq!(a.reads, 2, "run record + effects point read");
    assert_eq!(a.actions, vec![Action::InputAppended { run: r, seq }]);
    let (kind, payload) = h.row_at(&r, seq);
    assert_eq!(kind, EventKind::EffectCompleted.code());
    assert_eq!(kind, 65);
    assert_eq!(payload, out, "payload bytes equal the submitted outcome");
    assert_eq!(
        decoded(seq, kind, &inline_bytes(&payload)),
        EventBody::EffectCompleted(EffectCompleted {
            op,
            outcome: result(7)
        })
    );
    assert_eq!(h.effect(&r, op), None, "row deleted");
    assert_eq!(h.run(&r).next_seq, seq + 1);

    // Second completion (another bridge, a retry): Duplicate, nothing written.
    let a = h.apply_full(&complete_effect(&r, op, Some(outcome(op, 8))));
    assert_eq!((a.out, a.reads), (dup(), 2));
    assert!(a.actions.is_empty());
    assert_eq!(h.run(&r).next_seq, seq + 1);
    let completions = h
        .journal(&r)
        .iter()
        .filter(|(_, k, _)| *k == EventKind::EffectCompleted.code())
        .count();
    assert_eq!(completions, 1);

    // Determinism: the same log applied twice gives the same state and rolling hash.
    h.assert_rebuild_equal();
    assert_eq!(h.replay(), h.replay());
}

#[test]
fn complete_effect_wakes_a_suspended_run() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1, start(&rid(1)));
    let op = schedule(&mut h, &r, 2);
    suspend(&mut h, &r);
    let a = h.apply_full(&complete_effect(&r, op, Some(outcome(op, 1))));
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert_eq!(a.reads, 2);
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::Ready);
    assert!(matches!(
        a.actions.as_slice(),
        [Action::EnqueueReady { run, .. }] if *run == r
    ));
    let slot = rec.ready.expect("ready slot");
    assert!(
        h.get(
            Table::Ready,
            &ready_key("q", slot.priority, slot.ready_at_ms, &r)
        )
        .is_some()
    );
    h.assert_rebuild_equal();
}

#[test]
fn complete_effect_rejections() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1, start(&rid(1)));
    let op = schedule(&mut h, &r, 0);

    // Unknown run.
    let a = h.apply_full(&complete_effect(&rid(9), 0, Some(outcome(0, 1))));
    assert_eq!((a.out, a.reads), (rej(RejectReason::RunNotFound), 1));
    // op >= next_seq: never journaled.
    let next = h.run(&r).next_seq;
    let a = h.apply_full(&complete_effect(&r, next, Some(outcome(next, 1))));
    assert_eq!(
        (a.out, a.reads),
        (invalid(InvalidCode::BadEffectCompletion), 1)
    );
    // Missing outcome.
    assert_eq!(
        h.apply(&complete_effect(&r, op, None)),
        invalid(InvalidCode::BadEffectCompletion)
    );
    // Oversize inline outcome.
    let big = PayloadRef::inline(Bytes::from(vec![1u8; INLINE_PAYLOAD_LIMIT + 1]));
    assert_eq!(
        h.apply(&complete_effect(&r, op, Some(big))),
        rej(RejectReason::PayloadTooLarge)
    );
    // Foreign partition.
    let mut other = [0u8; 18];
    other[1] = 3;
    let foreign = RunId::from_bytes(&other).expect("id");
    assert_eq!(
        h.apply(&complete_effect(&foreign, 0, Some(outcome(0, 1)))),
        rej(RejectReason::WrongPartition)
    );
    assert!(h.effect(&r, op).is_some(), "rejections write nothing");
    assert_eq!(h.run(&r).next_seq, next);

    // Terminal run.
    let c = append(
        &h,
        &r,
        1,
        vec![complete_run(TerminalStatus::Completed, false)],
    );
    assert_eq!(h.apply(&c), ApplyOutcome::Accepted);
    let a = h.apply_full(&complete_effect(&r, op, Some(outcome(op, 1))));
    assert_eq!((a.out, a.reads), (rej(RejectReason::RunTerminal), 1));
    h.assert_rebuild_equal();
}

#[test]
fn complete_effect_blob_outcome_is_stored_as_a_blob_ref() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1, start(&rid(1)));
    let op = schedule(&mut h, &r, 2);
    // A > 256 KiB outcome travels as a blob ref; the test keeps the bytes (apply has no blob
    // store and never fetches them).
    let big = RecordedResult {
        codec_ver: 1,
        bytes: vec![0x5a; 300 * 1024],
    };
    let event_bytes = complete_effect_payload(op, &big, 1_000_000);
    assert!(event_bytes.len() > INLINE_PAYLOAD_LIMIT);
    let blob = PayloadRef::blob([0xb7; 32], event_bytes.len() as u64);
    let seq = h.run(&r).next_seq;
    let a = h.apply_full(&complete_effect(&r, op, Some(blob.clone())));
    assert_eq!((a.out, a.reads), (ApplyOutcome::Accepted, 2));
    let (kind, payload) = h.row_at(&r, seq);
    assert_eq!(kind, 65);
    assert_eq!(payload, blob, "the blob ref is journaled verbatim");
    // What a reader gets after resolving the ref decodes to the original event.
    assert_eq!(
        decoded(seq, kind, &event_bytes),
        EventBody::EffectCompleted(EffectCompleted { op, outcome: big })
    );
    h.assert_rebuild_equal();
}

// ---------------------------------------------------------------- cleanup (P-8)

#[test]
fn non_resumable_terminal_deletes_rows_with_one_extra_read() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1, start(&rid(1)));
    let a_op = schedule(&mut h, &r, 0);
    let b_op = schedule(&mut h, &r, 1);
    let other = running(&mut h, 2, 1, start(&rid(2)));
    let o_op = schedule(&mut h, &other, 0);
    assert_eq!(h.effects(&r).len(), 2);

    let a = h.apply_full(&append(
        &h,
        &r,
        1,
        vec![complete_run(TerminalStatus::Failed, false)],
    ));
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert_eq!(a.reads, 2, "record + 1 effects prefix scan (no waits)");
    assert!(h.effects(&r).is_empty());
    assert_eq!(h.effect(&r, a_op), None);
    assert_eq!(h.effect(&r, b_op), None);
    assert!(h.effect(&other, o_op).is_some(), "other runs untouched");
    h.assert_rebuild_equal();
}

#[test]
fn resumable_terminal_keeps_rows_and_resume_accepts_the_completion() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1, start(&rid(1)));
    let op = schedule(&mut h, &r, 0);

    // Executor-level failure, resumable: no effects scan, rows kept.
    let a = h.apply_full(&append(
        &h,
        &r,
        1,
        vec![complete_run(TerminalStatus::Failed, true)],
    ));
    assert_eq!((a.out, a.reads), (ApplyOutcome::Accepted, 1));
    assert!(h.effect(&r, op).is_some());
    assert_eq!(
        h.apply(&complete_effect(&r, op, Some(outcome(op, 1)))),
        rej(RejectReason::RunTerminal)
    );

    // Resume in place, then the old op's completion journals EffectCompleted.
    assert_eq!(h.apply(&retry(&r)), ApplyOutcome::Accepted);
    let seq = h.run(&r).next_seq;
    assert_eq!(
        h.apply(&complete_effect(&r, op, Some(outcome(op, 1)))),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.row_at(&r, seq).0, EventKind::EffectCompleted.code());
    assert_eq!(h.effect(&r, op), None);
    h.assert_rebuild_equal();
}

#[test]
fn terminate_keeps_rows_and_resumed_run_completes() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1, start(&rid(1)));
    let op = schedule(&mut h, &r, 1);
    assert_eq!(h.apply(&terminate(&r)), ApplyOutcome::Accepted);
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::Terminated);
    assert!(rec.resumable);
    assert!(h.effect(&r, op).is_some(), "Terminated is resumable (P-8)");
    assert_eq!(h.apply(&retry(&r)), ApplyOutcome::Accepted);
    assert_eq!(
        h.apply(&complete_effect(&r, op, Some(outcome(op, 2)))),
        ApplyOutcome::Accepted
    );
    h.assert_rebuild_equal();
}

#[test]
fn retention_expiry_deletes_effects_rows() {
    let mut h = H::new();
    let mut s = start(&rid(1));
    s.retention_ms = 1_000;
    let r = running(&mut h, 1, 1, s);
    let op = schedule(&mut h, &r, 0);
    assert_eq!(h.apply(&terminate(&r)), ApplyOutcome::Accepted);
    assert!(h.effect(&r, op).is_some());
    let rec = h.run(&r);
    let due = rec.completed_at_ms.expect("completed") + rec.retention_ms;
    let fire = Command::timer_fired(TimerFired {
        run_id: r.as_bytes().to_vec(),
        op: EXPIRE_OP,
        due_at_ms: due,
        fired_at_ms: due + 1,
    });
    assert_eq!(h.apply(&fire), ApplyOutcome::Accepted);
    assert!(h.get(Table::Runs, &run_key(&r)).is_none(), "run expired");
    assert_eq!(h.all_effects(), 0);
    h.assert_rebuild_equal();
}

// ---------------------------------------------------------------- leader wake (P-9)

#[test]
fn leader_announced_wakes_suspended_runs_with_effects() {
    let mut h = H::new();
    // Suspended run with two open effects: woken once.
    let sus = running(&mut h, 1, 1, start(&rid(1)));
    schedule(&mut h, &sus, 0);
    schedule(&mut h, &sus, 1);
    suspend(&mut h, &sus);
    // Running run with an effect: untouched.
    let run = running(&mut h, 2, 1, start(&rid(2)));
    schedule(&mut h, &run, 0);
    // Suspended run without effects: untouched.
    let idle = running(&mut h, 3, 1, start(&rid(3)));
    suspend(&mut h, &idle);
    // Resumable terminal run with an effect: untouched.
    let term = running(&mut h, 4, 1, start(&rid(4)));
    schedule(&mut h, &term, 0);
    assert_eq!(h.apply(&terminate(&term)), ApplyOutcome::Accepted);

    // A stale announcement does nothing.
    let a = h.apply_at(1, &Command::leader_announced(1, 1, 0));
    assert_eq!(a.out, rej(RejectReason::StaleEpoch { current: Epoch(1) }));
    assert_eq!((a.reads, a.writes), (0, 0));
    assert_eq!(h.run(&sus).status, RunStatus::Scheduled);

    let running_before = h.run(&run);
    let a = h.announce(2);
    assert_eq!(a.out, ApplyOutcome::Accepted);
    // 1 effects scan + 1 record read per distinct run with rows (sus, run, term).
    assert_eq!(a.reads, 1 + 3);
    let rec = h.run(&sus);
    assert_eq!(rec.status, RunStatus::Ready);
    assert!(matches!(
        a.actions.as_slice(),
        [Action::EnqueueReady { run, ready_at_ms, .. }] if *run == sus && *ready_at_ms == rec.created_at_ms
    ));
    let slot = rec.ready.expect("ready slot");
    assert!(
        h.get(
            Table::Ready,
            &ready_key("q", slot.priority, slot.ready_at_ms, &sus)
        )
        .is_some()
    );
    assert_eq!(h.run(&run), running_before);
    assert_eq!(h.run(&idle).status, RunStatus::Scheduled);
    assert_eq!(h.run(&term).status, RunStatus::Terminated);

    // A second leader change: the run is Ready now, nothing more.
    let a = h.announce(3);
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert!(a.actions.is_empty());
    h.assert_rebuild_equal();
}

#[test]
fn leader_announced_without_effects_is_a_pure_epoch_change() {
    // No effects rows: the accepted LeaderAnnounced reads the empty scan only and writes
    // nothing, so its state is exactly the pre-06-03 one (the cursor change is the commit's
    // meta update, as before).
    let mut h = H::new();
    let r = running(&mut h, 1, 2, start(&rid(1)));
    suspend(&mut h, &r);
    let a = h.announce(2);
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert_eq!((a.reads, a.writes), (1, 0));
    assert!(a.actions.is_empty());
    assert_eq!(h.run(&r).status, RunStatus::Scheduled);
    h.assert_rebuild_equal();
}

#[test]
fn leader_announced_does_not_wake_paused_runs() {
    let mut h = H::new();
    let r = running(&mut h, 1, 1, start(&rid(1)));
    schedule(&mut h, &r, 0);
    suspend(&mut h, &r);
    let mut rec = h.run(&r);
    rec.pause = Some(PauseMark {
        reason: 1,
        at_ms: 5,
        detail: "paused".into(),
    });
    h.put(vec![(Table::Runs, run_key(&r), values::encode(&rec))]);
    let a = h.announce(2);
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert_eq!((a.reads, a.writes), (2, 0), "pause wins");
    assert_eq!(h.run(&r).status, RunStatus::Scheduled);
}
