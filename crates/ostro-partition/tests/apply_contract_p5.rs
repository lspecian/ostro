//! Phase 5 contract seams in apply (plan 05-01): the real reserved-timer-op rule, the
//! temporary rejections of not-yet-implemented commands/intents/statuses, and stable
//! `RunStatus` postcard indices.
//!
//! The "rejected until implemented" tests are tripwires: the plan that implements an item
//! (05-02, 05-08, 05-09) replaces the matching assertion with its real behaviour.

use bytes::Bytes;
use ostro_partition::apply::{ApplyOutcome, Cursor, InvalidCode, RejectReason, apply_command};
use ostro_partition::store::keys::run_key;
use ostro_partition::store::values::{self, RunRecord, RunStatus};
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    self, AttemptGranted, AttemptTokenMsg, CancelTimer, CompleteRun, ConflictPolicy, Derived,
    EventsAppended, InboundBatch, InboundMsg, JournalEvent, OutboxMsg, ScheduleTimer, SendMessage,
    StartRun, TerminalStatus, derived, encode_command,
};
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};

struct H {
    _dir: tempfile::TempDir,
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
        H {
            _dir: dir,
            store,
            cur,
        }
    }

    fn apply(&mut self, c: &Command) -> ApplyOutcome {
        let mut b = self.store.batch();
        let mut actions = Vec::new();
        let off = self.cur.next;
        let out = apply_command(
            &mut b,
            &mut self.cur,
            off,
            Epoch(1),
            &encode_command(c),
            &mut actions,
        )
        .expect("apply");
        let mu = self.cur.meta_update();
        b.commit(&mu).expect("commit");
        out
    }

    fn run(&self, id: &RunId) -> Option<RunRecord> {
        self.store
            .reader()
            .get(Table::Runs, &run_key(id))
            .expect("read")
            .map(|v| values::decode(&v).expect("decode run"))
    }
}

fn rid(p: u16, n: u8) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&p.to_be_bytes());
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

/// Epoch 1 announced, run `r` started and granted `{1,1}`.
fn running(p: u16, n: u8) -> (H, RunId) {
    let mut h = H::new(p);
    let r = rid(p, n);
    assert_eq!(
        h.apply(&Command::leader_announced(1, 1, 0)),
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.apply(&Command::start_run(start(&r))),
        ApplyOutcome::Accepted
    );
    let grant = Command::attempt_granted(AttemptGranted {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n: 1 }),
        after_failure: false,
    });
    assert_eq!(h.apply(&grant), ApplyOutcome::Accepted);
    (h, r)
}

fn append(r: &RunId, derived: Vec<Derived>) -> Command {
    Command::events_appended(EventsAppended {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n: 1 }),
        from_seq: 0,
        events: vec![JournalEvent {
            seq: 0,
            kind: 1,
            payload: Some(PayloadRef::inline(Bytes::from_static(b"e"))),
            ..Default::default()
        }],
        derived,
    })
}

fn bad_intent() -> ApplyOutcome {
    ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::BadIntent))
}

#[test]
fn guest_timer_intents_in_the_reserved_op_range_are_rejected() {
    for op in [
        command::START_DELAY_OP,
        command::RUN_TIMEOUT_OP,
        command::SCHEDULE_TO_START_OP,
        command::EXPIRE_OP,
        command::RESERVED_OP_MIN,
    ] {
        let (mut h, r) = running(3, 1);
        let sched = Derived::new(derived::Intent::ScheduleTimer(ScheduleTimer {
            op,
            due_at_ms: 500,
        }));
        assert_eq!(h.apply(&append(&r, vec![sched])), bad_intent(), "{op}");
        let cancel = Derived::new(derived::Intent::CancelTimer(CancelTimer {
            op,
            due_at_ms: 500,
        }));
        assert_eq!(h.apply(&append(&r, vec![cancel])), bad_intent(), "{op}");
        assert_eq!(h.run(&r).expect("run").next_seq, 0, "nothing written");
    }
    // The highest guest op below the range is still a normal timer.
    let (mut h, r) = running(3, 2);
    let ok = Derived::new(derived::Intent::ScheduleTimer(ScheduleTimer {
        op: command::RESERVED_OP_MIN - 1,
        due_at_ms: 500,
    }));
    assert_eq!(h.apply(&append(&r, vec![ok])), ApplyOutcome::Accepted);
}

// Every Phase 5 command is implemented now: SignalRun, RequestCancel, CompleteToken by 05-08
// (tests/apply_waits.rs), TerminateRun and RetryRun by 05-09 (tests/apply_lifecycle.rs). The
// former `phase5_commands_are_rejected_until_implemented` tripwire is retired.

#[test]
fn phase5_intents_are_rejected_until_implemented() {
    let complete = |status: TerminalStatus| {
        Derived::new(derived::Intent::CompleteRun(CompleteRun {
            status: status as i32,
            result: None,
            completed_at_ms: 9,
            resumable: true,
            parent_event: None,
        }))
    };
    // (05-08 implemented RegisterWait / CancelWait: tests/apply_waits.rs.)
    let cases = [
        // Terminated/TimedOut: apply-side only (05-09), never from an executor.
        complete(TerminalStatus::Terminated),
        complete(TerminalStatus::TimedOut),
        // 05-09 (with the outbox successor start).
        complete(TerminalStatus::ContinuedAsNew),
        Derived::new(derived::Intent::SendMessage(SendMessage {
            msg: Some(OutboxMsg::start_run(start(&rid(5, 9)))),
        })),
    ];
    for (i, d) in cases.into_iter().enumerate() {
        let (mut h, r) = running(4, 2);
        assert_eq!(h.apply(&append(&r, vec![d])), bad_intent(), "case {i}");
        assert_eq!(h.run(&r).expect("run").status, RunStatus::Running);
    }
    // The existing statuses still complete, resumable or not.
    let (mut h, r) = running(4, 3);
    assert_eq!(
        h.apply(&append(&r, vec![complete(TerminalStatus::Failed)])),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.run(&r).expect("run").status, RunStatus::Failed);
}

#[test]
fn inbound_start_run_starts_the_successor() {
    // 05-09: `lifecycle::inbound_start_run` (details in tests/apply_lifecycle.rs).
    let mut h = H::new(6);
    assert_eq!(
        h.apply(&Command::leader_announced(1, 1, 0)),
        ApplyOutcome::Accepted
    );
    let successor = rid(6, 7);
    let batch = Command::inbound_batch(InboundBatch {
        src: 2,
        msgs: vec![InboundMsg {
            seq: 1,
            msg: Some(OutboxMsg::start_run(start(&successor))),
        }],
    });
    assert_eq!(h.apply(&batch), ApplyOutcome::Accepted);
    assert!(h.run(&successor).is_some(), "successor started");
    assert_eq!(h.cur.inbound_seen.get(&2), Some(&1), "message consumed");
}

#[test]
fn run_status_postcard_indices_are_stable() {
    // VALUE_FORMAT byte, then the postcard variant index. Phase 5 statuses are appended.
    let all = [
        (RunStatus::Scheduled, 0u8, false),
        (RunStatus::Ready, 1, false),
        (RunStatus::Running, 2, false),
        (RunStatus::Completed, 3, true),
        (RunStatus::Failed, 4, true),
        (RunStatus::Cancelled, 5, true),
        (RunStatus::Terminated, 6, true),
        (RunStatus::TimedOut, 7, true),
        (RunStatus::ContinuedAsNew, 8, true),
    ];
    for (st, idx, terminal) in all {
        let enc = values::encode(&st);
        assert_eq!(&enc[1..], &[idx], "{st:?}");
        assert_eq!(values::decode::<RunStatus>(&enc).expect("decode"), st);
        assert_eq!(st.is_terminal(), terminal, "{st:?}");
    }
}
