//! Shared single-partition harness of `children_close.rs` and `children_chain.rs` (plan
//! 09-08). Every run lives on partition [`P`] (count 16), so outbox messages can be fed
//! back as inbound messages from [`SRC`].
#![allow(dead_code)]

use bytes::Bytes;
use ostro_partition::apply::{Action, ApplyOutcome, Cursor, apply_command};
use ostro_partition::query::journal_rows;
use ostro_partition::store::keys::{decode_timer_key, run_key};
use ostro_partition::store::values::{self, RunRecord};
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, CancelRun, ChildCompleted, CompleteRun, ConflictPolicy,
    Derived, EXPIRE_OP, EventsAppended, InboundBatch, InboundMsg, JournalEvent, OutboxMsg,
    ParentRef, RetryRun, SendMessage, StartChild, StartRun, TerminalStatus, TerminateRun,
    TimerFired, derived, encode_command, outbox_msg,
};
use ostro_types::ids::child_run_uuid;
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};
use prost::Message;

pub const P: u16 = 2;
pub const SRC: u32 = 5;
/// `completed_at_ms` of every executor completion built here.
pub const DONE_AT: u64 = 900;
pub const RETENTION: u64 = 1_000;

pub type Rows = Vec<(Vec<u8>, Vec<u8>)>;

pub struct H {
    _dir: tempfile::TempDir,
    pub store: PartitionStore,
    pub cur: Cursor,
    /// Next inbound seq from `SRC`.
    seq: u64,
}

impl H {
    pub fn new() -> Self {
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
    pub fn apply_full(&mut self, c: &Command) -> (ApplyOutcome, Vec<Action>, u64) {
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

    pub fn apply(&mut self, c: &Command) -> ApplyOutcome {
        self.apply_full(c).0
    }

    pub fn ok(&mut self, c: &Command) -> Vec<Action> {
        let (out, actions, _) = self.apply_full(c);
        assert_eq!(out, ApplyOutcome::Accepted, "{}", c.kind_name());
        actions
    }

    pub fn get(&self, t: Table, k: &[u8]) -> Option<Vec<u8>> {
        self.store.reader().get(t, k).expect("read")
    }

    pub fn try_run(&self, id: &RunId) -> Option<RunRecord> {
        self.get(Table::Runs, &run_key(id))
            .map(|v| values::decode(&v).expect("decode"))
    }

    pub fn run(&self, id: &RunId) -> RunRecord {
        self.try_run(id).expect("run row")
    }

    pub fn rows(&self, t: Table) -> Rows {
        self.store
            .reader()
            .prefix(t, &[], usize::MAX)
            .expect("rows")
    }

    pub fn journal(&self, run: &RunId) -> Vec<(u64, u32, PayloadRef)> {
        journal_rows(&self.store.reader(), run, 0, usize::MAX).expect("journal")
    }

    /// `(due, op)` of every timer row of `run`.
    pub fn timers(&self, run: &RunId) -> Vec<(u64, u64)> {
        self.rows(Table::Timers)
            .into_iter()
            .filter_map(|(k, _)| decode_timer_key(&k))
            .filter(|(_, r, _)| r == run)
            .map(|(d, _, op)| (d, op))
            .collect()
    }

    pub fn outbox(&self) -> Rows {
        self.rows(Table::Outbox)
    }

    /// Outbox messages added since `before`, in key order.
    pub fn outbox_since(&self, before: &Rows) -> Vec<outbox_msg::Kind> {
        self.outbox()
            .into_iter()
            .filter(|row| !before.contains(row))
            .map(|(_, v)| {
                OutboxMsg::decode(v.as_slice())
                    .expect("outbox msg")
                    .kind
                    .expect("kind")
            })
            .collect()
    }

    /// Delivers `kind` from `SRC` at the next seq: (outcome, actions, reads).
    pub fn deliver(&mut self, kind: outbox_msg::Kind) -> (ApplyOutcome, Vec<Action>, u64) {
        self.seq += 1;
        let c = Command::inbound_batch(InboundBatch {
            src: SRC,
            msgs: vec![InboundMsg {
                seq: self.seq,
                msg: Some(OutboxMsg { kind: Some(kind) }),
            }],
        });
        self.apply_full(&c)
    }

    /// Delivers `kind` and returns the outbox messages it produced.
    pub fn deliver_out(&mut self, kind: outbox_msg::Kind) -> Vec<outbox_msg::Kind> {
        let before = self.outbox();
        let (o, _, _) = self.deliver(kind);
        assert_eq!(o, ApplyOutcome::Accepted);
        self.outbox_since(&before)
    }
}

pub fn rid(n: u8) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&P.to_be_bytes());
    b[17] = n;
    RunId::from_bytes(&b).expect("18 bytes")
}

/// The deterministic (D-P9-7) child id of `(parent, op)`, placed on this partition.
pub fn child_id(parent: &RunId, op: u64) -> RunId {
    RunId::new(PartitionId(P), child_run_uuid(parent, op))
}

pub fn start(id: &RunId) -> StartRun {
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
        retention_ms: RETENTION,
        ..Default::default()
    }
}

/// The child start of `parent`'s op `op` (`detached` = no ParentRef).
pub fn child_start(parent: &RunId, op: u64, child: &RunId, detached: bool) -> StartRun {
    let mut s = start(child);
    s.parent = (!detached).then(|| ParentRef {
        run_id: parent.as_bytes().to_vec(),
        op,
    });
    s
}

pub fn grant(r: &RunId, n: u32) -> Command {
    Command::attempt_granted(AttemptGranted {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n }),
        after_failure: false,
    })
}

pub fn ev(seq: u64) -> JournalEvent {
    JournalEvent {
        seq,
        kind: 16,
        payload: Some(PayloadRef::inline(Bytes::from_static(b"e"))),
    }
}

pub fn append_n(r: &RunId, n: u32, from: u64, count: u64, d: Vec<Derived>) -> Command {
    Command::events_appended(EventsAppended {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n }),
        from_seq: from,
        events: (from..from + count).map(ev).collect(),
        derived: d,
    })
}

pub fn append(r: &RunId, from: u64, count: u64, d: Vec<Derived>) -> Command {
    append_n(r, 1, from, count, d)
}

pub fn send(kind: outbox_msg::Kind) -> Derived {
    Derived::new(derived::Intent::SendMessage(SendMessage {
        msg: Some(OutboxMsg { kind: Some(kind) }),
    }))
}

pub fn start_child_kind(s: StartRun) -> outbox_msg::Kind {
    outbox_msg::Kind::StartChild(StartChild { start: Some(s) })
}

pub fn complete(status: TerminalStatus) -> Derived {
    Derived::new(derived::Intent::CompleteRun(CompleteRun {
        status: status as i32,
        result: None,
        completed_at_ms: DONE_AT,
        resumable: false,
        parent_event: None,
    }))
}

/// A continue-as-new tail: successor start + `CompleteRun{ContinuedAsNew}`.
pub fn can_tail(succ: &RunId) -> Vec<Derived> {
    let mut s = start(succ);
    s.requested_at_ms = 950;
    s.ready_at_ms = 950;
    vec![
        send(outbox_msg::Kind::StartRun(s)),
        complete(TerminalStatus::ContinuedAsNew),
    ]
}

pub fn terminate(r: &RunId, reason: &str, at: u64) -> Command {
    Command::terminate_run(TerminateRun {
        run_id: r.as_bytes().to_vec(),
        reason: reason.into(),
        terminated_at_ms: at,
    })
}

pub fn retry(r: &RunId, at: u64) -> Command {
    Command::retry_run(RetryRun {
        run_id: r.as_bytes().to_vec(),
        requested_at_ms: at,
    })
}

pub fn fire(r: &RunId, op: u64, due: u64) -> Command {
    Command::timer_fired(TimerFired {
        run_id: r.as_bytes().to_vec(),
        op,
        due_at_ms: due,
        fired_at_ms: due + 1,
    })
}

pub fn expire(r: &RunId, due: u64) -> Command {
    fire(r, EXPIRE_OP, due)
}

pub fn cancel_kind(target: &RunId, reason: &str) -> outbox_msg::Kind {
    outbox_msg::Kind::CancelRun(CancelRun {
        target_run: target.as_bytes().to_vec(),
        reason: reason.into(),
    })
}

pub fn completed_kind(
    parent: &RunId,
    child: &RunId,
    op: u64,
    status: TerminalStatus,
    event: Option<PayloadRef>,
) -> outbox_msg::Kind {
    outbox_msg::Kind::ChildCompleted(ChildCompleted {
        parent_run: parent.as_bytes().to_vec(),
        child_run: child.as_bytes().to_vec(),
        op,
        status: status as i32,
        result: None,
        event,
    })
}

/// Starts `s` and grants `{1,1}`.
pub fn running_with(h: &mut H, s: StartRun) -> RunId {
    let r = RunId::from_bytes(&s.run_id).expect("id");
    h.ok(&Command::start_run(s));
    h.ok(&grant(&r, 1));
    r
}

/// A running parent `p` (journal `0..3`) that started one marked attached child per op in
/// `ops` (each `< 3`; child id = [`child_id`]) in one flush. Returns the child ids.
pub fn parent_with_children(h: &mut H, s: StartRun, ops: &[u64]) -> Vec<RunId> {
    let p = running_with(h, s);
    let n = 3;
    assert!(ops.iter().all(|&op| op < n));
    let mut d = Vec::new();
    let mut kids = Vec::new();
    for &op in ops {
        let c = child_id(&p, op);
        d.push(send(start_child_kind(child_start(&p, op, &c, false))));
        d.push(Derived::child_journal(&c));
        kids.push(c);
    }
    h.ok(&append(&p, 0, n, d));
    assert_eq!(h.run(&p).children, ops.len() as u32);
    kids
}

/// The CancelRun targets and reasons among `msgs`.
pub fn cancels(msgs: &[outbox_msg::Kind]) -> Vec<(RunId, String)> {
    msgs.iter()
        .filter_map(|m| match m {
            outbox_msg::Kind::CancelRun(c) => Some((
                RunId::from_bytes(&c.target_run).expect("target"),
                c.reason.clone(),
            )),
            _ => None,
        })
        .collect()
}

pub fn status_of(h: &H, r: &RunId) -> ostro_partition::store::values::RunStatus {
    h.run(r).status
}
