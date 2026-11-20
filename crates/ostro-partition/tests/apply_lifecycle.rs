//! Run lifecycle apply (plan 05-09): run timeout and schedule-to-start (D2), TerminateRun
//! (D3), RetryRun resume-in-place (D1), continue-as-new (D10, PF-F2) and the inbound
//! successor start. Every terminal transition here emits `Action::RunCompleted` (W3).

use bytes::Bytes;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, apply_command,
};
use ostro_partition::query::journal_rows;
use ostro_partition::store::keys::{
    children_key, decode_timer_key, payload_key, ready_prefix, run_key, running_key, timer_key,
};
use ostro_partition::store::values::{self, RunRecord, RunStatus};
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, CompleteRun, ConflictPolicy, Derived, EXPIRE_OP, EnqueueReady,
    EventsAppended, InboundBatch, InboundMsg, JournalEvent, MAX_REASON_LEN, OutboxMsg, ParentRef,
    RUN_TIMEOUT_OP, RetryRun, SCHEDULE_TO_START_OP, SendMessage, StartRun, TerminalStatus,
    TerminateRun, TimerFired, derived, encode_command, outbox_msg,
};
use ostro_types::journal::{EventBody, EventKind, decode_event};
use ostro_types::payload::{INLINE_PAYLOAD_LIMIT, payload_ref};
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};
use prost::Message;

// ---------------------------------------------------------------- harness

struct H {
    _dir: tempfile::TempDir,
    store: PartitionStore,
    cur: Cursor,
}

impl H {
    fn new() -> Self {
        let dir = ostro_testkit::fs::tempdir();
        let (store, meta) = PartitionStore::open(
            dir.path(),
            PartitionId(2),
            PartitionCount::new(16).expect("non-zero"),
            &StoreConfig::default(),
        )
        .expect("open store");
        let cur = Cursor::from_meta(&meta);
        let mut h = H {
            _dir: dir,
            store,
            cur,
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

    fn put(&mut self, t: Table, k: Vec<u8>, v: Vec<u8>) {
        let mut b = self.store.batch();
        b.insert(t, k, v);
        let mu = self.cur.meta_update();
        b.commit(&mu).expect("commit");
    }

    fn get(&self, t: Table, k: &[u8]) -> Option<Vec<u8>> {
        self.store.reader().get(t, k).expect("read")
    }

    fn try_run(&self, id: &RunId) -> Option<RunRecord> {
        self.get(Table::Runs, &run_key(id))
            .map(|v| values::decode(&v).expect("decode"))
    }

    fn run(&self, id: &RunId) -> RunRecord {
        self.try_run(id).expect("run row")
    }

    fn rows(&self, t: Table) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.store
            .reader()
            .prefix(t, &[], usize::MAX)
            .expect("rows")
    }

    /// `(due, op)` of every timer row of `run`.
    fn timers(&self, run: &RunId) -> Vec<(u64, u64)> {
        self.rows(Table::Timers)
            .into_iter()
            .filter_map(|(k, _)| decode_timer_key(&k))
            .filter(|(_, r, _)| r == run)
            .map(|(d, _, op)| (d, op))
            .collect()
    }

    fn outbox(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.rows(Table::Outbox)
    }

    /// Outbox messages added since `before`, in key order.
    fn outbox_since(&self, before: &[(Vec<u8>, Vec<u8>)]) -> Vec<outbox_msg::Kind> {
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

    /// `(seq, kind)` of every journal row of `run`.
    fn journal(&self, run: &RunId) -> Vec<(u64, u32, PayloadRef)> {
        journal_rows(&self.store.reader(), run, 0, usize::MAX).expect("journal")
    }
}

fn rid(n: u8) -> RunId {
    rid_on(2, n)
}

fn rid_on(p: u16, n: u8) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&p.to_be_bytes());
    b[17] = n;
    RunId::from_bytes(&b).expect("18 bytes")
}

fn start(id: &RunId) -> StartRun {
    StartRun {
        run_id: id.as_bytes().to_vec(),
        workflow: "acme/wf".into(),
        workflow_version: 1,
        queue: "acme/q".into(),
        input: Some(PayloadRef::inline(Bytes::from_static(b"in"))),
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        ready_at_ms: 100,
        requested_at_ms: 100,
        rng_seed: vec![7; 32],
        ..Default::default()
    }
}

fn grant(r: &RunId, n: u32, after_failure: bool) -> Command {
    Command::attempt_granted(AttemptGranted {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n }),
        after_failure,
    })
}

/// Starts `s` and grants `{1,1}`.
fn running_with(h: &mut H, s: StartRun) -> RunId {
    let r = RunId::from_bytes(&s.run_id).expect("id");
    h.ok(&Command::start_run(s));
    h.ok(&grant(&r, 1, false));
    r
}

fn running(h: &mut H, n: u8) -> RunId {
    running_with(h, start(&rid(n)))
}

fn ev(seq: u64, kind: u32) -> JournalEvent {
    JournalEvent {
        seq,
        kind,
        payload: Some(PayloadRef::inline(Bytes::from_static(b"e"))),
    }
}

fn append_with(
    r: &RunId,
    n: u32,
    from: u64,
    events: Vec<JournalEvent>,
    d: Vec<Derived>,
) -> Command {
    Command::events_appended(EventsAppended {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n }),
        from_seq: from,
        events,
        derived: d,
    })
}

fn append(r: &RunId, from: u64, count: u64, d: Vec<Derived>) -> Command {
    append_with(
        r,
        1,
        from,
        (from..from + count).map(|s| ev(s, 16)).collect(),
        d,
    )
}

fn complete_with(status: TerminalStatus, resumable: bool, result: Option<PayloadRef>) -> Derived {
    Derived::new(derived::Intent::CompleteRun(CompleteRun {
        status: status as i32,
        result,
        completed_at_ms: 900,
        resumable,
        parent_event: None,
    }))
}

fn complete(status: TerminalStatus, resumable: bool) -> Derived {
    complete_with(status, resumable, None)
}

fn fire(r: &RunId, op: u64, due: u64) -> Command {
    Command::timer_fired(TimerFired {
        run_id: r.as_bytes().to_vec(),
        op,
        due_at_ms: due,
        fired_at_ms: due + 1,
    })
}

fn terminate(r: &RunId, reason: &str, at: u64) -> Command {
    Command::terminate_run(TerminateRun {
        run_id: r.as_bytes().to_vec(),
        reason: reason.into(),
        terminated_at_ms: at,
    })
}

fn retry(r: &RunId, at: u64) -> Command {
    Command::retry_run(RetryRun {
        run_id: r.as_bytes().to_vec(),
        requested_at_ms: at,
    })
}

fn rejected(r: RejectReason) -> ApplyOutcome {
    ApplyOutcome::Rejected(r)
}

fn bad_intent() -> ApplyOutcome {
    rejected(RejectReason::Invalid(InvalidCode::BadIntent))
}

fn run_completed(actions: &[Action]) -> Vec<(RunId, RunStatus)> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::RunCompleted { run, status } => Some((*run, *status)),
            _ => None,
        })
        .collect()
}

fn inline_bytes(p: &PayloadRef) -> Vec<u8> {
    match &p.kind {
        Some(payload_ref::Kind::Inline(b)) => b.to_vec(),
        other => panic!("not inline: {other:?}"),
    }
}

/// The decoded body of the last journal event of `run`.
fn last_event(h: &H, run: &RunId) -> (u64, EventBody) {
    let rows = h.journal(run);
    let (seq, kind, p) = rows.last().expect("journal not empty");
    let e = decode_event(*seq, *kind, &inline_bytes(p)).expect("decode");
    (*seq, e.body)
}

fn terminated_reason(body: &EventBody) -> &str {
    match body {
        EventBody::RunTerminated(t) => &t.reason,
        other => panic!("not RunTerminated: {other:?}"),
    }
}

fn no_ready_or_running(h: &H, r: &RunId) {
    assert!(h.get(Table::Running, &running_key(r)).is_none());
    assert!(
        h.store
            .reader()
            .prefix(Table::Ready, &ready_prefix("acme/q"), usize::MAX)
            .expect("ready")
            .iter()
            .all(|(k, _)| !k.ends_with(r.as_bytes())),
        "no ready row"
    );
    assert!(h.run(r).ready.is_none());
}

// ---------------------------------------------------------------- run timeout (D2)

#[test]
fn start_arms_the_run_timeout_and_completion_removes_it() {
    let mut h = H::new();
    let r = rid(1);
    let mut s = start(&r);
    s.run_timeout_ms = 1_000;
    let actions = h.ok(&Command::start_run(s));
    assert!(actions.contains(&Action::ScheduleTimer {
        due_at_ms: 1_100,
        run: r,
        op: RUN_TIMEOUT_OP
    }));
    assert_eq!(h.timers(&r), vec![(1_100, RUN_TIMEOUT_OP)]);
    h.ok(&grant(&r, 1, false));
    let actions = h.ok(&append(
        &r,
        0,
        1,
        vec![complete(TerminalStatus::Completed, false)],
    ));
    assert!(actions.contains(&Action::CancelTimer {
        due_at_ms: 1_100,
        run: r,
        op: RUN_TIMEOUT_OP
    }));
    assert!(
        h.timers(&r).is_empty(),
        "completion removed the timeout row"
    );
    assert_eq!(run_completed(&actions), vec![(r, RunStatus::Completed)]);
}

#[test]
fn run_timeout_on_a_sleeping_run_is_a_hard_stop() {
    let mut h = H::new();
    let mut s = start(&rid(1));
    s.run_timeout_ms = 1_000;
    let r = running_with(&mut h, s);
    // Two events, then Suspend: Scheduled (sleeping).
    h.ok(&append(&r, 0, 2, vec![Derived::suspend()]));
    assert_eq!(h.run(&r).status, RunStatus::Scheduled);

    let (out, actions, reads) = h.apply_full(&fire(&r, RUN_TIMEOUT_OP, 1_100));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(reads, 2, "timer row + record; no children, no waits");
    assert_eq!(run_completed(&actions), vec![(r, RunStatus::TimedOut)]);
    assert!(
        !actions.iter().any(|a| matches!(
            a,
            Action::EnqueueReady { .. } | Action::InputAppended { .. }
        )),
        "the marker wakes nothing"
    );
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::TimedOut);
    assert_eq!(rec.terminal_reason, "run_timeout");
    assert_eq!(rec.completed_at_ms, Some(1_101));
    assert!(rec.resumable, "D1: TimedOut is resumable");
    assert_eq!(rec.next_seq, 3);
    let (seq, body) = last_event(&h, &r);
    assert_eq!(seq, 2);
    assert_eq!(terminated_reason(&body), "run_timeout");
    no_ready_or_running(&h, &r);
    assert!(h.timers(&r).is_empty());
}

#[test]
fn run_timeout_on_a_running_run_emits_run_completed_and_fences_the_attempt() {
    let mut h = H::new();
    let mut s = start(&rid(1));
    s.run_timeout_ms = 500;
    let r = running_with(&mut h, s);
    h.ok(&append(&r, 0, 1, vec![]));
    assert!(h.get(Table::Running, &running_key(&r)).is_some());
    let actions = h.ok(&fire(&r, RUN_TIMEOUT_OP, 600));
    assert_eq!(run_completed(&actions), vec![(r, RunStatus::TimedOut)]);
    no_ready_or_running(&h, &r);
    // The executor's next append is rejected by the existing terminal check.
    assert_eq!(
        h.apply(&append(&r, 1, 1, vec![])),
        rejected(RejectReason::RunTerminal)
    );
    // A second fire finds no row.
    assert_eq!(
        h.apply(&fire(&r, RUN_TIMEOUT_OP, 600)),
        ApplyOutcome::Duplicate { existing: None }
    );
}

#[test]
fn stale_run_timeout_rows_only_lose_their_row() {
    let mut h = H::new();
    let mut s = start(&rid(1));
    s.run_timeout_ms = 500;
    let r = running_with(&mut h, s);
    // A row whose due time is not the record's deadline.
    h.put(
        Table::Timers,
        timer_key(777, &r, RUN_TIMEOUT_OP),
        Vec::new(),
    );
    let actions = h.ok(&fire(&r, RUN_TIMEOUT_OP, 777));
    assert!(actions.is_empty());
    assert_eq!(h.run(&r).status, RunStatus::Running);
}

// ---------------------------------------------------------------- schedule-to-start (D2)

#[test]
fn schedule_to_start_fires_before_the_first_grant() {
    let mut h = H::new();
    let r = rid(1);
    let mut s = start(&r);
    s.schedule_to_start_ms = 300;
    s.ready_at_ms = 0; // in the past: the clock starts at requested_at_ms
    let actions = h.ok(&Command::start_run(s));
    assert!(actions.contains(&Action::ScheduleTimer {
        due_at_ms: 400,
        run: r,
        op: SCHEDULE_TO_START_OP
    }));
    assert_eq!(h.run(&r).s2s_due_ms, 400);
    let (out, actions, _) = h.apply_full(&fire(&r, SCHEDULE_TO_START_OP, 400));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(run_completed(&actions), vec![(r, RunStatus::TimedOut)]);
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::TimedOut);
    assert_eq!(rec.terminal_reason, "schedule_to_start");
    assert_eq!(rec.next_seq, 0);
    assert!(
        h.journal(&r).is_empty(),
        "never started: no marker at seq 0"
    );
    no_ready_or_running(&h, &r);
    assert!(h.timers(&r).is_empty());
}

#[test]
fn first_grant_deletes_the_schedule_to_start_row_by_s2s_due_ms() {
    let mut h = H::new();
    let r = rid(1);
    let mut s = start(&r);
    s.schedule_to_start_ms = 300;
    s.ready_at_ms = 250; // delayed start: the clock starts at ready_at_ms
    s.requested_at_ms = 100;
    h.ok(&Command::start_run(s));
    assert_eq!(h.run(&r).s2s_due_ms, 550);
    // Wake via the start delay, then grant: the ready row is gone at grant (W13), the S2S
    // row is still found by s2s_due_ms.
    h.ok(&fire(&r, ostro_partition::apply::START_DELAY_OP, 250));
    let actions = h.ok(&grant(&r, 1, false));
    assert!(actions.contains(&Action::CancelTimer {
        due_at_ms: 550,
        run: r,
        op: SCHEDULE_TO_START_OP
    }));
    assert_eq!(h.run(&r).s2s_due_ms, 0);
    assert!(h.timers(&r).is_empty());
    assert_eq!(
        h.apply(&fire(&r, SCHEDULE_TO_START_OP, 550)),
        ApplyOutcome::Duplicate { existing: None }
    );
    // Even a leftover row after the grant is a no-op.
    h.put(
        Table::Timers,
        timer_key(550, &r, SCHEDULE_TO_START_OP),
        Vec::new(),
    );
    assert!(h.ok(&fire(&r, SCHEDULE_TO_START_OP, 550)).is_empty());
    assert_eq!(h.run(&r).status, RunStatus::Running);
    // Later grants do not touch timers.
    h.ok(&grant(&r, 2, true));
}

#[test]
fn terminate_before_the_grant_removes_the_schedule_to_start_row() {
    let mut h = H::new();
    let r = rid(1);
    let mut s = start(&r);
    s.schedule_to_start_ms = 300;
    s.run_timeout_ms = 5_000;
    h.ok(&Command::start_run(s));
    assert_eq!(h.timers(&r).len(), 2);
    h.ok(&terminate(&r, "ops", 150));
    assert!(h.timers(&r).is_empty(), "both timeout rows deleted by key");
    assert_eq!(h.run(&r).s2s_due_ms, 0);
}

// ---------------------------------------------------------------- TerminateRun (D3)

#[test]
fn terminate_before_start_journals_nothing() {
    let mut h = H::new();
    let r = rid(1);
    h.ok(&Command::start_run(start(&r)));
    assert!(h.run(&r).ready.is_some());
    let (out, actions, reads) = h.apply_full(&terminate(&r, "operator", 150));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(reads, 1, "the record only");
    assert_eq!(run_completed(&actions), vec![(r, RunStatus::Terminated)]);
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::Terminated);
    assert_eq!(rec.terminal_reason, "operator");
    assert_eq!(rec.completed_at_ms, Some(150));
    assert!(rec.resumable);
    assert!(h.journal(&r).is_empty(), "no RunTerminated at seq 0");
    no_ready_or_running(&h, &r);
}

#[test]
fn terminate_mid_run_journals_run_terminated_last() {
    let mut h = H::new();
    let r = running(&mut h, 1);
    h.ok(&append(&r, 0, 3, vec![]));
    let actions = h.ok(&terminate(&r, "bad deploy", 400));
    assert_eq!(run_completed(&actions), vec![(r, RunStatus::Terminated)]);
    let (seq, body) = last_event(&h, &r);
    assert_eq!(seq, 3);
    assert_eq!(terminated_reason(&body), "bad deploy");
    assert_eq!(h.run(&r).next_seq, 4);
    no_ready_or_running(&h, &r);
    // Appends after the stop are rejected by the existing terminal check.
    assert_eq!(
        h.apply(&append(&r, 4, 1, vec![])),
        rejected(RejectReason::RunTerminal)
    );
    assert_eq!(
        h.apply(&terminate(&r, "again", 500)),
        rejected(RejectReason::RunTerminal)
    );
    assert_eq!(h.journal(&r).len(), 4, "nothing more journaled");
}

#[test]
fn terminate_rejections() {
    let mut h = H::new();
    assert_eq!(
        h.apply(&terminate(&rid(9), "x", 1)),
        rejected(RejectReason::RunNotFound)
    );
    assert_eq!(
        h.apply(&terminate(&rid_on(3, 1), "x", 1)),
        rejected(RejectReason::WrongPartition)
    );
    let r = running(&mut h, 1);
    let long = "r".repeat(MAX_REASON_LEN + 1);
    assert_eq!(
        h.apply(&terminate(&r, &long, 1)),
        rejected(RejectReason::Invalid(InvalidCode::FieldTooLong))
    );
    assert_eq!(h.run(&r).status, RunStatus::Running);
}

#[test]
fn terminate_notifies_the_parent_and_cancels_children_gracefully() {
    let mut h = H::new();
    let parent = rid(1);
    let child = rid(2);
    let grandchild = rid_on(5, 3);
    // Parent run (on this partition) with an op 4 child.
    let p = running(&mut h, 1);
    assert_eq!(p, parent);
    let mut cs = start(&child);
    cs.parent = Some(ParentRef {
        run_id: parent.as_bytes().to_vec(),
        op: 4,
    });
    running_with(&mut h, cs);
    // The child starts a grandchild on partition 5.
    let gc = Derived::new(derived::Intent::SendMessage(SendMessage {
        msg: Some(OutboxMsg::start_child(start(&grandchild))),
    }));
    h.ok(&append(&child, 0, 1, vec![gc]));
    assert!(
        h.get(Table::Children, &children_key(&child, &grandchild))
            .is_some()
    );
    let before = h.outbox();

    let (out, actions, reads) = h.apply_full(&terminate(&child, "stop", 300));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(reads, 2, "record + children scan");
    assert_eq!(
        run_completed(&actions),
        vec![(child, RunStatus::Terminated)]
    );
    let new = h.outbox_since(&before);
    assert_eq!(new.len(), 2, "{new:?}");
    match &new[0] {
        outbox_msg::Kind::ChildCompleted(c) => {
            assert_eq!(c.parent_run, parent.as_bytes().to_vec());
            assert_eq!(c.child_run, child.as_bytes().to_vec());
            assert_eq!(c.op, 4);
            assert_eq!(c.status, TerminalStatus::Terminated as i32);
            assert!(c.result.is_none() && c.event.is_none());
        }
        other => panic!("expected ChildCompleted, got {other:?}"),
    }
    match &new[1] {
        outbox_msg::Kind::CancelRun(c) => {
            assert_eq!(c.target_run, grandchild.as_bytes().to_vec());
            assert_eq!(c.reason, "stop");
        }
        other => panic!("expected CancelRun, got {other:?}"),
    }
}

// ---------------------------------------------------------------- RetryRun (D1)

#[test]
fn retry_resumes_a_timed_out_run_in_place() {
    let mut h = H::new();
    let mut s = start(&rid(1));
    s.run_timeout_ms = 1_000;
    let r = running_with(&mut h, s);
    h.ok(&grant(&r, 2, true));
    assert_eq!(h.run(&r).attempt_failures, 1);
    h.ok(&append_with(&r, 2, 0, vec![ev(0, 16), ev(1, 16)], vec![]));
    h.ok(&fire(&r, RUN_TIMEOUT_OP, 1_100));
    let journal_before = h.journal(&r);

    let (out, actions, reads) = h.apply_full(&retry(&r, 2_000));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(reads, 1);
    assert!(actions.iter().any(|a| matches!(
        a,
        Action::EnqueueReady { run, ready_at_ms: 2_000, first_activation: false, .. } if *run == r
    )));
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::Ready);
    assert_eq!(rec.resumed_count, 1);
    assert_eq!(rec.attempt_failures, 0, "F9: fresh attempt-failure budget");
    assert!(rec.terminal_reason.is_empty());
    assert_eq!(rec.completed_at_ms, None);
    assert!(!rec.resumable);
    assert_eq!(rec.current_attempt, Some((1, 2)), "fencing token kept");
    assert_eq!(h.journal(&r), journal_before, "journal untouched");
    assert!(
        h.timers(&r).is_empty(),
        "no new RUN_TIMEOUT row for a resumed run"
    );
    // The resumed run continues with a higher token and can complete; no timeout row is
    // re-armed or deleted for it.
    h.ok(&grant(&r, 3, false));
    let actions = h.ok(&append_with(
        &r,
        3,
        3,
        vec![ev(3, 16)],
        vec![complete(TerminalStatus::Completed, false)],
    ));
    assert!(!actions.iter().any(|a| matches!(
        a,
        Action::CancelTimer {
            op: RUN_TIMEOUT_OP,
            ..
        }
    )));
    assert_eq!(h.run(&r).status, RunStatus::Completed);
    assert_eq!(h.run(&r).resumed_count, 1);
}

#[test]
fn retry_after_terminate_removes_the_expiry_row() {
    let mut h = H::new();
    let mut s = start(&rid(1));
    s.retention_ms = 5_000;
    let r = running_with(&mut h, s);
    let actions = h.ok(&terminate(&r, "ops", 200));
    assert!(actions.contains(&Action::ScheduleTimer {
        due_at_ms: 5_200,
        run: r,
        op: EXPIRE_OP
    }));
    assert_eq!(h.timers(&r), vec![(5_200, EXPIRE_OP)]);
    let actions = h.ok(&retry(&r, 300));
    assert!(actions.contains(&Action::CancelTimer {
        due_at_ms: 5_200,
        run: r,
        op: EXPIRE_OP
    }));
    assert!(h.timers(&r).is_empty());
    assert_eq!(h.run(&r).status, RunStatus::Ready);
    // Terminated a second time: a new expiry at the new completion time.
    h.ok(&terminate(&r, "again", 1_000));
    assert_eq!(h.timers(&r), vec![(6_000, EXPIRE_OP)]);
    assert_eq!(h.run(&r).resumed_count, 1);
}

#[test]
fn retry_rejections() {
    let mut h = H::new();
    assert_eq!(
        h.apply(&retry(&rid(9), 1)),
        rejected(RejectReason::RunNotFound)
    );
    // Live run.
    let live = running(&mut h, 1);
    assert_eq!(h.apply(&retry(&live, 1)), bad_intent());
    // Completed: 409 run_completed.
    let done = running(&mut h, 2);
    h.ok(&append(
        &done,
        0,
        1,
        vec![complete(TerminalStatus::Completed, false)],
    ));
    assert_eq!(
        h.apply(&retry(&done, 1)),
        rejected(RejectReason::RunCompleted)
    );
    // Guest failure (not resumable): restart-as-new is the server's job.
    let failed = running(&mut h, 3);
    h.ok(&append(
        &failed,
        0,
        1,
        vec![complete(TerminalStatus::Failed, false)],
    ));
    assert_eq!(h.apply(&retry(&failed, 1)), bad_intent());
    // Executor-level failure (resumable): resumes.
    let crashed = running(&mut h, 4);
    h.ok(&append(
        &crashed,
        0,
        1,
        vec![complete(TerminalStatus::Failed, true)],
    ));
    assert_eq!(h.apply(&retry(&crashed, 1)), ApplyOutcome::Accepted);
    assert_eq!(h.run(&crashed).status, RunStatus::Ready);
}

// ---------------------------------------------------------------- continue-as-new (D10, PF-F2)

fn successor_msg(s: StartRun) -> Derived {
    Derived::new(derived::Intent::SendMessage(SendMessage {
        msg: Some(OutboxMsg::start_run(s)),
    }))
}

fn successor_start(id: &RunId) -> StartRun {
    let mut s = start(id);
    s.input = Some(PayloadRef::inline(Bytes::from_static(b"next")));
    s.requested_at_ms = 950;
    s.ready_at_ms = 950;
    s
}

#[test]
fn continue_as_new_hands_off_without_child_completed() {
    let mut h = H::new();
    let parent = rid(1);
    let r = rid(2);
    let succ = rid_on(7, 3);
    let mut s = start(&r);
    s.parent = Some(ParentRef {
        run_id: parent.as_bytes().to_vec(),
        op: 9,
    });
    s.run_timeout_ms = 10_000;
    s.retention_ms = 1_000;
    running_with(&mut h, s);
    let before = h.outbox();
    let status = EventKind::StatusPublished.code();
    let cmd = append_with(
        &r,
        1,
        0,
        vec![ev(0, 16), ev(1, status), ev(2, 16)],
        vec![
            successor_msg(successor_start(&succ)),
            complete_with(
                TerminalStatus::ContinuedAsNew,
                true,
                Some(PayloadRef::inline(Bytes::from_static(b"res"))),
            ),
        ],
    );
    let (out, actions, reads) = h.apply_full(&cmd);
    assert_eq!(out, ApplyOutcome::Accepted);
    // Record read + the effects prefix scan of the non-resumable terminal (06-03, P-8;
    // docs/storage-format.md "Phase 6 additions").
    assert_eq!(reads, 2, "EventsAppended's record read + effects cleanup scan");
    assert_eq!(
        run_completed(&actions),
        vec![(r, RunStatus::ContinuedAsNew)]
    );
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::ContinuedAsNew);
    assert_eq!(rec.continued_to, Some(succ));
    assert_eq!(rec.status_seq, Some(1), "05-22 F10 on the CAN path too");
    assert_eq!(rec.next_seq, 3);
    assert!(!rec.resumable, "continue-as-new is never resumable");
    assert_eq!(rec.completed_at_ms, Some(900));
    let res = h
        .get(Table::Payloads, &payload_key(&r, 1))
        .expect("result stored");
    assert_eq!(
        inline_bytes(&PayloadRef::decode(res.as_slice()).expect("payload")),
        b"res"
    );
    // Run-timeout row removed, expiry armed.
    assert_eq!(h.timers(&r), vec![(1_900, EXPIRE_OP)]);
    no_ready_or_running(&h, &r);
    // Exactly one new outbox row: the successor start with lineage filled in, no
    // ChildCompleted for the parent.
    let new = h.outbox_since(&before);
    assert_eq!(new.len(), 1, "{new:?}");
    match &new[0] {
        outbox_msg::Kind::StartRun(s) => {
            assert_eq!(s.run_id, succ.as_bytes().to_vec());
            assert_eq!(s.continued_from, r.as_bytes().to_vec());
            assert_eq!(
                s.parent,
                Some(ParentRef {
                    run_id: parent.as_bytes().to_vec(),
                    op: 9
                }),
                "PF-F2: the successor inherits the parent link"
            );
        }
        other => panic!("expected StartRun, got {other:?}"),
    }
    // The predecessor is terminal: retry is refused (not resumable).
    assert_eq!(h.apply(&retry(&r, 1)), bad_intent());
}

#[test]
fn continue_as_new_validation_is_all_or_nothing() {
    let can = || complete(TerminalStatus::ContinuedAsNew, false);
    let succ = rid_on(7, 3);
    let mut foreign = successor_start(&succ);
    foreign.continued_from = rid(99).as_bytes().to_vec();
    let mut other_parent = successor_start(&succ);
    other_parent.parent = Some(ParentRef {
        run_id: rid(50).as_bytes().to_vec(),
        op: 1,
    });
    let mut self_succ = successor_start(&succ);
    self_succ.run_id = rid(1).as_bytes().to_vec();
    let mut big = successor_start(&succ);
    big.input = Some(PayloadRef::inline(Bytes::from(vec![
        0u8;
        INLINE_PAYLOAD_LIMIT
            + 1
    ])));
    let cases: Vec<(Vec<Derived>, ApplyOutcome)> = vec![
        (vec![can()], bad_intent()),
        (
            vec![
                successor_msg(successor_start(&succ)),
                successor_msg(successor_start(&rid_on(7, 4))),
                can(),
            ],
            bad_intent(),
        ),
        (vec![successor_msg(foreign), can()], bad_intent()),
        (vec![successor_msg(other_parent), can()], bad_intent()),
        (vec![successor_msg(self_succ), can()], bad_intent()),
        (
            vec![
                Derived::new(derived::Intent::EnqueueReady(EnqueueReady {
                    ready_at_ms: 5,
                })),
                successor_msg(successor_start(&succ)),
                can(),
            ],
            bad_intent(),
        ),
        (
            vec![
                complete(TerminalStatus::Failed, false),
                successor_msg(successor_start(&succ)),
                can(),
            ],
            bad_intent(),
        ),
        (
            vec![successor_msg(big), can()],
            rejected(RejectReason::PayloadTooLarge),
        ),
    ];
    for (i, (d, want)) in cases.into_iter().enumerate() {
        let mut h = H::new();
        let r = running(&mut h, 1);
        let before = h.rows(Table::Outbox);
        assert_eq!(h.apply(&append(&r, 0, 1, d)), want, "case {i}");
        let rec = h.run(&r);
        assert_eq!(rec.status, RunStatus::Running, "case {i}");
        assert_eq!(rec.next_seq, 0, "case {i}");
        assert_eq!(h.rows(Table::Outbox), before, "case {i}");
    }
}

#[test]
fn inbound_successor_start_stores_lineage_and_a_fresh_deadline() {
    let mut h = H::new();
    let pred = rid_on(7, 1);
    let parent = rid_on(3, 2);
    let succ = rid(5);
    let mut s = successor_start(&succ);
    s.continued_from = pred.as_bytes().to_vec();
    s.parent = Some(ParentRef {
        run_id: parent.as_bytes().to_vec(),
        op: 6,
    });
    s.run_timeout_ms = 2_000;
    let batch = |seq: u64, s: StartRun| {
        Command::inbound_batch(InboundBatch {
            src: 7,
            msgs: vec![InboundMsg {
                seq,
                msg: Some(OutboxMsg::start_run(s)),
            }],
        })
    };
    let actions = h.ok(&batch(1, s.clone()));
    assert!(actions.contains(&Action::ScheduleTimer {
        due_at_ms: 2_950,
        run: succ,
        op: RUN_TIMEOUT_OP
    }));
    let rec = h.run(&succ);
    assert_eq!(rec.continued_from, Some(pred));
    assert_eq!(rec.parent, Some((*parent.as_bytes(), 6)));
    assert_eq!(rec.status, RunStatus::Ready);
    // A redelivery (same seq) is skipped; a resend under a new seq finds the run row.
    assert_eq!(
        h.apply(&batch(1, s.clone())),
        ApplyOutcome::Duplicate { existing: None }
    );
    h.ok(&batch(2, s));
    assert_eq!(h.run(&succ), rec);
}

#[test]
fn every_lifecycle_terminal_transition_emits_run_completed() {
    // W3: terminate, run timeout, schedule-to-start, continue-as-new.
    let mut seen = Vec::new();
    {
        let mut h = H::new();
        let r = running(&mut h, 1);
        seen.extend(run_completed(&h.ok(&terminate(&r, "x", 5))));
    }
    {
        let mut h = H::new();
        let mut s = start(&rid(1));
        s.run_timeout_ms = 10;
        let r = running_with(&mut h, s);
        seen.extend(run_completed(&h.ok(&fire(&r, RUN_TIMEOUT_OP, 110))));
    }
    {
        let mut h = H::new();
        let mut s = start(&rid(1));
        s.schedule_to_start_ms = 10;
        let r = rid(1);
        h.ok(&Command::start_run(s));
        seen.extend(run_completed(&h.ok(&fire(&r, SCHEDULE_TO_START_OP, 110))));
    }
    {
        let mut h = H::new();
        let r = running(&mut h, 1);
        let cmd = append(
            &r,
            0,
            1,
            vec![
                successor_msg(successor_start(&rid_on(4, 2))),
                complete(TerminalStatus::ContinuedAsNew, false),
            ],
        );
        seen.extend(run_completed(&h.ok(&cmd)));
    }
    let statuses: Vec<_> = seen.into_iter().map(|(_, s)| s).collect();
    assert_eq!(
        statuses,
        vec![
            RunStatus::Terminated,
            RunStatus::TimedOut,
            RunStatus::TimedOut,
            RunStatus::ContinuedAsNew
        ]
    );
}
