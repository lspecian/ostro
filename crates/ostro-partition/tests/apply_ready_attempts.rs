//! Pre-freeze apply changes of plan 05-22:
//!
//! - Phase 7 F3/F7: ready rows (`ReadyValue`), `query::ReadyEntryRow` and
//!   `Action::EnqueueReady` carry the run's workflow, tenant and first-activation flag
//!   (`current_attempt.is_none()` at enqueue time), so a rebuild over `ready` needs no
//!   run-record read.
//! - F9: `AttemptGranted{after_failure: true}` increments `RunRecord.attempt_failures`.
//! - F10: `EventsAppended` sets `RunRecord.status_seq` to the last `StatusPublished` event
//!   of the batch (from `JournalEvent.kind` only).
//! - PF-F1: `CompleteRun.parent_event` is forwarded verbatim into the outbox
//!   `ChildCompleted.event`.

use bytes::Bytes;
use ostro_partition::apply::{Action, ApplyOutcome, Cursor, apply_command};
use ostro_partition::query::{ReadyEntryRow, ready_entries};
use ostro_partition::store::keys::{outbox_key, ready_key, run_key, timer_key};
use ostro_partition::store::values::{self, ReadyValue, RunRecord, RunStatus};
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, ChildCompleted, CompleteRun, ConflictPolicy, Derived,
    EventsAppended, JournalEvent, OutboxMsg, ParentRef, ScheduleTimer, StartRun, TerminalStatus,
    TimerFired, derived, encode_command, outbox_msg,
};
use ostro_types::journal::EventKind;
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};
use prost::Message;

// ---------------------------------------------------------------- helpers

struct H {
    dir: tempfile::TempDir,
    store: PartitionStore,
    cur: Cursor,
}

fn open(dir: &tempfile::TempDir, p: u16) -> (PartitionStore, Cursor) {
    let (store, meta) = PartitionStore::open(
        dir.path(),
        PartitionId(p),
        PartitionCount::new(16).expect("non-zero"),
        &StoreConfig::default(),
    )
    .expect("open store");
    let cur = Cursor::from_meta(&meta);
    (store, cur)
}

impl H {
    fn new(p: u16) -> Self {
        let dir = ostro_testkit::fs::tempdir();
        let (store, cur) = open(&dir, p);
        H { dir, store, cur }
    }

    /// Closes the store and reopens it from disk (the cursor from the stored meta).
    fn reopen(self, p: u16) -> Self {
        let H { dir, store, .. } = self;
        drop(store);
        let (store, cur) = open(&dir, p);
        H { dir, store, cur }
    }

    /// Applies one command in its own committed batch: (outcome, actions, reads).
    fn apply_full(&mut self, c: &Command) -> (ApplyOutcome, Vec<Action>, u64) {
        let bytes = encode_command(c);
        let mut b = self.store.batch();
        let mut actions = Vec::new();
        let off = self.cur.next;
        let before = b.reads();
        let out = apply_command(&mut b, &mut self.cur, off, Epoch(1), &bytes, &mut actions)
            .expect("apply");
        let reads = b.reads() - before;
        let mu = self.cur.meta_update();
        b.commit(&mu).expect("commit");
        (out, actions, reads)
    }

    fn ok(&mut self, c: &Command) -> Vec<Action> {
        let (o, a, _) = self.apply_full(c);
        assert_eq!(o, ApplyOutcome::Accepted, "{c:?}");
        a
    }

    fn get(&self, t: Table, k: &[u8]) -> Option<Vec<u8>> {
        self.store.reader().get(t, k).expect("read")
    }

    fn run(&self, id: &RunId) -> RunRecord {
        values::decode(&self.get(Table::Runs, &run_key(id)).expect("run row")).expect("decode")
    }

    fn ready_value(&self, id: &RunId, priority: i32, at: u64) -> ReadyValue {
        let k = ready_key("q", priority, at, id);
        values::decode(&self.get(Table::Ready, &k).expect("ready row")).expect("decode")
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

fn start(id: &RunId) -> StartRun {
    StartRun {
        run_id: id.as_bytes().to_vec(),
        workflow: "wf".into(),
        workflow_version: 1,
        queue: "q".into(),
        input: inline(b"in"),
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        concurrency_key: "ck".into(),
        priority: 5,
        ready_at_ms: 100,
        requested_at_ms: 100,
        rng_seed: vec![7; 32],
        ..Default::default()
    }
}

fn la() -> Command {
    Command::leader_announced(1, 1, 0)
}

fn grant(id: &RunId, n: u32, after_failure: bool) -> Command {
    Command::attempt_granted(AttemptGranted {
        run_id: id.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n }),
        after_failure,
    })
}

fn ev(seq: u64, kind: u32) -> JournalEvent {
    JournalEvent {
        seq,
        kind,
        payload: inline(b"e"),
        ..Default::default()
    }
}

fn events(id: &RunId, n: u32, from: u64, evs: Vec<JournalEvent>, d: Vec<Derived>) -> Command {
    Command::events_appended(EventsAppended {
        run_id: id.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n }),
        from_seq: from,
        events: evs,
        derived: d,
    })
}

fn intent(i: derived::Intent) -> Derived {
    Derived { intent: Some(i) }
}

fn enqueue_ready_of(a: &[Action]) -> (String, Option<String>, bool) {
    match a {
        [
            Action::EnqueueReady {
                workflow,
                tenant,
                first_activation,
                ..
            },
        ] => (workflow.clone(), tenant.clone(), *first_activation),
        other => panic!("expected one EnqueueReady, got {other:?}"),
    }
}

const STATUS: u32 = 25;

// ---------------------------------------------------------------- F3 / F7

#[test]
fn status_published_kind_code_is_25() {
    assert_eq!(EventKind::StatusPublished.code(), STATUS);
}

#[test]
fn fresh_start_is_first_activation_with_workflow_and_tenant() {
    let mut h = H::new(0);
    h.ok(&la());
    let (a, b) = (rid(0, 1), rid(0, 2));
    let acts = h.ok(&Command::start_run(StartRun {
        tenant: "acme".into(),
        ..start(&a)
    }));
    assert_eq!(
        enqueue_ready_of(&acts),
        ("wf".into(), Some("acme".into()), true)
    );
    assert_eq!(
        h.ready_value(&a, 5, 100),
        ReadyValue {
            concurrency_key: "ck".into(),
            workflow: "wf".into(),
            tenant: Some("acme".into()),
            first: true,
        }
    );
    // No tenant -> None (empty StartRun.tenant means none).
    let acts = h.ok(&Command::start_run(StartRun {
        workflow: "other".into(),
        ..start(&b)
    }));
    assert_eq!(enqueue_ready_of(&acts), ("other".into(), None, true));
    let rv = h.ready_value(&b, 5, 100);
    assert_eq!(
        (rv.workflow.as_str(), rv.tenant, rv.first),
        ("other", None, true)
    );
}

#[test]
fn delayed_start_wakes_as_first_activation() {
    let mut h = H::new(0);
    h.ok(&la());
    let r = rid(0, 1);
    h.ok(&Command::start_run(StartRun {
        ready_at_ms: 5000,
        ..start(&r)
    }));
    let acts = h.ok(&Command::timer_fired(TimerFired {
        run_id: r.as_bytes().to_vec(),
        op: ostro_partition::apply::START_DELAY_OP,
        due_at_ms: 5000,
        fired_at_ms: 5001,
    }));
    assert_eq!(enqueue_ready_of(&acts), ("wf".into(), None, true));
    assert!(h.ready_value(&r, 5, 5000).first);
}

#[test]
fn requeue_after_grant_and_wake_after_suspend_are_not_first() {
    let mut h = H::new(0);
    h.ok(&la());
    let r = rid(0, 1);
    h.ok(&Command::start_run(StartRun {
        tenant: "t1".into(),
        ..start(&r)
    }));
    h.ok(&grant(&r, 1, false));

    // Executor requeue (EnqueueReady intent) after a grant: first = false.
    let rdy = intent(derived::Intent::EnqueueReady(
        ostro_types::command::EnqueueReady { ready_at_ms: 150 },
    ));
    let acts = h.ok(&events(&r, 1, 0, vec![ev(0, 16)], vec![rdy]));
    assert_eq!(
        enqueue_ready_of(&acts),
        ("wf".into(), Some("t1".into()), false)
    );
    assert!(!h.ready_value(&r, 5, 150).first);

    // Grant again, schedule a guest timer and Suspend; the timer wakes the run.
    h.ok(&grant(&r, 2, false));
    let sched = intent(derived::Intent::ScheduleTimer(ScheduleTimer {
        op: 7,
        due_at_ms: 1000,
    }));
    h.ok(&events(
        &r,
        2,
        1,
        vec![ev(1, 16)],
        vec![sched, Derived::suspend()],
    ));
    assert_eq!(h.run(&r).status, RunStatus::Scheduled);
    assert!(h.get(Table::Timers, &timer_key(1000, &r, 7)).is_some());
    let acts = h.ok(&Command::timer_fired(TimerFired {
        run_id: r.as_bytes().to_vec(),
        op: 7,
        due_at_ms: 1000,
        fired_at_ms: 1001,
    }));
    assert_eq!(
        enqueue_ready_of(&acts),
        ("wf".into(), Some("t1".into()), false)
    );
    let rv = h.ready_value(&r, 5, 1001);
    assert_eq!(
        (rv.workflow.as_str(), rv.tenant.as_deref(), rv.first),
        ("wf", Some("t1"), false)
    );
}

#[test]
fn ready_entries_carry_workflow_tenant_first_without_record_reads() {
    let mut h = H::new(0);
    h.ok(&la());
    let (a, b) = (rid(0, 1), rid(0, 2));
    h.ok(&Command::start_run(StartRun {
        tenant: "acme".into(),
        ..start(&a)
    }));
    h.ok(&Command::start_run(StartRun {
        workflow: "wf-b".into(),
        ..start(&b)
    }));
    h.ok(&grant(&b, 1, false));
    let rdy = intent(derived::Intent::EnqueueReady(
        ostro_types::command::EnqueueReady { ready_at_ms: 100 },
    ));
    h.ok(&events(&b, 1, 0, vec![], vec![rdy]));

    // Delete both run records: the ready scan must still yield every field, so it
    // provably reads nothing but the `ready` rows.
    let mut batch = h.store.batch();
    batch.remove(Table::Runs, run_key(&a));
    batch.remove(Table::Runs, run_key(&b));
    let mu = h.cur.meta_update();
    batch.commit(&mu).expect("commit");
    assert!(h.get(Table::Runs, &run_key(&a)).is_none());

    let h = h.reopen(0);
    let rows = ready_entries(&h.store.reader(), 100).expect("scan");
    let row = |run, workflow: &str, tenant: Option<&str>, first| ReadyEntryRow {
        queue: "q".into(),
        priority: 5,
        ready_at_ms: 100,
        run,
        concurrency_key: "ck".into(),
        workflow: workflow.into(),
        tenant: tenant.map(Into::into),
        first_activation: first,
    };
    let mut want = vec![
        row(a, "wf", Some("acme"), true),
        row(b, "wf-b", None, false),
    ];
    want.sort_by_key(|r| r.run);
    assert_eq!(rows, want);
}

// ---------------------------------------------------------------- F9

#[test]
fn after_failure_grants_count_durably() {
    let mut h = H::new(0);
    h.ok(&la());
    let r = rid(0, 1);
    h.ok(&Command::start_run(start(&r)));
    // A normal grant leaves the counter alone.
    let (o, _, reads) = h.apply_full(&grant(&r, 1, false));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert_eq!(reads, 1, "no read added to AttemptGranted");
    assert_eq!(h.run(&r).attempt_failures, 0);
    for n in 2..=4 {
        let (o, _, reads) = h.apply_full(&grant(&r, n, true));
        assert_eq!(o, ApplyOutcome::Accepted);
        assert_eq!(reads, 1);
    }
    assert_eq!(h.run(&r).attempt_failures, 3);
    // A rejected (fenced) after-failure grant writes nothing.
    let (o, _, _) = h.apply_full(&grant(&r, 4, true));
    assert!(!o.is_accepted());
    let mut h = h.reopen(0);
    assert_eq!(h.run(&r).attempt_failures, 3, "durable across a reopen");
    h.ok(&grant(&r, 5, false));
    assert_eq!(h.run(&r).attempt_failures, 3);
}

// ---------------------------------------------------------------- F10

#[test]
fn status_seq_points_at_the_last_status_published_event() {
    let mut h = H::new(0);
    h.ok(&la());
    let r = rid(0, 1);
    h.ok(&Command::start_run(start(&r)));
    h.ok(&grant(&r, 1, false));
    assert_eq!(h.run(&r).status_seq, None);

    // No StatusPublished: unchanged (None).
    h.ok(&events(&r, 1, 0, vec![ev(0, 16), ev(1, 17)], vec![]));
    assert_eq!(h.run(&r).status_seq, None);

    // Two in one batch: the later one wins.
    let (o, _, reads) = h.apply_full(&events(
        &r,
        1,
        2,
        vec![ev(2, STATUS), ev(3, 16), ev(4, STATUS), ev(5, 16)],
        vec![],
    ));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert_eq!(reads, 1, "no read added to EventsAppended");
    assert_eq!(h.run(&r).status_seq, Some(4));

    // A batch without one leaves it unchanged.
    h.ok(&events(&r, 1, 6, vec![ev(6, 16)], vec![]));
    assert_eq!(h.run(&r).status_seq, Some(4));

    // A rejected batch (seq mismatch) carrying one changes nothing.
    let (o, _, _) = h.apply_full(&events(&r, 1, 3, vec![ev(3, STATUS)], vec![]));
    assert!(!o.is_accepted());
    assert_eq!(h.run(&r).status_seq, Some(4));

    let h = h.reopen(0);
    assert_eq!(h.run(&r).status_seq, Some(4));
}

// ---------------------------------------------------------------- PF-F1

/// Child `rid(2, 1)` of parent `rid(0, 1)` op 3, started and granted on partition 2.
fn child_running() -> (H, RunId) {
    let mut h = H::new(2);
    h.ok(&la());
    let child = rid(2, 1);
    let mut s = start(&child);
    s.parent = Some(ParentRef {
        run_id: rid(0, 1).as_bytes().to_vec(),
        op: 3,
    });
    h.ok(&Command::start_run(s));
    h.ok(&grant(&child, 1, false));
    (h, child)
}

fn complete_with(parent_event: Option<PayloadRef>) -> Derived {
    intent(derived::Intent::CompleteRun(CompleteRun {
        status: TerminalStatus::Completed as i32,
        result: inline(b"out"),
        completed_at_ms: 9,
        resumable: false,
        parent_event,
    }))
}

fn child_completed_row(h: &H) -> ChildCompleted {
    let v = h
        .get(Table::Outbox, &outbox_key(0, 1))
        .expect("ChildCompleted row");
    match OutboxMsg::decode(&*v).expect("prost outbox").kind {
        Some(outbox_msg::Kind::ChildCompleted(c)) => c,
        other => panic!("expected ChildCompleted, got {other:?}"),
    }
}

#[test]
fn parent_event_blob_is_forwarded_verbatim() {
    let (mut h, child) = child_running();
    // A 1 MiB blob-backed encoded ChildCompleted event.
    let ev_ref = PayloadRef::blob([0xab; 32], 1 << 20);
    let want = ev_ref.encode_to_vec();
    h.ok(&events(
        &child,
        1,
        0,
        vec![],
        vec![complete_with(Some(ev_ref.clone()))],
    ));
    let c = child_completed_row(&h);
    assert_eq!(c.event.as_ref(), Some(&ev_ref));
    assert_eq!(
        c.event.expect("event").encode_to_vec(),
        want,
        "byte-identical"
    );
    assert_eq!(c.child_run, child.as_bytes().to_vec());
    assert_eq!(c.op, 3);
    assert_eq!(c.result, inline(b"out"));
}

#[test]
fn parent_event_inline_is_forwarded_verbatim_and_none_stays_none() {
    let (mut h, child) = child_running();
    // Arbitrary bytes: apply never decodes them.
    let raw = Bytes::from(vec![0xff, 0x00, 0x13, 0x37]);
    let ev_ref = PayloadRef::inline(raw.clone());
    h.ok(&events(
        &child,
        1,
        0,
        vec![],
        vec![complete_with(Some(ev_ref.clone()))],
    ));
    assert_eq!(child_completed_row(&h).event, Some(ev_ref));

    let (mut h, child) = child_running();
    h.ok(&events(&child, 1, 0, vec![], vec![complete_with(None)]));
    assert_eq!(child_completed_row(&h).event, None);
}
