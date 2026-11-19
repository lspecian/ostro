//! Timer inputs go to the journal (plan 04-13; DISP-01, WF-07). A `TimerFired` for a run's
//! timer op is journaled by apply as a `TimerFired` event at `rec.next_seq`
//! (`ts_nanos = fired_at_ms * 1e6`) and bumps `next_seq`; no inbox row is written. The run is
//! woken by status: Scheduled -> Ready (`EnqueueReady`), Running -> `InputAppended{run, seq}`,
//! Ready -> unchanged. The start-delay timer keeps its Phase 2 behavior. Because `next_seq`
//! moves, a `Suspend` built before the input was seen fails its `from_seq` compare-and-set
//! (no lost wake-up).

use bytes::Bytes;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, RejectReason, START_DELAY_OP, apply_command,
};
use ostro_partition::query::journal_rows;
use ostro_partition::store::keys::{event_key, ready_key, run_key, timer_key};
use ostro_partition::store::values::{self, RunRecord, RunStatus};
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, CompleteRun, ConflictPolicy, Derived, EnqueueReady,
    EventsAppended, JournalEvent, ScheduleTimer, StartRun, TerminalStatus, TimerFired, derived,
    encode_command,
};
use ostro_types::journal::{self, EventBody, decode_event};
use ostro_types::payload::payload_ref;
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};

// ---------------------------------------------------------------- helpers

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
            PartitionId(0),
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

    fn apply(&mut self, c: &Command) -> ApplyOutcome {
        self.apply_full(c).0
    }

    fn get(&self, t: Table, k: &[u8]) -> Option<Vec<u8>> {
        self.store.reader().get(t, k).expect("read")
    }

    fn run(&self, id: &RunId) -> RunRecord {
        values::decode(&self.get(Table::Runs, &run_key(id)).expect("run row")).expect("decode")
    }

    fn rows(&self, t: Table, id: &RunId) -> usize {
        self.store
            .reader()
            .prefix(t, id.as_bytes(), usize::MAX)
            .expect("scan")
            .len()
    }

    /// Decoded journal of `id` from `from` on.
    fn journal(&self, id: &RunId, from: u64) -> Vec<journal::Event> {
        journal_rows(&self.store.reader(), id, from, usize::MAX)
            .expect("journal rows")
            .into_iter()
            .map(|(seq, kind, p)| {
                let Some(payload_ref::Kind::Inline(bytes)) = p.kind else {
                    panic!("event {seq} payload is not inline");
                };
                decode_event(seq, kind, &bytes).expect("decode event")
            })
            .collect()
    }

    /// Every row except `meta` (whose cursor moves with each applied command).
    fn data(&self) -> Vec<ostro_partition::store::Rows> {
        ostro_partition::TABLES
            .into_iter()
            .filter(|t| !matches!(t, Table::Meta))
            .map(|t| {
                self.store
                    .reader()
                    .prefix(t, &[], usize::MAX)
                    .expect("scan")
            })
            .collect()
    }

    fn digest(&self) -> [u8; 32] {
        self.store.digest().expect("digest")
    }
}

const R: [u8; 18] = [0, 0, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 1];

fn rid() -> RunId {
    RunId::from_bytes(&R).expect("18 bytes")
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
        ready_at_ms: 100,
        requested_at_ms: 100,
        rng_seed: vec![7; 32],
        ..Default::default()
    }
}

fn grant(id: &RunId, n: u32) -> Command {
    Command::attempt_granted(AttemptGranted {
        run_id: id.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n }),
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

fn events(id: &RunId, from: u64, evs: Vec<JournalEvent>, derived: Vec<Derived>) -> Command {
    Command::events_appended(EventsAppended {
        run_id: id.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n: 1 }),
        from_seq: from,
        events: evs,
        derived,
    })
}

fn intent(i: derived::Intent) -> Derived {
    Derived { intent: Some(i) }
}

fn sched(op: u64, due: u64) -> Derived {
    intent(derived::Intent::ScheduleTimer(ScheduleTimer {
        op,
        due_at_ms: due,
    }))
}

fn enqueue(at: u64) -> Derived {
    intent(derived::Intent::EnqueueReady(EnqueueReady {
        ready_at_ms: at,
    }))
}

fn complete() -> Derived {
    intent(derived::Intent::CompleteRun(CompleteRun {
        status: TerminalStatus::Completed as i32,
        result: inline(b"out"),
        completed_at_ms: 500,
        resumable: false,
        parent_event: None,
    }))
}

fn fired(id: &RunId, op: u64, due: u64, at: u64) -> Command {
    Command::timer_fired(TimerFired {
        run_id: id.as_bytes().to_vec(),
        op,
        due_at_ms: due,
        fired_at_ms: at,
    })
}

/// Epoch 1 announced, run started (ready at 100), granted `{1,1}`, then one flush of event 0
/// that schedules timer op 7 due at 1000 plus `extra` intents.
fn with_timer(extra: Vec<Derived>) -> (H, RunId) {
    let mut h = H::new();
    let r = rid();
    assert_eq!(
        h.apply(&Command::leader_announced(1, 1, 0)),
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.apply(&Command::start_run(start(&r))),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.apply(&grant(&r, 1)), ApplyOutcome::Accepted);
    let mut d = vec![sched(7, 1000)];
    d.extend(extra);
    assert_eq!(
        h.apply(&events(&r, 0, vec![ev(0)], d)),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.run(&r).next_seq, 1);
    (h, r)
}

/// The journaled timer input at `seq`.
fn timer_event(seq: u64, op: u64, fired_at_ms: u64) -> journal::Event {
    journal::Event {
        seq,
        ts_nanos: fired_at_ms * 1_000_000,
        body: EventBody::TimerFired(journal::TimerFired { op }),
    }
}

// ---------------------------------------------------------------- wake paths

#[test]
fn scheduled_run_gets_journal_event_and_becomes_ready() {
    let (mut h, r) = with_timer(vec![Derived::suspend()]);
    assert_eq!(h.run(&r).status, RunStatus::Scheduled);

    let (o, a, reads) = h.apply_full(&fired(&r, 7, 1000, 1000));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(reads <= 2, "TimerFired reads: {reads}");
    assert!(h.get(Table::Timers, &timer_key(1000, &r, 7)).is_none());

    assert_eq!(h.journal(&r, 1), vec![timer_event(1, 7, 1000)]);
    assert_eq!(h.journal(&r, 1)[0].ts_nanos, 1_000_000_000);
    let rec = h.run(&r);
    assert_eq!(rec.next_seq, 2);
    assert_eq!(rec.status, RunStatus::Ready);
    assert_eq!(rec.inbox_next, 0, "no inbox row for timers");
    assert_eq!(h.rows(Table::Inbox, &r), 0);
    assert_eq!(a.len(), 1, "{a:?}");
    assert!(
        matches!(&a[0], Action::EnqueueReady { run, ready_at_ms: 1000, .. } if *run == r),
        "{a:?}"
    );
    assert!(h.get(Table::Running, &run_key(&r)).is_none());
}

#[test]
fn running_run_gets_journal_event_and_input_appended() {
    let (mut h, r) = with_timer(vec![]);
    let running_before = h.get(Table::Running, &run_key(&r));
    assert!(running_before.is_some());

    let (o, a, reads) = h.apply_full(&fired(&r, 7, 1000, 1003));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(reads <= 2, "TimerFired reads: {reads}");
    assert_eq!(a, vec![Action::InputAppended { run: r, seq: 1 }]);
    assert_eq!(h.journal(&r, 1), vec![timer_event(1, 7, 1003)]);
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::Running);
    assert_eq!(rec.next_seq, 2);
    assert_eq!(rec.inbox_next, 0);
    assert_eq!(h.rows(Table::Inbox, &r), 0);
    assert_eq!(
        h.get(Table::Running, &run_key(&r)),
        running_before,
        "running row untouched"
    );
    assert_eq!(h.rows(Table::Ready, &r), 0);

    // The activation that received the input continues from seq 2.
    assert_eq!(
        h.apply(&events(&r, 2, vec![ev(2)], vec![])),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.run(&r).next_seq, 3);
}

#[test]
fn ready_run_gets_journal_event_only() {
    let (mut h, r) = with_timer(vec![enqueue(200)]);
    assert_eq!(h.run(&r).status, RunStatus::Ready);
    let ready = h.run(&r).ready.expect("ready slot");
    let rk = ready_key("q", ready.priority, ready.ready_at_ms, &r);
    assert!(h.get(Table::Ready, &rk).is_some());

    let (o, a, _) = h.apply_full(&fired(&r, 7, 1000, 1000));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(a.is_empty(), "{a:?}");
    assert_eq!(h.journal(&r, 1), vec![timer_event(1, 7, 1000)]);
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::Ready);
    assert_eq!(rec.next_seq, 2);
    assert_eq!(rec.ready, Some(ready));
    assert!(h.get(Table::Ready, &rk).is_some(), "ready row kept");
}

#[test]
fn terminal_run_and_duplicate_keep_phase2_semantics() {
    let (mut h, r) = with_timer(vec![complete()]);
    assert_eq!(h.run(&r).status, RunStatus::Completed);

    let before = h.run(&r);
    let (o, a, _) = h.apply_full(&fired(&r, 7, 1000, 1000));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(a.is_empty());
    assert!(h.get(Table::Timers, &timer_key(1000, &r, 7)).is_none());
    assert_eq!(h.run(&r), before, "terminal run unchanged");
    assert!(h.get(Table::Events, &event_key(&r, 1)).is_none());

    // Re-fire: no timer row any more.
    let d = h.data();
    let (o, a, reads) = h.apply_full(&fired(&r, 7, 1000, 1500));
    assert_eq!(o, ApplyOutcome::Duplicate { existing: None });
    assert!(a.is_empty());
    assert_eq!(reads, 1);
    assert_eq!(h.data(), d);
}

#[test]
fn duplicate_fire_on_live_run_journals_once() {
    let (mut h, r) = with_timer(vec![]);
    assert_eq!(h.apply(&fired(&r, 7, 1000, 1000)), ApplyOutcome::Accepted);
    let d = h.data();
    assert_eq!(
        h.apply(&fired(&r, 7, 1000, 1200)),
        ApplyOutcome::Duplicate { existing: None }
    );
    assert_eq!(h.data(), d);
    assert_eq!(h.journal(&r, 1).len(), 1);
    assert_eq!(h.run(&r).next_seq, 2);
}

#[test]
fn start_delay_timer_does_not_journal() {
    let mut h = H::new();
    let r = rid();
    assert_eq!(
        h.apply(&Command::leader_announced(1, 1, 0)),
        ApplyOutcome::Accepted
    );
    let s = StartRun {
        ready_at_ms: 10_000,
        ..start(&r)
    };
    assert_eq!(h.apply(&Command::start_run(s)), ApplyOutcome::Accepted);
    assert_eq!(h.run(&r).status, RunStatus::Scheduled);

    let (o, a, _) = h.apply_full(&fired(&r, START_DELAY_OP, 10_000, 10_004));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(
        matches!(&a[..], [Action::EnqueueReady { run, ready_at_ms: 10_000, .. }] if *run == r),
        "{a:?}"
    );
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::Ready);
    assert_eq!(rec.next_seq, 0);
    assert_eq!(h.rows(Table::Events, &r), 0);
    assert_eq!(h.rows(Table::Inbox, &r), 0);
}

// ---------------------------------------------------------------- lost wake-up

/// The executor flushed up to seq N (next_seq = N), the timer journals seq N, then the
/// executor's Suspend built at from_seq N must be rejected: the run stays Running and the
/// activation re-runs with the input. A Suspend that accounts for the input succeeds.
#[test]
fn input_between_flush_and_suspend_fails_the_suspend() {
    let (mut h, r) = with_timer(vec![]);
    // A second flush: next_seq = 2 = N.
    assert_eq!(
        h.apply(&events(&r, 1, vec![ev(1)], vec![])),
        ApplyOutcome::Accepted
    );
    let n = h.run(&r).next_seq;
    assert_eq!(n, 2);

    let (o, a, _) = h.apply_full(&fired(&r, 7, 1000, 1000));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert_eq!(a, vec![Action::InputAppended { run: r, seq: n }]);
    assert_eq!(h.journal(&r, n), vec![timer_event(n, 7, 1000)]);

    let d = h.data();
    assert_eq!(
        h.apply(&events(&r, n, vec![], vec![Derived::suspend()])),
        ApplyOutcome::Rejected(RejectReason::SeqMismatch { expected: n + 1 })
    );
    assert_eq!(h.data(), d, "rejected Suspend leaves state unchanged");
    assert_eq!(h.run(&r).status, RunStatus::Running);
    assert!(h.get(Table::Running, &run_key(&r)).is_some());

    // With the input consumed, the Suspend goes through.
    assert_eq!(
        h.apply(&events(&r, n + 1, vec![], vec![Derived::suspend()])),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.run(&r).status, RunStatus::Scheduled);
}

/// Same command sequence into two stores: same digest (apply is deterministic, never reads a
/// clock).
#[test]
fn timer_journaling_is_deterministic() {
    let run_both = || {
        let (mut h, r) = with_timer(vec![sched(8, 2000), Derived::suspend()]);
        assert_eq!(h.apply(&fired(&r, 7, 1000, 1001)), ApplyOutcome::Accepted);
        assert_eq!(h.apply(&grant(&r, 2)), ApplyOutcome::Accepted);
        assert_eq!(h.apply(&fired(&r, 8, 2000, 2002)), ApplyOutcome::Accepted);
        (h.digest(), h.rows(Table::Events, &r), h.journal(&r, 1))
    };
    let (d1, n1, j1) = run_both();
    let (d2, n2, j2) = run_both();
    assert_eq!(d1, d2);
    assert_eq!((n1, n2), (3, 3));
    assert_eq!(j1, vec![timer_event(1, 7, 1001), timer_event(2, 8, 2002)]);
    assert_eq!(j1, j2);
}
