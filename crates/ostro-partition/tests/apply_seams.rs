//! Phase 5 apply seams (plan 05-02): StartRun Phase 5 fields, the terminal hook (waits and
//! wait-timer cleanup, read budget), `CompleteRun.parent_event` oversize, reserved-timer
//! routing, and the stub entry points that 05-08 / 05-09 fill.
//!
//! Waits rows are written directly into the store here: `RegisterWait` is rejected until
//! 05-08 implements it.

use bytes::Bytes;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, apply_command,
};
use ostro_partition::query::journal_rows;
use ostro_partition::store::keys::{
    run_key, run_prefix, timer_key, wait_key, wait_name_key, wait_prefixes,
};
use ostro_partition::store::values::{self, RunRecord, RunStatus, WaitKind, WaitRow};
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    self, AttemptGranted, AttemptTokenMsg, CompleteRun, ConflictPolicy, Derived, EventsAppended,
    JournalEvent, StartRun, TerminalStatus, TimerFired, derived, encode_command,
};
use ostro_types::payload::INLINE_PAYLOAD_LIMIT;
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

    /// Applies one command in its own committed batch: (outcome, actions, keyspace reads).
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

    /// Writes raw rows outside apply (test setup only).
    fn put(&mut self, rows: Vec<(Table, Vec<u8>, Vec<u8>)>) {
        let mut b = self.store.batch();
        for (t, k, v) in rows {
            b.insert(t, k, v);
        }
        let mu = self.cur.meta_update();
        b.commit(&mu).expect("commit");
    }

    fn get(&self, t: Table, k: &[u8]) -> Option<Vec<u8>> {
        self.store.reader().get(t, k).expect("read")
    }

    fn run(&self, id: &RunId) -> RunRecord {
        values::decode(&self.get(Table::Runs, &run_key(id)).expect("run row")).expect("decode")
    }

    fn count(&self, t: Table, prefix: &[u8]) -> usize {
        self.store
            .reader()
            .prefix(t, prefix, usize::MAX)
            .expect("prefix")
            .len()
    }

    fn digest(&self) -> [u8; 32] {
        self.store.digest().expect("digest")
    }
}

fn rid(n: u8) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&2u16.to_be_bytes());
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

/// Run `n` started and granted `{1,1}`.
fn running(h: &mut H, n: u8) -> RunId {
    let r = rid(n);
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
    r
}

fn append(r: &RunId, from: u64, events: u64, derived: Vec<Derived>) -> Command {
    Command::events_appended(EventsAppended {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n: 1 }),
        from_seq: from,
        events: (from..from + events)
            .map(|seq| JournalEvent {
                seq,
                kind: 16,
                payload: Some(PayloadRef::inline(Bytes::from_static(b"e"))),
            })
            .collect(),
        derived,
    })
}

fn complete(status: TerminalStatus, resumable: bool, parent_event: Option<PayloadRef>) -> Derived {
    Derived::new(derived::Intent::CompleteRun(CompleteRun {
        status: status as i32,
        result: None,
        completed_at_ms: 900,
        resumable,
        parent_event,
    }))
}

fn fire(r: &RunId, op: u64, due: u64) -> Command {
    Command::timer_fired(TimerFired {
        run_id: r.as_bytes().to_vec(),
        op,
        due_at_ms: due,
        fired_at_ms: due + 1,
    })
}

fn bad_intent() -> ApplyOutcome {
    ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::BadIntent))
}

/// Two waits on `r` (op 3 with a timeout timer due 5000, op 7 without), `open_waits = 2`.
fn seed_waits(h: &mut H, r: &RunId) {
    let row = |name: &str, due: u64| {
        values::encode(&WaitRow {
            kind: WaitKind::Signal,
            name: name.into(),
            timeout_due_ms: due,
        })
    };
    let mut rec = h.run(r);
    rec.open_waits = 2;
    h.put(vec![
        (Table::Waits, wait_key(r, 3), row("go", 5000)),
        (Table::Waits, wait_name_key(r, "go", 3), Vec::new()),
        (Table::Waits, wait_key(r, 7), row("stop", 0)),
        (Table::Waits, wait_name_key(r, "stop", 7), Vec::new()),
        (Table::Timers, timer_key(5000, r, 3), Vec::new()),
        (Table::Runs, run_key(r), values::encode(&rec)),
    ]);
}

// ---------------------------------------------------------------- StartRun

#[test]
fn start_run_stores_phase5_fields() {
    let mut h = H::new();
    let r = rid(1);
    let s = StartRun {
        idempotency_key: b"idem-1".to_vec(),
        schedule_to_start_ms: 30_000,
        retention_ms: 86_400_000,
        retry_of: rid(8).as_bytes().to_vec(),
        continued_from: rid(9).as_bytes().to_vec(),
        ..start(&r)
    };
    assert_eq!(h.apply(&Command::start_run(s)), ApplyOutcome::Accepted);
    let rec = h.run(&r);
    assert_eq!(rec.schedule_to_start_ms, 30_000);
    assert_eq!(rec.retention_ms, 86_400_000);
    assert_eq!(rec.idempotency_key, b"idem-1");
    assert_eq!(rec.retry_of, Some(rid(8)));
    assert_eq!(rec.continued_from, Some(rid(9)));
    // Not set at start.
    assert_eq!(
        (
            rec.open_waits,
            rec.s2s_due_ms,
            rec.attempt_failures,
            rec.resumed_count
        ),
        (0, 0, 0, 0)
    );
    assert!(rec.terminal_reason.is_empty() && !rec.resumable);
    assert_eq!(
        (rec.status_seq, rec.continued_to, rec.pause),
        (None, None, None)
    );

    // Empty linkage fields are None; a malformed run id is rejected before any write.
    let r2 = rid(2);
    assert_eq!(
        h.apply(&Command::start_run(start(&r2))),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.run(&r2).retry_of, None);
    let before = h.digest();
    for bad in [
        StartRun {
            retry_of: vec![1, 2, 3],
            ..start(&rid(3))
        },
        StartRun {
            continued_from: vec![0; 17],
            ..start(&rid(3))
        },
    ] {
        assert_eq!(
            h.apply(&Command::start_run(bad)),
            ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::BadRunId))
        );
    }
    // Rejections still fold into the hash (meta), but no data row was written.
    assert_ne!(h.digest(), before);
    assert!(h.get(Table::Runs, &run_key(&rid(3))).is_none());
}

// ---------------------------------------------------------------- terminal hook

#[test]
fn complete_run_clears_waits_and_wait_timers() {
    let mut h = H::new();
    let r = running(&mut h, 1);
    seed_waits(&mut h, &r);
    let (out, actions, reads) = h.apply_full(&append(
        &r,
        0,
        1,
        vec![complete(TerminalStatus::Failed, true, None)],
    ));
    assert_eq!(out, ApplyOutcome::Accepted);
    // Run record + the two waits prefixes.
    assert_eq!(
        reads, 3,
        "documented read-budget exception (open_waits > 0)"
    );
    for p in wait_prefixes(&r) {
        assert_eq!(h.count(Table::Waits, &p), 0);
    }
    assert!(h.get(Table::Timers, &timer_key(5000, &r, 3)).is_none());
    assert!(actions.contains(&Action::CancelTimer {
        due_at_ms: 5000,
        run: r,
        op: 3
    }));
    assert!(actions.contains(&Action::RunCompleted {
        run: r,
        status: RunStatus::Failed
    }));
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::Failed);
    assert_eq!(rec.open_waits, 0);
    assert!(rec.resumable);
    assert!(rec.terminal_reason.is_empty());
    // The stale wait timer can no longer fire into the run.
    assert_eq!(
        h.apply(&fire(&r, 3, 5000)),
        ApplyOutcome::Duplicate { existing: None }
    );
}

#[test]
fn complete_run_without_waits_reads_no_waits() {
    let mut h = H::new();
    let r = running(&mut h, 1);
    let (out, _, reads) = h.apply_full(&append(
        &r,
        0,
        1,
        vec![complete(TerminalStatus::Completed, false, None)],
    ));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(
        reads, 1,
        "only the run record: open_waits == 0 skips both scans"
    );
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::Completed);
    assert!(!rec.resumable);
}

// ---------------------------------------------------------------- parent_event (W5)

#[test]
fn complete_run_parent_event_oversize_is_rejected() {
    let mut h = H::new();
    let r = running(&mut h, 1);
    let big = PayloadRef::inline(Bytes::from(vec![0u8; INLINE_PAYLOAD_LIMIT + 1]));
    let before = h.run(&r);
    assert_eq!(
        h.apply(&append(
            &r,
            0,
            1,
            vec![complete(TerminalStatus::Completed, false, Some(big))]
        )),
        ApplyOutcome::Rejected(RejectReason::PayloadTooLarge)
    );
    assert_eq!(h.run(&r), before, "nothing written");
    assert_eq!(h.count(Table::Events, &run_prefix(&r)), 0);

    // At the limit inline is fine, and a blob ref of any size is fine.
    let at_limit = PayloadRef::inline(Bytes::from(vec![0u8; INLINE_PAYLOAD_LIMIT]));
    assert_eq!(
        h.apply(&append(
            &r,
            0,
            1,
            vec![complete(TerminalStatus::Completed, false, Some(at_limit))]
        )),
        ApplyOutcome::Accepted
    );
    let r2 = running(&mut h, 2);
    let blob = PayloadRef::blob([0xee; 32], 8 << 20);
    assert_eq!(
        h.apply(&append(
            &r2,
            0,
            1,
            vec![complete(TerminalStatus::Completed, false, Some(blob))]
        )),
        ApplyOutcome::Accepted
    );
}

// ---------------------------------------------------------------- continue-as-new routing

#[test]
fn continued_as_new_routes_to_the_lifecycle_stub() {
    let mut h = H::new();
    let r = running(&mut h, 1);
    let before = h.run(&r);
    assert_eq!(
        h.apply(&append(
            &r,
            0,
            1,
            vec![complete(TerminalStatus::ContinuedAsNew, false, None)]
        )),
        bad_intent()
    );
    assert_eq!(h.run(&r), before);
    // Token, seq and contiguity checks still come first.
    assert_eq!(
        h.apply(&append(
            &r,
            4,
            1,
            vec![complete(TerminalStatus::ContinuedAsNew, false, None)]
        )),
        ApplyOutcome::Rejected(RejectReason::SeqMismatch { expected: 0 })
    );
}

// ---------------------------------------------------------------- reserved timer routing

#[test]
fn reserved_timer_ops_are_routed_and_journal_nothing() {
    for op in [
        command::RUN_TIMEOUT_OP,
        command::SCHEDULE_TO_START_OP,
        command::EXPIRE_OP,
        command::RESERVED_OP_MIN,
    ] {
        let mut h = H::new();
        let r = running(&mut h, 1);
        h.put(vec![(Table::Timers, timer_key(700, &r, op), Vec::new())]);
        let before = h.run(&r);
        let (out, actions, _) = h.apply_full(&fire(&r, op, 700));
        assert_eq!(out, ApplyOutcome::Accepted, "{op}");
        assert!(actions.is_empty(), "{op}");
        assert!(h.get(Table::Timers, &timer_key(700, &r, op)).is_none());
        assert_eq!(
            h.run(&r),
            before,
            "stub: no state change beyond the row ({op})"
        );
        assert_eq!(h.count(Table::Events, &run_prefix(&r)), 0);
        // Row absent = Duplicate is unchanged.
        assert_eq!(
            h.apply(&fire(&r, op, 700)),
            ApplyOutcome::Duplicate { existing: None }
        );
    }
}

#[test]
fn expire_timer_reaches_retention_on_a_terminal_run() {
    let mut h = H::new();
    let r = running(&mut h, 1);
    assert_eq!(
        h.apply(&append(
            &r,
            0,
            1,
            vec![complete(TerminalStatus::Completed, false, None)]
        )),
        ApplyOutcome::Accepted
    );
    h.put(vec![(
        Table::Timers,
        timer_key(9_000, &r, command::EXPIRE_OP),
        Vec::new(),
    )]);
    // Stub: Accepted; the run is untouched (05-09 deletes it).
    assert_eq!(
        h.apply(&fire(&r, command::EXPIRE_OP, 9_000)),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.run(&r).status, RunStatus::Completed);
}

#[test]
fn guest_timer_still_journals_timer_fired() {
    let mut h = H::new();
    let r = running(&mut h, 1);
    h.put(vec![(Table::Timers, timer_key(700, &r, 4), Vec::new())]);
    let (out, actions, _) = h.apply_full(&fire(&r, 4, 700));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(actions, vec![Action::InputAppended { run: r, seq: 0 }]);
    let rows = journal_rows(&h.store.reader(), &r, 0, 10).expect("journal");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1,
        ostro_types::journal::EventKind::TimerFired.code()
    );
}
