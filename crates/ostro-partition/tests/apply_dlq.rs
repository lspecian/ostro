//! Dead-letter marks (plan 11-08, OPS-04, D-P11-11): `Cmd::DlqUpdate` (45) upserts one
//! postcard `DlqRow` per run in keyspace 11; marks merge, re-application is idempotent, the
//! failure episode (W5) resets stale marks, malformed updates are `Invalid(BadDlqUpdate)` and
//! retention removes the row with the run.

use bytes::Bytes;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, apply_command, fold_hash,
};
use ostro_partition::store::keys::run_key;
use ostro_partition::store::values::{self, DlqRow, RunRecord, RunStatus};
use ostro_partition::{PartitionStore, StoreConfig, TABLES, Table};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, CompleteRun, ConflictPolicy, DLQ_DISMISS_REASON_MAX, Derived,
    DlqUpdate, EXPIRE_OP, EventsAppended, JournalEvent, RetryRun, SCHEDULE_TO_START_OP, StartRun,
    TerminalStatus, TimerFired, derived, dlq_update, encode_command,
};
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};

const P: u16 = 2;

struct H {
    _dir: tempfile::TempDir,
    store: PartitionStore,
    cur: Cursor,
    /// Attempt counter per test (one run per grant sequence is enough here).
    attempt: u32,
}

struct Applied {
    out: ApplyOutcome,
    actions: Vec<Action>,
    writes: usize,
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
            attempt: 0,
        };
        h.ok(&Command::leader_announced(1, 1, 0));
        h
    }

    fn apply_full(&mut self, c: &Command) -> Applied {
        let bytes = encode_command(c);
        let mut b = self.store.batch();
        let mut actions = Vec::new();
        let off = self.cur.next;
        let out = apply_command(&mut b, &mut self.cur, off, Epoch(1), &bytes, &mut actions)
            .expect("apply");
        let writes = b.len();
        let mu = self.cur.meta_update();
        b.commit(&mu).expect("commit");
        Applied {
            out,
            actions,
            writes,
        }
    }

    fn apply(&mut self, c: &Command) -> ApplyOutcome {
        self.apply_full(c).out
    }

    fn ok(&mut self, c: &Command) {
        assert_eq!(self.apply(c), ApplyOutcome::Accepted, "{}", c.kind_name());
    }

    fn get(&self, t: Table, k: &[u8]) -> Option<Vec<u8>> {
        self.store.reader().get(t, k).expect("read")
    }

    fn dlq(&self, r: &RunId) -> Option<DlqRow> {
        self.get(Table::Dlq, &run_key(r))
            .map(|v| values::decode(&v).expect("decode DlqRow"))
    }

    fn run(&self, r: &RunId) -> RunRecord {
        values::decode(&self.get(Table::Runs, &run_key(r)).expect("run row")).expect("decode")
    }

    fn dlq_rows(&self) -> usize {
        self.store
            .reader()
            .prefix(Table::Dlq, &[], usize::MAX)
            .expect("rows")
            .len()
    }

    /// Grants a fresh attempt and appends one event ending the run with `status` at `at`.
    fn end(&mut self, r: &RunId, status: TerminalStatus, at: u64) {
        self.attempt += 1;
        let n = self.attempt;
        self.ok(&Command::attempt_granted(AttemptGranted {
            run_id: r.as_bytes().to_vec(),
            token: Some(AttemptTokenMsg { epoch: 1, n }),
            after_failure: false,
        }));
        let from = self.run(r).next_seq;
        self.ok(&Command::events_appended(EventsAppended {
            run_id: r.as_bytes().to_vec(),
            token: Some(AttemptTokenMsg { epoch: 1, n }),
            from_seq: from,
            events: vec![JournalEvent {
                seq: from,
                kind: 16,
                payload: Some(PayloadRef::inline(Bytes::from_static(b"e"))),
                ..Default::default()
            }],
            derived: vec![Derived::new(derived::Intent::CompleteRun(CompleteRun {
                status: status as i32,
                result: Some(PayloadRef::inline(Bytes::from_static(b"res"))),
                completed_at_ms: at,
                resumable: true,
                parent_event: None,
            }))],
        }));
    }

    fn retry(&mut self, r: &RunId, at: u64) {
        self.ok(&Command::retry_run(RetryRun {
            run_id: r.as_bytes().to_vec(),
            requested_at_ms: at,
        }));
    }
}

fn rid(n: u8) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&P.to_be_bytes());
    b[17] = n;
    RunId::from_bytes(&b).expect("18 bytes")
}

fn start(h: &mut H, n: u8) -> RunId {
    start_s2s(h, n, 0)
}

/// A start with `schedule_to_start_ms` (0 = none): due at 10 + s2s.
fn start_s2s(h: &mut H, n: u8, s2s: u64) -> RunId {
    let r = rid(n);
    h.ok(&Command::start_run(StartRun {
        run_id: r.as_bytes().to_vec(),
        workflow: "acme/wf".into(),
        workflow_version: 1,
        queue: "acme/q".into(),
        input: Some(PayloadRef::inline(Bytes::from_static(b"in"))),
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        ready_at_ms: 10,
        requested_at_ms: 10,
        rng_seed: vec![7; 32],
        retention_ms: 5_000,
        schedule_to_start_ms: s2s,
        ..Default::default()
    }));
    r
}

fn upd(r: &[u8], op: Option<dlq_update::Op>, episode: u64) -> Command {
    Command::dlq_update(DlqUpdate {
        run_id: r.to_vec(),
        op,
        episode_completed_at_ms: episode,
    })
}

fn hook(r: &RunId, h: &RunId, episode: u64) -> Command {
    upd(
        r.as_bytes(),
        Some(dlq_update::Op::HookStarted(dlq_update::HookStarted {
            hook_run_id: h.as_bytes().to_vec(),
        })),
        episode,
    )
}

fn dismiss(r: &RunId, reason: &str, at_ms: u64, episode: u64) -> Command {
    upd(
        r.as_bytes(),
        Some(dlq_update::Op::Dismissed(dlq_update::Dismissed {
            reason: reason.into(),
            at_ms,
        })),
        episode,
    )
}

fn requeue(r: &RunId, mode: u32, to: &[u8], at_ms: u64, episode: u64) -> Command {
    upd(
        r.as_bytes(),
        Some(dlq_update::Op::Requeued(dlq_update::Requeued {
            mode,
            new_run_id: to.to_vec(),
            at_ms,
        })),
        episode,
    )
}

const BAD: ApplyOutcome = ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::BadDlqUpdate));

#[test]
fn hook_then_dismiss_merge_into_one_row_idempotently() {
    let mut h = H::new();
    let r = start(&mut h, 1);
    h.end(&r, TerminalStatus::Failed, 100);
    assert_eq!(h.dlq_rows(), 0, "no existing command writes a dlq row");
    let hk = rid(50);

    let a = h.apply_full(&hook(&r, &hk, 100));
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert!(a.actions.is_empty());
    let want = DlqRow {
        hook_run: Some(*hk.as_bytes()),
        episode_completed_at_ms: 100,
        ..DlqRow::default()
    };
    assert_eq!(h.dlq(&r), Some(want.clone()));
    // Re-applying: Accepted, nothing written (only cursor meta moves).
    let a = h.apply_full(&hook(&r, &hk, 100));
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert_eq!(a.writes, 0, "identical re-application writes nothing");
    assert_eq!(h.dlq(&r), Some(want));

    let run_before = h.get(Table::Runs, &run_key(&r));
    h.ok(&dismiss(&r, "noise", 5, 100));
    let want = DlqRow {
        hook_run: Some(*hk.as_bytes()),
        dismissed_at_ms: Some(5),
        dismiss_reason: Some("noise".into()),
        episode_completed_at_ms: 100,
        ..DlqRow::default()
    };
    assert_eq!(h.dlq(&r), Some(want.clone()));
    let a = h.apply_full(&dismiss(&r, "noise", 5, 100));
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert_eq!(a.writes, 0);
    assert_eq!(h.dlq(&r), Some(want));
    assert_eq!(
        h.get(Table::Runs, &run_key(&r)),
        run_before,
        "the runs row is never touched"
    );
    assert_eq!(h.dlq_rows(), 1);
}

#[test]
fn timed_out_run_takes_a_restart_requeue() {
    let mut h = H::new();
    // Never granted: the schedule-to-start timeout (due 70) stops it TimedOut.
    let r = start_s2s(&mut h, 1, 60);
    h.ok(&Command::timer_fired(TimerFired {
        run_id: r.as_bytes().to_vec(),
        op: SCHEDULE_TO_START_OP,
        due_at_ms: 70,
        fired_at_ms: 73,
    }));
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::TimedOut);
    let ep = rec.completed_at_ms.expect("completed_at_ms");
    assert_eq!(h.apply(&requeue(&r, 1, rid(2).as_bytes(), 7, ep + 1)), BAD);
    let r2 = rid(2);
    h.ok(&requeue(
        &r,
        dlq_update::REQUEUE_RESTART,
        r2.as_bytes(),
        7,
        ep,
    ));
    assert_eq!(
        h.dlq(&r),
        Some(DlqRow {
            requeued_to: Some(*r2.as_bytes()),
            requeued_at_ms: Some(7),
            requeue_mode: Some(1),
            episode_completed_at_ms: ep,
            ..DlqRow::default()
        })
    );
    // Rerun to another target overwrites the requeue mark.
    let r3 = rid(3);
    h.ok(&requeue(
        &r,
        dlq_update::REQUEUE_RERUN,
        r3.as_bytes(),
        8,
        ep,
    ));
    let row = h.dlq(&r).expect("row");
    assert_eq!(row.requeued_to, Some(*r3.as_bytes()));
    assert_eq!(row.requeue_mode, Some(2));
}

#[test]
fn rejections_map_to_bad_dlq_update_run_not_found_and_wrong_partition() {
    let mut h = H::new();
    let failed = start(&mut h, 1);
    h.end(&failed, TerminalStatus::Failed, 100);
    let done = start(&mut h, 2);
    h.end(&done, TerminalStatus::Completed, 100);
    let live = start(&mut h, 3);

    // Status: only Failed / TimedOut (except a resume requeue).
    assert_eq!(h.apply(&dismiss(&done, "x", 1, 100)), BAD);
    assert_eq!(h.apply(&hook(&live, &rid(50), 0)), BAD);
    assert_eq!(h.apply(&requeue(&done, 1, rid(9).as_bytes(), 1, 100)), BAD);
    // Unknown run on this partition.
    assert_eq!(
        h.apply(&dismiss(&rid(77), "x", 1, 100)),
        ApplyOutcome::Rejected(RejectReason::RunNotFound)
    );
    // Another partition.
    let mut other = *rid(1).as_bytes();
    other[..2].copy_from_slice(&5u16.to_be_bytes());
    assert_eq!(
        h.apply(&upd(
            &other,
            Some(dlq_update::Op::Dismissed(dlq_update::Dismissed::default())),
            100
        )),
        ApplyOutcome::Rejected(RejectReason::WrongPartition)
    );
    // Malformed: 17-byte run id, missing op, reason too long, bad ids / modes.
    let f = failed.as_bytes();
    assert_eq!(
        h.apply(&upd(
            &f[..17],
            Some(dlq_update::Op::Dismissed(dlq_update::Dismissed::default())),
            100
        )),
        BAD,
        "a malformed run id is BadDlqUpdate (not BadRunId)"
    );
    assert_eq!(h.apply(&upd(f, None, 100)), BAD);
    let long = "r".repeat(DLQ_DISMISS_REASON_MAX + 1);
    assert_eq!(h.apply(&dismiss(&failed, &long, 1, 100)), BAD);
    let max = "r".repeat(DLQ_DISMISS_REASON_MAX);
    assert_eq!(
        h.apply(&dismiss(&failed, &max, 1, 100)),
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.apply(&upd(
            f,
            Some(dlq_update::Op::HookStarted(dlq_update::HookStarted {
                hook_run_id: vec![1; 17],
            })),
            100
        )),
        BAD
    );
    assert_eq!(
        h.apply(&requeue(&failed, 3, rid(9).as_bytes(), 1, 100)),
        BAD
    );
    assert_eq!(h.apply(&requeue(&failed, 1, &[], 1, 100)), BAD);
    assert_eq!(h.apply(&requeue(&failed, 2, &[1; 17], 1, 100)), BAD);
    assert_eq!(
        h.apply(&requeue(&failed, 0, rid(9).as_bytes(), 1, 100)),
        BAD,
        "a resume keeps the run id: new_run_id must be empty"
    );
    // Episode mismatch on a failed run.
    assert_eq!(h.apply(&dismiss(&failed, "x", 1, 99)), BAD);
    // A second, different hook run.
    h.ok(&hook(&failed, &rid(50), 100));
    assert_eq!(h.apply(&hook(&failed, &rid(51), 100)), BAD);
    assert_eq!(
        h.dlq(&failed).expect("row").hook_run,
        Some(*rid(50).as_bytes())
    );
    // Nothing was written for the rejected runs.
    assert!(h.dlq(&done).is_none() && h.dlq(&live).is_none());
}

#[test]
fn rejections_fold_into_the_rolling_hash_and_write_nothing() {
    let mut h = H::new();
    let done = start(&mut h, 1);
    h.end(&done, TerminalStatus::Completed, 100);
    for c in [
        dismiss(&done, "x", 1, 100),
        upd(done.as_bytes(), None, 100),
        dismiss(&rid(9), "x", 1, 100),
    ] {
        let prev = h.cur.hash;
        let off = h.cur.next;
        let a = h.apply_full(&c);
        assert!(matches!(a.out, ApplyOutcome::Rejected(_)), "{:?}", a.out);
        assert_eq!(a.writes, 0, "a rejection writes nothing");
        assert_eq!(
            h.cur.hash,
            fold_hash(&prev, off, a.out.code(), &encode_command(&c))
        );
    }
    assert_eq!(h.dlq_rows(), 0);
}

#[test]
fn resume_requeue_is_accepted_after_retry_run() {
    let mut h = H::new();
    let r = start(&mut h, 1);
    h.end(&r, TerminalStatus::Failed, 100);
    h.retry(&r, 200);
    assert!(matches!(
        h.run(&r).status,
        RunStatus::Ready | RunStatus::Running
    ));
    h.ok(&requeue(&r, dlq_update::REQUEUE_RESUME, &[], 210, 100));
    assert_eq!(
        h.dlq(&r),
        Some(DlqRow {
            requeued_at_ms: Some(210),
            requeue_mode: Some(0),
            episode_completed_at_ms: 100,
            ..DlqRow::default()
        })
    );
    // Idempotent.
    let a = h.apply_full(&requeue(&r, dlq_update::REQUEUE_RESUME, &[], 210, 100));
    assert_eq!((a.out, a.writes), (ApplyOutcome::Accepted, 0));
    // A resume requeue on the failed run itself (before the RetryRun) needs the episode.
    let r2 = start(&mut h, 2);
    h.end(&r2, TerminalStatus::Failed, 300);
    assert_eq!(h.apply(&requeue(&r2, 0, &[], 1, 299)), BAD);
    h.ok(&requeue(&r2, 0, &[], 1, 300));
}

#[test]
fn a_new_failure_episode_resets_older_marks() {
    let mut h = H::new();
    let r = start(&mut h, 1);
    h.end(&r, TerminalStatus::Failed, 100);
    h.ok(&dismiss(&r, "flaky", 110, 100));
    h.retry(&r, 150);
    h.end(&r, TerminalStatus::Failed, 200);
    assert_eq!(h.run(&r).completed_at_ms, Some(200));
    // The stored row (episode 100) does not apply to the new failure: a view comparing
    // episodes sees the run in the DLQ again. The first mark of episode 200 resets it.
    h.ok(&hook(&r, &rid(60), 200));
    assert_eq!(
        h.dlq(&r),
        Some(DlqRow {
            hook_run: Some(*rid(60).as_bytes()),
            episode_completed_at_ms: 200,
            ..DlqRow::default()
        }),
        "dismissal of episode 100 gone"
    );
    // A stale mark for the earlier episode is refused.
    assert_eq!(h.apply(&dismiss(&r, "flaky", 110, 100)), BAD);
    assert_eq!(h.dlq(&r).expect("row").episode_completed_at_ms, 200);
}

#[test]
fn retention_expiry_deletes_the_dlq_row() {
    let mut h = H::new();
    let keep = start(&mut h, 2);
    h.end(&keep, TerminalStatus::Failed, 1_000);
    h.ok(&dismiss(&keep, "keep", 1, 1_000));
    let r = start(&mut h, 1);
    h.end(&r, TerminalStatus::Failed, 1_000);
    h.ok(&hook(&r, &rid(50), 1_000));
    h.ok(&dismiss(&r, "done", 1_100, 1_000));
    assert!(h.dlq(&r).is_some());
    h.ok(&Command::timer_fired(TimerFired {
        run_id: r.as_bytes().to_vec(),
        op: EXPIRE_OP,
        due_at_ms: 6_000,
        fired_at_ms: 6_003,
    }));
    assert!(h.get(Table::Runs, &run_key(&r)).is_none(), "run expired");
    assert!(h.dlq(&r).is_none(), "dlq row removed with the run");
    assert!(h.dlq(&keep).is_some(), "other runs' rows stay");
    // Nothing else of the run is left in any run-scoped table either.
    for t in TABLES {
        if matches!(
            t,
            Table::Meta | Table::Timers | Table::InboundSeen | Table::Outbox
        ) {
            continue;
        }
        let rows = h
            .store
            .reader()
            .prefix(t, r.as_bytes(), usize::MAX)
            .expect("rows");
        assert!(rows.is_empty(), "{t:?} still has rows of the run");
    }
}
