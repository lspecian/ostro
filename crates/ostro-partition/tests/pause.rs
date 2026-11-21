//! Run pause / resume apply suite (plan 07-04; OPS-01, research Pattern 9 / Pitfall 8).
//!
//! `PauseRun` / `ResumeRun` semantics, the `enqueue_ready` gate (paused ⇒ no ready row, no
//! `EnqueueReady`), `RunPaused` rejects of executor commands (after fencing), inputs journaled
//! without a wake, the terminal hook clearing the mark (B7) and the read budget (1 each).

use bytes::Bytes;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, apply_command,
};
use ostro_partition::store::keys::running_key;
use ostro_partition::store::values::{PauseMark, RunRecord, RunStatus};
use ostro_partition::{PartitionStore, StoreConfig, Table, query};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, CompleteEffect, CompleteRun, CompleteToken, ConflictPolicy,
    Derived, EnqueueReady, EventsAppended, JournalEvent, MAX_PAUSE_DETAIL_LEN,
    PAUSE_REASON_OPERATOR, PAUSE_REASON_RETRIES_EXHAUSTED, PauseRun, RUN_TIMEOUT_OP,
    RegisterEffect, RegisterWait, RequestCancel, ResumeRun, RetryRun, ScheduleTimer, SignalRun,
    StartRun, TerminalStatus, TerminateRun, TimerFired, WaitKind, derived, encode_command,
};
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};

const P: u16 = 3;

struct H {
    _dir: tempfile::TempDir,
    store: PartitionStore,
    cur: Cursor,
}

/// One applied command: outcome, actions, keyspace reads, overlay entries (writes).
struct Applied {
    out: ApplyOutcome,
    actions: Vec<Action>,
    reads: u64,
    writes: usize,
}

impl Applied {
    fn enqueues(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| matches!(a, Action::EnqueueReady { .. }))
            .count()
    }
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
        };
        h.ok(&Command::leader_announced(1, 1, 0));
        h
    }

    fn apply(&mut self, c: &Command) -> Applied {
        let bytes = encode_command(c);
        let mut b = self.store.batch();
        let mut actions = Vec::new();
        let off = self.cur.next;
        let out = apply_command(&mut b, &mut self.cur, off, Epoch(1), &bytes, &mut actions)
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

    fn ok(&mut self, c: &Command) -> Applied {
        let a = self.apply(c);
        assert_eq!(a.out, ApplyOutcome::Accepted, "{}", c.kind_name());
        a
    }

    fn rec(&self, id: &RunId) -> RunRecord {
        query::run_record(&self.store.reader(), id)
            .expect("read")
            .expect("run exists")
    }

    fn ready_rows(&self) -> usize {
        self.store
            .reader()
            .prefix(Table::Ready, &[], usize::MAX)
            .expect("scan")
            .len()
    }

    fn running_row(&self, id: &RunId) -> bool {
        self.store
            .reader()
            .get(Table::Running, &running_key(id))
            .expect("read")
            .is_some()
    }

    fn digest(&self) -> [u8; 32] {
        self.store.digest().expect("digest")
    }
}

fn rid(n: u8) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&P.to_be_bytes());
    b[17] = n;
    RunId::from_bytes(&b).expect("18 bytes")
}

fn inline(b: &'static [u8]) -> Option<PayloadRef> {
    Some(PayloadRef::inline(Bytes::from_static(b)))
}

fn start_at(id: &RunId, ready_at_ms: u64, run_timeout_ms: u64) -> Command {
    Command::start_run(StartRun {
        run_id: id.as_bytes().to_vec(),
        workflow: "wf".into(),
        workflow_version: 1,
        queue: "q".into(),
        input: inline(b"in"),
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        ready_at_ms,
        requested_at_ms: 100,
        rng_seed: vec![7; 32],
        run_timeout_ms,
        ..Default::default()
    })
}

fn start(id: &RunId) -> Command {
    start_at(id, 100, 0)
}

fn tok(n: u32) -> Option<AttemptTokenMsg> {
    Some(AttemptTokenMsg { epoch: 1, n })
}

fn grant(id: &RunId, n: u32) -> Command {
    Command::attempt_granted(AttemptGranted {
        run_id: id.as_bytes().to_vec(),
        token: tok(n),
        after_failure: false,
    })
}

fn pause_with(id: &RunId, reason: u32, detail: &str, token: Option<AttemptTokenMsg>) -> Command {
    Command::pause_run(PauseRun {
        run_id: id.as_bytes().to_vec(),
        reason,
        detail: detail.into(),
        token,
        requested_at_ms: 200,
    })
}

fn pause(id: &RunId) -> Command {
    pause_with(id, PAUSE_REASON_OPERATOR, "maintenance", None)
}

fn resume(id: &RunId) -> Command {
    Command::resume_run(ResumeRun {
        run_id: id.as_bytes().to_vec(),
        requested_at_ms: 300,
    })
}

fn append(id: &RunId, n: u32, from: u64, events: u64, d: Vec<derived::Intent>) -> Command {
    Command::events_appended(EventsAppended {
        run_id: id.as_bytes().to_vec(),
        token: tok(n),
        from_seq: from,
        events: (from..from + events)
            .map(|seq| JournalEvent {
                seq,
                kind: 16,
                payload: inline(b"e"),
            })
            .collect(),
        derived: d.into_iter().map(Derived::new).collect(),
    })
}

fn complete() -> derived::Intent {
    derived::Intent::CompleteRun(CompleteRun {
        status: TerminalStatus::Completed as i32,
        result: inline(b"out"),
        completed_at_ms: 900,
        resumable: false,
        parent_event: None,
    })
}

fn rejected(r: RejectReason) -> ApplyOutcome {
    ApplyOutcome::Rejected(r)
}

/// Start + grant(n=1) + one event at seq 0.
fn running_run(h: &mut H, id: &RunId) {
    h.ok(&start(id));
    h.ok(&grant(id, 1));
    h.ok(&append(id, 1, 0, 1, vec![]));
}

// ----------------------------------------------------------------------------- PauseRun

#[test]
fn pause_ready_run_removes_ready_row_with_one_read() {
    let mut h = H::new();
    let r = rid(1);
    h.ok(&start(&r));
    assert_eq!(h.ready_rows(), 1);
    let a = h.ok(&pause(&r));
    assert_eq!(a.reads, 1, "PauseRun reads exactly the run record");
    assert_eq!(a.actions, vec![Action::RunPaused { run: r }]);
    let rec = h.rec(&r);
    assert_eq!(rec.status, RunStatus::Ready, "status may stay Ready");
    assert_eq!(rec.ready, None);
    assert_eq!(
        rec.pause,
        Some(PauseMark {
            reason: 0,
            at_ms: 200,
            detail: "maintenance".into()
        })
    );
    assert_eq!(h.ready_rows(), 0, "no ready row while paused");
}

#[test]
fn pause_running_run_stays_running() {
    let mut h = H::new();
    let r = rid(2);
    running_run(&mut h, &r);
    let a = h.ok(&pause(&r));
    assert_eq!(a.reads, 1);
    assert_eq!(a.actions, vec![Action::RunPaused { run: r }]);
    let rec = h.rec(&r);
    assert_eq!(rec.status, RunStatus::Running);
    assert!(rec.pause.is_some());
    assert!(h.running_row(&r), "running row untouched (07-05 revokes)");
    assert_eq!(h.ready_rows(), 0);
}

#[test]
fn pause_scheduled_run_and_resume_leaves_it_scheduled() {
    let mut h = H::new();
    let r = rid(3);
    h.ok(&start_at(&r, 5_000, 0)); // delayed: Scheduled, START_DELAY timer
    assert_eq!(h.rec(&r).status, RunStatus::Scheduled);
    h.ok(&pause(&r));
    assert_eq!(h.rec(&r).status, RunStatus::Scheduled);
    let a = h.ok(&resume(&r));
    assert_eq!(a.reads, 1, "ResumeRun reads exactly the run record");
    assert_eq!(a.actions, vec![Action::RunResumed { run: r }]);
    let rec = h.rec(&r);
    assert_eq!(rec.status, RunStatus::Scheduled);
    assert_eq!(rec.pause, None);
    assert_eq!(h.ready_rows(), 0);
}

#[test]
fn pause_is_idempotent() {
    let mut h = H::new();
    let r = rid(4);
    h.ok(&start(&r));
    h.ok(&pause(&r));
    let before = h.rec(&r);
    let a = h.ok(&pause_with(&r, PAUSE_REASON_OPERATOR, "again", None));
    assert!(a.actions.is_empty());
    assert_eq!(a.writes, 0, "no state change");
    assert_eq!(a.reads, 1);
    assert_eq!(h.rec(&r), before);
    assert_eq!(h.rec(&r).pause.expect("paused").detail, "maintenance");
}

#[test]
fn pause_rejects_terminal_unknown_and_invalid() {
    let mut h = H::new();
    let r = rid(5);
    running_run(&mut h, &r);
    h.ok(&append(&r, 1, 1, 0, vec![complete()]));
    assert_eq!(h.apply(&pause(&r)).out, rejected(RejectReason::RunTerminal));
    assert_eq!(
        h.apply(&pause(&rid(99))).out,
        rejected(RejectReason::RunNotFound)
    );
    let bad = Command::pause_run(PauseRun::default());
    assert_eq!(
        h.apply(&bad).out,
        rejected(RejectReason::Invalid(InvalidCode::BadRunId))
    );
    let mut foreign = [0u8; 18];
    foreign[..2].copy_from_slice(&(P + 1).to_be_bytes());
    let foreign = RunId::from_bytes(&foreign).expect("id");
    assert_eq!(
        h.apply(&pause(&foreign)).out,
        rejected(RejectReason::WrongPartition)
    );
    let r2 = rid(6);
    h.ok(&start(&r2));
    let long = "d".repeat(MAX_PAUSE_DETAIL_LEN + 1);
    let a = h.apply(&pause_with(&r2, PAUSE_REASON_OPERATOR, &long, None));
    assert_eq!(
        a.out,
        rejected(RejectReason::Invalid(InvalidCode::FieldTooLong))
    );
    assert_eq!(a.reads, 0, "validated before the read");
    assert_eq!(
        h.apply(&pause_with(&r2, 7, "x", None)).out,
        rejected(RejectReason::Invalid(InvalidCode::Decode))
    );
    assert_eq!(h.rec(&r2).pause, None);
}

#[test]
fn auto_pause_is_fenced_like_events_appended() {
    let mut h = H::new();
    let r = rid(7);
    h.ok(&start(&r));
    h.ok(&grant(&r, 1));
    h.ok(&grant(&r, 2));
    let auto = |n: u32| pause_with(&r, PAUSE_REASON_RETRIES_EXHAUSTED, "retries", tok(n));
    let current = Some(ostro_types::AttemptToken {
        epoch: Epoch(1),
        n: 2,
    });
    // Older and newer attempts are both Fenced (only the current attempt may auto-pause).
    assert_eq!(
        h.apply(&auto(1)).out,
        rejected(RejectReason::Fenced { current })
    );
    assert_eq!(
        h.apply(&auto(3)).out,
        rejected(RejectReason::Fenced { current })
    );
    let stale_epoch = pause_with(
        &r,
        PAUSE_REASON_RETRIES_EXHAUSTED,
        "retries",
        Some(AttemptTokenMsg { epoch: 0, n: 2 }),
    );
    assert_eq!(
        h.apply(&stale_epoch).out,
        rejected(RejectReason::StaleEpoch { current: Epoch(1) })
    );
    assert_eq!(h.rec(&r).pause, None);
    let a = h.ok(&auto(2));
    assert_eq!(a.reads, 1);
    assert_eq!(h.rec(&r).pause.expect("paused").reason, 1);
}

// ------------------------------------------------------------------- executor rejects

#[test]
fn paused_run_rejects_events_appended_atomically() {
    let mut h = H::new();
    let r = rid(8);
    running_run(&mut h, &r);
    h.ok(&pause(&r));
    let before = h.rec(&r);
    let enq = derived::Intent::EnqueueReady(EnqueueReady { ready_at_ms: 400 });
    let timer = derived::Intent::ScheduleTimer(ScheduleTimer {
        op: 1,
        due_at_ms: 500,
    });
    for (name, c) in [
        ("plain", append(&r, 1, 1, 2, vec![])),
        (
            "timer+enqueue",
            append(&r, 1, 1, 1, vec![timer.clone(), enq]),
        ),
        (
            "suspend",
            append(
                &r,
                1,
                1,
                1,
                vec![timer, derived::Intent::Suspend(Default::default())],
            ),
        ),
        ("complete", append(&r, 1, 1, 1, vec![complete()])),
    ] {
        let a = h.apply(&c);
        assert_eq!(a.out, rejected(RejectReason::RunPaused), "{name}");
        assert_eq!(a.writes, 0, "{name}: nothing applied");
        assert!(a.actions.is_empty(), "{name}");
    }
    // Reject order: Fenced before RunPaused (a stale zombie still sees Fenced).
    let zombie = append(&r, 9, 1, 1, vec![]);
    assert!(matches!(
        h.apply(&zombie).out,
        ApplyOutcome::Rejected(RejectReason::Fenced { .. })
    ));
    // RunPaused before SeqMismatch.
    assert_eq!(
        h.apply(&append(&r, 1, 7, 1, vec![])).out,
        rejected(RejectReason::RunPaused)
    );
    assert_eq!(h.rec(&r), before);
    assert_eq!(h.rec(&r).next_seq, 1);
}

#[test]
fn paused_run_rejects_attempt_granted() {
    let mut h = H::new();
    let r = rid(9);
    h.ok(&start(&r));
    h.ok(&grant(&r, 1));
    h.ok(&pause(&r));
    let a = h.apply(&grant(&r, 2));
    assert_eq!(a.out, rejected(RejectReason::RunPaused));
    assert_eq!(a.writes, 0);
    // A stale grant is still Fenced first.
    assert!(matches!(
        h.apply(&grant(&r, 1)).out,
        ApplyOutcome::Rejected(RejectReason::Fenced { .. })
    ));
    // Never-granted paused run: RunPaused too.
    let r2 = rid(10);
    h.ok(&start(&r2));
    h.ok(&pause(&r2));
    assert_eq!(
        h.apply(&grant(&r2, 1)).out,
        rejected(RejectReason::RunPaused)
    );
}

// ------------------------------------------------------------------- inputs while paused

#[test]
fn paused_run_journals_inputs_without_waking() {
    let mut h = H::new();
    let r = rid(11);
    h.ok(&start(&r));
    h.ok(&grant(&r, 1));
    // A timer, a signal wait (op 2), a token wait (op 3) and an effect (op 4), then
    // Suspend (Scheduled).
    // Ops name journaled seqs (apply drops registrations for ops >= next_seq): 5 events.
    h.ok(&append(
        &r,
        1,
        0,
        5,
        vec![
            derived::Intent::ScheduleTimer(ScheduleTimer {
                op: 1,
                due_at_ms: 500,
            }),
            derived::Intent::RegisterWait(RegisterWait {
                op: 2,
                kind: WaitKind::Signal as i32,
                name: "go".into(),
                timeout_due_ms: 0,
                at_ms: 150,
            }),
            derived::Intent::RegisterWait(RegisterWait {
                op: 3,
                kind: WaitKind::Token as i32,
                name: String::new(),
                timeout_due_ms: 0,
                at_ms: 150,
            }),
            derived::Intent::RegisterEffect(RegisterEffect {
                op: 4,
                kind: 0,
                scheduled_at_ms: 150,
            }),
        ],
    ));
    h.ok(&append(
        &r,
        1,
        5,
        0,
        vec![derived::Intent::Suspend(Default::default())],
    ));
    assert_eq!(h.rec(&r).open_waits, 2);
    assert_eq!(h.rec(&r).status, RunStatus::Scheduled);
    h.ok(&pause(&r));

    let inputs = [
        Command::timer_fired(TimerFired {
            run_id: r.as_bytes().to_vec(),
            op: 1,
            due_at_ms: 500,
            fired_at_ms: 501,
        }),
        Command::complete_effect(CompleteEffect {
            run_id: r.as_bytes().to_vec(),
            op: 4,
            outcome: inline(b"fx"),
            completed_at_ms: 510,
        }),
        Command::signal_run(SignalRun {
            run_id: r.as_bytes().to_vec(),
            name: "go".into(),
            event: inline(b"sig"),
            request_id: vec![],
            sent_at_ms: 520,
        }),
        Command::complete_token(CompleteToken {
            run_id: r.as_bytes().to_vec(),
            op: 3,
            event: inline(b"tok"),
            completed_at_ms: 530,
        }),
        Command::request_cancel(RequestCancel {
            run_id: r.as_bytes().to_vec(),
            reason: "stop".into(),
            requested_at_ms: 540,
        }),
    ];
    for c in &inputs {
        let before = h.rec(&r).next_seq;
        let a = h.ok(c);
        assert_eq!(a.enqueues(), 0, "{}: no wake while paused", c.kind_name());
        assert_eq!(h.ready_rows(), 0, "{}", c.kind_name());
        let rec = h.rec(&r);
        assert!(rec.next_seq > before, "{}: journaled", c.kind_name());
        assert!(rec.pause.is_some());
        assert_eq!(rec.ready, None);
    }
    // The first wake moved the run to Ready without a row.
    assert_eq!(h.rec(&r).status, RunStatus::Ready);

    // Resume re-enqueues once; a started run is not a first activation (F7).
    let a = h.ok(&resume(&r));
    assert_eq!(a.reads, 1);
    assert_eq!(h.ready_rows(), 1);
    assert!(a.actions.contains(&Action::RunResumed { run: r }));
    let enq: Vec<_> = a
        .actions
        .iter()
        .filter_map(|x| match x {
            Action::EnqueueReady {
                run,
                ready_at_ms,
                first_activation,
                ..
            } => Some((*run, *ready_at_ms, *first_activation)),
            _ => None,
        })
        .collect();
    assert_eq!(enq, vec![(r, 300, false)]);
    let rec = h.rec(&r);
    assert_eq!(rec.status, RunStatus::Ready);
    assert_eq!(rec.pause, None);
    h.ok(&grant(&r, 2));
}

// ------------------------------------------------------------------------------ resume

#[test]
fn resume_running_run_re_enqueues() {
    let mut h = H::new();
    let r = rid(12);
    running_run(&mut h, &r);
    h.ok(&pause(&r));
    let a = h.ok(&resume(&r));
    assert_eq!(a.reads, 1);
    assert_eq!(a.enqueues(), 1);
    assert!(matches!(
        a.actions.last(),
        Some(Action::RunResumed { run }) if *run == r
    ));
    assert!(a.actions.iter().any(|x| matches!(
        x,
        Action::EnqueueReady {
            first_activation: false,
            ready_at_ms: 300,
            ..
        }
    )));
    let rec = h.rec(&r);
    assert_eq!(rec.status, RunStatus::Ready);
    assert!(!h.running_row(&r), "leave_running removed the running row");
    assert_eq!(h.ready_rows(), 1);
    h.ok(&grant(&r, 2));
    h.ok(&append(&r, 2, 1, 1, vec![complete()]));
    assert_eq!(h.rec(&r).status, RunStatus::Completed);
}

#[test]
fn resume_never_granted_ready_run_is_first_activation() {
    let mut h = H::new();
    let r = rid(13);
    h.ok(&start(&r));
    h.ok(&pause(&r));
    let a = h.ok(&resume(&r));
    assert!(a.actions.iter().any(|x| matches!(
        x,
        Action::EnqueueReady {
            first_activation: true,
            ..
        }
    )));
    assert_eq!(h.ready_rows(), 1);
}

#[test]
fn resume_is_idempotent_and_rejects_terminal() {
    let mut h = H::new();
    let r = rid(14);
    h.ok(&start(&r));
    let before = h.rec(&r);
    let a = h.ok(&resume(&r));
    assert!(a.actions.is_empty());
    assert_eq!(a.writes, 0);
    assert_eq!(a.reads, 1);
    assert_eq!(h.rec(&r), before);
    h.ok(&pause(&r));
    h.ok(&resume(&r));
    let a = h.ok(&resume(&r));
    assert!(a.actions.is_empty(), "second resume is a no-op");
    assert_eq!(
        h.apply(&resume(&rid(98))).out,
        rejected(RejectReason::RunNotFound)
    );
    h.ok(&Command::terminate_run(TerminateRun {
        run_id: r.as_bytes().to_vec(),
        reason: "x".into(),
        terminated_at_ms: 400,
    }));
    assert_eq!(
        h.apply(&resume(&r)).out,
        rejected(RejectReason::RunTerminal)
    );
}

// ------------------------------------------------------------------- terminal interplay

#[test]
fn run_timeout_while_paused_clears_mark_and_retry_resumes() {
    let mut h = H::new();
    let r = rid(15);
    h.ok(&start_at(&r, 100, 1_000)); // run timeout due at 1100
    h.ok(&grant(&r, 1));
    h.ok(&append(&r, 1, 0, 1, vec![]));
    h.ok(&pause(&r));
    // The run timeout keeps ticking while paused.
    let a = h.ok(&Command::timer_fired(TimerFired {
        run_id: r.as_bytes().to_vec(),
        op: RUN_TIMEOUT_OP,
        due_at_ms: 1_100,
        fired_at_ms: 1_101,
    }));
    assert!(a.actions.contains(&Action::RunCompleted {
        run: r,
        status: RunStatus::TimedOut
    }));
    let rec = h.rec(&r);
    assert_eq!(rec.status, RunStatus::TimedOut);
    assert!(rec.resumable);
    assert_eq!(rec.pause, None, "terminal transitions clear the mark (B7)");

    let a = h.ok(&Command::retry_run(RetryRun {
        run_id: r.as_bytes().to_vec(),
        requested_at_ms: 2_000,
    }));
    assert_eq!(a.enqueues(), 1, "RetryRun is not stuck behind the gate");
    assert_eq!(h.ready_rows(), 1);
    assert_eq!(h.rec(&r).status, RunStatus::Ready);
    h.ok(&grant(&r, 2));
    let seq = h.rec(&r).next_seq;
    h.ok(&append(&r, 2, seq, 1, vec![complete()]));
    assert_eq!(h.rec(&r).status, RunStatus::Completed);
}

#[test]
fn terminate_while_paused_clears_mark() {
    let mut h = H::new();
    let r = rid(16);
    running_run(&mut h, &r);
    h.ok(&pause(&r));
    h.ok(&Command::terminate_run(TerminateRun {
        run_id: r.as_bytes().to_vec(),
        reason: "ops".into(),
        terminated_at_ms: 600,
    }));
    let rec = h.rec(&r);
    assert_eq!(rec.status, RunStatus::Terminated);
    assert_eq!(rec.pause, None);
    assert!(!h.running_row(&r));
    assert_eq!(h.ready_rows(), 0);
}

#[test]
fn pause_resume_is_deterministic() {
    let script = |h: &mut H| {
        let r = rid(17);
        running_run(h, &r);
        h.ok(&pause(&r));
        h.ok(&resume(&r));
        h.ok(&grant(&r, 2));
        h.ok(&pause_with(
            &r,
            PAUSE_REASON_RETRIES_EXHAUSTED,
            "auto",
            tok(2),
        ));
        (h.digest(), h.cur.hash)
    };
    let (mut a, mut b) = (H::new(), H::new());
    assert_eq!(script(&mut a), script(&mut b));
}
