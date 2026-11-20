//! `Cmd::ScheduleFire` apply (plan 08-07; TRIG-02, TRIG-03): validation, the durable
//! watermark, overlap policies, manual fires, buffered fires started by the terminal hook,
//! schedule incarnations (W4), reverse-index cleanup and read counts.
//!
//! Every test drives `apply_command` against an on-disk partition store (partition
//! [`OWNER`] of [`PC`]), one committed batch per command, so the `ApplyBatch` read counter is
//! per command. Most tests end with a rebuild check (one-batch replay of the log equals the
//! incremental state).

mod common;

use common::gen_schedule::{
    OWNER, PC, advance_cmd, finish_cmd, fire_cmd, foreign_name, grant_cmd, manual_cmd, owned_names,
    pcount, run_id,
};
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, apply_command,
};
use ostro_partition::store::keys::{dedupe_key, run_key, schedule_run_key, schedule_state_key};
use ostro_partition::store::values::{
    self, RunRecord, RunStatus, ScheduleRunRow, ScheduleStateRow,
};
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    EXPIRE_OP, ScheduleFire, ScheduleFireKind, TerminalStatus, TimerFired, cron_idempotency_key,
};
use ostro_types::{Command, Epoch, PartitionId, RunId};

const SKIP: u32 = 0;
const BUFFER: u32 = 1;
const ALLOW: u32 = 2;
const CANCEL: u32 = 3;
const TERMINATE: u32 = 4;
const C1: u64 = 1_000;
const C2: u64 = 2_000;

// ---------------------------------------------------------------- harness

struct H {
    _dir: tempfile::TempDir,
    store: PartitionStore,
    cur: Cursor,
    epoch: u32,
    log: Vec<(u32, Vec<u8>)>,
    name: String,
}

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
        PartitionId(OWNER),
        pcount(PC),
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
            name: owned_names(OWNER, PC, 1).remove(0),
        };
        let a = h.apply_full(&Command::leader_announced(1, 1, 0));
        assert_eq!(a.out, ApplyOutcome::Accepted);
        h
    }

    fn apply_full(&mut self, c: &Command) -> Applied {
        let bytes = ostro_types::command::encode_command(c);
        self.log.push((self.epoch, bytes.to_vec()));
        let mut b = self.store.batch();
        let mut actions = Vec::new();
        let off = self.cur.next;
        let out = apply_command(
            &mut b,
            &mut self.cur,
            off,
            Epoch(self.epoch),
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

    fn apply(&mut self, c: &Command) -> ApplyOutcome {
        self.apply_full(c).out
    }

    fn fire(&mut self, s: ScheduleFire) -> Applied {
        self.apply_full(&Command::schedule_fire(s))
    }

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

    fn run(&self, id: &RunId) -> Option<RunRecord> {
        self.get(Table::Runs, &run_key(id))
            .map(|v| values::decode(&v).expect("decode run"))
    }

    fn status(&self, id: &RunId) -> RunStatus {
        self.run(id).expect("run exists").status
    }

    fn state(&self) -> ScheduleStateRow {
        self.state_of(&self.name.clone()).expect("schedule row")
    }

    fn state_of(&self, name: &str) -> Option<ScheduleStateRow> {
        self.get(Table::ScheduleState, &schedule_state_key(name))
            .map(|v| values::decode(&v).expect("decode state"))
    }

    fn reverse(&self, run: &RunId) -> Option<ScheduleRunRow> {
        self.get(Table::ScheduleState, &schedule_run_key(run))
            .map(|v| values::decode(&v).expect("decode reverse"))
    }

    /// Completes `run` (grant n=`n`, `CompleteRun{status}`); returns the completion.
    fn finish(&mut self, run: &RunId, n: u32, status: TerminalStatus) -> Applied {
        assert_eq!(
            self.apply(&grant_cmd(run, self.epoch, n)),
            ApplyOutcome::Accepted
        );
        let seq = self.run(run).expect("run").next_seq;
        self.apply_full(&finish_cmd(run, (self.epoch, n), seq, status, None))
    }

    fn assert_rebuild_equal(&self) {
        assert!(!self.log.is_empty(), "harness has raw puts");
        let (_dir, store, mut cur) = open_store();
        let mut b = store.batch();
        let mut actions = Vec::new();
        for (e, bytes) in &self.log {
            let off = cur.next;
            apply_command(&mut b, &mut cur, off, Epoch(*e), bytes, &mut actions).expect("apply");
        }
        let mu = cur.meta_update();
        b.commit(&mu).expect("commit");
        assert_eq!(
            store.digest().expect("digest"),
            self.store.digest().expect("digest"),
            "rebuild digest"
        );
        assert_eq!(cur.hash, self.cur.hash, "rebuild rolling hash");
    }
}

fn r(n: u64) -> RunId {
    run_id(OWNER, n)
}

fn invalid(c: InvalidCode) -> ApplyOutcome {
    ApplyOutcome::Rejected(RejectReason::Invalid(c))
}

fn dup(existing: Option<RunId>) -> ApplyOutcome {
    ApplyOutcome::Duplicate { existing }
}

fn started(a: &Applied, run: &RunId) -> bool {
    a.actions
        .iter()
        .any(|x| matches!(x, Action::EnqueueReady { run: r, .. } if r == run))
}

// ---------------------------------------------------------------- validation

#[test]
fn malformed_fires_are_rejected_without_writes() {
    let mut h = H::new();
    let n = h.name.clone();
    let ok = fire_cmd(&n, 5_000, SKIP, C1, &r(1));
    let mut cases: Vec<(&str, ScheduleFire, ApplyOutcome)> = Vec::new();
    let bad = invalid(InvalidCode::BadScheduleFire);

    let mut s = ok.clone();
    s.schedule = String::new();
    cases.push(("empty name", s, bad.clone()));
    let long = "x".repeat(257);
    let mut s = fire_cmd(&long, 5_000, SKIP, C1, &r(1));
    s.schedule = long.clone();
    cases.push(("long name", s, bad.clone()));
    let mut s = fire_cmd(&n, 0, SKIP, C1, &r(1));
    s.fire_time_ms = 0;
    cases.push(("fire time 0", s, bad.clone()));
    let mut s = ok.clone();
    s.start = None;
    cases.push(("fire without start", s, bad.clone()));
    let mut s = advance_cmd(&n, 5_000, 0, C1);
    s.start = ok.start.clone();
    cases.push(("advance with start", s, bad.clone()));
    let mut s = ok.clone();
    s.kind = 9;
    cases.push(("unknown kind", s, bad.clone()));
    let mut s = manual_cmd(&n, 77, b"q", SKIP, C1, &r(1));
    s.request_id.clear();
    cases.push(("manual empty request id", s, bad.clone()));
    let s = manual_cmd(&n, 77, &[1u8; 65], SKIP, C1, &r(1));
    cases.push(("manual long request id", s, bad.clone()));
    let mut s = ok.clone();
    s.overlap = 5;
    cases.push(("overlap out of range", s, bad.clone()));
    let mut s = ok.clone();
    s.start.as_mut().expect("start").idempotency_key = b"__cron/other/5000".to_vec();
    cases.push(("wrong idempotency key", s, bad.clone()));
    let mut s = ok.clone();
    s.start.as_mut().expect("start").idempotency_key = cron_idempotency_key(&n, 5_001).into();
    cases.push(("key of another fire time", s, bad.clone()));
    let mut s = ok.clone();
    s.start.as_mut().expect("start").conflict_policy = 1;
    cases.push(("conflict policy Reject", s, bad.clone()));
    let s = fire_cmd(&foreign_name(OWNER, PC), 5_000, SKIP, C1, &r(1));
    cases.push((
        "foreign schedule",
        s,
        invalid(InvalidCode::ScheduleOwnerMismatch),
    ));
    let s = fire_cmd(&n, 5_000, SKIP, C1, &run_id(OWNER + 1, 1));
    cases.push((
        "run on another partition",
        s,
        ApplyOutcome::Rejected(RejectReason::WrongPartition),
    ));
    let mut s = ok.clone();
    s.start.as_mut().expect("start").run_id = vec![1, 2, 3];
    cases.push(("bad run id", s, invalid(InvalidCode::BadRunId)));

    for (what, s, want) in cases {
        let a = h.fire(s);
        assert_eq!(a.out, want, "{what}");
        assert_eq!((a.reads, a.writes), (0, 0), "{what}: no reads, no writes");
    }
    assert!(h.state_of(&n).is_none());
    assert!(h.run(&r(1)).is_none());
    // The valid fire still works afterwards.
    assert_eq!(h.fire(ok).out, ApplyOutcome::Accepted);
    h.assert_rebuild_equal();
}

// ---------------------------------------------------------------- watermark

#[test]
fn fire_starts_run_and_moves_watermark_atomically() {
    let mut h = H::new();
    let n = h.name.clone();
    let a = h.fire(fire_cmd(&n, 60_000, SKIP, C1, &r(1)));
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert!(started(&a, &r(1)));
    assert_eq!(a.reads, 2, "schedule row + dedupe row");
    let st = h.state();
    assert_eq!(st.schedule_created_at_ms, C1);
    assert_eq!(st.watermark_ms, 60_000);
    assert_eq!(st.last_run, Some(r(1)));
    assert_eq!((st.fired, st.skipped, st.duplicates), (1, 0, 0));
    assert_eq!(
        h.reverse(&r(1)),
        Some(ScheduleRunRow {
            schedule: n.clone(),
            schedule_created_at_ms: C1
        })
    );
    let rec = h.run(&r(1)).expect("run");
    assert_eq!(
        rec.idempotency_key,
        cron_idempotency_key(&n, 60_000).into_bytes()
    );
    assert_eq!(
        h.get(
            Table::Dedupe,
            &dedupe_key("cron-wf", cron_idempotency_key(&n, 60_000).as_bytes())
        ),
        Some(r(1).as_bytes().to_vec())
    );
    h.assert_rebuild_equal();
}

#[test]
fn fires_at_or_below_watermark_are_duplicates() {
    let mut h = H::new();
    let n = h.name.clone();
    assert_eq!(
        h.fire(fire_cmd(&n, 60_000, ALLOW, C1, &r(1))).out,
        ApplyOutcome::Accepted
    );
    // Same fire time again (a restarted driver): Duplicate with the existing run, no start.
    let a = h.fire(fire_cmd(&n, 60_000, ALLOW, C1, &r(2)));
    assert_eq!(a.out, dup(Some(r(1))));
    assert!(!started(&a, &r(2)));
    // An older fire time.
    assert_eq!(
        h.fire(fire_cmd(&n, 30_000, ALLOW, C1, &r(3))).out,
        dup(Some(r(1)))
    );
    // Advance at the watermark.
    assert_eq!(h.fire(advance_cmd(&n, 60_000, 4, C1)).out, dup(None));
    assert!(h.run(&r(2)).is_none() && h.run(&r(3)).is_none());
    let st = h.state();
    assert_eq!(
        (st.watermark_ms, st.fired, st.duplicates, st.skipped),
        (60_000, 1, 3, 0)
    );
    h.assert_rebuild_equal();
}

#[test]
fn watermark_holds_after_the_dedupe_row_expires() {
    let mut h = H::new();
    let n = h.name.clone();
    let mut s = fire_cmd(&n, 60_000, SKIP, C1, &r(1));
    s.start.as_mut().expect("start").retention_ms = 100;
    assert_eq!(h.fire(s).out, ApplyOutcome::Accepted);
    assert_eq!(
        h.finish(&r(1), 1, TerminalStatus::Completed).out,
        ApplyOutcome::Accepted
    );
    assert!(
        h.reverse(&r(1)).is_none(),
        "terminal deletes the reverse row"
    );
    // Retention expiry: the run and its dedupe row are gone.
    let exp = h.apply(&Command::timer_fired(TimerFired {
        run_id: r(1).as_bytes().to_vec(),
        op: EXPIRE_OP,
        due_at_ms: 1_000,
        fired_at_ms: 1_000,
    }));
    assert_eq!(exp, ApplyOutcome::Accepted);
    assert!(h.run(&r(1)).is_none());
    let dk = dedupe_key("cron-wf", cron_idempotency_key(&n, 60_000).as_bytes());
    assert!(h.get(Table::Dedupe, &dk).is_none());
    // The same fire again: still a duplicate (the watermark, not the dedupe row).
    let a = h.fire(fire_cmd(&n, 60_000, SKIP, C1, &r(2)));
    assert_eq!(a.out, dup(Some(r(1))));
    assert!(h.run(&r(2)).is_none());
    h.assert_rebuild_equal();
}

#[test]
fn expiry_removes_a_leftover_reverse_row() {
    let mut h = H::new();
    let n = h.name.clone();
    let mut s = fire_cmd(&n, 60_000, SKIP, C1, &r(1));
    s.start.as_mut().expect("start").retention_ms = 100;
    assert_eq!(h.fire(s).out, ApplyOutcome::Accepted);
    assert_eq!(
        h.finish(&r(1), 1, TerminalStatus::Completed).out,
        ApplyOutcome::Accepted
    );
    // Put a reverse row back (unreachable through apply) and expire the run.
    let row = values::encode(&ScheduleRunRow {
        schedule: n.clone(),
        schedule_created_at_ms: C1,
    });
    h.put(vec![(Table::ScheduleState, schedule_run_key(&r(1)), row)]);
    let exp = h.apply(&Command::timer_fired(TimerFired {
        run_id: r(1).as_bytes().to_vec(),
        op: EXPIRE_OP,
        due_at_ms: 1_000,
        fired_at_ms: 1_000,
    }));
    assert_eq!(exp, ApplyOutcome::Accepted);
    assert!(h.reverse(&r(1)).is_none());
}

#[test]
fn advance_moves_watermark_and_counts_skips() {
    let mut h = H::new();
    let n = h.name.clone();
    assert_eq!(
        h.fire(advance_cmd(&n, 90_000, 5, C1)).out,
        ApplyOutcome::Accepted
    );
    let st = h.state();
    assert_eq!(
        (st.watermark_ms, st.skipped, st.fired, st.last_run),
        (90_000, 5, 0, None)
    );
    // A fire below the advanced watermark is a duplicate without an existing run.
    assert_eq!(h.fire(fire_cmd(&n, 60_000, SKIP, C1, &r(1))).out, dup(None));
    // Advance above.
    assert_eq!(
        h.fire(advance_cmd(&n, 120_000, 2, C1)).out,
        ApplyOutcome::Accepted
    );
    assert_eq!(h.state().watermark_ms, 120_000);
    assert_eq!(h.state().skipped, 7);
    h.assert_rebuild_equal();
}

// ---------------------------------------------------------------- overlap

#[test]
fn overlap_skip_records_a_skip_while_last_run_is_live() {
    let mut h = H::new();
    let n = h.name.clone();
    assert_eq!(
        h.fire(fire_cmd(&n, 1_000, SKIP, C1, &r(1))).out,
        ApplyOutcome::Accepted
    );
    let a = h.fire(fire_cmd(&n, 2_000, SKIP, C1, &r(2)));
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert_eq!(a.reads, 2, "schedule row + last run record");
    assert!(h.run(&r(2)).is_none());
    let st = h.state();
    assert_eq!(
        (st.watermark_ms, st.skipped, st.fired, st.last_run),
        (2_000, 1, 1, Some(r(1)))
    );
    // Once the last run is terminal the next fire starts.
    assert_eq!(
        h.finish(&r(1), 1, TerminalStatus::Completed).out,
        ApplyOutcome::Accepted
    );
    let a = h.fire(fire_cmd(&n, 3_000, SKIP, C1, &r(3)));
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert!(started(&a, &r(3)));
    assert_eq!(h.state().last_run, Some(r(3)));
    h.assert_rebuild_equal();
}

#[test]
fn overlap_buffer_one_starts_the_latest_buffered_fire_on_terminal() {
    let mut h = H::new();
    let n = h.name.clone();
    assert_eq!(
        h.fire(fire_cmd(&n, 1_000, BUFFER, C1, &r(1))).out,
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.fire(fire_cmd(&n, 2_000, BUFFER, C1, &r(2))).out,
        ApplyOutcome::Accepted
    );
    let st = h.state();
    assert_eq!(st.buffered.as_ref().map(|b| b.fire_time_ms), Some(2_000));
    assert_eq!((st.watermark_ms, st.skipped), (2_000, 0));
    // A newer fire replaces the buffered one (counted as skipped).
    assert_eq!(
        h.fire(fire_cmd(&n, 3_000, BUFFER, C1, &r(3))).out,
        ApplyOutcome::Accepted
    );
    let st = h.state();
    assert_eq!(st.buffered.as_ref().map(|b| b.fire_time_ms), Some(3_000));
    assert_eq!((st.watermark_ms, st.skipped, st.fired), (3_000, 1, 1));
    assert!(h.run(&r(2)).is_none() && h.run(&r(3)).is_none());

    // The last run's terminal transition starts the buffered fire in the same batch.
    let a = h.finish(&r(1), 1, TerminalStatus::Failed);
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert!(started(&a, &r(3)));
    assert_eq!(h.status(&r(3)), RunStatus::Ready);
    assert!(h.run(&r(2)).is_none());
    let st = h.state();
    assert_eq!(
        (st.last_run, st.buffered.clone(), st.fired),
        (Some(r(3)), None, 2)
    );
    assert!(h.reverse(&r(1)).is_none());
    assert!(h.reverse(&r(3)).is_some());

    // r3 ends without a buffered fire: nothing else starts, last_run stays (terminal).
    let a = h.finish(&r(3), 1, TerminalStatus::Completed);
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert_eq!(h.state().last_run, Some(r(3)));
    assert!(h.reverse(&r(3)).is_none());
    h.assert_rebuild_equal();
}

#[test]
fn overlap_allow_all_starts_every_fire() {
    let mut h = H::new();
    let n = h.name.clone();
    for (i, ft) in [1_000u64, 2_000, 3_000].into_iter().enumerate() {
        let run = r(i as u64 + 1);
        let a = h.fire(fire_cmd(&n, ft, ALLOW, C1, &run));
        assert_eq!(a.out, ApplyOutcome::Accepted);
        assert!(started(&a, &run));
    }
    for i in 1..=3 {
        assert_eq!(h.status(&r(i)), RunStatus::Ready);
    }
    let st = h.state();
    assert_eq!(
        (st.last_run, st.fired, st.watermark_ms),
        (Some(r(3)), 3, 3_000)
    );
    // Finishing an older run leaves the schedule row alone.
    let before = h.state();
    assert_eq!(
        h.finish(&r(1), 1, TerminalStatus::Completed).out,
        ApplyOutcome::Accepted
    );
    assert_eq!(h.state(), before);
    assert!(h.reverse(&r(1)).is_none());
    h.assert_rebuild_equal();
}

#[test]
fn overlap_cancel_other_cancels_gracefully_then_starts() {
    let mut h = H::new();
    let n = h.name.clone();
    assert_eq!(
        h.fire(fire_cmd(&n, 1_000, CANCEL, C1, &r(1))).out,
        ApplyOutcome::Accepted
    );
    assert_eq!(h.apply(&grant_cmd(&r(1), 1, 1)), ApplyOutcome::Accepted);
    let a = h.fire(fire_cmd(&n, 2_000, CANCEL, C1, &r(2)));
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert!(started(&a, &r(2)));
    assert!(
        a.actions
            .iter()
            .any(|x| matches!(x, Action::CancelRequested { run } if *run == r(1)))
    );
    let old = h.run(&r(1)).expect("run");
    assert!(old.cancel_requested);
    assert_eq!(old.cancel_reason, "schedule_overlap");
    assert!(!old.status.is_terminal(), "graceful: the run ends itself");
    assert_eq!(h.state().last_run, Some(r(2)));
    // When the cancelled run ends, the schedule row is untouched.
    let before = h.state();
    let seq = h.run(&r(1)).expect("run").next_seq;
    let fin = h.apply(&finish_cmd(
        &r(1),
        (1, 1),
        seq,
        TerminalStatus::Cancelled,
        None,
    ));
    assert_eq!(fin, ApplyOutcome::Accepted);
    assert_eq!(h.state(), before);
    assert!(h.reverse(&r(1)).is_none());
    h.assert_rebuild_equal();
}

#[test]
fn overlap_terminate_other_terminates_then_starts() {
    let mut h = H::new();
    let n = h.name.clone();
    assert_eq!(
        h.fire(fire_cmd(&n, 1_000, TERMINATE, C1, &r(1))).out,
        ApplyOutcome::Accepted
    );
    let a = h.fire(fire_cmd(&n, 2_000, TERMINATE, C1, &r(2)));
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert!(started(&a, &r(2)));
    let old = h.run(&r(1)).expect("run");
    assert_eq!(old.status, RunStatus::Terminated);
    assert_eq!(old.terminal_reason, "schedule_overlap");
    assert!(
        h.reverse(&r(1)).is_none(),
        "the terminal hook deleted the old reverse row"
    );
    let st = h.state();
    assert_eq!(
        (st.last_run, st.fired, st.watermark_ms),
        (Some(r(2)), 2, 2_000)
    );
    h.assert_rebuild_equal();
}

#[test]
fn a_started_fire_supersedes_a_buffered_one() {
    let mut h = H::new();
    let n = h.name.clone();
    assert_eq!(
        h.fire(fire_cmd(&n, 1_000, BUFFER, C1, &r(1))).out,
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.fire(fire_cmd(&n, 2_000, BUFFER, C1, &r(2))).out,
        ApplyOutcome::Accepted
    );
    // The policy changes to TerminateOther: the fire starts, the buffered fire is dropped.
    assert_eq!(
        h.fire(fire_cmd(&n, 3_000, TERMINATE, C1, &r(3))).out,
        ApplyOutcome::Accepted
    );
    assert_eq!(h.status(&r(1)), RunStatus::Terminated);
    assert!(h.run(&r(2)).is_none(), "the buffered fire never starts");
    let st = h.state();
    assert_eq!(
        (st.last_run, st.buffered.clone(), st.skipped, st.fired),
        (Some(r(3)), None, 1, 2)
    );
    h.assert_rebuild_equal();
}

// ---------------------------------------------------------------- manual

#[test]
fn manual_fires_ignore_the_watermark_and_dedupe_by_request_id() {
    let mut h = H::new();
    let n = h.name.clone();
    assert_eq!(
        h.fire(advance_cmd(&n, 90_000, 0, C1)).out,
        ApplyOutcome::Accepted
    );
    // Request time below the watermark: still starts; the watermark does not move.
    let a = h.fire(manual_cmd(&n, 500, b"req-1", ALLOW, C1, &r(1)));
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert!(started(&a, &r(1)));
    let st = h.state();
    assert_eq!(
        (st.watermark_ms, st.last_run, st.fired),
        (90_000, Some(r(1)), 1)
    );
    // The same request id again: the existing run (ReturnExisting), counted as a duplicate.
    let a = h.fire(manual_cmd(&n, 600, b"req-1", ALLOW, C1, &r(2)));
    assert_eq!(a.out, dup(Some(r(1))));
    assert!(h.run(&r(2)).is_none());
    assert_eq!(h.state().duplicates, 1);
    assert_eq!(h.state().watermark_ms, 90_000);
    // Overlap applies to manual fires too: Skip while r1 is live.
    let a = h.fire(manual_cmd(&n, 700, b"req-2", SKIP, C1, &r(3)));
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert!(h.run(&r(3)).is_none());
    assert_eq!(h.state().skipped, 1);
    assert_eq!(h.state().watermark_ms, 90_000);
    h.assert_rebuild_equal();
}

// ---------------------------------------------------------------- incarnations (W4)

#[test]
fn delete_while_buffered_does_not_start_the_old_buffered_fire() {
    let mut h = H::new();
    let n = h.name.clone();
    assert_eq!(
        h.fire(fire_cmd(&n, 1_000, BUFFER, C1, &r(1))).out,
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.fire(fire_cmd(&n, 2_000, BUFFER, C1, &r(2))).out,
        ApplyOutcome::Accepted
    );
    assert!(h.state().buffered.is_some());
    // Deleted and re-created (C2): the first fire of the new incarnation resets the row.
    let a = h.fire(fire_cmd(&n, 10_000, BUFFER, C2, &r(3)));
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert!(
        started(&a, &r(3)),
        "the old run is not the new incarnation's last run"
    );
    let st = h.state();
    assert_eq!(st.schedule_created_at_ms, C2);
    assert_eq!(
        (st.watermark_ms, st.last_run, st.buffered.clone()),
        (10_000, Some(r(3)), None)
    );
    assert_eq!((st.fired, st.skipped, st.duplicates), (1, 0, 0));
    // The old last run terminating starts nothing and leaves the new row alone.
    let before = h.state();
    assert_eq!(
        h.finish(&r(1), 1, TerminalStatus::Completed).out,
        ApplyOutcome::Accepted
    );
    assert!(h.run(&r(2)).is_none());
    assert_eq!(h.state(), before);
    assert!(
        h.reverse(&r(1)).is_none(),
        "the stale reverse row is deleted lazily"
    );
    h.assert_rebuild_equal();
}

#[test]
fn recreated_schedule_does_not_skip_for_the_old_incarnations_run() {
    let mut h = H::new();
    let n = h.name.clone();
    assert_eq!(
        h.fire(fire_cmd(&n, 1_000, SKIP, C1, &r(1))).out,
        ApplyOutcome::Accepted
    );
    // Re-created with overlap Skip while r1 still runs: not skipped.
    let a = h.fire(fire_cmd(&n, 5_000, SKIP, C2, &r(2)));
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert!(started(&a, &r(2)));
    assert_eq!(h.state().skipped, 0);
    // r1's terminal transition does not touch the new row.
    let before = h.state();
    assert_eq!(
        h.finish(&r(1), 1, TerminalStatus::Completed).out,
        ApplyOutcome::Accepted
    );
    assert_eq!(h.state(), before);
    h.assert_rebuild_equal();
}

#[test]
fn stale_incarnation_fire_after_reset_is_duplicate() {
    let mut h = H::new();
    let n = h.name.clone();
    assert_eq!(
        h.fire(fire_cmd(&n, 1_000, ALLOW, C1, &r(1))).out,
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.fire(fire_cmd(&n, 2_000, ALLOW, C2, &r(2))).out,
        ApplyOutcome::Accepted
    );
    let before = h.state();
    let a = h.fire(fire_cmd(&n, 3_000, ALLOW, C1, &r(3)));
    assert_eq!(a.out, dup(None));
    assert_eq!(a.writes, 0, "no reset, nothing written");
    assert!(h.run(&r(3)).is_none());
    assert_eq!(h.state(), before);
    h.assert_rebuild_equal();
}

// ---------------------------------------------------------------- continue-as-new

#[test]
fn continue_as_new_clears_last_run() {
    let mut h = H::new();
    let n = h.name.clone();
    assert_eq!(
        h.fire(fire_cmd(&n, 1_000, SKIP, C1, &r(1))).out,
        ApplyOutcome::Accepted
    );
    assert_eq!(h.apply(&grant_cmd(&r(1), 1, 1)), ApplyOutcome::Accepted);
    let can = finish_cmd(
        &r(1),
        (1, 1),
        0,
        TerminalStatus::ContinuedAsNew,
        Some(&r(50)),
    );
    assert_eq!(h.apply(&can), ApplyOutcome::Accepted);
    assert_eq!(h.status(&r(1)), RunStatus::ContinuedAsNew);
    assert_eq!(h.state().last_run, None, "chains are not tracked");
    assert!(h.reverse(&r(1)).is_none());
    // A later Skip fire therefore starts (it may overlap the continuation: documented).
    let a = h.fire(fire_cmd(&n, 2_000, SKIP, C1, &r(2)));
    assert!(started(&a, &r(2)));
    h.assert_rebuild_equal();
}

#[test]
fn continue_as_new_starts_a_buffered_fire() {
    let mut h = H::new();
    let n = h.name.clone();
    assert_eq!(
        h.fire(fire_cmd(&n, 1_000, BUFFER, C1, &r(1))).out,
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.fire(fire_cmd(&n, 2_000, BUFFER, C1, &r(2))).out,
        ApplyOutcome::Accepted
    );
    assert_eq!(h.apply(&grant_cmd(&r(1), 1, 1)), ApplyOutcome::Accepted);
    let can = finish_cmd(
        &r(1),
        (1, 1),
        0,
        TerminalStatus::ContinuedAsNew,
        Some(&r(50)),
    );
    assert_eq!(h.apply(&can), ApplyOutcome::Accepted);
    assert_eq!(h.status(&r(2)), RunStatus::Ready);
    assert_eq!(h.state().last_run, Some(r(2)));
    h.assert_rebuild_equal();
}

// ---------------------------------------------------------------- read counts

#[test]
fn terminal_hook_read_counts() {
    let mut h = H::new();
    let n = h.name.clone();

    // Baseline: a non-schedule run's non-resumable terminal reads record + effects scan (2).
    let plain = r(100);
    let mut s = common::gen_schedule::sched_start(&plain, String::new(), 100);
    s.idempotency_key.clear();
    assert_eq!(h.apply(&Command::start_run(s)), ApplyOutcome::Accepted);
    let a = h.finish(&plain, 1, TerminalStatus::Completed);
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert_eq!(
        a.reads, 2,
        "non-schedule run: no extra read for the schedule hook"
    );

    // Schedule run, no buffered fire: + reverse row + schedule row = 4.
    assert_eq!(
        h.fire(fire_cmd(&n, 1_000, SKIP, C1, &r(1))).out,
        ApplyOutcome::Accepted
    );
    let a = h.finish(&r(1), 1, TerminalStatus::Completed);
    assert_eq!(a.reads, 4);

    // Schedule run with a buffered fire: + the buffered start's dedupe read = 5.
    assert_eq!(
        h.fire(fire_cmd(&n, 2_000, BUFFER, C1, &r(2))).out,
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.fire(fire_cmd(&n, 3_000, BUFFER, C1, &r(3))).out,
        ApplyOutcome::Accepted
    );
    let a = h.finish(&r(2), 1, TerminalStatus::Completed);
    assert!(started(&a, &r(3)));
    assert_eq!(a.reads, 5);

    // A schedule-keyed run whose reverse row is gone (resumed then ended again): + 1.
    assert_eq!(
        h.apply(&Command::terminate_run(
            ostro_types::command::TerminateRun {
                run_id: r(3).as_bytes().to_vec(),
                reason: "ops".into(),
                terminated_at_ms: 10,
            }
        )),
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.apply(&Command::retry_run(ostro_types::command::RetryRun {
            run_id: r(3).as_bytes().to_vec(),
            requested_at_ms: 20,
        })),
        ApplyOutcome::Accepted
    );
    let a = h.finish(&r(3), 2, TerminalStatus::Completed);
    assert_eq!(a.out, ApplyOutcome::Accepted);
    assert_eq!(a.reads, 3, "record + effects scan + absent reverse row");
    h.assert_rebuild_equal();
}

#[test]
fn schedule_fire_kind_is_wire_stable() {
    assert_eq!(ScheduleFireKind::Fire as i32, 0);
    assert_eq!(ScheduleFireKind::Manual as i32, 1);
    assert_eq!(ScheduleFireKind::Advance as i32, 2);
}
