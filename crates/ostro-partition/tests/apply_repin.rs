//! `Cmd::RepinRun` apply (plan 13-17; CLUS-07, D-P13-21): accept on a non-terminal,
//! unleased run pinned to `from_sha` whose `next_seq` equals `expected_next_seq` (only
//! `RunRecord.module_sha` changes); reject unknown / terminal runs, a sha mismatch
//! (`ModuleMismatch`), a moved journal (`SeqMismatch`), a leased or hot-parked run
//! (`RunBusy`) and malformed fields, with nothing written. Determinism: a random command
//! mix including repins rebuilds to the same digest on a fresh store (the dedicated
//! proptest; the shared `rebuild_digest` generator lives outside this plan's files).

use bytes::Bytes;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, apply_command, fold_hash,
};
use ostro_partition::store::keys::{event_key, run_key, running_key};
use ostro_partition::store::values::{self, RunRecord, RunStatus};
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, CompleteRun, ConflictPolicy, Derived, EventsAppended,
    JournalEvent, PauseRun, RepinRun, StartRun, TerminalStatus, derived::Intent, encode_command,
};
use ostro_types::journal::EventKind;
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};
use proptest::prelude::*;

const P: u16 = 3;
const V1: [u8; 32] = [0xa1; 32];
const V2: [u8; 32] = [0xb2; 32];
const V3: [u8; 32] = [0xc3; 32];

struct H {
    _dir: tempfile::TempDir,
    store: PartitionStore,
    cur: Cursor,
    log: Vec<Vec<u8>>,
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

/// (outcome, actions, reads, writes) of one applied command.
type Applied = (ApplyOutcome, Vec<Action>, u64, usize);

impl H {
    fn new() -> Self {
        let (dir, store, cur) = open_store();
        let mut h = H {
            _dir: dir,
            store,
            cur,
            log: Vec::new(),
        };
        assert_eq!(
            h.apply(&Command::leader_announced(1, 1, 0)),
            ApplyOutcome::Accepted
        );
        h
    }

    fn apply_full(&mut self, c: &Command) -> Applied {
        let bytes = encode_command(c);
        self.log.push(bytes.to_vec());
        let mut b = self.store.batch();
        let mut actions = Vec::new();
        let off = self.cur.next;
        let prev = self.cur.hash;
        let out = apply_command(&mut b, &mut self.cur, off, Epoch(1), &bytes, &mut actions)
            .expect("apply");
        // The outcome code (39 / 40 for the Phase 13 reasons) is folded into the hash.
        assert_eq!(self.cur.hash, fold_hash(&prev, off, out.code(), &bytes));
        let (reads, writes) = (b.reads(), b.len());
        let mu = self.cur.meta_update();
        b.commit(&mu).expect("commit");
        (out, actions, reads, writes)
    }

    fn apply(&mut self, c: &Command) -> ApplyOutcome {
        self.apply_full(c).0
    }

    fn run(&self, id: &RunId) -> RunRecord {
        self.store
            .reader()
            .get(Table::Runs, &run_key(id))
            .expect("read")
            .map(|v| values::decode(&v).expect("decode"))
            .expect("run exists")
    }

    fn has_running_row(&self, id: &RunId) -> bool {
        self.store
            .reader()
            .get(Table::Running, &running_key(id))
            .expect("read")
            .is_some()
    }

    fn event_rows(&self, id: &RunId) -> usize {
        (0..)
            .take_while(|seq| {
                self.store
                    .reader()
                    .get(Table::Events, &event_key(id, *seq))
                    .expect("read")
                    .is_some()
            })
            .count()
    }

    fn digest(&self) -> [u8; 32] {
        self.store.digest().expect("digest")
    }

    /// Every logged command applied again to a fresh store reproduces the live digest and
    /// the rolling hash.
    fn assert_rebuild_equal(&self) {
        let (_dir, store, mut cur) = open_store();
        for bytes in &self.log {
            let mut b = store.batch();
            let mut actions = Vec::new();
            let off = cur.next;
            apply_command(&mut b, &mut cur, off, Epoch(1), bytes, &mut actions).expect("apply");
            let mu = cur.meta_update();
            b.commit(&mu).expect("commit");
        }
        assert_eq!(cur.hash, self.cur.hash, "rolling hash");
        assert_eq!(
            store.digest().expect("digest"),
            self.digest(),
            "rebuild digest"
        );
    }

    // ---- run lifecycle helpers ----

    fn start(&mut self, id: &RunId, module: Option<[u8; 32]>) {
        let s = StartRun {
            run_id: id.as_bytes().to_vec(),
            workflow: "wf".into(),
            workflow_version: 1,
            queue: "q".into(),
            input: Some(inline(b"in")),
            conflict_policy: ConflictPolicy::ReturnExisting as i32,
            ready_at_ms: 100,
            requested_at_ms: 100,
            rng_seed: vec![7; 32],
            run_started: Some(ev(0, EventKind::RunStarted)),
            module_sha: module.map(|m| m.to_vec()).unwrap_or_default(),
            ..Default::default()
        };
        assert_eq!(self.apply(&Command::start_run(s)), ApplyOutcome::Accepted);
    }

    fn grant(&mut self, id: &RunId, n: u32) -> ApplyOutcome {
        self.apply(&Command::attempt_granted(AttemptGranted {
            run_id: id.as_bytes().to_vec(),
            token: Some(AttemptTokenMsg { epoch: 1, n }),
            after_failure: false,
        }))
    }

    /// One step event at the journal end, optionally suspending (`Running` → `Scheduled`).
    fn step(&mut self, id: &RunId, n: u32, suspend: bool) -> ApplyOutcome {
        let from = self.run(id).next_seq;
        self.apply(&Command::events_appended(EventsAppended {
            run_id: id.as_bytes().to_vec(),
            token: Some(AttemptTokenMsg { epoch: 1, n }),
            from_seq: from,
            events: vec![ev(from, EventKind::StepCompleted)],
            derived: if suspend {
                vec![Derived::suspend()]
            } else {
                vec![]
            },
        }))
    }

    fn complete(&mut self, id: &RunId, n: u32) -> ApplyOutcome {
        let from = self.run(id).next_seq;
        self.apply(&Command::events_appended(EventsAppended {
            run_id: id.as_bytes().to_vec(),
            token: Some(AttemptTokenMsg { epoch: 1, n }),
            from_seq: from,
            events: vec![],
            derived: vec![Derived::new(Intent::CompleteRun(CompleteRun {
                status: TerminalStatus::Completed as i32,
                result: Some(inline(b"ok")),
                completed_at_ms: 200,
                resumable: false,
                parent_event: None,
            }))],
        }))
    }

    /// A run journaled to `next_seq == 2` and suspended (Scheduled, unleased) on `module`.
    fn suspended(&mut self, id: &RunId, module: [u8; 32]) {
        self.start(id, Some(module));
        assert_eq!(self.grant(id, 1), ApplyOutcome::Accepted);
        assert_eq!(self.step(id, 1, true), ApplyOutcome::Accepted);
        let r = self.run(id);
        assert_eq!((r.status, r.next_seq), (RunStatus::Scheduled, 2));
        assert!(!self.has_running_row(id));
    }

    fn repin(&mut self, id: &RunId, from: [u8; 32], to: [u8; 32], expected: u64) -> Applied {
        self.apply_full(&repin_cmd(id, from.to_vec(), to.to_vec(), expected))
    }
}

fn repin_cmd(id: &RunId, from: Vec<u8>, to: Vec<u8>, expected: u64) -> Command {
    Command::repin_run(RepinRun {
        run_id: id.as_bytes().to_vec(),
        from_sha: from,
        to_sha: to,
        expected_next_seq: expected,
        requested_at_ms: 500,
    })
}

fn inline(b: &[u8]) -> PayloadRef {
    PayloadRef::inline(Bytes::copy_from_slice(b))
}

fn ev(seq: u64, kind: EventKind) -> JournalEvent {
    JournalEvent {
        seq,
        kind: kind.code(),
        payload: Some(inline(format!("e{seq}").as_bytes())),
        ..Default::default()
    }
}

fn rid(n: u8) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&P.to_be_bytes());
    b[17] = n;
    RunId::from_bytes(&b).expect("18 bytes")
}

fn rejected(r: RejectReason) -> ApplyOutcome {
    ApplyOutcome::Rejected(r)
}

#[test]
fn accept_changes_only_module_sha() {
    let mut h = H::new();
    let r = rid(1);
    h.suspended(&r, V1);
    let before = h.run(&r);
    let rows = h.event_rows(&r);
    let (out, actions, reads, writes) = h.repin(&r, V1, V2, 2);
    assert_eq!(out, ApplyOutcome::Accepted);
    assert!(actions.is_empty(), "no action: {actions:?}");
    assert_eq!(
        (reads, writes),
        (1, 1),
        "one read, one write (the run record)"
    );
    let after = h.run(&r);
    assert_eq!(after.module_sha, Some(V2));
    let mut expected = before.clone();
    expected.module_sha = Some(V2);
    assert_eq!(after, expected, "nothing else in the record changed");
    assert_eq!(h.event_rows(&r), rows, "journal untouched");
    // Repinning again from the old sha is now a mismatch; from the new one it works.
    assert_eq!(
        h.repin(&r, V1, V3, 2).0,
        rejected(RejectReason::ModuleMismatch)
    );
    assert_eq!(h.repin(&r, V2, V3, 2).0, ApplyOutcome::Accepted);
    assert_eq!(h.run(&r).module_sha, Some(V3));
    h.assert_rebuild_equal();
}

#[test]
fn ready_and_paused_runs_accept() {
    let mut h = H::new();
    // Ready (started, never granted).
    let ready = rid(2);
    h.start(&ready, Some(V1));
    assert_eq!(h.run(&ready).status, RunStatus::Ready);
    assert_eq!(h.repin(&ready, V1, V2, 1).0, ApplyOutcome::Accepted);
    assert_eq!(h.run(&ready).module_sha, Some(V2));
    assert!(h.run(&ready).ready.is_some(), "ready row kept");
    // Paused and suspended: the pause mark stays.
    let paused = rid(3);
    h.suspended(&paused, V1);
    let pause = PauseRun {
        run_id: paused.as_bytes().to_vec(),
        reason: ostro_types::command::PAUSE_REASON_OPERATOR,
        requested_at_ms: 300,
        ..Default::default()
    };
    assert_eq!(h.apply(&Command::pause_run(pause)), ApplyOutcome::Accepted);
    assert_eq!(h.repin(&paused, V1, V2, 2).0, ApplyOutcome::Accepted);
    let rec = h.run(&paused);
    assert_eq!(rec.module_sha, Some(V2));
    assert!(rec.pause.is_some());
    h.assert_rebuild_equal();
}

#[test]
fn unknown_and_terminal_runs_reject() {
    let mut h = H::new();
    let (out, _, reads, writes) = h.repin(&rid(9), V1, V2, 0);
    assert_eq!(out, rejected(RejectReason::RunNotFound));
    assert_eq!((reads, writes), (1, 0));
    let done = rid(4);
    h.start(&done, Some(V1));
    assert_eq!(h.grant(&done, 1), ApplyOutcome::Accepted);
    assert_eq!(h.complete(&done, 1), ApplyOutcome::Accepted);
    assert!(h.run(&done).status.is_terminal());
    let next = h.run(&done).next_seq;
    let (out, _, _, writes) = h.repin(&done, V1, V2, next);
    assert_eq!(out, rejected(RejectReason::RunTerminal));
    assert_eq!(writes, 0);
    assert_eq!(h.run(&done).module_sha, Some(V1));
    h.assert_rebuild_equal();
}

#[test]
fn sha_mismatch_rejects_module_mismatch() {
    let mut h = H::new();
    let r = rid(5);
    h.suspended(&r, V1);
    let (out, _, _, writes) = h.repin(&r, V3, V2, 2);
    assert_eq!(out, rejected(RejectReason::ModuleMismatch));
    assert_eq!(out.code(), 39);
    assert_eq!(writes, 0);
    assert_eq!(h.run(&r).module_sha, Some(V1));
    // A run with no pin never matches.
    let unpinned = rid(6);
    h.start(&unpinned, None);
    assert_eq!(
        h.repin(&unpinned, V1, V2, 1).0,
        rejected(RejectReason::ModuleMismatch)
    );
    h.assert_rebuild_equal();
}

#[test]
fn moved_journal_rejects_seq_mismatch() {
    let mut h = H::new();
    let r = rid(7);
    h.suspended(&r, V1);
    // The replay saw 2 events; an input / activation moved the journal to 3.
    assert_eq!(h.grant(&r, 2), ApplyOutcome::Accepted);
    assert_eq!(h.step(&r, 2, true), ApplyOutcome::Accepted);
    assert_eq!(h.run(&r).next_seq, 3);
    let (out, _, _, writes) = h.repin(&r, V1, V2, 2);
    assert_eq!(out, rejected(RejectReason::SeqMismatch { expected: 3 }));
    assert_eq!(writes, 0);
    assert_eq!(h.run(&r).module_sha, Some(V1));
    // A stale expectation from the future is refused the same way.
    assert_eq!(
        h.repin(&r, V1, V2, 9).0,
        rejected(RejectReason::SeqMismatch { expected: 3 })
    );
    // Re-checked at the new length: accepted.
    assert_eq!(h.repin(&r, V1, V2, 3).0, ApplyOutcome::Accepted);
    h.assert_rebuild_equal();
}

#[test]
fn leased_run_rejects_run_busy() {
    let mut h = H::new();
    let r = rid(8);
    h.suspended(&r, V1);
    assert_eq!(h.grant(&r, 2), ApplyOutcome::Accepted);
    assert_eq!(h.run(&r).status, RunStatus::Running);
    assert!(h.has_running_row(&r), "leased: running row");
    let (out, _, _, writes) = h.repin(&r, V1, V2, 2);
    assert_eq!(out, rejected(RejectReason::RunBusy));
    assert_eq!(out.code(), 40);
    assert_eq!(writes, 0);
    // Once the activation suspends (no running row), the same repin is accepted.
    assert_eq!(h.step(&r, 2, true), ApplyOutcome::Accepted);
    assert!(!h.has_running_row(&r));
    assert_eq!(h.repin(&r, V1, V2, 3).0, ApplyOutcome::Accepted);
    h.assert_rebuild_equal();
}

/// Plan check W15: a hot-parked activation (Phase 3 hot wait) flushed its intents without
/// a `Suspend` and waits in process; its run stays `Running` with its `running` lease row,
/// so a repin is `RunBusy` (the migrate service needs no eviction; the next migrate after
/// the eviction succeeds).
#[test]
fn hot_parked_run_is_busy() {
    let mut h = H::new();
    let r = rid(10);
    h.start(&r, Some(V1));
    assert_eq!(h.grant(&r, 1), ApplyOutcome::Accepted);
    // The step completed and was flushed; the engine parked on a wait (no Suspend).
    assert_eq!(h.step(&r, 1, false), ApplyOutcome::Accepted);
    let rec = h.run(&r);
    assert_eq!((rec.status, rec.next_seq), (RunStatus::Running, 2));
    assert!(h.has_running_row(&r), "the parked run keeps its lease row");
    assert_eq!(h.repin(&r, V1, V2, 2).0, rejected(RejectReason::RunBusy));
    // Hot wait elapsed: the executor suspends at the journal end; now it can move.
    let from = h.run(&r).next_seq;
    let out = h.apply(&Command::events_appended(EventsAppended {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n: 1 }),
        from_seq: from,
        events: vec![],
        derived: vec![Derived::suspend()],
    }));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(h.repin(&r, V1, V2, 2).0, ApplyOutcome::Accepted);
    h.assert_rebuild_equal();
}

#[test]
fn malformed_fields_reject_invalid() {
    let mut h = H::new();
    let r = rid(11);
    h.suspended(&r, V1);
    let cases = [
        (
            Command::repin_run(RepinRun {
                run_id: vec![1, 2, 3],
                from_sha: V1.to_vec(),
                to_sha: V2.to_vec(),
                expected_next_seq: 2,
                requested_at_ms: 1,
            }),
            rejected(RejectReason::Invalid(InvalidCode::BadRunId)),
        ),
        (
            repin_cmd(&r, vec![], V2.to_vec(), 2),
            rejected(RejectReason::Invalid(InvalidCode::MissingField)),
        ),
        (
            repin_cmd(&r, V1.to_vec(), vec![], 2),
            rejected(RejectReason::Invalid(InvalidCode::MissingField)),
        ),
        (
            repin_cmd(&r, V1[..31].to_vec(), V2.to_vec(), 2),
            rejected(RejectReason::Invalid(InvalidCode::BadLength)),
        ),
        (
            repin_cmd(&r, V1.to_vec(), [0u8; 33].to_vec(), 2),
            rejected(RejectReason::Invalid(InvalidCode::BadLength)),
        ),
    ];
    for (cmd, want) in cases {
        let (out, actions, reads, writes) = h.apply_full(&cmd);
        assert_eq!(out, want, "{cmd:?}");
        assert!(actions.is_empty());
        assert_eq!(
            (reads, writes),
            (0, 0),
            "validation before the read: {cmd:?}"
        );
    }
    // Another partition's run.
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&(P + 1).to_be_bytes());
    b[17] = 1;
    let foreign = RunId::from_bytes(&b).expect("18 bytes");
    assert_eq!(
        h.repin(&foreign, V1, V2, 0).0,
        rejected(RejectReason::WrongPartition)
    );
    assert_eq!(h.run(&r).module_sha, Some(V1));
    h.assert_rebuild_equal();
}

#[test]
fn same_log_same_digest_on_two_stores() {
    let mut a = H::new();
    let mut b = H::new();
    for h in [&mut a, &mut b] {
        h.suspended(&rid(1), V1);
        h.start(&rid(2), Some(V1));
        assert_eq!(h.grant(&rid(2), 1), ApplyOutcome::Accepted);
        h.repin(&rid(1), V1, V2, 2);
        h.repin(&rid(2), V1, V2, 1);
        h.repin(&rid(1), V1, V3, 2);
        h.repin(&rid(3), V1, V3, 0);
    }
    assert_eq!(a.digest(), b.digest());
    assert_eq!(a.cur.hash, b.cur.hash);
}

// ---- determinism proptest ----

#[derive(Clone, Debug)]
enum Op {
    Start {
        run: u8,
        module: u8,
    },
    Grant {
        run: u8,
    },
    Step {
        run: u8,
        suspend: bool,
    },
    Complete {
        run: u8,
    },
    Repin {
        run: u8,
        from: u8,
        to: u8,
        seq_delta: i8,
    },
}

const MODULES: [[u8; 32]; 3] = [V1, V2, V3];

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0u8..4, 0u8..3).prop_map(|(run, module)| Op::Start { run, module }),
        (0u8..4).prop_map(|run| Op::Grant { run }),
        (0u8..4, any::<bool>()).prop_map(|(run, suspend)| Op::Step { run, suspend }),
        (0u8..4).prop_map(|run| Op::Complete { run }),
        (0u8..5, 0u8..3, 0u8..3, -1i8..=1).prop_map(|(run, from, to, seq_delta)| Op::Repin {
            run,
            from,
            to,
            seq_delta
        }),
    ]
}

/// Applies `ops` (commands whose preconditions are not met are applied anyway: their
/// rejections are part of the log). Returns the number of accepted repins.
fn drive(h: &mut H, ops: &[Op]) -> usize {
    let mut tokens = [0u32; 5];
    let mut repinned = 0;
    for o in ops {
        match *o {
            Op::Start { run, module } => {
                let id = rid(run + 1);
                let s = StartRun {
                    run_id: id.as_bytes().to_vec(),
                    workflow: "wf".into(),
                    workflow_version: 1,
                    queue: "q".into(),
                    input: Some(inline(b"in")),
                    conflict_policy: ConflictPolicy::ReturnExisting as i32,
                    ready_at_ms: 100,
                    requested_at_ms: 100,
                    rng_seed: vec![7; 32],
                    run_started: Some(ev(0, EventKind::RunStarted)),
                    module_sha: MODULES[usize::from(module)].to_vec(),
                    ..Default::default()
                };
                h.apply(&Command::start_run(s));
            }
            Op::Grant { run } => {
                let i = usize::from(run);
                tokens[i] += 1;
                h.grant(&rid(run + 1), tokens[i]);
            }
            Op::Step { run, suspend } => {
                let id = rid(run + 1);
                let from = run_next(h, &id);
                h.apply(&Command::events_appended(EventsAppended {
                    run_id: id.as_bytes().to_vec(),
                    token: Some(AttemptTokenMsg {
                        epoch: 1,
                        n: tokens[usize::from(run)],
                    }),
                    from_seq: from,
                    events: vec![ev(from, EventKind::StepCompleted)],
                    derived: if suspend {
                        vec![Derived::suspend()]
                    } else {
                        vec![]
                    },
                }));
            }
            Op::Complete { run } => {
                let id = rid(run + 1);
                let from = run_next(h, &id);
                h.apply(&Command::events_appended(EventsAppended {
                    run_id: id.as_bytes().to_vec(),
                    token: Some(AttemptTokenMsg {
                        epoch: 1,
                        n: tokens[usize::from(run)],
                    }),
                    from_seq: from,
                    events: vec![],
                    derived: vec![Derived::new(Intent::CompleteRun(CompleteRun {
                        status: TerminalStatus::Completed as i32,
                        result: Some(inline(b"ok")),
                        completed_at_ms: 200,
                        resumable: false,
                        parent_event: None,
                    }))],
                }));
            }
            Op::Repin {
                run,
                from,
                to,
                seq_delta,
            } => {
                let id = rid(run + 1);
                let expected = run_next(h, &id).saturating_add_signed(i64::from(seq_delta));
                let before = existing(h, &id);
                let (out, actions, _, _) = h.repin(
                    &id,
                    MODULES[usize::from(from)],
                    MODULES[usize::from(to)],
                    expected,
                );
                assert!(actions.is_empty());
                let after = existing(h, &id);
                if out == ApplyOutcome::Accepted {
                    repinned += 1;
                    let (b, a) = (before.expect("accepted on a run"), after.expect("run"));
                    assert_eq!(b.module_sha, Some(MODULES[usize::from(from)]));
                    assert_eq!(b.next_seq, expected);
                    assert!(!b.status.is_terminal() && b.status != RunStatus::Running);
                    let mut want = b;
                    want.module_sha = Some(MODULES[usize::from(to)]);
                    assert_eq!(a, want);
                } else {
                    assert_eq!(before, after, "a rejected repin wrote: {out:?}");
                }
            }
        }
    }
    repinned
}

fn existing(h: &H, id: &RunId) -> Option<RunRecord> {
    h.store
        .reader()
        .get(Table::Runs, &run_key(id))
        .expect("read")
        .map(|v| values::decode(&v).expect("decode"))
}

fn run_next(h: &H, id: &RunId) -> u64 {
    existing(h, id).map_or(0, |r| r.next_seq)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// A random mix of lifecycle commands and repins: every accepted repin met all its
    /// preconditions and changed only the pin, every rejection wrote nothing, and the log
    /// rebuilds to the same digest and rolling hash on a fresh store.
    #[test]
    fn repin_mix_rebuilds_to_the_same_digest(ops in prop::collection::vec(op(), 1..40)) {
        let mut h = H::new();
        drive(&mut h, &ops);
        h.assert_rebuild_equal();
    }
}
