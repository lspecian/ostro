//! Phase 7 format contract (plan 07-03; OPS-01, D-W18): `RejectReason::RunPaused` at
//! declaration index 15 (outcome code 31) with the `ReservedP6_12..14` placeholders, the
//! in-memory `Action::RunPaused` / `Action::RunResumed`, and apply routing of `Cmd::PauseRun`
//! (20) / `Cmd::ResumeRun` (21) to `apply/pause.rs` (stubs in 07-03, implemented by 07-04).

use bytes::Bytes;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, apply_command, fold_hash,
};
use ostro_partition::store::keys::run_key;
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, ConflictPolicy, PAUSE_REASON_OPERATOR,
    PAUSE_REASON_RETRIES_EXHAUSTED, PauseRun, ResumeRun, StartRun, encode_command,
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
        assert_eq!(
            h.apply(&Command::leader_announced(1, 1, 0)).out,
            ApplyOutcome::Accepted
        );
        h
    }

    fn apply_at(&mut self, epoch: u32, c: &Command) -> Applied {
        let bytes = encode_command(c);
        let mut b = self.store.batch();
        let mut actions = Vec::new();
        let off = self.cur.next;
        let out = apply_command(
            &mut b,
            &mut self.cur,
            off,
            Epoch(epoch),
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

    fn apply(&mut self, c: &Command) -> Applied {
        self.apply_at(1, c)
    }

    fn run_row(&self, id: &RunId) -> Option<Vec<u8>> {
        self.store
            .reader()
            .get(Table::Runs, &run_key(id))
            .expect("read")
    }
}

fn rid(n: u8) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&P.to_be_bytes());
    b[17] = n;
    RunId::from_bytes(&b).expect("18 bytes")
}

fn start(id: &RunId) -> Command {
    Command::start_run(StartRun {
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
    })
}

fn operator_pause(id: &RunId) -> Command {
    Command::pause_run(PauseRun {
        run_id: id.as_bytes().to_vec(),
        reason: PAUSE_REASON_OPERATOR,
        detail: "maintenance".into(),
        token: None,
        requested_at_ms: 200,
    })
}

fn auto_pause(id: &RunId) -> Command {
    Command::pause_run(PauseRun {
        run_id: id.as_bytes().to_vec(),
        reason: PAUSE_REASON_RETRIES_EXHAUSTED,
        detail: "retries exhausted".into(),
        token: Some(AttemptTokenMsg { epoch: 1, n: 1 }),
        requested_at_ms: 201,
    })
}

fn resume(id: &RunId) -> Command {
    Command::resume_run(ResumeRun {
        run_id: id.as_bytes().to_vec(),
        requested_at_ms: 300,
    })
}

const NOT_IMPLEMENTED: ApplyOutcome =
    ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::NotImplemented));

#[test]
fn run_paused_is_reject_index_15_outcome_code_31() {
    // Outcome code = 16 + declaration index (RESERVATIONS §7).
    assert_eq!(ApplyOutcome::Rejected(RejectReason::RunPaused).code(), 31);
    assert_eq!(
        ApplyOutcome::Rejected(RejectReason::RunPaused).code() - 16,
        15
    );
    // Phase 6's unused slots 12..=14 hold the canonical never-emitted placeholders.
    assert_eq!(
        ApplyOutcome::Rejected(RejectReason::ReservedP6_12).code(),
        28
    );
    assert_eq!(
        ApplyOutcome::Rejected(RejectReason::ReservedP6_13).code(),
        29
    );
    assert_eq!(
        ApplyOutcome::Rejected(RejectReason::ReservedP6_14).code(),
        30
    );
    // Earlier codes are unchanged.
    assert_eq!(
        ApplyOutcome::Rejected(RejectReason::RunCompleted).code(),
        27
    );
    assert_eq!(
        ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::NotImplemented)).code(),
        26
    );
}

#[test]
fn pause_actions_exist() {
    let r = rid(1);
    let a = [Action::RunPaused { run: r }, Action::RunResumed { run: r }];
    match &a[0] {
        Action::RunPaused { run } => assert_eq!(*run, r),
        other => panic!("unexpected {other:?}"),
    }
    match &a[1] {
        Action::RunResumed { run } => assert_eq!(*run, r),
        other => panic!("unexpected {other:?}"),
    }
    assert_ne!(a[0], a[1]);
}

#[test]
fn pause_and_resume_route_to_pause_rs() {
    // 07-04 replaced the NotImplemented stubs: both commands reach `apply/pause.rs`, read the
    // run record once and are folded into the hash with their real outcome code. The full
    // semantics are in tests/pause.rs.
    let mut h = H::new();
    let r = rid(1);
    assert_eq!(h.apply(&start(&r)).out, ApplyOutcome::Accepted);
    let grant = Command::attempt_granted(AttemptGranted {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n: 1 }),
        after_failure: false,
    });
    assert_eq!(h.apply(&grant).out, ApplyOutcome::Accepted);

    for (c, action) in [
        (operator_pause(&r), Action::RunPaused { run: r }),
        (resume(&r), Action::RunResumed { run: r }),
        (auto_pause(&r), Action::RunPaused { run: r }),
    ] {
        let kind = c.kind_name();
        let prev_hash = h.cur.hash;
        let off = h.cur.next;
        let a = h.apply(&c);
        assert_eq!(a.out, ApplyOutcome::Accepted, "{kind}");
        assert!(a.actions.contains(&action), "{kind}");
        assert_eq!(a.reads, 1, "{kind}: one run-record read");
        assert!(a.writes > 0, "{kind}");
        assert_eq!(h.cur.next, off.next(), "{kind}");
        assert_eq!(
            h.cur.hash,
            fold_hash(
                &prev_hash,
                off,
                ApplyOutcome::Accepted.code(),
                &encode_command(&c)
            ),
            "{kind}"
        );
    }
    assert!(h.run_row(&r).is_some());

    assert_eq!(
        h.apply(&operator_pause(&rid(9))).out,
        ApplyOutcome::Rejected(RejectReason::RunNotFound)
    );
    assert_eq!(
        h.apply(&resume(&rid(9))).out,
        ApplyOutcome::Rejected(RejectReason::RunNotFound)
    );
    assert_ne!(
        h.apply(&resume(&rid(9))).out,
        NOT_IMPLEMENTED,
        "the stubs are gone"
    );
}

#[test]
fn pause_and_resume_pass_the_epoch_gate_first() {
    let mut h = H::new();
    let r = rid(2);
    for c in [operator_pause(&r), resume(&r)] {
        let a = h.apply_at(2, &c);
        assert_eq!(
            a.out,
            ApplyOutcome::Rejected(RejectReason::StaleEpoch { current: Epoch(1) }),
            "{}",
            c.kind_name()
        );
    }
}
