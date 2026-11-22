//! Phase 11 format contract (plan 11-02; OPS-02, OPS-04): `InvalidCode::BadRerunPrefix` (29)
//! and `BadDlqUpdate` (30) above the canonical placeholders, and apply routing of
//! `Cmd::DlqUpdate` (45) to `apply/dlq.rs` (semantics: 11-08, `tests/apply_dlq.rs`): on a run
//! that is not failed, or an unknown run, it is rejected without touching state.

use bytes::Bytes;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, apply_command, fold_hash,
};
use ostro_partition::store::keys::run_key;
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{ConflictPolicy, DlqUpdate, StartRun, dlq_update, encode_command};
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

fn dlq_ops(id: &RunId) -> Vec<Command> {
    let ops = [
        dlq_update::Op::HookStarted(dlq_update::HookStarted {
            hook_run_id: rid(9).as_bytes().to_vec(),
        }),
        dlq_update::Op::Dismissed(dlq_update::Dismissed {
            reason: "known bad input".into(),
            at_ms: 500,
        }),
        dlq_update::Op::Requeued(dlq_update::Requeued {
            mode: dlq_update::REQUEUE_RESTART,
            new_run_id: rid(8).as_bytes().to_vec(),
            at_ms: 600,
        }),
    ];
    ops.into_iter()
        .map(|op| {
            Command::dlq_update(DlqUpdate {
                run_id: id.as_bytes().to_vec(),
                op: Some(op),
                episode_completed_at_ms: 400,
            })
        })
        .collect()
}

#[test]
fn phase11_invalid_codes_sit_at_29_and_30() {
    assert_eq!(InvalidCode::BadRerunPrefix as usize, 29);
    assert_eq!(InvalidCode::BadDlqUpdate as usize, 30);
    // The canonical never-emitted placeholders fill 24..=28 (RESERVATIONS §8, P-15).
    assert_eq!(InvalidCode::ReservedP8_24 as usize, 24);
    assert_eq!(InvalidCode::ReservedP9_25 as usize, 25);
    assert_eq!(InvalidCode::ReservedP9_28 as usize, 28);
    // The detail is not part of the outcome code: every Invalid is 16 + 10.
    for c in [InvalidCode::BadRerunPrefix, InvalidCode::BadDlqUpdate] {
        assert_eq!(ApplyOutcome::Rejected(RejectReason::Invalid(c)).code(), 26);
    }
}

#[test]
fn dlq_update_routes_to_apply_dlq_and_rejections_change_nothing() {
    let mut h = H::new();
    let r = rid(1);
    assert_eq!(h.apply(&start(&r)).out, ApplyOutcome::Accepted);
    let row = h.run_row(&r);
    let bad = ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::BadDlqUpdate));
    let ops = dlq_ops(&r).into_iter().map(|c| (c, bad.clone())).chain(
        dlq_ops(&rid(2))
            .into_iter()
            .map(|c| (c, ApplyOutcome::Rejected(RejectReason::RunNotFound))),
    );
    for (c, want) in ops {
        let c = &c;
        let prev = h.cur.hash;
        let off = h.cur.next;
        let a = h.apply(c);
        assert_eq!(a.out, want, "{c:?}");
        // No overlay entry: only the cursor (applied offset, rolling hash) moves, as for every
        // rejection.
        assert_eq!(a.writes, 0, "a rejection writes nothing");
        assert_eq!(a.reads, 1, "the run record only");
        assert!(a.actions.is_empty());
        assert_eq!(
            h.cur.hash,
            fold_hash(&prev, off, want.code(), &encode_command(c)),
            "folded into the rolling hash"
        );
    }
    assert_eq!(h.run_row(&r), row, "run record untouched");
}
