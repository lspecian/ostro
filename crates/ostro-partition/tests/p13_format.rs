//! Phase 13 format contract (plan 13-01; CLUS-07, D-P13-21/22): `RejectReason`
//! `ModuleMismatch` (23, outcome code 39) and `RunBusy` (24, code 40) above the placeholders
//! 16..=22, and apply routing of `Cmd::RepinRun` (43) to `apply/repin.rs`, which rejects with
//! `Invalid(NotImplemented)` without reads or writes until 13-17.

use ostro_partition::StoreError;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, apply_command, fold_hash,
};
use ostro_partition::{PartitionStore, StoreConfig};
use ostro_types::command::{RepinRun, encode_command};
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, RunId};

const P: u16 = 3;

#[test]
fn p13_reject_codes_are_pinned() {
    let codes = [
        (RejectReason::ReservedP7_16, 32),
        (RejectReason::ReservedP7_17, 33),
        (RejectReason::ReservedP8_18, 34),
        (RejectReason::ReservedP8_19, 35),
        (RejectReason::ReservedP9_20, 36),
        (RejectReason::ReservedP9_21, 37),
        (RejectReason::ReservedP9_22, 38),
        (RejectReason::ModuleMismatch, 39),
        (RejectReason::RunBusy, 40),
    ];
    for (r, code) in codes {
        assert_eq!(ApplyOutcome::Rejected(r.clone()).code(), code, "{r:?}");
    }
    // Phase 7's RunPaused is unchanged.
    assert_eq!(ApplyOutcome::Rejected(RejectReason::RunPaused).code(), 31);
}

#[test]
fn newer_format_error_names_the_remedy() {
    let e = StoreError::NewerFormat("unknown Cmd tag 50".into());
    assert!(e.to_string().contains("upgrade this binary"), "{e}");
}

#[test]
fn repin_run_routes_to_the_stub_and_touches_nothing() {
    let dir = ostro_testkit::fs::tempdir();
    let (store, meta) = PartitionStore::open(
        dir.path(),
        PartitionId(P),
        PartitionCount::new(16).expect("non-zero"),
        &StoreConfig::default(),
    )
    .expect("open store");
    let mut cur = Cursor::from_meta(&meta);
    let mut apply = |c: &Command| {
        let bytes = encode_command(c);
        let mut b = store.batch();
        let mut actions: Vec<Action> = Vec::new();
        let off = cur.next;
        let prev = cur.hash;
        let out =
            apply_command(&mut b, &mut cur, off, Epoch(1), &bytes, &mut actions).expect("apply");
        assert_eq!(cur.hash, fold_hash(&prev, off, out.code(), &bytes));
        let (reads, writes) = (b.reads(), b.len());
        let mu = cur.meta_update();
        b.commit(&mu).expect("commit");
        (out, actions, reads, writes)
    };
    assert_eq!(
        apply(&Command::leader_announced(1, 1, 0)).0,
        ApplyOutcome::Accepted
    );
    let run = RunId::new_v7(PartitionId(P));
    let (out, actions, reads, writes) = apply(&Command::repin_run(RepinRun {
        run_id: run.as_bytes().to_vec(),
        from_sha: vec![0xaa; 32],
        to_sha: vec![0xbb; 32],
        expected_next_seq: 5,
        requested_at_ms: 1,
    }));
    assert_eq!(
        out,
        ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::NotImplemented))
    );
    assert!(actions.is_empty());
    assert_eq!((reads, writes), (0, 0));
}
