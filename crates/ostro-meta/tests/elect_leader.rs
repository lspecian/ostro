//! 12-05 Task 1: `ElectLeader` / `AlterIsr` / `ResignLeader` validated at meta apply
//! (D-P12-7, SC3: a leader is elected only from the published ISR).
//!
//! Every rejection also asserts the state is byte-identical afterwards (snapshot bytes),
//! and the existing commands keep their pre-Phase-12 semantics (P13-F1 rule 4).

use ostro_meta::ext::{
    ELECTION_REASON_FAILOVER, ELECTION_REASON_OFFLINE, ELECTION_REASON_RESIGN, ElectionInfo,
    get_election_info,
};
use ostro_meta::{
    EXT_TAG_TEST, MetaCommand, MetaKey, MetaRecord, MetaResult, MetaSm, MetaState,
    PartitionAssignment, StateMachine, encode_command, encode_snapshot, ext_get, ext_put,
};

const P: u16 = 2;

fn info(reason: u8) -> ElectionInfo {
    ElectionInfo {
        reason,
        elected_at_ms: 1_700_000_000_000,
        prev_leader: 1,
    }
}

/// A 4-partition cluster whose partition `P` is at epoch 3, leader 1, replicas [1,2,3],
/// ISR `isr`, isr_version 7, with a test ext entry that must survive every command.
fn state_with_isr(isr: &[u64]) -> MetaState {
    let mut s = MetaState::default();
    let init = MetaCommand::InitCluster {
        cluster_id: "c".into(),
        node_id: 1,
        partitions: 4,
        created_at_ms: 1,
        token_key: [3; 32],
    };
    assert_eq!(s.apply(1, init), MetaResult::Ok { version: 1 });
    let rec = PartitionAssignment {
        partition: P,
        leader: 1,
        epoch: 3,
        isr: isr.to_vec(),
        replicas: vec![1, 2, 3],
        isr_version: 7,
        min_insync: 2,
        ext: ext_put(&[], EXT_TAG_TEST, &[9]).unwrap(),
    };
    let put = MetaCommand::Put {
        key: MetaKey::Partition(P),
        expected_version: Some(0),
        record: MetaRecord::Partition(rec),
    };
    assert_eq!(s.apply(2, put), MetaResult::Ok { version: 3 });
    s
}

fn state() -> MetaState {
    state_with_isr(&[1, 2, 3])
}

fn part(s: &MetaState) -> PartitionAssignment {
    s.partition(P).unwrap().value.clone()
}

fn elect(epoch: u32, leader: u64, isr: &[u64], expected_isr_version: u64) -> MetaCommand {
    MetaCommand::ElectLeader {
        partition: P,
        epoch,
        leader,
        isr: isr.to_vec(),
        expected_isr_version,
        info: info(ELECTION_REASON_FAILOVER),
    }
}

fn alter(epoch: u32, expected_isr_version: u64, isr: &[u64]) -> MetaCommand {
    MetaCommand::AlterIsr {
        partition: P,
        epoch,
        expected_isr_version,
        isr: isr.to_vec(),
    }
}

fn resign(epoch: u32, node: u64) -> MetaCommand {
    MetaCommand::ResignLeader {
        partition: P,
        epoch,
        node,
        info: info(ELECTION_REASON_RESIGN),
    }
}

/// Apply `cmd`, assert the result matches `check` and that the state did not change.
fn rejected(mut s: MetaState, cmd: MetaCommand, check: impl Fn(&MetaResult) -> bool) {
    let before = encode_snapshot(&s);
    let r = s.apply(100, cmd.clone());
    assert!(check(&r), "{cmd:?} -> {r:?}");
    assert_eq!(encode_snapshot(&s), before, "{cmd:?} changed the state");
}

fn invalid(r: &MetaResult) -> bool {
    matches!(r, MetaResult::Invalid(_))
}

fn invalid_with(needle: &'static str) -> impl Fn(&MetaResult) -> bool {
    move |r| matches!(r, MetaResult::Invalid(m) if m.contains(needle))
}

fn stale(current: u64) -> impl Fn(&MetaResult) -> bool {
    move |r| *r == MetaResult::StaleVersion { current }
}

// ---- ElectLeader ------------------------------------------------------------------------

#[test]
fn elect_from_isr_ok() {
    let mut s = state();
    assert_eq!(
        s.apply(10, elect(4, 2, &[2, 3], 7)),
        MetaResult::Ok { version: 4 }
    );
    let v = s.partition(P).unwrap();
    assert_eq!((v.version, v.updated_index), (4, 10));
    let p = &v.value;
    assert_eq!((p.epoch, p.leader, p.isr.clone()), (4, 2, vec![2, 3]));
    assert_eq!(p.isr_version, 8);
    assert_eq!((p.replicas.clone(), p.min_insync), (vec![1, 2, 3], 2));
    assert_eq!(
        get_election_info(&p.ext).unwrap(),
        Some(info(ELECTION_REASON_FAILOVER))
    );
    assert_eq!(
        ext_get(&p.ext, EXT_TAG_TEST),
        Some(&[9u8][..]),
        "other ext kept"
    );
    // The new ISR is a subset of the old one.
    assert!(p.isr.iter().all(|n| [1, 2, 3].contains(n)));
}

#[test]
fn elect_epoch_may_jump() {
    let mut s = state();
    assert_eq!(
        s.apply(10, elect(9, 3, &[3], 7)),
        MetaResult::Ok { version: 9 }
    );
    assert_eq!(part(&s).epoch, 9);
}

#[test]
fn leader_not_in_published_isr() {
    // Node 3 is a replica but not in the published ISR [1, 2].
    rejected(
        state_with_isr(&[1, 2]),
        elect(4, 3, &[3], 7),
        invalid_with("not in the published ISR"),
    );
    rejected(
        state_with_isr(&[1, 2]),
        elect(4, 3, &[1, 2, 3], 7),
        invalid_with("not in the published ISR"),
    );
}

#[test]
fn elect_isr_adds_non_member() {
    // Leader 2 is in the ISR, but the new ISR adds replica 3, outside the published ISR.
    rejected(
        state_with_isr(&[1, 2]),
        elect(4, 2, &[2, 3], 7),
        invalid_with("not in the published ISR"),
    );
    // A node that is not even a replica.
    rejected(state(), elect(4, 2, &[2, 9], 7), invalid);
}

#[test]
fn offline_isr_outside_published_isr() {
    rejected(
        state_with_isr(&[1, 2]),
        elect(4, 0, &[3], 7),
        invalid_with("not in the published ISR"),
    );
}

#[test]
fn empty_isr_is_invalid() {
    rejected(state(), elect(4, 2, &[], 7), invalid_with("empty"));
    rejected(state(), elect(4, 0, &[], 7), invalid_with("empty"));
}

#[test]
fn malformed_isr_is_invalid() {
    rejected(state(), elect(4, 2, &[2, 2], 7), invalid_with("twice"));
    rejected(state(), elect(4, 2, &[0, 2], 7), invalid_with("node 0"));
}

#[test]
fn stale_epoch_is_stale() {
    rejected(state(), elect(3, 2, &[2, 3], 7), stale(3));
    rejected(state(), elect(2, 2, &[2, 3], 7), stale(3));
}

#[test]
fn wrong_isr_version_is_stale() {
    rejected(state(), elect(4, 2, &[2, 3], 6), stale(7));
    rejected(state(), elect(4, 2, &[2, 3], 8), stale(7));
}

#[test]
fn leader_not_in_new_isr() {
    rejected(
        state(),
        elect(4, 2, &[1, 3], 7),
        invalid_with("not in the new ISR"),
    );
}

#[test]
fn offline_with_last_member_ok() {
    let mut s = state_with_isr(&[2]);
    let cmd = MetaCommand::ElectLeader {
        partition: P,
        epoch: 4,
        leader: 0,
        isr: vec![2],
        expected_isr_version: 7,
        info: info(ELECTION_REASON_OFFLINE),
    };
    assert_eq!(s.apply(10, cmd), MetaResult::Ok { version: 4 });
    let p = part(&s);
    assert_eq!(
        (p.leader, p.isr.clone(), p.epoch, p.isr_version),
        (0, vec![2], 4, 8)
    );
    assert_eq!(
        get_election_info(&p.ext).unwrap().map(|i| i.reason),
        Some(ELECTION_REASON_OFFLINE)
    );
}

#[test]
fn elect_unknown_partition_not_found() {
    let cmd = MetaCommand::ElectLeader {
        partition: 77,
        epoch: 4,
        leader: 2,
        isr: vec![2],
        expected_isr_version: 7,
        info: info(ELECTION_REASON_FAILOVER),
    };
    rejected(state(), cmd, |r| *r == MetaResult::NotFound);
}

#[test]
fn elect_out_of_offline_needs_published_isr() {
    // Offline at epoch 4 with ISR [2]: only node 2 may lead next.
    let mut s = state_with_isr(&[2]);
    assert!(matches!(
        s.apply(10, elect(4, 0, &[2], 7)),
        MetaResult::Ok { .. }
    ));
    rejected(
        s.clone(),
        elect(5, 1, &[1], 8),
        invalid_with("not in the published ISR"),
    );
    assert_eq!(
        s.apply(11, elect(5, 2, &[2], 8)),
        MetaResult::Ok { version: 5 }
    );
    assert_eq!(part(&s).leader, 2);
}

// ---- AlterIsr ---------------------------------------------------------------------------

#[test]
fn alter_isr_shrink_and_expand() {
    let mut s = state();
    assert_eq!(
        s.apply(10, alter(3, 7, &[1, 2])),
        MetaResult::Ok { version: 3 }
    );
    let v = s.partition(P).unwrap();
    assert_eq!((v.version, v.updated_index), (3, 10));
    let p = &v.value;
    assert_eq!(
        (p.epoch, p.leader, p.isr.clone(), p.isr_version),
        (3, 1, vec![1, 2], 8)
    );
    assert_eq!(ext_get(&p.ext, EXT_TAG_TEST), Some(&[9u8][..]));
    assert_eq!(
        get_election_info(&p.ext).unwrap(),
        None,
        "AlterIsr writes no tag 80"
    );
    // Expansion back to every replica: only AlterIsr can grow the ISR.
    assert_eq!(
        s.apply(11, alter(3, 8, &[1, 2, 3])),
        MetaResult::Ok { version: 3 }
    );
    let p = part(&s);
    assert_eq!(
        (p.epoch, p.isr.clone(), p.isr_version),
        (3, vec![1, 2, 3], 9)
    );
}

#[test]
fn alter_isr_wrong_epoch_is_stale() {
    rejected(state(), alter(4, 7, &[1, 2]), stale(3));
    rejected(state(), alter(2, 7, &[1, 2]), stale(3));
}

#[test]
fn alter_isr_wrong_version_is_stale() {
    rejected(state(), alter(3, 6, &[1, 2]), stale(7));
}

#[test]
fn alter_isr_leader_not_in_isr() {
    rejected(
        state(),
        alter(3, 7, &[2, 3]),
        invalid_with("not in the new ISR"),
    );
}

#[test]
fn alter_isr_not_replicas() {
    rejected(state(), alter(3, 7, &[1, 4]), invalid_with("not a replica"));
}

#[test]
fn alter_isr_empty_or_malformed() {
    rejected(state(), alter(3, 7, &[]), invalid_with("empty"));
    rejected(state(), alter(3, 7, &[1, 1]), invalid_with("twice"));
}

#[test]
fn alter_isr_offline_partition_rejected() {
    let mut s = state_with_isr(&[2]);
    assert!(matches!(
        s.apply(10, elect(4, 0, &[2], 7)),
        MetaResult::Ok { .. }
    ));
    rejected(s, alter(4, 8, &[2]), invalid_with("not in the new ISR"));
}

#[test]
fn alter_isr_unknown_partition() {
    let cmd = MetaCommand::AlterIsr {
        partition: 77,
        epoch: 3,
        expected_isr_version: 7,
        isr: vec![1],
    };
    rejected(state(), cmd, |r| *r == MetaResult::NotFound);
}

// ---- ResignLeader -----------------------------------------------------------------------

#[test]
fn resign_removes_node_from_multi_member_isr() {
    let mut s = state();
    assert_eq!(s.apply(10, resign(3, 1)), MetaResult::Ok { version: 4 });
    let v = s.partition(P).unwrap();
    assert_eq!((v.version, v.updated_index), (4, 10));
    let p = &v.value;
    assert_eq!(
        (p.epoch, p.leader, p.isr.clone(), p.isr_version),
        (4, 0, vec![2, 3], 8)
    );
    assert_eq!(
        get_election_info(&p.ext).unwrap(),
        Some(info(ELECTION_REASON_RESIGN))
    );
    assert_eq!(ext_get(&p.ext, EXT_TAG_TEST), Some(&[9u8][..]));
}

#[test]
fn resign_keeps_sole_member() {
    let mut s = state_with_isr(&[1]);
    assert_eq!(s.apply(10, resign(3, 1)), MetaResult::Ok { version: 4 });
    let p = part(&s);
    assert_eq!((p.epoch, p.leader, p.isr.clone()), (4, 0, vec![1]));
}

#[test]
fn resign_wrong_node_or_epoch_rejected() {
    rejected(state(), resign(3, 2), invalid_with("not the leader"));
    rejected(state(), resign(3, 0), invalid_with("not the leader"));
    rejected(state(), resign(2, 1), stale(3));
    rejected(state(), resign(4, 1), stale(3));
    let cmd = MetaCommand::ResignLeader {
        partition: 77,
        epoch: 3,
        node: 1,
        info: info(ELECTION_REASON_RESIGN),
    };
    rejected(state(), cmd, |r| *r == MetaResult::NotFound);
}

#[test]
fn resign_then_elect_from_remaining_isr() {
    let mut s = state();
    assert!(matches!(s.apply(10, resign(3, 1)), MetaResult::Ok { .. }));
    // The resigned node left the ISR, so it cannot be re-elected.
    rejected(
        s.clone(),
        elect(5, 1, &[1], 8),
        invalid_with("not in the published ISR"),
    );
    assert_eq!(
        s.apply(11, elect(5, 3, &[2, 3], 8)),
        MetaResult::Ok { version: 5 }
    );
}

// ---- unchanged commands (rule 4) and ext (rule 5) ---------------------------------------

#[test]
fn set_partition_leader_still_accepts_leader_outside_isr() {
    // Pre-Phase-12 semantics: SetPartitionLeader does not check ISR membership.
    let mut s = state_with_isr(&[1, 2]);
    let cmd = MetaCommand::SetPartitionLeader {
        partition: P,
        epoch: 4,
        leader: 9,
        isr: vec![1],
    };
    assert_eq!(s.apply(10, cmd), MetaResult::Ok { version: 4 });
    let p = part(&s);
    assert_eq!((p.leader, p.isr.clone(), p.epoch), (9, vec![1], 4));
    // The F7 fields and ext are carried over untouched.
    assert_eq!(
        (p.replicas.clone(), p.isr_version, p.min_insync),
        (vec![1, 2, 3], 7, 2)
    );
}

#[test]
fn election_info_is_stored_never_validated() {
    // An unknown reason and a zero timestamp are stored verbatim.
    let mut s = state();
    let odd = ElectionInfo {
        reason: 250,
        elected_at_ms: 0,
        prev_leader: 12345,
    };
    let cmd = MetaCommand::ElectLeader {
        partition: P,
        epoch: 4,
        leader: 2,
        isr: vec![2],
        expected_isr_version: 7,
        info: odd,
    };
    assert_eq!(s.apply(10, cmd), MetaResult::Ok { version: 4 });
    assert_eq!(get_election_info(&part(&s).ext).unwrap(), Some(odd));
}

// ---- determinism ------------------------------------------------------------------------

fn sequence() -> Vec<MetaCommand> {
    vec![
        MetaCommand::InitCluster {
            cluster_id: "c".into(),
            node_id: 1,
            partitions: 4,
            created_at_ms: 1,
            token_key: [3; 32],
        },
        MetaCommand::Put {
            key: MetaKey::Partition(P),
            expected_version: Some(0),
            record: MetaRecord::Partition(PartitionAssignment {
                partition: P,
                leader: 1,
                epoch: 3,
                isr: vec![1, 2, 3],
                replicas: vec![1, 2, 3],
                isr_version: 7,
                min_insync: 2,
                ext: Vec::new(),
            }),
        },
        alter(3, 7, &[1, 2]),
        elect(4, 2, &[2, 3], 8), // rejected: 3 left the ISR
        elect(4, 2, &[2], 8),
        alter(4, 9, &[2, 3]),
        resign(4, 2),
        elect(6, 3, &[3], 10),
        alter(6, 11, &[1, 3]),
    ]
}

#[test]
fn two_state_machines_reach_identical_snapshots() {
    let mut a = MetaSm::new().0;
    let mut b = MetaSm::new().0;
    let mut ra = Vec::new();
    for (i, cmd) in sequence().into_iter().enumerate() {
        let bytes = encode_command(&cmd);
        let idx = i as u64 + 2;
        ra.push(a.apply(idx, 1, &bytes));
        assert_eq!(b.apply(idx, 1, &bytes), *ra.last().unwrap());
    }
    assert_eq!(a.snapshot(), b.snapshot());
    // And through a snapshot restore.
    let mut c = MetaSm::new().0;
    c.restore(10, &a.snapshot()).unwrap();
    assert_eq!(c.snapshot(), a.snapshot());
}
