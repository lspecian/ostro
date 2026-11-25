//! 13-10 Task 1: `AlterReplicas` validated at meta apply (D-P13-12).
//!
//! Every rejection asserts the state is byte-identical afterwards (snapshot bytes). The
//! Phase 12 commands keep their semantics (`elect_leader.rs` is untouched); ext tag 100 is
//! written verbatim from the command and never decoded by `validate.rs` (05-20 rule 5).

use ostro_meta::ext::{EXT_REASSIGNMENT, Reassignment, get_reassignment, put_reassignment};
use ostro_meta::{
    EXT_TAG_TEST, MetaCommand, MetaKey, MetaRecord, MetaResult, MetaSm, MetaState,
    PartitionAssignment, StateMachine, decode_snapshot, encode_command, encode_snapshot, ext_get,
    ext_put,
};

const P: u16 = 2;

/// A 4-partition cluster whose partition `P` is at epoch 3, leader `leader`, replicas
/// `replicas`, ISR `isr`, isr_version 7, min_insync 1, with a test ext entry that must
/// survive every command.
fn state_with(leader: u64, replicas: &[u64], isr: &[u64]) -> MetaState {
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
        leader,
        epoch: 3,
        isr: isr.to_vec(),
        replicas: replicas.to_vec(),
        isr_version: 7,
        min_insync: 1,
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

/// RF 1 on node 1 (the expand starting point).
fn rf1() -> MetaState {
    state_with(1, &[1], &[1])
}

fn part(s: &MetaState) -> PartitionAssignment {
    s.partition(P).unwrap().value.clone()
}

fn ra(target: &[u64], adding: &[u64], removing: &[u64]) -> Reassignment {
    Reassignment {
        target: target.to_vec(),
        adding: adding.to_vec(),
        removing: removing.to_vec(),
        started_at_ms: 1_700_000_000_000,
    }
}

#[allow(clippy::too_many_arguments)]
fn alter_at(
    partition: u16,
    epoch: u32,
    expected_isr_version: u64,
    replicas: &[u64],
    isr: &[u64],
    min_insync: u8,
    reassignment: Option<Reassignment>,
) -> MetaCommand {
    MetaCommand::AlterReplicas {
        partition,
        epoch,
        expected_isr_version,
        replicas: replicas.to_vec(),
        isr: isr.to_vec(),
        min_insync,
        reassignment,
    }
}

fn alter(replicas: &[u64], isr: &[u64], min_insync: u8) -> MetaCommand {
    alter_at(P, 3, 7, replicas, isr, min_insync, None)
}

/// Apply `cmd`, assert the result matches `check` and that the state did not change.
fn rejected(mut s: MetaState, cmd: MetaCommand, check: impl Fn(&MetaResult) -> bool) {
    let before = encode_snapshot(&s);
    let r = s.apply(100, cmd.clone());
    assert!(check(&r), "{cmd:?} -> {r:?}");
    assert_eq!(encode_snapshot(&s), before, "{cmd:?} changed the state");
}

fn invalid_with(needle: &'static str) -> impl Fn(&MetaResult) -> bool {
    move |r| matches!(r, MetaResult::Invalid(m) if m.contains(needle))
}

fn stale(current: u64) -> impl Fn(&MetaResult) -> bool {
    move |r| *r == MetaResult::StaleVersion { current }
}

// ---- accept -------------------------------------------------------------------------------

#[test]
fn expand_rf1_to_rf3_ok() {
    let mut s = rf1();
    let r = ra(&[1, 2, 3], &[2, 3], &[]);
    let cmd = alter_at(P, 3, 7, &[1, 2, 3], &[1], 1, Some(r.clone()));
    assert_eq!(s.apply(10, cmd), MetaResult::Ok { version: 3 });
    let v = s.partition(P).unwrap();
    assert_eq!((v.version, v.updated_index), (3, 10));
    let p = &v.value;
    assert_eq!((p.epoch, p.leader), (3, 1), "epoch and leader unchanged");
    assert_eq!(p.replicas, vec![1, 2, 3]);
    assert_eq!((p.isr.clone(), p.min_insync), (vec![1], 1));
    assert_eq!(p.isr_version, 8);
    assert_eq!(get_reassignment(&p.ext).unwrap(), Some(r.clone()));
    // Verbatim: the stored tag-100 bytes are exactly the command's encoded Reassignment.
    let expected = put_reassignment(&[], &r).unwrap();
    assert_eq!(
        ext_get(&p.ext, EXT_REASSIGNMENT),
        ext_get(&expected, EXT_REASSIGNMENT)
    );
    assert_eq!(
        ext_get(&p.ext, EXT_TAG_TEST),
        Some(&[9u8][..]),
        "other ext kept"
    );
}

#[test]
fn shrink_with_new_leader_ok() {
    // [1,2,3,4] → [2,3,4], the leader already moved to 2, published ISR [2,3].
    let mut s = state_with(2, &[1, 2, 3, 4], &[2, 3]);
    let cmd = alter_at(P, 3, 7, &[2, 3, 4], &[2, 3], 2, None);
    assert_eq!(s.apply(10, cmd), MetaResult::Ok { version: 3 });
    let p = part(&s);
    assert_eq!(p.replicas, vec![2, 3, 4]);
    assert_eq!(
        (p.isr.clone(), p.min_insync, p.isr_version),
        (vec![2, 3], 2, 8)
    );
    assert_eq!(p.leader, 2);
}

#[test]
fn none_clears_ext_100() {
    let mut s = rf1();
    let r = ra(&[1, 2], &[2], &[]);
    assert!(matches!(
        s.apply(10, alter_at(P, 3, 7, &[1, 2], &[1], 1, Some(r))),
        MetaResult::Ok { .. }
    ));
    assert!(ext_get(&part(&s).ext, EXT_REASSIGNMENT).is_some());
    assert!(matches!(
        s.apply(11, alter_at(P, 3, 8, &[1, 2], &[1, 2], 2, None)),
        MetaResult::Ok { .. }
    ));
    let p = part(&s);
    assert_eq!(ext_get(&p.ext, EXT_REASSIGNMENT), None);
    assert_eq!(ext_get(&p.ext, EXT_TAG_TEST), Some(&[9u8][..]));
    assert_eq!(p.isr_version, 9);
}

#[test]
fn ext_100_stored_never_validated() {
    // Nonsense contents (target not matching the replicas, adding/removing inconsistent,
    // zero timestamp) are stored verbatim: apply never interprets ext 100.
    let mut s = rf1();
    let odd = Reassignment {
        target: vec![77, 77, 0],
        adding: vec![5],
        removing: vec![1, 9],
        started_at_ms: 0,
    };
    let cmd = alter_at(P, 3, 7, &[1, 2], &[1], 1, Some(odd.clone()));
    assert_eq!(s.apply(10, cmd), MetaResult::Ok { version: 3 });
    assert_eq!(get_reassignment(&part(&s).ext).unwrap(), Some(odd));
}

#[test]
fn offline_partition_may_change_replicas() {
    // Leader 0 (offline): the leader rules do not apply.
    let mut s = state_with(0, &[1, 2, 3], &[2]);
    assert_eq!(
        s.apply(10, alter(&[2, 3, 4], &[2], 1)),
        MetaResult::Ok { version: 3 }
    );
    assert_eq!(part(&s).replicas, vec![2, 3, 4]);
}

// ---- reject -------------------------------------------------------------------------------

#[test]
fn unplaced_partition_is_invalid() {
    // Partition 0 is still at epoch 0 (InitCluster placement), owned by bootstrap.
    let s = rf1();
    rejected(
        s.clone(),
        alter_at(0, 0, 0, &[1, 2], &[1], 1, None),
        invalid_with("unplaced"),
    );
    // Even a (wrong) non-zero epoch against an epoch-0 partition is refused as unplaced.
    rejected(
        s,
        alter_at(0, 1, 0, &[1, 2], &[1], 1, None),
        invalid_with("unplaced"),
    );
}

#[test]
fn stale_epoch_is_stale() {
    rejected(rf1(), alter_at(P, 2, 7, &[1, 2], &[1], 1, None), stale(3));
    rejected(rf1(), alter_at(P, 4, 7, &[1, 2], &[1], 1, None), stale(3));
}

#[test]
fn isr_version_mismatch_is_stale() {
    rejected(rf1(), alter_at(P, 3, 6, &[1, 2], &[1], 1, None), stale(7));
    rejected(rf1(), alter_at(P, 3, 8, &[1, 2], &[1], 1, None), stale(7));
}

#[test]
fn leader_not_in_replicas() {
    rejected(
        rf1(),
        alter(&[2, 3], &[2], 1),
        invalid_with("not in the replicas"),
    );
}

#[test]
fn isr_not_subset_of_replicas() {
    rejected(
        state_with(1, &[1, 2, 3], &[1, 2, 3]),
        alter(&[1, 2], &[1, 3], 1),
        invalid_with("ISR member 3 is not in the replicas"),
    );
}

#[test]
fn leader_not_in_isr() {
    rejected(
        state_with(1, &[1, 2, 3], &[1, 2, 3]),
        alter(&[1, 2, 3], &[2, 3], 1),
        invalid_with("not in the ISR"),
    );
}

#[test]
fn min_insync_above_isr_rejected() {
    rejected(
        rf1(),
        alter(&[1, 2, 3], &[1], 2),
        invalid_with("min_insync 2"),
    );
    rejected(
        state_with(1, &[1, 2, 3], &[1, 2]),
        alter(&[1, 2, 3], &[1, 2], 3),
        invalid_with("min_insync 3"),
    );
}

#[test]
fn min_insync_zero_rejected() {
    rejected(rf1(), alter(&[1, 2], &[1], 0), invalid_with("min_insync 0"));
}

#[test]
fn duplicate_replica_ids_rejected() {
    rejected(rf1(), alter(&[1, 2, 2], &[1], 1), invalid_with("twice"));
    rejected(rf1(), alter(&[1, 0], &[1], 1), invalid_with("node 0"));
    rejected(rf1(), alter(&[], &[1], 1), invalid_with("empty"));
    rejected(rf1(), alter(&[1, 2], &[1, 1], 1), invalid_with("twice"));
    rejected(rf1(), alter(&[1, 2], &[], 1), invalid_with("empty"));
}

#[test]
fn unknown_partition_not_found() {
    rejected(rf1(), alter_at(9, 3, 7, &[1], &[1], 1, None), |r| {
        *r == MetaResult::NotFound
    });
}

#[test]
fn rejected_command_records_result_through_the_state_machine() {
    // Through MetaSm: a rejected AlterReplicas is applied (the cursor advances, the result is
    // recorded) and the state is unchanged; the next valid one applies.
    let (mut sm, view) = MetaSm::new();
    let cmds = [
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
                isr: vec![1],
                replicas: vec![1],
                isr_version: 7,
                min_insync: 1,
                ext: Vec::new(),
            }),
        },
    ];
    for (i, c) in cmds.iter().enumerate() {
        sm.apply(i as u64 + 1, 1, &encode_command(c)).unwrap();
    }
    let before = sm.snapshot();
    let bad = alter_at(P, 3, 7, &[1, 2, 3], &[1], 2, None);
    let out = sm.apply(3, 1, &encode_command(&bad)).unwrap();
    assert!(!out.is_empty(), "a result is recorded");
    assert_eq!(view.applied(), 3, "the cursor advances on a rejection");
    assert_eq!(
        decode_snapshot(&sm.snapshot()).unwrap(),
        decode_snapshot(&before).unwrap()
    );
    let good = alter_at(P, 3, 7, &[1, 2, 3], &[1], 1, None);
    sm.apply(4, 1, &encode_command(&good)).unwrap();
    assert_eq!(view.applied(), 4);
    assert_eq!(view.read(|s| s.partition(P).unwrap().value.isr_version), 8);
}

// ---- unchanged semantics ------------------------------------------------------------------

#[test]
fn phase12_commands_unchanged_after_alter_replicas() {
    // After an AlterReplicas, ElectLeader / AlterIsr / ResignLeader / SetPartitionLeader use
    // the new replica set and the bumped isr_version exactly as Phase 12 defines.
    let mut s = rf1();
    assert!(matches!(
        s.apply(
            10,
            alter_at(
                P,
                3,
                7,
                &[1, 2, 3],
                &[1],
                1,
                Some(ra(&[1, 2, 3], &[2, 3], &[]))
            )
        ),
        MetaResult::Ok { .. }
    ));
    // AlterIsr (leader 1 at epoch 3) expands into the new replicas, CAS on 8.
    assert_eq!(
        s.apply(
            11,
            MetaCommand::AlterIsr {
                partition: P,
                epoch: 3,
                expected_isr_version: 8,
                isr: vec![1, 2, 3],
            }
        ),
        MetaResult::Ok { version: 3 }
    );
    // The ext 100 record survives AlterIsr untouched.
    assert!(ext_get(&part(&s).ext, EXT_REASSIGNMENT).is_some());
    // ElectLeader from the published ISR.
    let info = ostro_meta::ext::ElectionInfo {
        reason: ostro_meta::ext::ELECTION_REASON_FAILOVER,
        elected_at_ms: 1,
        prev_leader: 1,
    };
    assert_eq!(
        s.apply(
            12,
            MetaCommand::ElectLeader {
                partition: P,
                epoch: 4,
                leader: 2,
                isr: vec![2, 3],
                expected_isr_version: 9,
                info,
            }
        ),
        MetaResult::Ok { version: 4 }
    );
    let p = part(&s);
    assert_eq!((p.replicas.clone(), p.min_insync), (vec![1, 2, 3], 1));
    assert!(ext_get(&p.ext, EXT_REASSIGNMENT).is_some());
    assert_eq!(
        s.apply(
            13,
            MetaCommand::ResignLeader {
                partition: P,
                epoch: 4,
                node: 2,
                info,
            }
        ),
        MetaResult::Ok { version: 5 }
    );
    // SetPartitionLeader still accepts a leader outside the ISR (Phase 4 semantics).
    assert_eq!(
        s.apply(
            14,
            MetaCommand::SetPartitionLeader {
                partition: P,
                epoch: 6,
                leader: 3,
                isr: vec![3],
            }
        ),
        MetaResult::Ok { version: 6 }
    );
    // An AlterReplicas with the pre-election token is now stale.
    rejected(
        s.clone(),
        alter_at(P, 6, 8, &[1, 2, 3], &[3], 1, None),
        stale(11), // AlterReplicas 8, AlterIsr 9, ElectLeader 10, ResignLeader 11
    );
}

#[test]
fn validate_never_decodes_tag_100() {
    // Source check: the record validator only checks ext framing.
    let src = include_str!("../src/validate.rs");
    for needle in ["EXT_REASSIGNMENT", "get_reassignment", "Reassignment"] {
        assert!(!src.contains(needle), "validate.rs references {needle}");
    }
    // Behaviour: a Put of a partition record whose tag-100 bytes do not decode as a
    // Reassignment is accepted (framing is valid; the value is opaque to apply).
    let mut s = rf1();
    let mut rec = part(&s);
    rec.epoch = 4;
    rec.ext = ext_put(&rec.ext, EXT_REASSIGNMENT, &[0xFF, 0xFF, 0xFF]).unwrap();
    assert_eq!(
        s.apply(
            10,
            MetaCommand::Put {
                key: MetaKey::Partition(P),
                expected_version: Some(3),
                record: MetaRecord::Partition(rec),
            }
        ),
        MetaResult::Ok { version: 4 }
    );
    assert!(get_reassignment(&part(&s).ext).is_err());
}

#[test]
fn two_state_machines_agree_on_alter_replicas() {
    let seq = [
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
                isr: vec![1],
                replicas: vec![1],
                isr_version: 7,
                min_insync: 1,
                ext: Vec::new(),
            }),
        },
        alter_at(
            P,
            3,
            7,
            &[1, 2, 3],
            &[1],
            1,
            Some(ra(&[1, 2, 3], &[2, 3], &[])),
        ),
        alter_at(P, 3, 7, &[1, 2, 3], &[1], 1, None), // stale
        alter_at(P, 3, 8, &[1, 2, 3], &[1], 2, None), // min_insync > |isr|
        alter_at(P, 3, 8, &[1, 2, 3], &[1], 1, None),
    ];
    let (mut a, _) = MetaSm::new();
    let (mut b, _) = MetaSm::new();
    for (i, c) in seq.iter().enumerate() {
        let bytes = encode_command(c);
        let ra = a.apply(i as u64 + 1, 1, &bytes).unwrap();
        assert_eq!(b.apply(i as u64 + 1, 1, &bytes).unwrap(), ra);
    }
    assert_eq!(a.snapshot(), b.snapshot());
}
