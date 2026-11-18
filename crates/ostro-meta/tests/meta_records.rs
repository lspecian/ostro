//! META-02: every record kind round-trips propose -> snapshot -> restore, and
//! `MetaSm` snapshots are deterministic.

use ostro_meta::{
    MetaCommand, MetaKind, MetaRecord, MetaResult, MetaSm, MetaState, StateMachine, decode_result,
    encode_command,
};
use proptest::prelude::*;

fn apply(sm: &mut MetaSm, index: u64, cmd: &MetaCommand) -> MetaResult {
    decode_result(&sm.apply(index, 1, &encode_command(cmd))).expect("result decodes")
}

fn put(record: MetaRecord, expected_version: Option<u64>) -> MetaCommand {
    MetaCommand::Put {
        key: record.key(),
        expected_version,
        record,
    }
}

#[test]
fn every_kind_round_trips_through_snapshot() {
    let (mut sm, view) = MetaSm::new();
    let mut index = 1;
    let init = MetaCommand::InitCluster {
        cluster_id: "c".into(),
        node_id: 1,
        partitions: 16,
        created_at_ms: 1,
        token_key: [3; 32],
    };
    index += 1;
    assert_eq!(apply(&mut sm, index, &init), MetaResult::Ok { version: 1 });
    for kind in MetaKind::ALL {
        // id 20 avoids the InitCluster records (member 1, partitions 0..16).
        for (generation, expected) in [(1, None), (2, Some(1))] {
            index += 1;
            let record = MetaRecord::sample(kind, 20, generation);
            let r = apply(&mut sm, index, &put(record, expected));
            if kind == MetaKind::Cluster {
                // The cluster record already exists (InitCluster, version 1).
                let want = if expected.is_none() {
                    MetaResult::Conflict { current: 1 }
                } else {
                    MetaResult::Ok { version: 2 }
                };
                assert_eq!(r, want);
            } else {
                assert_eq!(
                    r,
                    MetaResult::Ok {
                        version: u64::from(generation)
                    },
                    "{kind:?}"
                );
            }
        }
    }
    assert_eq!(*view.subscribe().borrow(), index);
    let state = view.read(|s| s.clone());
    for kind in MetaKind::ALL {
        let key = MetaRecord::sample(kind, 20, 1).key();
        assert_eq!(
            state.get(&key).unwrap().value,
            MetaRecord::sample(kind, 20, 2),
            "{kind:?}"
        );
    }

    let snap = sm.snapshot();
    assert_eq!(snap[0], ostro_meta::META_FORMAT);
    let (mut restored, rview) = MetaSm::new();
    restored.restore(index, &snap).expect("restore");
    assert_eq!(rview.read(|s| s.clone()), state);
    assert_eq!(
        *rview.subscribe().borrow(),
        index,
        "restore bumps the applied watch"
    );
    assert_eq!(
        restored.snapshot(),
        snap,
        "equal states snapshot byte-identically"
    );
    assert_eq!(rview.read(|s| s.token_key()), Some([3; 32]));
}

#[test]
fn undecodable_command_is_invalid_and_bad_snapshot_is_rejected() {
    let (mut sm, view) = MetaSm::new();
    for bytes in [&[][..], &[2, 0, 0][..], &[1, 0xff, 0xff, 0xff][..]] {
        let r = decode_result(&sm.apply(1, 1, bytes)).unwrap();
        assert!(matches!(r, MetaResult::Invalid(_)), "{r:?}");
    }
    assert_eq!(view.read(|s| s.clone()), MetaState::default());
    assert!(sm.restore(1, &[]).is_err());
    assert!(sm.restore(1, &[9, 1, 2]).is_err());
    // Bootstrap: the empty state's snapshot restores to the empty state.
    let empty = sm.snapshot();
    sm.restore(1, &empty).unwrap();
    assert_eq!(view.read(|s| s.clone()), MetaState::default());
}

fn arb_command() -> impl Strategy<Value = MetaCommand> {
    prop_oneof![
        (0usize..14, 0u8..3, 1u32..5, prop::option::of(0u64..5))
            .prop_map(|(k, id, g, e)| put(MetaRecord::sample(MetaKind::ALL[k], id, g), e)),
        (0usize..14, 0u8..3, 0u64..5).prop_map(|(k, id, e)| MetaCommand::Delete {
            key: MetaRecord::sample(MetaKind::ALL[k], id, 1).key(),
            expected_version: e,
        }),
        (0u16..4, 0u32..6, 1u64..3).prop_map(|(p, epoch, leader)| {
            MetaCommand::SetPartitionLeader {
                partition: p,
                epoch,
                leader,
                isr: vec![leader],
            }
        }),
        (prop_oneof![Just(4u16), Just(8u16)], any::<u8>()).prop_map(|(partitions, b)| {
            MetaCommand::InitCluster {
                cluster_id: format!("c{b}"),
                node_id: 1,
                partitions,
                created_at_ms: u64::from(b),
                token_key: [b; 32],
            }
        }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    /// apply a prefix -> snapshot -> restore into a fresh SM; the restored SM
    /// equals the original and yields the same results for the suffix.
    #[test]
    fn snapshot_restore_preserves_state_and_results(
        cmds in prop::collection::vec(arb_command(), 0..80),
        split in 0usize..80,
    ) {
        let split = split.min(cmds.len());
        let (mut a, aview) = MetaSm::new();
        for (i, c) in cmds[..split].iter().enumerate() {
            apply(&mut a, i as u64 + 2, c);
        }
        let snap = a.snapshot();
        let (mut b, bview) = MetaSm::new();
        b.restore(split as u64 + 1, &snap).unwrap();
        prop_assert_eq!(aview.read(|s| s.clone()), bview.read(|s| s.clone()));
        for (i, c) in cmds[split..].iter().enumerate() {
            let idx = (split + i) as u64 + 2;
            prop_assert_eq!(apply(&mut a, idx, c), apply(&mut b, idx, c));
        }
        prop_assert_eq!(aview.read(|s| s.clone()), bview.read(|s| s.clone()));
        prop_assert_eq!(a.snapshot(), b.snapshot());
    }
}
