//! META-03: versioned compare-and-set at apply, for EVERY record kind.
//!
//! The table-driven test iterates `MetaKind::ALL` (one sample constructor per
//! kind) through create / conflict / CAS / stale / delete; the proptest drives
//! random interleavings of Puts with random expected versions.

use std::collections::BTreeMap;

use ostro_meta::{
    MetaCommand, MetaKey, MetaKind, MetaRecord, MetaResult, MetaState, TOKEN_KEY_SECRET,
    decode_command, decode_result, encode_command, encode_result,
};
use proptest::prelude::*;

fn put(record: MetaRecord, expected_version: Option<u64>) -> MetaCommand {
    MetaCommand::Put { key: record.key(), expected_version, record }
}

fn version_of(state: &MetaState, key: &MetaKey) -> Option<u64> {
    state.get(key).map(|v| v.version)
}

#[test]
fn every_kind_create_conflict_cas_stale_delete() {
    let mut idx = 10;
    let mut next = || {
        idx += 1;
        idx
    };
    for kind in MetaKind::ALL {
        let mut s = MetaState::default();
        let r1 = MetaRecord::sample(kind, 3, 1);
        let r2 = MetaRecord::sample(kind, 3, 2);
        let r3 = MetaRecord::sample(kind, 3, 3);
        let key = r1.key();
        assert_eq!(key.kind(), kind);
        assert_eq!(r2.key(), key, "{kind:?}: generations share a key");

        // Delete / CAS on an absent key.
        assert_eq!(
            s.apply(next(), MetaCommand::Delete { key: key.clone(), expected_version: 1 }),
            MetaResult::NotFound,
            "{kind:?}"
        );
        assert_eq!(s.apply(next(), put(r1.clone(), Some(1))), MetaResult::NotFound, "{kind:?}");
        assert_eq!(s, MetaState::default(), "{kind:?}: rejected ops leave state unchanged");

        // Create.
        let at = next();
        assert_eq!(s.apply(at, put(r1.clone(), None)), MetaResult::Ok { version: 1 }, "{kind:?}");
        let got = s.get(&key).expect("created");
        assert_eq!((got.version, got.updated_index, &got.value), (1, at, &r1), "{kind:?}");

        // Create of an existing key -> Conflict, unchanged.
        let before = s.clone();
        assert_eq!(
            s.apply(next(), put(r2.clone(), None)),
            MetaResult::Conflict { current: 1 },
            "{kind:?}"
        );
        assert_eq!(s, before, "{kind:?}");

        // Stale / future expected versions -> StaleVersion, unchanged.
        for bad in [0, 2, 7] {
            assert_eq!(
                s.apply(next(), put(r2.clone(), Some(bad))),
                MetaResult::StaleVersion { current: 1 },
                "{kind:?} expected {bad}"
            );
            assert_eq!(s, before, "{kind:?}");
        }

        // CAS chain.
        assert_eq!(s.apply(next(), put(r2.clone(), Some(1))), MetaResult::Ok { version: 2 }, "{kind:?}");
        assert_eq!(s.get(&key).unwrap().value, r2, "{kind:?}");
        let before = s.clone();
        assert_eq!(
            s.apply(next(), put(r3.clone(), Some(1))),
            MetaResult::StaleVersion { current: 2 },
            "{kind:?}: pre-CAS version is stale"
        );
        assert_eq!(s, before, "{kind:?}");
        assert_eq!(s.apply(next(), put(r3.clone(), Some(2))), MetaResult::Ok { version: 3 }, "{kind:?}");

        // Delete requires the exact version.
        let before = s.clone();
        assert_eq!(
            s.apply(next(), MetaCommand::Delete { key: key.clone(), expected_version: 2 }),
            MetaResult::StaleVersion { current: 3 },
            "{kind:?}"
        );
        assert_eq!(s, before, "{kind:?}");
        assert_eq!(
            s.apply(next(), MetaCommand::Delete { key: key.clone(), expected_version: 3 }),
            MetaResult::Ok { version: 3 },
            "{kind:?}"
        );
        assert_eq!(version_of(&s, &key), None, "{kind:?}");
        assert_eq!(s, MetaState::default(), "{kind:?}: delete leaves nothing behind");
    }
}

#[test]
fn key_record_mismatch_is_invalid() {
    let mut s = MetaState::default();
    for (i, kind) in MetaKind::ALL.into_iter().enumerate() {
        let other = MetaKind::ALL[(i + 1) % MetaKind::ALL.len()];
        let record = MetaRecord::sample(other, 1, 1);
        let key = MetaRecord::sample(kind, 1, 1).key();
        let r = s.apply(1, MetaCommand::Put { key, expected_version: None, record });
        assert!(matches!(r, MetaResult::Invalid(_)), "{kind:?} key with {other:?} record: {r:?}");
    }
    // Same kind, different identity.
    let record = MetaRecord::sample(MetaKind::Queue, 1, 1);
    let r = s.apply(1, MetaCommand::Put { key: MetaKey::Queue("other".into()), expected_version: None, record });
    assert!(matches!(r, MetaResult::Invalid(_)), "{r:?}");
    assert_eq!(s, MetaState::default());
}

fn init(partitions: u16) -> MetaCommand {
    MetaCommand::InitCluster {
        cluster_id: "c1".into(),
        node_id: 1,
        partitions,
        created_at_ms: 5,
        token_key: [7; 32],
    }
}

#[test]
fn init_cluster_creates_and_is_idempotent() {
    let mut s = MetaState::default();
    assert_eq!(s.apply(1, init(16)), MetaResult::Ok { version: 1 });
    let c = s.cluster().unwrap();
    assert_eq!((c.value.cluster_id.as_str(), c.value.partitions), ("c1", 16));
    assert_eq!(s.member(1).unwrap().value.node_id, 1);
    for p in 0..16 {
        let a = s.partition(p).unwrap();
        assert_eq!((a.version, a.value.epoch, a.value.leader, a.value.isr.clone()), (0, 0, 1, vec![1]));
    }
    assert!(s.partition(16).is_none());
    assert_eq!(s.token_key(), Some([7; 32]));
    assert!(s.get(&MetaKey::Secret(TOKEN_KEY_SECRET.into())).is_some());

    let before = s.clone();
    // Same partition count (even with a different id/key) -> idempotent Ok.
    let again = MetaCommand::InitCluster {
        cluster_id: "c2".into(),
        node_id: 1,
        partitions: 16,
        created_at_ms: 99,
        token_key: [9; 32],
    };
    assert_eq!(s.apply(2, again), MetaResult::Ok { version: 1 });
    assert_eq!(s, before, "repeat InitCluster changes nothing");

    match s.apply(3, init(8)) {
        MetaResult::Invalid(msg) => assert!(msg.contains("partition count mismatch"), "{msg}"),
        other => panic!("expected Invalid, got {other:?}"),
    }
    assert_eq!(s, before);
    assert!(matches!(MetaState::default().apply(1, init(0)), MetaResult::Invalid(_)));
}

#[test]
fn set_partition_leader_epochs() {
    let mut s = MetaState::default();
    let set = |epoch, leader| MetaCommand::SetPartitionLeader { partition: 3, epoch, leader, isr: vec![leader] };
    assert_eq!(s.apply(1, set(1, 1)), MetaResult::NotFound);
    s.apply(2, init(16));
    assert_eq!(s.apply(3, set(2, 1)), MetaResult::Ok { version: 2 });
    let p = s.partition(3).unwrap();
    assert_eq!((p.version, p.value.epoch, p.updated_index), (2, 2, 3));
    // Equal epoch re-assert: idempotent, same version, updated_index untouched.
    let before = s.clone();
    assert_eq!(s.apply(4, set(2, 1)), MetaResult::Ok { version: 2 });
    assert_eq!(s, before);
    // Equal epoch, different leader: conflict.
    assert_eq!(s.apply(5, set(2, 9)), MetaResult::Conflict { current: 2 });
    assert_eq!(s, before);
    // Lower epoch: stale.
    assert_eq!(s.apply(6, set(1, 1)), MetaResult::StaleVersion { current: 2 });
    assert_eq!(s, before);
    assert_eq!(s.apply(7, set(5, 2)), MetaResult::Ok { version: 5 });
    assert_eq!(s.partition(3).unwrap().value.leader, 2);
    // A Put with a lower epoch is stale too.
    let lower = MetaRecord::Partition(ostro_meta::PartitionAssignment { partition: 3, leader: 1, epoch: 4, isr: vec![1] });
    assert_eq!(s.apply(8, put(lower, Some(5))), MetaResult::StaleVersion { current: 5 });
}

#[test]
fn encoding_round_trips_and_rejects_unknown_format() {
    let cmd = put(MetaRecord::sample(MetaKind::Workflow, 1, 1), Some(4));
    let bytes = encode_command(&cmd);
    assert_eq!(bytes[0], ostro_meta::META_FORMAT);
    assert_eq!(decode_command(&bytes).unwrap(), cmd);
    let mut bad = bytes.clone();
    bad[0] = 2;
    assert!(decode_command(&bad).is_err());
    assert!(decode_command(&[]).is_err());
    let mut trailing = bytes;
    trailing.push(0);
    assert!(decode_command(&trailing).is_err());
    for r in [MetaResult::Ok { version: 3 }, MetaResult::StaleVersion { current: 1 }, MetaResult::Invalid("x".into())] {
        assert_eq!(decode_result(&encode_result(&r)).unwrap(), r);
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

    /// Random Puts with random expected versions over a few keys of a few
    /// kinds: final version == number of successful puts, and each key's
    /// successful versions form the chain 1, 2, 3, ...
    #[test]
    fn random_cas_interleavings(ops in prop::collection::vec((0usize..14, 0u8..3, prop::option::of(0u64..6)), 1..120)) {
        let mut s = MetaState::default();
        let mut successes: BTreeMap<MetaKey, Vec<u64>> = BTreeMap::new();
        for (i, (k, id, expected)) in ops.into_iter().enumerate() {
            let kind = MetaKind::ALL[k];
            let key = MetaRecord::sample(kind, id, 1).key();
            let current = version_of(&s, &key);
            let generation = current.map_or(1, |v| v + 1) as u32;
            let record = MetaRecord::sample(kind, id, generation);
            let before = s.clone();
            let r = s.apply(i as u64 + 1, put(record, expected));
            match r {
                MetaResult::Ok { version } => {
                    prop_assert_eq!(expected, current, "success only on exact match");
                    successes.entry(key.clone()).or_default().push(version);
                }
                MetaResult::StaleVersion { current: c } => {
                    prop_assert_eq!(Some(c), current);
                    prop_assert_ne!(expected, current);
                    prop_assert_eq!(&s, &before);
                }
                MetaResult::Conflict { current: c } => {
                    prop_assert_eq!(expected, None);
                    prop_assert_eq!(Some(c), current);
                    prop_assert_eq!(&s, &before);
                }
                MetaResult::NotFound => {
                    prop_assert!(expected.is_some() && current.is_none());
                    prop_assert_eq!(&s, &before);
                }
                MetaResult::Invalid(m) => prop_assert!(false, "unexpected Invalid: {}", m),
            }
        }
        for (key, versions) in successes {
            let expected_chain: Vec<u64> = (1..=versions.len() as u64).collect();
            prop_assert_eq!(&versions, &expected_chain, "{:?}", key);
            prop_assert_eq!(version_of(&s, &key), Some(versions.len() as u64));
        }
    }
}
