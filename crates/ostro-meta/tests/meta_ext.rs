//! 05-07 Task 2: the meta `ext` TLV codec (Phase 14 F2), apply-time ext
//! validation on every record kind, Phase 12 F7 partition/member fields,
//! Phase 14 F3 tenant quotas and Phase 14 F4 `WorkflowVersion.module`.

use ostro_meta::{
    ApiToken, EXT_TAG_TEST, ExtEntry, ExtError, MetaCommand, MetaKind, MetaRecord, MetaResult,
    MetaState, RateSpec, Tenant, WorkflowVersion, ext_entries, ext_get, ext_get_value, ext_put,
    ext_put_value, ext_remove, ext_validate,
};
use serde::{Deserialize, Serialize};

fn put(record: MetaRecord, expected_version: Option<u64>) -> MetaCommand {
    MetaCommand::Put {
        key: record.key(),
        expected_version,
        record,
    }
}

// ---- codec -----------------------------------------------------------------

#[test]
fn empty_ext_is_valid_and_has_no_entries() {
    assert_eq!(ext_validate(&[]), Ok(()));
    assert_eq!(ext_entries(&[]).unwrap(), vec![]);
    assert_eq!(ext_get(&[], 1), None);
}

#[test]
fn encoding_is_tag_len_value_with_canonical_varints() {
    let ext = ext_put(&[], 15, b"ab").unwrap();
    assert_eq!(ext, vec![15, 2, b'a', b'b']);
    // A tag >= 128 takes a two-byte varint; a 200-byte value too.
    let big = vec![7u8; 200];
    let ext = ext_put(&ext, 300, &big).unwrap();
    assert_eq!(&ext[..4], &[15, 2, b'a', b'b']);
    assert_eq!(&ext[4..8], &[0xac, 0x02, 0xc8, 0x01]);
    assert_eq!(&ext[8..], &big[..]);
    assert_eq!(ext_get(&ext, 300), Some(&big[..]));
    assert_eq!(ext_get(&ext, 15), Some(&b"ab"[..]));
    assert_eq!(ext_get(&ext, 16), None);
}

#[test]
fn put_keeps_order_replaces_and_round_trips() {
    let mut ext = Vec::new();
    for (tag, val) in [(40u32, &b"x"[..]), (3, b"yy"), (20, b""), (100, b"zzz")] {
        ext = ext_put(&ext, tag, val).unwrap();
        ext_validate(&ext).unwrap();
    }
    let tags: Vec<u32> = ext_entries(&ext).unwrap().iter().map(|e| e.tag).collect();
    assert_eq!(tags, vec![3, 20, 40, 100]);
    assert_eq!(ext_get(&ext, 20), Some(&b""[..]));
    // Replace in place.
    let ext2 = ext_put(&ext, 40, b"new").unwrap();
    assert_eq!(ext_get(&ext2, 40), Some(&b"new"[..]));
    assert_eq!(ext_entries(&ext2).unwrap().len(), 4);
    // Re-putting the same value is byte-identical.
    assert_eq!(ext_put(&ext, 40, b"x").unwrap(), ext);
    // Remove.
    let ext3 = ext_remove(&ext, 20).unwrap();
    assert_eq!(ext_get(&ext3, 20), None);
    assert_eq!(
        ext_remove(&ext3, 999).unwrap(),
        ext3,
        "absent tag: unchanged"
    );
    assert_eq!(
        ext_remove(&ext_put(&[], 5, b"v").unwrap(), 5).unwrap(),
        vec![]
    );
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Knob {
    level: u32,
    label: String,
}

#[test]
fn typed_values_are_postcard() {
    let knob = Knob {
        level: 3,
        label: "hi".into(),
    };
    let ext = ext_put_value(&[], EXT_TAG_TEST, &knob).unwrap();
    assert_eq!(
        ext_get_value::<Knob>(&ext, EXT_TAG_TEST).unwrap(),
        Some(knob)
    );
    assert_eq!(ext_get_value::<Knob>(&ext, 16).unwrap(), None);
    assert_eq!(
        ext_get(&ext, EXT_TAG_TEST).unwrap(),
        &postcard::to_stdvec(&Knob {
            level: 3,
            label: "hi".into()
        })
        .unwrap()[..]
    );
    let bad = ext_put(&[], 9, &[0xff]).unwrap();
    assert!(matches!(
        ext_get_value::<Knob>(&bad, 9),
        Err(ExtError::BadValue { tag: 9, .. })
    ));
}

#[test]
fn unknown_tags_survive_read_modify_write() {
    // A newer node wrote tags 15 and 200 with values this build cannot read.
    let foreign = vec![15, 3, 1, 2, 3, 0xc8, 0x01, 2, 0xde, 0xad];
    ext_validate(&foreign).unwrap();
    // This build sets its own tag 40 and removes nothing.
    let after = ext_put(&foreign, 40, b"mine").unwrap();
    let e = ext_entries(&after).unwrap();
    assert_eq!(
        e,
        vec![
            ExtEntry {
                tag: 15,
                value: &[1, 2, 3]
            },
            ExtEntry {
                tag: 40,
                value: b"mine"
            },
            ExtEntry {
                tag: 200,
                value: &[0xde, 0xad]
            },
        ]
    );
    // Removing ours restores the foreign bytes exactly.
    assert_eq!(ext_remove(&after, 40).unwrap(), foreign);

    // The same through a record CAS update.
    let mut s = MetaState::default();
    let mut rec = MetaRecord::sample(MetaKind::Queue, 1, 1);
    *rec.ext_mut() = foreign.clone();
    assert_eq!(s.apply(1, put(rec, None)), MetaResult::Ok { version: 1 });
    let mut read = s
        .get(&MetaRecord::sample(MetaKind::Queue, 1, 1).key())
        .unwrap();
    let new_ext = ext_put(read.value.ext(), 40, b"mine").unwrap();
    *read.value.ext_mut() = new_ext;
    assert_eq!(
        s.apply(2, put(read.value, Some(read.version))),
        MetaResult::Ok { version: 2 }
    );
    let stored = s
        .get(&MetaRecord::sample(MetaKind::Queue, 1, 1).key())
        .unwrap();
    assert_eq!(ext_get(stored.value.ext(), 200), Some(&[0xde, 0xad][..]));
    assert_eq!(stored.value.ext(), &after[..]);
}

#[test]
fn malformed_ext_is_rejected() {
    // Zero tag.
    assert_eq!(ext_validate(&[0, 0]), Err(ExtError::ZeroTag { offset: 0 }));
    assert!(matches!(
        ext_put(&[], 0, b""),
        Err(ExtError::ZeroTag { .. })
    ));
    // Non-increasing tags (equal and decreasing).
    assert!(matches!(
        ext_validate(&[5, 0, 5, 0]),
        Err(ExtError::NotIncreasing {
            prev: 5,
            tag: 5,
            ..
        })
    ));
    assert!(matches!(
        ext_validate(&[5, 0, 4, 0]),
        Err(ExtError::NotIncreasing {
            prev: 5,
            tag: 4,
            ..
        })
    ));
    // Truncated: missing length, length past the end, varint cut short.
    assert!(matches!(
        ext_validate(&[5]),
        Err(ExtError::Truncated { .. })
    ));
    assert!(matches!(
        ext_validate(&[5, 3, 1, 2]),
        Err(ExtError::Truncated { .. })
    ));
    assert!(matches!(
        ext_validate(&[0x80]),
        Err(ExtError::Truncated { .. })
    ));
    // Non-canonical varint (1 written as 0x81 0x00) and u32 overflow.
    assert!(matches!(
        ext_validate(&[0x81, 0x00, 0]),
        Err(ExtError::BadVarint { .. })
    ));
    assert!(matches!(
        ext_validate(&[0xff, 0xff, 0xff, 0xff, 0x7f, 0]),
        Err(ExtError::BadVarint { .. })
    ));
    assert!(matches!(
        ext_validate(&[0xff, 0xff, 0xff, 0xff, 0xff, 0x01]),
        Err(ExtError::BadVarint { .. })
    ));
    // Malformed input never yields a value and cannot be edited.
    assert_eq!(ext_get(&[5, 0, 5, 0], 5), None);
    assert!(ext_put(&[5], 6, b"").is_err());
}

#[test]
fn apply_rejects_an_invalid_ext_on_every_kind() {
    for kind in MetaKind::ALL {
        let mut s = MetaState::default();
        let mut rec = MetaRecord::sample(kind, 3, 1);
        assert!(!rec.ext().is_empty(), "{kind:?} sample carries tag 15");
        assert_eq!(ext_get(rec.ext(), EXT_TAG_TEST), Some(&[3, 1][..]));
        *rec.ext_mut() = vec![2, 0, 1, 0];
        let r = s.apply(1, put(rec, None));
        assert!(
            matches!(&r, MetaResult::Invalid(m) if m.contains("ext")),
            "{kind:?}: {r:?}"
        );
        assert_eq!(s, MetaState::default(), "{kind:?}: unchanged");
    }
}

// ---- Phase 12 F7 -------------------------------------------------------------

#[test]
fn init_cluster_writes_partition_and_member_defaults() {
    let mut s = MetaState::default();
    let r = s.apply(
        1,
        MetaCommand::InitCluster {
            cluster_id: "c".into(),
            node_id: 7,
            partitions: 3,
            created_at_ms: 5,
            token_key: [0; 32],
        },
    );
    assert_eq!(r, MetaResult::Ok { version: 1 });
    for p in s.partitions() {
        assert_eq!(p.value.replicas, vec![7]);
        assert_eq!(p.value.isr, vec![7]);
        assert_eq!(p.value.isr_version, 0);
        assert_eq!(p.value.min_insync, 1);
        assert!(p.value.ext.is_empty());
    }
    let m = &s.member(7).unwrap().value;
    assert_eq!(
        (m.cluster_addr.as_deref(), m.repl_addr.as_deref()),
        (None, None)
    );
    assert!(s.cluster().unwrap().value.ext.is_empty());

    // SetPartitionLeader: validation unchanged; F7 fields carried over.
    let r = s.apply(
        2,
        MetaCommand::SetPartitionLeader {
            partition: 1,
            epoch: 1,
            leader: 8,
            isr: vec![8],
        },
    );
    assert_eq!(r, MetaResult::Ok { version: 1 });
    let p = &s.partition(1).unwrap().value;
    assert_eq!((p.leader, p.epoch, p.isr.clone()), (8, 1, vec![8]));
    assert_eq!(
        (p.replicas.clone(), p.isr_version, p.min_insync),
        (vec![7], 0, 1)
    );
    // An equal-epoch re-assert is still decided on leader/isr only.
    let again = MetaCommand::SetPartitionLeader {
        partition: 1,
        epoch: 1,
        leader: 8,
        isr: vec![8],
    };
    assert_eq!(s.apply(3, again), MetaResult::Ok { version: 1 });
}

// ---- Phase 14 F3 / F4, Phase 7 F8 ---------------------------------------------

type Edit = fn(&mut Tenant);

fn tenant(f: impl FnOnce(&mut Tenant)) -> MetaRecord {
    let mut t = Tenant {
        id: "acme".into(),
        created_at_ms: 1,
        ..Default::default()
    };
    f(&mut t);
    MetaRecord::Tenant(t)
}

fn verdict(record: MetaRecord) -> MetaResult {
    MetaState::default().apply(1, put(record, None))
}

#[test]
fn tenant_quotas_must_be_positive() {
    assert_eq!(verdict(tenant(|_| {})), MetaResult::Ok { version: 1 });
    assert_eq!(
        verdict(MetaRecord::sample(MetaKind::Tenant, 1, 1)),
        MetaResult::Ok { version: 1 }
    );
    let cases: [(&str, Edit); 6] = [
        ("weight", |t| t.weight = Some(0)),
        ("max_concurrency", |t| t.max_concurrency = Some(0)),
        ("fuel_per_s", |t| t.fuel_per_s = Some(0)),
        ("fuel_burst", |t| t.fuel_burst = Some(0)),
        ("memory_bytes", |t| t.memory_bytes = Some(0)),
        ("event_rate", |t| {
            t.event_rate = Some(RateSpec {
                limit: 1,
                period_ms: 0,
                burst: 0,
            })
        }),
    ];
    for (field, f) in cases {
        let r = verdict(tenant(f));
        assert!(
            matches!(&r, MetaResult::Invalid(m) if m.contains(field)),
            "{field}: {r:?}"
        );
    }
}

fn workflow(module: &str) -> MetaRecord {
    MetaRecord::Workflow(WorkflowVersion {
        name: "order".into(),
        version: 1,
        module: module.into(),
        ..Default::default()
    })
}

#[test]
fn workflow_module_follows_the_d23_grammar() {
    let long_name = format!("a{}", "b".repeat(127));
    let long_tenant = format!("t{}", "x".repeat(62));
    for ok in [
        "",
        "orders",
        "Orders_v2.1-x",
        "acme/orders",
        "0tenant/0",
        &long_name,
        &format!("{long_tenant}/{long_name}"),
    ] {
        assert_eq!(
            verdict(workflow(ok)),
            MetaResult::Ok { version: 1 },
            "{ok:?}"
        );
    }
    for bad in [
        "/orders",
        "acme/",
        "_/orders",
        "Acme/orders",
        "acme/__x",
        "__ostro",
        "-x",
        "a:b",
        "acme/a/b",
        "a b",
        "acme_co/orders",
        &format!("{long_name}c"),
        &format!("{long_tenant}y/orders"),
    ] {
        let r = verdict(workflow(bad));
        assert!(
            matches!(&r, MetaResult::Invalid(m) if m.contains("module")),
            "{bad:?}: {r:?}"
        );
    }
}

#[test]
fn token_expiry_is_stored() {
    let mut s = MetaState::default();
    let rec = MetaRecord::Token(ApiToken {
        id: "t".into(),
        expires_at_ms: Some(99),
        ..Default::default()
    });
    assert_eq!(s.apply(1, put(rec, None)), MetaResult::Ok { version: 1 });
    assert_eq!(s.token("t").unwrap().value.expires_at_ms, Some(99));
}
