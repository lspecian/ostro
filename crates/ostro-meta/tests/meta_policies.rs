//! 05-07: the pre-freeze meta shapes (D9 run policies, Phase 7 F2 fields,
//! Phase 8 F2(a) Schedule fields, Phase 8 F3 Webhook): ordinals fixed by
//! RESERVATIONS §10, CAS per new kind, apply-time validation, and a snapshot
//! round trip with every kind populated.

use ostro_meta::{
    MetaCommand, MetaKey, MetaKind, MetaRecord, MetaResult, MetaSm, MetaState, QueuePolicy,
    RateSpec, Schedule, SpillPolicy, StateMachine, Webhook, WorkflowPolicy,
};

fn put(record: MetaRecord, expected_version: Option<u64>) -> MetaCommand {
    MetaCommand::Put {
        key: record.key(),
        expected_version,
        record,
    }
}

fn variant_index<T: serde::Serialize>(v: &T) -> u8 {
    // A postcard enum starts with its variant index as a varint (< 128 here).
    postcard::to_stdvec(v).unwrap()[0]
}

const NEW_KINDS: [MetaKind; 3] = [
    MetaKind::WorkflowPolicy,
    MetaKind::QueuePolicy,
    MetaKind::Webhook,
];

#[test]
fn new_kinds_take_the_reserved_ordinals() {
    assert_eq!(MetaKind::ALL.len(), 17);
    assert_eq!(MetaKey::ALL_KINDS, MetaKind::ALL);
    for (i, kind) in MetaKind::ALL.into_iter().enumerate() {
        assert_eq!(usize::from(variant_index(&kind)), i, "{kind:?}");
        let rec = MetaRecord::sample(kind, 1, 1);
        assert_eq!(usize::from(variant_index(&rec)), i, "record {kind:?}");
        assert_eq!(usize::from(variant_index(&rec.key())), i, "key {kind:?}");
        assert_eq!(rec.kind(), kind);
    }
    assert_eq!(variant_index(&MetaKind::WorkflowPolicy), 14);
    assert_eq!(variant_index(&MetaKind::QueuePolicy), 15);
    assert_eq!(variant_index(&MetaKind::Webhook), 16);
    // Identity fields drive the key.
    let wp = MetaRecord::WorkflowPolicy(WorkflowPolicy {
        workflow: "acme/order".into(),
        ..Default::default()
    });
    assert_eq!(wp.key(), MetaKey::WorkflowPolicy("acme/order".into()));
    let qp = MetaRecord::QueuePolicy(QueuePolicy {
        queue: "emails".into(),
        ..Default::default()
    });
    assert_eq!(qp.key(), MetaKey::QueuePolicy("emails".into()));
    let wh = MetaRecord::Webhook(Webhook {
        name: "stripe".into(),
        ..Default::default()
    });
    assert_eq!(wh.key(), MetaKey::Webhook("stripe".into()));
}

#[test]
fn cas_rules_per_new_kind() {
    for kind in NEW_KINDS {
        let mut s = MetaState::default();
        let r1 = MetaRecord::sample(kind, 7, 1);
        let r2 = MetaRecord::sample(kind, 7, 2);
        let r3 = MetaRecord::sample(kind, 7, 3);
        let key = r1.key();

        // Absent + Some -> NotFound.
        assert_eq!(s.apply(1, put(r1.clone(), Some(1))), MetaResult::NotFound);
        // Create with None.
        assert_eq!(
            s.apply(2, put(r1.clone(), None)),
            MetaResult::Ok { version: 1 },
            "{kind:?}"
        );
        // Create again -> Conflict.
        assert_eq!(
            s.apply(3, put(r2.clone(), None)),
            MetaResult::Conflict { current: 1 }
        );
        // Update with the exact version.
        assert_eq!(
            s.apply(4, put(r2.clone(), Some(1))),
            MetaResult::Ok { version: 2 }
        );
        // Stale version.
        let before = s.clone();
        assert_eq!(
            s.apply(5, put(r3.clone(), Some(1))),
            MetaResult::StaleVersion { current: 2 }
        );
        assert_eq!(s, before, "{kind:?}: rejection leaves state unchanged");
        assert_eq!(s.get(&key).unwrap().value, r2);
        // Key mismatch -> Invalid.
        let other = MetaRecord::sample(kind, 8, 1);
        let r = s.apply(
            6,
            MetaCommand::Put {
                key: key.clone(),
                expected_version: None,
                record: other,
            },
        );
        assert!(matches!(r, MetaResult::Invalid(_)), "{kind:?}: {r:?}");
        // Delete with the exact version.
        assert_eq!(
            s.apply(
                7,
                MetaCommand::Delete {
                    key: key.clone(),
                    expected_version: 2
                }
            ),
            MetaResult::Ok { version: 2 }
        );
        assert!(s.get(&key).is_none());
    }
}

#[test]
fn typed_accessors_see_new_records() {
    let mut s = MetaState::default();
    for kind in NEW_KINDS.into_iter().chain([MetaKind::Schedule]) {
        assert_eq!(
            s.apply(1, put(MetaRecord::sample(kind, 1, 1), None)),
            MetaResult::Ok { version: 1 }
        );
    }
    assert_eq!(
        s.workflow_policy("workflowpolicy-1")
            .unwrap()
            .value
            .retention_ms,
        Some(1000)
    );
    assert!(
        s.queue_policy("queuepolicy-1")
            .unwrap()
            .value
            .spill
            .is_some()
    );
    assert_eq!(s.webhook("webhook-1").unwrap().value.tolerance_s, 300);
    assert_eq!(s.webhooks().count(), 1);
    assert_eq!(
        s.schedule("schedule-1").unwrap().value.timezone,
        "Europe/Lisbon"
    );
    assert_eq!(s.schedules().count(), 1);
}

fn assert_invalid(record: MetaRecord, what: &str) {
    let mut s = MetaState::default();
    let r = s.apply(1, put(record, None));
    assert!(matches!(r, MetaResult::Invalid(_)), "{what}: {r:?}");
    assert_eq!(s, MetaState::default(), "{what}: state unchanged");
}

fn assert_ok(record: MetaRecord, what: &str) {
    let mut s = MetaState::default();
    assert_eq!(
        s.apply(1, put(record, None)),
        MetaResult::Ok { version: 1 },
        "{what}"
    );
}

fn schedule(f: impl FnOnce(&mut Schedule)) -> MetaRecord {
    let mut s = Schedule {
        name: "nightly".into(),
        workflow: "report".into(),
        cron: "0 3 * * *".into(),
        ..Default::default()
    };
    f(&mut s);
    MetaRecord::Schedule(s)
}

#[test]
fn schedule_validation() {
    assert_ok(schedule(|_| {}), "defaults");
    assert_ok(
        schedule(|s| {
            s.overlap = 4;
            s.catchup = 2;
            s.timezone = "x".repeat(64);
        }),
        "upper bounds",
    );
    assert_invalid(schedule(|s| s.overlap = 5), "overlap 5");
    assert_invalid(schedule(|s| s.catchup = 3), "catchup 3");
    assert_invalid(
        schedule(|s| s.timezone = "x".repeat(65)),
        "timezone 65 bytes",
    );
}

fn webhook(f: impl FnOnce(&mut Webhook)) -> MetaRecord {
    let mut w = Webhook {
        name: "gh".into(),
        ..Default::default()
    };
    f(&mut w);
    MetaRecord::Webhook(w)
}

#[test]
fn webhook_validation() {
    assert_ok(
        webhook(|w| {
            w.action = 2;
            w.verifier = 5;
        }),
        "upper bounds",
    );
    assert_invalid(webhook(|w| w.action = 3), "action 3");
    assert_invalid(webhook(|w| w.verifier = 6), "verifier 6");
}

fn queue_policy(f: impl FnOnce(&mut QueuePolicy)) -> MetaRecord {
    let mut p = QueuePolicy {
        queue: "q".into(),
        ..Default::default()
    };
    f(&mut p);
    MetaRecord::QueuePolicy(p)
}

#[test]
fn policy_validation() {
    let rate = |period_ms| {
        Some(RateSpec {
            limit: 10,
            period_ms,
            burst: 0,
        })
    };
    let spill = SpillPolicy {
        cpu_pct: 100,
        slots_pct: 100,
        mem_pct: 100,
        hysteresis_pct: 100,
        ..Default::default()
    };
    assert_ok(
        queue_policy(|p| {
            p.placement = Some(2);
            p.on_retries_exhausted = Some(1);
            p.selector = vec!["a".repeat(128); 32];
            p.spill = Some(spill.clone());
            p.throttle = rate(1);
            p.key_throttle = rate(1);
            p.rate_limit = rate(1);
            p.key_rate_limit = rate(1);
        }),
        "upper bounds",
    );
    assert_invalid(queue_policy(|p| p.placement = Some(3)), "placement 3");
    assert_invalid(
        queue_policy(|p| p.on_retries_exhausted = Some(2)),
        "on_retries_exhausted 2",
    );
    assert_invalid(
        queue_policy(|p| p.selector = vec!["a".into(); 33]),
        "33 selectors",
    );
    assert_invalid(
        queue_policy(|p| p.selector = vec!["a".repeat(129)]),
        "selector 129 bytes",
    );
    for pct in 0..4 {
        let mut sp = spill.clone();
        match pct {
            0 => sp.cpu_pct = 101,
            1 => sp.slots_pct = 101,
            2 => sp.mem_pct = 101,
            _ => sp.hysteresis_pct = 101,
        }
        assert_invalid(queue_policy(|p| p.spill = Some(sp)), "pct 101");
    }
    assert_invalid(queue_policy(|p| p.throttle = rate(0)), "throttle period 0");
    assert_invalid(
        queue_policy(|p| p.key_throttle = rate(0)),
        "key_throttle period 0",
    );
    assert_invalid(
        queue_policy(|p| p.rate_limit = rate(0)),
        "rate_limit period 0",
    );
    assert_invalid(
        queue_policy(|p| p.key_rate_limit = rate(0)),
        "key_rate_limit period 0",
    );

    assert_ok(
        MetaRecord::WorkflowPolicy(WorkflowPolicy {
            workflow: "w".into(),
            on_retries_exhausted: Some(1),
            paused: true,
            ..Default::default()
        }),
        "workflow policy",
    );
    assert_invalid(
        MetaRecord::WorkflowPolicy(WorkflowPolicy {
            workflow: "w".into(),
            on_retries_exhausted: Some(2),
            ..Default::default()
        }),
        "workflow on_retries_exhausted 2",
    );
}

#[test]
fn snapshot_round_trips_with_every_kind_populated() {
    let (mut sm, view) = MetaSm::new();
    let mut state = MetaState::default();
    let mut index = 1;
    state.apply(
        index,
        MetaCommand::InitCluster {
            cluster_id: "c".into(),
            node_id: 1,
            partitions: 4,
            created_at_ms: 1,
            token_key: [1; 32],
        },
    );
    for kind in MetaKind::ALL {
        if kind == MetaKind::Cluster {
            continue;
        }
        index += 1;
        assert_eq!(
            state.apply(index, put(MetaRecord::sample(kind, 9, 1), None)),
            MetaResult::Ok { version: 1 },
            "{kind:?}"
        );
    }
    for kind in MetaKind::ALL {
        let key = MetaRecord::sample(kind, 9, 1).key();
        assert!(state.get(&key).is_some(), "{kind:?} populated");
    }
    // Load it into a state machine through a snapshot and back.
    let (mut src, _src_view) = MetaSm::new();
    let snap = {
        // Replay the same commands through the SM so the snapshot is real.
        let mut i = 1;
        src.apply(
            i,
            1,
            &ostro_meta::encode_command(&MetaCommand::InitCluster {
                cluster_id: "c".into(),
                node_id: 1,
                partitions: 4,
                created_at_ms: 1,
                token_key: [1; 32],
            }),
        );
        for kind in MetaKind::ALL {
            if kind == MetaKind::Cluster {
                continue;
            }
            i += 1;
            src.apply(
                i,
                1,
                &ostro_meta::encode_command(&put(MetaRecord::sample(kind, 9, 1), None)),
            );
        }
        src.snapshot()
    };
    sm.restore(index, &snap).expect("restore");
    assert_eq!(view.read(|s| s.clone()), state);
    assert_eq!(sm.snapshot(), snap, "byte-identical re-snapshot");
}
