//! META-01: single-voter MetaHandle elects itself immediately (campaign), gives
//! read-your-writes, and its records survive a restart in the same directory.

use std::time::{Duration, Instant};

use ostro_meta::{
    ApiToken, MetaCommand, MetaError, MetaHandle, MetaKey, MetaRecord, MetaResult, ModuleRecord,
    QueueConfig, WorkflowVersion,
};

const SHA: [u8; 32] = [0xab; 32];

async fn start(dir: &std::path::Path) -> MetaHandle {
    let t0 = Instant::now();
    let h = MetaHandle::start(dir, 1).await.expect("meta starts");
    let elapsed = t0.elapsed();
    println!("meta start -> leader in {elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(2),
        "start->leader took {elapsed:?} (campaign() not effective?)"
    );
    assert!(h.is_leader());
    h
}

fn put(record: MetaRecord, expected_version: Option<u64>) -> MetaCommand {
    MetaCommand::Put {
        key: record.key(),
        expected_version,
        record,
    }
}

fn records() -> Vec<MetaRecord> {
    vec![
        MetaRecord::Module(ModuleRecord {
            sha256: SHA,
            size: 1234,
            language: Some("rust".into()),
            workflows: vec![("greet".into(), 1)],
            uploaded_at_ms: 10,
            ..Default::default()
        }),
        MetaRecord::Workflow(WorkflowVersion {
            name: "greet".into(),
            version: 1,
            module_sha256: SHA,
            default_queue: Some("default".into()),
            run_timeout_ms: None,
            created_at_ms: 10,
            ..Default::default()
        }),
        MetaRecord::Queue(QueueConfig {
            name: "default".into(),
            max_concurrency: Some(8),
            created_at_ms: 10,
            ..Default::default()
        }),
        MetaRecord::Token(ApiToken {
            id: "t1".into(),
            hash: [1; 32],
            scopes: vec!["admin".into()],
            tenant: None,
            created_at_ms: 10,
            ..Default::default()
        }),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn records_survive_restart_and_versions_never_regress() {
    let dir = tempfile::tempdir().unwrap();

    // ---- first boot ----
    let h = start(dir.path()).await;
    let info = h.init_cluster("c1", 1, 16, 5, [7; 32]).await.expect("init");
    assert_eq!(info.partitions, 16);
    for r in records() {
        let key = r.key();
        assert_eq!(
            h.propose(put(r.clone(), None)).await.unwrap(),
            MetaResult::Ok { version: 1 }
        );
        // Read-your-writes: visible immediately after propose returns.
        assert_eq!(h.view().read(|s| s.get(&key).map(|v| v.value)), Some(r));
    }
    // Bump the queue to version 2 so the restart check covers a CAS chain.
    let q2 = MetaRecord::Queue(QueueConfig {
        name: "default".into(),
        max_concurrency: Some(16),
        created_at_ms: 11,
        ..Default::default()
    });
    assert_eq!(
        h.propose(put(q2.clone(), Some(1))).await.unwrap(),
        MetaResult::Ok { version: 2 }
    );
    let before = h.view().read(|s| s.clone());
    h.stop().await;

    // ---- restart in the same dir with a fresh MetaSm ----
    let h = start(dir.path()).await;
    let after = h.view().read(|s| s.clone());
    assert_eq!(after, before, "state replayed exactly");
    assert_eq!(after.module(&SHA).unwrap().version, 1);
    assert_eq!(after.workflow("greet", 1).unwrap().version, 1);
    assert_eq!(after.queue("default").unwrap().version, 2);
    assert_eq!(after.token("t1").unwrap().version, 1);
    assert_eq!(after.token_key(), Some([7; 32]));
    assert_eq!(after.partitions().count(), 16);

    // InitCluster is idempotent with the same P, rejected with a different P.
    let again = h
        .init_cluster("other-id", 1, 16, 99, [9; 32])
        .await
        .expect("idempotent init");
    assert_eq!(again.cluster_id, "c1");
    assert_eq!(
        h.view().read(|s| s.token_key()),
        Some([7; 32]),
        "token key not replaced"
    );
    match h.init_cluster("c1", 1, 8, 5, [7; 32]).await {
        Err(MetaError::Rejected(msg)) => assert!(msg.contains("partition count mismatch"), "{msg}"),
        other => panic!("expected rejection, got {other:?}"),
    }

    // A pre-restart stale version is still rejected.
    let q3 = MetaRecord::Queue(QueueConfig {
        name: "default".into(),
        max_concurrency: Some(1),
        created_at_ms: 12,
        ..Default::default()
    });
    assert_eq!(
        h.propose(put(q3, Some(1))).await.unwrap(),
        MetaResult::StaleVersion { current: 2 }
    );
    assert_eq!(
        h.propose(MetaCommand::Delete {
            key: MetaKey::Module(SHA),
            expected_version: 0
        })
        .await
        .unwrap(),
        MetaResult::StaleVersion { current: 1 }
    );
    assert_eq!(
        h.view().read(|s| s.clone()),
        before,
        "rejections change nothing"
    );
    h.stop().await;
}

/// D20: a data dir whose Raft log holds a pre-Phase-5 (META_FORMAT 1) entry is
/// refused at start with the "delete the dev data dir" message, instead of
/// booting with that entry silently dropped.
#[tokio::test(flavor = "multi_thread")]
async fn pre_freeze_log_entry_fails_start() {
    let dir = tempfile::tempdir().unwrap();
    let h = start(dir.path()).await;
    h.init_cluster("c1", 1, 4, 5, [7; 32]).await.expect("init");
    // A Phase 4 dev build wrote this entry: [1] ‖ postcard(old command). The
    // body does not matter; the format byte is rejected before decoding.
    let applied = h
        .raft()
        .propose(vec![ostro_meta::PRE_FREEZE_META_FORMAT, 1, 2, 3])
        .await
        .expect("raw propose");
    let r = ostro_meta::decode_result(&applied.result).unwrap();
    assert!(matches!(r, MetaResult::Invalid(_)), "{r:?}");
    h.stop().await;

    match MetaHandle::start(dir.path(), 1).await {
        Err(MetaError::Start(msg)) => {
            assert!(msg.contains("delete the dev data dir"), "{msg}")
        }
        Err(e) => panic!("expected a start error, got {e:?}"),
        Ok(_) => panic!("a pre-freeze data dir must not start"),
    }
}
