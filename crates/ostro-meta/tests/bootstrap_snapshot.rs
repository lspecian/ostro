//! 08-18 spike + contract: `MetaHandle::bootstrap_from_snapshot` installs a metadata snapshot
//! natively into the Raft log store of an empty directory (no leader needed). The restored
//! `MetaState` is identical, record `updated_index` values included, the applied index is the
//! snapshot's, and later proposals continue after it.

use ostro_meta::{
    MetaCommand, MetaError, MetaHandle, MetaRecord, MetaResult, ModuleRecord, QueueConfig,
    encode_snapshot,
};

fn put(record: MetaRecord) -> MetaCommand {
    MetaCommand::Put {
        key: record.key(),
        expected_version: None,
        record,
    }
}

fn queue(name: &str) -> MetaRecord {
    MetaRecord::Queue(QueueConfig {
        name: name.into(),
        max_concurrency: Some(4),
        created_at_ms: 10,
        ..Default::default()
    })
}

/// A source group with a few records; returns `(index, snapshot bytes)` read atomically.
async fn source_snapshot(dir: &std::path::Path) -> (u64, Vec<u8>) {
    let h = MetaHandle::start(dir, 1).await.expect("meta starts");
    h.init_cluster("c-1", 1, 4, 100, [7; 32])
        .await
        .expect("init cluster");
    for name in ["a", "b", "c"] {
        let r = h.propose(put(queue(name))).await.expect("propose");
        assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    }
    let r = h
        .propose(put(MetaRecord::Module(ModuleRecord {
            sha256: [0xab; 32],
            size: 12,
            workflows: vec![("w".into(), 1)],
            uploaded_at_ms: 11,
            ..Default::default()
        })))
        .await
        .expect("module");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    let view = h.view().clone();
    let snap = view.read(|s| (view.applied(), encode_snapshot(s)));
    h.stop().await;
    snap
}

#[tokio::test]
async fn bootstrap_restores_identical_state_and_continues_after_the_index() {
    let src = tempfile::tempdir().expect("tempdir");
    let (index, bytes) = source_snapshot(src.path()).await;
    assert!(index > 1, "index {index}");

    let dst = tempfile::tempdir().expect("tempdir");
    let meta_dir = dst.path().join("meta");
    let h = MetaHandle::bootstrap_from_snapshot(&meta_dir, 1, index, &bytes)
        .await
        .expect("bootstrap");
    assert!(h.is_leader());
    // Identical state, record versions / updated indices included (native install).
    let restored = h.view().read(encode_snapshot);
    assert_eq!(restored, bytes, "restored MetaState differs");
    let st = h.raft_status();
    assert!(st.applied_index >= index, "{st:?}");
    assert_eq!(
        h.view()
            .read(|s| s.cluster().map(|c| c.value.cluster_id.clone())),
        Some("c-1".to_owned())
    );

    // Later proposals continue after the snapshot index.
    let r = h.propose(put(queue("d"))).await.expect("propose after");
    let MetaResult::Ok { .. } = r else {
        panic!("{r:?}")
    };
    let after = h.raft_status().applied_index;
    assert!(after > index, "applied {after} not after {index}");
    // InitCluster with the same partition count stays idempotent (server start path).
    let c = h
        .init_cluster("other", 1, 4, 999, [9; 32])
        .await
        .expect("idempotent init");
    assert_eq!(c.cluster_id, "c-1");
    h.stop().await;

    // A restart in the same directory replays snapshot + new entries.
    let h = MetaHandle::start(&meta_dir, 1).await.expect("restart");
    let n = h.view().read(|s| s.queues().count());
    assert_eq!(n, 4);
    h.stop().await;
}

#[tokio::test]
async fn bootstrap_refuses_an_existing_log_and_a_bad_snapshot() {
    let src = tempfile::tempdir().expect("tempdir");
    let (index, bytes) = source_snapshot(src.path()).await;
    // The source directory already has a log.
    let e = MetaHandle::bootstrap_from_snapshot(src.path(), 1, index, &bytes)
        .await
        .expect_err("existing log");
    assert!(
        matches!(&e, MetaError::Start(m) if m.contains("already exists")),
        "{e}"
    );

    let dst = tempfile::tempdir().expect("tempdir");
    let e = MetaHandle::bootstrap_from_snapshot(dst.path(), 1, index, b"garbage")
        .await
        .expect_err("bad snapshot");
    assert!(
        matches!(&e, MetaError::Start(m) if m.contains("snapshot")),
        "{e}"
    );
    assert_eq!(
        std::fs::read_dir(dst.path()).expect("read dir").count(),
        0,
        "nothing written for a bad snapshot"
    );
}
