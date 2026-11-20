//! Chain start rule (B4, plan 08-13): (a) fresh prefix at offset 0, (b) backup enabled on an
//! already-trimmed node, (c) a retention pass before the first upload, (d) uploaded end below
//! log_start after a long outage.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::path::Path;
use std::sync::Arc;

use common::{
    keys, local_store, node_cfg, open, shutdown, simulate_restore, start_runs, up_cfg, uploader,
};
use ostro_backup::chunk;
use ostro_backup::manifest::Manifest;
use ostro_log::ReplicatedLog as _;
use ostro_partition::Node;
use ostro_types::{Offset, PartitionId};

const P0: PartitionId = PartitionId(0);

fn live_digest(node: &Node, p: PartitionId) -> [u8; 32] {
    node.handle(p)
        .expect("ready")
        .store()
        .digest()
        .expect("digest")
}

/// Three snapshots with writes in between, then a retention pass (backup off: trims to the
/// local floor).
async fn trimmed_session(dir: &Path, backup: bool) -> Offset {
    let mut cfg = node_cfg(dir, 1);
    cfg.backup_enabled = backup;
    let node = open(cfg).await;
    for _ in 0..3 {
        start_runs(&node, 0, 60).await;
        node.snapshot_now(P0).await.expect("snap").expect("new");
    }
    start_runs(&node, 0, 10).await;
    let start = node.retention_pass(P0).await.expect("pass");
    shutdown(node).await;
    start
}

async fn check_restore(store: &Arc<dyn ostro_objstore::ObjectStore>, m: &Manifest, node: &Node) {
    let restored = simulate_restore(store, m, 1).await;
    assert_eq!(restored[0].applied, m.cut[0].cut_offset);
    assert_eq!(
        restored[0].digest,
        live_digest(node, P0),
        "snapshot + chunks reproduce the live digest"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_prefix_without_snapshots_starts_at_zero() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(dir.path(), 1)).await;
    start_runs(&node, 0, 25).await;
    let up = uploader(&node, up_cfg());
    let m = up
        .round(&store)
        .await
        .expect("round")
        .manifest
        .expect("manifest");
    m.validate().expect("valid");
    let e = &m.cut[0];
    assert_eq!(e.chain_start, 0);
    assert!(e.snapshot_key.is_none() && e.snapshot_offset.is_none());
    assert!(keys(&store, "p/0000/snap/").await.is_empty());
    check_restore(&store, &m, &node).await;
    drop(up);
    shutdown(node).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b_backup_enabled_on_an_already_trimmed_node() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let start = trimmed_session(dir.path(), false).await;
    assert!(start > Offset(0), "session 1 trimmed (log_start {start})");
    let node = open(node_cfg(dir.path(), 1)).await;
    let h = node.handle(P0).expect("ready");
    assert_eq!(h.log().log_start(), start);
    let newest = node
        .local_snapshots(P0)
        .await
        .last()
        .expect("snapshot")
        .applied_offset;
    let up = uploader(&node, up_cfg());
    let r = up.round(&store).await.expect("round");
    assert_eq!(r.snapshots, 2, "both local snapshots uploaded");
    let m = r.manifest.expect("manifest");
    let e = &m.cut[0];
    assert_eq!(
        e.chain_start, newest.0,
        "chain starts at the newest snapshot"
    );
    assert_eq!(e.snapshot_offset, Some(newest.0));
    // The first chunk links to nothing.
    let segs = keys(&store, "p/0000/seg/").await;
    let first = segs
        .iter()
        .min_by_key(|k| chunk::parse_chunk_key(k).expect("key").2)
        .expect("a chunk");
    let (b, _) = store.get(first).await.expect("get").expect("chunk");
    let c = chunk::decode(&b).expect("decode");
    assert_eq!(c.header.base, newest.0);
    assert_eq!(c.header.prev_chunk_sha256, [0; 32]);
    check_restore(&store, &m, &node).await;
    drop(up);
    shutdown(node).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c_retention_before_the_first_upload_trims_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let start = trimmed_session(dir.path(), true).await;
    assert_eq!(
        start,
        Offset(0),
        "upload floor Some(log_start) held trimming back"
    );
    let node = open(node_cfg(dir.path(), 1)).await;
    assert_eq!(node.retention_pass(P0).await.expect("pass"), Offset(0));
    let up = uploader(&node, up_cfg());
    let m = up
        .round(&store)
        .await
        .expect("round")
        .manifest
        .expect("manifest");
    let floor = m.cut[0].snapshot_offset.expect("snapshot");
    assert_eq!(
        *node.floor_handles(P0).expect("handles").upload.borrow(),
        Some(Offset(floor))
    );
    let start = node.retention_pass(P0).await.expect("pass");
    assert!(
        start > Offset(0) && start.0 <= floor,
        "log_start {start}, floor {floor}"
    );
    check_restore(&store, &m, &node).await;
    drop(up);
    shutdown(node).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d_uploaded_end_below_log_start_restarts_from_a_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    // Session 1: backup on, a chain from 0.
    let first = {
        let node = open(node_cfg(dir.path(), 1)).await;
        start_runs(&node, 0, 10).await;
        let up = uploader(&node, up_cfg());
        let m = up
            .round(&store)
            .await
            .expect("round")
            .manifest
            .expect("manifest");
        drop(up);
        shutdown(node).await;
        m
    };
    let old_end = first.cut[0].cut_offset;
    let old_segs = keys(&store, "p/0000/seg/").await;
    // Session 2: backup off for a while (outage); the node trims past the uploaded end.
    let start = trimmed_session(dir.path(), false).await;
    assert!(
        start.0 > old_end,
        "log_start {start} > uploaded end {old_end}"
    );
    // Session 3: backup back on; the chain restarts from the newest snapshot.
    let node = open(node_cfg(dir.path(), 1)).await;
    let newest = node
        .local_snapshots(P0)
        .await
        .last()
        .expect("snapshot")
        .applied_offset;
    let up = uploader(&node, up_cfg());
    let m = up
        .round(&store)
        .await
        .expect("round")
        .manifest
        .expect("manifest");
    let e = &m.cut[0];
    assert_eq!(
        m.generation,
        first.generation + 1,
        "continues the manifest chain"
    );
    assert_eq!(e.chain_start, newest.0);
    assert_eq!(e.snapshot_offset, Some(newest.0));
    m.validate().expect("valid");
    // The old chain is no longer reachable: retention removed it.
    let segs = keys(&store, "p/0000/seg/").await;
    assert!(
        old_segs.iter().all(|k| !segs.contains(k)),
        "old chain deleted"
    );
    check_restore(&store, &m, &node).await;
    drop(up);
    shutdown(node).await;
}
