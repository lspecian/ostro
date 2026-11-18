// Ostro-authored (see VENDORED.md): patch P2 (parent-directory fsync) tests.

#![allow(clippy::all)] // each integration test is its own crate; vendored crates allow clippy

//! Segment create, rotation, truncation and deletion fsync the partition
//! directory before acknowledging. These run without failpoints: they check
//! the helper itself and that the create/rotate paths still produce visible,
//! parseable segments with the extra fsyncs in place.

use std::path::Path;

use ostro_wal::group_commit::fsync_dir;
use ostro_wal::{GroupCommitConfig, GroupCommitWal, WalRecord};

const TOPIC: &str = "p";

fn record(offset: i64) -> WalRecord {
    WalRecord::new_v2(TOPIC.to_string(), 0, vec![0xAB; 64], offset, offset, 1)
}

fn segment(dir: &Path, id: u64) -> std::path::PathBuf {
    dir.join(TOPIC).join("0").join(format!("wal_0_{id}.log"))
}

#[tokio::test]
async fn fsync_dir_ok_on_real_dir_err_on_missing() {
    let dir = tempfile::tempdir().unwrap();
    fsync_dir(dir.path()).await.expect("fsync of an existing directory");
    let missing = dir.path().join("does-not-exist");
    assert!(fsync_dir(&missing).await.is_err(), "missing path must be an error");
}

#[tokio::test]
async fn new_segment_is_visible_after_first_ack() {
    let dir = tempfile::tempdir().unwrap();
    let wal = GroupCommitWal::new(dir.path().to_path_buf(), GroupCommitConfig::low_resource());
    wal.append(TOPIC.into(), 0, record(0), 1).await.expect("append");
    let path = segment(dir.path(), 0);
    assert!(path.exists(), "segment file must exist once a record is acked");
    assert!(std::fs::metadata(&path).unwrap().len() > 0);
}

#[tokio::test]
async fn rotation_leaves_a_parseable_sealed_segment() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = GroupCommitConfig::low_resource();
    config.rotation_size_bytes = 1; // rotate after every batch
    let wal = GroupCommitWal::new(dir.path().to_path_buf(), config);

    wal.append(TOPIC.into(), 0, record(0), 1).await.expect("append 0");
    wal.append(TOPIC.into(), 0, record(1), 1).await.expect("append 1");

    // Rotation happens right after a commit; give the worker a moment.
    for _ in 0..100 {
        if segment(dir.path(), 1).exists() && !wal.get_sealed_segments().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(segment(dir.path(), 1).exists(), "rotation must create the next segment");

    let sealed = wal.get_sealed_segments();
    let first = sealed
        .iter()
        .find(|s| s.segment_id == 0)
        .expect("segment 0 recorded as sealed");
    let bytes = std::fs::read(&first.file_path).unwrap();
    let parsed = WalRecord::from_bytes(&bytes).expect("sealed segment parses");
    match parsed {
        WalRecord::V2 { base_offset, .. } => assert_eq!(base_offset, 0),
        other => panic!("unexpected record {other:?}"),
    }
}

#[tokio::test]
async fn truncation_still_cuts_with_dir_fsync() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = GroupCommitConfig::low_resource();
    config.enable_rotation = false;
    let wal = GroupCommitWal::new(dir.path().to_path_buf(), config);
    for o in 0..5 {
        wal.append(TOPIC.into(), 0, record(o), 1).await.expect("append");
    }
    let outcome = wal.truncate_to(TOPIC, 0, 3).await.expect("truncate");
    assert_eq!(outcome.new_log_end_offset, Some(3));
    assert!(!wal.is_poisoned(TOPIC, 0));
    wal.append(TOPIC.into(), 0, record(3), 1).await.expect("append after truncation");
}
