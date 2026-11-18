//! Crash-window tests for BlobStore::put (run with `--features failpoints
//! -- --test-threads=1`; fail points are process-global).

use std::fs;
use std::path::Path;

use ostro_blob::{BlobError, BlobRef, BlobStore};
use sha2::{Digest, Sha256};

fn tmp_files(data: &Path) -> Vec<std::path::PathBuf> {
    let mut v: Vec<_> = fs::read_dir(data.join("tmp"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    v.sort();
    v
}

fn blob_files(data: &Path) -> usize {
    let mut n = 0;
    for shard in fs::read_dir(data.join("blobs")).unwrap() {
        let shard = shard.unwrap().path();
        n += fs::read_dir(shard).unwrap().count();
    }
    n
}

fn ref_of(bytes: &[u8]) -> BlobRef {
    BlobRef {
        sha256: Sha256::digest(bytes).into(),
        len: bytes.len() as u64,
    }
}

#[test]
fn crash_before_rename_leaves_no_visible_blob() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let bytes = vec![0x5au8; 300 * 1024];
    let r = ref_of(&bytes);

    {
        let store = BlobStore::open(dir.path()).unwrap();
        fail::cfg("blob::before_rename", "return").unwrap();
        let res = store.put(&bytes);
        fail::remove("blob::before_rename");
        assert!(matches!(res, Err(BlobError::Io(_))), "got {res:?}");
        assert!(!store.contains(&r));
        assert!(!store.path_of(&r).exists());
        let tmps = tmp_files(dir.path());
        assert_eq!(tmps.len(), 1, "tmp file must remain after crash: {tmps:?}");
        assert_eq!(blob_files(dir.path()), 0);
    }

    // Restart: open() removes the orphan, nothing becomes visible.
    let store = BlobStore::open(dir.path()).unwrap();
    assert!(tmp_files(dir.path()).is_empty());
    assert!(!store.contains(&r));
    assert!(matches!(store.get(&r), Err(BlobError::NotFound(_))));

    // A retry after restart succeeds.
    assert_eq!(store.put(&bytes).unwrap(), r);
    assert_eq!(store.get(&r).unwrap(), bytes);
    scenario.teardown();
}

#[test]
fn fsync_error_fails_put_and_nothing_is_visible() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let bytes = b"fsync will fail".repeat(1000);
    let r = ref_of(&bytes);

    let store = BlobStore::open(dir.path()).unwrap();
    fail::cfg("blob::fsync", "return").unwrap();
    let res = store.put(&bytes);
    fail::remove("blob::fsync");
    match res {
        Err(BlobError::Io(e)) => assert_eq!(e.raw_os_error(), Some(5), "expected EIO"),
        other => panic!("expected Io(EIO), got {other:?}"),
    }
    assert!(!store.contains(&r));
    assert_eq!(blob_files(dir.path()), 0);

    let store = BlobStore::open(dir.path()).unwrap();
    assert!(tmp_files(dir.path()).is_empty());
    assert!(!store.contains(&r));
    scenario.teardown();
}
