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

/// Blob GC crash window (08-19): a panic at `blobgc::before_unlink` (between the mark and
/// the unlink) loses no referenced blob; the store reopens and a rerun completes.
#[test]
fn gc_panic_before_unlink_is_safe_and_rerun_completes() {
    use std::collections::HashSet;
    use std::fs::OpenOptions;
    use std::time::{Duration, SystemTime};

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let hour = Duration::from_secs(3600);
    let keep_bytes = b"referenced".repeat(100);
    let drop_bytes: Vec<Vec<u8>> = (0..4u8).map(|i| vec![i; 2048]).collect();
    let (keep, drops) = {
        let store = BlobStore::open(dir.path()).unwrap();
        let keep = store.put(&keep_bytes).unwrap();
        let drops: Vec<BlobRef> = drop_bytes.iter().map(|b| store.put(b).unwrap()).collect();
        for r in drops.iter().chain([&keep]) {
            let f = OpenOptions::new().write(true).open(store.path_of(r)).unwrap();
            f.set_modified(SystemTime::now() - 2 * hour).unwrap();
        }
        (keep, drops)
    };
    let mark: HashSet<[u8; 32]> = [keep.sha256].into_iter().collect();
    let cutoff = SystemTime::now() - hour;

    {
        let store = BlobStore::open(dir.path()).unwrap();
        fail::cfg("blobgc::before_unlink", "1*off->panic").unwrap();
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            store.sweep(&mark, cutoff)
        }));
        fail::remove("blobgc::before_unlink");
        assert!(res.is_err(), "sweep must have panicked at the failpoint");
        assert!(store.contains(&keep), "referenced blob kept");
        // The lock is poisoned but still usable: put works on the same instance.
        assert_eq!(store.put(&keep_bytes).unwrap(), keep);
    }

    let store = BlobStore::open(dir.path()).unwrap();
    assert!(store.contains(&keep));
    assert_eq!(store.get(&keep).unwrap(), keep_bytes);
    let gone_after_crash = drops.iter().filter(|r| !store.contains(r)).count();
    assert_eq!(gone_after_crash, 1, "exactly the unlink before the panic happened");
    let rep = store.sweep(&mark, cutoff).unwrap();
    assert_eq!(rep.deleted, 3, "the rerun completes the sweep");
    assert!(drops.iter().all(|r| !store.contains(r)));
    assert!(store.contains(&keep));
    scenario.teardown();
}
