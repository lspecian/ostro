//! Blob GC (P8-D8, plan 08-19): mtime touch on put, sweep with mark + grace, and the
//! put/sweep race.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::sync::{Arc, Barrier};
use std::time::{Duration, SystemTime};

use ostro_blob::{BlobRef, BlobStore};

const HOUR: Duration = Duration::from_secs(3600);

fn payload(tag: u32, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u32).wrapping_mul(31).wrapping_add(tag) as u8)
        .collect()
}

fn age(store: &BlobStore, r: &BlobRef, by: Duration) {
    let f = OpenOptions::new()
        .write(true)
        .open(store.path_of(r))
        .unwrap();
    f.set_modified(SystemTime::now() - by).unwrap();
}

fn mtime(store: &BlobStore, r: &BlobRef) -> SystemTime {
    fs::metadata(store.path_of(r)).unwrap().modified().unwrap()
}

fn cutoff() -> SystemTime {
    SystemTime::now() - HOUR
}

#[test]
fn unreferenced_old_blob_is_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStore::open(dir.path()).unwrap();
    let bytes = payload(1, 4096);
    let r = store.put(&bytes).unwrap();
    age(&store, &r, 2 * HOUR);
    let rep = store.sweep(&HashSet::new(), cutoff()).unwrap();
    assert_eq!(rep.scanned, 1);
    assert_eq!(rep.deleted, 1);
    assert_eq!(rep.bytes_freed, 4096);
    assert_eq!(rep.deleted_shas, vec![r.sha256]);
    assert!(!store.contains(&r));
    // The store stays usable and the layout is unchanged.
    assert_eq!(store.put(&bytes).unwrap(), r);
    assert_eq!(store.get(&r).unwrap(), bytes);
}

#[test]
fn unreferenced_young_blob_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStore::open(dir.path()).unwrap();
    let r = store.put(&payload(2, 100)).unwrap();
    age(&store, &r, HOUR / 2);
    let rep = store.sweep(&HashSet::new(), cutoff()).unwrap();
    assert_eq!((rep.deleted, rep.kept_young), (0, 1));
    assert!(store.contains(&r));
}

#[test]
fn referenced_old_blob_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStore::open(dir.path()).unwrap();
    let r = store.put(&payload(3, 100)).unwrap();
    let other = store.put(&payload(4, 100)).unwrap();
    age(&store, &r, 10 * HOUR);
    age(&store, &other, 10 * HOUR);
    let mark: HashSet<[u8; 32]> = [r.sha256].into_iter().collect();
    let rep = store.sweep(&mark, cutoff()).unwrap();
    assert_eq!((rep.deleted, rep.kept_marked), (1, 1));
    assert!(store.contains(&r));
    assert!(!store.contains(&other));
}

#[test]
fn re_put_touches_mtime_and_keeps_the_blob() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStore::open(dir.path()).unwrap();
    let bytes = payload(5, 300 * 1024);
    let r = store.put(&bytes).unwrap();
    age(&store, &r, 2 * HOUR);
    let old = mtime(&store, &r);
    let mut rx = store.subscribe_puts();
    assert_eq!(store.put(&bytes).unwrap(), r);
    assert_eq!(rx.try_recv().unwrap(), r.sha256, "re-put still notifies");
    assert!(mtime(&store, &r) > old + HOUR, "mtime touched");
    let rep = store.sweep(&HashSet::new(), cutoff()).unwrap();
    assert_eq!(rep.deleted, 0);
    assert_eq!(store.get(&r).unwrap(), bytes, "content never rewritten");
}

#[test]
fn unknown_files_are_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStore::open(dir.path()).unwrap();
    let r = store.put(&payload(6, 10)).unwrap();
    age(&store, &r, 2 * HOUR);
    let shard = store.path_of(&r).parent().unwrap().to_path_buf();
    fs::write(shard.join("not-a-blob"), b"x").unwrap();
    fs::write(dir.path().join("blobs/README"), b"x").unwrap();
    // A valid sha name in the wrong shard is not a blob either.
    let wrong = dir.path().join("blobs/zz");
    fs::create_dir_all(&wrong).unwrap();
    fs::write(wrong.join(ostro_blob::hex(&r.sha256)), b"x").unwrap();
    let rep = store.sweep(&HashSet::new(), cutoff()).unwrap();
    assert_eq!(rep.deleted, 1);
    assert_eq!(rep.unknown, 3);
    assert!(shard.join("not-a-blob").exists());
    assert!(dir.path().join("blobs/README").exists());
    assert!(shard.exists(), "shard directories stay (frozen layout)");
}

/// The put/sweep race. Each iteration: the blob exists with an mtime 2 h old and is
/// unmarked; a sweeper thread (cut-off now − 1 h) and a putter thread (same bytes) start
/// together. Whatever the interleaving, a put that returned `Ok` must leave the blob present
/// and intact once both threads are done: either the put finished before the sweep's
/// write lock (its touch makes the blob young, so the re-stat under the lock keeps it),
/// or it ran after the unlink (and wrote the blob again). The putter also checks
/// `contains` right after its own put returns, while the sweep may still be running.
#[test]
fn concurrent_put_never_loses_a_blob() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(BlobStore::open(dir.path()).unwrap());
    let bytes = Arc::new(payload(7, 8192));
    let r = store.put(&bytes).unwrap();
    let (mut deleted_runs, mut kept_runs) = (0u32, 0u32);
    for i in 0..1000u32 {
        if !store.contains(&r) {
            store.put(&bytes).unwrap();
        }
        age(&store, &r, 2 * HOUR);
        let cut = cutoff();
        let barrier = Arc::new(Barrier::new(2));
        let sweeper = {
            let (store, barrier) = (Arc::clone(&store), Arc::clone(&barrier));
            std::thread::spawn(move || {
                barrier.wait();
                store.sweep(&HashSet::new(), cut).unwrap()
            })
        };
        let putter = {
            let (store, barrier, bytes) =
                (Arc::clone(&store), Arc::clone(&barrier), Arc::clone(&bytes));
            std::thread::spawn(move || {
                barrier.wait();
                if i % 3 == 0 {
                    std::thread::yield_now();
                }
                let got = store.put(&bytes).unwrap();
                assert!(
                    store.contains(&got),
                    "iteration {i}: blob missing right after put"
                );
                got
            })
        };
        let rep = sweeper.join().unwrap();
        let got = putter.join().unwrap();
        assert_eq!(got, r);
        assert!(
            store.contains(&r),
            "iteration {i}: put returned Ok but the blob is gone"
        );
        assert_eq!(
            store.get(&r).unwrap(),
            *bytes,
            "iteration {i}: content intact"
        );
        if rep.deleted == 1 {
            deleted_runs += 1;
        } else {
            kept_runs += 1;
        }
    }
    eprintln!("race: sweep deleted first in {deleted_runs} iterations, put won in {kept_runs}");
    assert_eq!(deleted_runs + kept_runs, 1000);
}

/// A put that completes after the mark but before the sweep: the blob is unmarked (the
/// mark was taken before the put referenced it) and was old, but the put touched it.
#[test]
fn put_between_mark_and_sweep_survives() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStore::open(dir.path()).unwrap();
    let bytes = payload(8, 1000);
    let r = store.put(&bytes).unwrap();
    age(&store, &r, 5 * HOUR);
    // Mark phase: nothing references r yet.
    let mark: HashSet<[u8; 32]> = HashSet::new();
    let gc_start = SystemTime::now();
    // The API puts the blob again (it is about to be referenced by an append).
    store.put(&bytes).unwrap();
    // Sweep with gc_start - grace.
    let rep = store.sweep(&mark, gc_start - HOUR).unwrap();
    assert_eq!(rep.deleted, 0);
    assert!(store.contains(&r));
}

/// Timing of one sweep over 100k blobs (reported in 08-19-SUMMARY; run explicitly with
/// `--ignored --nocapture`). Files are written straight into the layout (no fsync) to keep
/// setup fast; half are referenced, a quarter are young, a quarter are deleted.
#[test]
#[ignore = "timing report, not a correctness check"]
fn sweep_100k_timing() {
    use sha2::{Digest as _, Sha256};
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStore::open(dir.path()).unwrap();
    let old = SystemTime::now() - 2 * HOUR;
    let mut mark = HashSet::new();
    let t0 = std::time::Instant::now();
    for i in 0..100_000u32 {
        let bytes = i.to_le_bytes();
        let sha: [u8; 32] = Sha256::digest(bytes).into();
        let r = BlobRef {
            sha256: sha,
            len: 4,
        };
        let path = store.path_of(&r);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, bytes).unwrap();
        if i % 4 != 0 {
            fs::File::options()
                .write(true)
                .open(&path)
                .unwrap()
                .set_modified(old)
                .unwrap();
        }
        if i % 2 == 1 {
            mark.insert(sha);
        }
    }
    let setup = t0.elapsed();
    let t1 = std::time::Instant::now();
    let rep = store.sweep(&mark, cutoff()).unwrap();
    let sweep = t1.elapsed();
    eprintln!(
        "100k blobs: setup {setup:?}, sweep {sweep:?}: scanned {}, deleted {}, kept_marked {}, kept_young {}",
        rep.scanned, rep.deleted, rep.kept_marked, rep.kept_young
    );
    assert_eq!(rep.scanned, 100_000);
    assert_eq!(rep.deleted, 25_000);
}
