//! BlobStore durability, idempotence and corruption-detection tests (STOR-14).

use std::fs;
use std::io::Write;
use std::path::Path;

use ostro_blob::{BlobError, BlobRef, BlobStore, hex};
use sha2::{Digest, Sha256};

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 31 % 251) as u8).collect()
}

fn files_under(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if !dir.exists() {
        return out;
    }
    for e in fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            out.extend(files_under(&p));
        } else {
            out.push(p);
        }
    }
    out.sort();
    out
}

#[test]
fn put_writes_sha_addressed_file() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStore::open(dir.path()).unwrap();
    let bytes = payload(300 * 1024);
    let r = store.put(&bytes).unwrap();

    let expected: [u8; 32] = Sha256::digest(&bytes).into();
    assert_eq!(r.sha256, expected);
    assert_eq!(r.len, bytes.len() as u64);

    let h = hex(&r.sha256);
    assert_eq!(h.len(), 64);
    let path = dir.path().join("blobs").join(&h[..2]).join(&h);
    assert_eq!(store.path_of(&r), path);
    assert_eq!(fs::read(&path).unwrap(), bytes);
    assert!(store.contains(&r));
}

#[test]
fn put_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStore::open(dir.path()).unwrap();
    let bytes = payload(4096);
    let a = store.put(&bytes).unwrap();
    let md = fs::metadata(store.path_of(&a)).unwrap();
    let b = store.put(&bytes).unwrap();
    assert_eq!(a, b);
    assert_eq!(files_under(&dir.path().join("blobs")).len(), 1);
    assert!(files_under(&dir.path().join("tmp")).is_empty());
    let md2 = fs::metadata(store.path_of(&a)).unwrap();
    // The second put touches the mtime (blob GC grace, 08-19) but never rewrites the blob:
    // same file (inode), same content.
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        assert_eq!(md.ino(), md2.ino(), "second put must not rewrite the blob");
    }
    assert!(md2.modified().unwrap() >= md.modified().unwrap());
    assert_eq!(fs::read(store.path_of(&a)).unwrap(), bytes);
}

#[test]
fn get_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStore::open(dir.path()).unwrap();
    let bytes = payload(300 * 1024);
    let r = store.put(&bytes).unwrap();
    assert_eq!(store.get(&r).unwrap(), bytes);

    let empty = store.put(&[]).unwrap();
    assert_eq!(empty.len, 0);
    assert_eq!(store.get(&empty).unwrap(), Vec::<u8>::new());
}

#[test]
fn get_detects_flipped_byte() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStore::open(dir.path()).unwrap();
    let bytes = payload(8192);
    let r = store.put(&bytes).unwrap();
    let mut stored = fs::read(store.path_of(&r)).unwrap();
    stored[1234] ^= 0x01;
    fs::write(store.path_of(&r), &stored).unwrap();
    match store.get(&r) {
        Err(BlobError::ShaMismatch { hex: h, actual }) => {
            assert_eq!(h, hex(&r.sha256));
            assert_ne!(actual, h);
        }
        other => panic!("expected ShaMismatch, got {other:?}"),
    }
}

#[test]
fn get_detects_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStore::open(dir.path()).unwrap();
    let bytes = payload(8192);
    let r = store.put(&bytes).unwrap();
    let f = fs::OpenOptions::new()
        .write(true)
        .open(store.path_of(&r))
        .unwrap();
    f.set_len(100).unwrap();
    match store.get(&r) {
        Err(BlobError::LenMismatch {
            expected, actual, ..
        }) => {
            assert_eq!(expected, 8192);
            assert_eq!(actual, 100);
        }
        other => panic!("expected LenMismatch, got {other:?}"),
    }
}

#[test]
fn get_missing_is_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStore::open(dir.path()).unwrap();
    let r = BlobRef {
        sha256: [7u8; 32],
        len: 10,
    };
    assert!(!store.contains(&r));
    match store.get(&r) {
        Err(BlobError::NotFound(h)) => assert_eq!(h, hex(&r.sha256)),
        other => panic!("expected NotFound, got {other:?}"),
    }
}

#[test]
fn open_cleans_tmp_and_keeps_blobs() {
    let dir = tempfile::tempdir().unwrap();
    let r = {
        let store = BlobStore::open(dir.path()).unwrap();
        store.put(&payload(1000)).unwrap()
    };
    let stale = dir.path().join("tmp").join("deadbeef.1.0");
    fs::File::create(&stale)
        .unwrap()
        .write_all(b"partial")
        .unwrap();
    assert!(stale.exists());

    let store = BlobStore::open(dir.path()).unwrap();
    assert!(!stale.exists());
    assert!(files_under(&dir.path().join("tmp")).is_empty());
    assert!(store.contains(&r));
    assert_eq!(store.get(&r).unwrap(), payload(1000));
}

#[test]
fn hex_is_lowercase() {
    let mut sha = [0u8; 32];
    sha[0] = 0xAB;
    sha[31] = 0x0F;
    let h = hex(&sha);
    assert!(h.starts_with("ab00"));
    assert!(h.ends_with("000f"));
}
