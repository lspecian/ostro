//! BlobStore put observer (08-08): every successful put, new or already present, is
//! broadcast; a receiver that falls behind is told it lagged, never silently skipped.

use ostro_blob::{BlobStore, PUT_EVENTS_CAPACITY};
use sha2::{Digest, Sha256};
use tokio::sync::broadcast::error::{RecvError, TryRecvError};

fn sha(b: &[u8]) -> [u8; 32] {
    Sha256::digest(b).into()
}

#[test]
fn new_put_notifies() {
    let d = tempfile::tempdir().unwrap();
    let s = BlobStore::open(d.path()).unwrap();
    let mut rx = s.subscribe_puts();
    let r = s.put(b"hello observer").unwrap();
    assert_eq!(r.sha256, sha(b"hello observer"));
    assert_eq!(rx.try_recv().unwrap(), r.sha256);
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
}

#[test]
fn re_put_notifies() {
    let d = tempfile::tempdir().unwrap();
    let s = BlobStore::open(d.path()).unwrap();
    let r = s.put(b"already here").unwrap();
    // Subscribed after the first put: only later puts are seen.
    let mut rx = s.subscribe_puts();
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    assert_eq!(s.put(b"already here").unwrap(), r);
    assert_eq!(rx.try_recv().unwrap(), r.sha256);
    let r2 = s.put(b"another").unwrap();
    assert_eq!(rx.try_recv().unwrap(), r2.sha256);
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
}

#[test]
fn failed_put_does_not_notify_and_no_subscriber_is_fine() {
    let d = tempfile::tempdir().unwrap();
    let s = BlobStore::open(d.path()).unwrap();
    // No subscriber: puts still succeed.
    s.put(b"nobody listens").unwrap();
    let mut rx = s.subscribe_puts();
    // Make the tmp dir unusable so the next new put fails before rename.
    std::fs::remove_dir_all(d.path().join("tmp")).unwrap();
    assert!(s.put(b"cannot be written").is_err());
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
}

#[test]
fn slow_receiver_is_told_it_lagged() {
    const PUTS: usize = 10_000;
    let d = tempfile::tempdir().unwrap();
    let s = BlobStore::open(d.path()).unwrap();
    // 16 distinct blobs written once (fsync), then re-put: every put notifies, and re-puts
    // keep the test fast.
    let blobs: Vec<Vec<u8>> = (0..16u8).map(|i| vec![i; 64]).collect();
    for b in &blobs {
        s.put(b).unwrap();
    }
    let mut rx = s.subscribe_puts();
    let mut sent = Vec::with_capacity(PUTS);
    for i in 0..PUTS {
        sent.push(s.put(&blobs[i % blobs.len()]).unwrap().sha256);
    }

    // The receiver never ran during the puts: it must learn it lagged.
    let mut lagged = 0u64;
    let mut got = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(h) => got.push(h),
            Err(TryRecvError::Lagged(n)) => lagged += n,
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Closed) => panic!("sender dropped"),
        }
    }
    assert!(lagged > 0, "a slow receiver must be told it lagged");
    assert_eq!(
        lagged as usize + got.len(),
        PUTS,
        "every put is either received or counted as lagged"
    );
    assert_eq!(got.len(), PUT_EVENTS_CAPACITY);
    // What was kept is the tail, in order.
    assert_eq!(got, sent[PUTS - PUT_EVENTS_CAPACITY..]);
}

#[tokio::test]
async fn async_receiver_sees_lagged() {
    let d = tempfile::tempdir().unwrap();
    let s = BlobStore::open(d.path()).unwrap();
    s.put(b"x").unwrap();
    let mut rx = s.subscribe_puts();
    for _ in 0..PUT_EVENTS_CAPACITY + 10 {
        s.put(b"x").unwrap();
    }
    match rx.recv().await {
        Err(RecvError::Lagged(n)) => assert_eq!(n, 10),
        other => panic!("expected Lagged, got {other:?}"),
    }
    assert_eq!(rx.recv().await.unwrap(), sha(b"x"));
}
