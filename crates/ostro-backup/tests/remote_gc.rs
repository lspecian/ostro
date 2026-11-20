//! Remote delayed-deletion blob GC (P8-D8, plan 08-19) against a LocalFs remote: the
//! window, re-put protection, the re-put race guard, ledger crash safety and the window
//! computation from manifests.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ostro_backup::manifest::{self, Manifest};
use ostro_backup::remote_gc::{LEDGER_PATH, Ledger, retention_window};
use ostro_backup::uploader::blob_key;
use ostro_blob::{BlobRef, BlobStore};
use ostro_objstore::{LocalFs, ObjectStore};

const MIN: u64 = 60_000;

struct Fx {
    _data: tempfile::TempDir,
    _remote: tempfile::TempDir,
    data_dir: std::path::PathBuf,
    blobs: BlobStore,
    store: Arc<dyn ObjectStore>,
}

fn fx() -> Fx {
    let data = tempfile::tempdir().unwrap();
    let remote = tempfile::tempdir().unwrap();
    let blobs = BlobStore::open(data.path()).unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFs::new(remote.path()).unwrap());
    Fx {
        data_dir: data.path().to_path_buf(),
        _data: data,
        _remote: remote,
        blobs,
        store,
    }
}

/// Put locally and upload (what the uploader does).
async fn put_both(fx: &Fx, bytes: &[u8]) -> BlobRef {
    let r = fx.blobs.put(bytes).unwrap();
    fx.store
        .put(&blob_key(&r.sha256), Bytes::copy_from_slice(bytes))
        .await
        .unwrap();
    r
}

async fn remote_has(fx: &Fx, r: &BlobRef) -> bool {
    fx.store.head(&blob_key(&r.sha256)).await.unwrap().is_some()
}

/// Delete locally (what the local sweep does) and record in the ledger.
fn local_delete(fx: &Fx, ledger: &mut Ledger, r: &BlobRef, now_ms: u64) {
    fs::remove_file(fx.blobs.path_of(r)).unwrap();
    ledger.record(&[r.sha256], now_ms).unwrap();
}

#[tokio::test]
async fn deleted_sha_is_removed_remotely_only_after_the_window() {
    let fx = fx();
    let mut ledger = Ledger::open(&fx.data_dir).unwrap();
    assert!(ledger.path().ends_with(LEDGER_PATH));
    let a = put_both(&fx, b"blob a").await;
    let keep = put_both(&fx, b"still referenced").await;
    let t0 = 1_000 * MIN;
    local_delete(&fx, &mut ledger, &a, t0);
    let window = Duration::from_millis(30 * MIN);

    // Inside the window: nothing happens.
    let rep = ledger
        .run(&fx.store, window, t0 + 30 * MIN, &fx.blobs)
        .await
        .unwrap();
    assert_eq!((rep.due, rep.deleted, rep.remaining), (0, 0, 1));
    assert!(remote_has(&fx, &a).await);

    // Past it: the remote copy goes, the ledger is compacted.
    let rep = ledger
        .run(&fx.store, window, t0 + 30 * MIN + 1, &fx.blobs)
        .await
        .unwrap();
    assert_eq!((rep.due, rep.deleted, rep.remaining), (1, 1, 0));
    assert!(!remote_has(&fx, &a).await);
    assert!(
        remote_has(&fx, &keep).await,
        "never in the ledger, never deleted"
    );
    assert_eq!(fs::read_to_string(ledger.path()).unwrap(), "");
    // Reopen: empty.
    assert!(Ledger::open(&fx.data_dir).unwrap().is_empty());
}

#[tokio::test]
async fn re_put_before_the_window_is_never_remotely_deleted() {
    let fx = fx();
    let mut ledger = Ledger::open(&fx.data_dir).unwrap();
    let bytes = b"re-referenced later".to_vec();
    let x = put_both(&fx, &bytes).await;
    let t0 = 5_000 * MIN;
    local_delete(&fx, &mut ledger, &x, t0);
    // An API call puts it again before the window passed.
    fx.blobs.put(&bytes).unwrap();
    let rep = ledger
        .run(
            &fx.store,
            Duration::from_millis(MIN),
            t0 + 10 * MIN,
            &fx.blobs,
        )
        .await
        .unwrap();
    assert_eq!((rep.due, rep.deleted, rep.reput), (1, 0, 1));
    assert!(remote_has(&fx, &x).await);
    assert!(ledger.is_empty(), "entry dropped: the blob is live again");

    // Deleted, re-put, deleted again: only the newest deletion counts.
    local_delete(&fx, &mut ledger, &x, t0 + 20 * MIN);
    fx.blobs.put(&bytes).unwrap();
    local_delete(&fx, &mut ledger, &x, t0 + 50 * MIN);
    assert_eq!(ledger.deleted_at(&x.sha256), Some(t0 + 50 * MIN));
    let window = Duration::from_millis(20 * MIN);
    let rep = ledger
        .run(&fx.store, window, t0 + 60 * MIN, &fx.blobs)
        .await
        .unwrap();
    assert_eq!(
        rep.deleted, 0,
        "the second deletion's window has not passed"
    );
    assert!(remote_has(&fx, &x).await);
    // After reopening, the newest time still wins.
    drop(ledger);
    let mut ledger = Ledger::open(&fx.data_dir).unwrap();
    assert_eq!(ledger.deleted_at(&x.sha256), Some(t0 + 50 * MIN));
    let rep = ledger
        .run(&fx.store, window, t0 + 71 * MIN, &fx.blobs)
        .await
        .unwrap();
    assert_eq!(rep.deleted, 1);
    assert!(!remote_has(&fx, &x).await);
}

#[tokio::test]
async fn put_after_remote_delete_is_reuploaded_through_the_guard() {
    let fx = fx();
    let mut ledger = Ledger::open(&fx.data_dir).unwrap();
    let guard = ledger.guard();
    let bytes = b"comes back after the remote delete".to_vec();
    let x = put_both(&fx, &bytes).await;
    let other = fx.blobs.put(b"never deleted remotely").unwrap();
    local_delete(&fx, &mut ledger, &x, 0);
    let rep = ledger
        .run(&fx.store, Duration::ZERO, 10, &fx.blobs)
        .await
        .unwrap();
    assert_eq!(rep.deleted, 1);
    assert!(!remote_has(&fx, &x).await);
    assert!(guard.contains(&x.sha256));

    // The put watcher sees the re-put (the uploader would skip it: cached as uploaded).
    let mut rx = fx.blobs.subscribe_puts();
    fx.blobs.put(&bytes).unwrap();
    let sha = rx.recv().await.unwrap();
    assert!(guard.on_put(&fx.store, &fx.blobs, &sha).await.unwrap());
    assert!(remote_has(&fx, &x).await, "re-uploaded");
    assert_eq!(
        fx.store.get(&blob_key(&x.sha256)).await.unwrap().unwrap().0,
        Bytes::from(bytes)
    );
    // Unrelated puts are ignored; a lagged watcher re-uploads every noted local sha.
    assert!(
        !guard
            .on_put(&fx.store, &fx.blobs, &other.sha256)
            .await
            .unwrap()
    );
    fx.store.delete(&blob_key(&x.sha256)).await.unwrap();
    assert_eq!(guard.on_lagged(&fx.store, &fx.blobs).await.unwrap(), 1);
    assert!(remote_has(&fx, &x).await);
}

#[tokio::test]
async fn orphans_are_adopted() {
    let fx = fx();
    let mut ledger = Ledger::open(&fx.data_dir).unwrap();
    let live = put_both(&fx, b"live").await;
    let orphan = put_both(&fx, b"crash between unlink and ledger append").await;
    fs::remove_file(fx.blobs.path_of(&orphan)).unwrap();
    assert_eq!(
        ledger
            .adopt_orphans(&fx.store, &fx.blobs, 77)
            .await
            .unwrap(),
        1
    );
    assert_eq!(ledger.deleted_at(&orphan.sha256), Some(77));
    assert_eq!(ledger.deleted_at(&live.sha256), None);
    // Idempotent.
    assert_eq!(
        ledger
            .adopt_orphans(&fx.store, &fx.blobs, 99)
            .await
            .unwrap(),
        0
    );
    assert_eq!(ledger.deleted_at(&orphan.sha256), Some(77));
}

#[tokio::test]
async fn ledger_survives_a_torn_last_line_and_a_crashed_compaction() {
    let fx = fx();
    let shas: Vec<[u8; 32]> = (0..3u8).map(|i| [i + 1; 32]).collect();
    {
        let mut ledger = Ledger::open(&fx.data_dir).unwrap();
        ledger.record(&shas[..2], 100).unwrap();
        ledger.record(&shas[2..], 200).unwrap();
    }
    let path = fx.data_dir.join(LEDGER_PATH);
    // Crash mid-append: truncate inside the last line.
    let full = fs::read(&path).unwrap();
    let cut = full.len() - 10;
    OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(cut as u64)
        .unwrap();
    let mut ledger = Ledger::open(&fx.data_dir).unwrap();
    assert_eq!(ledger.len(), 2, "the torn entry is dropped");
    assert_eq!(ledger.deleted_at(&shas[0]), Some(100));
    assert_eq!(ledger.deleted_at(&shas[2]), None);
    // The file was truncated to the last complete line: appends stay parseable.
    assert!(fs::read(&path).unwrap().ends_with(b"\n"));
    ledger.record(&shas[2..], 300).unwrap();
    drop(ledger);
    let ledger = Ledger::open(&fx.data_dir).unwrap();
    assert_eq!(ledger.len(), 3);
    assert_eq!(ledger.deleted_at(&shas[2]), Some(300));

    // A crashed compaction leaves a tmp file next to the intact ledger: ignored, removed.
    let tmp = path.with_extension("tmp");
    let mut f = fs::File::create(&tmp).unwrap();
    f.write_all(b"garbage half-written compaction").unwrap();
    drop(f);
    drop(ledger);
    let ledger = Ledger::open(&fx.data_dir).unwrap();
    assert_eq!(ledger.len(), 3);
    assert!(!tmp.exists());
    // A compaction rewrites exactly the live entries.
    ledger.compact().unwrap();
    let text = fs::read_to_string(&path).unwrap();
    assert_eq!(text.lines().count(), 3);
    let reopened = Ledger::open(&fx.data_dir).unwrap();
    let got: HashSet<_> = shas.iter().filter_map(|s| reopened.deleted_at(s)).collect();
    assert_eq!(got, [100, 300].into_iter().collect());
}

fn manifest_at(generation: u64, cut_at_ms: u64) -> Vec<u8> {
    manifest::encode(&Manifest {
        format: manifest::FORMAT,
        cluster_id: "c".into(),
        node_id: 1,
        incarnation: [1; 16],
        generation,
        prev_manifest_sha256: [0; 32],
        cut_at_ms,
        cut: Vec::new(),
        meta_snapshot_key: None,
        meta_snapshot_sha256: None,
        ostro_version: "test".into(),
    })
}

#[tokio::test]
async fn window_is_the_age_of_the_oldest_retained_manifest() {
    let fx = fx();
    assert_eq!(retention_window(&fx.store, 1_000).await.unwrap(), None);
    fx.store
        .put(manifest::CURRENT_KEY, manifest_at(9, 900).into())
        .await
        .unwrap();
    assert_eq!(
        retention_window(&fx.store, 1_000).await.unwrap(),
        Some(Duration::from_millis(100))
    );
    for (g, at) in [(4, 400), (6, 600), (9, 900)] {
        fx.store
            .put(&manifest::gen_key(g), manifest_at(g, at).into())
            .await
            .unwrap();
    }
    assert_eq!(
        retention_window(&fx.store, 1_000).await.unwrap(),
        Some(Duration::from_millis(600)),
        "oldest gen (4, cut 400) is the oldest restore point"
    );
    // Retention removed gen 4: the window shrinks to gen 6.
    fx.store.delete(&manifest::gen_key(4)).await.unwrap();
    assert_eq!(
        retention_window(&fx.store, 1_000).await.unwrap(),
        Some(Duration::from_millis(400))
    );
}
