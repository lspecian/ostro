//! Prefix fencing (P8-D4, plan 08-13): LOCK acquire / takeover, the LOCK re-read fallback
//! without conditional PUT, immutable-object conflicts (W9), manifest If-Match between two
//! uploaders, and chain walk-back ignoring forks. An S3 leg (SeaweedFS in CI) is env-gated
//! on `OSTRO_TEST_S3_*`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;

use common::{local_store, node_cfg, open, shutdown, start_runs, up_cfg, uploader};
use ostro_backup::chunk;
use ostro_backup::fence::{
    self, FenceError, Ident, Immutable, LOCK_KEY, check_before_manifest, put_immutable,
};
use ostro_backup::uploader::UploadError;
use ostro_objstore::{Credentials, ObjectStore, S3Store, StoreOptions};
use ostro_types::{Epoch, Offset, PartitionId};
use sha2::{Digest as _, Sha256};

fn id(inc: u8) -> Ident {
    Ident::new("cluster", 1, [inc; 16])
}

fn sha(b: &[u8]) -> [u8; 32] {
    Sha256::digest(b).into()
}

async fn lock_cases(store: &Arc<dyn ObjectStore>) {
    let (a, b) = (id(1), id(2));
    fence::acquire(store, &a, false)
        .await
        .expect("first acquire");
    fence::acquire(store, &a, false)
        .await
        .expect("re-acquire by the owner");
    match fence::acquire(store, &b, false).await {
        Err(FenceError::Fenced(m)) => assert!(m.contains("takeover"), "{m}"),
        other => panic!("second incarnation must be fenced, got {other:?}"),
    }
    fence::acquire(store, &b, true).await.expect("takeover");
    assert!(matches!(
        fence::acquire(store, &a, false).await,
        Err(FenceError::Fenced(_))
    ));
    check_before_manifest(store, &b)
        .await
        .expect("owner passes");
}

async fn immutable_cases(store: &Arc<dyn ObjectStore>, prefix: &str) {
    let key = format!("{prefix}seg/x.oseg");
    let v1 = b"one".to_vec();
    assert_eq!(
        put_immutable(store, &key, v1.clone(), sha(&v1))
            .await
            .expect("put"),
        Immutable::Written
    );
    assert_eq!(
        put_immutable(store, &key, v1.clone(), sha(&v1))
            .await
            .expect("re-put"),
        Immutable::AlreadyPresent
    );
    let v2 = b"two".to_vec();
    assert!(matches!(
        put_immutable(store, &key, v2.clone(), sha(&v2)).await,
        Err(FenceError::Fenced(_))
    ));
}

#[tokio::test]
async fn lock_acquire_takeover_and_immutable_puts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = local_store(dir.path(), true);
    lock_cases(&store).await;
    immutable_cases(&store, "").await;
}

#[tokio::test]
async fn fallback_without_conditional_put_detects_lock_change() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = local_store(dir.path(), false);
    assert!(!store.supports_conditional_put());
    let (a, b) = (id(1), id(2));
    fence::acquire(&store, &a, false).await.expect("acquire");
    check_before_manifest(&store, &a).await.expect("still ours");
    // Another writer replaces LOCK (as a takeover would).
    store
        .put(LOCK_KEY, serde_json_lock(&b).into())
        .await
        .expect("overwrite");
    assert!(matches!(
        check_before_manifest(&store, &a).await,
        Err(FenceError::Fenced(_))
    ));
    // With conditional PUT the check is a no-op (the manifest If-Match fences instead).
    let dir2 = tempfile::tempdir().expect("tempdir");
    let cond = local_store(dir2.path(), true);
    check_before_manifest(&cond, &a).await.expect("no-op");
    immutable_cases(&store, "fallback/").await;
}

fn serde_json_lock(i: &Ident) -> Vec<u8> {
    format!(
        "{{\"cluster_id\":\"{}\",\"node_id\":{},\"incarnation\":\"{}\"}}",
        i.cluster_id, i.node_id, i.incarnation
    )
    .into_bytes()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_uploader_is_fenced_by_manifest_if_match() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(dir.path(), 2)).await;
    let a = uploader(&node, up_cfg());
    start_runs(&node, 0, 5).await;
    a.round(&store).await.expect("round a1");
    // B resumes the prefix (same node, as a restored copy would after `--takeover`).
    let b = uploader(&node, up_cfg());
    start_runs(&node, 1, 5).await;
    b.round(&store).await.expect("round b1 continues the chain");
    // A's etag is now stale: its next manifest PUT fails the precondition.
    start_runs(&node, 0, 5).await;
    match a.round(&store).await {
        Err(UploadError::Fenced(m)) => assert!(m.contains("precondition"), "{m}"),
        other => panic!("expected fenced, got {other:?}"),
    }
    assert!(a.is_fenced());
    assert!(matches!(a.round(&store).await, Err(UploadError::Fenced(_))));
    // B is unaffected.
    start_runs(&node, 1, 5).await;
    b.round(&store).await.expect("b keeps uploading");
    drop((a, b));
    shutdown(node).await;
}

#[tokio::test]
async fn walk_back_ignores_forked_chunks() {
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let env = |epoch: u32, base: u64, n: usize| {
        let cmds: Vec<bytes::Bytes> = (0..n)
            .map(|i| bytes::Bytes::from(format!("c{base}-{i}-{epoch}")))
            .collect();
        ostro_types::envelope::encode(PartitionId(0), Epoch(epoch), Offset(base), &cmds)
    };
    let mut prev = [0u8; 32];
    let mut head = None;
    let mut fork_parent = [0u8; 32];
    for (epoch, base) in [(1u32, 0u64), (1, 3), (2, 6)] {
        let e = env(epoch, base, 3);
        let (h, bytes) = chunk::encode(0, epoch, prev, &[&e]).expect("encode");
        let key = chunk::chunk_key(0, epoch, h.base, h.end);
        let s = chunk::chunk_sha(&bytes);
        store.put(&key, bytes.into()).await.expect("put");
        if base == 3 {
            fork_parent = prev;
        }
        prev = s;
        head = Some((key, s));
    }
    // A forked chunk with the same base 3 and end 6 (another epoch, other content) linked to
    // the same parent: only its sha tells it apart from the chain's chunk.
    let f = env(5, 3, 3);
    let (fh, fb) = chunk::encode(0, 5, fork_parent, &[&f]).expect("fork");
    store
        .put(&chunk::chunk_key(0, 5, fh.base, fh.end), fb.into())
        .await
        .expect("put fork");
    let (head_key, head_sha) = head.expect("head");
    let chain = chunk::walk_back(&store, &head_key, head_sha, 0)
        .await
        .expect("walk back");
    let bases: Vec<u64> = chain.iter().map(|c| c.chunk.header.base).collect();
    assert_eq!(bases, vec![0, 3, 6]);
    assert!(chain.iter().all(|c| c.chunk.header.epoch != 5));
    // Stop early (an older generation restoring from a snapshot at 4).
    let tail = chunk::walk_back(&store, &head_key, head_sha, 4)
        .await
        .expect("walk back to 4");
    assert_eq!(
        tail.iter().map(|c| c.chunk.header.base).collect::<Vec<_>>(),
        vec![3, 6]
    );
    // A wrong head sha is refused.
    assert!(
        chunk::walk_back(&store, &head_key, [1; 32], 0)
            .await
            .is_err()
    );
}

/// SeaweedFS (CI) / any S3-compatible store: the same LOCK and immutable-put semantics over
/// real conditional PUTs. Env-gated on the Phase 6 convention.
#[tokio::test]
async fn s3_fencing() {
    let Ok(url) = std::env::var("OSTRO_TEST_S3_URL") else {
        println!("SKIP (human_needed): OSTRO_TEST_S3_URL unset (CI SeaweedFS step / Docker host)");
        return;
    };
    let var = |n: &str| std::env::var(n).unwrap_or_else(|_| panic!("{n} must be set"));
    let opts = StoreOptions {
        endpoint: Some(url.clone()),
        credentials: Some(Credentials {
            access_key_id: var("OSTRO_TEST_S3_ACCESS_KEY"),
            secret_access_key: var("OSTRO_TEST_S3_SECRET_KEY"),
            session_token: None,
        }),
        allow_http: url.starts_with("http://"),
        ..StoreOptions::default()
    };
    let ns = format!(
        "ostro-backup-fencing/{}-{}",
        std::process::id(),
        std::time::SystemTime::now() // clock-exempt: unique test namespace
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let store: Arc<dyn ObjectStore> =
        Arc::new(S3Store::new(&var("OSTRO_TEST_S3_BUCKET"), &ns, &opts).expect("s3 store"));
    assert!(store.supports_conditional_put());
    lock_cases(&store).await;
    immutable_cases(&store, "").await;
    let keys: Vec<String> = store
        .list("", None)
        .await
        .expect("list")
        .into_iter()
        .map(|m| m.key)
        .collect();
    store.delete_many(&keys).await.expect("cleanup");
}
