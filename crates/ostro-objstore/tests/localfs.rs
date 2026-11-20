//! LocalFs backend: the shared conformance suite plus LocalFs-specific behaviour.

mod conformance;

use std::sync::Arc;

use bytes::Bytes;
use ostro_objstore::{Error, LocalFs, ObjectStore, StoreOptions, open_url};

fn store(dir: &tempfile::TempDir) -> Arc<dyn ObjectStore> {
    Arc::new(LocalFs::new(dir.path()).expect("LocalFs::new"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conformance_localfs() {
    let dir = tempfile::tempdir().unwrap();
    let s = store(&dir);
    assert!(s.supports_conditional_put());
    conformance::run_all(s.clone(), "ns", 3 * (1 << 20) + 5).await;
    conformance::put_if_absent_race(s, "ns").await;
}

/// 70 MiB streamed through `put_multipart` (LocalFs streams to tmp; no buffering).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multipart_70mib_localfs() {
    let dir = tempfile::tempdir().unwrap();
    conformance::multipart_dyn(store(&dir), "big", 70 << 20).await;
}

#[tokio::test]
async fn keys_map_to_paths_and_tmp_is_hidden() {
    let dir = tempfile::tempdir().unwrap();
    let s = store(&dir);
    s.put("p/0001/seg/00000001", Bytes::from_static(b"seg"))
        .await
        .unwrap();
    let root = dir.path().canonicalize().unwrap();
    assert_eq!(
        std::fs::read(root.join("p/0001/seg/00000001")).unwrap(),
        b"seg"
    );
    // The in-flight directory is reserved and never listed; no tmp files are left behind.
    assert!(matches!(
        s.put(".ostro-tmp/x", Bytes::new()).await,
        Err(Error::InvalidKey { .. })
    ));
    let listed = s.list("", None).await.unwrap();
    assert_eq!(
        listed.iter().map(|m| m.key.as_str()).collect::<Vec<_>>(),
        ["p/0001/seg/00000001"]
    );
    assert_eq!(
        std::fs::read_dir(root.join(".ostro-tmp")).unwrap().count(),
        0
    );

    // etag = hex sha256 of the content.
    let meta = s.head("p/0001/seg/00000001").await.unwrap().unwrap();
    let want = {
        use sha2::Digest as _;
        let d = sha2::Sha256::digest(b"seg");
        d.iter().map(|b| format!("{b:02x}")).collect::<String>()
    };
    assert_eq!(meta.e_tag.as_deref(), Some(want.as_str()));

    // Deleting the only key under a directory prunes it, so the path can become a key.
    s.delete("p/0001/seg/00000001").await.unwrap();
    assert!(!root.join("p").exists());
    s.put("p", Bytes::from_static(b"now a file")).await.unwrap();
}

#[tokio::test]
async fn quoted_etag_matches_on_localfs() {
    let dir = tempfile::tempdir().unwrap();
    let s = store(&dir);
    let t = s
        .put("k", Bytes::from_static(b"a"))
        .await
        .unwrap()
        .e_tag
        .unwrap();
    // Callers may pass the S3-style quoted form.
    assert!(
        s.put_if_match("k", Bytes::from_static(b"b"), &format!("\"{t}\""))
            .await
            .unwrap()
            .ok()
            .is_some()
    );
}

#[tokio::test]
async fn open_url_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("file://{}", dir.path().join("sub").display());
    let s = open_url(&url, &StoreOptions::default()).expect("file:// opens");
    s.put("x", Bytes::from_static(b"1")).await.unwrap();
    assert!(dir.path().join("sub/x").exists());

    let off = open_url(
        &url,
        &StoreOptions {
            conditional_put: false,
            ..StoreOptions::default()
        },
    )
    .unwrap();
    assert!(
        !off.supports_conditional_put(),
        "conditional_put = false forces the fallback"
    );

    let s3 = open_url(
        "s3://bucket/some/prefix",
        &StoreOptions {
            endpoint: Some("https://s3.example".into()),
            ..StoreOptions::default()
        },
    );
    assert!(s3.is_ok(), "s3:// opens without I/O");

    for bad in [
        "gs://bucket/x",
        "http://example.com/x",
        "file://remotehost/x",
        "s3:///nobucket",
        "s3://ak:sk@bucket/x",
        "not a url",
    ] {
        assert!(
            matches!(
                open_url(bad, &StoreOptions::default()),
                Err(Error::Config(_))
            ),
            "{bad} accepted"
        );
    }
    // plain http endpoints need allow_http
    assert!(matches!(
        open_url(
            "s3://bucket",
            &StoreOptions {
                endpoint: Some("http://127.0.0.1:9000".into()),
                ..StoreOptions::default()
            }
        ),
        Err(Error::Config(_))
    ));
}

#[test]
fn relative_root_rejected() {
    assert!(matches!(
        LocalFs::new("relative/dir"),
        Err(Error::Config(_))
    ));
}

// ---------------------------------------------------------------------------------------------
// Phase 6 (06-12) additions: put_with + content-type sidecar, presign, object safety
// ---------------------------------------------------------------------------------------------

use ostro_objstore::{PresignMethod, PresignOpts, PutOpts};

fn _assert_object_safe(_: std::sync::Arc<dyn ObjectStore>) {}

#[test]
fn localfs_is_object_safe() {
    let dir = tempfile::tempdir().unwrap();
    _assert_object_safe(Arc::new(LocalFs::new(dir.path()).unwrap()));
}

#[tokio::test]
async fn put_with_round_trip_and_sidecar_hidden() {
    let dir = tempfile::tempdir().unwrap();
    let fs = LocalFs::new(dir.path()).unwrap();
    let opts = PutOpts {
        content_type: Some("text/plain; charset=\"utf-8\"".into()),
    };
    let out = fs
        .put_with("docs/a.txt", Bytes::from_static(b"hello"), &opts)
        .await
        .unwrap();
    let (body, meta) = fs.get("docs/a.txt").await.unwrap().unwrap();
    assert_eq!(&body[..], b"hello");
    assert_eq!(meta.e_tag, out.e_tag);
    assert_eq!(
        fs.content_type("docs/a.txt").await.unwrap().as_deref(),
        Some("text/plain; charset=\"utf-8\"")
    );
    let root = dir.path().canonicalize().unwrap();
    assert!(root.join("docs/a.txt.ostro-meta.json").exists());
    // The sidecar is not an object: never listed, and its name is reserved.
    let keys: Vec<String> = fs
        .list("", None)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.key)
        .collect();
    assert_eq!(keys, ["docs/a.txt"]);
    assert!(matches!(
        fs.put("docs/a.txt.ostro-meta.json", Bytes::new()).await,
        Err(Error::InvalidKey { .. })
    ));
    assert!(matches!(
        fs.put("x.ostro-meta.json/y", Bytes::new()).await,
        Err(Error::InvalidKey { .. })
    ));
    // A plain put drops the stale content type; put_with without one too.
    fs.put("docs/a.txt", Bytes::from_static(b"v2"))
        .await
        .unwrap();
    assert_eq!(fs.content_type("docs/a.txt").await.unwrap(), None);
    assert!(!root.join("docs/a.txt.ostro-meta.json").exists());
    fs.put_with("docs/a.txt", Bytes::from_static(b"v3"), &opts)
        .await
        .unwrap();
    fs.put_with("docs/a.txt", Bytes::from_static(b"v4"), &PutOpts::default())
        .await
        .unwrap();
    assert_eq!(fs.content_type("docs/a.txt").await.unwrap(), None);
    // delete removes object and sidecar, and prunes the directory.
    fs.put_with("docs/a.txt", Bytes::from_static(b"v5"), &opts)
        .await
        .unwrap();
    fs.delete("docs/a.txt").await.unwrap();
    assert!(!root.join("docs").exists());
    assert_eq!(fs.content_type("docs/a.txt").await.unwrap(), None);
    // Through the trait object, put_with dispatches to the LocalFs override.
    let s: Arc<dyn ObjectStore> = Arc::new(fs.clone());
    s.put_with("k", Bytes::from_static(b"x"), &opts)
        .await
        .unwrap();
    assert!(fs.content_type("k").await.unwrap().is_some());
}

#[tokio::test]
async fn traversal_and_absolute_keys_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let s = store(&dir);
    for bad in ["../escape", "a/../../b", "/etc/passwd", "a//b", ""] {
        assert!(
            matches!(
                s.put_with(bad, Bytes::new(), &PutOpts::default()).await,
                Err(Error::InvalidKey { .. })
            ),
            "{bad:?} accepted"
        );
        assert!(s.get(bad).await.is_err(), "{bad:?} readable");
    }
    assert!(!dir.path().parent().unwrap().join("escape").exists());
}

#[tokio::test]
async fn list_order_start_after_and_head_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let s = store(&dir);
    for k in ["b", "a/2", "a/1", "c/x/y", "a/10"] {
        s.put(k, Bytes::from(k.as_bytes().to_vec())).await.unwrap();
    }
    let keys =
        |v: Vec<ostro_objstore::ObjectMeta>| v.into_iter().map(|m| m.key).collect::<Vec<_>>();
    assert_eq!(
        keys(s.list("", None).await.unwrap()),
        ["a/1", "a/10", "a/2", "b", "c/x/y"]
    );
    assert_eq!(
        keys(s.list("", Some("a/10")).await.unwrap()),
        ["a/2", "b", "c/x/y"]
    );
    assert_eq!(
        keys(s.list("a/", Some("a/1")).await.unwrap()),
        ["a/10", "a/2"]
    );
    let m = s.head("c/x/y").await.unwrap().unwrap();
    assert_eq!(m.size, 5);
    assert!(m.last_modified > jiff::Timestamp::UNIX_EPOCH);
    assert_eq!(m.e_tag.as_deref().map(str::len), Some(64));
    assert_eq!(s.head("nope").await.unwrap(), None);
    assert!(s.get("nope").await.unwrap().is_none());
    s.delete("nope").await.unwrap();
    s.delete_many(&["a/1".into(), "missing".into()])
        .await
        .unwrap();
    assert_eq!(s.head("a/1").await.unwrap(), None);
}

#[tokio::test]
async fn localfs_presign_is_none() {
    let dir = tempfile::tempdir().unwrap();
    let s = store(&dir);
    for m in [PresignMethod::Get, PresignMethod::Put] {
        assert_eq!(
            s.presign(
                m,
                "k",
                std::time::Duration::from_secs(60),
                &PresignOpts::default()
            )
            .await
            .unwrap(),
            None
        );
    }
}
