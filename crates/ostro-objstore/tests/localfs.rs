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
