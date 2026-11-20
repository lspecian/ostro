//! S3 backend: the shared conformance suite against
//! - an in-process **s3s-fs** server (always; SigV4 is verified, so signing is exercised), and
//! - an external **S3-compatible store** (SeaweedFS in CI), env-gated on the Phase 6 convention `OSTRO_TEST_S3_URL` / `_ACCESS_KEY` /
//!   `_SECRET_KEY` / `_BUCKET` (CI's SeaweedFS step exports them). Unset: a loud
//!   `SKIP (human_needed)` line and a pass (WSL has no Docker).
//!
//! Plus retry/backoff behaviour against a scripted raw-HTTP server.

mod conformance;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use ostro_objstore::{
    Conditional, Credentials, Error, ObjectStore, S3Store, StoreOptions, UrlStyle, open_url,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------------------------
// s3s-fs harness
// ---------------------------------------------------------------------------------------------

const BUCKET: &str = "ostro-test";
const AK: &str = "ostro-ak";
const SK: &str = "ostro-sk-0123456789";

struct S3sFs {
    _dir: tempfile::TempDir,
    endpoint: String,
}

async fn start_s3s_fs() -> S3sFs {
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder as ConnBuilder;

    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join(BUCKET)).unwrap();
    let fs = s3s_fs::FileSystem::new(dir.path()).unwrap();
    let service = {
        let mut b = s3s::service::S3ServiceBuilder::new(fs);
        b.set_auth(s3s::auth::SimpleAuth::from_single(AK, SK));
        b.build()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let http = ConnBuilder::new(TokioExecutor::new());
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                continue;
            };
            let conn = http
                .serve_connection(TokioIo::new(sock), service.clone())
                .into_owned();
            tokio::spawn(async move {
                let _ = conn.await;
            });
        }
    });
    S3sFs {
        _dir: dir,
        endpoint: format!("http://{addr}"),
    }
}

fn s3s_opts(endpoint: &str) -> StoreOptions {
    StoreOptions {
        endpoint: Some(endpoint.to_owned()),
        region: Some("us-east-1".into()),
        url_style: UrlStyle::Path,
        credentials: Some(Credentials {
            access_key_id: AK.into(),
            secret_access_key: SK.into(),
            session_token: None,
        }),
        allow_http: true,
        // Exercise real multipart without 64 MiB bodies: 6 MiB threshold, 5 MiB parts.
        multipart_threshold: 6 << 20,
        multipart_part_size: 5 << 20,
        retry_base: Duration::from_millis(10),
        ..StoreOptions::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3s_fs_smoke_put_get() {
    let srv = start_s3s_fs().await;
    let s = S3Store::new(BUCKET, "", &s3s_opts(&srv.endpoint)).unwrap();
    let out = s.put("smoke", Bytes::from_static(b"hi")).await.unwrap();
    assert!(out.e_tag.is_some());
    let (b, m) = s.get("smoke").await.unwrap().unwrap();
    assert_eq!(&b[..], b"hi");
    assert_eq!(m.e_tag, out.e_tag);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conformance_s3s_fs() {
    let srv = start_s3s_fs().await;
    let s: Arc<dyn ObjectStore> =
        open_url(&format!("s3://{BUCKET}/conf"), &s3s_opts(&srv.endpoint)).unwrap();
    // 12 MiB + 3: above the 6 MiB threshold -> 3 parts (5 + 5 + 2 MiB).
    conformance::run_all(s, "ns", (12 << 20) + 3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3s_fs_prefix_is_prepended() {
    let srv = start_s3s_fs().await;
    let opts = s3s_opts(&srv.endpoint);
    let prefixed = open_url(&format!("s3://{BUCKET}/pre/fix"), &opts).unwrap();
    prefixed.put("k", Bytes::from_static(b"v")).await.unwrap();
    let raw = S3Store::new(BUCKET, "", &opts).unwrap();
    let keys: Vec<String> = raw
        .list("pre/", None)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.key)
        .collect();
    assert_eq!(keys, ["pre/fix/k"]);
    // ...and stripped again on list through the prefixed store.
    let keys: Vec<String> = prefixed
        .list("", None)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.key)
        .collect();
    assert_eq!(keys, ["k"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3s_fs_rejects_mismatched_if_match_and_bad_signature() {
    let srv = start_s3s_fs().await;
    let opts = s3s_opts(&srv.endpoint);
    let s = S3Store::new(BUCKET, "", &opts).unwrap();
    mismatched_if_match(&s).await;

    let wrong = StoreOptions {
        credentials: Some(Credentials {
            access_key_id: AK.into(),
            secret_access_key: "wrong-secret".into(),
            session_token: None,
        }),
        ..opts
    };
    let bad = S3Store::new(BUCKET, "", &wrong).unwrap();
    match bad.put("x", Bytes::from_static(b"x")).await {
        Err(Error::Status { status: 403, .. }) => {}
        other => panic!("bad signature must be 403, got {other:?}"),
    }
}

/// The signed `if-match` header reaches the server: a mismatched etag is rejected with 412 and
/// nothing is written; `if-none-match: *` on an existing key likewise.
async fn mismatched_if_match(s: &S3Store) {
    let key = format!("ifmatch-{}", std::process::id());
    let t = s
        .put(&key, Bytes::from_static(b"orig"))
        .await
        .unwrap()
        .e_tag
        .expect("etag");
    let r = s
        .put_if_match(
            &key,
            Bytes::from_static(b"evil"),
            "\"ffffffffffffffffffffffffffffffff\"",
        )
        .await
        .unwrap();
    assert_eq!(
        r,
        Conditional::PreconditionFailed,
        "server enforced If-Match"
    );
    assert_eq!(&s.get(&key).await.unwrap().unwrap().0[..], b"orig");
    assert!(
        s.put_if_absent(&key, Bytes::from_static(b"evil"))
            .await
            .unwrap()
            .is_precondition_failed(),
        "server enforced If-None-Match"
    );
    assert!(
        s.put_if_match(&key, Bytes::from_static(b"new"), &t)
            .await
            .unwrap()
            .ok()
            .is_some()
    );
    s.delete(&key).await.unwrap();
}

// ---------------------------------------------------------------------------------------------
// External S3-compatible leg (CI SeaweedFS; human_needed locally)
// ---------------------------------------------------------------------------------------------

fn external_s3() -> Option<(StoreOptions, String)> {
    let Ok(url) = std::env::var("OSTRO_TEST_S3_URL") else {
        println!("SKIP (human_needed): OSTRO_TEST_S3_URL unset (CI SeaweedFS step / Docker host)");
        return None;
    };
    let var = |n: &str| {
        std::env::var(n).unwrap_or_else(|_| panic!("{n} must be set with OSTRO_TEST_S3_URL"))
    };
    let opts = StoreOptions {
        endpoint: Some(url.clone()),
        region: Some("us-east-1".into()),
        url_style: UrlStyle::Path,
        credentials: Some(Credentials {
            access_key_id: var("OSTRO_TEST_S3_ACCESS_KEY"),
            secret_access_key: var("OSTRO_TEST_S3_SECRET_KEY"),
            session_token: None,
        }),
        allow_http: url.starts_with("http://"),
        ..StoreOptions::default()
    };
    Some((opts, var("OSTRO_TEST_S3_BUCKET")))
}

fn run_ns() -> String {
    // Unique per run so a shared bucket never sees stale keys from an earlier run.
    let nanos = std::time::SystemTime::now() // clock-exempt: test namespace uniqueness only
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("objstore-conformance/{}-{nanos}", std::process::id())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conformance_external_s3() {
    let Some((opts, bucket)) = external_s3() else {
        return;
    };
    let big = std::env::var("OSTRO_TEST_S3_BIG").is_ok_and(|v| v == "1");
    let opts = if big {
        opts // real 64 MiB threshold, 70 MiB body
    } else {
        StoreOptions {
            multipart_threshold: 6 << 20,
            multipart_part_size: 5 << 20,
            ..opts
        }
    };
    let s: Arc<dyn ObjectStore> = Arc::new(S3Store::new(&bucket, "", &opts).unwrap());
    let ns = run_ns();
    let len = if big { 70 << 20 } else { (12 << 20) + 3 };
    conformance::run_all(s.clone(), &ns, len).await;
    conformance::put_if_absent_race(s, &ns).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_s3_rejects_mismatched_if_match() {
    let Some((opts, bucket)) = external_s3() else {
        return;
    };
    let s = S3Store::new(&bucket, &run_ns(), &opts).unwrap();
    mismatched_if_match(&s).await;
}

/// Smoke check: the store enforces `If-None-Match: *` (two creates of one key, the second
/// gets 412). A store that silently ignores the header would let the second PUT overwrite.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_s3_if_none_match_second_put_412() {
    let Some((opts, bucket)) = external_s3() else {
        return;
    };
    let s = S3Store::new(&bucket, &run_ns(), &opts).unwrap();
    let key = "inm-smoke/LOCK";
    let first = s
        .put_if_absent(key, Bytes::from_static(b"first"))
        .await
        .unwrap();
    assert!(
        !first.is_precondition_failed(),
        "first If-None-Match: * PUT must create"
    );
    let second = s
        .put_if_absent(key, Bytes::from_static(b"second"))
        .await
        .unwrap();
    assert_eq!(
        second,
        Conditional::PreconditionFailed,
        "second If-None-Match: * PUT must get 412"
    );
    assert_eq!(&s.get(key).await.unwrap().unwrap().0[..], b"first");
    s.delete(key).await.unwrap();
}

// ---------------------------------------------------------------------------------------------
// Retry policy against a scripted server
// ---------------------------------------------------------------------------------------------

/// Answers each request with the next status of `script` (the last one repeats). Returns the
/// endpoint and the request counter.
async fn scripted(script: Vec<u16>) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let c = count.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                continue;
            };
            let c = c.clone();
            let script = script.clone();
            tokio::spawn(async move {
                // Read the head, then Content-Length bytes of body.
                let mut buf = Vec::new();
                let mut tmp = [0u8; 4096];
                let head_end = loop {
                    let n = sock.read(&mut tmp).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                    if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break i + 4;
                    }
                };
                let head = String::from_utf8_lossy(&buf[..head_end]).to_ascii_lowercase();
                let len: usize = head
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse().ok())
                    .unwrap_or(0);
                while buf.len() < head_end + len {
                    let n = sock.read(&mut tmp).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
                let i = c.fetch_add(1, Ordering::SeqCst);
                let status = script[i.min(script.len() - 1)];
                let resp = format!(
                    "HTTP/1.1 {status} X\r\nETag: \"e{i}\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    (format!("http://{addr}"), count)
}

fn scripted_store(endpoint: &str, max_retries: u32) -> S3Store {
    S3Store::new(
        "b",
        "",
        &StoreOptions {
            max_retries,
            ..s3s_opts(endpoint)
        },
    )
    .unwrap()
}

#[tokio::test]
async fn retries_5xx_and_429_then_succeeds() {
    let (ep, count) = scripted(vec![503, 429, 500, 200]).await;
    let out = scripted_store(&ep, 5)
        .put("k", Bytes::from_static(b"x"))
        .await
        .expect("succeeds after 3 retries");
    assert_eq!(count.load(Ordering::SeqCst), 4);
    assert_eq!(out.e_tag.as_deref(), Some("\"e3\""));
}

#[tokio::test]
async fn retries_are_capped() {
    let (ep, count) = scripted(vec![503]).await;
    match scripted_store(&ep, 5)
        .put("k", Bytes::from_static(b"x"))
        .await
    {
        Err(Error::Status { status: 503, .. }) => {}
        other => panic!("expected 503 after retries, got {other:?}"),
    }
    assert_eq!(count.load(Ordering::SeqCst), 6, "1 attempt + 5 retries");
}

#[tokio::test]
async fn never_retries_412_or_4xx() {
    let (ep, count) = scripted(vec![412, 200]).await;
    let r = scripted_store(&ep, 5)
        .put_if_absent("k", Bytes::from_static(b"x"))
        .await
        .unwrap();
    assert_eq!(r, Conditional::PreconditionFailed);
    assert_eq!(count.load(Ordering::SeqCst), 1);

    let (ep, count) = scripted(vec![400, 200]).await;
    match scripted_store(&ep, 5)
        .put("k", Bytes::from_static(b"x"))
        .await
    {
        Err(Error::Status { status: 400, .. }) => {}
        other => panic!("expected 400, got {other:?}"),
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);

    let (ep, count) = scripted(vec![404, 200]).await;
    assert!(scripted_store(&ep, 5).head("k").await.unwrap().is_none());
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn retries_connection_errors() {
    // Nothing listens on this port: connect fails every time; the error surfaces as Transport.
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ep = format!("http://{}", l.local_addr().unwrap());
    drop(l);
    match scripted_store(&ep, 2)
        .put("k", Bytes::from_static(b"x"))
        .await
    {
        Err(Error::Transport(_)) => {}
        other => panic!("expected a transport error, got {other:?}"),
    }
}
