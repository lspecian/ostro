//! Presign (Phase 6, 06-12).
//!
//! - LocalFs never presigns (`Ok(None)`; the bridge serves an Ostro HMAC URL instead).
//! - S3: the same suite runs against an in-process **s3s-fs** server (always; s3s verifies
//!   SigV4 query signatures, including the expiry) and against an external S3-compatible store
//!   (SeaweedFS in CI), env-gated on `OSTRO_TEST_S3_*` with a loud SKIP when unset.
//!
//! The suite: a presigned GET is fetchable by a plain reqwest client (with the
//! `response-content-type` override), a presigned PUT carrying the signed content type
//! uploads, a PUT with a different content type is refused, and a 1 s URL fails after 2 s.

use std::time::Duration;

use bytes::Bytes;
use ostro_objstore::{
    Credentials, LocalFs, ObjectStore, PresignMethod, PresignOpts, PutOpts, S3Store, StoreOptions,
    UrlStyle,
};
use tokio::net::TcpListener;

#[tokio::test]
async fn localfs_presign_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let s = LocalFs::new(dir.path()).unwrap();
    s.put("k", Bytes::from_static(b"v")).await.unwrap();
    for m in [PresignMethod::Get, PresignMethod::Put] {
        let url = s
            .presign(m, "k", Duration::from_secs(900), &PresignOpts::default())
            .await
            .unwrap();
        assert!(url.is_none(), "LocalFs must not presign");
    }
}

// ---------------------------------------------------------------------------------------------
// The S3 suite
// ---------------------------------------------------------------------------------------------

async fn presign_suite(s: &S3Store, ns: &str) {
    let plain = reqwest::Client::new();

    // GET: object written with put_with (content type stored), fetched with a plain client.
    let key = format!("{ns}/get.json");
    s.put_with(
        &key,
        Bytes::from_static(b"{\"a\":1}"),
        &PutOpts {
            content_type: Some("application/json".into()),
        },
    )
    .await
    .unwrap();
    let url = s
        .presign(
            PresignMethod::Get,
            &key,
            Duration::from_secs(300),
            &PresignOpts {
                content_type: None,
                response_content_type: Some("text/plain".into()),
            },
        )
        .await
        .unwrap()
        .expect("S3 presigns");
    let r = plain.get(url.clone()).send().await.unwrap();
    assert_eq!(r.status(), 200, "presigned GET");
    let rct = r
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    assert_eq!(&r.bytes().await.unwrap()[..], b"{\"a\":1}");
    println!("presign GET response content-type: {rct:?}");
    // A tampered signature is refused.
    // A tampered signature (same length, one hex digit flipped) is refused.
    let mut tampered = url.clone();
    let pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(k, v)| {
            let v = if k == "X-Amz-Signature" {
                let mut b = v.into_owned().into_bytes();
                b[0] = if b[0] == b'0' { b'1' } else { b'0' };
                String::from_utf8(b).unwrap()
            } else {
                v.into_owned()
            };
            (k.into_owned(), v)
        })
        .collect();
    tampered.query_pairs_mut().clear().extend_pairs(pairs);
    let r = plain.get(tampered).send().await.unwrap();
    assert_eq!(r.status(), 403, "tampered presigned GET must be refused");

    // PUT with the signed content type.
    let key = format!("{ns}/put.txt");
    let url = s
        .presign(
            PresignMethod::Put,
            &key,
            Duration::from_secs(300),
            &PresignOpts {
                content_type: Some("text/plain".into()),
                response_content_type: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    let r = plain
        .put(url.clone())
        .header("content-type", "text/plain")
        .body("uploaded")
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "presigned PUT: {}", r.status());
    assert_eq!(&s.get(&key).await.unwrap().unwrap().0[..], b"uploaded");
    let r = plain
        .put(url)
        .header("content-type", "application/octet-stream")
        .body("other")
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        403,
        "a PUT with a different content type must fail the signature"
    );
    assert_eq!(&s.get(&key).await.unwrap().unwrap().0[..], b"uploaded");

    // Expiry: a 1 s URL is refused after 2 s.
    let url = s
        .presign(
            PresignMethod::Get,
            &format!("{ns}/get.json"),
            Duration::from_secs(1),
            &PresignOpts::default(),
        )
        .await
        .unwrap()
        .unwrap();
    // clock-exempt: waiting out a presign validity window in a test
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let r = plain.get(url).send().await.unwrap();
    assert_eq!(r.status(), 403, "expired presigned GET must be refused");

    for k in ["get.json", "put.txt"] {
        s.delete(&format!("{ns}/{k}")).await.unwrap();
    }
}

// ---------------------------------------------------------------------------------------------
// s3s-fs leg (always)
// ---------------------------------------------------------------------------------------------

const BUCKET: &str = "ostro-test";
const AK: &str = "ostro-ak";
const SK: &str = "ostro-sk-0123456789";

async fn start_s3s_fs() -> (tempfile::TempDir, String) {
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
    (dir, format!("http://{addr}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn presign_s3s_fs() {
    let (_dir, endpoint) = start_s3s_fs().await;
    let opts = StoreOptions {
        endpoint: Some(endpoint),
        region: Some("us-east-1".into()),
        url_style: UrlStyle::Path,
        credentials: Some(Credentials {
            access_key_id: AK.into(),
            secret_access_key: SK.into(),
            session_token: None,
        }),
        allow_http: true,
        ..StoreOptions::default()
    };
    let s = S3Store::new(BUCKET, "", &opts).unwrap();
    presign_suite(&s, "presign").await;
}

// ---------------------------------------------------------------------------------------------
// External S3-compatible leg (CI SeaweedFS; human_needed locally)
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn presign_external_s3() {
    let Ok(url) = std::env::var("OSTRO_TEST_S3_URL") else {
        println!("SKIP: OSTRO_TEST_S3_URL unset (human_needed / CI SeaweedFS)");
        return;
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
    let s = S3Store::new(&var("OSTRO_TEST_S3_BUCKET"), "", &opts).unwrap();
    let nanos = std::time::SystemTime::now() // clock-exempt: test namespace uniqueness only
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    presign_suite(
        &s,
        &format!("objstore-presign/{}-{nanos}", std::process::id()),
    )
    .await;
}
