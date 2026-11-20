//! ROADMAP Phase 6 SC5, object store through a guest (plan 06-19).
//!
//! - **local**: connection `files` (backend `local`, root in a temp dir) and egress
//!   `connection:files` for fx-object. The guest runs put → get → head → presign-get →
//!   presign-put; an unauthenticated client then fetches the presigned GET URL (served by
//!   the Ostro `/v1/objects` route) and uploads through the presigned PUT URL; the guest
//!   reads the new content back, deletes it and gets `not_found`. A tampered signature and
//!   an expired URL (`presign_expiry = "1s"`) are 403; a module whose policy lacks
//!   `connection:files` gets `egress_denied`.
//! - **s3** (env-gated, D-FX5/D-FX10: SeaweedFS in CI): the same sequence against an
//!   S3-compatible store with the secret key as a sealed `{{secret:..}}` credential; the
//!   presigned URLs are used by a plain reqwest client. Without `OSTRO_TEST_S3_URL` the leg
//!   prints a loud SKIP and passes (human_needed locally).
//!
//! No mock servers here, so nothing needs `spawn_quiet`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use common::fx::{configure, deploy_fx, start_fx_run, wait_terminal};
use common::{BridgeTestOpts, TestServer, start_server_with_bridge, tmpfs_dir};
use serde_json::{Value, json};

const RUN_LIMIT: Duration = Duration::from_secs(60);

/// Run fx-object over `ops` on `connection`; returns the `results` array.
async fn object_run(s: &TestServer, connection: &str, ops: Value) -> Vec<Value> {
    let run = start_fx_run(
        s,
        "fx-object",
        "fx-object",
        json!({ "connection": connection, "ops": ops }),
    )
    .await;
    let info = wait_terminal(s, &run, RUN_LIMIT).await;
    assert_eq!(
        serde_json::to_value(info.status).expect("status"),
        "completed",
        "{info:?}"
    );
    let out: Value =
        serde_json::from_slice(&s.result_bytes(&run).await.expect("result")).expect("json");
    out["results"].as_array().expect("results").clone()
}

fn bytes_of(item: &Value) -> Vec<u8> {
    let b = item["bytes_b64"]
        .as_str()
        .unwrap_or_else(|| panic!("not a success item: {item}"));
    STANDARD.decode(b).expect("base64")
}

fn text_of(item: &Value) -> String {
    String::from_utf8(bytes_of(item)).expect("utf-8")
}

fn error_kind(item: &Value) -> &str {
    item["error"]["kind"]
        .as_str()
        .unwrap_or_else(|| panic!("not an error item: {item}"))
}

fn b64(b: &[u8]) -> String {
    STANDARD.encode(b)
}

/// The URL with the last character of its `sig` value changed.
fn tamper(url: &str) -> String {
    let at = url.find("sig=").expect("sig param") + 4;
    let end = url[at..].find('&').map_or(url.len(), |e| at + e);
    let mut s = url.to_owned();
    let last = &url[end - 1..end];
    let repl = if last == "A" { "B" } else { "A" };
    s.replace_range(end - 1..end, repl);
    s
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_backend_through_guest() {
    let s = start_server_with_bridge(tmpfs_dir(), BridgeTestOpts::default()).await;
    let root = tempfile::tempdir().expect("object root");
    let module = deploy_fx(&s, "fx-object").await;
    let rootp = root.path().display().to_string();
    configure(&s)
        .connection(
            "files",
            "object",
            json!({ "v": 1, "backend": "local", "root": rootp }),
        )
        .connection(
            "files-short",
            "object",
            json!({ "v": 1, "backend": "local", "root": rootp, "prefix": "short",
                    "presign_expiry": "1s" }),
        )
        .egress(&module, &["connection:files", "connection:files-short"])
        .apply()
        .await;

    // put → get → head → presign-get → presign-put
    let r = object_run(
        &s,
        "files",
        json!([
            { "op": "put", "key": "a/b.txt", "body_b64": b64(b"hello") },
            { "op": "get", "key": "a/b.txt" },
            { "op": "head", "key": "a/b.txt" },
            { "op": "presign-get", "key": "a/b.txt" },
            { "op": "presign-put", "key": "a/b.txt" },
        ]),
    )
    .await;
    assert_eq!(r.len(), 5);
    let put: Value = serde_json::from_slice(&bytes_of(&r[0])).expect("put json");
    assert!(put.get("e_tag").is_some(), "put info: {put}");
    assert_eq!(bytes_of(&r[1]), b"hello");
    let head: Value = serde_json::from_slice(&bytes_of(&r[2])).expect("head json");
    assert_eq!(head["size"], 5, "{head}");
    let get_url = text_of(&r[3]);
    let put_url = text_of(&r[4]);
    let base = format!("http://{}/v1/objects/files/", s.addr());
    assert!(get_url.starts_with(&base), "{get_url}");
    assert!(put_url.starts_with(&base), "{put_url}");
    assert_eq!(
        std::fs::read(root.path().join("a/b.txt")).expect("stored file"),
        b"hello"
    );

    // An unauthenticated client uses both URLs.
    let anon = reqwest::Client::new();
    let resp = anon.get(&get_url).send().await.expect("presigned GET");
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.expect("body").as_ref(), b"hello");
    let resp = anon
        .put(&put_url)
        .body("uploaded via presign")
        .send()
        .await
        .expect("presigned PUT");
    assert_eq!(resp.status(), 200, "{:?}", resp.text().await);
    let resp = anon.get(tamper(&get_url)).send().await.expect("tampered");
    assert_eq!(resp.status(), 403, "tampered signature");
    let resp = anon
        .put(tamper(&put_url))
        .body("x")
        .send()
        .await
        .expect("tampered");
    assert_eq!(resp.status(), 403, "tampered PUT signature");

    // The guest sees the new content, deletes it, and then gets not_found.
    let r = object_run(
        &s,
        "files",
        json!([
            { "op": "get", "key": "a/b.txt" },
            { "op": "delete", "key": "a/b.txt" },
            { "op": "get", "key": "a/b.txt" },
        ]),
    )
    .await;
    assert_eq!(bytes_of(&r[0]), b"uploaded via presign");
    assert_eq!(bytes_of(&r[1]), b"", "delete returns empty output");
    assert_eq!(error_kind(&r[2]), "not_found", "{}", r[2]);

    // presign_expiry = 1s: valid at once, 403 after 2 s.
    let r = object_run(
        &s,
        "files-short",
        json!([
            { "op": "put", "key": "e.txt", "body_b64": b64(b"short-lived") },
            { "op": "presign-get", "key": "e.txt" },
        ]),
    )
    .await;
    let short_url = text_of(&r[1]);
    let resp = anon.get(&short_url).send().await.expect("fresh");
    assert_eq!(resp.status(), 200, "fresh short URL");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let resp = anon.get(&short_url).send().await.expect("expired");
    assert_eq!(resp.status(), 403, "expired URL");

    // A module whose policy lacks connection:files is denied.
    configure(&s)
        .egress(&module, &["connection:files-short"])
        .apply()
        .await;
    let r = object_run(&s, "files", json!([{ "op": "get", "key": "a/b.txt" }])).await;
    assert_eq!(error_kind(&r[0]), "egress_denied", "{}", r[0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_backend_through_guest() {
    let Ok(endpoint) = std::env::var("OSTRO_TEST_S3_URL") else {
        println!(
            "SKIP: OSTRO_TEST_S3_URL unset: S3 object leg not run (human_needed locally; \
             CI SeaweedFS, D-FX5/D-FX10)"
        );
        return;
    };
    let var = |n: &str| {
        std::env::var(n).unwrap_or_else(|_| panic!("{n} must be set with OSTRO_TEST_S3_URL"))
    };
    let access = var("OSTRO_TEST_S3_ACCESS_KEY");
    let secret = var("OSTRO_TEST_S3_SECRET_KEY");
    let bucket = var("OSTRO_TEST_S3_BUCKET");

    let s = start_server_with_bridge(tmpfs_dir(), BridgeTestOpts::default()).await;
    let module = deploy_fx(&s, "fx-object").await;
    // Templated secrets must be at least 8 bytes; a shorter access key id stays literal
    // (the config allows a literal access key id, never a literal secret key).
    let mut cfg = configure(&s).secret("s3-secret", &secret);
    let access_id = if access.len() >= 8 {
        cfg = cfg.secret("s3-key", &access);
        "{{secret:s3-key}}".to_owned()
    } else {
        access.clone()
    };
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    cfg.connection(
        "s3files",
        "object",
        json!({
            "v": 1, "backend": "s3", "endpoint": endpoint, "bucket": bucket,
            "region": "us-east-1", "allow_http": endpoint.starts_with("http://"),
            "prefix": format!("fx-object-e2e-{nanos}"),
            "content_type": "text/plain",
            "credentials": { "access_key_id": access_id,
                             "secret_access_key": "{{secret:s3-secret}}" },
        }),
    )
    .egress(&module, &["connection:s3files"])
    .apply()
    .await;

    let r = object_run(
        &s,
        "s3files",
        json!([
            { "op": "put", "key": "a/b.txt", "body_b64": b64(b"hello s3") },
            { "op": "get", "key": "a/b.txt" },
            { "op": "head", "key": "a/b.txt" },
            { "op": "presign-get", "key": "a/b.txt" },
            { "op": "presign-put", "key": "a/b.txt" },
        ]),
    )
    .await;
    assert_eq!(bytes_of(&r[1]), b"hello s3");
    let head: Value = serde_json::from_slice(&bytes_of(&r[2])).expect("head json");
    assert_eq!(head["size"], 8, "{head}");
    let plain = reqwest::Client::new();
    let resp = plain
        .get(text_of(&r[3]))
        .send()
        .await
        .expect("presigned GET");
    assert_eq!(resp.status(), 200, "presigned GET");
    assert_eq!(resp.bytes().await.expect("body").as_ref(), b"hello s3");
    let resp = plain
        .put(text_of(&r[4]))
        .header("content-type", "text/plain")
        .body("via presign")
        .send()
        .await
        .expect("presigned PUT");
    assert!(
        resp.status().is_success(),
        "presigned PUT: {}",
        resp.status()
    );

    let r = object_run(
        &s,
        "s3files",
        json!([
            { "op": "get", "key": "a/b.txt" },
            { "op": "delete", "key": "a/b.txt" },
            { "op": "get", "key": "a/b.txt" },
        ]),
    )
    .await;
    assert_eq!(bytes_of(&r[0]), b"via presign");
    assert_eq!(error_kind(&r[2]), "not_found", "{}", r[2]);
}
