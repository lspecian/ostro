//! Phase 6 bridge administration against a real in-process server (plan 06-16, D-FX9,
//! SC3): secrets (write-only, sealed at rest), grants, connections, egress policies over
//! REST and gRPC, and the unauthenticated local presign route
//! `GET|PUT /v1/objects/{connection}/{key}?exp=&sig=`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use common::{
    BridgeTestOpts, TestServer, put_connection, put_sealed_secret, start_server_with_bridge,
    tmpfs_dir,
};
use ostro_bridge::object::presign_local;
use ostro_meta::{MetaKey, MetaRecord};
use ostro_objstore::PresignMethod;
use ostro_proto::api::v1 as pb;
use pb::connectors_client::ConnectorsClient;
use pb::secrets_client::SecretsClient;
use reqwest::StatusCode;
use serde_json::{Value, json};

/// Write-only canaries: never expected in any response or on disk.
const CANARY_REST: &str = "cnry-rest-7f3a9c1e5b2d4086";
const CANARY_GRPC: &str = "cnry-grpc-2b8e6d0f4a1c9357";

async fn server() -> TestServer {
    start_server_with_bridge(tmpfs_dir(), BridgeTestOpts::default()).await
}

async fn send(
    s: &TestServer,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Value, String) {
    let mut rb = s.http.request(method, s.url(path));
    if let Some(b) = body {
        rb = rb.json(&b);
    }
    let r = rb.send().await.expect("request");
    let st = r.status();
    let text = r.text().await.expect("body");
    let v = if text.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&text).unwrap_or(Value::String(text.clone()))
    };
    (st, v, text)
}

async fn put(s: &TestServer, path: &str, body: Value) -> (StatusCode, Value) {
    let (st, v, _) = send(s, reqwest::Method::PUT, path, Some(body)).await;
    (st, v)
}

async fn get(s: &TestServer, path: &str) -> (StatusCode, Value) {
    let (st, v, _) = send(s, reqwest::Method::GET, path, None).await;
    (st, v)
}

async fn delete(s: &TestServer, path: &str) -> StatusCode {
    send(s, reqwest::Method::DELETE, path, None).await.0
}

fn reason(v: &Value) -> &str {
    v["error"]["details"]["reason"].as_str().unwrap_or("")
}

fn field(v: &Value) -> &str {
    v["error"]["details"]["field"].as_str().unwrap_or("")
}

/// Every file under `dir`, recursively.
fn files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            files(&p, out);
        } else {
            out.push(p);
        }
    }
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crud_round_trip_over_rest_and_grpc() {
    let mut s = server().await;
    let roots = tempfile::tempdir().expect("tempdir");

    // --- secrets ---
    let (st, v) = put(
        &s,
        "/v1/secrets/stripe/live",
        json!({ "value": CANARY_REST }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["name"], "stripe/live");
    assert!(
        v["key_id"].as_str().unwrap().starts_with("aes256gcm:"),
        "{v}"
    );
    assert!(v.get("value").is_none(), "{v}");
    let (st, v) = put(
        &s,
        "/v1/secrets/stripe/live?tenant=acme",
        json!({ "value": "acme-secret-value" }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["tenant"], "acme");

    let (st, v) = get(&s, "/v1/secrets").await;
    assert_eq!(st, StatusCode::OK);
    let names: Vec<&str> = v["secrets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["stripe/live"], "{v}");
    let (_, v) = get(&s, "/v1/secrets?tenant=acme").await;
    assert_eq!(v["secrets"].as_array().unwrap().len(), 1, "{v}");
    assert_eq!(v["secrets"][0]["tenant"], "acme");

    let mut sec = SecretsClient::new(s.channel().await);
    let info = sec
        .put_secret(pb::PutSecretRequest {
            name: "github/token".into(),
            value: CANARY_GRPC.as_bytes().to_vec(),
            tenant: None,
        })
        .await
        .expect("grpc put secret")
        .into_inner();
    assert_eq!(info.name, "github/token");
    let list = sec
        .list_secrets(pb::ListSecretsRequest { tenant: None })
        .await
        .expect("grpc list")
        .into_inner();
    let names: Vec<String> = list.secrets.iter().map(|x| x.name.clone()).collect();
    assert_eq!(names, ["github/token", "stripe/live"]);

    assert_eq!(
        delete(&s, "/v1/secrets/github/token").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        delete(&s, "/v1/secrets/github/token").await,
        StatusCode::NOT_FOUND
    );

    // --- grants ---
    let (st, v) = put(
        &s,
        "/v1/grants",
        json!({
            "module": "acme/billing",
            "secret": "stripe/live",
            "origins": ["https://API.Stripe.com/", "https://*.example.com:8443"],
            "body": true,
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["id"], "secret:acme/billing:stripe/live");
    assert_eq!(
        v["origins"],
        json!(["https://api.stripe.com", "https://*.example.com:8443"])
    );
    assert_eq!(v["body"], true);
    let (st, v) = put(
        &s,
        "/v1/grants",
        json!({ "module": "other", "secret": "stripe/live", "origins": ["https://a.example"] }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let (_, v) = get(&s, "/v1/grants?module=acme%2Fbilling").await;
    assert_eq!(v["grants"].as_array().unwrap().len(), 1, "{v}");
    let (_, v) = get(&s, "/v1/grants").await;
    assert_eq!(v["grants"].as_array().unwrap().len(), 2, "{v}");
    let grpc_grants = sec
        .list_grants(pb::ListGrantsRequest {
            module: Some("acme/billing".into()),
        })
        .await
        .expect("grpc grants")
        .into_inner();
    assert_eq!(grpc_grants.grants.len(), 1);
    assert_eq!(grpc_grants.grants[0].id, "secret:acme/billing:stripe/live");
    // Origin patterns are checked by the bridge parser too.
    let (st, v) = put(
        &s,
        "/v1/grants",
        json!({ "module": "m", "secret": "s1", "origins": ["*"] }),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(
        delete(&s, "/v1/grants/token:abc").await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        delete(&s, "/v1/grants/secret:other:stripe/live").await,
        StatusCode::NO_CONTENT
    );

    // --- connections ---
    let (st, v) = put(
        &s,
        "/v1/connections/https%3A%2F%2Fapi.stripe.com",
        json!({ "kind": "http-target", "config": { "v": 1 } }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["name"], "https://api.stripe.com");
    let (st, v) = put(
        &s,
        "/v1/connections/https%3A%2F%2FAPI.stripe.com%3A443",
        json!({ "kind": "http-target", "config": { "v": 1 } }),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(field(&v), "name", "{v}");
    let root = roots.path().join("files");
    let (st, v) = put(
        &s,
        "/v1/connections/files",
        json!({ "kind": "object", "config": { "v": 1, "backend": "local", "root": root } }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let (st, v) = put(
        &s,
        "/v1/connections/files2",
        json!({ "kind": "object", "config": { "v": 1, "backend": "local", "root": "rel" } }),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(field(&v), "config");
    let pg = json!({ "v": 1, "host": "127.0.0.1", "database": "app", "user": "app",
                     "password": "{{secret:db/pass}}" });
    let (st, v) = put(
        &s,
        "/v1/connections/orders-db",
        json!({ "kind": "sql-postgres", "config": pg }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    // A literal password is refused (the config would hold it in plain text).
    let (st, v) = put(
        &s,
        "/v1/connections/orders-db",
        json!({ "kind": "sql-postgres", "config": { "v": 1, "host": "h", "database": "d",
                "user": "u", "password": "literal-password-1" } }),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert!(!v.to_string().contains("literal-password-1"), "{v}");
    let (st, v) = put(
        &s,
        "/v1/connections/x",
        json!({ "kind": "sql-postgres", "config": { "v": 2 } }),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");

    let (st, v) = get(&s, "/v1/connections/orders-db").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["config"], pg, "templates come back verbatim");
    let (_, v) = get(&s, "/v1/connections").await;
    let names: Vec<&str> = v["connections"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["files", "https://api.stripe.com", "orders-db"]);
    let mut con = ConnectorsClient::new(s.channel().await);
    let c = con
        .get_connection(pb::GetConnectionRequest {
            name: "orders-db".into(),
        })
        .await
        .expect("grpc get connection")
        .into_inner();
    assert_eq!(c.kind, "sql-postgres");
    assert_eq!(serde_json::from_slice::<Value>(&c.config_json).unwrap(), pg);
    assert_eq!(
        delete(&s, "/v1/connections/https%3A%2F%2Fapi.stripe.com").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        get(&s, "/v1/connections/https%3A%2F%2Fapi.stripe.com")
            .await
            .0,
        StatusCode::NOT_FOUND
    );

    // --- egress ---
    let (st, v) = put(
        &s,
        "/v1/egress/acme%2Fbilling",
        json!({ "allow": ["https://api.stripe.com", "https://*.example.com:8443",
                          "connection:orders-db"] }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["module"], "acme/billing");
    let (st, v) = put(&s, "/v1/egress/m", json!({ "allow": ["ftp://x.example"] })).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(field(&v), "allow");
    let (st, v) = get(&s, "/v1/egress/acme%2Fbilling").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["allow"].as_array().unwrap().len(), 3);
    let e = con
        .list_egress(pb::ListEgressRequest {})
        .await
        .expect("grpc egress")
        .into_inner();
    assert_eq!(e.policies.len(), 1);
    assert_eq!(e.policies[0].module, "acme/billing");
    // Stored as the record the bridge reads.
    let rec = s
        .h()
        .meta()
        .view()
        .read(|st| st.get(&MetaKey::Egress("acme/billing".into())))
        .expect("egress record");
    let MetaRecord::Egress(p) = rec.value else {
        panic!("egress record kind")
    };
    assert_eq!(p.allow[2], "connection:orders-db");
    assert_eq!(
        delete(&s, "/v1/egress/acme%2Fbilling").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        get(&s, "/v1/egress/acme%2Fbilling").await.0,
        StatusCode::NOT_FOUND
    );

    drop(sec);
    drop(con);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secret_values_are_sealed_at_rest_and_never_returned() {
    let mut s = server().await;
    let (st, v) = put(
        &s,
        "/v1/secrets/canary/rest",
        json!({ "value": CANARY_REST }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let mut sec = SecretsClient::new(s.channel().await);
    sec.put_secret(pb::PutSecretRequest {
        name: "canary/grpc".into(),
        value: CANARY_GRPC.as_bytes().to_vec(),
        tenant: Some("acme".into()),
    })
    .await
    .expect("grpc put");

    // Every list/get response, REST and gRPC.
    let mut seen = String::new();
    for path in [
        "/v1/secrets",
        "/v1/secrets?tenant=acme",
        "/v1/grants",
        "/v1/connections",
        "/v1/egress",
        "/openapi.json",
    ] {
        let (st, _, text) = send(&s, reqwest::Method::GET, path, None).await;
        assert_eq!(st, StatusCode::OK, "{path}");
        seen.push_str(&text);
    }
    for tenant in [None, Some("acme".to_owned())] {
        let r = sec
            .list_secrets(pb::ListSecretsRequest { tenant })
            .await
            .expect("grpc list")
            .into_inner();
        seen.push_str(&format!("{r:?}"));
    }
    let r = sec
        .list_grants(pb::ListGrantsRequest { module: None })
        .await
        .expect("grants")
        .into_inner();
    seen.push_str(&format!("{r:?}"));
    assert!(seen.contains("canary/rest") && seen.contains("canary/grpc"));
    for c in [CANARY_REST, CANARY_GRPC] {
        assert!(!seen.contains(c), "canary {c} leaked through the API");
    }

    // The meta records are sealed under the tenant-qualified name and open to the value.
    let store = s.h().bridge().sealed_store().expect("key").clone();
    for (name, canary) in [
        ("_/canary/rest", CANARY_REST),
        ("acme/canary/grpc", CANARY_GRPC),
    ] {
        let rec = s
            .h()
            .meta()
            .view()
            .read(|st| st.get(&MetaKey::Secret(name.into())))
            .unwrap_or_else(|| panic!("record {name}"));
        let MetaRecord::Secret(sealed) = rec.value else {
            panic!("secret record kind")
        };
        assert!(sealed.key_id.starts_with("aes256gcm:"), "{}", sealed.key_id);
        assert!(!contains(&sealed.ciphertext, canary.as_bytes()));
        assert_eq!(
            store.open(&sealed).expect("open").expose(),
            canary.as_bytes()
        );
    }

    // Nothing on disk holds a canary in plain text (meta log and snapshots included).
    let data_dir = s.cfg.node.data_dir.clone();
    drop(sec);
    s.shutdown().await;
    let mut all = Vec::new();
    files(&data_dir, &mut all);
    assert!(
        all.iter().any(|p| p.starts_with(data_dir.join("meta"))),
        "meta files exist: {all:?}"
    );
    for p in &all {
        let bytes = std::fs::read(p).unwrap_or_default();
        for c in [CANARY_REST, CANARY_GRPC] {
            assert!(
                !contains(&bytes, c.as_bytes()),
                "{c} in plain text in {}",
                p.display()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reserved_names_and_short_values_are_refused() {
    let mut s = server().await;
    let (st, v) = put(
        &s,
        "/v1/secrets/__ostro/x",
        json!({ "value": "long-enough-value" }),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(reason(&v), "secret_reserved");
    let (st, v) = put(&s, "/v1/secrets/short", json!({ "value": "7-bytes" })).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(reason(&v), "secret_too_short");
    assert!(!v.to_string().contains("7-bytes"), "{v}");
    assert_eq!(
        delete(&s, "/v1/secrets/__ostro/token-key").await,
        StatusCode::BAD_REQUEST
    );
    // The token key is still there and never listed.
    assert!(s.h().meta().view().read(|st| st.token_key()).is_some());
    let (_, v) = get(&s, "/v1/secrets").await;
    assert!(!v.to_string().contains("__ostro"), "{v}");
    let mut sec = SecretsClient::new(s.channel().await);
    let e = sec
        .put_secret(pb::PutSecretRequest {
            name: "__ostro/presign/files".into(),
            value: b"0123456789abcdef".to_vec(),
            tenant: None,
        })
        .await
        .expect_err("reserved over gRPC");
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    drop(sec);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_profile_without_a_sealing_key_refuses_secrets() {
    let mut s = start_server_with_bridge(
        tmpfs_dir(),
        BridgeTestOpts {
            sealing_key: None,
            ..BridgeTestOpts::default()
        },
    )
    .await;
    assert!(s.h().bridge().sealed_store().is_none());
    let (st, v) = put(
        &s,
        "/v1/secrets/stripe/live",
        json!({ "value": CANARY_REST }),
    )
    .await;
    assert_eq!(st, StatusCode::PRECONDITION_FAILED, "{v}");
    assert_eq!(reason(&v), "sealing_key_missing");
    let msg = v["error"]["message"].as_str().unwrap();
    assert!(
        msg.contains("OSTRO_SEALING_KEY") && msg.contains("sealing_key_file"),
        "{msg}"
    );
    assert!(!v.to_string().contains(CANARY_REST));
    // Nothing was stored; the other resources still work.
    let (_, v) = get(&s, "/v1/secrets").await;
    assert_eq!(v["secrets"], json!([]));
    let (st, _) = put(
        &s,
        "/v1/egress/m",
        json!({ "allow": ["https://a.example"] }),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_presign_route_serves_signed_urls_without_auth() {
    let mut s = server().await;
    let roots = tempfile::tempdir().expect("tempdir");
    let (st, v) = put(
        &s,
        "/v1/connections/files",
        json!({ "kind": "object",
                "config": { "v": 1, "backend": "local", "root": roots.path(), "prefix": "p" } }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    // A known presign key, sealed under its reserved name exactly as MetaPresignKeyStore
    // writes it (the admin API itself refuses `__ostro/`).
    let hmac_key = [0x42u8; 32];
    put_sealed_secret(&s, &presign_local::key_name("files"), &hmac_key).await;
    let base = s.url("");
    let signed = |method: PresignMethod, key: &str, exp: u64| {
        let sig = presign_local::sign(&hmac_key, method, "files", key, exp).expect("sign");
        presign_local::url(&base, "files", key, exp, &sig)
            .expect("url")
            .to_string()
    };
    let exp = now_unix() + 600;
    // A fresh client: no default headers, no credentials.
    let anon = reqwest::Client::new();

    let put_url = signed(PresignMethod::Put, "dir/a.txt", exp);
    let r = anon
        .put(&put_url)
        .header("content-type", "text/plain")
        .body("hello presign")
        .send()
        .await
        .expect("PUT");
    assert_eq!(r.status(), StatusCode::OK, "{:?}", r.text().await);
    assert!(roots.path().join("p/dir/a.txt").exists());

    let get_url = signed(PresignMethod::Get, "dir/a.txt", exp);
    let r = anon.get(&get_url).send().await.expect("GET");
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.text().await.unwrap(), "hello presign");

    // The PUT signature does not authorise a GET.
    let swapped = put_url.clone();
    let r = anon.get(&swapped).send().await.expect("GET with PUT sig");
    assert_eq!(r.status(), StatusCode::FORBIDDEN);

    // Tampered signature.
    let sig = presign_local::sign(&hmac_key, PresignMethod::Get, "files", "dir/a.txt", exp)
        .expect("sign");
    let mut bad = sig.clone().into_bytes();
    bad[0] = if bad[0] == b'0' { b'1' } else { b'0' };
    let tampered = get_url.replace(&sig, std::str::from_utf8(&bad).unwrap());
    assert_ne!(tampered, get_url);
    assert_eq!(
        anon.get(&tampered).send().await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    // Tampered key (signature for another object).
    let other = get_url.replace("a.txt", "b.txt");
    assert_eq!(
        anon.get(&other).send().await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    // Expired (correctly signed, exp in the past).
    let expired = signed(PresignMethod::Get, "dir/a.txt", now_unix() - 5);
    let r = anon.get(&expired).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    assert!(r.text().await.unwrap().contains("expired"));
    // Absent object, unknown connection, non-local backend.
    let missing = signed(PresignMethod::Get, "dir/none.txt", exp);
    assert_eq!(
        anon.get(&missing).send().await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
    let unknown = get_url.replace("/files/", "/nope/");
    assert_eq!(
        anon.get(&unknown).send().await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
    put_connection(
        &s,
        "remote",
        "object",
        json!({ "v": 1, "backend": "s3", "endpoint": "http://127.0.0.1:1", "bucket": "b" }),
    )
    .await;
    let remote = get_url.replace("/files/", "/remote/");
    assert_eq!(
        anon.get(&remote).send().await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
    s.shutdown().await;
}
