//! VaultProvider against an axum mock of the Vault HTTP API (06-13).
//!
//! The mock implements `GET /v1/{mount}/data/{path}` (KV v2 response shape),
//! `POST /v1/auth/approle/login` and `POST /v1/auth/kubernetes/login`, and counts calls.
//!
//! The optional real leg runs against a Vault dev server when `OSTRO_TEST_VAULT_ADDR` and
//! `OSTRO_TEST_VAULT_TOKEN` are set; otherwise it prints SKIP.
#![cfg(feature = "vault")]

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::{Path as UrlPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use ostro_bridge::secrets::ProviderChain;
use ostro_bridge::secrets::config::VaultConfig;
use ostro_bridge::secrets::providers::vault::VaultProvider;
use ostro_bridge::secrets::providers::{ProviderError, SecretProvider};
use secrecy::ExposeSecret;
use serde_json::{Value, json};

const STATIC_TOKEN: &str = "hvs.static-canary-token";
const APPROLE_TOKEN: &str = "hvs.approle-canary-token";
const K8S_TOKEN: &str = "hvs.k8s-canary-token";
const ROLE_ID: &str = "role-123";
const SECRET_ID: &str = "secret-id-canary-456";
const SA_JWT: &str = "eyJ.sa-jwt-canary";

#[derive(Default)]
struct Mock {
    kv_calls: AtomicUsize,
    approle_logins: AtomicUsize,
    k8s_logins: AtomicUsize,
    tokens: Mutex<HashSet<String>>,
    secrets: Mutex<HashMap<String, Value>>,
    k8s_lease: AtomicUsize,
}

fn envelope(data: Value, auth: Value) -> Value {
    json!({
        "request_id": "req-1", "lease_id": "", "renewable": false, "lease_duration": 0,
        "data": data, "auth": auth, "wrap_info": null, "warnings": null
    })
}

fn metadata() -> Value {
    json!({"created_time": "2026-01-01T00:00:00Z", "deletion_time": "", "custom_metadata": null,
           "destroyed": false, "version": 1})
}

fn auth_info(token: &str, lease: u64) -> Value {
    json!({"client_token": token, "accessor": "acc", "policies": ["default"],
           "token_policies": ["default"], "metadata": {}, "lease_duration": lease,
           "renewable": true, "entity_id": "ent", "token_type": "service", "orphan": true})
}

fn denied() -> (StatusCode, Json<Value>) {
    (
        StatusCode::FORBIDDEN,
        Json(json!({"errors": ["permission denied"]})),
    )
}

async fn kv(
    State(m): State<Arc<Mock>>,
    UrlPath((mount, path)): UrlPath<(String, String)>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    m.kv_calls.fetch_add(1, Ordering::SeqCst);
    let token = headers
        .get("x-vault-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !m.tokens.lock().unwrap().contains(token) || path.starts_with("forbidden/") {
        return denied();
    }
    if path == "deleted" {
        // KV v2 shape of a deleted latest version: 404 with data but no `errors`.
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"data": {"data": null, "metadata": metadata()}})),
        );
    }
    match m.secrets.lock().unwrap().get(&format!("{mount}/{path}")) {
        Some(d) => (
            StatusCode::OK,
            Json(envelope(
                json!({"data": d, "metadata": metadata()}),
                Value::Null,
            )),
        ),
        None => (StatusCode::NOT_FOUND, Json(json!({"errors": []}))),
    }
}

async fn approle(State(m): State<Arc<Mock>>, body: axum::body::Bytes) -> (StatusCode, Json<Value>) {
    m.approle_logins.fetch_add(1, Ordering::SeqCst);
    // vaultrs sends the JSON body without a Content-Type (Vault accepts that).
    let b: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    if b["role_id"] == ROLE_ID && b["secret_id"] == SECRET_ID {
        m.tokens.lock().unwrap().insert(APPROLE_TOKEN.into());
        (
            StatusCode::OK,
            Json(envelope(Value::Null, auth_info(APPROLE_TOKEN, 3600))),
        )
    } else {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"errors": ["invalid role or secret ID"]})),
        )
    }
}

async fn k8s_login(
    State(m): State<Arc<Mock>>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<Value>) {
    m.k8s_logins.fetch_add(1, Ordering::SeqCst);
    // vaultrs sends the JSON body without a Content-Type (Vault accepts that).
    let b: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    if b["role"] == "ostro" && b["jwt"] == SA_JWT {
        m.tokens.lock().unwrap().insert(K8S_TOKEN.into());
        let lease = m.k8s_lease.load(Ordering::SeqCst) as u64;
        (
            StatusCode::OK,
            Json(envelope(Value::Null, auth_info(K8S_TOKEN, lease))),
        )
    } else {
        denied()
    }
}

async fn start() -> (Arc<Mock>, SocketAddr) {
    let m = Arc::new(Mock::default());
    m.tokens.lock().unwrap().insert(STATIC_TOKEN.into());
    {
        let mut s = m.secrets.lock().unwrap();
        s.insert(
            "secret/stripe/live".into(),
            json!({"value": "sk_live_canary_0001", "pub": "pk_live_0001"}),
        );
        s.insert(
            "secret/db/creds".into(),
            json!({"password": "db-canary-pass"}),
        );
        s.insert("kv/other".into(), json!({"value": "other-mount-value"}));
    }
    let app = Router::new()
        .route("/v1/{mount}/data/{*path}", get(kv))
        .route("/v1/auth/approle/login", post(approle))
        .route("/v1/auth/kubernetes/login", post(k8s_login))
        .with_state(m.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    (m, addr)
}

fn write(dir: &Path, name: &str, content: &str) -> String {
    let p = dir.join(name);
    std::fs::write(&p, content).unwrap();
    p.display().to_string()
}

fn cfg(v: Value) -> VaultConfig {
    serde_json::from_value(v).unwrap()
}

fn token_provider(addr: SocketAddr, dir: &Path, extra: Value) -> VaultProvider {
    let tf = write(dir, "token", &format!("{STATIC_TOKEN}\n"));
    let mut v = json!({"addr": format!("http://{addr}"), "token_file": tf});
    v.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    VaultProvider::from_config(&cfg(v)).unwrap()
}

async fn value(p: &VaultProvider, name: &str) -> Option<Vec<u8>> {
    p.get(name)
        .await
        .unwrap()
        .map(|v| v.expose_secret().to_vec())
}

#[tokio::test]
async fn token_read_field_selection_and_missing() {
    let (m, addr) = start().await;
    let dir = tempfile::tempdir().unwrap();
    let p = token_provider(addr, dir.path(), json!({"cache_ttl": "0s"}));
    assert_eq!(
        value(&p, "stripe/live").await.unwrap(),
        b"sk_live_canary_0001"
    );
    assert_eq!(value(&p, "stripe/live#pub").await.unwrap(), b"pk_live_0001");
    assert_eq!(
        value(&p, "db/creds#password").await.unwrap(),
        b"db-canary-pass"
    );
    // Missing secret, missing field (incl. the default `value`), deleted version: None.
    assert!(value(&p, "nope").await.is_none());
    assert!(value(&p, "stripe/live#nope").await.is_none());
    assert!(value(&p, "db/creds").await.is_none());
    assert!(value(&p, "deleted").await.is_none());
    assert_eq!(m.kv_calls.load(Ordering::SeqCst), 7);
    // Malformed names never reach Vault.
    assert!(matches!(
        p.get("a/../b").await,
        Err(ProviderError::Invalid(_))
    ));
    assert!(matches!(
        p.get("__ostro/x").await,
        Err(ProviderError::Invalid(_))
    ));
    assert_eq!(m.kv_calls.load(Ordering::SeqCst), 7);

    // Custom mount.
    let p = token_provider(addr, dir.path(), json!({"mount": "kv"}));
    assert_eq!(value(&p, "other").await.unwrap(), b"other-mount-value");
}

#[tokio::test]
async fn approle_login_then_read() {
    let (m, addr) = start().await;
    let dir = tempfile::tempdir().unwrap();
    let sid = write(dir.path(), "sid", SECRET_ID);
    let p = VaultProvider::from_config(&cfg(json!({
        "addr": format!("http://{addr}"), "auth": "approle",
        "role_id": ROLE_ID, "secret_id_file": sid, "cache_ttl": "0s"
    })))
    .unwrap();
    assert_eq!(m.approle_logins.load(Ordering::SeqCst), 0, "login is lazy");
    assert_eq!(
        value(&p, "stripe/live").await.unwrap(),
        b"sk_live_canary_0001"
    );
    assert_eq!(
        value(&p, "db/creds#password").await.unwrap(),
        b"db-canary-pass"
    );
    assert_eq!(
        m.approle_logins.load(Ordering::SeqCst),
        1,
        "one session for both reads"
    );

    // A wrong secret id: the login fails as Unavailable, without echoing the secret id.
    let bad = write(dir.path(), "bad-sid", "wrong-secret-id-canary");
    let p = VaultProvider::from_config(&cfg(json!({
        "addr": format!("http://{addr}"), "auth": "approle",
        "role_id": ROLE_ID, "secret_id_file": bad
    })))
    .unwrap();
    let e = p.get("stripe/live").await.unwrap_err();
    assert!(
        matches!(&e, ProviderError::Unavailable(m) if m.contains("status 400")),
        "{e}"
    );
    assert!(!e.to_string().contains("canary"), "{e}");
}

#[tokio::test]
async fn kubernetes_login_and_relogin_on_lease_expiry() {
    let (m, addr) = start().await;
    m.k8s_lease.store(1, Ordering::SeqCst);
    let dir = tempfile::tempdir().unwrap();
    let jwt = write(dir.path(), "sa-token", SA_JWT);
    let p = VaultProvider::from_config_with_sa_token(
        &cfg(
            json!({"addr": format!("http://{addr}"), "auth": "kubernetes",
                    "k8s_role": "ostro", "cache_ttl": "0s"}),
        ),
        jwt,
    )
    .unwrap();
    assert_eq!(
        value(&p, "stripe/live").await.unwrap(),
        b"sk_live_canary_0001"
    );
    assert_eq!(
        value(&p, "stripe/live").await.unwrap(),
        b"sk_live_canary_0001"
    );
    assert_eq!(m.k8s_logins.load(Ordering::SeqCst), 1);
    // The 1 s lease is refreshed after 750 ms.
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert_eq!(
        value(&p, "stripe/live").await.unwrap(),
        b"sk_live_canary_0001"
    );
    assert_eq!(m.k8s_logins.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cache_hits_within_ttl_and_refetches_after() {
    let (m, addr) = start().await;
    let dir = tempfile::tempdir().unwrap();
    let p = token_provider(addr, dir.path(), json!({"cache_ttl": "300ms"}));
    assert_eq!(
        value(&p, "stripe/live").await.unwrap(),
        b"sk_live_canary_0001"
    );
    assert_eq!(
        value(&p, "stripe/live").await.unwrap(),
        b"sk_live_canary_0001"
    );
    assert_eq!(
        m.kv_calls.load(Ordering::SeqCst),
        1,
        "second read is a cache hit"
    );
    // A different field is a different cache key.
    value(&p, "stripe/live#pub").await.unwrap();
    assert_eq!(m.kv_calls.load(Ordering::SeqCst), 2);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        value(&p, "stripe/live").await.unwrap(),
        b"sk_live_canary_0001"
    );
    assert_eq!(
        m.kv_calls.load(Ordering::SeqCst),
        3,
        "expired entry is refetched"
    );
    // Misses are not cached.
    assert!(value(&p, "nope").await.is_none());
    assert!(value(&p, "nope").await.is_none());
    assert_eq!(m.kv_calls.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn forbidden_is_unavailable_after_one_relogin() {
    let (m, addr) = start().await;
    let dir = tempfile::tempdir().unwrap();
    let p = token_provider(addr, dir.path(), json!({"cache_ttl": "0s"}));
    let e = p.get("forbidden/x").await.unwrap_err();
    assert!(
        matches!(&e, ProviderError::Unavailable(m) if m == "permission denied"),
        "{e}"
    );
    assert_eq!(
        m.kv_calls.load(Ordering::SeqCst),
        2,
        "one retry after a new session"
    );

    // A stale token in the file: 403, the file is re-read on the retry, and a rotated
    // token makes the next read succeed.
    let tf = write(dir.path(), "token2", "hvs.revoked-token");
    let p = VaultProvider::from_config(&cfg(
        json!({"addr": format!("http://{addr}"), "token_file": tf.clone(), "cache_ttl": "0s"}),
    ))
    .unwrap();
    assert!(matches!(
        p.get("stripe/live").await,
        Err(ProviderError::Unavailable(_))
    ));
    write(dir.path(), "token2", STATIC_TOKEN);
    assert_eq!(
        value(&p, "stripe/live").await.unwrap(),
        b"sk_live_canary_0001"
    );
}

#[tokio::test]
async fn connection_failure_is_unavailable_and_chain_continues() {
    // Bind and drop to get a closed port.
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    let dir = tempfile::tempdir().unwrap();
    let p = token_provider(addr, dir.path(), json!({}));
    let e = p.get("stripe/live").await.unwrap_err();
    assert!(matches!(e, ProviderError::Unavailable(_)), "{e}");
    assert!(!e.to_string().contains(STATIC_TOKEN));

    // In a chain, the provider slots in like any other.
    let (_, live) = start().await;
    let down = Arc::new(p) as Arc<dyn SecretProvider>;
    let up = Arc::new(token_provider(live, dir.path(), json!({}))) as Arc<dyn SecretProvider>;
    let chain = ProviderChain::new(vec![down, up]);
    assert_eq!(chain.names(), ["vault", "vault"]);
    let v = chain.get("stripe/live").await.unwrap().unwrap();
    assert_eq!(v.expose(), b"sk_live_canary_0001");
}

#[tokio::test]
async fn debug_and_errors_show_no_token_or_value() {
    let (_, addr) = start().await;
    let dir = tempfile::tempdir().unwrap();
    let p = token_provider(addr, dir.path(), json!({}));
    let v = p.get("stripe/live").await.unwrap().unwrap();
    let d = format!("{p:?} {v:?} {:?}", p.cache());
    for canary in [STATIC_TOKEN, "sk_live_canary"] {
        assert!(!d.contains(canary), "{d}");
    }
    assert!(d.contains("auth: \"token\""), "{d}");
    let sid = write(dir.path(), "sid", SECRET_ID);
    let p = VaultProvider::from_config(&cfg(json!({
        "addr": format!("http://{addr}"), "auth": "approle",
        "role_id": ROLE_ID, "secret_id_file": sid
    })))
    .unwrap();
    p.get("stripe/live").await.unwrap().unwrap();
    let d = format!("{p:?}");
    for canary in [SECRET_ID, APPROLE_TOKEN, "sk_live_canary"] {
        assert!(!d.contains(canary), "{d}");
    }
}

/// Optional real leg against a Vault dev server (`vault server -dev`).
#[tokio::test]
async fn real_vault_dev_server() {
    let (Ok(addr), Ok(token)) = (
        std::env::var("OSTRO_TEST_VAULT_ADDR"),
        std::env::var("OSTRO_TEST_VAULT_TOKEN"),
    ) else {
        println!(
            "SKIP real_vault_dev_server: OSTRO_TEST_VAULT_ADDR / OSTRO_TEST_VAULT_TOKEN unset (human_needed)"
        );
        return;
    };
    let settings = vaultrs::client::VaultClientSettingsBuilder::default()
        .address(&addr)
        .token(&token)
        .build()
        .unwrap();
    let client = vaultrs::client::VaultClient::new(settings).unwrap();
    let path = format!("ostro-test/{}", std::process::id());
    let mut data = HashMap::new();
    data.insert("value", "real-vault-canary-value");
    data.insert("other", "real-vault-other-field");
    vaultrs::kv2::set(&client, "secret", &path, &data)
        .await
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let tf = write(dir.path(), "token", &token);
    let p = VaultProvider::from_config(&cfg(json!({"addr": addr, "token_file": tf}))).unwrap();
    assert_eq!(value(&p, &path).await.unwrap(), b"real-vault-canary-value");
    assert_eq!(
        value(&p, &format!("{path}#other")).await.unwrap(),
        b"real-vault-other-field"
    );
    assert!(value(&p, "ostro-test/definitely-missing").await.is_none());
    vaultrs::kv2::delete_metadata(&client, "secret", &path)
        .await
        .unwrap();
}
