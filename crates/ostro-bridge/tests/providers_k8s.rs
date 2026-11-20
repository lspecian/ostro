//! K8sProvider against an axum mock of the Kubernetes API server (06-13).
//!
//! The mock serves `GET /api/v1/namespaces/{ns}/secrets/{name}` (a `Secret` with base64
//! `data`, a 404 `Status` for a missing secret, a 403 `Status` for `forbidden`) and checks
//! the bearer token from a generated kubeconfig pointing at `http://127.0.0.1:<port>`.
#![cfg(feature = "k8s")]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::{Path as UrlPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use ostro_bridge::secrets::ProviderChain;
use ostro_bridge::secrets::config::K8sConfig;
use ostro_bridge::secrets::providers::k8s::K8sProvider;
use ostro_bridge::secrets::providers::{ProviderError, SecretProvider};
use secrecy::ExposeSecret;
use serde_json::{Value, json};

const TOKEN: &str = "k8s-static-canary-token";
const LIVE: &[u8] = b"sk_live_k8s_canary_01";
const DB: &[u8] = b"db-pass\x00\xffbinary";

#[derive(Default)]
struct Mock {
    gets: AtomicUsize,
}

fn status(code: u16, reason: &str, msg: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::from_u16(code).unwrap(),
        Json(
            json!({"kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Failure",
                    "message": msg, "reason": reason, "code": code}),
        ),
    )
}

async fn secret(
    State(m): State<Arc<Mock>>,
    UrlPath((ns, name)): UrlPath<(String, String)>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    m.gets.fetch_add(1, Ordering::SeqCst);
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if auth != format!("Bearer {TOKEN}") {
        return status(401, "Unauthorized", "Unauthorized");
    }
    let data = match (ns.as_str(), name.as_str()) {
        ("ostro", "stripe") => json!({"live-key": B64.encode(LIVE), "pub": B64.encode(b"pk_live")}),
        ("ostro", "db") => json!({"password": B64.encode(DB)}),
        ("other", "stripe") => json!({"live-key": B64.encode(b"other-namespace-value")}),
        (_, "forbidden") => {
            return status(403, "Forbidden", "secrets \"forbidden\" is forbidden");
        }
        _ => return status(404, "NotFound", &format!("secrets \"{name}\" not found")),
    };
    (
        StatusCode::OK,
        Json(json!({
            "apiVersion": "v1", "kind": "Secret", "type": "Opaque",
            "metadata": {"name": name, "namespace": ns, "annotations": {
                "kubectl.kubernetes.io/last-applied-configuration": "{\"data\":{}}"}},
            "data": data
        })),
    )
}

async fn start() -> (Arc<Mock>, SocketAddr) {
    let m = Arc::new(Mock::default());
    let app = Router::new()
        .route("/api/v1/namespaces/{ns}/secrets/{name}", get(secret))
        .with_state(m.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    (m, addr)
}

fn kubeconfig(dir: &Path, addr: SocketAddr, token: &str) -> PathBuf {
    let p = dir.join("kubeconfig");
    std::fs::write(
        &p,
        format!(
            "apiVersion: v1
kind: Config
clusters:
- name: mock
  cluster:
    server: http://{addr}
contexts:
- name: mock
  context:
    cluster: mock
    user: mock
    namespace: ostro
current-context: mock
users:
- name: mock
  user:
    token: {token}
"
        ),
    )
    .unwrap();
    p
}

async fn provider(kc: PathBuf, namespace: Option<&str>, ttl: Duration) -> K8sProvider {
    K8sProvider::from_config(&K8sConfig {
        namespace: namespace.map(str::to_owned),
        kubeconfig: Some(kc),
        cache_ttl: ttl,
    })
    .await
    .unwrap()
}

async fn value(p: &K8sProvider, name: &str) -> Option<Vec<u8>> {
    p.get(name)
        .await
        .unwrap()
        .map(|v| v.expose_secret().to_vec())
}

#[tokio::test]
async fn reads_and_missing() {
    let (m, addr) = start().await;
    let dir = tempfile::tempdir().unwrap();
    let p = provider(kubeconfig(dir.path(), addr, TOKEN), None, Duration::ZERO).await;
    assert_eq!(
        p.namespace(),
        "ostro",
        "namespace from the kubeconfig context"
    );
    assert_eq!(value(&p, "stripe/live-key").await.unwrap(), LIVE);
    assert_eq!(value(&p, "stripe#pub").await.unwrap(), b"pk_live");
    assert_eq!(value(&p, "db/password").await.unwrap(), DB, "raw bytes");
    assert!(
        value(&p, "stripe/missing-key").await.is_none(),
        "missing key"
    );
    assert!(value(&p, "nosuch/key").await.is_none(), "missing secret");
    assert_eq!(m.gets.load(Ordering::SeqCst), 5);
    // Names that cannot be secret keys never reach the API; reserved names are Invalid.
    assert!(value(&p, "plain-name").await.is_none());
    assert!(matches!(
        p.get("__ostro/x/y").await,
        Err(ProviderError::Invalid(_))
    ));
    assert_eq!(m.gets.load(Ordering::SeqCst), 5);

    let p = provider(
        kubeconfig(dir.path(), addr, TOKEN),
        Some("other"),
        Duration::ZERO,
    )
    .await;
    assert_eq!(
        value(&p, "stripe/live-key").await.unwrap(),
        b"other-namespace-value"
    );
}

#[tokio::test]
async fn forbidden_and_unauthorized_are_unavailable() {
    let (_, addr) = start().await;
    let dir = tempfile::tempdir().unwrap();
    let p = provider(kubeconfig(dir.path(), addr, TOKEN), None, Duration::ZERO).await;
    let e = p.get("forbidden/key").await.unwrap_err();
    assert!(
        matches!(&e, ProviderError::Unavailable(m) if m == "permission denied"),
        "{e}"
    );

    let p = provider(
        kubeconfig(dir.path(), addr, "wrong-token-canary"),
        None,
        Duration::ZERO,
    )
    .await;
    let e = p.get("stripe/live-key").await.unwrap_err();
    assert!(matches!(e, ProviderError::Unavailable(_)), "{e}");
    assert!(!e.to_string().contains("canary"), "{e}");
}

#[tokio::test]
async fn cache_hit_within_ttl() {
    let (m, addr) = start().await;
    let dir = tempfile::tempdir().unwrap();
    let p = provider(
        kubeconfig(dir.path(), addr, TOKEN),
        None,
        Duration::from_millis(300),
    )
    .await;
    assert_eq!(value(&p, "stripe/live-key").await.unwrap(), LIVE);
    assert_eq!(value(&p, "stripe/live-key").await.unwrap(), LIVE);
    assert_eq!(
        m.gets.load(Ordering::SeqCst),
        1,
        "second read is a cache hit"
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(value(&p, "stripe/live-key").await.unwrap(), LIVE);
    assert_eq!(m.gets.load(Ordering::SeqCst), 2, "refetched after expiry");
    // Misses are not cached.
    assert!(value(&p, "nosuch/key").await.is_none());
    assert!(value(&p, "nosuch/key").await.is_none());
    assert_eq!(m.gets.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn unreachable_server_and_bad_config() {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    let dir = tempfile::tempdir().unwrap();
    let p = provider(kubeconfig(dir.path(), addr, TOKEN), None, Duration::ZERO).await;
    let e = p.get("stripe/live-key").await.unwrap_err();
    assert!(matches!(e, ProviderError::Unavailable(_)), "{e}");
    assert!(!e.to_string().contains(TOKEN));

    // A broken kubeconfig: Unavailable, without quoting the file (which holds the token).
    let bad = dir.path().join("broken");
    std::fs::write(&bad, format!("users: [ token: {TOKEN}\n")).unwrap();
    let e = K8sProvider::from_config(&K8sConfig {
        kubeconfig: Some(bad),
        ..K8sConfig::default()
    })
    .await
    .unwrap_err();
    assert!(matches!(e, ProviderError::Unavailable(_)), "{e}");
    assert!(!e.to_string().contains(TOKEN), "{e}");

    // An invalid namespace is refused.
    let e = K8sProvider::from_config(&K8sConfig {
        namespace: Some("Bad_NS".into()),
        kubeconfig: Some(kubeconfig(dir.path(), addr, TOKEN)),
        ..K8sConfig::default()
    })
    .await
    .unwrap_err();
    assert!(matches!(e, ProviderError::Invalid(_)), "{e}");
}

#[tokio::test]
async fn slots_into_chain_and_debug_is_clean() {
    let (_, addr) = start().await;
    let dir = tempfile::tempdir().unwrap();
    let p = provider(
        kubeconfig(dir.path(), addr, TOKEN),
        None,
        Duration::from_secs(30),
    )
    .await;
    let v = p.get("stripe/live-key").await.unwrap().unwrap();
    let d = format!("{p:?} {v:?} {:?}", p.cache());
    assert!(!d.contains(TOKEN) && !d.contains("sk_live"), "{d}");
    assert!(d.contains("namespace: \"ostro\""), "{d}");

    let chain = ProviderChain::new(vec![Arc::new(p) as Arc<dyn SecretProvider>]);
    assert_eq!(chain.names(), ["k8s"]);
    assert_eq!(
        chain
            .get("stripe/live-key")
            .await
            .unwrap()
            .unwrap()
            .expose(),
        LIVE
    );
    assert!(chain.get("nosuch/key").await.unwrap().is_none());
}
