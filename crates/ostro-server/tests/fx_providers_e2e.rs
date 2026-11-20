//! ROADMAP Phase 6 SC5, secret providers end to end (plan 06-19).
//!
//! One server with `[secrets] providers = ["sealed", "env", "file", "vault", "k8s"]`:
//! - sealed: set over REST;
//! - env: `OSTRO_SECRET_ENV_KEY`, set before the server starts (env mutex);
//! - file: `file_dir` holding `file-key`;
//! - Vault: an axum mock of the KV v2 API with token auth (`[secrets.vault]`, mount `kv`);
//! - Kubernetes: an axum mock of the API server plus a temp kubeconfig.
//!
//! For each provider fx-http POSTs to the recording mock's `/echo` with
//! `X-Api-Key: {{secret:<name>}}`; the provider must receive the provider's value and the
//! guest must see `[REDACTED]`. Provider order: a name present in env and file resolves from
//! whichever is listed first (checked in both orders).
//!
//! Every mock (echo, Vault, Kubernetes) runs through `spawn_quiet` (W6).
//!
//! Plan note: the plan's Vault name `kv/app#token` is not a valid placeholder name (the
//! template grammar has no `#`), so the Vault secret is read with the default field:
//! `{{secret:app/token}}` → mount `kv`, path `app/token`, field `value`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::{Path as UrlPath, Request, State};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::routing::get;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use common::fx::{configure, deploy_fx, wait_terminal};
use common::{BridgeTestOpts, TestServer, start_server_with_bridge, tmpfs_dir};
use ostro_testkit::canary::{Quiet, spawn_quiet};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

/// Serialises the env mutations of this file.
static ENV: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const RUN_LIMIT: Duration = Duration::from_secs(60);

const SEALED_VAL: &str = "sealed-provider-value-7a1c";
const ENV_VAL: &str = "env-provider-value-2b9d";
const FILE_VAL: &str = "file-provider-value-4e3f";
const VAULT_VAL: &str = "vault-provider-value-8c5a";
const K8S_VAL: &str = "k8s-provider-value-1d6b";
const DUP_ENV: &str = "dup-from-env-0f0f0f";
const DUP_FILE: &str = "dup-from-file-a5a5a5";

const VAULT_TOKEN: &str = "hvs.fx-providers-e2e-token";
const K8S_TOKEN: &str = "k8s-fx-providers-e2e-token";

/// Aborts the server task when dropped (inside a `Quiet`).
struct Served<S> {
    addr: SocketAddr,
    state: Arc<S>,
    task: JoinHandle<()>,
}

impl<S> Drop for Served<S> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve<S: Send + Sync + 'static>(app: Router, state: Arc<S>) -> Served<S> {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock");
    let addr = l.local_addr().expect("addr");
    let task = tokio::spawn(async move {
        let _ = axum::serve(l, app).await;
    });
    Served { addr, state, task }
}

// ---- recording echo -------------------------------------------------------------

#[derive(Default)]
struct Echo {
    api_keys: Mutex<Vec<Vec<u8>>>,
}

async fn echo(State(st): State<Arc<Echo>>, req: Request) -> Response<Body> {
    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_owned(),
                String::from_utf8_lossy(v.as_bytes()).into_owned(),
            )
        })
        .collect();
    if let Some(v) = req.headers().get("x-api-key") {
        st.api_keys.lock().unwrap().push(v.as_bytes().to_vec());
    }
    Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(Body::from(json!({ "headers": headers }).to_string()))
        .expect("response")
}

async fn start_echo() -> Served<Echo> {
    let st = Arc::new(Echo::default());
    let app = Router::new().fallback(echo).with_state(st.clone());
    serve(app, st).await
}

// ---- Vault KV v2 (shape from crates/ostro-bridge/tests/providers_vault.rs) -------------

async fn vault_kv(
    UrlPath((mount, path)): UrlPath<(String, String)>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    let token = headers
        .get("x-vault-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if token != VAULT_TOKEN {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"errors": ["permission denied"]})),
        );
    }
    if mount != "kv" || path != "app/token" {
        return (StatusCode::NOT_FOUND, Json(json!({"errors": []})));
    }
    let metadata = json!({"created_time": "2026-01-01T00:00:00Z", "deletion_time": "",
                          "custom_metadata": null, "destroyed": false, "version": 1});
    (
        StatusCode::OK,
        Json(json!({
            "request_id": "req-1", "lease_id": "", "renewable": false, "lease_duration": 0,
            "data": {"data": {"value": VAULT_VAL}, "metadata": metadata},
            "auth": null, "wrap_info": null, "warnings": null
        })),
    )
}

async fn start_vault() -> Served<()> {
    let app = Router::new().route("/v1/{mount}/data/{*path}", get(vault_kv));
    serve(app, Arc::new(())).await
}

// ---- Kubernetes API (shape from crates/ostro-bridge/tests/providers_k8s.rs) ------------

fn k8s_status(code: u16, reason: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::from_u16(code).expect("code"),
        Json(
            json!({"kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Failure",
                    "message": reason, "reason": reason, "code": code}),
        ),
    )
}

async fn k8s_secret(
    UrlPath((ns, name)): UrlPath<(String, String)>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if auth != format!("Bearer {K8S_TOKEN}") {
        return k8s_status(401, "Unauthorized");
    }
    if ns != "ostro" || name != "app-secret" {
        return k8s_status(404, "NotFound");
    }
    (
        StatusCode::OK,
        Json(json!({
            "apiVersion": "v1", "kind": "Secret", "type": "Opaque",
            "metadata": {"name": name, "namespace": ns},
            "data": {"api-key": STANDARD.encode(K8S_VAL)}
        })),
    )
}

async fn start_k8s() -> Served<()> {
    let app = Router::new().route("/api/v1/namespaces/{ns}/secrets/{name}", get(k8s_secret));
    serve(app, Arc::new(())).await
}

fn kubeconfig(dir: &Path, addr: SocketAddr) -> String {
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
    token: {K8S_TOKEN}
"
        ),
    )
    .expect("kubeconfig");
    p.display().to_string()
}

// ---- helpers ------------------------------------------------------------------------

/// Run fx-http once: POST `<echo>/echo` with `X-Api-Key: {{secret:<name>}}`. Returns the
/// value the provider received and asserts the guest saw `[REDACTED]`.
async fn resolve_through_guest(s: &TestServer, echo: &Quiet<Served<Echo>>, name: &str) -> Vec<u8> {
    let before = echo.state.api_keys.lock().unwrap().len();
    deploy_fx(s, "fx-http").await;
    let run = s
        .start_rest(
            "fx-http",
            json!({ "input": { "requests": [{
                "method": "POST",
                "url": format!("http://{}/echo", echo.addr),
                "headers": [["X-Api-Key", format!("{{{{secret:{name}}}}}")]],
            }]}}),
        )
        .await;
    let info = wait_terminal(s, &run, RUN_LIMIT).await;
    assert_eq!(
        serde_json::to_value(info.status).expect("status"),
        "completed",
        "{name}: {info:?}"
    );
    let out: Value =
        serde_json::from_slice(&s.result_bytes(&run).await.expect("result")).expect("json");
    let item = &out["results"][0];
    assert_eq!(item["status"], 200, "{name}: {out}");
    let body = STANDARD
        .decode(item["body_b64"].as_str().expect("body_b64"))
        .expect("base64");
    let echoed: Value = serde_json::from_slice(&body).expect("echo json");
    let seen = echoed["headers"]
        .as_array()
        .expect("headers")
        .iter()
        .find(|kv| kv[0] == "x-api-key")
        .and_then(|kv| kv[1].as_str())
        .expect("x-api-key echoed")
        .to_owned();
    assert_eq!(
        seen, "[REDACTED]",
        "{name}: the guest sees the echo scrubbed"
    );
    let keys = echo.state.api_keys.lock().unwrap().clone();
    assert_eq!(keys.len(), before + 1, "{name}: exactly one request");
    keys[before].clone()
}

fn set_env(k: &str, v: &str) {
    // SAFETY: every test of this file holds ENV while the variable is set and read.
    unsafe { std::env::set_var(k, v) };
}

fn remove_env(k: &str) {
    // SAFETY: as in set_env.
    unsafe { std::env::remove_var(k) };
}

// ---- tests --------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_five_providers_resolve_through_the_chain() {
    let _env = ENV.lock().await;
    let echo = spawn_quiet(start_echo());
    let vault = spawn_quiet(start_vault());
    let k8s = spawn_quiet(start_k8s());
    let aux = tempfile::tempdir().expect("aux dir");
    let files = aux.path().join("secrets");
    std::fs::create_dir_all(&files).expect("file_dir");
    std::fs::write(files.join("file-key"), format!("{FILE_VAL}\n")).expect("file-key");
    std::fs::write(files.join("dup-key"), DUP_FILE).expect("dup-key");
    let token_file = aux.path().join("vault-token");
    std::fs::write(&token_file, format!("{VAULT_TOKEN}\n")).expect("token");
    let kc = kubeconfig(aux.path(), k8s.addr);
    set_env("OSTRO_SECRET_ENV_KEY", ENV_VAL);
    set_env("OSTRO_SECRET_DUP_KEY", DUP_ENV);

    let extra = format!(
        r#"[secrets]
providers = ["sealed", "env", "file", "vault", "k8s"]
file_dir = "{files}"

[secrets.vault]
addr = "http://{vault}"
mount = "kv"
auth = "token"
token_file = "{token}"
cache_ttl = "0s"

[secrets.k8s]
kubeconfig = "{kc}"
namespace = "ostro"
cache_ttl = "0s"
"#,
        files = files.display(),
        vault = vault.addr,
        token = token_file.display(),
    );
    let s = start_server_with_bridge(
        tmpfs_dir(),
        BridgeTestOpts {
            extra_toml: extra,
            ..BridgeTestOpts::default()
        },
    )
    .await;
    let module = deploy_fx(&s, "fx-http").await;
    let origin = format!("http://{}", echo.addr);
    let names = [
        "sealed-key",
        "env-key",
        "file-key",
        "app/token",
        "app-secret/api-key",
        "dup-key",
    ];
    let mut cfg = configure(&s).secret("sealed-key", SEALED_VAL);
    for n in names {
        cfg = cfg.grant(&module, n, &[&origin], false);
    }
    cfg.egress(&module, &[&origin]).apply().await;

    for (name, want, provider) in [
        ("sealed-key", SEALED_VAL, "sealed"),
        ("env-key", ENV_VAL, "env"),
        ("file-key", FILE_VAL, "file"),
        ("app/token", VAULT_VAL, "vault"),
        ("app-secret/api-key", K8S_VAL, "k8s"),
        ("dup-key", DUP_ENV, "env before file"),
    ] {
        let got = resolve_through_guest(&s, &echo, name).await;
        assert_eq!(
            got,
            want.as_bytes(),
            "{name} resolved from the {provider} provider"
        );
        println!("{name:<20} <- {provider}");
    }
    remove_env("OSTRO_SECRET_ENV_KEY");
    remove_env("OSTRO_SECRET_DUP_KEY");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_order_file_before_env() {
    let _env = ENV.lock().await;
    let echo = spawn_quiet(start_echo());
    let aux = tempfile::tempdir().expect("aux dir");
    std::fs::write(aux.path().join("dup-key"), DUP_FILE).expect("dup-key");
    set_env("OSTRO_SECRET_DUP_KEY", DUP_ENV);
    let extra = format!(
        "[secrets]\nproviders = [\"file\", \"env\", \"sealed\"]\nfile_dir = \"{}\"\n",
        aux.path().display()
    );
    let s = start_server_with_bridge(
        tmpfs_dir(),
        BridgeTestOpts {
            extra_toml: extra,
            ..BridgeTestOpts::default()
        },
    )
    .await;
    let module = deploy_fx(&s, "fx-http").await;
    let origin = format!("http://{}", echo.addr);
    configure(&s)
        .grant(&module, "dup-key", &[&origin], false)
        .egress(&module, &[&origin])
        .apply()
        .await;
    let got = resolve_through_guest(&s, &echo, "dup-key").await;
    assert_eq!(got, DUP_FILE.as_bytes(), "file is listed first");
    remove_env("OSTRO_SECRET_DUP_KEY");
}
