//! Phase 11 CLI harness (plan 11-14): a spawned `ostro dev --ephemeral` for the rerun /
//! DLQ / limits / search verbs, an in-process API server over the `ostro_api::fake`
//! fakes for the bulk verbs, and snapshot masking helpers.
//!
//! Include it next to the shared harness (it uses `crate::common`):
//!
//! ```ignore
//! mod common;
//! #[path = "common/p11.rs"]
//! mod p11;
//! ```
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::Request;
use axum::middleware::Next;
use ostro_api::fake::{FakeControlPlane, FakeOps};
use ostro_api::phase11::OpsApi;
use ostro_api::{ApiConfig, ApiExtensions};
use serde_json::Value;

use crate::common::*;

/// One spawned dev server (P=4) plus a work dir for guest files.
pub struct P11Dev {
    pub srv: Spawned,
    pub addr: SocketAddr,
    pub work: tempfile::TempDir,
}

impl P11Dev {
    pub async fn start() -> Self {
        let work = tempfile::tempdir().expect("tempdir");
        let addr = free_addr();
        let a = addr.to_string();
        let mut cmd = dev(work.path(), &["--ephemeral", "--listen", &a]);
        cmd.env("OSTRO_NODE__PARTITIONS", "4")
            .env("OSTRO_LOG__LEVEL", "warn");
        let srv = Spawned::spawn(cmd);
        srv.wait_readyz(&http(), addr).await;
        Self { srv, addr, work }
    }

    pub fn cli(&self, args: &[&str]) -> (i32, String, String) {
        run_cli(self.addr, args)
    }

    pub fn json(&self, args: &[&str]) -> (i32, Value, String) {
        cli_json(self.addr, args)
    }

    /// `json` asserting exit 0.
    pub fn ok(&self, args: &[&str]) -> Value {
        let (code, body, err) = self.json(args);
        assert_eq!(code, 0, "{args:?}: {body}\n{err}");
        body
    }

    /// `ostro deploy <guest> --activate`.
    pub fn deploy(&self, guest: &str) -> Value {
        let f = write_guest(self.work.path(), guest);
        self.ok(&["deploy", f.to_str().unwrap(), "--activate"])
    }

    /// `ostro run <workflow> --input <input> [--tag t]... --wait --json`: the run id and
    /// the final status (exit 0 when completed, 3 otherwise).
    pub fn run_wait(&self, workflow: &str, input: &str, tags: &[&str]) -> (String, String) {
        let mut args = vec!["run", workflow, "--input", input, "--wait"];
        for t in tags {
            args.extend(["--tag", t]);
        }
        let (code, b, err) = self.json(&args);
        let status = b["result"]["status"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let want = if status == "completed" { 0 } else { 3 };
        assert_eq!(code, want, "{args:?}: {b}\n{err}");
        (
            b["start"]["run_id"].as_str().expect("run_id").to_owned(),
            status,
        )
    }

    /// Poll `args` (a `--json` verb) until `f` holds, for at most [`T`].
    pub fn until(&self, args: &[&str], f: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + T;
        loop {
            let v = self.ok(args);
            if f(&v) {
                return v;
            }
            assert!(Instant::now() < deadline, "{args:?} never matched: {v}");
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    pub fn stop(mut self) {
        self.srv.stop_clean("INT");
    }
}

/// The `run_id` values of a `runs` / `entries` array.
pub fn ids(v: &Value, key: &str) -> Vec<String> {
    v[key]
        .as_array()
        .unwrap_or_else(|| panic!("no {key} in {v}"))
        .iter()
        .map(|r| r["run_id"].as_str().expect("run_id").to_owned())
        .collect()
}

/// Replace every RFC 3339 whole-second UTC timestamp (`2026-11-22T10:00:00Z`) with
/// `[time]` (padded to the same width so table columns stay aligned).
pub fn mask_times(s: &str) -> String {
    const PAT: &[u8] = b"dddd-dd-ddTdd:dd:ddZ";
    let b = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < b.len() {
        let hit = b.len() - i >= PAT.len()
            && PAT.iter().zip(&b[i..]).all(|(p, c)| match p {
                b'd' => c.is_ascii_digit(),
                p => p == c,
            });
        if hit {
            out.push_str(&format!("{:<width$}", "[time]", width = PAT.len()));
            i += PAT.len();
        } else {
            let ch = s[i..].chars().next().expect("char");
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
}

/// Replace each `(needle, label)` (label padded to the needle's width so tables stay
/// aligned).
pub fn mask(s: &str, subst: &[(&str, &str)]) -> String {
    let mut out = s.to_owned();
    for (needle, label) in subst {
        if !needle.is_empty() {
            let padded = format!("{label:<width$}", width = needle.len());
            out = out.replace(needle, &padded);
        }
    }
    out
}

/// In-process API server: `ostro_api::router_with(FakeControlPlane, ApiConfig, ext)` with
/// [`FakeOps`] in `ext.ops`, on its own thread and runtime (the CLI processes are spawned
/// synchronously by the tests). Every `GET /v1/bulk/{job}` first advances that job by
/// one op (`done += 1`), so `ostro bulk status --watch` sees progress and a terminal
/// state.
pub struct FakeApi {
    pub addr: SocketAddr,
    pub ops: Arc<FakeOps>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl FakeApi {
    pub fn start() -> Self {
        let ops = FakeOps::new();
        let mut ext = ApiExtensions::default();
        ops.install(&mut ext);
        let advancing = Arc::clone(&ops);
        let router =
            ostro_api::router_with(Arc::new(FakeControlPlane::new()), ApiConfig::default(), ext)
                .layer(axum::middleware::from_fn(
                    move |req: Request, next: Next| {
                        let ops = Arc::clone(&advancing);
                        async move {
                            if req.method() == axum::http::Method::GET
                                && let Some(job) = req.uri().path().strip_prefix("/v1/bulk/")
                                && !job.contains('/')
                                && let Ok(j) = ops.get_bulk(job.to_owned()).await
                            {
                                ops.set_bulk_progress(&j.job_id, j.done + 1, j.skipped);
                            }
                            next.run(req).await
                        }
                    },
                ));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let addr = listener.local_addr().expect("addr");
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(async move {
                let l = tokio::net::TcpListener::from_std(listener).expect("listener");
                axum::serve(l, router)
                    .with_graceful_shutdown(async {
                        let _ = rx.await;
                    })
                    .await
                    .expect("serve");
            });
        });
        Self {
            addr,
            ops,
            shutdown: Some(tx),
            thread: Some(thread),
        }
    }

    pub fn cli(&self, args: &[&str]) -> (i32, String, String) {
        run_cli(self.addr, args)
    }

    pub fn json(&self, args: &[&str]) -> (i32, Value, String) {
        cli_json(self.addr, args)
    }
}

impl Drop for FakeApi {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

// ---------------------------------------------------------------------------------
// Phase 11 gate (plan 11-18): restartable nodes, REST helpers and a UI client for the
// spawned-binary suites `ops_e2e`, `ops_kill` and `ui_e2e`.
// ---------------------------------------------------------------------------------

use std::path::{Path, PathBuf};

use reqwest::StatusCode;
use reqwest::header::HeaderMap;

/// How a [`P11Node`] runs.
#[derive(Clone, Debug)]
pub enum Profile {
    /// `ostro dev --data-dir <work>/data` (UI on: loopback listener).
    Dev,
    /// `ostro server --config <work>/ostro.toml --data-dir <work>/data` with this TOML
    /// body (rewritten at every boot, so a restart can change it).
    Server(String),
}

/// One spawned node on a persistent data dir under `work`, restartable on the same
/// address (kill tests, config changes between boots).
pub struct P11Node {
    pub srv: Option<Spawned>,
    pub addr: SocketAddr,
    pub work: tempfile::TempDir,
    pub api: Api,
}

impl P11Node {
    /// Boot a fresh node (P=4, log level warn) with extra environment `env`.
    pub async fn start(profile: &Profile, env: &[(&str, &str)]) -> Self {
        let work = tempfile::tempdir().expect("tempdir");
        let addr = free_addr();
        let mut n = Self {
            srv: None,
            addr,
            work,
            api: Api::new(addr),
        };
        n.boot(profile, env).await;
        n
    }

    pub fn data_dir(&self) -> PathBuf {
        self.work.path().join("data")
    }

    /// Spawn the binary on the same data dir and address; waits for `/readyz`.
    pub async fn boot(&mut self, profile: &Profile, env: &[(&str, &str)]) {
        assert!(self.srv.is_none(), "node already running");
        let a = self.addr.to_string();
        let data = self.data_dir();
        let data = data.to_str().expect("utf-8 data dir");
        let mut cmd = match profile {
            Profile::Dev => dev(self.work.path(), &["--data-dir", data, "--listen", &a]),
            Profile::Server(toml) => {
                let cfg = self.work.path().join("ostro.toml");
                std::fs::write(&cfg, toml).expect("write ostro.toml");
                let cfg = cfg.to_str().expect("utf-8 config path").to_owned();
                server(
                    self.work.path(),
                    &["--config", &cfg, "--data-dir", data, "--listen", &a],
                )
            }
        };
        cmd.env("OSTRO_NODE__PARTITIONS", "4")
            .env("OSTRO_LOG__LEVEL", "warn");
        for (k, v) in env {
            cmd.env(k, v);
        }
        let srv = Spawned::spawn(cmd);
        srv.wait_readyz(&http(), self.addr).await;
        self.srv = Some(srv);
    }

    /// `kill -9` (no shutdown path runs).
    pub fn kill9(&mut self) {
        let mut srv = self.srv.take().expect("node running");
        srv.signal("KILL");
        srv.wait_exit(T);
    }

    /// SIGINT and a clean exit.
    pub fn stop(&mut self) {
        let mut srv = self.srv.take().expect("node running");
        srv.stop_clean("INT");
    }

    /// The running process's stderr.
    pub fn stderr(&self) -> String {
        self.srv.as_ref().map(Spawned::stderr).unwrap_or_default()
    }

    pub fn cli(&self, args: &[&str]) -> (i32, String, String) {
        run_cli(self.addr, args)
    }

    pub fn json(&self, args: &[&str]) -> (i32, Value, String) {
        cli_json(self.addr, args)
    }

    /// `json` asserting exit 0.
    pub fn ok(&self, args: &[&str]) -> Value {
        let (code, body, err) = self.json(args);
        assert_eq!(code, 0, "{args:?}: {body}\n{err}");
        body
    }

    /// `ostro deploy <guest> --activate` of the named guest of `guests/`.
    pub fn deploy(&self, guest: &str) -> Value {
        let f = write_guest(self.work.path(), guest);
        self.ok(&["deploy", f.to_str().unwrap(), "--activate"])
    }

    /// `ostro deploy <file> --activate` of a prebuilt component.
    pub fn deploy_file(&self, file: &Path) -> Value {
        self.ok(&["deploy", file.to_str().unwrap(), "--activate"])
    }

    /// `ostro run <workflow> --input <input> [--tag t]... --wait --json`: (run id, status).
    pub fn run_wait(&self, workflow: &str, input: &str, tags: &[&str]) -> (String, String) {
        let mut args = vec!["run", workflow, "--input", input, "--wait"];
        for t in tags {
            args.extend(["--tag", t]);
        }
        let (code, b, err) = self.json(&args);
        let status = b["result"]["status"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let want = if status == "completed" { 0 } else { 3 };
        assert_eq!(code, want, "{args:?}: {b}\n{err}");
        (
            b["start"]["run_id"].as_str().expect("run_id").to_owned(),
            status,
        )
    }

    /// `ostro run <workflow> --input <input> [--tag t]...` without waiting: the run id.
    pub fn run_nowait(&self, workflow: &str, input: &str, tags: &[&str]) -> String {
        let mut args = vec!["run", workflow, "--input", input];
        for t in tags {
            args.extend(["--tag", t]);
        }
        let b = self.ok(&args);
        b["run_id"]
            .as_str()
            .or_else(|| b["start"]["run_id"].as_str())
            .unwrap_or_else(|| panic!("no run_id in {b}"))
            .to_owned()
    }

    /// REST call against this node.
    pub async fn rest(&self, method: &str, path: &str, body: Option<Value>) -> (StatusCode, Value) {
        rest(self.addr, method, path, body).await
    }
}

/// `method http://addr<path> [json]` -> (status, JSON body, or the text as a JSON string,
/// or Null when empty).
pub async fn rest(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let m = reqwest::Method::from_bytes(method.as_bytes()).expect("method");
    let mut req = http().request(m, format!("http://{addr}{path}"));
    if let Some(b) = body {
        req = req.json(&b);
    }
    let r = req
        .send()
        .await
        .unwrap_or_else(|e| panic!("{method} {path}: {e}"));
    let st = r.status();
    let b = r.bytes().await.expect("body");
    let v = if b.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&b)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&b).into_owned()))
    };
    (st, v)
}

/// Poll `f` every 100 ms until it returns `Some`, for at most `limit`.
pub async fn poll<T, F, Fut>(what: &str, limit: Duration, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + limit;
    loop {
        if let Some(v) = f().await {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Every run (summary JSON) of `GET /v1/runs?<query>` across all pages.
pub async fn list_all(addr: SocketAddr, query: &str) -> Vec<Value> {
    let mut out = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut path = format!("/v1/runs?{query}&limit=200");
        if let Some(t) = &token {
            path.push_str(&format!("&page_token={t}"));
        }
        let (st, p) = rest(addr, "GET", &path, None).await;
        assert_eq!(st, StatusCode::OK, "{path}: {p}");
        out.extend(p["runs"].as_array().expect("runs").iter().cloned());
        match p["next_page_token"].as_str() {
            Some(t) => token = Some(t.to_owned()),
            None => return out,
        }
    }
}

/// The exact D-P11-15 Content-Security-Policy every `/ui` response carries.
pub const UI_CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; \
img-src 'self' data:; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; \
base-uri 'none'";

/// One `/ui` response.
#[derive(Debug)]
pub struct UiResp {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: String,
}

impl UiResp {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    /// 200 with the D-P11-15 security headers.
    pub fn assert_page(&self, what: &str) {
        assert_eq!(self.status, StatusCode::OK, "{what}: {}", self.body);
        self.assert_headers(what);
    }

    /// The D-P11-15 security headers (on every `/ui` response, errors included).
    pub fn assert_headers(&self, what: &str) {
        assert_eq!(
            self.header("content-security-policy"),
            Some(UI_CSP),
            "{what}"
        );
        assert_eq!(
            self.header("x-content-type-options"),
            Some("nosniff"),
            "{what}"
        );
        assert_eq!(self.header("x-frame-options"), Some("DENY"), "{what}");
        assert_eq!(
            self.header("referrer-policy"),
            Some("no-referrer"),
            "{what}"
        );
    }
}

/// A browser-like `/ui` client: no redirects followed, the `ostro_csrf` cookie captured
/// from the first page and sent back, and mutations as htmx (`HX-Request` +
/// `X-Ostro-Csrf`), plain forms (`csrf` field) or cross-site posts.
pub struct UiClient {
    pub http: reqwest::Client,
    pub base: String,
    pub token: String,
}

/// How a mutation is sent.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Via {
    /// htmx: `HX-Request: true` + `X-Ostro-Csrf` header, `Sec-Fetch-Site: same-origin`.
    Htmx,
    /// No-JS form post: `csrf` field, `Sec-Fetch-Site: same-origin`.
    Form,
    /// A cross-site form post with a valid token.
    CrossSite,
}

fn form_enc(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

impl UiClient {
    /// Connect to `base` (`http://addr` or `https://addr`; `ca_pem` trusts a TLS CA) and
    /// take the CSRF cookie from `GET /ui/`.
    pub async fn connect(base: &str, ca_pem: Option<&[u8]>) -> Self {
        let mut b = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(20));
        if let Some(pem) = ca_pem {
            b = b.add_root_certificate(reqwest::Certificate::from_pem(pem).expect("CA pem"));
        }
        let mut c = Self {
            http: b.build().expect("client"),
            base: base.to_owned(),
            token: String::new(),
        };
        let r = c.raw_get("/ui/", false).await;
        assert_eq!(r.status, StatusCode::OK, "/ui/: {}", r.body);
        let cookie = r
            .headers
            .get_all("set-cookie")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .find_map(|v| v.strip_prefix("ostro_csrf="))
            .expect("ostro_csrf cookie on the first page")
            .to_owned();
        assert!(
            cookie.contains("HttpOnly") && cookie.contains("SameSite=Strict"),
            "{cookie}"
        );
        c.token = cookie.split(';').next().expect("token").to_owned();
        assert_eq!(c.token.len(), 64, "{cookie}");
        c
    }

    async fn into_resp(r: reqwest::Response) -> UiResp {
        let status = r.status();
        let headers = r.headers().clone();
        let body = r.text().await.expect("body");
        UiResp {
            status,
            headers,
            body,
        }
    }

    async fn raw_get(&self, path: &str, cookie: bool) -> UiResp {
        let mut req = self.http.get(format!("{}{path}", self.base));
        if cookie {
            req = req.header("cookie", format!("ostro_csrf={}", self.token));
        }
        Self::into_resp(
            req.send()
                .await
                .unwrap_or_else(|e| panic!("GET {path}: {e}")),
        )
        .await
    }

    /// `GET path` with the cookie.
    pub async fn get(&self, path: &str) -> UiResp {
        self.raw_get(path, true).await
    }

    /// `GET path` with a foreign `Host` header.
    pub async fn get_with_host(&self, path: &str, host: &str) -> UiResp {
        let req = self
            .http
            .get(format!("{}{path}", self.base))
            .header("host", host)
            .header("cookie", format!("ostro_csrf={}", self.token));
        Self::into_resp(req.send().await.expect("GET")).await
    }

    /// htmx fragment `GET` (`HX-Request: true`).
    pub async fn get_fragment(&self, path: &str) -> UiResp {
        let req = self
            .http
            .get(format!("{}{path}", self.base))
            .header("hx-request", "true")
            .header("cookie", format!("ostro_csrf={}", self.token));
        Self::into_resp(req.send().await.expect("GET")).await
    }

    /// A mutating `POST path` with form `fields`.
    pub async fn post(&self, via: Via, path: &str, fields: &[(&str, &str)]) -> UiResp {
        let mut all: Vec<(&str, &str)> = fields.to_vec();
        if via != Via::Htmx {
            all.push(("csrf", self.token.as_str()));
        }
        let body = all
            .iter()
            .map(|(k, v)| format!("{}={}", form_enc(k), form_enc(v)))
            .collect::<Vec<_>>()
            .join("&");
        let mut req = self
            .http
            .post(format!("{}{path}", self.base))
            .header("cookie", format!("ostro_csrf={}", self.token))
            .header("content-type", "application/x-www-form-urlencoded")
            .header(
                "sec-fetch-site",
                if via == Via::CrossSite {
                    "cross-site"
                } else {
                    "same-origin"
                },
            );
        if via == Via::Htmx {
            req = req
                .header("hx-request", "true")
                .header("x-ostro-csrf", &self.token);
        }
        Self::into_resp(
            req.body(body)
                .send()
                .await
                .unwrap_or_else(|e| panic!("POST {path}: {e}")),
        )
        .await
    }
}
