//! Spawned-binary Phase 7 harness (plan 07-16; used by the wave-7 e2e plans 07-17..07-21):
//! one real `ostro server` (self-signed TLS, cluster CA, pinned ports) plus N real
//! `ostro worker` processes joined with a `wkr_` token, and an optional `MockProvider`.
//!
//! Include it next to the shared harness (it uses `crate::common`):
//!
//! ```ignore
//! mod common;
//! #[path = "common/p7.rs"]
//! mod p7;
//! ```
//!
//! # Ports (B5)
//!
//! [`P7Cluster::start`] reserves one free loopback port for `[api] listen` and one for
//! `[cluster] listen` (bind `127.0.0.1:0`, read, release) and writes both into the server
//! config. [`P7Cluster::restart_server`] reuses the **same** ports, so workers reconnect
//! to the endpoints they already know. If the server exits before its ready line with an
//! address-in-use error it is retried up to [`BIND_ATTEMPTS`] times with a short backoff;
//! fresh ports are reserved only on the very first start, never on a restart.
//!
//! # TLS
//!
//! The server runs `[api.tls] mode = "self-signed"`: the client listener serves a leaf
//! issued by the cluster CA (`<data_dir>/tls/ca.crt`). The harness's REST client trusts
//! that CA; workers pin it with the `--ca-hash` from the join token's join command.
//!
//! # Join tokens
//!
//! Created over REST (`POST /v1/workers/tokens`): the `ostro worker token create` verb
//! cannot reach an `https://` server yet (07-15 follow-up: the CLI client rejects https).
//!
//! # Processes
//!
//! Every child is killed on drop. Workers get `RUST_LOG` from [`WorkerOpts::rust_log`]
//! (default unset = the binary's default level); their stdout/stderr are captured for
//! assertions ([`P7Proc::stdout`], [`P7Proc::stderr`], [`P7Proc::error_lines`]).
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ostro_testkit::mock_provider::MockProvider;
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::common;

/// Hang guard for process starts and run waits.
pub const P7_T: Duration = Duration::from_secs(90);

/// Server start attempts on an address-in-use failure.
pub const BIND_ATTEMPTS: usize = 5;

// ---------------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------------

/// Server knobs.
#[derive(Clone, Debug)]
pub struct P7Opts {
    /// `[node] partitions` (default 4: fast opens).
    pub partitions: u16,
    /// `[executor] lease_ttl`.
    pub lease_ttl: Option<Duration>,
    /// `[executor] heartbeat`.
    pub heartbeat: Option<Duration>,
    /// `[executor] slots` (embedded slots).
    pub embedded_slots: Option<u32>,
    /// `[engine] hot_wait`.
    pub hot_wait: Option<Duration>,
    /// `[workers] max_frame_bytes`.
    pub max_frame_bytes: Option<u64>,
    /// `[workers] default_placement` (`embedded` | `workers` | `auto`).
    pub default_placement: Option<String>,
    /// `[workers.spill]` keys, written verbatim (`cooldown = "2s"`, ...).
    pub spill: Vec<(String, String)>,
    /// Start a `MockProvider` and allow loopback egress (`[egress] allow_private`).
    pub mock: bool,
    /// Appended to the generated TOML as is (secrets, connectors, extra sections).
    pub extra_toml: String,
    /// Extra environment for the server process (e.g. `OSTRO_SECRET_*`, `RUST_LOG`).
    pub env: Vec<(String, String)>,
}

impl Default for P7Opts {
    fn default() -> Self {
        Self {
            partitions: 4,
            lease_ttl: None,
            heartbeat: None,
            embedded_slots: None,
            hot_wait: None,
            max_frame_bytes: None,
            default_placement: None,
            spill: Vec::new(),
            mock: false,
            extra_toml: String::new(),
            env: Vec::new(),
        }
    }
}

/// Worker knobs.
#[derive(Clone, Debug)]
pub struct WorkerOpts {
    pub id: String,
    pub labels: BTreeMap<String, String>,
    pub slots: u32,
    /// `OSTRO_WORKER_RECONNECT_MAX_MS` (default 500: quick reconnects in tests).
    pub reconnect_max_ms: u64,
    /// `RUST_LOG` for the worker (`None` = the binary default).
    pub rust_log: Option<String>,
    /// Extra environment.
    pub env: Vec<(String, String)>,
}

impl WorkerOpts {
    pub fn new(id: &str, slots: u32) -> Self {
        Self {
            id: id.to_owned(),
            labels: BTreeMap::new(),
            slots,
            reconnect_max_ms: 500,
            rust_log: None,
            env: Vec::new(),
        }
    }

    pub fn label(mut self, k: &str, v: &str) -> Self {
        self.labels.insert(k.to_owned(), v.to_owned());
        self
    }
}

// ---------------------------------------------------------------------------------
// Processes
// ---------------------------------------------------------------------------------

/// A spawned `ostro` child with captured output; killed on drop.
pub struct P7Proc {
    child: Child,
    pub spawned_at: Instant,
    stdout: Arc<Mutex<Vec<String>>>,
    stderr: Arc<Mutex<Vec<String>>>,
}

fn drain(r: impl std::io::Read + Send + 'static, into: Arc<Mutex<Vec<String>>>) {
    std::thread::spawn(move || {
        for line in BufReader::new(r).lines() {
            let Ok(line) = line else { break };
            into.lock().unwrap_or_else(|e| e.into_inner()).push(line);
        }
    });
}

impl P7Proc {
    fn spawn(mut cmd: Command) -> Self {
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let spawned_at = Instant::now();
        let mut child = cmd.spawn().expect("spawn ostro");
        let stdout = Arc::new(Mutex::new(Vec::new()));
        let stderr = Arc::new(Mutex::new(Vec::new()));
        drain(child.stdout.take().expect("stdout"), Arc::clone(&stdout));
        drain(child.stderr.take().expect("stderr"), Arc::clone(&stderr));
        Self {
            child,
            spawned_at,
            stdout,
            stderr,
        }
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn stdout(&self) -> Vec<String> {
        self.stdout
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn stderr(&self) -> Vec<String> {
        self.stderr
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// stdout + stderr, for failure messages.
    pub fn output(&self) -> String {
        let mut s = self.stdout().join("\n");
        s.push_str("\n--- stderr ---\n");
        s.push_str(&self.stderr().join("\n"));
        s
    }

    /// Lines logged at ERROR level (tracing's ` ERROR ` field, ANSI or not).
    pub fn error_lines(&self) -> Vec<String> {
        self.stdout()
            .into_iter()
            .chain(self.stderr())
            .filter(|l| {
                l.contains(" ERROR ") || l.contains("ERROR\u{1b}") || l.starts_with("ERROR")
            })
            .collect()
    }

    /// `Some(status)` once exited.
    pub fn try_exit(&mut self) -> Option<ExitStatus> {
        self.child.try_wait().expect("try_wait")
    }

    /// Poll until a stdout line satisfies `f` (or the process exits / `limit`).
    pub fn wait_stdout<T>(
        &mut self,
        limit: Duration,
        f: impl Fn(&str) -> Option<T>,
    ) -> Result<T, String> {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(v) = self.stdout().iter().find_map(|l| f(l)) {
                return Ok(v);
            }
            if let Some(st) = self.try_exit() {
                // Give the drain threads a moment to collect the last lines.
                std::thread::sleep(Duration::from_millis(50));
                if let Some(v) = self.stdout().iter().find_map(|l| f(l)) {
                    return Ok(v);
                }
                return Err(format!(
                    "exited ({st:?}) before the expected line:\n{}",
                    self.output()
                ));
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "no expected line within {limit:?}:\n{}",
                    self.output()
                ));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Send a signal by name (`KILL`, `TERM`, `STOP`, `CONT`, `INT`) with `kill(1)`.
    pub fn signal(&self, sig: &str) {
        let st = Command::new("kill")
            .arg(format!("-{sig}"))
            .arg(self.pid().to_string())
            .status()
            .expect("run kill");
        assert!(st.success(), "kill -{sig} {} failed", self.pid());
    }

    pub fn kill9(&mut self) -> ExitStatus {
        self.signal("KILL");
        self.wait_exit(P7_T)
    }

    pub fn sigstop(&self) {
        self.signal("STOP");
    }

    pub fn sigcont(&self) {
        self.signal("CONT");
    }

    pub fn sigterm(&self) {
        self.signal("TERM");
    }

    pub fn wait_exit(&mut self, limit: Duration) -> ExitStatus {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(st) = self.try_exit() {
                return st;
            }
            assert!(
                Instant::now() < deadline,
                "process {} did not exit within {limit:?}:\n{}",
                self.pid(),
                self.output()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for P7Proc {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            // A stopped child must be continued to die promptly on some kernels.
            let _ = Command::new("kill")
                .arg("-CONT")
                .arg(self.child.id().to_string())
                .status();
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// The server's TLS ready line:
/// `NAME listening on https://ADDR (sha256 fingerprint HEX) (data: DIR) ready in N ms`.
#[derive(Clone, Debug)]
pub struct TlsReady {
    pub addr: SocketAddr,
    pub fingerprint: String,
    pub ms: u64,
}

impl TlsReady {
    pub fn parse(line: &str) -> Option<Self> {
        let (_, rest) = line.split_once(" listening on https://")?;
        let (addr, rest) = rest.split_once(" (sha256 fingerprint ")?;
        let (fp, rest) = rest.split_once(") ")?;
        let ms = rest
            .rsplit_once(" ready in ")?
            .1
            .strip_suffix(" ms")?
            .trim()
            .parse()
            .ok()?;
        if fp.len() != 64 || !fp.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        Some(Self {
            addr: addr.parse().ok()?,
            fingerprint: fp.to_owned(),
            ms,
        })
    }
}

/// The worker ready line: `ostro worker ready: id=<id> server=<endpoint> slots=<n>`.
pub fn worker_ready(line: &str) -> Option<(String, String, u32)> {
    let rest = line.strip_prefix("ostro worker ready: ")?;
    let mut id = None;
    let mut server = None;
    let mut slots = None;
    for kv in rest.split_whitespace() {
        match kv.split_once('=') {
            Some(("id", v)) => id = Some(v.to_owned()),
            Some(("server", v)) => server = Some(v.to_owned()),
            Some(("slots", v)) => slots = v.parse().ok(),
            _ => {}
        }
    }
    Some((id?, server?, slots?))
}

/// A spawned worker.
pub struct WorkerProc {
    pub proc: P7Proc,
    pub id: String,
    pub cache: tempfile::TempDir,
}

impl std::ops::Deref for WorkerProc {
    type Target = P7Proc;
    fn deref(&self) -> &P7Proc {
        &self.proc
    }
}

impl std::ops::DerefMut for WorkerProc {
    fn deref_mut(&mut self) -> &mut P7Proc {
        &mut self.proc
    }
}

// ---------------------------------------------------------------------------------
// REST client
// ---------------------------------------------------------------------------------

/// REST client for the TLS server (trusts the cluster CA).
#[derive(Clone)]
pub struct P7Api {
    pub base: String,
    pub http: reqwest::Client,
}

impl P7Api {
    fn new(addr: SocketAddr, ca_pem: &[u8]) -> Self {
        let http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(20))
            .add_root_certificate(reqwest::Certificate::from_pem(ca_pem).expect("cluster CA pem"))
            .build()
            .expect("reqwest client");
        Self {
            base: format!("https://{addr}"),
            http,
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// `method path [json]` -> (status, JSON body or Null).
    pub async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut req = self.http.request(method.clone(), self.url(path));
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

    pub async fn get(&self, path: &str) -> (StatusCode, Value) {
        self.send(reqwest::Method::GET, path, None).await
    }

    pub async fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        self.send(reqwest::Method::POST, path, Some(body)).await
    }

    pub async fn put(&self, path: &str, body: Value) -> (StatusCode, Value) {
        self.send(reqwest::Method::PUT, path, Some(body)).await
    }

    /// `GET /readyz` == 200.
    pub async fn ready(&self) -> bool {
        matches!(
            self.http.get(self.url("/readyz")).send().await,
            Ok(r) if r.status() == StatusCode::OK
        )
    }

    /// Upload a guest by name (`PUT /v1/modules/{sha}`); its workflows activate.
    pub async fn deploy(&self, guest: &str) {
        let wasm = common::guest_wasm(guest);
        let sha = common::sha_hex(&wasm);
        let r = self
            .http
            .put(self.url(&format!("/v1/modules/{sha}")))
            .header("content-type", "application/wasm")
            .body(wasm)
            .send()
            .await
            .expect("PUT module");
        assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
    }

    /// Read-modify-PUT of `/v1/queues/{q}/flow` with `fields` (returns the stored flow).
    pub async fn set_flow(&self, q: &str, fields: Value) -> Value {
        let (st, cur) = self.get(&format!("/v1/queues/{q}/flow")).await;
        assert_eq!(st, StatusCode::OK, "GET flow {q}: {cur}");
        let mut body = cur.clone();
        if let (Some(b), Some(f)) = (body.as_object_mut(), fields.as_object()) {
            for (k, v) in f {
                b.insert(k.clone(), v.clone());
            }
            b.insert("expected_version".into(), cur["version"].clone());
            b.remove("version");
            b.remove("queue");
        }
        let (st, v) = self.put(&format!("/v1/queues/{q}/flow"), body).await;
        assert_eq!(st, StatusCode::OK, "PUT flow {q}: {v}");
        v
    }

    /// Start a run (`POST /v1/workflows/{wf}/runs`), asserting 201; returns the run id.
    pub async fn start(&self, workflow: &str, body: Value) -> String {
        let (st, b) = self
            .post(&format!("/v1/workflows/{workflow}/runs"), body)
            .await;
        assert_eq!(st, StatusCode::CREATED, "start {workflow}: {b}");
        b["run_id"].as_str().expect("run_id").to_owned()
    }

    /// `GET /v1/runs/{id}` (None while the server is unreachable or the run unknown).
    pub async fn run(&self, id: &str) -> Option<Value> {
        let r = self
            .http
            .get(self.url(&format!("/v1/runs/{id}")))
            .send()
            .await
            .ok()?;
        if r.status() != StatusCode::OK {
            return None;
        }
        r.json().await.ok()
    }

    /// Poll until the run is terminal (tolerates a restarting server).
    pub async fn wait_terminal(&self, id: &str, limit: Duration) -> Value {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(b) = self.run(id).await
                && b["status"].as_str().is_some_and(common::is_terminal)
            {
                return b;
            }
            assert!(
                Instant::now() < deadline,
                "run {id} not terminal within {limit:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The full history over REST.
    pub async fn history(&self, id: &str) -> Vec<Value> {
        let mut out = Vec::new();
        let mut from = 0u64;
        loop {
            let (st, p) = self
                .get(&format!("/v1/runs/{id}/history?from_seq={from}&limit=100"))
                .await;
            assert_eq!(st, StatusCode::OK, "{p}");
            out.extend(p["events"].as_array().expect("events").iter().cloned());
            match p["next_from_seq"].as_u64() {
                Some(n) => from = n,
                None => return out,
            }
        }
    }

    /// `GET /v1/queues/{q}/demand`.
    pub async fn demand(&self, q: &str) -> Value {
        let (st, v) = self.get(&format!("/v1/queues/{q}/demand")).await;
        assert_eq!(st, StatusCode::OK, "demand {q}: {v}");
        v
    }

    /// `GET /v1/workers` entries.
    pub async fn workers(&self) -> Vec<Value> {
        let (st, v) = self.get("/v1/workers").await;
        assert_eq!(st, StatusCode::OK, "workers: {v}");
        v["workers"].as_array().cloned().unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------------

/// A free loopback port (bind :0, read, release).
pub fn reserve_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind :0");
    l.local_addr().expect("local addr").port()
}

fn dur(d: Duration) -> String {
    format!("{}ms", d.as_millis())
}

/// One server + workers.
pub struct P7Cluster {
    pub opts: P7Opts,
    /// Data dir + config + worker caches.
    pub dir: tempfile::TempDir,
    pub api_port: u16,
    pub cluster_port: u16,
    pub server: Option<P7Proc>,
    pub ready: TlsReady,
    pub api: P7Api,
    /// The cluster CA (PEM) and its `sha256:<hex>` pin.
    pub ca_pem: Vec<u8>,
    pub ca_hash: String,
    /// A `wkr_<id>.<secret>` join token valid for the whole test.
    pub token: String,
    pub mock: Option<MockProvider>,
    /// How many server starts needed a bind retry.
    pub bind_retries: usize,
}

impl P7Cluster {
    pub fn data_dir(&self) -> PathBuf {
        self.dir.path().join("data")
    }

    pub fn config_path(&self) -> PathBuf {
        self.dir.path().join("ostro.toml")
    }

    pub fn join_url(&self) -> String {
        format!("https://127.0.0.1:{}", self.api_port)
    }

    fn write_config(dir: &Path, opts: &P7Opts, api_port: u16, cluster_port: u16) {
        let data = dir.join("data");
        let mut t = format!(
            "[node]\ndata_dir = {data:?}\npartitions = {}\n\n\
             [api]\nlisten = \"127.0.0.1:{api_port}\"\n\n\
             [api.tls]\nmode = \"self-signed\"\n\n\
             [cluster]\nlisten = \"127.0.0.1:{cluster_port}\"\n\n",
            opts.partitions,
        );
        let mut exec = Vec::new();
        if let Some(d) = opts.lease_ttl {
            exec.push(format!("lease_ttl = \"{}\"", dur(d)));
        }
        if let Some(d) = opts.heartbeat {
            exec.push(format!("heartbeat = \"{}\"", dur(d)));
        }
        if let Some(n) = opts.embedded_slots {
            exec.push(format!("slots = {n}"));
        }
        if !exec.is_empty() {
            t.push_str(&format!("[executor]\n{}\n\n", exec.join("\n")));
        }
        if let Some(d) = opts.hot_wait {
            t.push_str(&format!("[engine]\nhot_wait = \"{}\"\n\n", dur(d)));
        }
        let mut workers = vec!["enabled = true".to_owned()];
        if let Some(n) = opts.max_frame_bytes {
            workers.push(format!("max_frame_bytes = {n}"));
        }
        if let Some(p) = &opts.default_placement {
            workers.push(format!("default_placement = \"{p}\""));
        }
        t.push_str(&format!("[workers]\n{}\n\n", workers.join("\n")));
        if !opts.spill.is_empty() {
            t.push_str("[workers.spill]\n");
            for (k, v) in &opts.spill {
                t.push_str(&format!("{k} = {v}\n"));
            }
            t.push('\n');
        }
        if opts.mock {
            t.push_str("[egress]\nallow_private = [\"127.0.0.0/8\"]\n\n");
        }
        t.push_str(&opts.extra_toml);
        std::fs::write(dir.join("ostro.toml"), t).expect("write config");
    }

    fn server_command(&self) -> Command {
        let mut cmd = common::command();
        cmd.current_dir(self.dir.path())
            .arg("server")
            .arg("--config")
            .arg(self.config_path())
            // The pinned cluster port wins over any profile default.
            .env(
                "OSTRO_CLUSTER__LISTEN",
                format!("127.0.0.1:{}", self.cluster_port),
            );
        for (k, v) in &self.opts.env {
            cmd.env(k, v);
        }
        cmd
    }

    /// Spawn the server until it prints its ready line, retrying on address-in-use.
    /// `fresh_ports`: re-reserve ports before a retry (first start only).
    fn boot(&mut self, fresh_ports: bool) -> (P7Proc, TlsReady) {
        let mut last = String::new();
        for attempt in 0..BIND_ATTEMPTS {
            if attempt > 0 {
                self.bind_retries += 1;
                std::thread::sleep(Duration::from_millis(200 * attempt as u64));
                if fresh_ports {
                    self.api_port = reserve_port();
                    self.cluster_port = reserve_port();
                    Self::write_config(
                        self.dir.path(),
                        &self.opts,
                        self.api_port,
                        self.cluster_port,
                    );
                }
            }
            let mut p = P7Proc::spawn(self.server_command());
            match p.wait_stdout(P7_T, TlsReady::parse) {
                Ok(r) => return (p, r),
                Err(e) => {
                    let lower = e.to_ascii_lowercase();
                    if lower.contains("address already in use")
                        || lower.contains("address in use")
                        || lower.contains("os error 98")
                    {
                        last = e;
                        continue;
                    }
                    panic!("ostro server did not start: {e}");
                }
            }
        }
        panic!("ostro server: address in use after {BIND_ATTEMPTS} attempts:\n{last}");
    }

    async fn wait_ready(&self) {
        let deadline = Instant::now() + P7_T;
        while !self.api.ready().await {
            assert!(Instant::now() < deadline, "/readyz not 200 within {P7_T:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Start the server (TLS, pinned ports) and create a join token.
    pub async fn start(opts: P7Opts) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("ostro-p7-")
            .tempdir()
            .expect("tempdir");
        let api_port = reserve_port();
        let cluster_port = reserve_port();
        Self::write_config(dir.path(), &opts, api_port, cluster_port);
        let mock = if opts.mock {
            Some(MockProvider::start().await)
        } else {
            None
        };
        let mut c = Self {
            opts,
            dir,
            api_port,
            cluster_port,
            server: None,
            ready: TlsReady {
                addr: SocketAddr::from(([127, 0, 0, 1], api_port)),
                fingerprint: String::new(),
                ms: 0,
            },
            api: P7Api {
                base: String::new(),
                http: reqwest::Client::new(),
            },
            ca_pem: Vec::new(),
            ca_hash: String::new(),
            token: String::new(),
            mock,
            bind_retries: 0,
        };
        let (p, ready) = tokio::task::block_in_place(|| c.boot(true));
        c.server = Some(p);
        c.ready = ready;
        c.ca_pem = std::fs::read(c.data_dir().join("tls/ca.crt")).expect("read cluster CA");
        c.api = P7Api::new(SocketAddr::from(([127, 0, 0, 1], c.api_port)), &c.ca_pem);
        c.wait_ready().await;
        let (token, ca_hash) = c.create_token(None).await;
        c.token = token;
        c.ca_hash = ca_hash;
        c
    }

    /// `POST /v1/workers/tokens` -> (token, `sha256:<hex>` CA pin from the join command).
    pub async fn create_token(&self, ttl_ms: Option<u64>) -> (String, String) {
        let body = match ttl_ms {
            Some(t) => json!({"ttl_ms": t}),
            None => json!({}),
        };
        let (st, v) = self.api.post("/v1/workers/tokens", body).await;
        assert_eq!(st, StatusCode::CREATED, "create token: {v}");
        let token = v["token"].as_str().expect("token").to_owned();
        let ca_hash = v["join_command"]
            .as_str()
            .expect("join_command")
            .split("--ca-hash ")
            .nth(1)
            .and_then(|s| s.split_whitespace().next())
            .expect("ca hash in join command")
            .to_owned();
        (token, ca_hash)
    }

    /// Spawn `ostro worker` and wait for its ready line (first session Welcome).
    pub fn spawn_worker(&self, w: WorkerOpts) -> WorkerProc {
        let cache = tempfile::Builder::new()
            .prefix("ostro-p7-cache-")
            .tempdir_in(self.dir.path())
            .expect("cache dir");
        let mut cmd = common::command();
        cmd.current_dir(self.dir.path())
            .arg("worker")
            .arg("--join")
            .arg(self.join_url())
            .arg("--ca-hash")
            .arg(&self.ca_hash)
            .arg("--slots")
            .arg(w.slots.to_string())
            .arg("--cache-dir")
            .arg(cache.path())
            .arg("--worker-id")
            .arg(&w.id)
            .env("OSTRO_WORKER_TOKEN", &self.token)
            .env(
                "OSTRO_WORKER_RECONNECT_MAX_MS",
                w.reconnect_max_ms.to_string(),
            );
        if !w.labels.is_empty() {
            let labels: Vec<String> = w.labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
            cmd.arg("--labels").arg(labels.join(","));
        }
        if let Some(l) = &w.rust_log {
            cmd.env("RUST_LOG", l);
        }
        for (k, v) in &w.env {
            cmd.env(k, v);
        }
        let mut proc = P7Proc::spawn(cmd);
        let (id, _, slots) = tokio::task::block_in_place(|| proc.wait_stdout(P7_T, worker_ready))
            .unwrap_or_else(|e| panic!("worker {} not ready: {e}", w.id));
        assert_eq!(id, w.id);
        assert_eq!(slots, w.slots);
        WorkerProc { proc, id, cache }
    }

    /// Wait until `GET /v1/workers` lists `n` distinct workers.
    pub async fn wait_workers(&self, n: usize) -> Vec<Value> {
        let deadline = Instant::now() + P7_T;
        loop {
            let ws = self.api.workers().await;
            let ids: std::collections::BTreeSet<_> = ws
                .iter()
                .filter_map(|w| w["worker_id"].as_str().map(str::to_owned))
                .collect();
            if ids.len() == n {
                return ws;
            }
            assert!(Instant::now() < deadline, "{n} workers not listed: {ws:?}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// SIGKILL the server (data dir kept).
    pub fn kill9_server(&mut self) {
        if let Some(mut p) = self.server.take() {
            p.kill9();
        }
    }

    /// SIGTERM the server and wait for a clean exit.
    pub fn stop_server(&mut self) {
        if let Some(mut p) = self.server.take() {
            p.sigterm();
            let st = p.wait_exit(P7_T);
            assert!(st.success(), "server exit {st:?}:\n{}", p.output());
        }
    }

    /// Start the server again on the same data dir and the **same** ports (killing a
    /// running one first).
    pub async fn restart_server(&mut self) {
        self.kill9_server();
        let (p, ready) = tokio::task::block_in_place(|| self.boot(false));
        self.server = Some(p);
        self.ready = ready;
        self.wait_ready().await;
    }

    pub fn server(&self) -> &P7Proc {
        self.server.as_ref().expect("server running")
    }

    pub fn mock(&self) -> &MockProvider {
        self.mock.as_ref().expect("P7Opts.mock = true")
    }
}

impl Drop for P7Cluster {
    fn drop(&mut self) {
        // Children are killed by P7Proc::drop; the server last (workers are owned by the
        // test and dropped before or after: either order is safe).
        self.server.take();
    }
}
