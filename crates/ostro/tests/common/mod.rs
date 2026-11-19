//! Spawned-binary harness (plan 04-19): run the real `ostro` binary, parse its ready
//! line, poll `/readyz`, send signals, wait for exit.
//!
//! The binary is `CARGO_BIN_EXE_ostro` unless `OSTRO_BIN` names another one (e.g. a
//! release build, to record release startup numbers with the same tests).
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// One spawned server at a time per test process, so parallel test threads do not skew
/// the startup numbers (tokio mutex: guards are held across awaits).
pub static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Generous limit for anything that is not the asserted startup budget. This is a hang
/// guard, not a performance target: under heavy load (load average 15-30 while parallel
/// cargo builds fsync gigabytes) an on-disk start has been seen to need more than 30 s, so it
/// is 60 s. A real hang still fails.
pub const T: Duration = Duration::from_secs(60);

/// Attempts for an asserted startup budget: the best (minimum) sample is compared with the
/// budget, so one start descheduled by an unrelated load spike does not fail the gate while
/// a start that is genuinely over budget still fails every attempt.
pub const BUDGET_ATTEMPTS: usize = 5;

/// The binary under test.
pub fn bin() -> PathBuf {
    std::env::var_os("OSTRO_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_ostro")))
}

/// Startup budget: 1 s, overridable with `OSTRO_STARTUP_BUDGET_MS` on slow machines.
pub fn startup_budget() -> Duration {
    let ms = std::env::var("OSTRO_STARTUP_BUDGET_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1000);
    Duration::from_millis(ms)
}

/// A currently free loopback port (bind :0, read, drop).
pub fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind :0");
    l.local_addr().expect("local addr").port()
}

/// `127.0.0.1:<free port>`.
pub fn free_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], free_port()))
}

/// The binary as a `Command`, with every inherited `OSTRO_*` variable removed (only
/// the ones a test sets apply) and `RUST_LOG` cleared (the profile level applies).
pub fn command() -> Command {
    let mut cmd = Command::new(bin());
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with("OSTRO_") {
            cmd.env_remove(&k);
        }
    }
    cmd.env_remove("RUST_LOG");
    cmd
}

/// Run to completion (`--print-config`, failing starts): (status, stdout, stderr).
pub fn run_to_end(mut cmd: Command) -> (ExitStatus, String, String) {
    let out = cmd.output().expect("run ostro");
    (
        out.status,
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// The parsed ready line:
/// `NAME listening on http://ADDR (data: DIR[, ephemeral]) ready in N ms`.
#[derive(Debug, Clone)]
pub struct ReadyLine {
    pub name: String,
    pub addr: SocketAddr,
    pub data_dir: PathBuf,
    pub ephemeral: bool,
    pub ms: u64,
}

impl ReadyLine {
    pub fn parse(line: &str) -> Option<Self> {
        let (name, rest) = line.split_once(" listening on http://")?;
        let (addr, rest) = rest.split_once(" (data: ")?;
        let (data, rest) = rest.split_once(") ready in ")?;
        let ms = rest.strip_suffix(" ms")?.trim().parse().ok()?;
        let (data_dir, ephemeral) = match data.strip_suffix(", ephemeral") {
            Some(d) => (d, true),
            None => (data, false),
        };
        Some(Self {
            name: name.to_owned(),
            addr: addr.parse().ok()?,
            data_dir: PathBuf::from(data_dir),
            ephemeral,
            ms,
        })
    }
}

/// A spawned `ostro dev` / `ostro server`; killed on drop if still running.
pub struct Spawned {
    child: Child,
    pub spawned_at: Instant,
    ready_rx: mpsc::Receiver<ReadyLine>,
    stdout: Arc<Mutex<Vec<String>>>,
    stderr: Arc<Mutex<String>>,
}

impl Spawned {
    /// Spawn `cmd` (from [`command`]) with stdout/stderr drained by threads.
    pub fn spawn(mut cmd: Command) -> Self {
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let spawned_at = Instant::now();
        let mut child = cmd.spawn().expect("spawn ostro");
        let (tx, ready_rx) = mpsc::channel();
        let stdout = Arc::new(Mutex::new(Vec::new()));
        let stderr = Arc::new(Mutex::new(String::new()));
        let out = child.stdout.take().expect("stdout");
        let lines = Arc::clone(&stdout);
        std::thread::spawn(move || {
            for line in BufReader::new(out).lines() {
                let Ok(line) = line else { break };
                if let Some(r) = ReadyLine::parse(&line) {
                    let _ = tx.send(r);
                }
                lines.lock().unwrap_or_else(|e| e.into_inner()).push(line);
            }
        });
        let mut err = child.stderr.take().expect("stderr");
        let buf = Arc::clone(&stderr);
        std::thread::spawn(move || {
            let mut s = String::new();
            let _ = err.read_to_string(&mut s);
            buf.lock().unwrap_or_else(|e| e.into_inner()).push_str(&s);
        });
        Self {
            child,
            spawned_at,
            ready_rx,
            stdout,
            stderr,
        }
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Wait for the ready line on stdout.
    pub fn ready_line(&self, limit: Duration) -> ReadyLine {
        match self.ready_rx.recv_timeout(limit) {
            Ok(r) => r,
            Err(e) => panic!(
                "no ready line within {limit:?} ({e}); stderr: {}",
                self.stderr()
            ),
        }
    }

    /// Poll `GET http://addr/readyz` every 10 ms until 200; returns the time since spawn.
    pub async fn wait_readyz(&self, http: &reqwest::Client, addr: SocketAddr) -> Duration {
        let url = format!("http://{addr}/readyz");
        let deadline = self.spawned_at + T;
        loop {
            if let Ok(r) = http.get(&url).send().await
                && r.status() == reqwest::StatusCode::OK
            {
                return self.spawned_at.elapsed();
            }
            assert!(
                Instant::now() < deadline,
                "/readyz not 200 within {T:?}; stderr: {}",
                self.stderr()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Send a signal by name (`INT`, `TERM`) with `kill(1)`.
    pub fn signal(&self, sig: &str) {
        let st = Command::new("kill")
            .arg(format!("-{sig}"))
            .arg(self.pid().to_string())
            .status()
            .expect("run kill");
        assert!(st.success(), "kill -{sig} failed");
    }

    /// Wait for the process to exit (polling; panics after `limit`).
    pub fn wait_exit(&mut self, limit: Duration) -> ExitStatus {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(st) = self.child.try_wait().expect("try_wait") {
                return st;
            }
            assert!(
                Instant::now() < deadline,
                "process did not exit within {limit:?}; stderr: {}",
                self.stderr()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Signal, wait, assert a clean exit.
    pub fn stop_clean(&mut self, sig: &str) {
        self.signal(sig);
        let st = self.wait_exit(T);
        assert!(
            st.success(),
            "exit after SIG{sig}: {st:?}; stderr: {}",
            self.stderr()
        );
    }

    pub fn stderr(&self) -> String {
        self.stderr
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn stdout_lines(&self) -> Vec<String> {
        self.stdout
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

impl Drop for Spawned {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// HTTP client without proxies (loopback only).
pub fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client")
}

/// `ostro dev` with the given extra args, run from `cwd`.
pub fn dev(cwd: &Path, args: &[&str]) -> Command {
    let mut cmd = command();
    cmd.current_dir(cwd).arg("dev").args(args);
    cmd
}

/// `ostro server` with the given extra args, run from `cwd`.
pub fn server(cwd: &Path, args: &[&str]) -> Command {
    let mut cmd = command();
    cmd.current_dir(cwd).arg("server").args(args);
    cmd
}

/// min / median of a non-empty sample.
pub fn min_median(mut v: Vec<Duration>) -> (Duration, Duration) {
    v.sort();
    (v[0], v[v.len() / 2])
}

// ---------------------------------------------------------------------------------
// End-to-end helpers (plan 04-20): SDK guest build, module upload, run start/get over
// REST and gRPC, history and list, all against a spawned binary's address.
// ---------------------------------------------------------------------------------

use std::sync::OnceLock;

use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::runs_client::RunsClient;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Repository root (`crates/ostro/../..`).
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Build the `guests/` workspace once per process and return the named guest's `.wasm`
/// (copy of ostro-engine's / ostro-server's helper: a dev-dep on test helpers is not
/// possible).
pub fn guest_wasm(name: &str) -> Vec<u8> {
    static BUILT: OnceLock<()> = OnceLock::new();
    let root = repo_root();
    let target_dir = root.join("target/guests");
    BUILT.get_or_init(|| {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let out = Command::new(cargo)
            .args([
                "build",
                "--release",
                "--target",
                "wasm32-wasip2",
                "--manifest-path",
            ])
            .arg(root.join("guests/Cargo.toml"))
            .arg("--target-dir")
            .arg(&target_dir)
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .env_remove("CARGO_BUILD_TARGET")
            .env_remove("CARGO_TARGET_DIR")
            .env_remove("RUSTC_WRAPPER")
            .output()
            .expect("failed to spawn cargo for guests build");
        if !out.status.success() {
            panic!(
                "guests build failed:\n{}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
    });
    let file = format!("{}.wasm", name.replace('-', "_"));
    let path = target_dir.join("wasm32-wasip2/release").join(&file);
    std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Lowercase hex sha256.
pub fn sha_hex(b: &[u8]) -> String {
    Sha256::digest(b)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}

/// Terminal REST run statuses.
pub fn is_terminal(status: &str) -> bool {
    matches!(status, "completed" | "failed" | "cancelled")
}

/// A REST + gRPC client pair for one server address.
pub struct Api {
    pub addr: SocketAddr,
    pub http: reqwest::Client,
}

impl Api {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr, http: http() }
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    /// A tonic `Runs` client on the same port.
    pub async fn runs(&self) -> RunsClient<tonic::transport::Channel> {
        let ch = tonic::transport::Channel::from_shared(format!("http://{}", self.addr))
            .expect("uri")
            .connect()
            .await
            .expect("grpc connect");
        RunsClient::new(ch)
    }

    /// `GET path` -> (status, JSON body).
    pub async fn get(&self, path: &str) -> (StatusCode, Value) {
        let r = self
            .http
            .get(self.url(path))
            .send()
            .await
            .unwrap_or_else(|e| panic!("GET {path}: {e}"));
        let st = r.status();
        (st, r.json().await.expect("json body"))
    }

    /// `PUT /v1/modules/{sha}` with the wasm bytes -> (status, body).
    pub async fn upload(&self, wasm: &[u8]) -> (StatusCode, Value) {
        let sha = sha_hex(wasm);
        let r = self
            .http
            .put(self.url(&format!("/v1/modules/{sha}")))
            .header("content-type", "application/wasm")
            .body(wasm.to_vec())
            .send()
            .await
            .expect("PUT module");
        let st = r.status();
        (st, r.json().await.expect("json body"))
    }

    /// The `GET /v1/workflows` entry for `name`.
    pub async fn workflow(&self, name: &str) -> Value {
        let (st, wf) = self.get("/v1/workflows").await;
        assert_eq!(st, StatusCode::OK, "{wf}");
        wf["workflows"]
            .as_array()
            .expect("workflows")
            .iter()
            .find(|w| w["name"] == name)
            .cloned()
            .unwrap_or_else(|| panic!("workflow {name} missing: {wf}"))
    }

    /// `PUT /v1/workflows/{name}/active {version}` -> status.
    pub async fn activate(&self, name: &str, version: u32) -> StatusCode {
        self.http
            .put(self.url(&format!("/v1/workflows/{name}/active")))
            .json(&json!({ "version": version }))
            .send()
            .await
            .expect("PUT active")
            .status()
    }

    /// `POST /v1/workflows/{workflow}/runs` -> (status, body).
    pub async fn start_http(&self, workflow: &str, body: Value) -> (StatusCode, Value) {
        let r = self
            .http
            .post(self.url(&format!("/v1/workflows/{workflow}/runs")))
            .json(&body)
            .send()
            .await
            .expect("POST run");
        let st = r.status();
        (st, r.json().await.expect("json body"))
    }

    /// Start over REST and return the new run id (asserts 201).
    pub async fn start_new(&self, workflow: &str, body: Value) -> String {
        let (st, b) = self.start_http(workflow, body).await;
        assert_eq!(st, StatusCode::CREATED, "{b}");
        b["run_id"].as_str().expect("run_id").to_owned()
    }

    /// gRPC `Runs.Start`.
    pub async fn start_grpc(
        &self,
        req: pb::StartRunRequest,
    ) -> Result<pb::StartRunResponse, tonic::Status> {
        self.runs().await.start(req).await.map(|r| r.into_inner())
    }

    /// gRPC `Runs.Get`.
    pub async fn get_grpc(&self, run_id: &str) -> pb::RunInfo {
        self.runs()
            .await
            .get(pb::GetRunRequest {
                run_id: run_id.to_owned(),
            })
            .await
            .expect("grpc get")
            .into_inner()
    }

    /// `GET /v1/runs/{id}` (asserts 200).
    pub async fn run(&self, run_id: &str) -> Value {
        let (st, b) = self.get(&format!("/v1/runs/{run_id}")).await;
        assert_eq!(st, StatusCode::OK, "{b}");
        b
    }

    /// Poll GET every 20 ms until `f(run)` holds.
    pub async fn wait_run(
        &self,
        run_id: &str,
        limit: Duration,
        f: impl Fn(&Value) -> bool,
    ) -> Value {
        let deadline = Instant::now() + limit;
        loop {
            let b = self.run(run_id).await;
            if f(&b) {
                return b;
            }
            assert!(
                Instant::now() < deadline,
                "run {run_id} wait timed out: {b}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Poll GET until the run is terminal.
    pub async fn wait_terminal(&self, run_id: &str, limit: Duration) -> Value {
        self.wait_run(run_id, limit, |b| {
            b["status"].as_str().is_some_and(is_terminal)
        })
        .await
    }

    /// The full history (every page) over REST.
    pub async fn history(&self, run_id: &str) -> Vec<Value> {
        let mut out = Vec::new();
        let mut from = 0u64;
        loop {
            let (st, p) = self
                .get(&format!(
                    "/v1/runs/{run_id}/history?from_seq={from}&limit=100"
                ))
                .await;
            assert_eq!(st, StatusCode::OK, "{p}");
            out.extend(p["events"].as_array().expect("events").iter().cloned());
            match p["next_from_seq"].as_u64() {
                Some(n) => from = n,
                None => return out,
            }
        }
    }

    /// Run ids of `GET /v1/runs?<query>` across every page.
    pub async fn list(&self, query: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut path = format!("/v1/runs?{query}&limit=50");
            if let Some(t) = &token {
                path.push_str(&format!("&page_token={t}"));
            }
            let (st, p) = self.get(&path).await;
            assert_eq!(st, StatusCode::OK, "{p}");
            for r in p["runs"].as_array().expect("runs") {
                out.push(r["run_id"].as_str().expect("run_id").to_owned());
            }
            match p["next_page_token"].as_str() {
                Some(t) => token = Some(t.to_owned()),
                None => return out,
            }
        }
    }
}

/// History events of `kind`.
pub fn events_of<'a>(history: &'a [Value], kind: &str) -> Vec<&'a Value> {
    history.iter().filter(|e| e["kind"] == kind).collect()
}

/// Number of `step_completed` events for step `name`.
pub fn step_completions(history: &[Value], name: &str) -> usize {
    history
        .iter()
        .filter(|e| e["kind"] == "step_completed" && e["details"]["name"] == name)
        .count()
}
