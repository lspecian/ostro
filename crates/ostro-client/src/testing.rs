//! The spawned-server test kit (SDK-06, 10-17): [`TestEnv`] starts a throwaway
//! `ostro dev --ephemeral --time-skipping` and drives it through the REST client.
//!
//! The server skips its dev clock forward whenever it is idle and a result long-poll is
//! waiting (D-P10-6, 10-13), so [`TestEnv::result`] on a workflow that sleeps for days
//! returns in milliseconds of wall time.
//!
//! ```no_run
//! use ostro_client::testing::TestEnv;
//! use serde_json::json;
//!
//! # async fn demo() -> Result<(), ostro_client::testing::TestError> {
//! let env = TestEnv::start().await?;
//! env.build_and_deploy(".").await?;
//! let run = env.start_run("hello", json!({ "name": "ada" })).await?;
//! env.signal(&run, "approve", json!({ "ok": true })).await?;
//! let output = env.result(&run, "30s").await?; // skips the 3-day sleep
//! assert_eq!(output["approved"], true);
//! # Ok(())
//! # }
//! ```
//!
//! - **Binary:** `OSTRO_BIN`, else `ostro` on `PATH`. Its `ostro build-info --json` version
//!   must be at least this crate's version.
//! - **Spawn:** `<ostro> dev --ephemeral --time-skipping --listen 127.0.0.1:0` in a fresh
//!   temp directory, with inherited `OSTRO_<SECTION>__*` variables removed and
//!   `OSTRO_RUNS__RETENTION=87600h` (terminal runs must not expire while days are
//!   skipped). [`TestEnvBuilder::egress_loopback`] adds
//!   `OSTRO_EGRESS__ALLOW_PRIVATE=127.0.0.0/8` for HTTP mocks on loopback.
//! - **Ready:** the first stdout line matching [`READY_LINE_PATTERN`] (frozen, D-P10-9).
//! - **Cleanup:** dropping the [`TestEnv`] kills and reaps the child and removes the temp
//!   directory; [`TestEnv::close`] does the same explicitly.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::Method;
use serde_json::{Value, json};

use crate::error::ClientError;
use crate::typed::{DevSkip, StartRun, TypedClient, UploadOutcome};

/// The frozen ready line (D-P10-9, amended). Every kit (Rust, Python, TS, Go) carries this
/// exact pattern; `crates/ostro/tests/ready_line.rs` checks them against each other and
/// against the binary's output.
pub const READY_LINE_PATTERN: &str =
    r"^(.+?) listening on http://(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$";

/// The env variable naming the `ostro` binary (reserved by 10-01).
pub const OSTRO_BIN_ENV: &str = "OSTRO_BIN";

/// How long [`TestEnv::start`] waits for the ready line.
pub const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Run retention given to the spawned server (ten years): skipped days must not expire
/// the runs a test still inspects.
pub const RETENTION: &str = "87600h";

const STDERR_KEEP: usize = 64 * 1024;

/// A test-kit failure.
#[derive(Debug, thiserror::Error)]
pub enum TestError {
    /// The `ostro` binary is missing, not runnable or too old.
    #[error("{0}")]
    Binary(String),
    /// `ostro dev` did not become ready (the message carries its stderr tail).
    #[error("{0}")]
    Startup(String),
    /// A spawned `ostro` subcommand (`deploy`) failed.
    #[error("{0}")]
    Command(String),
    /// A REST call failed.
    #[error(transparent)]
    Client(#[from] ClientError),
    /// A run did not reach the expected outcome.
    #[error("{0}")]
    Run(String),
    /// A bad argument (duration, path).
    #[error("{0}")]
    Usage(String),
}

/// The parsed ready line (groups of [`READY_LINE_PATTERN`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyLine {
    /// Group 1: `ostro dev` / `ostro server`.
    pub name: String,
    /// Group 2: the listen address.
    pub addr: String,
    /// Group 3: the data directory.
    pub data_dir: PathBuf,
    /// Group 4 present (`, ephemeral`).
    pub ephemeral: bool,
    /// Group 5: startup time.
    pub ms: u64,
}

impl ReadyLine {
    /// Match `line` against [`READY_LINE_PATTERN`] (a hand-written matcher with the same
    /// semantics: lazy name, non-space address, lazy data dir, optional `, ephemeral`).
    pub fn parse(line: &str) -> Option<Self> {
        let line = line.strip_suffix('\r').unwrap_or(line);
        // `) ready in (\d+) ms$`: the tail is anchored, so it has exactly one position.
        let rest = line.strip_suffix(" ms")?;
        let digits_at = rest.trim_end_matches(|c: char| c.is_ascii_digit()).len();
        let ms = &rest[digits_at..];
        if ms.is_empty() {
            return None;
        }
        let head = rest[..digits_at].strip_suffix(") ready in ")?;
        // `(.+?) listening on http://(\S+) \(data: `: try each occurrence, leftmost first.
        const MID: &str = " listening on http://";
        const DATA: &str = " (data: ";
        let mut from = 0;
        while let Some(i) = head[from..].find(MID) {
            let at = from + i;
            from = at + 1;
            if at == 0 {
                continue; // (.+?) needs at least one character
            }
            let after = &head[at + MID.len()..];
            let addr_len = after.find(char::is_whitespace).unwrap_or(after.len());
            if addr_len == 0 {
                continue;
            }
            let Some(data) = after[addr_len..].strip_prefix(DATA) else {
                continue;
            };
            let (data_dir, ephemeral) = match data.strip_suffix(", ephemeral") {
                Some(d) => (d, true),
                None => (data, false),
            };
            return Some(Self {
                name: head[..at].to_owned(),
                addr: after[..addr_len].to_owned(),
                data_dir: PathBuf::from(data_dir),
                ephemeral,
                ms: ms.parse().ok()?,
            });
        }
        None
    }
}

/// Parse a duration: `"30s"`, `"500ms"`, `"72h"`, `"3d"`, `"10m"`, `"1h30m"`, or a bare
/// number of seconds.
pub fn parse_duration(s: &str) -> Result<Duration, TestError> {
    let bad = || {
        TestError::Usage(format!(
            "invalid duration {s:?} (expected e.g. \"30s\", \"500ms\", \"72h\", \"3d\")"
        ))
    };
    let t = s.trim();
    if t.is_empty() {
        return Err(bad());
    }
    if let Ok(secs) = t.parse::<f64>() {
        return Duration::try_from_secs_f64(secs).map_err(|_| bad());
    }
    let mut total = Duration::ZERO;
    let mut rest = t;
    while !rest.is_empty() {
        let n_len = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .ok_or_else(bad)?;
        if n_len == 0 {
            return Err(bad());
        }
        let n: f64 = rest[..n_len].parse().map_err(|_| bad())?;
        let after = &rest[n_len..];
        let u_len = after
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(after.len());
        let unit = match &after[..u_len] {
            "ms" => 0.001,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            "d" => 86_400.0,
            _ => return Err(bad()),
        };
        total += Duration::try_from_secs_f64(n * unit).map_err(|_| bad())?;
        rest = &after[u_len..];
    }
    Ok(total)
}

/// A started run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    /// The run id.
    pub run_id: String,
    /// The workflow name.
    pub workflow: String,
    /// Server (dev clock) time at which the run was accepted, ms since the epoch.
    pub accepted_at_ms: u64,
}

impl AsRef<str> for Run {
    fn as_ref(&self) -> &str {
        &self.run_id
    }
}

/// One durable timer of a run ([`TestEnv::timers`]).
#[derive(Debug, Clone, PartialEq)]
pub struct Timer {
    /// History seq of `timer_started`.
    pub seq: u64,
    /// Server time the timer was started at (ms).
    pub started_ms: u64,
    /// Due time (ms).
    pub fire_at_ms: u64,
    /// History seq of the matching `timer_fired`, if it fired.
    pub fired_seq: Option<u64>,
    /// Server time it fired at (ms), if it fired.
    pub fired_ms: Option<u64>,
}

impl Timer {
    /// The timer fired.
    pub fn fired(&self) -> bool {
        self.fired_seq.is_some()
    }
}

/// Options for [`TestEnv`]; see [`TestEnv::builder`].
#[derive(Debug, Clone, Default)]
pub struct TestEnvBuilder {
    binary: Option<PathBuf>,
    egress_loopback: bool,
    time_skipping: Option<String>,
    env: Vec<(String, String)>,
    timeout: Option<Duration>,
}

impl TestEnvBuilder {
    /// Use this binary instead of `OSTRO_BIN` / `PATH`.
    pub fn binary(mut self, path: impl Into<PathBuf>) -> Self {
        self.binary = Some(path.into());
        self
    }

    /// Let effects reach `127.0.0.0/8` (`[egress] allow_private`), for loopback HTTP mocks.
    /// A module still needs an egress rule: [`TestEnv::allow_egress`].
    pub fn egress_loopback(mut self, on: bool) -> Self {
        self.egress_loopback = on;
        self
    }

    /// `--time-skipping=<mode>`: `auto` (default) or `manual` (skips only through
    /// [`TestEnv::skip`]).
    pub fn time_skipping(mut self, mode: &str) -> Self {
        self.time_skipping = Some(mode.to_owned());
        self
    }

    /// An extra environment variable for the server (e.g. `OSTRO_EXECUTOR__…`).
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    /// Per-request timeout of the client (default 30 s; result waits come on top).
    pub fn timeout(mut self, t: Duration) -> Self {
        self.timeout = Some(t);
        self
    }

    /// Spawn the server and wait for its ready line.
    pub async fn start(self) -> Result<TestEnv, TestError> {
        TestEnv::spawn(self).await
    }
}

/// A throwaway `ostro dev --ephemeral --time-skipping` server.
pub struct TestEnv {
    child: Option<Child>,
    ready: ReadyLine,
    url: String,
    bin: PathBuf,
    client: TypedClient,
    stderr: Arc<Mutex<String>>,
    cwd: PathBuf,
    modules: Mutex<HashMap<String, String>>,
}

impl std::fmt::Debug for TestEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestEnv")
            .field("url", &self.url)
            .field("pid", &self.pid())
            .field("bin", &self.bin)
            .finish_non_exhaustive()
    }
}

/// Resolve the binary: `explicit`, else `OSTRO_BIN`, else `ostro` on `PATH`.
pub fn ostro_bin(explicit: Option<&Path>) -> Result<PathBuf, TestError> {
    resolve_ostro_bin(
        explicit,
        std::env::var_os(OSTRO_BIN_ENV),
        std::env::var_os("PATH"),
    )
}

/// [`ostro_bin`] with the `OSTRO_BIN` and `PATH` values given explicitly.
pub fn resolve_ostro_bin(
    explicit: Option<&Path>,
    ostro_bin_env: Option<std::ffi::OsString>,
    path_env: Option<std::ffi::OsString>,
) -> Result<PathBuf, TestError> {
    if let Some(p) = explicit {
        return if p.is_file() {
            Ok(p.to_path_buf())
        } else {
            Err(TestError::Binary(format!(
                "the ostro binary {} does not exist",
                p.display()
            )))
        };
    }
    if let Some(p) = ostro_bin_env.filter(|p| !p.is_empty()) {
        let p = PathBuf::from(p);
        return if p.is_file() {
            Ok(p)
        } else {
            Err(TestError::Binary(format!(
                "{OSTRO_BIN_ENV}={} does not exist: point {OSTRO_BIN_ENV} at a built ostro \
                 binary (cargo build -p ostro) or unset it to use `ostro` on PATH",
                p.display()
            )))
        };
    }
    let name = format!("ostro{}", std::env::consts::EXE_SUFFIX);
    path_env
        .iter()
        .flat_map(std::env::split_paths)
        .map(|d| d.join(&name))
        .find(|p| p.is_file())
        .ok_or_else(|| {
            TestError::Binary(format!(
                "no `{name}` on PATH and {OSTRO_BIN_ENV} is not set: install ostro or point \
                 {OSTRO_BIN_ENV} at the binary"
            ))
        })
}

fn version_triple(v: &str) -> Option<(u64, u64, u64)> {
    let core = v.split(['-', '+']).next()?;
    let mut it = core.split('.').map(|p| p.parse::<u64>().ok());
    Some((it.next()??, it.next()??, it.next().unwrap_or(Some(0))?))
}

/// `ostro build-info --json` → its `version`; an error when older than this kit.
pub fn check_version(bin: &Path) -> Result<String, TestError> {
    let out = Command::new(bin)
        .args(["build-info", "--json"])
        .stdin(Stdio::null())
        .output()
        .map_err(|e| TestError::Binary(format!("cannot run {}: {e}", bin.display())))?;
    if !out.status.success() {
        return Err(TestError::Binary(format!(
            "`{} build-info --json` failed ({}): {}",
            bin.display(),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    let v: Value = serde_json::from_slice(&out.stdout).map_err(|e| {
        TestError::Binary(format!(
            "`{} build-info --json` printed no JSON: {e}",
            bin.display()
        ))
    })?;
    let version = v["version"].as_str().unwrap_or_default().to_owned();
    let kit = env!("CARGO_PKG_VERSION");
    match (version_triple(&version), version_triple(kit)) {
        (Some(have), Some(need)) if have >= need => Ok(version),
        _ => Err(TestError::Binary(format!(
            "{} is version {version:?}; this test kit needs ostro >= {kit}",
            bin.display()
        ))),
    }
}

fn fresh_dir() -> Result<PathBuf, TestError> {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "ostro-testenv-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir)
        .map_err(|e| TestError::Startup(format!("cannot create {}: {e}", dir.display())))?;
    Ok(dir)
}

/// `OSTRO_<SECTION>__*` (server configuration) and `RUST_LOG`: not inherited by the server.
fn is_server_config_var(k: &str) -> bool {
    k == "RUST_LOG" || (k.starts_with("OSTRO_") && k.contains("__"))
}

fn tail(s: &str, n: usize) -> &str {
    let mut at = s.len().saturating_sub(n);
    while !s.is_char_boundary(at) {
        at += 1;
    }
    &s[at..]
}

impl TestEnv {
    /// Spawn with the defaults (auto time skipping, no loopback egress).
    pub async fn start() -> Result<Self, TestError> {
        Self::builder().start().await
    }

    /// Options: binary, loopback egress, time-skipping mode, extra env.
    pub fn builder() -> TestEnvBuilder {
        TestEnvBuilder::default()
    }

    async fn spawn(opts: TestEnvBuilder) -> Result<Self, TestError> {
        let bin = ostro_bin(opts.binary.as_deref())?;
        check_version(&bin)?;
        let cwd = fresh_dir()?;
        let mode = opts.time_skipping.as_deref().unwrap_or("auto");
        let mut cmd = Command::new(&bin);
        cmd.current_dir(&cwd)
            .args(["dev", "--ephemeral", "--listen", "127.0.0.1:0"])
            .arg(format!("--time-skipping={mode}"))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (k, _) in std::env::vars_os() {
            if is_server_config_var(&k.to_string_lossy()) {
                cmd.env_remove(&k);
            }
        }
        cmd.env_remove("OSTRO_LISTEN");
        cmd.env("OSTRO_RUNS__RETENTION", RETENTION);
        if opts.egress_loopback {
            cmd.env("OSTRO_EGRESS__ALLOW_PRIVATE", "127.0.0.0/8");
        }
        for (k, v) in &opts.env {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().map_err(|e| {
            let _ = std::fs::remove_dir_all(&cwd);
            TestError::Startup(format!("cannot spawn {} dev: {e}", bin.display()))
        })?;

        // stdout: the ready line, then drained (the server logs there) so it never blocks.
        let (tx, rx) = mpsc::channel();
        let out = child.stdout.take().expect("piped stdout");
        std::thread::spawn(move || {
            let mut sent = false;
            for line in BufReader::new(out).lines() {
                let Ok(line) = line else { break };
                if !sent && let Some(r) = ReadyLine::parse(&line) {
                    sent = tx.send(r).is_ok();
                }
            }
        });
        // stderr: kept as a bounded tail for error messages.
        let stderr = Arc::new(Mutex::new(String::new()));
        let mut err = child.stderr.take().expect("piped stderr");
        let buf = Arc::clone(&stderr);
        std::thread::spawn(move || {
            let mut chunk = [0u8; 8192];
            while let Ok(n) = err.read(&mut chunk) {
                if n == 0 {
                    break;
                }
                let mut b = buf.lock().unwrap_or_else(|e| e.into_inner());
                b.push_str(&String::from_utf8_lossy(&chunk[..n]));
                if b.len() > 2 * STDERR_KEEP {
                    let keep = tail(&b, STDERR_KEEP).to_owned();
                    *b = keep;
                }
            }
        });

        let deadline = Instant::now() + READY_TIMEOUT;
        let ready = loop {
            match rx.try_recv() {
                Ok(r) => break Ok(r),
                Err(mpsc::TryRecvError::Disconnected) => {
                    break Err("ostro dev closed its stdout before printing the ready line");
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
            if let Ok(Some(_)) = child.try_wait() {
                // Give the stdout reader a moment to deliver a line printed just before exit.
                tokio::time::sleep(Duration::from_millis(50)).await;
                if let Ok(r) = rx.try_recv() {
                    break Ok(r);
                }
                break Err("ostro dev exited before printing the ready line");
            }
            if Instant::now() >= deadline {
                break Err("no ready line from ostro dev within 30 s");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let ready = match ready {
            Ok(r) => r,
            Err(why) => {
                let status = child.kill().and_then(|()| child.wait()).ok();
                tokio::time::sleep(Duration::from_millis(50)).await;
                let _ = std::fs::remove_dir_all(&cwd);
                let e = stderr.lock().unwrap_or_else(|e| e.into_inner()).clone();
                return Err(TestError::Startup(format!(
                    "{why} ({}; exit {status:?}); stderr:\n{}",
                    bin.display(),
                    tail(&e, 4096)
                )));
            }
        };
        let url = format!("http://{}", ready.addr);
        let client = match TypedClient::new(&url, opts.timeout.unwrap_or(Duration::from_secs(30))) {
            Ok(c) => c,
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = std::fs::remove_dir_all(&cwd);
                return Err(e.into());
            }
        };
        Ok(Self {
            child: Some(child),
            ready,
            url,
            bin,
            client,
            stderr,
            cwd,
            modules: Mutex::new(HashMap::new()),
        })
    }

    /// Kill the server, reap it and remove its temp directory (also done on drop).
    pub fn close(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
        let _ = std::fs::remove_dir_all(&self.cwd);
    }

    // --- accessors ----------------------------------------------------------------------

    /// The typed REST client bound to this server.
    pub fn client(&self) -> &TypedClient {
        &self.client
    }

    /// `http://ADDR`.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The listen address.
    pub fn addr(&self) -> SocketAddr {
        self.ready
            .addr
            .parse()
            .expect("the ready line's address is a socket address")
    }

    /// The parsed ready line.
    pub fn ready_line(&self) -> &ReadyLine {
        &self.ready
    }

    /// The server's process id (`None` after [`close`](Self::close)).
    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    /// The `ostro` binary in use.
    pub fn binary(&self) -> &Path {
        &self.bin
    }

    /// The tail of the server's stderr.
    pub fn stderr(&self) -> String {
        self.stderr
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn remember(&self, module: &str, workflows: impl IntoIterator<Item = String>) {
        if module.is_empty() {
            return;
        }
        let mut m = self.modules.lock().unwrap_or_else(|e| e.into_inner());
        for w in workflows {
            m.insert(w, module.to_owned());
        }
        m.insert(module.to_owned(), module.to_owned());
    }

    // --- deploy -------------------------------------------------------------------------

    /// `ostro deploy <dir> --activate --json --server <url>`: build the project if it is
    /// stale, upload it (module name = the project name) and activate its new versions.
    /// Returns the `ostro.deploy.result` JSON.
    ///
    /// The server refuses a changed module under an existing workflow name and version:
    /// bump the version in the source to redeploy a change into the same server.
    pub async fn build_and_deploy(&self, dir: impl AsRef<Path>) -> Result<Value, TestError> {
        let dir = dir.as_ref();
        let out = tokio::process::Command::new(&self.bin)
            .arg("deploy")
            .arg(dir)
            .args(["--activate", "--json", "--server", &self.url])
            .stdin(Stdio::null())
            .output()
            .await
            .map_err(|e| {
                TestError::Command(format!("cannot run {} deploy: {e}", self.bin.display()))
            })?;
        if !out.status.success() {
            return Err(TestError::Command(format!(
                "`ostro deploy {}` failed ({}):\n{}{}",
                dir.display(),
                out.status,
                String::from_utf8_lossy(&out.stderr),
                String::from_utf8_lossy(&out.stdout)
            )));
        }
        let v: Value = serde_json::from_slice(&out.stdout).map_err(|e| {
            TestError::Command(format!("`ostro deploy --json` printed no JSON: {e}"))
        })?;
        let name = v["build"]["name"].as_str().unwrap_or_default().to_owned();
        let wfs = v["build"]["workflows"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|w| w["name"].as_str().map(str::to_owned));
        self.remember(&name, wfs);
        Ok(v)
    }

    /// Upload a prebuilt component under module name = the file stem (`fx_http.wasm` →
    /// `fx-http`) and activate every workflow version it declares.
    pub async fn deploy_wasm(&self, path: impl AsRef<Path>) -> Result<UploadOutcome, TestError> {
        let path = path.as_ref();
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| TestError::Usage(format!("{} has no file name", path.display())))?
            .replace('_', "-");
        let wasm = std::fs::read(path)
            .map_err(|e| TestError::Usage(format!("cannot read {}: {e}", path.display())))?;
        self.deploy_wasm_bytes(wasm, &stem).await
    }

    /// Upload component bytes as module `name` and activate every workflow version in it.
    pub async fn deploy_wasm_bytes(
        &self,
        wasm: Vec<u8>,
        name: &str,
    ) -> Result<UploadOutcome, TestError> {
        let up = self.client.upload_module(wasm, Some(name), None).await?;
        for w in &up.module.workflows {
            self.client.activate(&w.name, w.version).await?;
        }
        self.remember(name, up.module.workflows.iter().map(|w| w.name.clone()));
        Ok(up)
    }

    // --- runs ---------------------------------------------------------------------------

    /// Start a run of the active version of `workflow`.
    pub async fn start_run(&self, workflow: &str, input: Value) -> Result<Run, TestError> {
        self.start_run_with(&StartRun::new(workflow, input)).await
    }

    /// Start a run with every option of [`StartRun`].
    pub async fn start_run_with(&self, req: &StartRun) -> Result<Run, TestError> {
        let started = self.client.start_run(req).await?;
        let info = self.client.get_run(&started.run_id).await?;
        Ok(Run {
            run_id: started.run_id,
            workflow: info.workflow,
            accepted_at_ms: info
                .created_at_ms
                .and_then(|t| u64::try_from(t).ok())
                .unwrap_or_default(),
        })
    }

    /// Send signal `name` to `run`.
    pub async fn signal(
        &self,
        run: impl AsRef<str>,
        name: &str,
        payload: Value,
    ) -> Result<(), TestError> {
        self.client
            .signal(run.as_ref(), name, payload, None)
            .await?;
        Ok(())
    }

    /// Wait up to `wait` (`"30s"`) for the run's outcome and return its output. The wait
    /// lets the server skip idle time. A run that failed, was cancelled or did not finish
    /// in time is an error.
    pub async fn result(&self, run: impl AsRef<str>, wait: &str) -> Result<Value, TestError> {
        let wait = parse_duration(wait)?;
        let id = run.as_ref();
        let r = self.client.result(id, Some(wait)).await?;
        if !r.completed {
            return Err(TestError::Run(format!(
                "run {id} did not finish within {wait:?} (status {:?}); is it waiting for a \
                 signal?",
                r.status
            )));
        }
        match r.status.as_deref() {
            Some("completed") => Ok(r.output.unwrap_or(Value::Null)),
            other => Err(TestError::Run(format!(
                "run {id} ended {}: {}",
                other.unwrap_or("?"),
                r.failure.unwrap_or(Value::Null)
            ))),
        }
    }

    /// Skip the dev clock forward by `by` (`"72h"`). 503 `busy` while activations are in
    /// flight (the server waits up to 5 s first).
    pub async fn skip(&self, by: &str) -> Result<DevSkip, TestError> {
        Ok(self.client.dev_skip_by(parse_duration(by)?).await?)
    }

    /// Server (dev clock) time, ms since the epoch.
    pub async fn now_ms(&self) -> Result<u64, TestError> {
        let t = self.client.dev_time().await?;
        Ok(u64::try_from(t.now_ms).unwrap_or_default())
    }

    /// Every history event of `run` (all pages), as the REST JSON objects
    /// (`{seq, kind, ts_ms, summary, details}`).
    pub async fn history(&self, run: impl AsRef<str>) -> Result<Vec<Value>, TestError> {
        let mut events = Vec::new();
        let mut from = None;
        loop {
            let page = self.client.history(run.as_ref(), from, None).await?;
            events.extend(page.events);
            match page.next {
                Some(n) if from != Some(n) => from = Some(n),
                _ => return Ok(events),
            }
        }
    }

    /// The names of the run's completed steps (`step_completed` with an `ok` outcome), in
    /// history order.
    pub async fn steps(&self, run: impl AsRef<str>) -> Result<Vec<String>, TestError> {
        Ok(self
            .history(run)
            .await?
            .iter()
            .filter(|e| {
                e["kind"] == "step_completed" && e["details"]["outcome"].get("ok").is_some()
            })
            .filter_map(|e| e["details"]["name"].as_str().map(str::to_owned))
            .collect())
    }

    /// The run's durable timers: each `timer_started` with its `timer_fired`, if any.
    pub async fn timers(&self, run: impl AsRef<str>) -> Result<Vec<Timer>, TestError> {
        let events = self.history(run).await?;
        let u = |v: &Value| v.as_u64().unwrap_or_default();
        let mut timers: Vec<Timer> = events
            .iter()
            .filter(|e| e["kind"] == "timer_started")
            .map(|e| Timer {
                seq: u(&e["seq"]),
                started_ms: u(&e["ts_ms"]),
                fire_at_ms: u(&e["details"]["fire_at_ms"]),
                fired_seq: None,
                fired_ms: None,
            })
            .collect();
        for e in events.iter().filter(|e| e["kind"] == "timer_fired") {
            let op = u(&e["details"]["op"]);
            if let Some(t) = timers.iter_mut().find(|t| t.seq == op) {
                t.fired_seq = Some(u(&e["seq"]));
                t.fired_ms = Some(u(&e["ts_ms"]));
            }
        }
        Ok(timers)
    }

    /// Wait (30 s, skipping idle time) for `run` and assert it completed, with `output`
    /// when given. Returns the output. Panics with the run's outcome otherwise.
    pub async fn assert_completed(&self, run: impl AsRef<str>, output: Option<Value>) -> Value {
        let id = run.as_ref().to_owned();
        let got = match self.result(&id, "30s").await {
            Ok(v) => v,
            Err(e) => panic!("run {id} did not complete: {e}"),
        };
        if let Some(want) = output {
            assert_eq!(got, want, "output of run {id}");
        }
        got
    }

    /// Allow `module_or_workflow` to call `url_prefix` (an egress rule such as
    /// `http://127.0.0.1:8080`) through `PUT /v1/egress/{module}`, keeping its existing
    /// rules. A workflow name deployed through this kit resolves to its module. Loopback
    /// targets also need [`TestEnvBuilder::egress_loopback`].
    pub async fn allow_egress(
        &self,
        module_or_workflow: &str,
        url_prefix: &str,
    ) -> Result<Value, TestError> {
        let module = self
            .modules
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(module_or_workflow)
            .cloned()
            .unwrap_or_else(|| module_or_workflow.to_owned());
        let raw = self.client.raw();
        let mut allow: Vec<Value> = match raw
            .call(Method::GET, &["v1", "egress", &module], &[], None)
            .await
        {
            Ok(r) => r.body["allow"].as_array().cloned().unwrap_or_default(),
            Err(e) => {
                let e = ClientError::from(e);
                if e.status != Some(404) {
                    return Err(e.into());
                }
                Vec::new()
            }
        };
        let rule = Value::String(url_prefix.trim_end_matches('/').to_owned());
        if !allow.contains(&rule) {
            allow.push(rule);
        }
        let r = raw
            .call(
                Method::PUT,
                &["v1", "egress", &module],
                &[],
                Some(&json!({ "allow": allow })),
            )
            .await
            .map_err(ClientError::from)?;
        Ok(r.body)
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_line_matches_the_frozen_pattern() {
        let r = ReadyLine::parse(
            "ostro dev listening on http://127.0.0.1:41234 (data: /tmp/x y, ephemeral) ready in 12 ms",
        )
        .expect("parse");
        assert_eq!(r.name, "ostro dev");
        assert_eq!(r.addr, "127.0.0.1:41234");
        assert_eq!(r.data_dir, PathBuf::from("/tmp/x y"));
        assert!(r.ephemeral);
        assert_eq!(r.ms, 12);
        let s = ReadyLine::parse(
            "ostro server listening on http://[::1]:7700 (data: ./.ostro) ready in 3 ms",
        )
        .expect("parse");
        assert_eq!(s.name, "ostro server");
        assert_eq!(s.addr, "[::1]:7700");
        assert!(!s.ephemeral);
        for bad in [
            "ostro dev listening on http://127.0.0.1:1 (data: d) ready in x ms",
            "ostro dev listening on http://127.0.0.1:1 (data: d) ready in 1 ms trailing",
            " listening on http://127.0.0.1:1 (data: d) ready in 1 ms",
            "ostro dev listening on http:// (data: d) ready in 1 ms",
            "INFO something",
        ] {
            assert!(ReadyLine::parse(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(
            parse_duration("72h").unwrap(),
            Duration::from_secs(72 * 3600)
        );
        assert_eq!(
            parse_duration("3d").unwrap(),
            Duration::from_secs(3 * 86_400)
        );
        assert_eq!(parse_duration("1h30m").unwrap(), Duration::from_secs(5400));
        assert_eq!(parse_duration("2").unwrap(), Duration::from_secs(2));
        for bad in ["", "s", "3x", "h3", "-1s"] {
            assert!(parse_duration(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn versions() {
        assert!(version_triple("0.1.0").unwrap() >= version_triple("0.1.0").unwrap());
        assert!(version_triple("0.2.0-dev").unwrap() > version_triple("0.1.9").unwrap());
        assert!(version_triple("0.0.9").unwrap() < version_triple("0.1.0").unwrap());
        assert!(version_triple("x").is_none());
    }
}
