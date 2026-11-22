//! Crash leg (plan 09-18, SC3; WF-06 / SDK-05): the TS -> Python -> Rust child chain on a
//! spawned failpoint-enabled `ostro dev`, under kill -9 and the `child::` failpoints.
//!
//! - `crash_chain_completes` (C-CRASH-01): per iteration, start `ts_conf_parent_chain`
//!   with `{"next": ["py_conf_parent_chain", "rs_conf_leaf_echo"]}`, then inject either
//!   1..=3 kill -9 at random delays (each after the start ack or the previous restart;
//!   uniform in [0, 1500] ms, half of the draws in the chain's in-flight window), or one
//!   `child::` failpoint crash (below), with the restarts unarmed. After the last restart
//!   the root must complete with the nested echo, the root and the Python run each journal
//!   exactly one `child_started` / `child_completed`, the child ids are the deterministic
//!   ones (D-P9-7), every run journals one `run_started` and one terminal event, the
//!   server lists exactly one Python and one Rust run per root so far, and every listed
//!   run is terminal.
//! - `crash_cancel_propagates` (C-CRASH-02): per iteration, start the chain on
//!   `rs_conf_leaf_sleep`, wait until the leaf journaled `timer_started`, then kill -9 just
//!   before `POST /v1/runs/{root}/cancel`, kill -9 a random 0..500 ms after the cancel ack,
//!   or no kill. Within 30 s of the last restart root, Python mid and Rust leaf are all
//!   `cancelled`, the leaf's shielded `cleanup` step is journaled exactly once, and no run
//!   is left non-terminal.
//!
//! Failpoint crash points (`FAILPOINTS` on a restart before the root starts): the `child::`
//! sites return `Unavailable` on `return`, which the engine maps to an eviction without
//! logging the message, so `return` gives no marker in a spawned process. Instead:
//! - `child::start_child_after_flush=pause`: the sink blocks right after the flush; the
//!   root's journal then shows `child_started`, and the kill lands exactly at the site.
//! - `<site>=1*print(crash-leg hit <site>)` for `child::parent_event_before_flush` and for
//!   the `all` mode (all three sites armed): the `fail` crate logs the marker through the
//!   `log` -> `tracing` bridge at the site, and the kill follows within milliseconds.
//!
//! `child::detached_after_flush` is unreachable from an attached chain (its coverage is
//! 09-08's `children_failpoints`); the report counts hits per mode.
//!
//! Iterations: `OSTRO_CRASH_CHAIN_ITERS` (default 20; the SC3 gate run uses 100). The
//! failpoint binary is `OSTRO_FAILPOINT_BIN` when set (05-19's override), else it is built
//! with `cargo build --release -p ostro --features failpoints --target-dir
//! target/failpoints --locked`. A failing iteration keeps its data dir as
//! `target/crash-leg/failed-<test>-<iter>` and prints the seed and the server log tail.
//!
//! ```text
//! OSTRO_CRASH_CHAIN_ITERS=100 cargo test -p ostro-conformance --features polyglot --locked \
//!     --test crash_leg -- --ignored --test-threads=1 --nocapture
//! ```
#![cfg(feature = "polyglot")]

use std::collections::VecDeque;
use std::io::{BufRead, BufReader};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ostro_conformance::langs::{self, Lang};
use ostro_conformance::repo_root;
use ostro_conformance::server_leg::substitute;
use ostro_conformance::spec::{self, Spec};
use ostro_server::children::child_run_id;
use ostro_types::RunId;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Env var with the iteration count per test (P9 reservation, D-P9-20).
const ITERS_ENV: &str = "OSTRO_CRASH_CHAIN_ITERS";
const DEFAULT_ITERS: usize = 20;
/// Log lines kept per server process.
const RING: usize = 400;
/// Ready line limit per spawn (recovery included).
const SPAWN_LIMIT: Duration = Duration::from_secs(90);
/// Limit for the root to finish after the last restart.
const COMPLETE_LIMIT: Duration = Duration::from_secs(60);
const CANCEL_LIMIT: Duration = Duration::from_secs(30);
/// Limit for an armed failpoint to be hit.
const FAULT_LIMIT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(25);
/// Busy windows for [`Rng::kill_delay`]: the chain completes well under 100 ms after its
/// start ack, the cancel cascade in about 30-40 ms after the cancel ack (idle machine).
const CHAIN_BUSY_MS: u64 = 80;
const CASCADE_BUSY_MS: u64 = 40;
/// Role per level: TS root, Python mid, Rust leaf (`wf:` / `wf2:` in the spec input).
const ROLES: [Lang; 3] = [Lang::Ts, Lang::Py, Lang::Rust];

/// The `child::` failpoints of 09-08.
const FP_START: &str = "child::start_child_after_flush";
const FP_PARENT_EVENT: &str = "child::parent_event_before_flush";
const FP_DETACHED: &str = "child::detached_after_flush";

// ---------------------------------------------------------------------------------
// RNG
// ---------------------------------------------------------------------------------

/// splitmix64: small, seedable, good enough for kill timings.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `[lo, hi]`.
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }

    /// A kill delay: uniform in `[0, max]`, but half of the draws fall in `[0, busy]`, the
    /// window in which the chain (or the cancel cascade) is actually in flight on an idle
    /// machine; with `[0, max]` alone nearly every kill lands after the runs finished.
    fn kill_delay(&mut self, busy: u64, max: u64) -> u64 {
        if self.next().is_multiple_of(2) {
            self.range(0, busy)
        } else {
            self.range(0, max)
        }
    }
}

fn fresh_seed() -> u64 {
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    t ^ u64::from(std::process::id()).rotate_left(32)
}

fn load(id: &str) -> Spec {
    let s = spec::load_spec(id).unwrap_or_else(|e| panic!("{id}: {e}"));
    assert_eq!(s.leg, spec::Leg::Crash, "{id}: leg");
    s
}

/// The spec input for the TS -> Python -> Rust roles, tagged with the iteration (the
/// chain guests ignore extra fields).
fn chain_input(s: &Spec, iter: usize) -> Value {
    let mut v = substitute(&s.input, &ROLES, "");
    v["v"] = json!(iter);
    v
}

fn iterations() -> usize {
    match std::env::var(ITERS_ENV) {
        Ok(v) if !v.trim().is_empty() => v
            .trim()
            .parse()
            .unwrap_or_else(|e| panic!("{ITERS_ENV}={v:?}: {e}")),
        _ => DEFAULT_ITERS,
    }
}

// ---------------------------------------------------------------------------------
// Failpoint binary + spawned server
// ---------------------------------------------------------------------------------

fn failpoint_bin() -> PathBuf {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        let bin = match std::env::var_os("OSTRO_FAILPOINT_BIN") {
            Some(b) => PathBuf::from(b),
            None => {
                let root = repo_root();
                let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
                let out = Command::new(cargo)
                    .current_dir(&root)
                    .args([
                        "build",
                        "--release",
                        "-p",
                        "ostro",
                        "--features",
                        "failpoints",
                        "--target-dir",
                        "target/failpoints",
                        "--locked",
                    ])
                    .env_remove("CARGO_TARGET_DIR")
                    .output()
                    .expect("spawn cargo build (failpoint binary)");
                assert!(
                    out.status.success(),
                    "failpoint binary build failed:\n{}",
                    String::from_utf8_lossy(&out.stderr)
                );
                root.join("target/failpoints/release/ostro")
            }
        };
        let info = Command::new(&bin)
            .arg("build-info")
            .env_remove("FAILPOINTS")
            .output()
            .unwrap_or_else(|e| panic!("run {} build-info: {e}", bin.display()));
        let info = String::from_utf8_lossy(&info.stdout);
        assert!(
            info.contains("failpoints=on"),
            "{} is not a failpoint build: {info}",
            bin.display()
        );
        bin
    })
    .clone()
}

/// A log line from a `print` failpoint action ([`HIT_MARKER`]).
fn is_fault_line(line: &str) -> bool {
    line.contains(HIT_MARKER)
}

/// `NAME listening on http://ADDR (data: ...) ready in N ms` -> ADDR.
fn parse_ready(line: &str) -> Option<String> {
    let (_, rest) = line.split_once(" listening on http://")?;
    let (addr, rest) = rest.split_once(" (data: ")?;
    rest.contains(") ready in ").then(|| addr.to_owned())
}

/// One spawned `ostro dev` process.
struct Server {
    child: Option<Child>,
    base: String,
    log: Arc<Mutex<VecDeque<String>>>,
    faults: Arc<AtomicU64>,
}

impl Server {
    /// Spawns `ostro dev --data-dir DIR --listen 127.0.0.1:0` (inherited `OSTRO_*`,
    /// `RUST_LOG` and `FAILPOINTS` stripped; `failpoints` set when given) and waits for the
    /// ready line, then for `/readyz` 200 when unarmed.
    async fn spawn(
        data_dir: &Path,
        failpoints: Option<&str>,
        http: &reqwest::Client,
    ) -> Result<Self, String> {
        std::fs::create_dir_all(data_dir).map_err(|e| format!("mkdir data dir: {e}"))?;
        let mut cmd = Command::new(failpoint_bin());
        for (k, _) in std::env::vars_os() {
            if k.to_string_lossy().starts_with("OSTRO_") {
                cmd.env_remove(&k);
            }
        }
        cmd.env_remove("RUST_LOG");
        match failpoints {
            Some(fp) => cmd.env("FAILPOINTS", fp),
            None => cmd.env_remove("FAILPOINTS"),
        };
        cmd.arg("dev")
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--listen")
            .arg("127.0.0.1:0")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().map_err(|e| format!("spawn ostro dev: {e}"))?;
        let log = Arc::new(Mutex::new(VecDeque::with_capacity(RING)));
        let faults = Arc::new(AtomicU64::new(0));
        let (tx, rx) = mpsc::channel::<String>();
        let drain = |r: Box<dyn std::io::Read + Send>, tx: Option<mpsc::Sender<String>>| {
            let log = Arc::clone(&log);
            let faults = Arc::clone(&faults);
            std::thread::spawn(move || {
                for line in BufReader::new(r).lines() {
                    let Ok(line) = line else { break };
                    if let Some(tx) = &tx
                        && let Some(a) = parse_ready(&line)
                    {
                        let _ = tx.send(a);
                    }
                    if is_fault_line(&line) {
                        faults.fetch_add(1, Ordering::SeqCst);
                    }
                    let mut g = log.lock().unwrap_or_else(|e| e.into_inner());
                    if g.len() == RING {
                        g.pop_front();
                    }
                    g.push_back(line);
                }
            });
        };
        drain(Box::new(child.stdout.take().expect("stdout")), Some(tx));
        drain(Box::new(child.stderr.take().expect("stderr")), None);
        let deadline = Instant::now() + SPAWN_LIMIT;
        let addr = loop {
            if let Ok(a) = rx.try_recv() {
                break a;
            }
            if let Ok(Some(st)) = child.try_wait() {
                tokio::time::sleep(Duration::from_millis(50)).await;
                return Err(format!(
                    "server exited before ready ({st}); log tail:\n{}",
                    tail_of(&log, 40)
                ));
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "no ready line within {SPAWN_LIMIT:?}; log tail:\n{}",
                    tail_of(&log, 40)
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let mut s = Server {
            child: Some(child),
            base: format!("http://{addr}"),
            log,
            faults,
        };
        while failpoints.is_none() {
            if let Ok(r) = http.get(format!("{}/readyz", s.base)).send().await
                && r.status() == reqwest::StatusCode::OK
            {
                break;
            }
            if s.exited() {
                return Err(format!(
                    "server exited before /readyz; log tail:\n{}",
                    s.tail(40)
                ));
            }
            if Instant::now() > deadline {
                s.kill9().await?;
                return Err(format!(
                    "/readyz not 200 within {SPAWN_LIMIT:?}; log tail:\n{}",
                    s.tail(40)
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(s)
    }

    fn exited(&mut self) -> bool {
        self.child
            .as_mut()
            .is_none_or(|c| matches!(c.try_wait(), Ok(Some(_))))
    }

    fn faults(&self) -> u64 {
        self.faults.load(Ordering::SeqCst)
    }

    fn tail(&self, n: usize) -> String {
        tail_of(&self.log, n)
    }

    /// SIGKILL + waitpid; `true` when the wait status says signal 9 (`false`: the process
    /// had already exited on its own).
    async fn kill9(&mut self) -> Result<bool, String> {
        let Some(mut child) = self.child.take() else {
            return Ok(false);
        };
        if let Ok(Some(_)) = child.try_wait() {
            return Ok(false);
        }
        let _ = child.kill();
        let st = tokio::task::spawn_blocking(move || child.wait())
            .await
            .map_err(|e| format!("wait task: {e}"))?
            .map_err(|e| format!("waitpid: {e}"))?;
        match st.signal() {
            Some(9) => Ok(true),
            _ if st.code().is_some() => Ok(false),
            other => Err(format!("unexpected wait status {st} (signal {other:?})")),
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn tail_of(log: &Mutex<VecDeque<String>>, n: usize) -> String {
    let g = log.lock().unwrap_or_else(|e| e.into_inner());
    let skip = g.len().saturating_sub(n);
    g.iter().skip(skip).cloned().collect::<Vec<_>>().join("\n")
}

// ---------------------------------------------------------------------------------
// REST client over the spawned server
// ---------------------------------------------------------------------------------

/// Retries a request across restarts (connection refused, 5xx while partitions recover)
/// until `limit`; 4xx answers are final.
async fn call(
    http: &reqwest::Client,
    rb: impl Fn() -> reqwest::RequestBuilder,
    what: &str,
    limit: Duration,
) -> Result<Value, String> {
    let deadline = Instant::now() + limit;
    loop {
        let err = match rb().send().await {
            Ok(r) => {
                let st = r.status();
                let b = r.bytes().await.unwrap_or_default();
                let v = if b.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_slice(&b)
                        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&b).into()))
                };
                if st.is_success() {
                    return Ok(v);
                }
                if st.is_client_error() {
                    return Err(format!("{what}: HTTP {st}: {v}"));
                }
                format!("{what}: HTTP {st}: {v}")
            }
            Err(e) => format!("{what}: {e}"),
        };
        if Instant::now() > deadline {
            return Err(format!("{err} (gave up after {limit:?})"));
        }
        let _ = http;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// REST view of the spawned server (base URL changes on every restart).
struct Api {
    http: reqwest::Client,
    base: String,
}

impl Api {
    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    async fn get(&self, path: &str, what: &str) -> Result<Value, String> {
        let url = self.url(path);
        call(&self.http, || self.http.get(&url), what, STEP_RETRY).await
    }

    async fn post(&self, path: &str, body: &Value, what: &str) -> Result<Value, String> {
        let url = self.url(path);
        call(
            &self.http,
            || self.http.post(&url).json(body),
            what,
            STEP_RETRY,
        )
        .await
    }

    async fn deploy(&self, wasm: &[u8], name: &str) -> Result<(), String> {
        let hex = hex(&Sha256::digest(wasm));
        let url = self.url(&format!("/v1/modules/{hex}?name={name}"));
        call(
            &self.http,
            || {
                self.http
                    .put(&url)
                    .header("content-type", "application/wasm")
                    .timeout(Duration::from_secs(300))
                    .body(wasm.to_vec())
            },
            &format!("deploy {name}"),
            Duration::from_secs(300),
        )
        .await
        .map(drop)
    }

    async fn start(&self, workflow: &str, input: &Value, key: &str) -> Result<RunId, String> {
        let v = self
            .post(
                &format!("/v1/workflows/{workflow}/runs"),
                &json!({ "input": input, "idempotency_key": key }),
                &format!("start {workflow}"),
            )
            .await?;
        parse_id(&v["run_id"])
    }

    async fn cancel(&self, run: &RunId) -> Result<(), String> {
        self.post(
            &format!("/v1/runs/{run}/cancel"),
            &json!({ "reason": "crash-leg" }),
            "cancel",
        )
        .await
        .map(drop)
    }

    async fn run(&self, run: &RunId) -> Result<Value, String> {
        self.get(&format!("/v1/runs/{run}"), "get run").await
    }

    /// The run's status right now, without retries (`None` when the server does not
    /// answer within 2 s).
    async fn run_status_now(&self, run: &RunId) -> Option<String> {
        let r = self
            .http
            .get(self.url(&format!("/v1/runs/{run}")))
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .ok()?;
        let v: Value = r.json().await.ok()?;
        v["status"].as_str().map(str::to_owned)
    }

    /// The run once it is terminal (long-poll on `/result`), within `limit`.
    async fn terminal(&self, run: &RunId, limit: Duration) -> Result<Value, String> {
        let deadline = Instant::now() + limit;
        loop {
            let v = self
                .get(&format!("/v1/runs/{run}/result?wait=2s"), "result")
                .await?;
            if v["completed"] == Value::Bool(true) {
                return Ok(v);
            }
            if Instant::now() > deadline {
                return Err(format!("run {run} not terminal within {limit:?}: {v}"));
            }
        }
    }

    /// The whole history (all pages).
    async fn history(&self, run: &RunId) -> Result<Vec<Value>, String> {
        let mut out = Vec::new();
        let mut from = 0u64;
        loop {
            let v = self
                .get(
                    &format!("/v1/runs/{run}/history?from_seq={from}&limit=1000"),
                    "history",
                )
                .await?;
            out.extend(v["events"].as_array().cloned().unwrap_or_default());
            match v["next_from_seq"].as_u64() {
                Some(n) => from = n,
                None => return Ok(out),
            }
        }
    }

    /// `(run id, status)` of every run of `workflow` (all pages).
    async fn list(&self, workflow: &str) -> Result<Vec<(RunId, String)>, String> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut path = format!("/v1/runs?limit=1000&workflow={workflow}");
            if let Some(t) = &token {
                path.push_str(&format!("&page_token={t}"));
            }
            let v = self.get(&path, "list runs").await?;
            for r in v["runs"].as_array().into_iter().flatten() {
                out.push((
                    parse_id(&r["run_id"])?,
                    r["status"].as_str().unwrap_or("?").to_owned(),
                ));
            }
            match v["next_page_token"].as_str() {
                Some(t) => token = Some(t.to_owned()),
                None => return Ok(out),
            }
        }
    }
}

/// Retry window of single REST calls (covers a restart's recovery).
const STEP_RETRY: Duration = Duration::from_secs(60);

fn parse_id(v: &Value) -> Result<RunId, String> {
    let s = v.as_str().ok_or_else(|| format!("no run id in {v}"))?;
    s.parse().map_err(|e| format!("run id {s}: {e:?}"))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn is_terminal(status: &str) -> bool {
    matches!(
        status,
        "completed" | "failed" | "cancelled" | "terminated" | "timed_out" | "continued_as_new"
    )
}

fn kind_count(history: &[Value], kind: &str) -> usize {
    history.iter().filter(|e| e["kind"] == kind).count()
}

/// The child run started by the run's single `child_started` (seq = op, D-P9-7).
fn only_child(run: &RunId, history: &[Value], partitions: u16) -> Result<RunId, String> {
    let starts: Vec<u64> = history
        .iter()
        .filter(|e| e["kind"] == "child_started")
        .filter_map(|e| e["seq"].as_u64())
        .collect();
    let [op] = starts.as_slice() else {
        return Err(format!(
            "run {run}: expected exactly one child_started, found ops {starts:?}"
        ));
    };
    Ok(child_run_id(run, *op, partitions))
}

// ---------------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------------

/// Data dir, server, modules and counters for one test.
struct Leg {
    name: &'static str,
    data_dir: PathBuf,
    http: reqwest::Client,
    server: Option<Server>,
    modules: Vec<(String, Vec<u8>)>,
    partitions: u16,
    seed: u64,
    kills: u64,
    restarts: u64,
}

impl Leg {
    async fn new(name: &'static str, seed: u64) -> Self {
        let bin = tokio::task::spawn_blocking(failpoint_bin)
            .await
            .expect("failpoint binary task");
        eprintln!("{name}: failpoint binary {}", bin.display());
        let mut modules = Vec::new();
        for lang in [Lang::Ts, Lang::Py, Lang::Rust] {
            let t = Instant::now();
            let wasm = tokio::task::spawn_blocking(move || langs::module_for(lang, None))
                .await
                .expect("module build task")
                .unwrap_or_else(|e| panic!("{} module: {e}", langs::name(lang)));
            eprintln!(
                "{name}: {} module {} bytes ({:.1?})",
                langs::name(lang),
                wasm.len(),
                t.elapsed()
            );
            modules.push((format!("conf-{}", langs::name(lang)), wasm));
        }
        let data_dir = repo_root()
            .join("target/crash-leg")
            .join(format!("{name}-{seed:016x}"));
        let _ = std::fs::remove_dir_all(&data_dir);
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("http client");
        let mut leg = Leg {
            name,
            data_dir,
            http,
            server: None,
            modules,
            partitions: 0,
            seed,
            kills: 0,
            restarts: 0,
        };
        leg.restart(None).await.expect("first spawn");
        let api = leg.api();
        for (n, wasm) in &leg.modules {
            let t = Instant::now();
            api.deploy(wasm, n)
                .await
                .unwrap_or_else(|e| panic!("deploy {n}: {e}"));
            eprintln!("{name}: deployed {n} ({:.1?})", t.elapsed());
        }
        leg.partitions = leg.partitions_from_log();
        eprintln!("{name}: dev node partitions = {}", leg.partitions);
        leg
    }

    fn api(&self) -> Api {
        Api {
            http: self.http.clone(),
            base: self
                .server
                .as_ref()
                .map_or_else(String::new, |s| s.base.clone()),
        }
    }

    fn server(&self) -> &Server {
        self.server.as_ref().expect("server running")
    }

    /// Partition count of the dev node, from the server's start line (`ostro server
    /// started, ... partitions: N, ...`); child ids derive from it (D-P9-7).
    fn partitions_from_log(&self) -> u16 {
        let log = self.server().log.lock().unwrap_or_else(|e| e.into_inner());
        log.iter()
            .filter(|l| l.contains("ostro server started"))
            .find_map(|l| {
                let (_, rest) = l.split_once("partitions: ")?;
                rest.split(|c: char| !c.is_ascii_digit())
                    .next()?
                    .parse()
                    .ok()
            })
            .expect("partition count in the server start line")
    }

    /// Kill -9 (when running) and respawn with `failpoints`.
    async fn restart(&mut self, failpoints: Option<&str>) -> Result<(), String> {
        self.kill().await?;
        let s = Server::spawn(&self.data_dir, failpoints, &self.http).await?;
        self.restarts += 1;
        self.server = Some(s);
        Ok(())
    }

    async fn kill(&mut self) -> Result<(), String> {
        if let Some(mut s) = self.server.take()
            && s.kill9().await?
        {
            self.kills += 1;
        }
        Ok(())
    }

    /// Keeps the data dir of a failed iteration and panics with seed, reason and log.
    fn fail(&mut self, iter: usize, what: &str, err: String) -> ! {
        let tail = self
            .server
            .as_ref()
            .map_or_else(String::new, |s| s.tail(60));
        if let Some(mut s) = self.server.take()
            && let Some(mut c) = s.child.take()
        {
            let _ = c.kill();
            let _ = c.wait();
        }
        let kept = repo_root()
            .join("target/crash-leg")
            .join(format!("failed-{}-{iter}", self.name));
        let _ = std::fs::remove_dir_all(&kept);
        let moved = std::fs::rename(&self.data_dir, &kept).is_ok();
        panic!(
            "{} iteration {iter} (seed {:#018x}): {what}: {err}\ndata dir: {}\nserver log tail:\n{tail}",
            self.name,
            self.seed,
            if moved {
                kept.display().to_string()
            } else {
                self.data_dir.display().to_string()
            }
        );
    }

    async fn finish(mut self) {
        let _ = self.kill().await;
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// 1..=3 kill -9 at random delays.
    Random,
    /// Restart armed with one or all `child::` failpoints, kill -9 on the first hit.
    Failpoint(&'static str),
}

const ALL: &str = "all";

/// Mode rotation: every other iteration kills at random times, the others cycle through
/// the failpoint modes.
fn mode_for(iter: usize) -> Mode {
    match iter % 6 {
        0 | 2 | 4 => Mode::Random,
        1 => Mode::Failpoint(FP_START),
        3 => Mode::Failpoint(FP_PARENT_EVENT),
        _ => Mode::Failpoint(ALL),
    }
}

/// Marker the `print` action logs (through the `log` -> `tracing` bridge) at a site.
const HIT_MARKER: &str = "crash-leg hit";

/// `FAILPOINTS` for a mode. `child::start_child_after_flush` alone uses `pause`: the
/// sink blocks right after the flush, which the root's journal shows (`child_started`), so
/// the kill lands exactly at the site. The other modes use `1*print(<marker>)`: the site
/// logs the marker and continues, and the kill follows within milliseconds of the line.
fn failpoint_spec(fp: &str) -> String {
    let print = |f: &str| format!("{f}=1*print({HIT_MARKER} {f})");
    match fp {
        FP_START => format!("{FP_START}=pause"),
        ALL => [FP_START, FP_PARENT_EVENT, FP_DETACHED]
            .map(print)
            .join(";"),
        other => print(other),
    }
}

fn median(v: &mut [Duration]) -> Duration {
    if v.is_empty() {
        return Duration::ZERO;
    }
    v.sort();
    v[v.len() / 2]
}

// ---------------------------------------------------------------------------------
// C-CRASH-01
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns the failpoint ostro binary and kills it; slow (09-18 crash leg)"]
async fn crash_chain_completes() {
    let started = Instant::now();
    let iters = iterations();
    let seed = fresh_seed();
    eprintln!("C-CRASH-01: {iters} iterations, seed {seed:#018x}");
    let spec = load("C-CRASH-01");
    let expected = spec.expect.result.clone().expect("C-CRASH-01 result");
    let mut leg = Leg::new("crash-chain", seed).await;
    let mut rng = Rng(seed);
    let (mut random_iters, mut fp_iters, mut in_flight_kills) = (0u64, 0u64, 0u64);
    let mut hits: Vec<(&str, u64, u64)> =
        vec![(FP_START, 0, 0), (FP_PARENT_EVENT, 0, 0), (ALL, 0, 0)];
    let mut to_done = Vec::new();
    for iter in 0..iters {
        let mode = mode_for(iter);
        // Failpoint mode: arm before the root starts (kill -9 of the previous process), so
        // the first flush at the site in this iteration is the injected one.
        if let Mode::Failpoint(fp) = mode
            && let Err(e) = leg.restart(Some(&failpoint_spec(fp))).await
        {
            leg.fail(iter, "armed restart", e);
        }
        let api = leg.api();
        let root = match api
            .start(
                &langs::workflow_name(ROLES[0], &spec.workflow),
                &chain_input(&spec, iter),
                &format!("crash-chain-{seed:x}-{iter}"),
            )
            .await
        {
            Ok(r) => r,
            Err(e) => leg.fail(iter, "start root", e),
        };
        let detail = match mode {
            Mode::Random => {
                random_iters += 1;
                // The root start is acked, so every kill crashes a run that exists.
                let n = rng.range(1, 3);
                let mut delays = Vec::new();
                for _ in 0..n {
                    let d = rng.kill_delay(CHAIN_BUSY_MS, 1500);
                    delays.push(d);
                    tokio::time::sleep(Duration::from_millis(d)).await;
                    // Whether this kill lands while the chain is still in flight.
                    let root_status = leg.api().run_status_now(&root).await;
                    if root_status.as_deref().is_some_and(|st| !is_terminal(st)) {
                        in_flight_kills += 1;
                    }
                    if let Err(e) = leg.restart(None).await {
                        leg.fail(iter, "restart", e);
                    }
                }
                format!("random kills at {delays:?} ms")
            }
            Mode::Failpoint(fp) => {
                fp_iters += 1;
                let deadline = Instant::now() + FAULT_LIMIT;
                let hit = loop {
                    let reached = if fp == FP_START {
                        // Paused right after the flush that journaled the ChildStarted.
                        api.history(&root)
                            .await
                            .is_ok_and(|h| kind_count(&h, "child_started") > 0)
                    } else {
                        leg.server().faults() > 0
                    };
                    if reached {
                        break true;
                    }
                    if Instant::now() > deadline {
                        break false;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                };
                let e = hits.iter_mut().find(|h| h.0 == fp).expect("mode");
                e.1 += 1;
                if hit {
                    e.2 += 1;
                }
                // Paused: any delay still kills at the site; printed: kill at once.
                let d = if fp == FP_START { rng.range(0, 20) } else { 0 };
                tokio::time::sleep(Duration::from_millis(d)).await;
                if let Err(e) = leg.restart(None).await {
                    leg.fail(iter, "restart after failpoint", e);
                }
                format!(
                    "failpoint {fp} {} +{d} ms",
                    if hit { "hit" } else { "NOT hit" }
                )
            }
        };
        let t = Instant::now();
        if let Err(e) = check_chain(&leg, &root, &expected, iter + 1).await {
            leg.fail(iter, &detail, e);
        }
        let took = t.elapsed();
        to_done.push(took);
        eprintln!("C-CRASH-01 iter {iter}: {detail}; ok {took:.2?} after last restart");
    }
    let report = format!(
        "C-CRASH-01 report: {iters} iterations (seed {seed:#018x}), {random_iters} random-kill / \
         {fp_iters} failpoint iterations, {} kill -9 ({in_flight_kills} random kills with the root \
         still running), {} restarts, failpoint hits {}, median time to completion after the \
         last restart {:.2?}, total {:.1?}",
        leg.kills,
        leg.restarts,
        hits.iter()
            .map(|(f, armed, hit)| format!("{f} {hit}/{armed}"))
            .collect::<Vec<_>>()
            .join(", "),
        median(&mut to_done),
        started.elapsed()
    );
    eprintln!("{report}");
    // Every failpoint mode actually crashed at its site at least once (when it ran).
    for (f, armed, hit) in &hits {
        assert!(*armed == 0 || *hit > 0, "failpoint mode {f} never hit");
    }
    leg.finish().await;
}

/// C-CRASH-01 checks for one root; `roots` = chain roots started so far.
async fn check_chain(
    leg: &Leg,
    root: &RunId,
    expected: &Value,
    roots: usize,
) -> Result<(), String> {
    let api = leg.api();
    let out = api.terminal(root, COMPLETE_LIMIT).await?;
    if out["status"] != "completed" || out["result"] != *expected {
        return Err(format!(
            "root {root}: {out} (expected completed {expected})"
        ));
    }
    let h_root = api.history(root).await?;
    let mid = only_child(root, &h_root, leg.partitions)?;
    let h_mid = api.history(&mid).await?;
    let leaf = only_child(&mid, &h_mid, leg.partitions)?;
    let h_leaf = api.history(&leaf).await?;
    for (who, h) in [("root", &h_root), ("python", &h_mid)] {
        let n = kind_count(h, "child_completed");
        if n != 1 {
            return Err(format!(
                "{who} history has {n} child_completed (expected 1)"
            ));
        }
    }
    for (who, h) in [
        ("root", &h_root),
        ("python", &h_mid),
        ("rust leaf", &h_leaf),
    ] {
        let starts = kind_count(h, "run_started");
        let completed = kind_count(h, "run_completed");
        if starts != 1 || completed != 1 || terminal_count(h) != 1 {
            return Err(format!(
                "{who}: {starts} run_started / {completed} run_completed / {} terminal \
                 (expected 1 / 1 / 1): {:?}",
                terminal_count(h),
                kinds(h)
            ));
        }
    }
    let mid_run = api.run(&mid).await?;
    let leaf_run = api.run(&leaf).await?;
    for (who, r, wf, res) in [
        (
            "python",
            &mid_run,
            "py_conf_parent_chain",
            json!({"child": {"echo": {"next": []}}}),
        ),
        (
            "rust leaf",
            &leaf_run,
            "rs_conf_leaf_echo",
            json!({"echo": {"next": []}}),
        ),
    ] {
        if r["status"] != "completed" || r["workflow"] != wf || r["result"] != res {
            return Err(format!("{who}: {r}"));
        }
    }
    // Exactly one run per level per root: the listing holds one run per root so far,
    // including the derived ids, and everything is terminal.
    for (wf, id) in [("py_conf_parent_chain", mid), ("rs_conf_leaf_echo", leaf)] {
        let runs = api.list(wf).await?;
        if runs.len() != roots || !runs.iter().any(|(r, _)| *r == id) {
            return Err(format!(
                "{wf}: {} runs listed for {roots} roots (derived {id} listed: {})",
                runs.len(),
                runs.iter().any(|(r, _)| *r == id)
            ));
        }
    }
    check_all_terminal(&api).await
}

/// Terminal events in a history (cancellation is a `run_failed` with failure type
/// `cancelled`).
fn terminal_count(h: &[Value]) -> usize {
    [
        "run_completed",
        "run_failed",
        "run_terminated",
        "run_continued_as_new",
    ]
    .iter()
    .map(|k| kind_count(h, k))
    .sum()
}

fn kinds(h: &[Value]) -> Vec<String> {
    h.iter()
        .map(|e| e["kind"].as_str().unwrap_or("?").to_owned())
        .collect()
}

async fn check_all_terminal(api: &Api) -> Result<(), String> {
    for wf in [
        "ts_conf_parent_chain",
        "py_conf_parent_chain",
        "rs_conf_leaf_echo",
        "rs_conf_leaf_sleep",
    ] {
        for (id, status) in api.list(wf).await? {
            if !is_terminal(&status) {
                return Err(format!("{wf} run {id} left {status}"));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------
// C-CRASH-02
// ---------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum CancelKill {
    Before,
    After,
    None,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns the failpoint ostro binary and kills it; slow (09-18 crash leg)"]
async fn crash_cancel_propagates() {
    let started = Instant::now();
    let iters = iterations();
    let seed = fresh_seed();
    eprintln!("C-CRASH-02: {iters} iterations, seed {seed:#018x}");
    let spec = load("C-CRASH-02");
    assert_eq!(spec.expect.status, "cancelled");
    let mut leg = Leg::new("crash-cancel", seed).await;
    let mut rng = Rng(seed);
    let mut counts = [0u64; 3];
    let mut mid_cascade_kills = 0u64;
    let mut to_done = Vec::new();
    for iter in 0..iters {
        let kill = match iter % 3 {
            0 => CancelKill::Before,
            1 => CancelKill::After,
            _ => CancelKill::None,
        };
        counts[iter % 3] += 1;
        let api = leg.api();
        let root = match api
            .start(
                &langs::workflow_name(ROLES[0], &spec.workflow),
                &chain_input(&spec, iter),
                &format!("crash-cancel-{seed:x}-{iter}"),
            )
            .await
        {
            Ok(r) => r,
            Err(e) => leg.fail(iter, "start root", e),
        };
        let leaf = match wait_sleeping_leaf(&leg, &root).await {
            Ok(l) => l,
            Err(e) => leg.fail(iter, "wait for the sleeping leaf", e),
        };
        let mut detail = format!("{kill:?}");
        match kill {
            CancelKill::Before => {
                if let Err(e) = leg.restart(None).await {
                    leg.fail(iter, "restart before cancel", e);
                }
                if let Err(e) = leg.api().cancel(&root).await {
                    leg.fail(iter, "cancel", e);
                }
            }
            CancelKill::After => {
                if let Err(e) = api.cancel(&root).await {
                    leg.fail(iter, "cancel", e);
                }
                let d = rng.kill_delay(CASCADE_BUSY_MS, 500);
                tokio::time::sleep(Duration::from_millis(d)).await;
                if leg
                    .api()
                    .run_status_now(&leaf)
                    .await
                    .as_deref()
                    .is_some_and(|st| !is_terminal(st))
                {
                    mid_cascade_kills += 1;
                }
                if let Err(e) = leg.restart(None).await {
                    leg.fail(iter, "restart after cancel", e);
                }
                detail = format!("After +{d} ms");
            }
            CancelKill::None => {
                if let Err(e) = api.cancel(&root).await {
                    leg.fail(iter, "cancel", e);
                }
            }
        }
        let t = Instant::now();
        if let Err(e) = check_cancelled(&leg, &root, &leaf, iter + 1).await {
            leg.fail(iter, &detail, e);
        }
        let took = t.elapsed();
        to_done.push(took);
        eprintln!("C-CRASH-02 iter {iter}: {detail}; ok {took:.2?} after the last restart/cancel");
    }
    eprintln!(
        "C-CRASH-02 report: {iters} iterations (seed {seed:#018x}), kill before cancel {} / \
         after cancel {} ({mid_cascade_kills} with the leaf not yet cancelled) / no kill {}, {} kill \
         -9, {} restarts, median time to all-cancelled \
         {:.2?}, total {:.1?}",
        counts[0],
        counts[1],
        counts[2],
        leg.kills,
        leg.restarts,
        median(&mut to_done),
        started.elapsed()
    );
    leg.finish().await;
}

/// Resolves root -> Python mid -> Rust leaf and waits until the leaf journaled
/// `timer_started`.
async fn wait_sleeping_leaf(leg: &Leg, root: &RunId) -> Result<RunId, String> {
    let api = leg.api();
    let deadline = Instant::now() + COMPLETE_LIMIT;
    let mut chain = vec![*root];
    loop {
        let last = *chain.last().expect("non-empty");
        let h = api.history(&last).await.unwrap_or_default();
        if chain.len() < 3 {
            if kind_count(&h, "child_started") == 1 {
                chain.push(only_child(&last, &h, leg.partitions)?);
                continue;
            }
        } else if kind_count(&h, "timer_started") >= 1 {
            return Ok(last);
        }
        if Instant::now() > deadline {
            return Err(format!(
                "chain {chain:?} did not reach a sleeping leaf within {COMPLETE_LIMIT:?}; \
                 last history {:?}",
                kinds(&h)
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

async fn check_cancelled(
    leg: &Leg,
    root: &RunId,
    leaf: &RunId,
    roots: usize,
) -> Result<(), String> {
    let api = leg.api();
    let out = api.terminal(root, CANCEL_LIMIT).await?;
    if out["status"] != "cancelled" {
        return Err(format!("root {root}: {out} (expected cancelled)"));
    }
    let h_root = api.history(root).await?;
    let mid = only_child(root, &h_root, leg.partitions)?;
    // The root ends as soon as its own cancellation resolves; the descendants follow
    // through the cascade, so their histories are read once they are terminal.
    for (who, id) in [("python", mid), ("rust leaf", *leaf)] {
        let v = api.terminal(&id, CANCEL_LIMIT).await?;
        if v["status"] != "cancelled" {
            return Err(format!("{who} {id}: {v} (expected cancelled)"));
        }
    }
    let h_mid = api.history(&mid).await?;
    let derived_leaf = only_child(&mid, &h_mid, leg.partitions)?;
    if derived_leaf != *leaf {
        return Err(format!("leaf {derived_leaf} != {leaf} seen sleeping"));
    }
    let h_leaf = api.history(leaf).await?;
    let cleanups = h_leaf
        .iter()
        .filter(|e| e["kind"] == "step_completed" && step_name(e) == Some("cleanup"))
        .count();
    if cleanups != 1 {
        return Err(format!(
            "leaf {leaf}: cleanup step journaled {cleanups} times (expected 1): {:?}",
            kinds(&h_leaf)
        ));
    }
    for (who, h) in [
        ("root", &h_root),
        ("python", &h_mid),
        ("rust leaf", &h_leaf),
    ] {
        let (starts, terminals) = (kind_count(h, "run_started"), terminal_count(h));
        let cancelled = h
            .iter()
            .filter(|e| e["kind"] == "run_failed" && e["details"]["failure"]["type"] == "cancelled")
            .count();
        if starts != 1 || terminals != 1 || cancelled != 1 {
            return Err(format!(
                "{who}: {starts} run_started / {terminals} terminal / {cancelled} cancelled \
                 run_failed (expected 1 / 1 / 1): {:?}",
                kinds(h)
            ));
        }
    }
    for (wf, id) in [("py_conf_parent_chain", mid), ("rs_conf_leaf_sleep", *leaf)] {
        let runs = api.list(wf).await?;
        if runs.len() != roots || !runs.iter().any(|(r, _)| *r == id) {
            return Err(format!(
                "{wf}: {} runs listed for {roots} roots",
                runs.len()
            ));
        }
    }
    check_all_terminal(&api).await
}

fn step_name(e: &Value) -> Option<&str> {
    e["details"]["name"].as_str()
}
