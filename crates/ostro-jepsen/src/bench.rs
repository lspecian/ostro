//! Bytes-on-disk verified benchmarks of the 1.0 configuration (14-23, QA-04, D-P14-21, D16).
//!
//! A step counts **only** when its run's `StepCompleted` was acknowledged and is found on
//! disk afterwards:
//!
//! - **single node** ([`BenchSpec::single`]): after the load the node is killed with
//!   `kill -9`, its WAL is scanned offline ([`verify_on_disk`]), then it is restarted on the
//!   same data dir and every acknowledged run is read back through the API (status
//!   `completed`, `steps` distinct `step_completed` events in its history);
//! - **RF=3** ([`BenchSpec::rf3`]): three loopback processes, `min_insync` 2; after the load
//!   the cluster is stopped (`kill -9`) and every replica's WAL is scanned offline: each
//!   acknowledged run needs its `steps` `StepCompleted` events on at least `min_insync`
//!   replicas.
//!
//! The configuration is the 1.0 one: token auth (a bootstrapped system admin creates tenant
//! [`TENANT`] and a tenant token; the load runs with the tenant token), the audit journal
//! on, tenant quotas set on the tenant (generous: on the admission path, never binding).
//!
//! **Load.** `clients` closed-loop clients each start a profile-A run (`trivial-steps`,
//! `{n: 10}`; profile B `{n: 1}`) and long-poll its result, then start the next one. The
//! clients run for `warmup + duration`; the measured window is `duration` after the warm-up.
//!
//! **Numbers.** `steps/s` = verified on-disk steps of the runs whose completion was
//! acknowledged inside the window / window length. Bytes on disk per step = growth of the
//! `wal/`, `state/`, `blobs/` and `audit/` directories (every node summed) from before the
//! load to after the stop / verified steps of every acknowledged run. **Commit latency:**
//! there is no Phase 12 commit-latency histogram (14-01 finding), so commit latency is
//! measured client-side as start request → start acknowledgement (the StartRun appended
//! and the HW past it: the 12-17 `bench_rf3` method); the server's
//! `ostro_apply_batch_seconds` p99 is scraped from `/metrics` as a side figure. **Client
//! ack latency** = start request → completed result received.
//!
//! **Thresholds** (≥ 10,000 steps/s single node; ≥ 5,000 steps/s and commit p99 < 20 ms
//! at RF=3) are asserted only with `OSTRO_BENCH_ENFORCE=1` (native NVMe box, D15). The
//! correctness property (every acknowledged run's steps on disk) is always asserted.
//!
//! Wall-clock reads in this module are benchmark instrumentation (`clock-exempt`).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ostro_repl_conformance::harness::inproc::Ack;
use ostro_repl_conformance::harness::process::{ProcCluster, ProcOptions, failpoint_bin};
use ostro_repl_conformance::harness::{ApiClient, ClusterHarness, ClusterSpec, DataDirView};
use ostro_repl_conformance::oracle::disk::{acked_on_disk, scan_partition};
use ostro_repl_conformance::scenarios::process::tools;
use ostro_repl_conformance::scenarios::process_p13::{HEAL_T, wait_healthy};
use ostro_types::command::decode_command;
use ostro_types::{Cmd, PartitionId, RunId};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::task::JoinSet;

use crate::report::workspace_root;
use crate::workload::pause;

/// `1` enables the throughput / latency thresholds.
pub const ENFORCE_ENV: &str = "OSTRO_BENCH_ENFORCE";
/// Measured window in seconds (default 60).
pub const SECS_ENV: &str = "OSTRO_BENCH_SECS";
/// Warm-up in seconds (default 10).
pub const WARMUP_ENV: &str = "OSTRO_BENCH_WARMUP_SECS";
/// Closed-loop clients (default 64).
pub const CLIENTS_ENV: &str = "OSTRO_BENCH_CLIENTS";
/// The `ostro` binary the nodes run (default: the release build when present).
pub const BIN_ENV: &str = "OSTRO_BENCH_BIN";
/// Report directory (default `<workspace>/target/bench`).
pub const OUT_ENV: &str = "OSTRO_BENCH_OUT";

/// The benchmark guest's workflow.
pub const WORKFLOW: &str = "trivial-steps";
/// The tenant the load runs as.
pub const TENANT: &str = "bench";
/// `EventKind::StepCompleted` (frozen journal code).
pub const STEP_COMPLETED: u32 = 16;
/// Single node: steps/s floor (NFR-2.5), enforced with `OSTRO_BENCH_ENFORCE=1`.
pub const SINGLE_MIN_STEPS_PER_S: f64 = 10_000.0;
/// RF=3: steps/s floor, enforced with `OSTRO_BENCH_ENFORCE=1`.
pub const RF3_MIN_STEPS_PER_S: f64 = 5_000.0;
/// RF=3: commit p99 ceiling (ms), enforced with `OSTRO_BENCH_ENFORCE=1`.
pub const RF3_MAX_COMMIT_P99_MS: f64 = 20.0;
/// Data subdirectories whose growth is reported.
pub const DISK_DIRS: [&str; 4] = ["wal", "state", "blobs", "audit"];

/// Bound on one API request.
const REQ_T: Duration = Duration::from_secs(10);

/// Whether `OSTRO_BENCH_ENFORCE=1`.
pub fn enforce() -> bool {
    std::env::var(ENFORCE_ENV).is_ok_and(|v| v.trim() == "1")
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.trim().parse().ok())
}

/// D16 profiles.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Profile {
    /// `trivial-steps` with 10 steps per run (the gate).
    A,
    /// `trivial-steps` with 1 step per run.
    B,
}

impl Profile {
    /// Steps per run.
    pub fn steps(self) -> u64 {
        match self {
            Profile::A => 10,
            Profile::B => 1,
        }
    }
}

/// What a benchmark does.
#[derive(Clone, Debug)]
pub struct BenchSpec {
    /// Report label.
    pub label: String,
    /// Processes: 1 (RF=1) or 3 (RF=3, `min_insync` 2).
    pub nodes: usize,
    /// D16 profile.
    pub profile: Profile,
    /// Closed-loop clients.
    pub clients: usize,
    /// Measured window.
    pub duration: Duration,
    /// Load before the window.
    pub warmup: Duration,
    /// Token auth (bootstrap admin, tenant token for the load).
    pub auth: bool,
    /// Audit journal on.
    pub audit: bool,
    /// Generous quotas on the tenant (needs `auth`).
    pub tenant_quotas: bool,
    /// Partitions.
    pub partitions: u16,
    /// Cluster seed (loopback range, ids).
    pub seed: u64,
    /// The `ostro` binary of the nodes (`None` = the harness's failpoint build).
    pub bin: Option<PathBuf>,
    /// Bound on waiting for acknowledged runs to complete after the load.
    pub drain_cap: Duration,
}

impl BenchSpec {
    fn base(label: &str, nodes: usize, partitions: u16, seed: u64) -> Self {
        Self {
            label: label.to_owned(),
            nodes,
            profile: Profile::A,
            clients: 64,
            duration: Duration::from_secs(60),
            warmup: Duration::from_secs(10),
            auth: true,
            audit: true,
            tenant_quotas: true,
            partitions,
            seed,
            bin: default_bin(),
            drain_cap: Duration::from_secs(120),
        }
    }

    /// One process, RF=1, 16 partitions (the single-node default).
    pub fn single() -> Self {
        Self::base("single node (RF=1)", 1, 16, 1423)
    }

    /// Three loopback processes, RF=3, `min_insync` 2, 4 partitions.
    pub fn rf3() -> Self {
        Self::base("3 processes (RF=3, min_insync 2)", 3, 4, 1424)
    }

    /// Apply `OSTRO_BENCH_SECS`, `OSTRO_BENCH_WARMUP_SECS`, `OSTRO_BENCH_CLIENTS`.
    pub fn with_env(mut self) -> Self {
        if let Some(s) = env_u64(SECS_ENV) {
            self.duration = Duration::from_secs(s.max(1));
        }
        if let Some(s) = env_u64(WARMUP_ENV) {
            self.warmup = Duration::from_secs(s);
        }
        if let Some(c) = env_u64(CLIENTS_ENV) {
            self.clients = usize::try_from(c.max(1)).unwrap_or(64);
        }
        self
    }

    /// The cluster configuration of this benchmark.
    pub fn rig(&self) -> RigConfig {
        RigConfig {
            nodes: self.nodes,
            partitions: self.partitions,
            seed: self.seed,
            auth: self.auth,
            audit: self.audit,
            tenant_quotas: self.tenant_quotas,
            bin: self.bin.clone(),
        }
    }
}

/// `OSTRO_BENCH_BIN`, else `$CARGO_TARGET_DIR/release/ostro`, else
/// `<workspace>/target/release/ostro`, else `None` (the failpoint build).
pub fn default_bin() -> Option<PathBuf> {
    if let Some(b) = std::env::var_os(BIN_ENV).filter(|b| !b.is_empty()) {
        return Some(PathBuf::from(b));
    }
    let mut cands = Vec::new();
    if let Some(t) = std::env::var_os("CARGO_TARGET_DIR").filter(|t| !t.is_empty()) {
        let t = PathBuf::from(t);
        let t = if t.is_absolute() {
            t
        } else {
            workspace_root().join(t)
        };
        cands.push(t.join("release").join("ostro"));
    }
    cands.push(
        workspace_root()
            .join("target")
            .join("release")
            .join("ostro"),
    );
    cands.into_iter().find(|p| p.is_file())
}

/// `<workspace>/target/bench` (`OSTRO_BENCH_OUT` overrides).
pub fn out_dir() -> PathBuf {
    match std::env::var_os(OUT_ENV) {
        Some(d) if !d.is_empty() => PathBuf::from(d),
        _ => workspace_root().join("target").join("bench"),
    }
}

// ---------------------------------------------------------------------------------
// Machine / binary provenance (W14)
// ---------------------------------------------------------------------------------

/// Machine load at one point.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Machine {
    /// `/proc/loadavg` (1, 5, 15 min).
    pub loadavg: String,
    /// Logical CPUs.
    pub nproc: usize,
    /// `MemAvailable` (kB).
    pub mem_available_kb: u64,
    /// Kernel (`/proc/sys/kernel/osrelease`).
    pub kernel: String,
}

impl Machine {
    /// Sample now.
    pub fn sample() -> Self {
        let read = |p: &str| std::fs::read_to_string(p).unwrap_or_default();
        let loadavg = read("/proc/loadavg")
            .split_whitespace()
            .take(3)
            .collect::<Vec<_>>()
            .join(" ");
        let mem_available_kb = read("/proc/meminfo")
            .lines()
            .find_map(|l| l.strip_prefix("MemAvailable:"))
            .and_then(|v| v.split_whitespace().next()?.parse().ok())
            .unwrap_or(0);
        Self {
            loadavg,
            nproc: std::thread::available_parallelism().map_or(0, usize::from),
            mem_available_kb,
            kernel: read("/proc/sys/kernel/osrelease").trim().to_owned(),
        }
    }
}

/// The binary under test and the source revision.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    /// The `ostro` binary the nodes ran.
    pub binary: String,
    /// Its sha256.
    pub binary_sha256: String,
    /// Release (`target/.../release/ostro`) or the failpoint build.
    pub build: String,
    /// `git rev-parse HEAD` of the workspace (`?` when unavailable).
    pub head: String,
}

impl Provenance {
    /// Of `bin` (`None` = the failpoint build).
    pub fn of(bin: Option<&Path>) -> Self {
        let path = bin.map(Path::to_path_buf).or_else(failpoint_bin);
        let (binary, binary_sha256) = match &path {
            Some(p) => (
                p.display().to_string(),
                std::fs::read(p)
                    .map(|b| {
                        ostro_server::tls::ca::sha256_fingerprint(&b)
                            .trim_start_matches("sha256:")
                            .to_owned()
                    })
                    .unwrap_or_else(|e| format!("? ({e})")),
            ),
            None => ("?".into(), "?".into()),
        };
        let head = std::process::Command::new("git")
            .arg("-C")
            .arg(workspace_root())
            .args(["rev-parse", "HEAD"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
            .unwrap_or_else(|| "?".into());
        Self {
            binary,
            binary_sha256,
            build: if bin.is_some() {
                "release (OSTRO_BENCH_BIN / target/release)".into()
            } else {
                "failpoint debug build (advisory numbers)".into()
            },
            head,
        }
    }
}

// ---------------------------------------------------------------------------------
// API client (bearer, leader hints, node rotation)
// ---------------------------------------------------------------------------------

/// One request's verb and body.
#[derive(Clone, Debug)]
pub enum Verb {
    /// `GET`.
    Get,
    /// `POST` with a JSON body.
    Post(Value),
    /// `PUT` with a JSON body.
    PutJson(Value),
    /// `PUT` with a wasm body.
    PutWasm(Vec<u8>),
    /// `DELETE`.
    Delete,
}

/// A REST client over every node's API with a bearer token. A 503 with a `leader_addr`
/// hint moves the shared target to that node; a node that does not answer rotates to the
/// next one. Clones share the target.
#[derive(Clone, Debug)]
pub struct Api {
    apis: Arc<Vec<ApiClient>>,
    cur: Arc<AtomicUsize>,
    token: Option<Arc<str>>,
}

impl Api {
    /// Over `apis` (non-empty), starting at index `first`.
    pub fn new(apis: Vec<ApiClient>, first: usize, token: Option<&str>) -> Self {
        let n = apis.len().max(1);
        Self {
            apis: Arc::new(apis),
            cur: Arc::new(AtomicUsize::new(first % n)),
            token: token.map(Arc::from),
        }
    }

    /// The same nodes, target and bearer over another HTTP client (p14-fixups-4: the soak's
    /// bounded connection pool, [`crate::soak::driver_http`]).
    pub fn with_http(&self, http: &reqwest::Client) -> Self {
        Self {
            apis: Arc::new(
                self.apis
                    .iter()
                    .map(|a| ApiClient {
                        base_url: a.base_url.clone(),
                        http: http.clone(),
                    })
                    .collect(),
            ),
            cur: Arc::new(AtomicUsize::new(self.cur.load(Ordering::Relaxed))),
            token: self.token.clone(),
        }
    }

    /// The same nodes and target with another bearer.
    pub fn with_token(&self, token: Option<&str>) -> Self {
        Self {
            apis: Arc::clone(&self.apis),
            cur: Arc::new(AtomicUsize::new(self.cur.load(Ordering::Relaxed))),
            token: token.map(Arc::from),
        }
    }

    fn follow(&self, failed: usize, hint: Option<&str>) {
        let n = self.apis.len();
        if n == 0 {
            return;
        }
        let next = hint
            .and_then(|addr| {
                let url = format!("http://{addr}");
                self.apis.iter().position(|a| a.base_url == url)
            })
            .filter(|i| *i != failed)
            .unwrap_or((failed + 1) % n);
        let _ = self
            .cur
            .compare_exchange(failed, next, Ordering::Relaxed, Ordering::Relaxed);
    }

    /// One request at the current target: `Ok((status, body))` for any HTTP answer, `Err`
    /// for no answer. A 503 hint or a silent node moves the target.
    pub async fn call(
        &self,
        verb: &Verb,
        path: &str,
        limit: Duration,
    ) -> Result<(u16, Value), String> {
        let i = self.cur.load(Ordering::Relaxed) % self.apis.len().max(1);
        let Some(api) = self.apis.get(i) else {
            return Err("no API".into());
        };
        let url = format!("{}{path}", api.base_url);
        let rb = match verb {
            Verb::Get => api.http.get(url),
            Verb::Post(b) => api.http.post(url).json(b),
            Verb::PutJson(b) => api.http.put(url).json(b),
            Verb::PutWasm(b) => api
                .http
                .put(url)
                .header("content-type", "application/wasm")
                .body(b.clone()),
            Verb::Delete => api.http.delete(url),
        };
        let mut rb = rb.timeout(limit);
        if let Some(t) = &self.token {
            rb = rb.bearer_auth(t);
        }
        match rb.send().await {
            Ok(r) => {
                let st = r.status().as_u16();
                let body = r.bytes().await.map_err(|e| e.to_string())?;
                let v: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                if st == 503 {
                    self.follow(i, v["error"]["details"]["leader_addr"].as_str());
                }
                Ok((st, v))
            }
            Err(e) => {
                self.follow(i, None);
                Err(e.to_string())
            }
        }
    }

    /// [`Api::call`] retried on no answer and on 5xx until a non-5xx answer or `cap`.
    pub async fn call_retry(
        &self,
        verb: &Verb,
        path: &str,
        cap: Duration,
    ) -> Result<(u16, Value), String> {
        let t0 = Instant::now(); // clock-exempt: benchmark timing
        let mut last = String::new();
        while t0.elapsed() < cap {
            match self.call(verb, path, REQ_T).await {
                Ok((st, v)) if st < 500 => return Ok((st, v)),
                Ok((st, v)) => last = format!("http {st}: {v}"),
                Err(e) => last = e,
            }
            pause(Duration::from_millis(200)).await;
        }
        Err(format!("{path}: {last}"))
    }

    /// Start `workflow` with `input` and idempotency key `key`: the acknowledgement, or the
    /// answer that refused it.
    pub async fn start(&self, workflow: &str, input: &Value, key: &str) -> Result<Ack, String> {
        let body = json!({ "input": input, "idempotency_key": key });
        let path = format!("/v1/workflows/{workflow}/runs");
        let (st, v) = self.call(&Verb::Post(body), &path, REQ_T).await?;
        if !(200..300).contains(&st) {
            return Err(format!("http {st}: {v}"));
        }
        let run_id = v["run_id"]
            .as_str()
            .ok_or_else(|| format!("http {st} without run_id: {v}"))?;
        let id: RunId = run_id
            .parse()
            .map_err(|e| format!("run id {run_id}: {e:?}"))?;
        Ok(Ack {
            run_id: run_id.to_owned(),
            partition: id.partition().0,
        })
    }

    /// Long-poll `run`'s result for up to `wait`: `Ok(Some((status, result)))` once it is
    /// terminal, `Ok(None)` while it runs, `Err` on no answer / an error status.
    pub async fn result(
        &self,
        run: &str,
        wait: Duration,
    ) -> Result<Option<(String, Value)>, String> {
        let path = format!("/v1/runs/{run}/result?wait={}ms", wait.as_millis());
        let (st, v) = self.call(&Verb::Get, &path, wait + REQ_T).await?;
        if st != 200 {
            return Err(format!("http {st}: {v}"));
        }
        if v["completed"].as_bool() == Some(true) {
            let status = v["status"].as_str().unwrap_or("").to_owned();
            return Ok(Some((
                status,
                v.get("result").cloned().unwrap_or(Value::Null),
            )));
        }
        Ok(None)
    }

    /// Every `(seq, kind)` of `run`'s journal through the history API.
    pub async fn history(&self, run: &str) -> Result<Vec<(u64, String)>, String> {
        let mut out = Vec::new();
        let mut from = 0u64;
        loop {
            let path = format!("/v1/runs/{run}/history?from_seq={from}&limit=1000");
            let (st, v) = self
                .call_retry(&Verb::Get, &path, Duration::from_secs(30))
                .await?;
            if st != 200 {
                return Err(format!("history http {st}: {v}"));
            }
            for e in v["events"].as_array().cloned().unwrap_or_default() {
                out.push((
                    e["seq"].as_u64().unwrap_or(u64::MAX),
                    e["kind"].as_str().unwrap_or("").to_owned(),
                ));
            }
            match v["next_from_seq"].as_u64() {
                Some(n) if n > from => from = n,
                _ => break,
            }
        }
        Ok(out)
    }

    /// The seqs of `run`'s `step_completed` events (history API).
    pub async fn history_steps(&self, run: &str) -> Result<Vec<u64>, String> {
        Ok(self
            .history(run)
            .await?
            .into_iter()
            .filter(|(_, k)| k == "step_completed")
            .map(|(s, _)| s)
            .collect())
    }
}

// ---------------------------------------------------------------------------------
// The 1.0 cluster (auth, audit, tenant quotas)
// ---------------------------------------------------------------------------------

/// How the benchmark / soak cluster is configured.
#[derive(Clone, Debug)]
pub struct RigConfig {
    /// Processes (1 = RF=1, 3 = RF=3 with `min_insync` 2).
    pub nodes: usize,
    /// Partitions.
    pub partitions: u16,
    /// Seed (loopback range, cluster id).
    pub seed: u64,
    /// Token auth.
    pub auth: bool,
    /// Audit journal.
    pub audit: bool,
    /// Generous tenant quotas (needs `auth`).
    pub tenant_quotas: bool,
    /// Node binary (`None` = failpoint build).
    pub bin: Option<PathBuf>,
}

/// Quotas that sit on every admission path without ever binding.
pub fn generous_quotas(tenant: &str) -> Value {
    json!({
        "id": tenant,
        "weight": 1,
        "max_concurrency": 1_000_000u64,
        "fuel_per_s": 1_000_000_000_000_000u64,
        "fuel_burst": 1_000_000_000_000_000u64,
        "memory_bytes": 1u64 << 42,
        "event_rate": { "limit": 100_000_000u64, "period_ms": 1000, "burst": 100_000_000u64 },
    })
}

/// A started cluster in the 1.0 configuration.
pub struct Rig {
    /// The processes.
    pub cluster: ProcCluster,
    /// The bootstrap system-admin bearer (`None` without auth).
    pub admin: Option<String>,
    /// The bearer of the load: the tenant token (or the admin without quotas).
    pub token: Option<String>,
    /// The tenant of the load (`None` without auth / quotas).
    pub tenant: Option<String>,
    /// `min_insync` of the spec.
    pub min_insync: usize,
    /// Binary and HEAD.
    pub provenance: Provenance,
    /// Remarks (what was configured).
    pub notes: Vec<String>,
}

impl Rig {
    /// Start the cluster, wait until healthy (full ISRs at RF=3), bootstrap the tenant and
    /// its token.
    pub async fn start(cfg: &RigConfig) -> Result<Rig, String> {
        let mut opts = ProcOptions {
            bin: cfg.bin.clone(),
            ..ProcOptions::default()
        };
        let mut notes = Vec::new();
        let admin = if cfg.auth {
            let (_, _, b) = ostro_server::p14::tokens::generate_os()
                .map_err(|e| format!("OS random source: {e:?}"))?;
            let bearer = b.expose().to_owned();
            opts.env.push(("OSTRO_AUTH__MODE".into(), "token".into()));
            opts.env
                .push(("OSTRO_BOOTSTRAP_TOKEN".into(), bearer.clone()));
            notes.push("auth: token mode, bootstrap system admin".into());
            Some(bearer)
        } else {
            notes.push("auth: none".into());
            None
        };
        opts.env.push((
            "OSTRO_AUDIT__ENABLED".into(),
            if cfg.audit { "true" } else { "false" }.into(),
        ));
        notes.push(format!(
            "audit journal: {}",
            if cfg.audit { "on" } else { "off" }
        ));
        let (rf, min_insync) = if cfg.nodes >= 3 { (3, 2) } else { (1, 1) };
        let spec = ClusterSpec {
            nodes: cfg.nodes,
            rf,
            min_insync,
            partitions: cfg.partitions,
            seed: cfg.seed,
            ..ClusterSpec::default()
        };
        let provenance = Provenance::of(cfg.bin.as_deref());
        let c = ProcCluster::start_with(spec, opts)
            .await
            .map_err(|e| format!("start cluster: {e}"))?;
        wait_healthy(&c, &[], HEAL_T, "start")
            .await
            .map_err(|f| format!("{}: {}", f.oracle, f.detail))?;
        if cfg.nodes >= 3 {
            let t = Instant::now(); // clock-exempt: benchmark timing
            while tools::published_all(&c)
                .iter()
                .any(|a| a.leader == 0 || a.isr.len() != a.replicas.len())
            {
                if t.elapsed() > Duration::from_secs(90) {
                    return Err("ISRs never filled".into());
                }
                pause(Duration::from_millis(100)).await;
            }
        }
        let mut rig = Rig {
            cluster: c,
            admin: admin.clone(),
            token: admin.clone(),
            tenant: None,
            min_insync,
            provenance,
            notes,
        };
        if admin.is_some() && cfg.tenant_quotas {
            let api = rig.admin_api();
            let (st, v) = api
                .call_retry(
                    &Verb::Post(generous_quotas(TENANT)),
                    "/v1/tenants",
                    Duration::from_secs(60),
                )
                .await?;
            if !(200..300).contains(&st) && st != 409 {
                return Err(format!("create tenant {TENANT}: http {st}: {v}"));
            }
            let body = json!({
                "name": "bench-load",
                "description": "14-23 benchmark / soak load",
                "scopes": ["deploy", "trigger", "read", "operate"],
                "tenant": TENANT,
                "no_expiry": true,
            });
            let (st, v) = api
                .call_retry(
                    &Verb::Post(body),
                    "/v1/auth/tokens",
                    Duration::from_secs(60),
                )
                .await?;
            let bearer = v["bearer"].as_str().filter(|_| (200..300).contains(&st));
            let Some(bearer) = bearer else {
                return Err(format!("create tenant token: http {st}: {v}"));
            };
            rig.token = Some(bearer.to_owned());
            rig.tenant = Some(TENANT.to_owned());
            rig.notes.push(format!(
                "tenant {TENANT} with generous quotas (concurrency, fuel, memory, event rate); the load runs with its token"
            ));
        }
        Ok(rig)
    }

    /// Every node's API client.
    pub fn apis(&self) -> Vec<ApiClient> {
        self.cluster
            .node_ids()
            .into_iter()
            .map(|n| self.cluster.client(n))
            .collect()
    }

    /// A client with the load's bearer, starting at node index `first`.
    pub fn api(&self, first: usize) -> Api {
        Api::new(self.apis(), first, self.token.as_deref())
    }

    /// A client with the system-admin bearer.
    pub fn admin_api(&self) -> Api {
        Api::new(self.apis(), 0, self.admin.as_deref())
    }

    /// Upload guest `guest` with the load's bearer (a tenant token deploys into its tenant).
    pub async fn deploy(&self, guest: &str) -> Result<(), String> {
        let wasm = ostro_repl_conformance::harness::inproc::guest_wasm(guest)
            .map_err(|e| format!("guest {guest}: {e}"))?;
        let sha = ostro_server::tls::ca::sha256_fingerprint(&wasm)
            .trim_start_matches("sha256:")
            .to_owned();
        let api = self.api(0);
        let (st, v) = api
            .call_retry(
                &Verb::PutWasm(wasm),
                &format!("/v1/modules/{sha}"),
                Duration::from_secs(90),
            )
            .await?;
        if (200..300).contains(&st) {
            Ok(())
        } else {
            Err(format!("deploy {guest}: http {st}: {v}"))
        }
    }

    /// Bytes per data subdirectory ([`DISK_DIRS`] plus `snapshots`, `meta`), every node
    /// summed.
    pub fn disk_usage(&self) -> BTreeMap<String, u64> {
        let mut out = BTreeMap::new();
        for n in self.cluster.node_ids() {
            for (k, v) in dir_sizes(&self.cluster.node_disk(n).root) {
                *out.entry(k).or_default() += v;
            }
        }
        out
    }
}

/// Apparent bytes of every file under `p`.
pub fn tree_bytes(p: &Path) -> u64 {
    let Ok(rd) = std::fs::read_dir(p) else {
        return 0;
    };
    rd.flatten()
        .map(|e| match e.file_type() {
            Ok(t) if t.is_dir() => tree_bytes(&e.path()),
            Ok(_) => e.metadata().map_or(0, |m| m.len()),
            Err(_) => 0,
        })
        .sum()
}

/// Bytes of [`DISK_DIRS`], `snapshots` and `meta` under data dir `root`.
pub fn dir_sizes(root: &Path) -> BTreeMap<String, u64> {
    DISK_DIRS
        .iter()
        .chain(["snapshots", "meta"].iter())
        .map(|d| ((*d).to_owned(), tree_bytes(&root.join(d))))
        .collect()
}

// ---------------------------------------------------------------------------------
// Metrics scrape
// ---------------------------------------------------------------------------------

/// `GET /metrics` of `api` (an unauthenticated route): the exposition text.
pub async fn scrape(api: &ApiClient) -> Option<String> {
    let r = api
        .http
        .get(format!("{}/metrics", api.base_url))
        .timeout(REQ_T)
        .send()
        .await
        .ok()?;
    if !r.status().is_success() {
        return None;
    }
    r.text().await.ok()
}

/// `(labels, value)` of every sample of metric `name` (exact name, no suffix).
pub fn samples<'a>(text: &'a str, name: &str) -> Vec<(&'a str, f64)> {
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .filter_map(|l| {
            let rest = l.strip_prefix(name)?;
            let (labels, value) = if let Some(r) = rest.strip_prefix('{') {
                r.split_once('}')?
            } else if rest.starts_with(' ') {
                ("", rest)
            } else {
                return None;
            };
            Some((labels, value.split_whitespace().next()?.parse().ok()?))
        })
        .collect()
}

/// The `q` quantile (seconds) of histogram `name` with every label set merged: the upper
/// bound of the first bucket holding it (`None` without observations).
pub fn histogram_quantile(text: &str, name: &str, q: f64) -> Option<f64> {
    let mut buckets: BTreeMap<u64, f64> = BTreeMap::new();
    let mut inf = 0.0;
    for (labels, v) in samples(text, &format!("{name}_bucket")) {
        let Some(le) = labels
            .split(',')
            .find_map(|kv| kv.trim().strip_prefix("le="))
            .map(|s| s.trim_matches('"'))
        else {
            continue;
        };
        if le == "+Inf" {
            inf += v;
        } else if let Ok(b) = le.parse::<f64>() {
            *buckets.entry((b * 1e9) as u64).or_default() += v;
        }
    }
    if inf <= 0.0 {
        return None;
    }
    let want = q * inf;
    buckets
        .iter()
        .find(|(_, c)| **c >= want)
        .map(|(b, _)| *b as f64 / 1e9)
        .or(Some(f64::INFINITY))
}

// ---------------------------------------------------------------------------------
// Verification on disk
// ---------------------------------------------------------------------------------

/// The offline disk check of a set of acknowledged runs.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskCheck {
    /// Acknowledged runs checked.
    pub runs: usize,
    /// Runs with their `StartRun` and all their `StepCompleted` events on at least
    /// `min_insync` replicas.
    pub verified: usize,
    /// `StepCompleted` events of verified runs (distinct seqs, counted once per run).
    pub steps: u64,
    /// Replica scans that were trimmed below their start (reported).
    pub trimmed_scans: usize,
    /// Run ids that failed the check.
    pub failed_runs: BTreeSet<String>,
    /// Every violation (first 50 kept).
    pub violations: Vec<String>,
    /// Violations beyond the kept ones.
    pub more_violations: usize,
}

impl DiskCheck {
    fn violation(&mut self, v: String) {
        if self.violations.len() < 50 {
            self.violations.push(v);
        } else {
            self.more_violations += 1;
        }
    }

    /// No violation at all and every run verified.
    pub fn ok(&self) -> bool {
        self.violations.is_empty() && self.more_violations == 0 && self.verified == self.runs
    }
}

/// Per run id bytes, the `StepCompleted` seqs in one verified WAL scan.
type StepsByRun = BTreeMap<Vec<u8>, BTreeSet<u64>>;

/// `StepCompleted` seqs per run in one replica's verified WAL scan of `p`; the flag is
/// whether the scan was trimmed.
fn steps_on_disk(view: &DataDirView, p: u16) -> Result<(StepsByRun, bool), String> {
    let scan = scan_partition(view, PartitionId(p))?;
    let mut out = StepsByRun::new();
    for (_, c) in scan.commands() {
        if let Ok(cmd) = decode_command(c)
            && let Some(Cmd::EventsAppended(e)) = cmd.cmd
        {
            let seqs = out.entry(e.run_id.clone()).or_default();
            for ev in e.events.iter().filter(|ev| ev.kind == STEP_COMPLETED) {
                seqs.insert(ev.seq);
            }
        }
    }
    Ok((out, scan.is_trimmed()))
}

/// D16 / D-P14-21: every acknowledged run in `acks` has its `StartRun` (the Phase 12
/// `acked_on_disk` oracle) and exactly `steps` distinct `StepCompleted` events in the
/// verified WAL scans of at least `min_insync` of `views` (the nodes must be stopped).
pub fn verify_on_disk(
    views: &[DataDirView],
    acks: &[Ack],
    min_insync: usize,
    steps: u64,
) -> DiskCheck {
    let mut out = DiskCheck {
        runs: acks.len(),
        ..DiskCheck::default()
    };
    let start_missing = acked_on_disk(views, acks, min_insync).violations;
    for v in &start_missing {
        out.violation(format!("StartRun: {v}"));
    }
    let parts: BTreeSet<u16> = acks.iter().map(|a| a.partition).collect();
    // node -> partition -> run -> seqs
    let mut per_node: Vec<BTreeMap<u16, StepsByRun>> = Vec::new();
    for v in views {
        let mut m = BTreeMap::new();
        for p in &parts {
            match steps_on_disk(v, *p) {
                Ok((s, trimmed)) => {
                    if trimmed {
                        out.trimmed_scans += 1;
                    }
                    m.insert(*p, s);
                }
                Err(e) => out.violation(format!("node {} partition {p}: scan failed: {e}", v.node)),
            }
        }
        per_node.push(m);
    }
    for a in acks {
        let Ok(run) = a.run_id.parse::<RunId>() else {
            out.violation(format!("run {}: unparsable id", a.run_id));
            out.failed_runs.insert(a.run_id.clone());
            continue;
        };
        let key = run.as_bytes().to_vec();
        let counts: Vec<usize> = per_node
            .iter()
            .map(|m| {
                m.get(&a.partition)
                    .and_then(|r| r.get(&key))
                    .map_or(0, BTreeSet::len)
            })
            .collect();
        let holders = counts.iter().filter(|c| **c as u64 == steps).count();
        let start_ok = !start_missing.iter().any(|v| v.contains(&a.run_id));
        if counts.iter().any(|c| *c as u64 > steps) {
            out.violation(format!(
                "run {} (partition {}): more than {steps} StepCompleted on a replica: {counts:?}",
                a.run_id, a.partition
            ));
            out.failed_runs.insert(a.run_id.clone());
        }
        if holders >= min_insync && start_ok {
            out.verified += 1;
            out.steps += steps;
        } else {
            out.failed_runs.insert(a.run_id.clone());
            if holders < min_insync {
                out.violation(format!(
                    "run {} (partition {}): {steps} StepCompleted on {holders} replica(s) < {min_insync} (per replica {counts:?})",
                    a.run_id, a.partition
                ));
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------------
// Percentiles
// ---------------------------------------------------------------------------------

/// The `q` quantile of `v` (µs) by sorting, in ms.
pub fn pct_ms(v: &[u64], q: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    let mut s = v.to_vec();
    s.sort_unstable();
    let idx = ((q * s.len() as f64).ceil() as usize).clamp(1, s.len()) - 1;
    s[idx] as f64 / 1000.0
}

// ---------------------------------------------------------------------------------
// The benchmark
// ---------------------------------------------------------------------------------

/// One run of the load.
#[derive(Clone, Debug)]
struct RunSample {
    ack: Ack,
    /// Start request sent (since load start).
    sent: Duration,
    /// Start request → start acknowledged (µs).
    ack_us: u64,
    /// Completion received (since load start).
    done: Option<Duration>,
    /// Start request → completed result (µs).
    done_us: Option<u64>,
    /// Terminal status seen.
    status: Option<String>,
}

/// The result of one benchmark (`target/bench/<name>.json`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct BenchReport {
    /// Spec label.
    pub label: String,
    /// Processes.
    pub nodes: usize,
    /// `min_insync`.
    pub min_insync: usize,
    /// Partitions.
    pub partitions: u16,
    /// Profile.
    pub profile: String,
    /// Steps per run.
    pub steps_per_run: u64,
    /// Closed-loop clients.
    pub clients: usize,
    /// Warm-up (s).
    pub warmup_s: f64,
    /// Measured window (s).
    pub window_s: f64,
    /// Auth / audit / quotas configuration remarks.
    pub config: Vec<String>,
    /// Binary and HEAD.
    pub provenance: Provenance,
    /// Machine load before the cluster started.
    pub machine_start: Machine,
    /// Machine load after the verification.
    pub machine_end: Machine,
    /// Runs whose start was acknowledged (warm-up and window).
    pub acked_runs: usize,
    /// Of those, completed (status `completed` seen).
    pub completed_runs: usize,
    /// Of those, verified on disk.
    pub verified_runs: usize,
    /// Runs whose completion was acknowledged inside the window.
    pub window_runs: usize,
    /// Verified on-disk steps of the window's runs / window (THE number).
    pub steps_per_s: f64,
    /// Verified window runs / window.
    pub runs_per_s: f64,
    /// Commit latency (client-side start → ack) p50, ms, window starts.
    pub commit_p50_ms: f64,
    /// Commit latency p99, ms.
    pub commit_p99_ms: f64,
    /// How commit latency was measured.
    pub commit_method: String,
    /// Client ack latency (start → completed result) p50, ms, window runs.
    pub client_ack_p50_ms: f64,
    /// Client ack latency p99, ms.
    pub client_ack_p99_ms: f64,
    /// Server-side `ostro_apply_batch_seconds` p99 (ms, bucket bound), max over nodes.
    pub server_apply_batch_p99_ms: Option<f64>,
    /// Data bytes per subdirectory before the load (every node summed).
    pub bytes_before: BTreeMap<String, u64>,
    /// After the stop.
    pub bytes_after: BTreeMap<String, u64>,
    /// Growth per verified step, per subdirectory and `total` (wal + state + blobs + audit).
    pub bytes_per_step: BTreeMap<String, f64>,
    /// The offline disk check.
    pub disk: DiskCheck,
    /// Single node: runs read back after the restart (status completed and `steps`
    /// distinct `step_completed` in the history).
    pub after_restart_verified: Option<usize>,
    /// Correctness violations (acked != on disk, ...).
    pub violations: Vec<String>,
    /// Remarks.
    pub notes: Vec<String>,
    /// `OSTRO_BENCH_ENFORCE=1`.
    pub enforced: bool,
    /// The thresholds (`name -> bound`).
    pub thresholds: BTreeMap<String, f64>,
    /// Each threshold met (always computed; asserted only when enforced).
    pub threshold_met: BTreeMap<String, bool>,
}

impl BenchReport {
    /// Acked == on disk and no other violation.
    pub fn correct(&self) -> bool {
        self.violations.is_empty() && self.acked_runs > 0 && self.verified_runs == self.acked_runs
    }

    /// Every threshold met.
    pub fn thresholds_met(&self) -> bool {
        self.threshold_met.values().all(|b| *b)
    }

    /// The printed table.
    pub fn table(&self) -> String {
        let mut s = String::new();
        let _ = writeln!(s, "== bench: {} ({}) ==", self.label, self.profile);
        let _ = writeln!(
            s,
            "binary {} [{}] sha256 {} | HEAD {}",
            self.provenance.binary,
            self.provenance.build,
            self.provenance.binary_sha256,
            self.provenance.head
        );
        let _ = writeln!(
            s,
            "machine start: load {} | {} cpus | {} MB available | kernel {}; end: load {} | {} MB available",
            self.machine_start.loadavg,
            self.machine_start.nproc,
            self.machine_start.mem_available_kb / 1024,
            self.machine_start.kernel,
            self.machine_end.loadavg,
            self.machine_end.mem_available_kb / 1024
        );
        let _ = writeln!(s, "config: {}", self.config.join("; "));
        let _ = writeln!(
            s,
            "| clients | warm-up s | window s | acked runs | completed | verified on disk | window runs |"
        );
        let _ = writeln!(
            s,
            "| {} | {:.0} | {:.1} | {} | {} | {} | {} |",
            self.clients,
            self.warmup_s,
            self.window_s,
            self.acked_runs,
            self.completed_runs,
            self.verified_runs,
            self.window_runs
        );
        let _ = writeln!(
            s,
            "steps/s (verified on disk): {:.1} | runs/s {:.1}",
            self.steps_per_s, self.runs_per_s
        );
        let _ = writeln!(
            s,
            "commit p50 {:.2} ms p99 {:.2} ms ({}) | client ack p50 {:.2} ms p99 {:.2} ms | server apply batch p99 {}",
            self.commit_p50_ms,
            self.commit_p99_ms,
            self.commit_method,
            self.client_ack_p50_ms,
            self.client_ack_p99_ms,
            self.server_apply_batch_p99_ms
                .map_or_else(|| "n/a".to_owned(), |v| format!("{v:.2} ms"))
        );
        let bps: Vec<String> = self
            .bytes_per_step
            .iter()
            .map(|(k, v)| format!("{k} {v:.1}"))
            .collect();
        let _ = writeln!(s, "bytes on disk per step: {}", bps.join(", "));
        let _ = writeln!(
            s,
            "bytes before {:?} | after {:?}",
            self.bytes_before, self.bytes_after
        );
        let _ = writeln!(
            s,
            "disk check: {} / {} runs verified ({} steps), {} trimmed scans{}",
            self.disk.verified,
            self.disk.runs,
            self.disk.steps,
            self.disk.trimmed_scans,
            self.after_restart_verified
                .map_or_else(String::new, |n| format!(
                    " | after restart: {n} runs read back"
                ))
        );
        for (k, b) in &self.thresholds {
            let _ = writeln!(
                s,
                "threshold {k} {b}: {} ({})",
                if self.threshold_met.get(k).copied().unwrap_or(false) {
                    "met"
                } else {
                    "NOT met"
                },
                if self.enforced {
                    "enforced"
                } else {
                    "advisory, OSTRO_BENCH_ENFORCE unset"
                }
            );
        }
        for v in &self.violations {
            let _ = writeln!(s, "VIOLATION {v}");
        }
        for n in &self.notes {
            let _ = writeln!(s, "note: {n}");
        }
        let _ = writeln!(
            s,
            "acked == on disk: {}",
            if self.correct() { "yes" } else { "NO" }
        );
        s
    }
}

/// Write `report` as `<out_dir>/<name>` (pretty JSON); the path.
pub fn write_report(report: &BenchReport, name: &str) -> std::io::Result<PathBuf> {
    let dir = out_dir();
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(name);
    let json = serde_json::to_vec_pretty(report).map_err(std::io::Error::other)?;
    std::fs::write(&path, json)?;
    Ok(path)
}

fn us(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

/// One closed-loop client until `end` (since `t0`); its runs.
async fn client(
    api: Api,
    w: usize,
    seed: u64,
    steps: u64,
    t0: Instant,
    end: Duration,
    drain: Duration,
) -> Vec<RunSample> {
    let input = json!({ "n": steps });
    let mut out = Vec::new();
    let mut i = 0u64;
    while t0.elapsed() < end {
        i += 1;
        let key = format!("bench-{seed}-{w}-{i}");
        let sent = t0.elapsed();
        // Same key until acknowledged (a 503 hop or a timeout retries it).
        let ack = loop {
            match api.start(WORKFLOW, &input, &key).await {
                Ok(a) => break Some(a),
                Err(_) if t0.elapsed() >= end => break None,
                Err(_) => pause(Duration::from_millis(20)).await,
            }
        };
        let Some(ack) = ack else { break };
        let mut s = RunSample {
            ack,
            sent,
            ack_us: us(t0.elapsed().saturating_sub(sent)),
            done: None,
            done_us: None,
            status: None,
        };
        while t0.elapsed() < end + drain {
            match api.result(&s.ack.run_id, Duration::from_secs(10)).await {
                Ok(Some((status, _))) => {
                    let now = t0.elapsed();
                    s.done = Some(now);
                    s.done_us = Some(us(now.saturating_sub(sent)));
                    s.status = Some(status);
                    break;
                }
                Ok(None) => {}
                Err(_) => pause(Duration::from_millis(20)).await,
            }
        }
        out.push(s);
    }
    out
}

/// Wait (bounded by `cap` overall) for every run of `pending` to reach a terminal status;
/// fills `status` of the ones that do.
async fn drain(api: &Api, pending: &mut [&mut RunSample], cap: Duration) {
    let t0 = Instant::now(); // clock-exempt: benchmark timing
    for s in pending.iter_mut() {
        while t0.elapsed() < cap {
            match api.result(&s.ack.run_id, Duration::from_secs(5)).await {
                Ok(Some((status, _))) => {
                    s.status = Some(status);
                    break;
                }
                Ok(None) => {}
                Err(_) => pause(Duration::from_millis(50)).await,
            }
        }
    }
}

/// Read back every run of `acks` through the API: status `completed` and exactly `steps`
/// distinct (and no repeated) `step_completed` seqs. The count that passed and the
/// failures (first 20).
async fn read_back(api: &Api, acks: &[Ack], steps: u64) -> (usize, Vec<String>) {
    let mut ok = 0usize;
    let mut bad = Vec::new();
    let mut set = JoinSet::new();
    let mut queue = acks.iter().cloned();
    loop {
        while set.len() < 64
            && let Some(a) = queue.next()
        {
            let api = api.clone();
            set.spawn(async move {
                let st = api
                    .call_retry(
                        &Verb::Get,
                        &format!("/v1/runs/{}", a.run_id),
                        Duration::from_secs(30),
                    )
                    .await;
                let status = match st {
                    Ok((200, v)) => v["status"].as_str().unwrap_or("").to_owned(),
                    Ok((code, v)) => return Err(format!("run {}: http {code}: {v}", a.run_id)),
                    Err(e) => return Err(format!("run {}: {e}", a.run_id)),
                };
                if status != "completed" {
                    return Err(format!(
                        "run {}: status {status} after the restart",
                        a.run_id
                    ));
                }
                let seqs = api
                    .history_steps(&a.run_id)
                    .await
                    .map_err(|e| format!("run {}: {e}", a.run_id))?;
                let distinct: BTreeSet<u64> = seqs.iter().copied().collect();
                if distinct.len() as u64 != steps || seqs.len() != distinct.len() {
                    return Err(format!(
                        "run {}: {} step_completed ({} distinct) after the restart, want {steps}",
                        a.run_id,
                        seqs.len(),
                        distinct.len()
                    ));
                }
                Ok(())
            });
        }
        match set.join_next().await {
            None => break,
            Some(Ok(Ok(()))) => ok += 1,
            Some(Ok(Err(e))) => {
                if bad.len() < 20 {
                    bad.push(e);
                }
            }
            Some(Err(e)) => {
                if bad.len() < 20 {
                    bad.push(format!("read-back task: {e}"));
                }
            }
        }
    }
    (ok, bad)
}

/// Run `spec` (module docs). `Err` only when the cluster could not start or be prepared;
/// correctness failures are in [`BenchReport::violations`].
pub async fn run(spec: &BenchSpec) -> Result<BenchReport, String> {
    let machine_start = Machine::sample();
    let steps = spec.profile.steps();
    let mut rig = Rig::start(&spec.rig()).await?;
    rig.deploy(WORKFLOW).await?;
    let bytes_before = rig.disk_usage();
    let mut notes = Vec::new();
    if spec.bin.is_none() {
        notes.push("nodes ran the failpoint debug build: throughput is advisory only".into());
    }

    // Load.
    let t0 = Instant::now(); // clock-exempt: benchmark timing
    let end = spec.warmup + spec.duration;
    let mut set = JoinSet::new();
    let n_apis = rig.apis().len().max(1);
    for w in 0..spec.clients {
        let api = rig.api(w % n_apis);
        set.spawn(client(api, w, spec.seed, steps, t0, end, spec.drain_cap));
    }
    let mut runs: Vec<RunSample> = Vec::new();
    while let Some(r) = set.join_next().await {
        match r {
            Ok(v) => runs.extend(v),
            Err(e) => notes.push(format!("client task failed: {e}")),
        }
    }
    let load_end = t0.elapsed();
    let api = rig.api(0);
    {
        let mut pending: Vec<&mut RunSample> =
            runs.iter_mut().filter(|r| r.status.is_none()).collect();
        if !pending.is_empty() {
            notes.push(format!(
                "{} acknowledged runs still running at the end of the load",
                pending.len()
            ));
            drain(&api, &mut pending, spec.drain_cap).await;
        }
    }
    let mut apply_p99: Option<f64> = None;
    for a in rig.apis() {
        if let Some(text) = scrape(&a).await
            && let Some(q) = histogram_quantile(&text, "ostro_apply_batch_seconds", 0.99)
        {
            apply_p99 = Some(apply_p99.map_or(q, |p| p.max(q)));
        }
    }

    // Stop (kill -9) and verify on disk.
    let acks: Vec<Ack> = runs.iter().map(|r| r.ack.clone()).collect();
    let mut violations = Vec::new();
    let completed_runs = runs
        .iter()
        .filter(|r| r.status.as_deref() == Some("completed"))
        .count();
    let not_completed: Vec<&RunSample> = runs
        .iter()
        .filter(|r| r.status.as_deref() != Some("completed"))
        .collect();
    for r in not_completed.iter().take(20) {
        violations.push(format!(
            "acknowledged run {} not completed: {}",
            r.ack.run_id,
            r.status
                .as_deref()
                .unwrap_or("not terminal within the drain cap")
        ));
    }
    if not_completed.len() > 20 {
        violations.push(format!(
            "{} more acknowledged runs not completed",
            not_completed.len() - 20
        ));
    }
    let ids = rig.cluster.node_ids();
    let views: Vec<DataDirView> = ids.iter().map(|n| rig.cluster.node_disk(*n)).collect();
    let mut after_restart = None;
    let (disk, bytes_after) = if spec.nodes == 1 {
        let n = ids[0];
        rig.cluster.kill(n).await;
        let bytes_after = rig.disk_usage();
        let disk = verify_on_disk(&views, &acks, rig.min_insync, steps);
        match rig.cluster.restart(n).await {
            Ok(()) => match wait_healthy(&rig.cluster, &[], HEAL_T, "the restart").await {
                Ok(_) => {
                    let (ok, bad) = read_back(&rig.api(0), &acks, steps).await;
                    after_restart = Some(ok);
                    let none_listed = bad.is_empty();
                    for b in bad {
                        violations.push(format!("after restart: {b}"));
                    }
                    if ok != acks.len() && none_listed {
                        violations.push(format!(
                            "after restart: {ok} of {} runs read back",
                            acks.len()
                        ));
                    }
                }
                Err(f) => violations.push(format!("after restart: {}: {}", f.oracle, f.detail)),
            },
            Err(e) => violations.push(format!("restart: {e}")),
        }
        drop(rig.cluster.stop_keeping_dirs());
        (disk, bytes_after)
    } else {
        let dirs = rig.cluster.stop_keeping_dirs();
        let mut bytes_after = BTreeMap::new();
        for v in &views {
            for (k, b) in dir_sizes(&v.root) {
                *bytes_after.entry(k).or_default() += b;
            }
        }
        let disk = verify_on_disk(&views, &acks, rig.min_insync, steps);
        drop(dirs);
        (disk, bytes_after)
    };
    for v in &disk.violations {
        violations.push(format!("disk: {v}"));
    }
    if disk.more_violations > 0 {
        violations.push(format!("disk: {} more violations", disk.more_violations));
    }
    if disk.trimmed_scans > 0 {
        notes.push(format!(
            "{} replica scans were trimmed (retention ran during the bench)",
            disk.trimmed_scans
        ));
    }

    // Numbers.
    let (w0, w1) = (spec.warmup, end);
    let window_s = spec.duration.as_secs_f64();
    let window: Vec<&RunSample> = runs
        .iter()
        .filter(|r| r.status.as_deref() == Some("completed"))
        .filter(|r| r.done.is_some_and(|d| d >= w0 && d <= w1))
        .collect();
    let window_verified = window
        .iter()
        .filter(|r| !disk.failed_runs.contains(&r.ack.run_id))
        .count();
    let steps_per_s = window_verified as f64 * steps as f64 / window_s;
    let commit: Vec<u64> = runs
        .iter()
        .filter(|r| r.sent >= w0 && r.sent <= w1)
        .map(|r| r.ack_us)
        .collect();
    let client_ack: Vec<u64> = window.iter().filter_map(|r| r.done_us).collect();
    let verified_steps = disk.steps.max(1) as f64;
    let mut bytes_per_step = BTreeMap::new();
    let mut total = 0.0;
    for d in DISK_DIRS {
        let grow = bytes_after.get(d).copied().unwrap_or(0) as f64
            - bytes_before.get(d).copied().unwrap_or(0) as f64;
        let per = grow / verified_steps;
        total += per;
        bytes_per_step.insert(d.to_owned(), per);
    }
    bytes_per_step.insert("total".to_owned(), total);

    let commit_p99 = pct_ms(&commit, 0.99);
    let mut thresholds = BTreeMap::new();
    let mut threshold_met = BTreeMap::new();
    let floor = if spec.nodes == 1 {
        SINGLE_MIN_STEPS_PER_S
    } else {
        RF3_MIN_STEPS_PER_S
    };
    thresholds.insert("steps_per_s >=".to_owned(), floor);
    threshold_met.insert("steps_per_s >=".to_owned(), steps_per_s >= floor);
    if spec.nodes > 1 {
        thresholds.insert("commit_p99_ms <".to_owned(), RF3_MAX_COMMIT_P99_MS);
        threshold_met.insert(
            "commit_p99_ms <".to_owned(),
            commit_p99 < RF3_MAX_COMMIT_P99_MS,
        );
    }
    notes.push(format!(
        "load phase {:.1} s (warm-up {:.0} s + window {:.0} s + the clients' last runs)",
        load_end.as_secs_f64(),
        spec.warmup.as_secs_f64(),
        window_s
    ));

    Ok(BenchReport {
        label: spec.label.clone(),
        nodes: spec.nodes,
        min_insync: rig.min_insync,
        partitions: spec.partitions,
        profile: format!("{:?} (trivial-steps n={steps})", spec.profile),
        steps_per_run: steps,
        clients: spec.clients,
        warmup_s: spec.warmup.as_secs_f64(),
        window_s,
        config: rig.notes.clone(),
        provenance: rig.provenance.clone(),
        machine_start,
        machine_end: Machine::sample(),
        acked_runs: acks.len(),
        completed_runs,
        verified_runs: disk.verified,
        window_runs: window.len(),
        steps_per_s,
        runs_per_s: window_verified as f64 / window_s,
        commit_p50_ms: pct_ms(&commit, 0.5),
        commit_p99_ms: commit_p99,
        commit_method:
            "client-side start request -> start ack (no Phase 12 commit-latency histogram exists)"
                .into(),
        client_ack_p50_ms: pct_ms(&client_ack, 0.5),
        client_ack_p99_ms: pct_ms(&client_ack, 0.99),
        server_apply_batch_p99_ms: apply_p99.map(|s| s * 1000.0),
        bytes_before,
        bytes_after,
        bytes_per_step,
        disk,
        after_restart_verified: after_restart,
        violations,
        notes,
        enforced: enforce(),
        thresholds,
        threshold_met,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const METRICS: &str = "# HELP x\n\
        ostro_apply_batch_seconds_bucket{partition=\"0\",le=\"0.001\"} 50\n\
        ostro_apply_batch_seconds_bucket{partition=\"0\",le=\"0.01\"} 99\n\
        ostro_apply_batch_seconds_bucket{partition=\"0\",le=\"+Inf\"} 100\n\
        ostro_apply_batch_seconds_bucket{partition=\"1\",le=\"0.001\"} 50\n\
        ostro_apply_batch_seconds_bucket{partition=\"1\",le=\"0.01\"} 100\n\
        ostro_apply_batch_seconds_bucket{partition=\"1\",le=\"+Inf\"} 100\n\
        ostro_wal_log_start{partition=\"3\"} 4096\n\
        ostro_raft_term 7\n";

    #[test]
    fn histogram_quantile_merges_label_sets() {
        assert_eq!(
            histogram_quantile(METRICS, "ostro_apply_batch_seconds", 0.5),
            Some(0.001)
        );
        assert_eq!(
            histogram_quantile(METRICS, "ostro_apply_batch_seconds", 0.99),
            Some(0.01)
        );
        assert_eq!(histogram_quantile(METRICS, "ostro_nothing", 0.99), None);
    }

    #[test]
    fn samples_parse_labels_and_plain_series() {
        assert_eq!(
            samples(METRICS, "ostro_wal_log_start"),
            vec![("partition=\"3\"", 4096.0)]
        );
        assert_eq!(samples(METRICS, "ostro_raft_term"), vec![("", 7.0)]);
    }

    #[test]
    fn pct_by_sorting() {
        let v: Vec<u64> = (1..=100).map(|i| i * 1000).collect();
        assert_eq!(pct_ms(&v, 0.5), 50.0);
        assert_eq!(pct_ms(&v, 0.99), 99.0);
        assert_eq!(pct_ms(&[], 0.99), 0.0);
    }
}
