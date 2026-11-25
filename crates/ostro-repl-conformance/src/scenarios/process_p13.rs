//! Phase 13 tier-C (multi-process) scenarios and tools (13-19; D-P13-7 / D-P13-26 /
//! D-P13-28, CLUS-05/06/07, API-08). 13-02 declared the module; 13-19 fills it.
//!
//! Tier B shares one process (globals, no real `kill -9`); the Phase 13 success criteria
//! need spawned failpoint binaries, real signals and network faults. Every tier-C Phase 13
//! test (`tests/process_{promotion,rolling_upgrade,ryw,downgrade,shutdown}.rs`,
//! `tests/upgrade_from_prev.rs`, `tests/cluster_campaign_p13.rs`) owns its cluster: a
//! single node promoted through `ostro cluster init` + `ostro cluster join`, or a static
//! three-node cluster (`[[cluster.peers]]` through the chaos proxy) started at an emulated
//! level ([`start_static`]). Joined nodes dial their peers directly (no per-dialer
//! override exists yet: 13-18 handover), so the chaos-proxy faults run on static clusters.
//! [`all`] therefore stays empty; the tests call the tools here directly.
//!
//! What lives here:
//!
//! - [`report_dir`] / [`write_report`]: every test writes `$OSTRO_P13_REPORT_DIR/<test>/
//!   report.txt` (absolute; default [`DEFAULT_REPORT_DIR`]).
//! - [`Load`]: the mixed continuous workload (`trivial-steps` 3-step runs, `sdk-basic`
//!   hot waits, `signals` signal waits the workload signals) with an ack log, plus one
//!   availability probe per partition (`smoke` starts every [`PROBE_PACE`]) whose ack
//!   instants give the per-partition unavailability of a window ([`Load::unavailability`]).
//!   Starts go to any running node (the router forwards them, 13-12) with an idempotency
//!   key routed to a chosen partition and are retried with the same key; the same key
//!   acknowledged with two run ids is an exactly-once violation.
//! - [`outcomes`]: every acknowledged run ends `completed` (none failed, cancelled,
//!   terminated, timed out), its journal seqs are strictly increasing, and a `trivial-steps`
//!   run holds exactly three distinct `step_completed` entries (0 duplicate journal
//!   entries).
//! - [`final_checks`]: the Phase 12 end-of-scenario oracles (replica identity below the HW
//!   over each live ISR, acknowledged ⇒ on disk on `min_insync` nodes, LA rule and the
//!   ISR-only election audit, both reading a compacted meta log from its snapshot state).
//! - [`standby_identity`], [`upgrade_oracles`] (`no_gated_before_finalize_from(1, ..)` and
//!   `no_reject_continue` over the replayed apply outcomes of every partition log).
//! - Version helpers over `GET /v1/cluster/status` ([`finalized`], [`wait_finalized`],
//!   [`stays_finalized`]) and the `/metrics` pre-warm counter ([`prewarm_counts`]).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use ostro_types::{PartitionCount, PartitionId};
use serde_json::{Value, json};
use tokio::task::JoinSet;
use tokio::time::Instant;

pub use super::ScenarioFailure;
use super::process::tools::{self, best_dump, fail, live, pause, published, published_all, settle};
use super::{Scenario, ScenarioResult};
use crate::harness::inproc::Ack;
use crate::harness::outcomes::partition_outcomes;
use crate::harness::process::{ProcCluster, ProcOptions};
use crate::harness::{ClusterHarness, ClusterSpec, DataDirView};
use crate::oracle::disk::{LogScan, acked_on_disk_copies, scan_partition};
use crate::oracle::p13_upgrade::{
    OutcomeRecord, no_gated_before_finalize_from, no_reject_continue,
};
use crate::oracle::{election, la_rule, replica, standby};

/// Every Phase 13 tier-C scenario: none through the runner (module docs: each test owns
/// its cluster lifecycle).
pub fn all() -> Vec<Scenario<ProcCluster>> {
    vec![]
}

// ------------------------------------------------------------------------- reports

/// Where reports go when `OSTRO_P13_REPORT_DIR` is unset.
pub const DEFAULT_REPORT_DIR: &str = "/tmp/ostro-p13-reports";

/// `$OSTRO_P13_REPORT_DIR/<test>` (created). The variable must be an absolute path.
pub fn report_dir(test: &str) -> PathBuf {
    let base = match std::env::var_os("OSTRO_P13_REPORT_DIR") {
        Some(d) if !d.is_empty() => PathBuf::from(d),
        _ => PathBuf::from(DEFAULT_REPORT_DIR),
    };
    assert!(
        base.is_absolute(),
        "OSTRO_P13_REPORT_DIR must be an absolute path, got {}",
        base.display()
    );
    let d = base.join(test);
    std::fs::create_dir_all(&d).unwrap_or_else(|e| panic!("create {}: {e}", d.display()));
    d
}

/// Write `<report_dir(test)>/report.txt` and print it; its path.
pub fn write_report(test: &str, text: &str) -> PathBuf {
    let path = report_dir(test).join("report.txt");
    std::fs::write(&path, text).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
    println!("{text}\nreport: {}", path.display());
    path
}

/// Where this report was measured: kernel, CPUs, WSL2 or native, the k8s node if any.
pub fn host_line() -> String {
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|s| s.trim().to_owned())
        .unwrap_or_else(|_| "unknown".into());
    let wsl = kernel.to_ascii_lowercase().contains("microsoft");
    let cpus = std::thread::available_parallelism().map_or(0, |n| n.get());
    let node = std::env::var("NODE_NAME").unwrap_or_default();
    format!(
        "host: kernel {kernel} ({}), {cpus} CPUs available{}",
        if wsl { "WSL2" } else { "native Linux" },
        if node.is_empty() {
            String::new()
        } else {
            format!(", k8s node {node}")
        }
    )
}

/// q-th quantile of an ascending slice (nearest rank; zero when empty).
pub fn pct(sorted: &[Duration], q: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let i = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

/// `p50 / p99 / max (n)` of `v` (sorted in place).
pub fn dist(v: &mut [Duration]) -> String {
    v.sort_unstable();
    format!(
        "p50 {:?} / p99 {:?} / max {:?} (n={})",
        pct(v, 0.5),
        pct(v, 0.99),
        v.last().copied().unwrap_or_default(),
        v.len()
    )
}

// ------------------------------------------------------------------------- cluster

/// Bound on one request of the tools here.
pub const REQ_T: Duration = Duration::from_secs(10);
/// Bound on the cluster becoming healthy after a fault.
pub const HEAL_T: Duration = Duration::from_secs(120);

/// A three-node RF 3 / min_insync 2 spec (4 partitions).
pub fn spec3(seed: u64) -> ClusterSpec {
    ClusterSpec {
        seed,
        ..ClusterSpec::default()
    }
}

/// A static three-node cluster (every link through the chaos proxy) whose processes start
/// at the emulated level `level` (`OSTRO_TEST_LEVEL_MAX`; `None` = this build's level),
/// healthy (meta leader, every partition led with a full ISR).
pub async fn start_static(seed: u64, level: Option<u32>) -> Result<ProcCluster, ScenarioFailure> {
    let opts = ProcOptions {
        level_max: level,
        ..ProcOptions::default()
    };
    let c = ProcCluster::start_with(spec3(seed), opts)
        .await
        .map_err(|e| fail("harness", format!("start static cluster: {e}")))?;
    wait_healthy(&c, &[], HEAL_T, "start").await?;
    Ok(c)
}

/// The shared HTTP client (per-request timeouts).
fn http() -> &'static reqwest::Client {
    static C: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    C.get_or_init(reqwest::Client::new)
}

/// One request (`None` = no answer within `limit`).
pub async fn call(
    method: reqwest::Method,
    url: &str,
    body: Option<&Value>,
    limit: Duration,
) -> Option<(u16, Value)> {
    let mut rb = http().request(method, url).timeout(limit);
    if let Some(b) = body {
        rb = rb.json(b);
    }
    let r = rb.send().await.ok()?;
    let st = r.status().as_u16();
    let b = r.bytes().await.ok()?;
    Some((st, serde_json::from_slice(&b).unwrap_or(Value::Null)))
}

/// `GET {path}` on `node`.
pub async fn get(c: &ProcCluster, node: u64, path: &str) -> Option<(u16, Value)> {
    let url = format!("{}{path}", c.client(node).base_url);
    call(reqwest::Method::GET, &url, None, REQ_T).await
}

/// `POST {path}` with `body` on `node`.
pub async fn post(c: &ProcCluster, node: u64, path: &str, body: &Value) -> Option<(u16, Value)> {
    let url = format!("{}{path}", c.client(node).base_url);
    call(reqwest::Method::POST, &url, Some(body), REQ_T).await
}

/// `error.details.reason` of an error body.
pub fn reason(v: &Value) -> &str {
    v["error"]["details"]["reason"].as_str().unwrap_or("")
}

/// Whether the cluster is healthy without the nodes in `skip`: every other node runs, a
/// meta leader answers, every partition is led by a running node with a full ISR.
pub async fn healthy(c: &ProcCluster, skip: &[u64]) -> bool {
    let up = live(c);
    if c.node_ids()
        .iter()
        .any(|n| !skip.contains(n) && !up.contains(n))
    {
        return false;
    }
    if published_all(c).iter().any(|a| {
        a.leader == 0
            || !up.contains(&a.leader)
            || a.isr.len() != a.replicas.len()
            || a.replicas.is_empty()
    }) {
        return false;
    }
    tools::meta_leader(c).await.is_some()
}

/// Wait until [`healthy`]; how long it took.
pub async fn wait_healthy(
    c: &ProcCluster,
    skip: &[u64],
    limit: Duration,
    at: &str,
) -> Result<Duration, ScenarioFailure> {
    let t0 = Instant::now(); // clock-exempt: test timing
    while t0.elapsed() < limit {
        if healthy(c, skip).await {
            return Ok(t0.elapsed());
        }
        pause(Duration::from_millis(100)).await;
    }
    let all: Vec<String> = published_all(c)
        .iter()
        .map(|a| {
            format!(
                "p{} leader {} replicas {:?} isr {:?}",
                a.partition, a.leader, a.replicas, a.isr
            )
        })
        .collect();
    Err(fail(
        "availability",
        format!(
            "cluster not healthy within {limit:?} after {at}: {all:?}; live {:?}",
            live(c)
        ),
    ))
}

/// `GET /v1/cluster/status` on `node`.
pub async fn status(c: &ProcCluster, node: u64) -> Option<Value> {
    match get(c, node, "/v1/cluster/status").await {
        Some((200, v)) => Some(v),
        _ => None,
    }
}

/// The finalized level each running node in `nodes` reports (absent = no answer).
pub async fn finalized(c: &ProcCluster, nodes: &[u64]) -> BTreeMap<u64, u32> {
    let mut out = BTreeMap::new();
    for n in nodes {
        if c.pid(*n).is_none() {
            continue;
        }
        if let Some(v) = status(c, *n).await
            && let Some(f) = v["finalized"].as_u64()
        {
            out.insert(*n, u32::try_from(f).unwrap_or(u32::MAX));
        }
    }
    out
}

/// Wait until every node in `nodes` reports finalized `level`; how long it took.
pub async fn wait_finalized(
    c: &ProcCluster,
    nodes: &[u64],
    level: u32,
    limit: Duration,
) -> Result<Duration, ScenarioFailure> {
    let t0 = Instant::now(); // clock-exempt: test timing
    loop {
        let l = finalized(c, nodes).await;
        if l.len() == nodes.len() && l.values().all(|v| *v == level) {
            return Ok(t0.elapsed());
        }
        if t0.elapsed() >= limit {
            return Err(fail(
                "finalized",
                format!("not finalized at {level} on {nodes:?} within {limit:?}: {l:?}"),
            ));
        }
        pause(Duration::from_millis(50)).await;
    }
}

/// Sample for `d` that no node in `nodes` reports a finalized level other than `level`
/// (and that some node answers).
pub async fn stays_finalized(
    c: &ProcCluster,
    nodes: &[u64],
    level: u32,
    d: Duration,
) -> ScenarioResult {
    let t0 = Instant::now(); // clock-exempt: test timing
    let mut answered = 0usize;
    while t0.elapsed() < d {
        let l = finalized(c, nodes).await;
        if l.values().any(|v| *v != level) {
            return Err(fail(
                "finalized",
                format!("finalized left {level} early: {l:?}"),
            ));
        }
        answered += l.len();
        pause(Duration::from_millis(100)).await;
    }
    if answered == 0 {
        return Err(fail(
            "finalized",
            format!("no node answered the status for {d:?} (vacuous)"),
        ));
    }
    Ok(())
}

/// `ostro_cluster_prewarm_modules_total{result}` from `node`'s `/metrics` (empty when the
/// counter was never incremented or the node does not answer).
pub async fn prewarm_counts(c: &ProcCluster, node: u64) -> BTreeMap<String, u64> {
    let url = format!("{}/metrics", c.client(node).base_url);
    let Ok(r) = http().get(&url).timeout(REQ_T).send().await else {
        return BTreeMap::new();
    };
    let text = r.text().await.unwrap_or_default();
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let Some(rest) = line.strip_prefix("ostro_cluster_prewarm_modules_total") else {
            continue;
        };
        let result = rest
            .split("result=\"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .unwrap_or("")
            .to_owned();
        let v = rest
            .split_whitespace()
            .last()
            .and_then(|x| x.parse::<f64>().ok())
            .unwrap_or(0.0);
        *out.entry(result).or_default() += v as u64;
    }
    out
}

/// The node leading the most partitions (the meta leader on a tie, else the lowest id);
/// `None` when no partition is led.
pub async fn busiest_leader(c: &ProcCluster) -> Option<u64> {
    let meta = tools::meta_leader(c).await;
    let mut counts: BTreeMap<u64, usize> = BTreeMap::new();
    for a in published_all(c) {
        if a.leader != 0 {
            *counts.entry(a.leader).or_default() += 1;
        }
    }
    let max = counts.values().max().copied()?;
    let tied: Vec<u64> = counts
        .iter()
        .filter(|(_, v)| **v == max)
        .map(|(n, _)| *n)
        .collect();
    meta.filter(|m| tied.contains(m)).or(tied.first().copied())
}

// ------------------------------------------------------------------------- guests

/// The workload guests (`guests/<name>`).
pub const WORKLOAD_GUESTS: [&str; 4] = ["smoke", "trivial-steps", "sdk-basic", "signals"];

/// Deploy `guests` through any live node.
pub async fn deploy_guests(c: &ProcCluster, guests: &[&str]) -> ScenarioResult {
    for g in guests {
        tools::deploy(c, g).await?;
    }
    Ok(())
}

// ------------------------------------------------------------------------- workload

/// Pause between two probe starts of one partition.
pub const PROBE_PACE: Duration = Duration::from_millis(50);
/// Pause between a workload client's acknowledged start and its next one.
pub const LOAD_PACE: Duration = Duration::from_millis(100);
/// Workload clients.
pub const LOAD_CLIENTS: usize = 4;
/// Bound on one start's retries (same key) before the key is given up.
pub const START_GIVE_UP: Duration = Duration::from_secs(60);
/// Steps of a `trivial-steps` run.
pub const STEPS: usize = 3;

/// What a workload run is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    /// `trivial-steps` with `n = 3` (exactly-once journal check).
    Steps,
    /// `sdk-basic`: steps and short durable sleeps (hot waits).
    Hot,
    /// `signals` mode `before`: a durable sleep, then a signal wait (the load signals it).
    Signal,
    /// `smoke`: the per-partition availability probe (completes at once).
    Probe,
}

impl Kind {
    /// The workflow name.
    pub fn workflow(self) -> &'static str {
        match self {
            Kind::Steps => "trivial-steps",
            Kind::Hot => "sdk-basic",
            Kind::Signal => "signals",
            Kind::Probe => "smoke",
        }
    }

    fn input(self, key: &str) -> Value {
        match self {
            Kind::Steps => json!({ "n": STEPS }),
            Kind::Hot => json!({ "name": key, "n": 3 }),
            Kind::Signal => json!({ "mode": "before", "sleep_ms": 200 }),
            Kind::Probe => json!({ "k": key }),
        }
    }
}

/// One acknowledged workload run.
#[derive(Clone, Debug)]
pub struct Started {
    /// The ack.
    pub ack: Ack,
    /// What it runs.
    pub kind: Kind,
    /// Its idempotency key.
    pub key: String,
}

#[derive(Default)]
struct LoadState {
    runs: Vec<Started>,
    by_key: BTreeMap<String, String>,
    dup_keys: Vec<String>,
    unsignalled: Vec<Ack>,
    probe_acks: BTreeMap<u16, Vec<Instant>>,
    given_up: usize,
    rejected: usize,
}

struct LoadShared {
    targets: Mutex<Vec<String>>,
    state: Mutex<LoadState>,
    stop: AtomicBool,
    n: AtomicU64,
    salt: String,
    count: PartitionCount,
}

impl LoadShared {
    fn st(&self) -> std::sync::MutexGuard<'_, LoadState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn target(&self, i: u64) -> Option<String> {
        let t = self.targets.lock().unwrap_or_else(|e| e.into_inner());
        if t.is_empty() {
            return None;
        }
        Some(t[(i % t.len() as u64) as usize].clone())
    }

    fn key_for(&self, wf: &str, p: u16) -> String {
        loop {
            let i = self.n.fetch_add(1, Ordering::Relaxed);
            let key = format!("p13c-{}-{i}", self.salt);
            if ostro_types::routing::partition_for_key(wf, key.as_bytes(), self.count).0 == p {
                return key;
            }
        }
    }

    /// Record an acknowledged start of `key` (the exactly-once key check).
    fn record(&self, s: &Started, p: u16) {
        let mut st = self.st();
        match st.by_key.get(&s.key).cloned() {
            Some(prev) if prev != s.ack.run_id => {
                let msg = format!(
                    "key {}: acknowledged as {prev} and as {}",
                    s.key, s.ack.run_id
                );
                st.dup_keys.push(msg);
            }
            Some(_) => return,
            None => {}
        }
        st.by_key.insert(s.key.clone(), s.ack.run_id.clone());
        if s.kind == Kind::Signal {
            st.unsignalled.push(s.ack.clone());
        }
        if s.kind == Kind::Probe {
            // clock-exempt: availability measurement
            st.probe_acks.entry(p).or_default().push(Instant::now());
        }
        st.runs.push(s.clone());
    }

    /// Start one `kind` run routed to `p` through the targets (retried with the same key
    /// until acknowledged, the load stops or [`START_GIVE_UP`] passes).
    async fn start(&self, kind: Kind, p: u16, rot: &mut u64) -> Option<Started> {
        let wf = kind.workflow();
        let key = self.key_for(wf, p);
        let body = json!({ "input": kind.input(&key), "idempotency_key": key });
        let t0 = Instant::now(); // clock-exempt: test timing
        while !self.stop.load(Ordering::Relaxed) && t0.elapsed() < START_GIVE_UP {
            *rot += 1;
            let Some(base) = self.target(*rot) else {
                pause(Duration::from_millis(50)).await;
                continue;
            };
            let url = format!("{base}/v1/workflows/{wf}/runs");
            let answer = call(
                reqwest::Method::POST,
                &url,
                Some(&body),
                Duration::from_secs(5),
            )
            .await;
            match answer {
                Some((st, v)) if (200..300).contains(&st) => {
                    let parsed = v["run_id"].as_str().and_then(|id| {
                        id.parse::<ostro_types::RunId>()
                            .ok()
                            .map(|r| (id.to_owned(), r.partition().0))
                    });
                    if let Some((run_id, partition)) = parsed {
                        let s = Started {
                            ack: Ack { run_id, partition },
                            kind,
                            key: key.clone(),
                        };
                        self.record(&s, p);
                        return Some(s);
                    }
                }
                Some(_) => self.st().rejected += 1,
                None => {}
            }
            pause(Duration::from_millis(50)).await;
        }
        if !self.stop.load(Ordering::Relaxed) {
            self.st().given_up += 1;
        }
        None
    }

    /// Signal every waiting `signals` run once (through the targets); the ones not yet
    /// accepted stay queued.
    async fn signal_pending(&self, rot: &mut u64) {
        let pending = std::mem::take(&mut self.st().unsignalled);
        let mut left = Vec::new();
        for ack in pending {
            *rot += 1;
            let Some(base) = self.target(*rot) else {
                left.push(ack);
                continue;
            };
            let url = format!("{base}/v1/runs/{}/signals/x", ack.run_id);
            match call(
                reqwest::Method::POST,
                &url,
                Some(&json!({ "payload": "go" })),
                Duration::from_secs(5),
            )
            .await
            {
                Some((st, _)) if (200..300).contains(&st) => {}
                // An earlier attempt was applied but its answer was lost: the run consumed
                // the signal and completed (the outcome oracle checks its status).
                Some((409, v)) if reason(&v) == "run_terminal" => {}
                _ => left.push(ack),
            }
        }
        self.st().unsignalled.extend(left);
    }
}

/// The continuous mixed workload (module docs).
pub struct Load {
    shared: Arc<LoadShared>,
    tasks: JoinSet<()>,
}

impl Load {
    /// Start the workload against the running nodes of `c` (except `exclude`); with
    /// `probes`, one availability probe per partition too.
    pub fn start(c: &ProcCluster, exclude: &[u64], probes: bool) -> Self {
        let partitions = c.spec().partitions;
        let shared = Arc::new(LoadShared {
            targets: Mutex::new(Vec::new()),
            state: Mutex::new(LoadState::default()),
            stop: AtomicBool::new(false),
            n: AtomicU64::new(0),
            salt: format!("{}-{}-{}", c.cluster_id(), c.r(), std::process::id()),
            count: PartitionCount::new(partitions).expect("partitions > 0"),
        });
        let mut load = Self {
            shared,
            tasks: JoinSet::new(),
        };
        load.refresh(c, exclude);
        for w in 0..LOAD_CLIENTS {
            let sh = Arc::clone(&load.shared);
            load.tasks.spawn(async move {
                let kinds = [Kind::Steps, Kind::Hot, Kind::Signal];
                let mut i = w as u64;
                let mut rot = w as u64;
                while !sh.stop.load(Ordering::Relaxed) {
                    let kind = kinds[(i % kinds.len() as u64) as usize];
                    let p = ((i / kinds.len() as u64) % u64::from(partitions)) as u16;
                    if sh.start(kind, p, &mut rot).await.is_some() {
                        pause(LOAD_PACE).await;
                    }
                    i += 1;
                }
            });
        }
        {
            let sh = Arc::clone(&load.shared);
            load.tasks.spawn(async move {
                let mut rot = 7u64;
                while !sh.stop.load(Ordering::Relaxed) {
                    sh.signal_pending(&mut rot).await;
                    pause(Duration::from_millis(300)).await;
                }
            });
        }
        if probes {
            for p in 0..partitions {
                let sh = Arc::clone(&load.shared);
                load.tasks.spawn(async move {
                    let mut rot = u64::from(p) * 13;
                    while !sh.stop.load(Ordering::Relaxed) {
                        sh.start(Kind::Probe, p, &mut rot).await;
                        pause(PROBE_PACE).await;
                    }
                });
            }
        }
        load
    }

    /// Point the workload at the running nodes of `c` except `exclude`.
    pub fn refresh(&self, c: &ProcCluster, exclude: &[u64]) {
        let t: Vec<String> = live(c)
            .into_iter()
            .filter(|n| !exclude.contains(n))
            .map(|n| c.client(n).base_url)
            .collect();
        *self
            .shared
            .targets
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = t;
    }

    /// Every acknowledged run so far (probes included).
    pub fn snapshot(&self) -> Vec<Ack> {
        self.shared
            .st()
            .runs
            .iter()
            .map(|r| r.ack.clone())
            .collect()
    }

    /// Acknowledged non-probe runs so far.
    pub fn acked(&self) -> usize {
        self.shared
            .st()
            .runs
            .iter()
            .filter(|r| r.kind != Kind::Probe)
            .count()
    }

    /// Wait until `k` more non-probe runs are acknowledged.
    pub async fn wait_acked(&self, k: usize, limit: Duration) -> ScenarioResult {
        let base = self.acked();
        let t0 = Instant::now(); // clock-exempt: test timeout
        while self.acked() < base + k {
            if t0.elapsed() > limit {
                return Err(fail(
                    "workload",
                    format!(
                        "{k} runs not acknowledged within {limit:?} ({} so far)",
                        self.acked() - base
                    ),
                ));
            }
            pause(Duration::from_millis(20)).await;
        }
        Ok(())
    }

    /// The longest interval without a probe ack on partition `p` that overlaps
    /// `[from, to]` (from the last ack at or before `from` to the next ack; a partition
    /// with no ack after `from` reports up to `to`).
    pub fn unavailability(&self, p: u16, from: Instant, to: Instant) -> Duration {
        let acks = self
            .shared
            .st()
            .probe_acks
            .get(&p)
            .cloned()
            .unwrap_or_default();
        let mut prev: Option<Instant> = acks.iter().rev().find(|t| **t <= from).copied();
        let mut worst = Duration::ZERO;
        let mut saw_after = false;
        for t in acks.iter().copied().filter(|t| *t > from) {
            let start = prev.unwrap_or(from);
            if start <= to {
                worst = worst.max(t.saturating_duration_since(start));
            }
            saw_after = true;
            prev = Some(t);
            if t > to {
                break;
            }
        }
        if !saw_after {
            worst = worst.max(to.saturating_duration_since(prev.unwrap_or(from)));
        }
        worst
    }

    /// Stop the clients, signal every waiting run (bounded) and return every acknowledged
    /// run plus the workload counters.
    pub async fn finish(mut self, limit: Duration) -> Result<LoadResult, ScenarioFailure> {
        self.shared.stop.store(true, Ordering::Relaxed);
        while self.tasks.join_next().await.is_some() {}
        let t0 = Instant::now(); // clock-exempt: test timeout
        let mut rot = 3u64;
        loop {
            self.shared.signal_pending(&mut rot).await;
            let left = self.shared.st().unsignalled.len();
            if left == 0 {
                break;
            }
            if t0.elapsed() > limit {
                return Err(fail(
                    "availability",
                    format!("{left} signal waits could not be signalled within {limit:?}"),
                ));
            }
            pause(Duration::from_millis(200)).await;
        }
        let st = std::mem::take(&mut *self.shared.st());
        Ok(LoadResult {
            runs: st.runs,
            dup_keys: st.dup_keys,
            given_up: st.given_up,
            rejected: st.rejected,
        })
    }
}

/// What [`Load::finish`] returns.
#[derive(Debug, Default)]
pub struct LoadResult {
    /// Every acknowledged run (probes included).
    pub runs: Vec<Started>,
    /// Keys acknowledged with two different run ids (exactly-once violations).
    pub dup_keys: Vec<String>,
    /// Keys given up after [`START_GIVE_UP`] of retries (never acknowledged).
    pub given_up: usize,
    /// Non-2xx answers to starts (retried).
    pub rejected: usize,
}

impl LoadResult {
    /// The acks.
    pub fn acks(&self) -> Vec<Ack> {
        self.runs.iter().map(|r| r.ack.clone()).collect()
    }

    /// Runs per kind.
    pub fn by_kind(&self) -> BTreeMap<Kind, usize> {
        let mut m = BTreeMap::new();
        for r in &self.runs {
            *m.entry(r.kind).or_default() += 1;
        }
        m
    }

    /// One line for the report.
    pub fn line(&self) -> String {
        format!(
            "workload: {} acknowledged runs {:?}; {} keys given up after {START_GIVE_UP:?}; {} non-2xx start answers (retried); exactly-once key violations {}",
            self.runs.len(),
            self.by_kind(),
            self.given_up,
            self.rejected,
            self.dup_keys.len()
        )
    }

    /// `Err` on an exactly-once key violation.
    pub fn verdict(&self) -> ScenarioResult {
        if self.dup_keys.is_empty() {
            Ok(())
        } else {
            Err(fail("exactly_once", format!("{:?}", self.dup_keys)))
        }
    }
}

// ------------------------------------------------------------------------- outcomes

/// Bound on one run reaching a terminal status in [`outcomes`].
pub const OUTCOME_T: Duration = Duration::from_secs(180);

/// What [`outcomes`] measured.
#[derive(Debug, Default)]
pub struct OutcomeReport {
    /// Runs checked.
    pub checked: usize,
    /// Terminal status counts.
    pub by_status: BTreeMap<String, usize>,
    /// Runs not terminal within [`OUTCOME_T`] (lost acknowledged runs).
    pub lost: Vec<String>,
    /// Runs that ended other than `completed`.
    pub not_completed: Vec<String>,
    /// Duplicate journal entries / non-increasing seqs.
    pub duplicates: Vec<String>,
}

impl OutcomeReport {
    /// One line for the report.
    pub fn line(&self) -> String {
        format!(
            "{} acknowledged runs checked: statuses {:?}; lost {}; not completed {}; duplicate journal entries {}",
            self.checked,
            self.by_status,
            self.lost.len(),
            self.not_completed.len(),
            self.duplicates.len()
        )
    }

    /// `Err` with the first offenders when anything is wrong.
    pub fn verdict(&self) -> ScenarioResult {
        let first = |v: &[String]| v[..v.len().min(10)].to_vec();
        if !self.lost.is_empty() {
            return Err(fail(
                "run_outcome",
                format!(
                    "{} acknowledged runs lost (not terminal within {OUTCOME_T:?}): {:?}",
                    self.lost.len(),
                    first(&self.lost)
                ),
            ));
        }
        if !self.not_completed.is_empty() {
            return Err(fail(
                "run_outcome",
                format!(
                    "{} runs did not complete: {:?}",
                    self.not_completed.len(),
                    first(&self.not_completed)
                ),
            ));
        }
        if !self.duplicates.is_empty() {
            return Err(fail(
                "exactly_once",
                format!(
                    "{} runs with duplicate journal entries: {:?}",
                    self.duplicates.len(),
                    first(&self.duplicates)
                ),
            ));
        }
        if self.checked == 0 {
            return Err(fail(
                "run_outcome",
                "no acknowledged run to check (vacuous)",
            ));
        }
        Ok(())
    }
}

/// `GET {path}` on the partition's leader, else any running node (the router forwards).
async fn get_for_partition(
    bases: &BTreeMap<u64, String>,
    leader: u64,
    path: &str,
) -> Option<(u16, Value)> {
    let order = std::iter::once(leader).chain(bases.keys().copied().filter(|n| *n != leader));
    for n in order {
        let Some(b) = bases.get(&n) else { continue };
        if let Some((st, v)) = call(
            reqwest::Method::GET,
            &format!("{b}{path}"),
            None,
            Duration::from_secs(5),
        )
        .await
            && (200..300).contains(&st)
        {
            return Some((st, v));
        }
    }
    None
}

/// The journal of `run`: `(seqs, step_completed names)`.
async fn journal(
    bases: &BTreeMap<u64, String>,
    leader: u64,
    run: &str,
) -> Option<(Vec<u64>, Vec<String>)> {
    let mut seqs = Vec::new();
    let mut steps = Vec::new();
    let mut from = 0u64;
    loop {
        let (_, v) = get_for_partition(
            bases,
            leader,
            &format!("/v1/runs/{run}/history?from_seq={from}&limit=1000"),
        )
        .await?;
        for e in v["events"].as_array().cloned().unwrap_or_default() {
            seqs.push(e["seq"].as_u64().unwrap_or(0));
            if e["kind"] == "step_completed" {
                steps.push(
                    e["details"]["name"]
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| e["summary"].to_string()),
                );
            }
        }
        match v["next_from_seq"].as_u64() {
            Some(n) if n > from => from = n,
            _ => break,
        }
    }
    Some((seqs, steps))
}

/// The journal verdict of a completed run (`None` = clean).
fn journal_verdict(r: &Started, j: Option<(Vec<u64>, Vec<String>)>) -> Option<String> {
    let Some((seqs, steps)) = j else {
        return Some(format!(
            "{} run {}: history unreadable",
            r.kind.workflow(),
            r.ack.run_id
        ));
    };
    if seqs.windows(2).any(|w| w[0] >= w[1]) {
        return Some(format!(
            "run {}: journal seqs not strictly increasing: {seqs:?}",
            r.ack.run_id
        ));
    }
    if r.kind == Kind::Steps {
        let distinct: BTreeSet<&String> = steps.iter().collect();
        if steps.len() != STEPS || distinct.len() != STEPS {
            return Some(format!(
                "run {}: {} step_completed entries ({} distinct), expected {STEPS}: {steps:?}",
                r.ack.run_id,
                steps.len(),
                distinct.len()
            ));
        }
    }
    None
}

/// Terminal status and journal verdict of one run.
async fn check_run(
    bases: Arc<BTreeMap<u64, String>>,
    leader: u64,
    r: Started,
) -> (Started, Option<String>, Option<String>) {
    let path = format!("/v1/runs/{}", r.ack.run_id);
    let t0 = Instant::now(); // clock-exempt: test timeout
    let status = loop {
        if let Some((_, v)) = get_for_partition(&bases, leader, &path).await
            && let Some(s) = v["status"].as_str()
            && crate::harness::inproc::TERMINAL.contains(&s)
        {
            break Some(s.to_owned());
        }
        if t0.elapsed() > OUTCOME_T {
            break None;
        }
        pause(Duration::from_millis(100)).await;
    };
    let dup = if status.as_deref() == Some("completed") {
        let j = journal(&bases, leader, &r.ack.run_id).await;
        journal_verdict(&r, j)
    } else {
        None
    };
    (r, status, dup)
}

/// The run-outcome and exactly-once oracle over `runs` (module docs), 32 runs at a time
/// against the partitions' published leaders.
pub async fn outcomes(c: &ProcCluster, runs: &[Started]) -> OutcomeReport {
    let bases: Arc<BTreeMap<u64, String>> = Arc::new(
        live(c)
            .into_iter()
            .map(|n| (n, c.client(n).base_url))
            .collect(),
    );
    let leaders: BTreeMap<u16, u64> = published_all(c)
        .iter()
        .map(|a| (a.partition, a.leader))
        .collect();
    let mut rep = OutcomeReport::default();
    let mut set = JoinSet::new();
    let mut queue = runs.iter().cloned();
    loop {
        while set.len() < 32
            && let Some(r) = queue.next()
        {
            let leader = leaders.get(&r.ack.partition).copied().unwrap_or(0);
            set.spawn(check_run(Arc::clone(&bases), leader, r));
        }
        match set.join_next().await {
            None => break,
            Some(Ok((r, status, dup))) => {
                rep.checked += 1;
                match status {
                    None => rep.lost.push(format!(
                        "{} run {} (p{})",
                        r.kind.workflow(),
                        r.ack.run_id,
                        r.ack.partition
                    )),
                    Some(s) => {
                        if s != "completed" {
                            rep.not_completed.push(format!(
                                "{} run {} (p{}) ended {s}",
                                r.kind.workflow(),
                                r.ack.run_id,
                                r.ack.partition
                            ));
                        }
                        *rep.by_status.entry(s).or_default() += 1;
                    }
                }
                if let Some(d) = dup {
                    rep.duplicates.push(d);
                }
            }
            Some(Err(e)) => panic!("harness: outcome task: {e}"),
        }
    }
    rep
}

// ------------------------------------------------------------------------- oracles

/// The Phase 12 end-of-scenario oracles over `nodes`' disks (module docs); a summary line.
pub async fn final_checks(
    c: &ProcCluster,
    nodes: &[u64],
    acks: &[Ack],
) -> Result<String, ScenarioFailure> {
    tools::wait_meta_leader(c).await?;
    for p in 0..c.spec().partitions {
        if let Some((_, hw, members)) = settle(c, p).await? {
            let views: Vec<DataDirView> = members.iter().map(|n| c.node_disk(*n)).collect();
            replica::identical_below_hw(&views, PartitionId(p), hw).into_result()?;
        }
    }
    let views: Vec<DataDirView> = nodes.iter().map(|n| c.node_disk(*n)).collect();
    let count = PartitionCount::new(c.spec().partitions).expect("partitions > 0");
    acked_on_disk_copies(&views, acks, c.spec().min_insync, count).into_result()?;
    let dump = best_dump(c);
    // 13-18: the audits continue from the snapshot state when the log was compacted (a
    // learner join compacts it): `la_rule::check_from`, `election::isr_only_from`.
    for v in &views {
        for p in 0..c.spec().partitions {
            la_rule::check_from(v, PartitionId(p), &dump.snapshot_state, &dump.entries)
                .into_result()?;
        }
    }
    election::isr_only_from(dump.snapshot_state.clone(), &dump.entries).into_result()?;
    Ok(format!(
        "replica identity, acked on disk ({} acks over {} nodes), LA rule, ISR-only elections: pass (meta snapshot index {}, {} log entries)",
        acks.len(),
        nodes.len(),
        dump.snapshot_index,
        dump.entries.len()
    ))
}

/// Every running in-sync replica of every partition holds the same state (applied offset,
/// state hash, logical digest, every table), polled up to `limit` per partition.
pub async fn standby_identity(c: &ProcCluster, limit: Duration) -> ScenarioResult {
    let count = PartitionCount::new(c.spec().partitions).expect("partitions > 0");
    for p in 0..c.spec().partitions {
        let t0 = Instant::now(); // clock-exempt: test timeout
        loop {
            let a = published(c, p);
            let members: Vec<u64> = a
                .isr
                .iter()
                .copied()
                .filter(|n| c.pid(*n).is_some())
                .collect();
            let views: Vec<DataDirView> = members.iter().map(|n| c.node_disk(*n)).collect();
            let r = standby::equal_state(&views, PartitionId(p), count);
            if r.is_ok() && views.len() >= 2 {
                break;
            }
            if t0.elapsed() > limit {
                return Err(fail(
                    "standby",
                    format!(
                        "partition {p}: replicas {members:?} not equal within {limit:?}: {:?}",
                        r.violations
                    ),
                ));
            }
            pause(Duration::from_millis(250)).await;
        }
    }
    Ok(())
}

/// Every verified partition log of `nodes`.
pub fn scans(c: &ProcCluster, nodes: &[u64]) -> Vec<LogScan> {
    let mut out = Vec::new();
    for n in nodes {
        let view = c.node_disk(*n);
        for p in 0..c.spec().partitions {
            if let Ok(s) = scan_partition(&view, PartitionId(p)) {
                out.push(s);
            }
        }
    }
    out
}

/// The apply-outcome streams of `scans` (trimmed logs skipped and counted); a real log
/// never fail-stops the replay.
pub fn outcome_streams(
    scans: &[LogScan],
    count: PartitionCount,
) -> Result<(Vec<OutcomeRecord>, usize), ScenarioFailure> {
    let mut all = Vec::new();
    let mut trimmed = 0;
    for s in scans {
        match partition_outcomes(s, count) {
            Ok(st) => {
                if let Some((off, why)) = st.halted {
                    return Err(fail(
                        "p13_upgrade",
                        format!(
                            "node {} partition {} offset {off}: replay fail-stopped: {why}",
                            s.node, s.partition.0
                        ),
                    ));
                }
                all.extend(st.records);
            }
            Err(_) => trimmed += 1,
        }
    }
    Ok((all, trimmed))
}

/// The upgrade oracles from level 1 over `nodes` (module docs); a summary line.
pub async fn upgrade_oracles(c: &ProcCluster, nodes: &[u64]) -> Result<String, ScenarioFailure> {
    let n = nodes
        .iter()
        .copied()
        .find(|n| c.pid(*n).is_some())
        .ok_or_else(|| fail("harness", "no running node for the meta log"))?;
    let meta = c.meta_log(n).await;
    let sc = scans(c, nodes);
    let r = no_gated_before_finalize_from(1, &meta, &sc);
    if !r.is_ok() {
        return Err(fail("p13_upgrade", r.violations.join("; ")));
    }
    let count = PartitionCount::new(c.spec().partitions).expect("partitions > 0");
    let (records, trimmed) = outcome_streams(&sc, count)?;
    if records.is_empty() {
        return Err(fail(
            "p13_upgrade",
            "no apply outcome was replayed (vacuous)",
        ));
    }
    no_reject_continue(&records).into_result()?;
    Ok(format!(
        "no_gated_before_finalize_from(1): pass ({} meta entries, {} partition logs); no_reject_continue: pass ({} apply outcomes, {trimmed} trimmed logs skipped)",
        meta.len(),
        sc.len(),
        records.len()
    ))
}

// ------------------------------------------------------------------------- files

/// Every partition file of `root` (everything outside `meta/`, the AOT `cache/` and `tls/`,
/// which a start may rewrite before the boot fence) with its mtime and length.
pub fn partition_files(root: &Path) -> BTreeMap<PathBuf, (SystemTime, u64)> {
    fn walk(dir: &Path, root: &Path, out: &mut BTreeMap<PathBuf, (SystemTime, u64)>) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for e in rd.flatten() {
            let p = e.path();
            let rel = p.strip_prefix(root).unwrap_or(&p).to_path_buf();
            let top = rel.components().next().map(|c| c.as_os_str().to_owned());
            if top
                .as_deref()
                .is_some_and(|t| t == "meta" || t == "cache" || t == "tls")
            {
                continue;
            }
            let Ok(md) = e.metadata() else { continue };
            if md.is_dir() {
                walk(&p, root, out);
            } else if let Ok(m) = md.modified() {
                out.insert(rel, (m, md.len()));
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

// ------------------------------------------------------------------------- timing

/// Start one `smoke` run routed to `p` through `node` and time start → `completed`
/// (polled every 5 ms); `None` when it is not acknowledged or not completed within 30 s.
pub async fn time_one_run(c: &ProcCluster, node: u64, p: u16, tag: &str) -> Option<Duration> {
    let key = tools::key_for(c, "smoke", p, tag);
    let base = c.client(node).base_url;
    let body = json!({ "input": { "k": key }, "idempotency_key": key });
    let t0 = Instant::now(); // clock-exempt: latency measurement
    let mut run = None;
    while t0.elapsed() < Duration::from_secs(30) {
        if let Some((st, v)) = call(
            reqwest::Method::POST,
            &format!("{base}/v1/workflows/smoke/runs"),
            Some(&body),
            Duration::from_secs(5),
        )
        .await
            && (200..300).contains(&st)
            && let Some(id) = v["run_id"].as_str()
        {
            run = Some(id.to_owned());
            break;
        }
        pause(Duration::from_millis(20)).await;
    }
    let run = run?;
    while t0.elapsed() < Duration::from_secs(30) {
        if let Some((200, v)) = call(
            reqwest::Method::GET,
            &format!("{base}/v1/runs/{run}"),
            None,
            Duration::from_secs(5),
        )
        .await
            && v["status"] == "completed"
        {
            return Some(t0.elapsed());
        }
        pause(Duration::from_millis(5)).await;
    }
    None
}
