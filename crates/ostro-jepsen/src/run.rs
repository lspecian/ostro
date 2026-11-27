//! Seeded Jepsen runs (14-09, D-P14-19) over the Phase 12/13 tier-C harness: a 3-process
//! RF=3 [`ProcCluster`] on `127.r.r.n` behind the Rust `ChaosProxy`.
//!
//! A [`RunSpec`] fixes the seed (`OSTRO_JEPSEN_SEED`, else random and printed), the fault
//! phase length (`OSTRO_JEPSEN_SECS`), the nemesis menu, the fault interval and hold ranges,
//! the client count and an optional bearer token (`OSTRO_JEPSEN_TOKEN`). The [`Schedule`] is
//! generated from a `StdRng` seeded with the seed **before** the run (`schedule.json`): the
//! same seed always yields the same schedule; the nodes each slot hits are resolved at
//! injection time from the slot's `pick` (who leads then is not seeded) and recorded in
//! `nemesis.jsonl`.
//!
//! Phases of [`run`]:
//!
//! 1. start the cluster, wait until healthy, deploy `crash-saga`, start the [`Workload`];
//! 2. warm-up (10 s by default);
//! 3. faults: each slot at its time (late slots run as soon as the previous one healed),
//!    injected, held, healed (partitions) or restarted (kills); no slot starts after the
//!    fault phase ends;
//! 4. heal: every proxy rule cleared, every stopped node restarted, wait until healthy;
//! 5. quiescence: no new runs; clients finish their current run; every acknowledged run
//!    polled until terminal (bounded by `quiesce_cap`, 120 s); every run observed (status,
//!    result, journal); every partition settled (its HW);
//! 6. stop the cluster (`kill -9`, data dirs kept), scan every replica's log offline (Phase
//!    12 verified scan), re-apply each on a fresh store;
//! 7. checkers ([`crate::checker`]) plus the Phase 12 `replica` and `la_rule` oracles over the
//!    same scans; report under [`report::seed_dir`]. A passing run deletes the cluster
//!    directory; a failing one keeps it (path in the report notes).
//!
//! A run never leaves processes behind: the cluster kills its children on drop, the
//! workload's tasks are aborted with it.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_repl_conformance::harness::process::{ProcCluster, ProcOptions, failpoint_bin};
use ostro_repl_conformance::harness::{ApiClient, ClusterHarness, ClusterSpec, DataDirView};
use ostro_repl_conformance::oracle::disk::{LogScan, scan_partition};
use ostro_repl_conformance::oracle::{la_rule, replica};
use ostro_repl_conformance::scenarios::process::tools;
use ostro_repl_conformance::scenarios::process_p13::{HEAL_T, wait_healthy};
use ostro_types::{Offset, PartitionCount, PartitionId};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::task::JoinSet;

use crate::checker::duplicate::check_duplicates;
use crate::checker::fencing::{check_fencing, reapply_scan};
use crate::checker::lost::{JournalEvent, LostInput, RunObservation, TERMINAL, check_lost};
use crate::checker::{LogExport, PartitionLogs, Verdict, from_scan};
use crate::history::{History, NemesisEvent, OpKind, Outcome};
use crate::nemesis::kill::{self, KillTarget};
use crate::nemesis::partition::{self, PartitionMode};
use crate::report::{self, RunReport};
use crate::workload::{self, Target, Workload, WorkloadConfig, pause};

/// The seed (decimal `u64`).
pub const SEED_ENV: &str = "OSTRO_JEPSEN_SEED";
/// Fault phase length in seconds.
pub const SECS_ENV: &str = "OSTRO_JEPSEN_SECS";
/// Bearer token of every client request.
pub const TOKEN_ENV: &str = "OSTRO_JEPSEN_TOKEN";

/// A boxed, non-`Send` future (nemeses run on the driver task).
pub type LocalBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// A nemesis outside the built-in menu (14-10: clock skew, disk stalls).
pub trait Nemesis: Send + Sync {
    /// Schedule / report name (`clock:skew`, ...).
    fn name(&self) -> String;
    /// Inject the fault with target choice `pick`; the resolved target.
    fn inject<'a>(
        &'a self,
        c: &'a mut ProcCluster,
        pick: u64,
    ) -> LocalBoxFuture<'a, Result<String, String>>;
    /// Undo it; a remark.
    fn heal<'a>(&'a self, c: &'a mut ProcCluster) -> LocalBoxFuture<'a, Result<String, String>>;
}

/// One entry of the nemesis menu.
#[derive(Clone)]
pub enum NemesisKind {
    /// A chaos-proxy partition.
    Partition(PartitionMode),
    /// `kill -9` of one node, restarted after the hold.
    Kill(KillTarget),
    /// `kill -9` of two of three nodes (quorum loss), restarted together after the hold.
    QuorumLoss,
    /// Anything else.
    Custom(Arc<dyn Nemesis>),
}

impl std::fmt::Debug for NemesisKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name())
    }
}

impl NemesisKind {
    /// Schedule / report name.
    pub fn name(&self) -> String {
        match self {
            NemesisKind::Partition(m) => m.name().to_owned(),
            NemesisKind::Kill(t) => t.name().to_owned(),
            NemesisKind::QuorumLoss => "kill:quorum_loss".to_owned(),
            NemesisKind::Custom(n) => n.name(),
        }
    }

    /// Every partition mode.
    pub fn partitions() -> Vec<NemesisKind> {
        PartitionMode::ALL
            .into_iter()
            .map(NemesisKind::Partition)
            .collect()
    }

    /// Every single-node kill target.
    pub fn kills() -> Vec<NemesisKind> {
        KillTarget::ALL.into_iter().map(NemesisKind::Kill).collect()
    }

    /// Partitions and kills (no quorum loss).
    pub fn partitions_and_kills() -> Vec<NemesisKind> {
        let mut v = Self::partitions();
        v.extend(Self::kills());
        v
    }
}

/// What a run does.
#[derive(Clone, Debug)]
pub struct RunSpec {
    /// The seed.
    pub seed: u64,
    /// Fault phase length.
    pub duration: Duration,
    /// Load before the first fault.
    pub warmup: Duration,
    /// The nemesis menu (each slot picks one, seeded).
    pub nemeses: Vec<NemesisKind>,
    /// Gap between the end of one fault and the start of the next (min, max).
    pub interval: (Duration, Duration),
    /// How long a fault is held (min, max).
    pub hold: (Duration, Duration),
    /// Workload client tasks.
    pub clients: usize,
    /// Bearer token of every client request.
    pub auth: Option<String>,
    /// Bound on the quiescence wait (acknowledged runs terminal).
    pub quiesce_cap: Duration,
    /// Partitions of the cluster.
    pub partitions: u16,
    /// Report directory.
    pub out_dir: PathBuf,
    /// Process options of every node (environment, failpoints: 14-10 nemeses).
    pub proc_options: ProcOptions,
    /// Faults at fixed points of the fault phase, added to the seeded slots (14-22: one
    /// quorum-loss window and one rolling restart per gate run): `(at, hold, kind)`. Their
    /// `pick` is derived from the seed and the entry's position; empty = the seeded
    /// schedule only (unchanged bytes).
    pub pinned: Vec<(Duration, Duration, NemesisKind)>,
    /// Keep the cluster directory after a passing run too (14-22 verifies every node's
    /// audit journal after the run); its path is in the report notes
    /// (`cluster directory kept: <path>`) and the caller removes it.
    pub keep_dirs: bool,
}

/// A seed from the wall clock and the pid (printed by the caller).
pub fn random_seed() -> u64 {
    // clock-exempt: harness timing (seed salt only)
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or_default();
    nanos ^ u64::from(std::process::id()).rotate_left(29)
}

impl RunSpec {
    /// A spec with the defaults: 10 s warm-up, 2–6 s between faults, faults held 1–8 s, 8
    /// clients, 120 s quiescence cap, 4 partitions, reports in `target/jepsen/<seed>/`.
    pub fn new(seed: u64, duration: Duration, nemeses: Vec<NemesisKind>) -> Self {
        Self {
            seed,
            duration,
            warmup: Duration::from_secs(10),
            nemeses,
            interval: (Duration::from_secs(2), Duration::from_secs(6)),
            hold: (Duration::from_secs(1), Duration::from_secs(8)),
            clients: workload::DEFAULT_CLIENTS,
            auth: None,
            quiesce_cap: Duration::from_secs(120),
            partitions: 4,
            out_dir: report::seed_dir(seed),
            proc_options: ProcOptions::default(),
            pinned: Vec::new(),
            keep_dirs: false,
        }
    }

    /// [`RunSpec::new`] with `OSTRO_JEPSEN_SEED` (else `default_seed`, else random),
    /// `OSTRO_JEPSEN_SECS` (else `default_secs`) and `OSTRO_JEPSEN_TOKEN`.
    pub fn from_env(
        default_seed: Option<u64>,
        default_secs: u64,
        nemeses: Vec<NemesisKind>,
    ) -> Self {
        let seed = std::env::var(SEED_ENV)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .or(default_seed)
            .unwrap_or_else(random_seed);
        let secs = std::env::var(SECS_ENV)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(default_secs);
        let mut spec = Self::new(seed, Duration::from_secs(secs), nemeses);
        spec.auth = std::env::var(TOKEN_ENV)
            .ok()
            .map(|t| t.trim().to_owned())
            .filter(|t| !t.is_empty());
        spec
    }

    /// The fault schedule (a pure function of the spec, see the module docs).
    pub fn schedule(&self) -> Schedule {
        let mut rng = StdRng::seed_from_u64(self.seed ^ 0x6e65_6d65_7369_7321);
        let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
        let range = |rng: &mut StdRng, (lo, hi): (Duration, Duration)| {
            let (lo, hi) = (ms(lo), ms(hi).max(ms(lo)));
            rng.random_range(lo..=hi)
        };
        let total = ms(self.duration);
        let mut slots = Vec::new();
        let mut t = 0u64;
        if !self.nemeses.is_empty() {
            loop {
                t += range(&mut rng, self.interval);
                if t >= total {
                    break;
                }
                let kind = &self.nemeses[rng.random_range(0..self.nemeses.len())];
                let hold_ms = range(&mut rng, self.hold);
                let pick: u64 = rng.random();
                slots.push(Slot {
                    index: slots.len(),
                    at_ms: t,
                    kind: kind.name(),
                    hold_ms,
                    pick,
                });
                t += hold_ms;
            }
        }
        if !self.pinned.is_empty() {
            for (i, (at, hold, kind)) in self.pinned.iter().enumerate() {
                let pick = StdRng::seed_from_u64(self.seed ^ 0x7069_6e6e_6564 ^ ((i as u64) << 48))
                    .random();
                slots.push(Slot {
                    index: 0,
                    at_ms: ms(*at).min(total.saturating_sub(1)),
                    kind: kind.name(),
                    hold_ms: ms(*hold),
                    pick,
                });
            }
            slots.sort_by_key(|s| s.at_ms);
            for (i, s) in slots.iter_mut().enumerate() {
                s.index = i;
            }
        }
        Schedule {
            seed: self.seed,
            duration_ms: total,
            warmup_ms: ms(self.warmup),
            menu: self.nemeses.iter().map(NemesisKind::name).collect(),
            slots,
        }
    }
}

/// One scheduled fault.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Slot {
    /// Position in the schedule.
    pub index: usize,
    /// Planned start (ms after the fault phase began).
    pub at_ms: u64,
    /// Nemesis name.
    pub kind: String,
    /// Hold time (ms).
    pub hold_ms: u64,
    /// Target choice (resolved at injection time).
    pub pick: u64,
}

/// The seeded fault schedule (`schedule.json`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schedule {
    /// The seed.
    pub seed: u64,
    /// Fault phase length (ms).
    pub duration_ms: u64,
    /// Warm-up (ms).
    pub warmup_ms: u64,
    /// The nemesis menu.
    pub menu: Vec<String>,
    /// The slots, in time order.
    pub slots: Vec<Slot>,
}

impl Schedule {
    /// Pretty JSON (the file format).
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_default()
    }
}

fn ms_since(t: Instant) -> u64 {
    u64::try_from(t.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// `GET path` on the first of `apis` that answers (`None` = nobody answered).
async fn get_any(apis: &[ApiClient], token: Option<&str>, path: &str) -> Option<(u16, Value)> {
    for api in apis {
        let mut rb = api
            .http
            .get(format!("{}{path}", api.base_url))
            .timeout(Duration::from_secs(5));
        if let Some(t) = token {
            rb = rb.bearer_auth(t);
        }
        let Ok(r) = rb.send().await else { continue };
        let st = r.status().as_u16();
        let Ok(b) = r.bytes().await else { continue };
        if st >= 500 {
            continue;
        }
        return Some((st, serde_json::from_slice(&b).unwrap_or(Value::Null)));
    }
    None
}

/// `PUT /v1/modules/{sha}` of the crash-saga guest through any node.
async fn deploy(c: &ProcCluster, token: Option<&str>) -> Result<(), String> {
    let wasm = ostro_repl_conformance::harness::inproc::guest_wasm(workload::WORKFLOW)
        .map_err(|e| format!("guest {}: {e}", workload::WORKFLOW))?;
    let fp = ostro_server::tls::ca::sha256_fingerprint(&wasm);
    let sha = fp.trim_start_matches("sha256:").to_owned();
    let t0 = Instant::now(); // clock-exempt: harness timing
    let mut last = String::new();
    while t0.elapsed() < Duration::from_secs(60) {
        for n in tools::live(c) {
            let api = c.client(n);
            let mut rb = api
                .http
                .put(format!("{}/v1/modules/{sha}", api.base_url))
                .header("content-type", "application/wasm")
                .body(wasm.clone())
                .timeout(Duration::from_secs(30));
            if let Some(t) = token {
                rb = rb.bearer_auth(t);
            }
            match rb.send().await {
                Ok(r) if r.status().is_success() => return Ok(()),
                Ok(r) => last = format!("node {n}: http {}", r.status()),
                Err(e) => last = format!("node {n}: {e}"),
            }
        }
        pause(Duration::from_millis(500)).await;
    }
    Err(format!("deploy {}: {last}", workload::WORKFLOW))
}

/// Status, result and journal of `run`.
async fn observe(apis: &[ApiClient], token: Option<&str>, run: &str) -> RunObservation {
    let mut obs = RunObservation::default();
    match get_any(apis, token, &format!("/v1/runs/{run}")).await {
        Some((200, v)) => {
            obs.exists = true;
            obs.status = v["status"].as_str().map(str::to_owned);
            obs.result = v.get("result").filter(|r| !r.is_null()).cloned();
        }
        _ => return obs,
    }
    let mut events = Vec::new();
    let mut from = 0u64;
    loop {
        let path = format!("/v1/runs/{run}/history?from_seq={from}&limit=1000");
        let Some((200, v)) = get_any(apis, token, &path).await else {
            return obs;
        };
        for e in v["events"].as_array().cloned().unwrap_or_default() {
            events.push(JournalEvent {
                seq: e["seq"].as_u64().unwrap_or(u64::MAX),
                kind: e["kind"].as_str().unwrap_or("").to_owned(),
                details: e.get("details").cloned().unwrap_or(Value::Null),
            });
        }
        match v["next_from_seq"].as_u64() {
            Some(n) if n > from => from = n,
            _ => break,
        }
    }
    obs.journal = Some(events);
    obs
}

/// Wait (bounded) until every run in `runs` is terminal; the ones that are not.
async fn wait_terminal(
    apis: &[ApiClient],
    token: Option<&str>,
    runs: &BTreeSet<String>,
    cap: Duration,
) -> BTreeSet<String> {
    let mut pending = runs.clone();
    let t0 = Instant::now(); // clock-exempt: harness timing
    loop {
        let mut set = JoinSet::new();
        let mut still = BTreeSet::new();
        let mut queue = pending.iter().cloned();
        loop {
            while set.len() < 32
                && let Some(r) = queue.next()
            {
                let (apis, token) = (apis.to_vec(), token.map(str::to_owned));
                set.spawn(async move {
                    let a = get_any(&apis, token.as_deref(), &format!("/v1/runs/{r}")).await;
                    let done = matches!(&a, Some((200, v)) if v["status"].as_str().is_some_and(|s| TERMINAL.contains(&s)));
                    (r, done)
                });
            }
            match set.join_next().await {
                None => break,
                Some(Ok((r, false))) => {
                    still.insert(r);
                }
                Some(_) => {}
            }
        }
        pending = still;
        if pending.is_empty() || t0.elapsed() >= cap {
            return pending;
        }
        pause(Duration::from_millis(500)).await;
    }
}

struct Driver {
    spec: RunSpec,
    history: Arc<History>,
    harness: Verdict,
    faults: BTreeMap<String, u64>,
}

impl Driver {
    fn kind(&self, name: &str) -> Option<NemesisKind> {
        self.spec
            .nemeses
            .iter()
            .chain(self.spec.pinned.iter().map(|(_, _, k)| k))
            .find(|k| k.name() == name)
            .cloned()
    }

    /// Execute one slot; `Err` = the cluster could not be restored (abort the fault phase).
    async fn slot(&mut self, c: &mut ProcCluster, slot: &Slot) -> Result<(), String> {
        let Some(kind) = self.kind(&slot.kind) else {
            return Err(format!(
                "slot {}: unknown nemesis {}",
                slot.index, slot.kind
            ));
        };
        let start_ms = self.history.now_ms();
        let hold = Duration::from_millis(slot.hold_ms);
        let mut detail = None;
        let mut fatal = None;
        let target = match &kind {
            NemesisKind::Partition(m) => match partition::inject(c, *m, slot.pick).await {
                Ok(t) => {
                    pause(hold).await;
                    partition::heal(c);
                    t
                }
                Err(e) => {
                    detail = Some(format!("skipped: {e}"));
                    String::new()
                }
            },
            NemesisKind::Kill(t) => {
                let all = c.node_ids().len();
                if tools::live(c).len() < all {
                    detail = Some("skipped: another node is down (minority rule)".into());
                    String::new()
                } else {
                    match kill::choose(c, *t, slot.pick).await {
                        None => {
                            detail = Some("skipped: no running node".into());
                            String::new()
                        }
                        Some((n, why)) => {
                            let k = kill::kill(c, n).await;
                            pause(hold).await;
                            match kill::restart(c, n).await {
                                Ok(d) => {
                                    detail =
                                        Some(format!("{k:?}; restarted in {} ms", d.as_millis()))
                                }
                                Err(e) => fatal = Some(e),
                            }
                            format!("{why} {n}")
                        }
                    }
                }
            }
            NemesisKind::QuorumLoss => match kill::quorum_loss(c, slot.pick).await {
                Ok(v) => {
                    pause(hold).await;
                    match kill::restart_many(c, &v).await {
                        Ok(d) => detail = Some(format!("restarted in {} ms", d.as_millis())),
                        Err(e) => fatal = Some(e),
                    }
                    format!("nodes {v:?}")
                }
                Err(e) => {
                    fatal = Some(e);
                    String::new()
                }
            },
            NemesisKind::Custom(n) => match n.inject(c, slot.pick).await {
                Ok(t) => {
                    pause(hold).await;
                    match n.heal(c).await {
                        Ok(r) => detail = Some(r),
                        Err(e) => fatal = Some(e),
                    }
                    t
                }
                Err(e) => {
                    detail = Some(format!("skipped: {e}"));
                    String::new()
                }
            },
        };
        if detail.as_deref().is_none_or(|d| !d.starts_with("skipped")) {
            *self.faults.entry(slot.kind.clone()).or_default() += 1;
        }
        self.history.record_nemesis(NemesisEvent {
            slot: Some(slot.index),
            kind: slot.kind.clone(),
            target,
            start_ms,
            end_ms: self.history.now_ms(),
            detail: fatal.clone().or(detail),
        });
        match fatal {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Phase 4: clear every rule, restart every stopped node, wait until healthy.
    async fn final_heal(&mut self, c: &mut ProcCluster) -> Result<(), String> {
        let start_ms = self.history.now_ms();
        partition::heal(c);
        let down: Vec<u64> = c
            .node_ids()
            .into_iter()
            .filter(|n| c.pid(*n).is_none())
            .collect();
        let mut detail = format!("restarted {down:?}");
        let res = async {
            if !down.is_empty() {
                kill::restart_many(c, &down).await?;
            }
            wait_healthy(c, &[], HEAL_T, "the final heal")
                .await
                .map_err(|f| format!("{}: {}", f.oracle, f.detail))
        }
        .await;
        match &res {
            Ok(d) => detail.push_str(&format!("; healthy after {} ms", d.as_millis())),
            Err(e) => detail.push_str(&format!("; {e}")),
        }
        self.history.record_nemesis(NemesisEvent {
            slot: None,
            kind: "heal".into(),
            target: "all".into(),
            start_ms,
            end_ms: self.history.now_ms(),
            detail: Some(detail),
        });
        res.map(|_| ())
    }
}

/// What the cluster looked like when the run settled, captured before it stopped.
struct Settled {
    views: BTreeMap<u64, DataDirView>,
    /// partition -> (hw, replicas, live ISR members)
    parts: BTreeMap<u16, (u64, Vec<u64>, Vec<u64>)>,
    dump: Option<ostro_repl_conformance::harness::inproc::MetaLogDump>,
}

async fn settle_all(c: &ProcCluster, harness: &mut Verdict) -> Settled {
    let views = c
        .node_ids()
        .into_iter()
        .map(|n| (n, c.node_disk(n)))
        .collect();
    let mut parts = BTreeMap::new();
    for p in 0..c.spec().partitions {
        let a = tools::published(c, p);
        let (hw, members) = match tools::settle(c, p).await {
            Ok(Some((_, hw, members))) => (hw.0, members),
            other => {
                // Not settled: compare below the smallest durable end of the live ISR.
                let live: Vec<u64> = a
                    .isr
                    .iter()
                    .copied()
                    .filter(|n| c.pid(*n).is_some())
                    .collect();
                let hw = live
                    .iter()
                    .filter_map(|n| tools::durable_end(c, *n, p).ok())
                    .map(|o| o.0)
                    .min()
                    .unwrap_or(0);
                harness.note(format!(
                    "partition {p} did not settle ({}); HW taken as the smallest live ISR durable end {hw}",
                    match other {
                        Err(f) => f.detail,
                        _ => "offline".into(),
                    }
                ));
                (hw, live)
            }
        };
        parts.insert(p, (hw, a.replicas.clone(), members));
    }
    let dump = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| tools::best_dump(c))).ok();
    Settled { views, parts, dump }
}

/// Phase 6: offline scans, re-apply, export; the Phase 12 oracles' violations.
fn export(settled: &Settled, count: PartitionCount) -> (LogExport, Verdict, Verdict) {
    let mut fen12 = Verdict::new("fencing");
    let mut dup12 = Verdict::new("duplicate");
    let mut out = LogExport::default();
    for (p, (hw, replicas, members)) in &settled.parts {
        let mut pl = PartitionLogs {
            partition: *p,
            hw: *hw,
            replicas: Vec::new(),
        };
        let mut isr_scans: Vec<LogScan> = Vec::new();
        for n in replicas {
            let Some(view) = settled.views.get(n) else {
                fen12.violation(format!("partition {p}: replica {n} has no data dir"));
                continue;
            };
            let scan = match scan_partition(view, PartitionId(*p)) {
                Ok(s) => s,
                Err(e) => {
                    fen12.violation(format!("partition {p} node {n}: scan failed: {e}"));
                    continue;
                }
            };
            let in_isr = members.contains(n);
            let mut rl = from_scan(&scan, in_isr);
            match reapply_scan(&scan, count) {
                Ok(a) => rl.reapply = Some(a),
                Err(e) => fen12.note(format!("partition {p} node {n}: re-apply skipped: {e}")),
            }
            if let Some(d) = &settled.dump {
                let r = la_rule::check_scan_from(&scan, &d.snapshot_state, &d.entries);
                for x in r.violations {
                    fen12.violation(format!("phase12 la_rule: {x}"));
                }
            }
            if in_isr {
                isr_scans.push(scan);
            }
            pl.replicas.push(rl);
        }
        let r = replica::compare_below_hw(&isr_scans, Offset(*hw));
        for x in r.violations {
            dup12.violation(format!("phase12 replica: {x}"));
        }
        out.partitions.push(pl);
    }
    if settled.dump.is_none() {
        fen12.note("no readable meta log: the Phase 12 la_rule oracle was skipped");
    }
    (out, fen12, dup12)
}

/// Run `spec` (module docs). `Err` only when the cluster could not start or the guest could
/// not be deployed; every later failure is a verdict in the report.
pub async fn run(spec: RunSpec) -> Result<RunReport, String> {
    let dir = spec.out_dir.clone();
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    let schedule = spec.schedule();
    std::fs::write(dir.join("schedule.json"), schedule.to_json())
        .map_err(|e| format!("write schedule: {e}"))?;
    println!(
        "ostro-jepsen: seed {} ({} faults scheduled over {} s), reports in {}",
        spec.seed,
        schedule.slots.len(),
        spec.duration.as_secs(),
        dir.display()
    );
    let history = Arc::new(History::new());
    history
        .stream_to(&dir)
        .map_err(|e| format!("history files: {e}"))?;
    let mut timings = BTreeMap::new();
    let token = spec.auth.clone();

    let t = Instant::now(); // clock-exempt: harness timing
    let cluster_spec = ClusterSpec {
        nodes: 3,
        rf: 3,
        min_insync: 2,
        partitions: spec.partitions,
        seed: spec.seed,
        ..ClusterSpec::default()
    };
    let mut c = ProcCluster::start_with(cluster_spec, spec.proc_options.clone())
        .await
        .map_err(|e| format!("start cluster: {e}"))?;
    wait_healthy(&c, &[], HEAL_T, "start")
        .await
        .map_err(|f| format!("{}: {}", f.oracle, f.detail))?;
    deploy(&c, token.as_deref()).await?;
    timings.insert("start".to_owned(), ms_since(t));

    let targets: Vec<Target> = c
        .node_ids()
        .into_iter()
        .map(|n| Target {
            node: n,
            api: c.client(n),
        })
        .collect();
    let apis: Vec<ApiClient> = targets.iter().map(|t| t.api.clone()).collect();
    let wl = Workload::start(
        WorkloadConfig {
            seed: spec.seed,
            clients: spec.clients,
            token: token.clone(),
        },
        targets,
        Arc::clone(&history),
    );
    let mut d = Driver {
        spec: spec.clone(),
        history: Arc::clone(&history),
        harness: Verdict::new("harness"),
        faults: BTreeMap::new(),
    };

    let t = Instant::now(); // clock-exempt: harness timing
    pause(spec.warmup).await;
    timings.insert("warmup".to_owned(), ms_since(t));

    let f0 = Instant::now(); // clock-exempt: harness timing
    for slot in &schedule.slots {
        let due = f0 + Duration::from_millis(slot.at_ms);
        // clock-exempt: harness timing
        if let Some(wait) = due.checked_duration_since(Instant::now()) {
            pause(wait).await;
        }
        if f0.elapsed() >= spec.duration {
            d.harness.note(format!(
                "slots from {} on not run: the fault phase ended",
                slot.index
            ));
            break;
        }
        if let Err(e) = d.slot(&mut c, slot).await {
            d.harness
                .violation(format!("slot {} ({}): {e}", slot.index, slot.kind));
            break;
        }
    }
    if let Some(rest) = spec.duration.checked_sub(f0.elapsed()) {
        pause(rest).await;
    }
    timings.insert("faults".to_owned(), ms_since(f0));

    let t = Instant::now(); // clock-exempt: harness timing
    if let Err(e) = d.final_heal(&mut c).await {
        d.harness.violation(format!("final heal: {e}"));
    }
    timings.insert("heal".to_owned(), ms_since(t));

    let t = Instant::now(); // clock-exempt: harness timing
    let expected = wl.finish(Duration::from_secs(60)).await;
    let ops = history.ops();
    let acked: BTreeSet<String> = ops
        .iter()
        .filter(|o| o.kind == OpKind::Start && o.outcome == Outcome::Ok)
        .filter_map(|o| o.run_id.clone())
        .collect();
    let pending = wait_terminal(&apis, token.as_deref(), &acked, spec.quiesce_cap).await;
    if !pending.is_empty() {
        d.harness.note(format!(
            "{} acknowledged runs not terminal within {:?} (the lost checker reports them)",
            pending.len(),
            spec.quiesce_cap
        ));
    }
    let mut observed = BTreeMap::new();
    for r in &acked {
        observed.insert(r.clone(), observe(&apis, token.as_deref(), r).await);
    }
    let settled = settle_all(&c, &mut d.harness).await;
    timings.insert("quiesce".to_owned(), ms_since(t));

    let t = Instant::now(); // clock-exempt: harness timing
    let binary = failpoint_bin().map_or_else(|| "?".to_owned(), |b| b.display().to_string());
    let root = c.stop_keeping_dirs();
    let count = PartitionCount::new(spec.partitions).ok_or("partitions must be > 0")?;
    let (logs, fen12, dup12) = export(&settled, count);
    timings.insert("export".to_owned(), ms_since(t));

    let t = Instant::now(); // clock-exempt: harness timing
    let mut lost = check_lost(LostInput {
        ops: &ops,
        runs: &observed,
        expected: &expected,
        logs: Some(&logs),
    });
    if lost.count("runs_checked") == 0 {
        lost.violation("no acknowledged run to check (vacuous run)");
    }
    let mut duplicate = check_duplicates(&logs, &observed);
    duplicate.absorb(dup12);
    let mut fencing = check_fencing(&logs, &ops);
    fencing.absorb(fen12);
    timings.insert("checks".to_owned(), ms_since(t));

    let mut by_op = BTreeMap::new();
    for o in &ops {
        *by_op
            .entry(format!(
                "{}/{}",
                serde_json::to_value(o.kind)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_owned))
                    .unwrap_or_default(),
                serde_json::to_value(o.outcome)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_owned))
                    .unwrap_or_default()
            ))
            .or_default() += 1;
    }
    let mut verdicts = vec![lost, duplicate, fencing];
    let harness = std::mem::replace(&mut d.harness, Verdict::new("harness"));
    let mut notes = Vec::new();
    let passed = verdicts.iter().all(Verdict::ok) && harness.ok();
    if !harness.ok() || !harness.notes.is_empty() {
        verdicts.push(harness);
    }
    if passed && !spec.keep_dirs {
        drop(root);
    } else {
        let kept = root.keep();
        notes.push(format!("cluster directory kept: {}", kept.display()));
    }
    let report = RunReport {
        seed: spec.seed,
        duration_secs: spec.duration.as_secs(),
        nodes: 3,
        partitions: spec.partitions,
        binary,
        ops: by_op,
        faults: d.faults,
        timings_ms: timings,
        verdicts,
        passed,
        notes,
    };
    history.flush().map_err(|e| format!("flush history: {e}"))?;
    report::write(&dir, &report, None).map_err(|e| format!("write report: {e}"))?;
    Ok(report)
}
