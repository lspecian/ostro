//! The soak driver (14-23, QA-05 soak half, D-P14-22): a fraction of the measured rated
//! load against the 1.0 configuration for a long time, sampled on an interval, judged
//! against the criteria pre-registered in `docs/soak.md` (C1..C9; the constants below are
//! that document's bounds).
//!
//! [`run`]:
//!
//! 1. the rated load: `--rated <steps/s>`, or `auto` = a 60 s [`crate::bench`] run (10 s
//!    warm-up) with the same node count and binary;
//! 2. a fresh [`Rig`] (token auth, audit, tenant quotas); `trivial-steps`, `signals`,
//!    `child-parent` and `child-leaf` deployed by the tenant token; cron schedule
//!    `soak-tick`;
//! 3. load: profile-A starts open loop at `load × rated / 10` runs/s (each long-polled to
//!    completion), a `signals` run with its signal and a `child-parent` run every second,
//!    an admin token create + revoke every 30 s;
//! 4. a [`Sample`] every `interval` (`samples.jsonl`): per node RSS, fds, data dir bytes,
//!    snapshot files, `ostro_wal_log_start`, the audit chain check, restarts; the interval's
//!    counters and latencies;
//! 5. stop: no new starts, in-flight runs get 60 s to finish; the final journal check of the
//!    last 2,000 completed profile-A runs (C8); the cluster stops; [`evaluate`] writes
//!    `verdict.json` (PASS / FAIL per criterion with values).
//!
//! Wall-clock reads in this module are soak instrumentation (`clock-exempt`).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use clap::Parser;
use ostro_repl_conformance::harness::ClusterHarness;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::task::JoinSet;

use crate::bench::{self, Api, BenchSpec, Machine, Provenance, Rig, RigConfig, TENANT, Verb};
use crate::workload::pause;

/// Smoke length in seconds (the `soak_smoke` test).
pub const SECS_ENV: &str = "OSTRO_SOAK_SECS";
/// Fraction of the rated load (the `soak_smoke` test).
pub const LOAD_ENV: &str = "OSTRO_SOAK_LOAD";
/// Rated steps/s (skips the rated benchmark of the `soak_smoke` test).
pub const RATED_ENV: &str = "OSTRO_SOAK_RATED";

/// C1: RSS slope bound for runs ≥ 24 h (% of the median per 24 h).
pub const RSS_SLOPE_PCT_PER_DAY: f64 = 2.0;
/// C1: RSS growth bound for runs < 24 h (% of the median over the post-warm-up window).
pub const SMOKE_RSS_GROWTH_PCT: f64 = 10.0;
/// C2: fd band around the median (%).
pub const FD_TOLERANCE_PCT: f64 = 5.0;
/// C2: smallest fd band (fds).
pub const FD_TOLERANCE_MIN: f64 = 64.0;
/// C3a: shortest run that judges the WAL floor.
pub const WAL_FLOOR_MIN_RUN: Duration = Duration::from_secs(2 * 3600);
/// C3b: local snapshots per partition (`[snapshots] keep`).
pub const MAX_SNAPSHOTS: u64 = 2;
/// C3c: `[audit] max_bytes` default.
pub const AUDIT_MAX_BYTES: u64 = 10 * 1024 * 1024 * 1024;
/// C4: p99 start-ack (ms), single node.
pub const START_ACK_P99_MS: f64 = 50.0;
/// C4: p99 step latency (ms, whole profile-A run), single node.
pub const STEP_P99_MS: f64 = 200.0;
/// C8: completed profile-A runs read back at the end.
pub const FINAL_SAMPLE: usize = 2000;
/// C9: achieved / target start rate.
pub const LOAD_SUSTAINED: f64 = 0.95;
/// C9: runs in flight before new starts are shed.
pub const MAX_INFLIGHT: usize = 4096;

/// The soft `RLIMIT_NOFILE` of this process (`/proc/self/limits`; the nodes inherit it).
pub fn nofile_soft() -> Option<u64> {
    std::fs::read_to_string("/proc/self/limits")
        .ok()?
        .lines()
        .find(|l| l.starts_with("Max open files"))?
        .split_whitespace()
        .nth(3)?
        .parse()
        .ok()
}

/// The in-flight cap in effect: [`MAX_INFLIGHT`], lowered to a quarter of the soft fd
/// limit (every in-flight run holds one connection in the driver and one in a node; the
/// nodes inherit the driver's limit). `docs/soak.md` amendment 2026-11-27.
pub fn inflight_cap() -> usize {
    match nofile_soft() {
        Some(n) => MAX_INFLIGHT.min(usize::try_from(n / 4).unwrap_or(MAX_INFLIGHT).max(16)),
        None => MAX_INFLIGHT,
    }
}
/// Runs longer than this use the 1 h warm-up and the C1 slope form.
pub const LONG_RUN: Duration = Duration::from_secs(24 * 3600);
/// Profile-A steps per run.
const STEPS: u64 = 10;
/// Grace for in-flight runs after the load stops (C7).
const GRACE: Duration = Duration::from_secs(60);

/// `168h`, `30m`, `300s`, `500ms`, `7d`, or plain seconds.
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let (num, unit) = match s.find(|c: char| !c.is_ascii_digit() && c != '.') {
        Some(i) => s.split_at(i),
        None => (s, "s"),
    };
    let v: f64 = num.parse().map_err(|_| format!("bad duration {s:?}"))?;
    let secs = match unit {
        "ms" => v / 1000.0,
        "s" => v,
        "m" => v * 60.0,
        "h" => v * 3600.0,
        "d" => v * 86400.0,
        _ => return Err(format!("bad duration unit in {s:?} (ms, s, m, h, d)")),
    };
    if !secs.is_finite() || secs < 0.0 {
        return Err(format!("bad duration {s:?}"));
    }
    Ok(Duration::from_secs_f64(secs))
}

/// Where the rated load comes from.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Rated {
    /// A 60 s benchmark first.
    Auto,
    /// Verified steps/s measured earlier on the same machine.
    Fixed(f64),
}

/// What a soak does.
#[derive(Clone, Debug)]
pub struct SoakSpec {
    /// 1 (RF=1) or 3 (RF=3).
    pub nodes: usize,
    /// Load phase length.
    pub duration: Duration,
    /// Fraction of the rated load (0..=1].
    pub load: f64,
    /// Rated steps/s.
    pub rated: Rated,
    /// Sampling interval.
    pub interval: Duration,
    /// Warm-up (`None` = 1 h for runs ≥ 24 h, else 60 s).
    pub warmup: Option<Duration>,
    /// Output directory (`samples.jsonl`, `verdict.json`).
    pub out: PathBuf,
    /// Seed.
    pub seed: u64,
    /// Node binary (`None` = the failpoint build).
    pub bin: Option<PathBuf>,
    /// Partitions (`None` = 16 single node, 4 at RF=3).
    pub partitions: Option<u16>,
}

impl SoakSpec {
    /// The warm-up in effect.
    pub fn warmup(&self) -> Duration {
        self.warmup.unwrap_or(if self.duration >= LONG_RUN {
            Duration::from_secs(3600)
        } else {
            Duration::from_secs(60)
        })
    }

    fn partitions(&self) -> u16 {
        self.partitions
            .unwrap_or(if self.nodes >= 3 { 4 } else { 16 })
    }
}

/// `ostro-soak` arguments.
#[derive(Clone, Debug, Parser)]
#[command(
    name = "ostro-soak",
    about = "Ostro soak driver (docs/soak.md): load × rated profile-A load plus a fixed mix, sampled, judged PASS/FAIL"
)]
pub struct SoakArgs {
    /// Processes: 1 (RF=1) or 3 (RF=3, min_insync 2).
    #[arg(long, default_value_t = 1)]
    pub nodes: usize,
    /// Load phase length (`168h`, `30m`, `300s`).
    #[arg(long)]
    pub duration: String,
    /// Fraction of the rated load.
    #[arg(long, default_value_t = 0.7)]
    pub load: f64,
    /// Rated verified steps/s on this machine, or `auto` (a 60 s benchmark first).
    #[arg(long, default_value = "auto")]
    pub rated: String,
    /// Sampling interval (default 1h for runs ≥ 24h, else 60s).
    #[arg(long)]
    pub interval: Option<String>,
    /// Warm-up (default 1h for runs ≥ 24h, else 60s).
    #[arg(long)]
    pub warmup: Option<String>,
    /// Output directory.
    #[arg(long)]
    pub out: PathBuf,
    /// Seed (loopback range, keys).
    #[arg(long, default_value_t = 1425)]
    pub seed: u64,
    /// Partitions (default 16 single node, 4 at RF=3).
    #[arg(long)]
    pub partitions: Option<u16>,
    /// The `ostro` binary of the nodes (default: the release build when present).
    #[arg(long)]
    pub bin: Option<PathBuf>,
}

impl SoakArgs {
    /// The spec.
    pub fn spec(&self) -> Result<SoakSpec, String> {
        if self.nodes != 1 && self.nodes != 3 {
            return Err(format!("--nodes must be 1 or 3, got {}", self.nodes));
        }
        if self.load.is_nan() || self.load <= 0.0 || self.load > 1.0 {
            return Err(format!("--load must be in (0, 1], got {}", self.load));
        }
        let duration = parse_duration(&self.duration)?;
        let rated = if self.rated.trim() == "auto" {
            Rated::Auto
        } else {
            let v: f64 =
                self.rated.trim().parse().map_err(|_| {
                    format!("--rated: {:?} is neither auto nor steps/s", self.rated)
                })?;
            if v.is_nan() || v <= 0.0 {
                return Err("--rated must be > 0".into());
            }
            Rated::Fixed(v)
        };
        let interval = match &self.interval {
            Some(i) => parse_duration(i)?,
            None if duration >= LONG_RUN => Duration::from_secs(3600),
            None => Duration::from_secs(60),
        };
        if interval.is_zero() {
            return Err("--interval must be > 0".into());
        }
        Ok(SoakSpec {
            nodes: self.nodes,
            duration,
            load: self.load,
            rated,
            interval,
            warmup: self.warmup.as_deref().map(parse_duration).transpose()?,
            out: self.out.clone(),
            seed: self.seed,
            bin: self.bin.clone().or_else(bench::default_bin),
            partitions: self.partitions,
        })
    }
}

// ---------------------------------------------------------------------------------
// Latency histogram (log buckets: bounded memory for 7 days)
// ---------------------------------------------------------------------------------

const H_BUCKETS: usize = 260;
const H_BASE: f64 = 1.1;

/// A latency histogram in µs with 10% wide log buckets (quantiles are bucket upper bounds).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hist {
    counts: Vec<u64>,
    n: u64,
}

impl Default for Hist {
    fn default() -> Self {
        Self {
            counts: vec![0; H_BUCKETS],
            n: 0,
        }
    }
}

impl Hist {
    /// Record `us`.
    pub fn record(&mut self, us: u64) {
        let i = if us <= 1 {
            0
        } else {
            ((us as f64).ln() / H_BASE.ln()) as usize
        };
        self.counts[i.min(H_BUCKETS - 1)] += 1;
        self.n += 1;
    }

    /// Observations.
    pub fn count(&self) -> u64 {
        self.n
    }

    /// Add `other`.
    pub fn merge(&mut self, other: &Hist) {
        for (a, b) in self.counts.iter_mut().zip(&other.counts) {
            *a += b;
        }
        self.n += other.n;
    }

    /// The `q` quantile in ms (0 when empty).
    pub fn quantile_ms(&self, q: f64) -> f64 {
        if self.n == 0 {
            return 0.0;
        }
        let want = ((q * self.n as f64).ceil() as u64).max(1);
        let mut acc = 0;
        for (i, c) in self.counts.iter().enumerate() {
            acc += c;
            if acc >= want {
                return H_BASE.powi(i as i32 + 1) / 1000.0;
            }
        }
        f64::INFINITY
    }
}

// ---------------------------------------------------------------------------------
// Shared counters
// ---------------------------------------------------------------------------------

/// Counters of one window (an interval, or the whole post-warm-up phase).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Window {
    /// Profile-A starts the generator issued.
    pub issued: u64,
    /// Profile-A starts not issued: [`inflight_cap`] runs in flight.
    pub shed: u64,
    /// Profile-A starts acknowledged.
    pub starts_ok: u64,
    /// Profile-A starts never acknowledged (gave up after 10 s).
    pub start_errors: u64,
    /// Profile-A runs completed.
    pub completed: u64,
    /// Acknowledged runs (any slice) lost (C7).
    pub lost: u64,
    /// Mix operations (signal / child) that ended as expected.
    pub mix_ok: u64,
    /// Mix operations that failed before an acknowledgement.
    pub mix_err: u64,
    /// Admin rounds (token create + revoke) acknowledged.
    pub admin_ok: u64,
    /// Admin rounds that failed.
    pub admin_err: u64,
    /// Profile-A start request → start ack.
    pub start_ack: Hist,
    /// Profile-A start request → completed result.
    pub run: Hist,
}

impl Window {
    fn merge(&mut self, o: &Window) {
        self.issued += o.issued;
        self.shed += o.shed;
        self.starts_ok += o.starts_ok;
        self.start_errors += o.start_errors;
        self.completed += o.completed;
        self.lost += o.lost;
        self.mix_ok += o.mix_ok;
        self.mix_err += o.mix_err;
        self.admin_ok += o.admin_ok;
        self.admin_err += o.admin_err;
        self.start_ack.merge(&o.start_ack);
        self.run.merge(&o.run);
    }
}

#[derive(Default)]
struct Stats {
    interval: Window,
    post_warmup: Window,
    recent: VecDeque<String>,
    lost_examples: Vec<String>,
}

struct Shared {
    stats: Mutex<Stats>,
    warm: AtomicBool,
    stop: AtomicBool,
    /// Ms after `t0` until which in-flight runs may still be polled (set at stop).
    deadline_ms: AtomicU64,
    inflight: AtomicUsize,
    /// [`inflight_cap`].
    cap: usize,
    t0: Instant,
}

impl Shared {
    fn with<R>(&self, f: impl FnOnce(&mut Window) -> R) -> R {
        let warm = self.warm.load(Ordering::Relaxed);
        let mut s = self.stats.lock().unwrap_or_else(PoisonError::into_inner);
        let mut w = Window::default();
        let r = f(&mut w);
        s.interval.merge(&w);
        if warm {
            s.post_warmup.merge(&w);
        }
        r
    }

    fn lost(&self, what: String) {
        self.with(|w| w.lost += 1);
        let mut s = self.stats.lock().unwrap_or_else(PoisonError::into_inner);
        if s.lost_examples.len() < 20 {
            s.lost_examples.push(what);
        }
    }

    fn polling(&self) -> bool {
        let d = self.deadline_ms.load(Ordering::Relaxed);
        d == 0 || ms_since(self.t0) < d
    }
}

fn ms_since(t: Instant) -> u64 {
    u64::try_from(t.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn us_since(t: Instant) -> u64 {
    u64::try_from(t.elapsed().as_micros()).unwrap_or(u64::MAX)
}

/// RAII in-flight counter.
struct Flight(Arc<Shared>);

impl Flight {
    fn new(s: &Arc<Shared>) -> Self {
        s.inflight.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(s))
    }
}

impl Drop for Flight {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Start with `key` until acknowledged or 10 s.
async fn start_acked(api: &Api, wf: &str, input: &Value, key: &str) -> Option<String> {
    let t = Instant::now(); // clock-exempt: soak timing
    while t.elapsed() < Duration::from_secs(10) {
        match api.start(wf, input, key).await {
            Ok(a) => return Some(a.run_id),
            Err(_) => pause(Duration::from_millis(50)).await,
        }
    }
    None
}

/// Poll `run` until terminal or the stop deadline: `Some((status, result))`, or `None`
/// (gone: 404, or not terminal in time).
async fn await_terminal(api: &Api, sh: &Shared, run: &str) -> Result<(String, Value), String> {
    while sh.polling() {
        match api.result(run, Duration::from_secs(10)).await {
            Ok(Some(r)) => return Ok(r),
            Ok(None) => {}
            Err(e) if e.starts_with("http 404") => return Err(format!("run {run}: 404")),
            Err(_) => pause(Duration::from_millis(50)).await,
        }
    }
    Err(format!(
        "run {run}: not terminal {GRACE:?} after the load stopped"
    ))
}

async fn profile_a(api: Api, sh: Arc<Shared>, key: String) {
    let _f = Flight::new(&sh);
    let sent = Instant::now(); // clock-exempt: soak timing
    let Some(run) = start_acked(&api, bench::WORKFLOW, &json!({ "n": STEPS }), &key).await else {
        sh.with(|w| w.start_errors += 1);
        return;
    };
    let ack = us_since(sent);
    sh.with(|w| {
        w.starts_ok += 1;
        w.start_ack.record(ack);
    });
    match await_terminal(&api, &sh, &run).await {
        Ok((status, _)) if status == "completed" => {
            let done = us_since(sent);
            sh.with(|w| {
                w.completed += 1;
                w.run.record(done);
            });
            let mut s = sh.stats.lock().unwrap_or_else(PoisonError::into_inner);
            s.recent.push_back(run);
            while s.recent.len() > FINAL_SAMPLE {
                s.recent.pop_front();
            }
        }
        Ok((status, r)) => sh.lost(format!("run {run}: ended {status}: {r}")),
        Err(e) => sh.lost(e),
    }
}

async fn signal_op(api: Api, sh: Arc<Shared>, key: String) {
    let _f = Flight::new(&sh);
    let input = json!({ "mode": "before", "sleep_ms": 200 });
    let Some(run) = start_acked(&api, "signals", &input, &key).await else {
        sh.with(|w| w.mix_err += 1);
        return;
    };
    pause(Duration::from_millis(50)).await;
    let body = json!({ "payload": key, "request_id": key });
    let path = format!("/v1/runs/{run}/signals/x");
    let sent = matches!(
        api.call_retry(&Verb::Post(body), &path, Duration::from_secs(10)).await,
        Ok((st, _)) if (200..300).contains(&st)
    );
    if !sent {
        sh.with(|w| w.mix_err += 1);
        // The run waits for the signal forever: it is acknowledged, so it counts as lost
        // only through the poll below (cancel it so it does not linger).
        let _ = api
            .call(
                &Verb::Post(json!({})),
                &format!("/v1/runs/{run}/cancel"),
                Duration::from_secs(5),
            )
            .await;
        return;
    }
    match await_terminal(&api, &sh, &run).await {
        Ok((status, r)) if status == "completed" && r == json!(key) => sh.with(|w| w.mix_ok += 1),
        Ok((status, r)) => sh.lost(format!("signals run {run}: ended {status}: {r}")),
        Err(e) => sh.lost(e),
    }
}

async fn child_op(api: Api, sh: Arc<Shared>, key: String) {
    let _f = Flight::new(&sh);
    let input = json!({ "workflow": "leaf_echo", "input": { "k": key } });
    let Some(run) = start_acked(&api, "parent_attached", &input, &key).await else {
        sh.with(|w| w.mix_err += 1);
        return;
    };
    match await_terminal(&api, &sh, &run).await {
        Ok((status, r)) if status == "completed" && r["child"]["echo"]["k"] == json!(key) => {
            sh.with(|w| w.mix_ok += 1)
        }
        Ok((status, r)) => sh.lost(format!("parent_attached run {run}: ended {status}: {r}")),
        Err(e) => sh.lost(e),
    }
}

async fn admin_op(admin: Api, sh: Arc<Shared>, n: u64) {
    let body = json!({
        "name": format!("soak-op-{n}"),
        "scopes": ["read"],
        "tenant": TENANT,
        "ttl_ms": 3_600_000u64,
    });
    let created = admin
        .call_retry(
            &Verb::Post(body),
            "/v1/auth/tokens",
            Duration::from_secs(10),
        )
        .await;
    let id = match created {
        Ok((st, v)) if (200..300).contains(&st) => v["info"]["id"].as_str().map(str::to_owned),
        _ => None,
    };
    let ok = match id {
        Some(id) => matches!(
            admin
                .call_retry(&Verb::Delete, &format!("/v1/auth/tokens/{id}"), Duration::from_secs(10))
                .await,
            Ok((st, _)) if (200..300).contains(&st)
        ),
        None => false,
    };
    sh.with(|w| {
        if ok {
            w.admin_ok += 1
        } else {
            w.admin_err += 1
        }
    });
}

/// The open-loop generator: `rate` profile-A runs/s until stop.
async fn generator(api: Api, sh: Arc<Shared>, rate: f64, seed: u64) {
    let mut credit = 0.0f64;
    let mut last = Instant::now(); // clock-exempt: soak timing
    let mut i = 0u64;
    while !sh.stop.load(Ordering::Relaxed) {
        pause(Duration::from_millis(5)).await;
        let now = Instant::now(); // clock-exempt: soak timing
        credit += rate * now.duration_since(last).as_secs_f64();
        last = now;
        while credit >= 1.0 {
            credit -= 1.0;
            if sh.inflight.load(Ordering::Relaxed) >= sh.cap {
                sh.with(|w| w.shed += 1);
                continue;
            }
            i += 1;
            sh.with(|w| w.issued += 1);
            tokio::spawn(profile_a(
                api.clone(),
                Arc::clone(&sh),
                format!("soak-{seed}-a-{i}"),
            ));
        }
    }
}

/// The fixed mix: a signals run and a child run every second, an admin round every 30 s.
async fn mixer(api: Api, admin: Api, sh: Arc<Shared>, seed: u64) {
    let mut i = 0u64;
    while !sh.stop.load(Ordering::Relaxed) {
        i += 1;
        tokio::spawn(signal_op(
            api.clone(),
            Arc::clone(&sh),
            format!("soak-{seed}-s-{i}"),
        ));
        tokio::spawn(child_op(
            api.clone(),
            Arc::clone(&sh),
            format!("soak-{seed}-c-{i}"),
        ));
        if i % 30 == 1 {
            tokio::spawn(admin_op(admin.clone(), Arc::clone(&sh), i));
        }
        pause(Duration::from_secs(1)).await;
    }
}

// ---------------------------------------------------------------------------------
// Samples
// ---------------------------------------------------------------------------------

/// One node at one sample.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeSample {
    /// Node id.
    pub node: u64,
    /// Its pid (`None` = not running).
    pub pid: Option<u32>,
    /// Restarted (pid changed) or not running since the previous sample.
    pub restarted: bool,
    /// VmRSS (kB).
    pub rss_kb: u64,
    /// RssAnon (kB): heap / anonymous memory.
    pub rss_anon_kb: u64,
    /// RssFile (kB): file-backed pages mapped by the process.
    pub rss_file_kb: u64,
    /// Open fds.
    pub fds: u64,
    /// Bytes per data subdirectory.
    pub disk: BTreeMap<String, u64>,
    /// Most `*.osnap` files of any partition.
    pub snapshots_max: u64,
    /// `ostro_wal_log_start` per partition (partitions this node reports).
    pub wal_log_start: BTreeMap<u16, u64>,
    /// The audit chain verified.
    pub audit_ok: bool,
    /// Records verified.
    pub audit_records: u64,
    /// Last seq on disk.
    pub audit_last_seq: u64,
    /// The verification error (after the retries).
    pub audit_error: Option<String>,
}

/// The driver's counters of one interval.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct IntervalStats {
    /// Interval length (s).
    pub secs: f64,
    /// The interval started after the warm-up.
    pub post_warmup: bool,
    /// Counters.
    pub issued: u64,
    /// Shed starts.
    pub shed: u64,
    /// Acknowledged starts.
    pub starts_ok: u64,
    /// Never acknowledged starts.
    pub start_errors: u64,
    /// Completed profile-A runs.
    pub completed: u64,
    /// Lost acknowledged runs.
    pub lost: u64,
    /// Mix ok / errors.
    pub mix_ok: u64,
    /// Mix errors.
    pub mix_err: u64,
    /// Admin rounds ok.
    pub admin_ok: u64,
    /// Admin rounds failed.
    pub admin_err: u64,
    /// Acknowledged starts per second.
    pub start_rate: f64,
    /// p99 start → ack (ms).
    pub start_ack_p99_ms: f64,
    /// p99 start → completed (ms).
    pub run_p99_ms: f64,
    /// Runs in flight at the sample.
    pub inflight: usize,
}

/// One line of `samples.jsonl`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    /// Seconds since the load started.
    pub t_s: f64,
    /// Taken after the warm-up (node criteria read it).
    pub post_warmup: bool,
    /// Per node.
    pub nodes: Vec<NodeSample>,
    /// The interval's counters.
    pub interval: IntervalStats,
    /// Machine load.
    pub machine: Machine,
}

fn proc_status_kb(pid: u32, key: &str) -> u64 {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .unwrap_or_default()
        .lines()
        .find_map(|l| l.strip_prefix(key))
        .and_then(|v| v.split_whitespace().next()?.parse().ok())
        .unwrap_or(0)
}

fn fd_count(pid: u32) -> u64 {
    std::fs::read_dir(format!("/proc/{pid}/fd")).map_or(0, |d| d.count() as u64)
}

fn snapshots_max(root: &Path) -> u64 {
    let Ok(rd) = std::fs::read_dir(root.join("snapshots")) else {
        return 0;
    };
    rd.flatten()
        .map(|p| {
            std::fs::read_dir(p.path()).map_or(0, |d| {
                d.flatten()
                    .filter(|f| f.file_name().to_string_lossy().ends_with(".osnap"))
                    .count() as u64
            })
        })
        .max()
        .unwrap_or(0)
}

/// `verify_dir` with two retries 250 ms apart (a record may be half-written).
async fn audit_check(dir: &Path) -> (bool, u64, u64, Option<String>) {
    let mut last = String::new();
    for i in 0..3 {
        if i > 0 {
            pause(Duration::from_millis(250)).await;
        }
        match ostro_server::audit::verify::verify_dir(dir) {
            Ok(r) => return (true, r.records, r.last_seq, None),
            Err(e) => last = e.to_string(),
        }
    }
    (false, 0, 0, Some(last))
}

struct Sampler {
    pids: BTreeMap<u64, Option<u32>>,
    file: std::fs::File,
    samples: Vec<Sample>,
}

impl Sampler {
    async fn take(
        &mut self,
        rig: &mut Rig,
        sh: &Shared,
        secs: f64,
        warmup: Duration,
        interval_start: f64,
    ) -> Result<(), String> {
        let t_s = sh.t0.elapsed().as_secs_f64();
        let mut nodes = Vec::new();
        for n in rig.cluster.node_ids() {
            let running = rig.cluster.is_running(n);
            let pid = if running { rig.cluster.pid(n) } else { None };
            let prev = self.pids.insert(n, pid).flatten();
            let restarted = pid.is_none() || prev.is_some_and(|p| Some(p) != pid);
            let root = rig.cluster.node_disk(n).root;
            let mut ns = NodeSample {
                node: n,
                pid,
                restarted,
                disk: bench::dir_sizes(&root),
                snapshots_max: snapshots_max(&root),
                ..NodeSample::default()
            };
            if let Some(p) = pid {
                ns.rss_kb = proc_status_kb(p, "VmRSS:");
                ns.rss_anon_kb = proc_status_kb(p, "RssAnon:");
                ns.rss_file_kb = proc_status_kb(p, "RssFile:");
                ns.fds = fd_count(p);
            }
            if let Some(text) = bench::scrape(&rig.cluster.client(n)).await {
                for (labels, v) in bench::samples(&text, "ostro_wal_log_start") {
                    if let Some(p) = labels
                        .split(',')
                        .find_map(|kv| kv.trim().strip_prefix("partition="))
                        .and_then(|s| s.trim_matches('"').parse::<u16>().ok())
                    {
                        ns.wal_log_start.insert(p, v as u64);
                    }
                }
            }
            let (ok, records, last, err) = audit_check(&root.join("audit")).await;
            ns.audit_ok = ok;
            ns.audit_records = records;
            ns.audit_last_seq = last;
            ns.audit_error = err;
            nodes.push(ns);
        }
        let w = {
            let mut s = sh.stats.lock().unwrap_or_else(PoisonError::into_inner);
            std::mem::take(&mut s.interval)
        };
        let interval = IntervalStats {
            secs,
            post_warmup: interval_start >= warmup.as_secs_f64(),
            issued: w.issued,
            shed: w.shed,
            starts_ok: w.starts_ok,
            start_errors: w.start_errors,
            completed: w.completed,
            lost: w.lost,
            mix_ok: w.mix_ok,
            mix_err: w.mix_err,
            admin_ok: w.admin_ok,
            admin_err: w.admin_err,
            start_rate: if secs > 0.0 {
                w.starts_ok as f64 / secs
            } else {
                0.0
            },
            start_ack_p99_ms: w.start_ack.quantile_ms(0.99),
            run_p99_ms: w.run.quantile_ms(0.99),
            inflight: sh.inflight.load(Ordering::Relaxed),
        };
        let s = Sample {
            t_s,
            post_warmup: t_s >= warmup.as_secs_f64(),
            nodes,
            interval,
            machine: Machine::sample(),
        };
        let line = serde_json::to_string(&s).map_err(|e| e.to_string())?;
        writeln!(self.file, "{line}").map_err(|e| format!("samples.jsonl: {e}"))?;
        self.file
            .flush()
            .map_err(|e| format!("samples.jsonl: {e}"))?;
        println!(
            "soak sample t={:.0}s{}: starts/s {:.1} (issued {}, shed {}), completed {}, lost {}, mix {}/{} err, start-ack p99 {:.1} ms, run p99 {:.1} ms, rss {:?} kB (anon {:?}, file {:?}), fds {:?}, audit {:?}",
            s.t_s,
            if s.post_warmup { "" } else { " (warm-up)" },
            s.interval.start_rate,
            s.interval.issued,
            s.interval.shed,
            s.interval.completed,
            s.interval.lost,
            s.interval.mix_ok,
            s.interval.mix_err,
            s.interval.start_ack_p99_ms,
            s.interval.run_p99_ms,
            s.nodes.iter().map(|n| n.rss_kb).collect::<Vec<_>>(),
            s.nodes.iter().map(|n| n.rss_anon_kb).collect::<Vec<_>>(),
            s.nodes.iter().map(|n| n.rss_file_kb).collect::<Vec<_>>(),
            s.nodes.iter().map(|n| n.fds).collect::<Vec<_>>(),
            s.nodes.iter().map(|n| n.audit_ok).collect::<Vec<_>>(),
        );
        self.samples.push(s);
        Ok(())
    }
}

// ---------------------------------------------------------------------------------
// Verdict
// ---------------------------------------------------------------------------------

/// One criterion's outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Criterion {
    /// `C1`..`C9`.
    pub id: String,
    /// Short name.
    pub name: String,
    /// `Some(true)` pass, `Some(false)` fail, `None` n/a.
    pub pass: Option<bool>,
    /// What was measured.
    pub value: String,
    /// The bound.
    pub bound: String,
}

impl Criterion {
    fn new(
        id: &str,
        name: &str,
        pass: Option<bool>,
        value: String,
        bound: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            pass,
            value,
            bound: bound.into(),
        }
    }
}

/// The end-of-run journal check (C8).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalCheck {
    /// Runs read back.
    pub runs: usize,
    /// Runs whose journal had a repeated seq or not exactly 10 distinct `step_completed`.
    pub duplicates: usize,
    /// Runs that could not be read.
    pub unreadable: usize,
    /// Examples.
    pub examples: Vec<String>,
}

/// What the criteria read besides the samples.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Evidence {
    /// Nodes.
    pub nodes: usize,
    /// Load phase (s).
    pub duration_s: f64,
    /// Warm-up (s).
    pub warmup_s: f64,
    /// Target profile-A runs/s.
    pub target_runs_per_s: f64,
    /// Post-warm-up counters.
    pub post_warmup: Window,
    /// Lost acknowledged runs (whole run, incl. warm-up and the grace period).
    pub lost_total: u64,
    /// The final check.
    pub final_check: FinalCheck,
}

fn regression_slope(xs: &[f64], ys: &[f64]) -> f64 {
    let n = xs.len() as f64;
    if xs.len() < 2 {
        return 0.0;
    }
    let mx = xs.iter().sum::<f64>() / n;
    let my = ys.iter().sum::<f64>() / n;
    let sxx: f64 = xs.iter().map(|x| (x - mx) * (x - mx)).sum();
    let sxy: f64 = xs.iter().zip(ys).map(|(x, y)| (x - mx) * (y - my)).sum();
    if sxx == 0.0 { 0.0 } else { sxy / sxx }
}

fn median(v: &[f64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    let mut s = v.to_vec();
    s.sort_by(f64::total_cmp);
    s[s.len() / 2]
}

/// Judge `samples` and `ev` against C1..C9 (`docs/soak.md` §4).
pub fn evaluate(samples: &[Sample], ev: &Evidence) -> Vec<Criterion> {
    let post: Vec<&Sample> = samples.iter().filter(|s| s.post_warmup).collect();
    let node_ids: BTreeSet<u64> = samples
        .iter()
        .flat_map(|s| s.nodes.iter().map(|n| n.node))
        .collect();
    let series = |n: u64, f: &dyn Fn(&NodeSample) -> f64| -> (Vec<f64>, Vec<f64>) {
        post.iter()
            .filter_map(|s| {
                s.nodes
                    .iter()
                    .find(|x| x.node == n && x.pid.is_some())
                    .map(|x| (s.t_s / 3600.0, f(x)))
            })
            .unzip()
    };
    let long = ev.duration_s >= LONG_RUN.as_secs_f64();
    let mut out = Vec::new();

    // C1 RSS
    let mut c1 = Some(true);
    let mut c1v = Vec::new();
    for n in &node_ids {
        let (xs, ys) = series(*n, &|x| x.rss_kb as f64);
        if xs.len() < 3 {
            c1 = Some(false);
            c1v.push(format!("node {n}: {} post-warm-up samples < 3", xs.len()));
            continue;
        }
        let med = median(&ys).max(1.0);
        let slope = regression_slope(&xs, &ys); // kB per hour
        let pct = if long {
            slope * 24.0 / med * 100.0
        } else {
            slope * (xs[xs.len() - 1] - xs[0]) / med * 100.0
        };
        // Runs >= 24 h: slope < 2% per day; smokes: growth <= 10%.
        let fails = if long {
            pct >= RSS_SLOPE_PCT_PER_DAY
        } else {
            pct > SMOKE_RSS_GROWTH_PCT
        };
        if fails {
            c1 = Some(false);
        }
        c1v.push(format!(
            "node {n}: median {:.0} kB, slope {:.1} kB/h, {} {pct:.2}%",
            med,
            slope,
            if long {
                "per 24 h"
            } else {
                "growth over the window"
            }
        ));
    }
    out.push(Criterion::new(
        "C1",
        "RSS",
        c1,
        c1v.join("; "),
        if long {
            format!("< {RSS_SLOPE_PCT_PER_DAY}% of median per 24 h")
        } else {
            format!(
                "<= {SMOKE_RSS_GROWTH_PCT}% of median over the post-warm-up window (run < 24 h)"
            )
        },
    ));

    // C2 fds
    let mut c2 = Some(true);
    let mut c2v = Vec::new();
    for n in &node_ids {
        let (_, ys) = series(*n, &|x| x.fds as f64);
        if ys.len() < 3 {
            c2 = Some(false);
            c2v.push(format!("node {n}: {} post-warm-up samples < 3", ys.len()));
            continue;
        }
        let med = median(&ys);
        let band = (med * FD_TOLERANCE_PCT / 100.0).max(FD_TOLERANCE_MIN);
        let (lo, hi) = ys
            .iter()
            .fold((f64::MAX, f64::MIN), |(l, h), y| (l.min(*y), h.max(*y)));
        if lo < med - band || hi > med + band {
            c2 = Some(false);
        }
        c2v.push(format!(
            "node {n}: median {med:.0}, range {lo:.0}..{hi:.0}, band ±{band:.0}"
        ));
    }
    out.push(Criterion::new(
        "C2",
        "open fds",
        c2,
        c2v.join("; "),
        format!("within ±{FD_TOLERANCE_PCT}% (min ±{FD_TOLERANCE_MIN}) of the post-warm-up median"),
    ));

    // C3 disk
    let mut c3 = Some(true);
    let mut c3v = Vec::new();
    if ev.duration_s >= WAL_FLOOR_MIN_RUN.as_secs_f64() {
        let floor = |s: &Sample| -> BTreeMap<u16, u64> {
            let mut m: BTreeMap<u16, u64> = BTreeMap::new();
            for n in &s.nodes {
                for (p, v) in &n.wal_log_start {
                    let e = m.entry(*p).or_default();
                    *e = (*e).max(*v);
                }
            }
            m
        };
        match (post.first(), post.last()) {
            (Some(a), Some(b)) => {
                let (fa, fb) = (floor(a), floor(b));
                let stuck: Vec<u16> = fa
                    .iter()
                    .filter(|(p, v)| fb.get(p).is_none_or(|w| w <= v))
                    .map(|(p, _)| *p)
                    .collect();
                if fa.is_empty() || !stuck.is_empty() {
                    c3 = Some(false);
                }
                c3v.push(format!(
                    "WAL floor: {} partitions, not advanced {stuck:?}",
                    fa.len()
                ));
            }
            _ => {
                c3 = Some(false);
                c3v.push("WAL floor: no post-warm-up samples".into());
            }
        }
    } else {
        c3v.push(format!(
            "WAL floor: n/a (run shorter than {} h)",
            WAL_FLOOR_MIN_RUN.as_secs() / 3600
        ));
    }
    let mut snap_bad = 0;
    let mut snap_max = 0;
    for n in &node_ids {
        let counts: Vec<u64> = samples
            .iter()
            .filter_map(|s| {
                s.nodes
                    .iter()
                    .find(|x| x.node == *n)
                    .map(|x| x.snapshots_max)
            })
            .collect();
        snap_max = snap_max.max(counts.iter().copied().max().unwrap_or(0));
        snap_bad += counts
            .windows(2)
            .filter(|w| w[0] > MAX_SNAPSHOTS && w[1] > MAX_SNAPSHOTS)
            .count();
    }
    if snap_bad > 0 {
        c3 = Some(false);
    }
    c3v.push(format!(
        "snapshots: max {snap_max} per partition, {snap_bad} consecutive samples above {MAX_SNAPSHOTS}"
    ));
    let audit_max = samples
        .iter()
        .flat_map(|s| {
            s.nodes
                .iter()
                .map(|n| n.disk.get("audit").copied().unwrap_or(0))
        })
        .max()
        .unwrap_or(0);
    if audit_max > AUDIT_MAX_BYTES {
        c3 = Some(false);
    }
    c3v.push(format!("audit dir max {audit_max} B"));
    out.push(Criterion::new(
        "C3",
        "disk bounded",
        c3,
        c3v.join("; "),
        format!(
            "WAL floor advances (runs >= 2 h); <= {MAX_SNAPSHOTS} snapshots per partition; audit <= {AUDIT_MAX_BYTES} B"
        ),
    ));

    // C4 latency
    let sa = ev.post_warmup.start_ack.quantile_ms(0.99);
    let rl = ev.post_warmup.run.quantile_ms(0.99);
    let worst_interval = samples
        .iter()
        .filter(|s| s.interval.post_warmup)
        .map(|s| s.interval.start_ack_p99_ms.max(0.0))
        .fold(0.0f64, f64::max);
    let c4 = if ev.nodes == 1 {
        Some(ev.post_warmup.start_ack.count() > 0 && sa <= START_ACK_P99_MS && rl <= STEP_P99_MS)
    } else {
        None
    };
    out.push(Criterion::new(
        "C4",
        "latency SLO",
        c4,
        format!(
            "p99 start-ack {sa:.1} ms, p99 step (whole run) {rl:.1} ms over {} runs; worst interval start-ack p99 {worst_interval:.1} ms{}",
            ev.post_warmup.run.count(),
            if ev.nodes == 1 { "" } else { " (cluster: recorded, not judged)" }
        ),
        format!("single node: start-ack p99 <= {START_ACK_P99_MS} ms, step p99 <= {STEP_P99_MS} ms"),
    ));

    // C5 restarts
    let restarts: usize = samples
        .iter()
        .map(|s| s.nodes.iter().filter(|n| n.restarted).count())
        .sum();
    out.push(Criterion::new(
        "C5",
        "restarts",
        Some(restarts == 0),
        format!("{restarts} restart / not-running observations"),
        "0",
    ));

    // C6 audit chain
    let audit_fail: Vec<String> = samples
        .iter()
        .flat_map(|s| {
            s.nodes.iter().filter(|n| !n.audit_ok).map(move |n| {
                format!(
                    "t={:.0}s node {}: {}",
                    s.t_s,
                    n.node,
                    n.audit_error.as_deref().unwrap_or("?")
                )
            })
        })
        .collect();
    let audit_last: Vec<u64> = samples
        .last()
        .map(|s| s.nodes.iter().map(|n| n.audit_last_seq).collect())
        .unwrap_or_default();
    out.push(Criterion::new(
        "C6",
        "audit chain",
        Some(!samples.is_empty() && audit_fail.is_empty()),
        if audit_fail.is_empty() {
            format!(
                "verified at all {} samples; last seq per node {audit_last:?}",
                samples.len()
            )
        } else {
            format!(
                "{} failures: {}",
                audit_fail.len(),
                audit_fail
                    .iter()
                    .take(5)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("; ")
            )
        },
        "verify_dir passes on every node at every sample",
    ));

    // C7 lost
    out.push(Criterion::new(
        "C7",
        "lost acked runs",
        Some(ev.lost_total == 0),
        format!("{} lost", ev.lost_total),
        "0",
    ));

    // C8 duplicates
    let fc = &ev.final_check;
    out.push(Criterion::new(
        "C8",
        "duplicate journal entries",
        Some(fc.runs > 0 && fc.duplicates == 0 && fc.unreadable == 0),
        format!(
            "{} runs read back: {} with duplicates / wrong step count, {} unreadable{}",
            fc.runs,
            fc.duplicates,
            fc.unreadable,
            if fc.examples.is_empty() {
                String::new()
            } else {
                format!(" ({})", fc.examples.join("; "))
            }
        ),
        format!("0 (last {FINAL_SAMPLE} completed profile-A runs: 10 distinct step_completed, no repeated seq)"),
    ));

    // C9 load sustained
    let post_secs = (ev.duration_s - ev.warmup_s).max(1e-9);
    let achieved = ev.post_warmup.starts_ok as f64 / post_secs;
    let ratio = if ev.target_runs_per_s > 0.0 {
        achieved / ev.target_runs_per_s
    } else {
        0.0
    };
    out.push(Criterion::new(
        "C9",
        "load sustained",
        Some(ratio >= LOAD_SUSTAINED),
        format!(
            "achieved {achieved:.1} acked starts/s of target {:.1} ({:.1}%), shed {}",
            ev.target_runs_per_s,
            ratio * 100.0,
            ev.post_warmup.shed
        ),
        format!(">= {:.0}% of target", LOAD_SUSTAINED * 100.0),
    ));
    out
}

/// `verdict.json`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SoakVerdict {
    /// `PASS` or `FAIL`.
    pub verdict: String,
    /// Each criterion.
    pub criteria: Vec<Criterion>,
    /// Nodes.
    pub nodes: usize,
    /// Load phase (s).
    pub duration_s: f64,
    /// Warm-up (s).
    pub warmup_s: f64,
    /// Interval (s).
    pub interval_s: f64,
    /// Load fraction.
    pub load: f64,
    /// Rated steps/s and where it came from.
    pub rated_steps_per_s: f64,
    /// `auto (bench ...)` or `--rated`.
    pub rated_source: String,
    /// Target profile-A runs/s.
    pub target_runs_per_s: f64,
    /// Binary and HEAD.
    pub provenance: Provenance,
    /// Machine load at the start.
    pub machine_start: Machine,
    /// Machine load at the end.
    pub machine_end: Machine,
    /// The evidence the criteria read.
    pub evidence: Evidence,
    /// Examples of lost runs.
    pub lost_examples: Vec<String>,
    /// Remarks.
    pub notes: Vec<String>,
}

impl SoakVerdict {
    /// Every criterion passed or is n/a.
    pub fn passed(&self) -> bool {
        self.verdict == "PASS"
    }

    /// The printed summary.
    pub fn summary(&self) -> String {
        let mut s = String::new();
        let _ = writeln!(
            s,
            "== soak verdict: {} == {} node(s), {:.0} s (warm-up {:.0} s, interval {:.0} s), load {} x rated {:.1} steps/s ({}) = {:.1} runs/s",
            self.verdict,
            self.nodes,
            self.duration_s,
            self.warmup_s,
            self.interval_s,
            self.load,
            self.rated_steps_per_s,
            self.rated_source,
            self.target_runs_per_s
        );
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
            "machine start: load {} | {} cpus | {} MB available; end: load {} | {} MB available",
            self.machine_start.loadavg,
            self.machine_start.nproc,
            self.machine_start.mem_available_kb / 1024,
            self.machine_end.loadavg,
            self.machine_end.mem_available_kb / 1024
        );
        for c in &self.criteria {
            let _ = writeln!(
                s,
                "{} {:<26} {:<4} | {} | bound: {}",
                c.id,
                c.name,
                match c.pass {
                    Some(true) => "PASS",
                    Some(false) => "FAIL",
                    None => "n/a",
                },
                c.value,
                c.bound
            );
        }
        for l in &self.lost_examples {
            let _ = writeln!(s, "lost: {l}");
        }
        for n in &self.notes {
            let _ = writeln!(s, "note: {n}");
        }
        s
    }
}

/// The C8 read-back of `runs`.
async fn final_check(api: &Api, runs: Vec<String>) -> FinalCheck {
    let mut fc = FinalCheck {
        runs: runs.len(),
        ..FinalCheck::default()
    };
    let mut set = JoinSet::new();
    let mut queue = runs.into_iter();
    loop {
        while set.len() < 32
            && let Some(r) = queue.next()
        {
            let api = api.clone();
            set.spawn(async move {
                let ev = api.history(&r).await;
                (r, ev)
            });
        }
        match set.join_next().await {
            None => break,
            Some(Ok((r, Ok(events)))) => {
                let seqs: Vec<u64> = events.iter().map(|(s, _)| *s).collect();
                let distinct: BTreeSet<u64> = seqs.iter().copied().collect();
                let steps: BTreeSet<u64> = events
                    .iter()
                    .filter(|(_, k)| k == "step_completed")
                    .map(|(s, _)| *s)
                    .collect();
                if distinct.len() != seqs.len() || steps.len() as u64 != STEPS {
                    fc.duplicates += 1;
                    if fc.examples.len() < 5 {
                        fc.examples.push(format!(
                            "run {r}: {} events, {} distinct seqs, {} step_completed",
                            seqs.len(),
                            distinct.len(),
                            steps.len()
                        ));
                    }
                }
            }
            Some(Ok((r, Err(e)))) => {
                fc.unreadable += 1;
                if fc.examples.len() < 5 {
                    fc.examples.push(format!("run {r}: {e}"));
                }
            }
            Some(Err(e)) => {
                fc.unreadable += 1;
                if fc.examples.len() < 5 {
                    fc.examples.push(format!("task: {e}"));
                }
            }
        }
    }
    fc
}

/// Run `spec` (module docs): the verdict, also written to `<out>/verdict.json`. `Err` only
/// when the soak could not start.
pub async fn run(spec: &SoakSpec) -> Result<SoakVerdict, String> {
    std::fs::create_dir_all(&spec.out)
        .map_err(|e| format!("create {}: {e}", spec.out.display()))?;
    for f in ["samples.jsonl", "verdict.json", "rated-bench.json"] {
        let _ = std::fs::remove_file(spec.out.join(f));
    }
    let machine_start = Machine::sample();
    let mut notes = Vec::new();
    let warmup = spec.warmup();

    // 1. Rated load.
    let (rated, rated_source) = match spec.rated {
        Rated::Fixed(v) => (v, "--rated".to_owned()),
        Rated::Auto => {
            let mut b = if spec.nodes >= 3 {
                BenchSpec::rf3()
            } else {
                BenchSpec::single()
            };
            b.bin = spec.bin.clone();
            b.duration = Duration::from_secs(60);
            b.warmup = Duration::from_secs(10);
            b.partitions = spec.partitions();
            println!(
                "ostro-soak: measuring the rated load (60 s benchmark, {} node(s))",
                spec.nodes
            );
            let r = bench::run(&b).await?;
            println!("{}", r.table());
            let json = serde_json::to_vec_pretty(&r).map_err(|e| e.to_string())?;
            std::fs::write(spec.out.join("rated-bench.json"), json)
                .map_err(|e| format!("rated-bench.json: {e}"))?;
            if !r.correct() {
                return Err(format!(
                    "the rated benchmark failed its on-disk check: {:?}",
                    r.violations
                ));
            }
            (
                r.steps_per_s,
                format!("auto: 60 s bench, {:.1} verified steps/s", r.steps_per_s),
            )
        }
    };
    if rated.is_nan() || rated <= 0.0 {
        return Err(format!("rated load {rated} steps/s"));
    }
    let target = spec.load * rated / STEPS as f64;
    println!(
        "ostro-soak: {} node(s), {:?}, load {} x {rated:.1} steps/s = {target:.1} profile-A runs/s, interval {:?}, warm-up {warmup:?}, out {}",
        spec.nodes,
        spec.duration,
        spec.load,
        spec.interval,
        spec.out.display()
    );

    notes.push(format!(
        "fd limit (soft) {:?}: in-flight cap {} (raise with `ulimit -n 65536` before the run)",
        nofile_soft(),
        inflight_cap()
    ));

    // 2. Cluster.
    let mut rig = Rig::start(&RigConfig {
        nodes: spec.nodes,
        partitions: spec.partitions(),
        seed: spec.seed,
        auth: true,
        audit: true,
        tenant_quotas: true,
        bin: spec.bin.clone(),
    })
    .await?;
    for g in [bench::WORKFLOW, "signals", "child-parent", "child-leaf"] {
        rig.deploy(g).await?;
    }
    let api = rig.api(0);
    let admin = rig.admin_api();
    // Schedule creation needs the admin scope (14-15 authz) and schedule names cannot be
    // tenant-qualified: the system admin creates it with a plain name and the tenant's
    // qualified workflow.
    let sched = json!({
        "name": "soak-tick",
        "workflow": format!("{TENANT}/{}", bench::WORKFLOW),
        "cron": "*/10 * * * * *",
        "input": { "n": STEPS },
    });
    match admin
        .call_retry(&Verb::Post(sched), "/v1/schedules", Duration::from_secs(30))
        .await
    {
        Ok((st, _)) if (200..300).contains(&st) => {
            notes.push("cron schedule soak-tick created".into())
        }
        other => notes.push(format!("cron schedule soak-tick NOT created: {other:?}")),
    }

    // 3. Load.
    let sh = Arc::new(Shared {
        stats: Mutex::new(Stats::default()),
        warm: AtomicBool::new(warmup.is_zero()),
        stop: AtomicBool::new(false),
        deadline_ms: AtomicU64::new(0),
        inflight: AtomicUsize::new(0),
        cap: inflight_cap(),
        t0: Instant::now(), // clock-exempt: soak timing
    });
    let file = std::fs::File::create(spec.out.join("samples.jsonl"))
        .map_err(|e| format!("samples.jsonl: {e}"))?;
    let mut sampler = Sampler {
        pids: rig
            .cluster
            .node_ids()
            .into_iter()
            .map(|n| (n, rig.cluster.pid(n)))
            .collect(),
        file,
        samples: Vec::new(),
    };
    let gen_task = tokio::spawn(generator(api.clone(), Arc::clone(&sh), target, spec.seed));
    let mix_task = tokio::spawn(mixer(
        api.clone(),
        admin.clone(),
        Arc::clone(&sh),
        spec.seed,
    ));

    // 4. Samples.
    let mut k = 1u32;
    let mut prev = 0.0f64;
    loop {
        let due = spec.interval.saturating_mul(k).min(spec.duration);
        // Wait for the sample, flipping the warm flag on time even with a long interval.
        loop {
            let el = sh.t0.elapsed();
            let warm = el >= warmup;
            if warm {
                sh.warm.store(true, Ordering::Relaxed);
            }
            if el >= due {
                break;
            }
            let next = if !warm && warmup < due { warmup } else { due };
            pause(next.saturating_sub(el).max(Duration::from_millis(1))).await;
        }
        let now = sh.t0.elapsed().as_secs_f64();
        let at_end = due >= spec.duration;
        if at_end {
            // 5. Stop the load first so the last interval is the load phase only.
            sh.stop.store(true, Ordering::SeqCst);
        }
        sampler
            .take(&mut rig, &sh, now - prev, warmup, prev)
            .await?;
        prev = now;
        if at_end {
            break;
        }
        k += 1;
    }
    let deadline = ms_since(sh.t0) + u64::try_from(GRACE.as_millis()).unwrap_or(60_000);
    sh.deadline_ms.store(deadline, Ordering::SeqCst);
    let _ = gen_task.await;
    let _ = mix_task.await;
    let t = Instant::now(); // clock-exempt: soak timing
    while sh.inflight.load(Ordering::SeqCst) > 0 && t.elapsed() < GRACE + Duration::from_secs(30) {
        pause(Duration::from_millis(200)).await;
    }
    let stuck = sh.inflight.load(Ordering::SeqCst);
    if stuck > 0 {
        notes.push(format!(
            "{stuck} operations still in flight after the grace period"
        ));
    }
    match admin
        .call_retry(
            &Verb::Get,
            "/v1/schedules/soak-tick",
            Duration::from_secs(10),
        )
        .await
    {
        Ok((200, _)) => notes.push("cron schedule soak-tick present at the end".into()),
        other => notes.push(format!("cron schedule soak-tick at the end: {other:?}")),
    }
    let (recent, lost_examples, post_warmup, late) = {
        let mut s = sh.stats.lock().unwrap_or_else(PoisonError::into_inner);
        let late = std::mem::take(&mut s.interval);
        (
            s.recent.iter().cloned().collect::<Vec<_>>(),
            s.lost_examples.clone(),
            s.post_warmup.clone(),
            late,
        )
    };
    let lost_total = sampler.samples.iter().map(|s| s.interval.lost).sum::<u64>() + late.lost;
    let fc = final_check(&api, recent).await;
    for n in rig.cluster.node_ids() {
        let tail = rig.cluster.log_tail(n, 200);
        let _ = std::fs::write(spec.out.join(format!("node{n}.log.tail")), tail);
    }
    let provenance = rig.provenance.clone();
    notes.extend(rig.notes.iter().cloned());
    drop(rig);

    let ev = Evidence {
        nodes: spec.nodes,
        duration_s: spec.duration.as_secs_f64(),
        warmup_s: warmup.as_secs_f64(),
        target_runs_per_s: target,
        post_warmup,
        lost_total,
        final_check: fc,
    };
    let criteria = evaluate(&sampler.samples, &ev);
    let passed = criteria.iter().all(|c| c.pass != Some(false));
    let v = SoakVerdict {
        verdict: if passed { "PASS" } else { "FAIL" }.into(),
        criteria,
        nodes: spec.nodes,
        duration_s: ev.duration_s,
        warmup_s: ev.warmup_s,
        interval_s: spec.interval.as_secs_f64(),
        load: spec.load,
        rated_steps_per_s: rated,
        rated_source,
        target_runs_per_s: target,
        provenance,
        machine_start,
        machine_end: Machine::sample(),
        evidence: ev,
        lost_examples,
        notes,
    };
    let json = serde_json::to_vec_pretty(&v).map_err(|e| e.to_string())?;
    std::fs::write(spec.out.join("verdict.json"), json)
        .map_err(|e| format!("verdict.json: {e}"))?;
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(t_s: f64, post: bool, rss: u64, fds: u64) -> Sample {
        Sample {
            t_s,
            post_warmup: post,
            nodes: vec![NodeSample {
                node: 1,
                pid: Some(42),
                rss_kb: rss,
                fds,
                audit_ok: true,
                ..NodeSample::default()
            }],
            interval: IntervalStats {
                post_warmup: post,
                ..IntervalStats::default()
            },
            machine: Machine::default(),
        }
    }

    fn evidence(duration_s: f64) -> Evidence {
        let mut w = Window {
            starts_ok: 1000,
            ..Window::default()
        };
        for _ in 0..1000 {
            w.start_ack.record(5_000);
            w.run.record(20_000);
        }
        Evidence {
            nodes: 1,
            duration_s,
            warmup_s: 60.0,
            target_runs_per_s: 1000.0 / (duration_s - 60.0),
            post_warmup: w,
            lost_total: 0,
            final_check: FinalCheck {
                runs: 10,
                ..FinalCheck::default()
            },
        }
    }

    fn get<'a>(c: &'a [Criterion], id: &str) -> &'a Criterion {
        c.iter().find(|x| x.id == id).expect("criterion")
    }

    #[test]
    fn durations_parse() {
        assert_eq!(parse_duration("168h"), Ok(Duration::from_secs(168 * 3600)));
        assert_eq!(parse_duration("300"), Ok(Duration::from_secs(300)));
        assert_eq!(parse_duration("500ms"), Ok(Duration::from_millis(500)));
        assert_eq!(parse_duration("7d"), Ok(Duration::from_secs(7 * 86400)));
        assert!(parse_duration("5x").is_err());
    }

    #[test]
    fn hist_quantiles_are_bucket_bounds() {
        let mut h = Hist::default();
        for i in 1..=100u64 {
            h.record(i * 1000);
        }
        let p99 = h.quantile_ms(0.99);
        assert!((99.0..=110.0).contains(&p99), "{p99}");
        assert_eq!(Hist::default().quantile_ms(0.99), 0.0);
    }

    #[test]
    fn flat_smoke_passes() {
        let s: Vec<Sample> = (1..=10u32)
            .map(|i| sample(30.0 * f64::from(i), i >= 2, 100_000, 200))
            .collect();
        let c = evaluate(&s, &evidence(300.0));
        assert!(c.iter().all(|x| x.pass != Some(false)), "{c:#?}");
        assert_eq!(get(&c, "C3").pass, Some(true));
    }

    #[test]
    fn growing_rss_fails_c1() {
        let s: Vec<Sample> = (1..=10u32)
            .map(|i| {
                sample(
                    30.0 * f64::from(i),
                    i >= 2,
                    100_000 + 5_000 * u64::from(i),
                    200,
                )
            })
            .collect();
        let c = evaluate(&s, &evidence(300.0));
        assert_eq!(get(&c, "C1").pass, Some(false), "{c:#?}");
    }

    #[test]
    fn long_run_uses_the_daily_slope() {
        // 0.5% of 100 MB per day over 7 days: passes; 5% per day: fails.
        let mk = |per_day: f64| -> Vec<Sample> {
            (1..=168u32)
                .map(|h| {
                    let rss = 100_000.0 + per_day / 24.0 * f64::from(h);
                    sample(3600.0 * f64::from(h), h >= 2, rss as u64, 200)
                })
                .collect()
        };
        let ev = evidence(168.0 * 3600.0);
        assert_eq!(get(&evaluate(&mk(500.0), &ev), "C1").pass, Some(true));
        assert_eq!(get(&evaluate(&mk(5_000.0), &ev), "C1").pass, Some(false));
    }

    #[test]
    fn too_few_samples_fail_c1_c2() {
        let s = vec![sample(30.0, false, 1, 1), sample(60.0, true, 1, 1)];
        let c = evaluate(&s, &evidence(300.0));
        assert_eq!(get(&c, "C1").pass, Some(false));
        assert_eq!(get(&c, "C2").pass, Some(false));
    }

    #[test]
    fn fd_leak_restart_lost_and_slow_fail() {
        let mut s: Vec<Sample> = (1..=10u32)
            .map(|i| {
                sample(
                    30.0 * f64::from(i),
                    i >= 2,
                    100_000,
                    200 + 30 * u64::from(i),
                )
            })
            .collect();
        s[5].nodes[0].restarted = true;
        s[6].nodes[0].audit_ok = false;
        s[7].nodes[0].snapshots_max = 3;
        s[8].nodes[0].snapshots_max = 3;
        let mut ev = evidence(300.0);
        ev.lost_total = 1;
        ev.final_check.duplicates = 1;
        for _ in 0..100 {
            ev.post_warmup.start_ack.record(80_000);
        }
        ev.post_warmup.starts_ok = 10;
        let c = evaluate(&s, &ev);
        for id in ["C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9"] {
            assert_eq!(get(&c, id).pass, Some(false), "{id}: {c:#?}");
        }
    }

    #[test]
    fn cluster_latency_is_recorded_not_judged() {
        let s: Vec<Sample> = (1..=10u32)
            .map(|i| sample(30.0 * f64::from(i), i >= 2, 100_000, 200))
            .collect();
        let mut ev = evidence(300.0);
        ev.nodes = 3;
        for _ in 0..1000 {
            ev.post_warmup.run.record(900_000);
        }
        assert_eq!(get(&evaluate(&s, &ev), "C4").pass, None);
    }
}
