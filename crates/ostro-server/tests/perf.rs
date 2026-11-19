//! Phase 5 success criterion 4 benchmark (plan 05-16; D15, D16). `#[ignore]`d: run it by
//! hand, in release, on the machine being measured:
//!
//! ```text
//! OSTRO_BENCH_PROFILE=a cargo test --release -p ostro-server --test perf --locked -- --ignored --nocapture
//! ```
//!
//! # What is counted (D16)
//!
//! A step counts only when its `StepCompleted` was **acknowledged** (the run's result came
//! back through the API as `completed`, with the guest's own step count) **and is found on
//! disk after a restart**: [`verify_on_disk`] shuts the server down, starts it again on the
//! same data dir and counts `step_completed` events through the history API. Steps/s uses
//! the verified count only; a mismatch with the acknowledged count fails the bench.
//!
//! # Profiles
//!
//! - `a` (the gate): runs of the `trivial-steps` guest with `{n: 10}`; default 2,000 runs.
//! - `b` (reported): `{n: 1}`; default 10,000 runs.
//! - `expiry` (OPS-06 cost): one run with 50,000 journal events and retention 1 s. A probe
//!   submits a rejected command to the same partition every millisecond; its worst latency
//!   while the expiry timer fires is the time apply was blocked by the EXPIRE_OP chunk.
//!
//! # Environment
//!
//! | variable | default | meaning |
//! |---|---|---|
//! | `OSTRO_BENCH_PROFILE` | `a` | `a`, `b` or `expiry` |
//! | `OSTRO_BENCH_RUNS` | A 2,000 / B 10,000 | runs to complete |
//! | `OSTRO_BENCH_CONCURRENCY` | 64 | runs in flight (closed loop: start, await result) |
//! | `OSTRO_BENCH_TRANSPORT` | `inproc` | `inproc` (ControlPlane calls) or `http` (REST) |
//! | `OSTRO_BENCH_PARTITIONS` | server default (16) | partitions |
//! | `OSTRO_BENCH_DATA_DIR` | `CARGO_TARGET_TMPDIR` | parent of the data dir (same fs as `target/`) |
//! | `OSTRO_BENCH_ALLOW_TMPFS` | unset | `1` allows a tmpfs/ramfs data dir |
//! | `OSTRO_BENCH_EXPIRY_EVENTS` | 50,000 | journal events of the expiry run |
//!
//! Output: one `PERF_JSON {...}` line plus a human table. fsyncs/s is the block device's
//! completed flush requests (`/proc/diskstats` field 19 of the device holding the data dir),
//! a device-wide proxy that also counts other writers; CPU is this process's
//! `utime + stime` (`/proc/self/stat`). Percentiles are computed by sorting (no new
//! dependencies). WSL2 numbers are advisory (D15); the gate is native Linux NVMe.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::path::{Path, PathBuf};
use std::process::Command as Process;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::{ServerOpts, TestServer, server_config};
use ostro_api::ControlPlane;
use ostro_api::domain::{AwaitResultRequest, HistoryRequest, StartRunRequest};
use ostro_partition::store::values::RunStatus;
use ostro_types::command::SignalRun;
use ostro_types::journal::EventKind;
use ostro_types::{Command, RunId};
use serde_json::json;
use tokio::sync::Mutex;

const GUEST: &str = "trivial-steps";
const LIMIT: Duration = Duration::from_secs(600);

// ------------------------------------------------------------------- configuration

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Profile {
    A,
    B,
    Expiry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Transport {
    Inproc,
    Http,
}

#[derive(Clone, Debug)]
struct Cfg {
    profile: Profile,
    runs: usize,
    steps: u32,
    concurrency: usize,
    transport: Transport,
    partitions: Option<u16>,
    data_parent: PathBuf,
    allow_tmpfs: bool,
    expiry_events: u64,
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn env_num<T: std::str::FromStr>(name: &str) -> Option<T> {
    env(name).map(|v| {
        v.parse()
            .unwrap_or_else(|_| panic!("{name}={v}: not a number"))
    })
}

impl Cfg {
    fn from_env() -> Self {
        let profile = match env("OSTRO_BENCH_PROFILE").as_deref().unwrap_or("a") {
            "a" | "A" => Profile::A,
            "b" | "B" => Profile::B,
            "expiry" => Profile::Expiry,
            p => panic!("OSTRO_BENCH_PROFILE={p}: expected a, b or expiry"),
        };
        let (runs, steps) = match profile {
            Profile::A => (2_000, 10),
            Profile::B => (10_000, 1),
            Profile::Expiry => (1, 0),
        };
        let transport = match env("OSTRO_BENCH_TRANSPORT").as_deref().unwrap_or("inproc") {
            "inproc" => Transport::Inproc,
            "http" => Transport::Http,
            t => panic!("OSTRO_BENCH_TRANSPORT={t}: expected inproc or http"),
        };
        Self {
            profile,
            runs: env_num("OSTRO_BENCH_RUNS").unwrap_or(runs),
            steps,
            concurrency: env_num("OSTRO_BENCH_CONCURRENCY").unwrap_or(64).max(1),
            transport,
            partitions: env_num("OSTRO_BENCH_PARTITIONS"),
            data_parent: env("OSTRO_BENCH_DATA_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(env!("CARGO_TARGET_TMPDIR"))),
            allow_tmpfs: env("OSTRO_BENCH_ALLOW_TMPFS").as_deref() == Some("1"),
            expiry_events: env_num("OSTRO_BENCH_EXPIRY_EVENTS").unwrap_or(50_000),
        }
    }
}

// ------------------------------------------------------------------- environment probes

fn sh(cmd: &str, args: &[&str]) -> String {
    Process::new(cmd)
        .args(args)
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_default()
}

fn fs_type(p: &Path) -> String {
    sh("stat", &["-f", "-c", "%T", &p.to_string_lossy()])
}

fn wall_ns() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("ns")
}

/// `utime + stime` of this process in seconds.
fn cpu_secs() -> f64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    // Fields after the parenthesised comm; utime/stime are fields 14/15 (1-based).
    let rest = stat.rsplit_once(')').map(|(_, r)| r).unwrap_or("");
    let f: Vec<&str> = rest.split_whitespace().collect();
    let ticks: f64 = f
        .get(11)
        .and_then(|u| u.parse::<f64>().ok())
        .zip(f.get(12).and_then(|s| s.parse::<f64>().ok()))
        .map_or(0.0, |(u, s)| u + s);
    let hz: f64 = sh("getconf", &["CLK_TCK"]).parse().unwrap_or(100.0);
    ticks / hz
}

/// The whole-disk `major:minor` holding `p` (a partition is mapped to its parent).
fn block_device(p: &Path) -> Option<String> {
    let dev = sh("stat", &["-c", "%d", &p.to_string_lossy()]);
    let dev: u64 = dev.parse().ok()?;
    // Linux dev_t encoding (glibc gnu_dev_major/minor).
    let major = ((dev >> 8) & 0xfff) | ((dev >> 32) & !0xfff);
    let minor = (dev & 0xff) | ((dev >> 12) & !0xff);
    let sys = PathBuf::from(format!("/sys/dev/block/{major}:{minor}"));
    if sys.join("partition").exists() {
        let parent = std::fs::canonicalize(&sys).ok()?.parent()?.join("dev");
        return std::fs::read_to_string(parent)
            .ok()
            .map(|s| s.trim().to_owned());
    }
    Some(format!("{major}:{minor}"))
}

/// Completed flush requests of `dev` (`/proc/diskstats` field 19), if the kernel reports it.
fn device_flushes(dev: Option<&str>) -> Option<u64> {
    let dev = dev?;
    let (maj, min) = dev.split_once(':')?;
    let stats = std::fs::read_to_string("/proc/diskstats").ok()?;
    stats.lines().find_map(|l| {
        let f: Vec<&str> = l.split_whitespace().collect();
        (f.first() == Some(&maj) && f.get(1) == Some(&min))
            .then(|| f.get(18).and_then(|v| v.parse().ok()))
            .flatten()
    })
}

/// Apparent bytes of every file under `p`.
fn bytes_on_disk(p: &Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![p.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            match e.metadata() {
                Ok(m) if m.is_dir() => stack.push(e.path()),
                Ok(m) => total += m.len(),
                Err(_) => {}
            }
        }
    }
    total
}

/// Median latency of `n` 4 KiB write + fdatasync pairs on a file in `dir` (raw fsync cost).
fn fsync_cost(dir: &Path, n: usize) -> Duration {
    use std::io::Write;
    let path = dir.join("fsync-probe");
    let mut f = std::fs::File::create(&path).expect("probe file");
    let buf = [0x5au8; 4096];
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        f.write_all(&buf).expect("write");
        f.sync_data().expect("fdatasync");
        v.push(t.elapsed());
    }
    let _ = std::fs::remove_file(&path);
    v.sort();
    v[v.len() / 2]
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

// ------------------------------------------------------------------- server and clients

struct Bench {
    cfg: Cfg,
    s: TestServer,
    data: PathBuf,
}

async fn boot(cfg: &Cfg, tweak: impl FnOnce(&mut ostro_server::config::ServerConfig)) -> Bench {
    std::fs::create_dir_all(&cfg.data_parent).expect("bench data parent");
    let fs = fs_type(&cfg.data_parent);
    if matches!(fs.as_str(), "tmpfs" | "ramfs") && !cfg.allow_tmpfs {
        panic!(
            "bench data dir {} is on {fs}: fsync is free there; set OSTRO_BENCH_DATA_DIR to a \
             disk-backed path or OSTRO_BENCH_ALLOW_TMPFS=1",
            cfg.data_parent.display()
        );
    }
    let dir = tempfile::Builder::new()
        .prefix("ostro-bench-")
        .tempdir_in(&cfg.data_parent)
        .expect("bench data dir");
    let data = dir.path().to_path_buf();
    let mut opts = ServerOpts::default();
    if let Some(p) = cfg.partitions {
        opts.partitions = p;
    } else {
        opts.partitions = ostro_server::config::ServerConfig::server_defaults()
            .node
            .partitions;
    }
    let mut sc = server_config(&data, &opts);
    tweak(&mut sc);
    let s = TestServer::start_cfg(sc, Some(dir)).await;
    s.wait_ready(Duration::from_secs(60)).await;
    s.deploy_guest(GUEST, None).await;
    Bench {
        cfg: cfg.clone(),
        s,
        data,
    }
}

impl Bench {
    fn cp(&self) -> Arc<ostro_server::control::ServerControlPlane> {
        Arc::clone(self.s.h().control())
    }

    /// Start one run and wait for its result; returns `(run id, steps the guest reported)`.
    async fn one(
        &self,
        cp: &ostro_server::control::ServerControlPlane,
        http: &reqwest::Client,
        base: &str,
        input: serde_json::Value,
        retention_ms: Option<u64>,
    ) -> (RunId, Option<u64>) {
        match self.cfg.transport {
            Transport::Inproc => {
                let r = cp
                    .start_run(StartRunRequest {
                        workflow: GUEST.into(),
                        input_json: serde_json::to_vec(&input).expect("json"),
                        retention_ms,
                        ..Default::default()
                    })
                    .await
                    .expect("start");
                loop {
                    let a = cp
                        .await_result(AwaitResultRequest {
                            run_id: r.run_id.clone(),
                            wait_ms: 60_000,
                            follow_continuations: false,
                        })
                        .await
                        .expect("await result");
                    if a.completed {
                        let n = a.result.as_ref().and_then(serde_json::Value::as_u64);
                        return (r.run_id.parse().expect("id"), n);
                    }
                }
            }
            Transport::Http => {
                let mut body = json!({ "input": input });
                if let Some(ms) = retention_ms {
                    body["retention"] = json!(format!("{ms}ms"));
                }
                let resp = http
                    .post(format!("{base}/v1/workflows/{GUEST}/runs"))
                    .json(&body)
                    .send()
                    .await
                    .expect("POST run");
                assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
                let v: serde_json::Value = resp.json().await.expect("json");
                let id = v["run_id"].as_str().expect("run_id").to_owned();
                loop {
                    let v: serde_json::Value = http
                        .get(format!("{base}/v1/runs/{id}/result?wait=30s"))
                        .send()
                        .await
                        .expect("GET result")
                        .json()
                        .await
                        .expect("json");
                    if v["completed"].as_bool() == Some(true) {
                        return (id.parse().expect("id"), v["result"].as_u64());
                    }
                }
            }
        }
    }
}

/// D16: restart on the same data dir and count `step_completed` events of `runs` through
/// the history API. Returns `(verified steps, runs whose count differs from `want`)`.
async fn verify_on_disk(b: &mut Bench, runs: &[RunId], want: u64) -> (u64, usize) {
    let t = Instant::now();
    b.s.restart().await;
    let booted = t.elapsed();
    let ready = b.s.wait_ready(Duration::from_secs(600)).await;
    println!("verify_on_disk: restart {booted:?}, then ready after {ready:?}");
    let cp = b.cp();
    let mut total = 0u64;
    let mut bad = 0usize;
    for r in runs {
        let mut from = 0;
        let mut n = 0u64;
        loop {
            let page = cp
                .run_history(HistoryRequest {
                    run_id: r.to_string(),
                    from_seq: from,
                    limit: 1_000,
                })
                .await
                .expect("history");
            n += page
                .events
                .iter()
                .filter(|e| e.kind == "step_completed")
                .count() as u64;
            match page.next_from_seq {
                Some(next) => from = next,
                None => break,
            }
        }
        if n != want {
            bad += 1;
        }
        total += n;
    }
    (total, bad)
}

// ------------------------------------------------------------------- profiles A / B

struct Done {
    run: RunId,
    submit_ns: u64,
    steps: Option<u64>,
}

async fn load(cfg: Cfg) {
    let b = boot(&cfg, |c| {
        c.executor.slots = 64.max(u32::try_from(cfg.concurrency).unwrap_or(u32::MAX));
    })
    .await;
    let http = reqwest::Client::new();
    let base = b.s.url("");
    let cp = b.cp();
    let input = json!({ "n": cfg.steps });
    // Warm-up: compile, instantiate caches, allocator pools, HTTP connections.
    for _ in 0..cfg.concurrency.min(16) {
        let _ = b.one(&cp, &http, &base, input.clone(), None).await;
    }

    let dev = block_device(&b.data);
    let flush0 = device_flushes(dev.as_deref());
    let cpu0 = cpu_secs();
    let next = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(Mutex::new(Vec::with_capacity(cfg.runs)));
    let bench = Arc::new(b);
    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..cfg.concurrency {
        let (bench, next, done, http, base, cp, input) = (
            Arc::clone(&bench),
            Arc::clone(&next),
            Arc::clone(&done),
            http.clone(),
            base.clone(),
            Arc::clone(&cp),
            input.clone(),
        );
        tasks.push(tokio::spawn(async move {
            while next.fetch_add(1, Ordering::Relaxed) < bench.cfg.runs {
                let submit_ns = wall_ns();
                let (run, steps) = bench.one(&cp, &http, &base, input.clone(), None).await;
                done.lock().await.push(Done {
                    run,
                    submit_ns,
                    steps,
                });
            }
        }));
    }
    for t in tasks {
        tokio::time::timeout(LIMIT, t)
            .await
            .expect("bench load timed out")
            .expect("load task");
    }
    let elapsed = t0.elapsed();
    let cpu = cpu_secs() - cpu0;
    let flush1 = device_flushes(dev.as_deref());
    let mut b = Arc::try_unwrap(bench).unwrap_or_else(|_| panic!("bench still shared"));
    let done = std::mem::take(&mut *done.lock().await);

    // Acknowledged: completed through the API with the guest's own step count.
    let acked_runs = done
        .iter()
        .filter(|d| d.steps == Some(u64::from(cfg.steps)))
        .count();
    let acked_steps = acked_runs as u64 * u64::from(cfg.steps);
    assert_eq!(acked_runs, cfg.runs, "every run completes with n steps");

    // Start -> first step: submit wall time to the first StepCompleted's journal ts.
    let mut lat = Vec::with_capacity(done.len());
    for d in &done {
        let j = b.s.journal(&d.run).await;
        if let Some(e) = j.iter().find(|e| e.body.kind() == EventKind::StepCompleted) {
            lat.push(Duration::from_nanos(e.ts_nanos.saturating_sub(d.submit_ns)));
        }
    }
    lat.sort();

    let runs: Vec<RunId> = done.iter().map(|d| d.run).collect();
    let fsync = fsync_cost(&b.data, 200);
    // The control plane holds the old server's meta and partition handles: release it
    // before the restart, or the new server cannot take over the data dir.
    drop(cp);
    let (verified, bad) = verify_on_disk(&mut b, &runs, u64::from(cfg.steps)).await;
    let disk = bytes_on_disk(&b.data);
    assert_eq!(bad, 0, "{bad} runs with a step count that differs on disk");
    assert_eq!(
        verified, acked_steps,
        "acknowledged steps must all be on disk after the restart"
    );

    let secs = elapsed.as_secs_f64();
    let flushes = flush0.zip(flush1).map(|(a, b)| b.saturating_sub(a));
    let report = json!({
        "profile": format!("{:?}", cfg.profile).to_lowercase(),
        "transport": format!("{:?}", cfg.transport).to_lowercase(),
        "fs": fs_type(&b.data),
        "kernel": sh("uname", &["-r"]),
        "cpus": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
        "partitions": b.s.cfg.node.partitions,
        "concurrency": cfg.concurrency,
        "runs_acked": acked_runs,
        "steps_acked": acked_steps,
        "steps_verified": verified,
        "elapsed_s": secs,
        "steps_per_s": verified as f64 / secs,
        "runs_per_s": acked_runs as f64 / secs,
        "start_first_step_p50_ms": ms(pct(&lat, 0.50)),
        "start_first_step_p99_ms": ms(pct(&lat, 0.99)),
        "cpu_s": cpu,
        "cpu_util": cpu / secs,
        "device_flushes_per_s": flushes.map(|f| f as f64 / secs),
        "fsync_p50_ms": ms(fsync),
        "bytes_on_disk": disk,
    });
    println!("PERF_JSON {report}");
    println!(
        "\n| metric | value |\n|---|---|\n\
         | profile / transport | {} / {} |\n\
         | env | fs {}, kernel {}, {} cpus, P={} |\n\
         | runs acked / steps verified | {} / {} |\n\
         | steps/s | {:.0} |\n| runs/s | {:.0} |\n\
         | start -> first step p50 / p99 | {:.2} ms / {:.2} ms |\n\
         | CPU | {:.2} s ({:.2} cores) |\n\
         | device flushes/s | {} |\n| raw fsync p50 | {:.2} ms |\n\
         | bytes on disk | {} |\n",
        report["profile"],
        report["transport"],
        report["fs"],
        report["kernel"],
        report["cpus"],
        report["partitions"],
        acked_runs,
        verified,
        verified as f64 / secs,
        acked_runs as f64 / secs,
        ms(pct(&lat, 0.50)),
        ms(pct(&lat, 0.99)),
        cpu,
        cpu / secs,
        flushes.map_or("n/a".to_string(), |f| format!("{:.0}", f as f64 / secs)),
        ms(fsync),
        disk,
    );
    b.s.shutdown().await;
}

// ------------------------------------------------------------------- expiry cost (OPS-06)

async fn expiry(cfg: Cfg) {
    let b = boot(&cfg, |c| {
        c.engine.continue_as_new_events = cfg.expiry_events * 2;
        c.engine.activation_timeout = Duration::from_secs(600).into();
    })
    .await;
    let http = reqwest::Client::new();
    let base = b.s.url("");
    let cp = b.cp();
    // RunStarted + n StepCompleted + RunCompleted.
    let n = cfg.expiry_events.saturating_sub(2);
    let t_run = Instant::now();
    let (run, steps) = b
        .one(&cp, &http, &base, json!({ "n": n }), Some(1_000))
        .await;
    let run_secs = t_run.elapsed().as_secs_f64();
    assert_eq!(steps, Some(n), "the expiry run completes");
    let events = b.s.journal(&run).await.len();
    let rec = b.s.run_record(&run);
    assert_eq!(rec.status, RunStatus::Completed);
    let due_ms = rec.completed_at_ms.unwrap_or(0) + rec.retention_ms;

    // Probe: a SignalRun to a run id that does not exist in the same partition (rejected
    // RunNotFound after one point read), once per ms, until the record is gone.
    let h = b.s.handle_of(&run);
    let ghost = RunId::new(run.partition(), RunId::new_v7(run.partition()).uuid());
    let mut lat: Vec<(u64, Duration)> = Vec::new();
    let t0 = Instant::now();
    let mut gone_at = None;
    while t0.elapsed() < Duration::from_secs(30) {
        let cmd = Command::signal_run(SignalRun {
            run_id: ghost.as_bytes().to_vec(),
            name: "probe".into(),
            ..Default::default()
        });
        let at = wall_ns() / 1_000_000;
        let t = Instant::now();
        let _ = h.submit_one(cmd).await.expect("probe submit");
        lat.push((at, t.elapsed()));
        if h.read_run(&run).expect("read").is_none() {
            gone_at = Some(wall_ns() / 1_000_000);
            // A few more probes after the delete.
            for _ in 0..50 {
                let t = Instant::now();
                let _ = h
                    .submit_one(Command::signal_run(SignalRun {
                        run_id: ghost.as_bytes().to_vec(),
                        name: "probe".into(),
                        ..Default::default()
                    }))
                    .await
                    .expect("probe");
                lat.push((wall_ns() / 1_000_000, t.elapsed()));
            }
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let gone_at = gone_at.expect("run expired within 30 s");
    let mut base_lat: Vec<Duration> = lat
        .iter()
        .filter(|(at, _)| *at + 200 < due_ms)
        .map(|(_, d)| *d)
        .collect();
    base_lat.sort();
    let worst = lat.iter().map(|(_, d)| *d).max().unwrap_or_default();
    let report = json!({
        "profile": "expiry",
        "fs": fs_type(&b.data),
        "kernel": sh("uname", &["-r"]),
        "journal_events": events,
        "run_s": run_secs,
        "probe_p50_ms": ms(pct(&base_lat, 0.5)),
        "apply_blocked_max_ms": ms(worst),
        "expired_after_due_ms": gone_at.saturating_sub(due_ms),
        "over_1s": worst > Duration::from_secs(1),
    });
    println!("PERF_JSON {report}");
    println!(
        "\n| metric | value |\n|---|---|\n\
         | journal events | {events} |\n\
         | probe latency p50 before expiry | {:.2} ms |\n\
         | worst probe latency (apply blocked by EXPIRE_OP) | {:.1} ms |\n\
         | record gone after due | {} ms |\n",
        ms(pct(&base_lat, 0.5)),
        ms(worst),
        gone_at.saturating_sub(due_ms),
    );
    let mut b = b;
    b.s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run by hand in release (docs/benchmarks.md)"]
async fn perf() {
    let cfg = Cfg::from_env();
    println!("bench config: {cfg:?}");
    match cfg.profile {
        Profile::A | Profile::B => load(cfg).await,
        Profile::Expiry => expiry(cfg).await,
    }
}
