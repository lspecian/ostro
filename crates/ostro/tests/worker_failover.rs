//! Phase 7 success criterion 2 through the binaries (plan 07-18; WORK-03/04, WORK-01
//! failover): a real `ostro server` (self-signed TLS) and real `ostro worker` processes
//! (the 07-16 `P7Cluster` harness).
//!
//! - `auto_spill`: an `auto` queue with 2 embedded slots and a 40-run spike enters
//!   Spilling, overflows to the worker, and returns to Embedded after the cool-down:
//!   exactly 2 transitions.
//! - `oscillation`: a CPU load toggling around the entry threshold faster than `sustain`
//!   for 60 s causes 0 transitions.
//! - `sustained_overload` (B6): a constant overload held for 3 × cool-down causes exactly
//!   1 transition (enter) with embedded slots filled first; after the drain exactly one
//!   more (exit).
//! - `selectors`: a `gpu=true` queue runs only on the labelled worker; without it the
//!   backlog shows up as demand and nothing runs anywhere else.
//!
//! # Signals and metric sources
//!
//! - Transition counts come from `GET /metrics`
//!   (`ostro_placement_transitions_total{queue,to}`, `ostro_placement_spilling{queue}`)
//!   and are cross-checked against the server's `placement spill transition` log lines.
//! - Where a run executed comes from the leader's info line
//!   `remote activate run=<id> attempt=<n> worker=<id>` (07-12): a run without such a
//!   line ran embedded.
//! - Embedded occupancy is sampled as `running` runs of the queue (`GET /v1/runs`) minus
//!   `running_on_workers` (`GET /v1/queues/{q}/demand`); there is no embedded-running
//!   gauge, and `cpu-burn`'s `started_ms` is the logical (journaled) time, not the start
//!   of execution.
//! - Spill triggers are deterministic (07-13 hand-off): the slots signal with small
//!   embedded slots (the CPU, memory and lag triggers disabled), except `oscillation`,
//!   which needs the only sustained signal (CPU) and calibrates its threshold from the
//!   measured server CPU (`/proc/<pid>/stat`).
//!
//! The spawned server reads its own system clock: these tests drive real time (the
//! `ManualClock` seam is exercised by `ostro-server/tests/placement_spill.rs`).
#![cfg(unix)]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p7.rs"]
mod p7;

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use p7::{P7_T, P7Api, P7Cluster, P7Opts, P7Proc, WorkerOpts};
use serde_json::{Value, json};

/// Spawned servers are heavy: one cluster at a time per test process.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ---------------------------------------------------------------------------------
// Log and metric helpers
// ---------------------------------------------------------------------------------

/// Remove ANSI escape sequences (`ESC [ ... letter`).
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut it = s.chars().peekable();
    while let Some(c) = it.next() {
        if c == '\u{1b}' {
            if it.peek() == Some(&'[') {
                it.next();
                for d in it.by_ref() {
                    if d.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}

/// All lines of a process (stdout, then stderr), ANSI stripped. Order is preserved
/// within each stream; tracing writes one stream.
fn lines(p: &P7Proc) -> Vec<String> {
    p.stdout()
        .iter()
        .chain(p.stderr().iter())
        .map(|l| strip_ansi(l))
        .collect()
}

/// `key=value` pairs after `marker` on a line.
fn kv_after(line: &str, marker: &str) -> Option<BTreeMap<String, String>> {
    let (_, rest) = line.split_once(marker)?;
    let full = format!("{}{rest}", marker.rsplit(' ').next().unwrap_or(""));
    Some(
        full.split_whitespace()
            .filter_map(|kv| kv.split_once('='))
            .map(|(k, v)| (k.to_owned(), v.trim_end_matches(',').to_owned()))
            .collect(),
    )
}

/// One `remote activate run=<id> attempt=<n> worker=<id>` line.
#[derive(Clone, Debug)]
struct Act {
    /// Line index (used by `sigstop_zombie`, failpoints builds only).
    #[cfg_attr(not(feature = "failpoints"), allow(dead_code))]
    idx: usize,
    run: String,
    #[allow(dead_code)]
    attempt: u32,
    worker: String,
}

fn activations(lines: &[String]) -> Vec<Act> {
    lines
        .iter()
        .enumerate()
        .filter_map(|(idx, l)| {
            let kv = kv_after(l, "remote activate run=")?;
            Some(Act {
                idx,
                run: kv.get("run")?.clone(),
                attempt: kv.get("attempt")?.parse().ok()?,
                worker: kv.get("worker")?.clone(),
            })
        })
        .collect()
}

/// `(line index, run, attempt)` of every `applied events run=<id> attempt=<n>` line.
#[cfg_attr(not(feature = "failpoints"), allow(dead_code))]
fn applied(lines: &[String]) -> Vec<(usize, String, u32)> {
    lines
        .iter()
        .enumerate()
        .filter_map(|(idx, l)| {
            let kv = kv_after(l, "applied events run=")?;
            Some((
                idx,
                kv.get("run")?.clone(),
                kv.get("attempt")?.parse().ok()?,
            ))
        })
        .collect()
}

/// Per-worker activation counts restricted to `runs`.
fn per_worker(acts: &[Act], runs: &BTreeSet<String>) -> BTreeMap<String, usize> {
    let mut m = BTreeMap::new();
    for a in acts.iter().filter(|a| runs.contains(&a.run)) {
        *m.entry(a.worker.clone()).or_insert(0) += 1;
    }
    m
}

/// `GET /metrics` text.
async fn metrics_text(api: &P7Api) -> String {
    let r = api
        .http
        .get(api.url("/metrics"))
        .send()
        .await
        .expect("GET /metrics");
    assert!(r.status().is_success(), "/metrics: {}", r.status());
    r.text().await.expect("metrics body")
}

/// Sum of the samples of `name` whose labels include every `want` pair.
fn metric_sum(text: &str, name: &str, want: &[(&str, &str)]) -> f64 {
    let mut sum = 0.0;
    for l in text.lines() {
        if l.starts_with('#') {
            continue;
        }
        let (series, value) = match l.rsplit_once(' ') {
            Some(x) => x,
            None => continue,
        };
        let (n, labels) = match series.split_once('{') {
            Some((n, rest)) => (n, rest.trim_end_matches('}')),
            None => (series, ""),
        };
        if n != name {
            continue;
        }
        let have: BTreeMap<&str, &str> = labels
            .split(',')
            .filter_map(|kv| kv.split_once('='))
            .map(|(k, v)| (k.trim(), v.trim_matches('"')))
            .collect();
        if want.iter().all(|(k, v)| have.get(k) == Some(v)) {
            sum += value.parse::<f64>().unwrap_or(0.0);
        }
    }
    sum
}

/// Spill transitions of `queue` from `/metrics`: (to spilling, to embedded, spilling gauge).
async fn transitions(api: &P7Api, queue: &str) -> (u64, u64, u64) {
    let t = metrics_text(api).await;
    let n = "ostro_placement_transitions_total";
    (
        metric_sum(&t, n, &[("queue", queue), ("to", "spilling")]) as u64,
        metric_sum(&t, n, &[("queue", queue), ("to", "embedded")]) as u64,
        metric_sum(&t, "ostro_placement_spilling", &[("queue", queue)]) as u64,
    )
}

/// Transition log lines of `queue` (`placement spill transition ... queue=<q> to=<to>`).
fn transition_lines(p: &P7Proc, queue: &str) -> Vec<String> {
    lines(p)
        .into_iter()
        .filter(|l| l.contains("placement spill transition") && l.contains(queue))
        .collect()
}

/// Server env: info logs (remote activations, spill transitions).
fn info_env() -> Vec<(String, String)> {
    vec![("RUST_LOG".into(), "info".into())]
}

/// `[workers.spill]`-shaped per-queue policy: the slots trigger only.
fn slots_spill(slots_pct: u8, cooldown_ms: u64) -> Value {
    json!({
        "cpu_pct": 100,
        "slots_pct": slots_pct,
        "mem_pct": 100,
        "lag_ms": 3_600_000u64,
        "sustain_ms": 0,
        "cooldown_ms": cooldown_ms,
        "hysteresis_pct": 50
    })
}

/// Runs of `queue` in status `running` (`GET /v1/runs?queue=..&status=running`).
async fn running_runs(api: &P7Api, queue: &str) -> u64 {
    let (st, v) = api
        .get(&format!("/v1/runs?queue={queue}&status=running&limit=1000"))
        .await;
    assert!(st.is_success(), "list runs: {v}");
    v["runs"].as_array().map_or(0, |a| a.len() as u64)
}

/// Poll until `f` holds (or panic with `what` after `limit`).
async fn until<F, Fut>(limit: Duration, what: &str, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + limit;
    while !f().await {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ---------------------------------------------------------------------------------
// auto_spill
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_spill() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let c = P7Cluster::start(P7Opts {
        embedded_slots: Some(2),
        env: info_env(),
        ..P7Opts::default()
    })
    .await;
    let w = c.spawn_worker(WorkerOpts::new("spill-w1", 4));
    c.wait_workers(1).await;
    c.api.deploy("cpu-burn").await;
    let cooldown = Duration::from_secs(5);
    c.api
        .set_flow(
            "burst",
            json!({"placement": "auto", "spill": slots_spill(50, cooldown.as_millis() as u64)}),
        )
        .await;
    assert_eq!(transitions(&c.api, "burst").await, (0, 0, 0));

    let mut ids = Vec::new();
    for _ in 0..40 {
        ids.push(
            c.api
                .start("cpu-burn", json!({"input": {"ms": 300}, "queue": "burst"}))
                .await,
        );
    }
    // Enters Spilling on the first sampler tick that sees both embedded slots taken.
    until(P7_T, "burst spilling", || async {
        transitions(&c.api, "burst").await.0 == 1
    })
    .await;
    let entered = t0.elapsed();
    for id in &ids {
        let r = c.api.wait_terminal(id, P7_T).await;
        assert_eq!(r["status"], "completed", "{r}");
    }
    let drained = Instant::now();
    // Back to Embedded after the cool-down.
    until(
        Duration::from_secs(60),
        "burst back to embedded",
        || async { transitions(&c.api, "burst").await.1 == 1 },
    )
    .await;
    let exit_after_drain = drained.elapsed();
    // No further transition once calm.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let (to_s, to_e, gauge) = transitions(&c.api, "burst").await;
    assert_eq!((to_s, to_e), (1, 1), "exactly 2 transitions");
    assert_eq!(gauge, 0, "queue back to embedded");
    let tl = transition_lines(c.server(), "burst");
    assert_eq!(tl.len(), 2, "transition log lines: {tl:#?}");

    let acts = activations(&lines(c.server()));
    let set: BTreeSet<String> = ids.iter().cloned().collect();
    let on_workers: BTreeSet<&str> = acts
        .iter()
        .filter(|a| set.contains(&a.run))
        .map(|a| a.run.as_str())
        .collect();
    assert!(!on_workers.is_empty(), "no run overflowed to the worker");
    assert!(on_workers.len() < ids.len(), "nothing ran embedded");
    assert!(
        acts.iter().all(|a| a.worker == "spill-w1"),
        "unknown worker: {acts:?}"
    );
    assert!(exit_after_drain >= cooldown - Duration::from_millis(1500));
    eprintln!(
        "worker_failover::auto_spill: entered at {entered:?}, worker runs {}/{}, \
         exit {exit_after_drain:?} after the drain (cooldown {cooldown:?}), transitions \
         spilling={to_s} embedded={to_e} (metric /metrics; {} log lines), total {:?}",
        on_workers.len(),
        ids.len(),
        tl.len(),
        t0.elapsed()
    );
    drop(w);
}

// ---------------------------------------------------------------------------------
// oscillation
// ---------------------------------------------------------------------------------

/// utime + stime of `pid` in seconds (`/proc/<pid>/stat`, USER_HZ = 100).
fn proc_cpu_secs(pid: u32) -> Option<f64> {
    let s = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let rest = s.rsplit_once(')')?.1;
    let f: Vec<&str> = rest.split_whitespace().collect();
    // After ") ": state is field 3 (index 0); utime is field 14, stime field 15.
    let ut: f64 = f.get(11)?.parse().ok()?;
    let st: f64 = f.get(12)?.parse().ok()?;
    Some((ut + st) / 100.0)
}

/// One on-phase: `n` concurrent `cpu-burn {ms}` runs; returns (wall, normalised server CPU).
async fn burn_phase(c: &P7Cluster, n: usize, ms: u64, par: f64) -> (Duration, Option<f64>) {
    let pid = c.server().pid();
    let cpu0 = proc_cpu_secs(pid);
    let t = Instant::now();
    let mut ids = Vec::new();
    for _ in 0..n {
        ids.push(
            c.api
                .start("cpu-burn", json!({"input": {"ms": ms}, "queue": "burst"}))
                .await,
        );
    }
    for id in &ids {
        let r = c.api.wait_terminal(id, P7_T).await;
        assert_eq!(r["status"], "completed", "{r}");
    }
    let wall = t.elapsed();
    let cpu = match (cpu0, proc_cpu_secs(pid)) {
        (Some(a), Some(b)) => Some((b - a) / (wall.as_secs_f64() * par)),
        _ => None,
    };
    (wall, cpu)
}

/// Nominal `cpu-burn` length of one run of an on-phase:
/// long enough that sampler windows inside it read above the line, short enough (with
/// the off-phase) to stay under `sustain`, and within the activation fuel budget.
const ON_MS: u64 = 800;
/// Runs per on-phase (4 waves on 4 embedded slots: about 1 s of load here).
const ON_RUNS: usize = 16;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oscillation() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let c = P7Cluster::start(P7Opts {
        embedded_slots: Some(4),
        env: info_env(),
        ..P7Opts::default()
    })
    .await;
    let w = c.spawn_worker(WorkerOpts::new("osc-w1", 2));
    c.wait_workers(1).await;
    c.api.deploy("cpu-burn").await;
    let par = std::thread::available_parallelism().map_or(1, |n| n.get()) as f64;
    // Calibrate with every trigger disabled.
    c.api
        .set_flow(
            "burst",
            json!({"placement": "auto", "spill": slots_spill(100, 5_000)}),
        )
        .await;
    let (cal_wall, cal_cpu) = burn_phase(&c, ON_RUNS, ON_MS, par).await;
    let cal = cal_cpu.unwrap_or(4.0 / par);
    // Entry threshold at about half the measured on-phase level: every on-phase crosses
    // it, every off-phase is far below it. Sustain is longer than any on-phase.
    let cpu_pct = ((cal * 100.0) / 2.0).floor().clamp(1.0, 99.0) as u8;
    let sustain_ms = 5_000u64;
    c.api
        .set_flow(
            "burst",
            json!({"placement": "auto", "spill": {
                "cpu_pct": cpu_pct, "slots_pct": 100, "mem_pct": 100, "lag_ms": 3_600_000u64,
                "sustain_ms": sustain_ms, "cooldown_ms": 5_000, "hysteresis_pct": 85
            }}),
        )
        .await;
    let base = transitions(&c.api, "burst").await;
    assert_eq!(base, (0, 0, 0), "calibration caused a transition");

    let mut on_levels = Vec::new();
    let mut max_on = Duration::ZERO;
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(60) {
        let (wall, cpu) = burn_phase(&c, ON_RUNS, ON_MS, par).await;
        max_on = max_on.max(wall);
        if let Some(x) = cpu {
            on_levels.push(x);
        }
        assert_eq!(
            transitions(&c.api, "burst").await,
            (0, 0, 0),
            "oscillating load caused a transition"
        );
        // Off-phase: at least two full sampler periods below the line.
        tokio::time::sleep(Duration::from_millis(2_500)).await;
    }
    let (to_s, to_e, gauge) = transitions(&c.api, "burst").await;
    assert_eq!((to_s, to_e, gauge), (0, 0, 0), "0 transitions");
    assert!(transition_lines(c.server(), "burst").is_empty());
    let above = on_levels
        .iter()
        .filter(|&&x| x * 100.0 > f64::from(cpu_pct))
        .count();
    assert!(
        on_levels.is_empty() || above > 0,
        "no on-phase crossed the entry line ({cpu_pct}%): {on_levels:?}"
    );
    assert!(
        max_on < Duration::from_millis(sustain_ms),
        "an on-phase ({max_on:?}) outlasted sustain: the toggle was not faster than sustain"
    );
    // Positive control: the same load held continuously past `sustain` does enter.
    let held = Instant::now();
    while transitions(&c.api, "burst").await.0 == 0 {
        assert!(
            held.elapsed() < Duration::from_secs(40),
            "continuous load above the line never entered Spilling (threshold meaningless)"
        );
        burn_phase(&c, ON_RUNS, ON_MS, par).await;
    }
    let entered_after = held.elapsed();
    assert!(
        entered_after >= Duration::from_millis(sustain_ms) - Duration::from_millis(1_500),
        "entered after {entered_after:?}, before sustain"
    );
    assert_eq!(transitions(&c.api, "burst").await.0, 1);
    eprintln!(
        "worker_failover::oscillation: parallelism {par}, calibration {cal_wall:?} at {:.1}% \
         -> cpu_pct {cpu_pct}, sustain {sustain_ms} ms; {} on-phases (max {max_on:?}), {} \
         above the line, levels {:?}; transitions 0 (/metrics); control: continuous load \
         entered after {entered_after:?}; total {:?}",
        cal * 100.0,
        on_levels.len(),
        above,
        on_levels
            .iter()
            .map(|x| format!("{:.1}", x * 100.0))
            .collect::<Vec<_>>(),
        t0.elapsed()
    );
    drop(w);
}

// ---------------------------------------------------------------------------------
// sustained_overload
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sustained_overload() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let c = P7Cluster::start(P7Opts {
        embedded_slots: Some(2),
        env: info_env(),
        ..P7Opts::default()
    })
    .await;
    let w = c.spawn_worker(WorkerOpts::new("over-w1", 2));
    c.wait_workers(1).await;
    c.api.deploy("cpu-burn").await;
    let cooldown = Duration::from_secs(5);
    c.api
        .set_flow(
            "burst",
            json!({"placement": "auto", "spill": slots_spill(50, cooldown.as_millis() as u64)}),
        )
        .await;

    // Constant overload: always at least 8 runs in flight for 4 slots (embedded 2 +
    // worker 2), so the backlog never empties.
    let mut ids: Vec<String> = Vec::new();
    let mut open: BTreeSet<String> = BTreeSet::new();
    let mut entered_at: Option<Instant> = None;
    let mut samples = 0usize;
    // (embedded running, running on workers) while Spilling.
    let mut occ: Vec<(u64, u64)> = Vec::new();
    loop {
        let done: Vec<String> = {
            let mut d = Vec::new();
            for id in &open {
                if let Some(r) = c.api.run(id).await
                    && r["status"].as_str().is_some_and(common::is_terminal)
                {
                    d.push(id.clone());
                }
            }
            d
        };
        for id in done {
            open.remove(&id);
        }
        while open.len() < 8 {
            let id = c
                .api
                .start("cpu-burn", json!({"input": {"ms": 500}, "queue": "burst"}))
                .await;
            open.insert(id.clone());
            ids.push(id);
        }
        let (to_s, to_e, gauge) = transitions(&c.api, "burst").await;
        if to_s == 1 {
            let running = running_runs(&c.api, "burst").await;
            let on_w = c.api.demand("burst").await["running_on_workers"]
                .as_u64()
                .unwrap_or(0);
            occ.push((running.saturating_sub(on_w), on_w));
        }
        samples += 1;
        assert_eq!(to_e, 0, "left Spilling under sustained overload");
        assert!(to_s <= 1, "re-entered Spilling: {to_s}");
        if to_s == 1 {
            assert_eq!(gauge, 1, "spilling gauge");
            entered_at.get_or_insert_with(Instant::now);
        }
        if let Some(e) = entered_at
            && e.elapsed() >= cooldown * 3
        {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(180),
            "never entered Spilling"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let held = entered_at.map(|e| e.elapsed()).unwrap_or_default();
    let (to_s, to_e, gauge) = transitions(&c.api, "burst").await;
    assert_eq!(
        (to_s, to_e, gauge),
        (1, 0, 1),
        "exactly 1 transition, still Spilling"
    );

    // Load stops; the worker drains; exactly one more transition.
    for id in &ids {
        let r = c.api.wait_terminal(id, P7_T).await;
        assert_eq!(r["status"], "completed", "{r}");
    }
    let drained = Instant::now();
    until(Duration::from_secs(60), "exit after the drain", || async {
        transitions(&c.api, "burst").await.1 == 1
    })
    .await;
    let exit_after = drained.elapsed();
    tokio::time::sleep(Duration::from_secs(4)).await;
    let (to_s, to_e, gauge) = transitions(&c.api, "burst").await;
    assert_eq!(
        (to_s, to_e, gauge),
        (1, 1, 0),
        "exactly one more transition (exit)"
    );
    assert_eq!(transition_lines(c.server(), "burst").len(), 2);

    // Embedded first: the embedded slots stay full while overflow runs on the worker.
    let acts = activations(&lines(c.server()));
    let set: BTreeSet<String> = ids.iter().cloned().collect();
    let remote: BTreeSet<String> = acts
        .iter()
        .filter(|a| set.contains(&a.run))
        .map(|a| a.run.clone())
        .collect();
    assert!(!remote.is_empty(), "nothing overflowed to the worker");
    assert!(remote.len() < ids.len(), "nothing ran embedded");
    assert!(acts.iter().all(|a| a.worker == "over-w1"));
    let emb_max = occ.iter().map(|o| o.0).max().unwrap_or(0);
    assert!(emb_max <= 2, "more embedded runs than slots: {occ:?}");
    let busy: Vec<&(u64, u64)> = occ.iter().filter(|o| o.1 > 0).collect();
    assert!(
        !busy.is_empty(),
        "no sample saw work on the worker: {occ:?}"
    );
    let full = busy.iter().filter(|o| o.0 == 2).count();
    let frac = full as f64 / busy.len() as f64;
    assert!(
        frac >= 0.8,
        "embedded slots not full while the worker ran overflow: {full}/{} {occ:?}",
        busy.len()
    );
    eprintln!(
        "worker_failover::sustained_overload: {} runs over {samples} samples, held \
         Spilling {held:?} (3 x cooldown {cooldown:?}) with transitions 1, exit \
         {exit_after:?} after the drain -> 2; worker runs {}, embedded runs {}; embedded \
         running == 2 in {full}/{} samples with worker work (max {emb_max}); total {:?}",
        ids.len(),
        remote.len(),
        ids.len() - remote.len(),
        busy.len(),
        t0.elapsed()
    );
    drop(w);
}

// ---------------------------------------------------------------------------------
// selectors
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selectors() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let c = P7Cluster::start(P7Opts {
        env: info_env(),
        ..P7Opts::default()
    })
    .await;
    let mut gpu = c.spawn_worker(WorkerOpts::new("gpu-t", 2).label("gpu", "true"));
    let plain = c.spawn_worker(WorkerOpts::new("gpu-f", 4).label("gpu", "false"));
    c.wait_workers(2).await;
    c.api.deploy("sdk-basic").await;
    let flow = c
        .api
        .set_flow(
            "gpu",
            json!({"placement": "workers", "selector": ["gpu=true"]}),
        )
        .await;
    assert_eq!(flow["selector"], json!(["gpu=true"]), "{flow}");

    let start = |i: usize| {
        let api = c.api.clone();
        async move {
            api.start(
                "sdk-basic",
                json!({"input": {"name": format!("g{i}"), "n": 2}, "queue": "gpu"}),
            )
            .await
        }
    };
    let mut first = Vec::new();
    for i in 0..10 {
        first.push(start(i).await);
    }
    for id in &first {
        let r = c.api.wait_terminal(id, P7_T).await;
        assert_eq!(r["status"], "completed", "{r}");
    }
    let set: BTreeSet<String> = first.iter().cloned().collect();
    let counts = per_worker(&activations(&lines(c.server())), &set);
    assert_eq!(
        counts.keys().cloned().collect::<Vec<_>>(),
        vec!["gpu-t".to_owned()],
        "gpu queue ran off the labelled worker: {counts:?}"
    );
    let ran: BTreeSet<String> = activations(&lines(c.server()))
        .into_iter()
        .filter(|a| set.contains(&a.run))
        .map(|a| a.run)
        .collect();
    assert_eq!(ran, set, "every run executed on gpu-t (none embedded)");

    // Without a matching worker the work waits as demand.
    gpu.sigterm();
    let st = gpu.wait_exit(P7_T);
    assert!(st.success(), "gpu-t drain exit {st:?}:\n{}", gpu.output());
    c.wait_workers(1).await;
    let mut held = Vec::new();
    for i in 10..13 {
        held.push(start(i).await);
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    let d = loop {
        let d = c.api.demand("gpu").await;
        if d["backlog"].as_u64().unwrap_or(0) >= 3 {
            break d;
        }
        assert!(Instant::now() < deadline, "backlog not in demand: {d}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(d["placement"], "workers", "{d}");
    assert!(d["demand"].as_u64().unwrap_or(0) >= 3, "{d}");
    assert_eq!(d["workers"], 0, "no matching worker: {d}");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let hset: BTreeSet<String> = held.iter().cloned().collect();
    for id in &held {
        let r = c.api.run(id).await.expect("run");
        assert!(
            !r["status"].as_str().is_some_and(common::is_terminal),
            "run {id} ran without a matching worker: {r}"
        );
        let h = c.api.history(id).await;
        // Only the acceptance event (`run_started`, seq 0): no activation journaled.
        assert_eq!(
            h.iter()
                .map(|e| e["kind"].as_str().unwrap_or(""))
                .collect::<Vec<_>>(),
            vec!["run_started"],
            "run {id} executed without a matching worker: {h:?}"
        );
    }
    assert!(
        per_worker(&activations(&lines(c.server())), &hset).is_empty(),
        "held runs were activated remotely"
    );

    // A matching worker comes back: the backlog completes on it.
    let gpu2 = c.spawn_worker(WorkerOpts::new("gpu-t2", 2).label("gpu", "true"));
    for id in &held {
        let r = c.api.wait_terminal(id, P7_T).await;
        assert_eq!(r["status"], "completed", "{r}");
    }
    let counts2 = per_worker(&activations(&lines(c.server())), &hset);
    assert_eq!(
        counts2.keys().cloned().collect::<Vec<_>>(),
        vec!["gpu-t2".to_owned()],
        "{counts2:?}"
    );
    assert!(plain.error_lines().is_empty(), "{}", plain.output());
    eprintln!(
        "worker_failover::selectors: 10 runs on gpu-t {counts:?}; 3 held with demand {} \
         backlog {}; completed on gpu-t2 {counts2:?}; total {:?}",
        d["demand"],
        d["backlog"],
        t0.elapsed()
    );
    drop((gpu2, plain));
}
