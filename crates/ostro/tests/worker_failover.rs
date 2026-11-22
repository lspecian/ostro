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
//! - `kill_redispatch`: `kill -9` of a worker holding leases re-dispatches every affected
//!   run on the other worker in well under `lease_ttl`; every run completes once.
//! - `sigstop_zombie` (`--features failpoints`): a SIGSTOPped worker is revoked after
//!   `lease_ttl`; its runs complete elsewhere as attempt n+1 with one charge per key; after
//!   SIGCONT its late appends / StartEffects change nothing.
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

use common::events_of;
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

/// One `(embedded running, running on workers)` sample of `queue`, or `None` when no
/// consistent pair was read.
///
/// The two sources are separate requests, so a worker run that finishes (or starts)
/// between them skews the difference: `running` 4 read before a worker run ended and
/// `running_on_workers` 1 read after it gave a phantom "3 embedded" on CI. The pair is
/// therefore read as worker / running / worker / running and kept only when both reads
/// of each agree (nothing moved in between); up to 5 tries.
async fn occupancy(api: &P7Api, queue: &str) -> Option<(u64, u64)> {
    let on_workers = || async move {
        api.demand(queue).await["running_on_workers"]
            .as_u64()
            .unwrap_or(0)
    };
    for _ in 0..5 {
        let w1 = on_workers().await;
        let r1 = running_runs(api, queue).await;
        let w2 = on_workers().await;
        let r2 = running_runs(api, queue).await;
        if w1 == w2 && r1 == r2 {
            return Some((r1.saturating_sub(w1), w1));
        }
    }
    None
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

fn median(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v.get(v.len() / 2).copied().unwrap_or_default()
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
        if to_s == 1
            && let Some(sample) = occupancy(&c.api, "burst").await
        {
            occ.push(sample);
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

// ---------------------------------------------------------------------------------
// kill_redispatch
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_redispatch() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let lease_ttl = Duration::from_secs(5);
    let c = P7Cluster::start(P7Opts {
        lease_ttl: Some(lease_ttl),
        hot_wait: Some(Duration::from_secs(30)),
        env: info_env(),
        ..P7Opts::default()
    })
    .await;
    // A joins alone and fills its 10 slots; B joins for the rest (and the failover).
    let mut a = c.spawn_worker(WorkerOpts::new("kill-a", 10));
    c.wait_workers(1).await;
    c.api.deploy("timeouts").await;
    c.api
        .set_flow("long", json!({"placement": "workers"}))
        .await;

    let mut ids = Vec::new();
    for _ in 0..20 {
        ids.push(
            c.api
                .start(
                    "timeouts",
                    json!({"input": {"mode": "sleep_ms", "n": 8000}, "queue": "long"}),
                )
                .await,
        );
    }
    let set: BTreeSet<String> = ids.iter().cloned().collect();
    until(P7_T, "kill-a holds 10 runs", || async {
        let on_a: BTreeSet<String> = activations(&lines(c.server()))
            .into_iter()
            .filter(|x| x.worker == "kill-a")
            .map(|x| x.run)
            .collect();
        on_a.len() >= 10
    })
    .await;
    let b = c.spawn_worker(WorkerOpts::new("kill-b", 30));
    c.wait_workers(2).await;
    // Every run activated, some on A (A holds leases).
    let deadline = Instant::now() + P7_T;
    loop {
        let acts = activations(&lines(c.server()));
        let runs: BTreeSet<&str> = acts.iter().map(|x| x.run.as_str()).collect();
        if set.iter().all(|r| runs.contains(r.as_str()))
            && acts.iter().any(|x| x.worker == "kill-a")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "not all 20 activated (some on kill-a): {} runs, {acts:?}\nworkers {:?}\ndemand {}\n{}",
            runs.len(),
            c.api.workers().await,
            c.api.demand("long").await,
            lines(c.server()).join("\n")
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let ws = c.api.workers().await;
    let a_leases = ws
        .iter()
        .find(|w| w["worker_id"] == "kill-a")
        .and_then(|w| w["leases"].as_u64())
        .unwrap_or(0);
    assert!(a_leases > 0, "kill-a holds no lease: {ws:?}");
    // The runs whose latest activation is on A are the affected ones.
    let before = activations(&lines(c.server()));
    let mut last: BTreeMap<String, Act> = BTreeMap::new();
    for x in &before {
        last.insert(x.run.clone(), x.clone());
    }
    let affected: BTreeMap<String, u32> = last
        .values()
        .filter(|x| x.worker == "kill-a")
        .map(|x| (x.run.clone(), x.attempt))
        .collect();
    assert!(!affected.is_empty());

    let t_kill = Instant::now();
    a.kill9();
    // Watch for the re-dispatch of every affected run (10 ms polling).
    let mut seen: BTreeMap<String, (Duration, u32, String)> = BTreeMap::new();
    let deadline = t_kill + P7_T;
    while seen.len() < affected.len() {
        let now = t_kill.elapsed();
        for x in activations(&lines(c.server())) {
            if let Some(&n) = affected.get(&x.run)
                && x.attempt > n
            {
                seen.entry(x.run.clone())
                    .or_insert((now, x.attempt, x.worker.clone()));
            }
        }
        assert!(
            Instant::now() < deadline,
            "not every affected run was re-dispatched: {} of {}",
            seen.len(),
            affected.len()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let lat: Vec<Duration> = seen.values().map(|v| v.0).collect();
    let max = lat.iter().copied().max().unwrap_or_default();
    let med = median(lat.clone());
    for (run, (d, attempt, worker)) in &seen {
        assert_eq!(worker, "kill-b", "{run} re-dispatched to {worker}");
        assert!(*attempt == affected[run] + 1, "{run}: attempt {attempt}");
        assert!(
            *d < lease_ttl,
            "{run} re-dispatched after {d:?} >= lease_ttl"
        );
    }

    // Exactly once.
    for id in &ids {
        let r = c.api.wait_terminal(id, P7_T).await;
        assert_eq!(r["status"], "completed", "{r}");
        let h = c.api.history(id).await;
        assert_eq!(events_of(&h, "run_completed").len(), 1, "{id}");
        let terminal = h
            .iter()
            .filter(|e| {
                matches!(
                    e["kind"].as_str(),
                    Some("run_completed" | "run_failed" | "run_cancelled")
                )
            })
            .count();
        assert_eq!(terminal, 1, "{id}: one terminal event");
        assert_eq!(
            events_of(&h, "run_started").len(),
            1,
            "{id}: one RunStarted"
        );
    }
    eprintln!(
        "worker_failover::kill_redispatch: lease_ttl {lease_ttl:?}, kill-a held {a_leases} \
         leases, {} affected runs re-dispatched to kill-b: max {max:?}, median {med:?}; \
         20/20 completed once; total {:?}",
        affected.len(),
        t0.elapsed()
    );
    drop(b);
}

// ---------------------------------------------------------------------------------
// sigstop_zombie
// ---------------------------------------------------------------------------------

/// SIGCONTs (and kills) a stopped worker however the test ends.
#[cfg(feature = "failpoints")]
struct ContGuard<'a>(&'a P7Proc);

#[cfg(feature = "failpoints")]
impl Drop for ContGuard<'_> {
    fn drop(&mut self) {
        let _ = std::process::Command::new("kill")
            .arg("-CONT")
            .arg(self.0.pid().to_string())
            .status();
    }
}

/// Event kinds of a history without per-attempt bookkeeping (`run_started` and lease /
/// attempt markers), for the journal equality check.
#[cfg(feature = "failpoints")]
fn journal_kinds(h: &[Value]) -> Vec<String> {
    h.iter()
        .filter_map(|e| e["kind"].as_str())
        .filter(|k| !matches!(*k, "run_started" | "attempt_failed" | "attempt_released"))
        .map(str::to_owned)
        .collect()
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigstop_zombie() {
    use reqwest::StatusCode;

    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let lease_ttl = Duration::from_secs(3);
    let c = P7Cluster::start(P7Opts {
        lease_ttl: Some(lease_ttl),
        mock: true,
        extra_toml: "[secrets]\nauto_sealing_key = true\n".into(),
        env: vec![
            ("RUST_LOG".into(), "info,ostro_server=debug".into()),
            // The zombie keeps its stream up: no h2 keepalive drop while it is stopped.
            ("OSTRO_WORKERS__KEEPALIVE".into(), "120s".into()),
        ],
        ..P7Opts::default()
    })
    .await;
    let base = c.mock().base_url();

    // pay-flow: secret, grant, egress.
    let wasm = common::guest_wasm("pay-flow");
    let sha = common::sha_hex(&wasm);
    let r = c
        .api
        .http
        .put(c.api.url(&format!("/v1/modules/{sha}?name=pay-flow")))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "deploy pay-flow: {}", r.status());
    for (path, body) in [
        (
            "/v1/secrets/pay_token".to_owned(),
            json!({"value": "tok-07-18"}),
        ),
        (
            "/v1/grants".to_owned(),
            json!({"module": "pay-flow", "secret": "pay_token", "origins": [base], "body": false}),
        ),
        ("/v1/egress/pay-flow".to_owned(), json!({"allow": [base]})),
    ] {
        let (st, v) = c.api.put(&path, body).await;
        assert_eq!(st, StatusCode::OK, "PUT {path}: {v}");
    }
    c.api.set_flow("pay", json!({"placement": "workers"})).await;

    // Undisturbed baseline on a clean worker.
    let mut clean = c.spawn_worker(WorkerOpts::new("zb-clean", 2));
    c.wait_workers(1).await;
    let baseline = c
        .api
        .start("pay-flow", json!({"input": {"url": base}, "queue": "pay"}))
        .await;
    let r = c.api.wait_terminal(&baseline, P7_T).await;
    assert_eq!(r["status"], "completed", "baseline: {r}");
    let baseline_kinds = journal_kinds(&c.api.history(&baseline).await);
    let baseline_requests = c.mock().total_requests();
    clean.sigterm();
    assert!(clean.wait_exit(P7_T).success());
    c.wait_workers(0).await;

    // The zombie-to-be: the first append and every StartEffect pause 4 s (W4: a late
    // append and a late StartEffect are forced once it is continued).
    let fp_wait = 4_000;
    let mut wa = WorkerOpts::new("z-a", 4);
    wa.rust_log = Some("info,ostro_worker=debug".into());
    wa.env.push((
        "FAILPOINTS".into(),
        format!(
            "worker::before_append=1*return(wait:{fp_wait});\
             worker::before_start_effect=return(wait:{fp_wait})"
        ),
    ));
    let a = c.spawn_worker(wa);
    c.wait_workers(1).await;
    let mut ids = Vec::new();
    for _ in 0..4 {
        ids.push(
            c.api
                .start("pay-flow", json!({"input": {"url": base}, "queue": "pay"}))
                .await,
        );
    }
    let set: BTreeSet<String> = ids.iter().cloned().collect();
    // All 4 on A; 3 got their EffectScheduled append applied (paused before StartEffect),
    // one is paused before its first append.
    until(
        Duration::from_secs(30),
        "4 activations on z-a, 3 applied",
        || async {
            let l = lines(c.server());
            let on_a = activations(&l)
                .iter()
                .filter(|x| set.contains(&x.run) && x.worker == "z-a")
                .count();
            let ap: BTreeSet<String> = applied(&l)
                .into_iter()
                .filter(|(_, r, _)| set.contains(r))
                .map(|(_, r, _)| r)
                .collect();
            on_a == 4 && ap.len() >= 3
        },
    )
    .await;
    let b = c.spawn_worker(WorkerOpts::new("z-b", 8));
    c.wait_workers(2).await;
    let t_stop = Instant::now();
    a.sigstop();
    let _cont = ContGuard(&a.proc);
    assert_eq!(
        c.mock().total_requests(),
        baseline_requests,
        "precondition: an effect was sent before SIGSTOP (raise fp_wait)"
    );
    let pre_l = lines(c.server());
    let paused_before_append: BTreeSet<String> = {
        let ap: BTreeSet<String> = applied(&pre_l).into_iter().map(|(_, r, _)| r).collect();
        set.iter().filter(|r| !ap.contains(*r)).cloned().collect()
    };

    // Revoked after lease_ttl; re-granted n+1 on z-b; completed with one charge per key.
    let mut regrant: BTreeMap<String, Duration> = BTreeMap::new();
    let deadline = t_stop + P7_T;
    while regrant.len() < ids.len() {
        let now = t_stop.elapsed();
        for x in activations(&lines(c.server())) {
            if set.contains(&x.run) && x.attempt >= 2 {
                assert_eq!(x.worker, "z-b", "re-granted to the zombie: {x:?}");
                regrant.entry(x.run.clone()).or_insert(now);
            }
        }
        assert!(Instant::now() < deadline, "not re-granted: {regrant:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut keys = BTreeMap::new();
    for id in &ids {
        let r = c.api.wait_terminal(id, P7_T).await;
        assert_eq!(r["status"], "completed", "{r}");
        assert_eq!(r["result"]["status"], 200, "{r}");
        let key = r["result"]["idempotency_key"]
            .as_str()
            .expect("key")
            .to_owned();
        assert_eq!(c.mock().charges(&key), 1, "{id}: one charge");
        keys.insert(key.clone(), c.mock().requests(&key));
    }
    let completed_at = t_stop.elapsed();

    // Stay stopped until the failpoint waits have expired, so on SIGCONT the late append /
    // StartEffects are due at once (racing the queued Cancel).
    let cont_at = t_stop + Duration::from_millis(fp_wait + 1_000);
    tokio::time::sleep(cont_at.saturating_duration_since(Instant::now())).await;
    let stopped_for = t_stop.elapsed();
    // SIGCONT: the expired failpoints release the late append / StartEffects.
    let l_before_cont = lines(c.server()).len();
    a.sigcont();
    tokio::time::sleep(Duration::from_secs(fp_wait / 1000 + 2)).await;
    let l = lines(c.server());
    let acts = activations(&l);
    for id in &ids {
        let regrant_idx = acts
            .iter()
            .find(|x| &x.run == id && x.attempt >= 2)
            .map(|x| x.idx)
            .expect("re-grant line");
        let late: Vec<_> = applied(&l)
            .into_iter()
            .filter(|(idx, r, n)| r == id && *n == 1 && *idx > regrant_idx)
            .collect();
        assert!(
            late.is_empty(),
            "{id}: attempt 1 applied after the revoke: {late:?}"
        );
    }
    for r in &paused_before_append {
        assert!(
            applied(&l).iter().all(|(_, run, n)| !(run == r && *n == 1)),
            "{r}: the zombie's late append was applied"
        );
    }
    for (k, n) in &keys {
        assert_eq!(
            c.mock().requests(k),
            *n,
            "late StartEffect reached the provider"
        );
        assert_eq!(c.mock().charges(k), 1);
    }
    let a_lines = lines(&a.proc);
    assert!(
        !a_lines
            .iter()
            .any(|x| x.contains("effect started on the leader")),
        "the zombie started an effect:\n{}",
        a_lines.join("\n")
    );
    for id in &ids {
        let h = c.api.history(id).await;
        assert_eq!(events_of(&h, "run_completed").len(), 1, "{id}");
        assert_eq!(
            journal_kinds(&h),
            baseline_kinds,
            "{id}: journal differs from the undisturbed run"
        );
    }
    // Which layer stopped the zombie (W4: either is acceptable).
    let late_effect_rejected = a_lines
        .iter()
        .filter(|x| x.contains("StartEffect rejected") || x.contains("StartEffect failed"))
        .count();
    let server_rejected = l[l_before_cont.min(l.len())..]
        .iter()
        .filter(|x| x.contains("StartEffect rejected"))
        .count();
    let self_fenced = a_lines
        .iter()
        .filter(|x| x.contains("self-fence") || x.contains("cancelled"))
        .count();
    // W4: the late StartEffects were really sent (the failpoint waits expired while the
    // worker was stopped) and the leader refused them; none reached the provider.
    assert!(
        late_effect_rejected >= 1,
        "no late StartEffect was forced through:\n{}",
        a_lines.join("\n")
    );
    let lat: Vec<Duration> = regrant.values().copied().collect();
    eprintln!(
        "worker_failover::sigstop_zombie: lease_ttl {lease_ttl:?}; re-granted n+1 on z-b \
         after max {:?} / median {:?} from SIGSTOP; completed at {completed_at:?}; stopped \
         for {stopped_for:?}; one \
         charge per key {keys:?}; after SIGCONT: worker-side StartEffect rejected/failed \
         lines {late_effect_rejected}, server StartEffect rejected lines {server_rejected}, \
         worker cancel/self-fence lines {self_fenced}; paused-before-append runs {}; no \
         attempt-1 append applied after the revoke; journals equal the baseline; total {:?}",
        lat.iter().max().copied().unwrap_or_default(),
        median(lat),
        paused_before_append.len(),
        t0.elapsed()
    );
    drop(b);
}
