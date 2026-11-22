//! Phase 7 success criterion 4 through the binary (plan 07-19; DISP-03/04/05, OPS-01):
//! a real `ostro server` (P7Cluster, self-signed TLS) and, for one leg, a real
//! `ostro worker`.
//!
//! Task 1 (flow control):
//! - `queue_limits`: queue concurrency 3 holds over 15 runs.
//! - `weights`: queues of weight 3 and 1 share the first 40 grants about 3:1.
//! - `priority`: priority-10 runs all start before priority-0 runs queued with them.
//! - `keyed_fairness`: key_concurrency 1 over 10 keys (one hot key with 10x the backlog):
//!   no key ever runs twice at once, Jain's index over the contended window >= 0.9
//!   (1 partition). `keyed_concurrency_node_wide`: the same load on 4 partitions, no key
//!   ever runs twice at once.
//! - `throttle_vs_rate_limit`: a throttle spaces first activations and accepts every
//!   start, a re-activation is not throttled; a rate limit answers 429 `rate_limited`
//!   + Retry-After and creates nothing; an idempotent re-submit is not limited.
//!
//! Task 2 (pause, OPS-01):
//! - `pause_run`, `pause_queue`, `pause_workflow`, `auto_pause`, `timeout_while_paused`
//!   and `pause_on_worker`.
//!
//! # Measurement
//!
//! `cpu-burn`'s `started_ms` is the run's first journal time, i.e. its creation (07-08
//! deviation 2), so it cannot date an activation. Activation starts are taken from the
//! dispatcher's grant log instead (`dispatching run=<id> n=<n>`, debug level, JSON log
//! format) and ends from the run's `completed_at_ms`. Every execution lies inside
//! `[grant, completed]`: the grant precedes the activation and the lease (with its flow
//! permit) is released only after the terminal record is applied, so overlap counts over
//! these intervals are an upper bound of the true execution overlap.
//!
//! Partitions: priorities, DRR weights and keyed fairness are per partition dispatcher, so
//! `weights`, `priority` and `keyed_fairness` run a 1-partition server; the node-wide
//! limits run 4 partitions.
#![cfg(unix)]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p7.rs"]
mod p7;

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use common::{events_of, step_completions};
use p7::{P7_T, P7Api, P7Cluster, P7Opts, WorkerOpts};
use reqwest::StatusCode;
use serde_json::{Value, json};

/// Spawned servers are heavy: one cluster at a time per test process.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Long waits (whole backlogs on a loaded machine).
const LONG: Duration = Duration::from_secs(180);

// ---------------------------------------------------------------------------------
// Cluster and log helpers
// ---------------------------------------------------------------------------------

/// Server options: JSON logs with the dispatcher's grant lines, `slots` embedded slots,
/// extra `[executor]` lines.
fn opts(partitions: u16, slots: u32, executor_extra: &str) -> P7Opts {
    P7Opts {
        partitions,
        extra_toml: format!(
            "[executor]\nslots = {slots}\n{executor_extra}\n\n[log]\nformat = \"json\"\n"
        ),
        env: vec![(
            "RUST_LOG".into(),
            "info,ostro_partition::leader::dispatcher=debug".into(),
        )],
        ..P7Opts::default()
    }
}

/// One dispatcher grant.
#[derive(Clone, Debug)]
struct Grant {
    run: String,
    n: u64,
    /// Wall time of the log line, µs since the epoch.
    at_us: u64,
}

/// Days since 1970-01-01 of a proleptic Gregorian date.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `YYYY-MM-DDTHH:MM:SS[.frac]Z` -> µs since the epoch.
fn rfc3339_us(s: &str) -> Option<u64> {
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;
    let mut d = date.split('-').map(|p| p.parse::<i64>());
    let (y, mo, da) = (d.next()?.ok()?, d.next()?.ok()?, d.next()?.ok()?);
    let (hms, frac) = time.split_once('.').unwrap_or((time, ""));
    let mut t = hms.split(':').map(|p| p.parse::<i64>());
    let (h, mi, se) = (t.next()?.ok()?, t.next()?.ok()?, t.next()?.ok()?);
    let mut us_frac = 0i64;
    for (i, c) in frac.chars().take(6).enumerate() {
        us_frac += i64::from(c.to_digit(10)?) * 10i64.pow(5 - i as u32);
    }
    let secs = days_from_civil(y, mo, da) * 86_400 + h * 3600 + mi * 60 + se;
    u64::try_from(secs * 1_000_000 + us_frac).ok()
}

/// Every `dispatching` grant line the server logged so far, in log order.
fn grants(c: &P7Cluster) -> Vec<Grant> {
    let p = c.server();
    let mut out: Vec<Grant> = p
        .stdout()
        .into_iter()
        .chain(p.stderr())
        .filter(|l| l.starts_with('{') && l.contains("\"dispatching\""))
        .filter_map(|l| {
            let v: Value = serde_json::from_str(&l).ok()?;
            let f = &v["fields"];
            if f["message"] != "dispatching" {
                return None;
            }
            Some(Grant {
                run: f["run"].as_str()?.to_owned(),
                n: f["n"].as_u64().unwrap_or(0),
                at_us: rfc3339_us(v["timestamp"].as_str()?)?,
            })
        })
        .collect();
    out.sort_by_key(|g| g.at_us);
    out
}

/// First grant time (µs) per run.
fn first_grants(c: &P7Cluster) -> BTreeMap<String, u64> {
    let mut m = BTreeMap::new();
    for g in grants(c) {
        m.entry(g.run).or_insert(g.at_us);
    }
    m
}

/// Grants of one run.
fn grants_of(c: &P7Cluster, run: &str) -> Vec<Grant> {
    grants(c).into_iter().filter(|g| g.run == run).collect()
}

/// Maximum number of simultaneously open `[start, end)` intervals.
fn max_overlap(iv: &[(u64, u64)]) -> usize {
    let mut ev: Vec<(u64, i32)> = Vec::with_capacity(iv.len() * 2);
    for &(s, e) in iv {
        ev.push((s, 1));
        ev.push((e.max(s), -1));
    }
    // Ends sort before starts at the same instant (half-open intervals).
    ev.sort();
    let (mut cur, mut max) = (0i32, 0i32);
    for (_, d) in ev {
        cur += d;
        max = max.max(cur);
    }
    usize::try_from(max).unwrap_or(0)
}

/// Jain's fairness index `(Σx)² / (n·Σx²)`.
fn jain(x: &[f64]) -> f64 {
    let s: f64 = x.iter().sum();
    let s2: f64 = x.iter().map(|v| v * v).sum();
    if s2 == 0.0 {
        return 0.0;
    }
    s * s / (x.len() as f64 * s2)
}

fn median(mut v: Vec<u64>) -> u64 {
    v.sort_unstable();
    v[v.len() / 2]
}

/// Every terminal REST status (`common::is_terminal` predates Phase 5 and lacks
/// `timed_out`, `terminated` and `continued_as_new`).
fn terminal(status: &str) -> bool {
    common::is_terminal(status) || matches!(status, "timed_out" | "terminated" | "continued_as_new")
}

/// Poll until the run has a terminal status (any of [`terminal`]).
async fn wait_end(api: &P7Api, id: &str, limit: Duration) -> Value {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(r) = api.run(id).await
            && r["status"].as_str().is_some_and(terminal)
        {
            return r;
        }
        assert!(
            Instant::now() < deadline,
            "run {id} not terminal within {limit:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_all(api: &P7Api, ids: &[String]) -> Vec<Value> {
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        out.push(wait_end(api, id, LONG).await);
    }
    out
}

fn assert_all_completed(runs: &[Value]) {
    for r in runs {
        assert_eq!(r["status"], "completed", "{r}");
    }
}

/// `[first grant, completed_at]` in µs for each run.
///
/// The grant time is the µs wall time of its log line, but `completed_at_ms` is the
/// journal timestamp of `RunCompleted` rounded **up** to the millisecond (`derive.rs`:
/// `ceil(t / 1e6)`), so `completed_at_ms * 1000` can lie up to 1 ms after the real
/// completion, and after the next grant that the completion's slot release allowed (a
/// sub-millisecond hand-off, seen on the aarch64 CI runner as a phantom `key_concurrency`
/// overlap of 2). The end used here is the lower bound `(completed_at_ms - 1) * 1000`,
/// which never lies after the real completion; a real overlap of two 40 ms+ runs is
/// still counted.
fn intervals(firsts: &BTreeMap<String, u64>, runs: &[Value]) -> Vec<(u64, u64)> {
    runs.iter()
        .map(|r| {
            let id = r["run_id"].as_str().expect("run_id");
            let s = *firsts
                .get(id)
                .unwrap_or_else(|| panic!("no grant logged for {id}"));
            let e = r["completed_at_ms"]
                .as_u64()
                .expect("completed_at_ms")
                .saturating_sub(1)
                * 1000;
            (s, e)
        })
        .collect()
}

async fn ok_post(api: &P7Api, path: &str, body: Value) -> Value {
    let (st, v) = api.post(path, body).await;
    assert_eq!(st, StatusCode::OK, "POST {path}: {v}");
    v
}

async fn wait_until<F, Fut>(what: &str, limit: Duration, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + limit;
    while !f().await {
        assert!(Instant::now() < deadline, "{what} not within {limit:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ---------------------------------------------------------------------------------
// Task 1: limits, weights, priority, fairness, throttle vs rate limit
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_limits() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let c = P7Cluster::start(opts(4, 8, "")).await;
    c.api.deploy("cpu-burn").await;
    let flow = c.api.set_flow("lim", json!({"concurrency": 3})).await;
    assert_eq!(flow["concurrency"], 3, "{flow}");

    let mut ids = Vec::new();
    for _ in 0..15 {
        ids.push(
            c.api
                .start("cpu-burn", json!({"input": {"ms": 300}, "queue": "lim"}))
                .await,
        );
    }
    let runs = wait_all(&c.api, &ids).await;
    assert_all_completed(&runs);
    let iv = intervals(&first_grants(&c), &runs);
    let max = max_overlap(&iv);
    // The guest's own (creation-based) intervals, for the record only.
    let guest_iv: Vec<(u64, u64)> = runs
        .iter()
        .map(|r| {
            (
                r["result"]["started_ms"].as_u64().unwrap_or(0) * 1000,
                r["result"]["ended_ms"].as_u64().unwrap_or(0) * 1000,
            )
        })
        .collect();
    let span_ms = (iv.iter().map(|x| x.1).max().unwrap_or(0)
        - iv.iter().map(|x| x.0).min().unwrap_or(0))
        / 1000;
    eprintln!(
        "flow_e2e::queue_limits: 15 runs, max overlap {max} (cap 3), guest-interval overlap \
         {} (creation-based, not a bound), grant-to-completion span {span_ms} ms, total {:?}",
        max_overlap(&guest_iv),
        t0.elapsed()
    );
    assert!(max <= 3, "queue concurrency 3 exceeded: {max} overlapping");
    assert!(max >= 2, "the cap was never contended: max overlap {max}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn weights() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let c = P7Cluster::start(opts(1, 2, "")).await;
    c.api.deploy("cpu-burn").await;
    c.api.set_flow("wa", json!({"weight": 3})).await;
    c.api.set_flow("wb", json!({"weight": 1})).await;
    // Hold both backlogs behind a workflow pause so that both are deep when dispatch
    // starts (one resume releases both queues at once).
    ok_post(&c.api, "/v1/workflows/cpu-burn/pause", json!({})).await;
    let mut a = BTreeSet::new();
    let mut b = BTreeSet::new();
    for i in 0..90 {
        let q = if i % 3 == 2 { "wb" } else { "wa" };
        let id = c
            .api
            .start("cpu-burn", json!({"input": {"ms": 10}, "queue": q}))
            .await;
        if q == "wa" {
            a.insert(id);
        } else {
            b.insert(id);
        }
    }
    assert!(first_grants(&c).is_empty(), "granted while paused");
    ok_post(&c.api, "/v1/workflows/cpu-burn/resume", json!({})).await;
    let ids: Vec<String> = a.iter().chain(b.iter()).cloned().collect();
    assert_all_completed(&wait_all(&c.api, &ids).await);

    let mut order: Vec<(u64, String)> = first_grants(&c)
        .into_iter()
        .filter(|(id, _)| a.contains(id) || b.contains(id))
        .map(|(id, t)| (t, id))
        .collect();
    order.sort();
    let first40: Vec<&String> = order.iter().take(40).map(|(_, id)| id).collect();
    assert_eq!(first40.len(), 40);
    let na = first40.iter().filter(|id| a.contains(**id)).count();
    let nb = 40 - na;
    let ratio = na as f64 / nb.max(1) as f64;
    eprintln!(
        "flow_e2e::weights: first 40 grants A(w3) {na} : B(w1) {nb} = {ratio:.2}, total {:?}",
        t0.elapsed()
    );
    assert!(
        (2.2..=3.8).contains(&ratio),
        "A:B over the first 40 grants = {na}:{nb} ({ratio:.2}), want about 3:1"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn priority() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let c = P7Cluster::start(opts(1, 1, "")).await;
    c.api.deploy("cpu-burn").await;
    ok_post(&c.api, "/v1/queues/pq/pause", json!({})).await;
    let mut hi = BTreeSet::new();
    let mut lo = BTreeSet::new();
    for i in 0..20 {
        let p = if i % 2 == 0 { 0 } else { 10 };
        let id = c
            .api
            .start(
                "cpu-burn",
                json!({"input": {"ms": 10}, "queue": "pq", "priority": p}),
            )
            .await;
        if p == 10 {
            hi.insert(id);
        } else {
            lo.insert(id);
        }
    }
    ok_post(&c.api, "/v1/queues/pq/resume", json!({})).await;
    let ids: Vec<String> = hi.iter().chain(lo.iter()).cloned().collect();
    let runs = wait_all(&c.api, &ids).await;
    assert_all_completed(&runs);
    for r in &runs {
        let p = r["priority"].as_i64().expect("priority");
        let id = r["run_id"].as_str().expect("run_id");
        assert_eq!(p == 10, hi.contains(id), "{r}");
    }
    let mut order: Vec<(u64, String)> = first_grants(&c)
        .into_iter()
        .filter(|(id, _)| hi.contains(id) || lo.contains(id))
        .map(|(id, t)| (t, id))
        .collect();
    order.sort();
    let bands: String = order
        .iter()
        .map(|(_, id)| if hi.contains(id) { 'H' } else { 'L' })
        .collect();
    eprintln!(
        "flow_e2e::priority: start order {bands} (H = 10, L = 0), total {:?}",
        t0.elapsed()
    );
    assert_eq!(order.len(), 20);
    assert_eq!(
        bands,
        format!("{}{}", "H".repeat(10), "L".repeat(10)),
        "priority-10 runs must all start before priority-0 runs queued with them"
    );
}

/// Keyed fairness (DRR over concurrency keys) is per partition dispatcher, like priorities
/// and queue weights (07-19-SUMMARY): with N partitions each dispatcher only arbitrates
/// between the keys whose runs hash to it, and the hot key has backlog in every partition,
/// so node-wide it wins a freed slot in each of them and the node-wide Jain index measures
/// partition placement, not the scheduler (macOS CI: 0.89 over [18,7,10,9,...] on 4
/// partitions). The fairness bound is therefore checked on 1 partition.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_fairness() {
    keyed(1, "keyed_fairness", true).await;
}

/// key_concurrency is a node-wide limit: no key runs twice at once across 4 partitions.
/// Jain is logged but not asserted here (see `keyed_fairness`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_concurrency_node_wide() {
    keyed(4, "keyed_concurrency_node_wide", false).await;
}

async fn keyed(partitions: u16, name: &str, assert_jain: bool) {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let c = P7Cluster::start(opts(partitions, 4, "")).await;
    c.api.deploy("cpu-burn").await;
    c.api.set_flow("kq", json!({"key_concurrency": 1})).await;
    // Everything is submitted "at t0": held behind a queue pause, released at once.
    ok_post(&c.api, "/v1/queues/kq/pause", json!({})).await;
    let mut key_of: BTreeMap<String, usize> = BTreeMap::new();
    for round in 0..100 {
        for k in 0..10usize {
            if k != 0 && round >= 10 {
                continue;
            }
            let id = c
                .api
                .start(
                    "cpu-burn",
                    json!({"input": {"ms": 40}, "queue": "kq", "concurrency_key": format!("k{k}")}),
                )
                .await;
            key_of.insert(id, k);
        }
    }
    assert_eq!(key_of.len(), 190);
    ok_post(&c.api, "/v1/queues/kq/resume", json!({})).await;
    let ids: Vec<String> = key_of.keys().cloned().collect();
    let runs = wait_all(&c.api, &ids).await;
    assert_all_completed(&runs);

    let firsts = first_grants(&c);
    let iv = intervals(&firsts, &runs);
    let mut per_key_iv: Vec<Vec<(u64, u64)>> = vec![Vec::new(); 10];
    let mut done: Vec<Vec<u64>> = vec![Vec::new(); 10];
    for (r, i) in runs.iter().zip(&iv) {
        let k = key_of[r["run_id"].as_str().expect("run_id")];
        per_key_iv[k].push(*i);
        done[k].push(r["completed_at_ms"].as_u64().expect("completed_at_ms"));
    }
    let per_key_max: Vec<usize> = per_key_iv.iter().map(|v| max_overlap(v)).collect();
    // The contended window ends when the first key runs out of backlog (its last run
    // completes).
    let window_end = done
        .iter()
        .map(|d| *d.iter().max().expect("runs"))
        .min()
        .expect("keys");
    let counts: Vec<f64> = done
        .iter()
        .map(|d| d.iter().filter(|t| **t <= window_end).count() as f64)
        .collect();
    let j = jain(&counts);
    eprintln!(
        "flow_e2e::{name}: partitions {partitions}, per-key max concurrency {per_key_max:?}, \
         node overlap {} (slots 4), completions per key in the contended window {counts:?}, \
         Jain {j:.4}, total {:?}",
        max_overlap(&iv),
        t0.elapsed()
    );
    for (k, m) in per_key_max.iter().enumerate() {
        assert!(*m <= 1, "key k{k} ran {m} at once (key_concurrency 1)");
    }
    if assert_jain {
        assert!(j >= 0.9, "Jain's index {j:.4} < 0.9 over {counts:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn throttle_vs_rate_limit() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let c = P7Cluster::start(P7Opts {
        // Short hot wait: the sleeper evicts and is re-activated by its timer.
        hot_wait: Some(Duration::from_millis(200)),
        ..opts(4, 8, "")
    })
    .await;
    c.api.deploy("cpu-burn").await;
    c.api.deploy("timeouts").await;

    // --- Throttle: every start accepted, first activations spaced.
    c.api
        .set_flow(
            "tq",
            json!({"throttle": {"limit": 5, "period_ms": 1000, "burst": 1}}),
        )
        .await;
    let t_starts = Instant::now();
    let mut ids = Vec::new();
    for _ in 0..15 {
        // `start` asserts 201.
        ids.push(
            c.api
                .start("cpu-burn", json!({"input": {"ms": 1}, "queue": "tq"}))
                .await,
        );
    }
    let starts_took = t_starts.elapsed();
    assert_all_completed(&wait_all(&c.api, &ids).await);
    let firsts = first_grants(&c);
    let mut ts: Vec<u64> = ids
        .iter()
        .map(|id| {
            *firsts
                .get(id)
                .unwrap_or_else(|| panic!("no grant for {id}"))
        })
        .collect();
    ts.sort_unstable();
    let gaps: Vec<u64> = ts.windows(2).map(|w| (w[1] - w[0]) / 1000).collect();
    let med = median(gaps.clone());
    let span = (ts[14] - ts[0]) / 1000;
    eprintln!(
        "flow_e2e::throttle: 15 starts accepted in {starts_took:?}; first-activation gaps ms \
         {gaps:?}, median {med} ms, min {} ms, span {span} ms",
        gaps.iter().min().expect("gaps")
    );
    assert!(med >= 150, "median first-activation gap {med} ms < 150 ms");

    // --- Re-activations are not throttled.
    let sleeper = c
        .api
        .start(
            "timeouts",
            json!({"input": {"mode": "sleep_ms", "n": 2500}, "queue": "tq"}),
        )
        .await;
    let t_sleep = Instant::now();
    wait_until("sleeper suspended", P7_T, || {
        let api = c.api.clone();
        let id = sleeper.clone();
        async move { !events_of(&api.history(&id).await, "timer_started").is_empty() }
    })
    .await;
    // One first activation per 10 minutes from now on.
    c.api
        .set_flow(
            "tq",
            json!({"throttle": {"limit": 1, "period_ms": 600000, "burst": 1}}),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let probes = [
        c.api
            .start("cpu-burn", json!({"input": {"ms": 1}, "queue": "tq"}))
            .await,
        c.api
            .start("cpu-burn", json!({"input": {"ms": 1}, "queue": "tq"}))
            .await,
    ];
    let r = wait_end(&c.api, &sleeper, P7_T).await;
    let sleeper_took = t_sleep.elapsed();
    assert_eq!(r["status"], "completed", "{r}");
    assert_eq!(r["result"], "slept", "{r}");
    let sg = grants_of(&c, &sleeper);
    assert!(sg.len() >= 2, "sleeper was not re-activated: {sg:?}");
    assert!(
        sg.windows(2).all(|w| w[0].n < w[1].n),
        "re-activation tokens not increasing: {sg:?}"
    );
    let held: Vec<&String> = probes
        .iter()
        .filter(|id| grants_of(&c, id).is_empty())
        .collect();
    eprintln!(
        "flow_e2e::throttle: sleeper (2.5 s timer) re-activated (grants {}) and completed \
         {sleeper_took:?} after start under a 1-per-10-min throttle; probes held: {}",
        sg.len(),
        held.len()
    );
    assert!(
        sleeper_took < Duration::from_secs(20),
        "re-activation delayed: {sleeper_took:?}"
    );
    assert!(
        !held.is_empty(),
        "the 1-per-10-min throttle held no first activation"
    );

    // --- Rate limit: 3 accepted, 3 x 429, nothing created for the rejects.
    // 3 per 60 s with burst 3 (07-08's spec): no token refills during 6 rapid starts even
    // on a loaded machine (T = 20 s).
    c.api
        .set_flow(
            "rq",
            json!({"rate_limit": {"limit": 3, "period_ms": 60000, "burst": 3}}),
        )
        .await;
    let mut accepted = Vec::new();
    let mut limited = Vec::new();
    for i in 0..6 {
        let r = c
            .api
            .http
            .post(c.api.url("/v1/workflows/cpu-burn/runs"))
            .json(&json!({"input": {"ms": 1}, "queue": "rq", "idempotency_key": format!("rk-{i}")}))
            .send()
            .await
            .expect("start");
        let st = r.status();
        let retry_after = r
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let body: Value = r.json().await.expect("json");
        match st {
            StatusCode::CREATED => {
                accepted.push((i, body["run_id"].as_str().expect("id").to_owned()))
            }
            StatusCode::TOO_MANY_REQUESTS => limited.push((i, retry_after, body)),
            other => panic!("start {i}: {other} {body}"),
        }
    }
    eprintln!(
        "flow_e2e::rate_limit: accepted {:?}, 429s {:?}",
        accepted.iter().map(|a| a.0).collect::<Vec<_>>(),
        limited
            .iter()
            .map(|(i, ra, b)| format!("#{i} Retry-After={ra:?} code={}", b["error"]["code"]))
            .collect::<Vec<_>>()
    );
    assert_eq!(accepted.len(), 3, "accepted {accepted:?}");
    assert_eq!(limited.len(), 3, "limited {limited:?}");
    for (i, ra, b) in &limited {
        assert_eq!(b["error"]["code"], "rate_limited", "#{i}: {b}");
        let secs: u64 = ra
            .as_deref()
            .unwrap_or_else(|| panic!("#{i}: no Retry-After"))
            .parse()
            .expect("Retry-After seconds");
        assert!(secs >= 1, "#{i}: Retry-After {secs}");
    }
    let listed = list_runs(&c.api, "queue=rq").await;
    assert_eq!(listed.len(), 3, "runs in rq: {listed:?}");
    // An idempotent re-submit of an accepted start returns the existing run, not 429.
    let (i0, id0) = &accepted[0];
    let (st, b) = c
        .api
        .post(
            "/v1/workflows/cpu-burn/runs",
            json!({"input": {"ms": 1}, "queue": "rq", "idempotency_key": format!("rk-{i0}")}),
        )
        .await;
    assert!(st.is_success(), "idempotent re-submit: {st} {b}");
    assert_eq!(b["run_id"].as_str(), Some(id0.as_str()), "{b}");
    assert_eq!(b["created"], false, "{b}");
    assert_eq!(list_runs(&c.api, "queue=rq").await.len(), 3);
    eprintln!(
        "flow_e2e::throttle_vs_rate_limit: re-submit {st} same run, total {:?}",
        t0.elapsed()
    );
}

/// Run ids of `GET /v1/runs?<query>` across every page.
async fn list_runs(api: &P7Api, query: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut path = format!("/v1/runs?{query}&limit=50");
        if let Some(t) = &token {
            path.push_str(&format!("&page_token={t}"));
        }
        let (st, p) = api.get(&path).await;
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

// ---------------------------------------------------------------------------------
// Task 2: pause, resume, auto-pause, timeout while paused
// ---------------------------------------------------------------------------------

/// Whether the run is paused and live: `/retry` dry run answers `mode = resumed` only for
/// a non-terminal paused run (07-14 alias; the run view has no pause field).
async fn is_paused(api: &P7Api, id: &str) -> bool {
    let (st, v) = api
        .post(&format!("/v1/runs/{id}/retry"), json!({"dry_run": true}))
        .await;
    let live = api
        .run(id)
        .await
        .is_some_and(|r| r["status"].as_str().is_some_and(|s| !terminal(s)));
    st == StatusCode::OK && v["mode"] == "resumed" && live
}

/// Journal length via a binary search over `history?from_seq=N&limit=1`.
async fn journal_len(api: &P7Api, id: &str) -> u64 {
    let has = |n: u64| {
        let api = api.clone();
        let id = id.to_owned();
        async move {
            let (st, p) = api
                .get(&format!("/v1/runs/{id}/history?from_seq={n}&limit=1"))
                .await;
            assert_eq!(st, StatusCode::OK, "{p}");
            p["events"].as_array().is_some_and(|e| !e.is_empty())
        }
    };
    let mut hi = 1u64;
    while has(hi).await {
        hi *= 2;
    }
    let mut lo = 0u64; // has(lo) or lo == 0
    while lo + 1 < hi {
        let mid = (lo + hi) / 2;
        if has(mid).await {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    if has(0).await { lo + 1 } else { 0 }
}

/// Kinds of a run's history, in order.
async fn kinds(api: &P7Api, id: &str) -> Vec<String> {
    api.history(id)
        .await
        .iter()
        .map(|e| e["kind"].as_str().unwrap_or("").to_owned())
        .collect()
}

fn count(k: &[String], kind: &str) -> usize {
    k.iter().filter(|x| *x == kind).count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_run() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let c = P7Cluster::start(opts(4, 4, "")).await;
    c.api.deploy("signals").await;
    // `fifo`: three waits for signal `x`, one per step of the run.
    let id = c
        .api
        .start("signals", json!({"input": {"mode": "fifo"}}))
        .await;
    wait_until("first wait", P7_T, || {
        let api = c.api.clone();
        let id = id.clone();
        async move { count(&kinds(&api, &id).await, "signal_wait_started") >= 1 }
    })
    .await;
    ok_post(
        &c.api,
        &format!("/v1/runs/{id}/signals/x"),
        json!({"payload": 1}),
    )
    .await;
    wait_until("second wait", P7_T, || {
        let api = c.api.clone();
        let id = id.clone();
        async move { count(&kinds(&api, &id).await, "signal_wait_started") >= 2 }
    })
    .await;

    // Pause mid-run.
    let p = ok_post(&c.api, &format!("/v1/runs/{id}/pause"), json!({})).await;
    assert_eq!(
        (p["paused"].clone(), p["changed"].clone()),
        (json!(true), json!(true)),
        "{p}"
    );
    let p = ok_post(&c.api, &format!("/v1/runs/{id}/pause"), json!({})).await;
    assert_eq!(p["changed"], false, "a repeat pause reports no change: {p}");
    assert!(is_paused(&c.api, &id).await, "run not reported paused");
    let before = kinds(&c.api, &id).await;
    let grants_before = grants_of(&c, &id).len();

    // A signal while paused is journaled but does not wake the run.
    ok_post(
        &c.api,
        &format!("/v1/runs/{id}/signals/x"),
        json!({"payload": 2}),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let during = kinds(&c.api, &id).await;
    let added: Vec<&String> = during[before.len()..].iter().collect();
    eprintln!(
        "flow_e2e::pause_run: before pause {} events, while paused +{added:?}, grants {} -> {}",
        before.len(),
        grants_before,
        grants_of(&c, &id).len()
    );
    assert_eq!(during[..before.len()], before[..], "history rewritten");
    assert!(
        !added.is_empty(),
        "the signal sent while paused was not journaled"
    );
    for k in &added {
        assert!(
            k.starts_with("signal_"),
            "progress while paused (event {k}): {added:?}"
        );
    }
    assert_eq!(
        count(&during, "signal_wait_started"),
        count(&before, "signal_wait_started"),
        "a new wait started while paused"
    );
    assert_eq!(
        grants_of(&c, &id).len(),
        grants_before,
        "activated while paused"
    );
    let r = c.api.run(&id).await.expect("run");
    assert!(!terminal(r["status"].as_str().unwrap_or("")), "{r}");

    ok_post(
        &c.api,
        &format!("/v1/runs/{id}/signals/x"),
        json!({"payload": 3}),
    )
    .await;
    let rs = ok_post(&c.api, &format!("/v1/runs/{id}/resume"), json!({})).await;
    assert_eq!(
        (rs["paused"].clone(), rs["changed"].clone()),
        (json!(false), json!(true)),
        "{rs}"
    );
    let r = wait_end(&c.api, &id, P7_T).await;
    assert_eq!(r["status"], "completed", "{r}");
    assert_eq!(r["result"], json!([1, 2, 3]), "{r}");
    eprintln!("flow_e2e::pause_run: total {:?}", t0.elapsed());
}

/// Queue or workflow pause: an in-flight activation finishes, 5 new runs do not start
/// for 2 s, resume completes them all.
async fn pause_scope(workflow_scope: bool) {
    let label = if workflow_scope {
        "pause_workflow"
    } else {
        "pause_queue"
    };
    let t0 = Instant::now();
    let c = P7Cluster::start(opts(4, 4, "")).await;
    c.api.deploy("cpu-burn").await;
    c.api.deploy("smoke").await;
    let q = "pzq";
    let inflight = c
        .api
        .start("cpu-burn", json!({"input": {"ms": 1500}, "queue": q}))
        .await;
    wait_until("in-flight grant", P7_T, || {
        let ok = !grants_of(&c, &inflight).is_empty();
        async move { ok }
    })
    .await;
    let (pause, resume) = if workflow_scope {
        (
            "/v1/workflows/cpu-burn/pause".to_owned(),
            "/v1/workflows/cpu-burn/resume".to_owned(),
        )
    } else {
        (
            format!("/v1/queues/{q}/pause"),
            format!("/v1/queues/{q}/resume"),
        )
    };
    let p = ok_post(&c.api, &pause, json!({})).await;
    assert_eq!(
        (p["paused"].clone(), p["changed"].clone()),
        (json!(true), json!(true)),
        "{p}"
    );
    let t_paused = Instant::now();
    let mut held = Vec::new();
    for _ in 0..5 {
        held.push(
            c.api
                .start("cpu-burn", json!({"input": {"ms": 10}, "queue": q}))
                .await,
        );
    }
    // The in-flight activation finishes its current activation.
    let r = wait_end(&c.api, &inflight, P7_T).await;
    assert_eq!(r["status"], "completed", "{r}");
    // A workflow pause leaves other workflows on the same queue running (fix-0705).
    let other = if workflow_scope {
        let s = c
            .api
            .start("smoke", json!({"input": {"k": 1}, "queue": q}))
            .await;
        let r = wait_end(&c.api, &s, P7_T).await;
        assert_eq!(
            r["status"], "completed",
            "smoke under a cpu-burn pause: {r}"
        );
        "; smoke on the same queue completed while paused"
    } else {
        ""
    };
    let wait = Duration::from_secs(2).saturating_sub(t_paused.elapsed());
    tokio::time::sleep(wait.max(Duration::from_secs(2))).await;
    for id in &held {
        assert!(grants_of(&c, id).is_empty(), "{id} granted while paused");
        let r = c.api.run(id).await.expect("run");
        assert_eq!(r["status"], "ready", "{r}");
    }
    let held_for = t_paused.elapsed();
    let rs = ok_post(&c.api, &resume, json!({})).await;
    assert_eq!(
        (rs["paused"].clone(), rs["changed"].clone()),
        (json!(false), json!(true)),
        "{rs}"
    );
    let t_res = Instant::now();
    assert_all_completed(&wait_all(&c.api, &held).await);
    eprintln!(
        "flow_e2e::{label}: in-flight run completed while paused, 5 runs held {held_for:?} \
         (no grant), all 5 completed {:?} after resume{other}; total {:?}",
        t_res.elapsed(),
        t0.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_queue() {
    let _serial = SERIAL.lock().await;
    pause_scope(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_workflow() {
    let _serial = SERIAL.lock().await;
    pause_scope(true).await;
}

/// `wit-surface` workflow `panic`: step `before-panic`, then a guest panic outside any
/// step (`AttemptFailed(Trap)`, retryable) on every attempt. retry-probe's `trap` mode is
/// selected by raw UTF-8 input, which the JSON start APIs cannot send.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_pause() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let c = P7Cluster::start(opts(
        4,
        4,
        "max_attempt_failures = 2\nretry_backoff = \"50ms\"",
    ))
    .await;
    c.api.deploy("wit-surface").await;
    let flow = c
        .api
        .set_flow("ap", json!({"on_retries_exhausted": "pause"}))
        .await;
    assert_eq!(flow["on_retries_exhausted"], "pause", "{flow}");

    let id = c.api.start("panic", json!({"queue": "ap"})).await;
    wait_until("auto-pause", P7_T, || {
        let api = c.api.clone();
        let id = id.clone();
        async move {
            if let Some(r) = api.run(&id).await {
                assert!(
                    !terminal(r["status"].as_str().unwrap_or("")),
                    "run ended instead of pausing: {r}"
                );
            }
            is_paused(&api, &id).await
        }
    })
    .await;
    let r = c.api.run(&id).await.expect("run");
    // Both attempts used (max_attempt_failures = 2); the exhausting failure pauses
    // instead of being counted.
    assert_eq!(r["attempt"], 2, "{r}");
    assert!(r["result"].is_null(), "{r}");
    let g1 = grants_of(&c, &id).len();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(grants_of(&c, &id).len(), g1, "granted while auto-paused");
    assert!(is_paused(&c.api, &id).await);

    // `/retry` on a paused run is resume: 200, mode resumed, the run is granted again.
    let rv = ok_post(&c.api, &format!("/v1/runs/{id}/retry"), json!({})).await;
    assert_eq!(rv["mode"], "resumed", "{rv}");
    assert_eq!(rv["run_id"].as_str(), Some(id.as_str()), "{rv}");
    wait_until("re-grant after /retry", P7_T, || {
        let n = grants_of(&c, &id).len();
        async move { n > g1 }
    })
    .await;
    // The guest panics on every attempt, so the resumed run exhausts again and is
    // auto-paused again rather than failing.
    wait_until("auto-pause after resume", P7_T, || {
        let api = c.api.clone();
        let id = id.clone();
        async move { is_paused(&api, &id).await }
    })
    .await;
    let r2 = c.api.run(&id).await.expect("run");
    assert!(!terminal(r2["status"].as_str().unwrap_or("")), "{r2}");
    let g2 = grants_of(&c, &id).len();

    // Without the policy the same workflow fails (Phase 5 behaviour).
    let plain = c.api.start("panic", json!({"queue": "fq"})).await;
    let rf = wait_end(&c.api, &plain, P7_T).await;
    assert_eq!(rf["status"], "failed", "{rf}");
    eprintln!(
        "flow_e2e::auto_pause: paused at attempt {} (grants {g1}); /retry -> \
         {rv}; re-granted and re-paused (grants {g2}, attempt_failures {}); without the \
         policy: {} after {} grants; total {:?}",
        r["attempt"],
        r2["attempt_failures"],
        rf["status"],
        grants_of(&c, &plain).len(),
        t0.elapsed()
    );
}

/// `smoke` declares a 60 s run timeout (the only short one among the test guests; the
/// start API has no run-timeout field).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeout_while_paused() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let c = P7Cluster::start(opts(4, 4, "")).await;
    c.api.deploy("smoke").await;
    // Pause before the first grant: hold the queue, pause the run, release the queue.
    ok_post(&c.api, "/v1/queues/tw/pause", json!({})).await;
    let id = c
        .api
        .start("smoke", json!({"input": {"v": 7}, "queue": "tw"}))
        .await;
    let created = c.api.run(&id).await.expect("run")["created_at_ms"]
        .as_u64()
        .expect("created_at_ms");
    let p = ok_post(&c.api, &format!("/v1/runs/{id}/pause"), json!({})).await;
    assert_eq!(p["changed"], true, "{p}");
    ok_post(&c.api, "/v1/queues/tw/resume", json!({})).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(grants_of(&c, &id).is_empty(), "granted while paused");
    assert!(is_paused(&c.api, &id).await);

    // The run timeout fires although the run is paused.
    let r = wait_end(&c.api, &id, Duration::from_secs(150)).await;
    assert_eq!(r["status"], "timed_out", "{r}");
    let ended = r["completed_at_ms"].as_u64().expect("completed_at_ms");
    let (st, b) = c
        .api
        .post(&format!("/v1/runs/{id}/resume"), json!({}))
        .await;
    assert_eq!(st, StatusCode::CONFLICT, "resume of a timed-out run: {b}");

    // D1 resume in place: granted again (a stale pause mark would hold it) and completes.
    let rv = ok_post(&c.api, &format!("/v1/runs/{id}/retry"), json!({})).await;
    assert_eq!(rv["mode"], "resumed", "{rv}");
    let t_retry = Instant::now();
    wait_until("completed after retry", P7_T, || {
        let api = c.api.clone();
        let id = id.clone();
        async move {
            api.run(&id)
                .await
                .is_some_and(|r| r["status"] == "completed")
        }
    })
    .await;
    let r = c.api.run(&id).await.expect("run");
    assert_eq!(r["result"], json!({"v": 7}), "{r}");
    assert!(!grants_of(&c, &id).is_empty());
    eprintln!(
        "flow_e2e::timeout_while_paused: timed out {} ms after creation while paused; \
         resume 409; /retry {rv} -> completed in {:?}; total {:?}",
        ended.saturating_sub(created),
        t_retry.elapsed(),
        t0.elapsed()
    );
}

/// Workers queue: pausing a run executing on a worker revokes its lease (the worker
/// cancels the activation, progress stops); resume continues it on the worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_on_worker() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let c = P7Cluster::start(opts(4, 4, "")).await;
    let w = c.spawn_worker(WorkerOpts::new("pause-w1", 2).label("pool", "pz"));
    c.wait_workers(1).await;
    c.api.deploy("timeouts").await;
    c.api
        .set_flow(
            "wq",
            json!({"placement": "workers", "selector": ["pool=pz"]}),
        )
        .await;
    // The worker appends a busy activation's events when the activation ends (no append
    // was visible mid-activation in 07-19's probes), so "executing" is read from the
    // dispatcher's lease gauge, not from journal growth.
    let n: u64 = std::env::var("P719_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000);
    let id = c
        .api
        .start(
            "timeouts",
            json!({"input": {"mode": "busy_steps", "n": n}, "queue": "wq"}),
        )
        .await;
    wait_until("executing on the worker", P7_T, || {
        let api = c.api.clone();
        async move {
            api.demand("wq").await["running_on_workers"]
                .as_u64()
                .unwrap_or(0)
                > 0
        }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let p = ok_post(&c.api, &format!("/v1/runs/{id}/pause"), json!({})).await;
    assert_eq!(p["changed"], true, "{p}");
    let t_pause = Instant::now();
    // The revoke releases the lease (the worker cancels the activation).
    wait_until("lease released after pause", P7_T, || {
        let api = c.api.clone();
        async move { api.demand("wq").await["running_on_workers"] == 0 }
    })
    .await;
    let released = t_pause.elapsed();
    let r = c.api.run(&id).await.expect("run");
    assert!(
        !terminal(r["status"].as_str().unwrap_or("")),
        "the run finished before the pause took effect (raise P719_N): {r}"
    );
    let l1 = journal_len(&c.api, &id).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let l2 = journal_len(&c.api, &id).await;
    assert_eq!(l1, l2, "journal grew while paused");
    let r = c.api.run(&id).await.expect("run");
    assert!(!terminal(r["status"].as_str().unwrap_or("")), "{r}");
    assert!(is_paused(&c.api, &id).await);
    assert_eq!(grants_of(&c, &id).len(), 1, "{:?}", grants_of(&c, &id));
    let at_pause = l1;

    let rs = ok_post(&c.api, &format!("/v1/runs/{id}/resume"), json!({})).await;
    assert_eq!(rs["changed"], true, "{rs}");
    let t_res = Instant::now();
    let r = wait_end(&c.api, &id, LONG).await;
    assert_eq!(r["status"], "completed", "{r}");
    assert_eq!(r["result"], format!("busy:{n}"), "{r}");
    let ns: Vec<u64> = grants_of(&c, &id).iter().map(|g| g.n).collect();
    let g = ns.len();
    assert!(g >= 2, "not re-granted after resume: {ns:?}");
    assert!(
        ns.windows(2).all(|w| w[0] < w[1]),
        "attempt tokens not increasing: {ns:?}"
    );
    let h = c.api.history(&id).await;
    assert_eq!(step_completions(&h, "b0"), 1, "steps re-executed");
    let out = c.server().output();
    let remote = out.matches(&format!("remote activate run={id}")).count();
    assert!(remote >= 2, "both activations on the worker: {remote}");
    let errors = w.error_lines();
    eprintln!(
        "flow_e2e::pause_on_worker: n = {n}; lease released {released:?} after the pause, \
         journal frozen at {at_pause} events for 2 s; resumed -> completed in {:?}, grant tokens {ns:?}, \
         remote activations {remote}, worker ERROR lines {}; total {:?}",
        t_res.elapsed(),
        errors.len(),
        t0.elapsed()
    );
    drop(w);
}
