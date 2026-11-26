//! Dispatch-side tenant quotas and weighted fairness on a real node (plan 14-06, DISP-06,
//! D-P14-14/15/16). Tenants are created by writing `Tenant` meta records directly (the
//! admin API lands in 14-17); runs start with `tenant` set on the 05-24 start path, so
//! their workflow, queue and records are tenant-qualified.
//!
//! - `max_concurrency` caps a tenant's leased activations (sampled from the node flow's
//!   tenant counters); every run completes.
//! - `memory_bytes` below two instances' memory cap admits one activation at a time.
//! - A small `fuel_per_s` spreads a fuel-heavy workload in time (completion time >=
//!   (total fuel - burst) / rate); no run fails.
//! - A suspended tenant's ready runs are held (not dispatched, not failed) and complete
//!   after the resume.
//! - Weights 1:2:4 on a saturated node: throughput shares within +-15% and Jain's index over
//!   weight-normalised throughput >= 0.9.
//! - Noisy neighbour: tenant b's queueing p99 with tenant a at 10x load stays within 2x of
//!   b's solo p99 (a is capped by `max_concurrency`).
//! - `/metrics` carries the `ostro_tenant_*` series without a `tenant` label by default.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, guest_wasm};
use ostro_meta::{MetaCommand, MetaRecord, MetaResult, Tenant};
use ostro_types::RunId;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use tokio::time::sleep;

const LIMIT: Duration = Duration::from_secs(120);

async fn server(partitions: u16, slots: u32) -> TestServer {
    let s = TestServer::start_with(
        ServerOpts {
            partitions,
            ..ServerOpts::default()
        },
        |c| c.executor.slots = slots,
    )
    .await;
    s.wait_ready(Duration::from_secs(10)).await;
    s
}

/// Upload `cpu-burn` into `tenant` (`None` = the default tenant).
async fn deploy(s: &TestServer, tenant: Option<&str>) {
    let wasm = guest_wasm("cpu-burn");
    let sha: String = Sha256::digest(&wasm)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let q = tenant.map_or(String::new(), |t| format!("?tenant={t}&name=burn"));
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{sha}{q}")))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "deploy {tenant:?}: {}", r.status());
}

/// Create or overwrite a `Tenant` record (CAS on the current version).
async fn tenant(s: &TestServer, t: Tenant) {
    let meta = s.h().meta();
    let record = MetaRecord::Tenant(t);
    let key = record.key();
    let expected_version = meta.view().read(|st| st.get(&key).map(|v| v.version));
    let r = meta
        .propose(MetaCommand::Put {
            key,
            expected_version,
            record,
        })
        .await
        .expect("meta put");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
}

/// Wait until the node's quota view applies `pred` to tenant `t`'s quotas (the view swaps
/// asynchronously after the meta apply).
async fn wait_view(
    s: &TestServer,
    t: &str,
    pred: impl Fn(&ostro_server::p14::quota_view::Quotas) -> bool,
) {
    let flow = Arc::clone(&s.h().p7().flow);
    let t0 = Instant::now();
    loop {
        let view = flow.tenants().view().expect("quota view installed");
        if view.quotas(t).is_some_and(|q| pred(&q)) {
            return;
        }
        assert!(t0.elapsed() < LIMIT, "quota view never saw tenant {t}");
        sleep(Duration::from_millis(5)).await;
    }
}

fn rec(id: &str) -> Tenant {
    Tenant {
        id: id.into(),
        ..Tenant::default()
    }
}

/// Start a `cpu-burn` run of `ms` milliseconds in `tenant`.
async fn start(s: &TestServer, tenant: Option<&str>, ms: u64) -> String {
    let mut body = json!({"input": {"ms": ms}});
    if let Some(t) = tenant {
        body["tenant"] = json!(t);
    }
    let r = s
        .http
        .post(s.url("/v1/workflows/cpu-burn/runs"))
        .json(&body)
        .send()
        .await
        .expect("POST run");
    let st = r.status();
    let b: Value = r.json().await.expect("json body");
    assert_eq!(st, StatusCode::CREATED, "{tenant:?}: {b}");
    b["run_id"].as_str().expect("run_id").to_owned()
}

async fn get(s: &TestServer, run_id: &str) -> Value {
    let r = s
        .http
        .get(s.url(&format!("/v1/runs/{run_id}")))
        .send()
        .await
        .expect("GET run");
    assert_eq!(r.status(), StatusCode::OK);
    r.json().await.expect("json")
}

async fn wait_completed(s: &TestServer, run_id: &str) -> Value {
    let t0 = Instant::now();
    loop {
        let b = get(s, run_id).await;
        match b["status"].as_str() {
            Some("completed") => return b,
            Some("failed" | "cancelled" | "terminated" | "timed_out") => {
                panic!("run {run_id} ended badly (quotas must only delay): {b}")
            }
            _ => {}
        }
        assert!(t0.elapsed() < LIMIT, "run {run_id} not completed: {b}");
        sleep(Duration::from_millis(20)).await;
    }
}

fn run_id(id: &str) -> RunId {
    id.parse().expect("run id")
}

fn granted(s: &TestServer, id: &str) -> bool {
    s.run_record(&run_id(id)).current_attempt.is_some()
}

/// Sample `f` every 2 ms until stopped; returns the maximum seen.
fn sampler<T: Ord + Default + Send + 'static>(
    f: impl Fn() -> T + Send + 'static,
) -> (Arc<AtomicBool>, tokio::task::JoinHandle<T>) {
    let stop = Arc::new(AtomicBool::new(false));
    let st = Arc::clone(&stop);
    let h = tokio::spawn(async move {
        let mut max = T::default();
        while !st.load(Ordering::Acquire) {
            max = max.max(f());
            sleep(Duration::from_millis(2)).await;
        }
        max
    });
    (stop, h)
}

/// Queueing delay of a completed `cpu-burn` run: its first journaled clock read minus its
/// creation time (both the server's clock), ms.
fn queueing_ms(s: &TestServer, id: &str, b: &Value) -> u64 {
    let started = b["result"]["started_ms"].as_u64().expect("started_ms");
    started.saturating_sub(s.run_record(&run_id(id)).created_at_ms)
}

fn p99(mut v: Vec<u64>) -> u64 {
    assert!(!v.is_empty());
    v.sort_unstable();
    let i = (v.len() * 99).div_ceil(100).saturating_sub(1);
    v[i.min(v.len() - 1)]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_concurrency_caps_a_tenants_leased_activations() {
    let mut s = server(4, 8).await;
    deploy(&s, Some("a")).await;
    tenant(
        &s,
        Tenant {
            max_concurrency: Some(2),
            ..rec("a")
        },
    )
    .await;
    wait_view(&s, "a", |q| q.max_concurrency == Some(2)).await;
    let flow = Arc::clone(&s.h().p7().flow);
    let f = Arc::clone(&flow);
    let (stop, h) = sampler(move || f.tenants().running("a"));
    let mut ids = Vec::new();
    for _ in 0..10 {
        ids.push(start(&s, Some("a"), 150).await);
    }
    for id in &ids {
        wait_completed(&s, id).await;
    }
    stop.store(true, Ordering::Release);
    let max = h.await.expect("sampler");
    assert_eq!(max, 2, "sampled leased activations of tenant a");
    assert_eq!(flow.tenants().running("a"), 0, "every charge released");

    // W9: the tenant series exist, without a `tenant` label by default.
    let body = s
        .http
        .get(s.url("/metrics"))
        .send()
        .await
        .expect("GET /metrics")
        .text()
        .await
        .expect("text");
    assert!(body.contains("ostro_tenant_running"), "series missing");
    assert!(
        body.contains("ostro_tenant_blocked_total"),
        "series missing"
    );
    assert!(
        !body.contains("tenant=\""),
        "a tenant label leaked into /metrics with metrics_tenant_labels = false"
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_quota_admits_one_instance_at_a_time() {
    let mut s = server(4, 8).await;
    deploy(&s, Some("m")).await;
    let flow = Arc::clone(&s.h().p7().flow);
    let inst = flow.tenants().instance_memory();
    assert!(inst > 0, "instance memory cap");
    tenant(
        &s,
        Tenant {
            memory_bytes: Some(inst + inst / 2),
            ..rec("m")
        },
    )
    .await;
    wait_view(&s, "m", |q| q.memory_bytes.is_some()).await;
    let (f1, f2) = (Arc::clone(&flow), Arc::clone(&flow));
    let (stop, h) = sampler(move || f1.tenants().running("m"));
    let (stop2, h2) = sampler(move || f2.tenants().memory_reserved("m"));
    let mut ids = Vec::new();
    for _ in 0..6 {
        ids.push(start(&s, Some("m"), 100).await);
    }
    for id in &ids {
        wait_completed(&s, id).await;
    }
    stop.store(true, Ordering::Release);
    stop2.store(true, Ordering::Release);
    assert_eq!(h.await.expect("sampler"), 1, "one activation at a time");
    assert_eq!(h2.await.expect("sampler"), inst, "one instance reserved");
    assert_eq!(flow.tenants().memory_reserved("m"), 0);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuel_quota_spreads_a_fuel_heavy_workload_in_time() {
    let mut s = server(4, 8).await;
    let flow = Arc::clone(&s.h().p7().flow);
    // Calibrate: one run's fuel, charged to a tenant with a huge burst (no refill visible).
    deploy(&s, Some("cal")).await;
    const BURST: u64 = 1_000_000_000_000_000;
    tenant(
        &s,
        Tenant {
            fuel_per_s: Some(1),
            fuel_burst: Some(BURST),
            ..rec("cal")
        },
    )
    .await;
    wait_view(&s, "cal", |q| q.fuel_burst == Some(BURST)).await;
    let id = start(&s, Some("cal"), 20).await;
    wait_completed(&s, &id).await;
    let t0 = Instant::now();
    let fuel = loop {
        // The charge lands when the activation returns (just after the terminal append).
        if let Some(b) = flow.tenants().fuel_balance("cal")
            && b < i128::from(BURST)
        {
            break u64::try_from(i128::from(BURST) - b).expect("positive");
        }
        assert!(t0.elapsed() < Duration::from_secs(10), "no fuel charged");
        sleep(Duration::from_millis(10)).await;
    };
    assert!(fuel > 1_000_000, "a cpu-burn run is fuel-heavy: {fuel}");

    // rate = 2 runs/s, burst = 1 run, one at a time. The fuel is charged after each
    // activation (debt), so the last run's fuel may be owed when it completes: n runs
    // need >= ((n - 1) * fuel - burst) / rate = (n - 2) / 2 s.
    deploy(&s, Some("f")).await;
    tenant(
        &s,
        Tenant {
            fuel_per_s: Some(2 * fuel),
            fuel_burst: Some(fuel),
            max_concurrency: Some(1),
            ..rec("f")
        },
    )
    .await;
    wait_view(&s, "f", |q| q.fuel_per_s.is_some()).await;
    let n = 6u64;
    let t0 = Instant::now();
    let mut ids = Vec::new();
    for _ in 0..n {
        ids.push(start(&s, Some("f"), 20).await);
    }
    for id in &ids {
        wait_completed(&s, id).await;
    }
    let elapsed = t0.elapsed();
    let bound = Duration::from_secs_f64(((n - 1) * fuel - fuel) as f64 / (2 * fuel) as f64);
    eprintln!(
        "fuel: {fuel} per run, {n} runs at rate {} burst {fuel}: {elapsed:?} (bound {bound:?})",
        2 * fuel
    );
    // 10% slack for run-to-run fuel variation (SDK bookkeeping).
    assert!(
        elapsed >= bound.mul_f64(0.9),
        "fuel quota did not spread the work: {elapsed:?} < {bound:?}"
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn suspended_tenant_is_held_then_resumed() {
    let mut s = server(4, 8).await;
    deploy(&s, Some("s")).await;
    // One at a time, so the queued runs are still ready when the suspension lands (the
    // starts happen before it: ingress refusal of suspended tenants is 14-20's).
    let base = Tenant {
        max_concurrency: Some(1),
        ..rec("s")
    };
    tenant(&s, base.clone()).await;
    wait_view(&s, "s", |q| q.max_concurrency == Some(1)).await;
    let long = start(&s, Some("s"), 1_000).await;
    let t0 = Instant::now();
    while !granted(&s, &long) {
        assert!(t0.elapsed() < LIMIT, "long run never granted");
        sleep(Duration::from_millis(10)).await;
    }
    let mut ids = Vec::new();
    for _ in 0..5 {
        ids.push(start(&s, Some("s"), 10).await);
    }
    tenant(
        &s,
        Tenant {
            suspended: true,
            ..base.clone()
        },
    )
    .await;
    wait_view(&s, "s", |q| q.suspended).await;
    // Precondition: the suspension landed while the long run still held the only slot.
    assert!(
        ids.iter().all(|id| !granted(&s, id)),
        "a queued run was dispatched before the suspension landed"
    );
    wait_completed(&s, &long).await;
    // Held for 2 s: nothing dispatched, nothing failed.
    sleep(Duration::from_secs(2)).await;
    for id in &ids {
        assert!(
            !granted(&s, id),
            "suspended tenant's run {id} was dispatched"
        );
        let b = get(&s, id).await;
        assert!(
            !matches!(
                b["status"].as_str(),
                Some("completed" | "failed" | "cancelled" | "terminated" | "timed_out")
            ),
            "{b}"
        );
    }
    tenant(&s, base).await;
    for id in &ids {
        wait_completed(&s, id).await;
    }
    s.shutdown().await;
}

/// Jain's fairness index of `x`.
fn jain(x: &[f64]) -> f64 {
    let sum: f64 = x.iter().sum();
    let sq: f64 = x.iter().map(|v| v * v).sum();
    sum * sum / (x.len() as f64 * sq)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn weights_one_two_four_share_a_saturated_node() {
    // One partition, one executor slot: grants are strictly sequential, so job order is
    // the dispatcher's selection order.
    let mut s = server(1, 1).await;
    deploy(&s, None).await;
    let weights = [("a", 1u32), ("b", 2), ("c", 4)];
    for (t, w) in weights {
        deploy(&s, Some(t)).await;
        tenant(
            &s,
            Tenant {
                weight: Some(w),
                ..rec(t)
            },
        )
        .await;
        wait_view(&s, t, move |q| q.weight == Some(w)).await;
    }
    // A blocker holds the only slot (stalled at its first append) while the backlogs
    // build, so every tenant is saturated when selection starts.
    let gate = Arc::new(Semaphore::new(0));
    *s.hooks.pause_before_append.lock().expect("lock") = Some(Arc::clone(&gate));
    let blocker = start(&s, None, 1).await;
    let t0 = Instant::now();
    while !granted(&s, &blocker) {
        assert!(t0.elapsed() < LIMIT, "blocker never granted");
        sleep(Duration::from_millis(10)).await;
    }
    let per = 42;
    let mut tenant_of: BTreeMap<RunId, &str> = BTreeMap::new();
    let mut ids = Vec::new();
    for _ in 0..per {
        for (t, _) in weights {
            let id = start(&s, Some(t), 1).await;
            tenant_of.insert(run_id(&id), t);
            ids.push(id);
        }
    }
    gate.add_permits(1 << 20);
    wait_completed(&s, &blocker).await;
    for id in &ids {
        wait_completed(&s, id).await;
    }
    let order: Vec<&str> = s
        .hooks
        .jobs()
        .iter()
        .filter_map(|j| tenant_of.get(&j.run).copied())
        .collect();
    // The first 70 picks: every tenant is still backlogged (c needs 40 of its 42).
    let window = &order[..70];
    let total: u32 = weights.iter().map(|(_, w)| w).sum();
    let mut normalised = Vec::new();
    for (t, w) in weights {
        let n = window.iter().filter(|x| **x == t).count() as f64;
        let share = n / window.len() as f64;
        let want = f64::from(w) / f64::from(total);
        eprintln!("share {t} (weight {w}): {share:.3} (want {want:.3})");
        assert!(
            (share - want).abs() <= 0.15 * want,
            "tenant {t} share {share:.3} not within 15% of {want:.3}: {window:?}"
        );
        normalised.push(n / f64::from(w));
    }
    let j = jain(&normalised);
    eprintln!("jain index over weight-normalised throughput: {j:.4}");
    assert!(j >= 0.9, "Jain's index {j} < 0.9");
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn noisy_neighbour_does_not_raise_queueing_p99() {
    let mut s = server(1, 4).await;
    for t in ["a", "b"] {
        deploy(&s, Some(t)).await;
    }
    // a is capped below the node's slots; b has no limit.
    tenant(
        &s,
        Tenant {
            max_concurrency: Some(2),
            ..rec("a")
        },
    )
    .await;
    tenant(&s, rec("b")).await;
    wait_view(&s, "a", |q| q.max_concurrency == Some(2)).await;
    // b: bursts of 2 runs every 250 ms. `probe` runs after the last submission.
    async fn b_load(s: &TestServer, probe: impl FnOnce()) -> Vec<u64> {
        let mut ids = Vec::new();
        for _ in 0..15 {
            for _ in 0..2 {
                ids.push(start(s, Some("b"), 20).await);
            }
            sleep(Duration::from_millis(250)).await;
        }
        probe();
        let mut q = Vec::new();
        for id in &ids {
            let b = wait_completed(s, id).await;
            q.push(queueing_ms(s, id, &b));
        }
        q
    }
    let solo = p99(b_load(&s, || {}).await);
    // a at 10x b's load in runs (300 heavier runs, capped at 2: a backlog that outlasts
    // b's load), submitted right before b's load.
    const A_MS: u64 = 300;
    let mut a_ids = Vec::new();
    for _ in 0..300 {
        a_ids.push(start(&s, Some("a"), A_MS).await);
    }
    let mut a_left = 0;
    let noisy = p99(b_load(&s, || {
        a_left = a_ids.iter().filter(|id| !granted(&s, id)).count();
    })
    .await);
    assert!(a_left > 0, "a's backlog drained before b's load ended");
    for id in &a_ids {
        wait_completed(&s, id).await;
    }
    // Floor: below 25 ms the solo baseline is dispatch noise (ms-scale timer and
    // scheduling jitter), not queueing.
    const FLOOR_MS: u64 = 25;
    eprintln!(
        "queueing p99 of b: solo {solo} ms, with a at 10x {noisy} ms (a backlog left when b \
         finished: {a_left})"
    );
    assert!(
        noisy <= 2 * solo.max(FLOOR_MS),
        "noisy neighbour raised b's queueing p99 from {solo} ms to {noisy} ms"
    );
    s.shutdown().await;
}
