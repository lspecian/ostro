//! 13-19 Task 1 (CLUS-06 / CLUS-07, D-P13-7 / D-P13-26, tier C): an emulated rolling
//! upgrade under load and chaos-proxy faults. `#[ignore]`:
//!
//! ```text
//! cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
//! OSTRO_UPGRADE_ITERS=3 cargo test --release -p ostro-repl-conformance --features failpoints \
//!   --locked --test process_rolling_upgrade -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Per iteration (`OSTRO_UPGRADE_ITERS`, default 1, from a fresh cluster each time): three
//! static `ostro server` processes (every link through the chaos proxy) start with
//! `OSTRO_TEST_LEVEL_MAX=1`, so the cluster is finalized at level 1. Before the upgrade the
//! gated features are refused (reassign and a submitting migrate answer 409
//! `feature_not_enabled`). The continuous mixed workload runs throughout (3-step runs, hot
//! waits, signal waits, child runs, one availability probe per partition), the proxy delays
//! every link by 20 ms, and one short partition between two nodes is cut and healed per iteration.
//! Each node in turn gets SIGTERM (controlled shutdown, 13-11 `before_shutdown`: its
//! partitions are handed off) and is restarted without the variable; finalized must stay 1
//! until the last node is restarted and then reach 2 on every node. After each restart the
//! first activations through the restarted node are timed (start → completed of one run per
//! partition; the AOT pre-warm counter `ostro_cluster_prewarm_modules_total` of the node is
//! recorded) and every partition's unavailability over the restart window (the longest gap
//! between probe acks) is measured. After finalization a reassignment (p0 shrinks to two
//! replicas and grows back) and a migrate (`replay_demo` v1 → v3: the sleeping runs are
//! repinned, their module becomes v3, and they complete) succeed. A level-2 module upload
//! cannot be exercised here (no level-2 WIT line in the shipped binary; tier B covers the
//! upload gate, 13-14 / 13-18).
//!
//! Oracles: 0 lost acknowledged runs, every run `completed` exactly once (no failed,
//! cancelled, terminated or drained run; no duplicate journal entry; one run id per key),
//! standby identity (every in-sync replica's state equal), `no_gated_before_finalize_from(1)`,
//! `no_reject_continue` over the replayed apply outcomes, and the Phase 12 final checks.
//! Report: `$OSTRO_P13_REPORT_DIR/process_rolling_upgrade/report.txt`.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::time::Duration;

use ostro_repl_conformance::harness::ClusterHarness;
use ostro_repl_conformance::harness::inproc::guest_wasm;
use ostro_repl_conformance::harness::p13::P13Harness;
use ostro_repl_conformance::harness::process::ProcCluster;
use ostro_repl_conformance::scenarios::process::skip_reason;
use ostro_repl_conformance::scenarios::process::tools::{self, published, published_all};
use ostro_repl_conformance::scenarios::process_p13::{
    HEAL_T, Load, ScenarioFailure, WORKLOAD_GUESTS, deploy_guests, dist, final_checks, finalized,
    get, host_line, outcomes, post, prewarm_counts, reason, standby_identity, start_static,
    stays_finalized, time_one_run, upgrade_oracles, wait_finalized, wait_healthy, write_report,
};
use serde_json::{Value, json};
use tokio::time::Instant;

const TEST: &str = "process_rolling_upgrade";
const RUN_T: Duration = Duration::from_secs(120);
const FINALIZE_T: Duration = Duration::from_secs(60);
const MOVE_T: Duration = Duration::from_secs(120);
const MIGRATE_WF: &str = "replay_demo";
/// `next_seq` once a `replay_demo` v1 run journaled RunStarted, steps a and b, its timer.
const SLEEPING_EVENTS: usize = 4;

fn f(oracle: &'static str, detail: impl Into<String>) -> ScenarioFailure {
    ScenarioFailure {
        oracle,
        detail: detail.into(),
    }
}

fn sha_hex(guest: &str) -> String {
    use sha2::{Digest, Sha256};
    let wasm = guest_wasm(guest).unwrap_or_else(|e| panic!("harness: guest {guest}: {e}"));
    Sha256::digest(&wasm)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Measurements over every iteration.
#[derive(Default)]
struct Measured {
    stop: Vec<Duration>,
    start: Vec<Duration>,
    unavailable: Vec<Duration>,
    first_activation: Vec<Duration>,
    baseline: Vec<Duration>,
    second_run: Vec<Duration>,
    finalize: Vec<Duration>,
    acked: usize,
}

/// A gated call must be refused before finalization: 409 `feature_not_enabled`.
fn expect_gated(what: &str, r: Option<(u16, Value)>) -> Result<(), ScenarioFailure> {
    match r {
        Some((409, v)) if reason(&v) == "feature_not_enabled" => Ok(()),
        other => Err(f(
            "p13_upgrade",
            format!("{what} before finalization: want 409 feature_not_enabled, got {other:?}"),
        )),
    }
}

/// `POST /v1/cluster/reassign` for p0 to `replicas` (retried on 503 / 409 ISR races), then
/// wait until p0 has exactly that replica set with a full ISR.
async fn reassign_p0(c: &ProcCluster, replicas: &[u64]) -> Result<Duration, ScenarioFailure> {
    let t0 = Instant::now(); // clock-exempt: measurement
    let body = json!({ "partition": 0, "replicas": replicas });
    loop {
        let via = tools::live(c)[0];
        match post(c, via, "/v1/cluster/reassign", &body).await {
            Some((200, _)) => break,
            Some((st, v)) if (st == 503 || st == 409) && t0.elapsed() < MOVE_T => {
                println!("reassign p0 -> {replicas:?}: retrying after {st} {v}");
            }
            other => {
                return Err(f(
                    "reassign",
                    format!("reassign p0 -> {replicas:?} after finalization: {other:?}"),
                ));
            }
        }
        tools::pause(Duration::from_millis(500)).await;
    }
    let want: BTreeSet<u64> = replicas.iter().copied().collect();
    loop {
        let a = published(c, 0);
        let got: BTreeSet<u64> = a.replicas.iter().copied().collect();
        if got == want && a.isr.len() == want.len() && want.contains(&a.leader) {
            return Ok(t0.elapsed());
        }
        if t0.elapsed() > MOVE_T {
            return Err(f(
                "reassign",
                format!(
                    "p0 not at {replicas:?} within {MOVE_T:?}: replicas {:?} isr {:?} leader {}",
                    a.replicas, a.isr, a.leader
                ),
            ));
        }
        tools::pause(Duration::from_millis(100)).await;
    }
}

/// Start a sleeping `replay_demo` run (`sleep_s`) through any node; wait until it sleeps.
async fn sleeping_run(c: &ProcCluster, i: usize, sleep_s: u64) -> Result<String, ScenarioFailure> {
    let key = format!("mig-{}-{}-{i}", c.cluster_id(), c.r());
    let body = json!({ "input": { "sleep_s": sleep_s }, "idempotency_key": key });
    let t0 = Instant::now(); // clock-exempt: test timeout
    let run = loop {
        let via = tools::live(c)[i % tools::live(c).len()];
        if let Some((st, v)) =
            post(c, via, &format!("/v1/workflows/{MIGRATE_WF}/runs"), &body).await
            && (200..300).contains(&st)
            && let Some(id) = v["run_id"].as_str()
        {
            break id.to_owned();
        }
        if t0.elapsed() > RUN_T {
            return Err(f(
                "migrate",
                format!("replay_demo start {key} not acknowledged"),
            ));
        }
        tools::pause(Duration::from_millis(200)).await;
    };
    loop {
        let via = tools::live(c)[0];
        let st = get(c, via, &format!("/v1/runs/{run}")).await;
        let hist = get(
            c,
            via,
            &format!("/v1/runs/{run}/history?from_seq=0&limit=100"),
        )
        .await;
        let events = hist
            .as_ref()
            .and_then(|(_, v)| v["events"].as_array().map(Vec::len))
            .unwrap_or(0);
        if matches!(&st, Some((200, v)) if v["status"] == "scheduled") && events >= SLEEPING_EVENTS
        {
            return Ok(run);
        }
        if t0.elapsed() > RUN_T {
            return Err(f(
                "migrate",
                format!("run {run} not sleeping within {RUN_T:?}: {st:?}, {events} events"),
            ));
        }
        tools::pause(Duration::from_millis(100)).await;
    }
}

/// The migrate of the sleeping runs after finalization (v1 → v3).
async fn migrate_after(c: &ProcCluster, out: &mut String) -> Result<(), ScenarioFailure> {
    let v3 = sha_hex("p10-replay-v3");
    let mut runs = Vec::new();
    for i in 0..2 {
        runs.push(sleeping_run(c, i, 20).await?);
    }
    let body = json!({ "workflow": MIGRATE_WF, "from": { "version": 1 }, "to_version": 3, "dry_run": false });
    let t0 = Instant::now(); // clock-exempt: test timeout
    let rep = loop {
        let via = tools::live(c)[0];
        match post(c, via, "/v1/runs/migrate", &body).await {
            Some((200, v)) => break v,
            Some((503, v)) if t0.elapsed() < RUN_T => println!("migrate: retrying after 503 {v}"),
            other => {
                return Err(f(
                    "migrate",
                    format!("migrate after finalization: {other:?}"),
                ));
            }
        }
        tools::pause(Duration::from_millis(500)).await;
    };
    let repinned: BTreeSet<String> = rep["repinned"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| {
                    x.as_str()
                        .map(str::to_owned)
                        .or_else(|| x["run_id"].as_str().map(str::to_owned))
                })
                .collect()
        })
        .unwrap_or_default();
    for r in &runs {
        if !repinned.contains(r) {
            return Err(f(
                "migrate",
                format!("sleeping run {r} not repinned: report {rep}"),
            ));
        }
    }
    for r in &runs {
        let t = Instant::now(); // clock-exempt: test timeout
        loop {
            let via = tools::live(c)[0];
            if let Some((200, v)) = get(c, via, &format!("/v1/runs/{r}")).await {
                if v["module_sha256"] != json!(v3) {
                    return Err(f(
                        "migrate",
                        format!(
                            "run {r}: module {} after the migrate, want v3 {v3}",
                            v["module_sha256"]
                        ),
                    ));
                }
                if v["status"] == "completed" {
                    break;
                }
                if crate_terminal(&v) {
                    return Err(f(
                        "migrate",
                        format!("repinned run {r} ended {}", v["status"]),
                    ));
                }
            }
            if t.elapsed() > RUN_T {
                return Err(f(
                    "migrate",
                    format!("repinned run {r} not completed within {RUN_T:?}"),
                ));
            }
            tools::pause(Duration::from_millis(200)).await;
        }
    }
    let _ = writeln!(
        out,
        "  migrate replay_demo v1 -> v3 after finalization: {} runs repinned (report: repinned {}), module v3, completed",
        runs.len(),
        repinned.len()
    );
    Ok(())
}

fn crate_terminal(v: &Value) -> bool {
    v["status"]
        .as_str()
        .is_some_and(|s| ostro_repl_conformance::harness::inproc::TERMINAL.contains(&s))
}

async fn iteration(
    c: &mut ProcCluster,
    iter: usize,
    m: &mut Measured,
    out: &mut String,
) -> Result<(), ScenarioFailure> {
    let ids = c.node_ids();
    deploy_guests(c, &WORKLOAD_GUESTS).await?;
    deploy_guests(c, &["p10-replay-v1", "p10-replay-v2", "p10-replay-v3"]).await?;
    let l = finalized(c, &ids).await;
    if l.len() != ids.len() || l.values().any(|v| *v != 1) {
        return Err(f(
            "finalized",
            format!("level-1 cluster not finalized at 1: {l:?}"),
        ));
    }
    // Gated features are refused before finalization.
    let a = published(c, 0);
    let shrink: Vec<u64> = std::iter::once(a.leader)
        .chain(
            a.replicas
                .iter()
                .copied()
                .filter(|r| *r != a.leader)
                .take(1),
        )
        .collect();
    expect_gated(
        "reassign",
        post(
            c,
            1,
            "/v1/cluster/reassign",
            &json!({ "partition": 0, "replicas": shrink }),
        )
        .await,
    )?;
    expect_gated(
        "migrate",
        post(
            c,
            2,
            "/v1/runs/migrate",
            &json!({ "workflow": MIGRATE_WF, "from": { "version": 1 }, "to_version": 3, "dry_run": false }),
        )
        .await,
    )?;
    let _ = writeln!(
        out,
        "iteration {iter}: finalized 1 on {ids:?}; reassign + migrate refused 409 feature_not_enabled"
    );

    let load = Load::start(c, &[], true);
    load.wait_acked(10, RUN_T).await?;
    for l in c.proxy().links() {
        c.proxy().delay(l.from, l.to, 20);
    }
    // Baseline: the same timing before any restart, under the same 20 ms link delay
    // (every module already activated).
    let mut base = Vec::new();
    for n in &ids {
        for p in 0..c.spec().partitions {
            if let Some(d) = time_one_run(c, *n, p, &format!("base{iter}-{n}")).await {
                base.push(d);
            }
        }
    }
    let _ = writeln!(
        out,
        "  baseline (before any restart, same link delay, through every node, one run per partition): {}",
        dist(&mut base.clone())
    );
    m.baseline.extend(base);
    for (i, n) in ids.iter().copied().enumerate() {
        if i == 1 {
            // One short partition per iteration (between the two nodes not upgraded next).
            let others: Vec<u64> = ids.iter().copied().filter(|x| *x != n).collect();
            c.partition(others[0], others[1]);
            tools::pause(Duration::from_millis(1500)).await;
            c.proxy().heal(others[0], others[1]);
            c.proxy().heal(others[1], others[0]);
            c.proxy().delay(others[0], others[1], 20);
            c.proxy().delay(others[1], others[0], 20);
            wait_healthy(c, &[], HEAL_T, "the short partition").await?;
            let _ = writeln!(
                out,
                "  short partition n{}|n{} held 1.5 s, healed",
                others[0], others[1]
            );
        }
        load.refresh(c, &[n]);
        let t_stop = Instant::now(); // clock-exempt: measurement
        c.upgrade(n, None)
            .await
            .map_err(|e| f("harness", format!("upgrade node {n}: {e}")))?;
        let total = t_stop.elapsed();
        let start = c.timings().last_start;
        let stop = total.saturating_sub(start);
        load.refresh(c, &[]);
        let healthy_after = wait_healthy(c, &[], HEAL_T, &format!("restart of node {n}")).await?;
        let t_end = Instant::now(); // clock-exempt: measurement
        let mut gaps = Vec::new();
        for p in 0..c.spec().partitions {
            gaps.push(load.unavailability(p, t_stop, t_end));
        }
        let pre = prewarm_counts(c, n).await;
        let mut acts = Vec::new();
        for p in 0..c.spec().partitions {
            match time_one_run(c, n, p, &format!("act{iter}-{n}")).await {
                Some(d) => acts.push(d),
                None => {
                    return Err(f(
                        "availability",
                        format!("first run on p{p} through node {n} not completed"),
                    ));
                }
            }
        }
        // The same timing once more right after (one-time cost vs a persistent one).
        let mut again = Vec::new();
        for p in 0..c.spec().partitions {
            if let Some(d) = time_one_run(c, n, p, &format!("again{iter}-{n}")).await {
                again.push(d);
            }
        }
        let leads: Vec<u16> = published_all(c)
            .iter()
            .filter(|a| a.leader == n)
            .map(|a| a.partition)
            .collect();
        let _ = writeln!(
            out,
            "  node {n}: SIGTERM -> exit {stop:?}, restart to ready {start:?}, healthy +{healthy_after:?}; partition unavailability {gaps:?}; pre-warm {pre:?}; first activations via node {n} {acts:?}, second runs {again:?}; leads {leads:?} after restart"
        );
        m.stop.push(stop);
        m.start.push(start);
        m.unavailable.extend(gaps);
        m.first_activation.extend(acts);
        m.second_run.extend(again);
        if i + 1 < ids.len() {
            stays_finalized(c, &ids, 1, Duration::from_secs(1)).await?;
        }
        load.wait_acked(5, RUN_T).await?;
    }
    let fin = wait_finalized(c, &ids, 2, FINALIZE_T).await?;
    m.finalize.push(fin);
    let _ = writeln!(
        out,
        "  finalized 2 on every node {fin:?} after the last restart"
    );
    c.proxy().heal_all();

    // Gated features after finalization.
    let a = published(c, 0);
    let all: Vec<u64> = a.replicas.clone();
    let shrink: Vec<u64> = std::iter::once(a.leader)
        .chain(
            a.replicas
                .iter()
                .copied()
                .filter(|r| *r != a.leader)
                .take(1),
        )
        .collect();
    let d1 = reassign_p0(c, &shrink).await?;
    let d2 = reassign_p0(c, &all).await?;
    let _ = writeln!(
        out,
        "  reassign p0 {all:?} -> {shrink:?} ({d1:?}) -> {all:?} ({d2:?})"
    );
    migrate_after(c, out).await?;

    load.wait_acked(5, RUN_T).await?;
    let res = load.finish(RUN_T).await?;
    let _ = writeln!(out, "  {}", res.line());
    res.verdict()?;
    m.acked += res.runs.len();
    let oc = outcomes(c, &res.runs).await;
    let _ = writeln!(out, "  outcomes: {}", oc.line());
    oc.verdict()?;
    standby_identity(c, Duration::from_secs(30)).await?;
    let _ = writeln!(out, "  standby identity: pass");
    let uo = upgrade_oracles(c, &ids).await?;
    let _ = writeln!(out, "  upgrade oracles: {uo}");
    let fc = final_checks(c, &ids, &res.acks()).await?;
    let _ = writeln!(out, "  final checks: {fc}");
    if let Ok(v) = c.cluster_status(ids[0]).await {
        let _ = writeln!(
            out,
            "  status: finalized {} level_max {}",
            v["finalized"], v["level_max"]
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "tier C: needs the failpoint binary (cargo build -p ostro --features failpoints --target-dir target/failpoints)"]
async fn emulated_rolling_upgrade_under_load_and_faults() {
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    let iters: usize = std::env::var("OSTRO_UPGRADE_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
        .max(1);
    let t0 = Instant::now(); // clock-exempt: measurement
    let mut out = String::new();
    let _ = writeln!(
        out,
        "process_rolling_upgrade (13-19, tier C): OSTRO_UPGRADE_ITERS={iters}, 3 static processes at OSTRO_TEST_LEVEL_MAX=1, RF 3, min_insync 2, 20 ms proxy delay on every link\n{}",
        host_line()
    );
    let mut m = Measured::default();
    let mut result: Result<(), ScenarioFailure> = Ok(());
    for iter in 1..=iters {
        let mut c = match start_static(1930 + iter as u64, Some(1)).await {
            Ok(c) => c,
            Err(e) => {
                result = Err(e);
                break;
            }
        };
        let r = iteration(&mut c, iter, &mut m, &mut out).await;
        if r.is_err() {
            for n in c.node_ids() {
                println!(
                    "--- iteration {iter} node {n} log tail ---\n{}",
                    c.log_tail(n, 40)
                );
            }
        }
        c.stop().await;
        if let Err(e) = r {
            let _ = writeln!(out, "iteration {iter}: FAIL {e}");
            result = Err(e);
            break;
        }
        let _ = writeln!(out, "iteration {iter}: PASS");
    }
    let _ = writeln!(
        out,
        "controlled shutdown (SIGTERM -> exit): {}",
        dist(&mut m.stop)
    );
    let _ = writeln!(out, "restart (spawn -> ready line): {}", dist(&mut m.start));
    let _ = writeln!(
        out,
        "handoff unavailability per partition per restart (longest probe-ack gap over the restart window): {}",
        dist(&mut m.unavailable)
    );
    let _ = writeln!(
        out,
        "first activation after an upgraded restart (start -> completed through the restarted node, one run per partition): {}",
        dist(&mut m.first_activation)
    );
    let _ = writeln!(
        out,
        "the second run per partition through the restarted node, right after: {}",
        dist(&mut m.second_run)
    );
    let _ = writeln!(
        out,
        "the same timing before any restart under the same link delay (baseline, every node): {}",
        dist(&mut m.baseline)
    );
    let _ = writeln!(
        out,
        "finalization after the last restart: {}",
        dist(&mut m.finalize)
    );
    let _ = writeln!(out, "acknowledged runs (all iterations): {}", m.acked);
    let _ = writeln!(
        out,
        "result: {}; elapsed {:?}",
        match &result {
            Ok(()) => "PASS".to_owned(),
            Err(e) => format!("FAIL {e}"),
        },
        t0.elapsed()
    );
    write_report(TEST, &out);
    result.expect("process_rolling_upgrade");
}
