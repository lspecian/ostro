//! 12-17: the multi-process cluster crash-campaign smoke. `#[ignore]`;
//!
//! ```text
//! OSTRO_CAMPAIGN_FAULTS=500 cargo test --release -p ostro-repl-conformance --test cluster_campaign \
//!   -- --ignored --nocapture --test-threads=1
//! ```
//!
//! The cluster lives on `/dev/shm` unless `OSTRO_PROC_ROOT` names another dir (a 10,000-fault
//! run needs ~13 KB per acknowledged run: put it on disk).
//!
//! Three `ostro server` processes (RF 3, min_insync 2, the tier-C test timings) under a
//! continuous workload (4 closed-loop clients, 100 ms apart, starting 3-step `trivial-steps`
//! runs through the API, following leader hints, with an ack log: the 05-19 pattern). A seeded schedule injects
//! `OSTRO_CAMPAIGN_FAULTS` faults (default 500), one at a time, each followed by recovery and
//! a healthy cluster (meta leader, every partition led, full ISRs):
//!
//! - partition leader `kill -9` (not the controller), follower `kill -9`, controller
//!   (`meta` leader) `kill -9`, each restarted after a random 0.3-1.5 s;
//! - a chaos-proxy partition between two nodes (RST) held 0.5-2 s, then healed;
//! - follower WAL failure + kill: the follower restarts with `wal::write=return` (its WAL
//!   writer fails every write before writing), runs 0.5-1.5 s under load, is `kill -9`'d and
//!   restarts clean.
//!
//! After every batch of 10 faults: acknowledged ⇒ on disk on `min_insync` nodes (a trimmed
//! copy may evidence a run by its state store: D-P12-27, applied here by D-P12-28). At the end:
//! every acknowledged run completes (0 lost acked runs), no run's journal holds a duplicate
//! entry (history seqs strictly increasing, exactly 3 `step_completed` with distinct names,
//! read from the leader after convergence), replica identity below the HW, the LA rule and
//! the ISR-only election audit. Writes `target/campaign-p12/report.txt` with fault counts by
//! kind.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use ostro_repl_conformance::harness::inproc::Ack;
use ostro_repl_conformance::harness::process::{ProcCluster, ProcOptions};
use ostro_repl_conformance::harness::{ApiClient, ClusterHarness, ClusterSpec, DataDirView};
use ostro_repl_conformance::oracle::disk::acked_on_disk_copies;
use ostro_repl_conformance::scenarios::process::tools::{
    HintClient, Start, deploy, final_checks, meta_leader, published, published_all,
};
use ostro_repl_conformance::scenarios::process::{skip_reason, with_failpoint};
use ostro_types::PartitionCount;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::json;
use tokio::sync::Mutex;

const WF: &str = "trivial-steps";
const STEPS: usize = 3;
const CLIENTS: usize = 4;
/// Pause between a client's acknowledged start and its next one (bounds the ack log).
const PACE: Duration = Duration::from_millis(100);
const BATCH: usize = 10;
const HEAL_LIMIT: Duration = Duration::from_secs(90);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Fault {
    LeaderKill,
    FollowerKill,
    ControllerKill,
    ProxyPartition,
    WalFailKill,
}

const KINDS: [Fault; 5] = [
    Fault::LeaderKill,
    Fault::FollowerKill,
    Fault::ControllerKill,
    Fault::ProxyPartition,
    Fault::WalFailKill,
];

fn report_dir() -> PathBuf {
    let d = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/campaign-p12");
    std::fs::create_dir_all(&d).expect("report dir");
    d
}

fn routed_key(salt: &str, p: u16, partitions: u16, i: &mut u64) -> String {
    let count = PartitionCount::new(partitions).expect("partitions > 0");
    loop {
        *i += 1;
        let key = format!("camp-{salt}-{i}");
        if ostro_types::routing::partition_for_key(WF, key.as_bytes(), count).0 == p {
            return key;
        }
    }
}

async fn healthy(c: &ProcCluster) -> bool {
    if c.node_ids().iter().any(|n| c.pid(*n).is_none()) {
        return false;
    }
    if published_all(c)
        .iter()
        .any(|a| a.leader == 0 || a.isr.len() != a.replicas.len())
    {
        return false;
    }
    meta_leader(c).await.is_some()
}

async fn wait_healthy(c: &ProcCluster, at: &str) {
    let t0 = Instant::now();
    while t0.elapsed() < HEAL_LIMIT {
        if healthy(c).await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let all: Vec<String> = published_all(c)
        .iter()
        .map(|a| format!("p{} leader {} isr {:?}", a.partition, a.leader, a.isr))
        .collect();
    for n in c.node_ids() {
        println!("--- node {n} log tail ---\n{}", c.log_tail(n, 40));
    }
    panic!("availability: cluster not healthy within {HEAL_LIMIT:?} after {at}: {all:?}");
}

fn spawn_workload(
    c: &ProcCluster,
    acks: Arc<Mutex<Vec<Ack>>>,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinSet<()> {
    let partitions = c.spec().partitions;
    let mut set = tokio::task::JoinSet::new();
    for w in 0..CLIENTS {
        let p = (w % usize::from(partitions)) as u16;
        let cl = HintClient::new(c, 1);
        let (acks, stop) = (Arc::clone(&acks), Arc::clone(&stop));
        let salt = format!("{}-{}-{w}", c.cluster_id(), c.r());
        set.spawn(async move {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let key = routed_key(&salt, p, partitions, &mut i);
                let t = Instant::now();
                while !stop.load(Ordering::Relaxed) && t.elapsed() < Duration::from_secs(20) {
                    match cl
                        .start(WF, json!({ "n": STEPS }), &key, Duration::from_secs(3))
                        .await
                    {
                        Start::Acked(a) => {
                            acks.lock().await.push(a);
                            tokio::time::sleep(PACE).await;
                            break;
                        }
                        _ => tokio::time::sleep(Duration::from_millis(50)).await,
                    }
                }
            }
        });
    }
    set
}

/// A node other than `avoid` (the controller), preferring `prefer`.
fn non_controller(c: &ProcCluster, prefer: &[u64], avoid: u64, rng: &mut StdRng) -> u64 {
    let pool: Vec<u64> = prefer.iter().copied().filter(|n| *n != avoid).collect();
    if !pool.is_empty() {
        return pool[rng.random_range(0..pool.len())];
    }
    let rest: Vec<u64> = c.node_ids().into_iter().filter(|n| *n != avoid).collect();
    rest[rng.random_range(0..rest.len())]
}

async fn hold(rng: &mut StdRng, lo_ms: u64, hi_ms: u64) {
    tokio::time::sleep(Duration::from_millis(rng.random_range(lo_ms..=hi_ms))).await;
}

async fn inject(c: &mut ProcCluster, kind: Fault, rng: &mut StdRng) -> String {
    let meta = meta_leader(c).await.unwrap_or(0);
    let parts = published_all(c);
    let p = rng.random_range(0..parts.len());
    let a = &parts[p];
    match kind {
        Fault::LeaderKill => {
            let leaders: Vec<u64> = parts.iter().map(|a| a.leader).collect();
            let t = non_controller(c, &leaders, meta, rng);
            c.kill(t).await;
            hold(rng, 300, 1500).await;
            c.restart(t).await.expect("restart");
            format!("leader kill n{t}")
        }
        Fault::FollowerKill => {
            let followers: Vec<u64> = a
                .replicas
                .iter()
                .copied()
                .filter(|r| *r != a.leader)
                .collect();
            let t = non_controller(c, &followers, meta, rng);
            c.kill(t).await;
            hold(rng, 300, 1500).await;
            c.restart(t).await.expect("restart");
            format!("follower kill n{t} (p{p})")
        }
        Fault::ControllerKill => {
            let t = if meta != 0 { meta } else { 1 };
            c.kill(t).await;
            hold(rng, 300, 1500).await;
            c.restart(t).await.expect("restart");
            format!("controller kill n{t}")
        }
        Fault::ProxyPartition => {
            let ids = c.node_ids();
            let x = ids[rng.random_range(0..ids.len())];
            let others: Vec<u64> = ids.iter().copied().filter(|n| *n != x).collect();
            let y = others[rng.random_range(0..others.len())];
            c.partition(x, y);
            hold(rng, 500, 2000).await;
            c.proxy().heal_all();
            format!("proxy partition n{x}|n{y}")
        }
        Fault::WalFailKill => {
            let followers: Vec<u64> = a
                .replicas
                .iter()
                .copied()
                .filter(|r| *r != a.leader)
                .collect();
            let t = non_controller(c, &followers, meta, rng);
            let base = c.failpoints(t).map(str::to_owned);
            c.set_failpoints(
                t,
                Some(&with_failpoint(base.as_deref(), "wal::write=return")),
            );
            c.restart(t).await.expect("restart with failing WAL");
            hold(rng, 500, 1500).await;
            c.kill(t).await;
            c.set_failpoints(t, base.as_deref());
            c.restart(t).await.expect("restart clean");
            format!("follower WAL failure + kill n{t} (p{p})")
        }
    }
}

/// Acknowledged ⇒ on disk on `min_insync` nodes. A copy whose WAL was trimmed (follower
/// snapshot install, retention) may evidence a run by its checkpointed state store, probed
/// on a copy of the running node's store (trimmed-log rule D-P12-27, applied to the
/// campaign by D-P12-28, user-approved 2026-11-24); an untrimmed copy needs the WAL record.
fn disk_check(c: &ProcCluster, acks: &[Ack]) -> Result<(), String> {
    let views: Vec<DataDirView> = c.node_ids().iter().map(|n| c.node_disk(*n)).collect();
    let count = PartitionCount::new(c.spec().partitions).expect("partitions > 0");
    let r = acked_on_disk_copies(&views, acks, c.spec().min_insync, count);
    if r.is_ok() {
        Ok(())
    } else {
        Err(r.to_string())
    }
}

/// Duplicate journal entries of `run` on its partition's leader (`None` = clean).
async fn duplicates(api: &ApiClient, a: &Ack) -> Result<Option<String>, String> {
    let mut seqs = Vec::new();
    let mut steps = Vec::new();
    let mut from = 0u64;
    loop {
        let (st, v) = api
            .get_json(&format!(
                "/v1/runs/{}/history?from_seq={from}&limit=1000",
                a.run_id
            ))
            .await
            .map_err(|e| e.to_string())?;
        if !st.is_success() {
            return Err(format!("history of {}: {st} {v}", a.run_id));
        }
        for e in v["events"].as_array().cloned().unwrap_or_default() {
            seqs.push(e["seq"].as_u64().unwrap_or(0));
            if e["kind"] == "step_completed" {
                let name = e["details"]["name"]
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| e["summary"].to_string());
                steps.push(name);
            }
        }
        match v["next_from_seq"].as_u64() {
            Some(n) if n > from => from = n,
            _ => break,
        }
    }
    if seqs.windows(2).any(|w| w[0] >= w[1]) {
        return Ok(Some(format!(
            "run {}: journal seqs not strictly increasing: {seqs:?}",
            a.run_id
        )));
    }
    let distinct: BTreeSet<&String> = steps.iter().collect();
    if steps.len() != STEPS || distinct.len() != STEPS {
        return Ok(Some(format!(
            "run {}: {} step_completed entries ({} distinct), expected {STEPS}: {steps:?}",
            a.run_id,
            steps.len(),
            distinct.len()
        )));
    }
    Ok(None)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "tier C campaign: OSTRO_CAMPAIGN_FAULTS faults (default 500, ~40 min)"]
async fn cluster_crash_campaign() {
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    let faults: usize = std::env::var("OSTRO_CAMPAIGN_FAULTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500);
    let seed = 1741u64;
    let spec = ClusterSpec {
        seed,
        ..ClusterSpec::default()
    };
    let t_start = Instant::now();
    let mut c = ProcCluster::start_with(spec, ProcOptions::default())
        .await
        .expect("start cluster");
    wait_healthy(&c, "start").await;
    deploy(&c, WF).await.expect("deploy trivial-steps");
    let acks: Arc<Mutex<Vec<Ack>>> = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut workload = spawn_workload(&c, Arc::clone(&acks), Arc::clone(&stop));
    let mut rng = StdRng::seed_from_u64(seed);
    let mut counts: BTreeMap<Fault, usize> = BTreeMap::new();
    let mut batch_checks = 0usize;
    for i in 0..faults {
        let kind = KINDS[rng.random_range(0..KINDS.len())];
        let what = inject(&mut c, kind, &mut rng).await;
        *counts.entry(kind).or_default() += 1;
        wait_healthy(&c, &what).await;
        if (i + 1) % BATCH == 0 {
            let snapshot = acks.lock().await.clone();
            if let Err(e) = disk_check(&c, &snapshot) {
                panic!("after fault {} ({what}): {e}", i + 1);
            }
            batch_checks += 1;
            println!(
                "faults {}/{faults}: {} acked runs on disk on >= min_insync nodes ({:?} elapsed)",
                i + 1,
                snapshot.len(),
                t_start.elapsed()
            );
        }
    }
    stop.store(true, Ordering::Relaxed);
    while workload.join_next().await.is_some() {}
    c.proxy().heal_all();
    wait_healthy(&c, "the campaign").await;
    let acks = acks.lock().await.clone();

    // Every partition's leader after convergence (the cluster is healthy and idle).
    let leaders: BTreeMap<u16, ApiClient> = (0..c.spec().partitions)
        .map(|p| (p, c.client(published(&c, p).leader)))
        .collect();
    // 0 lost acked runs: every acknowledged run completes; 0 duplicate journal entries in
    // the leader's state. Checked 32 runs at a time.
    let mut lost = Vec::new();
    let mut dups = Vec::new();
    let mut set = tokio::task::JoinSet::new();
    let mut queue = acks.iter().cloned();
    loop {
        while set.len() < 32
            && let Some(a) = queue.next()
        {
            let api = leaders[&a.partition].clone();
            set.spawn(async move {
                let t = Instant::now();
                let path = format!("/v1/runs/{}", a.run_id);
                let done = loop {
                    if let Ok((st, v)) = api.get_json(&path).await
                        && st.is_success()
                        && v["status"] == "completed"
                    {
                        break true;
                    }
                    if t.elapsed() > Duration::from_secs(120) {
                        break false;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                };
                if !done {
                    return (a, false, None);
                }
                let dup = match duplicates(&api, &a).await {
                    Ok(d) => d,
                    Err(e) => panic!("harness: {e}"),
                };
                (a, true, dup)
            });
        }
        match set.join_next().await {
            None => break,
            Some(Ok((a, done, dup))) => {
                if !done {
                    lost.push(a.run_id.clone());
                }
                if let Some(d) = dup {
                    dups.push(d);
                }
            }
            Some(Err(e)) => panic!("harness: check task: {e}"),
        }
    }
    let lost_n = lost.len();
    for l in lost.iter().take(20) {
        println!("LOST: acknowledged run {l} not completed within 120 s");
    }
    let final_result = final_checks(&c, &acks).await;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "cluster crash campaign (12-17): seed {seed}, {faults} faults, 3 processes, RF 3, min_insync 2, {:?}",
        t_start.elapsed()
    );
    let _ = writeln!(out, "faults by kind: {counts:?}");
    let _ = writeln!(
        out,
        "acknowledged runs: {}; batch disk checks: {batch_checks} (every {BATCH} faults)",
        acks.len()
    );
    let _ = writeln!(out, "lost acked runs: {lost_n}");
    let _ = writeln!(out, "duplicate journal entries: {}", dups.len());
    for d in dups.iter().take(20) {
        let _ = writeln!(out, "  {d}");
    }
    let _ = writeln!(
        out,
        "final checks (replica identity, acked on disk, LA rule, ISR-only elections): {}",
        match &final_result {
            Ok(()) => "pass".to_owned(),
            Err(f) => format!("FAIL {f}"),
        }
    );
    let path = report_dir().join("report.txt");
    std::fs::write(&path, &out).expect("write report");
    println!("{out}\nreport: {}", path.display());
    c.stop().await;
    assert!(lost.is_empty(), "lost acknowledged runs: {lost:?}");
    assert!(dups.is_empty(), "duplicate journal entries: {dups:?}");
    assert!(final_result.is_ok(), "final checks: {final_result:?}");
}
