//! 12-17 (docs/04 §4, D16 profile A): the RF=3 user-path benchmark, with every acknowledged
//! step verified on disk. `#[ignore]`; reported, not gated (Phase 14 QA-05).
//!
//! ```text
//! OSTRO_REPL_BENCH_SECS=60 cargo test --release -p ostro-repl-conformance --test bench_rf3 \
//!   -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Three `ostro server` processes (RF 3, min_insync 2) and an RF=1 single process with the
//! same binary. 16 closed-loop clients start `trivial-steps` runs with 10 steps (profile A)
//! through the API (keyed routing, following leader hints) for `OSTRO_REPL_BENCH_SECS`
//! (default 60). Every acknowledged run is then waited to completion, the cluster is stopped,
//! and the data dirs are scanned offline: each acknowledged run must have its `StartRun`
//! (the `acked_on_disk` oracle) and its 10 `StepCompleted` journal events on at least
//! `min_insync` replicas. Reports acknowledged / verified steps per second, p50 / p99 commit
//! latency (start request → acknowledgement: append plus the HW passing it), the largest
//! envelopes-per-fetch the leaders measured (batching on user partitions, asserted ≥ 2 at
//! RF 3), WAL bytes on disk per node, and the RF=1 / RF=3 ratio. Writes
//! `target/bench-rf3/report.txt`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_repl_conformance::harness::inproc::Ack;
use ostro_repl_conformance::harness::process::{ProcCluster, ProcOptions};
use ostro_repl_conformance::harness::{ClusterHarness, ClusterSpec, DataDirView};
use ostro_repl_conformance::oracle::disk::{acked_on_disk, scan_partition};
use ostro_repl_conformance::scenarios::process::skip_reason;
use ostro_repl_conformance::scenarios::process::tools::{
    HintClient, Start, deploy, partition_status, published_all, wait_meta_leader,
};
use ostro_types::command::decode_command;
use ostro_types::{Cmd, PartitionCount, PartitionId, RunId};
use serde_json::json;
use tokio::sync::Mutex;

const WF: &str = "trivial-steps";
const STEPS: u64 = 10;
const CLIENTS: usize = 16;
/// `EventKind::StepCompleted` (frozen journal code).
const STEP_COMPLETED: u32 = 16;

fn report_dir() -> PathBuf {
    let d = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/bench-rf3");
    std::fs::create_dir_all(&d).expect("report dir");
    d
}

fn pct(v: &[u64], q: f64) -> u64 {
    if v.is_empty() {
        return 0;
    }
    let mut s = v.to_vec();
    s.sort_unstable();
    let idx = ((q * s.len() as f64).ceil() as usize).clamp(1, s.len()) - 1;
    s[idx]
}

fn routed_key(salt: &str, p: u16, partitions: u16, i: &mut u64) -> String {
    let count = PartitionCount::new(partitions).expect("partitions > 0");
    loop {
        *i += 1;
        let key = format!("bench-{salt}-{i}");
        if ostro_types::routing::partition_for_key(WF, key.as_bytes(), count).0 == p {
            return key;
        }
    }
}

#[derive(Debug, Default)]
struct BenchResult {
    label: String,
    secs: f64,
    acked_runs: usize,
    completed_runs: usize,
    verified_runs: usize,
    latencies_us: Vec<u64>,
    max_envelopes_per_fetch: u64,
    wal_bytes: BTreeMap<u64, u64>,
    disk_violations: Vec<String>,
}

impl BenchResult {
    fn steps_per_s(&self, runs: usize) -> f64 {
        runs as f64 * STEPS as f64 / self.secs
    }
}

fn wal_bytes(root: &std::path::Path) -> u64 {
    fn walk(p: &std::path::Path) -> u64 {
        let Ok(rd) = std::fs::read_dir(p) else {
            return 0;
        };
        rd.flatten()
            .map(|e| {
                let path = e.path();
                if path.is_dir() {
                    walk(&path)
                } else {
                    e.metadata().map(|m| m.len()).unwrap_or(0)
                }
            })
            .sum()
    }
    walk(&root.join("wal"))
}

/// Journal seqs of `StepCompleted` events, per run id bytes.
type StepsByRun = BTreeMap<Vec<u8>, BTreeSet<u64>>;

/// Collect `StepCompleted` events per run in `view`'s partition `p` (verified WAL scan).
fn steps_on_disk(view: &DataDirView, p: u16) -> StepsByRun {
    let mut out = StepsByRun::new();
    let Ok(scan) = scan_partition(view, PartitionId(p)) else {
        return out;
    };
    for (_, c) in scan.commands() {
        if let Ok(cmd) = decode_command(c)
            && let Some(Cmd::EventsAppended(e)) = cmd.cmd
        {
            let seqs = out.entry(e.run_id.clone()).or_default();
            for ev in e.events.iter().filter(|ev| ev.kind == STEP_COMPLETED) {
                seqs.insert(ev.seq);
            }
        }
    }
    out
}

async fn run_bench(label: &str, spec: ClusterSpec, secs: u64) -> BenchResult {
    let c = ProcCluster::start_with(spec.clone(), ProcOptions::default())
        .await
        .expect("start cluster");
    wait_meta_leader(&c).await.expect("meta leader");
    // Full ISRs before measuring (RF 3).
    let t = Instant::now();
    while published_all(&c)
        .iter()
        .any(|a| a.leader == 0 || a.isr.len() != a.replicas.len())
    {
        assert!(t.elapsed() < Duration::from_secs(60), "ISRs never filled");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    deploy(&c, WF).await.expect("deploy trivial-steps");
    let partitions = spec.partitions;
    let acks: Arc<Mutex<Vec<(Ack, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let t0 = Instant::now();
    let window = Duration::from_secs(secs);
    let mut workers = tokio::task::JoinSet::new();
    for w in 0..CLIENTS {
        let p = (w % usize::from(partitions)) as u16;
        let cl = HintClient::new(&c, 1);
        let acks = Arc::clone(&acks);
        let salt = format!("{label}-{w}-{}", c.r());
        workers.spawn(async move {
            let mut i = 0u64;
            while t0.elapsed() < window {
                let key = routed_key(&salt, p, partitions, &mut i);
                // Same key until acknowledged (a 503 hop or a timeout retries it).
                loop {
                    let sent = Instant::now();
                    match cl
                        .start(WF, json!({ "n": STEPS }), &key, Duration::from_secs(10))
                        .await
                    {
                        Start::Acked(a) => {
                            let us = sent.elapsed().as_micros() as u64;
                            acks.lock().await.push((a, us));
                            break;
                        }
                        _ if t0.elapsed() >= window => break,
                        _ => tokio::time::sleep(Duration::from_millis(20)).await,
                    }
                }
            }
        });
    }
    while workers.join_next().await.is_some() {}
    let elapsed = t0.elapsed().as_secs_f64();
    let acks = acks.lock().await.clone();
    let mut max_epf = 0;
    for n in c.node_ids() {
        for p in 0..partitions {
            if let Some(row) = partition_status(&c, n, p).await {
                max_epf = max_epf.max(row["max_envelopes_per_fetch"].as_u64().unwrap_or(0));
            }
        }
    }
    // Every acknowledged run completes (checked 32 at a time, each within 120 s).
    let mut completed = 0usize;
    let mut set = tokio::task::JoinSet::new();
    let mut queue = acks.iter().map(|(a, _)| a.run_id.clone());
    let cl = HintClient::new(&c, 1);
    loop {
        while set.len() < 32
            && let Some(run_id) = queue.next()
        {
            let cl = cl.clone();
            set.spawn(async move {
                let t = Instant::now();
                let path = format!("/v1/runs/{run_id}");
                loop {
                    if let Some((200, v)) = cl.get(&path, Duration::from_secs(5)).await
                        && v["status"] == "completed"
                    {
                        return true;
                    }
                    if t.elapsed() > Duration::from_secs(120) {
                        return false;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            });
        }
        match set.join_next().await {
            None => break,
            Some(Ok(true)) => completed += 1,
            Some(_) => {}
        }
    }
    // Stop, keep the data dirs, verify offline.
    let views: Vec<DataDirView> = c.node_ids().iter().map(|n| c.node_disk(*n)).collect();
    let min_insync = spec.min_insync;
    let dirs = c.stop_keeping_dirs();
    let ack_list: Vec<Ack> = acks.iter().map(|(a, _)| a.clone()).collect();
    let mut violations: Vec<String> = acked_on_disk(&views, &ack_list, min_insync)
        .violations
        .clone();
    let mut per_node: Vec<BTreeMap<u16, StepsByRun>> = Vec::new();
    for v in &views {
        per_node.push((0..partitions).map(|p| (p, steps_on_disk(v, p))).collect());
    }
    let mut verified = 0usize;
    for (a, _) in &acks {
        let run: RunId = a.run_id.parse().expect("run id");
        let key = run.as_bytes().to_vec();
        let holders = per_node
            .iter()
            .filter(|m| {
                m.get(&a.partition)
                    .and_then(|r| r.get(&key))
                    .is_some_and(|s| s.len() as u64 == STEPS)
            })
            .count();
        if holders >= min_insync {
            verified += 1;
        } else if violations.len() < 20 {
            violations.push(format!(
                "run {} (partition {}): {STEPS} StepCompleted on {holders} node(s) < {min_insync}",
                a.run_id, a.partition
            ));
        }
    }
    let wal: BTreeMap<u64, u64> = views.iter().map(|v| (v.node, wal_bytes(&v.root))).collect();
    drop(dirs);
    BenchResult {
        label: label.to_owned(),
        secs: elapsed,
        acked_runs: acks.len(),
        completed_runs: completed,
        verified_runs: verified,
        latencies_us: acks.iter().map(|(_, us)| *us).collect(),
        max_envelopes_per_fetch: max_epf,
        wal_bytes: wal,
        disk_violations: violations,
    }
}

fn describe(r: &BenchResult, out: &mut String) {
    let _ = writeln!(
        out,
        "{}: {:.1} s, runs acknowledged {}, completed {}, verified on disk {} | steps/s acknowledged {:.1}, verified {:.1} | commit latency p50 {:.1} ms p99 {:.1} ms | max envelopes/fetch {} | WAL bytes per node {:?}",
        r.label,
        r.secs,
        r.acked_runs,
        r.completed_runs,
        r.verified_runs,
        r.steps_per_s(r.acked_runs),
        r.steps_per_s(r.verified_runs),
        pct(&r.latencies_us, 0.5) as f64 / 1000.0,
        pct(&r.latencies_us, 0.99) as f64 / 1000.0,
        r.max_envelopes_per_fetch,
        r.wal_bytes
    );
    for v in &r.disk_violations {
        let _ = writeln!(out, "  DISK VIOLATION: {v}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "tier C benchmark: RF=3 and RF=1 for OSTRO_REPL_BENCH_SECS each (default 60)"]
async fn bench_rf3_bytes_on_disk() {
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    let secs = std::env::var("OSTRO_REPL_BENCH_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    let rf3 = run_bench(
        "RF=3 (3 processes, min_insync 2)",
        ClusterSpec {
            seed: 1731,
            ..ClusterSpec::default()
        },
        secs,
    )
    .await;
    let rf1 = run_bench(
        "RF=1 (1 process)",
        ClusterSpec {
            seed: 1732,
            ..ClusterSpec::single()
        },
        secs,
    )
    .await;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "RF=3 benchmark (12-17, profile A: trivial-steps n={STEPS}, {CLIENTS} closed-loop clients, {secs} s per cluster; failpoint debug server build, WSL2: advisory)"
    );
    describe(&rf3, &mut out);
    describe(&rf1, &mut out);
    let ratio = rf3.steps_per_s(rf3.verified_runs)
        / rf1.steps_per_s(rf1.verified_runs).max(f64::MIN_POSITIVE);
    let _ = writeln!(out, "RF=3 / RF=1 verified steps/s ratio: {ratio:.2}");
    let path = report_dir().join("report.txt");
    std::fs::write(&path, &out).expect("write report");
    println!("{out}\nreport: {}", path.display());
    for r in [&rf3, &rf1] {
        assert!(r.acked_runs > 0, "{}: nothing acknowledged", r.label);
        assert!(
            r.disk_violations.is_empty(),
            "{}: acknowledged work missing on disk",
            r.label
        );
        assert_eq!(
            r.completed_runs, r.acked_runs,
            "{}: every acknowledged run completes",
            r.label
        );
        assert_eq!(
            r.verified_runs, r.acked_runs,
            "{}: every acknowledged run's steps are on disk",
            r.label
        );
    }
    assert!(
        rf3.max_envelopes_per_fetch >= 2,
        "batching on user partitions: max envelopes per fetch {} < 2",
        rf3.max_envelopes_per_fetch
    );
}
