//! 13-19 Task 2 (CLUS-06, D-P13-13, tier C): controlled-shutdown timing with real SIGTERM.
//! `#[ignore]`:
//!
//! ```text
//! cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
//! cargo test --release -p ostro-repl-conformance --features failpoints --locked \
//!   --test process_shutdown -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Three static processes (RF 3, this build's level) under the continuous mixed workload
//! and one availability probe per partition. Ten times (`OSTRO_SHUTDOWN_REPEATS`): the node
//! leading the most partitions (the meta leader on a tie) gets SIGTERM; the test measures
//! the time until the process exits (13-11 `before_shutdown` hands every partition it led
//! off first), checks that each partition it led has a new leader whose election reason is
//! `RESIGN` (a handoff, not a failover), measures each partition's unavailability (the
//! longest gap between probe acks from SIGTERM until the restarted node is back and the
//! cluster healthy), then restarts it. Zero failed runs: every acknowledged run completes.
//! Report: `$OSTRO_P13_REPORT_DIR/process_shutdown/report.txt` (p50 / p99).
//!
//! 14-33 (D-P14-50) adds `controlled_shutdown_followed_partition_stall` (same command with
//! `-- --ignored controlled_shutdown_followed_partition_stall`): three static processes with
//! the **shipped** `[replication]` defaults (`session_timeout` 3 s, `replica_lag_max` 10 s;
//! the stall being measured is about `session_timeout` without the self-shrink), the
//! continuous workload plus one probe per partition. `OSTRO_SHUTDOWN_STALL_REPEATS` times
//! (default 20), round-robin over the nodes: SIGTERM, wait for the exit, keep the node down
//! for `session_timeout + 1 s`, restart it and wait until the cluster is healthy. For every
//! partition the stopped node only **followed** at the SIGTERM, the write stall is the
//! longest gap between probe acks from the SIGTERM to the end of the hold. Asserts p99 < 1 s
//! over all (repeat, followed partition) pairs and zero failed runs. Report:
//! `$OSTRO_P13_REPORT_DIR/process_shutdown_followed/report.txt`.

use std::fmt::Write as _;
use std::time::Duration;

use ostro_meta::ext::{ELECTION_REASON_RESIGN, get_election_info};
use ostro_repl_conformance::harness::ClusterHarness;
use ostro_repl_conformance::harness::process::{ProcCluster, ProcOptions};
use ostro_repl_conformance::scenarios::process::skip_reason;
use ostro_repl_conformance::scenarios::process::tools::{meta_leader, published, published_all};
use ostro_repl_conformance::scenarios::process_p13::{
    HEAL_T, Load, ScenarioFailure, WORKLOAD_GUESTS, busiest_leader, deploy_guests, dist, host_line,
    outcomes, pct, spec3, start_static, wait_healthy, write_report,
};
use ostro_server::config::ReplicationSection;
use tokio::time::Instant;

const TEST: &str = "process_shutdown";
const RUN_T: Duration = Duration::from_secs(120);

fn f(oracle: &'static str, detail: impl Into<String>) -> ScenarioFailure {
    ScenarioFailure {
        oracle,
        detail: detail.into(),
    }
}

#[derive(Default)]
struct Measured {
    exit: Vec<Duration>,
    restart: Vec<Duration>,
    unavailable_led: Vec<Duration>,
    unavailable_other: Vec<Duration>,
    handed_off: usize,
    not_resign: Vec<String>,
}

async fn body(
    c: &mut ProcCluster,
    out: &mut String,
    repeats: usize,
) -> Result<(), ScenarioFailure> {
    deploy_guests(c, &WORKLOAD_GUESTS).await?;
    let load = Load::start(c, &[], true);
    load.wait_acked(10, RUN_T).await?;
    let mut m = Measured::default();
    for i in 1..=repeats {
        let victim = busiest_leader(c)
            .await
            .ok_or_else(|| f("placement", "no partition led"))?;
        let meta = meta_leader(c).await;
        let before: Vec<(u16, u64, u32)> = published_all(c)
            .iter()
            .map(|a| (a.partition, a.leader, a.epoch))
            .collect();
        let led: Vec<u16> = before
            .iter()
            .filter(|(_, l, _)| *l == victim)
            .map(|(p, _, _)| *p)
            .collect();
        load.refresh(c, &[victim]);
        let t0 = Instant::now(); // clock-exempt: measurement
        let graceful = c
            .stop_gracefully(victim)
            .await
            .map_err(|e| f("harness", format!("SIGTERM node {victim}: {e}")))?;
        let exit = t0.elapsed();
        if !graceful {
            return Err(f(
                "controlled_shutdown",
                format!("node {victim} did not exit on SIGTERM within the harness STOP_T"),
            ));
        }
        // Each partition it led was handed off before the exit (election reason RESIGN).
        for p in &led {
            let a = published(c, *p);
            let reason = get_election_info(&a.ext).ok().flatten().map(|i| i.reason);
            if a.leader == victim || a.leader == 0 {
                m.not_resign.push(format!(
                    "repeat {i}: p{p} still led by {} after the exit",
                    a.leader
                ));
            } else if reason != Some(ELECTION_REASON_RESIGN) {
                m.not_resign.push(format!(
                    "repeat {i}: p{p} now led by {} with election reason {reason:?}",
                    a.leader
                ));
            } else {
                m.handed_off += 1;
            }
        }
        let t1 = Instant::now(); // clock-exempt: measurement
        c.restart(victim)
            .await
            .map_err(|e| f("harness", format!("restart node {victim}: {e}")))?;
        let restart = t1.elapsed();
        load.refresh(c, &[]);
        wait_healthy(c, &[], HEAL_T, &format!("restart {i} of node {victim}")).await?;
        let t_end = Instant::now(); // clock-exempt: measurement
        let mut gaps = Vec::new();
        for p in 0..c.spec().partitions {
            let g = load.unavailability(p, t0, t_end);
            gaps.push((p, g));
            if led.contains(&p) {
                m.unavailable_led.push(g);
            } else {
                m.unavailable_other.push(g);
            }
        }
        let _ = writeln!(
            out,
            "repeat {i}: SIGTERM node {victim} (led {led:?}, meta leader {meta:?}) exited after {exit:?}; restart {restart:?}; unavailability {gaps:?}"
        );
        m.exit.push(exit);
        m.restart.push(restart);
        load.wait_acked(5, RUN_T).await?;
    }
    let res = load.finish(RUN_T).await?;
    let _ = writeln!(out, "{}", res.line());
    let oc = outcomes(c, &res.runs).await;
    let _ = writeln!(out, "outcomes: {}", oc.line());
    let _ = writeln!(out, "SIGTERM -> process exit: {}", dist(&mut m.exit));
    let _ = writeln!(out, "restart -> ready line: {}", dist(&mut m.restart));
    let _ = writeln!(
        out,
        "unavailability of the partitions the stopped node led: {}",
        dist(&mut m.unavailable_led)
    );
    let _ = writeln!(
        out,
        "unavailability of the other partitions over the same windows: {}",
        dist(&mut m.unavailable_other)
    );
    let _ = writeln!(
        out,
        "handed off before the exit (election reason RESIGN): {}; not: {:?}",
        m.handed_off, m.not_resign
    );
    res.verdict()?;
    oc.verdict()?;
    if !m.not_resign.is_empty() {
        return Err(f(
            "controlled_shutdown",
            format!(
                "partitions not handed off before the exit: {:?}",
                m.not_resign
            ),
        ));
    }
    if m.handed_off == 0 {
        return Err(f("controlled_shutdown", "no handoff measured (vacuous)"));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "tier C: needs the failpoint binary (cargo build -p ostro --features failpoints --target-dir target/failpoints)"]
async fn controlled_shutdown_timing() {
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    let repeats: usize = std::env::var("OSTRO_SHUTDOWN_REPEATS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10)
        .max(1);
    let t0 = Instant::now(); // clock-exempt: measurement
    let mut c = start_static(1915, None).await.expect("start cluster");
    let mut out = String::new();
    let _ = writeln!(
        out,
        "process_shutdown (13-19, tier C): 3 static processes, RF 3, {repeats} SIGTERMs of the busiest leader under load\n{}",
        host_line()
    );
    let r = body(&mut c, &mut out, repeats).await;
    let _ = writeln!(
        out,
        "result: {}; elapsed {:?}",
        match &r {
            Ok(()) => "PASS".to_owned(),
            Err(e) => format!("FAIL {e}"),
        },
        t0.elapsed()
    );
    if r.is_err() {
        for n in c.node_ids() {
            println!("--- node {n} log tail ---\n{}", c.log_tail(n, 40));
        }
    }
    write_report(TEST, &out);
    c.stop().await;
    r.expect("process_shutdown");
}

// ------------------------------------------------------------------ 14-33: followed partitions

const FOLLOWED_TEST: &str = "process_shutdown_followed";
/// The p99 bound on the write stall of a followed partition (14-33).
const FOLLOWED_STALL_MAX: Duration = Duration::from_secs(1);

async fn followed_body(
    c: &mut ProcCluster,
    out: &mut String,
    repeats: usize,
    hold: Duration,
) -> Result<(), ScenarioFailure> {
    deploy_guests(c, &WORKLOAD_GUESTS).await?;
    let load = Load::start(c, &[], true);
    load.wait_acked(10, RUN_T).await?;
    let nodes = c.node_ids();
    let mut stalls: Vec<Duration> = Vec::new();
    let mut not_shrinkable: Vec<(usize, u64, u16, Vec<u64>, Duration)> = Vec::new();
    let mut worst: Vec<(usize, u64, u16, Duration)> = Vec::new();
    for i in 1..=repeats {
        let victim = nodes[(i - 1) % nodes.len()];
        // Every repeat starts healthy (a leader and a full ISR everywhere).
        wait_healthy(c, &[], HEAL_T, &format!("before repeat {i}")).await?;
        // Followed partitions with their ISR at the SIGTERM; the self-shrink applies where
        // the ISR without the victim keeps the leader and min_insync members.
        let followed: Vec<(u16, Vec<u64>, bool)> = published_all(c)
            .iter()
            .filter(|a| a.leader != 0 && a.leader != victim && a.isr.contains(&victim))
            .map(|a| {
                let rest = a.isr.iter().filter(|n| **n != victim).count();
                let ok = a.isr.contains(&a.leader) && rest >= usize::from(a.min_insync.max(1));
                (a.partition, a.isr.clone(), ok)
            })
            .collect();
        let log_from = std::fs::metadata(c.log_path(victim)).map_or(0, |m| m.len());
        load.refresh(c, &[victim]);
        let t0 = Instant::now(); // clock-exempt: measurement
        let graceful = c
            .stop_gracefully(victim)
            .await
            .map_err(|e| f("harness", format!("SIGTERM node {victim}: {e}")))?;
        let exit = t0.elapsed();
        if !graceful {
            return Err(f(
                "controlled_shutdown",
                format!("node {victim} did not exit on SIGTERM within the harness STOP_T"),
            ));
        }
        tokio::time::sleep(hold).await;
        let t_hold = Instant::now(); // clock-exempt: measurement
        let out_of_isr: Vec<u16> = followed
            .iter()
            .map(|(p, _, _)| *p)
            .filter(|p| !published(c, *p).isr.contains(&victim))
            .collect();
        let mut gaps = Vec::new();
        for (p, isr, shrinkable) in &followed {
            let g = load.unavailability(*p, t0, t_hold);
            gaps.push((*p, g));
            if *shrinkable {
                stalls.push(g);
                worst.push((i, victim, *p, g));
            } else {
                not_shrinkable.push((i, victim, *p, isr.clone(), g));
            }
        }
        let shrink_log = self_shrink_lines(c.log_path(victim), log_from);
        c.restart(victim)
            .await
            .map_err(|e| f("harness", format!("restart node {victim}: {e}")))?;
        load.refresh(c, &[]);
        wait_healthy(c, &[], HEAL_T, &format!("restart {i} of node {victim}")).await?;
        let _ = writeln!(
            out,
            "repeat {i}: SIGTERM node {victim}, exited after {exit:?}; followed (partition, ISR at SIGTERM, shrinkable) {followed:?} (out of the ISR after the hold: {out_of_isr:?}); stall per followed partition {gaps:?}"
        );
        for line in shrink_log {
            let _ = writeln!(out, "    node {victim}: {line}");
        }
        load.wait_acked(5, RUN_T).await?;
    }
    let res = load.finish(RUN_T).await?;
    let _ = writeln!(out, "{}", res.line());
    let oc = outcomes(c, &res.runs).await;
    let _ = writeln!(out, "outcomes: {}", oc.line());
    stalls.sort_unstable();
    let p99 = pct(&stalls, 0.99);
    worst.sort_unstable_by_key(|w| std::cmp::Reverse(w.3));
    worst.truncate(5);
    let _ = writeln!(
        out,
        "write stall of the partitions the stopped node only followed ({} samples): {}",
        stalls.len(),
        dist(&mut stalls)
    );
    let _ = writeln!(out, "worst (repeat, node, partition, stall): {worst:?}");
    let _ = writeln!(
        out,
        "followed partitions where no self-shrink was possible (ISR without the node below min_insync; not counted): {not_shrinkable:?}"
    );
    res.verdict()?;
    oc.verdict()?;
    if stalls.is_empty() {
        return Err(f(
            "controlled_shutdown",
            "no followed partition measured (vacuous)",
        ));
    }
    if p99 >= FOLLOWED_STALL_MAX {
        return Err(f(
            "controlled_shutdown",
            format!(
                "write stall p99 of followed partitions {p99:?} >= {FOLLOWED_STALL_MAX:?} (worst {worst:?})"
            ),
        ));
    }
    Ok(())
}

/// The controlled-shutdown / self-shrink lines node `log` wrote after byte `from`.
fn self_shrink_lines(log: &std::path::Path, from: u64) -> Vec<String> {
    let text = std::fs::read(log).unwrap_or_default();
    let from = usize::try_from(from).unwrap_or(usize::MAX).min(text.len());
    String::from_utf8_lossy(&text[from..])
        .lines()
        .filter(|l| l.contains("controlled shutdown") || l.contains("stopped following"))
        .map(|l| l.chars().take(400).collect())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "tier C: needs the failpoint binary (cargo build -p ostro --features failpoints --target-dir target/failpoints)"]
async fn controlled_shutdown_followed_partition_stall() {
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    let repeats: usize = std::env::var("OSTRO_SHUTDOWN_STALL_REPEATS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20)
        .max(1);
    // The shipped timings: the pre-14-33 stall is about `session_timeout`.
    let timings = ReplicationSection::default();
    let hold = timings.session_timeout.get() + Duration::from_secs(1);
    let t0 = Instant::now(); // clock-exempt: measurement
    let opts = ProcOptions {
        replication: Some(timings.clone()),
        ..ProcOptions::default()
    };
    let mut c = ProcCluster::start_with(spec3(1433), opts)
        .await
        .expect("start cluster");
    wait_healthy(&c, &[], HEAL_T, "start")
        .await
        .expect("healthy cluster");
    let mut out = String::new();
    let _ = writeln!(
        out,
        "process_shutdown_followed (14-33, tier C): 3 static processes, RF 3, shipped [replication] timings (session_timeout {:?}, replica_lag_max {:?}), {repeats} SIGTERMs round-robin under load, hold {hold:?}\n{}",
        timings.session_timeout.get(),
        timings.replica_lag_max.get(),
        host_line()
    );
    let r = followed_body(&mut c, &mut out, repeats, hold).await;
    let _ = writeln!(
        out,
        "result: {}; elapsed {:?}",
        match &r {
            Ok(()) => "PASS".to_owned(),
            Err(e) => format!("FAIL {e}"),
        },
        t0.elapsed()
    );
    println!("{out}");
    if r.is_err() {
        for n in c.node_ids() {
            println!("--- node {n} log tail ---\n{}", c.log_tail(n, 40));
        }
    }
    write_report(FOLLOWED_TEST, &out);
    c.stop().await;
    r.expect("process_shutdown_followed");
}
