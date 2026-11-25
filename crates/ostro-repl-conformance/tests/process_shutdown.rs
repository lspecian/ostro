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

use std::fmt::Write as _;
use std::time::Duration;

use ostro_meta::ext::{ELECTION_REASON_RESIGN, get_election_info};
use ostro_repl_conformance::harness::ClusterHarness;
use ostro_repl_conformance::harness::process::ProcCluster;
use ostro_repl_conformance::scenarios::process::skip_reason;
use ostro_repl_conformance::scenarios::process::tools::{meta_leader, published, published_all};
use ostro_repl_conformance::scenarios::process_p13::{
    HEAL_T, Load, ScenarioFailure, WORKLOAD_GUESTS, busiest_leader, deploy_guests, dist, host_line,
    outcomes, start_static, wait_healthy, write_report,
};
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
