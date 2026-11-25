//! 13-19 Task 1 (API-08 read half, D-P13-15/16, tier C): read-your-writes across processes
//! through leader moves, failover and a zombie isolated by the chaos proxy. `#[ignore]`:
//!
//! ```text
//! cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
//! cargo test --release -p ostro-repl-conformance --locked --test process_ryw \
//!   -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Three static `ostro server` processes (RF 3; every link through the chaos proxy) and the
//! 13-13 session workload (`scenarios::ryw::run_load`: six client sessions, each request to
//! a node picked at random from the "load balancer" list; keyed starts, signals,
//! terminates, activations, queue policies, and reads of each), judged by the 13-08 session
//! checker (`RywChecker`) over REST answers only. While it runs:
//!
//! 1. **Leader moves:** the node leading the most partitions gets SIGTERM (controlled
//!    shutdown hands its partitions and, if held, the meta leadership off) and restarts;
//!    the load balancer skips it while it is down.
//! 2. **Failover:** a partition leader that is not the meta leader is `kill -9`ed (dropped
//!    from the load balancer first), its partitions fail over, it restarts.
//! 3. **Zombie:** a partition leader (not the meta leader) is isolated by the chaos proxy
//!    (every link to and from it cut) while the load balancer keeps sending to it: it must
//!    never serve a stale read while a successor is elected; healed after 4 s.
//!
//! Pass: 0 RYW violations, no answer that is neither success nor availability, and the
//! workload still running after the three faults (they overlapped it). Report:
//! `$OSTRO_P13_REPORT_DIR/process_ryw/report.txt`.

use std::fmt::Write as _;
use std::time::Duration;

use ostro_repl_conformance::harness::ClusterHarness;
use ostro_repl_conformance::harness::process::ProcCluster;
use ostro_repl_conformance::scenarios::process::skip_reason;
use ostro_repl_conformance::scenarios::process::tools::{meta_leader, published, published_all};
use ostro_repl_conformance::scenarios::process_p13::{
    HEAL_T, ScenarioFailure, busiest_leader, deploy_guests, host_line, start_static, wait_healthy,
    write_report,
};
use ostro_repl_conformance::scenarios::ryw::tier_c;
use tokio::time::Instant;

const TEST: &str = "process_ryw";

fn f(oracle: &'static str, detail: impl Into<String>) -> ScenarioFailure {
    ScenarioFailure {
        oracle,
        detail: detail.into(),
    }
}

/// A partition leader that is not the meta leader (`None` when every leader is the meta
/// leader).
async fn non_meta_leader(c: &ProcCluster) -> Option<(u16, u64, u32)> {
    let meta = meta_leader(c).await?;
    published_all(c)
        .into_iter()
        .find(|a| a.leader != 0 && a.leader != meta)
        .map(|a| (a.partition, a.leader, a.epoch))
}

async fn wait_moved(
    c: &ProcCluster,
    p: u16,
    from: u64,
    epoch: u32,
) -> Result<Duration, ScenarioFailure> {
    let t0 = Instant::now(); // clock-exempt: measurement
    loop {
        let a = published(c, p);
        if a.leader != 0 && a.leader != from && a.epoch > epoch {
            return Ok(t0.elapsed());
        }
        if t0.elapsed() > HEAL_T {
            return Err(f(
                "election",
                format!("p{p}: no successor of node {from} within {HEAL_T:?}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn body(c: &mut ProcCluster, out: &mut String) -> Result<(), ScenarioFailure> {
    deploy_guests(c, &["smoke", "signals"]).await?;
    tier_c::wait_visible(c).await?;
    let run = tier_c::Run::start(c, "pc-ryw", 9000, Duration::from_millis(30));
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 1. Leader moves through a controlled shutdown.
    let victim = busiest_leader(c).await.unwrap_or(1);
    let led: Vec<u16> = published_all(c)
        .iter()
        .filter(|a| a.leader == victim)
        .map(|a| a.partition)
        .collect();
    run.lb_remove(victim);
    let t = Instant::now(); // clock-exempt: measurement
    let graceful = c
        .stop_gracefully(victim)
        .await
        .map_err(|e| f("harness", format!("SIGTERM node {victim}: {e}")))?;
    let stop = t.elapsed();
    let moved: Vec<(u16, u64)> = led.iter().map(|p| (*p, published(c, *p).leader)).collect();
    c.restart(victim)
        .await
        .map_err(|e| f("harness", format!("restart node {victim}: {e}")))?;
    wait_healthy(c, &[], HEAL_T, "the controlled shutdown").await?;
    run.lb_add(victim);
    let _ = writeln!(
        out,
        "leader moves: SIGTERM node {victim} (led {led:?}) exited in {stop:?} (graceful {graceful}); leaders after {moved:?}; restarted"
    );

    // 2. Failover: kill -9 a partition leader that is not the meta leader.
    let (p, leader, epoch) = non_meta_leader(c)
        .await
        .ok_or_else(|| f("placement", "no partition led off the meta leader"))?;
    run.lb_remove(leader);
    c.kill(leader).await;
    let fo = wait_moved(c, p, leader, epoch).await?;
    c.restart(leader)
        .await
        .map_err(|e| f("harness", format!("restart node {leader}: {e}")))?;
    wait_healthy(c, &[], HEAL_T, "the failover").await?;
    run.lb_add(leader);
    let _ = writeln!(
        out,
        "failover: kill -9 node {leader} (led p{p}); successor after {fo:?}; restarted"
    );

    // 3. Zombie: isolate a partition leader; the load balancer keeps sending to it.
    let (p, zombie, epoch) = non_meta_leader(c)
        .await
        .ok_or_else(|| f("placement", "no partition led off the meta leader"))?;
    c.isolate(zombie);
    let succ = wait_moved(c, p, zombie, epoch).await?;
    tokio::time::sleep(Duration::from_secs(4)).await;
    c.heal(zombie);
    wait_healthy(c, &[], HEAL_T, "the zombie heal").await?;
    let _ = writeln!(
        out,
        "zombie: node {zombie} (led p{p}) isolated by the chaos proxy, still in the load balancer; successor after {succ:?}; healed after +4 s"
    );
    let still_running = !run.is_finished();
    let mut rep = run.join().await;
    let _ = writeln!(out, "workload: {}", tier_c::summary(&mut rep));
    if !still_running {
        return Err(f(
            "harness",
            "the session workload finished before the three faults were done (raise the op count)",
        ));
    }
    tier_c::verdict(TEST, &mut rep)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "tier C: needs the failpoint binary (cargo build -p ostro --features failpoints --target-dir target/failpoints)"]
async fn ryw_across_processes_through_moves_failover_and_zombie() {
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    let t0 = Instant::now(); // clock-exempt: measurement
    let mut c = start_static(1913, None).await.expect("start cluster");
    let mut out = String::new();
    let _ = writeln!(
        out,
        "process_ryw (13-19, tier C): 3 static processes, RF 3, 6 sessions\n{}",
        host_line()
    );
    let r = body(&mut c, &mut out).await;
    let _ = writeln!(
        out,
        "result: {}; elapsed {:?}",
        match &r {
            Ok(()) => "PASS (0 RYW violations)".to_owned(),
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
    r.expect("process_ryw");
}
