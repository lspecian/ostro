//! 13-19 Task 1 (CLUS-05, D-P13-11, tier C): a running single node is promoted with runs in
//! flight. `#[ignore]`:
//!
//! ```text
//! cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
//! cargo test --release -p ostro-repl-conformance --locked --test process_promotion \
//!   -- --ignored --nocapture --test-threads=1
//! ```
//!
//! One spawned `ostro server` (server profile, no peers) under the continuous mixed workload
//! (`process_p13::Load`) → `POST /v1/cluster/init` → two `ostro cluster join` processes
//! (the real join flow: node token, CA pin, joiner, then the server) → every partition at
//! RF 3 / ISR 3 / min_insync 2 and three meta voters (time-to-RF-3 recorded) → `kill -9` of
//! node 1 (the original node) → the survivors re-elect, the workload continues on them, and
//! every acknowledged run completes exactly once (0 lost acked runs); acknowledged ⇒ on
//! disk on ≥ min_insync nodes (Phase 12 disk oracle), replica identity below the HW, the
//! LA rule / election audits. Report: `$OSTRO_P13_REPORT_DIR/process_promotion/report.txt`.

use std::fmt::Write as _;
use std::time::Duration;

use ostro_repl_conformance::harness::p13::{NodeOpts, P13Harness};
use ostro_repl_conformance::harness::process::{ProcCluster, ProcOptions};
use ostro_repl_conformance::harness::{ClusterHarness, ClusterSpec};
use ostro_repl_conformance::scenarios::process::skip_reason;
use ostro_repl_conformance::scenarios::process::tools::{live, published_all, wait_meta_leader};
use ostro_repl_conformance::scenarios::process_p13::{
    HEAL_T, Load, ScenarioFailure, WORKLOAD_GUESTS, deploy_guests, final_checks, host_line,
    outcomes, status, write_report,
};
use tokio::time::Instant;

const TEST: &str = "process_promotion";
const RF_T: Duration = Duration::from_secs(180);
const RUN_T: Duration = Duration::from_secs(120);

fn f(oracle: &'static str, detail: impl Into<String>) -> ScenarioFailure {
    ScenarioFailure {
        oracle,
        detail: detail.into(),
    }
}

/// Wait until every partition is RF 3 / ISR 3 / min_insync 2 and the status shows three
/// voters and none outgoing.
async fn wait_rf3(c: &ProcCluster) -> Result<Duration, ScenarioFailure> {
    let t0 = Instant::now(); // clock-exempt: measurement
    let mut last = String::new();
    while t0.elapsed() < RF_T {
        let parts = published_all(c);
        let rf3 = parts
            .iter()
            .all(|a| a.replicas.len() == 3 && a.isr.len() == 3 && a.min_insync == 2);
        let voters = match status(c, live(c)[0]).await {
            Some(v) => {
                let n = v["voters"].as_array().map_or(0, Vec::len);
                let out = v["voters_outgoing"].as_array().map_or(0, Vec::len);
                (n == 3 && out == 0, v["policy"].clone())
            }
            None => (false, serde_json::Value::Null),
        };
        if rf3 && voters.0 {
            return Ok(t0.elapsed());
        }
        last = format!(
            "{:?}; voters ok {}, policy {}",
            parts
                .iter()
                .map(|a| (a.partition, a.replicas.clone(), a.isr.clone(), a.min_insync))
                .collect::<Vec<_>>(),
            voters.0,
            voters.1
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(f(
        "promotion",
        format!("RF 3 not reached within {RF_T:?}: {last}"),
    ))
}

async fn body(c: &mut ProcCluster, out: &mut String) -> Result<(), ScenarioFailure> {
    wait_meta_leader(c).await?;
    deploy_guests(c, &WORKLOAD_GUESTS).await?;
    let load = Load::start(c, &[], true);
    load.wait_acked(10, RUN_T).await?;

    let t_init = Instant::now(); // clock-exempt: measurement
    let st = c
        .cluster_init(1)
        .await
        .map_err(|e| f("promotion", format!("cluster init: {e}")))?;
    let init_took = t_init.elapsed();
    let _ = writeln!(
        out,
        "init: {init_took:?}; policy {}; members {}",
        st["policy"],
        st["members"].as_array().map_or(0, Vec::len)
    );
    load.wait_acked(5, RUN_T).await?;

    let t_join = Instant::now(); // clock-exempt: measurement
    let mut joined = Vec::new();
    for _ in 0..2 {
        let t = Instant::now(); // clock-exempt: measurement
        let id = c
            .add_node(NodeOpts {
                level_max: None,
                advertise: true,
            })
            .await
            .map_err(|e| f("promotion", format!("add_node: {e}")))?;
        let _ = writeln!(out, "join: node {id} ready after {:?}", t.elapsed());
        if t.elapsed() > Duration::from_secs(20) {
            let log = std::fs::read_to_string(c.log_path(id)).unwrap_or_default();
            let head: Vec<&str> = log.lines().take(80).collect();
            println!("--- slow join: node {id} log head ---\n{}", head.join("\n"));
            println!(
                "--- slow join: seed node 1 log tail ---\n{}",
                c.log_tail(1, 60)
            );
        }
        joined.push(id);
        load.refresh(c, &[]);
    }
    let joins_took = t_join.elapsed();
    let rf_after_joins = wait_rf3(c).await?;
    let to_rf3 = t_join.elapsed();
    let _ = writeln!(
        out,
        "time-to-RF-3: {to_rf3:?} from the first join (two joins {joins_took:?}, then RF 3 / ISR 3 / min_insync 2 + 3 voters after {rf_after_joins:?})"
    );
    let leaders: Vec<(u16, u64)> = published_all(c)
        .iter()
        .map(|a| (a.partition, a.leader))
        .collect();
    let _ = writeln!(out, "leaders before the kill: {leaders:?}");
    load.wait_acked(10, RUN_T).await?;

    // Lose the original node for real.
    load.refresh(c, &[1]);
    let t_kill = Instant::now(); // clock-exempt: measurement
    let k = c
        .kill9(1)
        .await
        .map_err(|e| f("harness", format!("kill -9 node 1: {e}")))?;
    // Every partition must be led by a survivor (node 1 stays a replica, out of the ISR).
    let failover = loop {
        let parts = published_all(c);
        if parts.iter().all(|a| a.leader != 0 && a.leader != 1) {
            break t_kill.elapsed();
        }
        if t_kill.elapsed() > HEAL_T {
            return Err(f(
                "availability",
                format!("partitions not led by survivors after the kill: {parts:?}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let meta = wait_meta_leader(c).await?;
    let _ = writeln!(
        out,
        "kill -9 node 1 ({k:?}): meta leader now {meta}; every partition led by a survivor after {failover:?}"
    );
    if meta == 1 {
        return Err(f("promotion", "the killed node still leads meta"));
    }
    load.wait_acked(10, RUN_T).await?;
    let res = load.finish(RUN_T).await?;
    let _ = writeln!(out, "{}", res.line());
    res.verdict()?;
    let oc = outcomes(c, &res.runs).await;
    let _ = writeln!(out, "outcomes: {}", oc.line());
    oc.verdict()?;
    let mut nodes = vec![1];
    nodes.extend(&joined);
    let fc = final_checks(c, &nodes, &res.acks()).await?;
    let _ = writeln!(out, "final checks: {fc}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "tier C: needs the failpoint binary (cargo build -p ostro --features failpoints --target-dir target/failpoints)"]
async fn promote_running_single_node_then_kill_it() {
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    let t0 = Instant::now(); // clock-exempt: measurement
    let spec = ClusterSpec {
        seed: 1901,
        ..ClusterSpec::single()
    };
    let mut c = ProcCluster::start_single_with(spec, ProcOptions::default(), None)
        .await
        .expect("start single node");
    let mut out = String::new();
    let _ = writeln!(
        out,
        "process_promotion (13-19, tier C): single node + workload -> init -> 2 joins -> RF 3 -> kill -9 node 1\n{}",
        host_line()
    );
    let r = body(&mut c, &mut out).await;
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
    r.expect("process_promotion");
}
