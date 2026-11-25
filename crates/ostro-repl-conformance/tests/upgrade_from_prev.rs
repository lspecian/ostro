//! 13-19 Task 2 (CLUS-06, D-P13-28): the real N−1 upgrade, gated on `OSTRO_PREV_BIN`.
//! `#[ignore]`; human_needed until the first release tag exists:
//!
//! ```text
//! cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
//! OSTRO_PREV_BIN=/path/to/ostro-<previous release> cargo test --release \
//!   -p ostro-repl-conformance --features failpoints --locked --test upgrade_from_prev \
//!   -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Without `OSTRO_PREV_BIN` the test prints `SKIP: OSTRO_PREV_BIN not set (human_needed until
//! the first release tag)` and passes. With it: three static processes start from the
//! previous binary (no level emulation: its own level), the continuous mixed workload runs,
//! and each node in turn gets SIGTERM and restarts from this build's failpoint binary. Then
//! the cluster must finalize at this build's level on every node, every acknowledged run
//! completes exactly once, and the Phase 12 final checks hold. Report:
//! `$OSTRO_P13_REPORT_DIR/upgrade_from_prev/report.txt`.

use std::fmt::Write as _;
use std::path::PathBuf;
use std::time::Duration;

use ostro_repl_conformance::harness::ClusterHarness;
use ostro_repl_conformance::harness::p13::P13Harness;
use ostro_repl_conformance::harness::process::{ProcCluster, ProcOptions};
use ostro_repl_conformance::scenarios::process::skip_reason;
use ostro_repl_conformance::scenarios::process_p13::{
    HEAL_T, Load, ScenarioFailure, WORKLOAD_GUESTS, deploy_guests, final_checks, finalized,
    host_line, outcomes, spec3, wait_finalized, wait_healthy, write_report,
};
use tokio::time::Instant;

const TEST: &str = "upgrade_from_prev";
const RUN_T: Duration = Duration::from_secs(120);
/// The skip line without `OSTRO_PREV_BIN`.
const SKIP: &str = "SKIP: OSTRO_PREV_BIN not set (human_needed until the first release tag)";

fn f(oracle: &'static str, detail: impl Into<String>) -> ScenarioFailure {
    ScenarioFailure {
        oracle,
        detail: detail.into(),
    }
}

async fn body(c: &mut ProcCluster, out: &mut String) -> Result<(), ScenarioFailure> {
    let ids = c.node_ids();
    wait_healthy(c, &[], HEAL_T, "start on the previous binary").await?;
    let start_levels = finalized(c, &ids).await;
    let _ = writeln!(out, "previous binary: finalized {start_levels:?}");
    deploy_guests(c, &WORKLOAD_GUESTS).await?;
    let load = Load::start(c, &[], true);
    load.wait_acked(10, RUN_T).await?;
    for n in &ids {
        c.set_bin(*n, None);
        load.refresh(c, &[*n]);
        let t = Instant::now(); // clock-exempt: measurement
        c.upgrade(*n, None)
            .await
            .map_err(|e| f("harness", format!("upgrade node {n} to this build: {e}")))?;
        load.refresh(c, &[]);
        let h = wait_healthy(c, &[], HEAL_T, &format!("upgrade of node {n}")).await?;
        let _ = writeln!(
            out,
            "node {n}: previous -> this build in {:?} (+{h:?} to healthy)",
            t.elapsed()
        );
        load.wait_acked(5, RUN_T).await?;
    }
    let level = ostro_types::compat::LEVEL;
    let fin = wait_finalized(c, &ids, level, Duration::from_secs(60)).await?;
    let _ = writeln!(
        out,
        "finalized {level} on every node {fin:?} after the last upgrade"
    );
    let res = load.finish(RUN_T).await?;
    let _ = writeln!(out, "{}", res.line());
    res.verdict()?;
    let oc = outcomes(c, &res.runs).await;
    let _ = writeln!(out, "outcomes: {}", oc.line());
    oc.verdict()?;
    let fc = final_checks(c, &ids, &res.acks()).await?;
    let _ = writeln!(out, "final checks: {fc}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "tier C, env-gated: OSTRO_PREV_BIN (human_needed until the first release tag)"]
async fn rolling_upgrade_from_the_previous_binary() {
    let prev = match std::env::var_os("OSTRO_PREV_BIN") {
        Some(p) if !p.is_empty() => PathBuf::from(p),
        _ => {
            println!("{SKIP}");
            return;
        }
    };
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    assert!(
        prev.is_file(),
        "OSTRO_PREV_BIN {} is not a file",
        prev.display()
    );
    let t0 = Instant::now(); // clock-exempt: measurement
    let opts = ProcOptions {
        bin: Some(prev.clone()),
        ..ProcOptions::default()
    };
    let mut c = ProcCluster::start_with(spec3(1916), opts)
        .await
        .expect("start the cluster on the previous binary");
    let mut out = String::new();
    let _ = writeln!(
        out,
        "upgrade_from_prev (13-19, tier C): 3 static processes from {} -> this build\n{}",
        prev.display(),
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
    r.expect("upgrade_from_prev");
}
