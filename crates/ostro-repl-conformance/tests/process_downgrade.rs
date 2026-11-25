//! 13-19 Task 2 (CLUS-06, D-P13-4/6, tier C): downgrade fences with real processes.
//! `#[ignore]`:
//!
//! ```text
//! cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
//! cargo test --release -p ostro-repl-conformance --features failpoints --locked \
//!   --test process_downgrade -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Three static processes start at `OSTRO_TEST_LEVEL_MAX=1` (finalized 1) under the
//! continuous workload.
//!
//! - **Before finalize, with a hold:** `POST /v1/cluster/upgrade/hold`, every node restarted
//!   at this build's level (SIGTERM + restart): finalized stays 1 (sampled); one upgraded
//!   process restarted at level 1 again serves (a run started through it completes).
//! - **After finalize:** that node back at this build's level, `POST
//!   /v1/cluster/upgrade/release` → finalized 2 on every node. One node is stopped, its
//!   partition files (`state/`, `wal/` and everything outside `meta/`, `cache/`, `tls/`)
//!   are snapshotted with their mtimes and lengths from the test process, and it is started
//!   at level 1: the process exits with a non-zero code before its ready line, the log
//!   carries "finalized by a newer Ostro", and not one partition file changed. Restarted at
//!   this build's level it serves again.
//!
//! Every acknowledged workload run completes. Report:
//! `$OSTRO_P13_REPORT_DIR/process_downgrade/report.txt`.

use std::fmt::Write as _;
use std::path::Path;
use std::time::Duration;

use ostro_repl_conformance::harness::ClusterHarness;
use ostro_repl_conformance::harness::p13::P13Harness;
use ostro_repl_conformance::harness::process::ProcCluster;
use ostro_repl_conformance::scenarios::process::skip_reason;
use ostro_repl_conformance::scenarios::process_p13::{
    HEAL_T, Load, ScenarioFailure, WORKLOAD_GUESTS, deploy_guests, finalized, host_line, outcomes,
    partition_files, post, start_static, stays_finalized, time_one_run, wait_finalized,
    wait_healthy, write_report,
};
use serde_json::json;
use tokio::time::Instant;

const TEST: &str = "process_downgrade";
const RUN_T: Duration = Duration::from_secs(120);

fn f(oracle: &'static str, detail: impl Into<String>) -> ScenarioFailure {
    ScenarioFailure {
        oracle,
        detail: detail.into(),
    }
}

/// Files under `root/<dir>` in a snapshot.
fn under(
    files: &std::collections::BTreeMap<std::path::PathBuf, (std::time::SystemTime, u64)>,
    dir: &str,
) -> usize {
    files
        .keys()
        .filter(|p| p.starts_with(Path::new(dir)))
        .count()
}

async fn upgrade_op(c: &ProcCluster, path: &str) -> Result<(), ScenarioFailure> {
    let t0 = Instant::now(); // clock-exempt: test timeout
    loop {
        let via = ostro_repl_conformance::scenarios::process::tools::live(c)[0];
        match post(c, via, path, &json!({})).await {
            Some((200, _)) => return Ok(()),
            Some((503, _)) | None if t0.elapsed() < RUN_T => {}
            other => return Err(f("upgrade", format!("POST {path}: {other:?}"))),
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn body(c: &mut ProcCluster, out: &mut String) -> Result<(), ScenarioFailure> {
    let ids = c.node_ids();
    deploy_guests(c, &WORKLOAD_GUESTS).await?;
    let load = Load::start(c, &[], false);
    load.wait_acked(10, RUN_T).await?;

    // Before finalize, with a hold.
    upgrade_op(c, "/v1/cluster/upgrade/hold").await?;
    for n in &ids {
        load.refresh(c, &[*n]);
        c.upgrade(*n, None)
            .await
            .map_err(|e| f("harness", format!("upgrade node {n}: {e}")))?;
        load.refresh(c, &[]);
        wait_healthy(c, &[], HEAL_T, &format!("upgrade of node {n}")).await?;
    }
    stays_finalized(c, &ids, 1, Duration::from_secs(5)).await?;
    let back = *ids.last().expect("nodes");
    load.refresh(c, &[back]);
    c.upgrade(back, Some(1))
        .await
        .map_err(|e| f("downgrade", format!("level-1 restart under the hold: {e}")))?;
    load.refresh(c, &[]);
    wait_healthy(c, &[], HEAL_T, "the level-1 restart").await?;
    let served = time_one_run(c, back, 0, "downgraded")
        .await
        .ok_or_else(|| {
            f(
                "downgrade",
                format!("node {back} at level 1 did not serve a run"),
            )
        })?;
    stays_finalized(c, &ids, 1, Duration::from_secs(2)).await?;
    let _ = writeln!(
        out,
        "before finalize (hold set): all nodes restarted at level 2, finalized stayed 1 (5 s sampled); node {back} restarted at level 1 serves (run completed in {served:?}); finalized still 1"
    );

    // After finalize: the boot fence.
    load.refresh(c, &[back]);
    c.upgrade(back, None)
        .await
        .map_err(|e| f("harness", format!("upgrade node {back}: {e}")))?;
    load.refresh(c, &[]);
    wait_healthy(c, &[], HEAL_T, "the re-upgrade").await?;
    upgrade_op(c, "/v1/cluster/upgrade/release").await?;
    let fin = wait_finalized(c, &ids, 2, Duration::from_secs(60)).await?;
    let _ = writeln!(out, "release -> finalized 2 on every node after {fin:?}");
    load.wait_acked(5, RUN_T).await?;

    let victim = back;
    load.refresh(c, &[victim]);
    c.stop_gracefully(victim)
        .await
        .map_err(|e| f("harness", format!("stop node {victim}: {e}")))?;
    let root = c.node_disk(victim).root;
    let before = partition_files(&root);
    if before.is_empty() {
        return Err(f("boot_fence", "no partition file to compare (vacuous)"));
    }
    let log_len = std::fs::metadata(c.log_path(victim)).map_or(0, |m| m.len());
    match c.upgrade(victim, Some(1)).await {
        Ok(()) => {
            return Err(f(
                "boot_fence",
                "a level-1 binary started on data finalized at level 2",
            ));
        }
        Err(e) => {
            let msg = e.to_string();
            let log = std::fs::read(c.log_path(victim)).unwrap_or_default();
            let tail = String::from_utf8_lossy(
                &log[usize::try_from(log_len).unwrap_or(0).min(log.len())..],
            )
            .to_string();
            if !msg.contains("exited before ready") || msg.contains("signal") {
                return Err(f("boot_fence", format!("not a clean non-zero exit: {msg}")));
            }
            if !tail.contains("finalized by a newer Ostro") {
                return Err(f(
                    "boot_fence",
                    format!("no \"finalized by a newer Ostro\" in the log: {tail}"),
                ));
            }
            let status = msg
                .split("exited before ready (")
                .nth(1)
                .and_then(|s| s.split(')').next())
                .unwrap_or("?");
            let _ = writeln!(
                out,
                "after finalize: node {victim} started at level 1 -> refused, process {status}, log: {}",
                tail.lines()
                    .find(|l| l.contains("finalized by a newer Ostro"))
                    .unwrap_or("")
                    .trim()
            );
        }
    }
    let after = partition_files(&root);
    if before != after {
        let changed: Vec<String> = after
            .iter()
            .filter(|(p, v)| before.get(*p) != Some(*v))
            .map(|(p, _)| p.display().to_string())
            .chain(
                before
                    .keys()
                    .filter(|p| !after.contains_key(*p))
                    .map(|p| format!("{} (removed)", p.display())),
            )
            .collect();
        return Err(f(
            "boot_fence",
            format!("the refused start touched partition files: {changed:?}"),
        ));
    }
    let _ = writeln!(
        out,
        "  partition files unchanged (mtime + length) from the test process: {} files ({} under state/, {} under wal/)",
        before.len(),
        under(&before, "state"),
        under(&before, "wal")
    );
    c.upgrade(victim, None)
        .await
        .map_err(|e| f("harness", format!("restart node {victim} at level 2: {e}")))?;
    load.refresh(c, &[]);
    wait_healthy(c, &[], HEAL_T, "the restart at level 2").await?;
    let again = time_one_run(c, victim, 1, "after-fence")
        .await
        .ok_or_else(|| {
            f(
                "availability",
                format!("node {victim} did not serve after the fence"),
            )
        })?;
    let _ = writeln!(
        out,
        "  node {victim} restarted at level 2 serves again (run in {again:?}); finalized {:?}",
        finalized(c, &ids).await
    );
    load.wait_acked(5, RUN_T).await?;
    let res = load.finish(RUN_T).await?;
    let _ = writeln!(out, "{}", res.line());
    res.verdict()?;
    let oc = outcomes(c, &res.runs).await;
    let _ = writeln!(out, "outcomes: {}", oc.line());
    oc.verdict()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "tier C: needs the failpoint binary (cargo build -p ostro --features failpoints --target-dir target/failpoints)"]
async fn downgrade_fences_with_real_processes() {
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    let t0 = Instant::now(); // clock-exempt: measurement
    let mut c = start_static(1914, Some(1))
        .await
        .expect("start level-1 cluster");
    let mut out = String::new();
    let _ = writeln!(
        out,
        "process_downgrade (13-19, tier C): 3 static processes at OSTRO_TEST_LEVEL_MAX=1\n{}",
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
    r.expect("process_downgrade");
}
