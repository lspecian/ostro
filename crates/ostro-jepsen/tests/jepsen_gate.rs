//! 14-22 Task 2 (QA-05, D-P14-19/20): the Jepsen gate campaign on the 1.0 configuration
//! (`#[ignore]`, release, the failpoint binary):
//!
//! ```text
//! cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
//! OSTRO_JEPSEN_SECS=1800 OSTRO_JEPSEN_SEEDS=1402,1403 cargo test --release -p ostro-jepsen \
//!   --locked --test jepsen_gate -- --ignored --nocapture --test-threads=1
//! ```
//!
//! (`scripts/jepsen-run.sh gate` wraps it.) The smoke's configuration (token auth with the
//! bootstrap bearer, audit on, every fault class every 5..=15 s, the operator) for each seed
//! of `OSTRO_JEPSEN_SEEDS` (default `1402,1403`; `OSTRO_JEPSEN_SEED` = a single seed) and
//! `OSTRO_JEPSEN_SECS` (default 1800) per seed, plus two pinned faults per run: one
//! quorum-loss window (every node cut from both others for 20 s at a third of the fault
//! phase; the checkers expect unavailability, not loss) and one rolling restart of all three
//! nodes (SIGTERM, restart, healthy, next) at two thirds. Every seed runs even when an
//! earlier one fails; `target/jepsen/gate-summary.txt` aggregates per seed: ops, acked
//! runs, faults by class, verdicts, audit counts and the longest unavailability window.

#[path = "support/p14.rs"]
mod p14;

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use ostro_jepsen::report;
use ostro_jepsen::run::{NemesisKind, SEED_ENV, run};
use ostro_repl_conformance::harness::process::{SKIP_NO_BIN, failpoint_bin};

/// Seeds of the gate (comma separated).
const SEEDS_ENV: &str = "OSTRO_JEPSEN_SEEDS";
/// The default gate seeds.
const SEEDS: [u64; 2] = [1402, 1403];
/// Quorum-loss window length.
const QUORUM_LOSS_HOLD: Duration = Duration::from_secs(20);

fn seeds() -> Vec<u64> {
    if let Ok(s) = std::env::var(SEEDS_ENV) {
        let v: Vec<u64> = s.split(',').filter_map(|x| x.trim().parse().ok()).collect();
        assert!(!v.is_empty(), "{SEEDS_ENV}={s}: no seed");
        return v;
    }
    if let Some(s) = std::env::var(SEED_ENV)
        .ok()
        .and_then(|s| s.trim().parse().ok())
    {
        return vec![s];
    }
    SEEDS.to_vec()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Jepsen gate campaign (2 seeds x 30 min by default; needs the failpoint ostro binary)"]
async fn jepsen_gate() {
    assert!(failpoint_bin().is_some(), "{SKIP_NO_BIN}");
    let summary_path = report::out_root().join("gate-summary.txt");
    let mut summary =
        String::from("ostro-jepsen gate (14-22): 3 nodes, RF 3, auth token + audit\n");
    let mut failures = Vec::new();
    for seed in seeds() {
        let (mut spec, op) = p14::spec(seed, 1800, Some(seed));
        let d = spec.duration;
        spec.pinned.push((
            d / 3,
            QUORUM_LOSS_HOLD,
            NemesisKind::Custom(Arc::new(p14::QuorumLossPartition)),
        ));
        spec.pinned.push((
            d * 2 / 3,
            Duration::ZERO,
            NemesisKind::Custom(Arc::new(p14::RollingRestart)),
        ));
        let dir = spec.out_dir.clone();
        let rep = run(spec).await;
        let rep = match rep {
            Ok(r) => r,
            Err(e) => {
                let _ = op.finish().await;
                let _ = writeln!(summary, "\nseed {seed}: HARNESS ERROR {e}");
                failures.push(format!("seed {seed}: {e}"));
                continue;
            }
        };
        let (audit, log) = p14::after_run(&rep, &op, &dir).await;
        println!("{}", rep.text());
        print!("{}", p14::audit_lines(&audit, &log));
        let (gap, at) = p14::max_unavailability(&dir);
        let acked = rep.ops.get("start/ok").copied().unwrap_or(0);
        let verdicts: Vec<String> = rep.verdicts.iter().map(|v| v.line()).collect();
        let pass = rep.passed && audit.ok();
        let _ = writeln!(
            summary,
            "\nseed {seed} | {} s of faults | {}\nreport: {}\nops: {:?}\nacked runs: {acked}\nfaults: {:?}\nfaults by class: {:?}\n{}\n{}max unavailability window: {gap} ms (from {at} ms on the history clock)",
            rep.duration_secs,
            if pass { "PASS" } else { "FAIL" },
            dir.join("report.json").display(),
            rep.ops,
            rep.faults,
            p14::faults_by_class(&rep),
            verdicts.join("\n"),
            p14::audit_lines(&audit, &log),
        );
        std::fs::create_dir_all(report::out_root()).expect("out root");
        std::fs::write(&summary_path, &summary).expect("gate summary");
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            p14::assert_run(&rep, &audit, &log);
            for k in ["partition:quorum_loss", "restart:rolling"] {
                assert!(
                    rep.faults.get(k).is_some_and(|n| *n == 1),
                    "pinned fault {k} did not run exactly once: {:?}",
                    rep.faults
                );
            }
        }));
        if let Err(p) = r {
            let msg = p
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| p.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                .unwrap_or_default();
            failures.push(format!("seed {seed}: {msg}"));
        }
    }
    std::fs::create_dir_all(report::out_root()).expect("out root");
    std::fs::write(&summary_path, &summary).expect("gate summary");
    println!("{summary}\ngate summary: {}", summary_path.display());
    assert!(
        failures.is_empty(),
        "gate failures:\n{}",
        failures.join("\n")
    );
}
