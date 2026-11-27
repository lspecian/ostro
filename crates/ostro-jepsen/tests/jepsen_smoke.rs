//! 14-22 Task 1 (QA-05, D-P14-19/20): the Jepsen smoke on the 1.0 configuration
//! (`#[ignore]`, release, the failpoint binary):
//!
//! ```text
//! cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
//! OSTRO_JEPSEN_SECS=180 OSTRO_JEPSEN_SEED=1401 cargo test --release -p ostro-jepsen --locked \
//!   --test jepsen_smoke -- --ignored --nocapture --test-threads=1
//! ```
//!
//! (`scripts/jepsen-run.sh smoke` wraps it.) Three `ostro server` processes (RF 3,
//! min_insync 2) behind the chaos proxy with `[auth] mode = "token"` (bootstrap admin from
//! `OSTRO_BOOTSTRAP_TOKEN` = `OSTRO_JEPSEN_TOKEN` or a fresh bearer) and `[audit] enabled =
//! true`; the crash-saga workload; every fault class interleaved every 5..=15 s from the
//! seed: partitions (majority/minority, bridge, one-way, leader isolation), kill -9
//! (leader, follower, meta leader, any), clock (bump ≤ ±60 s, strobe, drift ±5%) and disk
//! (fsync / commit / persist stalls of 100..=2,000 ms); an operator creating tenant tokens
//! and cancelling runs through random nodes every 30 s. After the run: the lost / duplicate
//! / fencing checkers (14-09) and every node's audit journal (torn tail repaired, chain
//! verified, every acknowledged operator action exactly once). `OSTRO_JEPSEN_SECS`
//! (default 180) and `OSTRO_JEPSEN_SEED` (default 1401) override. Report in
//! `target/jepsen/<seed>/`.

#[path = "support/p14.rs"]
mod p14;

use ostro_jepsen::run::run;
use ostro_repl_conformance::harness::process::{SKIP_NO_BIN, failpoint_bin};

/// The smoke's default seed.
const SEED: u64 = 1401;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "tier-C Jepsen smoke on the 1.0 configuration (needs the failpoint ostro binary)"]
async fn jepsen_smoke() {
    assert!(failpoint_bin().is_some(), "{SKIP_NO_BIN}");
    let (spec, op) = p14::spec(SEED, 180, None);
    let dir = spec.out_dir.clone();
    let classes: std::collections::BTreeSet<String> = spec
        .schedule()
        .slots
        .iter()
        .filter_map(|s| p14::class_of(&s.kind).map(str::to_owned))
        .collect();
    let report = run(spec).await.expect("jepsen run");
    let (audit, log) = p14::after_run(&report, &op, &dir).await;
    println!("{}", report.text());
    print!("{}", p14::audit_lines(&audit, &log));
    println!("faults by class: {:?}", p14::faults_by_class(&report));
    let (gap, at) = p14::max_unavailability(&dir);
    println!("max unavailability: {gap} ms (from {at} ms)");
    println!("report: {}", dir.join("report.json").display());

    for f in [
        "report.json",
        "history.jsonl",
        "nemesis.jsonl",
        "schedule.json",
    ] {
        assert!(dir.join(f).is_file(), "{} missing", dir.join(f).display());
    }
    p14::assert_run(&report, &audit, &log);
    // Every class the seeded schedule holds really ran (a skipped slot does not count).
    let ran = p14::faults_by_class(&report);
    for c in &classes {
        assert!(
            ran.get(c).is_some_and(|n| *n > 0),
            "fault class {c} was scheduled but never executed: {ran:?}"
        );
    }
}
