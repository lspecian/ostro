//! 14-09 Task 2: a short seeded Jepsen run (`#[ignore]`, release):
//!
//! ```text
//! cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
//! OSTRO_JEPSEN_SECS=180 cargo test --release -p ostro-jepsen --locked --test harness_smoke \
//!   -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Three `ostro server` processes (RF 3, min_insync 2) behind the chaos proxy, 8 crash-saga
//! clients, partition (majority/minority, bridge, one-way, leader isolation) and kill -9
//! (leader, follower, meta leader, any) nemeses for `OSTRO_JEPSEN_SECS` (default 180) with a
//! fixed seed (`OSTRO_JEPSEN_SEED` overrides). Every checker must pass, non-vacuously (runs
//! acknowledged and checked, consistency tokens checked, logs re-applied, faults executed),
//! and the report must exist. Generating the schedule a second time from the same seed must
//! reproduce `schedule.json` byte for byte.

use ostro_jepsen::run::{NemesisKind, RunSpec, run};
use ostro_repl_conformance::harness::process::{SKIP_NO_BIN, failpoint_bin};

/// The smoke's fixed seed.
const SEED: u64 = 1409;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "tier-C Jepsen smoke (needs the failpoint ostro binary); run explicitly"]
async fn harness_smoke() {
    assert!(failpoint_bin().is_some(), "{SKIP_NO_BIN}");
    let spec = RunSpec::from_env(Some(SEED), 180, NemesisKind::partitions_and_kills());
    let dir = spec.out_dir.clone();
    let report = run(spec.clone()).await.expect("jepsen run");
    println!("{}", report.text());

    for f in [
        "report.json",
        "report.txt",
        "history.jsonl",
        "nemesis.jsonl",
        "schedule.json",
    ] {
        assert!(dir.join(f).is_file(), "{} missing", dir.join(f).display());
    }
    assert!(report.passed, "checkers failed:\n{}", report.text());
    let lost = report.verdict("lost").expect("lost verdict");
    let fencing = report.verdict("fencing").expect("fencing verdict");
    let duplicate = report.verdict("duplicate").expect("duplicate verdict");
    assert!(
        lost.count("runs_checked") > 0,
        "no acknowledged run checked"
    );
    assert!(lost.count("completed") > 0, "no run completed");
    assert!(
        fencing.count("tokens_checked") > 0,
        "no consistency token checked"
    );
    assert!(fencing.count("commands_reapplied") > 0, "no log re-applied");
    assert!(
        duplicate.count("records_compared") > 0,
        "no replica record compared"
    );
    assert!(
        report.faults.values().sum::<u64>() > 0,
        "no fault executed: {:?}",
        report.faults
    );

    // Same seed, same schedule (a fresh generation compared with the run's file).
    let again = RunSpec::from_env(Some(SEED), 180, NemesisKind::partitions_and_kills()).schedule();
    let file = std::fs::read_to_string(dir.join("schedule.json")).expect("schedule.json");
    assert_eq!(
        again.to_json(),
        file,
        "the schedule is not reproducible from the seed"
    );
}
