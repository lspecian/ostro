//! 14-23 (QA-04, D-P14-21, D16 profile A): the single-node benchmark of the 1.0
//! configuration (token auth, audit journal, tenant quotas on), every acknowledged step
//! verified on disk after `kill -9` (offline WAL scan) and after a restart (read back
//! through the API). `#[ignore]`; release:
//!
//! ```text
//! cargo build -p ostro --release --locked
//! cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
//! cargo test --release -p ostro-jepsen --locked --test bench_single -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Always asserts acked == on disk. With `OSTRO_BENCH_ENFORCE=1` also asserts
//! ≥ 10,000 verified steps/s (native NVMe box, D15). Writes `target/bench/single.json`
//! (workspace root). `OSTRO_BENCH_SECS` / `OSTRO_BENCH_WARMUP_SECS` / `OSTRO_BENCH_CLIENTS`
//! shorten or reshape it (defaults 60 / 10 / 64).

use ostro_jepsen::bench::{self, BenchSpec, SINGLE_MIN_STEPS_PER_S};
use ostro_repl_conformance::scenarios::process::skip_reason;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "benchmark: 10 s warm-up + 60 s measured, release build"]
async fn bench_single() {
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    let spec = BenchSpec::single().with_env();
    let r = bench::run(&spec).await.expect("benchmark run");
    println!("{}", r.table());
    let path = bench::write_report(&r, "single.json").expect("write target/bench/single.json");
    println!("report: {}", path.display());
    assert!(r.acked_runs > 0, "nothing acknowledged");
    assert!(
        r.violations.is_empty(),
        "acknowledged work missing on disk / not completed: {:?}",
        r.violations
    );
    assert_eq!(
        r.verified_runs, r.acked_runs,
        "every acknowledged run's steps are on disk"
    );
    assert_eq!(
        r.after_restart_verified,
        Some(r.acked_runs),
        "every acknowledged run reads back after the restart"
    );
    if r.enforced {
        assert!(
            r.steps_per_s >= SINGLE_MIN_STEPS_PER_S,
            "OSTRO_BENCH_ENFORCE: {:.1} verified steps/s < {SINGLE_MIN_STEPS_PER_S}",
            r.steps_per_s
        );
    }
}
