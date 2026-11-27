//! 14-23 (QA-04, D-P14-21, D16 profile A): the RF=3 benchmark of the 1.0 configuration
//! (three loopback processes, `min_insync` 2; token auth, audit journal, tenant quotas on).
//! After the load the cluster is stopped with `kill -9` and every replica's WAL is scanned
//! offline: each acknowledged run needs its `StartRun` and its 10 `StepCompleted` events on
//! at least `min_insync` replicas. `#[ignore]`; release:
//!
//! ```text
//! cargo build -p ostro --release --locked
//! cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
//! cargo test --release -p ostro-jepsen --locked --test bench_rf3 -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Commit latency is client-side (start request -> start ack): no Phase 12 commit-latency
//! histogram exists (14-01). Always asserts acked == on disk. With `OSTRO_BENCH_ENFORCE=1`
//! also asserts ≥ 5,000 verified steps/s and commit p99 < 20 ms. Writes
//! `target/bench/rf3.json` (workspace root).

use ostro_jepsen::bench::{self, BenchSpec, RF3_MAX_COMMIT_P99_MS, RF3_MIN_STEPS_PER_S};
use ostro_repl_conformance::scenarios::process::skip_reason;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "benchmark: 3 processes, 10 s warm-up + 60 s measured, release build"]
async fn bench_rf3() {
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    let spec = BenchSpec::rf3().with_env();
    let r = bench::run(&spec).await.expect("benchmark run");
    println!("{}", r.table());
    let path = bench::write_report(&r, "rf3.json").expect("write target/bench/rf3.json");
    println!("report: {}", path.display());
    assert!(r.acked_runs > 0, "nothing acknowledged");
    assert!(
        r.violations.is_empty(),
        "acknowledged work missing on disk / not completed: {:?}",
        r.violations
    );
    assert_eq!(
        r.verified_runs, r.acked_runs,
        "every acknowledged run's steps are on >= min_insync replicas"
    );
    if r.enforced {
        assert!(
            r.steps_per_s >= RF3_MIN_STEPS_PER_S,
            "OSTRO_BENCH_ENFORCE: {:.1} verified steps/s < {RF3_MIN_STEPS_PER_S}",
            r.steps_per_s
        );
        assert!(
            r.commit_p99_ms < RF3_MAX_COMMIT_P99_MS,
            "OSTRO_BENCH_ENFORCE: commit p99 {:.2} ms >= {RF3_MAX_COMMIT_P99_MS} ms",
            r.commit_p99_ms
        );
    }
}
