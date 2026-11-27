//! 14-23 (QA-05 soak half, D-P14-22): the soak smoke. Runs the `ostro-soak` driver
//! in-process on one node at `OSTRO_SOAK_LOAD` (default 0.7) of the rated load for
//! `OSTRO_SOAK_SECS` (default 300; 1800 at the gate), sampling every 30 s after a 60 s
//! warm-up (the smoke values of `docs/soak.md`), and asserts the verdict is PASS. The rated
//! load is measured first (a 60 s `bench` run on the same machine) unless
//! `OSTRO_SOAK_RATED=<steps/s>` gives it. Output: `target/soak/{samples.jsonl,verdict.json}`
//! relative to the workspace root. `#[ignore]`; release:
//!
//! ```text
//! cargo build -p ostro --release --locked
//! cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
//! OSTRO_SOAK_SECS=300 cargo test --release -p ostro-jepsen --locked --test soak_smoke -- --ignored --nocapture --test-threads=1
//! ```

use std::time::Duration;

use ostro_jepsen::bench;
use ostro_jepsen::report::workspace_root;
use ostro_jepsen::soak::{self, LOAD_ENV, RATED_ENV, Rated, SECS_ENV, SoakSpec};
use ostro_repl_conformance::scenarios::process::skip_reason;

fn env_f64(name: &str) -> Option<f64> {
    std::env::var(name).ok().and_then(|v| v.trim().parse().ok())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "soak smoke: rated bench (70 s) + OSTRO_SOAK_SECS (default 300 s), release build"]
async fn soak_smoke() {
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    let secs = env_f64(SECS_ENV).unwrap_or(300.0);
    let spec = SoakSpec {
        nodes: 1,
        duration: Duration::from_secs_f64(secs),
        load: env_f64(LOAD_ENV).unwrap_or(0.7),
        rated: env_f64(RATED_ENV).map_or(Rated::Auto, Rated::Fixed),
        interval: Duration::from_secs(30),
        warmup: Some(Duration::from_secs(60)),
        out: workspace_root().join("target").join("soak"),
        seed: 1426,
        bin: bench::default_bin(),
        partitions: None,
    };
    let v = soak::run(&spec).await.expect("soak run");
    println!("{}", v.summary());
    println!("verdict: {}", spec.out.join("verdict.json").display());
    assert!(v.passed(), "soak smoke verdict FAIL:\n{}", v.summary());
}
