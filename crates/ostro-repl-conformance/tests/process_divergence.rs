//! 12-17 tier C: SC2 with real process death. `process_leader_dies_with_unreplicated_writes`
//! runs `OSTRO_PROC_ITERS` times (default 10) on fresh three-process clusters; every run must
//! pass. Needs the failpoint binary (`OSTRO_FAILPOINT_BIN` or `target/failpoints/debug/ostro`);
//! prints a `SKIP:` line otherwise.

use ostro_repl_conformance::harness::process::ProcOptions;
use ostro_repl_conformance::scenarios::process::{run_proc, scenario, skip_reason};
use ostro_repl_conformance::scenarios::runner::Outcome;

fn iters() -> usize {
    std::env::var("OSTRO_PROC_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "tier C: kill -9 of a leader holding unreplicated writes (~30 s per iteration)"]
async fn process_leader_dies_with_unreplicated_writes() {
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    let s = scenario("process_leader_dies_with_unreplicated_writes").expect("registered");
    let n = iters();
    for i in 1..=n {
        let t0 = std::time::Instant::now();
        let out = run_proc(&s, ProcOptions::default()).await;
        println!(
            "{} iteration {i}/{n}: {} ({:?})",
            s.name,
            out.label(),
            t0.elapsed()
        );
        assert!(
            matches!(out, Outcome::Passed),
            "{}: {}",
            s.name,
            out.label()
        );
    }
}
