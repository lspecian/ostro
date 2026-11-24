//! 12-17 tier C: the process half of the mutant matrix. `repl::mutant::count_nondurable_leo`
//! cannot bite in tier B (v1 followers fetch after durability, and an in-process crash keeps
//! the page cache), so it is caught by `follower_power_loss` on real processes: every node is
//! started with `FAILPOINTS=repl::mutant::count_nondurable_leo=return` (the follower reports
//! its fetched LEO as durable and does not wait; the leader's tracker counts it), and the
//! scenario must fail with a measured oracle failure. The baseline (no mutant) must pass.
//! A harness error never counts as a bite (W11). Needs the failpoint binary.

use ostro_log::mutants::COUNT_NONDURABLE_LEO;
use ostro_repl_conformance::harness::process::ProcOptions;
use ostro_repl_conformance::mutants;
use ostro_repl_conformance::scenarios::process::{run_proc, scenario, skip_reason};
use ostro_repl_conformance::scenarios::runner::Outcome;

/// Mutant attempts before "did not bite" is reported (the bite needs the paused follower to
/// be the one the leader acknowledges on, which the scenario arranges every time; the bound
/// only absorbs harness errors).
const ATTEMPTS: usize = 3;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "tier C: count_nondurable_leo baseline + mutant on real processes (~2 min)"]
async fn count_nondurable_leo_bites_on_follower_power_loss() {
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    let entry = mutants::entry(COUNT_NONDURABLE_LEO).expect("mapped");
    let name = entry.tier_c.expect("count_nondurable_leo is tier C");
    let s = scenario(name).expect("registered tier-C scenario");

    let base = run_proc(&s, ProcOptions::default()).await;
    println!("BASELINE {name}: {}", base.label());
    assert!(
        matches!(base, Outcome::Passed),
        "baseline {name} must pass: {}",
        base.label()
    );

    let opts = ProcOptions {
        failpoints: Some(format!("{COUNT_NONDURABLE_LEO}=return")),
        ..ProcOptions::default()
    };
    let mut last = String::new();
    for attempt in 1..=ATTEMPTS {
        match run_proc(&s, opts.clone()).await {
            Outcome::Failed(f) => {
                println!("MUTANT {COUNT_NONDURABLE_LEO}: {name} FAILED as expected ({f})");
                return;
            }
            other => {
                last = other.label();
                println!("MUTANT {COUNT_NONDURABLE_LEO}: attempt {attempt}: {last}");
            }
        }
    }
    panic!("MUTANT {COUNT_NONDURABLE_LEO}: {name} did not bite in {ATTEMPTS} attempts: {last}");
}
