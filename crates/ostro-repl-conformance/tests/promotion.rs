//! Phase 13 `promotion` group (13-16, tier B): one test per scenario so a failure names it.
//! Run with `--test-threads=1` (each scenario starts a single in-process node and grows it).

use ostro_repl_conformance::scenarios::promotion;
use ostro_repl_conformance::scenarios::runner::{Outcome, run_scenario};

async fn run(name: &str) {
    let s = promotion::all()
        .into_iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("no scenario {name}"));
    let started = std::time::Instant::now(); // clock-exempt: report timing
    let outcome = run_scenario(&s).await;
    println!("{name}: {} in {:?}", outcome.label(), started.elapsed());
    assert!(
        matches!(outcome, Outcome::Passed),
        "{name}: {}",
        outcome.label()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promote_running_single_node_to_three() {
    run("promote_running_single_node_to_three").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promote_refused_in_dev() {
    run("promote_refused_in_dev").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promote_then_grow_to_five() {
    run("promote_then_grow_to_five").await;
}
