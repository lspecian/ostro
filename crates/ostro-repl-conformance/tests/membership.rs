//! Phase 13 `membership` group (13-09, tier B): one test per scenario so a failure names it.
//! Run with `--test-threads=1` (each scenario starts a 3-node in-process cluster and joins up
//! to three more).

use ostro_repl_conformance::scenarios::membership;
use ostro_repl_conformance::scenarios::runner::{Outcome, run_scenario};

async fn run(name: &str) {
    let s = membership::all()
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
async fn join_grows_to_five_voters() {
    run("join_grows_to_five_voters").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shrink_to_three() {
    run("shrink_to_three").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn force_remove_dead_node() {
    run("force_remove_dead_node").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joiner_never_bootstraps() {
    run("joiner_never_bootstraps").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ids_never_reused() {
    run("ids_never_reused").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn meta_leader_removal_transfers_first() {
    run("meta_leader_removal_transfers_first").await;
}
