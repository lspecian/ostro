//! Phase 13 `reassign` group (13-10, tier B): one test per scenario so a failure names it.
//! Run with `--test-threads=1` (each scenario starts an in-process cluster of 1–4 nodes).

use ostro_repl_conformance::scenarios::reassign;
use ostro_repl_conformance::scenarios::runner::{Outcome, run_scenario};

async fn run(name: &str) {
    let s = reassign::all()
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
async fn rf_raised_on_join() {
    run("rf_raised_on_join").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_evacuates_replicas() {
    run("drain_evacuates_replicas").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_reassign_and_cancel() {
    run("explicit_reassign_and_cancel").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_failover_mid_move() {
    run("controller_failover_mid_move").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn move_blocked_before_finalize() {
    run("move_blocked_before_finalize").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_replica_stops_and_cleans() {
    run("removed_replica_stops_and_cleans").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn readded_replica_truncates() {
    run("readded_replica_truncates").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn force_removed_dead_node_evacuated() {
    run("force_removed_dead_node_evacuated").await;
}
