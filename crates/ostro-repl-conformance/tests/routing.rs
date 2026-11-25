//! Phase 13 tier-B `routing` group (13-12, API-08 / D-P13-14). One test per scenario so a
//! single one can be run by name; `hop_limit_no_loop` exists only with
//! `--features failpoints` (failpoint `route::stale_view`). The tests of this binary run
//! one at a time: the failpoint is process-wide.

use ostro_repl_conformance::scenarios::routing;
use ostro_repl_conformance::scenarios::runner::{Outcome, run_scenario};

/// Serialises the scenarios of this binary (`route::stale_view` is process-wide).
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn run(name: &str) {
    let _one = ONE_AT_A_TIME.lock().await;
    let s = routing::all()
        .into_iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("no routing scenario {name}"));
    match run_scenario(&s).await {
        Outcome::Passed => {}
        other => panic!("{name}: {}", other.label()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn parity_through_every_node() {
    run("parity_through_every_node").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn not_leader_retry_after_move() {
    run("not_leader_retry_after_move").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn offline_partition_503() {
    run("offline_partition_503").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn unkeyed_start_zero_hops_when_local() {
    run("unkeyed_start_zero_hops_when_local").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn list_fanout_complete() {
    run("list_fanout_complete").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn internal_consumers_route() {
    run("internal_consumers_route").await;
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn hop_limit_no_loop() {
    run("hop_limit_no_loop").await;
}
