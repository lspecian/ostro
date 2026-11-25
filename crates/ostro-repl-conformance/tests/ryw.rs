//! Phase 13 tier-B `ryw` group (13-13, API-08 read half / D-P13-15). One test per scenario
//! so a single one can be run by name; `zombie_serve_without_lease_mutant_bites` exists only
//! with `--features failpoints`. The tests of this binary run one at a time: the mutant
//! failpoint and the consistency counters are process-wide.

use ostro_repl_conformance::scenarios::runner::{Outcome, run_scenario};
use ostro_repl_conformance::scenarios::ryw;

/// Serialises the scenarios of this binary.
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn run(name: &str) {
    let _one = ONE_AT_A_TIME.lock().await;
    let s = ryw::all()
        .into_iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("no ryw scenario {name}"));
    match run_scenario(&s).await {
        Outcome::Passed => {}
        other => panic!("{name}: {}", other.label()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ryw_across_nodes_steady() {
    run("ryw_across_nodes_steady").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ryw_across_nodes_through_moves() {
    run("ryw_across_nodes_through_moves").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ryw_through_failover() {
    run("ryw_through_failover").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn zombie_leader_never_serves_stale() {
    run("zombie_leader_never_serves_stale").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn tokens_skip_barrier() {
    run("tokens_skip_barrier").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn await_across_handoff() {
    run("await_across_handoff").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn await_across_leader_change() {
    run("await_across_leader_change").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn zombie_asymmetric() {
    run("zombie_asymmetric").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn strict_reads_mode() {
    run("strict_reads_mode").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn zombie_writes_are_retryable() {
    run("zombie_writes_are_retryable").await;
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn zombie_serve_without_lease_mutant_bites() {
    run("zombie_serve_without_lease_mutant_bites").await;
}
