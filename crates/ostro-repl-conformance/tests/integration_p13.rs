//! Phase 13 `integration_p13` group (13-18, tier B): one test per scenario so a failure names
//! it. Run with `--test-threads=1`. `start_during_gc_race` needs `--features failpoints`.

use ostro_repl_conformance::scenarios::integration_p13;
use ostro_repl_conformance::scenarios::runner::{Outcome, run_scenario};

async fn run(name: &str) {
    let s = integration_p13::all()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn drain_node_with_replicas() {
    run("drain_node_with_replicas").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn remove_replica_holding_voter() {
    run("remove_replica_holding_voter").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ryw_through_handoffs_and_reassignment() {
    run("ryw_through_handoffs_and_reassignment").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn emulated_rolling_upgrade_under_load() {
    run("emulated_rolling_upgrade_under_load").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn reject_continue_never_applies_newer_commands() {
    run("reject_continue_never_applies_newer_commands").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn dead_voter_replaced_by_learner() {
    run("dead_voter_replaced_by_learner").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn explicit_joint_left_after_leader_kill() {
    run("explicit_joint_left_after_leader_kill").await;
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn start_during_gc_race() {
    run("start_during_gc_race").await;
}

#[test]
fn every_scenario_has_a_test() {
    let mut names: Vec<&str> = integration_p13::all().iter().map(|s| s.name).collect();
    names.sort_unstable();
    let mut want = vec![
        "dead_voter_replaced_by_learner",
        "drain_node_with_replicas",
        "emulated_rolling_upgrade_under_load",
        "explicit_joint_left_after_leader_kill",
        "reject_continue_never_applies_newer_commands",
        "remove_replica_holding_voter",
        "ryw_through_handoffs_and_reassignment",
    ];
    if cfg!(feature = "failpoints") {
        want.push("start_during_gc_race");
    }
    want.sort_unstable();
    assert_eq!(names, want);
}
