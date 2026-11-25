//! Phase 13 tier-B group `migrate` (13-17, CLUS-07 / D-P13-21): `POST /v1/runs/migrate` on a
//! 3-node RF=3 cluster (scenarios in `src/scenarios/migrate.rs`). One test per scenario so a
//! single one can be run by name. The tests of this binary run one at a time: the migrate
//! before-submit hook and the failpoints are process-wide.
//!
//! Failpoints build: `concurrent_growth_mutant_bites` runs `concurrent_growth_seq_mismatch`
//! under `migrate::mutant::skip_seq_check` and requires it to FAIL (the grown journal is
//! repinned instead of `seq_mismatch`).

use ostro_repl_conformance::scenarios::migrate;
use ostro_repl_conformance::scenarios::runner::{Outcome, run_scenario};

/// Serialises the scenarios of this binary.
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn outcome(name: &str) -> Outcome {
    let s = migrate::all()
        .into_iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("no migrate scenario {name}"));
    run_scenario(&s).await
}

async fn run(name: &str) {
    let _one = ONE_AT_A_TIME.lock().await;
    match outcome(name).await {
        Outcome::Passed => {}
        other => panic!("{name}: {}", other.label()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn passing_runs_repinned_cluster_wide() {
    run("passing_runs_repinned_cluster_wide").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn diverging_runs_never_repinned() {
    run("diverging_runs_never_repinned").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrent_growth_seq_mismatch() {
    run("concurrent_growth_seq_mismatch").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn busy_runs_reported() {
    run("busy_runs_reported").await;
}

/// Three nodes at the emulated level 1 (two joined through the real join flow), two
/// upgraded: refused while one node is at level 1, repinned after the last upgrade.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn migrate_refused_before_finalize() {
    let _one = ONE_AT_A_TIME.lock().await;
    let r = tokio::time::timeout(
        std::time::Duration::from_secs(420),
        migrate::refused_before_finalize(3),
    )
    .await
    .expect("deadline");
    if let Err(f) = r {
        panic!("migrate_refused_before_finalize: {f}");
    }
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrent_growth_mutant_bites() {
    let _one = ONE_AT_A_TIME.lock().await;
    let name = ostro_server::control::migrate::MUTANT_SKIP_SEQ_CHECK;
    fail::cfg(name, "return").expect("fail cfg");
    let o = outcome("concurrent_growth_seq_mismatch").await;
    fail::remove(name);
    match o {
        Outcome::Failed(f) => {
            assert!(
                f.detail.contains("seq_mismatch"),
                "mutant bites, but how: {f}"
            );
        }
        other => panic!("the mutant did not bite: {}", other.label()),
    }
}
