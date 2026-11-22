//! Phase 12: implemented in 12-03 (scenario runner: fresh cluster per scenario,
//! `cluster_formed` first, hard deadline, pending handling, `red_run_all`).

/// Run every scenario in red-run mode (`OSTRO_REPL_RED_RUN=1`): pending ⇒ must fail with a
/// `ScenarioFailure`, non-pending ⇒ must pass. Called by `tests/red_run.rs`.
pub async fn red_run_all() {
    panic!("Phase 12: implemented in 12-03");
}
