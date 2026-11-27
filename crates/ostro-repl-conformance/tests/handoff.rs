//! Phase 13 group `handoff` (13-11, tier B): graceful handoff under load, an aborted
//! handoff to a lagging target, controlled shutdown and a rolling restart without drain;
//! 14-33: the controlled-shutdown ISR self-shrink (followed partitions keep committing, the
//! `min_insync` guard, the re-add after a fresh catch-up).

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handoff() {
    ostro_repl_conformance::scenarios::runner::run_group_test("handoff").await;
}
