//! Phase 13 tier-B group `module_gc` (13-15, CLUS-07 / D-P13-20): module retirement and
//! reference-checked module GC on a 3-node RF=3 cluster (scenarios in
//! `src/scenarios/module_gc.rs`).

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn module_gc() {
    ostro_repl_conformance::scenarios::runner::run_group_test("module_gc").await;
}
