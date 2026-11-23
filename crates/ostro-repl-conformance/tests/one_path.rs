//! CLUS-01 group `one_path` (12-07). Pending scenarios print `PENDING <name>` unless
//! `OSTRO_REPL_RED_RUN` is set (then they must fail with a measured `ScenarioFailure`).

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_path() {
    ostro_repl_conformance::scenarios::runner::run_group_test("one_path").await;
}
