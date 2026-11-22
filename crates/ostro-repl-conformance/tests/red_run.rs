//! Red-before-green evidence (D-P12-19): `OSTRO_REPL_RED_RUN=1 cargo test -p
//! ostro-repl-conformance --test red_run -- --ignored`. Implemented by 12-03's runner.

#[tokio::test(flavor = "multi_thread")]
#[ignore = "explicit red run (OSTRO_REPL_RED_RUN=1)"]
async fn red_run() {
    ostro_repl_conformance::scenarios::runner::red_run_all().await;
}
