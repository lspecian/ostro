//! Runs `hello` against a throwaway `ostro dev --ephemeral --time-skipping` server: the
//! three-day sleep is skipped automatically, so the test finishes in seconds.
//!
//! Needs the `ostro` binary (`OSTRO_BIN` or `PATH`). Run with `cargo test`.

use ostro_client::testing::TestEnv;
use serde_json::json;

const THREE_DAYS_MS: u64 = 3 * 24 * 60 * 60 * 1000;

#[tokio::test]
async fn hello_completes_after_three_days_and_approval() {
    let env = TestEnv::start().await.expect("start ostro dev");
    env.build_and_deploy(env!("CARGO_MANIFEST_DIR"))
        .await
        .expect("build and deploy");

    let run = env
        .start_run("hello", json!({ "name": "ada" }))
        .await
        .expect("start hello");
    env.signal(&run, "approve", json!({ "ok": true }))
        .await
        .expect("send approve");

    // `result` waits for the run; idle time is skipped (the 3-day sleep).
    let out = env.result(&run, "30s").await.expect("hello completes");
    assert_eq!(out["greeting"], "hello ada");
    assert_eq!(out["approved"], true);

    let now = env.now_ms().await.expect("server time");
    assert!(
        now - run.accepted_at_ms >= THREE_DAYS_MS,
        "the durable sleep lasted three days of server time"
    );
}
