//! The Rust test kit (`ostro_client::testing`, 10-17) against a real spawned
//! `ostro dev --ephemeral --time-skipping`.
//!
//! Needs the `ostro` binary (`OSTRO_BIN`, else the one next to this test binary) and the
//! wasm32-wasip2 target (guests `p10-time`, `fx-http`).
#![cfg(feature = "testing")]

mod common_p10;

use std::time::{Duration, Instant};

use axum::Router;
use axum::routing::get;
use ostro_client::testing::{READY_LINE_PATTERN, ReadyLine, TestEnv, TestError};
use serde_json::{Value, json};

const DAY_MS: u64 = 24 * 60 * 60 * 1000;

async fn env() -> TestEnv {
    TestEnv::builder()
        .binary(common_p10::ostro_bin())
        .start()
        .await
        .expect("start ostro dev")
}

async fn env_with_p10_time() -> TestEnv {
    let env = env().await;
    let up = env
        .deploy_wasm_bytes(common_p10::guest_wasm("p10-time"), "p10-time")
        .await
        .expect("deploy p10-time");
    assert!(up.module.workflows.iter().any(|w| w.name == "sleep_for"));
    env
}

#[tokio::test]
async fn three_day_sleep_completes_in_seconds() {
    let env = env_with_p10_time().await;
    assert!(env.ready_line().ephemeral);
    let t0 = Instant::now();
    let run = env
        .start_run("sleep_for", json!({ "ms": 3 * DAY_MS }))
        .await
        .expect("start");
    let out = env.result(&run, "30s").await.expect("result");
    let wall = t0.elapsed();
    assert_eq!(out, json!({ "slept": 3 * DAY_MS }));
    assert!(wall < Duration::from_secs(10), "wall {wall:?}");
    let now = env.now_ms().await.expect("now");
    assert!(
        now - run.accepted_at_ms >= 3 * DAY_MS,
        "now {now} accepted {}",
        run.accepted_at_ms
    );
    let timers = env.timers(&run).await.expect("timers");
    assert_eq!(timers.len(), 1, "{timers:?}");
    assert!(timers[0].fired(), "{timers:?}");
    assert_eq!(timers[0].fire_at_ms, run.accepted_at_ms + 3 * DAY_MS);
    let kinds: Vec<Value> = env
        .history(&run)
        .await
        .expect("history")
        .iter()
        .map(|e| e["kind"].clone())
        .collect();
    assert_eq!(kinds.first(), Some(&json!("run_started")), "{kinds:?}");
    assert_eq!(kinds.last(), Some(&json!("run_completed")), "{kinds:?}");
    env.assert_completed(&run, Some(json!({ "slept": 3 * DAY_MS })))
        .await;
    eprintln!("kit wall time: 3-day sleep_for {wall:?}");
}

#[tokio::test]
async fn signal_then_sleep_returns_the_payload() {
    let env = env_with_p10_time().await;
    let t0 = Instant::now();
    let run = env
        .start_run("signal_then_sleep", json!({}))
        .await
        .expect("start");
    env.signal(&run, "go", json!({ "ok": 7 }))
        .await
        .expect("signal");
    let out = env.result(&run, "30s").await.expect("result");
    assert_eq!(out, json!({ "ok": 7 }));
    assert!(env.now_ms().await.expect("now") - run.accepted_at_ms >= DAY_MS);
    assert!(env.steps(&run).await.expect("steps").is_empty());
    eprintln!("kit wall time: signal_then_sleep {:?}", t0.elapsed());
}

#[tokio::test]
async fn skip_advances_the_clock() {
    let env = env().await;
    let before = env.now_ms().await.expect("now");
    let s = env.skip("72h").await.expect("skip");
    assert!(s.skipped_ms >= 72 * 3_600_000, "{s:?}");
    let after = env.now_ms().await.expect("now");
    assert!(after - before >= 72 * 3_600_000, "{before} -> {after}");
}

#[tokio::test]
async fn a_run_that_waits_forever_is_a_clear_error() {
    let env = env_with_p10_time().await;
    let run = env
        .start_run("wait_forever", json!({}))
        .await
        .expect("start");
    // The 48 h run timeout is skipped to: the run ends timed out, which `result` reports.
    let err = env.result(&run, "30s").await.expect_err("not completed");
    assert!(matches!(err, TestError::Run(_)), "{err}");
}

fn b64decode(s: &str) -> Vec<u8> {
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::new();
    let (mut buf, mut bits) = (0u32, 0);
    for c in s.bytes().filter(|&c| c != b'=') {
        let v = A.iter().position(|&a| a == c).expect("base64") as u32;
        buf = (buf << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    out
}

#[tokio::test]
async fn loopback_http_mock_with_allow_egress() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let app = Router::new().route("/hello", get(|| async { "hello from the mock" }));
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let env = TestEnv::builder()
        .binary(common_p10::ostro_bin())
        .egress_loopback(true)
        .start()
        .await
        .expect("start");
    env.deploy_wasm_bytes(common_p10::guest_wasm("fx-http"), "fx-http")
        .await
        .expect("deploy fx-http");
    let origin = format!("http://127.0.0.1:{port}");
    // By workflow name: the kit resolves it to the module it deployed.
    env.allow_egress("fx-http", &origin).await.expect("egress");
    let run = env
        .start_run(
            "fx-http",
            json!({
                "requests": [{ "method": "GET", "url": format!("{origin}/hello") }],
                "parallel": false
            }),
        )
        .await
        .expect("start");
    let out = env.result(&run, "30s").await.expect("result");
    let r = &out["results"][0];
    assert_eq!(r["status"], 200, "{out}");
    let body = b64decode(r["body_b64"].as_str().expect("body_b64"));
    assert_eq!(String::from_utf8_lossy(&body), "hello from the mock");
}

#[tokio::test]
async fn dropping_the_env_kills_the_server() {
    let env = env().await;
    let pid = env.pid().expect("pid");
    let url = env.url().to_owned();
    drop(env);
    let t0 = Instant::now();
    loop {
        #[cfg(target_os = "linux")]
        let gone = !std::path::Path::new(&format!("/proc/{pid}")).exists();
        #[cfg(not(target_os = "linux"))]
        let gone = std::net::TcpStream::connect(url.trim_start_matches("http://")).is_err();
        if gone {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(2),
            "pid {pid} ({url}) still alive"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_missing_ostro_bin_is_a_clear_error() {
    let err = ostro_client::testing::resolve_ostro_bin(
        None,
        Some("/nonexistent/ostro-kit-test".into()),
        None,
    )
    .expect_err("missing");
    let msg = err.to_string();
    assert!(msg.contains("OSTRO_BIN"), "{msg}");
    assert!(msg.contains("/nonexistent/ostro-kit-test"), "{msg}");
    let err = TestEnv::builder()
        .binary("/nonexistent/ostro-kit-test")
        .start()
        .await
        .expect_err("missing");
    assert!(matches!(err, TestError::Binary(_)), "{err}");
    let err = ostro_client::testing::resolve_ostro_bin(None, None, Some("".into()))
        .expect_err("not on PATH");
    assert!(err.to_string().contains("OSTRO_BIN"), "{err}");
}

#[test]
fn the_pattern_constant_is_the_frozen_one() {
    assert_eq!(
        READY_LINE_PATTERN,
        r"^(.+?) listening on http://(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$"
    );
    assert!(ReadyLine::parse("x listening on http://a:1 (data: d) ready in 0 ms").is_some());
}
