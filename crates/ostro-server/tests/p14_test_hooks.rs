//! 14-10 Task 1 (D-P14-20/34): the failpoints-only test endpoints on the real in-process
//! server.
//!
//! - default build: `POST /v1/admin/test/clock`, `GET /v1/admin/test/clock` and
//!   `POST /v1/admin/test/failpoints` answer 404 (no knob in release builds);
//! - failpoints build (`--test-threads=1`: failpoints are process-global):
//!   - the clock endpoint sets / validates / resets the skew of the node's injected clock;
//!   - a wall offset of +30 s makes a 60 s durable timer fire ~30 s earlier in real time;
//!   - `wal::fsync=sleep(200)` raises the start latency above 200 ms, `off` restores it;
//!   - token mode: no bearer 401, a tenant admin or a system reader 403, the bootstrap
//!     system admin 200.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::Duration;

use common::{ServerOpts, TestServer};
use reqwest::StatusCode;
use serde_json::json;

const LIMIT: Duration = Duration::from_secs(60);

#[cfg(not(feature = "failpoints"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_endpoints_do_not_exist_in_default_builds() {
    let mut s = TestServer::start(ServerOpts::default()).await;
    s.wait_ready(LIMIT).await;
    let (st, v) = s
        .post_json("/v1/admin/test/clock", &json!({ "offset_ms": 1000 }))
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND, "clock: {v}");
    let (st, v) = s.get_json("/v1/admin/test/clock").await;
    assert_eq!(st, StatusCode::NOT_FOUND, "GET clock: {v}");
    let (st, v) = s
        .post_json(
            "/v1/admin/test/failpoints",
            &json!({ "name": "wal::fsync", "action": "sleep(200)" }),
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND, "failpoints: {v}");
    s.shutdown().await;
}

#[cfg(feature = "failpoints")]
mod failpoints {
    use std::time::Instant;

    use ostro_meta::{ApiToken, MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult};
    use ostro_server::config::AuthMode;
    use ostro_server::p14::tokens;
    use ostro_types::journal::EventKind;

    use super::*;

    async fn wait_kind(s: &TestServer, run: &ostro_types::RunId, kind: EventKind) {
        let t0 = Instant::now();
        loop {
            if s.history_kinds(run).await.contains(&kind) {
                return;
            }
            assert!(t0.elapsed() < LIMIT, "{run}: no {kind:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_completed(s: &TestServer, run: &str, limit: Duration) {
        let t0 = Instant::now();
        loop {
            let (st, v) = s.get_json(&format!("/v1/runs/{run}")).await;
            if st == StatusCode::OK && v["status"] == "completed" {
                return;
            }
            assert!(t0.elapsed() < limit, "{run} not completed: {st} {v}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn clock_endpoint_sets_validates_and_resets_the_skew() {
        let mut s = TestServer::start(ServerOpts::default()).await;
        s.wait_ready(LIMIT).await;
        let (st, v) = s.get_json("/v1/admin/test/clock").await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["offset_ms"], 0);
        assert_eq!(v["mono_rate"], 0.0);
        let skew = |v: &serde_json::Value| {
            v["wall_ms"].as_i64().expect("wall") - v["inner_wall_ms"].as_i64().expect("inner")
        };
        assert_eq!(skew(&v), 0, "no skew by default: {v}");

        let (st, v) = s
            .post_json("/v1/admin/test/clock", &json!({ "offset_ms": 30_000 }))
            .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(skew(&v), 30_000, "{v}");
        let (st, v) = s
            .post_json(
                "/v1/admin/test/clock",
                &json!({ "offset_ms": -5_000, "strobe_ms": 100, "strobe_period_ms": 1_000, "mono_rate": -0.05 }),
            )
            .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["mono_rate_ppm"], -50_000);
        assert_eq!(v["strobe_ms"], 100);
        assert!((skew(&v) + 5_000).abs() <= 150, "{v}");

        // Invalid changes are 400 and apply nothing.
        for bad in [
            json!({ "mono_rate": 0.6 }),
            json!({ "strobe_ms": 10, "strobe_period_ms": 0 }),
            json!({ "unknown": 1 }),
        ] {
            let (st, v) = s.post_json("/v1/admin/test/clock", &bad).await;
            assert_eq!(st, StatusCode::BAD_REQUEST, "{bad}: {v}");
        }
        let (_, v) = s.get_json("/v1/admin/test/clock").await;
        assert_eq!(v["mono_rate_ppm"], -50_000, "{v}");

        let (st, v) = s
            .post_json("/v1/admin/test/clock", &json!({ "reset": true }))
            .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(
            (v["offset_ms"].clone(), v["strobe_ms"].clone()),
            (json!(0), json!(0))
        );
        assert_eq!(skew(&v), 0, "{v}");
        assert!(
            v["mono_ns"].as_u64() <= v["inner_mono_ns"].as_u64(),
            "a slow drift leaves the monotonic clock behind after reset: {v}"
        );
        s.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn wall_offset_fires_a_durable_timer_earlier() {
        let mut s = TestServer::start(ServerOpts::default()).await;
        s.wait_ready(LIMIT).await;
        s.deploy_guest("p10-time", None).await;
        let t0 = Instant::now();
        let run = s
            .start_rest("sleep_for", json!({ "input": { "ms": 60_000 } }))
            .await;
        wait_kind(&s, &run, EventKind::TimerStarted).await;
        let (st, v) = s
            .post_json("/v1/admin/test/clock", &json!({ "offset_ms": 30_000 }))
            .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        wait_completed(&s, &run.to_string(), Duration::from_secs(55)).await;
        let took = t0.elapsed();
        println!("60 s timer under a +30 s offset fired after {took:?}");
        assert!(
            took >= Duration::from_secs(25) && took < Duration::from_secs(50),
            "the timer should fire ~30 s after its start, took {took:?}"
        );
        let (st, _) = s
            .post_json("/v1/admin/test/clock", &json!({ "reset": true }))
            .await;
        assert_eq!(st, StatusCode::OK);
        s.shutdown().await;
    }

    /// The fastest of `n` smoke starts.
    async fn start_latency(s: &TestServer, n: usize) -> Duration {
        let mut best = Duration::MAX;
        for _ in 0..n {
            let t = Instant::now();
            s.start_rest("smoke", json!({ "input": { "x": 1 } })).await;
            best = best.min(t.elapsed());
        }
        best
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn failpoint_endpoint_stalls_wal_fsync_until_off() {
        let mut s = TestServer::start(ServerOpts::default()).await;
        s.wait_ready(LIMIT).await;
        s.deploy_guest("smoke", None).await;
        let base = start_latency(&s, 3).await;
        assert!(base < Duration::from_millis(200), "baseline start {base:?}");

        let (st, v) = s
            .post_json(
                "/v1/admin/test/failpoints",
                &json!({ "name": "wal::fsync", "action": "sleep(200)" }),
            )
            .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert!(
            v["active"]
                .as_array()
                .expect("active")
                .iter()
                .any(|a| a["name"] == "wal::fsync"),
            "{v}"
        );
        let stalled = start_latency(&s, 2).await;
        println!("start latency: baseline {base:?}, wal::fsync=sleep(200) {stalled:?}");
        assert!(
            stalled >= Duration::from_millis(200),
            "the stall must show in the start latency: {stalled:?}"
        );

        let (st, v) = s
            .post_json(
                "/v1/admin/test/failpoints",
                &json!({ "name": "wal::fsync", "action": "off" }),
            )
            .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert!(
            !v["active"]
                .as_array()
                .expect("active")
                .iter()
                .any(|a| a["name"] == "wal::fsync"),
            "{v}"
        );
        let after = start_latency(&s, 3).await;
        assert!(after < Duration::from_millis(200), "after off {after:?}");

        // A bad action is 400 (and configures nothing).
        let (st, v) = s
            .post_json(
                "/v1/admin/test/failpoints",
                &json!({ "name": "wal::fsync", "action": "explode(" }),
            )
            .await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
        let (st, v) = s
            .post_json(
                "/v1/admin/test/failpoints",
                &json!({ "name": " ", "action": "return" }),
            )
            .await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
        s.shutdown().await;
    }

    async fn put_token(meta: &MetaHandle, scopes: &[&str], tenant: Option<&str>) -> String {
        let (id, secret, bearer) = tokens::generate_os().expect("generate");
        let rec = ApiToken {
            id: id.as_str().to_owned(),
            hash: tokens::hash(&secret),
            scopes: scopes.iter().map(|s| (*s).to_owned()).collect(),
            tenant: tenant.map(str::to_owned),
            created_at_ms: 1,
            expires_at_ms: None,
            ext: Vec::new(),
        };
        let res = meta
            .propose(MetaCommand::Put {
                key: MetaKey::Token(rec.id.clone()),
                expected_version: None,
                record: MetaRecord::Token(rec),
            })
            .await
            .expect("propose");
        assert!(matches!(res, MetaResult::Ok { .. }), "{res:?}");
        bearer.expose().to_owned()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn token_mode_requires_a_system_admin() {
        let mut s =
            TestServer::start_with(ServerOpts::default(), |c| c.auth.mode = AuthMode::Token).await;
        s.wait_ready(LIMIT).await;
        let admin = std::fs::read_to_string(s.h().data_dir().join("admin.token"))
            .expect("admin token")
            .trim()
            .to_owned();
        let meta = s.h().meta();
        let tenant_admin = put_token(meta, &["admin"], Some("acme")).await;
        let system_reader = put_token(meta, &["read"], None).await;

        let call = |path: &'static str, bearer: Option<String>| {
            let mut r = s.http.post(s.url(path)).json(&if path.ends_with("clock") {
                json!({ "offset_ms": 0 })
            } else {
                json!({ "name": "p14::test_hooks::probe", "action": "off" })
            });
            if let Some(b) = bearer {
                r = r.bearer_auth(b);
            }
            async move { r.send().await.expect("POST").status() }
        };
        for path in ["/v1/admin/test/clock", "/v1/admin/test/failpoints"] {
            assert_eq!(call(path, None).await, StatusCode::UNAUTHORIZED, "{path}");
            assert_eq!(
                call(path, Some("ostro_otk_garbage".into())).await,
                StatusCode::UNAUTHORIZED,
                "{path}"
            );
            assert_eq!(
                call(path, Some(tenant_admin.clone())).await,
                StatusCode::FORBIDDEN,
                "{path} tenant admin"
            );
            assert_eq!(
                call(path, Some(system_reader.clone())).await,
                StatusCode::FORBIDDEN,
                "{path} system reader"
            );
            assert_eq!(
                call(path, Some(admin.clone())).await,
                StatusCode::OK,
                "{path} system admin"
            );
        }
        s.shutdown().await;
    }
}
