//! Phase 10 (10-05) tooling routes against the real server: until 10-10 / 10-11 fill
//! `control/dev.rs` and `control/replay.rs`, the dev clock answers 409
//! `time_skipping_disabled` and replay answers 501 `unimplemented`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::Duration;

use common::{ServerOpts, TestServer};
use reqwest::StatusCode;
use serde_json::{Value, json};

async fn decode(r: reqwest::Response) -> (StatusCode, Value) {
    let st = r.status();
    (st, r.json().await.expect("json body"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dev_routes_409_and_replay_501() {
    let s = TestServer::start(ServerOpts {
        ephemeral: true,
        ..ServerOpts::default()
    })
    .await;
    s.wait_ready(Duration::from_secs(10)).await;

    let (st, v) = decode(
        s.http
            .get(s.url("/v1/dev/time"))
            .send()
            .await
            .expect("GET dev/time"),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(v["error"]["code"], "conflict", "{v}");
    assert_eq!(v["error"]["details"]["reason"], "time_skipping_disabled");
    assert!(
        v["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("--time-skipping")),
        "{v}"
    );

    let (st, v) = decode(
        s.http
            .post(s.url("/v1/dev/time/skip"))
            .json(&json!({"by": "72h"}))
            .send()
            .await
            .expect("POST skip"),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(v["error"]["details"]["reason"], "time_skipping_disabled");

    let sha = "a".repeat(64);
    let (st, v) = decode(
        s.http
            .post(s.url(&format!("/v1/modules/{sha}/replay")))
            .json(&json!({"sample": 5}))
            .send()
            .await
            .expect("POST replay"),
    )
    .await;
    // 10-11 implemented replay: an unknown module is a plain 404 (tests/replay_endpoint.rs).
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");
    assert_eq!(v["error"]["code"], "not_found", "{v}");

    // Edge validation runs before the control plane.
    let (st, v) = decode(
        s.http
            .post(s.url(&format!("/v1/modules/{sha}/replay")))
            .json(&json!({"sample": 1001}))
            .send()
            .await
            .expect("POST replay"),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(v["error"]["details"]["reason"], "sample_too_large");
}
