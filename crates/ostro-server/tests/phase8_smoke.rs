//! Phase 8 API wiring smoke (08-10): on the real server every Phase 8 route reaches its
//! controller: a stub (503 `unavailable` naming the owning plan, never 404) until the
//! plan lands, the real controller after (webhooks since 08-15, schedules since 08-14,
//! run logs since 08-17, the online backup since 08-18). The gRPC services are
//! mounted (not UNIMPLEMENTED), the hook route enforces
//! `[triggers] webhook_body_limit`, a start-run `traceparent` header is validated, and
//! the `tests/common/phase8.rs` signers agree with ostro-triggers' verifiers.
//!
//! One test function: `with_telemetry_collector()` installs the process-global
//! subscriber.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::Duration;

use common::phase8::{Phase8Server, sign_github, sign_slack, sign_stripe};
use ostro_proto::api::v1 as pb;
use ostro_triggers::webhook::{VerifierSpec, verify};
use reqwest::StatusCode;
use serde_json::{Value, json};

fn headers(pairs: Vec<(String, String)>) -> Vec<(String, Vec<u8>)> {
    pairs
        .into_iter()
        .map(|(k, v)| (k, v.into_bytes()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase8_routes_reach_controller_stubs() {
    let s = Phase8Server::builder()
        .with_telemetry_collector()
        .with_triggers(|t| t.webhook_body_limit = 4096)
        .start()
        .await;
    s.wait_ready(Duration::from_secs(10)).await;
    assert!(s.collector.is_some());

    // 08-18 implemented the online backup: a tar of the bottomless layout.
    let r = s
        .http
        .get(s.url("/v1/admin/backup"))
        .send()
        .await
        .expect("backup");
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(
        r.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/x-tar")
    );
    let tar = r.bytes().await.expect("tar body");
    assert!(
        tar.len() > 512 && tar.len() % 512 == 0,
        "{} bytes",
        tar.len()
    );
    assert_eq!(&tar[257..262], b"ustar", "a tar archive");
    let has = |name: &[u8]| tar.windows(name.len()).any(|w| w == name);
    assert!(
        has(b"manifest/current") && has(b"LOCK"),
        "bottomless layout"
    );

    // 08-14 implemented the schedule controller: real answers instead of the stub.
    let (st, v) = s.get_json("/v1/schedules").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v, json!({"schedules": []}));
    let (st, _) = s.get_json("/v1/schedules/n").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, v) = s
        .create_schedule(&json!({"name": "n", "workflow": "w", "cron": "* * * * *"}))
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "unknown workflow: {v}");

    // 08-15 implemented the webhook controller: real answers instead of the stub.
    let (st, v) = s.get_json("/v1/webhooks").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v, json!({"webhooks": []}));
    let (st, _, _) = s.deliver_hook("h", &[], br#"{"any":"body"}"#).await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // 08-17 implemented run logs: an unknown run is an empty list (best effort, not 404).
    let (st, v) = s.get_json("/v1/runs/run-1/logs").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v, json!({"run_id": "run-1", "lines": [], "dropped": 0}));

    // The hook route has its own limit ([triggers] webhook_body_limit = 4096 here).
    let r = s
        .http
        .post(s.url("/hooks/h"))
        .body(vec![b'x'; 8192])
        .send()
        .await
        .expect("big hook");
    assert_eq!(r.status(), StatusCode::PAYLOAD_TOO_LARGE);

    // gRPC services are mounted and reach the stubs.
    let ch = s.channel().await;
    let listed = pb::schedules_client::SchedulesClient::new(ch.clone())
        .list(pb::ListSchedulesRequest {})
        .await
        .expect("schedules implemented (08-14)");
    assert!(listed.into_inner().schedules.is_empty());
    let listed = pb::webhooks_client::WebhooksClient::new(ch.clone())
        .list(pb::ListWebhooksRequest {})
        .await
        .expect("webhooks implemented (08-15)");
    assert!(listed.into_inner().webhooks.is_empty());
    let logs = pb::run_logs_client::RunLogsClient::new(ch)
        .get_run_logs(pb::GetRunLogsRequest {
            run_id: "run-1".into(),
            ..Default::default()
        })
        .await
        .expect("run logs implemented (08-17)");
    assert!(logs.into_inner().lines.is_empty());

    // Inbound traceparent: an invalid header is rejected before the control plane.
    let r = s
        .http
        .post(s.url("/v1/workflows/greet/runs"))
        .header("traceparent", "00-not-a-trace")
        .json(&json!({}))
        .send()
        .await
        .expect("start");
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    let v: Value = r.json().await.expect("json");
    assert_eq!(v["error"]["details"]["reason"], "invalid_traceparent");

    // The signers agree with the ostro-triggers verifiers.
    let secret = b"whsec_test";
    let body = br#"{"id":"evt_1"}"#;
    let now = 1_700_000_000;
    verify(
        &VerifierSpec::Stripe { tolerance_s: 300 },
        secret,
        &headers(sign_stripe(secret, now, body)),
        body,
        now,
    )
    .expect("stripe signer");
    verify(
        &VerifierSpec::GitHub,
        secret,
        &headers(sign_github(secret, body)),
        body,
        now,
    )
    .expect("github signer");
    verify(
        &VerifierSpec::Slack { tolerance_s: 300 },
        secret,
        &headers(sign_slack(secret, now, body)),
        body,
        now,
    )
    .expect("slack signer");
    assert!(
        verify(
            &VerifierSpec::GitHub,
            b"other",
            &headers(sign_github(secret, body)),
            body,
            now,
        )
        .is_err()
    );

    let mut s = s;
    s.shutdown().await;
}
