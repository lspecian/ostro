//! Phase 8 API wiring smoke (08-10): on the real server every Phase 8 route reaches its
//! controller stub (503 `unavailable` naming the owning plan, never 404), the gRPC
//! services are mounted (UNAVAILABLE, not UNIMPLEMENTED), the hook route enforces
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

    let schedule = json!({"name": "n", "workflow": "w", "cron": "* * * * *"});
    let webhook = json!({"name": "h", "action": {"kind": "start", "workflow": "w"}});
    for (method, path, body, owner) in [
        ("GET", "/v1/schedules", Value::Null, "08-14"),
        ("POST", "/v1/schedules", schedule.clone(), "08-14"),
        ("GET", "/v1/schedules/n", Value::Null, "08-14"),
        ("PUT", "/v1/schedules/n", schedule.clone(), "08-14"),
        ("DELETE", "/v1/schedules/n", Value::Null, "08-14"),
        ("POST", "/v1/schedules/n/pause", Value::Null, "08-14"),
        ("POST", "/v1/schedules/n/resume", Value::Null, "08-14"),
        ("POST", "/v1/schedules/n/trigger", Value::Null, "08-14"),
        ("GET", "/v1/webhooks", Value::Null, "08-15"),
        ("POST", "/v1/webhooks", webhook.clone(), "08-15"),
        ("GET", "/v1/webhooks/h", Value::Null, "08-15"),
        ("PUT", "/v1/webhooks/h", webhook.clone(), "08-15"),
        ("DELETE", "/v1/webhooks/h", Value::Null, "08-15"),
        ("POST", "/hooks/h", json!({"any": "body"}), "08-15"),
        ("GET", "/v1/runs/run-1/logs", Value::Null, "08-17"),
        ("GET", "/v1/admin/backup", Value::Null, "08-18"),
    ] {
        let mut req = s.http.request(method.parse().unwrap(), s.url(path));
        if !body.is_null() {
            req = req.json(&body);
        }
        let r = req.send().await.expect("request");
        let st = r.status();
        let v: Value = r.json().await.expect("json error body");
        assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE, "{method} {path}: {v}");
        assert_eq!(v["error"]["code"], "unavailable", "{method} {path}");
        let msg = v["error"]["message"].as_str().unwrap_or_default();
        assert_eq!(
            msg,
            format!("not yet implemented ({owner})"),
            "{method} {path}"
        );
    }

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
    let e = pb::schedules_client::SchedulesClient::new(ch.clone())
        .list(pb::ListSchedulesRequest {})
        .await
        .expect_err("stub");
    assert_eq!(e.code(), tonic::Code::Unavailable, "{e:?}");
    assert!(e.message().contains("08-14"));
    let e = pb::webhooks_client::WebhooksClient::new(ch.clone())
        .list(pb::ListWebhooksRequest {})
        .await
        .expect_err("stub");
    assert_eq!(e.code(), tonic::Code::Unavailable, "{e:?}");
    assert!(e.message().contains("08-15"));
    let e = pb::run_logs_client::RunLogsClient::new(ch)
        .get_run_logs(pb::GetRunLogsRequest {
            run_id: "run-1".into(),
            ..Default::default()
        })
        .await
        .expect_err("stub");
    assert_eq!(e.code(), tonic::Code::Unavailable, "{e:?}");
    assert!(e.message().contains("08-17"));

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
