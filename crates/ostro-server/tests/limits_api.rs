//! API caps end to end (plan 11-11, OPS-07 / D-P11-13): with `[limits] max_payload_bytes =
//! 64 KiB`, over-cap start / signal / token payloads are 413 (gRPC RESOURCE_EXHAUSTED) with
//! `details{reason: limit_exceeded, limit: payload_bytes, max, actual}` and nothing is
//! proposed; a `__ostro/` user tag is 400 (INVALID_ARGUMENT); 65 tags is 400 with
//! `details.limit = tags`; `GET /v1/limits` reflects the overrides.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p11.rs"]
mod p11;

use std::time::Duration;

use ostro_partition::store::values::RunStatus;
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::runs_client::RunsClient;
use ostro_proto::api::v1::tokens_client::TokensClient;
use ostro_server::config::ByteSize;
use p11::P11Server;
use reqwest::StatusCode;
use serde_json::{Value, json};

const MAX: u64 = 64 * 1024;
const ACTUAL: u64 = 100 * 1024;
const LIMIT: Duration = Duration::from_secs(60);

/// A JSON string whose compact encoding (with the quotes) is exactly `bytes` long.
fn blob(bytes: u64) -> Value {
    Value::String("x".repeat(usize::try_from(bytes).expect("usize") - 2))
}

fn payload_details() -> Value {
    json!({"reason": "limit_exceeded", "limit": "payload_bytes", "max": MAX, "actual": ACTUAL})
}

async fn run_count(s: &P11Server) -> usize {
    s.list_all("").await.len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn caps_are_enforced_before_proposal_on_both_transports() {
    let mut s = P11Server::builder()
        .with_limits(|l| {
            l.max_payload_bytes = ByteSize(MAX);
            l.max_history_events = 1234;
        })
        .start()
        .await;
    s.wait_ready(Duration::from_secs(20)).await;
    s.deploy_guest("sdk-basic", None).await;

    // GET /v1/limits carries the effective [limits] values.
    let (st, v) = s.get_json("/v1/limits").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let value = |name: &str| {
        v["limits"]
            .as_array()
            .expect("limits")
            .iter()
            .find(|l| l["name"] == name)
            .unwrap_or_else(|| panic!("{name} in {v}"))["value"]
            .clone()
    };
    assert_eq!(value("max_payload_bytes"), json!(MAX));
    assert_eq!(value("max_history_events"), json!(1234));

    // A run to signal (completed is fine: the cap is checked before anything else).
    let target = s.start_rest("sdk-echo", json!({"input": {"i": 1}})).await;
    s.wait_record(&target, LIMIT, |r| r.status == RunStatus::Completed)
        .await;
    let before = run_count(&s).await;
    let history_len = s.journal(&target).await.len();

    // REST start: 413, exact details, no run.
    let (st, v) = s
        .post_start("sdk-echo", &json!({"input": blob(ACTUAL)}))
        .await;
    assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE, "{v}");
    assert_eq!(v["error"]["code"], "payload_too_large", "{v}");
    assert_eq!(v["error"]["details"], payload_details(), "{v}");
    // Exactly at the cap is accepted.
    let (st, v) = s.post_start("sdk-echo", &json!({"input": blob(MAX)})).await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let at_cap: ostro_types::RunId = v["run_id"].as_str().expect("id").parse().expect("id");
    s.wait_record(&at_cap, LIMIT, |r| r.status.is_terminal())
        .await;
    let before = before + 1;
    assert_eq!(run_count(&s).await, before);

    // REST signal and token completion.
    let (st, v) = s
        .post_json(
            &format!("/v1/runs/{target}/signals/go"),
            &json!({"payload": blob(ACTUAL)}),
        )
        .await;
    assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE, "{v}");
    assert_eq!(v["error"]["details"], payload_details(), "{v}");
    let (st, v) = s
        .post_json(
            "/v1/tokens/complete",
            &json!({"token": "ot1_not_checked_before_the_cap", "payload": blob(ACTUAL)}),
        )
        .await;
    assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE, "{v}");
    assert_eq!(v["error"]["details"], payload_details(), "{v}");

    // Tags: the system prefix and the count cap.
    let (st, v) = s
        .post_start(
            "sdk-echo",
            &json!({"input": 1, "tags": ["ok", "__ostro/x"]}),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(v["error"]["details"]["tag"], "__ostro/x", "{v}");
    let many: Vec<String> = (0..65).map(|i| format!("t{i}")).collect();
    let (st, v) = s
        .post_start("sdk-echo", &json!({"input": 1, "tags": many}))
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(
        v["error"]["details"],
        json!({"reason": "limit_exceeded", "limit": "tags", "max": 64, "actual": 65}),
        "{v}"
    );
    let long = "y".repeat(129);
    let (st, v) = s
        .post_start("sdk-echo", &json!({"input": 1, "tags": [long]}))
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(v["error"]["details"]["limit"], "tag_bytes", "{v}");

    // gRPC: the same requests, the same details JSON.
    let ch = s.channel().await;
    let mut runs = RunsClient::new(ch.clone());
    let mut tokens = TokensClient::new(ch);
    let big = serde_json::to_vec(&blob(ACTUAL)).expect("json");
    assert_eq!(big.len() as u64, ACTUAL);
    let e = runs
        .start(pb::StartRunRequest {
            workflow: "sdk-echo".into(),
            input_json: big.clone(),
            ..Default::default()
        })
        .await
        .expect_err("over cap");
    assert_eq!(e.code(), tonic::Code::ResourceExhausted, "{e:?}");
    assert_eq!(
        ostro_api::grpc::status_details(&e),
        Some(payload_details()),
        "{e:?}"
    );
    let e = runs
        .signal(pb::SignalRunRequest {
            run_id: target.to_string(),
            name: "go".into(),
            payload_json: big.clone(),
            ..Default::default()
        })
        .await
        .expect_err("over cap");
    assert_eq!(e.code(), tonic::Code::ResourceExhausted, "{e:?}");
    assert_eq!(ostro_api::grpc::status_details(&e), Some(payload_details()));
    let e = tokens
        .complete(pb::CompleteTokenRequest {
            token: "ot1_not_checked_before_the_cap".into(),
            payload_json: big,
            ..Default::default()
        })
        .await
        .expect_err("over cap");
    assert_eq!(e.code(), tonic::Code::ResourceExhausted, "{e:?}");
    assert_eq!(ostro_api::grpc::status_details(&e), Some(payload_details()));
    let e = runs
        .start(pb::StartRunRequest {
            workflow: "sdk-echo".into(),
            input_json: b"1".to_vec(),
            tags: vec!["__ostro/x".into()],
            ..Default::default()
        })
        .await
        .expect_err("system tag");
    assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e:?}");
    let e = runs
        .start(pb::StartRunRequest {
            workflow: "sdk-echo".into(),
            input_json: b"1".to_vec(),
            tags: (0..65).map(|i| format!("t{i}")).collect(),
            ..Default::default()
        })
        .await
        .expect_err("65 tags");
    assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e:?}");
    assert_eq!(
        ostro_api::grpc::status_details(&e).expect("details")["limit"],
        "tags"
    );

    // Nothing was proposed for any rejected request.
    assert_eq!(run_count(&s).await, before, "no run was created");
    assert_eq!(
        s.journal(&target).await.len(),
        history_len,
        "no signal reached the run"
    );
    s.shutdown().await;
}
