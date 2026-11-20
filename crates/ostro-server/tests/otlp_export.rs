//! OBS-01/OBS-03 pipes (plan 08-04): with `[telemetry] otlp_endpoint` set, node-internal
//! `tracing` spans and events reach an in-process OTLP gRPC collector as spans and
//! LogRecords, over the locked tonic; the SDK/transport targets and `ostro::guest` are
//! never bridged into LogRecords (no feedback loop, no double export).
//!
//! One test function: it installs the process-global subscriber.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::Duration;

use common::{ServerOpts, TestServer};
use ostro_testkit::otlp::Collector;
use reqwest::StatusCode;

const WAIT: Duration = Duration::from_secs(20);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn otlp_exports_node_spans_and_logs_without_feedback() {
    let (addr, collector) = Collector::start().await;
    let mut s = TestServer::start_with(ServerOpts::default(), |c| {
        c.telemetry.otlp_endpoint = Some(format!("http://{addr}"));
        c.telemetry.service_name = "ostro-otlp-test".into();
        // tower-http's request span is DEBUG.
        c.log.level = "info,tower_http=debug".into();
    })
    .await;
    s.wait_ready(Duration::from_secs(5)).await;
    assert!(
        ostro_server::logging::subscriber_is_ours(),
        "Telemetry::init installed the composed subscriber"
    );

    // An API request: tower-http's `request` span through the tracing-opentelemetry layer.
    let r = s
        .http
        .get(s.url("/v1/workflows"))
        .send()
        .await
        .expect("GET workflows");
    assert_eq!(r.status(), StatusCode::OK);

    // Events that must never become LogRecords, emitted before the marker (the batch
    // processor exports in order, so once the marker arrived these would have too).
    for target_probe in [
        "tonic-probe",
        "h2-probe",
        "hyper-probe",
        "otel-probe",
        "guest-probe",
    ] {
        match target_probe {
            "tonic-probe" => tracing::info!(target: "tonic::transport", "{target_probe}"),
            "h2-probe" => tracing::info!(target: "h2::proto", "{target_probe}"),
            "hyper-probe" => tracing::info!(target: "hyper_util::client", "{target_probe}"),
            "otel-probe" => tracing::info!(target: "opentelemetry_sdk", "{target_probe}"),
            _ => tracing::info!(target: "ostro::guest", run_id = "r", "{target_probe}"),
        }
    }
    tracing::info!(target: "ostro::probe", answer = 42, "otlp-export-marker");

    s.h().telemetry().force_flush().await;
    collector
        .wait_for(
            |c| c.logs().iter().any(|l| l.body == "otlp-export-marker"),
            WAIT,
        )
        .await;
    collector
        .wait_for(|c| c.spans().iter().any(|sp| sp.name == "request"), WAIT)
        .await;

    let logs = collector.logs();
    let probe = logs
        .iter()
        .find(|l| l.body == "otlp-export-marker")
        .expect("probe log");
    assert_eq!(probe.scope, "ostro::probe", "the target is the scope name");
    assert_eq!(
        probe.attributes.get("answer").map(String::as_str),
        Some("42")
    );
    assert_eq!(
        probe.resource.get("service.name").map(String::as_str),
        Some("ostro-otlp-test")
    );
    for l in &logs {
        for excluded in ["opentelemetry", "tonic", "h2", "hyper", "ostro::guest"] {
            assert!(
                !l.scope.starts_with(excluded),
                "LogRecord from excluded target {}: {l:?}",
                l.scope
            );
        }
        assert!(
            !l.body.ends_with("-probe"),
            "excluded probe exported: {l:?}"
        );
    }

    let spans = collector.spans();
    let req = spans
        .iter()
        .find(|sp| {
            sp.name == "request"
                && sp
                    .attributes
                    .get("uri")
                    .is_some_and(|u| u.contains("/v1/workflows"))
        })
        .unwrap_or_else(|| panic!("no request span for /v1/workflows: {spans:#?}"));
    assert_eq!(req.trace_id.len(), 16);
    assert_eq!(req.span_id.len(), 8);
    assert_eq!(
        req.resource.get("service.name").map(String::as_str),
        Some("ostro-otlp-test")
    );
    assert_eq!(
        req.resource.get("service.instance.id").map(String::as_str),
        Some("1")
    );
    assert!(req.resource.contains_key("service.version"));

    s.shutdown().await;
    // After shutdown the layers are detached: later events go nowhere (and do not panic).
    tracing::info!(target: "ostro::probe", "after-shutdown");
}
