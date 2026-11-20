//! ROADMAP Phase 6 SC1, in process (plan 06-17): HTTP effects of the fx-http guest through a
//! bridge-enabled server against a loopback `MockProvider` (`allow_private = 127.0.0.0/8`;
//! the module's egress policy allows the mock origin).
//!
//! - **derived key**: a `POST /pay` carries `Idempotency-Key: ostro-<32 hex>`, the key the
//!   connector derives from (run id, op), and is charged once;
//! - **guest keys**: an `idempotency_key` override is sent verbatim; a `semantic_key`
//!   yields the SDK key (`ostro/idem/v1/name`) in both the header and the body;
//! - **intent before I/O** (failpoints): while `bridge::http::before_send` holds the send,
//!   the history already shows `effect_scheduled` and the provider has seen nothing;
//! - **retries keep the key**: `/flaky?fail=2` with a retry policy completes 200 after 3
//!   attempts, all with the same key;
//! - **parallel**: 10 in-flight requests complete, each with its own key.
//!
//! The crash re-drive clauses of SC1 (kill -9 of the real binary) are in
//! `crates/ostro/tests/fx_redrive.rs`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use common::fx::{configure, deploy_fx, wait_terminal};
use common::{BridgeTestOpts, TestServer, start_server_with_bridge, tmpfs_dir};
use ostro_bridge::http::derive_key;
use ostro_testkit::mock_provider::MockProvider;
use ostro_types::RunId;
use ostro_types::journal::{Event, EventBody};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Generous limit for a run reaching a terminal state.
const RUN_LIMIT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------

/// A bridge server with fx-http deployed and allowed to reach `mock`.
async fn setup(mock: &MockProvider) -> TestServer {
    let s = start_server_with_bridge(tmpfs_dir(), BridgeTestOpts::default()).await;
    let module = deploy_fx(&s, "fx-http").await;
    configure(&s)
        .egress(&module, &[&mock.base_url()])
        .apply()
        .await;
    s
}

/// One fx-http request; `POST`s carry an empty JSON object.
fn req(method: &str, url: String) -> Value {
    let mut r = json!({ "method": method, "url": url });
    if method == "POST" {
        r["headers"] = json!([["content-type", "application/json"]]);
        r["body_b64"] = json!(B64.encode(b"{}"));
    }
    r
}

/// Start fx-http over REST.
async fn start(s: &TestServer, requests: Vec<Value>, parallel: bool) -> RunId {
    s.start_rest(
        "fx-http",
        json!({ "input": { "requests": requests, "parallel": parallel } }),
    )
    .await
}

/// Wait for a terminal state, require `completed`, return the guest output.
async fn completed_output(s: &TestServer, run: &RunId) -> Value {
    let info = wait_terminal(s, run, RUN_LIMIT).await;
    let st = serde_json::to_value(info.status).expect("status");
    assert_eq!(st, "completed", "run {run}: {info:?}");
    let b = s.result_bytes(run).await.expect("stored result");
    serde_json::from_slice(&b).expect("result json")
}

/// Ops (seqs) of the run's `EffectScheduled` events, in journal order.
fn effect_ops(j: &[Event]) -> Vec<u64> {
    j.iter()
        .filter(|e| matches!(e.body, EventBody::EffectScheduled(_)))
        .map(|e| e.seq)
        .collect()
}

/// Whether `key` has the derived-key shape `ostro-[0-9a-f]{32}`.
fn is_ostro_key(key: &str) -> bool {
    key.strip_prefix("ostro-").is_some_and(|h| {
        h.len() == 32
            && h.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

/// The SDK's semantic key (`ostro_sdk::effects::idempotency_key`, pinned by its vectors):
/// `"ostro-" + hex(sha256("ostro/idem/v1/name" || run_id || 0x00 || name))[..32]`.
fn sdk_semantic_key(run_id: &str, name: &str) -> String {
    let mut h = Sha256::new();
    h.update(b"ostro/idem/v1/name");
    h.update(run_id.as_bytes());
    h.update([0u8]);
    h.update(name.as_bytes());
    let d: [u8; 32] = h.finalize().into();
    let hex: String = d[..16].iter().map(|b| format!("{b:02x}")).collect();
    format!("ostro-{hex}")
}

fn body_json(item: &Value) -> Value {
    let raw = B64
        .decode(item["body_b64"].as_str().expect("body_b64"))
        .expect("base64");
    serde_json::from_slice(&raw).expect("json body")
}

/// The value of header `name` (case-insensitive) in an `/echo` body.
fn echoed_header(echo: &Value, name: &str) -> Option<String> {
    echo["headers"].as_array()?.iter().find_map(|kv| {
        kv[0]
            .as_str()
            .filter(|k| k.eq_ignore_ascii_case(name))
            .and(kv[1].as_str().map(str::to_owned))
    })
}

// ---------------------------------------------------------------------------------
// (a) derived key, one charge
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pay_carries_derived_key_and_charges_once() {
    let mock = MockProvider::start().await;
    let s = setup(&mock).await;

    let run = start(
        &s,
        vec![req("POST", format!("{}/pay", mock.base_url()))],
        false,
    )
    .await;
    let out = completed_output(&s, &run).await;
    let item = &out["results"][0];
    assert_eq!(item["status"], 200, "{out}");

    let j = s.journal(&run).await;
    let ops = effect_ops(&j);
    assert_eq!(ops.len(), 1);
    let key = derive_key(&run.to_string(), ops[0]);
    assert!(is_ostro_key(&key), "derived key shape: {key}");
    // The provider answered with the key it received.
    assert_eq!(body_json(item)["key"], key, "{out}");
    assert_eq!(mock.requests(&key), 1);
    assert_eq!(mock.charges(&key), 1);
    assert_eq!(mock.requests_to("/pay"), 1);
}

// ---------------------------------------------------------------------------------
// (b) guest override and semantic keys
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn override_and_semantic_keys() {
    let mock = MockProvider::start().await;
    let s = setup(&mock).await;
    let custom = "order-4711-attempt-key";

    let mut over = req("POST", format!("{}/pay", mock.base_url()));
    over["idempotency_key"] = json!(custom);
    let mut sem = req("POST", format!("{}/echo", mock.base_url()));
    sem["semantic_key"] = json!("charge-order-4711");
    let run = start(&s, vec![over, sem], false).await;
    let out = completed_output(&s, &run).await;

    // Override: verbatim, one charge under it, nothing under the derived key.
    let pay = &out["results"][0];
    assert_eq!(pay["status"], 200, "{out}");
    assert_eq!(body_json(pay)["key"], custom);
    assert_eq!(mock.charges(custom), 1);
    assert_eq!(mock.requests(custom), 1);
    let j = s.journal(&run).await;
    let ops = effect_ops(&j);
    assert_eq!(ops.len(), 2);
    assert_eq!(mock.requests(&derive_key(&run.to_string(), ops[0])), 0);

    // Semantic key: the SDK-derived key in the header and in the JSON body.
    let want = sdk_semantic_key(&run.to_string(), "charge-order-4711");
    assert!(is_ostro_key(&want));
    let echo = body_json(&out["results"][1]);
    assert_eq!(
        echoed_header(&echo, "idempotency-key").as_deref(),
        Some(want.as_str()),
        "{echo}"
    );
    let sent: Value = serde_json::from_slice(
        &B64.decode(echo["body_b64"].as_str().expect("echo body"))
            .expect("base64"),
    )
    .expect("sent body json");
    assert_eq!(sent["idempotency_key"], want, "{sent}");
    assert_eq!(mock.requests(&want), 1);
}

// ---------------------------------------------------------------------------------
// (c) intent before I/O (failpoint)
// ---------------------------------------------------------------------------------

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn intent_durable_before_send() {
    use common::fx::{events_of, history};
    use tokio::time::{Instant, sleep};
    let _scenario = fail::FailScenario::setup();
    let mock = MockProvider::start().await;
    let s = setup(&mock).await;

    fail::cfg("bridge::http::before_send", "return(wait:2000)").expect("failpoint");
    let run = start(
        &s,
        vec![req("POST", format!("{}/pay", mock.base_url()))],
        false,
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let h = history(&s, &run).await;
        if !events_of(&h, "effect_scheduled").is_empty() {
            assert!(events_of(&h, "effect_completed").is_empty());
            break;
        }
        assert!(Instant::now() < deadline, "no effect_scheduled in history");
        sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        mock.total_requests(),
        0,
        "the provider saw the request while it was held"
    );
    sleep(Duration::from_millis(500)).await;
    assert_eq!(mock.total_requests(), 0, "still held");

    let out = completed_output(&s, &run).await;
    fail::cfg("bridge::http::before_send", "off").expect("failpoint off");
    assert_eq!(out["results"][0]["status"], 200, "{out}");
    let ops = effect_ops(&s.journal(&run).await);
    let key = derive_key(&run.to_string(), ops[0]);
    assert_eq!(mock.requests(&key), 1);
    assert_eq!(mock.charges(&key), 1);
}

// ---------------------------------------------------------------------------------
// (d) retries keep the key
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flaky_get_retries_with_same_key() {
    let mock = MockProvider::start().await;
    let s = setup(&mock).await;

    let mut r = req("GET", format!("{}/flaky?fail=2", mock.base_url()));
    r["retry"] = json!({
        "max_attempts": 5,
        "initial_interval_ms": 50,
        "max_interval_ms": 200,
    });
    let run = start(&s, vec![r], false).await;
    let out = completed_output(&s, &run).await;
    assert_eq!(out["results"][0]["status"], 200, "{out}");

    let ops = effect_ops(&s.journal(&run).await);
    assert_eq!(ops.len(), 1, "one effect, retried inside the bridge");
    let key = derive_key(&run.to_string(), ops[0]);
    assert_eq!(mock.requests_to("/flaky"), 3, "2 failures + 1 success");
    assert_eq!(mock.requests(&key), 3, "every attempt carried the same key");
}

// ---------------------------------------------------------------------------------
// (e) parallel requests, distinct keys
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_requests_get_distinct_keys() {
    let mock = MockProvider::start().await;
    let s = setup(&mock).await;

    let reqs = (0..10)
        .map(|_| req("POST", format!("{}/pay", mock.base_url())))
        .collect();
    let run = start(&s, reqs, true).await;
    let out = completed_output(&s, &run).await;
    let results = out["results"].as_array().expect("results");
    assert_eq!(results.len(), 10);
    let mut seen = BTreeSet::new();
    for item in results {
        assert_eq!(item["status"], 200, "{item}");
        let key = body_json(item)["key"]
            .as_str()
            .expect("key in response")
            .to_owned();
        assert!(is_ostro_key(&key), "{key}");
        assert_eq!(mock.charges(&key), 1, "{key}");
        seen.insert(key);
    }
    assert_eq!(seen.len(), 10, "keys are distinct");

    let ops = effect_ops(&s.journal(&run).await);
    assert_eq!(ops.len(), 10);
    let derived: BTreeSet<String> = ops
        .iter()
        .map(|op| derive_key(&run.to_string(), *op))
        .collect();
    assert_eq!(derived, seen, "each key is the derived key of its op");
    assert_eq!(mock.requests_to("/pay"), 10);
}
