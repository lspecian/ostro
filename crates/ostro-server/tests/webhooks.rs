//! TRIG-01 (08-15): `POST /hooks/{name}` and webhook CRUD on the real server.
//!
//! - Start per verifier kind (Stripe, GitHub with an AES-sealed secret, Slack, Standard
//!   Webhooks, generic HMAC, none): a valid delivery starts one run, a duplicate returns
//!   the same run (`created=false`), a tampered body is 401 and starts nothing.
//! - Stale Stripe timestamps against the injected clock (a `ManualClock`), the Slack
//!   challenge, Slack retries, the delivery metric.
//! - Signal: by idempotency pointer and by run-id pointer; a redelivered signal is
//!   applied once (SignalSeen), also when redelivered after the run finished (200
//!   duplicate; a never-applied delivery to a finished run is 409 `run_terminal`); an
//!   unknown target is 404.
//! - complete_token completes the `tokens` guest; a redelivery is a 200 duplicate.
//! - CRUD over REST and gRPC: validation (400), stale CAS (409), no secret bytes in any
//!   response.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use common::phase8::{Phase8Server, hex, hmac_sha256, sign_github, sign_slack, sign_stripe};
use common::{ServerOpts, TestServer, now_ms, server_config, tmpfs_dir};
use ostro_bridge::secrets::{SealedStore, SealingKeyRing};
use ostro_meta::records::{SealedSecret, TOKEN_KEY_PLAINTEXT_ID};
use ostro_meta::{MetaCommand, MetaKey, MetaRecord, MetaResult};
use ostro_partition::clock::ManualClock;
use ostro_proto::api::v1 as pb;
use ostro_server::executor::ExecutorHooks;
use ostro_server::server::{ServerOptions, start_with_options};
use reqwest::StatusCode;
use serde_json::{Value, json};

const LIMIT: Duration = Duration::from_secs(30);

type Headers = Vec<(String, String)>;

async fn server() -> Phase8Server {
    let s = Phase8Server::builder()
        .with_config(|c| c.engine.hot_wait = Duration::from_millis(50).into())
        .start()
        .await;
    s.wait_ready(Duration::from_secs(10)).await;
    s
}

/// A `plaintext-v0` sealed meta secret (Phase 4 format).
async fn put_secret(s: &TestServer, name: &str, value: &[u8]) {
    put_record(
        s,
        SealedSecret {
            name: name.into(),
            ciphertext: value.to_vec(),
            key_id: TOKEN_KEY_PLAINTEXT_ID.into(),
            ext: Vec::new(),
        },
    )
    .await;
}

async fn put_record(s: &TestServer, rec: SealedSecret) {
    let r = s
        .h()
        .meta()
        .propose(MetaCommand::Put {
            key: MetaKey::Secret(rec.name.clone()),
            expected_version: None,
            record: MetaRecord::Secret(rec),
        })
        .await
        .expect("propose secret");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
}

async fn create(s: &TestServer, w: Value) -> Value {
    let (st, v) = s.create_webhook(&w).await;
    assert_eq!(st, StatusCode::CREATED, "create {w}: {v}");
    v
}

async fn deliver(s: &TestServer, name: &str, h: &Headers, body: &[u8]) -> (StatusCode, Value) {
    let (st, _, b) = s.deliver_hook(name, h, body).await;
    let v = serde_json::from_slice(&b).unwrap_or_else(|_| Value::String(format!("{b:?}")));
    (st, v)
}

async fn deliver_ok(s: &TestServer, name: &str, h: &Headers, body: &[u8]) -> Value {
    let (st, v) = deliver(s, name, h, body).await;
    assert_eq!(st, StatusCode::OK, "deliver {name}: {v}");
    v
}

fn now_s() -> i64 {
    i64::try_from(now_ms() / 1000).expect("secs")
}

/// Standard Webhooks: `webhook-id`, `webhook-timestamp`, `webhook-signature: v1,<b64>`
/// over `"{id}.{ts}.{body}"` with the base64 key after `whsec_`.
fn sign_standard(secret: &str, id: &str, ts: i64, body: &[u8]) -> Headers {
    let key = B64
        .decode(secret.strip_prefix("whsec_").expect("whsec_"))
        .expect("b64 key");
    let t = ts.to_string();
    let sig = B64.encode(hmac_sha256(
        &key,
        &[id.as_bytes(), b".", t.as_bytes(), b".", body],
    ));
    vec![
        ("webhook-id".into(), id.into()),
        ("webhook-timestamp".into(), t),
        ("webhook-signature".into(), format!("v1,{sig}")),
    ]
}

fn with(mut h: Headers, k: &str, v: &str) -> Headers {
    h.push((k.into(), v.into()));
    h
}

fn tamper(body: &[u8]) -> Vec<u8> {
    let mut b = body.to_vec();
    let i = b.iter().position(|c| *c == b'1').expect("a '1' to flip");
    b[i] = b'2';
    b
}

async fn metrics_text(s: &TestServer) -> String {
    s.http
        .get(s.url("/metrics"))
        .send()
        .await
        .expect("GET /metrics")
        .text()
        .await
        .expect("text")
}

// ---------------------------------------------------------------------------------
// start, per verifier
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_action_per_verifier_dedupes_deliveries() {
    let mut s = server().await;
    s.deploy_guest("sdk-basic", None).await;

    let stripe_secret = b"whsec_stripe_test_secret".to_vec();
    let gh_secret = b"github-webhook-secret".to_vec();
    let slack_secret = b"8f742231b10e8888abcd99yyyzzz85a5".to_vec();
    let std_secret = format!("whsec_{}", B64.encode(b"standard-webhooks-key-0123456789"));
    let hmac_secret = b"generic-hmac-secret".to_vec();
    put_secret(&s, "_/stripe", &stripe_secret).await;
    put_secret(&s, "slack-signing", &slack_secret).await;
    put_secret(&s, "_/standard", std_secret.as_bytes()).await;
    put_secret(&s, "_/hmac", &hmac_secret).await;
    // GitHub: an AES-256-GCM sealed record under the node sealing key
    // (`<data_dir>/sealing.key`, read lazily by the controller on first need).
    let key = [0x42u8; 32];
    std::fs::write(s.h().data_dir().join("sealing.key"), key).expect("sealing key");
    let sealed = SealedStore::new(Arc::new(SealingKeyRing::from_bytes(key)))
        .seal("_/github", &gh_secret)
        .expect("seal");
    put_record(&s, sealed).await;

    let wf = |input: &str| json!({"kind": "start", "workflow": "sdk-echo", "input": input});
    create(
        &s,
        json!({"name": "stripe", "action": wf("/data/object"),
        "verifier": {"kind": "stripe", "secret_ref": "stripe"}}),
    )
    .await;
    create(
        &s,
        json!({"name": "gh", "action": wf("body"),
        "verifier": {"kind": "github", "secret_ref": "github"}}),
    )
    .await;
    create(
        &s,
        json!({"name": "slack", "action": wf("body"),
        "verifier": {"kind": "slack", "secret_ref": "slack-signing"}}),
    )
    .await;
    create(
        &s,
        json!({"name": "std", "action": wf("body"),
        "verifier": {"kind": "standard", "secret_ref": "standard"}}),
    )
    .await;
    create(
        &s,
        json!({"name": "hmac", "action": wf("body"),
        "verifier": {"kind": "hmac", "secret_ref": "hmac", "header": "x-sig",
                     "prefix": "sha256=", "encoding": "hex"},
        "idempotency": "header:x-delivery"}),
    )
    .await;
    create(&s, json!({"name": "open", "action": wf("body")})).await;

    let now = now_s();
    let stripe_body = br#"{"id":"evt_1","data":{"object":{"amount":1}}}"#.to_vec();
    let gh_body = br#"{"action":"opened","n":1}"#.to_vec();
    let slack_body = br#"{"type":"event_callback","event_id":"Ev1","n":1}"#.to_vec();
    let std_body = br#"{"type":"user.created","n":1}"#.to_vec();
    let hmac_body = br#"{"n":1}"#.to_vec();
    let open_body = br#"{"open":1}"#.to_vec();
    let hmac_sig = |b: &[u8]| -> Headers {
        vec![
            (
                "x-sig".into(),
                format!("sha256={}", hex(&hmac_sha256(&hmac_secret, &[b]))),
            ),
            ("x-delivery".into(), "d-1".into()),
        ]
    };
    type Signer<'a> = Box<dyn Fn(&[u8]) -> Headers + 'a>;
    let cases: Vec<(&str, Vec<u8>, Signer)> = vec![
        (
            "stripe",
            stripe_body.clone(),
            Box::new(|b: &[u8]| sign_stripe(&stripe_secret, now, b)),
        ),
        (
            "gh",
            gh_body,
            Box::new(|b: &[u8]| with(sign_github(&gh_secret, b), "x-github-delivery", "gh-d-1")),
        ),
        (
            "slack",
            slack_body.clone(),
            Box::new(|b: &[u8]| sign_slack(&slack_secret, now, b)),
        ),
        (
            "std",
            std_body,
            Box::new(|b: &[u8]| sign_standard(&std_secret, "msg_1", now, b)),
        ),
        ("hmac", hmac_body, Box::new(hmac_sig)),
        ("open", open_body, Box::new(|_: &[u8]| Vec::new())),
    ];

    let mut runs = Vec::new();
    for (name, body, sign) in &cases {
        let h = sign(body);
        let first = deliver_ok(&s, name, &h, body).await;
        assert_eq!(first["created"], true, "{name}: {first}");
        assert_eq!(first["duplicate"], false, "{name}");
        let run_id = first["run_id"].as_str().expect("run_id").to_owned();
        // Same delivery again: the same run, not created.
        let again = deliver_ok(&s, name, &h, body).await;
        assert_eq!(again["run_id"], run_id.as_str(), "{name}");
        assert_eq!(again["created"], false, "{name}");
        assert_eq!(again["duplicate"], true, "{name}");
        if *name != "open" {
            // Tampered body with the original signature: 401, nothing started.
            let (st, v) = deliver(&s, name, &h, &tamper(body)).await;
            assert_eq!(st, StatusCode::UNAUTHORIZED, "{name}: {v}");
            assert_eq!(v["error"]["details"]["reason"], "bad_signature", "{name}");
            // No signature at all.
            let (st, v) = deliver(&s, name, &Vec::new(), body).await;
            assert_eq!(st, StatusCode::UNAUTHORIZED, "{name}: {v}");
        }
        runs.push((name.to_string(), run_id));
    }
    // One run per hook, no more.
    let listed = s.list_all("workflow=sdk-echo").await;
    assert_eq!(listed.len(), cases.len(), "{listed:?}");

    // The start input: the pointer selection or the whole body.
    for (name, run_id) in &runs {
        let v = s.wait_run_status(run_id, "completed", LIMIT).await;
        let want: Value = match name.as_str() {
            "stripe" => json!({"amount": 1}),
            _ => {
                let (_, body, _) = cases.iter().find(|c| c.0 == name).expect("case");
                serde_json::from_slice(body).expect("json")
            }
        };
        let got: Value = serde_json::from_slice(
            &s.result_bytes(&run_id.parse().expect("id"))
                .await
                .expect("result"),
        )
        .expect("json result");
        assert_eq!(got, want, "{name}: {v}");
    }

    // Stripe: a stale timestamp (outside 300 s) is 401 "stale"; no run.
    let stale_body = br#"{"id":"evt_stale","data":{"object":{}}}"#;
    let (st, v) = deliver(
        &s,
        "stripe",
        &sign_stripe(&stripe_secret, now - 3600, stale_body),
        stale_body,
    )
    .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED, "{v}");
    assert_eq!(v["error"]["details"]["reason"], "stale");
    assert!(v.to_string().contains("stale"));

    // Slack url_verification: the challenge, as text/plain, no run.
    let challenge = br#"{"type":"url_verification","token":"t","challenge":"3eZbrw1aBm2rZgRNFdxV2595E9CY3gmdALWMmHkvFXO7tYXAYM8P"}"#;
    let (st, ct, b) = s
        .deliver_hook(
            "slack",
            &sign_slack(&slack_secret, now, challenge),
            challenge,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert!(ct.starts_with("text/plain"), "{ct}");
    assert_eq!(
        &b[..],
        b"3eZbrw1aBm2rZgRNFdxV2595E9CY3gmdALWMmHkvFXO7tYXAYM8P"
    );
    // ... only after verification.
    let (st, _, _) = s
        .deliver_hook("slack", &sign_slack(b"wrong", now, challenge), challenge)
        .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    // A Slack retry (X-Slack-Retry-Num, re-signed later) is the same delivery.
    let retry = with(
        sign_slack(&slack_secret, now + 5, &slack_body),
        "x-slack-retry-num",
        "1",
    );
    let v = deliver_ok(
        &s,
        "slack",
        &with(retry, "x-slack-retry-reason", "http_timeout"),
        &slack_body,
    )
    .await;
    assert_eq!(v["duplicate"], true, "{v}");

    // Unknown hook: 404.
    let (st, _) = deliver(&s, "nope", &Vec::new(), b"{}").await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    assert_eq!(s.list_all("workflow=sdk-echo").await.len(), cases.len());

    // The delivery counter.
    let m = metrics_text(&s).await;
    for needle in [
        "ostro_webhook_requests_total",
        "outcome=\"ok\"",
        "outcome=\"duplicate\"",
        "outcome=\"unauthorized\"",
        "outcome=\"not_found\"",
        "hook=\"stripe\"",
    ] {
        assert!(m.contains(needle), "{needle} missing from /metrics");
    }
    assert!(
        !m.contains("hook=\"nope\""),
        "unknown hook names are not labels"
    );
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// The injected clock feeds the replay window
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stripe_tolerance_uses_the_injected_clock() {
    // 2023-11-14T22:13:20Z: far from the system clock.
    let t0: i64 = 1_700_000_000;
    let clock = Arc::new(ManualClock::new(u64::try_from(t0).expect("ms") * 1000));
    let dir = tmpfs_dir();
    let cfg = server_config(dir.path(), &ServerOpts::default());
    let hooks = Arc::new(ExecutorHooks::default());
    let handle = start_with_options(
        cfg.clone(),
        ServerOptions::default()
            .with_clock(clock.clone())
            .with_hooks(Arc::clone(&hooks)),
    )
    .await
    .expect("server start");
    let mut s = TestServer {
        handle: Some(handle),
        cfg,
        hooks,
        http: reqwest::Client::new(),
        dir: Some(dir),
    };
    s.wait_ready(Duration::from_secs(10)).await;
    s.deploy_guest("sdk-basic", None).await;
    let secret = b"whsec_clock".to_vec();
    put_secret(&s, "_/stripe", &secret).await;
    create(
        &s,
        json!({"name": "stripe", "action": {"kind": "start", "workflow": "sdk-echo"},
        "verifier": {"kind": "stripe", "secret_ref": "stripe", "tolerance_s": 60}}),
    )
    .await;

    // Signed at the system time: stale for the server's clock.
    let body = br#"{"id":"evt_a"}"#;
    let (st, v) = deliver(&s, "stripe", &sign_stripe(&secret, now_s(), body), body).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED, "{v}");
    assert_eq!(v["error"]["details"]["reason"], "stale");
    // Signed at the injected time (within 60 s): accepted.
    let v = deliver_ok(&s, "stripe", &sign_stripe(&secret, t0 - 59, body), body).await;
    assert_eq!(v["created"], true, "{v}");
    // Advance the clock: the same timestamp is now stale.
    clock.advance(120_000);
    let body2 = br#"{"id":"evt_b"}"#;
    let (st, v) = deliver(&s, "stripe", &sign_stripe(&secret, t0, body2), body2).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED, "{v}");
    assert_eq!(s.list_all("workflow=sdk-echo").await.len(), 1);
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// signal
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signal_action_targets_runs_and_dedupes_redelivery() {
    let mut s = server().await;
    s.deploy_guest("signals", None).await;
    let stripe = b"whsec_signal".to_vec();
    let gh = b"gh-signal-secret".to_vec();
    put_secret(&s, "_/stripe", &stripe).await;
    put_secret(&s, "_/gh", &gh).await;
    // Hook A starts the run keyed by the checkout session id (SC1 shape).
    create(
        &s,
        json!({"name": "checkout", "verifier": {"kind": "stripe", "secret_ref": "stripe"},
        "action": {"kind": "start", "workflow": "signals", "input": "/data/object/start"},
        "idempotency": "json:/data/object/id"}),
    )
    .await;
    // Hook B signals the run found by (workflow, the same session id).
    create(
        &s,
        json!({"name": "paid", "verifier": {"kind": "github", "secret_ref": "gh"},
        "action": {"kind": "signal", "workflow": "signals", "name": "x",
                   "run_by": {"idempotency_pointer": "/session"}}}),
    )
    .await;
    // Hook C signals by run id.
    create(
        &s,
        json!({"name": "by-id", "verifier": {"kind": "github", "secret_ref": "gh"},
        "action": {"kind": "signal", "workflow": "signals", "name": "x",
                   "run_by": {"run_id_pointer": "/run_id"}}}),
    )
    .await;

    let now = now_s();
    let start = br#"{"id":"evt_s","data":{"object":{"id":"cs_1","start":{"mode":"fifo"}}}}"#;
    let v = deliver_ok(&s, "checkout", &sign_stripe(&stripe, now, start), start).await;
    let run_id = v["run_id"].as_str().expect("run").to_owned();

    let gh_delivery = |id: &str, body: &[u8]| with(sign_github(&gh, body), "x-github-delivery", id);
    let b1 = br#"{"session":"cs_1","n":1}"#;
    let v = deliver_ok(&s, "paid", &gh_delivery("d1", b1), b1).await;
    assert_eq!(v["run_id"], run_id.as_str(), "{v}");
    assert_eq!(v["duplicate"], false);
    // Redelivery of d1: SignalSeen, applied once.
    let v = deliver_ok(&s, "paid", &gh_delivery("d1", b1), b1).await;
    assert_eq!(v["duplicate"], true, "{v}");
    let b2 = br#"{"session":"cs_1","n":2}"#;
    deliver_ok(&s, "paid", &gh_delivery("d2", b2), b2).await;
    let b3 = format!(r#"{{"run_id":"{run_id}","n":3}}"#);
    let v = deliver_ok(
        &s,
        "by-id",
        &gh_delivery("d3", b3.as_bytes()),
        b3.as_bytes(),
    )
    .await;
    assert_eq!(v["run_id"], run_id.as_str());

    s.wait_run_status(&run_id, "completed", LIMIT).await;
    let got: Value = serde_json::from_slice(
        &s.result_bytes(&run_id.parse().expect("id"))
            .await
            .expect("result"),
    )
    .expect("json");
    let ns: Vec<Value> = got
        .as_array()
        .expect("array")
        .iter()
        .map(|p| p["n"].clone())
        .collect();
    assert_eq!(ns, vec![json!(1), json!(2), json!(3)], "{got}");

    // Redeliveries after the run finished (08-20 found these answered 409): a delivery
    // whose signal applied before the run ended is a 200 duplicate, by either targeting.
    let v = deliver_ok(&s, "paid", &gh_delivery("d1", b1), b1).await;
    assert_eq!(
        (v["duplicate"].clone(), v["run_id"].clone()),
        (json!(true), json!(run_id)),
        "{v}"
    );
    let v = deliver_ok(
        &s,
        "by-id",
        &gh_delivery("d3", b3.as_bytes()),
        b3.as_bytes(),
    )
    .await;
    assert_eq!(v["duplicate"], true, "{v}");
    // A new delivery to the finished run never applied: 409 run_terminal.
    let b4 = br#"{"session":"cs_1","n":4}"#;
    let (st, v) = deliver(&s, "paid", &gh_delivery("d4", b4), b4).await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(v["error"]["details"]["reason"], "run_terminal", "{v}");

    // Unknown targets: 404, nothing submitted.
    let b = br#"{"session":"cs_unknown","n":9}"#;
    let (st, v) = deliver(&s, "paid", &gh_delivery("d9", b), b).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");
    let b = br#"{"run_id":"not-a-run","n":9}"#;
    let (st, v) = deliver(&s, "by-id", &gh_delivery("d10", b), b).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");
    // A bad signature on a signal hook: 401.
    let (st, _) = deliver(
        &s,
        "paid",
        &with(sign_github(b"x", b1), "x-github-delivery", "d11"),
        b1,
    )
    .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    assert_eq!(s.list_all("workflow=signals").await, vec![run_id]);
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// complete_token
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn complete_token_action_completes_an_await_token() {
    let mut s = server().await;
    s.deploy_guest("tokens", None).await;
    let secret = b"token-hook-secret".to_vec();
    put_secret(&s, "_/tok", &secret).await;
    create(
        &s,
        json!({"name": "approve",
        "verifier": {"kind": "hmac", "secret_ref": "tok", "header": "x-signature",
                     "encoding": "base64"},
        "action": {"kind": "complete_token", "token_pointer": "/token"}}),
    )
    .await;
    let id = s
        .start_rest("tokens", json!({"input": {}}))
        .await
        .to_string();
    let token = s.published_token(&id, LIMIT).await;
    let body = json!({"token": token, "approved": true}).to_string();
    let h: Headers = vec![(
        "x-signature".into(),
        B64.encode(hmac_sha256(&secret, &[body.as_bytes()])),
    )];
    let v = deliver_ok(&s, "approve", &h, body.as_bytes()).await;
    assert_eq!(v["run_id"], id.as_str(), "{v}");
    assert_eq!(v["duplicate"], false);
    s.wait_run_status(&id, "completed", LIMIT).await;
    let got: Value = serde_json::from_slice(
        &s.result_bytes(&id.parse().expect("id"))
            .await
            .expect("result"),
    )
    .expect("json");
    assert_eq!(got["approved"], true, "{got}");
    // Redelivery: 200 duplicate.
    let v = deliver_ok(&s, "approve", &h, body.as_bytes()).await;
    assert_eq!(v["duplicate"], true, "{v}");
    // A body without the token: 400 after verification.
    let b = br#"{"approved":true}"#;
    let h: Headers = vec![("x-signature".into(), B64.encode(hmac_sha256(&secret, &[b])))];
    let (st, v) = deliver(&s, "approve", &h, b).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// CRUD
// ---------------------------------------------------------------------------------

const CANARY: &[u8] = b"CANARY-webhook-secret-7f3a91";

fn assert_no_secret(what: &str, text: &str) {
    let forms = [
        String::from_utf8_lossy(CANARY).into_owned(),
        hex(CANARY),
        B64.encode(CANARY),
    ];
    for f in forms {
        assert!(!text.contains(&f), "{what} leaks the secret ({f}): {text}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crud_rest_and_grpc_validate_and_never_leak_secrets() {
    let mut s = server().await;
    s.deploy_guest("sdk-basic", None).await;
    put_secret(&s, "_/canary", CANARY).await;

    let base = json!({"name": "c1", "action": {"kind": "start", "workflow": "sdk-echo"},
        "verifier": {"kind": "stripe", "secret_ref": "canary", "tolerance_s": 120}});
    let created = create(&s, base.clone()).await;
    assert_no_secret("create", &created.to_string());
    assert_eq!(created["verifier"]["secret_ref"], "canary");
    assert_eq!(created["verifier"]["tolerance_s"], 120);
    let tag = created["version_tag"].as_u64().expect("tag");

    // Duplicate name: 409.
    let (st, v) = s.create_webhook(&base).await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");

    // Validation: 400.
    for (why, bad) in [
        (
            "github tolerance",
            json!({"name": "b1", "action": {"kind": "start", "workflow": "sdk-echo"},
            "verifier": {"kind": "github", "secret_ref": "canary", "tolerance_s": 5}}),
        ),
        (
            "hmac without header",
            json!({"name": "b2", "action": {"kind": "start", "workflow": "sdk-echo"},
            "verifier": {"kind": "hmac", "secret_ref": "canary"}}),
        ),
        (
            "hmac sha1",
            json!({"name": "b3", "action": {"kind": "start", "workflow": "sdk-echo"},
            "verifier": {"kind": "hmac", "secret_ref": "canary", "header": "x", "algorithm": "sha1"}}),
        ),
        (
            "stripe with hmac option",
            json!({"name": "b4", "action": {"kind": "start", "workflow": "sdk-echo"},
            "verifier": {"kind": "stripe", "secret_ref": "canary", "header": "x"}}),
        ),
        (
            "missing secret",
            json!({"name": "b5", "action": {"kind": "start", "workflow": "sdk-echo"},
            "verifier": {"kind": "stripe", "secret_ref": "no-such-secret"}}),
        ),
        (
            "missing workflow",
            json!({"name": "b6", "action": {"kind": "start", "workflow": "nope"},
            "verifier": {"kind": "stripe", "secret_ref": "canary"}}),
        ),
        (
            "bad extractor",
            json!({"name": "b7", "action": {"kind": "start", "workflow": "sdk-echo"},
            "verifier": {"kind": "stripe", "secret_ref": "canary"}, "idempotency": "header:bad header"}),
        ),
        (
            "reserved name",
            json!({"name": "__x", "action": {"kind": "start", "workflow": "sdk-echo"}}),
        ),
    ] {
        let (st, v) = s.create_webhook(&bad).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{why}: {v}");
        assert_no_secret(why, &v.to_string());
    }

    // GET / list.
    let (st, v) = s.get_json("/v1/webhooks/c1").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v, created);
    let (st, v) = s.get_json("/v1/webhooks").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["webhooks"].as_array().expect("list").len(), 1);
    assert_no_secret("list", &v.to_string());
    let (st, _) = s.get_json("/v1/webhooks/missing").await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // PUT: stale tag 409, current tag ok.
    let mut upd = base.clone();
    upd["verifier"]["tolerance_s"] = json!(200);
    upd["version_tag"] = json!(tag + 7);
    let (st, v) = s.put_json("/v1/webhooks/c1", &upd).await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(v["error"]["details"]["reason"], "version_mismatch");
    upd["version_tag"] = json!(tag);
    let (st, v) = s.put_json("/v1/webhooks/c1", &upd).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["verifier"]["tolerance_s"], 200);
    let tag2 = v["version_tag"].as_u64().expect("tag");
    assert!(tag2 > tag);
    assert_no_secret("update", &v.to_string());
    let mut missing = base.clone();
    missing["name"] = json!("missing");
    let (st, v) = s.put_json("/v1/webhooks/missing", &missing).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");

    // Deliveries never echo the secret either (401 and 404 bodies).
    let body = br#"{"id":"evt_x"}"#;
    let (st, _, b) = s
        .deliver_hook("c1", &sign_stripe(b"wrong", now_s(), body), body)
        .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    assert_no_secret("401", &String::from_utf8_lossy(&b));

    // DELETE: stale tag 409, then ok, then 404.
    let (st, v) = s
        .delete_json(&format!("/v1/webhooks/c1?version_tag={tag}"))
        .await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    let (st, v) = s
        .delete_json(&format!("/v1/webhooks/c1?version_tag={tag2}"))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let (st, _) = s.delete_json("/v1/webhooks/c1").await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // gRPC: the same controller.
    let mut g = pb::webhooks_client::WebhooksClient::new(s.channel().await);
    let w = pb::Webhook {
        name: "g1".into(),
        action: Some(pb::WebhookAction {
            kind: Some(pb::webhook_action::Kind::Signal(pb::WebhookSignalAction {
                workflow: "sdk-echo".into(),
                name: "x".into(),
                run_by: Some(pb::webhook_signal_action::RunBy::IdempotencyPointer(
                    "/id".into(),
                )),
            })),
        }),
        verifier: Some(pb::WebhookVerifier {
            kind: pb::WebhookVerifierKind::Github as i32,
            secret_ref: "canary".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let made = g
        .create(pb::CreateWebhookRequest {
            webhook: Some(w.clone()),
        })
        .await
        .expect("grpc create")
        .into_inner();
    assert_no_secret("grpc create", &format!("{made:?}"));
    let got = g
        .get(pb::GetWebhookRequest { name: "g1".into() })
        .await
        .expect("grpc get")
        .into_inner();
    assert_eq!(got, made);
    let e = g
        .create(pb::CreateWebhookRequest {
            webhook: Some(w.clone()),
        })
        .await
        .expect_err("duplicate");
    assert_eq!(e.code(), tonic::Code::AlreadyExists, "{e:?}");
    let mut bad = w.clone();
    bad.name = "g2".into();
    bad.verifier = Some(pb::WebhookVerifier {
        kind: pb::WebhookVerifierKind::Github as i32,
        secret_ref: "absent".into(),
        ..Default::default()
    });
    let e = g
        .create(pb::CreateWebhookRequest { webhook: Some(bad) })
        .await
        .expect_err("missing secret");
    assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e:?}");
    let listed = g
        .list(pb::ListWebhooksRequest {})
        .await
        .expect("grpc list")
        .into_inner();
    assert_eq!(listed.webhooks.len(), 1);
    assert_no_secret("grpc list", &format!("{listed:?}"));
    let e = g
        .delete(pb::DeleteWebhookRequest {
            name: "g1".into(),
            version_tag: Some(made.version_tag.unwrap_or(0) + 9),
        })
        .await
        .expect_err("stale");
    assert_eq!(e.code(), tonic::Code::AlreadyExists, "{e:?}");
    g.delete(pb::DeleteWebhookRequest {
        name: "g1".into(),
        version_tag: made.version_tag,
    })
    .await
    .expect("grpc delete");
    s.shutdown().await;
}
