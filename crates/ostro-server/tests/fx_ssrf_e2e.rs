//! ROADMAP Phase 6 SC4 (SSRF), through a guest (plan 06-17).
//!
//! - **bypass table**: the fx-ssrf guest GETs every payload below on a server with the
//!   **default** `[egress]` config (no `allow_private`). The module's egress policy lists
//!   every host and IP literal of the table, so the allow-list cannot be the denier; only the
//!   classifier / gate can (W2). Each effect completes `egress_denied` with the expected
//!   `details.reason`, read from the journaled `EffectCompleted` failure;
//! - **redirects**: the mock's own address is allowed (IP rule + `allow_private =
//!   127.0.0.1/32`) and the redirect targets are allowed too; an untemplated request is
//!   denied at the hop by the classifier, a templated one gets the 302 back unchanged;
//! - **env proxies**: `HTTPS_PROXY` / `HTTP_PROXY` / `ALL_PROXY` point at a recording
//!   listener while the server starts; a request to the mock never touches it;
//! - **allow-lists**: no policy denies everything; scheme, port and wildcard depth are
//!   enforced per module.
//!
//! No internet: every row is an IP literal or loopback; DNS-name rows are covered by 06-04's
//! `StaticLookup` table (`crates/ostro-bridge/tests/ssrf_table.rs`).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::fx::{configure, deploy_fx, wait_terminal};
use common::{
    BridgeTestOpts, TestServer, bridge_server_config, start_server_with_bridge, tmpfs_dir,
};
use ostro_server::executor::ExecutorHooks;
use ostro_testkit::mock_provider::{MockProvider, RecordingListener};
use ostro_types::RunId;
use ostro_types::journal::{EffectRequest, Event, EventBody, Failure, OpOutput, decode_result};
use serde_json::{Value, json};

/// Every test of this file holds it: one of them mutates the process environment.
static ENV: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const RUN_LIMIT: Duration = Duration::from_secs(60);

/// Canary secret value for the templated redirect row.
const CANARY: &str = "canary-ssrf-0e1d2c3b4a5f6978";

/// (payload, the allow rule naming its host in normalised form, accepted
/// `details.reason` values). The rules make the allow-list admit every row (W2).
const TABLE: &[(&str, &str, &[&str])] = &[
    // loopback and its literal encodings
    (
        "http://127.0.0.1/",
        "http://127.0.0.1",
        &["ip_literal", "blocked"],
    ),
    (
        "http://2130706433/",
        "http://127.0.0.1",
        &["ip_literal", "blocked"],
    ),
    (
        "http://0177.0.0.1/",
        "http://127.0.0.1",
        &["ip_literal", "blocked"],
    ),
    (
        "http://0x7f.1/",
        "http://127.0.0.1",
        &["ip_literal", "blocked"],
    ),
    (
        "http://127.1/",
        "http://127.0.0.1",
        &["ip_literal", "blocked"],
    ),
    (
        "http://%31%32%37.0.0.1/",
        "http://127.0.0.1",
        &["ip_literal", "blocked"],
    ),
    ("http://0/", "http://0.0.0.0", &["ip_literal", "blocked"]),
    // cloud metadata v4
    (
        "http://169.254.169.254/",
        "http://169.254.169.254",
        &["ip_literal", "blocked"],
    ),
    (
        "http://169.254.170.2/",
        "http://169.254.170.2",
        &["ip_literal", "blocked"],
    ),
    (
        "http://100.100.100.200/",
        "http://100.100.100.200",
        &["ip_literal", "blocked"],
    ),
    // v6: ULA metadata, mapped, compatible, NAT64, 6to4, Teredo, loopback, link-local
    (
        "http://[fd00:ec2::254]/",
        "http://[fd00:ec2::254]",
        &["ip_literal", "blocked"],
    ),
    (
        "http://[fd20:ce::254]/",
        "http://[fd20:ce::254]",
        &["ip_literal", "blocked"],
    ),
    (
        "http://[::ffff:169.254.169.254]/",
        "http://[::ffff:a9fe:a9fe]",
        &["ip_literal", "blocked"],
    ),
    (
        "http://[::ffff:a9fe:a9fe]/",
        "http://[::ffff:a9fe:a9fe]",
        &["ip_literal", "blocked"],
    ),
    (
        "http://[::127.0.0.1]/",
        "http://[::7f00:1]",
        &["ip_literal", "blocked"],
    ),
    (
        "http://[64:ff9b::a9fe:a9fe]/",
        "http://[64:ff9b::a9fe:a9fe]",
        &["ip_literal", "blocked"],
    ),
    (
        "http://[2002:a9fe:a9fe::]/",
        "http://[2002:a9fe:a9fe::]",
        &["ip_literal", "blocked"],
    ),
    (
        "http://[2001:0::1]/",
        "http://[2001::1]",
        &["ip_literal", "blocked"],
    ),
    ("http://[::1]/", "http://[::1]", &["ip_literal", "blocked"]),
    (
        "http://[fe80::1]/",
        "http://[fe80::1]",
        &["ip_literal", "blocked"],
    ),
    // userinfo confusion
    (
        "http://user:pw@evil.com@127.0.0.1/",
        "http://127.0.0.1",
        &["userinfo"],
    ),
];

/// `localhost` resolves through the server's system resolver (hickory, /etc/hosts); a
/// resolver failure is reported as `dns`, which also denies, but is recorded separately.
const LOCALHOST: &str = "http://localhost/";

// ---------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------

/// The allow rule naming `url`'s host (no userinfo, no percent escapes or odd IP
/// spellings in it): `scheme://host[:port]`.
fn rule_for(url: &str) -> String {
    let (scheme, rest) = url.split_once("://").expect("scheme");
    let authority = rest.split('/').next().expect("authority");
    format!("{scheme}://{authority}")
}

async fn start(s: &TestServer, workflow: &str, input: Value) -> RunId {
    s.start_rest(workflow, json!({ "input": input })).await
}

/// Wait for `completed` and return the guest output.
async fn output(s: &TestServer, run: &RunId) -> Value {
    let info = wait_terminal(s, run, RUN_LIMIT).await;
    assert_eq!(
        serde_json::to_value(info.status).expect("status"),
        "completed",
        "{info:?}"
    );
    serde_json::from_slice(&s.result_bytes(run).await.expect("result")).expect("json")
}

/// `(url, outcome)` of every HTTP effect of the run, in journal order.
fn outcomes(j: &[Event]) -> Vec<(String, Result<OpOutput, Failure>)> {
    let mut out = Vec::new();
    for e in j {
        if let EventBody::EffectScheduled(sch) = &e.body
            && let EffectRequest::Http(r) = &sch.request
        {
            let done = j.iter().find_map(|c| match &c.body {
                EventBody::EffectCompleted(c) if c.op == e.seq => {
                    Some(decode_result(&c.outcome).expect("decode outcome"))
                }
                _ => None,
            });
            out.push((
                r.url.clone(),
                done.unwrap_or_else(|| panic!("no completion for op {}", e.seq)),
            ));
        }
    }
    out
}

/// `(kind, details.reason)` of a failed effect outcome.
fn denial(r: &Result<OpOutput, Failure>) -> Option<(String, String)> {
    match r {
        Err(Failure::NonRetryable(i) | Failure::Retryable(i)) => {
            let reason = i
                .details
                .as_ref()
                .and_then(|d| serde_json::from_slice::<Value>(&d.data).ok())
                .and_then(|v| v["reason"].as_str().map(str::to_owned))
                .unwrap_or_default();
            Some((i.kind.clone(), reason))
        }
        _ => None,
    }
}

/// Run fx-ssrf over `urls`; returns `(url, kind, reason)` per effect and checks the guest
/// output agrees (every item an `egress_denied` error).
async fn ssrf_run(s: &TestServer, urls: &[&str]) -> Vec<(String, String, String)> {
    let run = start(s, "fx-ssrf", json!({ "urls": urls })).await;
    let out = output(s, &run).await;
    let items = out["results"].as_array().expect("results");
    assert_eq!(items.len(), urls.len(), "{out}");
    let j = s.journal(&run).await;
    let got = outcomes(&j);
    assert_eq!(got.len(), urls.len());
    got.iter()
        .zip(items)
        .map(|((url, r), item)| {
            let (kind, reason) = denial(r).unwrap_or_else(|| panic!("{url}: ALLOWED {r:?}"));
            assert_eq!(item["error"]["kind"], kind.as_str(), "guest view of {url}");
            (url.clone(), kind, reason)
        })
        .collect()
}

// ---------------------------------------------------------------------------------
// Bypass table
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bypass_table_denied_by_default() {
    let _env = ENV.lock().await;
    // Default [egress]: no allow_private at all.
    let s = start_server_with_bridge(
        tmpfs_dir(),
        BridgeTestOpts {
            allow_private_loopback: false,
            ..BridgeTestOpts::default()
        },
    )
    .await;
    assert!(s.cfg.egress.allow_private.is_empty());
    let module = deploy_fx(&s, "fx-ssrf").await;
    let mut rules: Vec<String> = TABLE.iter().map(|(_, r, _)| (*r).to_owned()).collect();
    rules.push("http://evil.com".into());
    rules.push(rule_for(LOCALHOST));
    rules.sort();
    rules.dedup();
    let refs: Vec<&str> = rules.iter().map(String::as_str).collect();
    configure(&s).egress(&module, &refs).apply().await;

    let urls: Vec<&str> = TABLE.iter().map(|(u, _, _)| *u).collect();
    let got = ssrf_run(&s, &urls).await;
    let mut failures = Vec::new();
    for ((url, _, accepted), (_, kind, reason)) in TABLE.iter().zip(&got) {
        println!("{url:40} -> {kind} {reason}");
        if kind != "egress_denied" || !accepted.contains(&reason.as_str()) {
            failures.push(format!(
                "{url}: {kind}/{reason}, want egress_denied/{accepted:?}"
            ));
        }
    }
    assert!(failures.is_empty(), "bypass rows:\n{}", failures.join("\n"));

    // localhost: blocked via the resolver; `dns` only if the host has no resolver at all.
    let got = ssrf_run(&s, &[LOCALHOST]).await;
    let (_, kind, reason) = &got[0];
    println!("{LOCALHOST:40} -> {kind} {reason}");
    assert_eq!(kind, "egress_denied");
    if reason == "dns" {
        println!("SKIPPED localhost row: no system resolver (denied with reason dns)");
    } else {
        assert_eq!(reason, "blocked", "localhost");
    }
}

// ---------------------------------------------------------------------------------
// Redirects
// ---------------------------------------------------------------------------------

/// Percent-encode a query value.
fn q(s: &str) -> String {
    common::fx::seg(s)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redirects_to_internal_literals() {
    let _env = ENV.lock().await;
    let mock = MockProvider::start().await;
    let s = start_server_with_bridge(
        tmpfs_dir(),
        BridgeTestOpts {
            allow_private_loopback: false,
            extra_toml: "[egress]\nallow_private = [\"127.0.0.1/32\"]\n".into(),
            ..BridgeTestOpts::default()
        },
    )
    .await;
    assert_eq!(s.cfg.egress.allow_private, vec!["127.0.0.1/32".to_owned()]);
    let targets = ["http://[::1]/", "http://169.254.169.254/"];
    let mock_rule = format!("http://{}", mock.addr());
    let mut allow: Vec<String> = vec![mock_rule];
    allow.extend(targets.iter().map(|t| rule_for(t)));
    let allow: Vec<&str> = allow.iter().map(String::as_str).collect();
    let ssrf = deploy_fx(&s, "fx-ssrf").await;
    let http = deploy_fx(&s, "fx-http").await;
    configure(&s)
        .secret("tok", CANARY)
        .grant(&http, "tok", &[&mock.base_url()], false)
        .egress(&ssrf, &allow)
        .egress(&http, &allow)
        .apply()
        .await;

    let urls: Vec<String> = targets
        .iter()
        .map(|t| format!("{}/redirect?to={}", mock.base_url(), q(t)))
        .collect();

    // Untemplated: followed by the connector, denied at the hop by the classifier.
    let refs: Vec<&str> = urls.iter().map(String::as_str).collect();
    let run = start(&s, "fx-ssrf", json!({ "urls": refs })).await;
    let out = output(&s, &run).await;
    let j = s.journal(&run).await;
    for ((url, r), item) in outcomes(&j)
        .iter()
        .zip(out["results"].as_array().expect("results"))
    {
        let (kind, reason) = denial(r).unwrap_or_else(|| panic!("{url}: ALLOWED {r:?}"));
        println!("untemplated {url} -> {kind} {reason}");
        assert_eq!(kind, "egress_denied", "{url}");
        assert!(
            matches!(reason.as_str(), "ip_literal" | "blocked"),
            "{url}: {reason}"
        );
        assert_eq!(item["error"]["kind"], "egress_denied");
    }
    assert_eq!(
        mock.requests_to("/redirect"),
        2,
        "each first hop reached the mock"
    );

    // Templated: never followed, the 302 comes back unchanged.
    let reqs: Vec<Value> = urls
        .iter()
        .map(|u| {
            json!({
                "method": "GET",
                "url": u,
                "headers": [["X-Api-Key", "{{secret:tok}}"]],
            })
        })
        .collect();
    let run = start(
        &s,
        "fx-http",
        json!({ "requests": reqs, "parallel": false }),
    )
    .await;
    let out = output(&s, &run).await;
    for (item, target) in out["results"]
        .as_array()
        .expect("results")
        .iter()
        .zip(targets)
    {
        assert_eq!(
            item["status"], 302,
            "templated redirect to {target}: {item}"
        );
        let loc = item["headers"]
            .as_array()
            .expect("headers")
            .iter()
            .find(|kv| {
                kv[0]
                    .as_str()
                    .is_some_and(|k| k.eq_ignore_ascii_case("location"))
            })
            .and_then(|kv| kv[1].as_str())
            .unwrap_or_else(|| panic!("no Location: {item}"));
        assert_eq!(loc, target);
    }
    assert_eq!(mock.requests_to("/redirect"), 4);
}

// ---------------------------------------------------------------------------------
// Env proxies
// ---------------------------------------------------------------------------------

const PROXY_VARS: &[&str] = &[
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "ALL_PROXY",
    "https_proxy",
    "http_proxy",
    "all_proxy",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn env_proxies_are_ignored() {
    let _env = ENV.lock().await;
    let mock = MockProvider::start().await;
    let (proxy, accepted) = RecordingListener::start().await;
    let dir = tmpfs_dir();
    let cfg = bridge_server_config(dir.path(), &BridgeTestOpts::default());
    let hooks = Arc::new(ExecutorHooks::default());

    // The proxy variables are set only while the server (and its egress client) is built;
    // the test's own REST client is created afterwards, without them.
    for v in PROXY_VARS {
        // SAFETY: every test of this binary holds ENV, so no other thread of this test
        // process reads or writes the environment concurrently.
        unsafe { std::env::set_var(v, format!("http://{proxy}")) };
    }
    let booted = TestServer::boot(cfg.clone(), Arc::clone(&hooks)).await;
    for v in PROXY_VARS {
        // SAFETY: as above.
        unsafe { std::env::remove_var(v) };
    }
    let mut s = TestServer {
        handle: Some(booted.expect("server start")),
        cfg,
        hooks,
        http: reqwest::Client::new(),
        dir: Some(dir),
    };
    s.wait_ready(common::T).await;

    let module = deploy_fx(&s, "fx-ssrf").await;
    configure(&s)
        .egress(&module, &[&mock.base_url()])
        .apply()
        .await;
    let url = format!("{}/status/200", mock.base_url());
    let run = start(&s, "fx-ssrf", json!({ "urls": [url] })).await;
    let out = output(&s, &run).await;
    assert_eq!(out["results"][0]["status"], 200, "{out}");
    assert_eq!(mock.requests_to("/status/200"), 1, "direct to the mock");
    assert_eq!(
        accepted.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the egress client connected to the env proxy"
    );
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// Allow-lists
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_module_allow_lists() {
    let _env = ENV.lock().await;
    let mock = MockProvider::start().await;
    // Loopback allowed node-wide: only the module policies decide.
    let s = start_server_with_bridge(tmpfs_dir(), BridgeTestOpts::default()).await;
    let ssrf = deploy_fx(&s, "fx-ssrf").await;
    deploy_fx(&s, "fx-http").await; // no egress policy
    configure(&s)
        .egress(&ssrf, &[&mock.base_url()])
        .apply()
        .await;
    let url = format!("{}/status/200", mock.base_url());

    // Control: the allowed module reaches the mock.
    let run = start(&s, "fx-ssrf", json!({ "urls": [url] })).await;
    assert_eq!(output(&s, &run).await["results"][0]["status"], 200);
    assert_eq!(mock.requests_to("/status/200"), 1);

    // A module without a policy reaches nothing, not even an origin allowed elsewhere.
    let run = start(
        &s,
        "fx-http",
        json!({ "requests": [{ "method": "GET", "url": url }], "parallel": false }),
    )
    .await;
    let out = output(&s, &run).await;
    assert_eq!(out["results"][0]["error"]["kind"], "egress_denied", "{out}");
    let got = outcomes(&s.journal(&run).await);
    assert_eq!(
        denial(&got[0].1),
        Some(("egress_denied".into(), "not_allowed".into()))
    );
    assert_eq!(
        mock.requests_to("/status/200"),
        1,
        "no request from fx-http"
    );

    // https://api.example.test only: another scheme or port is refused (no DNS needed).
    configure(&s)
        .egress(&ssrf, &["https://api.example.test"])
        .apply()
        .await;
    let got = ssrf_run(
        &s,
        &["http://api.example.test/", "https://api.example.test:8443/"],
    )
    .await;
    let reasons: Vec<&str> = got.iter().map(|(_, _, r)| r.as_str()).collect();
    assert_eq!(reasons, ["scheme", "port"], "{got:?}");

    // A wildcard matches one label only.
    configure(&s)
        .egress(&ssrf, &["https://*.example.test"])
        .apply()
        .await;
    let got = ssrf_run(&s, &["https://a.b.example.test/"]).await;
    assert_eq!(got[0].2, "not_allowed", "{got:?}");
    assert_eq!(mock.total_requests(), 1);
}
