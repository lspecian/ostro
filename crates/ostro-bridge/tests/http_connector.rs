//! HTTP connector (06-10) against the testkit MockProvider: idempotency keys, the
//! unknown-outcome window at re-drive, retries, per-attempt timeout, size cap, manual
//! redirects, origin-bound templating, compression refusal and response scrubbing.
//!
//! Loopback only, with `allow_private = ["127.0.0.0/8"]` set explicitly (P-6). Run with
//! `--test-threads=1` when built with `failpoints` (failpoint config is process-global).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use ostro_bridge::config::ConnectorsConfig;
use ostro_bridge::connector::{
    CompletionError, CompletionSink, Connector, EffectCtx, GateDecision, SendGate,
};
use ostro_bridge::driver::{Bridge, EffectBase};
use ostro_bridge::egress::{Egress, EgressConfig, StaticLookup};
use ostro_bridge::http::send::COMPRESSED_REFUSED;
use ostro_bridge::http::{HTTP_TARGET_KIND, HttpConfig, HttpConnector, derive_key};
use ostro_bridge::kinds;
use ostro_bridge::meta_view::StaticMetaView;
use ostro_bridge::origin::OriginPattern;
use ostro_bridge::ratelimit::RateLimiter;
use ostro_bridge::secrets::{
    GrantSpec, ProviderChain, ProviderError, SecretProvider, SecretResolver,
};
use ostro_meta::records::{Connection, EgressPolicy};
use ostro_testkit::mock_provider::MockProvider;
use ostro_types::journal::{
    EffectRequest, ErrorInfo, Failure, Header, HttpRequest, HttpResponse, OpOutput, RecordedResult,
    decode_result,
};
use ostro_types::live::{EffectRetry, LiveEffect};
use secrecy::SecretBox;

const MODULE: &str = "t/m";
const SECRET: &str = "api-key";
const CANARY: &[u8] = b"CANARY-9d41e7a2-secret-value";
const HOUR: u64 = 3_600_000_000_000;

// ---- fixtures ----

struct MapProvider {
    values: BTreeMap<String, Vec<u8>>,
    fetches: Arc<AtomicUsize>,
}

#[async_trait]
impl SecretProvider for MapProvider {
    fn name(&self) -> &'static str {
        "map"
    }
    async fn get(&self, name: &str) -> Result<Option<SecretBox<[u8]>>, ProviderError> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .values
            .get(name)
            .map(|v| SecretBox::new(v.clone().into_boxed_slice())))
    }
}

struct Gate(GateDecision);

#[async_trait]
impl SendGate for Gate {
    async fn check(&self, _: u16, _: &str, _: u64) -> GateDecision {
        self.0
    }
}

const PROCEED: Gate = Gate(GateDecision::Proceed);

#[derive(Default)]
struct Opts {
    /// Extra egress allow rules (the mock's origin is always allowed).
    allow: Vec<String>,
    /// (secret, origin pattern, body scope).
    grants: Vec<(&'static str, String, bool)>,
    /// `http-target` config of the mock origin.
    target: Option<&'static str>,
}

struct Fx {
    mock: MockProvider,
    conn: Arc<HttpConnector>,
    fetches: Arc<AtomicUsize>,
}

async fn fx(opts: Opts) -> Fx {
    let mock = MockProvider::start().await;
    fx_with(mock, opts)
}

fn fx_with(mock: MockProvider, opts: Opts) -> Fx {
    let mut allow = vec![mock.base_url()];
    allow.extend(opts.allow);
    let mut meta = StaticMetaView::new().insert_egress_policy(EgressPolicy {
        name: MODULE.into(),
        allow,
        ext: vec![],
    });
    for (secret, origin, body) in opts.grants {
        meta = meta.insert_grant(
            GrantSpec {
                module: MODULE.into(),
                secret: secret.into(),
                origins: vec![OriginPattern::parse(&origin).unwrap()],
                body,
            }
            .to_record(),
        );
    }
    if let Some(cfg) = opts.target {
        meta = meta.insert_connection(Connection {
            name: mock.base_url(),
            kind: HTTP_TARGET_KIND.into(),
            config: cfg.as_bytes().to_vec(),
            ext: vec![],
        });
    }
    let meta = Arc::new(meta);
    let fetches = Arc::new(AtomicUsize::new(0));
    let chain = ProviderChain::new(vec![Arc::new(MapProvider {
        values: BTreeMap::from([(SECRET.to_owned(), CANARY.to_vec())]),
        fetches: fetches.clone(),
    })]);
    let resolver = SecretResolver::new(meta.clone(), chain);
    let egress = Egress::new(
        EgressConfig {
            allow_private: vec!["127.0.0.0/8".parse().unwrap()],
            ..EgressConfig::default()
        },
        Arc::new(StaticLookup::new()),
    )
    .unwrap();
    let conn = HttpConnector::new(Arc::new(egress), resolver, meta, HttpConfig::default());
    Fx {
        mock,
        conn: Arc::new(conn),
        fetches,
    }
}

fn now_nanos() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap()
}

fn ctx(run: &str) -> EffectCtx {
    EffectCtx {
        cluster_id: "c1".into(),
        partition: 0,
        run_id: run.into(),
        attempt: 1,
        module: MODULE.into(),
        tenant: None,
        cancel: tokio_util::sync::CancellationToken::new(),
    }
}

fn req(method: &str, url: &str, headers: &[(&str, &str)], body: Option<&[u8]>) -> HttpRequest {
    HttpRequest {
        method: method.into(),
        url: url.into(),
        headers: headers
            .iter()
            .map(|(n, v)| Header {
                name: (*n).into(),
                value: (*v).into(),
            })
            .collect(),
        body: body.map(<[u8]>::to_vec),
        timeout_ms: None,
        idempotency_key: None,
        redact: vec![],
    }
}

fn live(op: u64, r: HttpRequest, retry: Option<EffectRetry>) -> LiveEffect {
    LiveEffect {
        op,
        request: EffectRequest::Http(r),
        http_retry: retry,
        scheduled_ts_nanos: now_nanos(),
        redrive: false,
    }
}

fn fast_retry(max_attempts: u32) -> EffectRetry {
    EffectRetry {
        max_attempts,
        initial_interval_ms: 20,
        max_interval_ms: 100,
        backoff_coefficient: 2.0,
        non_retryable_kinds: vec![],
    }
}

async fn run(fx: &Fx, c: &EffectCtx, e: &LiveEffect) -> Result<OpOutput, Failure> {
    fx.conn.execute(c, e, &PROCEED).await
}

fn ok(r: Result<OpOutput, Failure>) -> HttpResponse {
    match r {
        Ok(OpOutput::Http(h)) => h,
        other => panic!("expected an HTTP response, got {other:?}"),
    }
}

fn nr(r: Result<OpOutput, Failure>) -> ErrorInfo {
    match r {
        Err(Failure::NonRetryable(i)) => i,
        other => panic!("expected NonRetryable, got {other:?}"),
    }
}

fn details(i: &ErrorInfo) -> serde_json::Value {
    serde_json::from_slice(&i.details.as_ref().expect("details").data).unwrap()
}

fn echo_json(h: &HttpResponse) -> serde_json::Value {
    serde_json::from_slice(&h.body).expect("echo JSON")
}

/// Values of request header `name` the echo endpoint saw.
fn echoed(h: &HttpResponse, name: &str) -> Vec<String> {
    echo_json(h)["headers"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|p| p[0].as_str().unwrap().eq_ignore_ascii_case(name))
        .map(|p| p[1].as_str().unwrap().to_owned())
        .collect()
}

fn enc(url: &str) -> String {
    url::form_urlencoded::byte_serialize(url.as_bytes()).collect()
}

fn header<'a>(h: &'a HttpResponse, name: &str) -> Option<&'a str> {
    h.headers
        .iter()
        .find(|x| x.name.eq_ignore_ascii_case(name))
        .map(|x| x.value.as_str())
}

/// Every encoding of the canary a leak could take.
fn canary_encodings() -> Vec<Vec<u8>> {
    let hex_lower: String = CANARY.iter().map(|b| format!("{b:02x}")).collect();
    let pct: String = CANARY.iter().map(|b| format!("%{b:02X}")).collect();
    vec![
        CANARY.to_vec(),
        hex_lower.clone().into_bytes(),
        hex_lower.to_uppercase().into_bytes(),
        STANDARD
            .encode(CANARY)
            .trim_end_matches('=')
            .as_bytes()
            .to_vec(),
        URL_SAFE_NO_PAD.encode(CANARY).into_bytes(),
        pct.into_bytes(),
    ]
}

fn assert_no_canary(bytes: &[u8], what: &str) {
    for e in canary_encodings() {
        assert!(
            !bytes.windows(e.len()).any(|w| w == e.as_slice()),
            "canary ({}) leaked into {what}",
            String::from_utf8_lossy(&e)
        );
    }
}

fn assert_response_clean(h: &HttpResponse) {
    assert_no_canary(&h.body, "the response body");
    for x in &h.headers {
        assert_no_canary(x.value.as_bytes(), "a response header");
    }
    assert_no_canary(format!("{h:?}").as_bytes(), "Debug of the response");
}

// ---- idempotency key ----

#[tokio::test]
async fn key_is_present_and_stable_across_attempts() {
    let fx = fx(Opts::default()).await;
    let c = ctx("run-key-stable");
    let url = format!("{}/flaky?fail=1", fx.mock.base_url());
    let e = live(4, req("POST", &url, &[], None), Some(fast_retry(3)));
    let h = ok(run(&fx, &c, &e).await);
    assert_eq!(h.status, 200);
    let key = derive_key("run-key-stable", 4);
    assert_eq!(fx.mock.requests_to("/flaky"), 2);
    assert_eq!(
        fx.mock.requests(&key),
        2,
        "both attempts carried the same key"
    );

    // The derived key on the wire.
    let e = live(
        5,
        req("GET", &format!("{}/echo", fx.mock.base_url()), &[], None),
        None,
    );
    let h = ok(run(&fx, &c, &e).await);
    assert_eq!(
        echoed(&h, "idempotency-key"),
        vec![derive_key("run-key-stable", 5)]
    );
}

#[tokio::test]
async fn override_and_guest_header_are_used_verbatim_once() {
    let fx = fx(Opts::default()).await;
    let c = ctx("run-key-override");
    let url = format!("{}/echo", fx.mock.base_url());
    let mut r = req("POST", &url, &[("Idempotency-Key", "guest-value")], None);
    r.idempotency_key = Some("Order#42 custom".into());
    let h = ok(run(&fx, &c, &live(1, r, None)).await);
    assert_eq!(echoed(&h, "idempotency-key"), vec!["Order#42 custom"]);
    assert_eq!(fx.mock.requests("Order#42 custom"), 1);

    let r = req("POST", &url, &[("idempotency-key", "guest-value")], None);
    let h = ok(run(&fx, &c, &live(2, r, None)).await);
    assert_eq!(echoed(&h, "idempotency-key"), vec!["guest-value"]);
}

#[tokio::test]
async fn key_header_name_is_configurable_and_suppressible() {
    let fx = fx(Opts {
        target: Some(r#"{"v":1,"idempotency_header":"PayPal-Request-Id"}"#),
        ..Opts::default()
    })
    .await;
    let c = ctx("run-key-name");
    let url = format!("{}/echo", fx.mock.base_url());
    let h = ok(run(&fx, &c, &live(1, req("POST", &url, &[], None), None)).await);
    assert_eq!(
        echoed(&h, "paypal-request-id"),
        vec![derive_key("run-key-name", 1)]
    );
    assert!(echoed(&h, "idempotency-key").is_empty());

    let fx = fx_with(
        MockProvider::start().await,
        Opts {
            target: Some(r#"{"v":1,"idempotency_header":null}"#),
            ..Opts::default()
        },
    );
    let url = format!("{}/echo", fx.mock.base_url());
    let h = ok(run(&fx, &c, &live(1, req("POST", &url, &[], None), None)).await);
    assert!(echoed(&h, "idempotency-key").is_empty());
}

#[tokio::test]
async fn invalid_http_target_is_connection_unknown() {
    let fx = fx(Opts {
        target: Some(r#"{"v":9}"#),
        ..Opts::default()
    })
    .await;
    let url = format!("{}/echo", fx.mock.base_url());
    let i = nr(run(&fx, &ctx("r"), &live(1, req("GET", &url, &[], None), None)).await);
    assert_eq!(i.kind, kinds::CONNECTION_UNKNOWN);
    assert_eq!(fx.mock.total_requests(), 0);
}

#[tokio::test]
async fn concurrent_same_key_gets_409_retried_and_one_charge() {
    let fx = fx(Opts::default()).await;
    let c = ctx("run-pay-concurrent");
    let url = format!("{}/pay?delay_ms=300", fx.mock.base_url());
    let retry = EffectRetry {
        max_attempts: 10,
        initial_interval_ms: 100,
        max_interval_ms: 200,
        backoff_coefficient: 1.5,
        non_retryable_kinds: vec![],
    };
    let e = live(3, req("POST", &url, &[], None), Some(retry));
    let (a, b) = tokio::join!(run(&fx, &c, &e), async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        run(&fx, &c, &e).await
    });
    let (a, b) = (ok(a), ok(b));
    assert_eq!((a.status, b.status), (200, 200));
    assert_eq!(echo_json(&a)["charge_id"], echo_json(&b)["charge_id"]);
    let key = derive_key("run-pay-concurrent", 3);
    assert_eq!(fx.mock.charges(&key), 1);
    assert!(
        fx.mock.requests(&key) >= 3,
        "the second call saw a 409 and retried: {}",
        fx.mock.requests(&key)
    );
}

// ---- timeouts, caps, statuses ----

#[tokio::test]
async fn timeout_per_attempt_then_retryable_transport() {
    let fx = fx(Opts::default()).await;
    let url = format!("{}/slow?ms=3000", fx.mock.base_url());
    let mut r = req("GET", &url, &[], None);
    r.timeout_ms = Some(200);
    let started = std::time::Instant::now();
    match run(&fx, &ctx("run-timeout"), &live(1, r, Some(fast_retry(2)))).await {
        Err(Failure::Retryable(i)) => {
            assert_eq!(i.kind, kinds::HTTP_TRANSPORT);
            assert!(i.message.contains("timed out"), "{}", i.message);
        }
        other => panic!("expected Retryable http_transport, got {other:?}"),
    }
    assert_eq!(fx.mock.requests_to("/slow"), 2);
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn response_over_cap_is_response_too_large() {
    let fx = fx(Opts {
        target: Some(r#"{"v":1,"max_response_bytes":1024}"#),
        ..Opts::default()
    })
    .await;
    let c = ctx("run-cap");
    for q in ["bytes=5000", "bytes=5000&no_length=1"] {
        let url = format!("{}/big?{q}", fx.mock.base_url());
        let i = nr(run(&fx, &c, &live(1, req("GET", &url, &[], None), None)).await);
        assert_eq!(i.kind, kinds::RESPONSE_TOO_LARGE, "{q}");
        assert_eq!(details(&i)["cap"], 1024);
    }
    let url = format!("{}/big?bytes=1024&no_length=1", fx.mock.base_url());
    let h = ok(run(&fx, &c, &live(2, req("GET", &url, &[], None), None)).await);
    assert_eq!(h.body.len(), 1024);
    assert_eq!(
        header(&h, "content-length"),
        None,
        "streamed without a length"
    );
}

#[tokio::test]
async fn final_status_is_a_successful_completion() {
    let fx = fx(Opts::default()).await;
    let c = ctx("run-status");
    let url = format!("{}/status/500", fx.mock.base_url());
    let h = ok(run(
        &fx,
        &c,
        &live(1, req("GET", &url, &[], None), Some(fast_retry(3))),
    )
    .await);
    assert_eq!(h.status, 500);
    assert_eq!(fx.mock.requests_to("/status/500"), 3);
    // No policy: statuses are never retried.
    let h = ok(run(&fx, &c, &live(2, req("GET", &url, &[], None), None)).await);
    assert_eq!(h.status, 500);
    assert_eq!(fx.mock.requests_to("/status/500"), 4);
    let url = format!("{}/status/404", fx.mock.base_url());
    let h = ok(run(
        &fx,
        &c,
        &live(3, req("GET", &url, &[], None), Some(fast_retry(3))),
    )
    .await);
    assert_eq!(h.status, 404);
    assert_eq!(fx.mock.requests_to("/status/404"), 1);
}

// ---- redirects ----

fn grant_mock(mock_origin: &str, body: bool) -> Vec<(&'static str, String, bool)> {
    vec![(SECRET, mock_origin.to_owned(), body)]
}

#[tokio::test]
async fn templated_redirect_is_returned_not_followed() {
    let mock = MockProvider::start().await;
    let base = mock.base_url();
    let fx = fx_with(
        mock,
        Opts {
            grants: grant_mock(&base, false),
            ..Opts::default()
        },
    );
    let url = format!("{base}/redirect?to={}", enc(&format!("{base}/echo")));
    let r = req("GET", &url, &[("x-api-key", "{{secret:api-key}}")], None);
    let h = ok(run(&fx, &ctx("run-tpl-redirect"), &live(1, r, None)).await);
    assert_eq!(h.status, 302);
    assert_eq!(fx.mock.requests_to("/echo"), 0);
    assert_response_clean(&h);
}

#[tokio::test]
async fn untemplated_redirect_is_followed_with_method_rewrite_and_cross_origin_stripping() {
    let other = MockProvider::start().await;
    let fx = fx(Opts {
        allow: vec![other.base_url()],
        ..Opts::default()
    })
    .await;
    let base = fx.mock.base_url();
    let c = ctx("run-redirect");

    // Same origin: followed, POST -> GET on 302, key kept.
    let url = format!("{base}/redirect?to={}", enc(&format!("{base}/echo")));
    let r = req(
        "POST",
        &url,
        &[("authorization", "Bearer plain")],
        Some(b"x=1"),
    );
    let h = ok(run(&fx, &c, &live(1, r, None)).await);
    assert_eq!(h.status, 200);
    let j = echo_json(&h);
    assert_eq!(j["method"], "GET");
    assert_eq!(j["body_b64"], "");
    assert_eq!(echoed(&h, "authorization"), vec!["Bearer plain"]);
    assert_eq!(
        echoed(&h, "idempotency-key"),
        vec![derive_key("run-redirect", 1)]
    );

    // 307 keeps the method and body.
    let url = format!(
        "{base}/redirect?code=307&to={}",
        enc(&format!("{base}/echo"))
    );
    let h = ok(run(
        &fx,
        &c,
        &live(2, req("POST", &url, &[], Some(b"x=1")), None),
    )
    .await);
    assert_eq!(echo_json(&h)["method"], "POST");
    assert_eq!(echo_json(&h)["body_b64"], STANDARD.encode(b"x=1"));

    // Cross origin: Authorization, Cookie and the key header are dropped.
    let url = format!(
        "{base}/redirect?to={}",
        enc(&format!("{}/echo", other.base_url()))
    );
    let r = req(
        "GET",
        &url,
        &[
            ("authorization", "Bearer plain"),
            ("cookie", "s=1"),
            ("x-keep", "1"),
        ],
        None,
    );
    let h = ok(run(&fx, &c, &live(3, r, None)).await);
    assert_eq!(h.status, 200);
    assert_eq!(other.requests_to("/echo"), 1);
    assert!(echoed(&h, "authorization").is_empty());
    assert!(echoed(&h, "cookie").is_empty());
    assert!(echoed(&h, "idempotency-key").is_empty());
    assert_eq!(echoed(&h, "x-keep"), vec!["1"]);
}

#[tokio::test]
async fn redirect_outside_the_allow_list_is_denied() {
    let fx = fx(Opts::default()).await;
    let base = fx.mock.base_url();
    let c = ctx("run-redirect-deny");
    // A host no rule lists.
    let url = format!("{base}/redirect?to={}", enc("http://other.test:1/x"));
    let i = nr(run(&fx, &c, &live(1, req("GET", &url, &[], None), None)).await);
    assert_eq!(i.kind, kinds::EGRESS_DENIED);
    assert_eq!(details(&i)["reason"], "not_allowed");
    // The mock's own host on another port: the rule admits the host, not the port.
    let other_port = fx.mock.addr().port().wrapping_add(1).max(1);
    let url = format!(
        "{base}/redirect?to={}",
        enc(&format!("http://127.0.0.1:{other_port}/"))
    );
    let i = nr(run(&fx, &c, &live(2, req("GET", &url, &[], None), None)).await);
    assert_eq!(i.kind, kinds::EGRESS_DENIED);
    assert_eq!(details(&i)["reason"], "port");
}

#[tokio::test]
async fn redirect_to_allowed_but_blocked_literals_is_denied_by_the_classifier() {
    // Both targets are in the module's allow rules; only the classifier can deny them (W2).
    let fx = fx(Opts {
        allow: vec!["http://[::1]".into(), "http://169.254.169.254".into()],
        ..Opts::default()
    })
    .await;
    let base = fx.mock.base_url();
    for target in ["http://[::1]/", "http://169.254.169.254/latest/meta-data/"] {
        let url = format!("{base}/redirect?to={}", enc(target));
        let i = nr(run(
            &fx,
            &ctx("run-literal"),
            &live(1, req("GET", &url, &[], None), None),
        )
        .await);
        assert_eq!(i.kind, kinds::EGRESS_DENIED, "{target}");
        let reason = details(&i)["reason"].as_str().unwrap().to_owned();
        assert!(
            reason == "ip_literal" || reason == "blocked",
            "{target}: {reason}"
        );
    }
    assert_eq!(fx.mock.requests_to("/redirect"), 2);
}

// ---- templating, grants, scrubbing ----

#[tokio::test]
async fn templated_request_sends_identity_and_refuses_compressed_responses() {
    let mock = MockProvider::start().await;
    let base = mock.base_url();
    let fx = fx_with(
        mock,
        Opts {
            grants: grant_mock(&base, false),
            ..Opts::default()
        },
    );
    let c = ctx("run-identity");
    let headers = [
        ("x-api-key", "{{secret:api-key}}"),
        ("accept-encoding", "gzip"),
        ("te", "trailers"),
    ];
    let h = ok(run(
        &fx,
        &c,
        &live(1, req("GET", &format!("{base}/echo"), &headers, None), None),
    )
    .await);
    assert_eq!(echoed(&h, "accept-encoding"), vec!["identity"]);
    assert!(echoed(&h, "te").is_empty());
    assert_response_clean(&h);

    let r = req("GET", &format!("{base}/echo?gzip=1"), &headers, None);
    let i = nr(run(&fx, &c, &live(2, r, None)).await);
    assert_eq!(i.kind, kinds::HTTP_TRANSPORT);
    assert_eq!(i.message, COMPRESSED_REFUSED);
    assert_no_canary(format!("{i:?}").as_bytes(), "the failure");

    // Untemplated: the guest's encoding header is sent as is, the labelled body returned.
    let r = req(
        "GET",
        &format!("{base}/echo?gzip=1"),
        &[("accept-encoding", "gzip")],
        None,
    );
    let h = ok(run(&fx, &c, &live(3, r, None)).await);
    assert_eq!(header(&h, "content-encoding"), Some("gzip"));
    assert_eq!(echoed(&h, "accept-encoding"), vec!["gzip"]);
}

#[tokio::test]
async fn grant_for_another_origin_is_refused_before_anything_is_sent() {
    let fx = fx(Opts {
        grants: vec![(SECRET, "https://api.example.com".into(), true)],
        ..Opts::default()
    })
    .await;
    let url = format!("{}/echo", fx.mock.base_url());
    let r = req("GET", &url, &[("x-api-key", "{{secret:api-key}}")], None);
    let i = nr(run(&fx, &ctx("run-grant"), &live(1, r, None)).await);
    assert_eq!(i.kind, kinds::SECRET_NOT_GRANTED);
    assert_eq!(fx.mock.total_requests(), 0);
    assert_eq!(fx.fetches.load(Ordering::SeqCst), 0, "never fetched");
}

#[tokio::test]
async fn body_placeholder_needs_the_body_grant() {
    let mock = MockProvider::start().await;
    let base = mock.base_url();
    let fx = fx_with(
        mock,
        Opts {
            grants: grant_mock(&base, false),
            ..Opts::default()
        },
    );
    let body = br#"{"token":"{{secret:api-key}}"}"#;
    let r = req("POST", &format!("{base}/echo"), &[], Some(body));
    let i = nr(run(&fx, &ctx("run-body"), &live(1, r, None)).await);
    assert_eq!(i.kind, kinds::SECRET_NOT_GRANTED);
    assert_eq!(fx.mock.total_requests(), 0);
    assert_eq!(fx.fetches.load(Ordering::SeqCst), 0);

    // With the body scope the body is expanded and the echo comes back scrubbed.
    let mock = MockProvider::start().await;
    let base = mock.base_url();
    let fx = fx_with(
        mock,
        Opts {
            grants: grant_mock(&base, true),
            ..Opts::default()
        },
    );
    let r = req("POST", &format!("{base}/echo"), &[], Some(body));
    let h = ok(run(&fx, &ctx("run-body"), &live(1, r, None)).await);
    assert_eq!(h.status, 200);
    assert_response_clean(&h);
    assert!(
        String::from_utf8_lossy(&h.body).contains("[REDACTED]"),
        "the base64 echo of the body is scrubbed"
    );
}

#[tokio::test]
async fn echoed_secret_is_scrubbed_in_every_encoding() {
    let mock = MockProvider::start().await;
    let base = mock.base_url();
    let fx = fx_with(
        mock,
        Opts {
            grants: grant_mock(&base, false),
            ..Opts::default()
        },
    );
    let r = req(
        "GET",
        &format!("{base}/echo"),
        &[("x-api-key", "Bearer {{secret:api-key}}")],
        None,
    );
    let h = ok(run(&fx, &ctx("run-scrub"), &live(1, r, None)).await);
    assert_eq!(h.status, 200);
    let v = echoed(&h, "x-api-key");
    assert_eq!(v, vec!["Bearer [REDACTED]"]);
    assert_response_clean(&h);
    assert_eq!(fx.fetches.load(Ordering::SeqCst), 1);

    // A secret reflected in a response header is scrubbed too.
    let value: String = url::form_urlencoded::byte_serialize(CANARY).collect();
    let r = req(
        "GET",
        &format!("{base}/set-header?name=x-reflect&value={value}"),
        &[("x-api-key", "{{secret:api-key}}")],
        None,
    );
    let h = ok(run(&fx, &ctx("run-scrub"), &live(2, r, None)).await);
    assert_eq!(header(&h, "x-reflect"), Some("[REDACTED]"));
    assert_response_clean(&h);
}

#[tokio::test]
async fn redacted_response_headers() {
    let fx = fx(Opts::default()).await;
    let url = format!(
        "{}/set-header?name=x-token&value=abc123",
        fx.mock.base_url()
    );
    let mut r = req("GET", &url, &[], None);
    r.redact = vec!["X-Token".into()];
    let h = ok(run(&fx, &ctx("run-redact"), &live(1, r, None)).await);
    assert_eq!(header(&h, "x-token"), Some("[redacted]"));
}

#[tokio::test]
async fn placeholders_outside_header_values_are_egress_denied() {
    let mock = MockProvider::start().await;
    let base = mock.base_url();
    let fx = fx_with(
        mock,
        Opts {
            grants: grant_mock(&base, true),
            ..Opts::default()
        },
    );
    let c = ctx("run-ph");
    let cases = [
        req(
            "GET",
            &format!("{base}/echo?k={{{{secret:api-key}}}}"),
            &[],
            None,
        ),
        req("{{secret:api-key}}", &format!("{base}/echo"), &[], None),
        req(
            "GET",
            &format!("{base}/echo"),
            &[("x-{{secret:api-key}}", "v")],
            None,
        ),
    ];
    for r in cases {
        let i = nr(run(&fx, &c, &live(1, r, None)).await);
        assert_eq!(i.kind, kinds::EGRESS_DENIED);
    }
    assert_eq!(fx.mock.total_requests(), 0);
    assert_eq!(fx.fetches.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn abandoned_gate_cancels_before_resolution() {
    let mock = MockProvider::start().await;
    let base = mock.base_url();
    let fx = fx_with(
        mock,
        Opts {
            grants: grant_mock(&base, false),
            ..Opts::default()
        },
    );
    let c = ctx("run-abandon");
    let r = req(
        "GET",
        &format!("{base}/echo"),
        &[("x-api-key", "{{secret:api-key}}")],
        None,
    );
    let out = fx
        .conn
        .execute(&c, &live(1, r, None), &Gate(GateDecision::Abandon))
        .await;
    assert_eq!(out, Err(Failure::Cancelled));
    assert!(c.cancel.is_cancelled());
    assert_eq!(fx.mock.total_requests(), 0);
    assert_eq!(fx.fetches.load(Ordering::SeqCst), 0);
}

// ---- the unknown-outcome window at re-drive ----

fn redrive(mut e: LiveEffect, age_nanos: u64) -> LiveEffect {
    e.redrive = true;
    e.scheduled_ts_nanos = now_nanos() - age_nanos;
    e
}

#[tokio::test]
async fn redrive_post_beyond_window_is_unknown_and_not_sent() {
    let fx = fx(Opts::default()).await;
    let url = format!("{}/pay", fx.mock.base_url());
    let e = redrive(live(9, req("POST", &url, &[], None), None), 25 * HOUR);
    let i = nr(run(&fx, &ctx("run-redrive-old"), &e).await);
    assert_eq!(i.kind, kinds::EFFECT_OUTCOME_UNKNOWN);
    let key = derive_key("run-redrive-old", 9);
    let d = details(&i);
    assert_eq!(d["op"], 9);
    assert_eq!(d["key"], key);
    assert!(d["elapsed_ms"].as_u64().unwrap() >= 25 * 3_600_000);
    assert_eq!(fx.mock.requests(&key), 0);
    assert_eq!(fx.mock.total_requests(), 0);
}

#[tokio::test]
async fn redrive_get_beyond_window_is_resent() {
    let fx = fx(Opts::default()).await;
    let url = format!("{}/echo", fx.mock.base_url());
    let e = redrive(live(2, req("GET", &url, &[], None), None), 25 * HOUR);
    let h = ok(run(&fx, &ctx("run-redrive-get"), &e).await);
    assert_eq!(h.status, 200);
    assert_eq!(fx.mock.requests(&derive_key("run-redrive-get", 2)), 1);
}

#[tokio::test]
async fn redrive_post_within_window_resends_same_key_one_charge() {
    let fx = fx(Opts::default()).await;
    let c = ctx("run-redrive-in");
    let url = format!("{}/pay", fx.mock.base_url());
    let first = live(6, req("POST", &url, &[], None), None);
    let a = ok(run(&fx, &c, &first).await);
    let b = ok(run(&fx, &c, &redrive(first, HOUR)).await);
    assert_eq!((a.status, b.status), (200, 200));
    assert_eq!(echo_json(&a)["charge_id"], echo_json(&b)["charge_id"]);
    let key = derive_key("run-redrive-in", 6);
    assert_eq!(fx.mock.requests(&key), 2);
    assert_eq!(fx.mock.charges(&key), 1);
}

#[tokio::test]
async fn redrive_respects_resend_never_and_force_window() {
    let fx = fx(Opts {
        target: Some(r#"{"v":1,"resend":"never","force_window":true,"idempotency_window":"2h"}"#),
        ..Opts::default()
    })
    .await;
    let c = ctx("run-redrive-cfg");
    let url = format!("{}/echo", fx.mock.base_url());
    let i = nr(run(
        &fx,
        &c,
        &redrive(live(1, req("POST", &url, &[], None), None), 0),
    )
    .await);
    assert_eq!(i.kind, kinds::EFFECT_OUTCOME_UNKNOWN);
    // force_window applies the (never) rule to GET as well.
    let i = nr(run(
        &fx,
        &c,
        &redrive(live(2, req("GET", &url, &[], None), None), 3 * HOUR),
    )
    .await);
    assert_eq!(i.kind, kinds::EFFECT_OUTCOME_UNKNOWN);
    assert_eq!(fx.mock.total_requests(), 0);
    // A live (non-re-driven) dispatch always sends.
    let h = ok(run(&fx, &c, &live(3, req("POST", &url, &[], None), None)).await);
    assert_eq!(h.status, 200);
}

// ---- re-drive of a request still in progress at the provider (07-21 gap closure) ----

/// A re-drive (no guest policy) that meets 409 in_progress while the original send is still
/// running retries until the provider's final answer: the original's 200, one charge.
#[tokio::test]
async fn redrive_in_progress_409_is_retried_without_policy_until_final() {
    let fx = fx(Opts::default()).await;
    let c = ctx("run-redrive-inprog");
    let url = format!("{}/pay?delay_ms=700", fx.mock.base_url());
    let first = live(4, req("POST", &url, &[], None), None);
    let again = redrive(first.clone(), 0);
    let (a, b) = tokio::join!(run(&fx, &c, &first), async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        run(&fx, &c, &again).await
    });
    let (a, b) = (ok(a), ok(b));
    assert_eq!(
        (a.status, b.status),
        (200, 200),
        "{:?}",
        String::from_utf8_lossy(&b.body)
    );
    assert_eq!(echo_json(&a)["charge_id"], echo_json(&b)["charge_id"]);
    assert_eq!(header(&b, "idempotent-replayed"), Some("true"));
    let key = derive_key("run-redrive-inprog", 4);
    assert_eq!(fx.mock.charges(&key), 1);
    assert!(
        fx.mock.requests(&key) >= 3,
        "the re-drive saw at least one 409 and retried: {}",
        fx.mock.requests(&key)
    );
}

/// Unchanged: a first (live) attempt without a policy completes with the provider's 409.
#[tokio::test]
async fn first_attempt_409_in_progress_without_policy_is_the_outcome() {
    let fx = fx(Opts::default()).await;
    let c = ctx("run-live-inprog");
    let url = format!("{}/pay?delay_ms=500", fx.mock.base_url());
    let e = live(5, req("POST", &url, &[], None), None);
    let (a, b) = tokio::join!(run(&fx, &c, &e), async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        run(&fx, &c, &e).await
    });
    let (a, b) = (ok(a), ok(b));
    assert_eq!((a.status, b.status), (200, 409));
    assert_eq!(echo_json(&b)["error"], "in_progress");
    let key = derive_key("run-live-inprog", 5);
    assert_eq!(fx.mock.charges(&key), 1);
    assert_eq!(fx.mock.requests(&key), 2);
}

/// A re-drive without a key header (suppressed by the target) is not retried on 409: the
/// provider cannot be relying on a key it never saw.
#[tokio::test]
async fn redrive_409_without_key_header_is_not_retried() {
    let fx = fx(Opts {
        target: Some(r#"{"v":1,"idempotency_header":null}"#),
        ..Opts::default()
    })
    .await;
    let url = format!("{}/pay", fx.mock.base_url());
    let key = "guest-key-1";
    // The guest sends the provider key under another name, so the connector sees no key header.
    let mut r = req("POST", &url, &[], None);
    r.headers.push(Header {
        name: "Idempotency-Key".into(),
        value: key.into(),
    });
    // Hold the key in progress with a direct request, then re-drive.
    let client = reqwest::Client::new();
    let held = client
        .post(format!("{url}?delay_ms=500"))
        .header("Idempotency-Key", key)
        .send();
    let c = ctx("run-redrive-nokey");
    let e = redrive(live(1, r, None), 0);
    let (h, b) = tokio::join!(held, async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        run(&fx, &c, &e).await
    });
    assert_eq!(h.unwrap().status().as_u16(), 200);
    assert_eq!(ok(b).status, 409);
    assert_eq!(fx.mock.requests(key), 2);
}

/// A provider that keeps answering in_progress past the window ends the re-drive with
/// effect_outcome_unknown (FX-03); it is never retried forever.
#[tokio::test]
async fn redrive_in_progress_past_the_window_is_unknown() {
    let fx = fx(Opts {
        target: Some(r#"{"v":1,"idempotency_window":"1500ms","safety_margin":"0s"}"#),
        ..Opts::default()
    })
    .await;
    let c = ctx("run-redrive-stuck");
    let url = format!("{}/pay?delay_ms=4000", fx.mock.base_url());
    let first = live(8, req("POST", &url, &[], None), None);
    let again = redrive(first.clone(), 0);
    let started = std::time::Instant::now();
    let (a, b) = tokio::join!(run(&fx, &c, &first), async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let r = run(&fx, &c, &again).await;
        (r, started.elapsed())
    });
    assert_eq!(ok(a).status, 200);
    let (b, took) = b;
    let i = nr(b);
    assert_eq!(i.kind, kinds::EFFECT_OUTCOME_UNKNOWN);
    let key = derive_key("run-redrive-stuck", 8);
    let d = details(&i);
    assert_eq!(d["op"], 8);
    assert_eq!(d["key"], key);
    assert!(d["elapsed_ms"].as_u64().unwrap() < 1500, "{d}");
    assert!(
        took < Duration::from_millis(3500),
        "gave up at the window: {took:?}"
    );
    assert_eq!(fx.mock.charges(&key), 1);
    assert!(fx.mock.requests(&key) >= 3, "{}", fx.mock.requests(&key));
}

// ---- through the Bridge driver ----

#[derive(Default)]
struct FakeSink(Mutex<Vec<(u64, RecordedResult)>>);

#[async_trait]
impl CompletionSink for FakeSink {
    async fn complete(
        &self,
        _partition: u16,
        _run_id: &str,
        op: u64,
        outcome: RecordedResult,
    ) -> Result<(), CompletionError> {
        self.0.lock().unwrap().push((op, outcome));
        Ok(())
    }
}

#[tokio::test]
async fn bridge_runs_the_http_connector_end_to_end() {
    let fx = fx(Opts::default()).await;
    let sink = Arc::new(FakeSink::default());
    let mut bridge = Bridge::new(
        ConnectorsConfig::default(),
        Duration::from_secs(5),
        Arc::new(PROCEED),
        sink.clone(),
        Arc::new(RateLimiter::new(None)),
    );
    bridge.register(fx.conn.clone());
    let url = format!("{}/pay", fx.mock.base_url());
    bridge.start(
        EffectBase {
            cluster_id: "c1".into(),
            partition: 0,
            run_id: "run-bridge".into(),
            attempt: 1,
            module: MODULE.into(),
            tenant: None,
        },
        live(11, req("POST", &url, &[], None), None),
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let outcome = loop {
        if let Some((op, r)) = sink.0.lock().unwrap().first().cloned() {
            assert_eq!(op, 11);
            break r;
        }
        assert!(std::time::Instant::now() < deadline, "no completion");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let decoded: Result<OpOutput, Failure> = decode_result(&outcome).unwrap();
    let h = ok(decoded);
    assert_eq!(h.status, 200);
    assert_eq!(fx.mock.charges(&derive_key("run-bridge", 11)), 1);
    bridge.shutdown(Duration::from_secs(1)).await;
}

// ---- failpoints ----

#[cfg(feature = "failpoints")]
mod failpoints {
    use super::*;

    #[tokio::test]
    async fn before_send_error_never_sends_and_after_response_error_is_transport() {
        let scenario = fail::FailScenario::setup();
        let fx = fx(Opts::default()).await;
        let url = format!("{}/echo", fx.mock.base_url());
        let c = ctx("run-fp");

        fail::cfg("bridge::http::before_send", "return(error)").unwrap();
        match run(&fx, &c, &live(1, req("POST", &url, &[], None), None)).await {
            Err(Failure::Retryable(i)) => assert_eq!(i.kind, kinds::HTTP_TRANSPORT),
            other => panic!("{other:?}"),
        }
        assert_eq!(fx.mock.total_requests(), 0);
        fail::remove("bridge::http::before_send");

        fail::cfg("bridge::http::after_response", "return(error)").unwrap();
        match run(&fx, &c, &live(2, req("POST", &url, &[], None), None)).await {
            Err(Failure::Retryable(i)) => assert_eq!(i.kind, kinds::HTTP_TRANSPORT),
            other => panic!("{other:?}"),
        }
        assert_eq!(
            fx.mock.total_requests(),
            1,
            "sent once, not retried without a policy"
        );
        fail::remove("bridge::http::after_response");

        fail::cfg("bridge::http::before_send", "return(wait:50)").unwrap();
        let h = ok(run(&fx, &c, &live(3, req("GET", &url, &[], None), None)).await);
        assert_eq!(h.status, 200);
        fail::remove("bridge::http::before_send");
        scenario.teardown();
    }
}
