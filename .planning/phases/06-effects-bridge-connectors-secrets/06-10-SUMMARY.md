---
phase: 06-effects-bridge-connectors-secrets
plan: 10
subsystem: effects-bridge
tags: [http, idempotency, fx-01, fx-02, fx-03, fx-07, redirects, scrubbing, mock-provider]
requires:
  - "06-04 egress (Egress::check/client, Denied::{to_failure, from_error}, read_capped, EgressConfig)"
  - "06-05 secrets (template::{parse, contains, expand}, check_grant, SecretResolver::resolve, Scrubber)"
  - "06-07 driver core (Connector/EffectCtx/SendGate/Target, Bridge, outcome helpers, QuotaSpec::parse_with_burst)"
provides:
  - "ostro_bridge::http::{HttpConnector, HttpConfig}: Connector for EffectKind::Http"
  - "ostro_bridge::http::key::{derive_key, effective_key}: the pinned idempotency key contract"
  - "ostro_bridge::http::window::{decide, unknown, is_idempotent}: the re-drive window (D-FX3)"
  - "ostro_bridge::http::retry::{classify_status, Transport, Schedule, retry_after}"
  - "ostro_bridge::http::target::HttpTarget: the http-target v1 connection config"
  - "ostro_testkit::mock_provider::{MockProvider, RecordingListener} (feature mock-http)"
affects: [06-14, 06-17, 06-19, 06-20]
tech-stack:
  added: []
  patterns:
    - "Grant checks for every referenced secret run before the gate re-check and before any provider fetch"
    - "Per-attempt deadline spans send + body read; failpoints sit outside the deadline so return(hang) really hangs"
    - "Redirects followed by the connector, never by reqwest: Egress::check on every hop"
key-files:
  created:
    - crates/ostro-bridge/src/http/key.rs
    - crates/ostro-bridge/src/http/window.rs
    - crates/ostro-bridge/src/http/retry.rs
    - crates/ostro-bridge/src/http/target.rs
    - crates/ostro-bridge/src/http/send.rs
    - crates/ostro-bridge/tests/http_connector.rs
  modified:
    - crates/ostro-bridge/src/http/mod.rs
    - crates/ostro-testkit/src/mock_provider.rs
key-decisions:
  - "The idempotency key formula is a persistent contract, pinned by four vectors (two run ids x two ops) cross-checked with Python hashlib"
  - "Key precedence: the guest's idempotency-key override (it replaces a guest header of the same name), then a guest-set header of the configured name (sent once, as written), then the derived key"
  - "idempotency_header null or \"\" suppresses the header; the key is still computed for effect_outcome_unknown details"
  - "Method names are case-sensitive for the window: only upper-case GET/HEAD/OPTIONS/PUT/DELETE count as idempotent"
  - "An AfterSend failure is retried only with a guest policy, when (a key header is sent OR the method is idempotent) AND the window decision is Resend"
  - "A guest non_retryable_kinds entry of http_transport disables transport retries"
  - "http-target configs reject unknown keys and a missing or unknown v (connection_unknown), so a typo cannot silently fall back to the default window"
  - "Past [egress] max_redirects hops the last 3xx is returned as the response, not an error"
  - "A failing before_send failpoint counts as NeverLeft and a failing after_response failpoint as AfterSend, so both follow the normal retry rules"
metrics:
  duration: "~50 min"
  completed: 2026-11-20
---

# Phase 6 Plan 10: HTTP Connector Summary

**`HttpConnector` sends every HTTP effect with a stable `ostro-<32 hex>` idempotency key. On re-drive it refuses non-idempotent requests that are older than the destination's window, and returns `effect_outcome_unknown` without sending. Retries follow the guest policy and are limited to conditions where a retry is safe. Every attempt has a deadline and a streaming size cap. Placeholders in header values are expanded only toward origins the grant lists. Templated requests never follow redirects, and they refuse compressed responses. Untemplated redirects are followed by the connector, which re-checks the allow-list and IP literals on every hop. Each response is scrubbed of every resolved secret, and headers named in `redact` become `[redacted]`.**

## Pinned idempotency key vectors

`"ostro-" + lower-hex(sha256(b"ostro/idem/v1" || run_id || op.to_le_bytes())[..16])`, 38 chars:

| run_id | op | key |
|--------|----|-----|
| `0197a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b` | 1 | `ostro-bde26f8232abe4e7e94943f970fc2c33` |
| `0197a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b` | 0x0102030405060708 | `ostro-07d79386f8bb969ca49792f371a64344` |
| `run-1` | 1 | `ostro-6ac4a780ae09b9770d9fb056ef9a5364` |
| `run-1` | 0x0102030405060708 | `ostro-25d328cd03c15ed4fc3e117a5e82626f` |

## Final API

```rust
// ostro_bridge::http
pub struct HttpConfig { pub default_retry: Schedule }            // Default = Schedule::new(None)
pub struct HttpConnector;
impl HttpConnector {
    pub fn new(egress: Arc<Egress>, resolver: SecretResolver, meta: Arc<dyn MetaView>, cfg: HttpConfig) -> Self;
    pub fn http_target(&self, origin: &str) -> Result<HttpTarget, TargetError>;
}
impl Connector for HttpConnector { /* kind Http; target = {normalised origin, http-target quota} */ }

pub mod key    { derive_key(run_id, op) -> String; effective_key(req, header: Option<&str>, run_id, op) -> EffectiveKey{key, source: Derived|Override|GuestHeader};
                 KEY_DOMAIN, KEY_PREFIX, KEY_LEN = 38, DEFAULT_HEADER = "Idempotency-Key" }
pub mod window { is_idempotent(m); decide(method, elapsed_nanos, &HttpTarget) -> Decision{Resend|Unknown};
                 unknown(op, key, elapsed_nanos) -> Failure; UNKNOWN_MESSAGE }
pub mod retry  { Class{NeverLeft|AfterSend|RetryStatus|Final}; Transport{Connect|Timeout|Other}::{from_error, class};
                 classify_status(status, &HeaderMap, body) -> Class; is_in_progress_conflict; retry_after(&HeaderMap);
                 Schedule::{new(Option<&EffectRetry>), should_retry(class, attempt, after_send_ok), delay(retry, retry_after)} }
pub mod target { HTTP_TARGET_KIND = "http-target"; HttpTarget{idempotency_window, idempotency_header: Option<String>, resend: ResendMode,
                 force_window, safety_margin, quota: Option<QuotaSpec>, max_response_bytes, timeout}
                 ::{defaults(&EgressConfig), from_connection(Option<&Connection>, &EgressConfig)}; TargetError::to_failure() -> connection_unknown }
pub mod send   { FP_BEFORE_SEND, FP_AFTER_RESPONSE, REDACTED_HEADER = "[redacted]", COMPRESSED_REFUSED }
```

The send path order is documented in `send.rs` and follows the plan's 14 steps.

## MockProvider endpoints (ostro-testkit, feature `mock-http`)

`MockProvider::start().await` binds `127.0.0.1:0` and provides `base_url()` and `addr()`. The server stops when the provider is dropped.

| Endpoint | Behaviour |
|----------|-----------|
| `POST /pay[?delay_ms=N][&fail_first=N]` | Requires `Idempotency-Key`, else 400. The first `fail_first` requests of a key get 503. The first successful request increments `charges(key)`, sleeps `delay_ms`, then returns 200 `{"charge_id","key"}`. A completed key replays the stored response with `idempotent-replayed: true`. A key still in progress gets 409 `{"error":"in_progress"}`. |
| `ANY /echo[?gzip=1]` | Returns `{"method","path","headers":[[k,v]..],"body_b64"}`. `gzip=1` labels the response `Content-Encoding: gzip` while the bytes stay plain. |
| `GET /redirect?to=<url>[&code=302]` | Returns `code` with `Location`. |
| `GET /slow?ms=N` | Sleeps, then returns 200 `slow`. |
| `GET /big?bytes=N[&no_length=1]` | Returns N bytes of `x`. With `no_length=1` the body is streamed chunked, without a length. |
| `GET /status/{code}` | Returns that status. |
| `GET /flaky?fail=N` | The first N requests per path+query get 503, later ones 200. |
| `GET /set-header?name=&value=` | Returns 200 with that response header. |

Counters: `requests(key)` (any endpoint), `charges(key)`, `total_requests()`, `requests_to(path)`. `RecordingListener::start().await -> (SocketAddr, Arc<AtomicUsize>)` is a raw TCP listener that counts accepts and closes each connection.

## Verification

- `cargo test -p ostro-bridge --lib --locked -- http::`: **16 passed** (key vectors, window table, retry classes and schedules, target parsing).
- `cargo test -p ostro-bridge --test http_connector --locked -- --test-threads=1`: **24 passed**. The suite also passes multi-threaded.
- `cargo test -p ostro-bridge --features failpoints --test http_connector --locked -- --test-threads=1`: **25 passed**.
- `cargo test -p ostro-bridge --locked -- --test-threads=1` on main at e3eb06a: lib 110, driver 14, egress_client 5, http_connector 24, ratelimit 4, secrets 10, ssrf_table 5. All ok.
- The same suite with `--features failpoints`: lib 110, driver 15, http_connector 25, and the rest unchanged. All ok.
- `cargo test -p ostro-testkit --features mock-http --locked`: 12 passed.
- Clippy with `-D warnings` is clean for:
  - `-p ostro-bridge --all-targets --all-features`;
  - `-p ostro-bridge --all-targets --no-default-features`;
  - `-p ostro-testkit --features mock-http --all-targets`.
- The build is warning-free and `cargo fmt --check` is clean.
- **Not run:** the full `bash scripts/ci-local.sh` gate. Four other executors were building at the same time. Only ostro-bridge's http module, its new test file and the testkit mock changed, and no other crate uses them yet.

## Deviations from Plan

1. **[Plan text vs the 06-04 gate] Redirect to `http://127.0.0.1:<other>`.** The plan expects `not_allowed`. The 06-04 gate returns `port` here, because the mock's rule `http://127.0.0.1:<port>` admits the host but not the port. The test asserts both cases separately:
   - a redirect to an unlisted host (`http://other.test:1/x`) gives `not_allowed`;
   - a redirect to the mock's IP on another port gives `port`.
2. **[Rule 1 - Bug in this plan's Task 2] `/big?no_length=1`.** Setting a `Transfer-Encoding: chunked` header on a full-length body made hyper drop the reply ("Empty reply from server"). The endpoint now streams the body through `Body::from_stream(body.into_data_stream())`, and the test asserts that no `content-length` header arrives. Fixed in the Task 3 commit (e3eb06a).
3. **AfterSend retry condition widened slightly.** The plan says "only when a key header is sent and the window allows". The connector also allows it when the method is RFC 9110 idempotent and the header is suppressed. Every other condition still applies: a guest policy is required and the window must say Resend.
4. **Extra hardening (Rule 2):**
   - The gate is re-checked before every retry attempt, not only before resolution.
   - Every referenced secret's grant (origin, plus body scope for body placeholders) is checked before the gate and before any fetch, so a refused request never touches a provider.
   - A body containing a placeholder must be valid UTF-8, otherwise the request fails with `secret_not_granted`.
5. **One commit per task.** Each commit holds tests and implementation together, with no separate RED commits, to keep CAS updates on a busy main to a minimum (the same approach as 06-04, 06-05 and 06-07).

## Notes for later plans

- **06-14:** build `HttpConnector::new(Arc<Egress>, SecretResolver, Arc<dyn MetaView>, HttpConfig::default())` and register it on `Bridge`.
- **06-17 / 06-19:**
  - Use `MockProvider` as documented above.
  - Use `derive_key(run_id, op)` to look up `requests(key)` and `charges(key)`.
  - The failpoints are `bridge::http::before_send` and `bridge::http::after_response`.
  - `effect_outcome_unknown` details are `{"op","key","elapsed_ms"}`.
- **06-20:** document the following:
  - the key contract and its vectors;
  - `http-target` keys, including null/empty `idempotency_header` to suppress it;
  - case-sensitive methods for the window;
  - the 3xx returned after `max_redirects`;
  - `[redacted]` for redacted headers vs `[REDACTED]` for scrubbed values.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | 2c2c4df | feat(06-10): idempotency key, unknown-outcome window, retry classification and http-target config |
| 2 | 8962059 | feat(06-10): testkit mock provider and recording proxy listener (mock-http) |
| 3 | e3eb06a | feat(06-10): HttpConnector send path with templating, manual redirects, scrubbing and integration tests |

## Self-Check: PASSED

- FOUND: crates/ostro-bridge/src/http/{mod,key,window,retry,target,send}.rs, crates/ostro-bridge/tests/http_connector.rs, crates/ostro-testkit/src/mock_provider.rs
- FOUND: commits 2c2c4df, 8962059 and e3eb06a on main
