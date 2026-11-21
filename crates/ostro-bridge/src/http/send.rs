//! The HTTP send path. Order (each step can end the effect):
//!
//! 1. A `{{secret:` placeholder in the method, URL or a header name → `egress_denied`
//!    (D-FX4), nothing sent.
//! 2. `Egress::check(policy(module), url)` → `egress_denied` on deny. The destination's
//!    `http-target` config is loaded (invalid → `connection_unknown`) and the idempotency key
//!    computed ([`super::key`]).
//! 3. Re-drive only: the window decision ([`super::window`]) → `effect_outcome_unknown`
//!    **without sending**.
//! 4. Placeholders are parsed in header values and the body. Every referenced secret's grant
//!    is checked for this origin (and for body use when it appears in the body) before
//!    anything is fetched → `secret_not_granted`.
//! 5. The send gate again: `Abandon` cancels `ctx.cancel` and returns `Failure::Cancelled`
//!    (the driver submits nothing for a cancelled token).
//! 6. Secrets are resolved through [`SecretResolver::resolve`] (an ungranted secret is never
//!    fetched).
//! 7. Headers are built: expanded values live in `Zeroizing` buffers and become sensitive
//!    `HeaderValue`s; the key header is added. Templated requests drop guest
//!    `Accept-Encoding`/`TE` and send `Accept-Encoding: identity` (W7, P-21).
//! 8. `bridge::http::before_send`, 9. the send with the per-attempt deadline,
//!    10. `bridge::http::after_response`.
//! 11. Templated requests refuse a non-identity `Content-Encoding` before the body is read
//!     (`http_transport`). The body is read with [`read_capped`] (`response_too_large`).
//! 12. 3xx: templated → returned as is; untemplated → followed manually, up to
//!     `[egress] max_redirects` hops (P-5): every hop re-checked by `Egress::check` (allow-list
//!     and IP literals), 303 (and 301/302 for POST) become GET without a body, and
//!     cross-origin hops drop `Authorization`, `Cookie`, `Proxy-Authorization` and the key
//!     header. Past the hop limit the last 3xx is the response.
//! 13. The body and every response header value are scrubbed of every resolved secret
//!     ([`Scrubber`]); headers named in `redact` become `[redacted]`.
//! 14. `OpOutput::Http`.
//!
//! Retries ([`super::retry`]) happen per hop, with the same headers (same key). The gate is
//! re-checked before every retry. A re-drive whose key header is on the hop retries a 409
//! "in progress" without a policy and outside the budget until the provider's final answer,
//! or ends `effect_outcome_unknown` once the window closes (07-21 gap closure, see
//! [`super::retry`]). Only the origin, status and attempt number are logged;
//! header values and bodies never are.
//!
//! Honest limit: resolved values are zeroized in Ostro-owned buffers; the copies inside
//! `HeaderValue`/`Bytes`, hyper and rustls are not.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use ostro_types::journal::{Failure, Header, HttpRequest, HttpResponse, OpOutput};
use ostro_types::live::LiveEffect;
use reqwest::Method;
use reqwest::header::{
    ACCEPT_ENCODING, AUTHORIZATION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, COOKIE,
    HeaderMap, HeaderName, HeaderValue, LOCATION, PROXY_AUTHORIZATION, TE,
};
use tokio::time::{Instant, timeout_at};
use url::Url;

use crate::bridge_fail;
use crate::connector::{EffectCtx, GateDecision, SendGate};
use crate::egress::{Denied, ReadError, read_capped};
use crate::failpoints::apply;
use crate::kinds::{HTTP_TRANSPORT, RESPONSE_TOO_LARGE, SECRET_NOT_GRANTED};
use crate::origin::Origin;
use crate::outcome::{non_retryable, retryable};
use crate::secrets::{ResolveCtx, Scrubber, SecretValue, check_grant, template};

use super::HttpConnector;
use super::key::{KeySource, effective_key};
use super::retry::{
    Class, Schedule, Transport, classify_status, in_progress_delay, is_redrive_in_progress,
    retry_after,
};
use super::target::HttpTarget;
use super::window::{self, Decision};

/// Failpoint before every send attempt.
pub const FP_BEFORE_SEND: &str = "bridge::http::before_send";
/// Failpoint after the response head of every attempt.
pub const FP_AFTER_RESPONSE: &str = "bridge::http::after_response";

/// The value of a redacted response header.
pub const REDACTED_HEADER: &str = "[redacted]";

/// Message of the refused compressed response (W7).
pub const COMPRESSED_REFUSED: &str = "compressed response refused for templated request";

/// Wall-clock nanoseconds since the Unix epoch.
fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn transport_failure(msg: impl Into<String>) -> Failure {
    non_retryable(HTTP_TRANSPORT, msg, None)
}

fn not_granted(msg: impl Into<String>) -> Failure {
    non_retryable(SECRET_NOT_GRANTED, msg, None)
}

/// `err` and its sources, `": "`-joined (reqwest's own Display hides the cause).
fn error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut s = err.to_string();
    let mut cur = err.source();
    while let Some(e) = cur {
        let m = e.to_string();
        if !s.contains(&m) {
            s.push_str(": ");
            s.push_str(&m);
        }
        cur = e.source();
    }
    s
}

/// Placeholders found in a request.
struct Placeholders {
    /// header index → its placeholders' names (only templated headers).
    headers: BTreeMap<usize, Vec<String>>,
    /// Names referenced in the body.
    body: BTreeSet<String>,
    /// The body as UTF-8 when it is templated.
    body_str: Option<String>,
}

impl Placeholders {
    fn is_templated(&self) -> bool {
        !self.headers.is_empty() || !self.body.is_empty()
    }

    fn names(&self) -> BTreeSet<String> {
        let mut all: BTreeSet<String> = self.body.clone();
        for names in self.headers.values() {
            all.extend(names.iter().cloned());
        }
        all
    }
}

fn find_placeholders(req: &HttpRequest) -> Result<Placeholders, Failure> {
    let mut headers = BTreeMap::new();
    for (i, h) in req.headers.iter().enumerate() {
        let ph = template::parse(&h.value).map_err(|e| {
            not_granted(format!(
                "invalid secret placeholder in header {:?}: {e}",
                h.name
            ))
        })?;
        if !ph.is_empty() {
            headers.insert(i, ph.into_iter().map(|p| p.name).collect());
        }
    }
    let mut body = BTreeSet::new();
    let mut body_str = None;
    if let Some(b) = &req.body
        && b.windows(template::OPEN.len())
            .any(|w| w == template::OPEN.as_bytes())
    {
        let s = std::str::from_utf8(b)
            .map_err(|_| not_granted("a request body with secret placeholders must be UTF-8"))?;
        let ph = template::parse(s)
            .map_err(|e| not_granted(format!("invalid secret placeholder in the body: {e}")))?;
        body.extend(ph.into_iter().map(|p| p.name));
        body_str = Some(s.to_owned());
    }
    Ok(Placeholders {
        headers,
        body,
        body_str,
    })
}

/// One prepared hop.
struct Hop {
    url: Url,
    origin: Origin,
    method: Method,
    headers: HeaderMap,
    body: Option<Bytes>,
}

/// The response of a finished attempt.
struct Raw {
    status: u16,
    headers: HeaderMap,
    body: Bytes,
}

/// Why an attempt ended without a response.
enum AttemptError {
    /// Not retried.
    Fatal(Failure),
    /// A transport failure (classified for retry).
    Transport(Transport, String),
}

/// Settings shared by every attempt of the effect.
struct Plan<'a> {
    conn: &'a HttpConnector,
    ctx: &'a EffectCtx,
    effect: &'a LiveEffect,
    gate: &'a dyn SendGate,
    target: HttpTarget,
    schedule: Schedule,
    templated: bool,
    /// Per-attempt timeout.
    timeout: Duration,
    /// A key header is sent.
    key_sent: bool,
    /// The effective idempotency key (for `effect_outcome_unknown` details).
    key: String,
    /// The key header's name, when one is configured.
    key_header: Option<HeaderName>,
}

impl Plan<'_> {
    /// Whether an `AfterSend` failure may be retried now (key or idempotent method, and the
    /// window still allows a re-send).
    fn after_send_ok(&self, method: &Method) -> bool {
        let m = method.as_str();
        let elapsed = now_nanos().saturating_sub(self.effect.scheduled_ts_nanos);
        (self.key_sent || window::is_idempotent(m))
            && window::decide(m, elapsed, &self.target) == Decision::Resend
    }

    /// Whether `raw` is a provider "same key in progress" answer to a re-drive that carries
    /// the key header on this hop (07-21 gap closure).
    fn redrive_in_progress(&self, hop: &Hop, raw: &Raw) -> bool {
        self.effect.redrive
            && self
                .key_header
                .as_ref()
                .is_some_and(|kh| hop.headers.contains_key(kh))
            && is_redrive_in_progress(raw.status, &raw.headers, &raw.body)
    }

    async fn abandon_if_gated(&self) -> Result<(), Failure> {
        if self.ctx.cancel.is_cancelled() {
            return Err(Failure::Cancelled);
        }
        let d = self
            .gate
            .check(self.ctx.partition, &self.ctx.run_id, self.effect.op)
            .await;
        if d == GateDecision::Abandon {
            self.ctx.cancel.cancel();
            return Err(Failure::Cancelled);
        }
        Ok(())
    }

    async fn attempt(&self, hop: &Hop) -> Result<Raw, AttemptError> {
        let client = self.conn.egress.client();
        let mut rb = client
            .request(hop.method.clone(), hop.url.clone())
            .headers(hop.headers.clone());
        if let Some(b) = &hop.body {
            rb = rb.body(b.clone());
        }
        let request = rb
            .build()
            .map_err(|e| AttemptError::Fatal(transport_failure(error_chain(&e))))?;

        if let Some(a) = bridge_fail!(FP_BEFORE_SEND)
            && apply(a).await.is_err()
        {
            return Err(AttemptError::Transport(
                Transport::Connect,
                format!("{FP_BEFORE_SEND} failpoint"),
            ));
        }
        let deadline = Instant::now() + self.timeout;
        let timed_out = || {
            AttemptError::Transport(
                Transport::Timeout,
                format!(
                    "request to {} timed out after {} ms",
                    hop.origin,
                    self.timeout.as_millis()
                ),
            )
        };
        let resp = match timeout_at(deadline, client.execute(request)).await {
            Err(_) => return Err(timed_out()),
            Ok(Err(e)) => {
                if let Some(d) = Denied::from_error(&e) {
                    return Err(AttemptError::Fatal(d.to_failure()));
                }
                return Err(AttemptError::Transport(
                    Transport::from_error(&e),
                    format!("request to {} failed: {}", hop.origin, error_chain(&e)),
                ));
            }
            Ok(Ok(r)) => r,
        };
        if let Some(a) = bridge_fail!(FP_AFTER_RESPONSE)
            && apply(a).await.is_err()
        {
            return Err(AttemptError::Transport(
                Transport::Other,
                format!("{FP_AFTER_RESPONSE} failpoint"),
            ));
        }
        if self.templated
            && let Some(enc) = resp.headers().get(CONTENT_ENCODING)
        {
            let identity = enc
                .to_str()
                .map(|s| s.trim().eq_ignore_ascii_case("identity") || s.trim().is_empty())
                .unwrap_or(false);
            if !identity {
                return Err(AttemptError::Fatal(transport_failure(COMPRESSED_REFUSED)));
            }
        }
        let status = resp.status().as_u16();
        let headers = resp.headers().clone();
        let cap = self.target.max_response_bytes;
        let body = match timeout_at(deadline, read_capped(resp, cap)).await {
            Err(_) => return Err(timed_out()),
            Ok(Err(ReadError::TooLarge(t))) => {
                return Err(AttemptError::Fatal(non_retryable(
                    RESPONSE_TOO_LARGE,
                    format!("response body exceeds {} bytes", t.cap),
                    Some(serde_json::json!({ "cap": t.cap })),
                )));
            }
            Ok(Err(ReadError::Body(e))) => {
                return Err(AttemptError::Transport(
                    Transport::from_error(&e),
                    format!(
                        "reading the response from {} failed: {}",
                        hop.origin,
                        error_chain(&e)
                    ),
                ));
            }
            Ok(Ok(b)) => b,
        };
        Ok(Raw {
            status,
            headers,
            body,
        })
    }

    /// Every attempt of one hop.
    async fn attempts(&self, hop: &Hop) -> Result<Raw, Failure> {
        let mut attempt: u32 = 1;
        // Re-drive in-progress retries: they do not consume the attempt budget.
        let mut in_progress: u32 = 0;
        loop {
            if attempt > 1 {
                self.abandon_if_gated().await?;
            }
            let (class, delay_hint, result) = match self.attempt(hop).await {
                Ok(raw) => {
                    let class = classify_status(raw.status, &raw.headers, &raw.body);
                    tracing::debug!(
                        origin = %hop.origin,
                        status = raw.status,
                        attempt,
                        op = self.effect.op,
                        "http effect attempt"
                    );
                    (class, retry_after(&raw.headers), Ok(raw))
                }
                Err(AttemptError::Fatal(f)) => return Err(f),
                Err(AttemptError::Transport(t, msg)) => {
                    tracing::debug!(
                        origin = %hop.origin,
                        attempt,
                        op = self.effect.op,
                        transport = ?t,
                        "http effect attempt failed"
                    );
                    (t.class(), None, Err(retryable(HTTP_TRANSPORT, msg)))
                }
            };
            if let Ok(raw) = &result
                && self.redrive_in_progress(hop, raw)
            {
                in_progress += 1;
                let delay = in_progress_delay(in_progress, delay_hint);
                let elapsed = now_nanos().saturating_sub(self.effect.scheduled_ts_nanos);
                let at_next =
                    elapsed.saturating_add(u64::try_from(delay.as_nanos()).unwrap_or(u64::MAX));
                if !window::may_await_in_progress(at_next, &self.target) {
                    tracing::debug!(
                        origin = %hop.origin,
                        attempt,
                        op = self.effect.op,
                        "http re-drive still in progress at the provider; window closed"
                    );
                    return Err(window::unknown(self.effect.op, &self.key, elapsed));
                }
                tracing::debug!(
                    origin = %hop.origin,
                    attempt,
                    op = self.effect.op,
                    delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                    "http re-drive in progress at the provider; retrying"
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
                continue;
            }
            let budget = attempt - in_progress;
            let after_ok = class == Class::AfterSend && self.after_send_ok(&hop.method);
            if !self.schedule.should_retry(class, budget, after_ok) {
                return result;
            }
            tokio::time::sleep(self.schedule.delay(budget, delay_hint)).await;
            attempt += 1;
        }
    }
}

/// Executes one HTTP effect (see the module docs for the order).
pub(crate) async fn execute(
    conn: &HttpConnector,
    ctx: &EffectCtx,
    effect: &LiveEffect,
    req: &HttpRequest,
    gate: &dyn SendGate,
) -> Result<OpOutput, Failure> {
    // 1. No placeholders in the method, URL or header names.
    if template::contains(&req.method)
        || template::contains(&req.url)
        || req.headers.iter().any(|h| template::contains(&h.name))
    {
        return Err(Denied::Placeholder.to_failure());
    }

    // 2. URL gate, destination config, key.
    let policy = conn.meta.egress_policy(&ctx.module);
    let checked = conn
        .egress
        .check(policy.as_ref(), &req.url)
        .map_err(|d| d.to_failure())?;
    let origin_name = super::origin_key(&checked.origin);
    let target = conn.http_target(&origin_name).map_err(|e| e.to_failure())?;
    let key = effective_key(
        req,
        target.idempotency_header.as_deref(),
        &ctx.run_id,
        effect.op,
    );

    // 3. The unknown-outcome window, at re-drive time only.
    if effect.redrive {
        let elapsed = now_nanos().saturating_sub(effect.scheduled_ts_nanos);
        if window::decide(&req.method, elapsed, &target) == Decision::Unknown {
            return Err(window::unknown(effect.op, &key.key, elapsed));
        }
    }

    // 4. Placeholders and grants (nothing fetched yet).
    let ph = find_placeholders(req)?;
    let names = ph.names();
    if !names.is_empty() {
        let grants = conn.meta.grants_for(&ctx.module);
        for name in &names {
            check_grant(
                &grants,
                &ctx.module,
                name,
                &checked.origin,
                ph.body.contains(name),
            )?;
        }
    }

    let method = Method::from_bytes(req.method.as_bytes())
        .map_err(|_| transport_failure(format!("invalid HTTP method {:?}", req.method)))?;

    let timeout = req
        .timeout_ms
        .filter(|ms| *ms > 0)
        .map_or(target.timeout, Duration::from_millis);
    let mut plan = Plan {
        conn,
        ctx,
        effect,
        gate,
        target,
        schedule: match &effect.http_retry {
            Some(p) => Schedule::new(Some(p)),
            None => conn.cfg.default_retry.clone(),
        },
        templated: ph.is_templated(),
        timeout,
        key_sent: false,
        key: key.key.clone(),
        key_header: None,
    };

    // 5. The gate, immediately before secret resolution.
    plan.abandon_if_gated().await?;

    // 6. Resolve.
    let mut values: BTreeMap<String, SecretValue> = BTreeMap::new();
    let rctx = ResolveCtx {
        module: &ctx.module,
        tenant: ctx.tenant.as_deref(),
        run_id: &ctx.run_id,
        attempt: ctx.attempt,
    };
    for name in &names {
        let v = conn
            .resolver
            .resolve(&rctx, name, &checked.origin, ph.body.contains(name))
            .await?;
        values.insert(name.clone(), v);
    }
    let scrubber = Scrubber::new(&values.values().collect::<Vec<_>>());

    // 7. Headers and body.
    let key_header = match &plan.target.idempotency_header {
        Some(n) => Some(
            HeaderName::from_bytes(n.as_bytes())
                .map_err(|_| transport_failure(format!("invalid idempotency header {n:?}")))?,
        ),
        None => None,
    };
    let mut headers = HeaderMap::new();
    for (i, h) in req.headers.iter().enumerate() {
        let name = HeaderName::from_bytes(h.name.as_bytes())
            .map_err(|_| transport_failure(format!("invalid header name {:?}", h.name)))?;
        if plan.templated && (name == ACCEPT_ENCODING || name == TE) {
            continue;
        }
        if key.source == KeySource::Override && key_header.as_ref() == Some(&name) {
            continue;
        }
        let value = if ph.headers.contains_key(&i) {
            let expanded = template::expand(&h.value, &values);
            let mut v = HeaderValue::from_bytes(&expanded).map_err(|_| {
                transport_failure(format!(
                    "header {:?} is not a valid header value after expansion",
                    h.name
                ))
            })?;
            v.set_sensitive(true);
            v
        } else {
            let mut v = HeaderValue::from_str(&h.value).map_err(|_| {
                transport_failure(format!("header {:?} has an invalid value", h.name))
            })?;
            if name == AUTHORIZATION || name == COOKIE || name == PROXY_AUTHORIZATION {
                v.set_sensitive(true);
            }
            v
        };
        headers.append(name, value);
    }
    if plan.templated {
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    }
    if let Some(kh) = &key_header {
        if key.source != KeySource::GuestHeader {
            let v = HeaderValue::from_str(&key.key).map_err(|_| {
                transport_failure("the idempotency key is not a valid header value")
            })?;
            headers.insert(kh.clone(), v);
        }
        plan.key_sent = true;
    }
    plan.key_header.clone_from(&key_header);
    let body = match (&ph.body_str, &req.body) {
        (Some(s), _) if !ph.body.is_empty() => {
            let expanded = template::expand(s, &values);
            Some(Bytes::copy_from_slice(&expanded))
        }
        (_, Some(b)) => Some(Bytes::copy_from_slice(b)),
        (_, None) => None,
    };
    drop(values);

    // 8-12. Attempts and redirects.
    let mut hop = Hop {
        url: checked.url,
        origin: checked.origin,
        method,
        headers,
        body,
    };
    let max_redirects = conn.egress.config().max_redirects;
    let mut hops: u8 = 0;
    let raw = loop {
        let raw = plan.attempts(&hop).await?;
        if plan.templated || hops >= max_redirects {
            break raw;
        }
        if !matches!(raw.status, 301 | 302 | 303 | 307 | 308) {
            break raw;
        }
        let Some(next) = raw
            .headers
            .get(LOCATION)
            .and_then(|l| l.to_str().ok())
            .and_then(|l| hop.url.join(l).ok())
        else {
            break raw;
        };
        let next = conn
            .egress
            .check(policy.as_ref(), next.as_str())
            .map_err(|d| d.to_failure())?;
        let to_get = raw.status == 303 && hop.method != Method::HEAD
            || matches!(raw.status, 301 | 302) && hop.method == Method::POST;
        if to_get {
            hop.method = Method::GET;
            hop.body = None;
            hop.headers.remove(CONTENT_TYPE);
            hop.headers.remove(CONTENT_LENGTH);
        }
        if next.origin != hop.origin {
            hop.headers.remove(AUTHORIZATION);
            hop.headers.remove(COOKIE);
            hop.headers.remove(PROXY_AUTHORIZATION);
            if let Some(kh) = &key_header {
                hop.headers.remove(kh);
            }
        }
        hop.url = next.url;
        hop.origin = next.origin;
        hops += 1;
    };

    // 13. Scrub and redact.
    let mut body = raw.body.to_vec();
    if !scrubber.is_empty() {
        scrubber.scrub_bytes(&mut body);
    }
    let mut out_headers = Vec::with_capacity(raw.headers.len());
    for (name, value) in &raw.headers {
        let n = name.as_str();
        let v = if req.redact.iter().any(|r| r.eq_ignore_ascii_case(n)) {
            REDACTED_HEADER.to_owned()
        } else {
            let s = String::from_utf8_lossy(value.as_bytes());
            if scrubber.is_empty() {
                s.into_owned()
            } else {
                scrubber.scrub_str(&s)
            }
        };
        out_headers.push(Header {
            name: n.to_owned(),
            value: v,
        });
    }

    // 14.
    Ok(OpOutput::Http(HttpResponse {
        status: raw.status,
        headers: out_headers,
        body,
    }))
}
