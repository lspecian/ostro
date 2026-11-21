//! Ergonomic builders for host effects (HTTP, SQL, object store) and the semantic
//! idempotency key helper.
//!
//! The builders are thin: each one fills in the unchanged WIT `ostro:runtime@0.1.0` request
//! record (`http-request`, `sql-request`, `object-request`) and issues the existing
//! `http-fetch` / `sql-query` / `object-store` call. No new host calls exist. Reach them
//! through [`Ctx::http`](crate::Ctx::http), [`Ctx::sql`](crate::Ctx::sql) and
//! [`Ctx::object`](crate::Ctx::object); the `new` constructors and `build` / `request`
//! methods exist so the records can be inspected without a host.
//!
//! ```ignore
//! let key = ctx.idempotency_key("charge");
//! let resp = ctx
//!     .http()
//!     .post("https://api.example.com/v1/charges")
//!     .secret_header("Authorization", "Bearer {{secret:stripe}}")
//!     .json(&body)?
//!     .idempotency_key(&key)
//!     .timeout(Duration::from_secs(10))
//!     .redact(["Authorization"])
//!     .send()
//!     .wait()?;
//! ```
//!
//! Semantics, one line each:
//! - W1 templating: `{{secret:NAME}}` is expanded by the host in header values only, and
//!   only for origins the secret is granted to; bodies and URLs are never expanded.
//! - W2 redirects: a templated request never follows redirects; others re-check every hop.
//! - W3 redaction: `redact` names are replaced in the journal and logs, and response headers
//!   with those names come back to the guest redacted too (live and replay identical).
//! - W4 status: any final HTTP status is a successful completion; inspect `status`.
//! - W5 keys: without an override the host sends `ostro-<32 hex>` derived from (run, op);
//!   [`idempotency_key`] gives a name-based key you can also embed in the body.
//! - W6 SQL: `exactly_once` runs one transactional statement with a token row; statements
//!   that commit implicitly fail `sql_not_transactional`; otherwise at-least-once.
//! - W7 object ops: `get` -> bytes, `put`/`head` -> UTF-8 JSON, `delete` -> empty,
//!   `presign-*` -> UTF-8 URL; any other op fails `unsupported_op`.
//! - W8 failures: connector failures (egress denied, secret not granted, transport, caps)
//!   arrive as the op's `failure` and surface as a [`StepError`] on `wait`.

use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::bindings::ostro::runtime::effects::{self as wit, OpOutput};
use crate::bindings::ostro::runtime::types::RetryPolicy as WitRetryPolicy;
use crate::ctx::duration_ms;
use crate::error::StepError;
use crate::ops::Op;
use crate::step::RetryPolicy;

pub use crate::bindings::ostro::runtime::effects::{
    Header, HttpRequest, HttpResponse, ObjectRequest, SqlRequest, SqlResult, SqlValue,
};

/// Domain separator of the semantic idempotency key (P-11).
pub const IDEMPOTENCY_KEY_DOMAIN: &[u8] = b"ostro/idem/v1/name";

/// The semantic idempotency key for `name` in run `run_id` (P-11):
/// `"ostro-" + hex(sha256("ostro/idem/v1/name" || run_id || 0x00 || name))[..32]`.
///
/// 38 characters, deterministic per (run, name), stable across replay and attempts, and
/// different for every run (a retry-as-new or continue-as-new run gets new keys). Inside a
/// workflow use [`Ctx::idempotency_key`](crate::Ctx::idempotency_key).
///
/// Pure (no host call), so it runs anywhere. The cross-SDK vectors in
/// `conformance/vectors/idempotency.json` pin its output; the Python and TypeScript SDKs
/// must produce the same 38 characters.
pub fn idempotency_key_for(run_id: &str, name: &str) -> String {
    let mut h = Sha256::new();
    h.update(IDEMPOTENCY_KEY_DOMAIN);
    h.update(run_id.as_bytes());
    h.update([0u8]);
    h.update(name.as_bytes());
    let digest = h.finalize();
    let mut out = String::with_capacity(38);
    out.push_str("ostro-");
    out.push_str(&hex(&digest[..16]));
    out
}

/// Alias of [`idempotency_key_for`] (the 06-09 name).
pub fn idempotency_key(run_id: &str, name: &str) -> String {
    idempotency_key_for(run_id, name)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(DIGITS[usize::from(b >> 4)] as char);
        s.push(DIGITS[usize::from(b & 0x0f)] as char);
    }
    s
}

fn protocol<T>(what: &str, out: &OpOutput) -> Result<T, StepError> {
    Err(StepError::non_retryable(
        "protocol",
        format!("expected {what} output, got {out:?}"),
    ))
}

// ---------------------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------------------

/// Entry point returned by [`Ctx::http`](crate::Ctx::http): pick the method and URL.
#[derive(Debug, Clone, Copy, Default)]
pub struct HttpClient {
    _p: (),
}

impl HttpClient {
    pub(crate) fn new() -> Self {
        Self { _p: () }
    }

    /// `GET url`.
    pub fn get(self, url: impl Into<String>) -> HttpBuilder {
        HttpBuilder::new("GET", url)
    }

    /// `POST url`.
    pub fn post(self, url: impl Into<String>) -> HttpBuilder {
        HttpBuilder::new("POST", url)
    }

    /// `PUT url`.
    pub fn put(self, url: impl Into<String>) -> HttpBuilder {
        HttpBuilder::new("PUT", url)
    }

    /// `DELETE url`.
    pub fn delete(self, url: impl Into<String>) -> HttpBuilder {
        HttpBuilder::new("DELETE", url)
    }

    /// Any method.
    pub fn request(self, method: impl Into<String>, url: impl Into<String>) -> HttpBuilder {
        HttpBuilder::new(method, url)
    }
}

/// Builder for one outbound HTTP request (WIT `http-request`).
///
/// Any final status is a success (W4); templated requests do not follow redirects (W2).
#[derive(Debug, Clone)]
#[must_use = "call .send() to issue the request"]
pub struct HttpBuilder {
    req: HttpRequest,
}

impl HttpBuilder {
    /// A request with no headers, body, timeout, key override, retry or redaction.
    pub fn new(method: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            req: HttpRequest {
                method: method.into(),
                url: url.into(),
                headers: Vec::new(),
                body: None,
                timeout_ms: None,
                idempotency_key: None,
                retry: None,
                redact: Vec::new(),
            },
        }
    }

    /// Append a header (sent verbatim; never templated unless it contains a template).
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.req.headers.push(Header {
            name: name.into(),
            value: value.into(),
        });
        self
    }

    /// Append a header whose value carries a `{{secret:NAME}}` template, expanded by the
    /// host for granted origins only (W1). Otherwise identical to [`header`](Self::header).
    /// Consider also listing the name in [`redact`](Self::redact).
    ///
    /// # Panics
    ///
    /// If `template` contains no `{{secret:` marker (a literal value here is almost
    /// certainly a leaked secret).
    pub fn secret_header(self, name: impl Into<String>, template: impl Into<String>) -> Self {
        let template = template.into();
        assert!(
            template.contains("{{secret:"),
            "secret_header value must contain a {{{{secret:NAME}}}} template"
        );
        self.header(name, template)
    }

    /// Set the raw request body (never templated, W1).
    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.req.body = Some(body.into());
        self
    }

    /// JSON-encode `value` as the body and set `content-type: application/json`
    /// (replacing any content-type header already set).
    pub fn json<T: Serialize + ?Sized>(mut self, value: &T) -> Result<Self, StepError> {
        let body = serde_json::to_vec(value)?;
        self.req
            .headers
            .retain(|h| !h.name.eq_ignore_ascii_case("content-type"));
        self.req.headers.push(Header {
            name: "content-type".into(),
            value: "application/json".into(),
        });
        self.req.body = Some(body);
        Ok(self)
    }

    /// Override the idempotency key the host sends (W5); see
    /// [`Ctx::idempotency_key`](crate::Ctx::idempotency_key) for a deterministic one.
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.req.idempotency_key = Some(key.into());
        self
    }

    /// Timeout of one attempt (not of all retries together).
    pub fn timeout(mut self, d: Duration) -> Self {
        self.req.timeout_ms = Some(duration_ms(d));
        self
    }

    /// Host retry policy: retries only connect failures, 408/425/429/5xx and transport
    /// loss, always with the same idempotency key.
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.req.retry = Some(WitRetryPolicy {
            max_attempts: policy.max_attempts,
            initial_interval_ms: duration_ms(policy.initial_interval),
            max_interval_ms: duration_ms(policy.max_interval),
            backoff_coefficient: policy.backoff_coefficient,
            non_retryable_kinds: policy.non_retryable_kinds,
        });
        self
    }

    /// Header names to redact in the journal and logs; response headers with these names
    /// are also returned redacted (W3).
    pub fn redact<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.req.redact.extend(names.into_iter().map(Into::into));
        self
    }

    /// The WIT request record this builder describes.
    pub fn build(self) -> HttpRequest {
        self.req
    }

    /// Issue the request (`http-fetch`).
    pub fn send(self) -> Op<HttpResponse> {
        Op::new(wit::http_fetch(&self.req), crate::ops::decode_http)
    }
}

// ---------------------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------------------

/// Builder for SQL statements against a named connection (WIT `sql-request`).
#[derive(Debug, Clone)]
#[must_use = "call .query(..) to run a statement"]
pub struct SqlBuilder {
    connection: String,
    exactly_once: bool,
}

impl SqlBuilder {
    /// Statements against `connection` (resolved inside the run's tenant), at-least-once.
    pub fn new(connection: impl Into<String>) -> Self {
        Self {
            connection: connection.into(),
            exactly_once: false,
        }
    }

    /// Exactly-once (W6): one statement in a transaction with an `ostro_effects` token row;
    /// a re-drive returns the stored result. Implicitly-committing or transaction-control
    /// statements fail `sql_not_transactional`.
    pub fn exactly_once(mut self) -> Self {
        self.exactly_once = true;
        self
    }

    /// The WIT request record for `statement` with positional `params`.
    pub fn request(
        &self,
        statement: impl Into<String>,
        params: impl IntoIterator<Item = SqlValue>,
    ) -> SqlRequest {
        SqlRequest {
            connection: self.connection.clone(),
            statement: statement.into(),
            params: params.into_iter().collect(),
            exactly_once: self.exactly_once,
        }
    }

    /// Run `statement` (`sql-query`).
    pub fn query(
        &self,
        statement: impl Into<String>,
        params: impl IntoIterator<Item = SqlValue>,
    ) -> Op<SqlResult> {
        let req = self.request(statement, params);
        Op::new(wit::sql_query(&req), decode_sql)
    }
}

pub(crate) fn decode_sql(out: OpOutput) -> Result<SqlResult, StepError> {
    match out {
        OpOutput::Sql(r) => Ok(r),
        other => protocol("sql", &other),
    }
}

// ---------------------------------------------------------------------------------------
// Object store
// ---------------------------------------------------------------------------------------

/// `put` result metadata (UTF-8 JSON from the host).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ObjectPutInfo {
    /// Entity tag of the stored object, if the backend reports one.
    pub e_tag: Option<String>,
}

/// `head` result metadata (UTF-8 JSON from the host).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ObjectHead {
    /// Size in bytes.
    pub size: u64,
    /// Last-modified time as rendered by the host.
    pub last_modified: String,
    /// Entity tag, if any.
    pub e_tag: Option<String>,
    /// Object version, if the backend versions objects.
    pub version: Option<String>,
}

/// Object-store operations against a named connection (WIT `object-request`).
///
/// Failures (including `unsupported_op` and missing objects) surface as the op's failure,
/// i.e. a [`StepError`] from `wait`.
#[derive(Debug, Clone)]
pub struct ObjectClient {
    connection: String,
}

impl ObjectClient {
    /// Operations against `connection` (resolved inside the run's tenant).
    pub fn new(connection: impl Into<String>) -> Self {
        Self {
            connection: connection.into(),
        }
    }

    /// The WIT request record for `op` on `key`.
    pub fn request(
        &self,
        op: impl Into<String>,
        key: impl Into<String>,
        body: Option<Vec<u8>>,
    ) -> ObjectRequest {
        ObjectRequest {
            connection: self.connection.clone(),
            op: op.into(),
            key: key.into(),
            body,
        }
    }

    /// Any op string; returns the raw `op-output.object` bytes.
    pub fn raw(
        &self,
        op: impl Into<String>,
        key: impl Into<String>,
        body: Option<Vec<u8>>,
    ) -> Op<Vec<u8>> {
        self.issue(self.request(op, key, body), decode_object_bytes)
    }

    fn issue<T>(&self, req: ObjectRequest, decode: fn(OpOutput) -> Result<T, StepError>) -> Op<T> {
        Op::new(wit::object_store(&req), decode)
    }

    /// `get`: the object bytes.
    pub fn get(&self, key: impl Into<String>) -> Op<Vec<u8>> {
        self.issue(self.request("get", key, None), decode_object_bytes)
    }

    /// `put`: store `body`; returns the put metadata.
    pub fn put(&self, key: impl Into<String>, body: impl Into<Vec<u8>>) -> Op<ObjectPutInfo> {
        self.issue(
            self.request("put", key, Some(body.into())),
            decode_object_json::<ObjectPutInfo>,
        )
    }

    /// `head`: object metadata.
    pub fn head(&self, key: impl Into<String>) -> Op<ObjectHead> {
        self.issue(
            self.request("head", key, None),
            decode_object_json::<ObjectHead>,
        )
    }

    /// `delete`.
    pub fn delete(&self, key: impl Into<String>) -> Op<()> {
        self.issue(self.request("delete", key, None), decode_object_unit)
    }

    /// `presign-get`: a URL (expiry from the connection config).
    pub fn presign_get(&self, key: impl Into<String>) -> Op<String> {
        self.issue(self.request("presign-get", key, None), decode_object_string)
    }

    /// `presign-put`: a URL (expiry from the connection config).
    pub fn presign_put(&self, key: impl Into<String>) -> Op<String> {
        self.issue(self.request("presign-put", key, None), decode_object_string)
    }
}

pub(crate) fn decode_object_bytes(out: OpOutput) -> Result<Vec<u8>, StepError> {
    match out {
        OpOutput::Object(b) => Ok(b),
        other => protocol("object", &other),
    }
}

fn decode_object_unit(out: OpOutput) -> Result<(), StepError> {
    decode_object_bytes(out).map(|_| ())
}

fn decode_object_string(out: OpOutput) -> Result<String, StepError> {
    String::from_utf8(decode_object_bytes(out)?)
        .map_err(|e| StepError::non_retryable("protocol", format!("object URL not UTF-8: {e}")))
}

fn decode_object_json<T: DeserializeOwned>(out: OpOutput) -> Result<T, StepError> {
    Ok(serde_json::from_slice(&decode_object_bytes(out)?)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_encodes_lowercase() {
        assert_eq!(hex(&[0x00, 0x0f, 0xa5, 0xff]), "000fa5ff");
    }

    #[test]
    fn object_decoders() {
        let o = |s: &str| OpOutput::Object(s.as_bytes().to_vec());
        assert_eq!(
            decode_object_json::<ObjectPutInfo>(o(r#"{"e_tag":"\"x\""}"#)).unwrap(),
            ObjectPutInfo {
                e_tag: Some("\"x\"".into())
            }
        );
        let head = decode_object_json::<ObjectHead>(o(
            r#"{"size":3,"last_modified":"2026-01-01T00:00:00Z","e_tag":null,"version":null}"#,
        ))
        .unwrap();
        assert_eq!(head.size, 3);
        assert_eq!(head.last_modified, "2026-01-01T00:00:00Z");
        assert_eq!(decode_object_unit(o("")), Ok(()));
        assert_eq!(
            decode_object_string(o("https://x/y?sig=1")).unwrap(),
            "https://x/y?sig=1"
        );
        assert_eq!(
            decode_object_string(OpOutput::Object(vec![0xff]))
                .unwrap_err()
                .kind,
            "protocol"
        );
        assert_eq!(
            decode_object_json::<ObjectHead>(o("nope"))
                .unwrap_err()
                .kind,
            "invalid_json"
        );
        assert_eq!(
            decode_object_bytes(OpOutput::Timer).unwrap_err().kind,
            "protocol"
        );
        assert_eq!(decode_sql(OpOutput::Timer).unwrap_err().kind, "protocol");
    }
}
