//! Live (in-memory) effect types shared by the engine seam and the effects bridge (Phase 6,
//! plan 06-02, P-2).
//!
//! These are **plain data, never serialised and never journaled**: nothing here derives
//! `Serialize`, so adding them is not a format change. The journal keeps only what the
//! [`crate::journal::EffectScheduled`] event records (header names, never header values); a
//! [`LiveEffect`] carries the *real* request (header values included) from the guest call to the
//! connector, and is dropped once the effect completes.
//!
//! `ostro-bridge` depends on this module rather than on `ostro-engine` or `ostro-partition`
//! (dependency direction in 06-RESEARCH), and Phase 7 workers reuse it unchanged.

use std::fmt;

use crate::journal::{EffectRequest, HttpRequest, ObjectRequest, SqlRequest};

/// The connector family of an effect. The numeric values are the `kind` of a
/// [`crate::command::RegisterEffect`] intent (`EffectKind as u32`) and are frozen.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum EffectKind {
    /// `http-fetch`.
    Http = 0,
    /// `sql-execute`.
    Sql = 1,
    /// `object-op`.
    Object = 2,
}

impl EffectKind {
    /// The frozen numeric value.
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// The kind with numeric value `v`; `None` for an unknown value.
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Http),
            1 => Some(Self::Sql),
            2 => Some(Self::Object),
            _ => None,
        }
    }

    /// The kind of a request.
    pub const fn from_request(r: &EffectRequest) -> Self {
        match r {
            EffectRequest::Http(_) => Self::Http,
            EffectRequest::Sql(_) => Self::Sql,
            EffectRequest::Object(_) => Self::Object,
        }
    }
}

/// The HTTP retry policy of one effect (the guest's `retry-policy`, applied by the host and
/// never journaled). Field meanings match `ostro_engine::retry::RetryPolicy`.
#[derive(Clone, Debug, PartialEq)]
pub struct EffectRetry {
    /// Total attempts including the first; `0` and `1` mean no retry.
    pub max_attempts: u32,
    /// First backoff interval, in ms.
    pub initial_interval_ms: u64,
    /// Backoff cap, in ms.
    pub max_interval_ms: u64,
    /// Backoff multiplier.
    pub backoff_coefficient: f64,
    /// `ErrorInfo.kind` values that are never retried.
    pub non_retryable_kinds: Vec<String>,
}

/// One in-flight effect, as handed to the bridge (live or on re-drive).
///
/// `Debug` is hand-written: header values print as `"<value>"` and bodies print as their
/// length, so a secret or credential carried in a header or body never reaches a log line.
#[derive(Clone, PartialEq)]
pub struct LiveEffect {
    /// Operation id of the effect in its run's journal.
    pub op: u64,
    /// The request with real header values (not the journaled, value-free form).
    pub request: EffectRequest,
    /// The guest's HTTP retry policy, if any.
    pub http_retry: Option<EffectRetry>,
    /// `ts_nanos` of the journaled `EffectScheduled` event.
    pub scheduled_ts_nanos: u64,
    /// `true` when the effect is re-driven after a restart or leader change (the request may
    /// already have been sent; FX-03 decides at re-drive time).
    pub redrive: bool,
}

impl LiveEffect {
    /// The connector family of this effect.
    pub const fn kind(&self) -> EffectKind {
        EffectKind::from_request(&self.request)
    }
}

/// Redacted view of an optional body: its length only.
struct BodyLen<'a>(&'a Option<Vec<u8>>);

impl fmt::Debug for BodyLen<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            None => f.write_str("None"),
            Some(b) => write!(f, "<{} bytes>", b.len()),
        }
    }
}

/// Header names with every value replaced by `"<value>"`.
struct RedactedHeaders<'a>(&'a HttpRequest);

impl fmt::Debug for RedactedHeaders<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.0.headers.iter().map(|h| (h.name.as_str(), "<value>")))
            .finish()
    }
}

struct RedactedRequest<'a>(&'a EffectRequest);

impl fmt::Debug for RedactedRequest<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            EffectRequest::Http(r) => f
                .debug_struct("Http")
                .field("method", &r.method)
                .field("url", &r.url)
                .field("headers", &RedactedHeaders(r))
                .field("body", &BodyLen(&r.body))
                .field("timeout_ms", &r.timeout_ms)
                .field(
                    "idempotency_key",
                    &r.idempotency_key.as_ref().map(|_| "<value>"),
                )
                .field("redact", &r.redact)
                .finish(),
            EffectRequest::Sql(SqlRequest {
                connection,
                statement,
                params,
                exactly_once,
            }) => f
                .debug_struct("Sql")
                .field("connection", connection)
                .field("statement", statement)
                .field("params", &format_args!("<{} values>", params.len()))
                .field("exactly_once", exactly_once)
                .finish(),
            EffectRequest::Object(ObjectRequest {
                connection,
                op,
                key,
                body,
            }) => f
                .debug_struct("Object")
                .field("connection", connection)
                .field("op", op)
                .field("key", key)
                .field("body", &BodyLen(body))
                .finish(),
        }
    }
}

impl fmt::Debug for LiveEffect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveEffect")
            .field("op", &self.op)
            .field("request", &RedactedRequest(&self.request))
            .field("http_retry", &self.http_retry)
            .field("scheduled_ts_nanos", &self.scheduled_ts_nanos)
            .field("redrive", &self.redrive)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{Header, SqlValue};

    const CANARY: &str = "canary-7f3a9c2e5b1d";

    fn http(headers: Vec<Header>, body: Option<Vec<u8>>) -> LiveEffect {
        LiveEffect {
            op: 3,
            request: EffectRequest::Http(HttpRequest {
                method: "POST".into(),
                url: "https://api.example.com/charge".into(),
                headers,
                body,
                timeout_ms: Some(5_000),
                idempotency_key: Some(format!("key-{CANARY}")),
                redact: vec!["authorization".into()],
            }),
            http_retry: None,
            scheduled_ts_nanos: 42,
            redrive: false,
        }
    }

    #[test]
    fn kind_values_are_frozen() {
        assert_eq!(EffectKind::Http.as_u8(), 0);
        assert_eq!(EffectKind::Sql.as_u8(), 1);
        assert_eq!(EffectKind::Object.as_u8(), 2);
        for k in [EffectKind::Http, EffectKind::Sql, EffectKind::Object] {
            assert_eq!(EffectKind::from_u8(k.as_u8()), Some(k));
        }
        assert_eq!(EffectKind::from_u8(3), None);
        assert_eq!(http(vec![], None).kind(), EffectKind::Http);
    }

    #[test]
    fn debug_never_prints_header_values_or_bodies() {
        let e = http(
            vec![
                Header {
                    name: "authorization".into(),
                    value: format!("Bearer {CANARY}"),
                },
                Header {
                    name: "x-api-key".into(),
                    value: CANARY.into(),
                },
            ],
            Some(format!("{{\"card\":\"{CANARY}\"}}").into_bytes()),
        );
        let s = format!("{e:?}");
        assert!(!s.contains(CANARY), "{s}");
        assert!(s.contains("authorization"), "{s}");
        assert!(s.contains("x-api-key"), "{s}");
        assert!(s.contains("<value>"), "{s}");
        assert!(s.contains("bytes>"), "{s}");
        let pretty = format!("{e:#?}");
        assert!(!pretty.contains(CANARY), "{pretty}");
    }

    #[test]
    fn debug_redacts_sql_params_and_object_bodies() {
        let sql = LiveEffect {
            op: 1,
            request: EffectRequest::Sql(SqlRequest {
                connection: "orders-db".into(),
                statement: "insert into t values ($1)".into(),
                params: vec![SqlValue::Text(CANARY.into())],
                exactly_once: true,
            }),
            http_retry: Some(EffectRetry {
                max_attempts: 3,
                initial_interval_ms: 100,
                max_interval_ms: 1_000,
                backoff_coefficient: 2.0,
                non_retryable_kinds: vec![],
            }),
            scheduled_ts_nanos: 0,
            redrive: true,
        };
        assert_eq!(sql.kind(), EffectKind::Sql);
        let s = format!("{sql:?}");
        assert!(!s.contains(CANARY), "{s}");
        let obj = LiveEffect {
            op: 2,
            request: EffectRequest::Object(ObjectRequest {
                connection: "files".into(),
                op: "put".into(),
                key: "a/b".into(),
                body: Some(CANARY.as_bytes().to_vec()),
            }),
            http_retry: None,
            scheduled_ts_nanos: 0,
            redrive: false,
        };
        assert_eq!(obj.kind(), EffectKind::Object);
        let s = format!("{obj:?}");
        assert!(!s.contains(CANARY), "{s}");
    }
}
