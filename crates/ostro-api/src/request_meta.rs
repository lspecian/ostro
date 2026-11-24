//! Phase 13 request metadata (13-01 contract; D-P13-14/15/24). The REST and gRPC adapters
//! (13-03) fill a [`RequestMeta`] from the `ostro-*` headers and run the call inside
//! [`scope`]; the routed control plane (13-12) and the consistency hooks (13-13) read it with
//! [`RequestMeta::current`] and publish a write's consistency token with
//! [`set_consistency`], which the adapter emits as `ostro-consistency` after
//! [`take_consistency`].
//!
//! Headers (RESERVATIONS §11 P13): `ostro-hop`, `ostro-partition`, `ostro-partitions`,
//! `ostro-principal`, `ostro-consistency`, `ostro-read-after`, plus the remaining deadline
//! (`grpc-timeout` on gRPC, `ostro-deadline-ms` on REST). `partition_pin`, `partitions`,
//! `principal` and `from_node` are honoured only from node identities (Pitfall 7); the
//! adapters enforce that.
//!
//! # Adapter side (13-03)
//!
//! [`middleware`] wraps the REST and gRPC routes of [`crate::router_with`]: it parses the
//! headers with [`from_headers`] (malformed values answer 400 / `INVALID_ARGUMENT` before
//! the handler runs), runs the call inside [`scope_with_consistency`], and on success emits
//! the published token as `ostro-consistency`. Node identity comes only from a
//! [`TrustedNode`] request extension, which the cluster listener (13-12) inserts after its
//! mTLS check; without it the pinned headers are ignored and `from_node` stays `None`.
//! [`to_metadata`] is the client half ([`crate::grpc_client::GrpcControlPlane`]).

use std::cell::RefCell;
use std::future::Future;
use std::time::{Duration, Instant};

use axum::http::{HeaderMap, HeaderValue};
use axum::response::IntoResponse;

use crate::error::ApiError;

/// `ostro-hop`: 1 on a forwarded call (never forwarded again).
pub const HEADER_HOP: &str = "ostro-hop";
/// `ostro-partition`: the partition an unkeyed start was placed on.
pub const HEADER_PARTITION: &str = "ostro-partition";
/// `ostro-partitions`: the partitions a fan-out leg serves.
pub const HEADER_PARTITIONS: &str = "ostro-partitions";
/// `ostro-principal`: the original caller (Phase 14 hook).
pub const HEADER_PRINCIPAL: &str = "ostro-principal";
/// `ostro-consistency`: a write's token (`p<P>:<epoch>:<offset>` or `m:<index>`).
pub const HEADER_CONSISTENCY: &str = "ostro-consistency";
/// `ostro-read-after`: a token a read must observe.
pub const HEADER_READ_AFTER: &str = "ostro-read-after";

/// `ostro-deadline-ms`: the remaining deadline of a REST call in milliseconds (the REST twin
/// of `grpc-timeout`).
pub const HEADER_DEADLINE_MS: &str = "ostro-deadline-ms";
/// `grpc-timeout` (gRPC wire format: up to 8 digits and a unit `H M S m u n`).
pub const HEADER_GRPC_TIMEOUT: &str = "grpc-timeout";

/// Longest accepted `ostro-principal` / `ostro-read-after` / `ostro-consistency` value.
pub const MAX_TOKEN_LEN: usize = 256;
/// Longest honoured remaining deadline.
const MAX_DEADLINE: Duration = Duration::from_secs(86_400 * 365);
/// Most partitions in one `ostro-partitions` header.
pub const MAX_PARTITIONS: usize = 4096;

/// Metadata of the current API call.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RequestMeta {
    /// Forwarding hops so far (0 = from a client).
    pub hop: u8,
    /// Partition pin of a forwarded unkeyed start.
    pub partition_pin: Option<u16>,
    /// Partitions of a fan-out leg.
    pub partitions: Option<Vec<u16>>,
    /// Original principal (trusted only from node SANs).
    pub principal: Option<String>,
    /// Consistency token the read must observe.
    pub read_after: Option<String>,
    /// The forwarding node, when the caller is a node identity.
    pub from_node: Option<u64>,
    /// Remaining deadline (`grpc-timeout`).
    pub deadline: Option<Instant>,
}

struct Slot {
    meta: RequestMeta,
    consistency: RefCell<Option<String>>,
}

tokio::task_local! {
    static REQUEST_META: Slot;
}

impl RequestMeta {
    /// The metadata of the current call; `None` outside [`scope`] (in-process consumers such
    /// as triggers, cron and bulk jobs).
    pub fn current() -> Option<RequestMeta> {
        REQUEST_META.try_with(|s| s.meta.clone()).ok()
    }

    /// Whether this call was already forwarded once.
    pub fn is_forwarded(&self) -> bool {
        self.hop > 0
    }
}

/// Run `fut` with `meta` as the current request metadata.
pub async fn scope<F: Future>(meta: RequestMeta, fut: F) -> F::Output {
    REQUEST_META
        .scope(
            Slot {
                meta,
                consistency: RefCell::new(None),
            },
            fut,
        )
        .await
}

/// Run `fut` with `meta` and return its output plus the consistency token it published.
pub async fn scope_with_consistency<F: Future>(
    meta: RequestMeta,
    fut: F,
) -> (F::Output, Option<String>) {
    scope(meta, async move {
        let out = fut.await;
        (out, take_consistency())
    })
    .await
}

/// Publish the consistency token of a write (no-op outside [`scope`]).
pub fn set_consistency(token: String) {
    let _ = REQUEST_META.try_with(|s| *s.consistency.borrow_mut() = Some(token));
}

/// Take the published consistency token (inside [`scope`]).
pub fn take_consistency() -> Option<String> {
    REQUEST_META
        .try_with(|s| s.consistency.borrow_mut().take())
        .ok()
        .flatten()
}

// ---------------------------------------------------------------------------------
// Adapter side (13-03): parse, scope, emit
// ---------------------------------------------------------------------------------

/// Request extension naming the node identity of the caller. Only the cluster listener
/// (13-12) inserts it, after authenticating the peer certificate; its presence makes the
/// pinned headers (`ostro-partition`, `ostro-partitions`, `ostro-principal`) count and sets
/// [`RequestMeta::from_node`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrustedNode(pub u64);

fn bad_header(name: &str, why: impl std::fmt::Display) -> ApiError {
    ApiError::invalid_argument(format!("header {name}: {why}"))
        .with_details(serde_json::json!({ "header": name }))
}

fn header_str<'a>(h: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, ApiError> {
    let mut values = h.get_all(name).iter();
    let Some(v) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(bad_header(name, "repeated"));
    }
    let s = v
        .to_str()
        .map_err(|_| bad_header(name, "not visible ASCII"))?
        .trim();
    if s.is_empty() {
        return Err(bad_header(name, "empty"));
    }
    Ok(Some(s))
}

fn token(h: &HeaderMap, name: &str) -> Result<Option<String>, ApiError> {
    match header_str(h, name)? {
        Some(s) if s.len() > MAX_TOKEN_LEN => Err(bad_header(
            name,
            format!("longer than {MAX_TOKEN_LEN} bytes"),
        )),
        Some(s) => Ok(Some(s.to_owned())),
        None => Ok(None),
    }
}

fn partition(name: &str, s: &str) -> Result<u16, ApiError> {
    s.trim()
        .parse::<u16>()
        .map_err(|_| bad_header(name, format!("{s:?} is not a partition (0..=65535)")))
}

/// Parse a `grpc-timeout` value (`<1..8 digits><H|M|S|m|u|n>`).
pub fn parse_grpc_timeout(s: &str) -> Option<Duration> {
    if s.len() < 2 || s.len() > 9 {
        return None;
    }
    let (digits, unit) = s.split_at(s.len() - 1);
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: u64 = digits.parse().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(n.checked_mul(3600)?),
        "M" => Duration::from_secs(n.checked_mul(60)?),
        "S" => Duration::from_secs(n),
        "m" => Duration::from_millis(n),
        "u" => Duration::from_micros(n),
        "n" => Duration::from_nanos(n),
        _ => return None,
    })
}

/// Format a duration as a `grpc-timeout` value (milliseconds, rounded up; at most 8
/// digits, so very long deadlines saturate at 99,999,999 ms ≈ 27.7 h).
pub fn format_grpc_timeout(d: Duration) -> String {
    let ms = d.as_nanos().div_ceil(1_000_000).min(99_999_999);
    format!("{ms}m")
}

/// The [`RequestMeta`] of a request's headers. `trusted` is the caller's node identity
/// ([`TrustedNode`]); without one the pinned headers are validated but ignored. `now` is
/// the arrival instant the deadline is relative to. Malformed values are
/// `invalid_argument` with `details.header`.
pub fn from_headers(
    h: &HeaderMap,
    trusted: Option<u64>,
    now: Instant,
) -> Result<RequestMeta, ApiError> {
    let hop = match header_str(h, HEADER_HOP)? {
        Some(s) => s
            .parse::<u8>()
            .map_err(|_| bad_header(HEADER_HOP, format!("{s:?} is not 0..=255")))?,
        None => 0,
    };
    let partition_pin = header_str(h, HEADER_PARTITION)?
        .map(|s| partition(HEADER_PARTITION, s))
        .transpose()?;
    let partitions = match header_str(h, HEADER_PARTITIONS)? {
        Some(s) => {
            let v = s
                .split(',')
                .map(|p| partition(HEADER_PARTITIONS, p))
                .collect::<Result<Vec<u16>, _>>()?;
            if v.len() > MAX_PARTITIONS {
                return Err(bad_header(
                    HEADER_PARTITIONS,
                    format!("more than {MAX_PARTITIONS} partitions"),
                ));
            }
            Some(v)
        }
        None => None,
    };
    let principal = token(h, HEADER_PRINCIPAL)?;
    let read_after = token(h, HEADER_READ_AFTER)?;
    let grpc = header_str(h, HEADER_GRPC_TIMEOUT)?
        .map(|s| {
            parse_grpc_timeout(s).ok_or_else(|| bad_header(HEADER_GRPC_TIMEOUT, format!("{s:?}")))
        })
        .transpose()?;
    let rest = header_str(h, HEADER_DEADLINE_MS)?
        .map(|s| {
            s.parse::<u64>()
                .map(Duration::from_millis)
                .map_err(|_| bad_header(HEADER_DEADLINE_MS, format!("{s:?} is not a u64")))
        })
        .transpose()?;
    let remaining = match (grpc, rest) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    let trusted_node = trusted.is_some();
    Ok(RequestMeta {
        hop,
        partition_pin: partition_pin.filter(|_| trusted_node),
        partitions: partitions.filter(|_| trusted_node),
        principal: principal.filter(|_| trusted_node),
        read_after,
        from_node: trusted,
        // A remaining budget above one year is treated as one year (no Instant overflow).
        deadline: remaining.and_then(|d| now.checked_add(d.min(MAX_DEADLINE))),
    })
}

/// Copy `meta` into outgoing gRPC metadata (the client half of [`from_headers`]):
/// `ostro-hop`, `ostro-partition`, `ostro-partitions`, `ostro-principal` and
/// `ostro-read-after`. The deadline travels as the request timeout (`grpc-timeout`), set by
/// the caller; `from_node` never travels (the receiver derives it from the peer identity).
pub fn to_metadata(meta: &RequestMeta, md: &mut tonic::metadata::MetadataMap) {
    fn put(md: &mut tonic::metadata::MetadataMap, name: &'static str, v: &str) {
        if let Ok(v) = tonic::metadata::MetadataValue::try_from(v) {
            md.insert(name, v);
        }
    }
    if meta.hop > 0 {
        put(md, HEADER_HOP, &meta.hop.to_string());
    }
    if let Some(p) = meta.partition_pin {
        put(md, HEADER_PARTITION, &p.to_string());
    }
    if let Some(ps) = &meta.partitions {
        let list = ps.iter().map(u16::to_string).collect::<Vec<_>>().join(",");
        if !list.is_empty() {
            put(md, HEADER_PARTITIONS, &list);
        }
    }
    if let Some(p) = &meta.principal {
        put(md, HEADER_PRINCIPAL, p);
    }
    if let Some(r) = &meta.read_after {
        put(md, HEADER_READ_AFTER, r);
    }
}

fn is_grpc(h: &HeaderMap) -> bool {
    h.get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/grpc"))
}

fn succeeded(res: &axum::response::Response, grpc: bool) -> bool {
    if grpc {
        // Trailers-only errors carry `grpc-status` in the headers; success puts it in the
        // trailers.
        res.status().is_success()
            && res
                .headers()
                .get("grpc-status")
                .is_none_or(|v| v.as_bytes() == b"0")
    } else {
        res.status().is_success()
    }
}

/// The axum middleware of the API routes (REST and gRPC): parse the request metadata, run
/// the call in its scope, emit `ostro-consistency` on success.
pub async fn middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let grpc = is_grpc(req.headers());
    let trusted = req.extensions().get::<TrustedNode>().map(|t| t.0);
    // clock-exempt: request deadlines are wall-time budgets of the caller.
    let meta = match from_headers(req.headers(), trusted, Instant::now()) {
        Ok(m) => m,
        Err(e) if grpc => return crate::grpc::to_status(e).into_http(),
        Err(e) => return e.into_response(),
    };
    let (mut res, token) = scope_with_consistency(meta, next.run(req)).await;
    if let Some(t) = token
        && succeeded(&res, grpc)
        && let Ok(v) = HeaderValue::from_str(&t)
    {
        res.headers_mut().insert(HEADER_CONSISTENCY, v);
    }
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scope_carries_meta_and_the_consistency_out_slot() {
        assert_eq!(RequestMeta::current(), None);
        set_consistency("ignored".into());
        assert_eq!(take_consistency(), None);
        let meta = RequestMeta {
            hop: 1,
            partition_pin: Some(3),
            from_node: Some(2),
            ..RequestMeta::default()
        };
        let (seen, token) = scope_with_consistency(meta.clone(), async {
            let m = RequestMeta::current().expect("in scope");
            tokio::task::yield_now().await;
            set_consistency("p3:7:42".into());
            m
        })
        .await;
        assert_eq!(seen, meta);
        assert!(seen.is_forwarded());
        assert_eq!(token.as_deref(), Some("p3:7:42"));
        assert_eq!(RequestMeta::current(), None);
    }

    fn headers(kv: &[(&'static str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in kv {
            h.append(*k, HeaderValue::from_str(v).expect("value"));
        }
        h
    }

    #[test]
    fn parses_every_header_and_trusts_pins_only_from_nodes() {
        let now = Instant::now();
        let h = headers(&[
            (HEADER_HOP, "1"),
            (HEADER_PARTITION, "7"),
            (HEADER_PARTITIONS, "1, 2,3"),
            (HEADER_PRINCIPAL, "user:alice"),
            (HEADER_READ_AFTER, "p7:3:42"),
            (HEADER_GRPC_TIMEOUT, "1500m"),
        ]);
        let m = from_headers(&h, Some(2), now).expect("ok");
        assert_eq!(m.hop, 1);
        assert_eq!(m.partition_pin, Some(7));
        assert_eq!(m.partitions, Some(vec![1, 2, 3]));
        assert_eq!(m.principal.as_deref(), Some("user:alice"));
        assert_eq!(m.read_after.as_deref(), Some("p7:3:42"));
        assert_eq!(m.from_node, Some(2));
        assert_eq!(m.deadline, Some(now + Duration::from_millis(1500)));
        // A client: pins ignored, hop / read-after / deadline kept.
        let m = from_headers(&h, None, now).expect("ok");
        assert_eq!(
            (m.partition_pin, m.partitions, m.principal, m.from_node),
            (None, None, None, None)
        );
        assert_eq!(m.hop, 1);
        assert_eq!(m.read_after.as_deref(), Some("p7:3:42"));
        // REST deadline; the smaller of the two wins.
        let m = from_headers(
            &headers(&[(HEADER_DEADLINE_MS, "250"), (HEADER_GRPC_TIMEOUT, "1S")]),
            None,
            now,
        )
        .expect("ok");
        assert_eq!(m.deadline, Some(now + Duration::from_millis(250)));
        assert_eq!(
            from_headers(&HeaderMap::new(), None, now).expect("empty"),
            RequestMeta::default()
        );
    }

    #[test]
    fn malformed_headers_are_invalid_argument_naming_the_header() {
        let now = Instant::now();
        for (k, v) in [
            (HEADER_HOP, "x"),
            (HEADER_HOP, "256"),
            (HEADER_PARTITION, "70000"),
            (HEADER_PARTITIONS, "1,,2"),
            (HEADER_PARTITIONS, "a"),
            (HEADER_GRPC_TIMEOUT, "10"),
            (HEADER_GRPC_TIMEOUT, "123456789m"),
            (HEADER_DEADLINE_MS, "-1"),
            (HEADER_READ_AFTER, " "),
        ] {
            let e = from_headers(&headers(&[(k, v)]), Some(1), now).expect_err(v);
            assert_eq!(e.code, crate::ErrorCode::InvalidArgument, "{k}: {v}");
            assert_eq!(e.details.as_ref().expect("details")["header"], k);
        }
        let e = from_headers(&headers(&[(HEADER_HOP, "1"), (HEADER_HOP, "1")]), None, now)
            .expect_err("repeated");
        assert!(e.message.contains("repeated"));
        let long = "x".repeat(MAX_TOKEN_LEN + 1);
        assert!(from_headers(&headers(&[(HEADER_PRINCIPAL, &long)]), Some(1), now).is_err());
    }

    #[test]
    fn grpc_timeout_round_trips() {
        for (s, d) in [
            ("1H", Duration::from_secs(3600)),
            ("2M", Duration::from_secs(120)),
            ("3S", Duration::from_secs(3)),
            ("4m", Duration::from_millis(4)),
            ("5u", Duration::from_micros(5)),
            ("6n", Duration::from_nanos(6)),
        ] {
            assert_eq!(parse_grpc_timeout(s), Some(d), "{s}");
        }
        assert_eq!(format_grpc_timeout(Duration::from_micros(1500)), "2m");
        assert_eq!(
            format_grpc_timeout(Duration::from_secs(1 << 40)),
            "99999999m"
        );
        assert_eq!(parse_grpc_timeout("x1m"), None);
    }

    #[test]
    fn to_metadata_is_the_inverse_of_from_headers() {
        let meta = RequestMeta {
            hop: 1,
            partition_pin: Some(4),
            partitions: Some(vec![4, 9]),
            principal: Some("user:bob".into()),
            read_after: Some("m:17".into()),
            from_node: Some(3),
            deadline: None,
        };
        let mut md = tonic::metadata::MetadataMap::new();
        to_metadata(&meta, &mut md);
        let back = from_headers(&md.into_headers(), Some(3), Instant::now()).expect("parse");
        assert_eq!(back, meta);
        let mut md = tonic::metadata::MetadataMap::new();
        to_metadata(&RequestMeta::default(), &mut md);
        assert!(md.is_empty());
    }
}
