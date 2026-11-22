//! The `/ui` guard and security headers (D-P11-15), plan 11-15.
//!
//! - [`guard`] (installed on every `/ui` route by [`crate::router`]) checks the `Host`
//!   header **and** the request URI authority (HTTP/2 `:authority`) against the
//!   [`HostAllowlist`] on every request, GET included, while auth is off (DNS rebinding).
//!   It then stamps the security headers on the response, including the guard's own 403.
//! - [`SameOriginMutation`] is the extractor every mutating `/ui` handler takes: (a)
//!   `Sec-Fetch-Site: same-origin` (or, when absent, an `Origin` equal to the request's
//!   scheme://host), (b) `HX-Request: true` plus a matching `X-Ostro-Csrf` header, or a
//!   matching `csrf` form field (double-submit cookie `ostro_csrf`), (c) the Host check
//!   again. Anything else is 403.
//! - [`csrf_token`] reads or mints the double-submit token for a page render.

use std::net::SocketAddr;

use axum::body::Body;
use axum::extract::{FromRequest, Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http::header::{self, HeaderMap, HeaderName, HeaderValue};

use crate::pages::{UiError, UiState};

/// The exact D-P11-15 Content-Security-Policy.
pub(crate) const CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; \
img-src 'self' data:; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; \
base-uri 'none'";

/// CSRF double-submit cookie (RESERVATIONS §11).
pub(crate) const CSRF_COOKIE: &str = "ostro_csrf";
/// CSRF request header (RESERVATIONS §11).
pub(crate) const CSRF_HEADER: &str = "x-ostro-csrf";
/// CSRF form field.
pub(crate) const CSRF_FIELD: &str = "csrf";
/// Largest mutating request body the extractor buffers.
pub(crate) const MAX_FORM_BYTES: usize = 256 * 1024;

/// Hostnames accepted with any port (D-P11-15).
const LOOPBACK_NAMES: [&str; 3] = ["localhost", "127.0.0.1", "[::1]"];

/// The `Host` / `:authority` allowlist: the bound address, the loopback names with any
/// port, and `[ui] allowed_hosts` (an entry with a port matches exactly; without a port
/// it matches any port).
#[derive(Debug, Clone)]
pub(crate) struct HostAllowlist {
    exact: Vec<String>,
    any_port: Vec<String>,
}

impl HostAllowlist {
    pub(crate) fn new(bound: Option<SocketAddr>, allowed_hosts: &[String]) -> Self {
        let mut exact = Vec::new();
        let mut any_port: Vec<String> = LOOPBACK_NAMES.iter().map(|s| (*s).to_owned()).collect();
        if let Some(addr) = bound {
            exact.push(addr.to_string().to_ascii_lowercase());
        }
        for h in allowed_hosts {
            let h = h.trim().to_ascii_lowercase();
            match split_host_port(&h) {
                Some((_, Some(_))) => exact.push(h),
                Some((name, None)) => any_port.push(name.to_owned()),
                None => {}
            }
        }
        HostAllowlist { exact, any_port }
    }

    /// Whether `host` (a `Host` header value or URI authority) is allowed.
    pub(crate) fn allows(&self, host: &str) -> bool {
        let h = host.trim().to_ascii_lowercase();
        let Some((name, _port)) = split_host_port(&h) else {
            return false;
        };
        self.exact.contains(&h) || self.any_port.iter().any(|n| n == name)
    }
}

/// `(host, port)` of a `host[:port]` / `[v6][:port]` authority; `None` when malformed.
fn split_host_port(h: &str) -> Option<(&str, Option<u16>)> {
    if h.is_empty() || h.contains('@') {
        return None;
    }
    let (name, port) = if h.starts_with('[') {
        let end = h.find(']')?;
        let (name, rest) = h.split_at(end + 1);
        match rest {
            "" => (name, None),
            r => (name, Some(r.strip_prefix(':')?)),
        }
    } else {
        match h.split_once(':') {
            Some((n, p)) => (n, Some(p)),
            None => (h, None),
        }
    };
    let valid = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':' | '[' | ']');
    if name.is_empty() || name.contains(|c: char| !valid(c)) {
        return None;
    }
    let port = match port {
        None => None,
        Some(p) if !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) => Some(p.parse().ok()?),
        Some(_) => return None,
    };
    Some((name, port))
}

/// Every host the request names (the `Host` header and the URI authority); `Err` when a
/// `Host` header is not visible ASCII.
fn request_hosts(headers: &HeaderMap, uri: &http::Uri) -> Result<Vec<String>, ()> {
    let mut hosts = Vec::new();
    for v in headers.get_all(header::HOST) {
        hosts.push(v.to_str().map_err(|_| ())?.to_owned());
    }
    if let Some(a) = uri.authority() {
        hosts.push(a.as_str().to_owned());
    }
    Ok(hosts)
}

/// (c): at least one host is named and every named host is allowlisted.
pub(crate) fn host_allowed(allow: &HostAllowlist, headers: &HeaderMap, uri: &http::Uri) -> bool {
    match request_hosts(headers, uri) {
        Ok(hosts) => !hosts.is_empty() && hosts.iter().all(|h| allow.allows(h)),
        Err(()) => false,
    }
}

/// The security headers of every `/ui` response; `Cache-Control: no-store` unless the
/// handler set its own (assets: `no-cache`).
pub(crate) fn apply_headers(headers: &mut HeaderMap) {
    let set = |h: &mut HeaderMap, k: HeaderName, v: &'static str| {
        h.insert(k, HeaderValue::from_static(v));
    };
    set(headers, header::CONTENT_SECURITY_POLICY, CSP);
    set(headers, header::X_CONTENT_TYPE_OPTIONS, "nosniff");
    set(headers, header::REFERRER_POLICY, "no-referrer");
    set(headers, header::X_FRAME_OPTIONS, "DENY");
    if !headers.contains_key(header::CACHE_CONTROL) {
        set(headers, header::CACHE_CONTROL, "no-store");
    }
}

/// The `/ui` middleware: Host / `:authority` allowlist on every route, then the
/// security headers on whatever answers.
pub(crate) async fn guard(State(state): State<UiState>, req: Request, next: Next) -> Response {
    let mut resp = if host_allowed(&state.hosts, req.headers(), req.uri()) {
        next.run(req).await
    } else {
        UiError::Forbidden("host not allowed".to_owned()).into_response()
    };
    apply_headers(resp.headers_mut());
    resp
}

/// The `ostro_csrf` cookie value, when present and well formed (64 lowercase hex).
pub(crate) fn cookie_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|kv| kv.trim().split_once('='))
        .find(|(k, _)| *k == CSRF_COOKIE)
        .map(|(_, v)| v.trim().to_owned())
        .filter(|v| is_token(v))
}

fn is_token(v: &str) -> bool {
    v.len() == 64 && v.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Whether the request arrived over https (the URI scheme, which HTTP/2 carries).
fn is_https(uri: &http::Uri) -> bool {
    uri.scheme_str() == Some("https")
}

/// The CSRF token for a page render: the request's `ostro_csrf` cookie when present,
/// else a fresh 32-byte random token plus the `Set-Cookie` that stores it. An OS RNG
/// failure is a 500 (there is no safe fallback token).
pub(crate) fn csrf_token(
    headers: &HeaderMap,
    uri: &http::Uri,
) -> Result<(String, Option<HeaderValue>), UiError> {
    if let Some(t) = cookie_token(headers) {
        return Ok((t, None));
    }
    let mut raw = [0u8; 32];
    getrandom::fill(&mut raw).map_err(|e| UiError::Internal(format!("csrf token: {e}")))?;
    let token: String = raw.iter().map(|b| format!("{b:02x}")).collect();
    let secure = if is_https(uri) { "; Secure" } else { "" };
    let cookie = format!("{CSRF_COOKIE}={token}; Path=/ui; HttpOnly; SameSite=Strict{secure}");
    let value = HeaderValue::from_str(&cookie).ok();
    Ok((token, value))
}

fn ct_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

/// A mutating `/ui` request that passed D-P11-15 (a), (b) and (c). Must be the last
/// extractor (it buffers the body, at most [`MAX_FORM_BYTES`]); the parsed form fields
/// are available through [`SameOriginMutation::field`].
#[derive(Debug, Clone)]
pub(crate) struct SameOriginMutation {
    htmx: bool,
    #[allow(dead_code)] // read by 11-16's actions through `field`/`fields`
    fields: Vec<(String, String)>,
}

impl SameOriginMutation {
    /// The request came from htmx (`HX-Request: true` + header token).
    pub(crate) fn is_htmx(&self) -> bool {
        self.htmx
    }

    /// The first value of form field `name`.
    #[allow(dead_code)] // consumed by 11-16's actions
    pub(crate) fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// Every form field, in order.
    #[allow(dead_code)] // consumed by 11-16's actions
    pub(crate) fn fields(&self) -> &[(String, String)] {
        &self.fields
    }
}

fn header_str<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
    h.get(name).and_then(|v| v.to_str().ok())
}

/// (a): `Sec-Fetch-Site: same-origin`, or no `Sec-Fetch-Site` and an `Origin` equal to
/// scheme://host. A present `Origin` must match either way.
fn same_origin(headers: &HeaderMap, uri: &http::Uri) -> bool {
    let host = header_str(headers, "host")
        .map(str::to_owned)
        .or_else(|| uri.authority().map(|a| a.as_str().to_owned()));
    let origin_ok = |o: &str| {
        host.as_deref().is_some_and(|h| {
            let o = o.to_ascii_lowercase();
            let h = h.to_ascii_lowercase();
            // Without TLS context on HTTP/1.1 the scheme is unknown; either scheme with
            // the exact (allowlisted) host is same-origin for this server.
            o == format!("http://{h}") || o == format!("https://{h}")
        })
    };
    let origin = header_str(headers, "origin");
    match header_str(headers, "sec-fetch-site") {
        Some(s) if s.eq_ignore_ascii_case("same-origin") => origin.is_none_or(origin_ok),
        Some(_) => false,
        None => origin.is_some_and(origin_ok),
    }
}

impl FromRequest<UiState> for SameOriginMutation {
    type Rejection = UiError;

    async fn from_request(req: Request, state: &UiState) -> Result<Self, Self::Rejection> {
        let forbidden = |m: &str| UiError::Forbidden(m.to_owned());
        let (parts, body) = req.into_parts();
        if !host_allowed(&state.hosts, &parts.headers, &parts.uri) {
            return Err(forbidden("host not allowed"));
        }
        if !same_origin(&parts.headers, &parts.uri) {
            return Err(forbidden("cross-origin request"));
        }
        let cookie = cookie_token(&parts.headers);
        let htmx = header_str(&parts.headers, "hx-request") == Some("true");
        let header_tok = header_str(&parts.headers, CSRF_HEADER);
        let bytes = axum::body::to_bytes(body, MAX_FORM_BYTES)
            .await
            .map_err(|_| UiError::BadRequest("request body too large".to_owned()))?;
        let is_form = header_str(&parts.headers, "content-type").is_some_and(|ct| {
            ct.split(';')
                .next()
                .is_some_and(|m| m.trim() == "application/x-www-form-urlencoded")
        });
        let fields = if is_form && !bytes.is_empty() {
            let req = Request::from_parts(parts.clone(), Body::from(bytes));
            axum::Form::<Vec<(String, String)>>::from_request(req, &())
                .await
                .map(|f| f.0)
                .map_err(|_| UiError::BadRequest("malformed form".to_owned()))?
        } else {
            Vec::new()
        };
        let Some(cookie) = cookie else {
            return Err(forbidden("missing CSRF cookie"));
        };
        let via_header = htmx && header_tok.is_some_and(|t| ct_eq(t, &cookie));
        let via_form = fields
            .iter()
            .find(|(k, _)| k == CSRF_FIELD)
            .is_some_and(|(_, v)| ct_eq(v, &cookie));
        if !(via_header || via_form) {
            return Err(forbidden("missing or mismatched CSRF token"));
        }
        Ok(SameOriginMutation {
            htmx: via_header,
            fields,
        })
    }
}

/// The guard probe (`POST /ui/_probe`), mounted only when `UiConfig::test_probe` is set.
pub(crate) async fn probe(m: SameOriginMutation) -> impl IntoResponse {
    if m.is_htmx() { "ok htmx" } else { "ok form" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_allowlist_rules() {
        let a = HostAllowlist::new(
            Some("10.0.0.5:7700".parse().expect("addr")),
            &["ops.example".to_owned(), "lb.example:8443".to_owned()],
        );
        for ok in [
            "127.0.0.1:7700",
            "127.0.0.1",
            "localhost:1",
            "LOCALHOST:7700",
            "[::1]:7700",
            "[::1]",
            "10.0.0.5:7700",
            "ops.example",
            "ops.example:9",
            "lb.example:8443",
        ] {
            assert!(a.allows(ok), "{ok}");
        }
        for bad in [
            "evil.example",
            "10.0.0.5:7701",
            "10.0.0.5",
            "lb.example",
            "lb.example:8444",
            "localhost.evil.example",
            "127.0.0.1.nip.io",
            "",
            "::1",
            "localhost:x",
            "user@localhost",
            "localhost:99999",
        ] {
            assert!(!a.allows(bad), "{bad}");
        }
    }

    #[test]
    fn csrf_token_reuses_cookie_or_mints() {
        let uri: http::Uri = "/ui/".parse().expect("uri");
        let (t, set) = csrf_token(&HeaderMap::new(), &uri).expect("rng");
        assert!(is_token(&t));
        let set = set.expect("set-cookie");
        let s = set.to_str().expect("ascii");
        assert!(s.starts_with(&format!("ostro_csrf={t};")), "{s}");
        assert!(s.contains("Path=/ui") && s.contains("HttpOnly") && s.contains("SameSite=Strict"));
        assert!(!s.contains("Secure"));
        let mut h = HeaderMap::new();
        h.insert(
            header::COOKIE,
            HeaderValue::from_str(&format!("a=b; ostro_csrf={t}")).expect("hv"),
        );
        assert_eq!(csrf_token(&h, &uri).expect("cookie"), (t, None));
        let https: http::Uri = "https://localhost/ui/".parse().expect("uri");
        let (_, set) = csrf_token(&HeaderMap::new(), &https).expect("rng");
        assert!(
            set.expect("cookie")
                .to_str()
                .expect("ascii")
                .ends_with("; Secure")
        );
    }
}
