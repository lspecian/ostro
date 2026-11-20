//! Mock HTTP provider and recording proxy listener for the effects bridge tests (06-10;
//! feature `mock-http`). Used by ostro-bridge's `tests/http_connector.rs` and by the e2e plans
//! 06-17 / 06-19.
//!
//! [`MockProvider::start`] serves on `127.0.0.1:<ephemeral>` (loopback only: tests must set
//! `[egress] allow_private = ["127.0.0.0/8"]` explicitly). Every request is counted.
//!
//! # Endpoints
//!
//! | Endpoint | Behaviour |
//! |----------|-----------|
//! | `POST /pay[?delay_ms=N][&fail_first=N]` | Payment API with key dedupe. Requires `Idempotency-Key` (400 `{"error":"missing_idempotency_key"}` otherwise). The first `fail_first` requests of a key get 503. The first successful request of a key increments [`charges`](MockProvider::charges), sleeps `delay_ms`, stores and returns 200 `{"charge_id":"<n>","key":"<key>"}` (`n` = global charge counter). A completed key replays the stored response; a request for a key still in progress gets 409 `{"error":"in_progress"}`. |
//! | `ANY /echo[?gzip=1]` | 200 JSON `{"method", "path", "headers": [[name, value], ...], "body_b64"}`; header values are echoed verbatim (lossy UTF-8). `gzip=1` labels the response `Content-Encoding: gzip` (the body stays plain JSON bytes: a connector must refuse it before decoding). |
//! | `GET /redirect?to=<url>[&code=302]` | Status `code` (default 302) with `Location: <url>` (percent-decoded). |
//! | `GET /slow?ms=N` | Sleeps `N` ms, then 200 `slow`. |
//! | `GET /big?bytes=N[&no_length=1]` | 200 with `N` bytes of `x`. `no_length=1` sends `Transfer-Encoding: chunked` and no `Content-Length`. |
//! | `GET /status/{code}` | That status, body `status <code>`. |
//! | `GET /flaky?fail=N` | The first `N` requests per path+query get 503 `flaky`; then 200 `ok`. |
//! | `GET /set-header?name=<n>&value=<v>` | 200 `ok` with response header `n: v`. |
//! | anything else | 404 `not found`. |
//!
//! # Counters
//!
//! [`requests`](MockProvider::requests) (requests carrying `Idempotency-Key: key`, any
//! endpoint), [`charges`](MockProvider::charges), [`total_requests`](MockProvider::total_requests)
//! and [`requests_to`](MockProvider::requests_to) (by path, without the query).
//!
//! [`RecordingListener::start`] is a raw TCP listener that counts accepted connections and
//! closes them: a fake proxy for "env proxies are ignored" tests.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Response, StatusCode, Uri, header};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// The key header `/pay` dedupes on (and [`MockProvider::requests`] counts).
pub const KEY_HEADER: &str = "idempotency-key";

#[derive(Clone)]
enum PayState {
    InProgress,
    Done(String),
}

#[derive(Default)]
struct Inner {
    total: AtomicUsize,
    charge_seq: AtomicUsize,
    by_key: Mutex<HashMap<String, usize>>,
    charges: Mutex<HashMap<String, usize>>,
    by_path: Mutex<HashMap<String, usize>>,
    flaky: Mutex<HashMap<String, usize>>,
    pay: Mutex<HashMap<String, PayState>>,
}

fn bump(m: &Mutex<HashMap<String, usize>>, k: &str) -> usize {
    let mut g = m.lock().unwrap_or_else(|e| e.into_inner());
    let n = g.entry(k.to_owned()).or_default();
    *n += 1;
    *n
}

fn get(m: &Mutex<HashMap<String, usize>>, k: &str) -> usize {
    m.lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(k)
        .copied()
        .unwrap_or(0)
}

/// A running mock provider; the server stops when this is dropped.
pub struct MockProvider {
    addr: SocketAddr,
    inner: Arc<Inner>,
    task: JoinHandle<()>,
}

impl MockProvider {
    /// Starts the provider on `127.0.0.1:0`.
    pub async fn start() -> MockProvider {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock provider");
        let addr = listener.local_addr().expect("local addr");
        let inner = Arc::new(Inner::default());
        let app = Router::new().fallback(handle).with_state(inner.clone());
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        MockProvider { addr, inner, task }
    }

    /// The bound address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// `http://127.0.0.1:<port>` (no trailing slash).
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Requests that carried `Idempotency-Key: key` (any endpoint).
    pub fn requests(&self, key: &str) -> usize {
        get(&self.inner.by_key, key)
    }

    /// Charges `/pay` made for `key` (0 or 1 when dedupe works).
    pub fn charges(&self, key: &str) -> usize {
        get(&self.inner.charges, key)
    }

    /// Every request received.
    pub fn total_requests(&self) -> usize {
        self.inner.total.load(Ordering::SeqCst)
    }

    /// Requests to `path` (query excluded), e.g. `"/pay"`.
    pub fn requests_to(&self, path: &str) -> usize {
        get(&self.inner.by_path, path)
    }
}

impl Drop for MockProvider {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A raw TCP listener that counts accepted connections and closes each one.
pub struct RecordingListener;

impl RecordingListener {
    /// Starts the listener on `127.0.0.1:0`: its address and the accept counter. It runs
    /// until the runtime shuts down.
    pub async fn start() -> (SocketAddr, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind recording listener");
        let addr = listener.local_addr().expect("local addr");
        let accepted = Arc::new(AtomicUsize::new(0));
        let count = accepted.clone();
        tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                count.fetch_add(1, Ordering::SeqCst);
                drop(sock);
            }
        });
        (addr, accepted)
    }
}

// ---- request handling ----

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => {
                let hex = std::str::from_utf8(&b[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(v) => {
                        out.push(v);
                        i += 2;
                    }
                    None => out.push(b'%'),
                }
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn query(uri: &Uri) -> HashMap<String, String> {
    uri.query()
        .unwrap_or("")
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (percent_decode(k), percent_decode(v)),
            None => (percent_decode(p), String::new()),
        })
        .collect()
}

fn num(q: &HashMap<String, String>, k: &str) -> u64 {
    q.get(k).and_then(|v| v.parse().ok()).unwrap_or(0)
}

fn resp(status: StatusCode, body: impl Into<Body>) -> Response<Body> {
    let mut r = Response::new(body.into());
    *r.status_mut() = status;
    r
}

fn json(status: StatusCode, v: &serde_json::Value) -> Response<Body> {
    let mut r = resp(status, v.to_string());
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    r
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard padded base64 (the testkit has no base64 dependency under `mock-http`).
fn base64(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for (i, shift) in [18u32, 12, 6, 0].into_iter().enumerate() {
            if i <= chunk.len() {
                out.push(char::from(B64[((n >> shift) & 63) as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

async fn handle(
    State(s): State<Arc<Inner>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    s.total.fetch_add(1, Ordering::SeqCst);
    let path = uri.path().to_owned();
    bump(&s.by_path, &path);
    let key = headers
        .get(KEY_HEADER)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned());
    if let Some(k) = &key {
        bump(&s.by_key, k);
    }
    let q = query(&uri);

    match path.as_str() {
        "/pay" if method == Method::POST => pay(&s, key, &q).await,
        "/echo" => echo(&method, &uri, &headers, &body, q.contains_key("gzip")),
        "/redirect" => {
            let code = q
                .get("code")
                .and_then(|c| c.parse::<u16>().ok())
                .and_then(|c| StatusCode::from_u16(c).ok())
                .unwrap_or(StatusCode::FOUND);
            let mut r = resp(code, "");
            if let Some(to) = q.get("to")
                && let Ok(v) = HeaderValue::from_str(to)
            {
                r.headers_mut().insert(header::LOCATION, v);
            }
            r
        }
        "/slow" => {
            tokio::time::sleep(Duration::from_millis(num(&q, "ms"))).await;
            resp(StatusCode::OK, "slow")
        }
        "/big" => {
            let n = usize::try_from(num(&q, "bytes")).unwrap_or(usize::MAX);
            let mut r = resp(StatusCode::OK, vec![b'x'; n]);
            if q.contains_key("no_length") {
                r.headers_mut().insert(
                    header::TRANSFER_ENCODING,
                    HeaderValue::from_static("chunked"),
                );
            }
            r
        }
        "/flaky" => {
            let fail = usize::try_from(num(&q, "fail")).unwrap_or(usize::MAX);
            let n = bump(&s.flaky, &uri.to_string());
            if n <= fail {
                resp(StatusCode::SERVICE_UNAVAILABLE, "flaky")
            } else {
                resp(StatusCode::OK, "ok")
            }
        }
        "/set-header" => {
            let mut r = resp(StatusCode::OK, "ok");
            if let (Some(n), Some(v)) = (q.get("name"), q.get("value"))
                && let (Ok(n), Ok(v)) = (
                    HeaderName::from_bytes(n.as_bytes()),
                    HeaderValue::from_str(v),
                )
            {
                r.headers_mut().insert(n, v);
            }
            r
        }
        p if p.starts_with("/status/") => {
            match p["/status/".len()..]
                .parse::<u16>()
                .ok()
                .and_then(|c| StatusCode::from_u16(c).ok())
            {
                Some(code) => resp(code, format!("status {}", code.as_u16())),
                None => resp(StatusCode::BAD_REQUEST, "bad status"),
            }
        }
        _ => resp(StatusCode::NOT_FOUND, "not found"),
    }
}

async fn pay(s: &Inner, key: Option<String>, q: &HashMap<String, String>) -> Response<Body> {
    let Some(key) = key else {
        return json(
            StatusCode::BAD_REQUEST,
            &serde_json::json!({"error": "missing_idempotency_key"}),
        );
    };
    let fail_first = usize::try_from(num(q, "fail_first")).unwrap_or(usize::MAX);
    if get(&s.by_key, &key) <= fail_first {
        return json(
            StatusCode::SERVICE_UNAVAILABLE,
            &serde_json::json!({"error": "unavailable"}),
        );
    }
    let previous = {
        let mut g = s.pay.lock().unwrap_or_else(|e| e.into_inner());
        let prev = g.get(&key).cloned();
        if prev.is_none() {
            g.insert(key.clone(), PayState::InProgress);
        }
        prev
    };
    match previous {
        Some(PayState::Done(body)) => {
            let mut r = resp(StatusCode::OK, body);
            r.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            r.headers_mut()
                .insert("idempotent-replayed", HeaderValue::from_static("true"));
            r
        }
        Some(PayState::InProgress) => json(
            StatusCode::CONFLICT,
            &serde_json::json!({"error": "in_progress"}),
        ),
        None => {
            bump(&s.charges, &key);
            let n = s.charge_seq.fetch_add(1, Ordering::SeqCst) + 1;
            tokio::time::sleep(Duration::from_millis(num(q, "delay_ms"))).await;
            let body = serde_json::json!({"charge_id": n.to_string(), "key": key}).to_string();
            s.pay
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(key, PayState::Done(body.clone()));
            let mut r = resp(StatusCode::OK, body);
            r.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            r
        }
    }
}

fn echo(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &[u8],
    gzip: bool,
) -> Response<Body> {
    let hs: Vec<serde_json::Value> = headers
        .iter()
        .map(|(k, v)| serde_json::json!([k.as_str(), String::from_utf8_lossy(v.as_bytes())]))
        .collect();
    let mut r = json(
        StatusCode::OK,
        &serde_json::json!({
            "method": method.as_str(),
            "path": uri.path(),
            "headers": hs,
            "body_b64": base64(body),
        }),
    );
    if gzip {
        r.headers_mut()
            .insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_rfc4648_vectors() {
        for (i, o) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(i.as_bytes()), o);
        }
    }

    #[test]
    fn percent_decoding() {
        assert_eq!(percent_decode("a%20b+c"), "a b c");
        assert_eq!(
            percent_decode("http%3A%2F%2F127.0.0.1%3A80%2Fx"),
            "http://127.0.0.1:80/x"
        );
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz"), "%zz");
    }
}
