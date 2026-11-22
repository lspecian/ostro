//! Embedded static assets (plan 11-15, D-P11-14/15): `include_bytes!`, a strong
//! `"sha256-<hex>"` ETag computed once, `Cache-Control: no-cache`, 304 on a matching
//! `If-None-Match`. Served at `GET /ui/assets/{*path}`.

use std::sync::OnceLock;

use axum::extract::Path;
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use http::header::{self, HeaderMap, HeaderValue};
use sha2::{Digest, Sha256};

use crate::pages::UiError;

/// `text/javascript` (RFC 9239) for scripts.
const JS: &str = "text/javascript; charset=utf-8";
/// CSS.
const CSS: &str = "text/css; charset=utf-8";

/// `(path under /ui/assets/, bytes, content type)`.
pub(crate) const ASSETS: &[(&str, &[u8], &str)] = &[
    ("app.css", include_bytes!("../assets/app.css"), CSS),
    ("app.js", include_bytes!("../assets/app.js"), JS),
    (
        "vendor/htmx-2.0.11.min.js",
        include_bytes!("../assets/vendor/htmx-2.0.11.min.js"),
        JS,
    ),
];

/// The strong ETag of every asset, in [`ASSETS`] order.
fn etags() -> &'static [String] {
    static ETAGS: OnceLock<Vec<String>> = OnceLock::new();
    ETAGS.get_or_init(|| {
        ASSETS
            .iter()
            .map(|(_, bytes, _)| {
                let hex: String = Sha256::digest(bytes)
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect();
                format!("\"sha256-{hex}\"")
            })
            .collect()
    })
}

/// Whether `If-None-Match` lists `etag` (or `*`). Weak comparison, as RFC 9110 requires
/// for If-None-Match.
fn not_modified(headers: &HeaderMap, etag: &str) -> bool {
    headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|t| t.trim().trim_start_matches("W/"))
        .any(|t| t == "*" || t == etag)
}

/// `GET /ui/assets/{*path}`.
pub(crate) async fn serve(Path(path): Path<String>, headers: HeaderMap) -> Response {
    let Some(i) = ASSETS.iter().position(|(n, _, _)| *n == path) else {
        return UiError::NotFound(format!("no asset {path}")).into_response();
    };
    let (_, bytes, ct) = ASSETS[i];
    let etag = &etags()[i];
    let mut resp = if not_modified(&headers, etag) {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        let mut r = bytes.into_response();
        r.headers_mut()
            .insert(header::CONTENT_TYPE, HeaderValue::from_static(ct));
        r
    };
    let h = resp.headers_mut();
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    if let Ok(v) = HeaderValue::from_str(etag) {
        h.insert(header::ETAG, v);
    }
    resp
}
