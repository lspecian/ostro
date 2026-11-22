//! D-P11-15 hardening (11-15): CSP and security headers on every `/ui` route, the
//! template lint, the htmx config, and the embedded assets (types, ETags, 304).

mod common;

use std::path::{Path, PathBuf};

use axum::body::Body;
use common::{HOST, TOKEN, get, get_ok, seed, seed_inventory, send, ui, ui_bare};
use http::{Request, StatusCode};
use sha2::{Digest, Sha256};

const CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; \
connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'";

fn assert_security_headers(r: &common::Resp, what: &str) {
    assert_eq!(r.header("content-security-policy"), Some(CSP), "{what}");
    assert_eq!(
        r.header("x-content-type-options"),
        Some("nosniff"),
        "{what}"
    );
    assert_eq!(r.header("referrer-policy"), Some("no-referrer"), "{what}");
    assert_eq!(r.header("x-frame-options"), Some("DENY"), "{what}");
}

/// A concrete request path for every route pattern of [`ostro_ui::ROUTES`].
fn concrete(pattern: &str, run: &str, job: &str) -> Option<(&'static str, String)> {
    let (method, path) = match pattern.split_once(' ') {
        Some((m, p)) => (m, p),
        None => ("GET", pattern),
    };
    let method = match method {
        "GET" => "GET",
        "POST" => "POST",
        other => panic!("unexpected method {other}"),
    };
    let path = path
        .replace("{run_id}", run)
        .replace("{job_id}", job)
        // 11-17 inventory (seed_inventory names)
        .replace("{w}", "greet")
        .replace("{q}", "default")
        .replace("{name}", "nightly")
        .replace("{id}", "wrk-1")
        .replace("{*path}", "vendor/htmx-2.0.11.min.js");
    assert!(!path.contains('{'), "unmapped parameter in {pattern}");
    Some((method, path))
}

#[tokio::test]
async fn every_route_carries_csp_and_security_headers() {
    let ui = ui();
    let s = seed(&ui).await;
    seed_inventory(&ui).await;
    assert!(ostro_ui::ROUTES.len() >= 10);
    let job = bulk_job(&ui).await;
    for pattern in ostro_ui::ROUTES {
        let (method, path) = concrete(pattern, &s.completed, &job).expect("route");
        let req = Request::builder()
            .method(method)
            .uri(&path)
            .header("host", HOST)
            .header("cookie", format!("ostro_csrf={TOKEN}"))
            .header("sec-fetch-site", "same-origin")
            .header("hx-request", "true")
            .header("x-ostro-csrf", TOKEN)
            .body(Body::empty())
            .expect("request");
        let r = send(&ui.router, req).await;
        assert!(
            r.status.is_success() || r.status.is_redirection(),
            "{pattern} -> {}: {}",
            r.status,
            r.body
        );
        assert_security_headers(&r, pattern);
        let cache = r.header("cache-control");
        if path.starts_with("/ui/assets/") {
            assert_eq!(cache, Some("no-cache"), "{pattern}");
        } else {
            assert_eq!(cache, Some("no-store"), "{pattern}");
        }
    }
    // Errors too: an unknown run, an unknown asset, a refused host.
    for (path, host, status) in [
        ("/ui/runs/run-999999", HOST, StatusCode::NOT_FOUND),
        ("/ui/assets/nope.js", HOST, StatusCode::NOT_FOUND),
        ("/ui/runs", "evil.example", StatusCode::FORBIDDEN),
    ] {
        let req = Request::builder()
            .uri(path)
            .header("host", host)
            .body(Body::empty())
            .expect("request");
        let r = send(&ui.router, req).await;
        assert_eq!(r.status, status, "{path}");
        assert_security_headers(&r, path);
        assert_eq!(r.header("cache-control"), Some("no-store"), "{path}");
    }
}

/// A bulk job on the fake (11-16's `/ui/bulk/{job_id}` routes).
async fn bulk_job(ui: &common::Ui) -> String {
    use ostro_api::phase11::{BulkAction, BulkCreateRequest, OpsApi};
    ui.ops
        .create_bulk(BulkCreateRequest {
            action: BulkAction::Cancel,
            filter: Default::default(),
            rate_per_sec: None,
            max_in_flight: None,
            dry_run: false,
        })
        .await
        .expect("bulk job")
        .job_id
        .expect("job id")
}

fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn files_under(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).expect("read_dir") {
        let p = e.expect("entry").path();
        if p.is_dir() {
            files_under(&p, out);
        } else {
            out.push(p);
        }
    }
}

/// `<script` tags without `src=`, `style=`, ` on<word>=`, `hx-on`, `javascript:`.
fn lint(text: &str) -> Vec<String> {
    let lower = text.to_ascii_lowercase();
    let mut found = Vec::new();
    for (i, _) in lower.match_indices("<script") {
        let end = lower[i..].find('>').map_or(lower.len(), |e| i + e);
        if !lower[i..end].contains("src=") {
            found.push(format!("inline <script> at byte {i}"));
        }
    }
    for needle in ["style=", "hx-on", "javascript:"] {
        if lower.contains(needle) {
            found.push(needle.to_owned());
        }
    }
    let b = lower.as_bytes();
    for (i, _) in lower.match_indices(" on") {
        let rest = &b[i + 3..];
        let n = rest.iter().take_while(|c| c.is_ascii_lowercase()).count();
        if n > 0 && rest.get(n) == Some(&b'=') {
            found.push(format!("event handler attribute at byte {i}"));
        }
    }
    found
}

/// [`lint`] over the raw markup only (every `<...>` tag): escaped user text in a
/// rendered page has no raw `<`, so it cannot form a tag.
fn lint_tags(html: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut rest = html;
    while let Some(start) = rest.find('<') {
        let end = rest[start..]
            .find('>')
            .map_or(rest.len(), |e| start + e + 1);
        let tag = &rest[start..end];
        // A script element's own tag is checked by `lint`; its (empty) body follows.
        found.extend(lint(tag).into_iter().map(|f| format!("{f} in {tag}")));
        rest = &rest[end..];
    }
    found
}

#[test]
fn templates_have_no_inline_script_style_or_handlers() {
    let mut files = Vec::new();
    files_under(&crate_dir().join("templates"), &mut files);
    assert!(files.len() >= 8, "{files:?}");
    for f in &files {
        let text = std::fs::read_to_string(f).expect("template");
        assert_eq!(lint(&text), Vec::<String>::new(), "{}", f.display());
    }
    // The lint itself catches each banned form.
    for bad in [
        "<script>x()</script>",
        "<p style=\"color:red\">",
        "<a onclick=\"x()\">",
        "<div hx-on:click=\"x\">",
        "<a href=\"javascript:x\">",
    ] {
        assert!(!lint(bad).is_empty(), "{bad}");
    }
    let js = std::fs::read_to_string(crate_dir().join("assets/app.js")).expect("app.js");
    assert!(!js.contains("eval("), "app.js uses eval");
    assert!(!js.contains("new Function"), "app.js uses new Function");
}

#[tokio::test]
async fn rendered_pages_pass_the_lint_and_carry_the_htmx_config() {
    let ui = ui();
    let s = seed(&ui).await;
    seed_inventory(&ui).await;
    for path in [
        "/ui/".to_owned(),
        "/ui/dlq".to_owned(),
        format!("/ui/runs/{}", s.completed),
        format!("/ui/runs/{}?tab=logs", s.running),
        // 11-16
        format!("/ui/runs/{}", s.running),
        format!("/ui/runs/{}/rerun?from_seq=2", s.completed),
        "/ui/bulk?workflow=greet".to_owned(),
        format!("/ui/bulk/{}", bulk_job(&ui).await),
        // 11-17
        "/ui/modules".to_owned(),
        "/ui/workflows".to_owned(),
        "/ui/queues".to_owned(),
        "/ui/schedules".to_owned(),
        "/ui/workers".to_owned(),
        "/ui/partitions".to_owned(),
    ] {
        let r = get_ok(&ui, &path).await;
        assert_eq!(lint_tags(&r.body), Vec::<String>::new(), "{path}");
        assert!(
            r.body.contains(
                r#"<meta name="htmx-config" content='{"allowEval":false,"allowScriptTags":false,"includeIndicatorStyles":false,"historyCacheSize":0,"selfRequestsOnly":true}'>"#
            ),
            "{path}"
        );
        assert!(r.body.contains(r#"hx-history="false""#), "{path}");
        assert!(
            r.body
                .contains(&format!(r#"hx-headers='{{"X-Ostro-Csrf":"{TOKEN}"}}'"#)),
            "{path}"
        );
        assert!(
            r.body
                .contains(r#"<script src="/ui/assets/vendor/htmx-2.0.11.min.js" defer></script>"#)
        );
        assert_eq!(
            r.header("content-type"),
            Some("text/html; charset=utf-8"),
            "{path}"
        );
    }
}

#[tokio::test]
async fn pages_mint_the_csrf_cookie_when_absent() {
    let ui = ui();
    let req = Request::builder()
        .uri("/ui/runs")
        .header("host", HOST)
        .body(Body::empty())
        .expect("request");
    let r = send(&ui.router, req).await;
    assert_eq!(r.status, StatusCode::OK);
    let cookie = r.header("set-cookie").expect("set-cookie").to_owned();
    let token = cookie
        .strip_prefix("ostro_csrf=")
        .and_then(|c| c.split(';').next())
        .expect("token");
    assert_eq!(token.len(), 64);
    assert!(
        cookie.ends_with("; Path=/ui; HttpOnly; SameSite=Strict"),
        "{cookie}"
    );
    assert!(r.body.contains(token), "the page embeds the minted token");
    // With the cookie present, nothing is minted.
    let r = send(&ui.router, get("/ui/runs")).await;
    assert!(r.header("set-cookie").is_none());
}

fn sha256_etag(bytes: &[u8]) -> String {
    let hex: String = Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("\"sha256-{hex}\"")
}

#[tokio::test]
async fn assets_are_embedded_with_types_etags_and_304() {
    let ui = ui();
    for (name, ct) in [
        ("app.css", "text/css; charset=utf-8"),
        ("app.js", "text/javascript; charset=utf-8"),
        (
            "vendor/htmx-2.0.11.min.js",
            "text/javascript; charset=utf-8",
        ),
    ] {
        let path = format!("/ui/assets/{name}");
        let disk = std::fs::read(crate_dir().join("assets").join(name)).expect("asset");
        let r = get_ok(&ui, &path).await;
        assert_eq!(r.header("content-type"), Some(ct), "{name}");
        assert_eq!(r.header("cache-control"), Some("no-cache"), "{name}");
        let etag = sha256_etag(&disk);
        assert_eq!(r.header("etag"), Some(etag.as_str()), "{name}");
        assert_eq!(
            r.body.as_bytes(),
            disk.as_slice(),
            "{name} served byte-exact"
        );

        for inm in [etag.clone(), format!("\"x\", {etag}"), "*".to_owned()] {
            let req = Request::builder()
                .uri(&path)
                .header("host", HOST)
                .header("if-none-match", inm.as_str())
                .body(Body::empty())
                .expect("request");
            let r = send(&ui.router, req).await;
            assert_eq!(r.status, StatusCode::NOT_MODIFIED, "{name} {inm}");
            assert!(r.body.is_empty());
            assert_eq!(r.header("etag"), Some(etag.as_str()));
            assert_eq!(r.header("cache-control"), Some("no-cache"));
        }
        let req = Request::builder()
            .uri(&path)
            .header("host", HOST)
            .header("if-none-match", "\"sha256-stale\"")
            .body(Body::empty())
            .expect("request");
        assert_eq!(send(&ui.router, req).await.status, StatusCode::OK);
    }
}

#[tokio::test]
async fn ui_root_redirects_to_the_run_list() {
    let ui = ui();
    let r = send(&ui.router, get("/ui")).await;
    assert_eq!(r.status, StatusCode::PERMANENT_REDIRECT);
    assert_eq!(r.header("location"), Some("/ui/"));
}

#[tokio::test]
async fn inventory_fragments_and_unavailable_views_pass_the_lint() {
    let ui = ui();
    seed(&ui).await;
    seed_inventory(&ui).await;
    let r = get_ok(&ui, "/ui/partitions/fragment").await;
    assert_eq!(lint_tags(&r.body), Vec::<String>::new());
    let bare = ui_bare();
    for path in [
        "/ui/modules",
        "/ui/workflows",
        "/ui/queues",
        "/ui/schedules",
        "/ui/workers",
        "/ui/partitions",
        "/ui/partitions/fragment",
    ] {
        let r = get_ok(&bare, path).await;
        assert_eq!(lint_tags(&r.body), Vec::<String>::new(), "{path}");
        assert_security_headers(&r, path);
    }
}
