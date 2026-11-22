//! The D-P11-15 guard (11-15): CSRF double-submit, Sec-Fetch-Site / Origin, and the
//! Host / `:authority` allowlist (DNS rebinding), exercised through the
//! `POST /ui/_probe` route (mounted by `UiConfig::test_probe`) and every GET route.

mod common;

use std::sync::Arc;

use axum::body::Body;
use common::{HOST, TOKEN, seed, send, ui};
use http::{Request, StatusCode};
use ostro_api::ControlPlane;
use ostro_api::fake::FakeControlPlane;
use ostro_ui::auth::AllowAll;
use ostro_ui::{UiConfig, UiDeps};

const OTHER: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

/// A probe POST; `None` header values are omitted.
struct Probe<'a> {
    host: Option<&'a str>,
    uri: &'a str,
    fetch_site: Option<&'a str>,
    origin: Option<&'a str>,
    cookie: Option<&'a str>,
    hx: bool,
    header_token: Option<&'a str>,
    form_token: Option<&'a str>,
}

impl Default for Probe<'_> {
    fn default() -> Self {
        Probe {
            host: Some(HOST),
            uri: "/ui/_probe",
            fetch_site: Some("same-origin"),
            origin: None,
            cookie: Some(TOKEN),
            hx: true,
            header_token: Some(TOKEN),
            form_token: None,
        }
    }
}

impl Probe<'_> {
    fn request(&self) -> Request<Body> {
        let mut b = Request::builder().method("POST").uri(self.uri);
        if let Some(h) = self.host {
            b = b.header("host", h);
        }
        if let Some(s) = self.fetch_site {
            b = b.header("sec-fetch-site", s);
        }
        if let Some(o) = self.origin {
            b = b.header("origin", o);
        }
        if let Some(c) = self.cookie {
            b = b.header("cookie", format!("theme=dark; ostro_csrf={c}"));
        }
        if self.hx {
            b = b.header("hx-request", "true");
        }
        if let Some(t) = self.header_token {
            b = b.header("x-ostro-csrf", t);
        }
        match self.form_token {
            Some(t) => b
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(format!("action=probe&csrf={t}")))
                .expect("request"),
            None => b.body(Body::empty()).expect("request"),
        }
    }
}

async fn probe(p: Probe<'_>) -> (StatusCode, String) {
    let ui = ui();
    let r = send(&ui.router, p.request()).await;
    (r.status, r.body)
}

#[tokio::test]
async fn same_origin_htmx_with_header_token_is_allowed() {
    let (st, body) = probe(Probe::default()).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body, "ok htmx");
}

#[tokio::test]
async fn same_origin_form_with_matching_field_is_allowed() {
    let (st, body) = probe(Probe {
        hx: false,
        header_token: None,
        form_token: Some(TOKEN),
        ..Probe::default()
    })
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body, "ok form");
}

#[tokio::test]
async fn origin_fallback_without_sec_fetch_site() {
    for origin in ["http://127.0.0.1:7700", "https://127.0.0.1:7700"] {
        let (st, body) = probe(Probe {
            fetch_site: None,
            origin: Some(origin),
            ..Probe::default()
        })
        .await;
        assert_eq!(st, StatusCode::OK, "{origin}: {body}");
    }
}

#[tokio::test]
async fn cross_site_and_foreign_origins_are_403() {
    for (fetch_site, origin) in [
        (Some("cross-site"), None),
        (Some("same-site"), None),
        (Some("none"), None),
        (None, Some("http://evil.example")),
        (None, Some("http://127.0.0.1:7701")),
        (None, None),
        (Some("same-origin"), Some("http://evil.example")),
    ] {
        let (st, body) = probe(Probe {
            fetch_site,
            origin,
            ..Probe::default()
        })
        .await;
        assert_eq!(
            st,
            StatusCode::FORBIDDEN,
            "{fetch_site:?} {origin:?}: {body}"
        );
    }
}

#[tokio::test]
async fn missing_or_mismatched_tokens_are_403() {
    let cases: Vec<(&str, Probe<'_>)> = vec![
        (
            "no HX header and no form token",
            Probe {
                hx: false,
                header_token: Some(TOKEN),
                ..Probe::default()
            },
        ),
        (
            "HX header without the token header",
            Probe {
                header_token: None,
                ..Probe::default()
            },
        ),
        (
            "header token mismatch",
            Probe {
                header_token: Some(OTHER),
                ..Probe::default()
            },
        ),
        (
            "form token mismatch",
            Probe {
                hx: false,
                header_token: None,
                form_token: Some(OTHER),
                ..Probe::default()
            },
        ),
        (
            "no cookie",
            Probe {
                cookie: None,
                ..Probe::default()
            },
        ),
        (
            "no cookie, form token",
            Probe {
                cookie: None,
                hx: false,
                header_token: None,
                form_token: Some(TOKEN),
                ..Probe::default()
            },
        ),
        (
            "malformed cookie equal to the header",
            Probe {
                cookie: Some("short"),
                header_token: Some("short"),
                ..Probe::default()
            },
        ),
    ];
    for (what, p) in cases {
        let (st, body) = probe(p).await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{what}: {body}");
    }
}

#[tokio::test]
async fn foreign_host_is_403_on_every_route_while_auth_is_off() {
    let ui = ui();
    let s = seed(&ui).await;
    let mut paths: Vec<String> = Vec::new();
    for pattern in ostro_ui::ROUTES {
        let path = pattern.trim_start_matches("POST ");
        paths.push(
            path.replace("{run_id}", &s.completed)
                .replace("{*path}", "app.css"),
        );
    }
    for host in [
        "evil.example",
        "evil.example:7700",
        "127.0.0.1.evil.example",
        "10.1.2.3:7701",
    ] {
        for (i, path) in paths.iter().enumerate() {
            let method = if ostro_ui::ROUTES[i].starts_with("POST ") {
                "POST"
            } else {
                "GET"
            };
            let req = Request::builder()
                .method(method)
                .uri(path)
                .header("host", host)
                .header("cookie", format!("ostro_csrf={TOKEN}"))
                .header("sec-fetch-site", "same-origin")
                .header("hx-request", "true")
                .header("x-ostro-csrf", TOKEN)
                .body(Body::empty())
                .expect("request");
            let r = send(&ui.router, req).await;
            assert_eq!(r.status, StatusCode::FORBIDDEN, "{host} {path}: {}", r.body);
            assert!(r.body.contains("host not allowed"), "{path}");
        }
    }
    // No Host and no authority at all.
    let req = Request::builder()
        .uri("/ui/runs")
        .body(Body::empty())
        .expect("request");
    assert_eq!(send(&ui.router, req).await.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn http2_authority_is_checked() {
    let ui = ui();
    // HTTP/2 carries no Host header; axum exposes `:authority` as the URI authority.
    for (uri, want) in [
        ("http://evil.example/ui/runs", StatusCode::FORBIDDEN),
        ("http://127.0.0.1:7700/ui/runs", StatusCode::OK),
        ("https://[::1]:9/ui/runs", StatusCode::OK),
    ] {
        let req = Request::builder()
            .version(http::Version::HTTP_2)
            .uri(uri)
            .body(Body::empty())
            .expect("request");
        let r = send(&ui.router, req).await;
        assert_eq!(r.status, want, "{uri}");
    }
    // A Host header and an authority that disagree: both must be allowed.
    let req = Request::builder()
        .uri("http://evil.example/ui/runs")
        .header("host", HOST)
        .body(Body::empty())
        .expect("request");
    assert_eq!(send(&ui.router, req).await.status, StatusCode::FORBIDDEN);
    // The probe over HTTP/2: same-origin against the authority.
    let req = Request::builder()
        .version(http::Version::HTTP_2)
        .method("POST")
        .uri("http://evil.example/ui/_probe")
        .header("cookie", format!("ostro_csrf={TOKEN}"))
        .header("sec-fetch-site", "same-origin")
        .header("hx-request", "true")
        .header("x-ostro-csrf", TOKEN)
        .body(Body::empty())
        .expect("request");
    assert_eq!(send(&ui.router, req).await.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn allowlisted_hosts_are_accepted() {
    let ui = ui();
    for host in [
        "127.0.0.1:7700",
        "127.0.0.1:1234",
        "localhost:7700",
        "localhost",
        "[::1]:7700",
        "10.1.2.3:7700", // the bound address
        "ops.example",   // [ui] allowed_hosts
        "ops.example:443",
    ] {
        let req = Request::builder()
            .uri("/ui/runs")
            .header("host", host)
            .body(Body::empty())
            .expect("request");
        let r = send(&ui.router, req).await;
        assert_eq!(r.status, StatusCode::OK, "{host}: {}", r.body);
        let (st, body) = probe(Probe {
            host: Some(host),
            ..Probe::default()
        })
        .await;
        assert_eq!(st, StatusCode::OK, "probe via {host}: {body}");
    }
}

#[tokio::test]
async fn disabled_ui_has_no_routes() {
    let router = ostro_ui::router(UiDeps {
        cp: Arc::new(FakeControlPlane::new()) as Arc<dyn ControlPlane>,
        ext: ostro_api::ApiExtensions::default(),
        cfg: UiConfig {
            enabled: false,
            ..common::cfg()
        },
        auth: Arc::new(AllowAll),
    });
    for path in ["/ui/", "/ui/runs", "/ui/assets/app.css", "/ui/_probe"] {
        let r = send(&router, common::get(path)).await;
        assert_eq!(r.status, StatusCode::NOT_FOUND, "{path}");
    }
}

#[tokio::test]
async fn probe_is_absent_without_the_test_flag() {
    let router = ostro_ui::router(UiDeps {
        cp: Arc::new(FakeControlPlane::new()) as Arc<dyn ControlPlane>,
        ext: ostro_api::ApiExtensions::default(),
        cfg: UiConfig {
            test_probe: false,
            ..common::cfg()
        },
        auth: Arc::new(AllowAll),
    });
    let r = send(&router, Probe::default().request()).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}
