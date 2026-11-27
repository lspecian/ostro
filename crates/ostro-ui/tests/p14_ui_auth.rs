//! 14-21 Task 2: sign-in, sign-out and scope gating over a fake session [`UiAuth`].
//!
//! - no session: a page `GET` is a 303 to `/ui/login`; an htmx `GET` and every mutation are
//!   401 with `HX-Redirect: /ui/login`; the extractor never reaches the control plane;
//! - `GET /ui/login` renders the form with the Phase 11 CSP and security headers, mints the
//!   CSRF cookie, and the template has no inline script/style/handlers;
//! - `POST /ui/login` and `POST /ui/logout` need the CSRF token like every mutation;
//! - a valid bearer sets `ostro_session` (HttpOnly, SameSite=Strict, Path=/ui, Max-Age,
//!   Secure only when the auth says so); a bad one is a 401 with one generic message;
//! - logout clears the cookie;
//! - the layout shows the session's principal and marks a read-only principal's page
//!   `readonly` (mutating forms hidden), and a read-only principal's actions are 403;
//! - `AllowAll` (Phase 11, `[auth] mode = "none"`): the login routes redirect to `/ui/`.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::body::Body;
use common::{HOST, TOKEN, send, ui};
use http::{Request, StatusCode};
use ostro_api::ControlPlane;
use ostro_ui::auth::{
    AllowAll, LoginFuture, SESSION_COOKIE, UiAuth, UiPrincipal, UiReject, UiSession,
};
use ostro_ui::{UiDeps, router};

const GOOD: &str = "ostro_otk_good";
const READER: &str = "ostro_otk_reader";
const CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; \
img-src 'self' data:; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; \
base-uri 'none'";

/// A session auth: `ostro_session=s-good` is an operator, `s-reader` a reader; anything
/// else is signed out. Login accepts [`GOOD`] and [`READER`].
#[derive(Default)]
struct FakeSessions {
    secure: bool,
    logins: AtomicUsize,
}

fn session_of(parts: &http::request::Parts) -> Option<String> {
    parts
        .headers
        .get_all(http::header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|kv| kv.trim().split_once('='))
        .find(|(k, _)| *k == SESSION_COOKIE)
        .map(|(_, v)| v.to_owned())
}

impl UiAuth for FakeSessions {
    fn principal(&self, parts: &http::request::Parts) -> Result<UiPrincipal, UiReject> {
        let scopes: &[&str] = match session_of(parts).as_deref() {
            Some("s-good") => &["read", "operate"],
            Some("s-reader") => &["read"],
            _ => return Err(UiReject::signed_out(parts, "sign in")),
        };
        Ok(UiPrincipal {
            scopes: scopes.iter().map(|s| (*s).to_owned()).collect(),
            token_id: Some("otk_aaaaaaaaaaaaaaaa".into()),
            name: Some("ops <b>bot</b>".into()),
            tenant: Some("acme".into()),
        })
    }

    fn sessions(&self) -> bool {
        true
    }

    fn login<'a>(&'a self, bearer: &'a str) -> LoginFuture<'a> {
        self.logins.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            match bearer {
                GOOD => Ok(UiSession {
                    value: "s-good".into(),
                    max_age_secs: 3600,
                }),
                READER => Ok(UiSession {
                    value: "s-reader".into(),
                    max_age_secs: 60,
                }),
                _ => Err(UiReject::unauthorized("expired_token")),
            }
        })
    }

    fn secure_cookies(&self, _uri: &http::Uri) -> bool {
        self.secure
    }
}

fn with_auth(ui: &common::Ui, auth: Arc<dyn UiAuth>) -> axum::Router {
    let mut ext = ostro_api::ApiExtensions::default();
    ui.ops.install(&mut ext);
    router(UiDeps {
        cp: Arc::clone(&ui.cp) as Arc<dyn ControlPlane>,
        ext,
        cfg: common::cfg(),
        auth,
    })
}

fn get(path: &str, session: Option<&str>, htmx: bool) -> Request<Body> {
    let mut b = Request::builder().uri(path).header("host", HOST);
    let mut cookie = format!("ostro_csrf={TOKEN}");
    if let Some(s) = session {
        cookie.push_str(&format!("; {SESSION_COOKIE}={s}"));
    }
    b = b.header("cookie", cookie);
    if htmx {
        b = b.header("hx-request", "true");
    }
    b.body(Body::empty()).expect("request")
}

/// A same-origin form POST; `csrf` = the form token (None: omitted).
fn post(path: &str, session: Option<&str>, csrf: Option<&str>, fields: &str) -> Request<Body> {
    let mut cookie = format!("ostro_csrf={TOKEN}");
    if let Some(s) = session {
        cookie.push_str(&format!("; {SESSION_COOKIE}={s}"));
    }
    let mut body = fields.to_owned();
    if let Some(c) = csrf {
        if !body.is_empty() {
            body.push('&');
        }
        body.push_str(&format!("csrf={c}"));
    }
    Request::builder()
        .method("POST")
        .uri(path)
        .header("host", HOST)
        .header("cookie", cookie)
        .header("sec-fetch-site", "same-origin")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from(body))
        .expect("request")
}

fn assert_security_headers(r: &common::Resp, what: &str) {
    assert_eq!(r.header("content-security-policy"), Some(CSP), "{what}");
    assert_eq!(
        r.header("x-content-type-options"),
        Some("nosniff"),
        "{what}"
    );
    assert_eq!(r.header("x-frame-options"), Some("DENY"), "{what}");
    assert_eq!(r.header("referrer-policy"), Some("no-referrer"), "{what}");
}

#[tokio::test]
async fn signed_out_pages_redirect_and_htmx_or_actions_get_401() {
    let ui = ui();
    let s = common::seed(&ui).await;
    let r = with_auth(&ui, Arc::new(FakeSessions::default()));
    for path in [
        "/ui/".to_owned(),
        "/ui/runs".to_owned(),
        format!("/ui/runs/{}", s.completed),
        "/ui/dlq".to_owned(),
        "/ui/bulk".to_owned(),
        "/ui/modules".to_owned(),
        "/ui/partitions".to_owned(),
    ] {
        let resp = send(&r, get(&path, None, false)).await;
        assert_eq!(resp.status, StatusCode::SEE_OTHER, "{path}: {}", resp.body);
        assert_eq!(resp.header("location"), Some("/ui/login"), "{path}");
        assert_security_headers(&resp, &path);
        // A forged session is no session.
        let resp = send(&r, get(&path, Some("forged"), false)).await;
        assert_eq!(resp.status, StatusCode::SEE_OTHER, "{path} forged");
        // htmx: 401 + HX-Redirect.
        let resp = send(&r, get(&path, None, true)).await;
        assert_eq!(resp.status, StatusCode::UNAUTHORIZED, "{path} htmx");
        assert_eq!(resp.header("hx-redirect"), Some("/ui/login"), "{path}");
    }
    // Mutations: 401 before any trait call; the run stays as it was.
    let before = ui.cp.get_run(s.running.clone()).await.expect("run").status;
    for path in [
        format!("/ui/runs/{}/cancel", s.running),
        format!("/ui/runs/{}/terminate", s.running),
        "/ui/workflows/greet/pause".to_owned(),
    ] {
        let resp = send(&r, post(&path, None, Some(TOKEN), "")).await;
        assert_eq!(
            resp.status,
            StatusCode::UNAUTHORIZED,
            "{path}: {}",
            resp.body
        );
        assert_eq!(resp.header("hx-redirect"), Some("/ui/login"), "{path}");
    }
    let after = ui.cp.get_run(s.running.clone()).await.expect("run").status;
    assert_eq!(before, after);
    // Assets stay public (the login page needs its stylesheet and scripts).
    let resp = send(&r, get("/ui/assets/app.css", None, false)).await;
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn login_page_follows_the_phase_11_rules() {
    let ui = ui();
    let r = with_auth(&ui, Arc::new(FakeSessions::default()));
    // No CSRF cookie yet: the page mints one.
    let req = Request::builder()
        .uri("/ui/login")
        .header("host", HOST)
        .body(Body::empty())
        .expect("request");
    let resp = send(&r, req).await;
    assert_eq!(resp.status, StatusCode::OK, "{}", resp.body);
    assert_security_headers(&resp, "/ui/login");
    assert_eq!(resp.header("cache-control"), Some("no-store"));
    let set = resp.header("set-cookie").expect("csrf cookie");
    assert!(set.starts_with("ostro_csrf="), "{set}");
    let token = set
        .trim_start_matches("ostro_csrf=")
        .split(';')
        .next()
        .expect("token");
    assert!(
        resp.body
            .contains(&format!(r#"name="csrf" value="{token}""#)),
        "{}",
        resp.body
    );
    assert!(resp.body.contains(r#"action="/ui/login""#));
    assert!(resp.body.contains(r#"type="password" name="token""#));
    // Signed out: no main navigation, no session line.
    assert!(!resp.body.contains(r#"href="/ui/dlq""#));
    assert!(!resp.body.contains("/ui/logout"));
    // The template itself: no inline script, style or handler.
    let tpl = include_str!("../templates/ui/login.html").to_ascii_lowercase();
    for bad in [
        "<script",
        "style=",
        "onclick=",
        "onsubmit=",
        "hx-on",
        "javascript:",
    ] {
        assert!(!tpl.contains(bad), "login.html contains {bad}");
    }
    let rendered = resp.body.to_ascii_lowercase();
    assert!(!rendered.contains("<script>"), "inline script rendered");
    assert!(!rendered.contains("style="), "inline style rendered");
}

#[tokio::test]
async fn login_and_logout_need_the_csrf_token() {
    let ui = ui();
    let auth = Arc::new(FakeSessions::default());
    let r = with_auth(&ui, Arc::clone(&auth) as Arc<dyn UiAuth>);
    let field = format!("token={GOOD}");
    for (what, req) in [
        (
            "login, no form token",
            post("/ui/login", None, None, &field),
        ),
        (
            "login, wrong form token",
            post("/ui/login", None, Some(&"f".repeat(64)), &field),
        ),
        (
            "logout, no form token",
            post("/ui/logout", Some("s-good"), None, ""),
        ),
    ] {
        let resp = send(&r, req).await;
        assert_eq!(resp.status, StatusCode::FORBIDDEN, "{what}: {}", resp.body);
        assert!(resp.header("set-cookie").is_none(), "{what}");
    }
    // Cross-origin with a valid token: still refused.
    let mut req = post("/ui/login", None, Some(TOKEN), &field);
    req.headers_mut()
        .insert("sec-fetch-site", "cross-site".parse().expect("hv"));
    let resp = send(&r, req).await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN);
    assert_eq!(
        auth.logins.load(Ordering::SeqCst),
        0,
        "login never attempted"
    );
}

#[tokio::test]
async fn login_sets_the_session_cookie_and_logout_clears_it() {
    let ui = ui();
    common::seed(&ui).await;
    for secure in [false, true] {
        let r = with_auth(
            &ui,
            Arc::new(FakeSessions {
                secure,
                ..FakeSessions::default()
            }),
        );
        let resp = send(
            &r,
            post("/ui/login", None, Some(TOKEN), &format!("token={GOOD}")),
        )
        .await;
        assert_eq!(resp.status, StatusCode::SEE_OTHER, "{}", resp.body);
        assert_eq!(resp.header("location"), Some("/ui/"));
        let c = resp.header("set-cookie").expect("session cookie");
        assert!(c.starts_with("ostro_session=s-good;"), "{c}");
        for attr in ["Path=/ui", "HttpOnly", "SameSite=Strict", "Max-Age=3600"] {
            assert!(c.contains(attr), "{c} lacks {attr}");
        }
        assert_eq!(c.contains("Secure"), secure, "{c}");

        // Bad bearer (and an empty one): 401, one generic message, no cookie.
        for field in ["token=ostro_otk_nope", "token=", ""] {
            let resp = send(&r, post("/ui/login", None, Some(TOKEN), field)).await;
            assert_eq!(resp.status, StatusCode::UNAUTHORIZED, "{field}");
            assert!(resp.header("set-cookie").is_none(), "{field}");
            assert!(resp.body.contains("Sign-in failed"), "{}", resp.body);
            assert!(!resp.body.contains("expired_token"), "reason leaked");
            assert_security_headers(&resp, field);
        }

        // Logout clears the cookie and lands on the login page.
        let resp = send(&r, post("/ui/logout", Some("s-good"), Some(TOKEN), "")).await;
        assert_eq!(resp.status, StatusCode::SEE_OTHER);
        assert_eq!(resp.header("location"), Some("/ui/login"));
        let c = resp.header("set-cookie").expect("clearing cookie");
        assert!(c.starts_with("ostro_session=;"), "{c}");
        assert!(c.contains("Max-Age=0") && c.contains("Path=/ui"), "{c}");
        assert_eq!(c.contains("Secure"), secure, "{c}");
    }
}

#[tokio::test]
async fn layout_shows_the_principal_and_hides_forms_from_readers() {
    let ui = ui();
    let s = common::seed(&ui).await;
    let r = with_auth(&ui, Arc::new(FakeSessions::default()));
    let path = format!("/ui/runs/{}", s.running);
    // Operator: session line, sign-out form, no readonly marker.
    let resp = send(&r, get(&path, Some("s-good"), false)).await;
    assert_eq!(resp.status, StatusCode::OK, "{}", resp.body);
    assert!(
        resp.body
            .contains("ops &#60;b&#62;bot&#60;/b&#62; (otk_aaaaaaaaaaaaaaaa) · tenant acme")
            || resp
                .body
                .contains("ops &lt;b&gt;bot&lt;/b&gt; (otk_aaaaaaaaaaaaaaaa) · tenant acme"),
        "{}",
        resp.body
    );
    assert!(resp.body.contains(r#"action="/ui/logout""#));
    assert!(!resp.body.contains(r#"class="readonly""#));
    // Reader: the page renders, marked readonly (CSS hides the mutating forms; the
    // sign-out form is kept), and an action is 403.
    let resp = send(&r, get(&path, Some("s-reader"), false)).await;
    assert_eq!(resp.status, StatusCode::OK);
    assert!(resp.body.contains(r#"<body hx-history="false""#));
    assert!(resp.body.contains(r#" class="readonly">"#), "{}", resp.body);
    assert!(resp.body.contains(r#"class="logout ui-keep""#));
    let before = ui.cp.get_run(s.running.clone()).await.expect("run").status;
    let resp = send(
        &r,
        post(
            &format!("/ui/runs/{}/cancel", s.running),
            Some("s-reader"),
            Some(TOKEN),
            "",
        ),
    )
    .await;
    assert_eq!(resp.status, StatusCode::FORBIDDEN, "{}", resp.body);
    assert!(resp.body.contains("missing scope"), "{}", resp.body);
    let after = ui.cp.get_run(s.running.clone()).await.expect("run").status;
    assert_eq!(before, after, "the run is unchanged");
    // The stylesheet carries the readonly rule.
    let css = include_str!("../assets/app.css");
    assert!(css.contains(r#"body.readonly form[method="post"]:not(.ui-keep)"#));
}

#[tokio::test]
async fn allow_all_keeps_phase_11_behaviour() {
    let ui = ui();
    common::seed(&ui).await;
    let r = with_auth(&ui, Arc::new(AllowAll));
    let resp = send(&r, get("/ui/runs", None, false)).await;
    assert_eq!(resp.status, StatusCode::OK);
    assert!(!resp.body.contains("/ui/logout"), "no session line");
    assert!(!resp.body.contains("readonly"));
    let resp = send(&r, get("/ui/login", None, false)).await;
    assert_eq!(resp.status, StatusCode::SEE_OTHER);
    assert_eq!(resp.header("location"), Some("/ui/"));
    let resp = send(
        &r,
        post("/ui/login", None, Some(TOKEN), &format!("token={GOOD}")),
    )
    .await;
    assert_eq!(resp.status, StatusCode::SEE_OTHER);
    assert!(resp.header("set-cookie").is_none());
    let resp = send(&r, post("/ui/logout", None, Some(TOKEN), "")).await;
    assert_eq!(resp.status, StatusCode::SEE_OTHER);
    assert_eq!(resp.header("location"), Some("/ui/"));
    assert!(resp.header("set-cookie").is_none());
}
