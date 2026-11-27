//! Sign-in and sign-out (Phase 14, plan 14-21, D-P14-31).
//!
//! | Method | Path | Answer |
//! |---|---|---|
//! | GET | `/ui/login` | the sign-in form (bearer field + CSRF token); 303 -> `/ui/` without sessions |
//! | POST | `/ui/login` | valid bearer: 303 -> `/ui/` with `ostro_session`; else 401 + the form, generic message |
//! | POST | `/ui/logout` | clears `ostro_session`, 303 -> `/ui/login` |
//!
//! Both POSTs are [`SameOriginMutation`]s: the Phase 11 CSRF rules (same origin + the
//! double-submit `ostro_csrf` token) apply to sign-in and sign-out like to every other
//! mutation. The session cookie is `HttpOnly; SameSite=Strict; Path=/ui`, `Secure` when
//! the server says so ([`crate::auth::UiAuth::secure_cookies`]), with `Max-Age` = the
//! session's lifetime. Its value is opaque here: the server's [`crate::auth::UiAuth`] signs
//! and verifies it.

use askama::Template;
use axum::extract::State;
use axum::response::{IntoResponse, Redirect, Response};
use http::header::{self, HeaderMap, HeaderValue};
use http::{StatusCode, Uri};

use crate::auth::SESSION_COOKIE;
use crate::pages::{UiError, UiState, page_response};
use crate::security::{self, SameOriginMutation};

/// The form field carrying the bearer token.
pub(crate) const TOKEN_FIELD: &str = "token";

/// The only message a failed sign-in shows (no reason is leaked).
pub(crate) const FAILED: &str = "Sign-in failed: the token is invalid, expired or revoked.";

#[derive(Template)]
#[template(path = "ui/login.html")]
struct LoginPage {
    title: String,
    csrf: String,
    nav: &'static str,
    error: Option<String>,
}

fn render(csrf: String, error: Option<String>) -> Result<String, UiError> {
    Ok(LoginPage {
        title: "Sign in".to_owned(),
        csrf,
        nav: "login",
        error,
    }
    .render()?)
}

fn session_cookie(value: &str, max_age: u64, secure: bool) -> Option<HeaderValue> {
    let secure = if secure { "; Secure" } else { "" };
    HeaderValue::from_str(&format!(
        "{SESSION_COOKIE}={value}; Path=/ui; HttpOnly; SameSite=Strict; Max-Age={max_age}{secure}"
    ))
    .ok()
}

/// `GET /ui/login`.
pub(crate) async fn login_page(
    State(st): State<UiState>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, UiError> {
    if !st.deps.auth.sessions() {
        return Ok(Redirect::to("/ui/").into_response());
    }
    let (csrf, set_cookie) = security::csrf_token(&headers, &uri)?;
    Ok(page_response(render(csrf, None)?, set_cookie))
}

/// `POST /ui/login`.
pub(crate) async fn login(
    State(st): State<UiState>,
    headers: HeaderMap,
    uri: Uri,
    m: SameOriginMutation,
) -> Result<Response, UiError> {
    let auth = &st.deps.auth;
    if !auth.sessions() {
        return Ok(Redirect::to("/ui/").into_response());
    }
    let bearer = m.field(TOKEN_FIELD).map(str::trim).unwrap_or_default();
    let session = if bearer.is_empty() {
        None
    } else {
        auth.login(bearer).await.ok()
    };
    let Some(session) = session else {
        // The CSRF cookie is present (SameOriginMutation checked it): re-render with it.
        let (csrf, _) = security::csrf_token(&headers, &uri)?;
        let mut resp = page_response(render(csrf, Some(FAILED.to_owned()))?, None);
        *resp.status_mut() = StatusCode::UNAUTHORIZED;
        return Ok(resp);
    };
    let mut resp = Redirect::to("/ui/").into_response();
    if let Some(c) = session_cookie(
        &session.value,
        session.max_age_secs,
        auth.secure_cookies(&uri),
    ) {
        resp.headers_mut().insert(header::SET_COOKIE, c);
    } else {
        return Err(UiError::Internal(
            "session cookie is not a header value".to_owned(),
        ));
    }
    Ok(resp)
}

/// `POST /ui/logout`.
pub(crate) async fn logout(
    State(st): State<UiState>,
    uri: Uri,
    m: SameOriginMutation,
) -> Result<Response, UiError> {
    let auth = &st.deps.auth;
    let target = if auth.sessions() {
        crate::auth::LOGIN_PATH
    } else {
        "/ui/"
    };
    let mut resp = Redirect::to(target).into_response();
    if auth.sessions()
        && let Some(c) = session_cookie("", 0, auth.secure_cookies(&uri))
    {
        resp.headers_mut().insert(header::SET_COOKIE, c);
    }
    if m.is_htmx() {
        resp.headers_mut()
            .insert("hx-redirect", HeaderValue::from_static(target));
    }
    Ok(resp)
}
