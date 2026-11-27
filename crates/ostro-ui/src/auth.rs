//! The Phase 14 seam (D-P11-16): who is asking, and may they do this.
//!
//! Pages need the `read` scope; actions, bulk, rerun and DLQ need `operate`. Phase 11
//! enforces nothing: [`AllowAll`] grants every scope. Phase 14 (14-21) supplies a real
//! [`UiAuth`] (bearer -> session cookie via `POST /ui/login`, see [`crate::login`]).
//!
//! The router resolves the principal once per request (`viewer` middleware): the result
//! rides in the request extensions for the [`Reader`] / [`Operator`] extractors, and a
//! task-local lets the layout show who is signed in and hide the forms a read-only
//! principal cannot use (the server still enforces every scope).

use std::future::Future;
use std::pin::Pin;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;

/// Every scope the UI checks.
pub const SCOPES: &[&str] = &["read", "operate"];

/// The session cookie (RESERVATIONS, P14 block).
pub const SESSION_COOKIE: &str = "ostro_session";

/// The login page.
pub const LOGIN_PATH: &str = "/ui/login";

/// What [`UiAuth::login`] answers.
pub type LoginFuture<'a> = Pin<Box<dyn Future<Output = Result<UiSession, UiReject>> + Send + 'a>>;

/// A session issued by [`UiAuth::login`]: the `ostro_session` cookie value and its lifetime.
#[derive(Clone, PartialEq, Eq)]
pub struct UiSession {
    /// Cookie value (opaque to the UI).
    pub value: String,
    /// `Max-Age` of the cookie, seconds (the session's remaining lifetime).
    pub max_age_secs: u64,
}

impl std::fmt::Debug for UiSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UiSession")
            .field("max_age_secs", &self.max_age_secs)
            .finish_non_exhaustive()
    }
}

/// Resolves the principal behind a `/ui` request.
pub trait UiAuth: Send + Sync {
    /// The principal for this request, or the rejection to send.
    fn principal(&self, parts: &http::request::Parts) -> Result<UiPrincipal, UiReject>;

    /// Whether this auth signs callers in (`/ui/login`, `/ui/logout` and the session
    /// line of the layout). [`AllowAll`]: false (the login routes redirect to `/ui/`).
    fn sessions(&self) -> bool {
        false
    }

    /// Exchange a bearer token for a session. Failures answer a generic message (the
    /// reason is for the server's audit, never for the page).
    fn login<'a>(&'a self, _bearer: &'a str) -> LoginFuture<'a> {
        Box::pin(async {
            Err(UiReject {
                status: http::StatusCode::NOT_FOUND,
                message: "sign-in is not enabled".to_owned(),
            })
        })
    }

    /// Whether session cookies carry `Secure` for a request to `uri`. Default: the URI
    /// scheme is https (HTTP/2 carries it); the server overrides it from its TLS mode.
    fn secure_cookies(&self, uri: &http::Uri) -> bool {
        uri.scheme_str() == Some("https")
    }
}

/// Phase 11's default: every request is a principal with every scope.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

impl UiAuth for AllowAll {
    fn principal(&self, _parts: &http::request::Parts) -> Result<UiPrincipal, UiReject> {
        Ok(UiPrincipal {
            scopes: SCOPES.iter().map(|s| (*s).to_owned()).collect(),
            ..UiPrincipal::default()
        })
    }
}

/// An authenticated caller and the scopes it holds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UiPrincipal {
    /// Granted scopes (`read`, `operate`).
    pub scopes: Vec<String>,
    /// The token behind the session (Phase 14); `None` without sessions.
    pub token_id: Option<String>,
    /// The token's display name, when it has one.
    pub name: Option<String>,
    /// The token's tenant; `None` = system scope.
    pub tenant: Option<String>,
}

impl UiPrincipal {
    /// Ok when the principal holds `scope`; otherwise a 403 rejection.
    pub fn require(&self, scope: &str) -> Result<(), UiReject> {
        if self.has(scope) {
            Ok(())
        } else {
            Err(UiReject {
                status: http::StatusCode::FORBIDDEN,
                message: format!("missing scope `{scope}`"),
            })
        }
    }

    /// Whether the principal holds `scope`.
    pub fn has(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == scope)
    }

    /// `name (token id) · tenant t` for the layout; empty without a session.
    pub fn label(&self) -> String {
        let Some(id) = &self.token_id else {
            return String::new();
        };
        let who = match self.name.as_deref().filter(|n| !n.is_empty()) {
            Some(n) => format!("{n} ({id})"),
            None => id.clone(),
        };
        match &self.tenant {
            Some(t) => format!("{who} · tenant {t}"),
            None => format!("{who} · system"),
        }
    }
}

/// A refused `/ui` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UiReject {
    /// Response status (303 = sign in first, 401, 403).
    pub status: http::StatusCode,
    /// Human-readable reason (rendered escaped).
    pub message: String,
}

impl UiReject {
    /// No (valid) session on a page request: 303 to [`LOGIN_PATH`].
    pub fn login(message: impl Into<String>) -> Self {
        Self {
            status: http::StatusCode::SEE_OTHER,
            message: message.into(),
        }
    }

    /// No (valid) session on an htmx or mutating request: 401 (with `HX-Redirect` to the
    /// login page).
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: http::StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }

    /// [`UiReject::login`] for a plain page `GET`, [`UiReject::unauthorized`] for htmx
    /// (`HX-Request`) and non-GET requests.
    pub fn signed_out(parts: &http::request::Parts, message: impl Into<String>) -> Self {
        let htmx = parts
            .headers
            .get("hx-request")
            .is_some_and(|v| v.as_bytes() == b"true");
        if parts.method == http::Method::GET && !htmx {
            Self::login(message)
        } else {
            Self::unauthorized(message)
        }
    }
}

/// The router's per-request resolution (`viewer` middleware), read by the extractors.
#[derive(Clone)]
pub(crate) struct Resolved(pub(crate) Result<UiPrincipal, UiReject>);

tokio::task_local! {
    /// The principal of the `/ui` request being rendered (`None`: refused).
    static VIEWER: Option<UiPrincipal>;
}

/// Resolve the principal once and run the rest of the request with it as the viewer.
pub(crate) async fn viewer(
    State(state): State<crate::pages::UiState>,
    req: Request,
    next: Next,
) -> Response {
    let (mut parts, body) = req.into_parts();
    let res = state.deps.auth.principal(&parts);
    let shown = res.as_ref().ok().cloned();
    parts.extensions.insert(Resolved(res));
    VIEWER
        .scope(shown, next.run(Request::from_parts(parts, body)))
        .await
}

/// The layout's session line (`name (id) · tenant t`); empty without a session.
pub(crate) fn viewer_label() -> String {
    VIEWER
        .try_with(|v| v.as_ref().map(UiPrincipal::label).unwrap_or_default())
        .unwrap_or_default()
}

/// Whether the viewer may use the mutating forms. Outside a request (template unit
/// tests) true: nothing is hidden.
pub(crate) fn viewer_can_operate() -> bool {
    VIEWER
        .try_with(|v| v.as_ref().is_some_and(|p| p.has("operate")))
        .unwrap_or(true)
}

/// Whether the viewer is signed in with a session (the layout shows "Sign out").
pub(crate) fn viewer_signed_in() -> bool {
    VIEWER
        .try_with(|v| v.as_ref().is_some_and(|p| p.token_id.is_some()))
        .unwrap_or(false)
}

/// The caller of a `/ui` page: [`UiAuth::principal`] holding the `read` scope.
pub(crate) struct Reader(#[allow(dead_code)] pub(crate) UiPrincipal);

/// The caller of a mutating `/ui` route: [`UiAuth::principal`] holding `operate`
/// (actions, bulk, rerun, DLQ and the inventory actions).
pub(crate) struct Operator(#[allow(dead_code)] pub(crate) UiPrincipal);

fn principal_with(
    parts: &http::request::Parts,
    state: &crate::pages::UiState,
    scope: &str,
) -> Result<UiPrincipal, crate::pages::UiError> {
    let p = match parts.extensions.get::<Resolved>() {
        Some(Resolved(r)) => r.clone()?,
        None => state.deps.auth.principal(parts)?,
    };
    p.require(scope)?;
    Ok(p)
}

impl axum::extract::FromRequestParts<crate::pages::UiState> for Reader {
    type Rejection = crate::pages::UiError;

    async fn from_request_parts(
        parts: &mut http::request::Parts,
        state: &crate::pages::UiState,
    ) -> Result<Self, Self::Rejection> {
        principal_with(parts, state, "read").map(Reader)
    }
}

impl axum::extract::FromRequestParts<crate::pages::UiState> for Operator {
    type Rejection = crate::pages::UiError;

    async fn from_request_parts(
        parts: &mut http::request::Parts,
        state: &crate::pages::UiState,
    ) -> Result<Self, Self::Rejection> {
        principal_with(parts, state, "operate").map(Operator)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn require_rejects_missing_scope() {
        let p = UiPrincipal {
            scopes: vec!["read".into()],
            ..UiPrincipal::default()
        };
        assert!(p.require("read").is_ok());
        let e = p.require("operate").expect_err("no operate");
        assert_eq!(e.status, http::StatusCode::FORBIDDEN);
    }
}
