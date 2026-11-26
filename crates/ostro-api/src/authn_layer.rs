//! Phase 14 bearer authentication on the client listener (14-02; D-P14-6, revision B2/B3,
//! W4, W12).
//!
//! [`middleware`] runs around every REST and gRPC route of [`crate::router_with`] (one axum
//! layer covers both: tonic's services are mounted on the same axum router, so the layer
//! wraps the whole service future and the [`PRINCIPAL`] task-local covers the entire call,
//! which a tonic interceptor alone could not do). For each request it:
//!
//! 1. skips requests carrying [`TrustedNode`]: forwarded calls on the cluster listener are
//!    the Phase 13 node API's (their principal ingest is 14-15's), never this layer's;
//! 2. classifies the path with [`classify`] against [`UNAUTHENTICATED`]: those routes never
//!    call the authenticator; webhooks and the join routes run inside an explicit
//!    [`Principal::system`] scope, health / metrics / UI with **no** scope
//!    ([`crate::auth::current`] is `Anonymous` unless `UiAuth` sets one, 14-21);
//! 3. otherwise reads `Authorization: Bearer <token>` (scheme case-insensitive; the gRPC
//!    `authorization` metadata is the same HTTP/2 header), calls
//!    [`Authenticator::authenticate`] (`None` when the header is absent) and runs the inner
//!    service inside `PRINCIPAL.scope(principal, …)`. A failure answers before any handler
//!    (so before any `ControlPlane` call): REST 401 `{"error": {"code": "unauthenticated",
//!    "details": {"reason": "unauthenticated"}}}` with `WWW-Authenticate: Bearer
//!    realm="ostro"`, gRPC `UNAUTHENTICATED`; an unreachable token store answers 503 /
//!    `UNAVAILABLE`.
//!
//! With [`crate::auth::AllowDev`] (`[auth] mode = "none"`, the [`crate::ApiConfig`] default)
//! every authenticated route runs as [`Principal::dev`].
//!
//! Task-locals do not cross `tokio::spawn`, and a response body is polled after the handler
//! returned (outside the scope). Producers that call the control plane from a spawned task
//! or a body stream use [`spawn_scoped`] / [`scoped`], which carry the caller's principal.

use std::future::Future;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::auth::{self, AuthError, Authenticator, PRINCIPAL, Principal};
use crate::error::ApiError;
use crate::request_meta::TrustedNode;

/// `WWW-Authenticate` value of a REST 401 (RFC 6750).
pub const WWW_AUTHENTICATE_VALUE: &str = "Bearer realm=\"ostro\"";

/// How a route is authenticated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RouteAuth {
    /// Bearer authentication; the handler runs as the authenticated principal.
    Authenticate,
    /// No authentication; the handler runs inside an explicit [`Principal::system`] scope
    /// (the route authenticates the caller itself: webhook signatures, join tokens,
    /// presign signatures).
    System,
    /// No authentication and no scope: [`crate::auth::current`] is `Anonymous` (health,
    /// metrics, the UI tree whose `UiAuth` sets its own scope).
    Anonymous,
}

/// One unauthenticated route of [`UNAUTHENTICATED`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Exempt {
    /// HTTP method (`None` = any).
    pub method: Option<&'static str>,
    /// Exact path, or a prefix ending in `/*` that matches every path below it (any
    /// depth; the bare prefix itself does not match).
    pub pattern: &'static str,
    /// [`RouteAuth::System`] or [`RouteAuth::Anonymous`].
    pub auth: RouteAuth,
}

const fn exempt(method: Option<&'static str>, pattern: &'static str, auth: RouteAuth) -> Exempt {
    Exempt {
        method,
        pattern,
        auth,
    }
}

/// Every route that is **not** bearer-authenticated (REST paths and gRPC full method
/// paths; D-P14-6). Everything else, including the demand endpoint, `/openapi.json` and
/// `/v1/admin/*`, authenticates.
pub const UNAUTHENTICATED: &[Exempt] = &[
    // Probes and metrics (Phase 8 exposure rules unchanged).
    exempt(Some("GET"), "/healthz", RouteAuth::Anonymous),
    exempt(Some("GET"), "/readyz", RouteAuth::Anonymous),
    exempt(Some("GET"), "/metrics", RouteAuth::Anonymous),
    // The shell page / redirect to the UI (no control-plane call; the UI's entry point).
    exempt(Some("GET"), "/", RouteAuth::Anonymous),
    // The UI tree: `UiAuth` (14-21) authenticates it and sets the principal (B3).
    exempt(None, "/ui", RouteAuth::Anonymous),
    exempt(None, "/ui/*", RouteAuth::Anonymous),
    // Signature-verified webhooks (`/hooks/{name}` and `/hooks/{tenant}/{name}`).
    exempt(Some("POST"), "/hooks/*", RouteAuth::System),
    // Join routes: the body's join token is the credential.
    exempt(Some("POST"), "/v1/workers/join", RouteAuth::System),
    exempt(Some("POST"), "/v1/cluster/join", RouteAuth::System),
    // Presigned object URLs (Phase 6): the query signature is the authorisation.
    exempt(None, "/v1/objects/*", RouteAuth::System),
    // gRPC equivalents.
    exempt(
        Some("POST"),
        "/grpc.health.v1.Health/*",
        RouteAuth::Anonymous,
    ),
    exempt(
        Some("POST"),
        "/ostro.health.v1.Health/*",
        RouteAuth::Anonymous,
    ),
    exempt(
        Some("POST"),
        "/ostro.api.v1.Cluster/Join",
        RouteAuth::System,
    ),
    exempt(
        Some("POST"),
        "/ostro.api.v1.Workers/JoinWorker",
        RouteAuth::System,
    ),
];

fn matches(pattern: &str, path: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => path.len() > prefix.len() && path.starts_with(prefix),
        None => path == pattern,
    }
}

/// How `method path` is authenticated (first match of [`UNAUTHENTICATED`], else
/// [`RouteAuth::Authenticate`]).
pub fn classify(method: &Method, path: &str) -> RouteAuth {
    UNAUTHENTICATED
        .iter()
        .find(|e| e.method.is_none_or(|m| m == method.as_str()) && matches(e.pattern, path))
        .map_or(RouteAuth::Authenticate, |e| e.auth)
}

fn is_grpc(h: &HeaderMap) -> bool {
    h.get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/grpc"))
}

/// The bearer token of `Authorization: Bearer <token>` (`Ok(None)` when absent). A present
/// header that is not one well-formed bearer credential is [`AuthError::Malformed`].
pub fn bearer(h: &HeaderMap) -> Result<Option<String>, AuthError> {
    let mut values = h.get_all(header::AUTHORIZATION).iter();
    let Some(v) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(AuthError::Malformed);
    }
    let v = v.to_str().map_err(|_| AuthError::Malformed)?;
    let (scheme, token) = v.trim().split_once(' ').ok_or(AuthError::Malformed)?;
    let token = token.trim();
    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() || token.contains(' ') {
        return Err(AuthError::Malformed);
    }
    Ok(Some(token.to_owned()))
}

/// The rejection of a failed authentication (REST JSON or gRPC status).
fn reject(e: &AuthError, grpc: bool) -> Response {
    let err: ApiError = e.to_api_error();
    if grpc {
        return crate::grpc::to_status(err).into_http();
    }
    let unauthenticated = err.code == crate::error::ErrorCode::Unauthenticated;
    let mut res = err.into_response();
    if unauthenticated {
        res.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static(WWW_AUTHENTICATE_VALUE),
        );
    }
    res
}

/// The authentication middleware (see the module docs). State: the authenticator
/// (`ApiConfig.authenticator`).
pub async fn middleware(
    State(authn): State<Arc<dyn Authenticator>>,
    req: Request,
    next: Next,
) -> Response {
    if req.extensions().get::<TrustedNode>().is_some() {
        return next.run(req).await;
    }
    match classify(req.method(), req.uri().path()) {
        RouteAuth::Anonymous => next.run(req).await,
        RouteAuth::System => PRINCIPAL.scope(Principal::system(), next.run(req)).await,
        RouteAuth::Authenticate => {
            let grpc = is_grpc(req.headers());
            let token = match bearer(req.headers()) {
                Ok(t) => t,
                Err(e) => return reject(&e, grpc),
            };
            match authn.authenticate(token.as_deref()).await {
                Ok(p) => PRINCIPAL.scope(p, next.run(req)).await,
                Err(e) => reject(&e, grpc),
            }
        }
    }
}

/// Wrap `router` (REST and gRPC routes) in the authentication [`middleware`].
pub fn apply(router: axum::Router, authn: Arc<dyn Authenticator>) -> axum::Router {
    router.layer(axum::middleware::from_fn_with_state(authn, middleware))
}

/// `fut` inside the current principal's scope, for producers polled outside the request
/// future (response body streams, server-streaming gRPC).
pub fn scoped<F: Future>(fut: F) -> impl Future<Output = F::Output> {
    PRINCIPAL.scope(auth::current(), fut)
}

/// `tokio::spawn` carrying the current principal into the task (task-locals do not cross
/// `spawn`): streaming and long-poll producers use it so control-plane calls made by the
/// spawned task run as the caller, not as `Anonymous`.
pub fn spawn_scoped<F>(fut: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(scoped(fut))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(v: &str) -> HeaderMap {
        let mut m = HeaderMap::new();
        m.insert(header::AUTHORIZATION, HeaderValue::from_str(v).unwrap());
        m
    }

    #[test]
    fn bearer_parsing() {
        assert_eq!(bearer(&HeaderMap::new()), Ok(None));
        assert_eq!(bearer(&h("Bearer abc")), Ok(Some("abc".into())));
        assert_eq!(bearer(&h("bearer abc")), Ok(Some("abc".into())));
        assert_eq!(bearer(&h("BEARER   abc ")), Ok(Some("abc".into())));
        for bad in [
            "Basic abc",
            "Bearer",
            "Bearer ",
            "abc",
            "Bearer a b",
            "Token abc",
        ] {
            assert_eq!(bearer(&h(bad)), Err(AuthError::Malformed), "{bad:?}");
        }
        let mut two = h("Bearer a");
        two.append(header::AUTHORIZATION, HeaderValue::from_static("Bearer b"));
        assert_eq!(bearer(&two), Err(AuthError::Malformed));
    }

    #[test]
    fn classification() {
        use RouteAuth::*;
        let get = Method::GET;
        let post = Method::POST;
        for (m, p, want) in [
            (&get, "/healthz", Anonymous),
            (&get, "/readyz", Anonymous),
            (&get, "/metrics", Anonymous),
            (&get, "/", Anonymous),
            (&get, "/ui", Anonymous),
            (&get, "/ui/runs/r1", Anonymous),
            (&post, "/ui/login", Anonymous),
            (&post, "/hooks/gh", System),
            (&post, "/hooks/acme/gh", System),
            (&get, "/hooks/gh", Authenticate),
            (&post, "/hooks", Authenticate),
            (&post, "/v1/workers/join", System),
            (&post, "/v1/cluster/join", System),
            (&get, "/v1/objects/c/k", System),
            (&post, "/ostro.health.v1.Health/Check", Anonymous),
            (&post, "/grpc.health.v1.Health/Watch", Anonymous),
            (&post, "/ostro.api.v1.Cluster/Join", System),
            (&post, "/ostro.api.v1.Workers/JoinWorker", System),
            (&post, "/ostro.api.v1.Cluster/Init", Authenticate),
            (&get, "/v1/queues/q/demand", Authenticate),
            (&get, "/v1/admin/backup", Authenticate),
            (&get, "/openapi.json", Authenticate),
            (&get, "/healthz/x", Authenticate),
            (&get, "/uix", Authenticate),
            (&post, "/v1/cluster/joinx", Authenticate),
        ] {
            assert_eq!(classify(m, p), want, "{m} {p}");
        }
    }

    #[tokio::test]
    async fn spawn_scoped_carries_the_principal() {
        let p = Principal::token("otk_x", Some("acme".into()), auth::ScopeSet::EMPTY);
        let (plain, scoped_p) = auth::scope(p.clone(), async {
            let plain = tokio::spawn(async { auth::current() }).await.unwrap();
            let scoped_p = spawn_scoped(async { auth::current() }).await.unwrap();
            (plain, scoped_p)
        })
        .await;
        assert!(plain.is_anonymous(), "task-locals do not cross spawn");
        assert_eq!(scoped_p, p);
    }
}
