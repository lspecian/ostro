//! Embedded operator UI (Phase 11, D-P11-14..16): server-rendered askama templates plus
//! vendored htmx 2.0.11, mounted by `ostro-server` under `/ui`.
//!
//! The UI calls the same [`ostro_api::ControlPlane`] and [`ostro_api::ApiExtensions`] traits
//! as REST, so Phase 14's ControlPlane-level authorization and audit cover both. The
//! [`auth::UiAuth`] seam is where Phase 14 plugs in; Phase 11 ships [`auth::AllowAll`].
//!
//! Plan 11-15 registers the run pages, the assets and the security layer (route
//! registration is serialised 11-15 -> 11-16 -> 11-17; later plans append their routes
//! in their own functions and to [`ROUTES`]).
//!
//! # Routes (11-15)
//!
//! | Method | Path | Answer |
//! |---|---|---|
//! | GET | `/ui` | 308 -> `/ui/` |
//! | GET | `/ui/`, `/ui/runs` | run list (filters, tags, order, pagination) |
//! | GET | `/ui/runs/{run_id}` | run detail (`?tab=timeline` default, `?tab=logs`) |
//! | GET | `/ui/runs/{run_id}/status` | status card fragment (polls every 2 s while non-terminal) |
//! | GET | `/ui/runs/{run_id}/timeline` | timeline fragment |
//! | GET | `/ui/runs/{run_id}/logs` | logs fragment |
//! | GET | `/ui/dlq` | DLQ tab |
//! | GET | `/ui/assets/{*path}` | embedded assets (ETag, 304) |
//! | POST | `/ui/_probe` | guard probe, only with `UiConfig::test_probe` |
//!
//! Every route sits behind [`security::guard`] (Host / `:authority` allowlist, security
//! headers, D-P11-15).

use std::net::SocketAddr;
use std::sync::Arc;

pub mod auth;

mod actions;
mod assets;
mod inventory;
mod pages;
mod security;
mod timeline;

use axum::routing::{get, post};

/// Every `/ui` route pattern 11-15 registers (GET unless noted), for the hardening
/// tests; 11-16/11-17 append theirs.
#[doc(hidden)]
pub const ROUTES: &[&str] = &[
    "/ui",
    "/ui/",
    "/ui/runs",
    "/ui/runs/{run_id}",
    "/ui/runs/{run_id}/status",
    "/ui/runs/{run_id}/timeline",
    "/ui/runs/{run_id}/logs",
    "/ui/dlq",
    "/ui/assets/{*path}",
    "POST /ui/_probe",
];

/// `[ui]` configuration as the router sees it.
#[derive(Debug, Clone, Default)]
pub struct UiConfig {
    /// Serve `/ui` at all. When false, [`router`] has no routes.
    pub enabled: bool,
    /// Extra `Host` values accepted besides the bound address and loopback names
    /// (`[ui] allowed_hosts`, D-P11-15).
    pub allowed_hosts: Vec<String>,
    /// The address the server bound, so its `host:port` is in the Host allowlist.
    pub bound_addr: Option<SocketAddr>,
    /// Mounts the guard probe route for integration tests (11-15). Always false in
    /// production configuration.
    #[doc(hidden)]
    pub test_probe: bool,
}

/// Everything the UI handlers need.
#[derive(Clone)]
pub struct UiDeps {
    /// The control plane (same trait object REST uses).
    pub cp: Arc<dyn ostro_api::ControlPlane>,
    /// Phase 8 extension slots (and the Phase 11 `ops`/`inventory` slots added by 11-04).
    pub ext: ostro_api::ApiExtensions,
    /// UI configuration.
    pub cfg: UiConfig,
    /// Who is asking (Phase 14 seam; [`auth::AllowAll`] in Phase 11).
    pub auth: Arc<dyn auth::UiAuth>,
}

/// The `/ui` router; empty whenever `deps.cfg.enabled` is false.
pub fn router(deps: UiDeps) -> axum::Router {
    if !deps.cfg.enabled {
        return axum::Router::new();
    }
    let probe = deps.cfg.test_probe;
    let state = pages::UiState::new(deps);
    let mut r = page_routes().merge(asset_routes());
    if probe {
        r = r.route("/ui/_probe", post(security::probe));
    }
    // 11-16 merges actions::routes() here, 11-17 inventory::routes().
    r.layer(axum::middleware::from_fn_with_state(
        state.clone(),
        security::guard,
    ))
    .with_state(state)
}

/// The run pages and fragments (11-15).
fn page_routes() -> axum::Router<pages::UiState> {
    axum::Router::new()
        .route("/ui", get(pages::ui_root))
        .route("/ui/", get(pages::runs))
        .route("/ui/runs", get(pages::runs))
        .route("/ui/runs/{run_id}", get(pages::run_detail))
        .route("/ui/runs/{run_id}/status", get(pages::run_status))
        .route("/ui/runs/{run_id}/timeline", get(pages::run_timeline))
        .route("/ui/runs/{run_id}/logs", get(pages::run_logs))
        .route("/ui/dlq", get(pages::dlq))
}

/// The embedded assets (11-15).
fn asset_routes() -> axum::Router<pages::UiState> {
    axum::Router::new().route("/ui/assets/{*path}", get(assets::serve))
}

#[cfg(test)]
mod tests {
    use super::*;
    use auth::{AllowAll, UiAuth};
    use tower::ServiceExt;

    fn deps(enabled: bool) -> UiDeps {
        UiDeps {
            cp: Arc::new(ostro_api::fake::FakeControlPlane::new()),
            ext: ostro_api::ApiExtensions::default(),
            cfg: UiConfig {
                enabled,
                ..UiConfig::default()
            },
            auth: Arc::new(AllowAll),
        }
    }

    #[tokio::test]
    async fn disabled_router_has_no_routes() {
        for path in ["/ui", "/ui/", "/ui/runs", "/"] {
            let req = http::Request::builder()
                .uri(path)
                .body(axum::body::Body::empty())
                .expect("request");
            let resp = router(deps(false)).oneshot(req).await.expect("infallible");
            assert_eq!(resp.status(), http::StatusCode::NOT_FOUND, "{path}");
        }
    }

    #[test]
    fn allow_all_grants_every_scope() {
        let (parts, ()) = http::Request::builder()
            .uri("/ui")
            .body(())
            .expect("request")
            .into_parts();
        let p = AllowAll.principal(&parts).expect("AllowAll never rejects");
        p.require("read").expect("read");
        p.require("operate").expect("operate");
    }

    #[tokio::test]
    async fn template_engine_renders_and_escapes() {
        let req = http::Request::builder()
            .uri("/ui/runs/%3Cb%3E%26")
            .header("host", "localhost")
            .body(axum::body::Body::empty())
            .expect("request");
        let resp = router(deps(true)).oneshot(req).await.expect("infallible");
        assert_eq!(resp.status(), http::StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .expect("body");
        let html = String::from_utf8(body.to_vec()).expect("utf8");
        assert!(
            html.contains("&#60;b&#62;&#38;") || html.contains("&lt;b&gt;&amp;"),
            "{html}"
        );
        assert!(!html.contains("<b>"), "{html}");
        assert!(
            html.contains("/ui/assets/vendor/htmx-2.0.11.min.js"),
            "{html}"
        );
    }
}
