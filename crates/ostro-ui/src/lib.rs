//! Embedded operator UI (Phase 11, D-P11-14..16): server-rendered askama templates plus
//! vendored htmx 2.0.11, mounted by `ostro-server` under `/ui`.
//!
//! The UI calls the same [`ostro_api::ControlPlane`] and [`ostro_api::ApiExtensions`] traits
//! as REST, so Phase 14's ControlPlane-level authorization and audit cover both. The
//! [`auth::UiAuth`] seam is where Phase 14 plugs in; Phase 11 ships [`auth::AllowAll`].
//!
//! Plan 11-03 is the skeleton: [`router`] returns an empty router until 11-15 registers the
//! pages (route registration is serialised 11-15 -> 11-16 -> 11-17).

use std::net::SocketAddr;
use std::sync::Arc;

pub mod auth;

mod actions;
mod assets;
mod inventory;
mod pages;
mod security;
mod timeline;

// Declared by 11-03 for 11-15 (sha2: asset ETags; getrandom: CSRF tokens); anchored so
// cargo's unused-dependency lint stays quiet until `assets`/`security` consume them.
mod dep_anchors {
    use getrandom as _;
    use sha2 as _;
}

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

/// The `/ui` router. Empty until 11-15; empty whenever `deps.cfg.enabled` is false.
pub fn router(deps: UiDeps) -> axum::Router {
    if !deps.cfg.enabled {
        return axum::Router::new();
    }
    // 11-15 registers the pages here, 11-16 the actions, 11-17 the inventory.
    axum::Router::new()
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

    #[test]
    fn template_engine_renders_and_escapes() {
        let html = pages::render_layout_probe("<b>&</b>").expect("render");
        assert!(
            html.contains("&#60;b&#62;&#38;&#60;/b&#62;")
                || html.contains("&lt;b&gt;&amp;&lt;/b&gt;"),
            "{html}"
        );
        assert!(!html.contains("<b>"), "{html}");
        assert!(
            html.contains("/ui/assets/vendor/htmx-2.0.11.min.js"),
            "{html}"
        );
    }
}
