//! Phase 14: implemented in 14-21 (UI authentication: bearer → session cookie via
//! `POST /ui/login`, `TokenUiAuth`).
//!
//! The `/ui` scope layer (N2, 14-01b) lives here: in `[auth] mode = "none"` every UI handler
//! runs inside `PRINCIPAL.scope(Principal::dev(), …)` (this branch stays); in token mode the
//! principal comes from the UI auth (14-21 replaces [`token_principal`]; stub: the dev
//! principal, so the Phase 11 UI keeps working behind the guard until then).

use std::sync::{Arc, Weak};

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use ostro_api::auth::Principal;
use ostro_ui::auth::UiAuth;

use super::P14Ctx;
use crate::config::AuthMode;

/// The UI's [`UiAuth`]. Stub: `AllowAll`.
pub fn ui_auth(_ctx: &Arc<P14Ctx>) -> Arc<dyn UiAuth> {
    Arc::new(ostro_ui::auth::AllowAll)
}

/// The `/ui` principal scope (built by [`super::ui_scope_layer`]); [`UiScopeLayer::apply`]
/// installs it on the UI router.
#[derive(Clone)]
pub struct UiScopeLayer {
    inner: Arc<Inner>,
}

struct Inner {
    mode: AuthMode,
    auth: Arc<dyn UiAuth>,
    ctx: Weak<P14Ctx>,
}

impl std::fmt::Debug for UiScopeLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UiScopeLayer")
            .field("mode", &self.inner.mode)
            .finish_non_exhaustive()
    }
}

/// Build the layer for `ctx`'s `[auth] mode`.
pub fn scope_layer(ctx: &Arc<P14Ctx>, auth: Arc<dyn UiAuth>) -> UiScopeLayer {
    UiScopeLayer {
        inner: Arc::new(Inner {
            mode: ctx.cfg.auth.mode,
            auth,
            ctx: Arc::downgrade(ctx),
        }),
    }
}

impl UiScopeLayer {
    /// The layer for an explicit mode without a context (tests).
    pub fn for_mode(mode: AuthMode, auth: Arc<dyn UiAuth>) -> Self {
        Self {
            inner: Arc::new(Inner {
                mode,
                auth,
                ctx: Weak::new(),
            }),
        }
    }

    /// Run every handler of `router` inside the request's principal scope.
    pub fn apply(self, router: axum::Router) -> axum::Router {
        router.layer(axum::middleware::from_fn_with_state(self, scope_ui))
    }

    /// The `[auth] mode` this layer serves.
    pub fn mode(&self) -> AuthMode {
        self.inner.mode
    }
}

async fn scope_ui(State(layer): State<UiScopeLayer>, req: Request, next: Next) -> Response {
    let principal = match layer.inner.mode {
        AuthMode::None => Principal::dev(),
        AuthMode::Token => token_principal(
            layer.inner.ctx.upgrade().as_deref(),
            layer.inner.auth.as_ref(),
            &req,
        ),
    };
    ostro_api::auth::scope(principal, next.run(req)).await
}

/// Token mode: the principal of a `/ui` request (14-21: the session cookie's token; no
/// session → `Principal::anonymous()`, denied by the guard). Stub: the dev principal.
fn token_principal(_ctx: Option<&P14Ctx>, _auth: &dyn UiAuth, _req: &Request) -> Principal {
    Principal::dev()
}
