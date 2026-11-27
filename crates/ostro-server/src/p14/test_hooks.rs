//! Phase 14 test-only fault knobs (14-10, D-P14-19/20/34): the skewable clock and the
//! runtime failpoint endpoint. **Everything here exists only in `failpoints` builds**
//! (`target/failpoints/ostro`); a default build wraps nothing and both paths answer a plain
//! REST 404 there.
//!
//! - [`wrap_clock`]: failpoints builds return an `ostro_partition::clock::SkewClock` over
//!   the seam clock (with no skew set it returns the inner value unchanged); the handle is
//!   found again through `Clock::skew` on [`P14Ctx::clock`].
//! - [`mount`] adds (failpoints builds only), next to `/v1/admin/replication`:
//!   - `GET /v1/admin/test/clock`: the current skew and both clocks;
//!   - `POST /v1/admin/test/clock` `{reset?, offset_ms?, strobe_ms?, strobe_period_ms?,
//!     mono_rate?}`: `reset` first, then every field present (`mono_rate` is a fraction,
//!     `0.05` = 5% fast, bounded to ±0.5); answers the new state;
//!   - `POST /v1/admin/test/failpoints` `{name, action}`: `fail::cfg(name, action)` (e.g.
//!     `sleep(200)`, `return`, `1%return`); `action = "off"` removes the failpoint;
//!     answers `{name, action, active}` (the configured failpoints).
//!
//!   Both require a system admin (`tenant = None` with the `admin` scope) through the
//!   server's own authenticator ([`super::authenticator`]): `[auth] mode = "none"` admits
//!   the dev principal, token mode answers 401 without a valid bearer and 403
//!   (`missing_scope`) for any other principal.
//!
//! The skew moves the node's injected clock only (leader timers, cron, dispatcher leases,
//! timestamps): OS timers (tokio sleeps, TCP keepalive) and the D-P13-15 read lease (an
//! `Instant` anchor, 14-01) are not skewed.

use std::sync::Arc;

use ostro_partition::Clock;

use super::P14Ctx;

/// Wrap the seam clock: a `SkewClock` in failpoints builds, the identity otherwise.
pub fn wrap_clock(clock: Arc<dyn Clock>) -> Arc<dyn Clock> {
    #[cfg(feature = "failpoints")]
    {
        // An already skewable clock is kept (one skew layer per server).
        if clock.skew().is_some() {
            return clock;
        }
        Arc::new(ostro_partition::clock::SkewClock::new(clock))
    }
    #[cfg(not(feature = "failpoints"))]
    {
        clock
    }
}

/// The two test endpoint paths.
pub const PATHS: [&str; 2] = ["/v1/admin/test/clock", "/v1/admin/test/failpoints"];

/// Mount the test endpoints on `app` (failpoints builds). A default build has no knob: both
/// paths answer a REST 404 (`not_found`) for every method, instead of falling through to the
/// gRPC router (whose unknown-service answer is HTTP 200 with `grpc-status: 12`).
pub fn mount(app: axum::Router, ctx: &Arc<P14Ctx>) -> axum::Router {
    #[cfg(feature = "failpoints")]
    {
        app.merge(endpoints::router(ctx))
    }
    #[cfg(not(feature = "failpoints"))]
    {
        use axum::response::IntoResponse as _;
        let _ = ctx;
        let absent = || async {
            ostro_api::ApiError::not_found("test endpoints exist only in failpoints builds")
                .into_response()
        };
        PATHS
            .iter()
            .fold(app, |app, p| app.route(p, axum::routing::any(absent)))
    }
}

#[cfg(feature = "failpoints")]
mod endpoints {
    use std::sync::{Arc, Weak};

    use axum::Json;
    use axum::extract::State;
    use axum::http::HeaderMap;
    use axum::response::{IntoResponse, Response};
    use axum::routing::{get, post};
    use ostro_api::ApiError;
    use ostro_api::auth::{Authenticator, Principal, Scope};
    use ostro_partition::clock::{MAX_RATE_PPM, SkewClock, SkewSettings, SkewSnapshot};
    use serde::Deserialize;
    use serde_json::{Value, json};
    use tracing::warn;

    use super::P14Ctx;

    #[derive(Clone)]
    struct Hooks {
        ctx: Weak<P14Ctx>,
        auth: Arc<dyn Authenticator>,
    }

    pub(super) fn router(ctx: &Arc<P14Ctx>) -> axum::Router {
        let st = Hooks {
            ctx: Arc::downgrade(ctx),
            auth: super::super::authenticator(ctx),
        };
        axum::Router::new()
            .route(super::PATHS[0], get(get_clock).post(post_clock))
            .route(super::PATHS[1], post(post_failpoint))
            .with_state(st)
    }

    /// The caller must be a system admin (module docs).
    async fn admit(h: &Hooks, headers: &HeaderMap) -> Result<Principal, ApiError> {
        let bearer = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| {
                v.strip_prefix("Bearer ")
                    .or_else(|| v.strip_prefix("bearer "))
            })
            .map(str::trim);
        let p = h
            .auth
            .authenticate(bearer)
            .await
            .map_err(|e| e.to_api_error())?;
        if p.is_anonymous() {
            return Err(ApiError::unauthenticated("missing bearer token"));
        }
        if !(p.is_system_scope() && p.scopes.allows(Scope::Admin)) {
            return Err(ApiError::missing_scope("admin.test", "admin"));
        }
        Ok(p)
    }

    fn skew_of(h: &Hooks) -> Result<SkewClock, ApiError> {
        let ctx = h
            .ctx
            .upgrade()
            .ok_or_else(|| ApiError::unavailable("the server is shutting down"))?;
        ctx.clock.skew().ok_or_else(|| {
            ApiError::new(
                ostro_api::ErrorCode::PreconditionFailed,
                "the node clock is not skewable (dev time skipping wraps it)",
            )
        })
    }

    fn snapshot_json(s: &SkewSnapshot) -> Value {
        json!({
            "offset_ms": s.offset_ms,
            "strobe_ms": s.strobe_ms,
            "strobe_period_ms": s.strobe_period_ms,
            "mono_rate": s.mono_rate_ppm as f64 / 1e6,
            "mono_rate_ppm": s.mono_rate_ppm,
            "wall_ms": s.wall_ms,
            "inner_wall_ms": s.inner_wall_ms,
            "mono_ns": s.mono_ns,
            "inner_mono_ns": s.inner_mono_ns,
        })
    }

    async fn get_clock(State(h): State<Hooks>, headers: HeaderMap) -> Response {
        let res = async {
            admit(&h, &headers).await?;
            Ok::<_, ApiError>(Json(snapshot_json(&skew_of(&h)?.snapshot())))
        }
        .await;
        res.into_response()
    }

    /// `POST /v1/admin/test/clock` body.
    #[derive(Debug, Default, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ClockBody {
        #[serde(default)]
        reset: bool,
        offset_ms: Option<i64>,
        strobe_ms: Option<u64>,
        strobe_period_ms: Option<u64>,
        mono_rate: Option<f64>,
    }

    fn settings(b: &ClockBody) -> Result<SkewSettings, ApiError> {
        let max = MAX_RATE_PPM as f64 / 1e6;
        let mono_rate_ppm = match b.mono_rate {
            None => None,
            Some(r) if r.is_finite() && r.abs() <= max => Some((r * 1e6).round() as i64),
            Some(r) => {
                return Err(ApiError::invalid_argument(format!(
                    "mono_rate {r} out of range (allowed ±{max})"
                )));
            }
        };
        Ok(SkewSettings {
            reset: b.reset,
            offset_ms: b.offset_ms,
            strobe_ms: b.strobe_ms,
            strobe_period_ms: b.strobe_period_ms,
            mono_rate_ppm,
        })
    }

    async fn post_clock(State(h): State<Hooks>, headers: HeaderMap, body: String) -> Response {
        let res = async {
            let p = admit(&h, &headers).await?;
            let b: ClockBody = if body.trim().is_empty() {
                ClockBody::default()
            } else {
                serde_json::from_str(&body)
                    .map_err(|e| ApiError::invalid_argument(format!("clock body: {e}")))?
            };
            let s = settings(&b)?;
            let snap = skew_of(&h)?.apply(&s).map_err(ApiError::invalid_argument)?;
            warn!(principal = %p.token_id, ?s, "test hook: clock skew changed");
            Ok::<_, ApiError>(Json(snapshot_json(&snap)))
        }
        .await;
        res.into_response()
    }

    /// `POST /v1/admin/test/failpoints` body.
    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct FailpointBody {
        name: String,
        action: String,
    }

    async fn post_failpoint(State(h): State<Hooks>, headers: HeaderMap, body: String) -> Response {
        let res = async {
            let p = admit(&h, &headers).await?;
            let b: FailpointBody = serde_json::from_str(&body)
                .map_err(|e| ApiError::invalid_argument(format!("failpoint body: {e}")))?;
            let name = b.name.trim();
            if name.is_empty() {
                return Err(ApiError::invalid_argument("failpoint name is empty"));
            }
            let action = b.action.trim();
            if action == "off" {
                fail::remove(name);
            } else {
                fail::cfg(name, action).map_err(|e| {
                    ApiError::invalid_argument(format!("failpoint {name}={action}: {e}"))
                })?;
            }
            warn!(principal = %p.token_id, name, action, "test hook: failpoint changed");
            let active: Vec<Value> = fail::list()
                .into_iter()
                .map(|(n, a)| json!({ "name": n, "action": a }))
                .collect();
            Ok(Json(
                json!({ "name": name, "action": action, "active": active }),
            ))
        }
        .await;
        res.into_response()
    }
}
