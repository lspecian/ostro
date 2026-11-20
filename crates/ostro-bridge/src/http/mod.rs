//! The HTTP connector (06-10; FX-01, FX-02, FX-03, FX-07).
//!
//! - [`key`]: the idempotency key (persistent contract, pinned by test vectors).
//! - [`window`]: the unknown-outcome window, applied at re-drive time only.
//! - [`retry`]: in-attempt retry classification and backoff.
//! - [`target`]: the per-destination `http-target` config.
//! - [`send`]: the send path (gates, templating, attempts, manual redirects, scrubbing).
//!
//! [`HttpConnector`] implements [`Connector`] for [`EffectKind::Http`]. It never builds a
//! reqwest client: every request goes through [`Egress::client`], after [`Egress::check`]
//! on the URL and on every redirect hop. See [`send`] for the order of the send path.

pub mod key;
pub mod retry;
pub mod send;
pub mod target;
pub mod window;

use std::sync::Arc;

use async_trait::async_trait;
use ostro_types::journal::{EffectRequest, Failure, OpOutput};
use ostro_types::live::{EffectKind, LiveEffect};

use crate::connector::{Connector, EffectCtx, SendGate, Target};
use crate::egress::Egress;
use crate::kinds::UNSUPPORTED_OP;
use crate::meta_view::MetaView;
use crate::origin::Origin;
use crate::outcome::non_retryable;
use crate::secrets::SecretResolver;

pub use key::{EffectiveKey, KeySource, derive_key, effective_key};
pub use retry::{Class, Schedule, Transport};
pub use target::{HTTP_TARGET_KIND, HttpTarget, ResendMode, TargetError};
pub use window::{Decision, decide};

/// Node-level HTTP connector settings (the rest comes from `[egress]` and `http-target`).
#[derive(Clone, Debug, PartialEq)]
pub struct HttpConfig {
    /// The retry schedule of effects without a guest retry policy (`NeverLeft` failures
    /// only; see [`retry`]).
    pub default_retry: Schedule,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            default_retry: Schedule::new(None),
        }
    }
}

/// The HTTP connector.
pub struct HttpConnector {
    egress: Arc<Egress>,
    resolver: SecretResolver,
    meta: Arc<dyn MetaView>,
    cfg: HttpConfig,
}

impl std::fmt::Debug for HttpConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpConnector")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

impl HttpConnector {
    /// A connector over the shared egress layer, the secret resolver and the meta view.
    pub fn new(
        egress: Arc<Egress>,
        resolver: SecretResolver,
        meta: Arc<dyn MetaView>,
        cfg: HttpConfig,
    ) -> Self {
        Self {
            egress,
            resolver,
            meta,
            cfg,
        }
    }

    /// The effective config of destination `origin` (`scheme://host[:port]`): its
    /// `http-target` connection, or the `[egress]` defaults.
    pub fn http_target(&self, origin: &str) -> Result<HttpTarget, TargetError> {
        let conn = self
            .meta
            .connection(origin)
            .filter(|c| c.kind == HTTP_TARGET_KIND);
        HttpTarget::from_connection(conn.as_ref(), self.egress.config())
    }
}

/// The normalised origin string of `url` (trailing host dot stripped), the rate-limit key and
/// the `http-target` connection name.
pub(crate) fn origin_key(origin: &Origin) -> String {
    let mut o = origin.clone();
    if let Some(h) = o.host.strip_suffix('.') {
        o.host = h.to_owned();
    }
    o.to_string()
}

#[async_trait]
impl Connector for HttpConnector {
    fn kind(&self) -> EffectKind {
        EffectKind::Http
    }

    fn target(&self, _ctx: &EffectCtx, effect: &LiveEffect) -> Option<Target> {
        let EffectRequest::Http(req) = &effect.request else {
            return None;
        };
        let url = url::Url::parse(&req.url).ok()?;
        let key = origin_key(&Origin::from_url(&url)?);
        let quota = self.http_target(&key).ok().and_then(|t| t.quota);
        Some(Target { key, quota })
    }

    async fn execute(
        &self,
        ctx: &EffectCtx,
        effect: &LiveEffect,
        gate: &dyn SendGate,
    ) -> Result<OpOutput, Failure> {
        let EffectRequest::Http(req) = &effect.request else {
            return Err(non_retryable(
                UNSUPPORTED_OP,
                "the HTTP connector only executes http-fetch effects",
                None,
            ));
        };
        send::execute(self, ctx, effect, req, gate).await
    }
}
