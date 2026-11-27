//! Phase 14 audited secret resolution (14-18, D-P14-12/13: secret-access capture).
//!
//! Every guest-template resolution (`{{secret:name}}` in an effect) is reported to the
//! node's [`AuditSink`] as a `secret.access` event through
//! [`AuditSink::record_coalesced`], keyed by [`CoalesceKey::SecretAccess`]
//! (run, module, secret, origin): the journal writes **one** record per key and
//! `[audit] secret_coalesce` window, with a `count`. The event names the secret
//! (`<tenant|_>/<name>`), the module, the origin, the run and the outcome; never a value.
//!
//! - granted and fetched: outcome `ok`;
//! - refused by the grant check (`secret_not_granted`): outcome `denied`;
//! - not found / provider failure / too short: outcome `failed` (the failure kind).
//!
//! Denied and failed resolutions are coalesced separately from successes (the outcome
//! class is part of the coalescing key's origin field), so a success summary never hides a
//! refusal.
//!
//! Two ways in: [`Audited`] wraps a resolver together with a sink (the bridge services'
//! default resolver), and [`SecretResolver::with_audit`] puts the sink inside the resolver
//! itself, which is how the connectors (they hold a concrete [`SecretResolver`]) get
//! audited. Both call [`record_access`]. Operator (connection config) and reserved
//! (`__ostro/presign/*`) lookups stay unaudited per access.

use std::sync::Arc;

use ostro_types::audit::{ActorKind, AuditEvent, AuditOutcome, AuditSink, CoalesceKey};
use ostro_types::journal::Failure;

use super::resolver::{ResolveCtx, SecretResolver};
use super::value::SecretValue;
use crate::kinds::SECRET_NOT_GRANTED;
use crate::origin::Origin;

/// The audit action of a secret resolution.
pub const SECRET_ACCESS: &str = "secret.access";

/// A resolver whose secret accesses go to an audit sink.
pub struct Audited<R> {
    inner: R,
    sink: Arc<dyn AuditSink>,
}

impl<R: std::fmt::Debug> std::fmt::Debug for Audited<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Audited")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<R> Audited<R> {
    /// Wrap `inner`; accesses are reported to `sink`.
    pub fn new(inner: R, sink: Arc<dyn AuditSink>) -> Self {
        Audited { inner, sink }
    }

    /// The wrapped resolver.
    pub fn inner(&self) -> &R {
        &self.inner
    }

    /// The audit sink.
    pub fn sink(&self) -> &Arc<dyn AuditSink> {
        &self.sink
    }

    /// Unwrap.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl Audited<SecretResolver> {
    /// [`SecretResolver::resolve`], recorded once to this wrapper's sink (the inner
    /// resolver's own sink, if any, is not used, so nothing is recorded twice).
    pub async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        name: &str,
        origin: &Origin,
        in_body: bool,
    ) -> Result<SecretValue, Failure> {
        let res = self
            .inner
            .resolve_unaudited(ctx, name, origin, in_body)
            .await;
        record_access(self.sink.as_ref(), ctx, name, origin, &res);
        res
    }

    /// [`SecretResolver::resolve_operator`] (never audited per access).
    pub async fn resolve_operator(&self, name: &str) -> Result<SecretValue, Failure> {
        self.inner.resolve_operator(name).await
    }

    /// [`SecretResolver::resolve_reserved`] (never audited per access).
    pub async fn resolve_reserved(&self, name: &str) -> Result<Option<SecretValue>, Failure> {
        self.inner.resolve_reserved(name).await
    }
}

/// The recorded outcome of a resolution result.
fn outcome_of(res: &Result<SecretValue, Failure>) -> AuditOutcome {
    match res {
        Ok(_) => AuditOutcome::Ok,
        Err(Failure::NonRetryable(i) | Failure::Retryable(i)) if i.kind == SECRET_NOT_GRANTED => {
            AuditOutcome::Denied(i.kind.clone())
        }
        Err(Failure::NonRetryable(i) | Failure::Retryable(i)) => {
            AuditOutcome::Failed(i.kind.clone())
        }
        Err(Failure::Timeout) => AuditOutcome::Failed("timeout".into()),
        Err(Failure::Cancelled) => AuditOutcome::Failed("cancelled".into()),
    }
}

/// The short outcome name (`ok`, `denied`, `failed`).
fn outcome_name(o: &AuditOutcome) -> &'static str {
    match o {
        AuditOutcome::Ok => "ok",
        AuditOutcome::Denied(_) => "denied",
        AuditOutcome::Failed(_) => "failed",
    }
}

/// Record one resolution of `name` for `ctx` towards `origin` (coalesced per run, module,
/// secret, origin and outcome class). Only names, ids and the outcome are recorded; the
/// value is never looked at.
pub fn record_access(
    sink: &dyn AuditSink,
    ctx: &ResolveCtx<'_>,
    name: &str,
    origin: &Origin,
    res: &Result<SecretValue, Failure>,
) {
    let outcome = outcome_of(res);
    let class = outcome_name(&outcome);
    let secret = format!("{}/{name}", ctx.tenant.unwrap_or("_"));
    let origin = origin.to_string();
    let mut ev = AuditEvent::new(
        SECRET_ACCESS,
        format!("run:{}", ctx.run_id),
        ActorKind::System,
        secret.clone(),
    );
    ev.tenant = ctx.tenant.map(str::to_owned);
    ev.outcome = outcome;
    ev.details.insert("module".into(), ctx.module.to_owned());
    ev.details.insert("origin".into(), origin.clone());
    ev.details.insert("outcome".into(), class.to_owned());
    ev.details.insert("run_id".into(), ctx.run_id.to_owned());
    let key_origin = if class == "ok" {
        origin
    } else {
        format!("{origin} {class}")
    };
    sink.record_coalesced(
        ev,
        CoalesceKey::SecretAccess {
            run: ctx.run_id.to_owned(),
            module: ctx.module.to_owned(),
            secret,
            origin: key_origin,
        },
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use ostro_types::audit::{AuditPermit, AuditUnavailable};
    use secrecy::SecretBox;
    use url::Url;

    use super::*;
    use crate::meta_view::StaticMetaView;
    use crate::origin::OriginPattern;
    use crate::secrets::grants::GrantSpec;
    use crate::secrets::providers::chain::ProviderChain;
    use crate::secrets::providers::{ProviderError, SecretProvider};

    const VALUE: &[u8] = b"CANARY-audited-5b1e-value";

    struct Fixed;

    #[async_trait::async_trait]
    impl SecretProvider for Fixed {
        fn name(&self) -> &'static str {
            "fixed"
        }
        async fn get(&self, _: &str) -> Result<Option<SecretBox<[u8]>>, ProviderError> {
            Ok(Some(SecretBox::new(VALUE.into())))
        }
    }

    type Log = Vec<(AuditEvent, CoalesceKey)>;

    #[derive(Default)]
    struct Recording(Mutex<Log>);

    impl AuditSink for Recording {
        fn reserve(&self) -> Result<AuditPermit, AuditUnavailable> {
            Ok(AuditPermit::noop())
        }
        fn record_coalesced(&self, ev: AuditEvent, key: CoalesceKey) {
            self.0.lock().unwrap().push((ev, key));
        }
    }

    impl Recording {
        fn log(&self) -> Log {
            self.0.lock().unwrap().clone()
        }
    }

    fn ctx() -> ResolveCtx<'static> {
        ResolveCtx {
            module: "acme/billing",
            tenant: Some("acme"),
            run_id: "run-1",
            attempt: 1,
        }
    }

    fn origin(s: &str) -> Origin {
        Origin::from_url(&Url::parse(s).unwrap()).unwrap()
    }

    fn resolver() -> SecretResolver {
        let grant = GrantSpec {
            module: "acme/billing".into(),
            secret: "k".into(),
            origins: vec![OriginPattern::parse("https://api.stripe.com").unwrap()],
            body: false,
        }
        .to_record();
        let chain = ProviderChain::new(vec![Arc::new(Fixed) as Arc<dyn SecretProvider>]);
        SecretResolver::new(Arc::new(StaticMetaView::new().insert_grant(grant)), chain)
    }

    fn no_value(log: &Log) {
        let needle = String::from_utf8_lossy(VALUE).into_owned();
        for (ev, key) in log {
            let text = format!("{ev:?} {key:?}");
            assert!(!text.contains(&needle), "value leaked: {text}");
        }
    }

    #[tokio::test]
    async fn operator_and_reserved_lookups_are_not_audited() {
        let sink = Arc::new(Recording::default());
        let audited = Audited::new(resolver(), Arc::clone(&sink) as Arc<dyn AuditSink>);
        let v = audited.resolve_operator("acme/db-password").await.unwrap();
        assert_eq!(v.expose(), VALUE);
        assert!(audited.resolve_reserved("acme/x").await.is_err());
        assert!(
            audited
                .resolve_reserved("__ostro/presign/c")
                .await
                .unwrap()
                .is_none()
        );
        assert!(sink.log().is_empty());
        assert_eq!(audited.inner().chain().names(), ["fixed"]);
    }

    #[tokio::test]
    async fn granted_and_refused_resolutions_are_recorded_by_name() {
        let sink = Arc::new(Recording::default());
        let audited = Audited::new(resolver(), Arc::clone(&sink) as Arc<dyn AuditSink>);
        let stripe = origin("https://api.stripe.com");
        let v = audited.resolve(&ctx(), "k", &stripe, false).await.unwrap();
        assert_eq!(v.expose(), VALUE);
        // Another origin: refused by the grant check, recorded as denied.
        let evil = origin("https://evil.test");
        assert!(audited.resolve(&ctx(), "k", &evil, false).await.is_err());
        let log = sink.log();
        assert_eq!(log.len(), 2);
        let (ok, ok_key) = &log[0];
        assert_eq!(ok.action, SECRET_ACCESS);
        assert_eq!(ok.actor, "run:run-1");
        assert_eq!(ok.actor_kind, ActorKind::System);
        assert_eq!(ok.tenant.as_deref(), Some("acme"));
        assert_eq!(ok.target, "acme/k");
        assert_eq!(ok.outcome, AuditOutcome::Ok);
        assert_eq!(ok.details["module"], "acme/billing");
        assert_eq!(ok.details["origin"], "https://api.stripe.com");
        assert_eq!(
            ok_key,
            &CoalesceKey::SecretAccess {
                run: "run-1".into(),
                module: "acme/billing".into(),
                secret: "acme/k".into(),
                origin: "https://api.stripe.com".into(),
            }
        );
        let (denied, denied_key) = &log[1];
        assert_eq!(
            denied.outcome,
            AuditOutcome::Denied(SECRET_NOT_GRANTED.into())
        );
        assert_ne!(denied_key, ok_key);
        no_value(&log);
    }

    #[tokio::test]
    async fn a_resolver_with_audit_records_once_and_audited_does_not_double() {
        let sink = Arc::new(Recording::default());
        let inner = resolver().with_audit(Arc::clone(&sink) as Arc<dyn AuditSink>);
        let stripe = origin("https://api.stripe.com");
        inner.resolve(&ctx(), "k", &stripe, false).await.unwrap();
        assert_eq!(sink.log().len(), 1);
        // Wrapped again: the wrapper's sink records, the inner sink is bypassed.
        let audited = Audited::new(inner, Arc::clone(&sink) as Arc<dyn AuditSink>);
        audited.resolve(&ctx(), "k", &stripe, false).await.unwrap();
        assert_eq!(sink.log().len(), 2);
        // An ungranted name through the inner hook: recorded, not as a success.
        let r = audited
            .inner()
            .resolve(&ctx(), "other", &stripe, false)
            .await;
        assert!(r.is_err());
        let log = sink.log();
        assert_eq!(log.len(), 3);
        assert_ne!(log[2].0.outcome, AuditOutcome::Ok);
        no_value(&log);
    }
}
