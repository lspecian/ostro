//! Phase 14 audited secret resolution (14-01 stub; secret-access capture is 14-18,
//! D-P14-12/13).
//!
//! [`Audited`] wraps a resolver together with the node's [`AuditSink`]. Until 14-18 it is a
//! pure passthrough: every call delegates to the inner [`SecretResolver`] unchanged and
//! records nothing. 14-18 adds one coalesced `secret.access` event per
//! (run, module, secret, origin) and window ([`CoalesceKey::SecretAccess`]) after a
//! successful [`Audited::resolve`]; operator and reserved lookups stay unaudited.

use std::sync::Arc;

use ostro_types::audit::AuditSink;
#[cfg(doc)]
use ostro_types::audit::CoalesceKey;
use ostro_types::journal::Failure;

use super::resolver::{ResolveCtx, SecretResolver};
use super::value::SecretValue;
use crate::origin::Origin;

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
    /// [`SecretResolver::resolve`] (passthrough until 14-18).
    pub async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        name: &str,
        origin: &Origin,
        in_body: bool,
    ) -> Result<SecretValue, Failure> {
        self.inner.resolve(ctx, name, origin, in_body).await
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use ostro_types::audit::{AuditEvent, AuditPermit, AuditUnavailable, CoalesceKey};
    use secrecy::SecretBox;

    use super::*;
    use crate::meta_view::StaticMetaView;
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

    #[derive(Default)]
    struct Recording(Mutex<Vec<CoalesceKey>>);

    impl AuditSink for Recording {
        fn reserve(&self) -> Result<AuditPermit, AuditUnavailable> {
            Ok(AuditPermit::noop())
        }
        fn record_coalesced(&self, _ev: AuditEvent, key: CoalesceKey) {
            self.0.lock().unwrap().push(key);
        }
    }

    #[tokio::test]
    async fn passthrough_delegates_to_the_inner_resolver() {
        let chain = ProviderChain::new(vec![Arc::new(Fixed) as Arc<dyn SecretProvider>]);
        let resolver = SecretResolver::new(Arc::new(StaticMetaView::new()), chain);
        let sink = Arc::new(Recording::default());
        let audited = Audited::new(resolver, Arc::clone(&sink) as Arc<dyn AuditSink>);
        let v = audited.resolve_operator("acme/db-password").await.unwrap();
        assert_eq!(v.expose(), VALUE);
        // Reserved names keep the inner resolver's rules.
        assert!(audited.resolve_reserved("acme/x").await.is_err());
        assert!(
            audited
                .resolve_reserved("__ostro/presign/c")
                .await
                .unwrap()
                .is_none()
        );
        // Nothing is recorded before 14-18.
        assert!(sink.0.lock().unwrap().is_empty());
        assert_eq!(audited.inner().chain().names(), ["fixed"]);
    }
}
