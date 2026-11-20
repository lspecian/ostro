//! The sealed-store provider: secret `NAME` of tenant `T` is the `SealedSecret`
//! `"<T|_>/NAME"` (06-DECISIONS), opened with the [`SealedStore`].
//!
//! The tenant is fixed per provider instance; the server builds one chain per tenant (the
//! providers are cheap `Arc`s). Reserved `__ostro/*` records are never reachable here: names
//! are validated against the template grammar, and those records have no tenant prefix.

use std::sync::Arc;

use secrecy::SecretBox;

use super::{ProviderError, SecretProvider};
use crate::meta_view::MetaView;
use crate::secrets::sealed::SealedStore;
use crate::secrets::template;

/// Reads secrets from the replicated meta store's sealed records.
#[derive(Clone)]
pub struct SealedProvider {
    meta: Arc<dyn MetaView>,
    store: SealedStore,
    tenant: Option<String>,
}

impl std::fmt::Debug for SealedProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealedProvider")
            .field("store", &self.store)
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl SealedProvider {
    /// A provider for `tenant` (`None` = the `_` namespace).
    pub fn new(meta: Arc<dyn MetaView>, store: SealedStore, tenant: Option<String>) -> Self {
        Self {
            meta,
            store,
            tenant,
        }
    }

    /// The record name of secret `name`: `"<tenant|_>/<name>"`.
    pub fn record_name(&self, name: &str) -> String {
        format!("{}/{name}", self.tenant.as_deref().unwrap_or("_"))
    }
}

#[async_trait::async_trait]
impl SecretProvider for SealedProvider {
    fn name(&self) -> &'static str {
        "sealed"
    }

    async fn get(&self, name: &str) -> Result<Option<SecretBox<[u8]>>, ProviderError> {
        template::validate_name(name).map_err(|e| ProviderError::Invalid(e.to_string()))?;
        let record_name = self.record_name(name);
        let Some(rec) = self.meta.sealed_secret(&record_name) else {
            return Ok(None);
        };
        let v = self
            .store
            .open(&rec)
            .map_err(|e| ProviderError::Invalid(format!("sealed secret {record_name:?}: {e}")))?;
        Ok(Some(v.into_box()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta_view::StaticMetaView;
    use crate::secrets::sealed::SealingKeyRing;
    use secrecy::ExposeSecret;

    #[tokio::test]
    async fn looks_up_tenant_qualified_records() {
        let store = SealedStore::new(Arc::new(SealingKeyRing::from_bytes([4; 32])));
        let meta = StaticMetaView::new()
            .insert_sealed_secret(store.seal("_/api-key", b"underscore-value").unwrap())
            .insert_sealed_secret(store.seal("acme/api-key", b"acme-tenant-value").unwrap());
        let meta: Arc<dyn MetaView> = Arc::new(meta);
        let global = SealedProvider::new(meta.clone(), store.clone(), None);
        let acme = SealedProvider::new(meta.clone(), store.clone(), Some("acme".into()));
        let other = SealedProvider::new(meta.clone(), store.clone(), Some("other".into()));
        assert_eq!(
            global
                .get("api-key")
                .await
                .unwrap()
                .unwrap()
                .expose_secret(),
            b"underscore-value"
        );
        assert_eq!(
            acme.get("api-key").await.unwrap().unwrap().expose_secret(),
            b"acme-tenant-value"
        );
        assert!(other.get("api-key").await.unwrap().is_none());
        assert!(global.get("__ostro/token-key").await.is_err());
        assert!(!format!("{acme:?}").contains("acme-tenant-value"));
    }

    #[tokio::test]
    async fn tampered_record_is_invalid() {
        let store = SealedStore::new(Arc::new(SealingKeyRing::from_bytes([4; 32])));
        let mut rec = store.seal("_/k", b"some-secret-value").unwrap();
        let last = rec.ciphertext.len() - 1;
        rec.ciphertext[last] ^= 1;
        let meta: Arc<dyn MetaView> = Arc::new(StaticMetaView::new().insert_sealed_secret(rec));
        let p = SealedProvider::new(meta, store, None);
        let e = p.get("k").await.unwrap_err();
        assert!(matches!(e, ProviderError::Invalid(_)));
        assert!(!e.to_string().contains("some-secret"));
    }
}
