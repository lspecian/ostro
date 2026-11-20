//! [`SecretResolver`]: the only way the bridge turns a secret name into a value.
//!
//! # When resolution happens
//!
//! Per request, inside the bridge task, **after** the pre-send gate and the URL gate and
//! immediately before the request is built (docs/03 §5.3). The scope is one (run, attempt):
//! [`ResolveCtx`] carries `run_id` and `attempt` so Phase 7's `ResolveSecret` slots in
//! without an API change. Nothing here caches values across requests.
//!
//! # Order (guest templates, [`SecretResolver::resolve`])
//!
//! 1. [`template::validate_name`]: a bad or reserved (`__ostro/`) name is
//!    `secret_not_granted`;
//! 2. [`check_grant`] over `MetaView::grants_for(module)`: no grant, another origin, or a body
//!    placeholder without the `body` scope is `secret_not_granted`;
//! 3. the provider chain: not found anywhere is `secret_not_found`;
//! 4. the 8-byte minimum ([`SecretValue::for_templating`]).
//!
//! An ungranted secret is never fetched from any provider.
//!
//! Provider failures: `Unavailable` (no provider had the secret and at least one was down)
//! is `Retryable{kind: secret_not_found}`; `Invalid` (tampered record, file outside its
//! directory) is `NonRetryable{kind: secret_not_found}`. Messages name the secret and the
//! provider, never a value.

use std::sync::Arc;

use ostro_types::journal::{ErrorInfo, Failure};

use super::grants::check_grant;
use super::providers::ProviderError;
use super::providers::chain::ProviderChain;
use super::sealed::{SealedStore, open_record};
use super::template::{self, RESERVED_PREFIX};
use super::value::SecretValue;
use crate::kinds::{SECRET_NOT_FOUND, SECRET_NOT_GRANTED};
use crate::meta_view::MetaView;
use crate::origin::Origin;

/// Reserved names [`SecretResolver::resolve_reserved`] serves (06-12 presign keys).
pub const PRESIGN_PREFIX: &str = "__ostro/presign/";

/// Who is asking: one attempt of one run of one module.
#[derive(Clone, Copy, Debug)]
pub struct ResolveCtx<'a> {
    /// Tenant-qualified deployed module name (`<tenant>/<name>`).
    pub module: &'a str,
    /// Tenant of the run.
    pub tenant: Option<&'a str>,
    /// Run id.
    pub run_id: &'a str,
    /// Connector-level attempt (1-based).
    pub attempt: u32,
}

/// Grant-checked secret resolution over the provider chain.
#[derive(Clone)]
pub struct SecretResolver {
    meta: Arc<dyn MetaView>,
    chain: ProviderChain,
    sealed: Option<SealedStore>,
}

impl std::fmt::Debug for SecretResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretResolver")
            .field("chain", &self.chain)
            .field("sealed", &self.sealed.is_some())
            .finish_non_exhaustive()
    }
}

fn failure(retryable: bool, kind: &str, message: String) -> Failure {
    let info = ErrorInfo {
        kind: kind.to_owned(),
        message,
        details: None,
    };
    if retryable {
        Failure::Retryable(info)
    } else {
        Failure::NonRetryable(info)
    }
}

fn provider_failure(name: &str, e: ProviderError) -> Failure {
    match e {
        ProviderError::Unavailable(m) => failure(
            true,
            SECRET_NOT_FOUND,
            format!("secret {name:?}: provider unavailable: {m}"),
        ),
        ProviderError::Invalid(m) => failure(
            false,
            SECRET_NOT_FOUND,
            format!("secret {name:?} could not be read: {m}"),
        ),
    }
}

impl SecretResolver {
    /// A resolver reading grants from `meta` and values from `chain`.
    pub fn new(meta: Arc<dyn MetaView>, chain: ProviderChain) -> Self {
        Self {
            meta,
            chain,
            sealed: None,
        }
    }

    /// Sets the sealed store used by [`resolve_reserved`](Self::resolve_reserved). Without
    /// it, only `plaintext-v0` reserved records can be opened.
    pub fn with_sealed_store(mut self, store: SealedStore) -> Self {
        self.sealed = Some(store);
        self
    }

    /// The provider chain.
    pub fn chain(&self) -> &ProviderChain {
        &self.chain
    }

    async fn fetch(&self, name: &str) -> Result<SecretValue, Failure> {
        match self.chain.get(name).await {
            Ok(Some(v)) => Ok(v),
            Ok(None) => Err(failure(
                false,
                SECRET_NOT_FOUND,
                format!("secret {name:?} was not found in any provider"),
            )),
            Err(e) => Err(provider_failure(name, e)),
        }
    }

    /// Resolves `{{secret:name}}` for a request of `ctx.module` to `origin` (in the body if
    /// `in_body`). See the module docs for the order of checks.
    pub async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        name: &str,
        origin: &Origin,
        in_body: bool,
    ) -> Result<SecretValue, Failure> {
        template::validate_name(name)
            .map_err(|e| failure(false, SECRET_NOT_GRANTED, e.to_string()))?;
        let grants = self.meta.grants_for(ctx.module);
        check_grant(&grants, ctx.module, name, origin, in_body)?;
        self.fetch(name).await?.for_templating()
    }

    /// Resolves a secret referenced by an operator-owned connection config (P-7): no module
    /// grant is needed, but `__ostro/` names are still refused and the 8-byte minimum
    /// applies.
    pub async fn resolve_operator(&self, name: &str) -> Result<SecretValue, Failure> {
        template::validate_name(name)
            .map_err(|e| failure(false, SECRET_NOT_GRANTED, e.to_string()))?;
        self.fetch(name).await?.for_templating()
    }

    /// Reads a reserved `__ostro/presign/*` key (06-12). The sealed record is looked up by
    /// the **exact** name, with no `<tenant|_>/` prefix and no env/file/vault/k8s provider
    /// (W5). `Ok(None)` when absent. Other names are `secret_not_granted`.
    pub async fn resolve_reserved(&self, name: &str) -> Result<Option<SecretValue>, Failure> {
        let ok_name = name
            .strip_prefix(PRESIGN_PREFIX)
            .is_some_and(|rest| !rest.is_empty());
        if !ok_name {
            return Err(failure(
                false,
                SECRET_NOT_GRANTED,
                format!(
                    "{name:?} is not a reserved presign key ({RESERVED_PREFIX}presign/<connection>)"
                ),
            ));
        }
        let Some(rec) = self.meta.sealed_secret(name) else {
            return Ok(None);
        };
        open_record(self.sealed.as_ref().map(|s| s.ring().as_ref()), &rec)
            .map(Some)
            .map_err(|e| {
                failure(
                    false,
                    SECRET_NOT_FOUND,
                    format!("reserved secret {name:?} could not be opened: {e}"),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use secrecy::SecretBox;
    use url::Url;

    use super::*;
    use crate::meta_view::StaticMetaView;
    use crate::origin::OriginPattern;
    use crate::secrets::grants::GrantSpec;
    use crate::secrets::providers::SecretProvider;
    use crate::secrets::sealed::SealingKeyRing;

    const VALUE: &[u8] = b"CANARY-resolver-31c7-value";

    struct Counting(AtomicUsize, Option<&'static [u8]>);

    #[async_trait::async_trait]
    impl SecretProvider for Counting {
        fn name(&self) -> &'static str {
            "counting"
        }
        async fn get(&self, _: &str) -> Result<Option<SecretBox<[u8]>>, ProviderError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(self.1.map(|b| SecretBox::new(b.into())))
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

    fn grant(secret: &str, body: bool) -> ostro_meta::records::Grant {
        GrantSpec {
            module: "acme/billing".into(),
            secret: secret.into(),
            origins: vec![OriginPattern::parse("https://api.stripe.com").unwrap()],
            body,
        }
        .to_record()
    }

    fn setup(
        meta: StaticMetaView,
        value: Option<&'static [u8]>,
    ) -> (SecretResolver, Arc<Counting>) {
        let p = Arc::new(Counting(AtomicUsize::new(0), value));
        let chain = ProviderChain::new(vec![p.clone() as Arc<dyn SecretProvider>]);
        (SecretResolver::new(Arc::new(meta), chain), p)
    }

    fn kind(f: &Failure) -> (&str, bool) {
        match f {
            Failure::NonRetryable(i) => (i.kind.as_str(), false),
            Failure::Retryable(i) => (i.kind.as_str(), true),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn granted_secret_resolves() {
        let (r, p) = setup(
            StaticMetaView::new().insert_grant(grant("k", false)),
            Some(VALUE),
        );
        let v = r
            .resolve(&ctx(), "k", &origin("https://api.stripe.com/v1"), false)
            .await
            .unwrap();
        assert_eq!(v.expose(), VALUE);
        assert_eq!(p.0.load(Ordering::SeqCst), 1);
        assert!(!format!("{r:?}").contains("CANARY"));
    }

    #[tokio::test]
    async fn ungranted_secret_is_never_fetched() {
        let (r, p) = setup(
            StaticMetaView::new().insert_grant(grant("k", false)),
            Some(VALUE),
        );
        let o = origin("https://api.stripe.com");
        for (name, origin, body) in [
            ("other", o.clone(), false),
            ("k", origin("https://evil.test"), false),
            ("k", o.clone(), true),
            ("__ostro/token-key", o.clone(), false),
            ("BAD", o.clone(), false),
        ] {
            let e = r.resolve(&ctx(), name, &origin, body).await.unwrap_err();
            assert_eq!(kind(&e), (SECRET_NOT_GRANTED, false), "{name}");
        }
        assert_eq!(
            p.0.load(Ordering::SeqCst),
            0,
            "provider called without a grant"
        );
    }

    #[tokio::test]
    async fn not_found_and_short_values() {
        let (r, _) = setup(StaticMetaView::new().insert_grant(grant("k", true)), None);
        let e = r
            .resolve(&ctx(), "k", &origin("https://api.stripe.com"), true)
            .await
            .unwrap_err();
        assert_eq!(kind(&e), (SECRET_NOT_FOUND, false));

        let (r, _) = setup(
            StaticMetaView::new().insert_grant(grant("k", true)),
            Some(b"short"),
        );
        let e = r
            .resolve(&ctx(), "k", &origin("https://api.stripe.com"), true)
            .await
            .unwrap_err();
        assert_eq!(kind(&e), (SECRET_NOT_GRANTED, false));
    }

    #[tokio::test]
    async fn provider_errors_map_to_secret_not_found() {
        struct Down(fn(String) -> ProviderError);
        #[async_trait::async_trait]
        impl SecretProvider for Down {
            fn name(&self) -> &'static str {
                "down"
            }
            async fn get(&self, _: &str) -> Result<Option<SecretBox<[u8]>>, ProviderError> {
                Err((self.0)("boom".into()))
            }
        }
        for (f, retryable) in [
            (
                ProviderError::Unavailable as fn(String) -> ProviderError,
                true,
            ),
            (ProviderError::Invalid, false),
        ] {
            let chain = ProviderChain::new(vec![Arc::new(Down(f)) as Arc<dyn SecretProvider>]);
            let r = SecretResolver::new(Arc::new(StaticMetaView::new()), chain);
            let e = r.resolve_operator("db/pass").await.unwrap_err();
            assert_eq!(kind(&e), (SECRET_NOT_FOUND, retryable));
        }
    }

    #[tokio::test]
    async fn operator_skips_grants_but_refuses_reserved() {
        let (r, p) = setup(StaticMetaView::new(), Some(VALUE));
        assert_eq!(r.resolve_operator("db/pass").await.unwrap().expose(), VALUE);
        let e = r.resolve_operator("__ostro/token-key").await.unwrap_err();
        assert_eq!(kind(&e), (SECRET_NOT_GRANTED, false));
        let e = r.resolve_operator("__ostro/presign/x").await.unwrap_err();
        assert_eq!(kind(&e), (SECRET_NOT_GRANTED, false));
        assert_eq!(p.0.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reserved_reads_exact_sealed_name_only() {
        let store = SealedStore::new(Arc::new(SealingKeyRing::from_bytes([2; 32])));
        let exact = store.seal("__ostro/presign/objects", VALUE).unwrap();
        // A tenant-prefixed decoy must not be used.
        let decoy = store
            .seal("_/__ostro/presign/other", b"decoy-value-xx")
            .unwrap();
        let meta = StaticMetaView::new()
            .insert_sealed_secret(exact)
            .insert_sealed_secret(decoy);
        // The chain would answer anything: it must never be consulted.
        let (r, p) = setup(meta, Some(b"chain-value-never"));
        let r = r.with_sealed_store(store);
        let v = r
            .resolve_reserved("__ostro/presign/objects")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(v.expose(), VALUE);
        assert!(
            r.resolve_reserved("__ostro/presign/other")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            r.resolve_reserved("__ostro/presign/missing")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            p.0.load(Ordering::SeqCst),
            0,
            "reserved lookup used the chain"
        );
        for bad in [
            "__ostro/token-key",
            "objects",
            "__ostro/presign/",
            "_/__ostro/presign/x",
        ] {
            let e = r.resolve_reserved(bad).await.unwrap_err();
            assert_eq!(kind(&e), (SECRET_NOT_GRANTED, false), "{bad}");
        }
    }

    #[tokio::test]
    async fn reserved_without_store_fails_closed() {
        let store = SealedStore::new(Arc::new(SealingKeyRing::from_bytes([2; 32])));
        let rec = store.seal("__ostro/presign/objects", VALUE).unwrap();
        let (r, _) = setup(StaticMetaView::new().insert_sealed_secret(rec), None);
        let e = r
            .resolve_reserved("__ostro/presign/objects")
            .await
            .unwrap_err();
        assert_eq!(kind(&e), (SECRET_NOT_FOUND, false));
    }
}
