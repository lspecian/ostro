//! [`BridgeServices`]: the Phase 6 effects bridge, built once per server (06-14).
//!
//! # Construction order ([`BridgeServices::build`])
//!
//! 1. **Sealing key ring** (`SealingKeyRing::load`): `OSTRO_SEALING_KEY`, then
//!    `[secrets] sealing_key_file`, then (with `auto_sealing_key`, the dev profile) a
//!    generated `<data_dir>/sealing.key`. The path is logged, the key never is. No key = the
//!    node refuses to store secrets (no `sealed` provider, presign keys refused) but runs.
//! 2. **SealedStore** (`Option`, present iff a ring exists).
//! 3. **Provider chain** in `[secrets] providers` order: `sealed` only with a ring, `file`
//!    only with `file_dir`, `vault` / `k8s` only when their table is configured. The remote
//!    providers are built once and shared; `sealed` is per tenant (a `SealedProvider` serves
//!    one tenant, `<tenant|_>/<name>`), so HTTP effects resolve through a per-tenant chain
//!    ([`TenantHttp`]). Connection configs (SQL / object) are operator-owned and resolve
//!    through the default-tenant chain ([`BridgeServices::resolver`]).
//! 4. **SecretResolver** (`with_sealed_store`, so `resolve_reserved` opens presign keys).
//! 5. **Egress** over the system DNS resolver (`HickoryLookup`).
//! 6. **RateLimiter** with `[connectors] default_rate`.
//! 7. **Connectors**: HTTP (per tenant), SQL, object (with [`MetaPresignKeyStore`] and the
//!    effective `public_base_url`).
//! 8. **Bridge** with [`PartitionSendGate`] / [`PartitionCompletionSink`] and
//!    `[egress] rate_max_wait`; wrapped by [`ServerEffectDriver`].
//! 9. **SQL GC task** (`sql_effects_retention`).
//!
//! The server builds it after meta is ready and before `Node::open` (the executor hands
//! the driver to every activation), registers every started partition in its
//! [`PartitionTable`] afterwards, and calls [`BridgeServices::shutdown`] on stop.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use async_trait::async_trait;
use ostro_bridge::config::{ConnectorsConfig, SecretsConfig};
use ostro_bridge::connector::{Connector, EffectCtx, SendGate, Target};
use ostro_bridge::driver::Bridge;
use ostro_bridge::egress::{Egress, HickoryLookup};
use ostro_bridge::http::{HttpConfig, HttpConnector};
use ostro_bridge::meta_view::MetaView as BridgeMetaView;
use ostro_bridge::object::{ObjectConnector, PresignKeyStore};
use ostro_bridge::outcome::{non_retryable, retryable};
use ostro_bridge::ratelimit::RateLimiter;
use ostro_bridge::secrets::config::ProviderKind;
use ostro_bridge::secrets::resolver::PRESIGN_PREFIX;
use ostro_bridge::secrets::{
    EnvProvider, FileProvider, ProviderChain, SealedProvider, SealedStore, SealingKeyRing,
    SecretProvider, SecretResolver, SecretValue,
};
use ostro_bridge::sql::{GcHandle, SqlConfig, SqlConnector};
use ostro_meta::{MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult};
use ostro_partition::Clock;
use ostro_types::journal::{Failure, OpOutput};
use ostro_types::live::{EffectKind, LiveEffect};

use crate::config::ServerConfig;
use crate::effects::{
    EffectSeam, PartitionCompletionSink, PartitionSendGate, PartitionTable, ServerEffectDriver,
};
use crate::meta_view::MetaHandleView;

/// How long [`BridgeServices::shutdown`] lets in-flight effect tasks finish.
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);
/// Failure kind of a local presign without a sealing key (W5).
pub const SEALING_KEY_MISSING: &str = "sealing_key_missing";

/// Why the bridge could not be built (fatal at server start).
#[derive(Debug, thiserror::Error)]
pub enum BridgeBuildError {
    #[error("{0}")]
    Config(String),
    #[error("sealing key: {0}")]
    Sealing(String),
    #[error("secret provider {provider}: {message}")]
    Provider {
        provider: &'static str,
        message: String,
    },
    #[error("egress: {0}")]
    Egress(String),
}

/// Inputs of [`BridgeServices::build`] that are not configuration.
#[derive(Clone)]
pub struct BridgeEnv {
    /// The meta handle; held weakly (presign key writes), so meta still stops cleanly.
    pub meta: Weak<MetaHandle>,
    /// The live meta state (reads).
    pub view: ostro_meta::MetaView,
    /// The data directory (`sealing.key`).
    pub data_dir: std::path::PathBuf,
    /// Late-bound partition handles (gate, completion sink).
    pub partitions: Arc<PartitionTable>,
    /// Cluster id (idempotency keys, SQL tokens).
    pub cluster_id: String,
    /// This node (leadership check of the gate).
    pub node_id: u64,
    /// The one injectable clock (completion timestamps).
    pub clock: Arc<dyn Clock>,
    /// Effective base of local presigned URLs ([`ServerConfig::public_base_url`]).
    pub public_base_url: String,
}

impl std::fmt::Debug for BridgeEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BridgeEnv")
            .field("data_dir", &self.data_dir)
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.node_id)
            .field("public_base_url", &self.public_base_url)
            .finish_non_exhaustive()
    }
}

/// The node's effects bridge (see the module docs).
pub struct BridgeServices {
    sealed: Option<SealedStore>,
    resolver: SecretResolver,
    object: Arc<ObjectConnector>,
    bridge: Arc<Bridge>,
    driver: Arc<ServerEffectDriver>,
    seam: Arc<EffectSeam>,
    gc: Mutex<Option<GcHandle>>,
}

impl std::fmt::Debug for BridgeServices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BridgeServices")
            .field("sealing_key", &self.sealed.is_some())
            .field("providers", &self.resolver.chain().names())
            .field("seam", &self.seam)
            .finish_non_exhaustive()
    }
}

/// The ordered provider list for one tenant.
#[derive(Clone)]
struct ChainSpec {
    order: Vec<ProviderKind>,
    meta: Arc<dyn BridgeMetaView>,
    sealed: Option<SealedStore>,
    env: Arc<dyn SecretProvider>,
    file: Option<Arc<dyn SecretProvider>>,
    vault: Option<Arc<dyn SecretProvider>>,
    k8s: Option<Arc<dyn SecretProvider>>,
}

impl ChainSpec {
    fn chain(&self, tenant: Option<&str>) -> ProviderChain {
        let mut v: Vec<Arc<dyn SecretProvider>> = Vec::new();
        for kind in &self.order {
            let p: Option<Arc<dyn SecretProvider>> = match kind {
                ProviderKind::Sealed => self.sealed.as_ref().map(|s| {
                    Arc::new(SealedProvider::new(
                        Arc::clone(&self.meta),
                        s.clone(),
                        tenant.map(str::to_owned),
                    )) as Arc<dyn SecretProvider>
                }),
                ProviderKind::Env => Some(Arc::clone(&self.env)),
                ProviderKind::File => self.file.clone(),
                ProviderKind::Vault => self.vault.clone(),
                ProviderKind::K8s => self.k8s.clone(),
            };
            v.extend(p);
        }
        ProviderChain::new(v)
    }

    fn resolver(&self, tenant: Option<&str>) -> SecretResolver {
        let r = SecretResolver::new(Arc::clone(&self.meta), self.chain(tenant));
        match &self.sealed {
            Some(s) => r.with_sealed_store(s.clone()),
            None => r,
        }
    }
}

/// HTTP connector routing each effect to an `HttpConnector` whose secret chain serves the
/// run's tenant (`SealedProvider` is single-tenant). Inner connectors are created on first
/// use per tenant and share the egress layer.
pub struct TenantHttp {
    egress: Arc<Egress>,
    spec: ChainSpec,
    by_tenant: Mutex<HashMap<Option<String>, Arc<HttpConnector>>>,
}

impl std::fmt::Debug for TenantHttp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantHttp").finish_non_exhaustive()
    }
}

impl TenantHttp {
    fn inner(&self, tenant: Option<&str>) -> Arc<HttpConnector> {
        let mut m = self
            .by_tenant
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let key = tenant.map(str::to_owned);
        Arc::clone(m.entry(key).or_insert_with(|| {
            Arc::new(HttpConnector::new(
                Arc::clone(&self.egress),
                self.spec.resolver(tenant),
                Arc::clone(&self.spec.meta),
                HttpConfig::default(),
            ))
        }))
    }
}

#[async_trait]
impl Connector for TenantHttp {
    fn kind(&self) -> EffectKind {
        EffectKind::Http
    }

    fn target(&self, ctx: &EffectCtx, effect: &LiveEffect) -> Option<Target> {
        self.inner(ctx.tenant.as_deref()).target(ctx, effect)
    }

    async fn execute(
        &self,
        ctx: &EffectCtx,
        effect: &LiveEffect,
        gate: &dyn SendGate,
    ) -> Result<OpOutput, Failure> {
        self.inner(ctx.tenant.as_deref())
            .execute(ctx, effect, gate)
            .await
    }
}

impl BridgeServices {
    /// Build every bridge component in the documented order (module docs).
    pub async fn build(cfg: &ServerConfig, env: BridgeEnv) -> Result<Arc<Self>, BridgeBuildError> {
        let cfg_err = |e: crate::config::ConfigError| BridgeBuildError::Config(e.to_string());
        let secrets: SecretsConfig = cfg.secrets.to_bridge().map_err(cfg_err)?;
        let egress_cfg = cfg.egress.to_bridge().map_err(cfg_err)?;
        let mut connectors: ConnectorsConfig = cfg.connectors.to_bridge().map_err(cfg_err)?;
        connectors.public_base_url = Some(env.public_base_url.clone());
        let meta: Arc<dyn BridgeMetaView> = Arc::new(MetaHandleView::new(env.view.clone()));

        // 1-2. Sealing key ring + sealed store.
        let auto = cfg.secrets.auto_sealing_key;
        let dev_key = env
            .data_dir
            .join(ostro_bridge::secrets::sealed::DEV_KEY_FILE);
        let existed = dev_key.is_file();
        let ring = SealingKeyRing::load(&secrets, &env.data_dir, auto)
            .map_err(|e| BridgeBuildError::Sealing(e.to_string()))?;
        if ring.is_some() && auto && !existed && dev_key.is_file() {
            tracing::warn!(path = %dev_key.display(),
                "generated a dev sealing key; back it up or set OSTRO_SEALING_KEY / [secrets] sealing_key_file");
        }
        if ring.is_none() {
            tracing::warn!(
                "no sealing key configured: this node refuses to store secrets (set OSTRO_SEALING_KEY or [secrets] sealing_key_file)"
            );
        }
        let sealed = ring.map(|r| SealedStore::new(Arc::new(r)));

        // 3. Providers.
        let file: Option<Arc<dyn SecretProvider>> = secrets
            .file_dir
            .as_ref()
            .map(|d| Arc::new(FileProvider::new(d.clone())) as Arc<dyn SecretProvider>);
        let vault: Option<Arc<dyn SecretProvider>> = match &secrets.vault {
            Some(v) if secrets.providers.contains(&ProviderKind::Vault) => Some(Arc::new(
                ostro_bridge::secrets::providers::vault::VaultProvider::from_config(v).map_err(
                    |e| BridgeBuildError::Provider {
                        provider: "vault",
                        message: e.to_string(),
                    },
                )?,
            )),
            _ => None,
        };
        let k8s: Option<Arc<dyn SecretProvider>> = match &secrets.k8s {
            Some(k) if secrets.providers.contains(&ProviderKind::K8s) => Some(Arc::new(
                ostro_bridge::secrets::providers::k8s::K8sProvider::from_config(k)
                    .await
                    .map_err(|e| BridgeBuildError::Provider {
                        provider: "k8s",
                        message: e.to_string(),
                    })?,
            )),
            _ => None,
        };
        for (kind, present) in [
            (ProviderKind::File, file.is_some()),
            (ProviderKind::Vault, vault.is_some()),
            (ProviderKind::K8s, k8s.is_some()),
            (ProviderKind::Sealed, sealed.is_some()),
        ] {
            if secrets.providers.contains(&kind) && !present {
                tracing::info!(
                    provider = kind.as_str(),
                    "secret provider listed but not configured; skipped"
                );
            }
        }
        let spec = ChainSpec {
            order: secrets.providers.clone(),
            meta: Arc::clone(&meta),
            sealed: sealed.clone(),
            env: Arc::new(EnvProvider::new(secrets.env_prefix.clone())),
            file,
            vault,
            k8s,
        };

        // 4. Default-tenant resolver (operator-owned connection configs, presign keys).
        let resolver = spec.resolver(None);

        // 5. Egress.
        let lookup = HickoryLookup::new().map_err(|e| BridgeBuildError::Egress(e.to_string()))?;
        let egress = Arc::new(
            Egress::new(egress_cfg.clone(), Arc::new(lookup))
                .map_err(|e| BridgeBuildError::Egress(e.to_string()))?,
        );
        let operator_client = Egress::operator_client(&egress_cfg)
            .map_err(|e| BridgeBuildError::Egress(e.to_string()))?;

        // 6. Rate limiter.
        let limiter = Arc::new(RateLimiter::new(connectors.default_rate.clone()));

        // 7. Connectors.
        let http = Arc::new(TenantHttp {
            egress: Arc::clone(&egress),
            spec: spec.clone(),
            by_tenant: Mutex::new(HashMap::new()),
        });
        let sql = Arc::new(SqlConnector::new(
            Arc::clone(&meta),
            resolver.clone(),
            env.cluster_id.clone(),
            SqlConfig::from_connectors(&connectors),
        ));
        let presign = Arc::new(MetaPresignKeyStore {
            meta: env.meta.clone(),
            view: env.view.clone(),
            sealed: sealed.clone(),
        });
        let object = Arc::new(
            ObjectConnector::new(
                Arc::clone(&meta),
                resolver.clone(),
                operator_client,
                connectors.clone(),
            )
            .with_presign_key_store(presign),
        );

        // 8. Bridge + driver.
        let seam = Arc::new(EffectSeam::new(
            Arc::clone(&env.partitions),
            env.view.clone(),
            env.node_id,
            env.cluster_id.clone(),
            Arc::clone(&env.clock),
        ));
        let mut bridge = Bridge::new(
            connectors.clone(),
            egress_cfg.rate_max_wait,
            Arc::new(PartitionSendGate::new(Arc::clone(&seam))),
            Arc::new(PartitionCompletionSink::new(Arc::clone(&seam))),
            limiter,
        );
        bridge.register(http);
        bridge.register(Arc::clone(&sql) as Arc<dyn Connector>);
        bridge.register(Arc::clone(&object) as Arc<dyn Connector>);
        let bridge = Arc::new(bridge);
        let driver = Arc::new(ServerEffectDriver::new(
            Arc::clone(&seam),
            Arc::clone(&bridge),
        ));

        // 9. SQL token GC.
        let gc = sql.spawn_gc(connectors.sql_effects_retention);

        let svc = Arc::new(Self {
            sealed,
            resolver,
            object,
            bridge,
            driver,
            seam,
            gc: Mutex::new(Some(gc)),
        });
        tracing::info!(
            sealing_key = svc.sealed.is_some(),
            providers = ?svc.resolver.chain().names(),
            "effects bridge ready"
        );
        Ok(svc)
    }

    /// The sealed store; `None` when no sealing key is configured (secrets refused).
    pub fn sealed_store(&self) -> Option<&SealedStore> {
        self.sealed.as_ref()
    }

    /// The default-tenant secret resolver (operator connection configs, presign keys).
    pub fn resolver(&self) -> &SecretResolver {
        &self.resolver
    }

    /// The object connector (06-16 serves `/v1/objects/...` through `serve_local`).
    pub fn object_connector(&self) -> &Arc<ObjectConnector> {
        &self.object
    }

    /// The bridge (in-flight effect tasks).
    pub fn bridge(&self) -> &Arc<Bridge> {
        &self.bridge
    }

    /// The engine `EffectDriver` handed to every activation.
    pub fn driver(&self) -> &Arc<ServerEffectDriver> {
        &self.driver
    }

    /// The late-bound partition table of the gate and the completion sink.
    pub fn partitions(&self) -> &Arc<PartitionTable> {
        self.seam.table()
    }

    /// Stop: refuse new effects, let in-flight tasks finish for [`SHUTDOWN_GRACE`], stop the
    /// SQL GC and forget the partition handles. Idempotent.
    pub async fn shutdown(&self) {
        self.bridge.shutdown(SHUTDOWN_GRACE).await;
        let gc = self
            .gc
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(gc) = gc {
            gc.stop().await;
        }
        self.seam.table().close();
    }
}

/// Seals local presign keys as `MetaRecord::Secret` named **exactly**
/// `__ostro/presign/<connection>` (no `<tenant|_>/` prefix: `resolve_reserved` reads the
/// exact name, W5), create-only through the meta log (insert-if-absent; the loser of a race
/// gets the winner's key). Refuses with `NonRetryable{sealing_key_missing}` when the node
/// has no sealing key.
pub struct MetaPresignKeyStore {
    meta: Weak<MetaHandle>,
    view: ostro_meta::MetaView,
    sealed: Option<SealedStore>,
}

impl std::fmt::Debug for MetaPresignKeyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaPresignKeyStore")
            .field("sealing_key", &self.sealed.is_some())
            .finish_non_exhaustive()
    }
}

impl MetaPresignKeyStore {
    /// A store writing through `meta` (held weakly) and reading `view`.
    pub fn new(
        meta: Weak<MetaHandle>,
        view: ostro_meta::MetaView,
        sealed: Option<SealedStore>,
    ) -> Self {
        Self { meta, view, sealed }
    }

    fn existing(&self, store: &SealedStore, name: &str) -> Result<Option<SecretValue>, Failure> {
        let rec = match self
            .view
            .read(|s| s.get(&MetaKey::Secret(name.to_owned())))
            .map(|v| v.value)
        {
            Some(MetaRecord::Secret(s)) => s,
            _ => return Ok(None),
        };
        store.open(&rec).map(Some).map_err(|e| {
            non_retryable(
                ostro_bridge::kinds::SECRET_NOT_FOUND,
                format!("presign key {name:?} could not be opened: {e}"),
                None,
            )
        })
    }
}

#[async_trait]
impl PresignKeyStore for MetaPresignKeyStore {
    async fn create(&self, name: &str, key: SecretValue) -> Result<SecretValue, Failure> {
        if name.strip_prefix(PRESIGN_PREFIX).is_none_or(str::is_empty) {
            return Err(non_retryable(
                ostro_bridge::kinds::SECRET_NOT_GRANTED,
                format!("{name:?} is not a presign key name ({PRESIGN_PREFIX}<connection>)"),
                None,
            ));
        }
        let Some(store) = &self.sealed else {
            return Err(non_retryable(
                SEALING_KEY_MISSING,
                "no sealing key is configured; local presign keys cannot be stored",
                None,
            ));
        };
        if let Some(v) = self.existing(store, name)? {
            return Ok(v);
        }
        let rec = store.seal(name, key.expose()).map_err(|e| {
            non_retryable(
                ostro_bridge::outcome::INTERNAL_ERROR,
                format!("sealing presign key {name:?}: {e}"),
                None,
            )
        })?;
        let meta = self
            .meta
            .upgrade()
            .ok_or_else(|| retryable("unavailable", "metadata is shutting down"))?;
        let res = meta
            .propose(MetaCommand::Put {
                key: MetaKey::Secret(name.to_owned()),
                expected_version: None,
                record: MetaRecord::Secret(rec),
            })
            .await
            .map_err(|e| retryable("unavailable", format!("storing presign key: {e}")))?;
        match res {
            MetaResult::Ok { .. } => Ok(key),
            MetaResult::Conflict { .. } => self
                .existing(store, name)?
                .ok_or_else(|| retryable("unavailable", "presign key vanished after a conflict")),
            other => Err(retryable(
                "unavailable",
                format!("storing presign key: {other:?}"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_bridge::object::presign_local::key_name;
    use std::path::Path;

    async fn meta(dir: &Path) -> Arc<MetaHandle> {
        Arc::new(
            MetaHandle::start(&dir.join("meta"), 1)
                .await
                .expect("meta start"),
        )
    }

    fn store() -> SealedStore {
        SealedStore::new(Arc::new(SealingKeyRing::from_bytes([7u8; 32])))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bridge_services_presign_store_creates_reuses_and_refuses() {
        let dir = tempfile::tempdir().expect("tempdir");
        let m = meta(dir.path()).await;
        let s = MetaPresignKeyStore::new(Arc::downgrade(&m), m.view().clone(), Some(store()));
        let name = key_name("files");
        assert_eq!(name, "__ostro/presign/files");

        // Creation: sealed under exactly the reserved name.
        let k1 = SecretValue::from_slice(&[1u8; 32]);
        let got = s.create(&name, k1).await.expect("create");
        assert_eq!(got.expose(), &[1u8; 32]);
        let rec = m
            .view()
            .read(|st| st.get(&MetaKey::Secret(name.clone())))
            .expect("record written");
        let MetaRecord::Secret(sec) = rec.value else {
            panic!("not a secret record")
        };
        assert_eq!(sec.name, "__ostro/presign/files", "no tenant prefix (W5)");
        assert!(sec.key_id.starts_with("aes256gcm:"));
        assert_ne!(
            sec.ciphertext,
            vec![1u8; 32],
            "stored sealed, not in plaintext"
        );

        // Reuse: a second create returns the key in force, not the new one.
        let got2 = s
            .create(&name, SecretValue::from_slice(&[2u8; 32]))
            .await
            .expect("reuse");
        assert_eq!(got2.expose(), &[1u8; 32]);

        // The default-tenant resolver opens it by its exact name.
        let r = SecretResolver::new(
            Arc::new(MetaHandleView::new(m.view().clone())),
            ProviderChain::default(),
        )
        .with_sealed_store(store());
        let v = r
            .resolve_reserved(&name)
            .await
            .expect("resolve")
            .expect("found");
        assert_eq!(v.expose(), &[1u8; 32]);

        // A bad name is refused.
        assert!(
            s.create("_/x", SecretValue::from_slice(&[3u8; 32]))
                .await
                .is_err()
        );

        // No sealing key: refused with sealing_key_missing, nothing written.
        let none = MetaPresignKeyStore::new(Arc::downgrade(&m), m.view().clone(), None);
        let other = key_name("other");
        match none
            .create(&other, SecretValue::from_slice(&[4u8; 32]))
            .await
        {
            Err(Failure::NonRetryable(i)) => assert_eq!(i.kind, SEALING_KEY_MISSING),
            other => panic!("expected sealing_key_missing, got {other:?}"),
        }
        assert!(
            m.view()
                .read(|st| st.get(&MetaKey::Secret(other.clone())))
                .is_none()
        );
        drop(s);
        drop(none);
        match Arc::try_unwrap(m) {
            Ok(m) => m.stop().await,
            Err(_) => panic!("meta still referenced"),
        }
    }
}
