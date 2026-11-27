//! Phase 14 (14-19, SEC-03, D-P14-10/14/15): `SecurityApi` (module policy, trusted keys,
//! attached signatures) and the `[security.modules]` seed of the cluster policy.
//!
//! # Wire methods (authorization is the guard's, 14-15: policy get = system read, policy set
//! = system admin, keys = tenant admin, attach = deploy)
//!
//! | Method | Effect |
//! |---|---|
//! | `get_module_policy` | the cluster policy (`ClusterInfo` ext 115, else the node's `[security.modules]`); keys as alg + key id + public key + comment; a system principal also sees every tenant key (`tenant` set) |
//! | `set_module_policy` | sets `require_signed_modules` (CAS on `ClusterInfo`); a non-empty `trusted_keys` replaces the cluster keys, an empty one keeps them |
//! | `add_trusted_key` | `tenant = None`: a cluster key (ext 115); `Some(t)`: a key of tenant `t` (ext 116 on its `Tenant` record; 404 for an unknown tenant, 400 for `_`). The key is parsed with `signing::parse_trusted_key`; `key_id` must be the key's id (`signing::key_id_hex`, case-insensitive) or `auto`; a key id already trusted in the cluster set or that tenant's set → 409 |
//! | `remove_trusted_key` | `<key id>` or `<tenant>:<key id>`. A tenant principal only reaches its own tenant's keys: any other key answers the 404 of an unknown id. A system principal removes the cluster key with that id, else the one tenant key with it (409 if several tenants hold it: name the tenant) |
//! | `attach_signatures` | verifies against the stored bytes with the caller's effective keys (a system principal: cluster keys plus the keys of every tenant that registered the module) and merges ext 113 evidence |
//!
//! Every write needs the feature level of its ext (`compat::require`: 115/116/113 at
//! finalized level 2), else 409 `feature_not_enabled`.
//!
//! The tenant-level `require_signed_modules` (ext 116) has no wire field in the 1.0 DTOs
//! (`ModulePolicy` carries no tenant); [`SecurityService::set_tenant_require`] implements it
//! for a later wire addition (and the tests).
//!
//! # Seed and drift ([`seed_task`])
//!
//! When this node has `[security.modules]` and `ClusterInfo` has no ext 115, the node writes
//! it once from the section (CAS on `ClusterInfo`, forwarded to the meta leader, after
//! `compat::require`; a node that is not the leader seeds too, so the policy does not depend
//! on which node leads). Once meta
//! holds a policy that differs from the section, a WARN is logged once and
//! `ostro_security_policy_drift` is set to 1. Meta always wins.

use std::sync::{Arc, Weak};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use ostro_api::auth::{self, Principal};
use ostro_api::error::reason;
use ostro_api::phase14::{
    AttachSignaturesRequest, AttachSignaturesResult, ModulePolicy, ResealResult, SealingStatus,
    SecurityApi, TrustedKeySpec,
};
use ostro_api::{ApiError, ApiExtensions};
use ostro_meta::MetaState;
use ostro_meta::ext::{
    self, SecurityPolicy, TenantSecurity, get_module_signatures, put_security_policy,
    put_tenant_security,
};
use ostro_meta::{MetaCommand, MetaKey, MetaRecord, MetaResult};
use ostro_types::compat::Feature;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::P14Ctx;
use super::module_sign::{self, PolicySource, key_id_hex_of, tenant_record_id};
use super::signing::{self, KeyFamily};
use super::{UploadEvidence, module_sign::merge_evidence};
use crate::control::names::split_qualified;

/// CAS attempts before answering 503.
const CAS_RETRIES: usize = 8;
/// `key_id` value asking the server to derive the id from the key.
pub const AUTO_KEY_ID: &str = "auto";
/// Seed / drift re-check period.
const SEED_PERIOD: Duration = Duration::from_secs(1);

/// Fill `ApiExtensions.security`.
pub fn install(ctx: &Arc<P14Ctx>, ext: &mut ApiExtensions) {
    ext.security = Some(Arc::new(SecurityService::new(ctx)) as Arc<dyn SecurityApi>);
}

// ---------------------------------------------------------------------------------
// Wire forms
// ---------------------------------------------------------------------------------

/// The wire alg name of a stored key.
pub fn alg_name(alg: u8) -> String {
    match signing::Alg::from_u8(alg).map(signing::Alg::family) {
        Some(KeyFamily::Minisign) => "minisign".to_owned(),
        Some(KeyFamily::Cosign) => "cosign-p256".to_owned(),
        None => format!("unknown-{alg}"),
    }
}

/// The public key text of a stored key: the minisign `RW…` line or a PEM SPKI.
pub fn public_key_text(k: &ext::TrustedKey) -> String {
    match signing::Alg::from_u8(k.alg).map(signing::Alg::family) {
        Some(KeyFamily::Minisign) => {
            let mut raw = Vec::with_capacity(42);
            raw.extend_from_slice(b"Ed");
            raw.extend_from_slice(&k.key_id);
            raw.extend_from_slice(&k.public_key);
            B64.encode(raw)
        }
        _ => {
            let b = B64.encode(&k.public_key);
            let mut pem = String::from("-----BEGIN PUBLIC KEY-----\n");
            for chunk in b.as_bytes().chunks(64) {
                pem.push_str(std::str::from_utf8(chunk).unwrap_or_default());
                pem.push('\n');
            }
            pem.push_str("-----END PUBLIC KEY-----\n");
            pem
        }
    }
}

/// A stored key on the wire.
pub fn spec_of(k: &ext::TrustedKey, tenant: Option<&str>) -> TrustedKeySpec {
    TrustedKeySpec {
        alg: alg_name(k.alg),
        key_id: key_id_hex_of(k.alg, &k.key_id),
        public_key: public_key_text(k),
        comment: k.comment.clone(),
        tenant: tenant.map(str::to_owned),
    }
}

/// The policy as `p_tenant` sees it: cluster keys, plus every tenant key for a system
/// principal (`None`) or the principal's own tenant keys.
pub fn view_for(
    cluster: &SecurityPolicy,
    state: &MetaState,
    p_tenant: Option<&str>,
) -> ModulePolicy {
    let mut keys: Vec<TrustedKeySpec> = cluster
        .trusted_keys
        .iter()
        .map(|k| spec_of(k, None))
        .collect();
    let mut add = |id: &str, ext_bytes: &[u8]| {
        if let Ok(Some(ts)) = ext::get_tenant_security(ext_bytes) {
            keys.extend(ts.trusted_keys.iter().map(|k| spec_of(k, Some(id))));
        }
    };
    match p_tenant {
        None => {
            for t in state.tenants() {
                add(&t.value.id, &t.value.ext);
            }
        }
        Some(pt) => {
            if let Some(id) = tenant_record_id(Some(pt))
                && let Some(t) = state.tenant(id)
            {
                add(id, &t.value.ext);
            }
        }
    }
    ModulePolicy {
        require_signed_modules: cluster.require_signed_modules,
        trusted_keys: keys,
        tenant: None,
    }
}

fn field_error(field: &str, message: impl Into<String>) -> ApiError {
    ApiError::invalid_argument(format!("{field}: {}", message.into()))
        .with_details(serde_json::json!({ "field": field }))
}

fn key_not_found(id: &str) -> ApiError {
    ApiError::not_found(format!("trusted key {id} not found"))
}

fn feature_error(e: ostro_cluster::compat::FeatureNotEnabled) -> ApiError {
    ApiError::conflict(e.detail()).with_details(serde_json::json!({
        "reason": crate::control::modules::REASON_FEATURE_NOT_ENABLED,
        "feature": e.feature.name(),
        "needed": e.needed,
        "finalized": e.finalized,
    }))
}

/// Parse a wire key into its stored form (400 on any mismatch).
pub fn parse_spec(spec: &TrustedKeySpec) -> Result<ext::TrustedKey, ApiError> {
    let key = signing::parse_trusted_key(&spec.public_key)
        .map_err(|e| field_error("public_key", e.to_string()))?;
    let alg = spec.alg.trim().to_ascii_lowercase();
    let family_ok = match alg.as_str() {
        "" => true,
        "minisign" => key.alg_family == KeyFamily::Minisign,
        "cosign" | "cosign-p256" => key.alg_family == KeyFamily::Cosign,
        other => return Err(field_error("alg", format!("unknown alg {other:?}"))),
    };
    if !family_ok {
        return Err(field_error(
            "alg",
            format!(
                "{:?} does not match the key ({:?})",
                spec.alg, key.alg_family
            ),
        ));
    }
    let id = signing::key_id_hex(&key);
    if spec.key_id != AUTO_KEY_ID && !spec.key_id.eq_ignore_ascii_case(&id) {
        return Err(field_error(
            "key_id",
            format!(
                "{:?} is not the key's id ({id}); send it or \"auto\"",
                spec.key_id
            ),
        ));
    }
    if spec.comment.len() > signing::MAX_COMMENT_BYTES {
        return Err(field_error("comment", "longer than 1 KiB"));
    }
    let mut m = key.to_meta();
    if !spec.comment.is_empty() {
        m.comment = spec.comment.clone();
    }
    Ok(m)
}

fn same_id(a: &ext::TrustedKey, id: &str) -> bool {
    key_id_hex_of(a.alg, &a.key_id).eq_ignore_ascii_case(id)
}

fn duplicate(id: &str, place: &str) -> ApiError {
    ApiError::conflict(format!("key id {id} is already trusted ({place})"))
        .with_details(serde_json::json!({ "key_id": id }))
}

// ---------------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------------

/// `SecurityApi` over the cluster metadata. Holds the context weakly (503 once released).
pub struct SecurityService {
    ctx: Weak<P14Ctx>,
}

impl SecurityService {
    /// The service of `ctx`.
    pub fn new(ctx: &Arc<P14Ctx>) -> Self {
        Self {
            ctx: Arc::downgrade(ctx),
        }
    }

    fn ctx(&self) -> Result<Arc<P14Ctx>, ApiError> {
        self.ctx
            .upgrade()
            .ok_or_else(|| ApiError::unavailable("the server is shutting down"))
    }

    fn caller() -> Result<Principal, ApiError> {
        let p = auth::current();
        if p.is_anonymous() {
            return Err(ApiError::unauthenticated("authentication required"));
        }
        Ok(p)
    }

    fn view(ctx: &P14Ctx, p: &Principal) -> Result<ModulePolicy, ApiError> {
        ctx.meta.view().read(|s| {
            let (cluster, _) = module_sign::cluster_policy(&ctx.cfg, s)?;
            Ok(view_for(&cluster, s, p.tenant.as_deref()))
        })
    }

    /// CAS-update the cluster policy (`f` returns false for "no change").
    async fn update_cluster(
        ctx: &P14Ctx,
        mut f: impl FnMut(&mut SecurityPolicy) -> Result<bool, ApiError>,
    ) -> Result<(), ApiError> {
        ostro_cluster::compat::require(ctx.meta.view(), Feature::SecurityPolicyExt, ctx.level_max)
            .map_err(feature_error)?;
        for _ in 0..CAS_RETRIES {
            let (cur, mut pol) = ctx.meta.view().read(|s| {
                let cur = s
                    .cluster()
                    .cloned()
                    .ok_or_else(|| ApiError::unavailable("the cluster is not initialized"))?;
                let (pol, _) = module_sign::cluster_policy(&ctx.cfg, s)?;
                Ok::<_, ApiError>((cur, pol))
            })?;
            if !f(&mut pol)? {
                return Ok(());
            }
            let mut info = cur.value.clone();
            info.ext = put_security_policy(&info.ext, &pol)
                .map_err(|e| ApiError::internal(format!("cluster ext 115: {e}")))?;
            match ctx
                .meta
                .propose(MetaCommand::Put {
                    key: MetaKey::Cluster,
                    expected_version: Some(cur.version),
                    record: MetaRecord::Cluster(info),
                })
                .await
                .map_err(|e| ApiError::unavailable(format!("metadata unavailable: {e}")))?
            {
                MetaResult::Ok { .. } => return Ok(()),
                MetaResult::StaleVersion { .. } | MetaResult::Conflict { .. } => {}
                other => {
                    return Err(ApiError::internal(format!(
                        "metadata rejected the security policy update: {other:?}"
                    )));
                }
            }
        }
        Err(ApiError::unavailable(
            "the cluster record is being updated concurrently; retry",
        ))
    }

    /// CAS-update tenant `id`'s ext 116 (404 for an unknown tenant).
    async fn update_tenant(
        ctx: &P14Ctx,
        id: &str,
        mut f: impl FnMut(&mut TenantSecurity) -> Result<bool, ApiError>,
    ) -> Result<(), ApiError> {
        ostro_cluster::compat::require(ctx.meta.view(), Feature::TenantSecurityExt, ctx.level_max)
            .map_err(feature_error)?;
        for _ in 0..CAS_RETRIES {
            let cur = ctx
                .meta
                .view()
                .read(|s| s.tenant(id).cloned())
                .ok_or_else(|| ApiError::not_found(format!("tenant {id} not found")))?;
            let mut ts = ext::get_tenant_security(&cur.value.ext)
                .map_err(|e| ApiError::internal(format!("tenant ext 116: {e}")))?
                .unwrap_or_default();
            if !f(&mut ts)? {
                return Ok(());
            }
            let mut t = cur.value.clone();
            t.ext = put_tenant_security(&t.ext, &ts)
                .map_err(|e| ApiError::internal(format!("tenant ext 116: {e}")))?;
            match ctx
                .meta
                .propose(MetaCommand::Put {
                    key: MetaKey::Tenant(id.to_owned()),
                    expected_version: Some(cur.version),
                    record: MetaRecord::Tenant(t),
                })
                .await
                .map_err(|e| ApiError::unavailable(format!("metadata unavailable: {e}")))?
            {
                MetaResult::Ok { .. } => return Ok(()),
                MetaResult::StaleVersion { .. } | MetaResult::Conflict { .. } => {}
                other => {
                    return Err(ApiError::internal(format!(
                        "metadata rejected the tenant security update: {other:?}"
                    )));
                }
            }
        }
        Err(ApiError::unavailable(
            "the tenant record is being updated concurrently; retry",
        ))
    }

    /// Tenant-level `require_signed_modules` (ext 116). A tenant principal may only set
    /// `Some(true)` on its own tenant; a system principal may set any value (`Some(false)` /
    /// `None` never relax the cluster policy).
    pub async fn set_tenant_require(
        &self,
        tenant: &str,
        require: Option<bool>,
    ) -> Result<(), ApiError> {
        let ctx = self.ctx()?;
        let p = Self::caller()?;
        let id = tenant_record_id(Some(tenant))
            .ok_or_else(|| field_error("tenant", "the default tenant has no tenant policy"))?;
        if let Some(pt) = &p.tenant {
            if pt != id {
                return Err(ApiError::not_found(format!("tenant {id} not found")));
            }
            if require != Some(true) {
                return Err(ApiError::permission_denied(
                    reason::MISSING_SCOPE,
                    "a tenant principal can only tighten its policy (require = true)",
                ));
            }
        }
        Self::update_tenant(&ctx, id, |ts| {
            let changed = ts.require_signed_modules != require;
            ts.require_signed_modules = require;
            Ok(changed)
        })
        .await
    }
}

#[async_trait::async_trait]
impl SecurityApi for SecurityService {
    async fn get_module_policy(&self) -> Result<ModulePolicy, ApiError> {
        let ctx = self.ctx()?;
        let p = Self::caller()?;
        Self::view(&ctx, &p)
    }

    async fn set_module_policy(&self, policy: ModulePolicy) -> Result<ModulePolicy, ApiError> {
        let ctx = self.ctx()?;
        let p = Self::caller()?;
        // p14-fixups-2: `tenant` = that tenant's requirement (ext 116, `set_tenant_require`:
        // a tenant principal only tightens its own tenant; `false` clears the tightening).
        if let Some(t) = policy.tenant.as_deref().filter(|t| !t.is_empty()) {
            if !policy.trusted_keys.is_empty() {
                return Err(field_error(
                    "trusted_keys",
                    "tenant keys are added with POST /v1/security/modules/keys",
                ));
            }
            self.set_tenant_require(t, policy.require_signed_modules.then_some(true))
                .await?;
            info!(
                tenant = t,
                require_signed_modules = policy.require_signed_modules,
                actor = %p.token_id,
                "tenant module policy set"
            );
            return ctx.meta.view().read(|s| {
                let (cluster, _) = module_sign::cluster_policy(&ctx.cfg, s)?;
                let ts = module_sign::tenant_security(s, Some(t))?;
                let mut v = view_for(&cluster, s, Some(t));
                v.require_signed_modules = module_sign::effective(&cluster, ts.as_ref()).require;
                v.tenant = Some(t.to_owned());
                Ok(v)
            });
        }
        if p.tenant.is_some() {
            return Err(ApiError::permission_denied(
                reason::FORBIDDEN_TENANT,
                "the cluster module policy needs a system principal",
            ));
        }
        let mut keys: Option<Vec<ext::TrustedKey>> = None;
        if !policy.trusted_keys.is_empty() {
            let mut v: Vec<ext::TrustedKey> = Vec::new();
            for spec in &policy.trusted_keys {
                if spec.tenant.is_some() {
                    return Err(field_error(
                        "trusted_keys",
                        "tenant keys are added with POST /v1/security/modules/keys",
                    ));
                }
                let k = parse_spec(spec)?;
                let id = key_id_hex_of(k.alg, &k.key_id);
                if v.iter().any(|x| same_id(x, &id)) {
                    return Err(duplicate(&id, "twice in the request"));
                }
                v.push(k);
            }
            keys = Some(v);
        }
        Self::update_cluster(&ctx, |pol| {
            let mut changed = pol.require_signed_modules != policy.require_signed_modules;
            pol.require_signed_modules = policy.require_signed_modules;
            if let Some(k) = &keys
                && pol.trusted_keys != *k
            {
                pol.trusted_keys = k.clone();
                changed = true;
            }
            Ok(changed)
        })
        .await?;
        info!(
            require_signed_modules = policy.require_signed_modules,
            actor = %p.token_id,
            "cluster module policy set"
        );
        Self::view(&ctx, &p)
    }

    async fn add_trusted_key(&self, key: TrustedKeySpec) -> Result<ModulePolicy, ApiError> {
        let ctx = self.ctx()?;
        let p = Self::caller()?;
        // The guard's tenant hook forced the tenant of a tenant principal; re-check.
        if let Some(pt) = &p.tenant
            && key.tenant.as_deref() != Some(pt.as_str())
        {
            return Err(ApiError::permission_denied(
                reason::FORBIDDEN_TENANT,
                format!("a token of tenant {pt} can only add keys of its own tenant"),
            ));
        }
        let k = parse_spec(&key)?;
        let id = key_id_hex_of(k.alg, &k.key_id);
        match key.tenant.as_deref() {
            None => {
                Self::update_cluster(&ctx, |pol| {
                    if pol.trusted_keys.iter().any(|x| same_id(x, &id)) {
                        return Err(duplicate(&id, "cluster key"));
                    }
                    pol.trusted_keys.push(k.clone());
                    Ok(true)
                })
                .await?;
            }
            Some(t) => {
                let tid = tenant_record_id(Some(t)).ok_or_else(|| {
                    field_error(
                        "tenant",
                        "the default tenant has no tenant keys; add a cluster key",
                    )
                })?;
                let in_cluster = ctx.meta.view().read(|s| {
                    module_sign::cluster_policy(&ctx.cfg, s)
                        .map(|(c, _)| c.trusted_keys.iter().any(|x| same_id(x, &id)))
                })?;
                if in_cluster {
                    return Err(duplicate(&id, "cluster key"));
                }
                Self::update_tenant(&ctx, tid, |ts| {
                    if ts.trusted_keys.iter().any(|x| same_id(x, &id)) {
                        return Err(duplicate(&id, "tenant key"));
                    }
                    ts.trusted_keys.push(k.clone());
                    Ok(true)
                })
                .await?;
            }
        }
        Self::view(&ctx, &p)
    }

    async fn remove_trusted_key(&self, key_id: String) -> Result<ModulePolicy, ApiError> {
        let ctx = self.ctx()?;
        let p = Self::caller()?;
        let (named_tenant, id) = match key_id.split_once(':') {
            Some((t, id)) => (Some(t.to_owned()), id.to_owned()),
            None => (None, key_id.clone()),
        };
        // A tenant principal reaches only its own tenant's keys (404 like an unknown id).
        let target: Option<String> = match (&p.tenant, named_tenant) {
            (Some(pt), None) => Some(pt.clone()),
            (Some(pt), Some(t)) if *pt == t => Some(t),
            (Some(_), Some(_)) => return Err(key_not_found(&key_id)),
            (None, t) => t,
        };
        let target = match target {
            Some(t) => {
                let Some(tid) = tenant_record_id(Some(&t)) else {
                    return Err(key_not_found(&key_id));
                };
                Some(tid.to_owned())
            }
            None => {
                // System principal, no tenant named: the cluster key, else the one tenant key.
                let (in_cluster, holders) = ctx.meta.view().read(|s| {
                    let (c, src) = module_sign::cluster_policy(&ctx.cfg, s)?;
                    let in_cluster = src != PolicySource::Default
                        && c.trusted_keys.iter().any(|x| same_id(x, &id));
                    let holders: Vec<String> = s
                        .tenants()
                        .filter(|t| {
                            ext::get_tenant_security(&t.value.ext)
                                .ok()
                                .flatten()
                                .is_some_and(|ts| ts.trusted_keys.iter().any(|x| same_id(x, &id)))
                        })
                        .map(|t| t.value.id.clone())
                        .collect();
                    Ok::<_, ApiError>((in_cluster, holders))
                })?;
                if in_cluster {
                    None
                } else {
                    match holders.as_slice() {
                        [] => return Err(key_not_found(&key_id)),
                        [one] => Some(one.clone()),
                        many => {
                            return Err(ApiError::conflict(format!(
                                "key id {id} is trusted by several tenants ({}); name one as <tenant>:{id}",
                                many.join(", ")
                            )));
                        }
                    }
                }
            }
        };
        let mut found = false;
        match &target {
            None => {
                Self::update_cluster(&ctx, |pol| {
                    let before = pol.trusted_keys.len();
                    pol.trusted_keys.retain(|x| !same_id(x, &id));
                    found = pol.trusted_keys.len() != before;
                    Ok(found)
                })
                .await?;
            }
            Some(tid) => {
                let exists = ctx.meta.view().read(|s| s.tenant(tid).is_some());
                if !exists {
                    return Err(key_not_found(&key_id));
                }
                Self::update_tenant(&ctx, tid, |ts| {
                    let before = ts.trusted_keys.len();
                    ts.trusted_keys.retain(|x| !same_id(x, &id));
                    found = ts.trusted_keys.len() != before;
                    Ok(found)
                })
                .await?;
            }
        }
        if !found {
            return Err(key_not_found(&key_id));
        }
        Self::view(&ctx, &p)
    }

    async fn attach_signatures(
        &self,
        req: AttachSignaturesRequest,
    ) -> Result<AttachSignaturesResult, ApiError> {
        let ctx = self.ctx()?;
        let p = Self::caller()?;
        let sha = parse_sha(&req.sha256)?;
        let rec = ctx
            .meta
            .view()
            .read(|s| s.module(&sha).map(|m| m.value.clone()))
            .ok_or_else(|| ApiError::not_found(format!("module {} not found", req.sha256)))?;
        // A tenant principal only sees modules registering a workflow in its tenant (the
        // guard's tenant hook checked it too).
        let tenants: Vec<Option<String>> = {
            let mut v: Vec<Option<String>> = rec
                .workflows
                .iter()
                .map(|(w, _)| split_qualified(w).0.map(str::to_owned))
                .collect();
            v.sort();
            v.dedup();
            v
        };
        let keys = match &p.tenant {
            Some(pt) => {
                let own = tenant_record_id(Some(pt)).map(str::to_owned);
                if !tenants.contains(&own) {
                    return Err(ApiError::not_found(format!(
                        "module {} not found",
                        req.sha256
                    )));
                }
                module_sign::effective_policy(&ctx, own.as_deref())?.keys
            }
            None => {
                let mut keys = module_sign::effective_policy(&ctx, None)?.keys;
                for t in tenants.iter().flatten() {
                    for k in module_sign::effective_policy(&ctx, Some(t))?.keys {
                        if !keys.contains(&k) {
                            keys.push(k);
                        }
                    }
                }
                keys
            }
        };
        let raw: Vec<Vec<u8>> = req
            .signatures
            .iter()
            .map(|s| {
                B64.decode(s.trim()).map_err(|_| {
                    ApiError::invalid_argument("a signature is not base64")
                        .with_details(serde_json::json!({ "reason": reason::SIGNATURE_INVALID }))
                })
            })
            .collect::<Result<_, _>>()?;
        if raw.is_empty() {
            return Err(ApiError::invalid_argument("no signature supplied")
                .with_details(serde_json::json!({ "reason": reason::SIGNATURE_INVALID })));
        }
        ostro_cluster::compat::require(
            ctx.meta.view(),
            Feature::ModuleSignaturesExt,
            ctx.level_max,
        )
        .map_err(feature_error)?;
        let bytes = module_sign::module_bytes(&ctx, sha, rec.size).await?;
        let added_by = p.token_id.clone();
        let now = ctx.clock.now_ms();
        let sigs = tokio::task::spawn_blocking(move || {
            module_sign::verify_raw(&sha, &bytes, &raw, &keys, now, &added_by)
        })
        .await
        .map_err(|e| ApiError::internal(format!("verification task failed: {e}")))??;
        merge_evidence(&ctx, sha, UploadEvidence { sigs }).await?;
        let stored = ctx
            .meta
            .view()
            .read(|s| s.module(&sha).map(|m| m.value.ext.clone()))
            .and_then(|e| get_module_signatures(&e).ok().flatten())
            .map(|m| m.sigs)
            .unwrap_or_default();
        Ok(AttachSignaturesResult {
            sha256: req.sha256,
            key_ids: stored
                .iter()
                .map(|s| key_id_hex_of(s.alg, &s.key_id))
                .collect(),
        })
    }

    async fn get_sealing_status(&self) -> Result<SealingStatus, ApiError> {
        let ctx = self.ctx()?;
        super::reseal::sealing_status(&ctx)
    }

    async fn reseal(&self) -> Result<ResealResult, ApiError> {
        let ctx = self.ctx()?;
        super::reseal::reseal_all(&ctx).await
    }
}

fn parse_sha(s: &str) -> Result<[u8; 32], ApiError> {
    let bad = || field_error("sha256", "expected 64 lowercase hex digits");
    if s.len() != 64 {
        return Err(bad());
    }
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(s.get(2 * i..2 * i + 2).ok_or_else(bad)?, 16).map_err(|_| bad())?;
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------
// Seed and drift
// ---------------------------------------------------------------------------------

/// What one seed step observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeedStep {
    /// This node has no `[security.modules]`: nothing to do.
    NoSection,
    /// Meta holds a policy equal to the section.
    InSync,
    /// Meta holds a different policy (meta wins).
    Drift,
    /// The seed was written now.
    Seeded,
    /// Not yet possible (feature level, lost CAS, metadata unavailable): retry later.
    Pending,
}

fn same_policy(a: &SecurityPolicy, b: &SecurityPolicy) -> bool {
    let ids = |p: &SecurityPolicy| {
        let mut v: Vec<(u8, Vec<u8>, Vec<u8>)> = p
            .trusted_keys
            .iter()
            .map(|k| (k.alg, k.key_id.clone(), k.public_key.clone()))
            .collect();
        v.sort();
        v
    };
    a.require_signed_modules == b.require_signed_modules && ids(a) == ids(b)
}

/// One seed / drift step.
pub async fn seed_once(ctx: &P14Ctx) -> SeedStep {
    let Some(seed) = module_sign::config_policy(&ctx.cfg.security.modules) else {
        return SeedStep::NoSection;
    };
    let stored = match ctx.meta.view().read(module_sign::meta_policy) {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e.message, "cluster security policy unreadable");
            return SeedStep::Pending;
        }
    };
    if let Some(stored) = stored {
        return if same_policy(&stored, &seed) {
            SeedStep::InSync
        } else {
            SeedStep::Drift
        };
    }
    // Any node holding the section may seed (the proposal is forwarded to the meta leader;
    // the CAS on `ClusterInfo` keeps it to one write cluster-wide).
    if ostro_cluster::compat::require(ctx.meta.view(), Feature::SecurityPolicyExt, ctx.level_max)
        .is_err()
    {
        return SeedStep::Pending;
    }
    let Some(cur) = ctx.meta.view().read(|s| s.cluster().cloned()) else {
        return SeedStep::Pending;
    };
    let mut info = cur.value.clone();
    info.ext = match put_security_policy(&info.ext, &seed) {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "cluster ext 115 not written");
            return SeedStep::Pending;
        }
    };
    match ctx
        .meta
        .propose(MetaCommand::Put {
            key: MetaKey::Cluster,
            expected_version: Some(cur.version),
            record: MetaRecord::Cluster(info),
        })
        .await
    {
        Ok(MetaResult::Ok { .. }) => {
            info!(
                require_signed_modules = seed.require_signed_modules,
                keys = seed.trusted_keys.len(),
                "cluster module policy seeded from [security.modules]"
            );
            SeedStep::Seeded
        }
        _ => SeedStep::Pending,
    }
}

/// The seed / drift task (`reseal::start` runs it with the reseal): `None` without
/// `[security.modules]`. Ends once meta holds a policy (drift logged once).
pub fn seed_task(ctx: &Arc<P14Ctx>, stop: CancellationToken) -> Option<JoinHandle<()>> {
    module_sign::config_policy(&ctx.cfg.security.modules)?;
    let weak = Arc::downgrade(ctx);
    Some(tokio::spawn(async move {
        loop {
            let Some(ctx) = weak.upgrade() else { return };
            match seed_once(&ctx).await {
                SeedStep::NoSection | SeedStep::InSync | SeedStep::Seeded => {
                    metrics::gauge!("ostro_security_policy_drift").set(0.0);
                    return;
                }
                SeedStep::Drift => {
                    metrics::gauge!("ostro_security_policy_drift").set(1.0);
                    warn!(
                        "[security.modules] of this node differs from the cluster module policy \
                         in metadata; the metadata policy applies (ostro_security_policy_drift = 1)"
                    );
                    return;
                }
                SeedStep::Pending => {}
            }
            drop(ctx);
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(SEED_PERIOD) => {}
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha_parsing() {
        assert!(parse_sha(&"ab".repeat(32)).is_ok());
        assert!(parse_sha("ab").is_err());
        assert!(parse_sha(&"zz".repeat(32)).is_err());
    }

    #[test]
    fn drift_compares_require_and_key_material() {
        let k = |b: u8| ext::TrustedKey {
            alg: ext::SIG_ALG_MINISIGN_ED,
            key_id: vec![b; 8],
            public_key: vec![b; 32],
            comment: format!("c{b}"),
        };
        let a = SecurityPolicy {
            require_signed_modules: true,
            trusted_keys: vec![k(1), k(2)],
            auth_mode: 0,
        };
        let mut b = a.clone();
        b.trusted_keys.reverse();
        b.trusted_keys[0].comment = "other".into();
        assert!(same_policy(&a, &b));
        b.require_signed_modules = false;
        assert!(!same_policy(&a, &b));
    }

    #[test]
    fn minisign_key_text_round_trips() {
        let k = ext::TrustedKey {
            alg: ext::SIG_ALG_MINISIGN_ED,
            key_id: vec![7; 8],
            public_key: vec![9; 32],
            comment: String::new(),
        };
        let parsed = signing::parse_trusted_key(&public_key_text(&k)).expect("parse");
        assert_eq!(parsed.to_meta(), k);
    }
}
