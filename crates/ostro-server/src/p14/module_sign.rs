//! Phase 14 (14-19, SEC-03, D-P14-10 B1a-c, D-P14-14/15): signed-module enforcement.
//!
//! # Policy
//!
//! The **cluster policy** is `ClusterInfo` ext 115 [`SecurityPolicy`]. While meta holds none,
//! this node's `[security.modules]` stands in for it (the seed, written once by the meta
//! leader through `security_api::seed_loop`). A node without `[security.modules]` therefore
//! still enforces a policy stored in meta: the policy is cluster-wide.
//!
//! The **effective policy** of a tenant ([`effective`]):
//! `require = cluster.require_signed_modules || tenant.require_signed_modules == Some(true)`
//! (a tenant's `Some(false)` never relaxes) and `keys = cluster keys ∪ tenant keys` (ext 116
//! `TenantSecurity` of that tenant's `Tenant` record). Tenant keys only apply to that tenant.
//! The default tenant (`None` / `_`) has no `Tenant` record: cluster policy only.
//!
//! # Gates
//!
//! | Gate | Called from | Rule |
//! |---|---|---|
//! | [`upload_gate`] | `upload_module`, right after the sha check (before the existing-sha lookup, compilation, every meta write, auto-activation) | required + no signature → 403 `signature_required`; any signature supplied and none valid → 400 `signature_invalid` (also when not required); valid → evidence |
//! | [`activation_gate`] | `activate_workflow` | required for the activating tenant → the stored ext 113 evidence must re-verify over the blob with a key of the tenant's effective set (by fingerprint), else 403 `signature_required` |
//! | [`version_gate`] | start (explicit version), migrate target, rerun target | required → the active version passes at once; any other version needs evidence as for activation, else 403 |
//!
//! Already-active versions keep serving and runs in flight are never touched: the gates only
//! run on uploads, activations and explicit version selections.
//!
//! # Evidence (ext 113)
//!
//! `ModuleSig{alg, key_fingerprint = sha256(alg ‖ public_key), key_id, sig, added_at_ms,
//! added_by}` per verified signature ([`signing::ModuleSigEvidence::to_module_sig`]); written
//! only once `compat::require(Feature::ModuleSignaturesExt)` passes (finalized level 2; below
//! it signatures are still verified but not persisted). Merging keeps one signature per key
//! fingerprint (the newest) and at most [`signing::MAX_SIGS`] (oldest dropped).
//!
//! Module bytes for re-verification come from this node's blob store; a node that does not
//! hold the blob answers 503 (retry through a node that does).

use std::sync::Mutex;

use ostro_api::ApiError;
use ostro_api::auth;
use ostro_api::error::reason;
use ostro_meta::MetaState;
use ostro_meta::ext::{
    self, ModuleSig, ModuleSignatures, SecurityPolicy, TenantSecurity, get_module_signatures,
    get_security_policy, get_tenant_security, put_module_signatures,
};
use ostro_meta::{MetaCommand, MetaKey, MetaRecord, MetaResult};
use ostro_types::compat::Feature;
use tracing::{debug, warn};

use super::signing::{self, KeyFamily, SigError, TrustedKey};
use super::{P14Ctx, UploadEvidence};
use crate::config::{SecurityModulesSection, ServerConfig};
use crate::control::names::DEFAULT_TENANT_TOKEN;

/// CAS attempts on a concurrently updated record before answering 503.
const CAS_RETRIES: usize = 8;
/// Largest key file `[security.modules]` reads.
const MAX_KEY_FILE: u64 = 64 * 1024;

// ---------------------------------------------------------------------------------
// Policy
// ---------------------------------------------------------------------------------

/// The `Tenant` record id of a request tenant: `None` for the default tenant (`None`, `""`,
/// `_`), which has no record.
pub fn tenant_record_id(tenant: Option<&str>) -> Option<&str> {
    tenant.filter(|t| !t.is_empty() && *t != DEFAULT_TENANT_TOKEN)
}

/// One trusted key from `[security.modules]`: the key text itself, or a path to it.
fn config_key(entry: &str, family: KeyFamily) -> Result<TrustedKey, String> {
    let key = match signing::parse_trusted_key(entry) {
        Ok(k) => k,
        Err(text_err) => {
            let path = std::path::Path::new(entry.trim());
            let meta = std::fs::metadata(path)
                .map_err(|_| format!("not a key ({text_err}) and not a readable file"))?;
            if meta.len() > MAX_KEY_FILE {
                return Err(format!("{} is larger than 64 KiB", path.display()));
            }
            let text = std::fs::read_to_string(path)
                .map_err(|e| format!("reading {}: {e}", path.display()))?;
            signing::parse_trusted_key(&text).map_err(|e| format!("{}: {e}", path.display()))?
        }
    };
    if key.alg_family != family {
        return Err(format!(
            "a {:?} key in the {:?} list",
            key.alg_family, family
        ));
    }
    Ok(key)
}

/// `[security.modules]` as a policy; `None` when the section is absent (all defaults).
/// Unusable key entries are logged and skipped. Parsed once per distinct section.
pub fn config_policy(sec: &SecurityModulesSection) -> Option<SecurityPolicy> {
    static CACHE: Mutex<Vec<(SecurityModulesSection, Option<SecurityPolicy>)>> =
        Mutex::new(Vec::new());
    if *sec == SecurityModulesSection::default() {
        return None;
    }
    let mut cache = CACHE.lock().unwrap_or_else(|p| p.into_inner());
    if let Some((_, p)) = cache.iter().find(|(s, _)| s == sec) {
        return p.clone();
    }
    let mut keys: Vec<ext::TrustedKey> = Vec::new();
    for (list, family, field) in [
        (&sec.minisign_keys, KeyFamily::Minisign, "minisign_keys"),
        (&sec.cosign_keys, KeyFamily::Cosign, "cosign_keys"),
    ] {
        for entry in list {
            match config_key(entry, family) {
                Ok(k) => {
                    let m = k.to_meta();
                    if !keys.iter().any(|x| x.key_id == m.key_id && x.alg == m.alg) {
                        keys.push(m);
                    }
                }
                Err(e) => warn!(field, error = %e, "[security.modules]: key skipped"),
            }
        }
    }
    let p = Some(SecurityPolicy {
        require_signed_modules: sec.require_signed,
        trusted_keys: keys,
        auth_mode: ext::AUTH_MODE_UNSET,
    });
    cache.push((sec.clone(), p.clone()));
    p
}

/// Where the cluster policy came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicySource {
    /// `ClusterInfo` ext 115.
    Meta,
    /// This node's `[security.modules]` (meta holds none yet).
    Config,
    /// Neither: nothing required, no keys.
    Default,
}

/// The stored ext 115 policy, if any (an undecodable value fails closed: 503).
pub fn meta_policy(state: &MetaState) -> Result<Option<SecurityPolicy>, ApiError> {
    let Some(c) = state.cluster() else {
        return Ok(None);
    };
    get_security_policy(&c.value.ext)
        .map_err(|e| ApiError::unavailable(format!("cluster security policy (ext 115): {e}")))
}

/// The cluster policy: meta's ext 115, else this node's `[security.modules]`, else default.
pub fn cluster_policy(
    cfg: &ServerConfig,
    state: &MetaState,
) -> Result<(SecurityPolicy, PolicySource), ApiError> {
    if let Some(p) = meta_policy(state)? {
        return Ok((p, PolicySource::Meta));
    }
    Ok(match config_policy(&cfg.security.modules) {
        Some(p) => (p, PolicySource::Config),
        None => (SecurityPolicy::default(), PolicySource::Default),
    })
}

/// The ext 116 tightening of `tenant` (`None` for the default tenant or an unknown one).
pub fn tenant_security(
    state: &MetaState,
    tenant: Option<&str>,
) -> Result<Option<TenantSecurity>, ApiError> {
    let Some(id) = tenant_record_id(tenant) else {
        return Ok(None);
    };
    let Some(t) = state.tenant(id) else {
        return Ok(None);
    };
    get_tenant_security(&t.value.ext)
        .map_err(|e| ApiError::unavailable(format!("tenant {id} security (ext 116): {e}")))
}

/// A tenant's effective policy.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EffectivePolicy {
    /// Uploads, activations and explicit version selections need valid signatures.
    pub require: bool,
    /// Cluster keys, then the tenant's own keys (unusable stored keys skipped).
    pub keys: Vec<TrustedKey>,
}

/// The effective policy from the cluster policy and the tenant's ext 116 (pure).
pub fn effective(cluster: &SecurityPolicy, tenant: Option<&TenantSecurity>) -> EffectivePolicy {
    let require = cluster.require_signed_modules
        || tenant.is_some_and(|t| t.require_signed_modules == Some(true));
    let mut keys: Vec<TrustedKey> = Vec::new();
    let stored = cluster
        .trusted_keys
        .iter()
        .chain(tenant.into_iter().flat_map(|t| t.trusted_keys.iter()));
    for k in stored {
        match TrustedKey::from_meta(k) {
            Ok(k) => {
                if !keys.contains(&k) {
                    keys.push(k);
                }
            }
            Err(e) => debug!(error = %e, "stored trusted key skipped"),
        }
    }
    EffectivePolicy { require, keys }
}

/// The effective policy of `tenant` on this node's meta view.
pub fn effective_policy(ctx: &P14Ctx, tenant: Option<&str>) -> Result<EffectivePolicy, ApiError> {
    ctx.meta.view().read(|s| {
        let (cluster, _) = cluster_policy(&ctx.cfg, s)?;
        let ts = tenant_security(s, tenant)?;
        Ok(effective(&cluster, ts.as_ref()))
    })
}

// ---------------------------------------------------------------------------------
// Errors and helpers
// ---------------------------------------------------------------------------------

/// Display form of a stored key id ([`signing::key_id_hex`] rules by alg).
pub fn key_id_hex_of(alg: u8, key_id: &[u8]) -> String {
    match signing::Alg::from_u8(alg).map(signing::Alg::family) {
        Some(KeyFamily::Minisign) => key_id.iter().rev().map(|b| format!("{b:02X}")).collect(),
        _ => key_id.iter().map(|b| format!("{b:02x}")).collect(),
    }
}

fn sha_hex(sha: &[u8; 32]) -> String {
    ostro_blob::hex(sha)
}

/// 403 `signature_required`.
pub fn signature_required(sha: &[u8; 32], why: impl Into<String>) -> ApiError {
    let why = why.into();
    ApiError::permission_denied(
        reason::SIGNATURE_REQUIRED,
        format!(
            "module {} needs a valid signature by a trusted key: {why}",
            sha_hex(sha)
        ),
    )
    .with_details(serde_json::json!({
        "reason": reason::SIGNATURE_REQUIRED,
        "sha256": sha_hex(sha),
        "why": why,
    }))
}

/// 400 `signature_invalid`.
pub fn signature_invalid(sha: &[u8; 32], err: &SigError, key_ids: &[String]) -> ApiError {
    ApiError::invalid_argument(format!(
        "module {}: no supplied signature is valid: {err}",
        sha_hex(sha)
    ))
    .with_details(serde_json::json!({
        "reason": reason::SIGNATURE_INVALID,
        "sha256": sha_hex(sha),
        "error": err.to_string(),
        "key_ids": key_ids,
    }))
}

/// The actor recorded in `ModuleSig.added_by`: the token id, else `system`.
fn actor() -> String {
    let p = auth::current();
    if p.is_anonymous() {
        "system".to_owned()
    } else {
        p.token_id
    }
}

/// Whether ext 113 may be written (finalized level 2).
pub fn evidence_storable(ctx: &P14Ctx) -> bool {
    ostro_cluster::compat::require(ctx.meta.view(), Feature::ModuleSignaturesExt, ctx.level_max)
        .is_ok()
}

/// Parse and verify `raw` signatures over `bytes` with `keys`.
pub fn verify_raw(
    sha: &[u8; 32],
    bytes: &[u8],
    raw: &[Vec<u8>],
    keys: &[TrustedKey],
    now_ms: u64,
    added_by: &str,
) -> Result<Vec<ModuleSig>, ApiError> {
    if raw.len() > signing::MAX_SIGS {
        return Err(signature_invalid(
            sha,
            &SigError::TooLarge("signature count"),
            &[],
        ));
    }
    let mut parsed = Vec::with_capacity(raw.len());
    let mut worst: Option<SigError> = None;
    let mut key_ids: Vec<String> = Vec::new();
    for r in raw {
        match signing::parse_signature(r) {
            Ok(p) => {
                if !p.key_id().is_empty() {
                    key_ids.push(key_id_hex_of(p.alg().as_u8(), p.key_id()));
                }
                parsed.push(p);
            }
            Err(e) => worst = Some(e),
        }
    }
    match signing::verify_module(bytes, &parsed, keys) {
        Ok(evs) => Ok(evs
            .iter()
            .map(|e| e.to_module_sig(now_ms, added_by))
            .collect()),
        Err(SigError::Unsigned) => Err(signature_invalid(
            sha,
            &worst.unwrap_or(SigError::Unsigned),
            &key_ids,
        )),
        Err(e) => Err(signature_invalid(sha, &e, &key_ids)),
    }
}

/// `existing` with `new` merged: one signature per key fingerprint (the newest), at most
/// [`signing::MAX_SIGS`] (oldest dropped).
pub fn merge_sigs(existing: Vec<ModuleSig>, new: Vec<ModuleSig>) -> Vec<ModuleSig> {
    let mut out = existing;
    for n in new {
        out.retain(|e| e.key_fingerprint != n.key_fingerprint);
        out.push(n);
    }
    while out.len() > signing::MAX_SIGS {
        out.remove(0);
    }
    out
}

/// The module bytes from this node's blob store.
pub async fn module_bytes(ctx: &P14Ctx, sha: [u8; 32], size: u64) -> Result<Vec<u8>, ApiError> {
    let blobs = ctx
        .blobs()
        .cloned()
        .ok_or_else(|| ApiError::unavailable("no blob store on this node yet"))?;
    tokio::task::spawn_blocking(move || {
        blobs.get(&ostro_blob::BlobRef {
            sha256: sha,
            len: size,
        })
    })
    .await
    .map_err(|e| ApiError::internal(format!("blob store task failed: {e}")))?
    .map_err(|e| {
        ApiError::unavailable(format!(
            "module {} bytes are not readable on this node: {e}",
            sha_hex(&sha)
        ))
    })
}

/// The stored evidence of `sha` must re-verify over the blob with a key of `keys` (by
/// fingerprint). 404 for an unknown module, 403 `signature_required` otherwise.
pub async fn evidence_check(
    ctx: &P14Ctx,
    sha: [u8; 32],
    keys: &[TrustedKey],
) -> Result<(), ApiError> {
    let rec = ctx
        .meta
        .view()
        .read(|s| s.module(&sha).map(|m| m.value.clone()))
        .ok_or_else(|| ApiError::not_found(format!("module {} not found", sha_hex(&sha))))?;
    let stored = match get_module_signatures(&rec.ext) {
        Ok(Some(ms)) if !ms.sigs.is_empty() => ms.sigs,
        Ok(_) => {
            return Err(signature_required(
                &sha,
                "the module carries no signature; attach one (POST /v1/modules/{sha}/signatures)",
            ));
        }
        Err(e) => {
            return Err(ApiError::unavailable(format!(
                "module signatures (ext 113): {e}"
            )));
        }
    };
    if keys.is_empty() {
        return Err(signature_required(&sha, "no trusted key is configured"));
    }
    let fps: Vec<[u8; 32]> = keys.iter().map(signing::key_fingerprint).collect();
    if !stored.iter().any(|s| fps.contains(&s.key_fingerprint)) {
        return Err(signature_required(
            &sha,
            "no stored signature was made by a currently trusted key",
        ));
    }
    let bytes = module_bytes(ctx, sha, rec.size).await?;
    let keys = keys.to_vec();
    tokio::task::spawn_blocking(move || signing::reverify(&bytes, &stored, &keys))
        .await
        .map_err(|e| ApiError::internal(format!("verification task failed: {e}")))?
        .map(|_| ())
        .map_err(|e| signature_required(&sha, format!("stored signatures do not verify: {e}")))
}

// ---------------------------------------------------------------------------------
// Hooks
// ---------------------------------------------------------------------------------

/// The upload gate (B1b) for `tenant`.
pub async fn upload_gate(
    ctx: &P14Ctx,
    tenant: Option<&str>,
    sha: [u8; 32],
    bytes: &[u8],
    sigs: &[Vec<u8>],
) -> Result<UploadEvidence, ApiError> {
    let pol = effective_policy(ctx, tenant)?;
    if sigs.is_empty() {
        if pol.require {
            return Err(signature_required(&sha, "the upload carries no signature"));
        }
        return Ok(UploadEvidence::default());
    }
    let sigs = verify_raw(&sha, bytes, sigs, &pol.keys, ctx.clock.now_ms(), &actor())?;
    if !evidence_storable(ctx) {
        debug!(
            sha256 = %sha_hex(&sha),
            "valid signatures not persisted: ext 113 needs finalized level 2"
        );
        return Ok(UploadEvidence::default());
    }
    Ok(UploadEvidence { sigs })
}

/// Write `evidence` into a new module record's ext (ext 113).
pub fn evidence_ext(evidence: &UploadEvidence, ext: &mut Vec<u8>) {
    if evidence.is_empty() {
        return;
    }
    match put_module_signatures(
        ext,
        &ModuleSignatures {
            sigs: evidence.sigs.clone(),
        },
    ) {
        Ok(v) => *ext = v,
        Err(e) => warn!(error = %e, "module signatures (ext 113) not written"),
    }
}

/// Merge `evidence` into the stored record of `sha` (CAS).
pub async fn merge_evidence(
    ctx: &P14Ctx,
    sha: [u8; 32],
    evidence: UploadEvidence,
) -> Result<(), ApiError> {
    if evidence.is_empty() {
        return Ok(());
    }
    for _ in 0..CAS_RETRIES {
        let Some(cur) = ctx.meta.view().read(|s| s.module(&sha).cloned()) else {
            return Err(ApiError::not_found(format!(
                "module {} not found",
                sha_hex(&sha)
            )));
        };
        let existing = get_module_signatures(&cur.value.ext)
            .map_err(|e| ApiError::internal(format!("module ext 113: {e}")))?
            .map(|m| m.sigs)
            .unwrap_or_default();
        let merged = merge_sigs(existing.clone(), evidence.sigs.clone());
        if merged == existing {
            return Ok(());
        }
        let mut record = cur.value.clone();
        record.ext = put_module_signatures(&record.ext, &ModuleSignatures { sigs: merged })
            .map_err(|e| ApiError::internal(format!("module ext 113: {e}")))?;
        match ctx
            .meta
            .propose(MetaCommand::Put {
                key: MetaKey::Module(sha),
                expected_version: Some(cur.version),
                record: MetaRecord::Module(record),
            })
            .await
            .map_err(|e| ApiError::unavailable(format!("metadata unavailable: {e}")))?
        {
            MetaResult::Ok { .. } => return Ok(()),
            MetaResult::StaleVersion { .. } | MetaResult::Conflict { .. } => {}
            other => {
                return Err(ApiError::internal(format!(
                    "metadata rejected the module signatures update: {other:?}"
                )));
            }
        }
    }
    Err(ApiError::unavailable(
        "the module record is being updated concurrently; retry",
    ))
}

/// The version gate (B1a): an explicit version selection by `tenant`.
pub async fn version_gate(
    ctx: &P14Ctx,
    tenant: Option<&str>,
    workflow: &str,
    version: u32,
    sha: [u8; 32],
) -> Result<(), ApiError> {
    let pol = effective_policy(ctx, tenant)?;
    if !pol.require {
        return Ok(());
    }
    // W3: the active version was gated when it was activated (or predates enforcement and
    // keeps serving).
    let active = ctx
        .meta
        .view()
        .read(|s| s.active(workflow).map(|a| a.value.version));
    if active == Some(version) {
        return Ok(());
    }
    evidence_check(ctx, sha, &pol.keys).await
}

/// The activation gate (B1c) for the activating `tenant`.
pub async fn activation_gate(
    ctx: &P14Ctx,
    tenant: Option<&str>,
    sha: [u8; 32],
) -> Result<(), ApiError> {
    let pol = effective_policy(ctx, tenant)?;
    if !pol.require {
        return Ok(());
    }
    evidence_check(ctx, sha, &pol.keys).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(id: u8) -> ext::TrustedKey {
        ext::TrustedKey {
            alg: ext::SIG_ALG_MINISIGN_ED,
            key_id: vec![id; 8],
            public_key: vec![id; 32],
            comment: String::new(),
        }
    }

    fn sig(fp: u8, at: u64) -> ModuleSig {
        ModuleSig {
            alg: ext::SIG_ALG_MINISIGN_ED,
            key_fingerprint: [fp; 32],
            key_id: vec![fp; 8],
            sig: vec![fp],
            added_at_ms: at,
            added_by: "t".into(),
        }
    }

    #[test]
    fn tenant_tightening_never_relaxes() {
        let on = SecurityPolicy {
            require_signed_modules: true,
            ..Default::default()
        };
        let off = SecurityPolicy::default();
        let relax = TenantSecurity {
            require_signed_modules: Some(false),
            trusted_keys: vec![],
        };
        let tighten = TenantSecurity {
            require_signed_modules: Some(true),
            trusted_keys: vec![key(2)],
        };
        assert!(effective(&on, Some(&relax)).require);
        assert!(effective(&on, None).require);
        assert!(!effective(&off, None).require);
        assert!(!effective(&off, Some(&relax)).require);
        assert!(effective(&off, Some(&tighten)).require);
    }

    #[test]
    fn keys_are_cluster_then_tenant_and_bad_keys_are_skipped() {
        let mut bad = key(9);
        bad.public_key.truncate(3);
        let cluster = SecurityPolicy {
            trusted_keys: vec![key(1), bad],
            ..Default::default()
        };
        let t = TenantSecurity {
            require_signed_modules: None,
            trusted_keys: vec![key(2), key(1)],
        };
        let e = effective(&cluster, Some(&t));
        let ids: Vec<u8> = e.keys.iter().map(|k| k.key_id[0]).collect();
        assert_eq!(ids, [1, 2]);
    }

    #[test]
    fn merge_keeps_one_per_fingerprint_and_caps() {
        let m = merge_sigs(vec![sig(1, 1), sig(2, 2)], vec![sig(1, 3)]);
        assert_eq!(
            m.iter()
                .map(|s| (s.key_fingerprint[0], s.added_at_ms))
                .collect::<Vec<_>>(),
            [(2, 2), (1, 3)]
        );
        let m = merge_sigs(
            vec![sig(1, 1), sig(2, 2), sig(3, 3), sig(4, 4)],
            vec![sig(5, 5)],
        );
        assert_eq!(m.len(), signing::MAX_SIGS);
        assert_eq!(m[0].key_fingerprint[0], 2);
    }

    #[test]
    fn default_tenant_has_no_record() {
        assert_eq!(tenant_record_id(None), None);
        assert_eq!(tenant_record_id(Some("_")), None);
        assert_eq!(tenant_record_id(Some("")), None);
        assert_eq!(tenant_record_id(Some("acme")), Some("acme"));
    }

    #[test]
    fn key_id_display_follows_the_family() {
        assert_eq!(
            key_id_hex_of(ext::SIG_ALG_MINISIGN_ED, &[1, 2, 3, 4, 5, 6, 7, 0xab]),
            "AB07060504030201"
        );
        assert_eq!(key_id_hex_of(ext::SIG_ALG_COSIGN_P256, &[1, 0xab]), "01ab");
    }

    #[test]
    fn absent_config_section_is_no_policy() {
        assert_eq!(config_policy(&SecurityModulesSection::default()), None);
        let p = config_policy(&SecurityModulesSection {
            require_signed: true,
            minisign_keys: vec!["not a key and not a file".into()],
            cosign_keys: vec![],
        })
        .expect("policy");
        assert!(p.require_signed_modules);
        assert!(p.trusted_keys.is_empty());
    }
}
