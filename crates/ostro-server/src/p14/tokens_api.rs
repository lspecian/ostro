//! Phase 14 (14-17, D-P14-4): `ApiTokensApi`, API token create / list / revoke / rotate and
//! whoami.
//!
//! - Only `otk_` tokens are touched (W10): get / revoke / rotate of a `wkr_` (worker join) or
//!   `njt_` (node join) id answers 404 and listings never include them; those stay with their
//!   Phase 7 / Phase 13 commands.
//! - Stored: `ApiToken{id, hash = sha256(secret), scopes, tenant, created_at_ms,
//!   expires_at_ms}` plus ext 112 `ApiTokenMeta{name, description, created_by, rotated_from}`
//!   once `compat::require(Feature::ApiTokenMetaExt)` passes (finalized level 2). The secret
//!   is only in the create / rotate answer; listings never carry a hash or a secret.
//! - Revoke = `Delete` (CAS): the authenticator rejects the bearer on the next request.
//!
//! # Creation rules (the principal is `ostro_api::auth::current()`)
//!
//! | Rule | Answer otherwise |
//! |---|---|
//! | every requested scope is allowed by the caller's own scopes (`admin` allows all) | 403 `missing_scope` |
//! | a tenant principal creates tokens for its own tenant only (an unset tenant means its own) | 403 `forbidden_tenant` |
//! | `tenant = None` (system scope) carries only `admin` / `read` | 400 |
//! | the named tenant exists (`_`, the default tenant, always does) | 404 |
//! | `no_expiry` only by a system admin (no tenant, `admin`) | 403 `missing_scope` |
//! | expiry = now + (`ttl_ms` or `[auth] default_token_ttl`) on the injected clock | — |
//!
//! Revoke / rotate check the record's tenant themselves: a tenant principal gets the same
//! 404 for another tenant's token as for an unknown id (the guard's `tenant_scope` cannot read
//! the record). Authorization by scope (`admin` for the method) is the guard's (14-15);
//! audit is recorded by the guard (14-18).

use std::sync::{Arc, Weak};

use ostro_api::auth::{self, Principal, Scope, ScopeSet};
use ostro_api::error::reason;
use ostro_api::phase14::{
    ApiTokensApi, CreateTokenRequest, CreatedToken, RotateTokenRequest, TokenInfo, WhoAmI,
};
use ostro_api::{ApiError, ApiExtensions};
use ostro_meta::command::{MetaCommand, MetaKey, MetaRecord, MetaResult};
use ostro_meta::ext::{ApiTokenMeta, get_api_token_meta, put_api_token_meta};
use ostro_meta::{ApiToken, MetaError};
use ostro_types::compat::Feature;

use super::P14Ctx;
use super::tokens;

/// CAS / id-collision attempts.
const RETRIES: usize = 8;
/// Longest token name.
pub const MAX_NAME: usize = 128;
/// Longest token description.
pub const MAX_DESCRIPTION: usize = 1024;

/// Fill `ApiExtensions.api_tokens`.
pub fn install(ctx: &Arc<P14Ctx>, ext: &mut ApiExtensions) {
    ext.api_tokens = Some(Arc::new(TokensService::new(ctx)) as Arc<dyn ApiTokensApi>);
}

// ---------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------

fn meta_unavailable(e: MetaError) -> ApiError {
    ApiError::unavailable(format!("metadata unavailable: {e}"))
}

fn token_not_found(id: &str) -> ApiError {
    ApiError::not_found(format!("token {id} not found"))
}

fn field_error(field: &str, message: impl Into<String>) -> ApiError {
    ApiError::invalid_argument(format!("{field}: {}", message.into()))
        .with_details(serde_json::json!({ "field": field }))
}

/// A system admin: system scope (no tenant) holding `admin`.
pub fn is_system_admin(p: &Principal) -> bool {
    p.tenant.is_none() && p.scopes.contains(Scope::Admin)
}

/// A record the API manages: an `otk_` id whose scopes are API scopes.
fn is_managed(t: &ApiToken) -> bool {
    tokens::is_api_token_id(&t.id) && ScopeSet::parse(&t.scopes).is_ok()
}

/// Whether principal `p` may see / change a token of `tenant`.
fn visible_to(p: &Principal, tenant: Option<&str>) -> bool {
    match &p.tenant {
        None => true,
        Some(pt) => tenant == Some(pt.as_str()),
    }
}

/// `ApiToken` record -> listing row (never the hash).
pub fn info_of(t: &ApiToken) -> TokenInfo {
    let meta = get_api_token_meta(&t.ext)
        .ok()
        .flatten()
        .unwrap_or_default();
    TokenInfo {
        id: t.id.clone(),
        name: meta.name,
        description: meta.description,
        scopes: t.scopes.clone(),
        tenant: t.tenant.clone(),
        created_at_ms: t.created_at_ms,
        expires_at_ms: t.expires_at_ms,
        created_by: meta.created_by,
        rotated_from: meta.rotated_from,
    }
}

/// Every requested scope must be allowed by the caller's own (`admin` allows all).
pub fn check_grantable(p: &Principal, wanted: ScopeSet) -> Result<(), ApiError> {
    if let Some(s) = wanted.iter().find(|s| !p.scopes.allows(*s)) {
        return Err(ApiError::permission_denied(
            reason::MISSING_SCOPE,
            format!(
                "cannot grant scope {:?}: the calling principal does not hold it",
                s.as_str()
            ),
        )
        .with_details(serde_json::json!({
            "reason": reason::MISSING_SCOPE,
            "scope": s.as_str(),
        })));
    }
    Ok(())
}

/// The tenant a token is created for, per the caller (`None` = system scope).
pub fn resolve_tenant(p: &Principal, asked: Option<String>) -> Result<Option<String>, ApiError> {
    match (&p.tenant, asked) {
        (None, asked) => Ok(asked),
        (Some(pt), None) => Ok(Some(pt.clone())),
        (Some(pt), Some(t)) if *pt == t => Ok(Some(t)),
        (Some(pt), Some(t)) => Err(ApiError::permission_denied(
            reason::FORBIDDEN_TENANT,
            format!("a token of tenant {pt} cannot create tokens for tenant {t}"),
        )
        .with_details(serde_json::json!({
            "reason": reason::FORBIDDEN_TENANT,
            "tenant": t,
        }))),
    }
}

/// `tenant = None` (system scope) only with `admin` / `read`.
pub fn check_system_scopes(tenant: Option<&str>, wanted: ScopeSet) -> Result<(), ApiError> {
    if tenant.is_none()
        && let Some(s) = wanted
            .iter()
            .find(|s| !matches!(s, Scope::Admin | Scope::Read))
    {
        return Err(field_error(
            "scopes",
            format!(
                "a system token (no tenant) may only carry admin or read, not {:?}; name a tenant",
                s.as_str()
            ),
        ));
    }
    Ok(())
}

fn check_text(field: &str, s: &str, max: usize, required: bool) -> Result<(), ApiError> {
    if required && s.trim().is_empty() {
        return Err(field_error(field, "is required"));
    }
    if s.len() > max {
        return Err(field_error(field, format!("at most {max} bytes")));
    }
    if s.chars().any(char::is_control) {
        return Err(field_error(field, "must not contain control characters"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------------
// The service
// ---------------------------------------------------------------------------------

/// What a new token record is built from.
struct NewToken {
    name: String,
    description: String,
    scopes: ScopeSet,
    tenant: Option<String>,
    /// Lifetime from the creation instant (`None` = no expiry).
    ttl_ms: Option<u64>,
    created_by: String,
    rotated_from: Option<String>,
}

/// `ApiTokensApi` over the cluster metadata. Holds the context weakly (503 once released).
pub struct TokensService {
    ctx: Weak<P14Ctx>,
}

impl TokensService {
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

    /// The managed record `id` visible to `p` (404 otherwise: unknown id, a `wkr_` / `njt_`
    /// id, or another tenant's token for a tenant principal).
    fn lookup(ctx: &P14Ctx, p: &Principal, id: &str) -> Result<(u64, ApiToken), ApiError> {
        if !tokens::is_api_token_id(id) {
            return Err(token_not_found(id));
        }
        let (version, t) = ctx
            .meta
            .view()
            .read(|s| s.token(id).map(|v| (v.version, v.value.clone())))
            .ok_or_else(|| token_not_found(id))?;
        if !is_managed(&t) || !visible_to(p, t.tenant.as_deref()) {
            return Err(token_not_found(id));
        }
        Ok((version, t))
    }

    /// 404 unless `tenant` is `_` or an existing tenant.
    fn check_tenant_exists(ctx: &P14Ctx, tenant: Option<&str>) -> Result<(), ApiError> {
        match tenant {
            None | Some("_") => Ok(()),
            Some(t) => {
                if !ostro_meta::is_tenant_id(t) {
                    return Err(field_error(
                        "tenant",
                        format!("{t:?} is not a tenant id (^[a-z0-9][a-z0-9-]{{0,62}}$)"),
                    ));
                }
                if ctx.meta.view().read(|s| s.tenant(t).is_none()) {
                    return Err(ApiError::not_found(format!("tenant {t} not found"))
                        .with_details(serde_json::json!({ "tenant": t })));
                }
                Ok(())
            }
        }
    }

    /// A requested lifetime (`ttl_ms = 0` is refused).
    fn lifetime(ttl_ms: u64) -> Result<u64, ApiError> {
        if ttl_ms == 0 {
            return Err(field_error("ttl_ms", "must be > 0"));
        }
        Ok(ttl_ms)
    }

    fn default_ttl_ms(ctx: &P14Ctx) -> u64 {
        u64::try_from(ctx.cfg.auth.default_token_ttl.get().as_millis()).unwrap_or(u64::MAX)
    }

    /// Generate, store (create-only, a fresh id on collision) and answer a token.
    async fn mint(ctx: &P14Ctx, n: NewToken) -> Result<CreatedToken, ApiError> {
        let with_ext = ostro_cluster::compat::require(
            ctx.meta.view(),
            Feature::ApiTokenMetaExt,
            ctx.level_max,
        )
        .is_ok();
        let ext = if with_ext {
            put_api_token_meta(
                &[],
                &ApiTokenMeta {
                    name: n.name.clone(),
                    description: n.description.clone(),
                    created_by: n.created_by.clone(),
                    rotated_from: n.rotated_from.clone(),
                },
            )
            .map_err(|e| ApiError::internal(format!("encoding ApiTokenMeta: {e}")))?
        } else {
            Vec::new()
        };
        // One clock read: `expires_at_ms - created_at_ms` is exactly the lifetime.
        let now = ctx.clock.now_ms();
        let expires_at_ms = n
            .ttl_ms
            .map(|ttl| {
                now.checked_add(ttl)
                    .ok_or_else(|| field_error("ttl_ms", "is too large"))
            })
            .transpose()?;
        for _ in 0..RETRIES {
            let (id, secret, bearer) = tokens::generate_os()
                .map_err(|e| ApiError::unavailable(format!("token generation: {e}")))?;
            let record = ApiToken {
                id: id.as_str().to_owned(),
                hash: tokens::hash(&secret),
                scopes: n.scopes.to_strings(),
                tenant: n.tenant.clone(),
                created_at_ms: now,
                expires_at_ms,
                ext: ext.clone(),
            };
            let res = ctx
                .meta
                .propose(MetaCommand::Put {
                    key: MetaKey::Token(record.id.clone()),
                    expected_version: None,
                    record: MetaRecord::Token(record.clone()),
                })
                .await
                .map_err(meta_unavailable)?;
            match res {
                MetaResult::Ok { .. } => {
                    return Ok(CreatedToken {
                        info: info_of(&record),
                        bearer: bearer.expose().to_owned(),
                    });
                }
                MetaResult::Conflict { .. } | MetaResult::StaleVersion { .. } => continue,
                MetaResult::NotFound => {}
                MetaResult::Invalid(m) => {
                    return Err(ApiError::invalid_argument(format!(
                        "metadata rejected the token: {m}"
                    )));
                }
            }
        }
        Err(ApiError::unavailable(
            "could not allocate a unique token id; retry",
        ))
    }

    /// Delete `id` (CAS on the version read before; re-read on a race).
    async fn delete(ctx: &P14Ctx, p: &Principal, id: &str) -> Result<(), ApiError> {
        for _ in 0..RETRIES {
            let (version, _) = Self::lookup(ctx, p, id)?;
            let res = ctx
                .meta
                .propose(MetaCommand::Delete {
                    key: MetaKey::Token(id.to_owned()),
                    expected_version: version,
                })
                .await
                .map_err(meta_unavailable)?;
            match res {
                MetaResult::Ok { .. } => return Ok(()),
                MetaResult::NotFound => return Err(token_not_found(id)),
                MetaResult::StaleVersion { .. } | MetaResult::Conflict { .. } => {}
                MetaResult::Invalid(m) => {
                    return Err(ApiError::invalid_argument(format!(
                        "metadata rejected the delete: {m}"
                    )));
                }
            }
        }
        Err(ApiError::unavailable(
            "the token is being updated concurrently; retry",
        ))
    }
}

#[async_trait::async_trait]
impl ApiTokensApi for TokensService {
    async fn create_token(&self, req: CreateTokenRequest) -> Result<CreatedToken, ApiError> {
        let p = Self::caller()?;
        let ctx = self.ctx()?;
        check_text("name", &req.name, MAX_NAME, true)?;
        check_text("description", &req.description, MAX_DESCRIPTION, false)?;
        if req.scopes.is_empty() {
            return Err(field_error("scopes", "at least one scope is required"));
        }
        let scopes = ScopeSet::parse(&req.scopes).map_err(|e| field_error("scopes", e))?;
        check_grantable(&p, scopes)?;
        let tenant = resolve_tenant(&p, req.tenant)?;
        check_system_scopes(tenant.as_deref(), scopes)?;
        Self::check_tenant_exists(&ctx, tenant.as_deref())?;
        let ttl_ms = if req.no_expiry {
            if req.ttl_ms.is_some() {
                return Err(field_error("no_expiry", "cannot be combined with ttl_ms"));
            }
            if !is_system_admin(&p) {
                return Err(ApiError::permission_denied(
                    reason::MISSING_SCOPE,
                    "only a system admin may create a token without expiry",
                )
                .with_details(serde_json::json!({
                    "reason": reason::MISSING_SCOPE,
                    "scope": "admin",
                })));
            }
            None
        } else {
            Some(Self::lifetime(
                req.ttl_ms.unwrap_or_else(|| Self::default_ttl_ms(&ctx)),
            )?)
        };
        Self::mint(
            &ctx,
            NewToken {
                name: req.name,
                description: req.description,
                scopes,
                tenant,
                ttl_ms,
                created_by: p.token_id,
                rotated_from: None,
            },
        )
        .await
    }

    async fn list_tokens(&self, tenant: Option<String>) -> Result<Vec<TokenInfo>, ApiError> {
        let p = Self::caller()?;
        let ctx = self.ctx()?;
        let filter = match (&p.tenant, tenant) {
            (None, t) => t,
            (Some(pt), None) => Some(pt.clone()),
            (Some(pt), Some(t)) if *pt == t => Some(t),
            (Some(pt), Some(t)) => {
                return Err(ApiError::permission_denied(
                    reason::FORBIDDEN_TENANT,
                    format!("a token of tenant {pt} cannot list tenant {t}"),
                )
                .with_details(serde_json::json!({
                    "reason": reason::FORBIDDEN_TENANT,
                    "tenant": t,
                })));
            }
        };
        Ok(ctx.meta.view().read(|s| {
            s.tokens()
                .map(|v| &v.value)
                .filter(|t| is_managed(t))
                .filter(|t| filter.is_none() || t.tenant == filter)
                .map(info_of)
                .collect()
        }))
    }

    async fn revoke_token(&self, id: String) -> Result<(), ApiError> {
        let p = Self::caller()?;
        let ctx = self.ctx()?;
        Self::delete(&ctx, &p, &id).await
    }

    async fn rotate_token(
        &self,
        id: String,
        req: RotateTokenRequest,
    ) -> Result<CreatedToken, ApiError> {
        let p = Self::caller()?;
        let ctx = self.ctx()?;
        let (_, old) = Self::lookup(&ctx, &p, &id)?;
        let scopes = ScopeSet::parse(&old.scopes).map_err(ApiError::internal)?;
        check_grantable(&p, scopes)?;
        let meta = get_api_token_meta(&old.ext)
            .ok()
            .flatten()
            .unwrap_or_default();
        let ttl_ms = match (req.ttl_ms, old.expires_at_ms) {
            (Some(ttl), _) => Some(Self::lifetime(ttl)?),
            // The old token's lifetime, from now.
            (None, Some(e)) => Some(e.saturating_sub(old.created_at_ms).max(1)),
            // No expiry stays no expiry only for a system admin.
            (None, None) if is_system_admin(&p) => None,
            (None, None) => Some(Self::default_ttl_ms(&ctx)),
        };
        let created = Self::mint(
            &ctx,
            NewToken {
                name: meta.name,
                description: meta.description,
                scopes,
                tenant: old.tenant.clone(),
                ttl_ms,
                created_by: p.token_id.clone(),
                rotated_from: Some(id.clone()),
            },
        )
        .await?;
        if req.revoke_old {
            match Self::delete(&ctx, &p, &id).await {
                // Revoked concurrently: the outcome the caller asked for.
                Ok(()) => {}
                Err(e) if e.code == ostro_api::ErrorCode::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(created)
    }

    async fn whoami(&self) -> Result<WhoAmI, ApiError> {
        let p = auth::current();
        Ok(WhoAmI {
            token_id: p.token_id,
            tenant: p.tenant,
            scopes: p.scopes.to_strings(),
            kind: p.kind.as_str().to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant_p(t: &str, scopes: &[Scope]) -> Principal {
        Principal::token("otk_x", Some(t.to_owned()), ScopeSet::of(scopes))
    }

    #[test]
    fn grantable_follows_the_callers_scopes() {
        let admin = tenant_p("a", &[Scope::Admin]);
        assert!(check_grantable(&admin, ScopeSet::of(&Scope::ALL)).is_ok());
        let dr = tenant_p("a", &[Scope::Deploy, Scope::Read]);
        assert!(check_grantable(&dr, ScopeSet::of(&[Scope::Deploy])).is_ok());
        let e = check_grantable(&dr, ScopeSet::of(&[Scope::Operate])).unwrap_err();
        assert_eq!(e.reason(), Some(reason::MISSING_SCOPE));
        assert!(check_grantable(&dr, ScopeSet::of(&[Scope::Admin])).is_err());
    }

    #[test]
    fn tenant_resolution() {
        let sys = Principal::dev();
        assert_eq!(resolve_tenant(&sys, None).unwrap(), None);
        assert_eq!(
            resolve_tenant(&sys, Some("b".into())).unwrap(),
            Some("b".into())
        );
        let a = tenant_p("a", &[Scope::Admin]);
        assert_eq!(resolve_tenant(&a, None).unwrap(), Some("a".into()));
        assert_eq!(
            resolve_tenant(&a, Some("a".into())).unwrap(),
            Some("a".into())
        );
        let e = resolve_tenant(&a, Some("b".into())).unwrap_err();
        assert_eq!(e.reason(), Some(reason::FORBIDDEN_TENANT));
    }

    #[test]
    fn system_tokens_only_admin_or_read() {
        assert!(check_system_scopes(None, ScopeSet::of(&[Scope::Admin, Scope::Read])).is_ok());
        assert!(check_system_scopes(None, ScopeSet::of(&[Scope::Deploy])).is_err());
        assert!(check_system_scopes(Some("a"), ScopeSet::of(&[Scope::Deploy])).is_ok());
    }

    #[test]
    fn visibility() {
        let a = tenant_p("a", &[Scope::Admin]);
        assert!(visible_to(&a, Some("a")));
        assert!(!visible_to(&a, Some("b")));
        assert!(!visible_to(&a, None));
        assert!(visible_to(&Principal::dev(), Some("b")));
        assert!(is_system_admin(&Principal::system()));
        assert!(!is_system_admin(&a));
    }
}
