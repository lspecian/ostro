//! Phase 14 API contract (14-01; D-P14-8/4/11/10/29): tenants ([`TenantsApi`]), API tokens
//! ([`ApiTokensApi`]), the audit query ([`AuditApi`]) and module security policy
//! ([`SecurityApi`]).
//!
//! Like Phases 8, 11 and 13, the operations live in extension traits held in optional slots
//! of [`crate::ApiExtensions`] (`tenants`, `api_tokens`, `audit`, `security`); an absent slot
//! answers 404 once the routes land (14-02). The traits take and return wire DTOs directly
//! (serde + utoipa derives). Every method has a default body answering `Unimplemented` and
//! naming the plan that implements it. Authorization (scopes, tenant scoping, audit) is the
//! server guard's (14-03/14-15), never these traits'.
//!
//! # Routes (14-02; RESERVATIONS §11 P14)
//!
//! | Method | Path | Trait method |
//! |---|---|---|
//! | GET / POST | `/v1/tenants` | [`TenantsApi::list_tenants`] / [`TenantsApi::create_tenant`] (14-17) |
//! | GET / PATCH / DELETE | `/v1/tenants/{id}` | [`TenantsApi::get_tenant`] / `update_tenant` / `delete_tenant` (14-17) |
//! | POST | `/v1/tenants/{id}/{suspend,resume}` | [`TenantsApi::suspend_tenant`] / `resume_tenant` (14-17) |
//! | GET / POST | `/v1/auth/tokens` | [`ApiTokensApi::list_tokens`] / [`ApiTokensApi::create_token`] (14-17) |
//! | DELETE | `/v1/auth/tokens/{id}` | [`ApiTokensApi::revoke_token`] (14-17) |
//! | POST | `/v1/auth/tokens/{id}/rotate` | [`ApiTokensApi::rotate_token`] (14-17) |
//! | GET | `/v1/auth/whoami` | [`ApiTokensApi::whoami`] (14-17) |
//! | GET | `/v1/audit` | [`AuditApi::query`] (14-18) |
//! | GET / PUT | `/v1/security/modules/policy` | [`SecurityApi::get_module_policy`] / `set_module_policy` (14-19) |
//! | POST / DELETE | `/v1/security/modules/keys`, `/v1/security/modules/keys/{key_id}` | [`SecurityApi::add_trusted_key`] / `remove_trusted_key` (14-19) |
//! | POST | `/v1/modules/{sha}/signatures` | [`SecurityApi::attach_signatures`] (14-19) |

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::{ApiError, ErrorCode};
use crate::phase7::RateSpecDto;

fn lands_in(method: &str, plan: &str) -> ApiError {
    ApiError::new(
        ErrorCode::Unimplemented,
        format!("{method} lands in {plan}"),
    )
    .with_details(serde_json::json!({ "method": method }))
}

// ---- tenants (D-P14-8) --------------------------------------------------------------------

/// `POST /v1/tenants` body: a new tenant and its quotas (unset = unlimited).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TenantSpec {
    /// Tenant id `^[a-z0-9][a-z0-9-]{0,62}$` (`_` is never creatable).
    pub id: String,
    /// DRR weight (clamped to `[1, 10000]`, default 1).
    #[serde(default)]
    pub weight: Option<u32>,
    /// Leased activations at once.
    #[serde(default)]
    pub max_concurrency: Option<u32>,
    /// Fuel refill per second.
    #[serde(default)]
    pub fuel_per_s: Option<u64>,
    /// Fuel bucket size.
    #[serde(default)]
    pub fuel_burst: Option<u64>,
    /// Instance memory reserved at grant.
    #[serde(default)]
    pub memory_bytes: Option<u64>,
    /// Starts / signals / token completions / webhook deliveries.
    #[serde(default)]
    pub event_rate: Option<RateSpecDto>,
}

/// A tenant as stored.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TenantInfo {
    /// Quotas and id.
    #[serde(flatten)]
    pub spec: TenantSpec,
    /// Suspended tenants are refused at ingress and held at dispatch.
    pub suspended: bool,
    /// Creation time (Unix ms).
    pub created_at_ms: u64,
}

/// `PATCH /v1/tenants/{id}` body: only the present fields change. `Some(None)` clears a quota
/// (unlimited); absent keeps it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TenantPatch {
    /// New weight.
    #[serde(default, deserialize_with = "double_option")]
    pub weight: Option<Option<u32>>,
    /// New concurrency cap.
    #[serde(default, deserialize_with = "double_option")]
    pub max_concurrency: Option<Option<u32>>,
    /// New fuel refill.
    #[serde(default, deserialize_with = "double_option")]
    pub fuel_per_s: Option<Option<u64>>,
    /// New fuel burst.
    #[serde(default, deserialize_with = "double_option")]
    pub fuel_burst: Option<Option<u64>>,
    /// New memory reservation.
    #[serde(default, deserialize_with = "double_option")]
    pub memory_bytes: Option<Option<u64>>,
    /// New event rate.
    #[serde(default, deserialize_with = "double_option")]
    pub event_rate: Option<Option<RateSpecDto>>,
}

/// `TenantPatch` fields (14-17): an absent key stays `None` (keep), JSON `null` becomes
/// `Some(None)` (clear the quota), a value becomes `Some(Some(v))`. Used with
/// `#[serde(default)]`, so absence never reaches this function.
fn double_option<'de, T, D>(d: D) -> Result<Option<Option<T>>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Option::<T>::deserialize(d).map(Some)
}

// ---- API tokens (D-P14-4) -----------------------------------------------------------------

/// `POST /v1/auth/tokens` body.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateTokenRequest {
    /// Operator-chosen name.
    pub name: String,
    /// Free-form description.
    #[serde(default)]
    pub description: String,
    /// API scopes (`admin`, `deploy`, `operate`, `trigger`, `read`).
    pub scopes: Vec<String>,
    /// Tenant (`None` = system scope: only with `admin` or `read`).
    #[serde(default)]
    pub tenant: Option<String>,
    /// Lifetime; `None` = `[auth] default_token_ttl`.
    #[serde(default)]
    pub ttl_ms: Option<u64>,
    /// No expiry at all (admin only).
    #[serde(default)]
    pub no_expiry: bool,
}

/// An API token as listed. Never carries the secret or its hash.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TokenInfo {
    /// `otk_…` id.
    pub id: String,
    /// Name (ext 112).
    pub name: String,
    /// Description (ext 112).
    pub description: String,
    /// Scopes.
    pub scopes: Vec<String>,
    /// Tenant (`None` = system scope).
    pub tenant: Option<String>,
    /// Creation time (Unix ms).
    pub created_at_ms: u64,
    /// Expiry (Unix ms), `None` = never.
    pub expires_at_ms: Option<u64>,
    /// Creator (ext 112).
    pub created_by: String,
    /// Predecessor when rotated (ext 112).
    pub rotated_from: Option<String>,
}

/// Create / rotate answer: the only time the bearer is shown.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreatedToken {
    /// The stored token.
    pub info: TokenInfo,
    /// `ostro_otk_<16 base32>_<52 base32>`.
    pub bearer: String,
}

impl std::fmt::Debug for CreatedToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreatedToken")
            .field("info", &self.info)
            .field("bearer", &"<redacted>")
            .finish()
    }
}

/// `POST /v1/auth/tokens/{id}/rotate` body.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RotateTokenRequest {
    /// Revoke the old token once the new one exists.
    #[serde(default)]
    pub revoke_old: bool,
    /// Lifetime of the new token; `None` = the old token's remaining policy.
    #[serde(default)]
    pub ttl_ms: Option<u64>,
}

/// `GET /v1/auth/whoami`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WhoAmI {
    /// Token id or `dev` / `system`.
    pub token_id: String,
    /// Tenant (`None` = system scope).
    pub tenant: Option<String>,
    /// Scopes.
    pub scopes: Vec<String>,
    /// `token`, `dev`, `system`, `node` or `anonymous`.
    pub kind: String,
}

// ---- audit (D-P14-11) ---------------------------------------------------------------------

/// `GET /v1/audit` query.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AuditQuery {
    /// Tenant filter (forced to the principal's tenant for tenant principals).
    #[serde(default)]
    pub tenant: Option<String>,
    /// Actor filter (token id).
    #[serde(default)]
    pub actor: Option<String>,
    /// Action filter (exact or `prefix.*`).
    #[serde(default)]
    pub action: Option<String>,
    /// Inclusive lower bound (Unix ms).
    #[serde(default)]
    pub since_ms: Option<u64>,
    /// Exclusive upper bound (Unix ms).
    #[serde(default)]
    pub until_ms: Option<u64>,
    /// Page size (server-capped).
    #[serde(default)]
    pub limit: Option<u32>,
    /// Continuation cursor from the previous page.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// One journal record as served (merged across nodes by `(ts_ms, node, seq)`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AuditRecordView {
    /// Journal sequence on its node.
    pub seq: u64,
    /// Record time (Unix ms).
    pub ts_ms: u64,
    /// Node id.
    pub node: u64,
    /// Actor id.
    pub actor: String,
    /// Actor kind.
    pub actor_kind: String,
    /// Tenant.
    pub tenant: Option<String>,
    /// Action.
    pub action: String,
    /// Target.
    pub target: String,
    /// `ok`, `denied:<reason>` or `failed:<message>`.
    pub outcome: String,
    /// Request id.
    pub request_id: Option<String>,
    /// Extra fields.
    pub details: BTreeMap<String, String>,
    /// Chain hash (hex).
    pub hash: String,
}

/// `GET /v1/audit` answer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AuditPage {
    /// Records, oldest first.
    pub records: Vec<AuditRecordView>,
    /// Cursor for the next page, `None` at the end.
    pub cursor: Option<String>,
    /// Members that did not answer the fan-out (the page is partial).
    pub partial_nodes: Vec<u64>,
}

// ---- module security (D-P14-10) -----------------------------------------------------------

/// A trusted signing key on the wire.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TrustedKeySpec {
    /// `minisign` or `cosign-p256`.
    pub alg: String,
    /// Key id (minisign key id hex / cosign hint).
    pub key_id: String,
    /// Public key: a minisign public key line or a PEM SPKI.
    pub public_key: String,
    /// Operator comment.
    #[serde(default)]
    pub comment: String,
    /// `Some(t)` = a tenant key (accepted for that tenant's uploads only).
    #[serde(default)]
    pub tenant: Option<String>,
}

/// `GET|PUT /v1/security/modules/policy`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ModulePolicy {
    /// Signed modules required cluster-wide (with `tenant`: required for that tenant).
    pub require_signed_modules: bool,
    /// Cluster-wide trusted keys.
    #[serde(default)]
    pub trusted_keys: Vec<TrustedKeySpec>,
    /// `Some(t)` = tenant `t`'s policy (ext 116). On `PUT`, `require_signed_modules: true`
    /// tightens the tenant and `false` clears its tightening (a tenant never relaxes the
    /// cluster policy); `trusted_keys` must be empty (tenant keys go through
    /// `POST /v1/security/modules/keys`). A tenant admin may only tighten its own tenant.
    /// The answer then carries the tenant's effective requirement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
}

/// `POST /v1/modules/{sha}/signatures` body.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AttachSignaturesRequest {
    /// Module sha256 (lowercase hex; the path parameter).
    pub sha256: String,
    /// Signature files, each base64 of a `.minisig` or cosign signature / bundle (≤ 8 KiB
    /// decoded, ≤ 4).
    pub signatures: Vec<String>,
}

/// `POST /v1/modules/{sha}/signatures` answer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AttachSignaturesResult {
    /// Module sha256.
    pub sha256: String,
    /// Key ids now attached (ext 113).
    pub key_ids: Vec<String>,
}

// ---- traits ---------------------------------------------------------------------------------

/// Tenant lifecycle (system admin; 14-17).
#[async_trait::async_trait]
pub trait TenantsApi: Send + Sync + 'static {
    /// Create a tenant (409 if it exists).
    async fn create_tenant(&self, spec: TenantSpec) -> Result<TenantInfo, ApiError> {
        let _ = spec;
        Err(lands_in("create_tenant", "14-17"))
    }
    /// One tenant (404 if absent).
    async fn get_tenant(&self, id: String) -> Result<TenantInfo, ApiError> {
        let _ = id;
        Err(lands_in("get_tenant", "14-17"))
    }
    /// Every tenant, sorted by id.
    async fn list_tenants(&self) -> Result<Vec<TenantInfo>, ApiError> {
        Err(lands_in("list_tenants", "14-17"))
    }
    /// Change quotas / weight.
    async fn update_tenant(&self, id: String, patch: TenantPatch) -> Result<TenantInfo, ApiError> {
        let _ = (id, patch);
        Err(lands_in("update_tenant", "14-17"))
    }
    /// Suspend (idempotent).
    async fn suspend_tenant(&self, id: String) -> Result<TenantInfo, ApiError> {
        let _ = id;
        Err(lands_in("suspend_tenant", "14-17"))
    }
    /// Resume (idempotent).
    async fn resume_tenant(&self, id: String) -> Result<TenantInfo, ApiError> {
        let _ = id;
        Err(lands_in("resume_tenant", "14-17"))
    }
    /// Delete a suspended, empty tenant (409 `tenant_not_empty` otherwise).
    async fn delete_tenant(&self, id: String) -> Result<(), ApiError> {
        let _ = id;
        Err(lands_in("delete_tenant", "14-17"))
    }
}

/// API token administration (14-17). Touches only `otk_` tokens.
#[async_trait::async_trait]
pub trait ApiTokensApi: Send + Sync + 'static {
    /// Create a token; the bearer is in the answer only.
    async fn create_token(&self, req: CreateTokenRequest) -> Result<CreatedToken, ApiError> {
        let _ = req;
        Err(lands_in("create_token", "14-17"))
    }
    /// Tokens visible to the caller (`tenant` filter for system principals).
    async fn list_tokens(&self, tenant: Option<String>) -> Result<Vec<TokenInfo>, ApiError> {
        let _ = tenant;
        Err(lands_in("list_tokens", "14-17"))
    }
    /// Revoke (idempotent; unknown = 404).
    async fn revoke_token(&self, id: String) -> Result<(), ApiError> {
        let _ = id;
        Err(lands_in("revoke_token", "14-17"))
    }
    /// Create a successor (ext `rotated_from`), optionally revoking the old one.
    async fn rotate_token(
        &self,
        id: String,
        req: RotateTokenRequest,
    ) -> Result<CreatedToken, ApiError> {
        let _ = (id, req);
        Err(lands_in("rotate_token", "14-17"))
    }
    /// The calling principal.
    async fn whoami(&self) -> Result<WhoAmI, ApiError> {
        Err(lands_in("whoami", "14-17"))
    }
}

/// The audit query (14-18).
#[async_trait::async_trait]
pub trait AuditApi: Send + Sync + 'static {
    /// Records matching `q`, fanned out across members.
    async fn query(&self, q: AuditQuery) -> Result<AuditPage, ApiError> {
        let _ = q;
        Err(lands_in("audit query", "14-18"))
    }
}

/// Module security policy and signatures (14-19).
#[async_trait::async_trait]
pub trait SecurityApi: Send + Sync + 'static {
    /// The cluster policy (ext 115).
    async fn get_module_policy(&self) -> Result<ModulePolicy, ApiError> {
        Err(lands_in("get_module_policy", "14-19"))
    }
    /// Replace the cluster policy.
    async fn set_module_policy(&self, policy: ModulePolicy) -> Result<ModulePolicy, ApiError> {
        let _ = policy;
        Err(lands_in("set_module_policy", "14-19"))
    }
    /// Add a cluster or tenant key.
    async fn add_trusted_key(&self, key: TrustedKeySpec) -> Result<ModulePolicy, ApiError> {
        let _ = key;
        Err(lands_in("add_trusted_key", "14-19"))
    }
    /// Remove a key by id (404 if absent).
    async fn remove_trusted_key(&self, key_id: String) -> Result<ModulePolicy, ApiError> {
        let _ = key_id;
        Err(lands_in("remove_trusted_key", "14-19"))
    }
    /// Verify and attach signatures to a stored module (ext 113).
    async fn attach_signatures(
        &self,
        req: AttachSignaturesRequest,
    ) -> Result<AttachSignaturesResult, ApiError> {
        let _ = req;
        Err(lands_in("attach_signatures", "14-19"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    struct Stubs;
    impl TenantsApi for Stubs {}
    impl ApiTokensApi for Stubs {}
    impl AuditApi for Stubs {}
    impl SecurityApi for Stubs {}

    #[tokio::test]
    async fn default_bodies_name_their_plan() {
        let s = Arc::new(Stubs);
        let e = s.list_tenants().await.unwrap_err();
        assert_eq!(e.code, ErrorCode::Unimplemented);
        assert!(e.message.contains("14-17"), "{e}");
        assert!(
            s.query(AuditQuery::default())
                .await
                .unwrap_err()
                .message
                .contains("14-18")
        );
        assert!(
            s.get_module_policy()
                .await
                .unwrap_err()
                .message
                .contains("14-19")
        );
        assert!(s.whoami().await.unwrap_err().message.contains("14-17"));
    }

    #[test]
    fn created_token_debug_redacts_the_bearer() {
        let t = CreatedToken {
            info: TokenInfo {
                id: "otk_x".into(),
                ..TokenInfo::default()
            },
            bearer: "ostro_otk_x_secret".into(),
        };
        let dbg = format!("{t:?}");
        assert!(!dbg.contains("secret"), "{dbg}");
        assert!(dbg.contains("otk_x"));
    }

    #[test]
    fn wire_shapes() {
        let info = TenantInfo {
            spec: TenantSpec {
                id: "acme".into(),
                weight: Some(2),
                ..TenantSpec::default()
            },
            suspended: false,
            created_at_ms: 1,
        };
        let v = serde_json::to_value(&info).unwrap();
        assert_eq!(v["id"], "acme", "spec is flattened");
        assert_eq!(v["weight"], 2);
        let back: TenantInfo = serde_json::from_value(v).unwrap();
        assert_eq!(back, info);
        let patch: TenantPatch =
            serde_json::from_str(r#"{"weight": 3, "max_concurrency": null}"#).unwrap();
        assert_eq!(patch.weight, Some(Some(3)));
        // 14-17: `null` clears a quota (`Some(None)`); an absent key keeps it (`None`).
        assert_eq!(patch.max_concurrency, Some(None));
        assert_eq!(patch.fuel_per_s, None);
        let req: CreateTokenRequest =
            serde_json::from_str(r#"{"name":"ci","scopes":["deploy"]}"#).unwrap();
        assert_eq!(req.tenant, None);
        assert!(!req.no_expiry);
    }
}
