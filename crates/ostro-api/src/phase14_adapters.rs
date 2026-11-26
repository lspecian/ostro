//! Phase 14 adapters (14-02; D-P14-4/8/10/11/29): the REST routes and the
//! `ostro.api.v1.{Tenants, ApiTokens, Audit, Security}` gRPC services over the
//! [`crate::phase14`] slots of [`crate::ApiExtensions`], following the Phase 11/13 adapter
//! pattern, plus the module-signature carriage shared by the upload adapters.
//!
//! An absent `tenants` / `api_tokens` / `audit` / `security` slot answers 404 `not_found` on
//! REST, and its gRPC service is not mounted (`UNIMPLEMENTED`). Stateless validation (tenant
//! ids, sha syntax, signature counts and sizes) runs here for both transports before the
//! trait is called. Authorization (scopes, tenant scoping, audit) is never the adapters':
//! the server's guard wraps the slots (14-03/14-15).
//!
//! Bearer secrets appear only in the create / rotate answers, which carry
//! `Cache-Control: no-store`.
//!
//! # Signature carriage (D-P14-10)
//!
//! `PUT /v1/modules/{sha256}` accepts up to [`check::MAX_SIGNATURES`] repeatable
//! `Ostro-Module-Signature: <base64 of a .minisig or cosign signature / bundle>` headers, each
//! at most [`check::MAX_SIGNATURE_BYTES`] decoded; more headers, a larger or undecodable one
//! answer 400 `signature_invalid` before the body is read ([`ModuleSignatures`] is a
//! request-parts extractor). gRPC `UploadModuleRequest.signatures` carries the raw bytes
//! with the same limits; `POST /v1/modules/{sha}/signatures` attaches them later.

use std::sync::Arc;

use axum::Json;
use axum::extract::rejection::{BytesRejection, JsonRejection, QueryRejection};
use axum::extract::{FromRequestParts, Path, Query, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use bytes::Bytes;
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::{api_tokens_server, audit_server, security_server, tenants_server};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::error::{ApiError, ErrorBody, reason};
use crate::phase8::{ApiExtensions, ExtState};
use crate::phase14::*;
use crate::rest::{json_body, optional_json_body, query};

/// The upload signature header (D-P14-29).
pub const SIGNATURE_HEADER: &str = "ostro-module-signature";

// ---------------------------------------------------------------------------------
// Validation (both transports)
// ---------------------------------------------------------------------------------

/// Stateless request checks shared by REST and gRPC.
pub mod check {
    use super::*;

    /// Most signatures per upload / attach (D-P14-10).
    pub const MAX_SIGNATURES: usize = 4;
    /// Largest decoded signature file (D-P14-10).
    pub const MAX_SIGNATURE_BYTES: usize = 8 * 1024;

    /// Standard alphabet, padding optional.
    const B64: base64::engine::GeneralPurpose = base64::engine::GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        base64::engine::GeneralPurposeConfig::new()
            .with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent),
    );

    /// 400 `signature_invalid`.
    pub fn signature_invalid(message: impl Into<String>) -> ApiError {
        ApiError::invalid_argument(message)
            .with_details(serde_json::json!({ "reason": reason::SIGNATURE_INVALID }))
    }

    /// At most [`MAX_SIGNATURES`] signatures.
    pub fn signature_count(n: usize) -> Result<(), ApiError> {
        if n > MAX_SIGNATURES {
            return Err(signature_invalid(format!(
                "{n} signatures: at most {MAX_SIGNATURES} are accepted"
            )));
        }
        Ok(())
    }

    /// One signature file: non-empty, at most [`MAX_SIGNATURE_BYTES`].
    pub fn signature_bytes(i: usize, sig: &[u8]) -> Result<(), ApiError> {
        if sig.is_empty() {
            return Err(signature_invalid(format!("signature {i} is empty")));
        }
        if sig.len() > MAX_SIGNATURE_BYTES {
            return Err(signature_invalid(format!(
                "signature {i} is {} bytes: at most {MAX_SIGNATURE_BYTES}",
                sig.len()
            )));
        }
        Ok(())
    }

    /// Raw signatures (gRPC): count and sizes.
    pub fn signatures(sigs: &[Vec<u8>]) -> Result<(), ApiError> {
        signature_count(sigs.len())?;
        for (i, s) in sigs.iter().enumerate() {
            signature_bytes(i, s)?;
        }
        Ok(())
    }

    /// One base64 signature (header value or JSON string), size-checked before decoding.
    pub fn decode_signature(i: usize, s: &str) -> Result<Vec<u8>, ApiError> {
        let s = s.trim();
        if s.len() > MAX_SIGNATURE_BYTES.div_ceil(3) * 4 {
            return Err(signature_invalid(format!(
                "signature {i} exceeds {MAX_SIGNATURE_BYTES} bytes"
            )));
        }
        let bytes = B64
            .decode(s)
            .map_err(|e| signature_invalid(format!("signature {i} is not base64 ({e})")))?;
        signature_bytes(i, &bytes)?;
        Ok(bytes)
    }

    /// Base64 signatures (REST attach body): count, decoding, sizes.
    pub fn decode_signatures(sigs: &[String]) -> Result<Vec<Vec<u8>>, ApiError> {
        signature_count(sigs.len())?;
        sigs.iter()
            .enumerate()
            .map(|(i, s)| decode_signature(i, s))
            .collect()
    }

    /// Every `Ostro-Module-Signature` header, decoded (empty = unsigned).
    pub fn header_signatures(h: &HeaderMap) -> Result<Vec<Vec<u8>>, ApiError> {
        let values: Vec<&HeaderValue> = h.get_all(SIGNATURE_HEADER).iter().collect();
        signature_count(values.len())?;
        values
            .into_iter()
            .enumerate()
            .map(|(i, v)| {
                let s = v.to_str().map_err(|_| {
                    signature_invalid(format!("signature {i}: header is not ASCII"))
                })?;
                decode_signature(i, s)
            })
            .collect()
    }

    /// D23 tenant id `^[a-z0-9][a-z0-9-]{0,62}$` (`_` is never a creatable id).
    pub fn is_tenant_id(s: &str) -> bool {
        let b = s.as_bytes();
        !b.is_empty()
            && b.len() <= 63
            && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
            && b[1..]
                .iter()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
    }

    /// A tenant id path / body field: 400 unless D23.
    pub fn tenant_id(field: &str, s: &str) -> Result<(), ApiError> {
        if is_tenant_id(s) {
            Ok(())
        } else {
            Err(ApiError::invalid_argument(format!(
                "{field}: {s:?} is not a tenant id (^[a-z0-9][a-z0-9-]{{0,62}}$)"
            ))
            .with_details(serde_json::json!({ "field": field })))
        }
    }

    /// The run-list tenant filter: absent or empty = no filter, `_` = the default tenant,
    /// else a D23 tenant id (400 otherwise).
    pub fn tenant_filter(t: Option<String>) -> Result<Option<String>, ApiError> {
        match t {
            None => Ok(None),
            Some(t) if t.is_empty() => Ok(None),
            Some(t) if t == "_" => Ok(Some(t)),
            Some(t) => {
                tenant_id("tenant", &t)?;
                Ok(Some(t))
            }
        }
    }

    /// A non-empty path id (token id, key id).
    pub fn id(field: &str, s: &str) -> Result<(), ApiError> {
        if s.is_empty() || s.len() > 256 || s.chars().any(char::is_control) {
            return Err(ApiError::invalid_argument(format!(
                "{field} must be 1..=256 printable characters"
            ))
            .with_details(serde_json::json!({ "field": field })));
        }
        Ok(())
    }

    /// A module sha256 (64 lowercase hex digits).
    pub fn sha256(field: &str, s: &str) -> Result<(), ApiError> {
        crate::phase13_adapters::check::sha256(field, s)
    }
}

/// The decoded `Ostro-Module-Signature` headers of a request (400 `signature_invalid`
/// before the body is read).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ModuleSignatures(pub Vec<Vec<u8>>);

impl<S: Send + Sync> FromRequestParts<S> for ModuleSignatures {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, ApiError> {
        check::header_signatures(&parts.headers).map(ModuleSignatures)
    }
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn absent(what: &str) -> ApiError {
    ApiError::not_found(format!("the {what} API is not enabled on this server"))
}

fn tenants(ext: &ApiExtensions) -> Result<&dyn TenantsApi, ApiError> {
    ext.tenants.as_deref().ok_or_else(|| absent("tenants"))
}

fn api_tokens(ext: &ApiExtensions) -> Result<&dyn ApiTokensApi, ApiError> {
    ext.api_tokens
        .as_deref()
        .ok_or_else(|| absent("API tokens"))
}

fn audit(ext: &ApiExtensions) -> Result<&dyn AuditApi, ApiError> {
    ext.audit.as_deref().ok_or_else(|| absent("audit"))
}

fn security(ext: &ApiExtensions) -> Result<&dyn SecurityApi, ApiError> {
    ext.security.as_deref().ok_or_else(|| absent("security"))
}

fn no_store<T: IntoResponse>(status: StatusCode, body: T) -> Response {
    let mut res = (status, body).into_response();
    res.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    res
}

/// `since` / `until`: Unix ms digits or an RFC 3339 timestamp.
fn parse_time(field: &str, s: &str) -> Result<u64, ApiError> {
    if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
        return s
            .parse::<u64>()
            .map_err(|_| ApiError::invalid_argument(format!("{field}: out of range")));
    }
    crate::rest::parse_rfc3339_ms(field, s)
}

// ---------------------------------------------------------------------------------
// REST DTOs
// ---------------------------------------------------------------------------------

/// `GET /v1/tenants`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TenantListResponse {
    /// Tenants sorted by id.
    pub tenants: Vec<TenantInfo>,
}

/// `DELETE /v1/tenants/{id}`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TenantDeleted {
    /// The tenant id.
    pub id: String,
    /// Always true.
    pub deleted: bool,
}

/// `GET /v1/auth/tokens` query.
#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct TokenListQuery {
    /// Tenant filter (system principals).
    pub tenant: Option<String>,
}

/// `GET /v1/auth/tokens`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TokenListResponse {
    /// Tokens (never secrets or hashes).
    pub tokens: Vec<TokenInfo>,
}

/// `DELETE /v1/auth/tokens/{id}`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TokenRevoked {
    /// The token id.
    pub id: String,
    /// Always true (idempotent).
    pub revoked: bool,
}

/// `GET /v1/audit` query.
#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct AuditQueryParams {
    /// Inclusive lower bound: Unix ms or RFC 3339.
    pub since: Option<String>,
    /// Exclusive upper bound: Unix ms or RFC 3339.
    pub until: Option<String>,
    /// Tenant filter (forced for tenant principals).
    pub tenant: Option<String>,
    /// Action filter (exact or `prefix.*`).
    pub action: Option<String>,
    /// Actor filter (token id).
    pub actor: Option<String>,
    /// Page size (server-capped).
    pub limit: Option<u32>,
    /// Cursor from the previous page.
    pub cursor: Option<String>,
}

/// `POST /v1/modules/{sha}/signatures` body.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AttachSignaturesBody {
    /// Base64 signature files (`.minisig`, cosign signature or bundle), ≤ 8 KiB decoded, ≤ 4.
    pub signatures: Vec<String>,
}

// ---------------------------------------------------------------------------------
// REST handlers
// ---------------------------------------------------------------------------------

/// List tenants.
#[utoipa::path(
    get,
    path = "/v1/tenants",
    tag = "tenants",
    responses(
        (status = 200, description = "Every tenant, sorted by id", body = TenantListResponse),
        (status = 404, description = "Tenants API not enabled", body = ErrorBody),
    )
)]
async fn list_tenants(State(ext): State<ExtState>) -> Result<Json<TenantListResponse>, ApiError> {
    Ok(Json(TenantListResponse {
        tenants: tenants(&ext)?.list_tenants().await?,
    }))
}

/// Create a tenant.
#[utoipa::path(
    post,
    path = "/v1/tenants",
    tag = "tenants",
    request_body = TenantSpec,
    responses(
        (status = 201, description = "Created", body = TenantInfo),
        (status = 400, description = "Invalid id or quota", body = ErrorBody),
        (status = 404, description = "Tenants API not enabled", body = ErrorBody),
        (status = 409, description = "Tenant exists", body = ErrorBody),
    )
)]
async fn create_tenant(
    State(ext): State<ExtState>,
    body: Result<Json<TenantSpec>, JsonRejection>,
) -> Result<Response, ApiError> {
    let api = tenants(&ext)?;
    let spec = json_body(body)?;
    check::tenant_id("id", &spec.id)?;
    Ok((StatusCode::CREATED, Json(api.create_tenant(spec).await?)).into_response())
}

/// Get a tenant.
#[utoipa::path(
    get,
    path = "/v1/tenants/{id}",
    tag = "tenants",
    params(("id" = String, Path, description = "Tenant id")),
    responses(
        (status = 200, description = "The tenant", body = TenantInfo),
        (status = 400, description = "Invalid id", body = ErrorBody),
        (status = 404, description = "Unknown tenant, or tenants API not enabled", body = ErrorBody),
    )
)]
async fn get_tenant(
    State(ext): State<ExtState>,
    Path(id): Path<String>,
) -> Result<Json<TenantInfo>, ApiError> {
    let api = tenants(&ext)?;
    check::tenant_id("id", &id)?;
    Ok(Json(api.get_tenant(id).await?))
}

/// Change a tenant's quotas / weight (only the present fields).
#[utoipa::path(
    patch,
    path = "/v1/tenants/{id}",
    tag = "tenants",
    params(("id" = String, Path, description = "Tenant id")),
    request_body = TenantPatch,
    responses(
        (status = 200, description = "The updated tenant", body = TenantInfo),
        (status = 400, description = "Invalid id or quota", body = ErrorBody),
        (status = 404, description = "Unknown tenant, or tenants API not enabled", body = ErrorBody),
    )
)]
async fn update_tenant(
    State(ext): State<ExtState>,
    Path(id): Path<String>,
    body: Result<Json<TenantPatch>, JsonRejection>,
) -> Result<Json<TenantInfo>, ApiError> {
    let api = tenants(&ext)?;
    check::tenant_id("id", &id)?;
    let patch = json_body(body)?;
    Ok(Json(api.update_tenant(id, patch).await?))
}

/// Delete a suspended, empty tenant.
#[utoipa::path(
    delete,
    path = "/v1/tenants/{id}",
    tag = "tenants",
    params(("id" = String, Path, description = "Tenant id")),
    responses(
        (status = 200, description = "Deleted", body = TenantDeleted),
        (status = 400, description = "Invalid id", body = ErrorBody),
        (status = 404, description = "Unknown tenant, or tenants API not enabled", body = ErrorBody),
        (status = 409, description = "Not suspended or not empty (details.reason = tenant_not_empty)", body = ErrorBody),
    )
)]
async fn delete_tenant(
    State(ext): State<ExtState>,
    Path(id): Path<String>,
) -> Result<Json<TenantDeleted>, ApiError> {
    let api = tenants(&ext)?;
    check::tenant_id("id", &id)?;
    api.delete_tenant(id.clone()).await?;
    Ok(Json(TenantDeleted { id, deleted: true }))
}

/// Suspend a tenant (idempotent).
#[utoipa::path(
    post,
    path = "/v1/tenants/{id}/suspend",
    tag = "tenants",
    params(("id" = String, Path, description = "Tenant id")),
    responses(
        (status = 200, description = "The tenant", body = TenantInfo),
        (status = 400, description = "Invalid id", body = ErrorBody),
        (status = 404, description = "Unknown tenant, or tenants API not enabled", body = ErrorBody),
    )
)]
async fn suspend_tenant(
    State(ext): State<ExtState>,
    Path(id): Path<String>,
) -> Result<Json<TenantInfo>, ApiError> {
    let api = tenants(&ext)?;
    check::tenant_id("id", &id)?;
    Ok(Json(api.suspend_tenant(id).await?))
}

/// Resume a tenant (idempotent).
#[utoipa::path(
    post,
    path = "/v1/tenants/{id}/resume",
    tag = "tenants",
    params(("id" = String, Path, description = "Tenant id")),
    responses(
        (status = 200, description = "The tenant", body = TenantInfo),
        (status = 400, description = "Invalid id", body = ErrorBody),
        (status = 404, description = "Unknown tenant, or tenants API not enabled", body = ErrorBody),
    )
)]
async fn resume_tenant(
    State(ext): State<ExtState>,
    Path(id): Path<String>,
) -> Result<Json<TenantInfo>, ApiError> {
    let api = tenants(&ext)?;
    check::tenant_id("id", &id)?;
    Ok(Json(api.resume_tenant(id).await?))
}

/// List API tokens (never secrets).
#[utoipa::path(
    get,
    path = "/v1/auth/tokens",
    tag = "auth",
    params(TokenListQuery),
    responses(
        (status = 200, description = "Tokens visible to the caller", body = TokenListResponse),
        (status = 400, description = "Invalid tenant", body = ErrorBody),
        (status = 404, description = "API tokens not enabled", body = ErrorBody),
    )
)]
async fn list_tokens(
    State(ext): State<ExtState>,
    q: Result<Query<TokenListQuery>, QueryRejection>,
) -> Result<Json<TokenListResponse>, ApiError> {
    let api = api_tokens(&ext)?;
    let q = query(q)?;
    let tenant = check::tenant_filter(q.tenant)?;
    Ok(Json(TokenListResponse {
        tokens: api.list_tokens(tenant).await?,
    }))
}

/// Create an API token. The bearer is in this response only.
#[utoipa::path(
    post,
    path = "/v1/auth/tokens",
    tag = "auth",
    request_body = CreateTokenRequest,
    responses(
        (status = 201, description = "Created (Cache-Control: no-store)", body = CreatedToken),
        (status = 400, description = "Invalid scopes, tenant or lifetime", body = ErrorBody),
        (status = 404, description = "API tokens not enabled", body = ErrorBody),
    )
)]
async fn create_token(
    State(ext): State<ExtState>,
    body: Result<Json<CreateTokenRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let api = api_tokens(&ext)?;
    let req = json_body(body)?;
    if let Some(t) = &req.tenant {
        check::tenant_id("tenant", t)?;
    }
    Ok(no_store(
        StatusCode::CREATED,
        Json(api.create_token(req).await?),
    ))
}

/// Revoke an API token (idempotent).
#[utoipa::path(
    delete,
    path = "/v1/auth/tokens/{id}",
    tag = "auth",
    params(("id" = String, Path, description = "Token id (otk_…)")),
    responses(
        (status = 200, description = "Revoked", body = TokenRevoked),
        (status = 404, description = "Unknown token, or API tokens not enabled", body = ErrorBody),
    )
)]
async fn revoke_token(
    State(ext): State<ExtState>,
    Path(id): Path<String>,
) -> Result<Json<TokenRevoked>, ApiError> {
    let api = api_tokens(&ext)?;
    check::id("id", &id)?;
    api.revoke_token(id.clone()).await?;
    Ok(Json(TokenRevoked { id, revoked: true }))
}

/// Rotate an API token: a successor, optionally revoking the old one. The new bearer is in
/// this response only.
#[utoipa::path(
    post,
    path = "/v1/auth/tokens/{id}/rotate",
    tag = "auth",
    params(("id" = String, Path, description = "Token id (otk_…)")),
    request_body(content = RotateTokenRequest, description = "Optional"),
    responses(
        (status = 201, description = "The successor (Cache-Control: no-store)", body = CreatedToken),
        (status = 404, description = "Unknown token, or API tokens not enabled", body = ErrorBody),
    )
)]
async fn rotate_token(
    State(ext): State<ExtState>,
    Path(id): Path<String>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Response, ApiError> {
    let api = api_tokens(&ext)?;
    check::id("id", &id)?;
    let req: RotateTokenRequest = optional_json_body(body)?;
    Ok(no_store(
        StatusCode::CREATED,
        Json(api.rotate_token(id, req).await?),
    ))
}

/// The calling principal.
#[utoipa::path(
    get,
    path = "/v1/auth/whoami",
    tag = "auth",
    responses(
        (status = 200, description = "The caller", body = WhoAmI),
        (status = 404, description = "API tokens not enabled", body = ErrorBody),
    )
)]
async fn whoami(State(ext): State<ExtState>) -> Result<Json<WhoAmI>, ApiError> {
    Ok(Json(api_tokens(&ext)?.whoami().await?))
}

/// Query the audit log (merged across members).
#[utoipa::path(
    get,
    path = "/v1/audit",
    tag = "audit",
    params(AuditQueryParams),
    responses(
        (status = 200, description = "A page of records, oldest first", body = AuditPage),
        (status = 400, description = "Invalid filter", body = ErrorBody),
        (status = 404, description = "Audit API not enabled", body = ErrorBody),
        (status = 503, description = "Journal unavailable (details.reason = audit_unavailable)", body = ErrorBody),
    )
)]
async fn query_audit(
    State(ext): State<ExtState>,
    q: Result<Query<AuditQueryParams>, QueryRejection>,
) -> Result<Json<AuditPage>, ApiError> {
    let api = audit(&ext)?;
    let q = query(q)?;
    let req = AuditQuery {
        tenant: check::tenant_filter(q.tenant)?,
        actor: q.actor.filter(|s| !s.is_empty()),
        action: q.action.filter(|s| !s.is_empty()),
        since_ms: q
            .since
            .as_deref()
            .map(|s| parse_time("since", s))
            .transpose()?,
        until_ms: q
            .until
            .as_deref()
            .map(|s| parse_time("until", s))
            .transpose()?,
        limit: q.limit,
        cursor: q.cursor.filter(|s| !s.is_empty()),
    };
    Ok(Json(api.query(req).await?))
}

/// The cluster module-signing policy.
#[utoipa::path(
    get,
    path = "/v1/security/modules/policy",
    tag = "security",
    responses(
        (status = 200, description = "The policy", body = ModulePolicy),
        (status = 404, description = "Security API not enabled", body = ErrorBody),
    )
)]
async fn get_module_policy(State(ext): State<ExtState>) -> Result<Json<ModulePolicy>, ApiError> {
    Ok(Json(security(&ext)?.get_module_policy().await?))
}

/// Replace the cluster module-signing policy.
#[utoipa::path(
    put,
    path = "/v1/security/modules/policy",
    tag = "security",
    request_body = ModulePolicy,
    responses(
        (status = 200, description = "The new policy", body = ModulePolicy),
        (status = 400, description = "Invalid key", body = ErrorBody),
        (status = 404, description = "Security API not enabled", body = ErrorBody),
    )
)]
async fn set_module_policy(
    State(ext): State<ExtState>,
    body: Result<Json<ModulePolicy>, JsonRejection>,
) -> Result<Json<ModulePolicy>, ApiError> {
    let api = security(&ext)?;
    let policy = json_body(body)?;
    Ok(Json(api.set_module_policy(policy).await?))
}

/// Add a trusted signing key (cluster-wide, or a tenant key).
#[utoipa::path(
    post,
    path = "/v1/security/modules/keys",
    tag = "security",
    request_body = TrustedKeySpec,
    responses(
        (status = 200, description = "The policy with the key", body = ModulePolicy),
        (status = 400, description = "Invalid key", body = ErrorBody),
        (status = 404, description = "Security API not enabled", body = ErrorBody),
        (status = 409, description = "Key id exists", body = ErrorBody),
    )
)]
async fn add_trusted_key(
    State(ext): State<ExtState>,
    body: Result<Json<TrustedKeySpec>, JsonRejection>,
) -> Result<Json<ModulePolicy>, ApiError> {
    let api = security(&ext)?;
    let key = json_body(body)?;
    check::id("key_id", &key.key_id)?;
    if let Some(t) = &key.tenant {
        check::tenant_id("tenant", t)?;
    }
    Ok(Json(api.add_trusted_key(key).await?))
}

/// Remove a trusted signing key.
#[utoipa::path(
    delete,
    path = "/v1/security/modules/keys/{key_id}",
    tag = "security",
    params(("key_id" = String, Path, description = "Key id")),
    responses(
        (status = 200, description = "The policy without the key", body = ModulePolicy),
        (status = 404, description = "Unknown key, or security API not enabled", body = ErrorBody),
    )
)]
async fn remove_trusted_key(
    State(ext): State<ExtState>,
    Path(key_id): Path<String>,
) -> Result<Json<ModulePolicy>, ApiError> {
    let api = security(&ext)?;
    check::id("key_id", &key_id)?;
    Ok(Json(api.remove_trusted_key(key_id).await?))
}

/// Verify and attach signatures to a stored module.
#[utoipa::path(
    post,
    path = "/v1/modules/{sha}/signatures",
    tag = "security",
    params(("sha" = String, Path, description = "Lowercase hex sha256 of an uploaded module")),
    request_body = AttachSignaturesBody,
    responses(
        (status = 200, description = "Key ids now attached", body = AttachSignaturesResult),
        (status = 400, description = "Invalid sha or signature (details.reason = signature_invalid)", body = ErrorBody),
        (status = 404, description = "Unknown module, or security API not enabled", body = ErrorBody),
    )
)]
async fn attach_signatures(
    State(ext): State<ExtState>,
    Path(sha): Path<String>,
    body: Result<Json<AttachSignaturesBody>, JsonRejection>,
) -> Result<Json<AttachSignaturesResult>, ApiError> {
    let api = security(&ext)?;
    check::sha256("sha256", &sha)?;
    let b = json_body(body)?;
    if b.signatures.is_empty() {
        return Err(check::signature_invalid("signatures is empty"));
    }
    let raw = check::decode_signatures(&b.signatures)?;
    Ok(Json(
        api.attach_signatures(AttachSignaturesRequest {
            sha256: sha,
            signatures: raw.iter().map(|s| b64(s)).collect(),
        })
        .await?,
    ))
}

/// The Phase 14 REST routes with their OpenAPI description, without state.
pub fn api_routes() -> OpenApiRouter<ExtState> {
    let r = OpenApiRouter::new()
        .routes(routes!(list_tenants, create_tenant))
        .routes(routes!(get_tenant, update_tenant, delete_tenant))
        .routes(routes!(suspend_tenant))
        .routes(routes!(resume_tenant))
        .routes(routes!(list_tokens, create_token))
        .routes(routes!(revoke_token))
        .routes(routes!(rotate_token))
        .routes(routes!(whoami))
        .routes(routes!(query_audit))
        .routes(routes!(get_module_policy, set_module_policy))
        .routes(routes!(add_trusted_key))
        .routes(routes!(remove_trusted_key));
    // Documented as `{sha}` (D-P14-29), mounted with the `{sha256}` segment name the
    // upload route uses (one parameter name per segment in the merged router).
    crate::rest::wildcard_routes(
        r,
        routes!(attach_signatures),
        "/v1/modules/{sha256}/signatures",
    )
}

/// The Phase 14 REST router over `ext` (merged by [`crate::router_with`]).
pub fn router(ext: ApiExtensions) -> axum::Router {
    let (r, _doc) = api_routes().split_for_parts();
    r.with_state(Arc::new(ext))
}

// ---------------------------------------------------------------------------------
// Conversions (both directions)
// ---------------------------------------------------------------------------------

fn required<T>(field: &str, v: Option<T>) -> Result<T, ApiError> {
    v.ok_or_else(|| {
        ApiError::invalid_argument(format!("{field} is required"))
            .with_details(serde_json::json!({ "field": field }))
    })
}

impl From<TenantSpec> for pb::TenantSpec {
    fn from(s: TenantSpec) -> Self {
        pb::TenantSpec {
            id: s.id,
            weight: s.weight,
            max_concurrency: s.max_concurrency,
            fuel_per_s: s.fuel_per_s,
            fuel_burst: s.fuel_burst,
            memory_bytes: s.memory_bytes,
            event_rate: s.event_rate.map(Into::into),
        }
    }
}

impl From<pb::TenantSpec> for TenantSpec {
    fn from(s: pb::TenantSpec) -> Self {
        TenantSpec {
            id: s.id,
            weight: s.weight,
            max_concurrency: s.max_concurrency,
            fuel_per_s: s.fuel_per_s,
            fuel_burst: s.fuel_burst,
            memory_bytes: s.memory_bytes,
            event_rate: s.event_rate.map(Into::into),
        }
    }
}

impl From<TenantInfo> for pb::Tenant {
    fn from(t: TenantInfo) -> Self {
        pb::Tenant {
            spec: Some(t.spec.into()),
            suspended: t.suspended,
            created_at_ms: t.created_at_ms,
        }
    }
}

impl TryFrom<pb::Tenant> for TenantInfo {
    type Error = ApiError;

    fn try_from(t: pb::Tenant) -> Result<Self, ApiError> {
        Ok(TenantInfo {
            spec: required("spec", t.spec)?.into(),
            suspended: t.suspended,
            created_at_ms: t.created_at_ms,
        })
    }
}

/// `TenantPatch` -> `pb::UpdateTenantRequest` for `id`.
pub fn patch_to_proto(id: String, p: TenantPatch) -> pb::UpdateTenantRequest {
    pb::UpdateTenantRequest {
        id,
        weight: p.weight.map(|value| pb::QuotaU32 { value }),
        max_concurrency: p.max_concurrency.map(|value| pb::QuotaU32 { value }),
        fuel_per_s: p.fuel_per_s.map(|value| pb::QuotaU64 { value }),
        fuel_burst: p.fuel_burst.map(|value| pb::QuotaU64 { value }),
        memory_bytes: p.memory_bytes.map(|value| pb::QuotaU64 { value }),
        event_rate: p.event_rate.map(|v| pb::QuotaRate {
            value: v.map(Into::into),
        }),
    }
}

/// `pb::UpdateTenantRequest` -> `(id, TenantPatch)`.
pub fn patch_from_proto(r: pb::UpdateTenantRequest) -> (String, TenantPatch) {
    (
        r.id,
        TenantPatch {
            weight: r.weight.map(|q| q.value),
            max_concurrency: r.max_concurrency.map(|q| q.value),
            fuel_per_s: r.fuel_per_s.map(|q| q.value),
            fuel_burst: r.fuel_burst.map(|q| q.value),
            memory_bytes: r.memory_bytes.map(|q| q.value),
            event_rate: r.event_rate.map(|q| q.value.map(Into::into)),
        },
    )
}

impl From<TokenInfo> for pb::ApiToken {
    fn from(t: TokenInfo) -> Self {
        pb::ApiToken {
            id: t.id,
            name: t.name,
            description: t.description,
            scopes: t.scopes,
            tenant: t.tenant,
            created_at_ms: t.created_at_ms,
            expires_at_ms: t.expires_at_ms,
            created_by: t.created_by,
            rotated_from: t.rotated_from,
        }
    }
}

impl From<pb::ApiToken> for TokenInfo {
    fn from(t: pb::ApiToken) -> Self {
        TokenInfo {
            id: t.id,
            name: t.name,
            description: t.description,
            scopes: t.scopes,
            tenant: t.tenant,
            created_at_ms: t.created_at_ms,
            expires_at_ms: t.expires_at_ms,
            created_by: t.created_by,
            rotated_from: t.rotated_from,
        }
    }
}

impl From<CreateTokenRequest> for pb::CreateTokenRequest {
    fn from(r: CreateTokenRequest) -> Self {
        pb::CreateTokenRequest {
            name: r.name,
            description: r.description,
            scopes: r.scopes,
            tenant: r.tenant,
            ttl_ms: r.ttl_ms,
            no_expiry: r.no_expiry,
        }
    }
}

impl From<pb::CreateTokenRequest> for CreateTokenRequest {
    fn from(r: pb::CreateTokenRequest) -> Self {
        CreateTokenRequest {
            name: r.name,
            description: r.description,
            scopes: r.scopes,
            tenant: r.tenant,
            ttl_ms: r.ttl_ms,
            no_expiry: r.no_expiry,
        }
    }
}

impl From<CreatedToken> for pb::CreateTokenResponse {
    fn from(t: CreatedToken) -> Self {
        pb::CreateTokenResponse {
            token: Some(t.info.into()),
            bearer: t.bearer,
        }
    }
}

impl From<CreatedToken> for pb::RotateTokenResponse {
    fn from(t: CreatedToken) -> Self {
        pb::RotateTokenResponse {
            token: Some(t.info.into()),
            bearer: t.bearer,
        }
    }
}

impl TryFrom<pb::CreateTokenResponse> for CreatedToken {
    type Error = ApiError;

    fn try_from(t: pb::CreateTokenResponse) -> Result<Self, ApiError> {
        Ok(CreatedToken {
            info: required("token", t.token)?.into(),
            bearer: t.bearer,
        })
    }
}

impl TryFrom<pb::RotateTokenResponse> for CreatedToken {
    type Error = ApiError;

    fn try_from(t: pb::RotateTokenResponse) -> Result<Self, ApiError> {
        Ok(CreatedToken {
            info: required("token", t.token)?.into(),
            bearer: t.bearer,
        })
    }
}

impl From<WhoAmI> for pb::WhoAmIResponse {
    fn from(w: WhoAmI) -> Self {
        pb::WhoAmIResponse {
            token_id: w.token_id,
            tenant: w.tenant,
            scopes: w.scopes,
            kind: w.kind,
        }
    }
}

impl From<pb::WhoAmIResponse> for WhoAmI {
    fn from(w: pb::WhoAmIResponse) -> Self {
        WhoAmI {
            token_id: w.token_id,
            tenant: w.tenant,
            scopes: w.scopes,
            kind: w.kind,
        }
    }
}

impl From<AuditQuery> for pb::AuditQueryRequest {
    fn from(q: AuditQuery) -> Self {
        pb::AuditQueryRequest {
            tenant: q.tenant,
            actor: q.actor,
            action: q.action,
            since_ms: q.since_ms,
            until_ms: q.until_ms,
            limit: q.limit,
            cursor: q.cursor,
        }
    }
}

impl From<pb::AuditQueryRequest> for AuditQuery {
    fn from(q: pb::AuditQueryRequest) -> Self {
        AuditQuery {
            tenant: q.tenant,
            actor: q.actor,
            action: q.action,
            since_ms: q.since_ms,
            until_ms: q.until_ms,
            limit: q.limit,
            cursor: q.cursor,
        }
    }
}

impl From<AuditRecordView> for pb::AuditRecord {
    fn from(r: AuditRecordView) -> Self {
        pb::AuditRecord {
            seq: r.seq,
            ts_ms: r.ts_ms,
            node: r.node,
            actor: r.actor,
            actor_kind: r.actor_kind,
            tenant: r.tenant,
            action: r.action,
            target: r.target,
            outcome: r.outcome,
            request_id: r.request_id,
            details: r.details.into_iter().collect(),
            hash: r.hash,
        }
    }
}

impl From<pb::AuditRecord> for AuditRecordView {
    fn from(r: pb::AuditRecord) -> Self {
        AuditRecordView {
            seq: r.seq,
            ts_ms: r.ts_ms,
            node: r.node,
            actor: r.actor,
            actor_kind: r.actor_kind,
            tenant: r.tenant,
            action: r.action,
            target: r.target,
            outcome: r.outcome,
            request_id: r.request_id,
            details: r.details.into_iter().collect(),
            hash: r.hash,
        }
    }
}

impl From<AuditPage> for pb::AuditPage {
    fn from(p: AuditPage) -> Self {
        pb::AuditPage {
            records: p.records.into_iter().map(Into::into).collect(),
            cursor: p.cursor,
            partial_nodes: p.partial_nodes,
        }
    }
}

impl From<pb::AuditPage> for AuditPage {
    fn from(p: pb::AuditPage) -> Self {
        AuditPage {
            records: p.records.into_iter().map(Into::into).collect(),
            cursor: p.cursor,
            partial_nodes: p.partial_nodes,
        }
    }
}

impl From<TrustedKeySpec> for pb::TrustedKey {
    fn from(k: TrustedKeySpec) -> Self {
        pb::TrustedKey {
            alg: k.alg,
            key_id: k.key_id,
            public_key: k.public_key,
            comment: k.comment,
            tenant: k.tenant,
        }
    }
}

impl From<pb::TrustedKey> for TrustedKeySpec {
    fn from(k: pb::TrustedKey) -> Self {
        TrustedKeySpec {
            alg: k.alg,
            key_id: k.key_id,
            public_key: k.public_key,
            comment: k.comment,
            tenant: k.tenant,
        }
    }
}

impl From<ModulePolicy> for pb::ModulePolicy {
    fn from(p: ModulePolicy) -> Self {
        pb::ModulePolicy {
            require_signed_modules: p.require_signed_modules,
            trusted_keys: p.trusted_keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<pb::ModulePolicy> for ModulePolicy {
    fn from(p: pb::ModulePolicy) -> Self {
        ModulePolicy {
            require_signed_modules: p.require_signed_modules,
            trusted_keys: p.trusted_keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<AttachSignaturesResult> for pb::AttachSignaturesResponse {
    fn from(r: AttachSignaturesResult) -> Self {
        pb::AttachSignaturesResponse {
            sha256: r.sha256,
            key_ids: r.key_ids,
        }
    }
}

impl From<pb::AttachSignaturesResponse> for AttachSignaturesResult {
    fn from(r: pb::AttachSignaturesResponse) -> Self {
        AttachSignaturesResult {
            sha256: r.sha256,
            key_ids: r.key_ids,
        }
    }
}

// ---------------------------------------------------------------------------------
// gRPC: ostro.api.v1.{Tenants, ApiTokens, Audit, Security}
// ---------------------------------------------------------------------------------

type GrpcResult<T> = Result<tonic::Response<T>, tonic::Status>;

fn ok<T>(v: T) -> GrpcResult<T> {
    Ok(tonic::Response::new(v))
}

fn st(e: ApiError) -> tonic::Status {
    crate::grpc::to_status(e)
}

/// `ostro.api.v1.Tenants` over a [`TenantsApi`] (same validation as REST).
#[derive(Clone)]
pub struct GrpcTenants(pub Arc<dyn TenantsApi>);

/// `ostro.api.v1.ApiTokens` over an [`ApiTokensApi`].
#[derive(Clone)]
pub struct GrpcApiTokens(pub Arc<dyn ApiTokensApi>);

/// `ostro.api.v1.Audit` over an [`AuditApi`].
#[derive(Clone)]
pub struct GrpcAudit(pub Arc<dyn AuditApi>);

/// `ostro.api.v1.Security` over a [`SecurityApi`].
#[derive(Clone)]
pub struct GrpcSecurity(pub Arc<dyn SecurityApi>);

#[tonic::async_trait]
impl tenants_server::Tenants for GrpcTenants {
    async fn create(&self, r: tonic::Request<pb::CreateTenantRequest>) -> GrpcResult<pb::Tenant> {
        let spec: TenantSpec = required("spec", r.into_inner().spec).map_err(st)?.into();
        check::tenant_id("id", &spec.id).map_err(st)?;
        ok(self.0.create_tenant(spec).await.map_err(st)?.into())
    }

    async fn get(&self, r: tonic::Request<pb::GetTenantRequest>) -> GrpcResult<pb::Tenant> {
        let id = r.into_inner().id;
        check::tenant_id("id", &id).map_err(st)?;
        ok(self.0.get_tenant(id).await.map_err(st)?.into())
    }

    async fn list(
        &self,
        _r: tonic::Request<pb::ListTenantsRequest>,
    ) -> GrpcResult<pb::ListTenantsResponse> {
        let tenants = self.0.list_tenants().await.map_err(st)?;
        ok(pb::ListTenantsResponse {
            tenants: tenants.into_iter().map(Into::into).collect(),
        })
    }

    async fn update(&self, r: tonic::Request<pb::UpdateTenantRequest>) -> GrpcResult<pb::Tenant> {
        let (id, patch) = patch_from_proto(r.into_inner());
        check::tenant_id("id", &id).map_err(st)?;
        ok(self.0.update_tenant(id, patch).await.map_err(st)?.into())
    }

    async fn suspend(&self, r: tonic::Request<pb::SuspendTenantRequest>) -> GrpcResult<pb::Tenant> {
        let id = r.into_inner().id;
        check::tenant_id("id", &id).map_err(st)?;
        ok(self.0.suspend_tenant(id).await.map_err(st)?.into())
    }

    async fn resume(&self, r: tonic::Request<pb::ResumeTenantRequest>) -> GrpcResult<pb::Tenant> {
        let id = r.into_inner().id;
        check::tenant_id("id", &id).map_err(st)?;
        ok(self.0.resume_tenant(id).await.map_err(st)?.into())
    }

    async fn delete(
        &self,
        r: tonic::Request<pb::DeleteTenantRequest>,
    ) -> GrpcResult<pb::DeleteTenantResponse> {
        let id = r.into_inner().id;
        check::tenant_id("id", &id).map_err(st)?;
        self.0.delete_tenant(id.clone()).await.map_err(st)?;
        ok(pb::DeleteTenantResponse { id, deleted: true })
    }
}

#[tonic::async_trait]
impl api_tokens_server::ApiTokens for GrpcApiTokens {
    async fn create(
        &self,
        r: tonic::Request<pb::CreateTokenRequest>,
    ) -> GrpcResult<pb::CreateTokenResponse> {
        let req: CreateTokenRequest = r.into_inner().into();
        if let Some(t) = &req.tenant {
            check::tenant_id("tenant", t).map_err(st)?;
        }
        ok(self.0.create_token(req).await.map_err(st)?.into())
    }

    async fn list(
        &self,
        r: tonic::Request<pb::ListTokensRequest>,
    ) -> GrpcResult<pb::ListTokensResponse> {
        let tenant = check::tenant_filter(r.into_inner().tenant).map_err(st)?;
        let tokens = self.0.list_tokens(tenant).await.map_err(st)?;
        ok(pb::ListTokensResponse {
            tokens: tokens.into_iter().map(Into::into).collect(),
        })
    }

    async fn revoke(
        &self,
        r: tonic::Request<pb::RevokeTokenRequest>,
    ) -> GrpcResult<pb::RevokeTokenResponse> {
        let id = r.into_inner().id;
        check::id("id", &id).map_err(st)?;
        self.0.revoke_token(id.clone()).await.map_err(st)?;
        ok(pb::RevokeTokenResponse { id, revoked: true })
    }

    async fn rotate(
        &self,
        r: tonic::Request<pb::RotateTokenRequest>,
    ) -> GrpcResult<pb::RotateTokenResponse> {
        let r = r.into_inner();
        check::id("id", &r.id).map_err(st)?;
        let req = RotateTokenRequest {
            revoke_old: r.revoke_old,
            ttl_ms: r.ttl_ms,
        };
        ok(self.0.rotate_token(r.id, req).await.map_err(st)?.into())
    }

    async fn who_am_i(
        &self,
        _r: tonic::Request<pb::WhoAmIRequest>,
    ) -> GrpcResult<pb::WhoAmIResponse> {
        ok(self.0.whoami().await.map_err(st)?.into())
    }
}

#[tonic::async_trait]
impl audit_server::Audit for GrpcAudit {
    async fn query(&self, r: tonic::Request<pb::AuditQueryRequest>) -> GrpcResult<pb::AuditPage> {
        let mut q: AuditQuery = r.into_inner().into();
        q.tenant = check::tenant_filter(q.tenant).map_err(st)?;
        q.actor = q.actor.filter(|s| !s.is_empty());
        q.action = q.action.filter(|s| !s.is_empty());
        q.cursor = q.cursor.filter(|s| !s.is_empty());
        ok(self.0.query(q).await.map_err(st)?.into())
    }
}

#[tonic::async_trait]
impl security_server::Security for GrpcSecurity {
    async fn get_module_policy(
        &self,
        _r: tonic::Request<pb::GetModulePolicyRequest>,
    ) -> GrpcResult<pb::ModulePolicy> {
        ok(self.0.get_module_policy().await.map_err(st)?.into())
    }

    async fn set_module_policy(
        &self,
        r: tonic::Request<pb::SetModulePolicyRequest>,
    ) -> GrpcResult<pb::ModulePolicy> {
        let policy: ModulePolicy = required("policy", r.into_inner().policy)
            .map_err(st)?
            .into();
        ok(self.0.set_module_policy(policy).await.map_err(st)?.into())
    }

    async fn add_trusted_key(
        &self,
        r: tonic::Request<pb::AddTrustedKeyRequest>,
    ) -> GrpcResult<pb::ModulePolicy> {
        let key: TrustedKeySpec = required("key", r.into_inner().key).map_err(st)?.into();
        check::id("key_id", &key.key_id).map_err(st)?;
        if let Some(t) = &key.tenant {
            check::tenant_id("tenant", t).map_err(st)?;
        }
        ok(self.0.add_trusted_key(key).await.map_err(st)?.into())
    }

    async fn remove_trusted_key(
        &self,
        r: tonic::Request<pb::RemoveTrustedKeyRequest>,
    ) -> GrpcResult<pb::ModulePolicy> {
        let key_id = r.into_inner().key_id;
        check::id("key_id", &key_id).map_err(st)?;
        ok(self.0.remove_trusted_key(key_id).await.map_err(st)?.into())
    }

    async fn attach_signatures(
        &self,
        r: tonic::Request<pb::AttachSignaturesRequest>,
    ) -> GrpcResult<pb::AttachSignaturesResponse> {
        let r = r.into_inner();
        check::sha256("sha256", &r.sha256).map_err(st)?;
        if r.signatures.is_empty() {
            return Err(st(check::signature_invalid("signatures is empty")));
        }
        check::signatures(&r.signatures).map_err(st)?;
        let req = AttachSignaturesRequest {
            sha256: r.sha256,
            signatures: r.signatures.iter().map(|s| b64(s)).collect(),
        };
        ok(self.0.attach_signatures(req).await.map_err(st)?.into())
    }
}

/// Mount `Tenants` / `ApiTokens` / `Audit` / `Security` for the slots that are present
/// (absent = not mounted, so clients see `UNIMPLEMENTED`, as for Phases 8, 11 and 13).
pub(crate) fn add_grpc_services(
    mut routes: tonic::service::Routes,
    ext: &ApiExtensions,
    max_request_bytes: usize,
) -> tonic::service::Routes {
    if let Some(t) = &ext.tenants {
        routes = routes.add_service(
            tenants_server::TenantsServer::new(GrpcTenants(Arc::clone(t)))
                .max_decoding_message_size(max_request_bytes),
        );
    }
    if let Some(t) = &ext.api_tokens {
        routes = routes.add_service(
            api_tokens_server::ApiTokensServer::new(GrpcApiTokens(Arc::clone(t)))
                .max_decoding_message_size(max_request_bytes),
        );
    }
    if let Some(a) = &ext.audit {
        routes = routes.add_service(
            audit_server::AuditServer::new(GrpcAudit(Arc::clone(a)))
                .max_decoding_message_size(max_request_bytes),
        );
    }
    if let Some(s) = &ext.security {
        routes = routes.add_service(
            security_server::SecurityServer::new(GrpcSecurity(Arc::clone(s)))
                .max_decoding_message_size(max_request_bytes),
        );
    }
    routes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_limits() {
        assert!(check::signatures(&[]).is_ok());
        assert!(check::signatures(&vec![vec![1]; 4]).is_ok());
        let e = check::signatures(&vec![vec![1]; 5]).unwrap_err();
        assert_eq!(e.reason(), Some(reason::SIGNATURE_INVALID));
        assert!(check::signatures(&[vec![0; check::MAX_SIGNATURE_BYTES]]).is_ok());
        let e = check::signatures(&[vec![0; check::MAX_SIGNATURE_BYTES + 1]]).unwrap_err();
        assert_eq!(e.reason(), Some(reason::SIGNATURE_INVALID));
        assert!(check::signatures(&[vec![]]).is_err());
        assert_eq!(check::decode_signature(0, "Zm9v").unwrap(), b"foo");
        assert_eq!(check::decode_signature(0, "Zm8").unwrap(), b"fo");
        assert!(check::decode_signature(0, "!!").is_err());
        let big = b64(&vec![7; check::MAX_SIGNATURE_BYTES]);
        assert_eq!(check::decode_signature(0, &big).unwrap().len(), 8192);
        let too_big = b64(&vec![7; check::MAX_SIGNATURE_BYTES + 1]);
        assert!(check::decode_signature(0, &too_big).is_err());
    }

    #[test]
    fn tenant_filters() {
        assert_eq!(check::tenant_filter(None).unwrap(), None);
        assert_eq!(check::tenant_filter(Some(String::new())).unwrap(), None);
        assert_eq!(
            check::tenant_filter(Some("_".into())).unwrap(),
            Some("_".into())
        );
        assert_eq!(
            check::tenant_filter(Some("acme-1".into())).unwrap(),
            Some("acme-1".into())
        );
        for bad in ["Acme", "-a", "a_b", "a/b", &"a".repeat(64)] {
            assert!(check::tenant_filter(Some(bad.to_owned())).is_err(), "{bad}");
        }
    }

    #[test]
    fn patch_round_trips_keep_clear_and_keep_apart() {
        let p = TenantPatch {
            weight: Some(Some(2)),
            max_concurrency: Some(None),
            event_rate: Some(Some(crate::phase7::RateSpecDto {
                limit: 1,
                period_ms: 2,
                burst: 3,
            })),
            ..TenantPatch::default()
        };
        let (id, back) = patch_from_proto(patch_to_proto("a".into(), p.clone()));
        assert_eq!(id, "a");
        assert_eq!(back, p);
    }
}
