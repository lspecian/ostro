//! Phase 6 bridge administration (D-FX9, plan 06-16): secrets, grants, connections,
//! egress policies and the local presign route, over meta and [`BridgeServices`].
//!
//! Every method first runs the `ostro-api` validation helpers (`PutSecret::validate`,
//! `GrantInfo::validate`, `ConnectionInfo::validate`, `EgressInfo::validate`), then the
//! stricter ostro-bridge parsers (`OriginPattern`, `parse_rule`, the SQL / object /
//! http-target config parsers), and stores the existing meta records through the
//! 06-DECISIONS conventions (no new `MetaRecord` variant):
//!
//! | Resource | Meta key | Record |
//! |---|---|---|
//! | secret `n` of tenant `t` | `Secret("<t or _>/n")` | `SealedSecret` sealed by the node's [`SealedStore`] (AAD binds the name) |
//! | grant | `Grant("secret:<module>:<secret>")` | `GrantSpec::to_record` (`module:<m>`, `origin:<p>`, `body`) |
//! | connection | `Connection(name)` | `{name, kind, config: JSON bytes}` |
//! | egress policy | `Egress(module)` | `EgressPolicy{name: module, allow}` |
//!
//! Secret values are write-only: they are sealed before the meta write, no response
//! carries them, and no error message echoes them. Names under `__ostro/` (presign keys,
//! the token key) are refused by every secret method and hidden from listings. Without a
//! sealing key (server profile, no `OSTRO_SEALING_KEY` / `[secrets] sealing_key_file`)
//! `put_secret` answers 412 `precondition_failed` with `details.reason =
//! "sealing_key_missing"`.
//!
//! Replacing a record keeps its `ext` bytes (Phase 14 TLV extensions written by newer
//! nodes are not dropped by an update through this API).

use bytes::Bytes;
use ostro_api::ApiError;
use ostro_api::domain::{
    ConnectionInfo, EgressInfo, GRANT_ID_PREFIX, GrantInfo, PresignedObjectRequest,
    PresignedObjectResponse, PutSecret, RESERVED_SECRET_PREFIX, SecretInfo, validate_secret_name,
};
use ostro_bridge::egress::parse_rule;
use ostro_bridge::http::{HTTP_TARGET_KIND, HttpTarget};
use ostro_bridge::object::{OBJECT_KIND, ObjectConnConfig, ServeError, VerifyError};
use ostro_bridge::origin::OriginPattern;
use ostro_bridge::secrets::GrantSpec;
use ostro_bridge::secrets::grants::SUBJECT_PREFIX;
use ostro_bridge::sql::SqlConnConfig;
use ostro_meta::records::{Connection, EgressPolicy, Grant};
use ostro_meta::{MetaCommand, MetaError, MetaKey, MetaRecord, MetaResult};

use super::ServerControlPlane;
use super::names::{self, DEFAULT_TENANT_TOKEN};
use crate::bridge_services::SEALING_KEY_MISSING;

/// CAS attempts before a concurrently updated record answers 503.
const CAS_RETRIES: usize = 8;

/// The message of a refused `put_secret` on a node without a sealing key.
pub const SEALING_KEY_HINT: &str = "no sealing key is configured, so secrets cannot be stored: \
     set OSTRO_SEALING_KEY or [secrets] sealing_key_file";

fn meta_unavailable(e: MetaError) -> ApiError {
    ApiError::unavailable(format!("metadata unavailable: {e}"))
}

/// 400 naming `field` (never a value).
fn bad(field: &str, message: impl Into<String>) -> ApiError {
    let message = message.into();
    ApiError::invalid_argument(format!("{field}: {message}"))
        .with_details(serde_json::json!({ "field": field }))
}

// ---------------------------------------------------------------------------------
// Meta helpers
// ---------------------------------------------------------------------------------

fn current(cp: &ServerControlPlane, key: &MetaKey) -> Option<(u64, MetaRecord)> {
    cp.meta()
        .view()
        .read(|s| s.get(key).map(|v| (v.version, v.value)))
}

/// Create or replace `key` (CAS on the version read just before). `build` gets the
/// current record's `ext` (empty on create).
async fn put_record(
    cp: &ServerControlPlane,
    key: MetaKey,
    build: impl Fn(Vec<u8>) -> MetaRecord,
) -> Result<(), ApiError> {
    for _ in 0..CAS_RETRIES {
        let cur = current(cp, &key);
        let expected = cur.as_ref().map(|(v, _)| *v);
        let ext = cur.map(|(_, r)| r.ext().to_vec()).unwrap_or_default();
        let res = cp
            .meta()
            .propose(MetaCommand::Put {
                key: key.clone(),
                expected_version: expected,
                record: build(ext),
            })
            .await
            .map_err(meta_unavailable)?;
        match res {
            MetaResult::Ok { .. } => return Ok(()),
            MetaResult::StaleVersion { .. }
            | MetaResult::Conflict { .. }
            | MetaResult::NotFound => {}
            MetaResult::Invalid(msg) => {
                return Err(ApiError::invalid_argument(format!(
                    "metadata rejected the record: {msg}"
                )));
            }
        }
    }
    Err(ApiError::unavailable(
        "the record is being updated concurrently; retry",
    ))
}

/// Delete `key`; 404 (`what`) when absent.
async fn delete_record(cp: &ServerControlPlane, key: MetaKey, what: &str) -> Result<(), ApiError> {
    for _ in 0..CAS_RETRIES {
        let Some((version, _)) = current(cp, &key) else {
            return Err(ApiError::not_found(format!("{what} not found")));
        };
        let res = cp
            .meta()
            .propose(MetaCommand::Delete {
                key: key.clone(),
                expected_version: version,
            })
            .await
            .map_err(meta_unavailable)?;
        match res {
            MetaResult::Ok { .. } => return Ok(()),
            MetaResult::NotFound => return Err(ApiError::not_found(format!("{what} not found"))),
            MetaResult::StaleVersion { .. } | MetaResult::Conflict { .. } => {}
            MetaResult::Invalid(msg) => {
                return Err(ApiError::invalid_argument(format!(
                    "metadata rejected the delete: {msg}"
                )));
            }
        }
    }
    Err(ApiError::unavailable(
        "the record is being updated concurrently; retry",
    ))
}

// ---------------------------------------------------------------------------------
// Secrets
// ---------------------------------------------------------------------------------

/// `<tenant or _>/` for a request tenant (validated; empty = default tenant).
fn tenant_prefix(tenant: Option<&str>) -> Result<(String, Option<String>), ApiError> {
    let t = names::request_tenant(tenant)?;
    let prefix = format!("{}/", t.unwrap_or(DEFAULT_TENANT_TOKEN));
    Ok((prefix, t.map(str::to_owned)))
}

/// The meta record name of secret `name` in `tenant`.
pub fn secret_record_name(tenant: Option<&str>, name: &str) -> Result<String, ApiError> {
    let (prefix, _) = tenant_prefix(tenant)?;
    Ok(format!("{prefix}{name}"))
}

pub(crate) async fn put_secret(
    cp: &ServerControlPlane,
    req: PutSecret,
) -> Result<SecretInfo, ApiError> {
    req.validate()?;
    let (prefix, tenant) = tenant_prefix(req.tenant.as_deref())?;
    let Some(store) = cp.bridge().sealed_store() else {
        return Err(ApiError::precondition_failed(SEALING_KEY_HINT)
            .with_details(serde_json::json!({ "reason": SEALING_KEY_MISSING })));
    };
    let record_name = format!("{prefix}{}", req.name);
    let sealed = store
        .seal(&record_name, &req.value)
        .map_err(|e| ApiError::internal(format!("sealing secret {:?} failed: {e}", req.name)))?;
    let key_id = sealed.key_id.clone();
    put_record(cp, MetaKey::Secret(record_name), |ext| {
        let mut rec = sealed.clone();
        rec.ext = ext;
        MetaRecord::Secret(rec)
    })
    .await?;
    Ok(SecretInfo {
        name: req.name,
        key_id,
        tenant,
    })
}

pub(crate) async fn list_secrets(
    cp: &ServerControlPlane,
    tenant: Option<String>,
) -> Result<Vec<SecretInfo>, ApiError> {
    let (prefix, tenant) = tenant_prefix(tenant.as_deref())?;
    Ok(cp.meta().view().read(|s| {
        s.secrets()
            .filter_map(|v| {
                let name = v.value.name.strip_prefix(&prefix)?;
                if name.starts_with(RESERVED_SECRET_PREFIX) {
                    return None;
                }
                Some(SecretInfo {
                    name: name.to_owned(),
                    key_id: v.value.key_id.clone(),
                    tenant: tenant.clone(),
                })
            })
            .collect()
    }))
}

pub(crate) async fn delete_secret(
    cp: &ServerControlPlane,
    name: String,
    tenant: Option<String>,
) -> Result<(), ApiError> {
    validate_secret_name("name", &name)?;
    let record = secret_record_name(tenant.as_deref(), &name)?;
    delete_record(cp, MetaKey::Secret(record), &format!("secret {name:?}")).await
}

// ---------------------------------------------------------------------------------
// Grants
// ---------------------------------------------------------------------------------

fn grant_info(spec: &GrantSpec) -> GrantInfo {
    GrantInfo {
        id: GrantSpec::id(&spec.module, &spec.secret),
        module: spec.module.clone(),
        secret: spec.secret.clone(),
        origins: spec.origins.iter().map(ToString::to_string).collect(),
        body: spec.body,
    }
}

pub(crate) async fn put_grant(
    cp: &ServerControlPlane,
    g: GrantInfo,
) -> Result<GrantInfo, ApiError> {
    g.validate()?;
    let origins = g
        .origins
        .iter()
        .map(|o| OriginPattern::parse(o).map_err(|e| bad("origins", e.to_string())))
        .collect::<Result<Vec<_>, _>>()?;
    let spec = GrantSpec {
        module: g.module.clone(),
        secret: g.secret.clone(),
        origins,
        body: g.body,
    };
    spec.validate().map_err(|e| bad("grant", e.to_string()))?;
    let rec = spec.to_record();
    put_record(cp, MetaKey::Grant(rec.id.clone()), |ext| {
        MetaRecord::Grant(Grant { ext, ..rec.clone() })
    })
    .await?;
    Ok(grant_info(&spec))
}

pub(crate) async fn list_grants(
    cp: &ServerControlPlane,
    module: Option<String>,
) -> Result<Vec<GrantInfo>, ApiError> {
    let subject = module.map(|m| format!("{SUBJECT_PREFIX}{m}"));
    Ok(cp.meta().view().read(|s| {
        s.grants()
            .filter(|v| v.value.id.starts_with(GRANT_ID_PREFIX))
            .filter(|v| subject.as_ref().is_none_or(|sub| &v.value.subject == sub))
            .filter_map(|v| GrantSpec::from_record(&v.value).ok())
            .map(|spec| grant_info(&spec))
            .collect()
    }))
}

pub(crate) async fn delete_grant(cp: &ServerControlPlane, id: String) -> Result<(), ApiError> {
    if !id.starts_with(GRANT_ID_PREFIX) {
        return Err(bad(
            "id",
            format!("only secret grants ({GRANT_ID_PREFIX}<module>:<secret>) can be deleted here"),
        ));
    }
    delete_record(cp, MetaKey::Grant(id.clone()), &format!("grant {id:?}")).await
}

// ---------------------------------------------------------------------------------
// Connections
// ---------------------------------------------------------------------------------

/// The kind-specific checks of a connection (beyond `ConnectionInfo::validate`).
fn check_connection(cp: &ServerControlPlane, conn: &Connection) -> Result<(), ApiError> {
    match conn.kind.as_str() {
        HTTP_TARGET_KIND => {
            let normalised = OriginPattern::parse(&conn.name)
                .ok()
                .filter(|_| !conn.name.contains('*'))
                .map(|p| p.to_string());
            if normalised.as_deref() != Some(conn.name.as_str()) {
                let hint = normalised
                    .map(|n| format!(" (expected {n:?})"))
                    .unwrap_or_default();
                return Err(bad(
                    "name",
                    format!(
                        "an http-target connection is named by its normalised origin, \
                         scheme://host[:port]{hint}"
                    ),
                ));
            }
            let egress = cp
                .config()
                .egress
                .to_bridge()
                .map_err(|e| ApiError::internal(format!("[egress] config: {e}")))?;
            HttpTarget::from_connection(Some(conn), &egress)
                .map_err(|e| bad("config", e.to_string()))?;
        }
        OBJECT_KIND => {
            let defaults = cp
                .config()
                .connectors
                .to_bridge()
                .map_err(|e| ApiError::internal(format!("[connectors] config: {e}")))?;
            ObjectConnConfig::parse(conn, &defaults).map_err(|e| bad("config", e.to_string()))?;
        }
        kind => {
            SqlConnConfig::parse(&conn.name, kind, &conn.config)
                .map_err(|e| bad("config", e.to_string()))?;
        }
    }
    Ok(())
}

fn connection_info(c: &Connection) -> Result<ConnectionInfo, ApiError> {
    let config = serde_json::from_slice(&c.config).map_err(|e| {
        ApiError::internal(format!("stored config of connection {:?}: {e}", c.name))
    })?;
    Ok(ConnectionInfo {
        name: c.name.clone(),
        kind: c.kind.clone(),
        config,
    })
}

pub(crate) async fn put_connection(
    cp: &ServerControlPlane,
    c: ConnectionInfo,
) -> Result<ConnectionInfo, ApiError> {
    c.validate()?;
    let conn = Connection {
        name: c.name.clone(),
        kind: c.kind.clone(),
        config: serde_json::to_vec(&c.config)
            .map_err(|e| ApiError::internal(format!("encoding config: {e}")))?,
        ext: Vec::new(),
    };
    check_connection(cp, &conn)?;
    put_record(cp, MetaKey::Connection(conn.name.clone()), |ext| {
        MetaRecord::Connection(Connection {
            ext,
            ..conn.clone()
        })
    })
    .await?;
    connection_info(&conn)
}

pub(crate) async fn get_connection(
    cp: &ServerControlPlane,
    name: String,
) -> Result<ConnectionInfo, ApiError> {
    match current(cp, &MetaKey::Connection(name.clone())) {
        Some((_, MetaRecord::Connection(c))) => connection_info(&c),
        _ => Err(ApiError::not_found(format!(
            "connection {name:?} not found"
        ))),
    }
}

pub(crate) async fn list_connections(
    cp: &ServerControlPlane,
) -> Result<Vec<ConnectionInfo>, ApiError> {
    let all: Vec<Connection> = cp
        .meta()
        .view()
        .read(|s| s.connections().map(|v| v.value.clone()).collect());
    all.iter().map(connection_info).collect()
}

pub(crate) async fn delete_connection(
    cp: &ServerControlPlane,
    name: String,
) -> Result<(), ApiError> {
    delete_record(
        cp,
        MetaKey::Connection(name.clone()),
        &format!("connection {name:?}"),
    )
    .await
}

// ---------------------------------------------------------------------------------
// Egress policies
// ---------------------------------------------------------------------------------

fn egress_info(p: &EgressPolicy) -> EgressInfo {
    EgressInfo {
        module: p.name.clone(),
        allow: p.allow.clone(),
    }
}

pub(crate) async fn put_egress(
    cp: &ServerControlPlane,
    e: EgressInfo,
) -> Result<EgressInfo, ApiError> {
    e.validate()?;
    for rule in &e.allow {
        parse_rule(rule).map_err(|err| bad("allow", err.to_string()))?;
    }
    let policy = EgressPolicy {
        name: e.module.clone(),
        allow: e.allow.clone(),
        ext: Vec::new(),
    };
    put_record(cp, MetaKey::Egress(e.module.clone()), |ext| {
        MetaRecord::Egress(EgressPolicy {
            ext,
            ..policy.clone()
        })
    })
    .await?;
    Ok(egress_info(&policy))
}

pub(crate) async fn get_egress(
    cp: &ServerControlPlane,
    module: String,
) -> Result<EgressInfo, ApiError> {
    match current(cp, &MetaKey::Egress(module.clone())) {
        Some((_, MetaRecord::Egress(p))) => Ok(egress_info(&p)),
        _ => Err(ApiError::not_found(format!(
            "egress policy of module {module:?} not found"
        ))),
    }
}

pub(crate) async fn list_egress(cp: &ServerControlPlane) -> Result<Vec<EgressInfo>, ApiError> {
    Ok(cp
        .meta()
        .view()
        .read(|s| s.egress_policies().map(|v| egress_info(&v.value)).collect()))
}

pub(crate) async fn delete_egress(cp: &ServerControlPlane, module: String) -> Result<(), ApiError> {
    delete_record(
        cp,
        MetaKey::Egress(module.clone()),
        &format!("egress policy of module {module:?}"),
    )
    .await
}

// ---------------------------------------------------------------------------------
// Local presign route
// ---------------------------------------------------------------------------------

fn plain(status: u16, msg: &str) -> PresignedObjectResponse {
    PresignedObjectResponse {
        status,
        body: msg.as_bytes().to_vec(),
        content_type: Some("text/plain; charset=utf-8".to_owned()),
    }
}

/// `GET|PUT /v1/objects/{connection}/{key}?exp=&sig=`: verified and served by
/// `ObjectConnector::serve_local`. Bad, tampered or expired signatures are 403, an
/// unknown connection or absent object 404, a connection whose backend is not `local`
/// 400. Error bodies are fixed short texts (no key material, no store paths).
pub(crate) async fn presigned_object(
    cp: &ServerControlPlane,
    req: PresignedObjectRequest,
) -> Result<PresignedObjectResponse, ApiError> {
    // A non-local object connection is a client error, not a missing one. The record is
    // read before the signature check only to choose the status; nothing is served.
    match current(cp, &MetaKey::Connection(req.connection.clone())) {
        Some((_, MetaRecord::Connection(c))) if c.kind == OBJECT_KIND => {
            let backend = serde_json::from_slice::<serde_json::Value>(&c.config)
                .ok()
                .and_then(|v| v.get("backend").and_then(|b| b.as_str()).map(str::to_owned));
            if backend.as_deref() != Some("local") {
                return Ok(plain(
                    400,
                    "this connection's backend is not local; use the backend's own presigned URLs",
                ));
            }
        }
        _ => return Ok(plain(404, "not found")),
    }
    let method = match req.method {
        ostro_api::domain::PresignMethod::Get => "GET",
        ostro_api::domain::PresignMethod::Put => "PUT",
    };
    let res = cp
        .bridge()
        .object_connector()
        .serve_local(
            &req.connection,
            &req.key,
            method,
            req.exp,
            &req.sig,
            req.body.map(Bytes::from),
        )
        .await;
    Ok(match res {
        Ok((status, body, content_type)) => PresignedObjectResponse {
            status,
            body,
            content_type,
        },
        Err(e) => {
            let status = e.status();
            let msg = match e {
                ServeError::Forbidden(VerifyError::Expired) => "presigned URL expired",
                ServeError::Forbidden(_) => "forbidden",
                ServeError::NotFound => "not found",
                ServeError::BadRequest(_) => "bad request",
                ServeError::TooLarge => "payload too large",
                ServeError::Backend(ref m) => {
                    tracing::warn!(connection = %req.connection, error = %m, "presigned object request failed");
                    "object store error"
                }
            };
            plain(status, msg)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_names_follow_the_tenant_convention() {
        assert_eq!(secret_record_name(None, "stripe").unwrap(), "_/stripe");
        assert_eq!(secret_record_name(Some(""), "a/b").unwrap(), "_/a/b");
        assert_eq!(
            secret_record_name(Some("acme"), "stripe").unwrap(),
            "acme/stripe"
        );
        assert!(secret_record_name(Some("_"), "x").is_err());
        assert!(secret_record_name(Some("Bad Tenant"), "x").is_err());
    }

    #[test]
    fn errors_name_the_field() {
        let e = bad("allow", "nope");
        assert_eq!(e.details.as_ref().unwrap()["field"], "allow");
        assert!(e.message.starts_with("allow: "));
    }
}
