//! D23 tenant-qualified names (Phase 14 F1), validated at the API.
//!
//! - **Tenant ids:** `^[a-z0-9][a-z0-9-]{0,62}$`. `_` is the reserved spelling of the
//!   default tenant (RESERVATIONS §11) and is never accepted from a client. An absent or
//!   empty tenant is the default tenant.
//! - **Resource names** (workflows, queues, deployed module names, and later schedules,
//!   webhooks and secrets): `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`, so no `/`, no `:` and no
//!   leading `__` (`__` is reserved for `__ostro/` and `__cron/`).
//! - **Storage form:** `<tenant>/<name>` for a non-default tenant, `<name>` for the default
//!   tenant. A key is qualified iff its prefix up to the first `/` is a tenant id.
//!
//! The grammar itself lives in `ostro-meta` (meta apply checks `WorkflowVersion.module`
//! with it), so the API and apply can never disagree. Apply stays permissive for
//! everything else: a new apply-time rejection after the freeze would change the replay of
//! the format corpus, so every rule here is enforced only on the request path.
//!
//! Signal names are not resource names (05-04): they keep only their 1..=256-byte limit.

use ostro_api::ApiError;
use ostro_meta::{is_resource_name, is_tenant_id};

/// The reserved spelling of the default tenant.
pub const DEFAULT_TENANT_TOKEN: &str = "_";

fn invalid(field: &str, msg: String) -> ApiError {
    ApiError::invalid_argument(msg).with_details(serde_json::json!({ "field": field }))
}

/// Check a client-supplied tenant id (400 with `details.field = "tenant"`).
pub fn validate_tenant(tenant: &str) -> Result<(), ApiError> {
    if tenant == DEFAULT_TENANT_TOKEN {
        return Err(invalid(
            "tenant",
            "tenant \"_\" is reserved for the default tenant; omit the tenant instead".into(),
        ));
    }
    if !is_tenant_id(tenant) {
        return Err(invalid(
            "tenant",
            format!("tenant {tenant:?} must match ^[a-z0-9][a-z0-9-]{{0,62}}$"),
        ));
    }
    Ok(())
}

/// Check a resource name (400 with `details.field = field`).
pub fn validate_name(field: &str, name: &str) -> Result<(), ApiError> {
    if !is_resource_name(name) {
        return Err(invalid(
            field,
            format!(
                "{field} {name:?} must match ^[A-Za-z0-9][A-Za-z0-9._-]{{0,127}}$ \
                 (no '/', no ':', no leading '__')"
            ),
        ));
    }
    Ok(())
}

/// The request's tenant: `None` (default tenant) when absent or empty, else a validated
/// tenant id.
pub fn request_tenant(tenant: Option<&str>) -> Result<Option<&str>, ApiError> {
    match tenant {
        None | Some("") => Ok(None),
        Some(t) => validate_tenant(t).map(|()| Some(t)),
    }
}

/// The storage form of `name` in `tenant`: `<tenant>/<name>`, or the bare name for the
/// default tenant (`None`, `""` or `_`).
pub fn qualify(tenant: Option<&str>, name: &str) -> String {
    match tenant {
        None | Some("") | Some(DEFAULT_TENANT_TOKEN) => name.to_owned(),
        Some(t) => format!("{t}/{name}"),
    }
}

/// Split a storage key into `(tenant, name)`. The key is qualified iff its prefix up to the
/// first `/` matches the tenant grammar; otherwise the whole key is a default-tenant name.
pub fn split_qualified(key: &str) -> (Option<&str>, &str) {
    match key.split_once('/') {
        Some((t, rest)) if is_tenant_id(t) => (Some(t), rest),
        _ => (None, key),
    }
}

/// Resolve a client-supplied resource name in `tenant` (already validated) to its storage
/// form. The name may be bare or already qualified; a qualified name must carry the
/// request's own tenant (D23: `tenant(name) == tenant`), and the module-local part must
/// match the resource grammar.
pub fn qualify_request_name(
    field: &str,
    tenant: Option<&str>,
    raw: &str,
) -> Result<String, ApiError> {
    let (prefix, local) = split_qualified(raw);
    if prefix.is_some() && prefix != tenant {
        return Err(invalid(
            field,
            format!(
                "{field} {raw:?} is qualified with tenant {:?} but the request's tenant is {}",
                prefix.unwrap_or_default(),
                tenant.map_or_else(|| "the default tenant".to_owned(), |t| format!("{t:?}")),
            ),
        ));
    }
    validate_name(field, local)?;
    Ok(qualify(tenant, local))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_grammar_boundaries() {
        for ok in ["a", "0", "acme", "a-b", "t-g", "a1-", &"a".repeat(63)] {
            assert!(validate_tenant(ok).is_ok(), "{ok:?}");
        }
        for bad in [
            "",
            "_",
            "-a",
            "A",
            "acme_co",
            "a.b",
            "a/b",
            "a:b",
            "é",
            &"a".repeat(64),
        ] {
            let e = validate_tenant(bad).expect_err(bad);
            assert_eq!(e.details.unwrap()["field"], "tenant");
        }
        assert!(
            validate_tenant("_")
                .unwrap_err()
                .message
                .contains("reserved")
        );
    }

    #[test]
    fn name_grammar_boundaries() {
        for ok in [
            "w",
            "W",
            "9",
            "sdk-basic",
            "a.b_c-d",
            "a__b",
            "a_",
            &"x".repeat(128),
        ] {
            assert!(validate_name("workflow", ok).is_ok(), "{ok:?}");
        }
        for bad in [
            "",
            "a/b",
            "x:y",
            "__z",
            "_z",
            ".a",
            "-a",
            "a b",
            "https://h",
            "é",
            &"x".repeat(129),
        ] {
            let e = validate_name("queue", bad).expect_err(bad);
            assert_eq!(e.details.unwrap()["field"], "queue");
        }
    }

    #[test]
    fn request_tenant_defaults() {
        assert_eq!(request_tenant(None).unwrap(), None);
        assert_eq!(request_tenant(Some("")).unwrap(), None);
        assert_eq!(request_tenant(Some("acme")).unwrap(), Some("acme"));
        assert!(request_tenant(Some("_")).is_err());
        assert!(request_tenant(Some("Acme")).is_err());
    }

    #[test]
    fn qualify_and_split_round_trip() {
        assert_eq!(qualify(None, "w"), "w");
        assert_eq!(qualify(Some(""), "w"), "w");
        assert_eq!(qualify(Some("_"), "w"), "w");
        assert_eq!(qualify(Some("acme"), "w"), "acme/w");
        assert_eq!(split_qualified("acme/w"), (Some("acme"), "w"));
        assert_eq!(split_qualified("w"), (None, "w"));
        // Not a tenant prefix: the whole key is a default-tenant name.
        assert_eq!(split_qualified("Acme/w"), (None, "Acme/w"));
        assert_eq!(split_qualified("https://h:1/x"), (None, "https://h:1/x"));
        assert_eq!(split_qualified("__cron/s"), (None, "__cron/s"));
        // Only the first '/' separates the tenant.
        assert_eq!(split_qualified("acme/a/b"), (Some("acme"), "a/b"));
        for t in [None, Some("acme"), Some("a-1")] {
            let q = qualify(t, "sdk-basic");
            assert_eq!(split_qualified(&q), (t, "sdk-basic"));
            assert!(ostro_meta::is_qualified_name(&q), "{q}");
        }
    }

    #[test]
    fn request_names_carry_the_request_tenant() {
        assert_eq!(qualify_request_name("workflow", None, "w").unwrap(), "w");
        assert_eq!(
            qualify_request_name("workflow", Some("acme"), "w").unwrap(),
            "acme/w"
        );
        assert_eq!(
            qualify_request_name("workflow", Some("acme"), "acme/w").unwrap(),
            "acme/w"
        );
        // A qualified name must carry the request's own tenant.
        for (t, raw) in [(None, "acme/w"), (Some("beta"), "acme/w"), (None, "a/b")] {
            let e = qualify_request_name("workflow", t, raw).expect_err(raw);
            assert_eq!(e.details.unwrap()["field"], "workflow");
        }
        // The local part must match the grammar ("Acme/w" is not qualified: '/' is bad).
        for raw in ["Acme/w", "x:y", "__z", "acme/x:y", "acme/a/b", ""] {
            assert!(
                qualify_request_name("queue", Some("acme"), raw).is_err(),
                "{raw:?}"
            );
        }
    }
}
