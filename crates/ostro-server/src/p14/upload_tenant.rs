//! Phase 14 (14-16, D-P14-9): per-tenant module registration of an already stored sha.
//!
//! An upload of bytes that are already stored (any tenant) never compiles them again but
//! still registers the uploading tenant's `WorkflowVersion`s (`control/modules.rs`, the
//! existing-sha branch; the Phase 4 "existing sha = no-op" short-circuit is gone since
//! Phase 13/F4). This hook runs at the start of that branch and records the uploader in the
//! module record's ext 114 `ModuleUploaders` (sorted, unique tenant ids, `_` = the default
//! tenant) with a CAS on the record version:
//!
//! - written only when `compat::require(Feature::ModuleUploadersExt)` passes (finalized
//!   feature level 2); otherwise nothing is written and the upload proceeds;
//! - the first write seeds the list with the tenants the record already registers
//!   (`ModuleRecord.workflows` prefixes), so the first uploader is listed too;
//! - an already listed tenant is a no-op (no meta write);
//! - other ext tags of the record (13-14's ext 101, 14-19's ext 113) are kept;
//! - a CAS that keeps losing answers 503 (the upload is refused, nothing else written).
//!
//! Blob and AOT caches stay shared by sha (identical bytes).

use std::collections::BTreeSet;

use ostro_api::ApiError;
use ostro_meta::ext::{ModuleUploaders, get_module_uploaders, put_module_uploaders};
use ostro_meta::{MetaCommand, MetaKey, MetaRecord, MetaResult, ModuleRecord};
use ostro_types::compat::Feature;

use super::{P14Ctx, RegisterDecision};
use crate::control::names::{DEFAULT_TENANT_TOKEN, split_qualified};

/// CAS attempts before a concurrently updated module record answers 503.
const CAS_RETRIES: usize = 8;

/// The ext 114 spelling of a tenant (`_` = the default tenant).
fn uploader(tenant: Option<&str>) -> String {
    tenant
        .filter(|t| !t.is_empty())
        .unwrap_or(DEFAULT_TENANT_TOKEN)
        .to_owned()
}

/// The ext 114 list after `tenant` uploaded `rec` (`None` = already listed, no write).
pub fn uploaders_after(
    rec: &ModuleRecord,
    tenant: Option<&str>,
) -> Result<Option<ModuleUploaders>, String> {
    let me = uploader(tenant);
    let current = get_module_uploaders(&rec.ext).map_err(|e| e.to_string())?;
    let listed = current.is_some();
    let mut set: BTreeSet<String> = match current {
        Some(mu) => mu.tenants.into_iter().collect(),
        // First write: everyone the record already registers.
        None => rec
            .workflows
            .iter()
            .map(|(w, _)| uploader(split_qualified(w).0))
            .collect(),
    };
    if !set.insert(me) && listed {
        return Ok(None);
    }
    Ok(Some(ModuleUploaders {
        tenants: set.into_iter().collect(),
    }))
}

/// The existing-sha branch of an upload by `tenant`: record the uploader (ext 114) and let
/// the upload register the tenant's versions ([`RegisterDecision::Proceed`]).
pub async fn register_existing(
    ctx: &P14Ctx,
    sha: [u8; 32],
    tenant: Option<&str>,
) -> RegisterDecision {
    if ostro_cluster::compat::require(ctx.meta.view(), Feature::ModuleUploadersExt, ctx.level_max)
        .is_err()
    {
        return RegisterDecision::Proceed;
    }
    match record_uploader(ctx, sha, tenant).await {
        Ok(()) => RegisterDecision::Proceed,
        Err(e) => RegisterDecision::Deny(e),
    }
}

async fn record_uploader(
    ctx: &P14Ctx,
    sha: [u8; 32],
    tenant: Option<&str>,
) -> Result<(), ApiError> {
    for _ in 0..CAS_RETRIES {
        // The record vanished (module GC raced the upload): the upload path reports it.
        let Some(cur) = ctx.meta.view().read(|s| s.module(&sha).cloned()) else {
            return Ok(());
        };
        let Some(mu) = uploaders_after(&cur.value, tenant)
            .map_err(|e| ApiError::internal(format!("module ext 114: {e}")))?
        else {
            return Ok(());
        };
        let mut record = cur.value.clone();
        record.ext = put_module_uploaders(&record.ext, &mu)
            .map_err(|e| ApiError::internal(format!("module ext 114: {e}")))?;
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
                    "metadata rejected the module uploaders update: {other:?}"
                )));
            }
        }
    }
    Err(ApiError::unavailable(
        "the module record is being updated concurrently; retry",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(workflows: &[&str], ext: Vec<u8>) -> ModuleRecord {
        ModuleRecord {
            sha256: [7; 32],
            size: 1,
            language: None,
            workflows: workflows.iter().map(|w| ((*w).to_owned(), 1)).collect(),
            uploaded_at_ms: 1,
            ext,
        }
    }

    #[test]
    fn first_write_seeds_from_the_registered_tenants() {
        let r = rec(&["a/w", "w"], Vec::new());
        let mu = uploaders_after(&r, Some("b")).unwrap().expect("write");
        assert_eq!(mu.tenants, ["_", "a", "b"]);
        // The same tenant again on a record without ext still writes (seeding).
        let mu = uploaders_after(&r, Some("a")).unwrap().expect("seed");
        assert_eq!(mu.tenants, ["_", "a"]);
    }

    #[test]
    fn listed_tenant_is_a_no_op_and_others_are_added_sorted() {
        let ext = put_module_uploaders(
            &[],
            &ModuleUploaders {
                tenants: vec!["a".into()],
            },
        )
        .unwrap();
        let r = rec(&["a/w"], ext);
        assert_eq!(uploaders_after(&r, Some("a")).unwrap(), None);
        let mu = uploaders_after(&r, None).unwrap().expect("write");
        assert_eq!(mu.tenants, ["_", "a"]);
    }
}
