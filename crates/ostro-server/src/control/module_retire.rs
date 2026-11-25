//! Module retirement (13-15, CLUS-07 / D-P13-20).
//!
//! `DELETE /v1/modules/{sha}` marks a module and every workflow version registered from it
//! Retired (ext tag 102, [`ostro_meta::ext::EXT_RETIRED`] `Retired{retired_at_ms}`).
//!
//! Rules:
//! - the module must exist (404);
//! - no workflow version of it may be Active (409 `module_active`, the active versions in
//!   `details.workflows`); activate another version first;
//! - the write is gated: `compat::require(RetiredExt)` before any proposal (P13-F1 rule 1;
//!   409 `feature_not_enabled` before the cluster finalized level 2; 13-18 aligned every
//!   gate on 409, D-P13-17). Apply never validates
//!   the tag (rule 5);
//! - read-modify-write with CAS on each record (Module first, so new starts are refused at
//!   once, then each WorkflowVersion); a lost CAS re-reads and retries;
//! - idempotent: retiring a retired module completes any missing version mark and answers
//!   with the original `retired_at_ms`.
//!
//! The meta read runs behind the ReadIndex barrier ([`MetaHandle::read_index_barrier`]).
//!
//! A retired module refuses new runs: the start path ([`super::runs`], explicit version or
//! active-version resolution) and activation ([`super::modules`]) call
//! [`check_not_retired`] (409 `module_retired`); 13-17's repin targets use it too.
//! Module GC (`p13::module_gc`) deletes a retired module once no run needs it.
//!
//! [`MetaHandle::read_index_barrier`]: ostro_meta::MetaHandle::read_index_barrier

use ostro_api::phase13::{RetireModuleResponse, WorkflowVersionRef};
use ostro_api::{ApiError, ErrorCode};
use ostro_cluster::compat::{self, FeatureNotEnabled};
use ostro_meta::ext::{Retired, get_retired, put_retired};
use ostro_meta::{MetaCommand, MetaError, MetaKey, MetaRecord, MetaResult, MetaState, MetaView};
use ostro_types::compat::Feature;

use crate::p13::P13Ctx;

/// `details.reason` of a start, activation or repin on a retired module.
pub const REASON_RETIRED: &str = "module_retired";
/// `details.reason` of a retire refused because a version of the module is active.
pub const REASON_ACTIVE: &str = "module_active";
/// `details.reason` of a gated write before the cluster finalized the feature's level.
pub const REASON_FEATURE: &str = "feature_not_enabled";

/// CAS attempts of one retire call before it answers 503.
const RETIRE_ATTEMPTS: usize = 8;

fn hex(sha: &[u8; 32]) -> String {
    ostro_blob::hex(sha)
}

/// Parse 64 lowercase hex digits.
pub fn parse_sha(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let nib = |c: u8| match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            _ => None,
        };
        out[i] = (nib(chunk[0])? << 4) | nib(chunk[1])?;
    }
    Some(out)
}

fn meta_unavailable(e: MetaError) -> ApiError {
    ApiError::unavailable(format!("metadata unavailable: {e}"))
}

/// 409 `feature_not_enabled` ([`FeatureNotEnabled::HTTP_STATUS`], D-P13-17): the one shape
/// of every gated refusal (retire, the cluster API's gated writes, migrate, the upload gate;
/// 13-18 aligned them).
pub(crate) fn feature_error(f: FeatureNotEnabled) -> ApiError {
    ApiError::conflict(f.detail()).with_details(serde_json::json!({
        "reason": REASON_FEATURE,
        "feature": f.feature.name(),
        "needed": f.needed,
        "finalized": f.finalized,
    }))
}

/// The `Retired` mark of a record's ext (an undecodable tag 102 counts as retired at 0:
/// the safe direction refuses starts and never lets GC skip its grace by accident, since
/// GC re-reads and requires a decodable mark).
fn retired_mark(ext: &[u8]) -> Option<Retired> {
    match get_retired(ext) {
        Ok(r) => r,
        Err(_) => Some(Retired { retired_at_ms: 0 }),
    }
}

/// When module `sha` was retired, if it is (its Module record carries ext 102).
pub fn retired_at(state: &MetaState, sha: &[u8; 32]) -> Option<u64> {
    let m = state.module(sha)?;
    retired_mark(&m.value.ext).map(|r| r.retired_at_ms)
}

/// Whether module `sha` is retired (its Module record carries ext 102).
pub fn is_retired(view: &MetaView, sha: &[u8; 32]) -> bool {
    view.read(|s| retired_at(s, sha).is_some())
}

/// `module_retired` (409) when module `sha` is retired.
pub fn check_not_retired(view: &MetaView, sha: &[u8; 32]) -> Result<(), ApiError> {
    if is_retired(view, sha) {
        return Err(retired_error(sha));
    }
    Ok(())
}

/// The 409 `module_retired` error for `sha`.
pub fn retired_error(sha: &[u8; 32]) -> ApiError {
    let hex = hex(sha);
    ApiError::conflict(format!(
        "module {hex} is retired; it takes no new runs, activations or repins"
    ))
    .with_details(serde_json::json!({ "reason": REASON_RETIRED, "module": hex }))
}

/// Every workflow version registered from `sha` (the WorkflowVersion records are the source
/// of truth; the Module record's `workflows` list is folded in for completeness).
fn versions_of(state: &MetaState, sha: &[u8; 32]) -> Vec<(String, u32)> {
    let mut out: Vec<(String, u32)> = state
        .workflows()
        .filter(|w| &w.value.module_sha256 == sha)
        .map(|w| (w.value.name.clone(), w.value.version))
        .collect();
    if let Some(m) = state.module(sha) {
        for (n, v) in &m.value.workflows {
            if state
                .workflow(n, *v)
                .is_some_and(|w| &w.value.module_sha256 == sha)
                && !out.iter().any(|(on, ov)| on == n && ov == v)
            {
                out.push((n.clone(), *v));
            }
        }
    }
    out.sort();
    out
}

/// The versions of `sha` that are some workflow's active version.
pub fn active_versions(state: &MetaState, sha: &[u8; 32]) -> Vec<(String, u32)> {
    versions_of(state, sha)
        .into_iter()
        .filter(|(n, v)| state.active(n).is_some_and(|a| a.value.version == *v))
        .collect()
}

fn refs(v: &[(String, u32)]) -> Vec<WorkflowVersionRef> {
    v.iter()
        .map(|(name, version)| WorkflowVersionRef {
            name: name.clone(),
            version: *version,
        })
        .collect()
}

/// What one pass of [`retire`] has to write next.
enum Step {
    Done(RetireModuleResponse),
    Write(Box<MetaCommand>),
}

fn plan_step(state: &MetaState, sha: &[u8; 32], now_ms: u64) -> Result<Step, ApiError> {
    let hex = hex(sha);
    let Some(module) = state.module(sha) else {
        return Err(ApiError::not_found(format!("module {hex} not found")));
    };
    let active = active_versions(state, sha);
    if !active.is_empty() {
        let list: Vec<String> = active.iter().map(|(n, v)| format!("{n}@{v}")).collect();
        return Err(ApiError::conflict(format!(
            "module {hex} has active workflow versions ({}); activate another version first",
            list.join(", ")
        ))
        .with_details(serde_json::json!({
            "reason": REASON_ACTIVE,
            "module": hex,
            "workflows": refs(&active),
        })));
    }
    let versions = versions_of(state, sha);
    let retired_at_ms = match retired_mark(&module.value.ext) {
        Some(r) => r.retired_at_ms,
        None => {
            let mark = Retired {
                retired_at_ms: now_ms,
            };
            let mut rec = module.value.clone();
            rec.ext = put_retired(&rec.ext, &mark)
                .map_err(|e| ApiError::internal(format!("module {hex} ext: {e}")))?;
            return Ok(Step::Write(Box::new(MetaCommand::Put {
                key: MetaKey::Module(*sha),
                expected_version: Some(module.version),
                record: MetaRecord::Module(rec),
            })));
        }
    };
    for (name, version) in &versions {
        let Some(w) = state.workflow(name, *version) else {
            continue;
        };
        if retired_mark(&w.value.ext).is_none() {
            let mut rec = w.value.clone();
            rec.ext = put_retired(&rec.ext, &Retired { retired_at_ms })
                .map_err(|e| ApiError::internal(format!("workflow {name}@{version} ext: {e}")))?;
            return Ok(Step::Write(Box::new(MetaCommand::Put {
                key: MetaKey::Workflow(name.clone(), *version),
                expected_version: Some(w.version),
                record: MetaRecord::Workflow(rec),
            })));
        }
    }
    Ok(Step::Done(RetireModuleResponse {
        sha256: hex,
        retired_at_ms,
        versions: refs(&versions),
    }))
}

/// Retire module `sha` (64 lowercase hex digits) and its workflow versions.
pub async fn retire(ctx: &P13Ctx, sha: String) -> Result<RetireModuleResponse, ApiError> {
    let sha = parse_sha(&sha).ok_or_else(|| {
        ApiError::invalid_argument("sha256: expected 64 lowercase hex digits")
            .with_details(serde_json::json!({ "field": "sha256" }))
    })?;
    ctx.meta
        .read_index_barrier()
        .await
        .map_err(meta_unavailable)?;
    let mut gated = false;
    for _ in 0..RETIRE_ATTEMPTS {
        let now_ms = ctx.clock.now_ms();
        let step = ctx.meta.view().read(|s| plan_step(s, &sha, now_ms))?;
        let cmd = match step {
            Step::Done(r) => return Ok(r),
            Step::Write(cmd) => cmd,
        };
        if !gated {
            // P13-F1 rule 1: ext 102 is emitted only once the cluster finalized its level.
            compat::require(ctx.meta.view(), Feature::RetiredExt, ctx.level_max)
                .map_err(feature_error)?;
            gated = true;
        }
        match ctx.meta.propose(*cmd).await.map_err(meta_unavailable)? {
            MetaResult::Ok { .. } => {}
            // A concurrent writer (retire, activation, GC, re-upload) won: re-read.
            MetaResult::StaleVersion { .. }
            | MetaResult::Conflict { .. }
            | MetaResult::NotFound => {}
            MetaResult::Invalid(msg) => {
                return Err(ApiError::internal(format!(
                    "metadata rejected the retirement: {msg}"
                )));
            }
        }
    }
    Err(ApiError::new(
        ErrorCode::Unavailable,
        format!("module {} is being changed concurrently; retry", hex(&sha)),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sha_lowercase_hex_only() {
        let s = "0a".repeat(32);
        assert_eq!(parse_sha(&s), Some([0x0a; 32]));
        assert!(parse_sha(&s.to_uppercase()).is_none());
        assert!(parse_sha(&s[..62]).is_none());
        assert!(parse_sha(&"zz".repeat(32)).is_none());
    }

    #[test]
    fn retired_error_shape() {
        let e = retired_error(&[1; 32]);
        assert_eq!(e.code, ErrorCode::Conflict);
        assert_eq!(e.reason(), Some(REASON_RETIRED));
    }
}
