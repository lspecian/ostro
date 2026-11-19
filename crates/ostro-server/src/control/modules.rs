//! Modules and workflows (API-02).
//!
//! # Upload
//!
//! 1. The path sha256 must be 64 lowercase hex characters and equal `sha256(body)`.
//! 2. A module already recorded in meta is an idempotent no-op (`created = false`): the
//!    runtime is not touched, so nothing is compiled again.
//! 3. Otherwise the bytes are validated and AOT-compiled (`Runtime::prepare_cached`, which
//!    also warms the L2 cache for the first dispatch) and `describe()` lists the
//!    workflows. Invalid bytes are `400` with the validation message.
//! 4. A `(name, version)` already bound to a different module is `409` naming that sha:
//!    bindings are write-once (research Pitfall 6). Nothing is stored in that case.
//! 5. The blob goes into the node-wide blob store (the executor loads modules from it).
//! 6. Meta: create-only `Workflow(name, version)` records, then a create-only `Active`
//!    record for every workflow name that has none (the first version auto-activates),
//!    and finally the create-only `Module` record. The module record is the commit point:
//!    an upload interrupted before it is simply repeated by the next upload of the same
//!    bytes (existing records with the same sha are accepted). `Conflict` on a create-only
//!    put means a concurrent upload won the race and is accepted unless it bound a
//!    workflow version to a different module.
//!
//! # Activation
//!
//! The CAS token is the `Active(name)` record's version (`record_version`, 0 while no
//! version is active). With `expected_version` the put is an exact CAS and a mismatch is
//! `412`; without it the current version is read and used, retrying if a concurrent
//! activation slips in between (the CAS still decides at apply).

use std::collections::BTreeMap;

use ostro_api::ApiError;
use ostro_api::domain::{
    ActivateWorkflow, ModuleInfo, UploadModule, UploadModuleResult, WorkflowInfo, WorkflowRef,
    WorkflowVersionInfo,
};
use ostro_engine::error::EngineError;
use ostro_meta::{
    ActiveVersion, MetaCommand, MetaError, MetaKey, MetaRecord, MetaResult, MetaState,
    ModuleRecord, Versioned, WorkflowVersion,
};
use sha2::{Digest, Sha256};

use super::ServerControlPlane;

/// Activation attempts without a client CAS token before giving up (each retry means a
/// concurrent activation changed the record in between).
const ACTIVATE_RETRIES: usize = 8;

/// Wasm binary magic (`\0asm`). Core modules and malformed components carrying it are
/// rejected by the engine with a specific message; anything else is rejected up front.
const WASM_MAGIC: [u8; 4] = [0x00, 0x61, 0x73, 0x6d];

fn meta_unavailable(e: MetaError) -> ApiError {
    ApiError::unavailable(format!("metadata unavailable: {e}"))
}

fn hex(sha: &[u8; 32]) -> String {
    ostro_blob::hex(sha)
}

/// Parse a lowercase hex sha256.
fn parse_sha(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 || !s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in s.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let nib = |c: u8| match c {
            b'0'..=b'9' => c - b'0',
            _ => c - b'a' + 10,
        };
        out[i] = (nib(chunk[0]) << 4) | nib(chunk[1]);
    }
    Some(out)
}

fn module_info(r: &Versioned<ModuleRecord>) -> ModuleInfo {
    let mut workflows: Vec<WorkflowRef> = r
        .value
        .workflows
        .iter()
        .map(|(name, version)| WorkflowRef {
            name: name.clone(),
            version: *version,
        })
        .collect();
    workflows.sort();
    ModuleInfo {
        sha256: hex(&r.value.sha256),
        size_bytes: r.value.size,
        language: r.value.language.clone(),
        workflows,
        uploaded_at_ms: r.value.uploaded_at_ms,
    }
}

fn binding_conflict(name: &str, version: u32, existing: &[u8; 32]) -> ApiError {
    let existing = hex(existing);
    ApiError::conflict(format!(
        "workflow {name}@{version} is already registered by module {existing}"
    ))
    .with_details(serde_json::json!({
        "workflow": name,
        "version": version,
        "existing_module_sha256": existing,
    }))
}

/// Engine validation / compile failures are the client's (400); anything else is ours.
fn engine_error(e: EngineError) -> ApiError {
    match e {
        EngineError::Validation(msgs) => {
            ApiError::invalid_argument(format!("invalid Wasm component: {}", msgs.join("; ")))
        }
        // The AOT cache reports compile failures of the submitted bytes this way.
        EngineError::Cache(msg) if msg.starts_with("compile failed") => {
            ApiError::invalid_argument(format!("invalid Wasm component: {msg}"))
        }
        other => ApiError::internal(format!("preparing the module failed: {other}")),
    }
}

/// Propose a create-only put; `Ok` and `Conflict` (already created) both succeed.
async fn create_only(cp: &ServerControlPlane, record: MetaRecord) -> Result<(), ApiError> {
    let key = record.key();
    match cp
        .meta
        .propose(MetaCommand::Put {
            key: key.clone(),
            expected_version: None,
            record,
        })
        .await
        .map_err(meta_unavailable)?
    {
        MetaResult::Ok { .. } | MetaResult::Conflict { .. } => Ok(()),
        other => Err(ApiError::internal(format!(
            "metadata rejected create of {key:?}: {other:?}"
        ))),
    }
}

pub(crate) async fn upload_module(
    cp: &ServerControlPlane,
    req: UploadModule,
) -> Result<UploadModuleResult, ApiError> {
    let sha = parse_sha(&req.sha256)
        .ok_or_else(|| ApiError::invalid_argument("sha256 must be 64 lowercase hex characters"))?;
    let actual: [u8; 32] = Sha256::digest(&req.wasm).into();
    if actual != sha {
        let actual = hex(&actual);
        return Err(ApiError::invalid_argument(format!(
            "sha256 mismatch: path says {}, body hashes to {actual}",
            req.sha256
        ))
        .with_details(serde_json::json!({ "actual_sha256": actual })));
    }
    if let Some(existing) = cp.meta.view().read(|s| s.module(&sha).cloned()) {
        return Ok(UploadModuleResult {
            module: module_info(&existing),
            created: false,
        });
    }

    // Validate + AOT compile (+ warm L2), then describe.
    if !req.wasm.starts_with(&WASM_MAGIC) {
        return Err(ApiError::invalid_argument(
            "invalid Wasm component: the body is not a Wasm binary (missing the \\0asm magic)",
        ));
    }
    let prepared = cp
        .runtime
        .prepare_cached(&req.wasm)
        .await
        .map_err(engine_error)?;
    let metas =
        cp.runtime.describe(&prepared).await.map_err(|e| {
            ApiError::invalid_argument(format!("describe() of the module failed: {e}"))
        })?;
    let mut by_key: BTreeMap<(String, u32), _> = BTreeMap::new();
    for m in metas {
        if m.name.is_empty() {
            return Err(ApiError::invalid_argument(
                "describe() returned a workflow with an empty name",
            ));
        }
        by_key.entry((m.name.clone(), m.version)).or_insert(m);
    }

    // Write-once bindings: check before storing anything.
    cp.meta.view().read(|s| {
        for (name, version) in by_key.keys() {
            if let Some(w) = s.workflow(name, *version)
                && w.value.module_sha256 != sha
            {
                return Err(binding_conflict(name, *version, &w.value.module_sha256));
            }
        }
        Ok(())
    })?;

    let blobs = std::sync::Arc::clone(cp.blobs()?);
    let wasm = req.wasm.clone();
    let blob = tokio::task::spawn_blocking(move || blobs.put(&wasm))
        .await
        .map_err(|e| ApiError::internal(format!("blob store task failed: {e}")))?
        .map_err(|e| ApiError::unavailable(format!("storing the module failed: {e}")))?;
    debug_assert_eq!(blob.sha256, sha);

    let now = cp.now_ms();
    for ((name, version), m) in &by_key {
        create_only(
            cp,
            MetaRecord::Workflow(WorkflowVersion {
                name: name.clone(),
                version: *version,
                module_sha256: sha,
                default_queue: m.default_queue.clone(),
                run_timeout_ms: m.run_timeout_ms,
                created_at_ms: now,
                // 05-24 sets the tenant-qualified upload name (D23, Phase 14 F4).
                module: String::new(),
                ext: Vec::new(),
            }),
        )
        .await?;
        // A concurrent upload of a different module may have won the create.
        if let Some(w) = cp.meta.view().read(|s| s.workflow(name, *version).cloned())
            && w.value.module_sha256 != sha
        {
            return Err(binding_conflict(name, *version, &w.value.module_sha256));
        }
    }
    let names: Vec<String> = {
        let mut n: Vec<String> = by_key.keys().map(|(name, _)| name.clone()).collect();
        n.dedup();
        n
    };
    for name in names {
        if cp.meta.view().read(|s| s.active(&name).is_none()) {
            // First registered version of this name (lowest of this module) goes active.
            let version = by_key
                .keys()
                .filter(|(n, _)| *n == name)
                .map(|(_, v)| *v)
                .min()
                .unwrap_or(0);
            create_only(
                cp,
                MetaRecord::Active(ActiveVersion {
                    name: name.clone(),
                    version,
                    ext: Vec::new(),
                }),
            )
            .await?;
        }
    }
    let record = ModuleRecord {
        sha256: sha,
        size: req.wasm.len() as u64,
        language: req.language.filter(|l| !l.is_empty()),
        workflows: by_key.keys().cloned().collect(),
        uploaded_at_ms: now,
        ext: Vec::new(),
    };
    let created = match cp
        .meta
        .propose(MetaCommand::Put {
            key: MetaKey::Module(sha),
            expected_version: None,
            record: MetaRecord::Module(record),
        })
        .await
        .map_err(meta_unavailable)?
    {
        MetaResult::Ok { .. } => true,
        MetaResult::Conflict { .. } => false, // a concurrent upload of the same bytes
        other => {
            return Err(ApiError::internal(format!(
                "metadata rejected the module record: {other:?}"
            )));
        }
    };
    let stored = cp
        .meta
        .view()
        .read(|s| s.module(&sha).cloned())
        .ok_or_else(|| ApiError::internal("module record missing after upload"))?;
    Ok(UploadModuleResult {
        module: module_info(&stored),
        created,
    })
}

pub(crate) fn get_module(cp: &ServerControlPlane, sha: &str) -> Result<ModuleInfo, ApiError> {
    let key = parse_sha(sha)
        .ok_or_else(|| ApiError::invalid_argument("sha256 must be 64 lowercase hex characters"))?;
    cp.meta
        .view()
        .read(|s| s.module(&key).map(module_info))
        .ok_or_else(|| ApiError::not_found(format!("module {sha} not found")))
}

/// The workflow `name` as the API shows it, if any version is registered.
fn workflow_info(s: &MetaState, name: &str) -> Option<WorkflowInfo> {
    let versions: Vec<WorkflowVersionInfo> = s
        .workflows()
        .filter(|w| w.value.name == name)
        .map(|w| version_info(&w.value))
        .collect();
    if versions.is_empty() {
        return None;
    }
    let active = s.active(name);
    Some(WorkflowInfo {
        name: name.to_string(),
        active_version: active.map(|a| a.value.version),
        versions,
        record_version: active.map_or(0, |a| a.version),
    })
}

fn version_info(w: &WorkflowVersion) -> WorkflowVersionInfo {
    WorkflowVersionInfo {
        version: w.version,
        module_sha256: hex(&w.module_sha256),
        default_queue: w.default_queue.clone(),
        run_timeout_ms: w.run_timeout_ms,
    }
}

pub(crate) fn list_workflows(cp: &ServerControlPlane) -> Vec<WorkflowInfo> {
    cp.meta.view().read(|s| {
        // Workflow records are ordered by (name, version): group consecutive names.
        let mut out: Vec<WorkflowInfo> = Vec::new();
        for w in s.workflows() {
            match out.last_mut() {
                Some(info) if info.name == w.value.name => {
                    info.versions.push(version_info(&w.value));
                }
                _ => {
                    let active = s.active(&w.value.name);
                    out.push(WorkflowInfo {
                        name: w.value.name.clone(),
                        active_version: active.map(|a| a.value.version),
                        versions: vec![version_info(&w.value)],
                        record_version: active.map_or(0, |a| a.version),
                    });
                }
            }
        }
        out
    })
}

pub(crate) async fn activate_workflow(
    cp: &ServerControlPlane,
    req: ActivateWorkflow,
) -> Result<WorkflowInfo, ApiError> {
    let precondition = |current: u64, expected: u64| {
        ApiError::precondition_failed(format!(
            "workflow {} record version is {current}, expected {expected}",
            req.name
        ))
        .with_details(serde_json::json!({ "record_version": current }))
    };
    for _ in 0..ACTIVATE_RETRIES {
        let (info, registered) = cp.meta.view().read(|s| {
            (
                workflow_info(s, &req.name),
                s.workflow(&req.name, req.version).is_some(),
            )
        });
        let info =
            info.ok_or_else(|| ApiError::not_found(format!("workflow {} not found", req.name)))?;
        let current = info.record_version;
        if let Some(expected) = req.expected_version
            && expected != current
        {
            return Err(precondition(current, expected));
        }
        if !registered {
            return Err(ApiError::not_found(format!(
                "workflow {}@{} not found",
                req.name, req.version
            )));
        }
        if info.active_version == Some(req.version) {
            return Ok(info);
        }
        let res = cp
            .meta
            .propose(MetaCommand::Put {
                key: MetaKey::Active(req.name.clone()),
                // 0 = no active record yet: create.
                expected_version: (current != 0).then_some(current),
                record: MetaRecord::Active(ActiveVersion {
                    name: req.name.clone(),
                    version: req.version,
                    ext: Vec::new(),
                }),
            })
            .await
            .map_err(meta_unavailable)?;
        match res {
            MetaResult::Ok { .. } => {
                return cp
                    .meta
                    .view()
                    .read(|s| workflow_info(s, &req.name))
                    .ok_or_else(|| ApiError::internal("workflow vanished after activation"));
            }
            MetaResult::StaleVersion { current: now } | MetaResult::Conflict { current: now } => {
                if let Some(expected) = req.expected_version {
                    return Err(precondition(now, expected));
                }
                // A concurrent activation won; re-read and try again.
            }
            MetaResult::NotFound => {
                if let Some(expected) = req.expected_version {
                    return Err(precondition(0, expected));
                }
            }
            MetaResult::Invalid(msg) => {
                return Err(ApiError::internal(format!(
                    "metadata rejected the activation: {msg}"
                )));
            }
        }
    }
    Err(ApiError::unavailable(format!(
        "workflow {} is being activated concurrently; retry",
        req.name
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sha_accepts_only_lowercase_hex() {
        let s = "00ff".repeat(16);
        let b = parse_sha(&s).expect("valid");
        assert_eq!(b[0], 0x00);
        assert_eq!(b[1], 0xff);
        assert_eq!(hex(&b), s);
        assert!(parse_sha(&s.to_uppercase()).is_none());
        assert!(parse_sha(&s[..62]).is_none());
        assert!(parse_sha(&"zz".repeat(32)).is_none());
    }
}
