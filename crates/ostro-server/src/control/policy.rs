//! Workflow and queue run policies (OPS-06 / WF-09, D9; plan 05-13).
//!
//! `PUT`/`GET /v1/workflows/{name}/policy` and `/v1/queues/{name}/policy` read and write the
//! 05-07 `WorkflowPolicy` / `QueuePolicy` meta records through the server's
//! [`ostro_meta::MetaHandle`] with versioned CAS (create with `expected_version: None`,
//! update with the exact current version; a lost race re-reads and retries).
//!
//! # Names (D23)
//!
//! The route carries one name and no tenant parameter. The name may be bare (the default
//! tenant) or tenant-qualified (`acme/w`, which over REST needs `%2F`). The tenant is taken
//! from a valid prefix ([`names::split_qualified`]), checked with
//! [`names::request_tenant`], and the name is then normalised with
//! [`names::qualify_request_name`], so the meta key is exactly the string a start submits
//! in `StartRun.workflow` / `queue` and [`crate::retention::resolve_run_policy`] reads.
//!
//! # Semantics
//!
//! - **Workflows must exist** (any registered version under the qualified name): an unknown
//!   workflow is 404 for both set and get, matching the fake. A policy for a workflow that
//!   does not exist yet would silently apply to a later upload of the same name, and a typo
//!   would go unnoticed.
//! - **Queues are implicit**: any valid queue name may carry a policy (queues have no
//!   record of their own).
//! - `PUT` replaces the Phase 5 fields (`retention_ms`, `schedule_to_start_ms`; omitted =
//!   cleared). The Phase 7 F2 fields of an existing record (pause, placement, flow
//!   control, `ext`) are preserved untouched.
//! - Policy changes affect **future runs only** (values are resolved at start).

use ostro_api::ApiError;
use ostro_api::domain::RunPolicy;
use ostro_meta::WorkflowPolicy;
use ostro_meta::{MetaCommand, MetaError, MetaKey, MetaRecord, MetaResult, QueuePolicy};

use super::ServerControlPlane;
use super::names;

/// CAS attempts before a concurrently updated policy answers 503.
const POLICY_RETRIES: usize = 8;

fn meta_unavailable(e: MetaError) -> ApiError {
    ApiError::unavailable(format!("metadata unavailable: {e}"))
}

/// The storage-form (qualified) name of a policy route's `{name}`.
pub(crate) fn policy_key(field: &str, raw: &str) -> Result<String, ApiError> {
    let (prefix, _) = names::split_qualified(raw);
    let tenant = names::request_tenant(prefix)?;
    names::qualify_request_name(field, tenant, raw)
}

fn workflow_exists(cp: &ServerControlPlane, key: &str) -> bool {
    cp.meta()
        .view()
        .read(|s| s.workflows().any(|w| w.value.name == key))
}

fn unknown_workflow(key: &str) -> ApiError {
    ApiError::not_found(format!("workflow {key} not found"))
}

/// Read-modify-CAS loop shared by both record kinds. `current` returns the existing
/// record's `(version, record)`, `build` the record to store.
async fn put_with_cas<R: Clone>(
    cp: &ServerControlPlane,
    key: MetaKey,
    current: impl Fn() -> Option<(u64, R)>,
    build: impl Fn(Option<R>) -> MetaRecord,
) -> Result<(), ApiError> {
    for _ in 0..POLICY_RETRIES {
        let cur = current();
        let expected = cur.as_ref().map(|(v, _)| *v);
        let record = build(cur.map(|(_, r)| r));
        let res = cp
            .meta()
            .propose(MetaCommand::Put {
                key: key.clone(),
                expected_version: expected,
                record,
            })
            .await
            .map_err(meta_unavailable)?;
        match res {
            MetaResult::Ok { .. } => return Ok(()),
            // A concurrent writer won (or the record vanished): re-read and retry.
            MetaResult::StaleVersion { .. }
            | MetaResult::Conflict { .. }
            | MetaResult::NotFound => {}
            MetaResult::Invalid(msg) => {
                return Err(ApiError::invalid_argument(format!(
                    "metadata rejected the policy: {msg}"
                )));
            }
        }
    }
    Err(ApiError::unavailable(
        "the policy is being updated concurrently; retry",
    ))
}

pub(crate) async fn set_workflow_policy(
    cp: &ServerControlPlane,
    workflow: String,
    p: RunPolicy,
) -> Result<RunPolicy, ApiError> {
    let key = policy_key("workflow", &workflow)?;
    if !workflow_exists(cp, &key) {
        return Err(unknown_workflow(&key));
    }
    put_with_cas(
        cp,
        MetaKey::WorkflowPolicy(key.clone()),
        || {
            cp.meta().view().read(|s| {
                s.workflow_policy(&key)
                    .map(|v| (v.version, v.value.clone()))
            })
        },
        |cur: Option<WorkflowPolicy>| {
            MetaRecord::WorkflowPolicy(WorkflowPolicy {
                workflow: key.clone(),
                retention_ms: p.retention_ms,
                schedule_to_start_ms: p.schedule_to_start_ms,
                ..cur.unwrap_or_default()
            })
        },
    )
    .await?;
    get_workflow_policy(cp, workflow).await
}

pub(crate) async fn get_workflow_policy(
    cp: &ServerControlPlane,
    workflow: String,
) -> Result<RunPolicy, ApiError> {
    let key = policy_key("workflow", &workflow)?;
    if !workflow_exists(cp, &key) {
        return Err(unknown_workflow(&key));
    }
    Ok(cp.meta().view().read(|s| {
        s.workflow_policy(&key)
            .map(|v| RunPolicy {
                retention_ms: v.value.retention_ms,
                schedule_to_start_ms: v.value.schedule_to_start_ms,
            })
            .unwrap_or_default()
    }))
}

pub(crate) async fn set_queue_policy(
    cp: &ServerControlPlane,
    queue: String,
    p: RunPolicy,
) -> Result<RunPolicy, ApiError> {
    let key = policy_key("queue", &queue)?;
    put_with_cas(
        cp,
        MetaKey::QueuePolicy(key.clone()),
        || {
            cp.meta()
                .view()
                .read(|s| s.queue_policy(&key).map(|v| (v.version, v.value.clone())))
        },
        |cur: Option<QueuePolicy>| {
            MetaRecord::QueuePolicy(QueuePolicy {
                queue: key.clone(),
                retention_ms: p.retention_ms,
                schedule_to_start_ms: p.schedule_to_start_ms,
                ..cur.unwrap_or_default()
            })
        },
    )
    .await?;
    get_queue_policy(cp, queue).await
}

pub(crate) async fn get_queue_policy(
    cp: &ServerControlPlane,
    queue: String,
) -> Result<RunPolicy, ApiError> {
    let key = policy_key("queue", &queue)?;
    Ok(cp.meta().view().read(|s| {
        s.queue_policy(&key)
            .map(|v| RunPolicy {
                retention_ms: v.value.retention_ms,
                schedule_to_start_ms: v.value.schedule_to_start_ms,
            })
            .unwrap_or_default()
    }))
}

#[cfg(test)]
mod tests {
    use ostro_api::ErrorCode;

    use super::policy_key;

    #[test]
    fn policy_keys_follow_d23() {
        assert_eq!(policy_key("workflow", "w").expect("bare"), "w");
        assert_eq!(
            policy_key("workflow", "acme/w").expect("qualified"),
            "acme/w"
        );
        assert_eq!(policy_key("queue", "acme/q1").expect("queue"), "acme/q1");
        for bad in ["", "x:y", "__z", "_/w", "Acme/w", "a/b/c"] {
            let e = policy_key("workflow", bad).expect_err(bad);
            assert_eq!(e.code, ErrorCode::InvalidArgument, "{bad}");
        }
    }
}
