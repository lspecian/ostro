//! `on_failure` hook and DLQ policy resolution (Phase 11, D-P11-12, D-P11-29; plan 11-12).
//!
//! # Storage
//!
//! Both policies live in meta `ext` TLV entries (RESERVATIONS §10b, P11 block) on the
//! existing policy records, so the meta encoding is unchanged (no new variant, no
//! `META_FORMAT` bump):
//!
//! | Tag | Value (postcard) | Records |
//! |---|---|---|
//! | [`EXT_ON_FAILURE`] (144) | [`OnFailure`] | `WorkflowPolicy`, `QueuePolicy` |
//! | [`EXT_DLQ_POLICY`] (145) | [`DlqPolicy`] | `QueuePolicy` |
//!
//! Writes go through [`ostro_meta::ext::ext_put_value`] / `ext_remove`, which keep every
//! other entry byte for byte (unknown tags survive a read-modify-write). The API DTO is
//! `ostro_api::phase11::FailurePolicy`; `RunPolicy` is untouched (B2). Every write checks
//! [`crate::p11::p11_feature_enabled`]`(FailurePolicyExt)` first (D-P11-30).
//!
//! # Resolution ([`resolve`])
//!
//! The hook of a failed run is the `OnFailure` of its workflow's `WorkflowPolicy`, else the
//! one of its queue's `QueuePolicy` (the workflow wins). The DLQ policy comes from the
//! queue only; absent = enabled with the `[dlq] hook_catchup` window. A malformed value
//! (never written by this build) reads as absent.
//!
//! # Hook runs
//!
//! A hook run is started with dedupe key [`hook_dedupe_key`] (`__onfail/<run>/<resumed_count>`:
//! one hook per failure episode, W5), conflict policy `ReturnExisting` and the system tag
//! `__ostro/on_failure_of=<run>` ([`ON_FAILURE_OF_TAG`]); a failed run that carries that tag
//! never fires a hook ([`is_hook_run`], the loop guard). The input is [`hook_input`].

use ostro_meta::MetaView;
use ostro_meta::ext::{ext_get_value, ext_put_value, ext_remove};
use serde::{Deserialize, Serialize};

pub use ostro_types::command::ON_FAILURE_OF_TAG;

/// Meta ext tag of [`OnFailure`] (RESERVATIONS §10b, P11 block).
pub const EXT_ON_FAILURE: u32 = 144;
/// Meta ext tag of [`DlqPolicy`] (RESERVATIONS §10b, P11 block).
pub const EXT_DLQ_POLICY: u32 = 145;
/// Dedupe namespace of hook runs (RESERVATIONS §11).
pub const ONFAIL_DEDUPE_PREFIX: &str = "__onfail/";
/// Dedupe namespace of DLQ restart / rerun requeues (RESERVATIONS §11).
pub const DLQ_DEDUPE_PREFIX: &str = "__dlq/";

/// The `on_failure` hook (ext tag 144). **Field order is the postcard format.**
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct OnFailure {
    /// Hook workflow (`[tenant/]name`).
    pub workflow: String,
    /// Queue of the hook run; `None` = the hook workflow's default queue.
    pub queue: Option<String>,
    /// Pass the failed run's input to the hook.
    pub include_input: bool,
}

/// DLQ policy of a queue (ext tag 145). **Field order is the postcard format.**
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DlqPolicy {
    /// Failed runs of the queue are DLQ members and fire hooks.
    pub enabled: bool,
    /// Hook catch-up window at leader start, in ms.
    pub hook_catchup_ms: u64,
}

impl DlqPolicy {
    /// The policy of a queue without tag 145.
    pub fn default_with(hook_catchup_ms: u64) -> Self {
        Self {
            enabled: true,
            hook_catchup_ms,
        }
    }
}

/// The `OnFailure` stored in `ext`, if any (malformed = absent).
pub fn on_failure_of_ext(ext: &[u8]) -> Option<OnFailure> {
    ext_get_value::<OnFailure>(ext, EXT_ON_FAILURE)
        .ok()
        .flatten()
}

/// The `DlqPolicy` stored in `ext`, if any (malformed = absent).
pub fn dlq_policy_of_ext(ext: &[u8]) -> Option<DlqPolicy> {
    ext_get_value::<DlqPolicy>(ext, EXT_DLQ_POLICY)
        .ok()
        .flatten()
}

/// `ext` with tag 144 set to `v` (or removed for `None`); every other entry is kept.
pub fn put_on_failure(ext: &[u8], v: Option<&OnFailure>) -> Result<Vec<u8>, String> {
    match v {
        Some(v) => ext_put_value(ext, EXT_ON_FAILURE, v),
        None => ext_remove(ext, EXT_ON_FAILURE),
    }
    .map_err(|e| e.to_string())
}

/// `ext` with tag 145 set to `v` (or removed for `None`); every other entry is kept.
pub fn put_dlq_policy(ext: &[u8], v: Option<&DlqPolicy>) -> Result<Vec<u8>, String> {
    match v {
        Some(v) => ext_put_value(ext, EXT_DLQ_POLICY, v),
        None => ext_remove(ext, EXT_DLQ_POLICY),
    }
    .map_err(|e| e.to_string())
}

/// The DLQ policy of `queue` (absent = enabled, `default_catchup_ms`).
pub fn dlq_policy(meta: &MetaView, queue: &str, default_catchup_ms: u64) -> DlqPolicy {
    meta.read(|s| {
        s.queue_policy(queue)
            .and_then(|v| dlq_policy_of_ext(&v.value.ext))
    })
    .unwrap_or_else(|| DlqPolicy::default_with(default_catchup_ms))
}

/// The hook and DLQ policy of a run of `workflow` on `queue` (stored, qualified names):
/// the workflow's `OnFailure` wins over the queue's; the DLQ policy is the queue's.
pub fn resolve(
    meta: &MetaView,
    workflow: &str,
    queue: &str,
    default_catchup_ms: u64,
) -> (Option<OnFailure>, DlqPolicy) {
    meta.read(|s| {
        let wf = s
            .workflow_policy(workflow)
            .and_then(|v| on_failure_of_ext(&v.value.ext));
        let qp = s.queue_policy(queue);
        let hook = wf.or_else(|| qp.and_then(|v| on_failure_of_ext(&v.value.ext)));
        let dlq = qp
            .and_then(|v| dlq_policy_of_ext(&v.value.ext))
            .unwrap_or_else(|| DlqPolicy::default_with(default_catchup_ms));
        (hook, dlq)
    })
}

/// The system tag of a hook run for `run`.
pub fn hook_tag(run: &str) -> String {
    format!("{ON_FAILURE_OF_TAG}{run}")
}

/// Loop guard: whether a run with `tags` is itself a hook run.
pub fn is_hook_run(tags: &[String]) -> bool {
    tags.iter().any(|t| t.starts_with(ON_FAILURE_OF_TAG))
}

/// Dedupe key of the hook run of `run`'s failure episode `resumed_count` (W5).
pub fn hook_dedupe_key(run: &str, resumed_count: u32) -> String {
    format!("{ONFAIL_DEDUPE_PREFIX}{run}/{resumed_count}")
}

/// Dedupe key of the `n`-th restart / rerun requeue of `run`.
pub fn requeue_dedupe_key(run: &str, n: u32) -> String {
    format!("{DLQ_DEDUPE_PREFIX}{run}/{n}")
}

/// What a hook run receives as input.
#[derive(Clone, Debug, PartialEq)]
pub struct HookInput<'a> {
    pub run_id: &'a str,
    pub workflow: &'a str,
    pub version: u32,
    /// `"failed"` or `"timed_out"`.
    pub status: &'a str,
    pub reason: &'a str,
    pub failed_at_ms: u64,
    pub tags: &'a [String],
    /// The source input (only with `include_input`), else `null`.
    pub input: Option<serde_json::Value>,
}

/// The hook input JSON: `{"run_id","workflow","version","status","reason","failed_at_ms",
/// "tags","input"}` (`input` is `null` unless `include_input`).
pub fn hook_input(i: &HookInput<'_>) -> serde_json::Value {
    serde_json::json!({
        "run_id": i.run_id,
        "workflow": i.workflow,
        "version": i.version,
        "status": i.status,
        "reason": i.reason,
        "failed_at_ms": i.failed_at_ms,
        "tags": i.tags,
        "input": i.input.clone().unwrap_or(serde_json::Value::Null),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_meta::ext::{ext_entries, ext_put};

    fn hook() -> OnFailure {
        OnFailure {
            workflow: "hook".into(),
            queue: Some("q".into()),
            include_input: true,
        }
    }

    #[test]
    fn ext_round_trip_keeps_unknown_tags() {
        let base = ext_put(&[], 150, b"keep").expect("put 150");
        let base = ext_put(&base, 15, b"t").expect("put 15");
        let e = put_on_failure(&base, Some(&hook())).expect("144");
        let d = DlqPolicy {
            enabled: false,
            hook_catchup_ms: 5,
        };
        let e = put_dlq_policy(&e, Some(&d)).expect("145");
        let tags: Vec<u32> = ext_entries(&e)
            .expect("valid")
            .iter()
            .map(|x| x.tag)
            .collect();
        assert_eq!(tags, vec![15, 144, 145, 150]);
        assert_eq!(on_failure_of_ext(&e), Some(hook()));
        assert_eq!(dlq_policy_of_ext(&e), Some(d));
        let e = put_on_failure(&e, None).expect("rm");
        let e = put_dlq_policy(&e, None).expect("rm");
        assert_eq!(e, base, "removing restores the original bytes");
    }

    #[test]
    fn malformed_values_read_as_absent() {
        let e = ext_put(&[], EXT_ON_FAILURE, &[0xff, 0xff]).expect("put");
        assert_eq!(on_failure_of_ext(&e), None);
        assert_eq!(dlq_policy_of_ext(&[]), None);
    }

    #[test]
    fn names_and_guard() {
        assert_eq!(hook_dedupe_key("r1", 0), "__onfail/r1/0");
        assert_eq!(requeue_dedupe_key("r1", 1), "__dlq/r1/1");
        assert_eq!(hook_tag("r1"), "__ostro/on_failure_of=r1");
        assert!(is_hook_run(&["a".into(), hook_tag("x")]));
        assert!(!is_hook_run(&["a".into()]));
        let v = hook_input(&HookInput {
            run_id: "r",
            workflow: "w",
            version: 2,
            status: "failed",
            reason: "x",
            failed_at_ms: 7,
            tags: &["t".into()],
            input: None,
        });
        assert_eq!(v["input"], serde_json::Value::Null);
        assert_eq!(v["version"], 2);
        assert_eq!(v["tags"][0], "t");
    }
}
