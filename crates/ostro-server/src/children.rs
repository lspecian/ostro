//! Child workflows, server side (plan 09-05, WF-06; D-P9-7..9, D-P9-25).
//!
//! - [`child_run_id`]: the deterministic child run id of `(parent, op)` (D-P9-7), so a
//!   re-derive after a crash or a repair re-send names the same run and the target
//!   partition's runs-row check keeps exactly one child.
//! - [`resolve_child`]: the `start-child` target (workflow, version, module, queue, run
//!   timeout) through the continue-as-new [`ModuleResolver`] (D-P9-8). An unknown target
//!   is not a derive error: the sink journals `ChildCompleted{op, Err(non-retryable
//!   unknown_workflow)}` right after the flush.
//! - [`child_start`]: the child's `StartRun` (inherited tenant, traceparent, priority;
//!   `ParentRef{run, op}` unless detached; fresh seed chosen by the caller).
//! - [`parent_event_bytes`] / [`parent_outcome`]: the submitter-encoded kind-67
//!   `ChildCompleted` bytes the child's terminal append carries in `CompleteRun.parent_event`
//!   (PF-F1, D21). A continue-as-new sets none (the successor reports, PF-F2).
//! - [`plan_child_repairs`]: the journal-load repair rule (W1, D-P9-25), shared with 09-08.
//!
//! **Opt-in journaling (09-05 decision).** Storage format v0.1 is frozen and its legacy
//! inbound `ChildCompleted` semantics (an inbox row) must not change, so every attached
//! `SendMessage(StartChild)` derived here is followed by the `ChildJournal` marker
//! (`Derived` tag 19, from the P9 fallback block): apply then journals the child's
//! completion as the parent's kind-67 event.

use ostro_partition::store::values::RunRecord;
use ostro_types::command::{ParentRef, StartRun};
use ostro_types::journal::{
    ChildCompleted, ChildOptions, ChildStarted, Event, EventBody, Failure, OpOutput, encode_event,
    encode_result,
};
use ostro_types::{PartitionId, RunId};
use sha2::{Digest, Sha256};

use crate::derive::{ModuleResolver, PolicyResolver, ceil_ms, qualify};

/// Domain separator of [`child_run_id`].
pub const CHILD_ID_DOMAIN: &[u8] = b"ostro/child/v1";
/// Error kind of an unresolvable child target (reused SDK kind, D-P9-20).
pub const UNKNOWN_WORKFLOW_KIND: &str = "unknown_workflow";
/// Queue of a child whose options and workflow name none (the API's default).
pub const DEFAULT_CHILD_QUEUE: &str = "default";

/// The deterministic run id of the child started by `parent`'s op `op` (D-P9-7):
/// `d = sha256("ostro/child/v1" ‖ parent (18 bytes) ‖ op u64 BE)`; uuid = `d[..16]` with
/// the RFC 9562 version nibble 8 and variant `10`; partition =
/// `u16::from_be_bytes(d[16..18]) % partitions` (`partitions` 0 is treated as 1).
pub fn child_run_id(parent: &RunId, op: u64, partitions: u16) -> RunId {
    let mut h = Sha256::new();
    h.update(CHILD_ID_DOMAIN);
    h.update(parent.as_bytes());
    h.update(op.to_be_bytes());
    let d = h.finalize();
    let p = u16::from_be_bytes([d[16], d[17]]) % partitions.max(1);
    // 09-08: the uuid part is the shared pure fn apply also uses (D-P9-27 row matching).
    RunId::new(PartitionId(p), ostro_types::ids::child_run_uuid(parent, op))
}

/// A resolved `start-child` target (D-P9-8). `workflow` and `queue` are qualified (D23).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildTarget {
    pub workflow: String,
    pub version: u32,
    pub module_sha: [u8; 32],
    pub queue: String,
    /// `child-options.run-timeout-ms`, else the version's `describe()` default; 0 = none.
    pub run_timeout_ms: u64,
}

/// Why a child target could not be resolved.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChildResolveError {
    #[error("child workflow {workflow:?} version {version:?}: not registered")]
    Unknown {
        workflow: String,
        version: Option<u32>,
    },
}

/// Resolves the target of a `start-child` from a run of `tenant` (D-P9-8): workflow = the
/// module-local name qualified with the parent's tenant; version = `opts.version`, else the
/// workflow's active version; module from that version; queue = `opts.queue`, else the
/// version's default queue, else [`DEFAULT_CHILD_QUEUE`] (qualified like the workflow).
pub fn resolve_child(
    resolver: &dyn ModuleResolver,
    tenant: Option<&str>,
    workflow: &str,
    opts: &ChildOptions,
) -> Result<ChildTarget, ChildResolveError> {
    let qualified = qualify(tenant, workflow);
    let Some(r) = resolver.resolve(&qualified, opts.version) else {
        return Err(ChildResolveError::Unknown {
            workflow: qualified,
            version: opts.version,
        });
    };
    let local_queue = match &opts.queue {
        Some(q) => q.clone(),
        None => resolver
            .default_queue(&qualified, r.version)
            .unwrap_or_else(|| DEFAULT_CHILD_QUEUE.to_owned()),
    };
    Ok(ChildTarget {
        workflow: qualified,
        version: r.version,
        module_sha: r.module_sha,
        queue: qualify(tenant, &local_queue),
        run_timeout_ms: opts.run_timeout_ms.or(r.run_timeout_ms).unwrap_or(0),
    })
}

/// Everything about a child start chosen by the caller (I/O, randomness).
#[derive(Clone, Debug)]
pub struct ChildParts<'a> {
    /// The parent and its record.
    pub parent_id: RunId,
    pub parent: &'a RunRecord,
    /// The `ChildStarted` event (seq = op).
    pub op: u64,
    pub ts_nanos: u64,
    pub detached: bool,
    /// [`child_run_id`] of `(parent_id, op)`.
    pub run_id: RunId,
    pub rng_seed: [u8; 32],
    /// The prepared child input bytes (blob > 256 KiB).
    pub input: ostro_types::PayloadRef,
}

/// The child's `StartRun` (D-P9-8): `ParentRef{parent, op}` unless detached; tenant,
/// traceparent and priority inherited; retention / schedule-to-start from `policy`;
/// requested and ready at the `ChildStarted` time.
pub fn child_start(
    target: &ChildTarget,
    parts: ChildParts<'_>,
    policy: &dyn PolicyResolver,
) -> StartRun {
    let at_ms = ceil_ms(parts.ts_nanos);
    let (retention_ms, schedule_to_start_ms) = policy.resolve(&target.workflow, &target.queue);
    StartRun {
        run_id: parts.run_id.as_bytes().to_vec(),
        workflow: target.workflow.clone(),
        workflow_version: target.version,
        queue: target.queue.clone(),
        input: Some(parts.input),
        priority: parts.parent.priority,
        ready_at_ms: at_ms,
        requested_at_ms: at_ms,
        parent: (!parts.detached).then(|| ParentRef {
            run_id: parts.parent_id.as_bytes().to_vec(),
            op: parts.op,
        }),
        rng_seed: parts.rng_seed.to_vec(),
        module_sha: target.module_sha.to_vec(),
        tenant: parts.parent.tenant.clone().unwrap_or_default(),
        traceparent: parts.parent.traceparent.clone().unwrap_or_default(),
        run_timeout_ms: target.run_timeout_ms,
        schedule_to_start_ms,
        retention_ms,
        ..Default::default()
    }
}

/// Journal-codec bytes of the parent's kind-67 `ChildCompleted{op, outcome}` at
/// `ts_nanos` (D21 submitter-encoded; the parent's apply stores them verbatim).
pub fn parent_event_bytes(op: u64, outcome: Result<OpOutput, Failure>, ts_nanos: u64) -> Vec<u8> {
    encode_event(&Event {
        // The codec does not encode the seq; apply assigns the parent's next_seq.
        seq: 0,
        ts_nanos,
        body: EventBody::ChildCompleted(ChildCompleted {
            op,
            outcome: encode_result(&outcome),
        }),
    })
    .1
}

/// The parent-visible outcome of a child's terminal event (D-P9-9): `RunCompleted{output}`
/// -> `Ok(Child(output))`, `RunFailed{failure}` -> `Err(failure)` (guest-cancelled =
/// `Err(Cancelled)`). `None` for a continue-as-new (the successor reports) and non-terminal
/// events.
pub fn parent_outcome(ev: &Event) -> Option<Result<OpOutput, Failure>> {
    match &ev.body {
        EventBody::RunCompleted(c) => Some(Ok(OpOutput::Child(c.output.clone()))),
        EventBody::RunFailed(f) => Some(Err(f.failure.clone())),
        _ => None,
    }
}

/// One journal-load repair action for an unresolved attached child op (D-P9-25).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChildRepair {
    /// No children row but the target resolves now: re-send the StartChild with the
    /// deterministic id (a duplicate at the target is harmless; apply re-inserts the row).
    Resend { op: u64, target: ChildTarget },
    /// No children row and still unresolvable: journal `unknown_workflow` once.
    UnknownWorkflow { op: u64, message: String },
}

/// The repair rule on journal load (W1, D-P9-25): for every attached `ChildStarted` of
/// `journal` without a completion, (a) a children row for [`child_run_id`] means the start
/// was delivered: skip, never re-decide from current meta; (b) otherwise re-resolve:
/// resolvable -> [`ChildRepair::Resend`], still unresolvable ->
/// [`ChildRepair::UnknownWorkflow`]. Empty for a journal that ends the run. Detached
/// children are left to 09-08 (their completion is journaled by the sink there).
pub(crate) fn plan_child_repairs(
    run: &RunId,
    partitions: u16,
    tenant: Option<&str>,
    journal: &[Event],
    has_row: &dyn Fn(&RunId) -> bool,
    resolver: &dyn ModuleResolver,
) -> Vec<ChildRepair> {
    let ended = journal.iter().any(|e| {
        matches!(
            e.body,
            EventBody::RunCompleted(_) | EventBody::RunFailed(_) | EventBody::RunContinuedAsNew(_)
        )
    });
    if ended {
        return Vec::new();
    }
    let completed: std::collections::BTreeSet<u64> = journal
        .iter()
        .filter_map(|e| e.body.completes_op())
        .collect();
    journal
        .iter()
        .filter(|e| !completed.contains(&e.seq))
        .filter_map(|e| match &e.body {
            EventBody::ChildStarted(c) if !c.options.detached => Some((e.seq, c)),
            _ => None,
        })
        .filter(|(op, _)| !has_row(&child_run_id(run, *op, partitions)))
        .map(|(op, c): (u64, &ChildStarted)| {
            match resolve_child(resolver, tenant, &c.workflow, &c.options) {
                Ok(target) => ChildRepair::Resend { op, target },
                Err(e) => ChildRepair::UnknownWorkflow {
                    op,
                    message: e.to_string(),
                },
            }
        })
        .collect()
}

/// The `unknown_workflow` completion of op `op` (D-P9-8).
pub fn unknown_workflow_completion(op: u64, message: &str) -> EventBody {
    let out: Result<OpOutput, Failure> =
        Err(Failure::NonRetryable(ostro_types::journal::ErrorInfo {
            kind: UNKNOWN_WORKFLOW_KIND.to_string(),
            message: message.to_string(),
            details: None,
        }));
    EventBody::ChildCompleted(ChildCompleted {
        op,
        outcome: encode_result(&out),
    })
}
