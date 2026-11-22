//! Run operations of the real control plane (plan 04-18): start (API-03) and get
//! (API-04 / WF-01). History and listing live in [`super::history`] and [`super::list`].
//!
//! # Start
//!
//! 0. D23 names ([`super::names`]): the tenant must be absent/empty (the default tenant)
//!    or a valid tenant id (`_` is reserved: 400). The workflow and an explicit queue may
//!    be bare or qualified with the request's own tenant (a different prefix is 400), and
//!    their module-local parts must match the resource grammar (400). Both are submitted
//!    in storage form (`<tenant>/<name>`, bare for the default tenant), so the dedupe row,
//!    the partition and the meta lookup are tenant-scoped with no format change, and
//!    `tenant(StartRun.workflow) == StartRun.tenant` holds by construction. A queue taken
//!    from the version's `describe()` default (or [`DEFAULT_QUEUE`]) is qualified the same
//!    way.
//! 1. Resolve the version: the explicit one, else the active one. An unknown workflow or
//!    version is 404; a workflow with versions but no active one is 412.
//! 2. Validate the input JSON (400) and the option sizes ([`MAX_RUN_TAGS`] etc., 400).
//! 3. Pick the partition: with an idempotency key `partition_for_key(workflow, key, P)`,
//!    so every retry of the key lands on the partition holding its dedupe row; without one
//!    `partition_for_new_run(uuid v7, P)`. The run id is `RunId::new(p, uuid v7)`.
//! 4. `prepare_payload(input)`: inline up to 256 KiB, else a durable blob written before the
//!    command is appended. The partition stores only bytes; the executor hands them to the
//!    workflow as `application/json` (`run_started::input_payload`).
//! 5. Submit `StartRun` with `module_sha` (the version's module), `requested_at_ms = now`,
//!    `ready_at_ms = now + delay_ms`, a 32-byte OS-random `rng_seed`, `run_timeout_ms` from
//!    the version's `describe()` metadata, and every option (tags, tenant, traceparent,
//!    queue, priority, concurrency key, idempotency key + conflict policy).
//! 6. Map the outcome ([`map_outcome`], [`partition_error`]): Accepted = 201 created;
//!    Duplicate = 200 with the existing id; IdempotencyConflict = 409 with
//!    `details.existing_run_id`; PayloadTooLarge = 413; NotLeader / Failed / log errors /
//!    lost acks = 503 (retry with the same idempotency key is safe); other rejections = 400.
//!
//! # Get
//!
//! The partition comes from the run id. The record gives status, version, module, queue,
//! options, timestamps and the attempt (`current_attempt.n`, `None` before the first
//! dispatch). A completed run's result is its `PAYLOAD_RESULT` (the workflow's JSON output);
//! a failed or cancelled run's result payload is the failure JSON
//! `{"reason_code", "kind", "message"}` and is returned as the `failure` text. The published
//! status is the single event at `RunRecord.status_seq` (F10): one journal row read, however
//! long the journal is. `attempt_failures` (F9, the durable retry count), and the resolved
//! `retention_ms` / `schedule_to_start_ms` (0 on the record = `None`) are exposed too.
//!
//! # Start fields (05-13)
//!
//! After the D23 block, retention and schedule-to-start are resolved with
//! [`resolve_run_policy`] (request > `WorkflowPolicy` > `QueuePolicy` > `[runs]`) on the
//! qualified workflow/queue and submitted in `StartRun.retention_ms` /
//! `schedule_to_start_ms`. A **dry run** runs every check above (names, version, input
//! JSON, options, policy resolution, partition availability), reads the dedupe row for an
//! idempotency key and reports the would-be outcome (`run_id` = the existing run, or empty
//! for a new run; 409 under `reject`); it writes no blob and submits nothing.
//!
//! # RunStarted at start (05-25, D24)
//!
//! A real start pre-encodes the run's `RunStarted` ([`attach_run_started`], through
//! `run_started::encode_for_start`, byte-identical to the executor's synthesis: D23 stripped
//! workflow name, tenant, deadline) into `StartRun.run_started` (tag 23). Apply journals it
//! at seq 0, so the first dispatch skips one WAL commit (the executor's RunStarted append).
//! D24 (amends D3): a run that was accepted but never activated has RunStarted at seq 0
//! (`next_seq == 1`), and a signal or cancel sent before its first activation is journaled
//! directly at seq >= 1. A dry run still encodes and writes nothing.

use bytes::Bytes;
use ostro_api::ApiError;
use ostro_api::domain::{ConflictPolicy, RunInfo, RunStatus, StartRunRequest, StartRunResult};
use ostro_log::PartitionLog;
use ostro_meta::WorkflowVersion;
use ostro_partition::store::Table;
use ostro_partition::store::keys::dedupe_key;
use ostro_partition::store::values::{self, RunRecord};
use ostro_partition::{ApplyOutcome, PartitionError, PartitionHandle, RejectReason, query};
use ostro_types::command::{
    self as cmd, Command, MAX_RUN_TAG_LEN, MAX_RUN_TAGS, MAX_TENANT_LEN, MAX_TRACEPARENT_LEN,
    StartRun,
};
use ostro_types::journal::{EventBody, EventKind, decode_event};
use ostro_types::routing::{partition_for_key, partition_for_new_run};
use ostro_types::{PayloadRef, RunId};

use super::ServerControlPlane;
use super::names;
use crate::retention::{RunPolicy, resolve_run_policy};
use crate::run_started;

/// Queue of runs whose request and workflow name none.
pub const DEFAULT_QUEUE: &str = "default";

// ---------------------------------------------------------------------------------
// Shared helpers (also used by history / list)
// ---------------------------------------------------------------------------------

/// Partition-side failure -> API error. Anything that means "the outcome is unknown or the
/// partition cannot serve right now" is 503 (retryable).
pub(crate) fn partition_error(e: PartitionError) -> ApiError {
    match e {
        PartitionError::NotLeader => ApiError::unavailable("partition leader unavailable"),
        PartitionError::Failed(m) => ApiError::unavailable(format!("partition failed: {m}")),
        PartitionError::Log(e) => ApiError::unavailable(format!("partition log: {e}")),
        PartitionError::AckLost(off) => ApiError::unavailable(format!(
            "outcome of offset {off} unknown; retry with the same idempotency key"
        )),
        PartitionError::Rejected(r) => reject_error(r),
        e @ (PartitionError::Store(_) | PartitionError::Blob(_)) => {
            ApiError::internal(e.to_string())
        }
    }
}

/// A rejected `StartRun` -> API error.
fn reject_error(r: RejectReason) -> ApiError {
    match r {
        RejectReason::IdempotencyConflict { existing } => {
            ApiError::conflict(format!("idempotency key already used by run {existing}"))
                .with_details(serde_json::json!({ "existing_run_id": existing.to_string() }))
        }
        RejectReason::PayloadTooLarge => {
            ApiError::payload_too_large("run input exceeds the inline payload limit")
        }
        RejectReason::StaleEpoch { .. } | RejectReason::WrongPartition => {
            ApiError::unavailable(format!("partition cannot accept the start: {r:?}"))
        }
        other => ApiError::invalid_argument(format!("start rejected: {other:?}")),
    }
}

/// Outcome of the submitted `StartRun` for run `run`.
pub(crate) fn map_outcome(run: RunId, o: ApplyOutcome) -> Result<StartRunResult, ApiError> {
    match o {
        ApplyOutcome::Accepted => Ok(StartRunResult {
            run_id: run.to_string(),
            created: true,
            dry_run: false,
        }),
        ApplyOutcome::Duplicate { existing } => Ok(StartRunResult {
            run_id: existing.unwrap_or(run).to_string(),
            created: false,
            dry_run: false,
        }),
        ApplyOutcome::Rejected(r) => Err(reject_error(r)),
    }
}

/// Parse a run id string (400) and check its partition against the count (404).
pub(crate) fn parse_run_id(cp: &ServerControlPlane, s: &str) -> Result<RunId, ApiError> {
    let run: RunId = s
        .parse()
        .map_err(|e| ApiError::invalid_argument(format!("run id {s:?}: {e}")))?;
    if !cp.partition_count().contains(run.partition()) {
        return Err(not_found(s));
    }
    Ok(run)
}

pub(crate) fn not_found(run: &str) -> ApiError {
    ApiError::not_found(format!("run {run} not found"))
}

pub(crate) fn store_error(e: ostro_partition::StoreError) -> ApiError {
    ApiError::internal(format!("state store: {e}"))
}

pub(crate) fn task_error(e: tokio::task::JoinError) -> ApiError {
    ApiError::internal(format!("read task: {e}"))
}

/// Partition run status -> API status.
pub(crate) fn status(s: values::RunStatus) -> RunStatus {
    match s {
        values::RunStatus::Scheduled => RunStatus::Scheduled,
        values::RunStatus::Ready => RunStatus::Ready,
        values::RunStatus::Running => RunStatus::Running,
        values::RunStatus::Completed => RunStatus::Completed,
        values::RunStatus::Failed => RunStatus::Failed,
        values::RunStatus::Cancelled => RunStatus::Cancelled,
        values::RunStatus::Terminated => RunStatus::Terminated,
        values::RunStatus::TimedOut => RunStatus::TimedOut,
        values::RunStatus::ContinuedAsNew => RunStatus::ContinuedAsNew,
    }
}

// ---------------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------------

/// The workflow version a start runs on: explicit, else active.
fn resolve_version(
    cp: &ServerControlPlane,
    name: &str,
    version: Option<u32>,
) -> Result<WorkflowVersion, ApiError> {
    cp.meta().view().read(|s| match version {
        Some(v) => s
            .workflow(name, v)
            .map(|w| w.value.clone())
            .ok_or_else(|| ApiError::not_found(format!("workflow {name} version {v} not found"))),
        None => match s.active(name) {
            Some(a) => s
                .workflow(name, a.value.version)
                .map(|w| w.value.clone())
                .ok_or_else(|| {
                    ApiError::internal(format!(
                        "active version {} of {name} is not registered",
                        a.value.version
                    ))
                }),
            None if s.workflows().any(|w| w.value.name == name) => Err(
                ApiError::precondition_failed(format!("workflow {name} has no active version")),
            ),
            None => Err(ApiError::not_found(format!("workflow {name} not found"))),
        },
    })
}

fn check_len(field: &str, v: &str, max: usize) -> Result<(), ApiError> {
    if v.len() > max {
        return Err(ApiError::invalid_argument(format!(
            "{field}: {} bytes exceeds the limit of {max}",
            v.len()
        )));
    }
    Ok(())
}

fn check_options(req: &StartRunRequest) -> Result<(), ApiError> {
    if req.workflow.is_empty() {
        return Err(ApiError::invalid_argument("workflow name is empty"));
    }
    // D-P11-13: uniform `details{reason: limit_exceeded, limit, max, actual}` (11-11).
    let count = |v: usize| u64::try_from(v).unwrap_or(u64::MAX);
    if req.tags.len() > MAX_RUN_TAGS {
        return Err(ostro_api::phase11::limit_error(
            ostro_api::ErrorCode::InvalidArgument,
            "tags",
            count(MAX_RUN_TAGS),
            count(req.tags.len()),
        ));
    }
    if let Some(t) = req.tags.iter().find(|t| t.len() > MAX_RUN_TAG_LEN) {
        return Err(ostro_api::phase11::limit_error(
            ostro_api::ErrorCode::InvalidArgument,
            "tag_bytes",
            count(MAX_RUN_TAG_LEN),
            count(t.len()),
        ));
    }
    if let Some(t) = &req.tenant {
        check_len("tenant", t, MAX_TENANT_LEN)?;
    }
    if let Some(t) = &req.traceparent {
        check_len("traceparent", t, MAX_TRACEPARENT_LEN)?;
    }
    Ok(())
}

fn non_empty(v: Option<String>) -> Option<String> {
    v.filter(|s| !s.is_empty())
}

pub(crate) async fn start_run(
    cp: &ServerControlPlane,
    req: StartRunRequest,
) -> Result<StartRunResult, ApiError> {
    check_options(&req)?;
    // D23: validate and qualify before anything is resolved, routed or stored.
    let tenant = names::request_tenant(req.tenant.as_deref())?.map(str::to_owned);
    let workflow = names::qualify_request_name("workflow", tenant.as_deref(), &req.workflow)?;
    let req_queue = non_empty(req.queue)
        .map(|q| names::qualify_request_name("queue", tenant.as_deref(), &q))
        .transpose()?;
    let wf = resolve_version(cp, &workflow, req.version)?;
    let queue = match req_queue {
        Some(q) => q,
        None => {
            let local = wf.default_queue.as_deref().unwrap_or(DEFAULT_QUEUE);
            names::qualify_request_name("queue", tenant.as_deref(), local)?
        }
    };
    serde_json::from_slice::<serde::de::IgnoredAny>(&req.input_json)
        .map_err(|e| ApiError::invalid_argument(format!("input: not valid JSON ({e})")))?;
    // 05-13 start fields (after the D23 block, on the qualified locals): D9 policy
    // resolution request > WorkflowPolicy > QueuePolicy > `[runs]`.
    let (retention_ms, schedule_to_start_ms) = resolve_run_policy(
        cp.meta().view(),
        &cp.config().runs,
        &workflow,
        &queue,
        RunPolicy {
            retention_ms: req.retention_ms,
            schedule_to_start_ms: req.schedule_to_start_ms,
        },
    );

    let count = cp.partition_count();
    let uuid = uuid::Uuid::now_v7();
    let idem = non_empty(req.idempotency_key);
    let p = match &idem {
        Some(k) => partition_for_key(&workflow, k.as_bytes(), count),
        None => partition_for_new_run(&uuid, count),
    };
    let handle = cp.partition(p)?;
    let policy = match req.conflict_policy {
        ConflictPolicy::ReturnExisting => cmd::ConflictPolicy::ReturnExisting,
        ConflictPolicy::Reject => cmd::ConflictPolicy::Reject,
    };

    if req.dry_run {
        // W15: same validation, the would-be idempotency outcome from the dedupe row, and
        // nothing written (no blob, no command).
        let existing = match &idem {
            Some(k) => {
                let reader = handle.reader();
                let dk = dedupe_key(&workflow, k.as_bytes());
                let row = tokio::task::spawn_blocking(move || reader.get(Table::Dedupe, &dk))
                    .await
                    .map_err(task_error)?
                    .map_err(store_error)?;
                row.map(|v| {
                    RunId::from_bytes(&v)
                        .map_err(|e| ApiError::internal(format!("dedupe row: {e}")))
                })
                .transpose()?
            }
            None => None,
        };
        return match existing {
            Some(existing) => match policy {
                cmd::ConflictPolicy::Reject => {
                    Err(reject_error(RejectReason::IdempotencyConflict { existing }))
                }
                _ => Ok(StartRunResult {
                    run_id: existing.to_string(),
                    created: false,
                    dry_run: true,
                }),
            },
            None => Ok(StartRunResult {
                run_id: String::new(),
                created: true,
                dry_run: true,
            }),
        };
    }

    // 07-08 (D-W21): StartRun rate limit before anything is written or proposed (429).
    crate::flow::rate_limit::admit(
        cp,
        handle,
        &workflow,
        &queue,
        req.concurrency_key.as_deref().unwrap_or_default(),
        idem.as_deref(),
    )
    .await?;
    let run = RunId::new(p, uuid);
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed)
        .map_err(|e| ApiError::internal(format!("OS random source unavailable: {e}")))?;
    let input_bytes = Bytes::from(req.input_json);
    let input = handle
        .prepare_payload(input_bytes.clone())
        .await
        .map_err(partition_error)?;

    let now = cp.now_ms();
    let start = StartRun {
        run_id: run.as_bytes().to_vec(),
        workflow,
        workflow_version: wf.version,
        queue,
        input: Some(input),
        idempotency_key: idem.map(String::into_bytes).unwrap_or_default(),
        conflict_policy: policy as i32,
        concurrency_key: req.concurrency_key.unwrap_or_default(),
        priority: req.priority.unwrap_or(0),
        ready_at_ms: now.saturating_add(req.delay_ms.unwrap_or(0)),
        requested_at_ms: now,
        parent: None,
        rng_seed: seed.to_vec(),
        module_sha: wf.module_sha256.to_vec(),
        tags: req.tags,
        tenant: tenant.unwrap_or_default(),
        traceparent: req.traceparent.unwrap_or_default(),
        run_timeout_ms: wf.run_timeout_ms.unwrap_or(0),
        schedule_to_start_ms,
        retention_ms,
        // Retry/continue-as-new links are set by the retry path (lifecycle.rs) and the
        // continue-as-new successor (derive), never by a plain start.
        retry_of: Vec::new(),
        continued_from: Vec::new(),
        // D24 (05-25): filled by `attach_run_started` below.
        run_started: None,
        ..Default::default()
    };
    let start = attach_run_started(handle, start, &input_bytes).await?;
    let outcome = handle
        .submit_one(Command::start_run(start))
        .await
        .map_err(partition_error)?;
    map_outcome(run, outcome)
}

/// D24 (05-25): sets `StartRun.run_started` (tag 23) to the run's `RunStarted` (seq 0),
/// pre-encoded from `start` itself (`run_started::encode_for_start`, so the D23 stripped
/// name, tenant, deadline and `continued_from` follow the command) with `input` as the run
/// input. The encoded event goes through `prepare_payload` like any payload: inline up to
/// 256 KiB, else a durable blob written before the command is appended. Apply journals it
/// verbatim at seq 0 and the executor skips its own RunStarted append (`next_seq > 0`).
pub(crate) async fn attach_run_started(
    handle: &PartitionHandle<PartitionLog>,
    mut start: StartRun,
    input: &[u8],
) -> Result<StartRun, ApiError> {
    let (kind, bytes) = run_started::encode_for_start(&start, input)
        .map_err(|e| ApiError::internal(format!("RunStarted encoding: {e}")))?;
    let payload = handle
        .prepare_payload(Bytes::from(bytes))
        .await
        .map_err(partition_error)?;
    start.run_started = Some(cmd::JournalEvent {
        seq: 0,
        kind,
        payload: Some(payload),
        ..Default::default()
    });
    Ok(start)
}

// ---------------------------------------------------------------------------------
// Get
// ---------------------------------------------------------------------------------

/// The published-status row of `run`: the single event at `RunRecord.status_seq` (F10,
/// set by apply from the `StatusPublished` kind), or `None` when nothing was published.
/// `rows_read` counts the journal rows read (the F10 test probe).
fn status_row(
    r: &ostro_partition::store::StoreReader,
    run: &RunId,
    status_seq: Option<u64>,
    rows_read: &std::sync::atomic::AtomicU64,
) -> Result<Option<(u64, u32, PayloadRef)>, ostro_partition::StoreError> {
    let Some(seq) = status_seq else {
        return Ok(None);
    };
    let rows = query::journal_rows(r, run, seq, 1)?;
    rows_read.fetch_add(rows.len() as u64, std::sync::atomic::Ordering::Relaxed);
    Ok(rows
        .into_iter()
        .find(|(s, k, _)| *s == seq && *k == EventKind::StatusPublished.code()))
}

pub(crate) async fn get_run(cp: &ServerControlPlane, run_id: String) -> Result<RunInfo, ApiError> {
    let run = parse_run_id(cp, &run_id)?;
    let handle = cp.partition(run.partition())?;
    let reader = handle.reader();
    let rows_read = std::sync::Arc::clone(&cp.status_rows_read);
    let (rec, result, status_row) = tokio::task::spawn_blocking(move || {
        let Some(rec) = query::run_record(&reader, &run)? else {
            return Ok((None, None, None));
        };
        let result = if rec.status.is_terminal() {
            query::run_payload(&reader, &run, query::PAYLOAD_RESULT)?
        } else {
            None
        };
        let status_row = status_row(&reader, &run, rec.status_seq, &rows_read)?;
        Ok::<_, ostro_partition::StoreError>((Some(rec), result, status_row))
    })
    .await
    .map_err(task_error)?
    .map_err(store_error)?;
    let rec: RunRecord = rec.ok_or_else(|| not_found(&run_id))?;

    let published_status_json = match status_row {
        Some((seq, kind, payload)) => {
            let bytes = handle
                .fetch_payload(&payload)
                .await
                .map_err(partition_error)?;
            match decode_event(seq, kind, &bytes) {
                Ok(ev) => match ev.body {
                    EventBody::StatusPublished(s) => Some(s.json.into_bytes()),
                    _ => None,
                },
                Err(e) => {
                    return Err(ApiError::internal(format!(
                        "run {run} seq {seq}: decode: {e}"
                    )));
                }
            }
        }
        None => None,
    };

    let st = status(rec.status);
    let (result_json, failure) = match result {
        Some(r) => {
            let bytes = handle.fetch_payload(&r).await.map_err(partition_error)?;
            // A continued-as-new run's payload is `{"next_run_id"}` (05-11): a result.
            if matches!(st, RunStatus::Completed | RunStatus::ContinuedAsNew) {
                (Some(bytes.to_vec()), None)
            } else {
                (None, Some(String::from_utf8_lossy(&bytes).into_owned()))
            }
        }
        None => (None, None),
    };

    Ok(RunInfo {
        run_id: run.to_string(),
        workflow: rec.workflow,
        version: rec.workflow_version,
        module_sha256: rec
            .module_sha
            .map(|s| ostro_blob::hex(&s))
            .unwrap_or_default(),
        queue: rec.queue,
        status: st,
        attempt: rec.current_attempt.map(|(_, n)| n),
        tags: rec.tags,
        tenant: rec.tenant,
        traceparent: rec.traceparent,
        priority: rec.priority,
        concurrency_key: (!rec.concurrency_key.is_empty()).then_some(rec.concurrency_key),
        created_at_ms: rec.created_at_ms,
        completed_at_ms: rec.completed_at_ms,
        published_status_json,
        result_json,
        failure,
        attempt_failures: rec.attempt_failures,
        retention_ms: (rec.retention_ms > 0).then_some(rec.retention_ms),
        schedule_to_start_ms: (rec.schedule_to_start_ms > 0).then_some(rec.schedule_to_start_ms),
        pause: rec.pause.map(|p| ostro_api::phase7::RunPauseMark {
            reason: ostro_api::phase7::RunPauseMark::reason_name(p.reason),
            detail: p.detail,
            at_ms: p.at_ms,
        }),
    })
}

#[cfg(test)]
mod tests {
    use ostro_api::ErrorCode;
    use ostro_types::{Offset, PartitionId};

    use super::*;

    fn run() -> RunId {
        RunId::new(PartitionId(3), uuid::Uuid::now_v7())
    }

    #[test]
    fn outcomes_map_to_created_existing_conflict() {
        let r = run();
        let other = run();
        assert_eq!(
            map_outcome(r, ApplyOutcome::Accepted).expect("ok"),
            StartRunResult {
                run_id: r.to_string(),
                created: true,
                dry_run: false,
            }
        );
        assert_eq!(
            map_outcome(
                r,
                ApplyOutcome::Duplicate {
                    existing: Some(other)
                }
            )
            .expect("dup"),
            StartRunResult {
                run_id: other.to_string(),
                created: false,
                dry_run: false,
            }
        );
        let e = map_outcome(
            r,
            ApplyOutcome::Rejected(RejectReason::IdempotencyConflict { existing: other }),
        )
        .expect_err("conflict");
        assert_eq!(e.code, ErrorCode::Conflict);
        assert_eq!(
            e.details,
            Some(serde_json::json!({ "existing_run_id": other.to_string() }))
        );
        let e =
            map_outcome(r, ApplyOutcome::Rejected(RejectReason::PayloadTooLarge)).expect_err("413");
        assert_eq!(e.code, ErrorCode::PayloadTooLarge);
        let e = map_outcome(r, ApplyOutcome::Rejected(RejectReason::RunTerminal)).expect_err("400");
        assert_eq!(e.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn unavailable_partitions_are_503() {
        for e in [
            PartitionError::NotLeader,
            PartitionError::Failed("stopped".into()),
            PartitionError::AckLost(Offset(7)),
            PartitionError::Rejected(RejectReason::WrongPartition),
        ] {
            assert_eq!(partition_error(e).code, ErrorCode::Unavailable);
        }
        assert_eq!(
            partition_error(PartitionError::Rejected(RejectReason::PayloadTooLarge)).code,
            ErrorCode::PayloadTooLarge
        );
    }
}
