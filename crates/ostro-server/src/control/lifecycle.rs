//! Run lifecycle operations of the real control plane (API-05/06/07, plan 05-13): signal,
//! graceful cancel, terminate, retry (D1 mode selection) and await-token completion, each
//! with a `dry_run` that performs the same validation and submits nothing.
//!
//! Every operation is one command on the run's partition (apply decides; there is no
//! in-memory signal or token state here). The pre-read of the run record gives clean
//! errors before submitting; the apply outcome stays authoritative for races:
//!
//! | Op | Command | Accepted | Duplicate | Rejected |
//! |---|---|---|---|---|
//! | signal | `SignalRun` (D21: submitter-encoded `SignalPayload` event) | delivered | `duplicate: true` (request_id seen, D13) | `RunNotFound` 404, `RunTerminal` 409 `run_terminal` |
//! | cancel | `RequestCancel` | first request (or forwarded along a continue-as-new chain) | `already_requested: true` | `RunTerminal` 409 |
//! | terminate | `TerminateRun` | `Terminated` | - | `RunTerminal` 409 |
//! | retry, resumable | `RetryRun` | `{resumed, same id}` | - | `RunCompleted` 409 `run_completed`, `Invalid(BadIntent)` 409 `run_not_terminal` |
//! | retry, guest terminal | `StartRun{retry_of}` | `{restarted, new id}` | - | - |
//! | token | `CompleteToken` (D21: submitter-encoded `TokenCompleted` event) | completed | 409 `token_already_completed` (+ `status`) | `RunTerminal` 409, `Invalid(BadIntent)` 400 `token_invalid` |
//!
//! # Retry (D1)
//!
//! Live runs are 409 `run_not_terminal`; `Completed` and `ContinuedAsNew` are 409
//! `run_completed` (a continued run's work goes on in its successor). A terminal run with
//! `RunRecord.resumable` (executor-level failures, Terminated, TimedOut) resumes in place.
//! Any other terminal run (guest `RunFailed`, Cancelled, module/record failures) restarts as
//! a new linked run: the same stored (qualified) workflow, version, module, input, queue,
//! priority, concurrency key, run timeout, retention and schedule-to-start, **the
//! predecessor's tenant, tags and traceparent** (Phase 14 F11), `retry_of = old`, a fresh
//! run id, seed and no idempotency key or parent. A dry run reports the mode with the
//! original run id (no id is allocated).
//!
//! # Tokens
//!
//! `ostro_engine::token::verify` checks the MAC with the cluster token key from meta; every
//! [`TokenError`](ostro_engine::token::TokenError) is 400 `token_invalid`, and so is a token
//! whose partition does not match its run id's. The token text never appears in an error
//! message, a log line or a span field (the request types redact it in `Debug`).
//! A timed-out token (`WaitTimedOut` journaled, the wait row gone) is `Duplicate` in apply,
//! hence 409 `token_already_completed`; once the run has finished it is 409 `run_terminal`.
//!
//! # Payloads (D21)
//!
//! Signal and token payloads are JSON values. The journal event bytes
//! (`[EVENT_CODEC_VER] ‖ postcard((ts_nanos, body))`, ts = the server clock, no seq) are
//! built here and sent as a `PayloadRef`: inline up to 256 KiB, otherwise written to the
//! blob store first (`PartitionHandle::prepare_payload`, the start-input offload path).

use bytes::Bytes;
use ostro_api::ApiError;
use ostro_api::domain::{
    CancelRequest, CancelResponse, CompleteTokenRequest, CompleteTokenResponse, RetryMode,
    RetryRequest, RetryResponse, RunStatus, SignalRequest, SignalResponse, TerminateRequest,
    TerminateResponse,
};
use ostro_log::Rf1Log;
use ostro_partition::apply::InvalidCode;
use ostro_partition::store::Table;
use ostro_partition::store::keys::{signal_seen_key, wait_key};
use ostro_partition::store::values::{self, RunRecord, WaitKind, WaitRow};
use ostro_partition::{ApplyOutcome, PartitionHandle, RejectReason, query};
use ostro_types::command::{
    self as cmd, Command, CompleteToken, RequestCancel, RetryRun, SignalRun, StartRun, TerminateRun,
};
use ostro_types::journal::{
    Event, EventBody, Payload, SignalPayload, TokenCompleted, encode_event,
};
use ostro_types::routing::partition_for_new_run;
use ostro_types::{PayloadRef, RunId};

use super::ServerControlPlane;
use super::runs::{not_found, parse_run_id, partition_error, status, store_error, task_error};

const JSON: &str = "application/json";

/// The run's record, or 404.
pub(crate) async fn read_record(
    handle: &PartitionHandle<Rf1Log>,
    run: RunId,
) -> Result<RunRecord, ApiError> {
    let reader = handle.reader();
    tokio::task::spawn_blocking(move || query::run_record(&reader, &run))
        .await
        .map_err(task_error)?
        .map_err(store_error)?
        .ok_or_else(|| not_found(&run.to_string()))
}

/// Point read on the partition's state store.
async fn point_read(
    handle: &PartitionHandle<Rf1Log>,
    table: Table,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, ApiError> {
    let reader = handle.reader();
    tokio::task::spawn_blocking(move || reader.get(table, &key))
        .await
        .map_err(task_error)?
        .map_err(store_error)
}

/// 409 `run_terminal` with the run's current status (re-read after a race).
async fn terminal_error(handle: &PartitionHandle<Rf1Log>, run: RunId) -> ApiError {
    match read_record(handle, run).await {
        Ok(rec) => ApiError::run_terminal(&run.to_string(), status(rec.status)),
        Err(e) => e,
    }
}

/// Map a rejection shared by every lifecycle command.
async fn rejected(handle: &PartitionHandle<Rf1Log>, run: RunId, r: RejectReason) -> ApiError {
    match r {
        RejectReason::RunNotFound => not_found(&run.to_string()),
        RejectReason::RunTerminal => terminal_error(handle, run).await,
        RejectReason::RunCompleted => ApiError::run_completed(&run.to_string()),
        RejectReason::PayloadTooLarge => {
            ApiError::payload_too_large("payload exceeds the inline payload limit")
        }
        RejectReason::StaleEpoch { .. } | RejectReason::WrongPartition => {
            ApiError::unavailable(format!("partition cannot accept the command: {r:?}"))
        }
        other => ApiError::invalid_argument(format!("command rejected: {other:?}")),
    }
}

/// Encode a journal event (D21: ts = the server clock, no seq) into a `PayloadRef`, inline
/// up to 256 KiB, else offloaded to the blob store.
async fn encoded_event(
    cp: &ServerControlPlane,
    handle: &PartitionHandle<Rf1Log>,
    body: EventBody,
) -> Result<PayloadRef, ApiError> {
    let (_, bytes) = encode_event(&Event {
        seq: 0,
        ts_nanos: cp.runtime().clock().now_unix_nanos(),
        body,
    });
    handle
        .prepare_payload(Bytes::from(bytes))
        .await
        .map_err(partition_error)
}

fn json_payload(v: &serde_json::Value) -> Payload {
    Payload {
        content_type: JSON.to_owned(),
        data: v.to_string().into_bytes(),
    }
}

async fn submit(handle: &PartitionHandle<Rf1Log>, c: Command) -> Result<ApplyOutcome, ApiError> {
    handle.submit_one(c).await.map_err(partition_error)
}

// ---------------------------------------------------------------------------------
// Signal
// ---------------------------------------------------------------------------------

pub(crate) async fn signal_run(
    cp: &ServerControlPlane,
    r: SignalRequest,
) -> Result<SignalResponse, ApiError> {
    r.validate()?;
    let run = parse_run_id(cp, &r.run_id)?;
    let handle = cp.partition(run.partition())?;
    let rec = read_record(handle, run).await?;
    if rec.status.is_terminal() {
        return Err(ApiError::run_terminal(&r.run_id, status(rec.status)));
    }
    let request_id = r.request_id.clone().filter(|id| !id.is_empty());
    if r.dry_run {
        let duplicate = match &request_id {
            Some(id) => point_read(
                handle,
                Table::SignalSeen,
                signal_seen_key(&run, id.as_bytes()),
            )
            .await?
            .is_some(),
            None => false,
        };
        return Ok(SignalResponse {
            accepted: true,
            duplicate,
            dry_run: true,
        });
    }
    let event = encoded_event(
        cp,
        handle,
        EventBody::SignalPayload(SignalPayload {
            name: r.name.clone(),
            payload: json_payload(&r.payload),
        }),
    )
    .await?;
    let outcome = submit(
        handle,
        Command::signal_run(SignalRun {
            run_id: run.as_bytes().to_vec(),
            name: r.name,
            event: Some(event),
            request_id: request_id.map(String::into_bytes).unwrap_or_default(),
            sent_at_ms: cp.now_ms(),
        }),
    )
    .await?;
    match outcome {
        ApplyOutcome::Accepted => Ok(SignalResponse {
            accepted: true,
            duplicate: false,
            dry_run: false,
        }),
        ApplyOutcome::Duplicate { .. } => Ok(SignalResponse {
            accepted: true,
            duplicate: true,
            dry_run: false,
        }),
        ApplyOutcome::Rejected(e) => Err(rejected(handle, run, e).await),
    }
}

// ---------------------------------------------------------------------------------
// Cancel / terminate
// ---------------------------------------------------------------------------------

pub(crate) async fn cancel_run(
    cp: &ServerControlPlane,
    r: CancelRequest,
) -> Result<CancelResponse, ApiError> {
    r.validate()?;
    let run = parse_run_id(cp, &r.run_id)?;
    let handle = cp.partition(run.partition())?;
    let rec = read_record(handle, run).await?;
    // A continued-as-new run forwards the cancel to its successor (PF-F2): not an error.
    let forwards = rec.status == values::RunStatus::ContinuedAsNew && rec.continued_to.is_some();
    if rec.status.is_terminal() && !forwards {
        return Err(ApiError::run_terminal(&r.run_id, status(rec.status)));
    }
    if r.dry_run {
        return Ok(CancelResponse {
            already_requested: !forwards && rec.cancel_requested,
            dry_run: true,
        });
    }
    let outcome = submit(
        handle,
        Command::request_cancel(RequestCancel {
            run_id: run.as_bytes().to_vec(),
            reason: r.reason.unwrap_or_default(),
            requested_at_ms: cp.now_ms(),
        }),
    )
    .await?;
    match outcome {
        ApplyOutcome::Accepted => Ok(CancelResponse {
            already_requested: false,
            dry_run: false,
        }),
        ApplyOutcome::Duplicate { .. } => Ok(CancelResponse {
            already_requested: true,
            dry_run: false,
        }),
        ApplyOutcome::Rejected(e) => Err(rejected(handle, run, e).await),
    }
}

pub(crate) async fn terminate_run(
    cp: &ServerControlPlane,
    r: TerminateRequest,
) -> Result<TerminateResponse, ApiError> {
    r.validate()?;
    let run = parse_run_id(cp, &r.run_id)?;
    let handle = cp.partition(run.partition())?;
    let rec = read_record(handle, run).await?;
    if rec.status.is_terminal() {
        return Err(ApiError::run_terminal(&r.run_id, status(rec.status)));
    }
    if r.dry_run {
        return Ok(TerminateResponse {
            status: RunStatus::Terminated,
            dry_run: true,
        });
    }
    let outcome = submit(
        handle,
        Command::terminate_run(TerminateRun {
            run_id: run.as_bytes().to_vec(),
            reason: r.reason.unwrap_or_default(),
            terminated_at_ms: cp.now_ms(),
        }),
    )
    .await?;
    match outcome {
        ApplyOutcome::Accepted | ApplyOutcome::Duplicate { .. } => Ok(TerminateResponse {
            status: RunStatus::Terminated,
            dry_run: false,
        }),
        ApplyOutcome::Rejected(e) => Err(rejected(handle, run, e).await),
    }
}

// ---------------------------------------------------------------------------------
// Retry
// ---------------------------------------------------------------------------------

/// The D1 mode for a run in `rec`'s state, or the 409 that forbids a retry.
fn retry_mode(run_id: &str, rec: &RunRecord) -> Result<RetryMode, ApiError> {
    match rec.status {
        s if !s.is_terminal() => Err(ApiError::run_not_terminal(run_id, status(s))),
        values::RunStatus::Completed | values::RunStatus::ContinuedAsNew => {
            Err(ApiError::run_completed(run_id))
        }
        _ if rec.resumable => Ok(RetryMode::Resumed),
        _ => Ok(RetryMode::Restarted),
    }
}

pub(crate) async fn retry_run(
    cp: &ServerControlPlane,
    r: RetryRequest,
) -> Result<RetryResponse, ApiError> {
    let run = parse_run_id(cp, &r.run_id)?;
    let handle = cp.partition(run.partition())?;
    let rec = read_record(handle, run).await?;
    let mode = retry_mode(&r.run_id, &rec)?;
    if r.dry_run {
        return Ok(RetryResponse {
            mode,
            run_id: r.run_id,
            dry_run: true,
        });
    }
    match mode {
        RetryMode::Resumed => {
            let outcome = submit(
                handle,
                Command::retry_run(RetryRun {
                    run_id: run.as_bytes().to_vec(),
                    requested_at_ms: cp.now_ms(),
                }),
            )
            .await?;
            match outcome {
                ApplyOutcome::Accepted | ApplyOutcome::Duplicate { .. } => Ok(RetryResponse {
                    mode,
                    run_id: r.run_id,
                    dry_run: false,
                }),
                // The run changed between the read and apply (e.g. a concurrent retry
                // already resumed it): report its current state.
                ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::BadIntent)) => {
                    let now = read_record(handle, run).await?;
                    Err(retry_mode(&r.run_id, &now).err().unwrap_or_else(|| {
                        ApiError::run_not_terminal(&r.run_id, status(now.status))
                    }))
                }
                ApplyOutcome::Rejected(e) => Err(rejected(handle, run, e).await),
            }
        }
        RetryMode::Restarted => restart(cp, handle, run, rec).await,
    }
}

/// Start a new run linked to `old` (`retry_of`), copying its definition and the F11 fields.
async fn restart(
    cp: &ServerControlPlane,
    old_handle: &PartitionHandle<Rf1Log>,
    old: RunId,
    rec: RunRecord,
) -> Result<RetryResponse, ApiError> {
    let reader = old_handle.reader();
    let input = tokio::task::spawn_blocking(move || {
        query::run_payload(&reader, &old, query::PAYLOAD_INPUT)
    })
    .await
    .map_err(task_error)?
    .map_err(store_error)?;
    // Inline refs are self-contained and blob refs name the node-wide, content-addressed
    // blob store, so the predecessor's input ref is valid on any partition.
    let input = input.unwrap_or_else(|| PayloadRef::inline(Bytes::from_static(b"null")));

    let uuid = uuid::Uuid::now_v7();
    let p = partition_for_new_run(&uuid, cp.partition_count());
    let run = RunId::new(p, uuid);
    let handle = cp.partition(p)?;
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed)
        .map_err(|e| ApiError::internal(format!("OS random source unavailable: {e}")))?;
    let now = cp.now_ms();
    let start = StartRun {
        run_id: run.as_bytes().to_vec(),
        // Stored (already qualified) form: never re-qualified (05-24).
        workflow: rec.workflow,
        workflow_version: rec.workflow_version,
        queue: rec.queue,
        input: Some(input),
        idempotency_key: Vec::new(),
        conflict_policy: cmd::ConflictPolicy::ReturnExisting as i32,
        concurrency_key: rec.concurrency_key,
        priority: rec.priority,
        ready_at_ms: now,
        requested_at_ms: now,
        parent: None,
        rng_seed: seed.to_vec(),
        module_sha: rec.module_sha.map(|s| s.to_vec()).unwrap_or_default(),
        // F11: tenant, tags and traceparent carry over.
        tags: rec.tags,
        tenant: rec.tenant.unwrap_or_default(),
        traceparent: rec.traceparent.unwrap_or_default(),
        run_timeout_ms: rec.run_timeout_ms.unwrap_or(0),
        schedule_to_start_ms: rec.schedule_to_start_ms,
        retention_ms: rec.retention_ms,
        retry_of: old.as_bytes().to_vec(),
        continued_from: Vec::new(),
        run_started: None,
    };
    match submit(handle, Command::start_run(start)).await? {
        ApplyOutcome::Accepted | ApplyOutcome::Duplicate { .. } => Ok(RetryResponse {
            mode: RetryMode::Restarted,
            run_id: run.to_string(),
            dry_run: false,
        }),
        ApplyOutcome::Rejected(e) => Err(rejected(handle, run, e).await),
    }
}

// ---------------------------------------------------------------------------------
// Await tokens
// ---------------------------------------------------------------------------------

/// 409 `token_already_completed`, with the run's current status in the details.
fn token_done(run: &RunId, rec: Option<&RunRecord>) -> ApiError {
    let mut e = ApiError::token_already_completed(&run.to_string());
    if let (Some(rec), Some(serde_json::Value::Object(d))) = (rec, e.details.as_mut()) {
        d.insert(
            "status".into(),
            serde_json::Value::String(status(rec.status).as_str().to_owned()),
        );
    }
    e
}

pub(crate) async fn complete_token(
    cp: &ServerControlPlane,
    r: CompleteTokenRequest,
) -> Result<CompleteTokenResponse, ApiError> {
    let key = cp
        .meta()
        .view()
        .read(|s| s.token_key())
        .ok_or_else(|| ApiError::unavailable("the cluster token key is not available"))?;
    let parts = ostro_engine::token::verify(&key, &r.token)
        .map_err(|e| ApiError::token_invalid(&e.to_string()))?;
    let run: RunId = parts
        .run_id
        .parse()
        .map_err(|_| ApiError::token_invalid("the token names no valid run"))?;
    if run.partition().0 != parts.partition || !cp.partition_count().contains(run.partition()) {
        return Err(ApiError::token_invalid(
            "the token's partition does not match its run",
        ));
    }
    let op = parts.op;
    let handle = cp.partition(run.partition())?;
    let rec = read_record(handle, run).await?;
    let run_id = run.to_string();
    if rec.status.is_terminal() {
        return Err(ApiError::run_terminal(&run_id, status(rec.status)));
    }
    if r.dry_run {
        let row = point_read(handle, Table::Waits, wait_key(&run, op)).await?;
        let row: Option<WaitRow> = row
            .map(|v| values::decode(&v))
            .transpose()
            .map_err(store_error)?;
        return match row {
            Some(w) if w.kind == WaitKind::Token => Ok(CompleteTokenResponse {
                run_id,
                accepted: true,
                dry_run: true,
            }),
            None if op < rec.next_seq => Err(token_done(&run, Some(&rec))),
            _ => Err(ApiError::token_invalid(
                "no open await-token wait for this op",
            )),
        };
    }
    let event = encoded_event(
        cp,
        handle,
        EventBody::TokenCompleted(TokenCompleted {
            op,
            payload: json_payload(&r.payload),
        }),
    )
    .await?;
    let outcome = submit(
        handle,
        Command::complete_token(CompleteToken {
            run_id: run.as_bytes().to_vec(),
            op,
            event: Some(event),
            completed_at_ms: cp.now_ms(),
        }),
    )
    .await?;
    match outcome {
        ApplyOutcome::Accepted => Ok(CompleteTokenResponse {
            run_id,
            accepted: true,
            dry_run: false,
        }),
        ApplyOutcome::Duplicate { .. } => {
            let now = read_record(handle, run).await.ok();
            Err(token_done(&run, now.as_ref()))
        }
        ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::BadIntent)) => Err(
            ApiError::token_invalid("no open await-token wait for this op"),
        ),
        ApplyOutcome::Rejected(e) => Err(rejected(handle, run, e).await),
    }
}

#[cfg(test)]
mod tests {
    use ostro_api::ErrorCode;

    use super::*;

    fn rec(status: values::RunStatus, resumable: bool) -> RunRecord {
        RunRecord {
            status,
            resumable,
            ..Default::default()
        }
    }

    #[test]
    fn retry_mode_follows_d1() {
        use values::RunStatus as S;
        for s in [S::Scheduled, S::Ready, S::Running] {
            let e = retry_mode("r", &rec(s, false)).expect_err("live");
            assert_eq!(e.reason(), Some("run_not_terminal"));
        }
        for s in [S::Completed, S::ContinuedAsNew] {
            let e = retry_mode("r", &rec(s, false)).expect_err("completed");
            assert_eq!(e.code, ErrorCode::Conflict);
            assert_eq!(e.reason(), Some("run_completed"));
        }
        for s in [S::Failed, S::Terminated, S::TimedOut] {
            assert_eq!(
                retry_mode("r", &rec(s, true)).expect("ok"),
                RetryMode::Resumed
            );
        }
        for s in [S::Failed, S::Cancelled] {
            assert_eq!(
                retry_mode("r", &rec(s, false)).expect("ok"),
                RetryMode::Restarted
            );
        }
    }

    #[test]
    fn token_done_carries_status() {
        let run = RunId::new(ostro_types::PartitionId(1), uuid::Uuid::now_v7());
        let e = token_done(&run, Some(&rec(values::RunStatus::Running, false)));
        assert_eq!(e.reason(), Some("token_already_completed"));
        assert_eq!(e.details.as_ref().expect("details")["status"], "running");
    }
}
