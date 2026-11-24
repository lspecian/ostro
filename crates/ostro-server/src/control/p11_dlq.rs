//! DLQ and failure-policy API (Phase 11, OPS-04, D-P11-11/12, D-P11-29; plan 11-12):
//! list / requeue / dismiss DLQ entries and `GET|PUT /v1/{workflows,queues}/{name}/failure-policy`.
//! Called by [`crate::p11::P11Controllers`]; [`is_dlq_member`] is also the DLQ predicate of
//! 11-11 search (`GET /v1/runs?dlq=true`) and the bulk `dlq` filter.
//!
//! # Membership (a view, not a status)
//!
//! A run is a DLQ member when it is `Failed` or `TimedOut`, its queue's DLQ policy is
//! enabled (ext tag 145, [`crate::onfailure`]), and its `dlq` row (keyspace 11, written
//! only by `Cmd::DlqUpdate`) carries neither `dismissed_at_ms` nor `requeued_to` **for the
//! current failure episode**. Marks count only while `DlqRow.episode_completed_at_ms ==
//! RunRecord.completed_at_ms` (W5): a run that is resumed and fails again has a new
//! `completed_at_ms`, so it re-enters the DLQ and the old row is not membership. Every
//! `DlqUpdate` submitted here carries the run's current `completed_at_ms`.
//! `include_dismissed` lists dismissed and requeued members too (the marks are shown).
//!
//! # Listing
//!
//! Newest first through the run index: the `Status(Failed)` and `Status(TimedOut)` prefixes
//! of every partition merged by `(created_at_ms desc, uuid desc, partition asc)`, page token
//! `i1.d.<created_at_ms>.<run id>` (the 11-11 DLQ token: the last run examined). A call
//! examines at most [`list::SCAN_BUDGET`] index rows. Filters (workflow, queue, every tag,
//! reason prefix, membership) run in memory on the record and the `dlq` row read with each
//! candidate. While an index is rebuilding, a request without an index token takes the
//! budgeted oldest-first scan (`c1.` tokens) with the same filters; an `i1.` token then
//! answers 503 `index_rebuilding`, as in 11-11.
//!
//! # Reason
//!
//! [`failure_reason`]: `terminal_reason` when set (`run_timeout`, `schedule_to_start`, an
//! operator reason; `timed_out` for an empty one on a `TimedOut` run); else the stored
//! result payload's failure JSON as `"<reason_code>: <message>"` (a guest `RunFailed` is
//! `workflow_failed: <message>`, an executor failure e.g. `nondeterminism: ...`). Inline
//! results are parsed directly; blob-backed results are fetched only up to
//! [`MAX_REASON_BLOB`] (64 KiB), larger ones read `"<n>-byte failure result (not loaded)"`.
//! The `reason` filter is a prefix match on this string.
//!
//! # Requeue
//!
//! - `auto`: `resume` when the run is resumable, else `restart`.
//! - `resume`: `DlqUpdate Requeued{resume}` (current episode), then the Phase 5 `RetryRun`;
//!   the run keeps its id and leaves the view by itself (it is live again).
//! - `restart`: a new linked run (the Phase 5 restart copy: same workflow, version, module,
//!   input, queue, tenant, tags, `retry_of`) with dedupe key `__dlq/<run>/<resumed_count+1>`
//!   (`ReturnExisting`) on `partition_for_key(workflow, key)`, then `DlqUpdate
//!   Requeued{restart, new_run}`. Re-issuing after a crash between the two returns the same
//!   new run; once the mark exists a repeat returns it without submitting anything.
//! - `rerun`: delegates to [`super::p11_rerun::rerun_run`] with the same dedupe key, then
//!   `Requeued{rerun, new_run}` (answers what the rerun control plane answers; 501 until
//!   11-10 lands).
//!
//! Dismissing a member writes `DlqUpdate Dismissed{reason, at_ms}`; a non-member is 409
//! `dlq_not_member`. Every Cmd 45 emission checks [`p11_feature_enabled`] first (D-P11-30),
//! and every `at_ms` comes from the injected clock.

use std::cmp::Reverse;
use std::collections::VecDeque;

use bytes::Bytes;
use ostro_api::ApiError;
use ostro_api::domain::{ListOrder, ListRunsRequest, RetryRequest, effective_limit};
use ostro_api::phase8::version_mismatch;
use ostro_api::phase11::{
    DlqDismissRequest, DlqEntry, DlqListRequest, DlqPage, DlqRequeueRequest, DlqRequeueResponse,
    FailurePolicy, PolicyScope, RequeueMode, RerunRequest, RerunVersion, dlq_not_member,
};
use ostro_api::rest::RunStatusDto;
use ostro_log::PartitionLog;
use ostro_meta::{MetaKey, MetaRecord, QueuePolicy, WorkflowPolicy};
use ostro_partition::query::{self, IndexError};
use ostro_partition::store::derived::{IndexKind, SUFFIX_LEN, index_prefix};
use ostro_partition::store::keys::run_key;
use ostro_partition::store::values::{self, DlqRow, RunRecord, RunStatus};
use ostro_partition::store::{StoreReader, Table};
use ostro_partition::{ApplyOutcome, PartitionHandle, RejectReason, StoreError};
use ostro_types::command::{self as cmd, Command, DlqUpdate, StartRun, dlq_update};
use ostro_types::routing::partition_for_key;
use ostro_types::{PayloadRef, RunId};

use super::list;
use super::p11_rerun;
use super::p11_search::{decode_token, encode_token};
use super::policy::policy_key;
use super::runs::{
    attach_run_started, not_found, parse_run_id, partition_error, status, store_error, task_error,
};
use crate::onfailure::{self, DlqPolicy, OnFailure};
use crate::p11::{P11Ctx, P11Feature, p11_feature_enabled};

/// Largest blob-backed failure result loaded to extract a reason.
pub const MAX_REASON_BLOB: u64 = 64 * 1024;
/// Index rows read from one stream per refill.
const REFILL: usize = 128;
/// Token kind of DLQ listings (shared with 11-11's `dlq=true` newest search).
const KIND: char = 'd';
/// CAS attempts of a failure-policy write.
const POLICY_RETRIES: usize = 8;

// ---------------------------------------------------------------------------------
// Membership
// ---------------------------------------------------------------------------------

/// The run's `dlq` row, if any.
pub fn dlq_row(r: &StoreReader, run: &RunId) -> Result<Option<DlqRow>, StoreError> {
    r.get(Table::Dlq, &run_key(run))?
        .map(|v| values::decode(&v))
        .transpose()
}

/// The marks of `row` that belong to `rec`'s current failure episode (W5).
pub fn current_marks(row: Option<DlqRow>, rec: &RunRecord) -> Option<DlqRow> {
    row.filter(|r| Some(r.episode_completed_at_ms) == rec.completed_at_ms)
}

/// The episode a mark for `rec` names (its latest `completed_at_ms`).
pub fn episode(rec: &RunRecord) -> u64 {
    rec.completed_at_ms.unwrap_or(0)
}

/// Membership from the record, the current-episode marks and the queue's policy.
pub fn member(
    rec: &RunRecord,
    marks: Option<&DlqRow>,
    policy_enabled: bool,
    include_dismissed: bool,
) -> bool {
    matches!(rec.status, RunStatus::Failed | RunStatus::TimedOut)
        && policy_enabled
        && (include_dismissed
            || marks.is_none_or(|m| m.dismissed_at_ms.is_none() && m.requeued_to.is_none()))
}

fn default_catchup_ms(ctx: &P11Ctx) -> u64 {
    u64::try_from(ctx.cfg.dlq.hook_catchup.get().as_millis()).unwrap_or(u64::MAX)
}

fn queue_enabled(ctx: &P11Ctx, queue: &str) -> bool {
    onfailure::dlq_policy(ctx.cc.meta.view(), queue, default_catchup_ms(ctx)).enabled
}

/// Whether `run` (record `rec`) is a DLQ member (dismissed and requeued runs are not).
/// One point read of the `dlq` row.
pub fn is_dlq_member(ctx: &P11Ctx, run: &RunId, rec: &RunRecord) -> Result<bool, ApiError> {
    if !matches!(rec.status, RunStatus::Failed | RunStatus::TimedOut) {
        return Ok(false);
    }
    let reader = ctx.cc.partitions.partition(run.partition())?.reader();
    let row = dlq_row(&reader, run).map_err(store_error)?;
    let marks = current_marks(row, rec);
    Ok(member(
        rec,
        marks.as_ref(),
        queue_enabled(ctx, &rec.queue),
        false,
    ))
}

// ---------------------------------------------------------------------------------
// Reason
// ---------------------------------------------------------------------------------

/// `"<reason_code>: <message>"` from a failure JSON body. A message that already starts
/// with `"<reason_code>:"` (the 11-06 `limit_exceeded: history_events 21 > 20` messages)
/// is used as is, so the code is not repeated.
pub fn reason_from_failure_json(bytes: &[u8]) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let code = v.get("reason_code").and_then(|c| c.as_str());
    let msg = v.get("message").and_then(|m| m.as_str());
    match (code, msg) {
        (Some(c), Some(m)) if m.strip_prefix(c).is_some_and(|r| r.starts_with(':')) => {
            Some(m.to_owned())
        }
        (Some(c), Some(m)) if !m.is_empty() => Some(format!("{c}: {m}")),
        (Some(c), _) => Some(c.to_owned()),
        (None, Some(m)) => Some(m.to_owned()),
        (None, None) => None,
    }
}

/// The failure reason of a failed / timed-out run (see the module docs).
pub async fn failure_reason(
    handle: &PartitionHandle<PartitionLog>,
    run: &RunId,
    rec: &RunRecord,
) -> Result<String, ApiError> {
    if !rec.terminal_reason.is_empty() {
        return Ok(rec.terminal_reason.clone());
    }
    let fallback = || match rec.status {
        RunStatus::TimedOut => "timed_out".to_owned(),
        _ => "failed".to_owned(),
    };
    let reader = handle.reader();
    let r = *run;
    let payload =
        tokio::task::spawn_blocking(move || query::run_payload(&reader, &r, query::PAYLOAD_RESULT))
            .await
            .map_err(task_error)?
            .map_err(store_error)?;
    let Some(p) = payload else {
        return Ok(fallback());
    };
    let bytes = match &p.kind {
        Some(ostro_types::payload::payload_ref::Kind::Inline(b)) => b.clone(),
        Some(ostro_types::payload::payload_ref::Kind::Blob(b)) if b.len > MAX_REASON_BLOB => {
            return Ok(format!("{}-byte failure result (not loaded)", b.len));
        }
        Some(ostro_types::payload::payload_ref::Kind::Blob(_)) => {
            match handle.fetch_payload(&p).await {
                Ok(b) => b,
                Err(e) => {
                    tracing::debug!(%run, error = %e, "failure result fetch failed");
                    return Ok(fallback());
                }
            }
        }
        None => return Ok(fallback()),
    };
    Ok(reason_from_failure_json(&bytes).unwrap_or_else(fallback))
}

fn api_status(s: RunStatus) -> RunStatusDto {
    RunStatusDto::from(status(s))
}

fn id18(b: &[u8; 18]) -> String {
    RunId::from_bytes(b)
        .map(|r| r.to_string())
        .unwrap_or_default()
}

/// Build the entry of a member.
pub async fn entry(
    handle: &PartitionHandle<PartitionLog>,
    run: &RunId,
    rec: RunRecord,
    marks: Option<DlqRow>,
) -> Result<DlqEntry, ApiError> {
    let reason = failure_reason(handle, run, &rec).await?;
    let m = marks.unwrap_or_default();
    Ok(DlqEntry {
        run_id: run.to_string(),
        workflow: rec.workflow,
        version: rec.workflow_version,
        queue: rec.queue,
        status: api_status(rec.status),
        reason,
        failed_at_ms: rec.completed_at_ms.unwrap_or(0),
        tags: rec.tags,
        resumable: rec.resumable,
        hook_run_id: m.hook_run.as_ref().map(id18),
        dismissed_at_ms: m.dismissed_at_ms,
        dismiss_reason: m.dismiss_reason,
        requeued_to: m.requeued_to.as_ref().map(id18),
        requeued_at_ms: m.requeued_at_ms,
    })
}

// ---------------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------------

type Pos = (Reverse<u64>, Reverse<u128>, u16);

fn pos(run: &RunId, created_at_ms: u64) -> Pos {
    (
        Reverse(created_at_ms),
        Reverse(run.uuid().as_u128()),
        run.partition().0,
    )
}

fn suffix(p: &Pos) -> [u8; SUFFIX_LEN] {
    let mut s = [0u8; SUFFIX_LEN];
    s[..8].copy_from_slice(&(!p.0.0).to_be_bytes());
    s[8..].copy_from_slice(&(!p.1.0).to_be_bytes());
    s
}

fn predecessor(mut s: [u8; SUFFIX_LEN]) -> Option<[u8; SUFFIX_LEN]> {
    for b in s.iter_mut().rev() {
        if *b == 0 {
            *b = 0xff;
        } else {
            *b -= 1;
            return Some(s);
        }
    }
    None
}

/// Where stream `(prefix, q)` resumes after `cur` (same rule as 11-11's search).
fn resume_after(prefix: &[u8], q: u16, cur: Option<&Pos>) -> Option<Vec<u8>> {
    let cur = cur?;
    let sfx = if q <= cur.2 {
        suffix(cur)
    } else {
        predecessor(suffix(cur))?
    };
    let mut k = prefix.to_vec();
    k.extend_from_slice(&sfx);
    Some(k)
}

/// The two status prefixes of DLQ candidates.
pub fn dlq_prefixes() -> [Vec<u8>; 2] {
    [
        index_prefix(IndexKind::Status, &[4]), // Failed
        index_prefix(IndexKind::Status, &[7]), // TimedOut
    ]
}

type Candidate = (Pos, RunId, Option<(RunRecord, Option<DlqRow>)>);

struct Stream {
    reader: StoreReader,
    prefix: Vec<u8>,
    after: Option<Vec<u8>>,
    buf: VecDeque<Candidate>,
    done: bool,
}

async fn refill(streams: &mut Vec<Stream>, per: usize) -> Result<(), IndexError> {
    if !streams.iter().any(|s| s.buf.is_empty() && !s.done) {
        return Ok(());
    }
    let taken = std::mem::take(streams);
    let back = tokio::task::spawn_blocking(move || {
        let mut taken = taken;
        for s in taken.iter_mut().filter(|s| s.buf.is_empty() && !s.done) {
            let page = query::index_page(&s.reader, &s.prefix, s.after.as_deref(), per, per + 1)?;
            for (run, created) in page.runs {
                let rec = query::run_record(&s.reader, &run)?;
                let item = match rec {
                    Some(rec) => Some((rec, dlq_row(&s.reader, &run)?)),
                    None => None,
                };
                s.buf.push_back((pos(&run, created), run, item));
            }
            match page.next {
                Some(n) => s.after = Some(n),
                None => s.done = true,
            }
        }
        Ok::<_, IndexError>(taken)
    })
    .await
    .map_err(|e| IndexError::Store(StoreError::Codec(format!("dlq list task: {e}"))))??;
    *streams = back;
    Ok(())
}

fn filters_match(req: &DlqListRequest, rec: &RunRecord) -> bool {
    req.workflow.as_ref().is_none_or(|w| *w == rec.workflow)
        && req.queue.as_ref().is_none_or(|q| *q == rec.queue)
        && req.tags.iter().all(|t| rec.tags.contains(t))
}

fn reason_matches(req: &DlqListRequest, reason: &str) -> bool {
    req.reason
        .as_deref()
        .filter(|r| !r.is_empty())
        .is_none_or(|r| reason.starts_with(r))
}

/// `GET /v1/dlq`.
pub async fn list_dlq(ctx: &P11Ctx, req: DlqListRequest) -> Result<DlqPage, ApiError> {
    list_dlq_with_budget(ctx, req, list::SCAN_BUDGET).await
}

/// [`list_dlq`] with an explicit budget (tests).
#[doc(hidden)]
pub async fn list_dlq_with_budget(
    ctx: &P11Ctx,
    req: DlqListRequest,
    budget: usize,
) -> Result<DlqPage, ApiError> {
    let token = req.page_token.clone().filter(|t| !t.is_empty());
    if token.as_deref().is_some_and(|t| t.starts_with("c1.")) {
        return scan_fallback(ctx, req, budget).await;
    }
    match indexed(ctx, &req, budget).await {
        Ok(p) => Ok(p),
        Err(IndexError::Rebuilding) if token.is_none() => scan_fallback(ctx, req, budget).await,
        Err(IndexError::Rebuilding) => Err(ApiError::unavailable(
            "the run index is rebuilding; restart the listing without a page token",
        )
        .with_details(serde_json::json!({ "reason": "index_rebuilding" }))),
        Err(IndexError::Store(e)) => Err(store_error(e)),
    }
}

async fn indexed(ctx: &P11Ctx, req: &DlqListRequest, budget: usize) -> Result<DlqPage, IndexError> {
    let cp = &ctx.cc.partitions;
    let budget = budget.max(1);
    let count = cp.partition_count();
    let limit = effective_limit(req.limit) as usize;
    let cursor = match req.page_token.as_deref().filter(|t| !t.is_empty()) {
        Some(t) => Some(decode_token(t, KIND, count).map_err(api_as_index)?),
        None => None,
    };
    let mut streams = Vec::new();
    for prefix in dlq_prefixes() {
        for p in count.ids() {
            let reader = cp.partition(p).map_err(api_as_index)?.reader();
            streams.push(Stream {
                after: resume_after(&prefix, p.0, cursor.as_ref()),
                reader,
                prefix: prefix.clone(),
                buf: VecDeque::new(),
                done: false,
            });
        }
    }
    let per = REFILL.min(budget);
    let mut entries = Vec::new();
    let mut last: (u64, RunId);
    let mut scanned = 0usize;
    loop {
        refill(&mut streams, per).await?;
        let next = streams
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.buf.front().map(|(p, _, _)| (*p, i)))
            .min();
        let Some((p, i)) = next else {
            return Ok(DlqPage {
                entries,
                next_page_token: None,
            });
        };
        let (_, run, item) = streams[i].buf.pop_front().expect("front exists");
        scanned += 1;
        last = (p.0.0, run);
        if let Some((rec, row)) = item
            && filters_match(req, &rec)
        {
            let marks = current_marks(row, &rec);
            if member(
                &rec,
                marks.as_ref(),
                queue_enabled(ctx, &rec.queue),
                req.include_dismissed,
            ) {
                let handle = &cp.partition(run.partition()).map_err(api_as_index)?;
                let e = entry(handle, &run, rec, marks)
                    .await
                    .map_err(api_as_index)?;
                if reason_matches(req, &e.reason) {
                    entries.push(e);
                }
            }
        }
        if entries.len() >= limit || scanned >= budget {
            break;
        }
    }
    refill(&mut streams, per).await?;
    let more = streams.iter().any(|s| !s.buf.is_empty());
    Ok(DlqPage {
        entries,
        next_page_token: more.then(|| encode_token(KIND, last.0, &last.1)),
    })
}

fn api_as_index(e: ApiError) -> IndexError {
    IndexError::Store(StoreError::Codec(e.message))
}

/// The budgeted oldest-first scan (index rebuilding): membership in the scan predicate,
/// the reason filter on the selected page.
async fn scan_fallback(
    ctx: &P11Ctx,
    req: DlqListRequest,
    budget: usize,
) -> Result<DlqPage, ApiError> {
    let lr = ListRunsRequest {
        workflow: req.workflow.clone(),
        queue: req.queue.clone(),
        tags: req.tags.clone(),
        tag: req.tags.first().cloned(),
        dlq: true,
        order: ListOrder::Oldest,
        limit: req.limit,
        page_token: req.page_token.clone(),
        ..Default::default()
    };
    let include = req.include_dismissed;
    let c2 = ctx.clone();
    let page = list::list_runs_scan(&ctx.cc.partitions, lr, budget, &move |run, rec| {
        let reader = c2.cc.partitions.partition(run.partition())?.reader();
        let marks = current_marks(dlq_row(&reader, run).map_err(store_error)?, rec);
        Ok(member(
            rec,
            marks.as_ref(),
            queue_enabled(&c2, &rec.queue),
            include,
        ))
    })
    .await?;
    let mut entries = Vec::with_capacity(page.runs.len());
    for s in page.runs {
        let run: RunId = s
            .run_id
            .parse()
            .map_err(|e| ApiError::internal(format!("listed run id: {e}")))?;
        let handle = &ctx.cc.partitions.partition(run.partition())?;
        let reader = handle.reader();
        let (rec, row) = tokio::task::spawn_blocking(move || {
            Ok::<_, StoreError>((query::run_record(&reader, &run)?, dlq_row(&reader, &run)?))
        })
        .await
        .map_err(task_error)?
        .map_err(store_error)?;
        let Some(rec) = rec else { continue };
        let marks = current_marks(row, &rec);
        let e = entry(handle, &run, rec, marks).await?;
        if reason_matches(&req, &e.reason) {
            entries.push(e);
        }
    }
    Ok(DlqPage {
        entries,
        next_page_token: page.next_page_token,
    })
}

// ---------------------------------------------------------------------------------
// Marks
// ---------------------------------------------------------------------------------

/// Submit a `DlqUpdate` for `run`'s episode `episode` (gated by D-P11-30).
pub async fn submit_mark(
    handle: &PartitionHandle<PartitionLog>,
    run: &RunId,
    episode: u64,
    op: dlq_update::Op,
) -> Result<ApplyOutcome, ApiError> {
    if !p11_feature_enabled(P11Feature::DlqUpdate) {
        return Err(ApiError::unavailable(
            "DLQ updates are not enabled on this cluster yet",
        ));
    }
    handle
        .submit_one(Command::dlq_update(DlqUpdate {
            run_id: run.as_bytes().to_vec(),
            op: Some(op),
            episode_completed_at_ms: episode,
        }))
        .await
        .map_err(partition_error)
}

async fn read_both(
    handle: &PartitionHandle<PartitionLog>,
    run: RunId,
) -> Result<(RunRecord, Option<DlqRow>), ApiError> {
    let reader = handle.reader();
    let (rec, row) = tokio::task::spawn_blocking(move || {
        Ok::<_, StoreError>((query::run_record(&reader, &run)?, dlq_row(&reader, &run)?))
    })
    .await
    .map_err(task_error)?
    .map_err(store_error)?;
    let rec = rec.ok_or_else(|| not_found(&run.to_string()))?;
    Ok((rec, row))
}

fn mark_rejected(run_id: &str, r: RejectReason) -> ApiError {
    match r {
        RejectReason::RunNotFound => not_found(run_id),
        // The run changed (resumed, or a newer episode) between the read and apply.
        RejectReason::Invalid(_) => dlq_not_member(run_id),
        other => ApiError::unavailable(format!("DLQ update rejected: {other:?}")),
    }
}

// ---------------------------------------------------------------------------------
// Dismiss
// ---------------------------------------------------------------------------------

/// `POST /v1/dlq/{run}/dismiss`.
pub async fn dismiss_dlq(ctx: &P11Ctx, req: DlqDismissRequest) -> Result<DlqEntry, ApiError> {
    let cp = &ctx.cc.partitions;
    let run = parse_run_id(cp, &req.run_id)?;
    let handle = &cp.partition(run.partition())?;
    let (rec, row) = read_both(handle, run).await?;
    let marks = current_marks(row, &rec);
    if !member(&rec, marks.as_ref(), queue_enabled(ctx, &rec.queue), true)
        || marks.as_ref().is_some_and(|m| m.requeued_to.is_some())
    {
        return Err(dlq_not_member(&req.run_id));
    }
    let op = dlq_update::Op::Dismissed(dlq_update::Dismissed {
        reason: req.reason.clone(),
        at_ms: ctx.cc.now_ms(),
    });
    match submit_mark(handle, &run, episode(&rec), op).await? {
        ApplyOutcome::Accepted | ApplyOutcome::Duplicate { .. } => {}
        ApplyOutcome::Rejected(r) => return Err(mark_rejected(&req.run_id, r)),
    }
    let (rec, row) = read_both(handle, run).await?;
    let marks = current_marks(row, &rec);
    entry(handle, &run, rec, marks).await
}

// ---------------------------------------------------------------------------------
// Requeue
// ---------------------------------------------------------------------------------

fn mode_label(m: u8) -> &'static str {
    match u32::from(m) {
        dlq_update::REQUEUE_RESUME => "resume",
        dlq_update::REQUEUE_RESTART => "restart",
        _ => "rerun",
    }
}

fn count_requeue(mode: &'static str) {
    metrics::counter!("ostro_dlq_requeues_total", "mode" => mode).increment(1);
}

/// `POST /v1/dlq/{run}/requeue`.
pub async fn requeue_dlq(
    ctx: &P11Ctx,
    req: DlqRequeueRequest,
) -> Result<DlqRequeueResponse, ApiError> {
    let cp = &ctx.cc.partitions;
    let run = parse_run_id(cp, &req.run_id)?;
    let handle = &cp.partition(run.partition())?;
    let (rec, row) = read_both(handle, run).await?;
    let marks = current_marks(row, &rec);
    // Idempotent re-issue: the episode was already requeued to a new run.
    if let Some(m) = &marks
        && let Some(to) = &m.requeued_to
    {
        return Ok(DlqRequeueResponse {
            run_id: req.run_id,
            mode: mode_label(m.requeue_mode.unwrap_or(1)).to_owned(),
            new_run_id: Some(id18(to)),
        });
    }
    if !member(&rec, marks.as_ref(), queue_enabled(ctx, &rec.queue), true) {
        return Err(dlq_not_member(&req.run_id));
    }
    let resolved = match &req.mode {
        RequeueMode::Auto if rec.resumable => RequeueMode::Resume,
        RequeueMode::Auto => RequeueMode::Restart,
        RequeueMode::Resume if !rec.resumable => {
            return Err(ApiError::conflict(format!(
                "run {} is not resumable; requeue it with mode restart or rerun",
                req.run_id
            ))
            .with_details(serde_json::json!({ "reason": "run_not_resumable" })));
        }
        m => m.clone(),
    };
    let label = match &resolved {
        RequeueMode::Resume => "resume",
        RequeueMode::Restart => "restart",
        _ => "rerun",
    };
    if req.dry_run {
        return Ok(DlqRequeueResponse {
            run_id: req.run_id,
            mode: label.to_owned(),
            new_run_id: None,
        });
    }
    let ep = episode(&rec);
    let key = onfailure::requeue_dedupe_key(&req.run_id, rec.resumed_count.saturating_add(1));
    let new_run = match resolved {
        RequeueMode::Resume | RequeueMode::Auto => {
            let op = dlq_update::Op::Requeued(dlq_update::Requeued {
                mode: dlq_update::REQUEUE_RESUME,
                new_run_id: Vec::new(),
                at_ms: ctx.cc.now_ms(),
            });
            if let ApplyOutcome::Rejected(r) = submit_mark(handle, &run, ep, op).await? {
                return Err(mark_rejected(&req.run_id, r));
            }
            // Phase 13 (13-02): through the routed control plane.
            ctx.cc
                .routed
                .retry_run(RetryRequest {
                    run_id: req.run_id.clone(),
                    dry_run: false,
                })
                .await?;
            count_requeue("resume");
            return Ok(DlqRequeueResponse {
                run_id: req.run_id,
                mode: "resume".to_owned(),
                new_run_id: None,
            });
        }
        RequeueMode::Restart => restart_with_key(ctx, handle, run, rec, &key).await?,
        RequeueMode::Rerun { from } => {
            let r = p11_rerun::rerun_run(
                ctx,
                RerunRequest {
                    run_id: req.run_id.clone(),
                    from,
                    version: RerunVersion::Active,
                    idempotency_key: Some(key),
                    dry_run: false,
                },
            )
            .await?;
            let id = r
                .new_run_id
                .ok_or_else(|| ApiError::internal("rerun returned no new run"))?;
            id.parse::<RunId>()
                .map_err(|e| ApiError::internal(format!("rerun run id: {e}")))?
        }
    };
    let mode = if label == "restart" {
        dlq_update::REQUEUE_RESTART
    } else {
        dlq_update::REQUEUE_RERUN
    };
    let op = dlq_update::Op::Requeued(dlq_update::Requeued {
        mode,
        new_run_id: new_run.as_bytes().to_vec(),
        at_ms: ctx.cc.now_ms(),
    });
    if let ApplyOutcome::Rejected(r) = submit_mark(handle, &run, ep, op).await? {
        return Err(mark_rejected(&req.run_id, r));
    }
    count_requeue(label);
    Ok(DlqRequeueResponse {
        run_id: req.run_id,
        mode: label.to_owned(),
        new_run_id: Some(new_run.to_string()),
    })
}

/// The Phase 5 restart copy (`control/lifecycle.rs`) with a dedupe key: the new run is
/// placed by `partition_for_key(workflow, key)` and a repeat returns the existing run.
async fn restart_with_key(
    ctx: &P11Ctx,
    old_handle: &PartitionHandle<PartitionLog>,
    old: RunId,
    rec: RunRecord,
    key: &str,
) -> Result<RunId, ApiError> {
    let cp = &ctx.cc.partitions;
    let reader = old_handle.reader();
    let input = tokio::task::spawn_blocking(move || {
        query::run_payload(&reader, &old, query::PAYLOAD_INPUT)
    })
    .await
    .map_err(task_error)?
    .map_err(store_error)?;
    let input = input.unwrap_or_else(|| PayloadRef::inline(Bytes::from_static(b"null")));
    let input_bytes = old_handle
        .fetch_payload(&input)
        .await
        .map_err(partition_error)?;
    let uuid = uuid::Uuid::now_v7();
    let p = partition_for_key(&rec.workflow, key.as_bytes(), cp.partition_count());
    let run = RunId::new(p, uuid);
    let handle = &cp.partition(p)?;
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed)
        .map_err(|e| ApiError::internal(format!("OS random source unavailable: {e}")))?;
    let now = ctx.cc.now_ms();
    let start = StartRun {
        run_id: run.as_bytes().to_vec(),
        workflow: rec.workflow,
        workflow_version: rec.workflow_version,
        queue: rec.queue,
        input: Some(input),
        idempotency_key: key.as_bytes().to_vec(),
        conflict_policy: cmd::ConflictPolicy::ReturnExisting as i32,
        concurrency_key: rec.concurrency_key,
        priority: rec.priority,
        ready_at_ms: now,
        requested_at_ms: now,
        parent: None,
        rng_seed: seed.to_vec(),
        module_sha: rec.module_sha.map(|s| s.to_vec()).unwrap_or_default(),
        // Hook-run lineage tags are not copied onto the restart (D-P11-6).
        tags: rec
            .tags
            .into_iter()
            .filter(|t| !t.starts_with(onfailure::ON_FAILURE_OF_TAG))
            .collect(),
        tenant: rec.tenant.unwrap_or_default(),
        traceparent: rec.traceparent.unwrap_or_default(),
        run_timeout_ms: rec.run_timeout_ms.unwrap_or(0),
        schedule_to_start_ms: rec.schedule_to_start_ms,
        retention_ms: rec.retention_ms,
        retry_of: old.as_bytes().to_vec(),
        continued_from: Vec::new(),
        run_started: None,
        ..Default::default()
    };
    let start = attach_run_started(handle, start, &input_bytes).await?;
    match handle
        .submit_one(Command::start_run(start))
        .await
        .map_err(partition_error)?
    {
        ApplyOutcome::Accepted => Ok(run),
        ApplyOutcome::Duplicate { existing } => Ok(existing.unwrap_or(run)),
        ApplyOutcome::Rejected(e) => Err(ApiError::invalid_argument(format!(
            "restart rejected: {e:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------------
// Failure policy
// ---------------------------------------------------------------------------------

fn to_dto_of(o: OnFailure) -> ostro_api::phase11::OnFailurePolicy {
    ostro_api::phase11::OnFailurePolicy {
        workflow: o.workflow,
        queue: o.queue,
        include_input: o.include_input,
    }
}

fn from_dto_of(o: &ostro_api::phase11::OnFailurePolicy) -> OnFailure {
    OnFailure {
        workflow: o.workflow.clone(),
        queue: o.queue.clone().filter(|q| !q.is_empty()),
        include_input: o.include_input,
    }
}

fn workflow_exists(ctx: &P11Ctx, key: &str) -> bool {
    ctx.cc
        .meta
        .view()
        .read(|s| s.workflows().any(|w| w.value.name == key))
}

/// `GET /v1/{workflows,queues}/{name}/failure-policy` (empty when never set).
pub async fn get_failure_policy(
    ctx: &P11Ctx,
    scope: PolicyScope,
) -> Result<FailurePolicy, ApiError> {
    let view = ctx.cc.meta.view();
    match scope {
        PolicyScope::Workflow(name) => {
            let key = policy_key("workflow", &name)?;
            if !workflow_exists(ctx, &key) {
                return Err(ApiError::not_found(format!("workflow {key} not found")));
            }
            Ok(view.read(|s| match s.workflow_policy(&key) {
                Some(v) => FailurePolicy {
                    on_failure: onfailure::on_failure_of_ext(&v.value.ext).map(to_dto_of),
                    dlq: None,
                    version_tag: Some(v.version),
                },
                None => FailurePolicy::default(),
            }))
        }
        PolicyScope::Queue(name) => {
            let key = policy_key("queue", &name)?;
            Ok(view.read(|s| match s.queue_policy(&key) {
                Some(v) => FailurePolicy {
                    on_failure: onfailure::on_failure_of_ext(&v.value.ext).map(to_dto_of),
                    dlq: onfailure::dlq_policy_of_ext(&v.value.ext).map(|d| {
                        ostro_api::phase11::DlqPolicyDto {
                            enabled: d.enabled,
                            hook_catchup_ms: d.hook_catchup_ms,
                        }
                    }),
                    version_tag: Some(v.version),
                },
                None => FailurePolicy::default(),
            }))
        }
    }
}

/// `PUT /v1/{workflows,queues}/{name}/failure-policy`: replaces the `on_failure` hook (and,
/// for queues, the DLQ policy) in ext tags 144/145 by read-modify-CAS. Every RunPolicy /
/// Phase 7 field and every other ext tag is kept; a missing record is created with
/// defaults. `version_tag`, when given, must equal the record's version (0 = absent).
pub async fn set_failure_policy(
    ctx: &P11Ctx,
    scope: PolicyScope,
    p: FailurePolicy,
) -> Result<FailurePolicy, ApiError> {
    if !p11_feature_enabled(P11Feature::FailurePolicyExt) {
        return Err(ApiError::unavailable(
            "failure policies are not enabled on this cluster yet",
        ));
    }
    let hook = p.on_failure.as_ref().map(from_dto_of);
    let dlq = p.dlq.as_ref().map(|d| DlqPolicy {
        enabled: d.enabled,
        hook_catchup_ms: d.hook_catchup_ms,
    });
    let (key, what) = match &scope {
        PolicyScope::Workflow(n) => {
            let key = policy_key("workflow", n)?;
            if !workflow_exists(ctx, &key) {
                return Err(ApiError::not_found(format!("workflow {key} not found")));
            }
            (key, "workflow failure policy")
        }
        PolicyScope::Queue(n) => (policy_key("queue", n)?, "queue failure policy"),
    };
    let is_wf = matches!(scope, PolicyScope::Workflow(_));
    for _ in 0..POLICY_RETRIES {
        let (cur_version, record) = ctx
            .cc
            .meta
            .view()
            .read(|s| {
                if is_wf {
                    let cur = s.workflow_policy(&key);
                    let v = cur.map(|c| c.version);
                    let mut r = cur
                        .map(|c| c.value.clone())
                        .unwrap_or_else(|| WorkflowPolicy {
                            workflow: key.clone(),
                            ..Default::default()
                        });
                    let ext = onfailure::put_on_failure(&r.ext, hook.as_ref());
                    ext.map(|e| {
                        r.ext = e;
                        (v, MetaRecord::WorkflowPolicy(r))
                    })
                } else {
                    let cur = s.queue_policy(&key);
                    let v = cur.map(|c| c.version);
                    let mut r = cur.map(|c| c.value.clone()).unwrap_or_else(|| QueuePolicy {
                        queue: key.clone(),
                        ..Default::default()
                    });
                    let ext = onfailure::put_on_failure(&r.ext, hook.as_ref())
                        .and_then(|e| onfailure::put_dlq_policy(&e, dlq.as_ref()));
                    ext.map(|e| {
                        r.ext = e;
                        (v, MetaRecord::QueuePolicy(r))
                    })
                }
            })
            .map_err(|e| ApiError::internal(format!("stored policy ext is malformed: {e}")))?;
        if let Some(want) = p.version_tag
            && want != cur_version.unwrap_or(0)
        {
            return Err(version_mismatch(what, &key, cur_version.unwrap_or(0)));
        }
        let mkey = if is_wf {
            MetaKey::WorkflowPolicy(key.clone())
        } else {
            MetaKey::QueuePolicy(key.clone())
        };
        let res = ctx
            .cc
            .meta
            .propose(ostro_meta::MetaCommand::Put {
                key: mkey,
                expected_version: cur_version,
                record,
            })
            .await
            .map_err(|e| ApiError::unavailable(format!("metadata unavailable: {e}")))?;
        match res {
            ostro_meta::MetaResult::Ok { .. } => return get_failure_policy(ctx, scope).await,
            ostro_meta::MetaResult::StaleVersion { .. }
            | ostro_meta::MetaResult::Conflict { .. }
            | ostro_meta::MetaResult::NotFound => {
                if p.version_tag.is_some() {
                    let now = ctx.cc.meta.view().read(|s| {
                        if is_wf {
                            s.workflow_policy(&key).map(|v| v.version)
                        } else {
                            s.queue_policy(&key).map(|v| v.version)
                        }
                    });
                    return Err(version_mismatch(what, &key, now.unwrap_or(0)));
                }
            }
            ostro_meta::MetaResult::Invalid(msg) => {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(status: RunStatus, completed: Option<u64>) -> RunRecord {
        RunRecord {
            status,
            completed_at_ms: completed,
            ..Default::default()
        }
    }

    #[test]
    fn marks_count_only_for_the_current_episode() {
        let r = rec(RunStatus::Failed, Some(10));
        let row = DlqRow {
            dismissed_at_ms: Some(5),
            episode_completed_at_ms: 9,
            ..Default::default()
        };
        assert!(current_marks(Some(row.clone()), &r).is_none(), "stale");
        let cur = current_marks(
            Some(DlqRow {
                episode_completed_at_ms: 10,
                ..row
            }),
            &r,
        );
        assert!(cur.is_some());
        assert!(!member(&r, cur.as_ref(), true, false), "dismissed");
        assert!(member(&r, cur.as_ref(), true, true), "include_dismissed");
        assert!(member(&r, None, true, false));
        assert!(!member(&r, None, false, false), "queue disabled");
        assert!(!member(
            &rec(RunStatus::Completed, Some(1)),
            None,
            true,
            true
        ));
        assert!(member(
            &rec(RunStatus::TimedOut, Some(1)),
            None,
            true,
            false
        ));
        let requeued = DlqRow {
            requeued_to: Some([1; 18]),
            episode_completed_at_ms: 10,
            ..Default::default()
        };
        assert!(!member(&r, Some(&requeued), true, false));
    }

    #[test]
    fn reasons_from_failure_json() {
        let j = crate::derive::failure_json("nondeterminism", "x", "seq=3 mismatch");
        assert_eq!(
            reason_from_failure_json(&j).as_deref(),
            Some("nondeterminism: seq=3 mismatch")
        );
        let j = crate::derive::failure_json("workflow_failed", "asked", "s3 fails");
        assert_eq!(
            reason_from_failure_json(&j).as_deref(),
            Some("workflow_failed: s3 fails")
        );
        assert_eq!(reason_from_failure_json(b"nope"), None);
        // 11-18: a message that already carries the code is not prefixed twice.
        let j = crate::derive::failure_json(
            "limit_exceeded",
            "history_events",
            "limit_exceeded: history_events 21 > 20",
        );
        assert_eq!(
            reason_from_failure_json(&j).as_deref(),
            Some("limit_exceeded: history_events 21 > 20")
        );
        let j = crate::derive::failure_json("limit_exceeded", "x", "limit_exceeded_other");
        assert_eq!(
            reason_from_failure_json(&j).as_deref(),
            Some("limit_exceeded: limit_exceeded_other")
        );
    }

    #[test]
    fn resume_positions() {
        let run = RunId::new(ostro_types::PartitionId(1), uuid::Uuid::now_v7());
        let p = pos(&run, 42);
        let pre = [0u8; 2];
        let a = resume_after(&pre, 1, Some(&p)).expect("after");
        assert_eq!(&a[2..], &suffix(&p));
        let b = resume_after(&pre, 2, Some(&p)).expect("before");
        assert!(b < a);
        assert_eq!(resume_after(&pre, 0, None), None);
    }
}
