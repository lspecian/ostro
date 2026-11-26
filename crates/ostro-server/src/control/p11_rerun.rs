//! Rerun API (Phase 11, OPS-02, D-P11-3/4/5/7/8/27): `POST /v1/runs/{id}/rerun` and
//! `GET /v1/runs/{id}/rerun-cuts`. Declared by 11-04 (D-P11-23), implemented by 11-10.
//! Called by [`crate::p11::P11Controllers`] and by the DLQ requeue `rerun` mode
//! ([`super::p11_dlq`]).
//!
//! # `rerun_run`
//!
//! 1. **Source.** The run record must exist (404) and be terminal (409
//!    `run_not_terminal`, D-P11-8: "terminate it first"). Its whole journal is read
//!    (`journal_io::read_journal`, blobs resolved).
//! 2. **Cut.** `from.seq`, or `from.step{name, occurrence}` resolved with
//!    [`crate::rerun::resolve_step`] (the seq of that op's first event; unknown step 400).
//!    [`crate::rerun::analyze_cut`] classifies it: unsafe → 409 `rerun_cut_unsafe{seq,
//!    detail, nearest_safe_seq}` (`detail` is the analyzer reason, e.g. `open_effect` or
//!    `terminal_or_out_of_range`). A prefix longer than `[limits] max_history_events` is
//!    413 `rerun_prefix_too_large`.
//! 3. **Target version.** `active` (default; 412 without one), `source` (the source's
//!    version and its exact pinned module) or `N` (404 when unknown). The module is
//!    prepared through the executor's runtime cache (`Runtime::prepared` /
//!    `prepare_cached` over the node blob store, as the executor does). A retired target
//!    module is 409 `module_retired` (13-18, as a start).
//! 4. **Run id.** With an idempotency key the dedupe key is `__rerun/<source>/<key>` and
//!    the partition `partition_for_key(workflow, dedupe)` (as starts route keyed runs); when
//!    the dedupe row already exists, the existing run id is used, else a fresh uuid v7 on
//!    that partition. Without a key: a fresh uuid v7 on a partition this node leads
//!    (13-18, `crate::rerun::restart_partition`: the StartRun is built on local handles).
//! 5. **Mandatory replay check** (D-P11-5): [`ostro_engine::replay_check::replay_check`]
//!    replays events `[0, cut)` against the target module **as the new run id**.
//!    `ok`/`ok_frontier` pass; `diverged{seq}` → 409 `rerun_nondeterministic{seq, detail}`;
//!    `error` → 500. A dry run stops here and answers the plan (`new_run_id: null`).
//!    An existing run for the key answers `existing: true` without submitting.
//! 6. **Envelope** (D-P11-7): seq 0 is the source row verbatim; events `1..cut` are the
//!    stored rows unchanged (`seq, kind, payload, attempt`). While the encoded command is
//!    over [`MAX_RERUN_PREFIX_BYTES`] (3.5 MiB), copied inline payloads over 1 KiB are
//!    written to the node blob store, largest first, and referenced instead (the bytes
//!    resolve identically). Still over → 413 `rerun_prefix_too_large`.
//! 7. **Submit.** StartRun with the new id, the source workflow / queue / tenant /
//!    priority / concurrency key / traceparent / run timeout, retention and
//!    schedule-to-start from the current policy (as a start without overrides), `rng_seed` and
//!    `input` PayloadRef of the source, user tags (older `__ostro/rerun_from=` and
//!    `__ostro/on_failure_of=` dropped) plus `__ostro/rerun_from=<cut>` (more than
//!    [`MAX_RUN_TAGS`] → 400 `limit: tags`), `retry_of` = source, no parent (reruns are
//!    detached), the target `module_sha` / `workflow_version`, fields 40/41, and the
//!    dedupe key with `ReturnExisting`. Emitted only when
//!    [`p11_feature_enabled`]`(RerunPrefix)` (else 503 `feature_gated`).
//!
//! Effects inside the prefix never re-run (their completions are copied); effects after
//! the cut really re-execute under the new run id. Shared blobs (input, copied refs) stay
//! alive while either run references them: blob GC is reachability-based (D-P11-27).
//!
//! Every timestamp produced here (`requested_at_ms`, `ready_at_ms`) comes from the
//! injected clock (`ControlCtx::now_ms`); the copied events keep their own `ts`.
//!
//! Metric: `ostro_rerun_total{result}` with `result` in `created | existing | dry_run |
//! not_terminal | cut_unsafe | nondeterministic | too_large | rejected | error`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use ostro_api::phase11::{
    RerunCut, RerunCutsView, RerunFrom, RerunRequest, RerunResponse, RerunVersion, limit_error,
    reason, reason_error, rerun_cut_unsafe,
};
use ostro_api::{ApiError, ErrorCode};
use ostro_engine::replay_check::{ReplayVerdict, replay_check};
use ostro_engine::runtime::Prepared;
use ostro_engine::sink::RunRef;
use ostro_log::PartitionLog;
use ostro_partition::store::Table;
use ostro_partition::store::keys::{dedupe_key, event_key};
use ostro_partition::store::values::RunRecord;
use ostro_partition::{PartitionHandle, query};
use ostro_types::command::{
    self as cmd, Command, JournalEvent, MAX_RUN_TAGS, ON_FAILURE_OF_TAG, RERUN_FROM_TAG, StartRun,
    encode_command,
};
use ostro_types::journal::Event;
use ostro_types::routing::partition_for_key;
use ostro_types::{PartitionId, PayloadRef, RunId};

use super::history::event_seq_and_attempt;
use super::runs::{not_found, parse_run_id, partition_error, status, store_error, task_error};
use crate::journal_io;
use crate::p11::{P11Ctx, P11Feature, p11_feature_enabled};
use crate::rerun::{CutUnsafe, analyze_cut, resolve_step, step_cuts};
use crate::retention::{RunPolicy, resolve_run_policy};

/// The encoded rerun StartRun bound (D-P11-7): 3.5 MiB, under the 4 MiB envelope.
pub const MAX_RERUN_PREFIX_BYTES: usize = 3 * 1024 * 1024 + 512 * 1024;
/// Copied inline payloads larger than this may be offloaded to blobs to fit the bound.
pub const OFFLOAD_MIN_BYTES: usize = 1024;
/// Dedupe namespace of keyed reruns (RESERVATIONS §11).
pub const RERUN_DEDUPE_PREFIX: &str = "__rerun/";
/// Lower bound on what replacing an inline payload of `n` bytes with a blob ref saves in
/// the encoded command (a blob ref encodes in at most ~47 bytes, an inline payload in at
/// least `n + 2`).
const OFFLOAD_OVERHEAD: usize = 64;

/// Test-only per-source overrides of [`MAX_RERUN_PREFIX_BYTES`].
fn bound_overrides() -> &'static Mutex<HashMap<String, usize>> {
    static M: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Test hook: lower (or restore, with `None`) the envelope bound for reruns of
/// `source_run_id` only, so a test can exercise offloading and the 413 path without
/// multi-megabyte sources. Never called by the server.
#[doc(hidden)]
pub fn set_prefix_bound_for_source(source_run_id: &str, bound: Option<usize>) {
    let mut m = bound_overrides().lock().unwrap_or_else(|e| e.into_inner());
    match bound {
        Some(b) => m.insert(source_run_id.to_owned(), b),
        None => m.remove(source_run_id),
    };
}

fn prefix_bound(source: &RunId) -> usize {
    bound_overrides()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&source.to_string())
        .copied()
        .unwrap_or(MAX_RERUN_PREFIX_BYTES)
}

fn count(result: &'static str) {
    metrics::counter!("ostro_rerun_total", "result" => result).increment(1);
}

/// The metrics label of a refusal.
fn error_label(e: &ApiError) -> &'static str {
    let r = e
        .details
        .as_ref()
        .and_then(|d| d.get("reason"))
        .and_then(|r| r.as_str());
    match r {
        Some(reason::RUN_NOT_TERMINAL) => "not_terminal",
        Some(reason::RERUN_CUT_UNSAFE) => "cut_unsafe",
        Some(reason::RERUN_NONDETERMINISTIC) => "nondeterministic",
        Some(reason::RERUN_PREFIX_TOO_LARGE) => "too_large",
        _ if e.code == ErrorCode::Internal || e.code == ErrorCode::Unavailable => "error",
        _ => "rejected",
    }
}

/// Rerun a terminal run from a cut (see the module docs).
pub async fn rerun_run(ctx: &P11Ctx, req: RerunRequest) -> Result<RerunResponse, ApiError> {
    let r = rerun(ctx, req).await;
    match &r {
        Ok(res) if res.existing => count("existing"),
        Ok(res) if res.new_run_id.is_none() => count("dry_run"),
        Ok(_) => count("created"),
        Err(e) => count(error_label(e)),
    }
    r
}

/// The source as loaded for a rerun.
struct Source {
    run: RunId,
    rec: RunRecord,
    journal: Vec<Event>,
}

async fn load_source(ctx: &P11Ctx, run_id: &str) -> Result<Source, ApiError> {
    let cp = &ctx.cc.partitions;
    let run = parse_run_id(cp, run_id)?;
    let handle = &cp.partition(run.partition())?;
    let reader = handle.reader();
    let rec = tokio::task::spawn_blocking(move || query::run_record(&reader, &run))
        .await
        .map_err(task_error)?
        .map_err(store_error)?
        .ok_or_else(|| not_found(run_id))?;
    let journal = journal_io::read_journal(handle, &run)
        .await
        .map_err(|e| ApiError::internal(format!("rerun source journal: {e}")))?;
    Ok(Source { run, rec, journal })
}

fn unsafe_error(u: CutUnsafe) -> ApiError {
    rerun_cut_unsafe(u.seq, u.reason, u.nearest_safe_seq)
}

fn too_large(msg: String, limit: &str, max: u64, actual: u64) -> ApiError {
    reason_error(
        ErrorCode::PayloadTooLarge,
        reason::RERUN_PREFIX_TOO_LARGE,
        msg,
        serde_json::json!({ "limit": limit, "max": max, "actual": actual }),
    )
}

/// The target `(version, module sha, run_timeout_ms)` of the rerun.
fn target_version(
    ctx: &P11Ctx,
    rec: &RunRecord,
    v: &RerunVersion,
) -> Result<(u32, [u8; 32]), ApiError> {
    let name = &rec.workflow;
    match v {
        RerunVersion::Source => {
            let sha = rec.module_sha.ok_or_else(|| {
                ApiError::precondition_failed(format!(
                    "the source run of {name} has no pinned module"
                ))
            })?;
            Ok((rec.workflow_version, sha))
        }
        RerunVersion::Version(n) => ctx.cc.meta.view().read(|s| {
            s.workflow(name, *n)
                .map(|w| (w.value.version, w.value.module_sha256))
                .ok_or_else(|| {
                    ApiError::not_found(format!("workflow {name} version {n} not found"))
                })
        }),
        RerunVersion::Active => ctx.cc.meta.view().read(|s| {
            let a = s.active(name).ok_or_else(|| {
                ApiError::precondition_failed(format!("workflow {name} has no active version"))
            })?;
            s.workflow(name, a.value.version)
                .map(|w| (w.value.version, w.value.module_sha256))
                .ok_or_else(|| {
                    ApiError::internal(format!(
                        "active version {} of {name} is not registered",
                        a.value.version
                    ))
                })
        }),
    }
}

/// The prepared module `sha` through the executor's runtime cache.
async fn prepare(ctx: &P11Ctx, sha: [u8; 32]) -> Result<Arc<Prepared>, ApiError> {
    if let Some(p) = ctx.runtime.prepared(&sha) {
        return Ok(p);
    }
    let blobs = ctx
        .cc
        .blobs
        .clone()
        .ok_or_else(|| ApiError::unavailable("no blob store (no partition started)"))?;
    let wasm = tokio::task::spawn_blocking(move || {
        let probe = ostro_blob::BlobRef {
            sha256: sha,
            len: 0,
        };
        let len = std::fs::metadata(blobs.path_of(&probe))
            .map_err(|e| format!("module {}: {e}", ostro_blob::hex(&sha)))?
            .len();
        blobs
            .get(&ostro_blob::BlobRef { sha256: sha, len })
            .map_err(|e| format!("module {}: {e}", ostro_blob::hex(&sha)))
    })
    .await
    .map_err(task_error)?
    .map_err(ApiError::internal)?;
    ctx.runtime
        .prepare_cached(&wasm)
        .await
        .map_err(|e| ApiError::internal(format!("module {}: {e}", ostro_blob::hex(&sha))))
}

/// Stored rows `0..cut` of `run` as `JournalEvent`s, verbatim (attempt included).
async fn source_rows(
    handle: &PartitionHandle<PartitionLog>,
    run: RunId,
    cut: u64,
) -> Result<Vec<JournalEvent>, ApiError> {
    let limit = usize::try_from(cut).unwrap_or(usize::MAX);
    let reader = handle.reader();
    let (rows, attempts) = tokio::task::spawn_blocking(move || {
        let rows = query::journal_rows(&reader, &run, 0, limit)?;
        let from = event_key(&run, 0);
        let mut to = run.as_bytes().to_vec();
        to.extend_from_slice(&[0xff; 9]);
        let attempts: HashMap<u64, u64> = reader
            .range(Table::Events, &from, &to, limit)?
            .iter()
            .filter_map(|(_, v)| event_seq_and_attempt(v))
            .collect();
        Ok::<_, ostro_partition::StoreError>((rows, attempts))
    })
    .await
    .map_err(task_error)?
    .map_err(store_error)?;
    if rows.len() != limit || rows.iter().enumerate().any(|(i, r)| r.0 != i as u64) {
        return Err(ApiError::internal(format!(
            "run {run}: journal rows 0..{cut} are not contiguous"
        )));
    }
    Ok(rows
        .into_iter()
        .map(|(seq, kind, payload)| JournalEvent {
            seq,
            kind,
            payload: Some(payload),
            attempt: attempts.get(&seq).copied().unwrap_or(0),
        })
        .collect())
}

/// The id of the run already deduped under `dedupe` on `handle`, if any.
pub(crate) async fn existing_run(
    handle: &PartitionHandle<PartitionLog>,
    workflow: &str,
    dedupe: &str,
) -> Result<Option<RunId>, ApiError> {
    let reader = handle.reader();
    let dk = dedupe_key(workflow, dedupe.as_bytes());
    let row = tokio::task::spawn_blocking(move || reader.get(Table::Dedupe, &dk))
        .await
        .map_err(task_error)?
        .map_err(store_error)?;
    row.map(|v| RunId::from_bytes(&v).map_err(|e| ApiError::internal(format!("dedupe row: {e}"))))
        .transpose()
}

/// Copied user tags plus the lineage tag (D-P11-6).
fn rerun_tags(rec: &RunRecord, cut: u64) -> Result<Vec<String>, ApiError> {
    let mut tags: Vec<String> = rec
        .tags
        .iter()
        .filter(|t| !t.starts_with(RERUN_FROM_TAG) && !t.starts_with(ON_FAILURE_OF_TAG))
        .cloned()
        .collect();
    tags.push(format!("{RERUN_FROM_TAG}{cut}"));
    if tags.len() > MAX_RUN_TAGS {
        let n = |v: usize| u64::try_from(v).unwrap_or(u64::MAX);
        return Err(limit_error(
            ErrorCode::InvalidArgument,
            "tags",
            n(MAX_RUN_TAGS),
            n(tags.len()),
        ));
    }
    Ok(tags)
}

fn encoded_len(start: &StartRun) -> usize {
    encode_command(&Command::start_run(start.clone())).len()
}

/// Offload copied inline payloads over [`OFFLOAD_MIN_BYTES`] (largest first) until the
/// encoded StartRun fits `bound` (D-P11-7). Returns the encoded size and the number of
/// payloads offloaded.
pub(crate) async fn fit_envelope(
    handle: &PartitionHandle<PartitionLog>,
    start: &mut StartRun,
    bound: usize,
) -> Result<(usize, usize), ApiError> {
    let mut size = encoded_len(start);
    if size <= bound {
        return Ok((size, 0));
    }
    let mut candidates: Vec<(usize, usize)> = start
        .rerun_prefix
        .iter()
        .enumerate()
        .filter_map(|(i, e)| {
            e.payload
                .as_ref()
                .and_then(PayloadRef::inline_len)
                .filter(|&n| n > OFFLOAD_MIN_BYTES)
                .map(|n| (n, i))
        })
        .collect();
    candidates.sort_unstable_by(|a, b| b.cmp(a));
    let mut estimate = size;
    let mut offloaded = 0;
    for (n, i) in candidates {
        let Some(payload) = start.rerun_prefix[i].payload.as_ref() else {
            continue;
        };
        let bytes = handle
            .fetch_payload(payload)
            .await
            .map_err(partition_error)?;
        let blobs = Arc::clone(handle.blobs());
        let r = tokio::task::spawn_blocking(move || blobs.put(&bytes))
            .await
            .map_err(task_error)?
            .map_err(|e| ApiError::internal(format!("rerun prefix blob: {e}")))?;
        start.rerun_prefix[i].payload = Some(PayloadRef::blob(r.sha256, r.len));
        offloaded += 1;
        estimate = estimate.saturating_sub(n.saturating_sub(OFFLOAD_OVERHEAD));
        if estimate <= bound {
            size = encoded_len(start);
            if size <= bound {
                return Ok((size, offloaded));
            }
            estimate = size;
        }
    }
    size = encoded_len(start);
    if size <= bound {
        return Ok((size, offloaded));
    }
    Err(too_large(
        format!(
            "the rerun prefix encodes to {size} bytes after offloading {offloaded} payload(s); \
             the limit is {bound}"
        ),
        "rerun_prefix_bytes",
        u64::try_from(bound).unwrap_or(u64::MAX),
        u64::try_from(size).unwrap_or(u64::MAX),
    ))
}

async fn rerun(ctx: &P11Ctx, req: RerunRequest) -> Result<RerunResponse, ApiError> {
    let cp = &ctx.cc.partitions;
    let src = load_source(ctx, &req.run_id).await?;
    if !src.rec.status.is_terminal() {
        let st = status(src.rec.status);
        return Err(reason_error(
            ErrorCode::Conflict,
            reason::RUN_NOT_TERMINAL,
            format!(
                "run {} is {}; only terminal runs can be rerun (terminate it first)",
                req.run_id,
                st.as_str()
            ),
            serde_json::json!({ "status": st.as_str() }),
        ));
    }

    // 2. Cut.
    let cut = match &req.from {
        RerunFrom::Seq(s) => *s,
        RerunFrom::Step { name, occurrence } => {
            let occurrence = (*occurrence).max(1);
            resolve_step(&src.journal, name, occurrence).ok_or_else(|| {
                ApiError::invalid_argument(format!(
                    "run {} has no occurrence {occurrence} of step {name:?}",
                    req.run_id
                ))
                .with_details(serde_json::json!({ "field": "from" }))
            })?
        }
    };
    analyze_cut(&src.journal, cut).map_err(unsafe_error)?;
    let cut_us = usize::try_from(cut).unwrap_or(usize::MAX);
    if cut > ctx.limits.max_history_events {
        return Err(too_large(
            format!(
                "the rerun prefix has {cut} events; [limits] max_history_events is {}",
                ctx.limits.max_history_events
            ),
            "history_events",
            ctx.limits.max_history_events,
            cut,
        ));
    }

    let tags = rerun_tags(&src.rec, cut)?;

    // 3. Target version and module.
    let (version, module_sha) = target_version(ctx, &src.rec, &req.version)?;
    // Phase 13 (13-18): a retired target module takes no new run (as a start).
    crate::rerun::check_restart_module(ctx.cc.meta.view(), Some(&module_sha))?;
    // Phase 14 (14-01b, B1a): every rerun target (source pin, explicit or active version)
    // passes the signature gate (14-19).
    if let Some(p14) = crate::p14::ctx_of(&ctx.cc.partitions)? {
        crate::p14::version_gate(
            &p14,
            src.rec.tenant.as_deref(),
            &src.rec.workflow,
            version,
            module_sha,
        )
        .await?;
    }
    let module = prepare(ctx, module_sha).await?;

    // 4. The new run id (pre-allocated: the replay check runs as this id).
    let workflow = src.rec.workflow.clone();
    let count_p = cp.partition_count();
    let dedupe = req
        .idempotency_key
        .as_deref()
        .filter(|k| !k.is_empty())
        .map(|k| format!("{RERUN_DEDUPE_PREFIX}{}/{k}", src.run));
    let uuid = uuid::Uuid::now_v7();
    let p: PartitionId = match &dedupe {
        Some(d) => partition_for_key(&workflow, d.as_bytes(), count_p),
        // Phase 13 (13-18): a partition this node leads (`crate::rerun::restart_partition`).
        None => crate::rerun::restart_partition(cp, &uuid),
    };
    // p13-follow: the dedupe partition may be led by another node; the lookup and the
    // submit go to its leader (`p13::keyed_start`). An unkeyed rerun's partition is led here.
    let existing = match &dedupe {
        Some(d) => crate::p13::keyed_start::lookup(cp, p, &workflow, d).await?,
        None => None,
    };
    let new_run = existing.unwrap_or_else(|| RunId::new(p, uuid));

    // 5. Mandatory replay check of [0, cut) as the new run.
    let run_ref = RunRef {
        partition: new_run.partition().0,
        run_id: new_run.to_string(),
    };
    let verdict = match replay_check(&ctx.runtime, &module, run_ref, &src.journal[..cut_us]).await {
        ReplayVerdict::Ok { .. } => "ok",
        ReplayVerdict::OkFrontier { .. } => "ok_frontier",
        ReplayVerdict::Diverged { seq, detail } => {
            return Err(reason_error(
                ErrorCode::Conflict,
                reason::RERUN_NONDETERMINISTIC,
                format!(
                    "the copied prefix does not replay on {workflow} version {version} as run \
                     {new_run}: seq {seq}: {detail}"
                ),
                serde_json::json!({ "seq": seq, "detail": detail }),
            ));
        }
        ReplayVerdict::Error(e) => {
            return Err(ApiError::internal(format!("rerun replay check: {e}")));
        }
    };
    let response = |new_run_id: Option<String>, existing: bool| RerunResponse {
        new_run_id,
        source_run_id: req.run_id.clone(),
        cut_seq: cut,
        copied_events: cut,
        workflow: workflow.clone(),
        version,
        verdict: verdict.to_owned(),
        existing,
    };
    if req.dry_run {
        return Ok(response(None, false));
    }
    if let Some(e) = existing {
        return Ok(response(Some(e.to_string()), true));
    }
    if !p11_feature_enabled(P11Feature::RerunPrefix) {
        return Err(reason_error(
            ErrorCode::Unavailable,
            "feature_gated",
            "rerun prefixes are not enabled on this cluster yet",
            serde_json::json!({}),
        ));
    }

    // 6. The StartRun, fitted into the envelope.
    let src_handle = &cp.partition(src.run.partition())?;
    let mut rows = source_rows(src_handle, src.run, cut).await?;
    let run_started = rows.remove(0);
    let src_run = src.run;
    let reader = src_handle.reader();
    let input = tokio::task::spawn_blocking(move || {
        query::run_payload(&reader, &src_run, query::PAYLOAD_INPUT)
    })
    .await
    .map_err(task_error)?
    .map_err(store_error)?
    .unwrap_or_else(|| PayloadRef::inline(Bytes::from_static(b"null")));
    let now = ctx.cc.now_ms();
    let rec = src.rec;
    // Retention and schedule-to-start follow the current policy (workflow, queue, then
    // `[runs]`), as for a start without overrides: the rerun is a new run, and a short
    // per-request retention on the source must not expire the rerun with it.
    let (retention_ms, schedule_to_start_ms) = resolve_run_policy(
        ctx.cc.meta.view(),
        &ctx.cfg.runs,
        &workflow,
        &rec.queue,
        RunPolicy::default(),
    );
    let start = StartRun {
        run_id: new_run.as_bytes().to_vec(),
        workflow: workflow.clone(),
        workflow_version: version,
        queue: rec.queue,
        input: Some(input),
        idempotency_key: dedupe.clone().map(String::into_bytes).unwrap_or_default(),
        conflict_policy: cmd::ConflictPolicy::ReturnExisting as i32,
        concurrency_key: rec.concurrency_key,
        priority: rec.priority,
        ready_at_ms: now,
        requested_at_ms: now,
        // D-P11-8: reruns are detached.
        parent: None,
        rng_seed: rec.rng_seed,
        module_sha: module_sha.to_vec(),
        tags,
        tenant: rec.tenant.unwrap_or_default(),
        traceparent: rec.traceparent.unwrap_or_default(),
        run_timeout_ms: rec.run_timeout_ms.unwrap_or(0),
        schedule_to_start_ms,
        retention_ms,
        retry_of: src.run.as_bytes().to_vec(),
        continued_from: Vec::new(),
        run_started: Some(run_started),
        rerun_prefix: rows,
        rerun_from_seq: cut,
    };
    // 7. Fit the envelope, admit through the rate limit and submit, on the target
    // partition's leader (`p13::keyed_start::submit_local` there).
    let ks = crate::p13::keyed_start::KeyedStart {
        start,
        rate_limit: true,
        prefix_bound: Some(prefix_bound(&src.run)),
    };
    let started = crate::p13::keyed_start::submit(cp, src_handle, ks).await?;
    tracing::info!(source = %src.run, run = %new_run, cut, verdict, "rerun submitted");
    match started {
        crate::p13::keyed_start::Started::Run { run, existing } => {
            Ok(response(Some(run.to_string()), existing))
        }
        crate::p13::keyed_start::Started::Rejected(r) => {
            Err(ApiError::internal(format!("rerun rejected: {r}")))
        }
    }
}

/// Per-step safe/unsafe cuts of a run (`GET /v1/runs/{id}/rerun-cuts`).
pub async fn analyze_rerun(ctx: &P11Ctx, run_id: String) -> Result<RerunCutsView, ApiError> {
    let src = load_source(ctx, &run_id).await?;
    let cuts = step_cuts(&src.journal)
        .into_iter()
        .map(|c| {
            let (safe, reason, nearest_safe_seq) = match c.safe {
                Ok(_) => (true, None, None),
                Err(u) => (false, Some(u.reason.to_owned()), u.nearest_safe_seq),
            };
            RerunCut {
                seq: c.seq,
                step: c.step,
                occurrence: c.occurrence,
                safe,
                reason,
                nearest_safe_seq,
            }
        })
        .collect();
    Ok(RerunCutsView { run_id, cuts })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bound_is_three_and_a_half_mib() {
        assert_eq!(MAX_RERUN_PREFIX_BYTES, 3_670_016);
        let r = RunId::new_v7(PartitionId(0));
        assert_eq!(prefix_bound(&r), MAX_RERUN_PREFIX_BYTES);
        set_prefix_bound_for_source(&r.to_string(), Some(10));
        assert_eq!(prefix_bound(&r), 10);
        set_prefix_bound_for_source(&r.to_string(), None);
        assert_eq!(prefix_bound(&r), MAX_RERUN_PREFIX_BYTES);
    }

    #[test]
    fn tags_drop_old_lineage_and_cap_at_64() {
        let mut rec = RunRecord {
            tags: vec![
                "a".into(),
                format!("{RERUN_FROM_TAG}3"),
                format!("{ON_FAILURE_OF_TAG}x"),
            ],
            ..RunRecord::default()
        };
        assert_eq!(
            rerun_tags(&rec, 7).expect("tags"),
            vec!["a".to_owned(), format!("{RERUN_FROM_TAG}7")]
        );
        rec.tags = (0..64).map(|i| format!("t{i}")).collect();
        let e = rerun_tags(&rec, 7).expect_err("65 tags");
        assert_eq!(e.code, ErrorCode::InvalidArgument);
        let d = e.details.expect("details");
        assert_eq!(d["limit"], "tags");
        assert_eq!(d["actual"], 65);
    }

    #[test]
    fn refusals_have_bounded_labels() {
        assert_eq!(
            error_label(&unsafe_error(CutUnsafe {
                seq: 3,
                reason: crate::rerun::REASON_OPEN_EFFECT,
                nearest_safe_seq: Some(2),
            })),
            "cut_unsafe"
        );
        assert_eq!(
            error_label(&too_large(String::new(), "x", 1, 2)),
            "too_large"
        );
        assert_eq!(error_label(&ApiError::internal("x")), "error");
        assert_eq!(error_label(&ApiError::not_found("x")), "rejected");
    }
}
