//! Server-side replay (CLI-07, D-P10-7): `POST /v1/modules/{sha}/replay`, delegated
//! from [`ServerControlPlane`] (10-05 contract, implemented by 10-11).
//!
//! 1. **Module.** The sha must name an uploaded module (404 `not_found` otherwise). It is
//!    prepared **once per request** through the executor's runtime cache
//!    (`Runtime::prepared`, else the node blob store + `Runtime::prepare_cached`, so the
//!    AOT cache is reused) and described (`Runtime::describe`) for its workflow names.
//! 2. **Selection.**
//!    - `runs`: exactly those ids, in request order. A malformed id is 400, an unknown run
//!      404 `not_found` (the 10-05 contract: the whole request fails).
//!    - `sample{n, workflow, status}`: the newest `n` runs of each described workflow (or
//!      only `workflow`), any status unless `status` is set, through the run listing
//!      (`ControlPlane::list_runs`, newest first; 11-11 index or scan).
//! 3. **Check.** A run whose workflow the module does not describe is `skipped`
//!    ("workflow not exported by module") without reading its journal. Every other run's
//!    journal is read with [`journal_io::read_journal`] (blobs resolved) and checked with
//!    [`ostro_engine::replay_check::replay_check`] (11-10, unchanged) on the server
//!    runtime, which holds the cluster await-token key; at most [`REPLAY_CONCURRENCY`]
//!    runs are read and replayed at a time. The check's sink refuses every append past the
//!    journal end and performs no effect I/O, so nothing is executed or written.
//! 4. **Result.** One verdict per run in selection order, plus counts. Nothing from a
//!    journal, payload or key leaves the server except the verdict detail strings.
//!
//! `allow_missing` does not change the verdicts (a missing workflow is always `skipped`);
//! it is the caller's exit policy (`ostro replay --allow-missing`).
//!
//! One info line per request: counts and the prepare / total durations (`// clock-exempt:`
//! duration metrics only, never used for logic).

use std::collections::BTreeSet;
use std::sync::Arc;

use ostro_api::domain::{
    ListOrder, ListRunsRequest, MAX_PAGE_LIMIT, ReplayRequest, ReplayResult, ReplaySelect,
    ReplaySummary, ReplayVerdictInfo, ReplayVerdictKind, RunStatus,
};
use ostro_api::{ApiError, ControlPlane};
use ostro_engine::replay_check::{ReplayVerdict, replay_check};
use ostro_engine::runtime::{Prepared, Runtime};
use ostro_engine::sink::RunRef;
use ostro_log::Rf1Log;
use ostro_partition::{PartitionHandle, query};
use ostro_types::RunId;
use tokio::sync::Semaphore;

use super::ServerControlPlane;
use super::names::split_qualified;
use super::runs::{not_found, parse_run_id, store_error, task_error};
use crate::journal_io;

/// Runs read and replayed at the same time by one request.
pub const REPLAY_CONCURRENCY: usize = 2;

/// Detail of a `skipped` verdict.
pub const SKIPPED_DETAIL: &str = "workflow not exported by module";

/// Lowercase hex sha256 → bytes (the request is validated already; `None` = malformed).
fn parse_sha(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}

/// The prepared module `sha` through the executor's runtime cache (as `p11_rerun`).
async fn prepare(cp: &ServerControlPlane, sha: [u8; 32]) -> Result<Arc<Prepared>, ApiError> {
    if let Some(p) = cp.runtime.prepared(&sha) {
        return Ok(p);
    }
    let blobs = cp.blobs()?.clone();
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
    cp.runtime
        .prepare_cached(&wasm)
        .await
        .map_err(|e| ApiError::internal(format!("module {}: {e}", ostro_blob::hex(&sha))))
}

/// A selected run.
struct Selected {
    id: String,
    run: RunId,
    workflow: String,
}

/// The explicit runs, in request order (404 on the first unknown one).
async fn explicit(cp: &ServerControlPlane, ids: &[String]) -> Result<Vec<Selected>, ApiError> {
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        let run = parse_run_id(cp, id)?;
        let reader = cp.partition(run.partition())?.reader();
        let rec = tokio::task::spawn_blocking(move || query::run_record(&reader, &run))
            .await
            .map_err(task_error)?
            .map_err(store_error)?
            .ok_or_else(|| not_found(id))?;
        out.push(Selected {
            id: id.clone(),
            run,
            workflow: rec.workflow,
        });
    }
    Ok(out)
}

/// The newest `n` runs of `workflow` (optionally in `status`), newest first.
async fn newest(
    cp: &ServerControlPlane,
    workflow: &str,
    n: u32,
    status: Option<RunStatus>,
) -> Result<Vec<Selected>, ApiError> {
    let mut picked = Vec::new();
    let mut page_token = None;
    loop {
        let want = n.saturating_sub(picked.len() as u32);
        let page = cp
            .list_runs(ListRunsRequest {
                workflow: Some(workflow.to_owned()),
                status,
                limit: want.min(MAX_PAGE_LIMIT),
                page_token: page_token.take(),
                order: ListOrder::Newest,
                ..ListRunsRequest::default()
            })
            .await?;
        picked.extend(page.runs);
        match page.next_page_token {
            Some(t) if (picked.len() as u32) < n => page_token = Some(t),
            _ => break,
        }
    }
    // Newest first whatever path served the listing (the scan fallback is oldest first).
    picked.sort_by(|a, b| (b.created_at_ms, &b.run_id).cmp(&(a.created_at_ms, &a.run_id)));
    picked.truncate(n as usize);
    picked
        .into_iter()
        .map(|r| {
            let run: RunId = r
                .run_id
                .parse()
                .map_err(|e| ApiError::internal(format!("listed run id {}: {e}", r.run_id)))?;
            Ok(Selected {
                id: r.run_id,
                run,
                workflow: r.workflow,
            })
        })
        .collect()
}

/// A verdict row for `sel`.
fn verdict_of(
    sel: &Selected,
    verdict: ReplayVerdictKind,
    seq: Option<u64>,
    detail: Option<String>,
    appended: usize,
) -> ReplayVerdictInfo {
    ReplayVerdictInfo {
        run_id: sel.id.clone(),
        workflow: sel.workflow.clone(),
        verdict,
        seq,
        detail,
        appended: appended as u64,
    }
}

/// One run's verdict (a task of the replay pool).
async fn check_one(
    runtime: Arc<Runtime>,
    module: Arc<Prepared>,
    handle: PartitionHandle<Rf1Log>,
    sel: &Selected,
) -> ReplayVerdictInfo {
    let journal = match journal_io::read_journal(&handle, &sel.run).await {
        Ok(j) => j,
        Err(e) => {
            return verdict_of(
                sel,
                ReplayVerdictKind::Error,
                None,
                Some(format!("journal read: {e}")),
                0,
            );
        }
    };
    let run = RunRef {
        partition: sel.run.partition().0,
        run_id: sel.run.to_string(),
    };
    match replay_check(&runtime, &module, run, &journal).await {
        ReplayVerdict::Ok { appended } => {
            verdict_of(sel, ReplayVerdictKind::Ok, None, None, appended)
        }
        ReplayVerdict::OkFrontier { appended } => {
            verdict_of(sel, ReplayVerdictKind::OkFrontier, None, None, appended)
        }
        ReplayVerdict::Diverged { seq, detail } => {
            verdict_of(sel, ReplayVerdictKind::Diverged, Some(seq), Some(detail), 0)
        }
        ReplayVerdict::Error(e) => verdict_of(sel, ReplayVerdictKind::Error, None, Some(e), 0),
    }
}

/// Every selected run's verdict, in selection order: skips inline, the rest on at most
/// [`REPLAY_CONCURRENCY`] concurrent tasks.
async fn check_all(
    cp: &ServerControlPlane,
    module: &Arc<Prepared>,
    exported: &BTreeSet<String>,
    selected: Vec<Selected>,
) -> Result<Vec<ReplayVerdictInfo>, ApiError> {
    let mut out: Vec<Option<ReplayVerdictInfo>> = (0..selected.len()).map(|_| None).collect();
    let sem = Arc::new(Semaphore::new(REPLAY_CONCURRENCY));
    let mut tasks = tokio::task::JoinSet::new();
    for (i, sel) in selected.into_iter().enumerate() {
        if !exported.contains(split_qualified(&sel.workflow).1) {
            out[i] = Some(verdict_of(
                &sel,
                ReplayVerdictKind::Skipped,
                None,
                Some(SKIPPED_DETAIL.to_owned()),
                0,
            ));
            continue;
        }
        let handle = match cp.partition(sel.run.partition()) {
            Ok(h) => h.clone(),
            Err(e) => {
                out[i] = Some(verdict_of(
                    &sel,
                    ReplayVerdictKind::Error,
                    None,
                    Some(e.message),
                    0,
                ));
                continue;
            }
        };
        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| ApiError::internal(format!("replay pool: {e}")))?;
        let (runtime, module) = (cp.runtime.clone(), module.clone());
        tasks.spawn(async move {
            let v = check_one(runtime, module, handle, &sel).await;
            drop(permit);
            (i, v)
        });
    }
    while let Some(r) = tasks.join_next().await {
        let (i, v) = r.map_err(|e| ApiError::internal(format!("replay task: {e}")))?;
        out[i] = Some(v);
    }
    Ok(out.into_iter().flatten().collect())
}

/// Replay stored journals against a module (see the module docs).
pub(super) async fn replay_module(
    cp: &ServerControlPlane,
    r: ReplayRequest,
) -> Result<ReplayResult, ApiError> {
    r.validate()?;
    let started = std::time::Instant::now(); // clock-exempt: duration metric for the log line only.
    let sha = parse_sha(&r.module_sha256)
        .ok_or_else(|| ApiError::invalid_argument("sha256 must be 64 lowercase hex characters"))?;
    if cp.meta.view().read(|s| s.module(&sha).is_none()) {
        return Err(ApiError::not_found(format!(
            "module {} not found",
            r.module_sha256
        )));
    }
    let module = prepare(cp, sha).await?;
    let exported: BTreeSet<String> = cp
        .runtime
        .describe(&module)
        .await
        .map_err(|e| ApiError::internal(format!("describe() of the module failed: {e}")))?
        .into_iter()
        .map(|m| m.name)
        .collect();
    let prepare_ms = started.elapsed().as_millis();

    let selected = match &r.select {
        ReplaySelect::Runs(ids) => explicit(cp, ids).await?,
        ReplaySelect::Sample {
            n,
            workflow,
            status,
        } => {
            let status = status.as_deref().and_then(RunStatus::parse);
            let workflows: Vec<String> = match workflow {
                Some(w) => vec![w.clone()],
                None => exported.iter().cloned().collect(),
            };
            let mut all = Vec::new();
            for w in &workflows {
                all.extend(newest(cp, w, *n, status).await?);
            }
            all
        }
    };

    let verdicts = check_all(cp, &module, &exported, selected).await?;
    let summary = ReplaySummary::of(&verdicts);
    tracing::info!(
        module = %r.module_sha256,
        runs = verdicts.len(),
        ok = summary.ok,
        ok_frontier = summary.ok_frontier,
        diverged = summary.diverged,
        skipped = summary.skipped,
        error = summary.error,
        prepare_ms,
        total_ms = started.elapsed().as_millis(),
        "module replay"
    );
    Ok(ReplayResult {
        module_sha256: r.module_sha256,
        verdicts,
        summary,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha_parsing() {
        assert_eq!(parse_sha(&"0a".repeat(32)), Some([10; 32]));
        assert_eq!(parse_sha("0a"), None);
        assert_eq!(parse_sha(&"zz".repeat(32)), None);
    }
}
