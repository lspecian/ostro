//! `POST /v1/runs/migrate` / `ostro migrate-runs` (13-17; CLUS-07, D-P13-21): move live
//! runs from one module to another workflow version, but only runs whose own journal
//! replays cleanly against the target.
//!
//! # Entry (any node)
//!
//! The routed layer (13-12, `route::fanout::migrate_runs`) fans the request out to every
//! partition leader with `ostro-partitions`; each leg runs [`migrate_runs`] on its leader,
//! which handles only `route::table::listed_partitions` (the leg's set; every partition for
//! a direct local call). Each leg, behind the meta read barrier
//! (`route::consistency::before_meta_read`):
//!
//! 1. resolves `from` (a version of `workflow` → its module sha, or a sha) and `to_version`
//!    → `to_sha` (404 for an unknown version, 400 for a malformed sha or `from == to`);
//! 2. refuses a retired target (`module_retire::check_not_retired`, 409 `module_retired`);
//! 3. without `dry_run`: `compat::require(Feature::RepinRun)` (P13-F1 rule 1) before any
//!    work, so a cluster not finalized at level 2 answers 409 `feature_not_enabled` and
//!    nothing is submitted (a dry run still works);
//! 4. prepares the target module once (runtime cache, else the blob store).
//!
//! # Per partition
//!
//! Runs are scanned oldest first (`query::runs_page`). Selected: non-terminal, `workflow`
//! equal, `module_sha == from_sha`, every tag of `filter` carried (comma-separated, AND),
//! up to the partition's share of `limit`. **Limit split:** `limit` L over the partition
//! count P gives partition `p` the share `L / P + (p < L % P)`: the total is exactly L and
//! every node computes the same shares, so the selection does not depend on which node
//! received the request. No `limit` = every matching run.
//!
//! Each selected run's journal is read (`journal_io::read_journal`, blobs resolved) and
//! replayed against the target with `ostro_engine::replay_check` (unchanged; its sink
//! refuses every append past the journal end and performs no effect I/O):
//! `Ok`/`OkFrontier` → `passed`, `Diverged{seq, detail}` → `failed` with that seq and
//! detail, `Error(e)` (or an unreadable journal) → `failed` with seq 0.
//!
//! Without `dry_run`, every passed run gets `Cmd::RepinRun{run_id, from_sha, to_sha,
//! expected_next_seq = the replayed journal's length, requested_at_ms}`. The apply is the
//! compare-and-set (`ostro-partition` `apply/repin.rs`), so a run is repinned only at the
//! journal that passed. Outcomes: `Accepted` → `repinned`; `SeqMismatch` → `seq_mismatch`
//! (the journal grew after the check: run the dry run again); `RunBusy` → `busy` (leased
//! or hot-parked); `ModuleMismatch` → `failed` "module changed concurrently"; `RunTerminal`
//! / `RunNotFound` → `failed` with the reason.
//!
//! # Report
//!
//! `passed`, `repinned`, `failed` (seq + detail), `busy`, `seq_mismatch`. Every list is
//! sorted by run id ([`sort_report`]; the router sorts the merged report too), so the same
//! request gives the same report at every node.
//!
//! # Mutant
//!
//! [`MUTANT_SKIP_SEQ_CHECK`] (`migrate::mutant::skip_seq_check`, failpoints build): the
//! submission uses the run's `next_seq` read at submit time instead of the replayed
//! length, which defeats the compare-and-set (a journal that grew after the check gets
//! repinned). `tests/migrate_runs.rs` shows it biting.

use std::sync::{Arc, Mutex};

use ostro_api::ApiError;
use ostro_api::phase13::{MigrateFailure, MigrateRunsReport, MigrateRunsRequest, VersionOrSha};
use ostro_engine::replay_check::{ReplayVerdict, replay_check};
use ostro_engine::runtime::Prepared;
use ostro_engine::sink::RunRef;
use ostro_log::PartitionLog;
use ostro_partition::store::values::RunRecord;
use ostro_partition::{ApplyOutcome, PartitionHandle, RejectReason, query};
use ostro_types::command::{Command, RepinRun};
use ostro_types::compat::Feature;
use ostro_types::{PartitionCount, PartitionId, RunId};

use super::module_retire::{check_not_retired, parse_sha};
use super::names::{split_qualified, validate_name};
use super::runs::{partition_error, store_error, task_error};
use crate::journal_io;
use crate::p13::P13Ctx;

/// The seq-check mutant (D-P13-25).
pub const MUTANT_SKIP_SEQ_CHECK: &str = "migrate::mutant::skip_seq_check";
/// Counter of migrate outcomes, by `outcome` (`passed`, `repinned`, `failed`, `busy`,
/// `seq_mismatch`).
pub const METRIC_RUNS: &str = "ostro_migrate_runs_total";
/// `details.reason` of a refusal before the cluster finalized `RepinRun`'s level.
pub const REASON_FEATURE: &str = "feature_not_enabled";
/// Rows read from a partition per scan page.
const SCAN_PAGE: usize = 256;

/// `BoxFuture` without a `futures` dependency in this crate.
type BoxFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>;

/// A test hook run after the replay checks and before the first submission of a leg, with
/// the passed run ids (tier-B race scenarios journal an input in this window).
pub type BeforeSubmitHook = Arc<dyn Fn(Vec<String>) -> BoxFuture<()> + Send + Sync>;

static BEFORE_SUBMIT: Mutex<Option<BeforeSubmitHook>> = Mutex::new(None);

/// Install (or clear) the process-wide before-submit hook. Tests only.
#[doc(hidden)]
pub fn set_before_submit_hook(h: Option<BeforeSubmitHook>) {
    *BEFORE_SUBMIT.lock().unwrap_or_else(|p| p.into_inner()) = h;
}

fn before_submit_hook() -> Option<BeforeSubmitHook> {
    BEFORE_SUBMIT
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
}

/// Whether failpoint `name` is set to `return`.
#[cfg(feature = "failpoints")]
fn failpoint(name: &str) -> bool {
    fail::eval(name, |_| ()).is_some()
}

#[cfg(not(feature = "failpoints"))]
#[inline(always)]
fn failpoint(_name: &str) -> bool {
    false
}

/// Sort every list of `r` by run id (the report is identical at every node).
pub fn sort_report(r: &mut MigrateRunsReport) {
    r.passed.sort();
    r.repinned.sort();
    r.failed.sort_by(|a, b| a.run_id.cmp(&b.run_id));
    r.busy.sort();
    r.seq_mismatch.sort();
}

/// Partition `p`'s share of a global `limit` over `count` partitions (module docs).
pub fn limit_share(limit: Option<u32>, p: PartitionId, count: PartitionCount) -> usize {
    let Some(l) = limit else {
        return usize::MAX;
    };
    let n = u32::from(count.get());
    let base = l / n;
    let extra = u32::from(u32::from(p.0) < l % n);
    (base + extra) as usize
}

/// Whether `rec` carries every tag of `filter` (comma-separated; empty entries ignored).
fn filter_matches(filter: Option<&str>, rec: &RunRecord) -> bool {
    filter.is_none_or(|f| {
        f.split(',')
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .all(|t| rec.tags.iter().any(|x| x == t))
    })
}

fn hex(sha: &[u8; 32]) -> String {
    ostro_blob::hex(sha)
}

/// `(from_sha, to_sha)` for the request (read under the caller's barrier).
fn resolve(ctx: &P13Ctx, r: &MigrateRunsRequest) -> Result<([u8; 32], [u8; 32]), ApiError> {
    let view = ctx.meta.view();
    let version_sha = |v: u32, field: &str| {
        view.read(|s| s.workflow(&r.workflow, v).map(|w| w.value.module_sha256))
            .ok_or_else(|| {
                ApiError::not_found(format!("workflow {} has no version {v}", r.workflow))
                    .with_details(serde_json::json!({ "field": field }))
            })
    };
    let from = match &r.from {
        VersionOrSha::Version(v) => version_sha(*v, "from")?,
        VersionOrSha::Sha(s) => parse_sha(s).ok_or_else(|| {
            ApiError::invalid_argument("from.sha: expected 64 lowercase hex digits")
                .with_details(serde_json::json!({ "field": "from" }))
        })?,
    };
    let to = version_sha(r.to_version, "to_version")?;
    if from == to {
        return Err(ApiError::invalid_argument(format!(
            "from and to_version name the same module {}",
            hex(&to)
        ))
        .with_details(serde_json::json!({ "field": "to_version" })));
    }
    Ok((from, to))
}

/// 409 `feature_not_enabled` for a submitting migrate before level 2 is finalized.
fn feature_error(e: ostro_cluster::compat::FeatureNotEnabled) -> ApiError {
    ApiError::conflict(format!("{}; a dry run works at any level", e.detail())).with_details(
        serde_json::json!({
            "reason": REASON_FEATURE,
            "feature": e.feature.name(),
            "needed": e.needed,
            "finalized": e.finalized,
        }),
    )
}

/// The prepared target module (runtime cache, else the node blob store).
async fn prepare(ctx: &P13Ctx, sha: [u8; 32]) -> Result<Arc<Prepared>, ApiError> {
    if let Some(p) = ctx.runtime.prepared(&sha) {
        return Ok(p);
    }
    let blobs = Arc::clone(ctx.local_cp.blobs()?);
    let wasm = tokio::task::spawn_blocking(move || {
        let probe = ostro_blob::BlobRef {
            sha256: sha,
            len: 0,
        };
        let len = std::fs::metadata(blobs.path_of(&probe))
            .map_err(|e| format!("module {}: {e}", hex(&sha)))?
            .len();
        blobs
            .get(&ostro_blob::BlobRef { sha256: sha, len })
            .map_err(|e| format!("module {}: {e}", hex(&sha)))
    })
    .await
    .map_err(task_error)?
    .map_err(|e| ApiError::unavailable(format!("target module not available here: {e}")))?;
    ctx.runtime
        .prepare_cached(&wasm)
        .await
        .map_err(|e| ApiError::internal(format!("target module {}: {e}", hex(&sha))))
}

/// The runs of one partition to check, oldest first.
async fn select(
    handle: &PartitionHandle<PartitionLog>,
    r: &MigrateRunsRequest,
    from: [u8; 32],
    share: usize,
) -> Result<Vec<RunId>, ApiError> {
    if share == 0 {
        return Ok(Vec::new());
    }
    let reader = handle.reader();
    let workflow = r.workflow.clone();
    let filter = r.filter.clone();
    tokio::task::spawn_blocking(move || {
        let mut out = Vec::new();
        let mut after = None;
        loop {
            let rows = query::runs_page(&reader, after, SCAN_PAGE)?;
            let done = rows.len() < SCAN_PAGE;
            after = rows.last().map(|(id, _)| *id);
            for (id, rec) in rows {
                if !rec.status.is_terminal()
                    && rec.workflow == workflow
                    && rec.module_sha == Some(from)
                    && filter_matches(filter.as_deref(), &rec)
                {
                    out.push(id);
                    if out.len() >= share {
                        return Ok(out);
                    }
                }
            }
            if done {
                return Ok(out);
            }
        }
    })
    .await
    .map_err(task_error)?
    .map_err(store_error)
}

/// A run that passed: its id and the replayed journal length.
struct Passed {
    run: RunId,
    expected_next_seq: u64,
}

fn failure(run: &RunId, seq: u64, detail: impl Into<String>) -> MigrateFailure {
    MigrateFailure {
        run_id: run.to_string(),
        seq,
        detail: detail.into(),
    }
}

/// Replay-check one run: `Ok(Passed)` or `Err(failure)`.
async fn check(
    ctx: &P13Ctx,
    handle: &PartitionHandle<PartitionLog>,
    module: &Prepared,
    run: RunId,
) -> Result<Passed, MigrateFailure> {
    let journal = journal_io::read_journal(handle, &run)
        .await
        .map_err(|e| failure(&run, 0, format!("journal read: {e}")))?;
    let rref = RunRef {
        partition: run.partition().0,
        run_id: run.to_string(),
    };
    match replay_check(&ctx.runtime, module, rref, &journal).await {
        ReplayVerdict::Ok { .. } | ReplayVerdict::OkFrontier { .. } => Ok(Passed {
            run,
            expected_next_seq: journal.len() as u64,
        }),
        ReplayVerdict::Diverged { seq, detail } => Err(failure(&run, seq, detail)),
        ReplayVerdict::Error(e) => Err(failure(&run, 0, format!("replay check: {e}"))),
    }
}

/// The run's current `next_seq` (the mutant's submit-time read).
async fn current_next_seq(handle: &PartitionHandle<PartitionLog>, run: RunId) -> Option<u64> {
    let reader = handle.reader();
    tokio::task::spawn_blocking(move || query::run_record(&reader, &run))
        .await
        .ok()?
        .ok()?
        .map(|r| r.next_seq)
}

/// Submit one `RepinRun` and file its outcome in `out`.
async fn submit(
    ctx: &P13Ctx,
    handle: &PartitionHandle<PartitionLog>,
    p: &Passed,
    from: [u8; 32],
    to: [u8; 32],
    out: &mut MigrateRunsReport,
) -> Result<(), ApiError> {
    let mut expected = p.expected_next_seq;
    if failpoint(MUTANT_SKIP_SEQ_CHECK)
        && let Some(now) = current_next_seq(handle, p.run).await
    {
        tracing::warn!(run = %p.run, "{MUTANT_SKIP_SEQ_CHECK} active: submitting at next_seq {now}");
        expected = now;
    }
    let cmd = Command::repin_run(RepinRun {
        run_id: p.run.as_bytes().to_vec(),
        from_sha: from.to_vec(),
        to_sha: to.to_vec(),
        expected_next_seq: expected,
        requested_at_ms: ctx.clock.now_ms(),
    });
    let id = p.run.to_string();
    match handle.submit_one(cmd).await.map_err(partition_error)? {
        ApplyOutcome::Accepted | ApplyOutcome::Duplicate { .. } => out.repinned.push(id),
        ApplyOutcome::Rejected(r) => match r {
            RejectReason::SeqMismatch { .. } => out.seq_mismatch.push(id),
            RejectReason::RunBusy => out.busy.push(id),
            RejectReason::ModuleMismatch => out.failed.push(failure(
                &p.run,
                expected,
                "module changed concurrently (the run is no longer on the source module)",
            )),
            RejectReason::RunTerminal => {
                out.failed.push(failure(
                    &p.run,
                    expected,
                    "the run finished before the repin",
                ));
            }
            RejectReason::RunNotFound => {
                out.failed
                    .push(failure(&p.run, expected, "the run no longer exists"));
            }
            RejectReason::StaleEpoch { .. } | RejectReason::WrongPartition => {
                return Err(ApiError::unavailable(format!(
                    "partition {} cannot accept the repin: {r:?}",
                    p.run.partition().0
                ))
                .with_details(serde_json::json!({ "reason": "not_leader" })));
            }
            other => {
                out.failed.push(failure(
                    &p.run,
                    expected,
                    format!("repin rejected: {other:?}"),
                ));
            }
        },
    }
    Ok(())
}

fn count(outcome: &'static str, n: usize) {
    if n > 0 {
        metrics::counter!(METRIC_RUNS, "outcome" => outcome).increment(n as u64);
    }
}

/// Migrate runs of one module to another workflow version (module docs).
pub async fn migrate_runs(
    ctx: &P13Ctx,
    r: MigrateRunsRequest,
) -> Result<MigrateRunsReport, ApiError> {
    let local = split_qualified(&r.workflow).1;
    validate_name("workflow", local)?;
    crate::route::consistency::before_meta_read(ctx).await?;
    let (from, to) = resolve(ctx, &r)?;
    check_not_retired(ctx.meta.view(), &to)?;
    if !r.dry_run {
        ostro_cluster::compat::require(ctx.meta.view(), Feature::RepinRun, ctx.level_max)
            .map_err(feature_error)?;
    }

    let count_p = ctx.local_cp.partition_count();
    let mut selected = Vec::new();
    for p in crate::route::table::listed_partitions(count_p) {
        let share = limit_share(r.limit, p, count_p);
        if share == 0 {
            continue;
        }
        crate::route::consistency::before_partition_read(ctx, p).await?;
        let handle = ctx.local_cp.partition(p)?;
        let runs = select(&handle, &r, from, share).await?;
        if !runs.is_empty() {
            selected.push((handle, runs));
        }
    }

    let mut out = MigrateRunsReport::default();
    if selected.is_empty() {
        return Ok(out);
    }
    let module = prepare(ctx, to).await?;
    let mut passed: Vec<(PartitionHandle<PartitionLog>, Passed)> = Vec::new();
    for (handle, runs) in &selected {
        for run in runs {
            match check(ctx, handle, &module, *run).await {
                Ok(p) => {
                    out.passed.push(run.to_string());
                    passed.push((handle.clone(), p));
                }
                Err(f) => out.failed.push(f),
            }
        }
    }

    if !r.dry_run && !passed.is_empty() {
        if let Some(h) = before_submit_hook() {
            h(passed.iter().map(|(_, p)| p.run.to_string()).collect()).await;
        }
        for (handle, p) in &passed {
            submit(ctx, handle, p, from, to, &mut out).await?;
        }
    }
    count("passed", out.passed.len());
    count("repinned", out.repinned.len());
    count("failed", out.failed.len());
    count("busy", out.busy.len());
    count("seq_mismatch", out.seq_mismatch.len());
    sort_report(&mut out);
    tracing::info!(
        workflow = %r.workflow,
        from = %hex(&from),
        to = %hex(&to),
        dry_run = r.dry_run,
        passed = out.passed.len(),
        repinned = out.repinned.len(),
        failed = out.failed.len(),
        busy = out.busy.len(),
        seq_mismatch = out.seq_mismatch.len(),
        "migrate-runs leg"
    );
    Ok(out)
}
