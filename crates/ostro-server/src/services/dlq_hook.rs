//! The `on_failure` hook leader service (Phase 11, OPS-04, D-P11-12; plan 11-12).
//!
//! Started through `Services::attach_p11` by `p11::wire` (only when `[dlq] hooks_enabled`);
//! exits on `ServiceCtx::shutdown`. It runs on every started partition slot, which is the
//! partition leader (RF1; Phase 12/13 restrict it to leader slots with the rest of the
//! leader services).
//!
//! # Live path
//!
//! The service subscribes to every ready partition's post-apply action stream (as
//! `run_spans` does). On `Action::RunCompleted{Failed | TimedOut}` it calls [`drive`]:
//!
//! 1. read the run; skip unless it is `Failed`/`TimedOut`;
//! 2. **loop guard:** a run carrying `__ostro/on_failure_of=` is itself a hook run and never
//!    fires a hook (so a failing hook, or a hook configured to hook itself, stops there);
//! 3. resolve the policy ([`crate::onfailure::resolve`]: workflow `OnFailure` wins over the
//!    queue's; the queue's `DlqPolicy` must be enabled);
//! 4. skip when the `dlq` row already has `hook_run` for the current episode;
//! 5. start the hook run through the control plane's `start_run` with dedupe key
//!    `__onfail/<run>/<resumed_count>` (`ReturnExisting`) and tag
//!    `__ostro/on_failure_of=<run>`; input [`crate::onfailure::hook_input`];
//! 6. failpoint `dlq::hook_after_start` (feature `failpoints`; `return` skips step 7, as a
//!    crash there would);
//! 7. `DlqUpdate HookStarted{hook_run}` for the current episode (`episode_completed_at_ms`
//!    = the run's `completed_at_ms`), gated by `p11_feature_enabled(DlqUpdate)`.
//!
//! # Episodes and exactly-once
//!
//! One hook per **failure episode** (W5): the dedupe key carries `resumed_count`, which a
//! `RetryRun` resume increments, so a run that is resumed and fails again gets a second
//! hook run; a repeat for the same episode (the live path racing catch-up, a re-drive after
//! a crash between steps 5 and 7) hits the dedupe row and returns the same hook run, and
//! apply accepts the identical `HookStarted` again (set-once, same id). The two commands
//! are idempotent, not atomic.
//!
//! # Catch-up ([`catch_up`])
//!
//! Actions are published only after a partition's startup catch-up, so failures applied
//! while the node was down (or while hooks were disabled) never reach the live path. At
//! service start, each partition's `Status(Failed)` and `Status(TimedOut)` index prefixes are
//! walked newest first (at most [`CATCHUP_BUDGET`] rows per partition) and every failed run
//! whose `completed_at_ms` is inside its queue's `hook_catchup` window (ext tag 145, default
//! `[dlq] hook_catchup`) and that has no `HookStarted` for the current episode is re-driven
//! through [`drive`]. While the partition's index is `Rebuilding` the pass retries with
//! backoff (250 ms doubling to 5 s) until the index is `Ready`, and after
//! [`REBUILD_FALLBACK`] (60 s, W6) falls back to a budgeted scan of the `runs` keyspace.
//!
//! # Metrics
//!
//! `ostro_dlq_hooks_total{result}`: `started` (new hook run), `existing` (dedupe returned
//! the episode's hook run), `error` (start or mark failed; catch-up retries at the next
//! leader start). `ostro_dlq_requeues_total{mode}` is counted by `control::p11_dlq`.
//!
//! Time: the catch-up window reads the injected clock (`P11Ctx.cc.clock`).

use std::sync::Arc;
use std::time::Duration;

use ostro_api::ApiError;
use ostro_api::domain::{ConflictPolicy, StartRunRequest};
use ostro_partition::query::{self, IndexError};
use ostro_partition::store::derived::IndexState;
use ostro_partition::store::values::{RunRecord, RunStatus};
use ostro_partition::{Action, ApplyOutcome, PartitionRole, PartitionSlot};
use ostro_types::command::dlq_update;
use ostro_types::{PartitionId, RunId};
use tokio::sync::broadcast::error::RecvError;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::ServiceCtx;
use crate::control::p11_dlq::{self, current_marks, dlq_prefixes, dlq_row, episode};
use crate::control::watch::next_actions;
use crate::onfailure::{self, HookInput};
use crate::p11::P11Ctx;

/// Index (or scan) rows examined per partition by one catch-up pass at most.
pub const CATCHUP_BUDGET: usize = 100_000;
/// How long catch-up waits for a rebuilding index before scanning `runs` (W6).
pub const REBUILD_FALLBACK: Duration = Duration::from_secs(60);
/// Rows read per catch-up batch.
const BATCH: usize = 256;

/// The hook service task: `None` when `[dlq] hooks_enabled = false` or no partition is
/// ready.
pub fn spawn(svc: &ServiceCtx, p11: &Arc<P11Ctx>) -> Option<JoinHandle<()>> {
    if !svc.config.dlq.hooks_enabled {
        return None;
    }
    // Subscribe before returning, so no action published after the start is missed. Only
    // receivers are kept (12-16: a held handle pins the state store, so a snapshot install
    // could never close it); each task re-subscribes when the partition's machine is
    // replaced, and runs a catch-up pass for its partition whenever it becomes the leader
    // after the start (the startup pass only covers partitions led then).
    let subs: Vec<_> = svc
        .node
        .slots()
        .iter()
        .filter_map(|s| match s {
            PartitionSlot::Ready(h) => Some((h.partition(), h.machine().subscribe_actions())),
            PartitionSlot::Failed { .. } => None,
        })
        .collect();
    if subs.is_empty() {
        return None;
    }
    let shutdown = svc.shutdown.clone();
    let ctx = Arc::clone(p11);
    let node = Arc::downgrade(&svc.node);
    Some(tokio::spawn(async move {
        let mut set = JoinSet::new();
        {
            let ctx = Arc::clone(&ctx);
            let shutdown = shutdown.clone();
            set.spawn(async move {
                let s = catch_up(&ctx, REBUILD_FALLBACK, &shutdown).await;
                info!(?s, "on_failure hook catch-up done");
            });
        }
        for (p, rx) in subs {
            let shutdown = shutdown.clone();
            let ctx = Arc::clone(&ctx);
            let node = node.clone();
            set.spawn(async move {
                let Some(mut role) = node.upgrade().and_then(|n| n.role(p)) else {
                    return;
                };
                let mut leading = matches!(*role.borrow_and_update(), PartitionRole::Leader { .. });
                let mut first = Some(rx);
                loop {
                    let mut rx = match first.take() {
                        Some(rx) => rx,
                        None => match next_actions(&node, p, Some(&shutdown)).await {
                            Some(rx) => rx,
                            None => return,
                        },
                    };
                    loop {
                        let batch = tokio::select! {
                            biased;
                            _ = shutdown.cancelled() => return,
                            r = role.changed() => {
                                if r.is_err() {
                                    return;
                                }
                                let now = matches!(*role.borrow_and_update(), PartitionRole::Leader { .. });
                                if now && !leading {
                                    let mut stats = CatchUpStats::default();
                                    catch_up_partition(&ctx, p, REBUILD_FALLBACK, &shutdown, &mut stats).await;
                                    info!(partition = p.0, ?stats, "on_failure hook catch-up done (new leader)");
                                }
                                leading = now;
                                continue;
                            }
                            r = rx.recv() => r,
                        };
                        let actions = match batch {
                            Ok(a) => a,
                            Err(RecvError::Lagged(n)) => {
                                warn!(
                                    partition = p.0,
                                    skipped = n,
                                    "dlq_hook lagged; the next leader start's catch-up re-drives \
                                     missed hooks"
                                );
                                continue;
                            }
                            Err(RecvError::Closed) => break,
                        };
                        for a in actions.iter() {
                            if let Action::RunCompleted { run, status } = a
                                && matches!(status, RunStatus::Failed | RunStatus::TimedOut)
                                && let Err(e) = drive(&ctx, *run).await
                            {
                                warn!(%run, error = %e, "on_failure hook not started");
                            }
                        }
                    }
                }
            });
        }
        while set.join_next().await.is_some() {}
    }))
}

/// What [`drive`] did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HookOutcome {
    /// The run is gone or not `Failed`/`TimedOut`.
    NotFailed,
    /// The run is a hook run (loop guard).
    LoopGuard,
    /// No `on_failure` policy, or the queue's DLQ policy is disabled.
    NoHook,
    /// `HookStarted` already recorded for the current episode.
    AlreadyStarted,
    /// The hook run (new or the episode's existing one) and whether it was created now.
    Started { hook_run: String, created: bool },
}

fn count(result: &'static str) {
    metrics::counter!("ostro_dlq_hooks_total", "result" => result).increment(1);
}

/// Failpoint `dlq::hook_after_start`: true when it fires (skip the `HookStarted` mark).
fn hook_after_start() -> bool {
    #[cfg(feature = "failpoints")]
    if fail::eval("dlq::hook_after_start", |_| ()).is_some() {
        warn!("dlq::hook_after_start (injected): skipping the HookStarted mark");
        return true;
    }
    false
}

/// The run's input as JSON (`null` when absent; a non-JSON input becomes a string).
async fn source_input(
    handle: &ostro_partition::PartitionHandle<ostro_log::PartitionLog>,
    run: &RunId,
) -> serde_json::Value {
    let reader = handle.reader();
    let r = *run;
    let p = match tokio::task::spawn_blocking(move || {
        query::run_payload(&reader, &r, query::PAYLOAD_INPUT)
    })
    .await
    {
        Ok(Ok(Some(p))) => p,
        _ => return serde_json::Value::Null,
    };
    match handle.fetch_payload(&p).await {
        Ok(b) => serde_json::from_slice(&b)
            .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(&b).into())),
        Err(e) => {
            debug!(%run, error = %e, "hook input fetch failed");
            serde_json::Value::Null
        }
    }
}

/// Start the `on_failure` hook of `run`'s current failure episode if it has one (see the
/// module docs). Idempotent per episode.
pub async fn drive(ctx: &P11Ctx, run: RunId) -> Result<HookOutcome, ApiError> {
    let cp = &ctx.cc.partitions;
    let handle = &cp.partition(run.partition())?;
    let reader = handle.reader();
    let (rec, row) = tokio::task::spawn_blocking(move || {
        Ok::<_, ostro_partition::StoreError>((
            query::run_record(&reader, &run)?,
            dlq_row(&reader, &run)?,
        ))
    })
    .await
    .map_err(|e| ApiError::internal(format!("dlq hook read task: {e}")))?
    .map_err(|e| ApiError::internal(format!("dlq hook read: {e}")))?;
    let Some(rec) = rec else {
        return Ok(HookOutcome::NotFailed);
    };
    if !matches!(rec.status, RunStatus::Failed | RunStatus::TimedOut) {
        return Ok(HookOutcome::NotFailed);
    }
    if onfailure::is_hook_run(&rec.tags) {
        return Ok(HookOutcome::LoopGuard);
    }
    let (hook, policy) = onfailure::resolve(
        ctx.cc.meta.view(),
        &rec.workflow,
        &rec.queue,
        default_catchup_ms(ctx),
    );
    let Some(hook) = hook.filter(|_| policy.enabled) else {
        return Ok(HookOutcome::NoHook);
    };
    if current_marks(row, &rec).is_some_and(|m| m.hook_run.is_some()) {
        return Ok(HookOutcome::AlreadyStarted);
    }
    let run_s = run.to_string();
    let reason = p11_dlq::failure_reason(handle, &run, &rec).await?;
    let input = if hook.include_input {
        Some(source_input(handle, &run).await)
    } else {
        None
    };
    let body = onfailure::hook_input(&HookInput {
        run_id: &run_s,
        workflow: &rec.workflow,
        version: rec.workflow_version,
        status: if rec.status == RunStatus::TimedOut {
            "timed_out"
        } else {
            "failed"
        },
        reason: &reason,
        failed_at_ms: episode(&rec),
        tags: &rec.tags,
        input,
    });
    // Phase 13 (13-02): the hook run starts through the routed control plane (its
    // idempotency key may hash to a partition led elsewhere).
    let started = ctx
        .cc
        .routed
        .start_run(StartRunRequest {
            workflow: hook.workflow.clone(),
            input_json: body.to_string().into_bytes(),
            queue: hook.queue.clone(),
            tags: vec![onfailure::hook_tag(&run_s)],
            idempotency_key: Some(onfailure::hook_dedupe_key(&run_s, rec.resumed_count)),
            conflict_policy: ConflictPolicy::ReturnExisting,
            ..Default::default()
        })
        .await
        .inspect_err(|_| count("error"))?;
    let hook_run: RunId = started
        .run_id
        .parse()
        .map_err(|e| ApiError::internal(format!("hook run id: {e}")))?;
    if hook_after_start() {
        return Ok(HookOutcome::Started {
            hook_run: started.run_id,
            created: started.created,
        });
    }
    let op = dlq_update::Op::HookStarted(dlq_update::HookStarted {
        hook_run_id: hook_run.as_bytes().to_vec(),
    });
    match p11_dlq::submit_mark(handle, &run, episode(&rec), op).await {
        Ok(ApplyOutcome::Accepted | ApplyOutcome::Duplicate { .. }) => {}
        Ok(ApplyOutcome::Rejected(r)) => {
            // The run moved on (resumed / a newer episode): the next episode drives its own.
            count("error");
            return Err(ApiError::conflict(format!("HookStarted rejected: {r:?}")));
        }
        Err(e) => {
            count("error");
            return Err(e);
        }
    }
    count(if started.created {
        "started"
    } else {
        "existing"
    });
    Ok(HookOutcome::Started {
        hook_run: started.run_id,
        created: started.created,
    })
}

fn default_catchup_ms(ctx: &P11Ctx) -> u64 {
    u64::try_from(ctx.cfg.dlq.hook_catchup.get().as_millis()).unwrap_or(u64::MAX)
}

/// What one [`catch_up`] pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CatchUpStats {
    /// Rows examined over every partition.
    pub examined: usize,
    /// Runs re-driven through [`drive`].
    pub driven: usize,
    /// Of those, hooks started or confirmed (`HookStarted` written).
    pub started: usize,
    /// Partitions served by the `runs` scan fallback (index still rebuilding).
    pub scan_fallbacks: usize,
}

/// Whether a candidate needs a re-drive (cheap checks; [`drive`] re-checks the rest).
fn needs_drive(ctx: &P11Ctx, rec: &RunRecord, row_has_hook: bool, now: u64) -> bool {
    if !matches!(rec.status, RunStatus::Failed | RunStatus::TimedOut)
        || onfailure::is_hook_run(&rec.tags)
        || row_has_hook
    {
        return false;
    }
    let policy = onfailure::dlq_policy(ctx.cc.meta.view(), &rec.queue, default_catchup_ms(ctx));
    policy.enabled && rec.completed_at_ms.unwrap_or(0) >= now.saturating_sub(policy.hook_catchup_ms)
}

/// One catch-up pass over every partition (see the module docs). `rebuild_fallback` is how
/// long a rebuilding index is waited for before the `runs` scan ([`REBUILD_FALLBACK`] in
/// production; tests pass a short one).
pub async fn catch_up(
    ctx: &P11Ctx,
    rebuild_fallback: Duration,
    shutdown: &CancellationToken,
) -> CatchUpStats {
    let mut stats = CatchUpStats::default();
    for p in ctx.cc.partitions.partition_count().ids() {
        if !catch_up_partition(ctx, p, rebuild_fallback, shutdown, &mut stats).await {
            break;
        }
    }
    stats
}

/// [`catch_up`] for one partition (skipped unless this node leads it); `false` once
/// `shutdown` fired.
pub async fn catch_up_partition(
    ctx: &P11Ctx,
    p: PartitionId,
    rebuild_fallback: Duration,
    shutdown: &CancellationToken,
    stats: &mut CatchUpStats,
) -> bool {
    let cp = &ctx.cc.partitions;
    {
        let Ok(handle) = cp.partition(p) else {
            return true;
        };
        let reader = handle.reader();
        drop(handle);
        // Waited time is the sum of the backoff sleeps (no clock read: a manual test clock
        // never stalls the fallback).
        let mut waited = Duration::ZERO;
        let mut backoff = Duration::from_millis(250);
        let candidates = loop {
            if shutdown.is_cancelled() {
                return false;
            }
            let now = ctx.cc.clock.now_ms();
            if query::index_state(&reader) == IndexState::Ready {
                match candidates_indexed(ctx, reader.clone(), now).await {
                    Ok(c) => break c,
                    Err(IndexError::Rebuilding) => {}
                    Err(IndexError::Store(e)) => {
                        warn!(partition = p.0, error = %e, "hook catch-up read failed");
                        break (0, Vec::new());
                    }
                }
            }
            if waited >= rebuild_fallback {
                stats.scan_fallbacks += 1;
                match candidates_scan(ctx, reader.clone(), now).await {
                    Ok(c) => break c,
                    Err(e) => {
                        warn!(partition = p.0, error = %e, "hook catch-up scan failed");
                        break (0, Vec::new());
                    }
                }
            }
            let nap = backoff
                .min(rebuild_fallback.saturating_sub(waited))
                .max(Duration::from_millis(10));
            tokio::select! {
                _ = shutdown.cancelled() => return false,
                _ = tokio::time::sleep(nap) => {}
            }
            waited += nap;
            backoff = (backoff * 2).min(Duration::from_secs(5));
        };
        stats.examined += candidates.0;
        for run in candidates.1 {
            if shutdown.is_cancelled() {
                return false;
            }
            stats.driven += 1;
            match drive(ctx, run).await {
                Ok(HookOutcome::Started { .. }) => stats.started += 1,
                Ok(_) => {}
                Err(e) => warn!(%run, error = %e, "catch-up: on_failure hook not started"),
            }
        }
    }
    true
}

type Candidates = (usize, Vec<RunId>);

async fn candidates_indexed(
    ctx: &P11Ctx,
    reader: ostro_partition::store::StoreReader,
    now: u64,
) -> Result<Candidates, IndexError> {
    let c = ctx.clone();
    tokio::task::spawn_blocking(move || {
        let mut out = Vec::new();
        let mut examined = 0usize;
        for prefix in dlq_prefixes() {
            let mut after: Option<Vec<u8>> = None;
            loop {
                let take = BATCH.min(CATCHUP_BUDGET.saturating_sub(examined));
                if take == 0 {
                    break;
                }
                let page = query::index_page(&reader, &prefix, after.as_deref(), take, take + 1)?;
                examined += page.scanned.min(take);
                for (run, _) in page.runs {
                    let Some(rec) = query::run_record(&reader, &run)? else {
                        continue;
                    };
                    let has = current_marks(dlq_row(&reader, &run)?, &rec)
                        .is_some_and(|m| m.hook_run.is_some());
                    if needs_drive(&c, &rec, has, now) {
                        out.push(run);
                    }
                }
                match page.next {
                    Some(n) => after = Some(n),
                    None => break,
                }
            }
        }
        if examined >= CATCHUP_BUDGET {
            warn!(
                budget = CATCHUP_BUDGET,
                "hook catch-up stopped at its budget"
            );
        }
        Ok((examined, out))
    })
    .await
    .map_err(|e| {
        IndexError::Store(ostro_partition::StoreError::Codec(format!(
            "catch-up task: {e}"
        )))
    })?
}

async fn candidates_scan(
    ctx: &P11Ctx,
    reader: ostro_partition::store::StoreReader,
    now: u64,
) -> Result<Candidates, ostro_partition::StoreError> {
    let c = ctx.clone();
    tokio::task::spawn_blocking(move || {
        let mut out = Vec::new();
        let mut examined = 0usize;
        let mut after = None;
        while examined < CATCHUP_BUDGET {
            let rows = query::runs_page(&reader, after, BATCH)?;
            let n = rows.len();
            examined += n;
            for (run, rec) in rows {
                after = Some(run);
                if !matches!(rec.status, RunStatus::Failed | RunStatus::TimedOut) {
                    continue;
                }
                let has = current_marks(dlq_row(&reader, &run)?, &rec)
                    .is_some_and(|m| m.hook_run.is_some());
                if needs_drive(&c, &rec, has, now) {
                    out.push(run);
                }
            }
            if n < BATCH {
                break;
            }
        }
        Ok((examined, out))
    })
    .await
    .map_err(|e| ostro_partition::StoreError::Codec(format!("catch-up task: {e}")))?
}
