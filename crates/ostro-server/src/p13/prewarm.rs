//! Phase 13 (13-14, D-P13-19, research Pitfall 9): AOT pre-warm.
//!
//! After a (re)start a node would otherwise compile every module on its first activation, all
//! at once (the compile storm of a rolling upgrade). One task per node compiles ahead of use:
//!
//! - **What:** the modules (`RunRecord.module_sha`) of the non-terminal runs that are ready
//!   (the ready queue, at most [`READY_SCAN_LIMIT`] rows per partition) or running (the
//!   `running` keyspace, bounded by the in-flight runs) on every partition this node leads,
//!   is taking over, or follows (a follower becomes the leader on failover).
//! - **When:** once at start, then after every partition role change (becoming leader or
//!   follower of a partition). A module is warmed once per process.
//! - **How:** the bytes come from the node's blob store (length from the `Module` record),
//!   then `Runtime::prepare_cached`: the L1 AOT artefact (compiled once and persisted, or
//!   loaded from disk) and the L2 `InstancePre` the first dispatch uses. At most
//!   [`PREWARM_CONCURRENCY`] modules at a time (low priority: the compile permits are shared
//!   with activations); a module already in L2 is skipped.
//! - **Metric:** `ostro_cluster_prewarm_modules_total{result}` (`compiled` = prepared now,
//!   `cached` = already in L2, `failed` = missing blob or invalid bytes; a failed module is
//!   left to its activation, which reports the error on the run).
//!
//! The task holds the node only weakly (it upgrades per pass) and stops with the
//! `P13Services` token.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use ostro_partition::roles::PartitionRole;
use ostro_partition::{Node, query};
use ostro_types::{PartitionId, RunId};
use tokio::sync::{Notify, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::P13Ctx;

/// Modules compiled at the same time (D-P13-19).
pub const PREWARM_CONCURRENCY: usize = 2;
/// Ready rows read per partition per pass.
pub const READY_SCAN_LIMIT: usize = 10_000;
/// The pre-warm counter (`result` = `compiled` | `cached` | `failed`).
pub const METRIC: &str = "ostro_cluster_prewarm_modules_total";

/// Start the pre-warm task (module docs).
pub fn start(ctx: &Arc<P13Ctx>, stop: CancellationToken) -> Vec<JoinHandle<()>> {
    let ctx = Arc::clone(ctx);
    vec![tokio::spawn(run(ctx, stop))]
}

/// Whether this node holds partition data it may serve: leader, taking over, follower.
fn serves(role: &PartitionRole) -> bool {
    matches!(
        role,
        PartitionRole::Leader { .. }
            | PartitionRole::Announcing { .. }
            | PartitionRole::Follower { .. }
    )
}

/// The modules of the ready and running non-terminal runs on the partitions `node` serves.
pub fn referenced_modules(node: &Node) -> BTreeSet<[u8; 32]> {
    let mut out = BTreeSet::new();
    for i in 0..node.slots().len() {
        let Ok(p) = u16::try_from(i).map(PartitionId) else {
            break;
        };
        if !node.role(p).is_some_and(|r| serves(&r.borrow())) {
            continue;
        }
        let Some(h) = node.handle(p) else {
            continue;
        };
        let r = h.reader();
        let mut runs: Vec<RunId> = match query::running_runs(&r) {
            Ok(v) => v.into_iter().map(|(run, _)| run).collect(),
            Err(e) => {
                debug!(partition = p.0, error = %e, "pre-warm: running scan failed");
                Vec::new()
            }
        };
        match query::ready_entries(&r, READY_SCAN_LIMIT) {
            Ok(v) => runs.extend(v.into_iter().map(|e| e.run)),
            Err(e) => debug!(partition = p.0, error = %e, "pre-warm: ready scan failed"),
        }
        for run in runs {
            if let Ok(Some(rec)) = query::run_record(&r, &run)
                && !rec.status.is_terminal()
                && let Some(sha) = rec.module_sha
            {
                out.insert(sha);
            }
        }
    }
    out
}

/// What one module's pre-warm did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Warmed {
    Compiled,
    Cached,
    Failed,
}

impl Warmed {
    fn label(self) -> &'static str {
        match self {
            Warmed::Compiled => "compiled",
            Warmed::Cached => "cached",
            Warmed::Failed => "failed",
        }
    }
}

/// Prepare module `sha` (module docs, "How").
async fn warm_one(ctx: &P13Ctx, sha: [u8; 32]) -> Warmed {
    if ctx.runtime.prepared(&sha).is_some() {
        return Warmed::Cached;
    }
    let Some(len) = ctx
        .meta
        .view()
        .read(|s| s.module(&sha).map(|m| m.value.size))
    else {
        return Warmed::Failed;
    };
    let Ok(blobs) = ctx.local_cp.blobs().map(Arc::clone) else {
        return Warmed::Failed;
    };
    let r = ostro_blob::BlobRef { sha256: sha, len };
    let bytes = match tokio::task::spawn_blocking(move || blobs.get(&r)).await {
        Ok(Ok(b)) => b,
        Ok(Err(e)) => {
            debug!(module = %ostro_blob::hex(&sha), error = %e, "pre-warm: blob unreadable");
            return Warmed::Failed;
        }
        Err(_) => return Warmed::Failed,
    };
    match ctx.runtime.prepare_cached(&bytes).await {
        Ok(_) => Warmed::Compiled,
        Err(e) => {
            warn!(module = %ostro_blob::hex(&sha), error = %e, "pre-warm: prepare failed");
            Warmed::Failed
        }
    }
}

/// Warm `shas` with at most [`PREWARM_CONCURRENCY`] in flight; returns each result.
pub async fn warm(ctx: &Arc<P13Ctx>, shas: Vec<[u8; 32]>) -> Vec<([u8; 32], Warmed)> {
    let permits = Arc::new(Semaphore::new(PREWARM_CONCURRENCY));
    let mut set = JoinSet::new();
    for sha in shas {
        let ctx = Arc::clone(ctx);
        let permits = Arc::clone(&permits);
        set.spawn(async move {
            let _permit = permits.acquire_owned().await.ok();
            let w = warm_one(&ctx, sha).await;
            metrics::counter!(METRIC, "result" => w.label()).increment(1);
            (sha, w)
        });
    }
    let mut out = Vec::new();
    while let Some(r) = set.join_next().await {
        if let Ok(r) = r {
            out.push(r);
        }
    }
    out
}

async fn run(ctx: Arc<P13Ctx>, stop: CancellationToken) {
    let wake = Arc::new(Notify::new());
    let mut forwarders = Vec::new();
    if let Some(node) = ctx.node() {
        for i in 0..node.slots().len() {
            let Ok(p) = u16::try_from(i).map(PartitionId) else {
                break;
            };
            let Some(mut rx) = node.role(p) else {
                continue;
            };
            let (wake, stop) = (Arc::clone(&wake), stop.clone());
            forwarders.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = stop.cancelled() => return,
                        r = rx.changed() => if r.is_err() { return },
                    }
                    wake.notify_one();
                }
            }));
        }
    }
    let mut done: HashSet<[u8; 32]> = HashSet::new();
    loop {
        let node = ctx.node();
        let Some(node) = node else { break };
        let found = tokio::task::spawn_blocking(move || referenced_modules(&node))
            .await
            .unwrap_or_default();
        let todo: Vec<[u8; 32]> = found.into_iter().filter(|s| !done.contains(s)).collect();
        if !todo.is_empty() {
            let results = tokio::select! {
                _ = stop.cancelled() => break,
                r = warm(&ctx, todo) => r,
            };
            let compiled = results
                .iter()
                .filter(|(_, w)| *w == Warmed::Compiled)
                .count();
            info!(
                modules = results.len(),
                compiled, "pre-warm: referenced modules prepared"
            );
            done.extend(
                results
                    .into_iter()
                    .filter(|(_, w)| *w != Warmed::Failed)
                    .map(|(s, _)| s),
            );
        }
        tokio::select! {
            _ = stop.cancelled() => break,
            _ = wake.notified() => {}
        }
    }
    for f in forwarders {
        f.abort();
    }
}
