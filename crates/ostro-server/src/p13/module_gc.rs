//! Module GC (13-15, CLUS-07 / D-P13-20): delete retired modules that no run needs.
//!
//! One round ([`ModuleGc::round`]):
//! 1. ReadIndex barrier, then the candidates: modules whose Module record carries a
//!    decodable `Retired` mark (ext 102) older than `gc_grace` (by the injected clock).
//!    Younger retirements are kept (`kept{reason="grace"}`).
//! 2. **Scan 1:** for every partition, the `{partition, epoch}` of its current leader per
//!    the meta assignment view, grouped by leader node; each node answers
//!    `NodeOps.ModuleRefs` (a local call for this node). A partition without a leader
//!    (`leader == 0` / epoch 0), a node that cannot be reached, or any partition a node
//!    reports `uncovered` (not led at that epoch any more) **postpones the whole round**:
//!    nothing is deleted (plan check W3).
//! 3. Wait `gc_grace / 2`, then **scan 2**, identically (fresh assignment view). The mutant
//!    `gc::mutant::single_scan` skips the wait and scan 2.
//! 4. Delete only candidates absent from both scans and, re-read behind a second barrier,
//!    still retired with the same mark and without an active version: every
//!    WorkflowVersion of the module first, then the Module record, each `Delete` with CAS
//!    (a lost CAS keeps the module for the next round).
//!
//! The blob itself is left to Phase 8 blob GC (its mark set includes every meta Module
//! sha, so it sweeps the blob once the Module record is gone). Failpoints:
//! `gc::between_scans` and `gc::before_delete` (`return` postpones the round there).
//! Metrics: `ostro_module_gc_deleted_total`, `ostro_module_gc_kept_total{reason}` with
//! `reason` in `grace`, `referenced`, `postponed`, `changed`, `active`.
//!
//! The periodic loop ([`start`], every `[modules] gc_interval`) acts only on the meta
//! leader. `POST /v1/modules/gc` ([`run_now`]) runs one round on the node that serves it
//! and answers after scan 2 (at least `gc_grace / 2`). Concurrent rounds are safe: every
//! deletion re-reads and CASes, and each round needs its own two clean scans.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use ostro_api::ApiError;
use ostro_api::phase13::GcRunReport;
use ostro_api::proto::cluster::v1::node_ops_client::NodeOpsClient;
use ostro_api::proto::cluster::v1::{ModuleRefsRequest, PartitionEpoch};
use ostro_cluster::contracts::{ModuleRefsSource, RefsAnswer};
use ostro_cluster::transport::ClusterTransport;
use ostro_meta::ext::get_retired;
use ostro_meta::{MetaCommand, MetaError, MetaHandle, MetaKey, MetaResult, MetaState};
use ostro_partition::Clock;
use ostro_types::{Epoch, PartitionId};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::P13Ctx;
use crate::control::module_retire::active_versions;

/// `BoxFuture` without a `futures` dependency in this crate.
type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// The single-scan mutant (D-P13-25): skip the wait and scan 2.
pub const MUTANT_SINGLE_SCAN: &str = "gc::mutant::single_scan";
/// Failpoint after scan 1 (`return` postpones the round).
pub const FP_BETWEEN_SCANS: &str = "gc::between_scans";
/// Failpoint before the deletions (`return` postpones the round).
pub const FP_BEFORE_DELETE: &str = "gc::before_delete";
/// Counter: modules deleted.
pub const METRIC_DELETED: &str = "ostro_module_gc_deleted_total";
/// Counter: retired modules kept, by `reason`.
pub const METRIC_KEPT: &str = "ostro_module_gc_kept_total";
/// Bound on one node's `NodeOps.ModuleRefs` answer.
pub const NODE_CALL_TIMEOUT: Duration = Duration::from_secs(30);
/// CAS attempts of one module's deletion within a round.
const DELETE_ATTEMPTS: usize = 8;

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

/// The knobs of one round.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GcOptions {
    /// A module retired longer than this is a candidate; the scans are `grace / 2` apart.
    pub grace: Duration,
    /// Also keep modules of retryable terminal runs (D1).
    pub protect_resumable: bool,
}

impl GcOptions {
    /// `[modules] gc_grace` / `gc_protect_resumable`.
    pub fn from_config(cfg: &crate::config::ServerConfig) -> Self {
        Self {
            grace: cfg.modules.gc_grace.get(),
            protect_resumable: cfg.modules.gc_protect_resumable,
        }
    }
}

/// One node's `ModuleRefs` answer, by node id.
pub trait NodeRefs: Send + Sync {
    /// Ask `node` which of `shas` its runs on `partitions` (led at those epochs) reference.
    fn module_refs(
        &self,
        node: u64,
        shas: Vec<[u8; 32]>,
        partitions: Vec<(PartitionId, Epoch)>,
        protect_resumable: bool,
    ) -> BoxFuture<'_, Result<RefsAnswer, String>>;
}

/// Production [`NodeRefs`]: a local call for this node, `NodeOps.ModuleRefs` over the
/// cluster transport for the others.
pub struct NodeOpsRefs {
    self_id: u64,
    local: Arc<dyn ModuleRefsSource>,
    transport: Arc<dyn ClusterTransport>,
}

impl NodeOpsRefs {
    /// Over this node's source and the cluster transport.
    pub fn new(
        self_id: u64,
        local: Arc<dyn ModuleRefsSource>,
        transport: Arc<dyn ClusterTransport>,
    ) -> Self {
        Self {
            self_id,
            local,
            transport,
        }
    }
}

impl NodeRefs for NodeOpsRefs {
    fn module_refs(
        &self,
        node: u64,
        shas: Vec<[u8; 32]>,
        partitions: Vec<(PartitionId, Epoch)>,
        protect_resumable: bool,
    ) -> BoxFuture<'_, Result<RefsAnswer, String>> {
        Box::pin(async move {
            if node == self.self_id {
                return self
                    .local
                    .referenced(shas, partitions, protect_resumable)
                    .await;
            }
            let ch = self
                .transport
                .channel(node)
                .map_err(|e| format!("node {node}: {e}"))?;
            let req = ModuleRefsRequest {
                shas: shas.iter().map(|s| s.to_vec()).collect(),
                protect_resumable,
                partitions: partitions
                    .iter()
                    .map(|(p, e)| PartitionEpoch {
                        partition: u32::from(p.0),
                        epoch: e.0,
                    })
                    .collect(),
            };
            let call = async move { NodeOpsClient::new(ch).module_refs(req).await };
            let resp = tokio::time::timeout(NODE_CALL_TIMEOUT, call)
                .await
                .map_err(|_| format!("node {node}: ModuleRefs timed out"))?
                .map_err(|s| format!("node {node}: ModuleRefs: {}", s.message()))?
                .into_inner();
            let referenced = resp
                .referenced
                .iter()
                .map(|s| {
                    <[u8; 32]>::try_from(s.as_slice())
                        .map_err(|_| format!("node {node}: malformed sha in ModuleRefs"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let uncovered = resp
                .uncovered
                .iter()
                .map(|pe| {
                    u16::try_from(pe.partition)
                        .map(|p| (PartitionId(p), Epoch(pe.epoch)))
                        .map_err(|_| {
                            format!("node {node}: partition {} out of range", pe.partition)
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(RefsAnswer {
                referenced,
                uncovered,
            })
        })
    }
}

/// The GC over one meta handle, a [`NodeRefs`] and the injected clock.
pub struct ModuleGc {
    meta: Arc<MetaHandle>,
    refs: Arc<dyn NodeRefs>,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for ModuleGc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModuleGc").finish_non_exhaustive()
    }
}

/// Why a scan could not cover every partition.
struct Postponed(String);

/// A module the round may delete: its sha and the `retired_at_ms` it was selected with.
#[derive(Clone, Copy)]
struct Candidate {
    sha: [u8; 32],
    retired_at_ms: u64,
}

fn hex(sha: &[u8; 32]) -> String {
    ostro_blob::hex(sha)
}

fn kept(reason: &'static str, n: usize) {
    if n > 0 {
        metrics::counter!(METRIC_KEPT, "reason" => reason).increment(n as u64);
    }
}

/// The decodable `Retired` mark of module `sha` (an undecodable mark is never collected).
fn retired_at(state: &MetaState, sha: &[u8; 32]) -> Option<u64> {
    let m = state.module(sha)?;
    get_retired(&m.value.ext)
        .ok()
        .flatten()
        .map(|r| r.retired_at_ms)
}

impl ModuleGc {
    /// A GC over `meta`, asking nodes through `refs`, timing grace with `clock`.
    pub fn new(meta: Arc<MetaHandle>, refs: Arc<dyn NodeRefs>, clock: Arc<dyn Clock>) -> Self {
        Self { meta, refs, clock }
    }

    /// The GC of a running node: its meta, `NodeOps.ModuleRefs` through the cluster
    /// transport (local source for itself) and the node's clock.
    pub fn for_ctx(ctx: &P13Ctx) -> Self {
        let refs = NodeOpsRefs::new(
            ctx.node_id(),
            Arc::clone(&ctx.module_refs) as Arc<dyn ModuleRefsSource>,
            Arc::clone(ctx.cluster.transport()),
        );
        Self::new(
            Arc::clone(&ctx.meta),
            Arc::new(refs),
            Arc::clone(&ctx.clock),
        )
    }

    /// One scan: every partition through its current leader. `Ok(referenced)` only when
    /// every partition was covered.
    async fn scan(
        &self,
        shas: &[[u8; 32]],
        protect_resumable: bool,
    ) -> Result<BTreeSet<[u8; 32]>, Postponed> {
        let assignments: Vec<(u16, u32, u64)> = self.meta.view().read(|s| {
            s.partitions()
                .map(|p| (p.value.partition, p.value.epoch, p.value.leader))
                .collect()
        });
        if assignments.is_empty() {
            return Err(Postponed("no partition assignments".into()));
        }
        let mut by_node: BTreeMap<u64, Vec<(PartitionId, Epoch)>> = BTreeMap::new();
        for (p, epoch, leader) in assignments {
            if leader == 0 || epoch == 0 {
                return Err(Postponed(format!("partition {p} has no leader")));
            }
            by_node
                .entry(leader)
                .or_default()
                .push((PartitionId(p), Epoch(epoch)));
        }
        let mut referenced = BTreeSet::new();
        for (node, parts) in by_node {
            let answer = self
                .refs
                .module_refs(node, shas.to_vec(), parts, protect_resumable)
                .await
                .map_err(|e| Postponed(format!("partition leader unreachable: {e}")))?;
            if let Some((p, e)) = answer.uncovered.first() {
                return Err(Postponed(format!(
                    "node {node} no longer leads partition {} at epoch {}",
                    p.0, e.0
                )));
            }
            referenced.extend(answer.referenced);
        }
        Ok(referenced)
    }

    /// Delete module `c` if it is still retired with the same mark and has no active
    /// version: WorkflowVersions first, then the Module record (CAS). `Ok(Some(reason))`
    /// when it was kept.
    async fn delete(&self, c: Candidate) -> Result<Option<&'static str>, MetaError> {
        for _ in 0..DELETE_ATTEMPTS {
            enum Next {
                Gone,
                Keep(&'static str),
                Cmd(Box<MetaCommand>, bool),
            }
            let next = self.meta.view().read(|s| {
                let Some(module) = s.module(&c.sha) else {
                    return Next::Gone;
                };
                if retired_at(s, &c.sha) != Some(c.retired_at_ms) {
                    return Next::Keep("changed");
                }
                if !active_versions(s, &c.sha).is_empty() {
                    return Next::Keep("active");
                }
                if let Some(w) = s.workflows().find(|w| w.value.module_sha256 == c.sha) {
                    return Next::Cmd(
                        Box::new(MetaCommand::Delete {
                            key: MetaKey::Workflow(w.value.name.clone(), w.value.version),
                            expected_version: w.version,
                        }),
                        false,
                    );
                }
                Next::Cmd(
                    Box::new(MetaCommand::Delete {
                        key: MetaKey::Module(c.sha),
                        expected_version: module.version,
                    }),
                    true,
                )
            });
            let (cmd, last) = match next {
                // Someone else (a concurrent round) finished it.
                Next::Gone => return Ok(Some("changed")),
                Next::Keep(r) => return Ok(Some(r)),
                Next::Cmd(cmd, last) => (cmd, last),
            };
            match self.meta.propose(*cmd).await? {
                MetaResult::Ok { .. } if last => return Ok(None),
                MetaResult::Ok { .. }
                | MetaResult::StaleVersion { .. }
                | MetaResult::Conflict { .. }
                | MetaResult::NotFound => {}
                MetaResult::Invalid(msg) => {
                    warn!(module = %hex(&c.sha), %msg, "module gc: delete rejected");
                    return Ok(Some("changed"));
                }
            }
        }
        Ok(Some("changed"))
    }

    /// One GC round (see the module docs).
    pub async fn round(&self, opts: GcOptions) -> Result<GcRunReport, MetaError> {
        self.meta.read_index_barrier().await?;
        let now = self.clock.now_ms();
        let grace_ms = u64::try_from(opts.grace.as_millis()).unwrap_or(u64::MAX);
        let (candidates, young): (Vec<Candidate>, usize) = self.meta.view().read(|s| {
            let mut young = 0;
            let mut out = Vec::new();
            for m in s.modules() {
                let sha = m.value.sha256;
                let Some(at) = retired_at(s, &sha) else {
                    continue;
                };
                if now.saturating_sub(at) >= grace_ms && at <= now {
                    out.push(Candidate {
                        sha,
                        retired_at_ms: at,
                    });
                } else {
                    young += 1;
                }
            }
            (out, young)
        });
        kept("grace", young);
        let mut report = GcRunReport {
            candidates: candidates.iter().map(|c| hex(&c.sha)).collect(),
            ..GcRunReport::default()
        };
        if candidates.is_empty() {
            return Ok(report);
        }
        let shas: Vec<[u8; 32]> = candidates.iter().map(|c| c.sha).collect();
        let postpone = |mut report: GcRunReport, why: String| {
            kept("postponed", candidates.len());
            info!(reason = %why, candidates = candidates.len(), "module gc: round postponed");
            report.postponed = true;
            report.reason = Some(why);
            report
        };
        let mut referenced = match self.scan(&shas, opts.protect_resumable).await {
            Ok(r) => r,
            Err(Postponed(why)) => return Ok(postpone(report, why)),
        };
        if failpoint(FP_BETWEEN_SCANS) {
            return Ok(postpone(report, format!("failpoint {FP_BETWEEN_SCANS}")));
        }
        if failpoint(MUTANT_SINGLE_SCAN) {
            warn!("module gc: {MUTANT_SINGLE_SCAN} active, scan 2 skipped");
        } else {
            tokio::time::sleep(opts.grace / 2).await;
            match self.scan(&shas, opts.protect_resumable).await {
                Ok(r) => referenced.extend(r),
                Err(Postponed(why)) => return Ok(postpone(report, why)),
            }
        }
        if failpoint(FP_BEFORE_DELETE) {
            return Ok(postpone(report, format!("failpoint {FP_BEFORE_DELETE}")));
        }
        report.referenced = candidates
            .iter()
            .filter(|c| referenced.contains(&c.sha))
            .map(|c| hex(&c.sha))
            .collect();
        kept("referenced", report.referenced.len());
        // Re-read behind a fresh barrier: a module re-uploaded, un-retired or re-activated
        // meanwhile is kept by `delete`.
        self.meta.read_index_barrier().await?;
        for c in candidates.iter().filter(|c| !referenced.contains(&c.sha)) {
            match self.delete(*c).await? {
                None => {
                    metrics::counter!(METRIC_DELETED).increment(1);
                    info!(module = %hex(&c.sha), "module gc: deleted");
                    report.deleted.push(hex(&c.sha));
                }
                Some(reason) => {
                    kept(reason, 1);
                    debug!(module = %hex(&c.sha), reason, "module gc: kept");
                }
            }
        }
        Ok(report)
    }
}

fn gc_error(e: MetaError) -> ApiError {
    ApiError::unavailable(format!("module gc: metadata unavailable: {e}"))
}

/// Start the periodic GC (`[modules] gc_interval`, meta leader only).
pub fn start(ctx: &Arc<P13Ctx>, stop: CancellationToken) -> Vec<JoinHandle<()>> {
    let interval = ctx.cfg.modules.gc_interval.get();
    let weak = Arc::downgrade(ctx);
    vec![tokio::spawn(async move {
        loop {
            tokio::select! {
                () = stop.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }
            let Some(ctx) = weak.upgrade() else { break };
            if !ctx.meta.is_leader() {
                continue;
            }
            let gc = ModuleGc::for_ctx(&ctx);
            let opts = GcOptions::from_config(&ctx.cfg);
            drop(ctx);
            tokio::select! {
                () = stop.cancelled() => break,
                r = gc.round(opts) => match r {
                    Ok(rep) if !rep.candidates.is_empty() => info!(
                        candidates = rep.candidates.len(),
                        deleted = rep.deleted.len(),
                        referenced = rep.referenced.len(),
                        postponed = rep.postponed,
                        "module gc round"
                    ),
                    Ok(_) => {}
                    Err(e) => warn!(error = %e, "module gc round failed"),
                },
            }
        }
    })]
}

/// `POST /v1/modules/gc`: one round now, with the configured grace and protection.
pub async fn run_now(ctx: &P13Ctx) -> Result<GcRunReport, ApiError> {
    run_with(ctx, GcOptions::from_config(&ctx.cfg)).await
}

/// One round with explicit options (tests and tier-B scenarios use short graces).
pub async fn run_with(ctx: &P13Ctx, opts: GcOptions) -> Result<GcRunReport, ApiError> {
    ModuleGc::for_ctx(ctx).round(opts).await.map_err(gc_error)
}
