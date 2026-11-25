//! `NodeOps.ModuleRefs` (13-15, D-P13-20): which of the given module shas runs on the
//! partitions this node leads still reference.
//!
//! For each asked `{partition, epoch}`:
//! - this node must lead the partition at exactly that epoch (the led set publishes a
//!   partition only after its `LeaderAnnounced` applied, so the store holds every run
//!   committed before the term) both before and after the scan; otherwise the pair is
//!   returned as `uncovered` and the caller's GC round is postponed (plan check W3);
//! - the `runs` keyspace is scanned in pages of [`SCAN_PAGE`] records (bounded memory), on
//!   the blocking pool;
//! - a run references its `RunRecord.module_sha` when it is non-terminal
//!   (`Scheduled/Ready/Running`, waiting or paused included), or, with `protect_resumable`,
//!   when it is terminal and `retry_run` would accept it ([`is_retryable`]: every terminal
//!   status except `Completed` and `ContinuedAsNew`, while the record is still retained).
//!   Retry resumes in place (D1) or restarts on the same `module_sha`, so both need the
//!   module.
//!
//! The answer lists the asked shas that were found, in the order asked.

use std::collections::HashSet;
use std::sync::{Arc, Weak};

use futures_util_shim::BoxFuture;
use ostro_cluster::contracts::{ModuleRefsSource, RefsAnswer};
use ostro_partition::Node;
use ostro_partition::query;
use ostro_partition::roles::PartitionRoles;
use ostro_partition::store::values::{RunRecord, RunStatus};
use ostro_types::{Epoch, PartitionId, RunId};

/// `BoxFuture` without a `futures` dependency in this crate.
mod futures_util_shim {
    pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
}

/// Run records read per page of the scan.
pub const SCAN_PAGE: usize = 512;

/// Whether `retry_run` accepts a run in `status` (terminal, not `Completed` /
/// `ContinuedAsNew`; `control::lifecycle::retry_mode`).
pub fn is_retryable(status: RunStatus) -> bool {
    status.is_terminal() && !matches!(status, RunStatus::Completed | RunStatus::ContinuedAsNew)
}

/// Whether `rec` needs its module (see the module docs).
pub fn needs_module(rec: &RunRecord, protect_resumable: bool) -> bool {
    !rec.status.is_terminal() || (protect_resumable && is_retryable(rec.status))
}

/// Answers `ModuleRefs` from the partitions this node leads. The node is held weakly (the
/// cluster listener outlives it).
pub struct LeaderModuleRefs {
    node: Weak<Node>,
    roles: Arc<dyn PartitionRoles>,
}

impl std::fmt::Debug for LeaderModuleRefs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderModuleRefs").finish_non_exhaustive()
    }
}

impl LeaderModuleRefs {
    /// A source over `node`'s partitions and its led set (`roles`).
    pub fn new(node: Weak<Node>, roles: Arc<dyn PartitionRoles>) -> Self {
        Self { node, roles }
    }

    fn leads(&self, p: PartitionId, e: Epoch) -> bool {
        self.roles
            .led()
            .borrow()
            .iter()
            .any(|&(lp, le)| lp == p && le == e)
    }

    /// Scan partition `p` (led at `e`) for `want`; adds hits to `found`. `Ok(false)` when
    /// the partition is not covered (not led at `e` before or after, or no handle).
    async fn scan(
        &self,
        p: PartitionId,
        e: Epoch,
        want: &Arc<HashSet<[u8; 32]>>,
        found: &mut HashSet<[u8; 32]>,
        protect_resumable: bool,
    ) -> Result<bool, String> {
        if !self.leads(p, e) {
            return Ok(false);
        }
        // Take the handle and drop the node before awaiting (never keep the node alive).
        let Some(handle) = self.node.upgrade().and_then(|n| n.handle(p)) else {
            return Ok(false);
        };
        let reader = handle.reader();
        drop(handle);
        let want = Arc::clone(want);
        let hits = tokio::task::spawn_blocking(move || {
            let mut hits = HashSet::new();
            let mut after: Option<RunId> = None;
            loop {
                let page = query::runs_page(&reader, after, SCAN_PAGE)
                    .map_err(|e| format!("partition {}: runs scan: {e}", p.0))?;
                let last = page.last().map(|(id, _)| *id);
                for (_, rec) in &page {
                    if let Some(sha) = rec.module_sha
                        && want.contains(&sha)
                        && needs_module(rec, protect_resumable)
                    {
                        hits.insert(sha);
                    }
                }
                if page.len() < SCAN_PAGE || hits.len() == want.len() {
                    return Ok::<_, String>(hits);
                }
                after = last;
            }
        })
        .await
        .map_err(|e| format!("partition {}: scan task: {e}", p.0))??;
        // Found references stand either way (err towards keeping); coverage needs the
        // same term at the end of the scan.
        found.extend(hits);
        Ok(self.leads(p, e))
    }
}

impl ModuleRefsSource for LeaderModuleRefs {
    fn referenced(
        &self,
        shas: Vec<[u8; 32]>,
        partitions: Vec<(PartitionId, Epoch)>,
        protect_resumable: bool,
    ) -> BoxFuture<'_, Result<RefsAnswer, String>> {
        Box::pin(async move {
            let want: Arc<HashSet<[u8; 32]>> = Arc::new(shas.iter().copied().collect());
            let mut found = HashSet::new();
            let mut uncovered = Vec::new();
            for (p, e) in partitions {
                let covered = if want.is_empty() {
                    self.leads(p, e)
                } else {
                    self.scan(p, e, &want, &mut found, protect_resumable)
                        .await?
                };
                if !covered {
                    uncovered.push((p, e));
                }
            }
            let mut seen = HashSet::new();
            let referenced = shas
                .into_iter()
                .filter(|s| found.contains(s) && seen.insert(*s))
                .collect();
            Ok(RefsAnswer {
                referenced,
                uncovered,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_statuses_match_retry_run() {
        use RunStatus as S;
        for s in [S::Scheduled, S::Ready, S::Running] {
            assert!(!is_retryable(s));
        }
        for s in [S::Completed, S::ContinuedAsNew] {
            assert!(!is_retryable(s));
        }
        for s in [S::Failed, S::Cancelled, S::Terminated, S::TimedOut] {
            assert!(is_retryable(s));
        }
    }
}
