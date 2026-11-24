//! Phase 13: implemented in 13-15 (`NodeOps.ModuleRefs`: which of the given module shas runs
//! on the partitions this node leads still reference; D-P13-20).
//!
//! Fixed constructor. Stub: an empty answer that covers nothing, so no caller can conclude
//! that a module is unreferenced.

use std::sync::{Arc, Weak};

use futures_util_shim::BoxFuture;
use ostro_cluster::contracts::{ModuleRefsSource, RefsAnswer};
use ostro_partition::Node;
use ostro_partition::roles::PartitionRoles;
use ostro_types::{Epoch, PartitionId};

/// `BoxFuture` without a `futures` dependency in this crate.
mod futures_util_shim {
    pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
}

/// Answers `ModuleRefs` from the partitions this node leads. The node is held weakly (the
/// cluster listener outlives it).
pub struct LeaderModuleRefs {
    _node: Weak<Node>,
    _roles: Arc<dyn PartitionRoles>,
}

impl std::fmt::Debug for LeaderModuleRefs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderModuleRefs").finish_non_exhaustive()
    }
}

impl LeaderModuleRefs {
    /// A source over `node`'s partitions and its led set (`roles`).
    pub fn new(node: Weak<Node>, roles: Arc<dyn PartitionRoles>) -> Self {
        Self {
            _node: node,
            _roles: roles,
        }
    }
}

impl ModuleRefsSource for LeaderModuleRefs {
    fn referenced(
        &self,
        shas: Vec<[u8; 32]>,
        partitions: Vec<(PartitionId, Epoch)>,
        protect_resumable: bool,
    ) -> BoxFuture<'_, Result<RefsAnswer, String>> {
        let _ = protect_resumable;
        Box::pin(async move {
            // Nothing is checked yet: every requested partition stays uncovered, and every
            // requested module counts as referenced.
            Ok(RefsAnswer {
                referenced: shas,
                uncovered: partitions,
            })
        })
    }
}
