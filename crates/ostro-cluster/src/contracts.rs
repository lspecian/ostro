//! Phase 13 cross-plan contracts (13-01; final, later plans never edit this file).
//!
//! | trait / type | implemented by | consumed by |
//! |--------------|----------------|-------------|
//! | [`LevelSource`] | 13-04 (compat) | gates, advertiser, fences, status |
//! | [`HandoffClient`] | 13-11 (`handoff::NodeOpsHandoffClient`) | 13-10 placement controller, 13-11 shutdown |
//! | [`HandoffExecutor`] | 13-11 (`handoff::LocalHandoff`) | `NodeOps.HandOff` server (ostro-server `p13/node_ops.rs`) |
//! | [`ModuleRefsSource`] | 13-15 (ostro-server `p13/module_refs.rs`) | `NodeOps.ModuleRefs` server, 13-15 GC |
//! | [`ClusterStatusView`] | 13-09 (`p13/status.rs`) | `ClusterApi::status`, 13-04 finalizer, 13-16 CLI |

use futures_util::future::BoxFuture;
use ostro_meta::ext::{NodeVersion, Reassignment, ReplicationPolicy};
use ostro_types::{Epoch, PartitionId};

/// The feature levels this node may emit and the cluster's finalized level.
pub trait LevelSource: Send + Sync {
    /// Highest level this process may emit (`ostro_types::compat::level_max()`, or a tier-B
    /// per-node override).
    fn level_max(&self) -> u32;
    /// The cluster's finalized level (`ClusterInfo` ext 98; absent = `LEVEL_MIN`).
    fn finalized(&self) -> u32;
}

/// Result of a graceful handoff.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandoffOutcome {
    /// Leadership moved; the partition is now at `new_epoch` with the target as leader.
    Moved { new_epoch: Epoch },
    /// The handoff was abandoned and the old leader kept (admission re-enabled).
    Aborted { reason: String },
}

/// Why a handoff could not even start or finish.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum HandoffError {
    /// The asked node does not lead the partition at that epoch.
    #[error("node does not lead partition {partition} at epoch {epoch}")]
    NotLeader { partition: u16, epoch: u32 },
    /// The target is not in the published ISR.
    #[error("target {target} is not in the ISR of partition {partition}")]
    TargetNotInIsr { partition: u16, target: u64 },
    /// The cluster transport failed.
    #[error("transport: {0}")]
    Transport(String),
    /// Meta refused or failed.
    #[error("meta: {0}")]
    Meta(String),
}

/// Controller → current leader: ask `leader` to hand partition `p` (at `epoch`) to `to`
/// (`NodeOps.HandOff`; 13-11 implements, 13-10 consumes).
pub trait HandoffClient: Send + Sync {
    /// Ask `leader` to move `p` to `to`. A local leader is served without the network.
    fn hand_off(
        &self,
        leader: u64,
        p: PartitionId,
        to: u64,
        epoch: Epoch,
    ) -> BoxFuture<'_, Result<HandoffOutcome, HandoffError>>;
}

/// Node-local execution of a handoff of a partition this node leads (13-11 implements;
/// served by ostro-server `p13/node_ops.rs`).
pub trait HandoffExecutor: Send + Sync {
    /// Move `p` (led here at `epoch`) to `to`.
    fn execute(
        &self,
        p: PartitionId,
        to: u64,
        epoch: Epoch,
    ) -> BoxFuture<'_, Result<HandoffOutcome, HandoffError>>;
}

/// Answer of one [`ModuleRefsSource::referenced`] call.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RefsAnswer {
    /// The asked shas that are referenced by a run on a covered partition.
    pub referenced: Vec<[u8; 32]>,
    /// Asked `(partition, epoch)` pairs this node does not lead at that epoch (the GC round
    /// is postponed when any is uncovered; D-P13-20 W3).
    pub uncovered: Vec<(PartitionId, Epoch)>,
}

/// Node-local scan over led partitions for module references (13-15 implements).
pub trait ModuleRefsSource: Send + Sync {
    /// Which of `shas` are referenced by non-terminal runs (plus retryable terminal runs when
    /// `protect_resumable`) on `partitions` this node leads at the given epochs.
    fn referenced(
        &self,
        shas: Vec<[u8; 32]>,
        partitions: Vec<(PartitionId, Epoch)>,
        protect_resumable: bool,
    ) -> BoxFuture<'_, Result<RefsAnswer, String>>;
}

/// One member as the cluster status shows it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemberView {
    /// Node id.
    pub node_id: u64,
    /// Client API address.
    pub addr: String,
    /// Cluster listener address.
    pub cluster_addr: Option<String>,
    /// `MemberState` ext 97 (`MEMBER_*`; absent = Active for a Phase 12 member).
    pub state: u8,
    /// `NodeVersion` ext 96, when reported.
    pub version: Option<NodeVersion>,
    /// Live per the liveness sampler (meta leader's view).
    pub live: bool,
}

/// The cluster as `ostro cluster status` shows it (assembled by 13-09).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterStatusView {
    /// Every member record, tombstones included.
    pub members: Vec<MemberView>,
    /// Meta Raft voters (incoming config).
    pub voters: Vec<u64>,
    /// Meta Raft learners.
    pub learners: Vec<u64>,
    /// Meta Raft outgoing voters (non-empty while in a joint config).
    pub voters_outgoing: Vec<u64>,
    /// Current meta leader (0 = unknown).
    pub meta_leader: u64,
    /// Finalized feature level.
    pub finalized: u32,
    /// Operator hold (`ClusterVersion.hold`).
    pub hold: Option<u32>,
    /// `[min over members of level_min, min over members of level_max]`.
    pub level_range: (u32, u32),
    /// `ClusterInfo` ext 99.
    pub policy: Option<ReplicationPolicy>,
    /// In-flight reassignments (`PartitionAssignment` ext 100).
    pub reassignments: Vec<(PartitionId, Reassignment)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct Fixed(u32, u32);
    impl LevelSource for Fixed {
        fn level_max(&self) -> u32 {
            self.0
        }
        fn finalized(&self) -> u32 {
            self.1
        }
    }

    #[test]
    fn contracts_are_object_safe() {
        let l: Arc<dyn LevelSource> = Arc::new(Fixed(2, 1));
        assert_eq!((l.level_max(), l.finalized()), (2, 1));
        let _: Option<Arc<dyn HandoffClient>> = None;
        let _: Option<Arc<dyn HandoffExecutor>> = None;
        let _: Option<Arc<dyn ModuleRefsSource>> = None;
        assert_eq!(ClusterStatusView::default().meta_leader, 0);
    }
}
