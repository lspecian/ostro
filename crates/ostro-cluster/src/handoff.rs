//! Phase 13: implemented in 13-11 (graceful leader handoff; D-P13-13).
//!
//! [`LocalHandoff`] executes a handoff of a partition this node leads (through
//! `ostro_partition::HandoffControl`); [`NodeOpsHandoffClient`] asks the current leader over
//! `NodeOps.HandOff`, or calls the local executor when the leader is this node. Fixed
//! constructors; the stubs answer `Aborted { "not implemented" }`, so a Phase 13 caller never
//! moves leadership before 13-11.

use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_meta::MetaHandle;
use ostro_partition::HandoffControl;
use ostro_partition::roles::PartitionLogHandle;
use ostro_types::{Epoch, PartitionId};

use crate::contracts::{HandoffClient, HandoffError, HandoffExecutor, HandoffOutcome};
use crate::transport::ClusterTransport;

/// Looks up the local log of a partition (for the leader LEO and the follower progress).
pub type LogLookup = Arc<dyn Fn(PartitionId) -> Option<Arc<PartitionLogHandle>> + Send + Sync>;

/// Handoff timing (`[cluster]` keys, D-P13-26).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandoffConfig {
    /// `handoff_grace`: wait for in-flight activations to reach a flush/suspend point.
    pub grace: Duration,
    /// `handoff_catchup_timeout`: wait for the target's durable LEO to equal ours.
    pub catchup_timeout: Duration,
}

impl Default for HandoffConfig {
    fn default() -> Self {
        Self {
            grace: Duration::from_secs(2),
            catchup_timeout: Duration::from_secs(10),
        }
    }
}

fn not_implemented() -> HandoffOutcome {
    HandoffOutcome::Aborted {
        reason: "not implemented".into(),
    }
}

/// Node-local handoff executor.
pub struct LocalHandoff {
    _control: Arc<dyn HandoffControl>,
    _meta: Arc<MetaHandle>,
    _logs: LogLookup,
    _cfg: HandoffConfig,
}

impl std::fmt::Debug for LocalHandoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalHandoff").finish_non_exhaustive()
    }
}

impl LocalHandoff {
    /// An executor driving `control`, proposing through `meta`, reading progress from `logs`.
    pub fn new(
        control: Arc<dyn HandoffControl>,
        meta: Arc<MetaHandle>,
        logs: LogLookup,
        cfg: HandoffConfig,
    ) -> Self {
        Self {
            _control: control,
            _meta: meta,
            _logs: logs,
            _cfg: cfg,
        }
    }
}

impl HandoffExecutor for LocalHandoff {
    fn execute(
        &self,
        p: PartitionId,
        to: u64,
        epoch: Epoch,
    ) -> BoxFuture<'_, Result<HandoffOutcome, HandoffError>> {
        let _ = (p, to, epoch);
        Box::pin(async { Ok(not_implemented()) })
    }
}

/// Controller-side client: `NodeOps.HandOff` to the leader, or the local executor.
pub struct NodeOpsHandoffClient {
    _transport: Arc<dyn ClusterTransport>,
    _local: Arc<LocalHandoff>,
}

impl std::fmt::Debug for NodeOpsHandoffClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeOpsHandoffClient")
            .finish_non_exhaustive()
    }
}

impl NodeOpsHandoffClient {
    /// A client dialing leaders over `transport`, short-circuiting to `local`.
    pub fn new(transport: Arc<dyn ClusterTransport>, local: Arc<LocalHandoff>) -> Self {
        Self {
            _transport: transport,
            _local: local,
        }
    }
}

impl HandoffClient for NodeOpsHandoffClient {
    fn hand_off(
        &self,
        leader: u64,
        p: PartitionId,
        to: u64,
        epoch: Epoch,
    ) -> BoxFuture<'_, Result<HandoffOutcome, HandoffError>> {
        let _ = (leader, p, to, epoch);
        Box::pin(async { Ok(not_implemented()) })
    }
}
