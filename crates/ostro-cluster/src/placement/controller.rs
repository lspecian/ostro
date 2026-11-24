//! Phase 13: implemented in 13-10 (the meta leader's placement controller: reassignment via
//! `MetaCommand::AlterReplicas`, preferred-leader balancing; D-P13-12).
//!
//! Fixed constructor; the stub loop is idle.

use std::sync::Arc;
use std::time::Duration;

use ostro_meta::MetaHandle;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::contracts::HandoffClient;

/// Placement controller configuration (`[cluster]` keys, D-P13-26).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementConfig {
    /// `max_concurrent_moves`.
    pub max_concurrent_moves: usize,
    /// `leader_rebalance_interval`.
    pub leader_rebalance_interval: Duration,
    /// Controller period.
    pub interval: Duration,
}

impl Default for PlacementConfig {
    fn default() -> Self {
        Self {
            max_concurrent_moves: 2,
            leader_rebalance_interval: Duration::from_secs(300),
            interval: Duration::from_millis(500),
        }
    }
}

/// The placement controller (runs on every node, acts only as meta leader).
pub struct PlacementController {
    _meta: Arc<MetaHandle>,
    _handoff: Arc<dyn HandoffClient>,
    _cfg: PlacementConfig,
}

impl std::fmt::Debug for PlacementController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlacementController")
            .finish_non_exhaustive()
    }
}

impl PlacementController {
    /// A controller over `meta`, moving leaders through `handoff`.
    pub fn new(
        meta: Arc<MetaHandle>,
        handoff: Arc<dyn HandoffClient>,
        cfg: PlacementConfig,
    ) -> Self {
        Self {
            _meta: meta,
            _handoff: handoff,
            _cfg: cfg,
        }
    }

    /// Start the loop. Stub: idle until `stop`.
    pub fn start(self, stop: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move { stop.cancelled().await })
    }
}
