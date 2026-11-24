//! Phase 13: implemented in 13-10 (the meta leader's placement controller: reassignment via
//! `MetaCommand::AlterReplicas`, preferred-leader balancing; D-P13-12).
//!
//! Fixed constructor; the stub loop is idle. 13-02 adds the explicit-request entry points the
//! server's `ClusterApi` delegates to ([`request_reassign`], [`request_rebalance`], the
//! [`PlacementTrigger`]) and `PlacementConfig::level_max` (the `compat::require` input), as
//! stubs answering [`PlacementError::NotImplemented`].

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
    /// This node's `level_max` (the `compat::require(AlterReplicas)` gate, 13-02).
    pub level_max: u32,
}

impl Default for PlacementConfig {
    fn default() -> Self {
        Self {
            max_concurrent_moves: 2,
            leader_rebalance_interval: Duration::from_secs(300),
            interval: Duration::from_millis(500),
            level_max: ostro_types::compat::level_max(),
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

    /// Wake the loop through `trigger` (`POST /v1/cluster/rebalance`). Stub: ignored.
    pub fn with_trigger(self, trigger: PlacementTrigger) -> Self {
        let _ = trigger;
        self
    }

    /// Start the loop. Stub: idle until `stop`.
    pub fn start(self, stop: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move { stop.cancelled().await })
    }
}

/// Wakes the placement controller for an immediate plan + balancer pass.
#[derive(Clone, Debug, Default)]
pub struct PlacementTrigger(Arc<tokio::sync::Notify>);

impl PlacementTrigger {
    /// A fresh trigger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request a pass.
    pub fn notify(&self) {
        self.0.notify_one();
    }

    /// Wait for a request.
    pub async fn notified(&self) {
        self.0.notified().await;
    }
}

/// Why an explicit placement request was refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PlacementError {
    /// The request is invalid (unknown partition, target not Active, duplicates, ...).
    #[error("invalid placement request: {0}")]
    Invalid(String),
    /// The cluster level does not allow it yet (`feature_not_enabled`).
    #[error(transparent)]
    FeatureNotEnabled(#[from] crate::compat::FeatureNotEnabled),
    /// Nothing to cancel, or the reassignment is past LeaderMove.
    #[error("{0}")]
    Conflict(String),
    /// Meta refused or failed.
    #[error("meta: {0}")]
    Meta(String),
    /// Not available before 13-10.
    #[error("Phase 13: implemented in 13-10")]
    NotImplemented,
}

/// `POST /v1/cluster/reassign`: start a reassignment of `partition` to `target`, or cancel
/// the in-flight one (`cancel`, allowed before LeaderMove). Stub: `NotImplemented`.
pub async fn request_reassign(
    meta: &MetaHandle,
    partition: u16,
    target: Vec<u64>,
    cancel: bool,
    level_max: u32,
    now_ms: u64,
) -> Result<(), PlacementError> {
    let _ = (meta, partition, target, cancel, level_max, now_ms);
    Err(PlacementError::NotImplemented)
}

/// `POST /v1/cluster/rebalance`: an immediate plan + balancer pass (through `trigger`). Stub:
/// `NotImplemented`.
pub async fn request_rebalance(
    meta: &MetaHandle,
    trigger: &PlacementTrigger,
    level_max: u32,
) -> Result<(), PlacementError> {
    let _ = (meta, trigger, level_max);
    Err(PlacementError::NotImplemented)
}
