//! Phase 13: implemented in 13-09 (the meta leader's membership loop: conf changes from
//! `reconcile::next`, Member tombstones, orphan joiners; D-P13-8/9).
//!
//! Fixed constructor; the stub loop is idle.

use std::sync::Arc;

use ostro_meta::MetaHandle;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Membership loop configuration (`[cluster]` keys, D-P13-26).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MembershipConfig {
    /// `promote_lag` (entries).
    pub promote_lag: u64,
    /// `join_orphan_timeout`.
    pub join_orphan_timeout: std::time::Duration,
    /// Reconcile period.
    pub interval: std::time::Duration,
}

impl Default for MembershipConfig {
    fn default() -> Self {
        Self {
            promote_lag: 64,
            join_orphan_timeout: std::time::Duration::from_secs(25 * 3600),
            interval: std::time::Duration::from_millis(500),
        }
    }
}

/// The membership loop (runs on every node, acts only as meta leader).
#[derive(Debug)]
pub struct MembershipService {
    _meta: Arc<MetaHandle>,
    _cfg: MembershipConfig,
}

impl MembershipService {
    /// A loop over `meta`.
    pub fn new(meta: Arc<MetaHandle>, cfg: MembershipConfig) -> Self {
        Self {
            _meta: meta,
            _cfg: cfg,
        }
    }

    /// Start the loop. Stub: idle until `stop`.
    pub fn start(self, stop: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move { stop.cancelled().await })
    }
}
