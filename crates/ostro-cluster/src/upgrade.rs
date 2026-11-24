//! Phase 13: implemented in 13-04 (finalization, hold/release; D-P13-4).
//!
//! Fixed signatures. The stubs never finalize: [`finalize_decision`] answers `Stay`, the
//! finalizer is idle and the operator calls report `NotImplemented`.

use std::sync::Arc;
use std::time::Duration;

use ostro_meta::MetaHandle;
use ostro_meta::ext::ClusterVersion;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::contracts::ClusterStatusView;

/// What the finalizer should do now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FinalizeDecision {
    /// Nothing to do (already at the highest common level).
    Stay,
    /// Propose `ClusterVersion.finalized = to`.
    Finalize { to: u32 },
    /// An operator hold keeps the level at `at`.
    Held { at: u32 },
    /// Not every member is ready (`reason` names the first blocker).
    Waiting { reason: String },
}

/// Why an operator upgrade call failed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum UpgradeError {
    /// Not every member supports the target level, or a member is not live.
    #[error("cannot finalize: {0}")]
    NotReady(String),
    /// Meta refused or failed.
    #[error("meta: {0}")]
    Meta(String),
    /// Not available before 13-04.
    #[error("Phase 13: implemented in 13-04")]
    NotImplemented,
}

/// Pure finalization rule (D-P13-4). Stub: `Stay`.
pub fn finalize_decision(status: &ClusterStatusView, auto_finalize: bool) -> FinalizeDecision {
    let _ = (status, auto_finalize);
    FinalizeDecision::Stay
}

/// The meta leader's auto-finalizer. Stub: idle until `stop`.
pub fn start_finalizer(
    meta: Arc<MetaHandle>,
    auto_finalize: bool,
    interval: Duration,
    stop: CancellationToken,
) -> JoinHandle<()> {
    let _ = (meta, auto_finalize, interval);
    tokio::spawn(async move { stop.cancelled().await })
}

/// `ostro cluster upgrade finalize`. Stub: `NotImplemented`.
pub async fn finalize(meta: &MetaHandle, now_ms: u64) -> Result<ClusterVersion, UpgradeError> {
    let _ = (meta, now_ms);
    Err(UpgradeError::NotImplemented)
}

/// `ostro cluster upgrade hold`. Stub: `NotImplemented`.
pub async fn hold(meta: &MetaHandle, now_ms: u64) -> Result<ClusterVersion, UpgradeError> {
    let _ = (meta, now_ms);
    Err(UpgradeError::NotImplemented)
}

/// `ostro cluster upgrade release`. Stub: `NotImplemented`.
pub async fn release(meta: &MetaHandle, now_ms: u64) -> Result<ClusterVersion, UpgradeError> {
    let _ = (meta, now_ms);
    Err(UpgradeError::NotImplemented)
}
