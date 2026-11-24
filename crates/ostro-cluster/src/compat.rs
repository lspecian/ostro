//! Phase 13: implemented in 13-04 (compatibility gate, boot/running fences, advertiser;
//! D-P13-3/5/6/7).
//!
//! Fixed signatures; the stubs preserve Phase 12 behaviour: the boot fence passes, every
//! feature gate allows, the advertiser and the running fence are idle tasks.

use std::sync::Arc;

use ostro_meta::ext::NodeVersion;
use ostro_meta::{MetaHandle, MetaView};
use ostro_types::compat::Feature;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Why a node must not open its partitions (boot fence) or keep running (running fence).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CompatError {
    /// The cluster is finalized above this binary's `level_max`.
    #[error(
        "data finalized by a newer Ostro (level {finalized}, this binary supports up to {level_max}); upgrade this binary"
    )]
    FinalizedAbove { finalized: u32, level_max: u32 },
    /// The cluster is finalized below this binary's `LEVEL_MIN`.
    #[error(
        "data at level {finalized} is older than this binary supports (minimum {level_min}); upgrade through {through} first"
    )]
    FinalizedBelow {
        finalized: u32,
        level_min: u32,
        through: String,
    },
    /// The meta format byte is above this build's maximum.
    #[error("metadata written by a newer Ostro (format {found}); upgrade this binary")]
    NewerMetaFormat { found: u8 },
}

/// A gated feature was asked for before the cluster finalized its level
/// (maps to 409 `feature_not_enabled`).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("feature {feature:?} needs level {needed}; the cluster is finalized at {finalized}")]
pub struct FeatureNotEnabled {
    /// The refused feature.
    pub feature: Feature,
    /// Its `introduced_at` level.
    pub needed: u32,
    /// The cluster's finalized level.
    pub finalized: u32,
}

/// Boot fence (rule 6): after meta catch-up and before `Node::open`. Stub: passes.
pub async fn boot_fence(meta: &MetaHandle, level_max: u32) -> Result<(), CompatError> {
    let _ = (meta, level_max);
    Ok(())
}

/// Proposer-side gate for a feature (`finalized >= f.introduced_at()` and `<= level_max`).
/// Stub: allows.
pub fn require(view: &MetaView, f: Feature, level_max: u32) -> Result<(), FeatureNotEnabled> {
    let _ = (view, f, level_max);
    Ok(())
}

/// Keep this node's `Member` ext 96 (NodeVersion) current. Stub: idle until `stop`.
pub fn start_advertiser(
    meta: Arc<MetaHandle>,
    node_id: u64,
    version: NodeVersion,
    stop: CancellationToken,
) -> JoinHandle<()> {
    let _ = (meta, node_id, version);
    tokio::spawn(async move { stop.cancelled().await })
}

/// Fail-stop when the finalized level rises above `level_max` (`on_fatal` gets the message).
/// Stub: idle until `stop`.
pub fn start_running_fence(
    meta: Arc<MetaHandle>,
    level_max: u32,
    on_fatal: Arc<dyn Fn(String) + Send + Sync>,
    stop: CancellationToken,
) -> JoinHandle<()> {
    let _ = (meta, level_max, on_fatal);
    tokio::spawn(async move { stop.cancelled().await })
}
