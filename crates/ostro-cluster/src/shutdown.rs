//! Phase 13: implemented in 13-11 (controlled shutdown: hand off every led partition, then
//! transfer meta leadership, bounded by `shutdown_handoff_timeout`; D-P13-13).
//!
//! Fixed signature. Stub: hands off nothing, so the existing Phase 12 shutdown runs as before.

use std::sync::Arc;
use std::time::Duration;

use ostro_meta::MetaHandle;

use crate::contracts::HandoffClient;

/// What a controlled shutdown achieved.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ShutdownReport {
    /// Partitions handed off.
    pub moved: Vec<u16>,
    /// Partitions still led here when the shutdown continued (aborted or timed out).
    pub kept: Vec<u16>,
    /// Meta leadership was transferred away.
    pub meta_transferred: bool,
}

/// Hand off every partition this node leads and transfer meta leadership, within `timeout`.
/// Stub: an empty report.
pub async fn controlled_shutdown(
    node_id: u64,
    meta: Arc<MetaHandle>,
    handoff: Arc<dyn HandoffClient>,
    timeout: Duration,
) -> ShutdownReport {
    let _ = (node_id, meta, handoff, timeout);
    ShutdownReport::default()
}
