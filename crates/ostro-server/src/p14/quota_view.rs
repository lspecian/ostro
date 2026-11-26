//! Phase 14: implemented in 14-06 (`TenantQuotaView`: meta-watched `Tenant` quotas and the
//! leadership share, D-P14-8).

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::P14Ctx;
use crate::config::ServerConfig;

/// Tenant quotas as this node applies them. Stub: empty (every tenant unlimited).
#[derive(Debug, Default)]
pub struct TenantQuotaView {}

impl TenantQuotaView {
    /// An empty view.
    pub fn new(_cfg: &ServerConfig) -> Self {
        Self {}
    }
}

/// Watch meta `Tenant` records and the node's led partitions. Stub: no task.
pub fn start(_ctx: &P14Ctx, _stop: CancellationToken) -> Option<JoinHandle<()>> {
    None
}
