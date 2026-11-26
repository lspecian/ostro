//! Phase 14: implemented in 14-04 (the group-committing writer with fail-closed permits).

use std::path::Path;
use std::sync::Arc;

use ostro_types::audit::{AuditSink, NoopAuditSink};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::ServerConfig;
use crate::p14::{P14Ctx, P14Error};

/// Open the journal (`[audit] dir`, default `<data>/audit`) and return its sink. Stub:
/// [`NoopAuditSink`].
pub fn open(
    _cfg: &ServerConfig,
    _data_dir: &Path,
    _node_id: u64,
    _incarnation: u64,
) -> Result<Arc<dyn AuditSink>, P14Error> {
    Ok(Arc::new(NoopAuditSink))
}

/// The writer's background task (group commit, retention, head export). Stub: none.
pub fn start(_ctx: &P14Ctx, _stop: CancellationToken) -> Option<JoinHandle<()>> {
    None
}

/// Flush pending records and coalescers (shutdown). Stub: nothing to flush.
pub async fn flush(_ctx: &P14Ctx) {}
