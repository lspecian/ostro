//! Log retention: trims whole segments below the safe floor (filled by plan 02-14).

use std::time::Duration;

use ostro_types::Offset;

use super::LeaderCtx;

/// Retention tuning.
#[derive(Clone, Debug)]
pub struct RetentionConfig {
    /// Pass interval. Default 60 s.
    pub interval: Duration,
    /// Offset covered by the latest snapshot (Phase 8). Default `Offset(0)`.
    pub snapshot_floor: Offset,
    /// Optional operator retention floor. Default `None`.
    pub retention_floor: Option<Offset>,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            snapshot_floor: Offset(0),
            retention_floor: None,
        }
    }
}

/// Start the retention service of `ctx.partition`. Runs until `ctx.shutdown` turns true.
pub fn spawn(ctx: LeaderCtx, cfg: RetentionConfig) -> tokio::task::JoinHandle<()> {
    // filled by plan 02-14
    let _ = cfg;
    tokio::spawn(async move {
        let mut s = ctx.shutdown;
        let _ = s.wait_for(|v| *v).await;
    })
}
