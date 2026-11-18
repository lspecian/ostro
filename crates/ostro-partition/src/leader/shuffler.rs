//! Outbox shuffler: moves outbox rows to their target partitions (filled by plan 02-13).

use std::time::Duration;

use super::LeaderCtx;

/// Shuffler tuning.
#[derive(Clone, Debug)]
pub struct ShufflerConfig {
    /// Most messages per `InboundBatch`. Default 256.
    pub max_batch_msgs: usize,
    /// Poll interval when no outbox action arrived. Default 200 ms.
    pub idle_poll: Duration,
}

impl Default for ShufflerConfig {
    fn default() -> Self {
        Self {
            max_batch_msgs: 256,
            idle_poll: Duration::from_millis(200),
        }
    }
}

/// Start the shuffler of `ctx.partition`. Runs until `ctx.shutdown` turns true.
pub fn spawn(ctx: LeaderCtx, cfg: ShufflerConfig) -> tokio::task::JoinHandle<()> {
    // filled by plan 02-13
    let _ = cfg;
    tokio::spawn(async move {
        let mut s = ctx.shutdown;
        let _ = s.wait_for(|v| *v).await;
    })
}
