//! Timer service: keeps a window of due timers and fires them (filled by plan 02-13).

use super::LeaderCtx;

/// Timer service tuning.
#[derive(Clone, Debug)]
pub struct TimerConfig {
    /// Most timers held in memory. Default 10 000.
    pub window_max: usize,
    /// How far ahead the window reaches (ms). Default 60 000.
    pub window_span_ms: u64,
    /// Most `TimerFired` commands per append. Default 256.
    pub fire_batch: usize,
}

impl Default for TimerConfig {
    fn default() -> Self {
        Self {
            window_max: 10_000,
            window_span_ms: 60_000,
            fire_batch: 256,
        }
    }
}

/// Start the timer service of `ctx.partition`. Runs until `ctx.shutdown` turns true.
pub fn spawn(ctx: LeaderCtx, cfg: TimerConfig) -> tokio::task::JoinHandle<()> {
    // filled by plan 02-13
    let _ = cfg;
    tokio::spawn(async move {
        let mut s = ctx.shutdown;
        let _ = s.wait_for(|v| *v).await;
    })
}
