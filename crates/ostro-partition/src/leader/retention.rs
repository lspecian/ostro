//! Log retention (STOR-08): trims whole WAL segments below the safe floor.
//!
//! Every `interval` (first pass after one interval) a pass
//! 1. forces a durable checkpoint (`persist(SyncAll)`) and takes its offset `ckpt`;
//! 2. computes `floor = min(ckpt, snapshot_floor, retention_floor)`;
//! 3. mints a [`TrimPermit`](ostro_log::TrimPermit) for `floor` from the partition's
//!    [`ReadyBarrier`](ostro_log::ReadyBarrier) (refused before the barrier opens);
//! 4. calls `trim_prefix(permit)`, which deletes only whole segments entirely below `floor`.
//!
//! Every pass is logged, including "nothing to trim". With the Phase 2 default
//! `snapshot_floor = Offset(0)` nothing is ever deleted: rebuilding a state store from the
//! log (STOR-06) needs the full log until Phase 8 snapshots exist.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ostro_log::{LogError, ReplicatedLog};
use ostro_types::Offset;
use tracing::{error, info, warn};

use super::LeaderCtx;

/// Counters for tests and introspection.
#[derive(Debug, Default)]
pub struct RetentionStats {
    /// Completed passes (a trim call was made).
    pub passes: AtomicU64,
    /// Passes skipped because the barrier refused the permit.
    pub not_ready: AtomicU64,
    /// Durable checkpoint offset forced by the last pass.
    pub last_checkpoint: AtomicU64,
    /// Floor of the last permit.
    pub last_floor: AtomicU64,
    /// Log start after the last pass.
    pub last_log_start: AtomicU64,
}

/// Retention tuning.
#[derive(Clone, Debug)]
pub struct RetentionConfig {
    /// Pass interval. Default 60 s.
    pub interval: Duration,
    /// Offset covered by the latest snapshot (Phase 8). Default `Offset(0)`.
    pub snapshot_floor: Offset,
    /// Optional operator retention floor. Default `None`.
    pub retention_floor: Option<Offset>,
    /// Optional introspection counters. Default `None`.
    pub stats: Option<Arc<RetentionStats>>,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            snapshot_floor: Offset(0),
            retention_floor: None,
            stats: None,
        }
    }
}

/// The trim floor: never above the durable checkpoint.
pub fn trim_floor(ckpt: Offset, cfg: &RetentionConfig) -> Offset {
    ckpt.min(cfg.snapshot_floor)
        .min(cfg.retention_floor.unwrap_or(Offset(u64::MAX)))
}

/// Start the retention service of `ctx.partition`. Runs until `ctx.shutdown` turns true.
pub fn spawn(ctx: LeaderCtx, cfg: RetentionConfig) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut shutdown = ctx.shutdown.clone();
        let stats = cfg.stats.clone().unwrap_or_default();
        let p = ctx.partition.0;
        loop {
            tokio::select! {
                _ = shutdown.wait_for(|v| *v) => break,
                () = tokio::time::sleep(cfg.interval) => {}
            }
            if *shutdown.borrow() {
                break;
            }
            if pass(&ctx, &cfg, &stats).await.is_err() {
                return;
            }
        }
        info!(partition = p, "retention service stopped");
    })
}

/// One retention pass. `Err` = the partition is failing; stop the service.
async fn pass(ctx: &LeaderCtx, cfg: &RetentionConfig, stats: &RetentionStats) -> Result<(), ()> {
    let p = ctx.partition.0;
    let machine = ctx.handle.machine();
    // 1. checkpoint first: the floor below is bounded by what is durable in the state store.
    let ckpt = match machine.force_checkpoint().await {
        Ok(o) => o,
        Err(e) => {
            error!(partition = p, error = %e, "retention: checkpoint failed; retention stops");
            return Err(());
        }
    };
    stats.last_checkpoint.store(ckpt.0, Ordering::Relaxed);
    // 2. floor.
    let floor = trim_floor(ckpt, cfg);
    // 3. permit (only from the opened barrier).
    let permit = match ctx.barrier.mint(floor) {
        Ok(permit) => permit,
        Err(LogError::NotReady) => {
            stats.not_ready.fetch_add(1, Ordering::Relaxed);
            info!(partition = p, floor = %floor, "retention pass skipped: barrier not open");
            return Ok(());
        }
        Err(e) => {
            warn!(partition = p, error = %e, "retention: permit refused");
            return Ok(());
        }
    };
    // 4. trim.
    let log = ctx.handle.log();
    let before = log.log_start();
    match log.trim_prefix(permit).await {
        Ok(new_start) => {
            stats.last_floor.store(floor.0, Ordering::Relaxed);
            stats.last_log_start.store(new_start.0, Ordering::Relaxed);
            stats.passes.fetch_add(1, Ordering::Relaxed);
            info!(
                partition = p,
                checkpoint = %ckpt,
                floor = %floor,
                new_start = %new_start,
                "retention pass{}",
                if new_start > before { "" } else { ": nothing to trim" }
            );
            Ok(())
        }
        Err(e) => {
            if machine.check_healthy().is_err() {
                error!(partition = p, error = %e, "retention: trim failed; partition failing, retention stops");
                return Err(());
            }
            warn!(partition = p, error = %e, "retention: trim failed; retrying next pass");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floor_is_min_of_checkpoint_snapshot_and_retention() {
        let mut cfg = RetentionConfig::default();
        assert_eq!(trim_floor(Offset(100), &cfg), Offset(0));
        cfg.snapshot_floor = Offset(u64::MAX);
        assert_eq!(trim_floor(Offset(100), &cfg), Offset(100));
        cfg.snapshot_floor = Offset(50);
        assert_eq!(trim_floor(Offset(100), &cfg), Offset(50));
        cfg.retention_floor = Some(Offset(20));
        assert_eq!(trim_floor(Offset(100), &cfg), Offset(20));
        assert_eq!(trim_floor(Offset(10), &cfg), Offset(10));
    }
}
