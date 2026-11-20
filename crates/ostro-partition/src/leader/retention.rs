//! Log retention (STOR-08): trims whole WAL segments below the safe floor.
//!
//! Every `interval` (first pass after one interval) a pass
//! 1. forces a durable checkpoint (`persist(SyncAll)`) and takes its offset `ckpt`;
//! 2. computes `floor = min(ckpt, local_floor, upload_floor?, retention_floor?)` (P8-D2) from
//!    the partition's [`FloorWatches`];
//! 3. mints a [`TrimPermit`](ostro_log::TrimPermit) for `floor` from the partition's
//!    [`ReadyBarrier`](ostro_log::ReadyBarrier) (refused before the barrier opens);
//! 4. calls `trim_prefix(permit)`, which deletes only whole segments entirely below `floor`.
//!
//! Every pass is logged, including "nothing to trim", and sets the gauge
//! `ostro_wal_log_start{partition}`; the fail point `trim::after_delete` sits right after
//! `trim_prefix` returns.
//!
//! Floors (plan 08-09):
//! - `local`: the `applied_offset` of the oldest retained local snapshot, published by the
//!   [snapshot service](super::snapshot) only after the snapshot is durable. `0` while no
//!   snapshot exists, so nothing is trimmed (rebuilding the state from the log, STOR-06, needs
//!   the full log until then). Hence `log_start <= oldest retained snapshot` (SC4).
//! - `upload`: `None` = unbounded (backup off). With backup on, `Node::open` initialises it to
//!   `Some(log_start)` before any pass runs (B4); the uploader (08-13) raises it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ostro_log::{LogError, ReplicatedLog};
use ostro_types::Offset;
use tokio::sync::watch;
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

/// The dynamic trim floors of one partition (P8-D2).
#[derive(Clone, Debug)]
pub struct FloorWatches {
    /// `applied_offset` of the oldest retained local snapshot (`0` = no snapshot).
    pub local: watch::Receiver<Offset>,
    /// Bottomless upload floor; `None` = unbounded (backup off).
    pub upload: watch::Receiver<Option<Offset>>,
}

impl FloorWatches {
    /// Fixed floors (tests, services without snapshots).
    pub fn fixed(local: Offset, upload: Option<Offset>) -> Self {
        Self {
            local: watch::channel(local).1,
            upload: watch::channel(upload).1,
        }
    }
}

/// Retention tuning.
#[derive(Clone, Debug)]
pub struct RetentionConfig {
    /// Pass interval. Default 60 s.
    pub interval: Duration,
    /// Optional operator retention floor. Default `None`.
    pub retention_floor: Option<Offset>,
    /// Optional introspection counters. Default `None`.
    pub stats: Option<Arc<RetentionStats>>,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            retention_floor: None,
            stats: None,
        }
    }
}

/// The trim floor `min(ckpt, local, upload?, retention?)`: never above the durable
/// checkpoint nor the oldest retained snapshot.
pub fn trim_floor(
    ckpt: Offset,
    local: Offset,
    upload: Option<Offset>,
    retention: Option<Offset>,
) -> Offset {
    ckpt.min(local)
        .min(upload.unwrap_or(Offset(u64::MAX)))
        .min(retention.unwrap_or(Offset(u64::MAX)))
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
            match pass(&ctx, &cfg, &stats).await {
                Ok(_) => {}
                Err(PassError::Fatal(_)) => return,
                Err(PassError::Skipped(_)) => {}
            }
        }
        info!(partition = p, "retention service stopped");
    })
}

/// Why a pass did not trim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PassError {
    /// The partition is failing (or a fail point fired); the service stops.
    Fatal(String),
    /// Barrier not open, permit refused or a retryable trim error.
    Skipped(String),
}

impl std::fmt::Display for PassError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PassError::Fatal(s) => write!(f, "retention pass failed: {s}"),
            PassError::Skipped(s) => write!(f, "retention pass skipped: {s}"),
        }
    }
}

/// Run one retention pass now (the ticker's pass; also `Node::retention_pass`). Returns the
/// log start after the pass.
pub async fn run_pass(ctx: &LeaderCtx, cfg: &RetentionConfig) -> Result<Offset, PassError> {
    let stats = cfg.stats.clone().unwrap_or_default();
    pass(ctx, cfg, &stats).await
}

/// One retention pass.
async fn pass(
    ctx: &LeaderCtx,
    cfg: &RetentionConfig,
    stats: &RetentionStats,
) -> Result<Offset, PassError> {
    let p = ctx.partition.0;
    let machine = ctx.handle.machine();
    // 1. checkpoint first: the floor below is bounded by what is durable in the state store.
    let ckpt = match machine.force_checkpoint().await {
        Ok(o) => o,
        Err(e) => {
            error!(partition = p, error = %e, "retention: checkpoint failed; retention stops");
            return Err(PassError::Fatal(format!("checkpoint: {e}")));
        }
    };
    stats.last_checkpoint.store(ckpt.0, Ordering::Relaxed);
    // 2. floor (read after the checkpoint; every floor only ever rises).
    let local = *ctx.floors.local.borrow();
    let upload = *ctx.floors.upload.borrow();
    let floor = trim_floor(ckpt, local, upload, cfg.retention_floor);
    // 3. permit (only from the opened barrier).
    let permit = match ctx.barrier.mint(floor) {
        Ok(permit) => permit,
        Err(LogError::NotReady) => {
            stats.not_ready.fetch_add(1, Ordering::Relaxed);
            info!(partition = p, floor = %floor, "retention pass skipped: barrier not open");
            return Err(PassError::Skipped("barrier not open".into()));
        }
        Err(e) => {
            warn!(partition = p, error = %e, "retention: permit refused");
            return Err(PassError::Skipped(format!("permit refused: {e}")));
        }
    };
    // 4. trim.
    let log = ctx.handle.log();
    let before = log.log_start();
    match log.trim_prefix(permit).await {
        Ok(new_start) => {
            #[cfg(feature = "failpoints")]
            fail::fail_point!("trim::after_delete", |_| {
                error!(
                    partition = p,
                    "retention: injected failure at trim::after_delete"
                );
                Err(PassError::Fatal(
                    "injected failure at trim::after_delete".into(),
                ))
            });
            metrics::gauge!("ostro_wal_log_start", "partition" => p.to_string())
                .set(new_start.0 as f64);
            stats.last_floor.store(floor.0, Ordering::Relaxed);
            stats.last_log_start.store(new_start.0, Ordering::Relaxed);
            stats.passes.fetch_add(1, Ordering::Relaxed);
            info!(
                partition = p,
                checkpoint = %ckpt,
                local_floor = %local,
                upload_floor = ?upload.map(|o| o.0),
                floor = %floor,
                new_start = %new_start,
                "retention pass{}",
                if new_start > before { "" } else { ": nothing to trim" }
            );
            Ok(new_start)
        }
        Err(e) => {
            if machine.check_healthy().is_err() {
                error!(partition = p, error = %e, "retention: trim failed; partition failing, retention stops");
                return Err(PassError::Fatal(format!("trim: {e}")));
            }
            warn!(partition = p, error = %e, "retention: trim failed; retrying next pass");
            Err(PassError::Skipped(format!("trim: {e}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floor_is_min_of_checkpoint_snapshot_upload_and_retention() {
        let max = Offset(u64::MAX);
        assert_eq!(trim_floor(Offset(100), Offset(0), None, None), Offset(0));
        assert_eq!(trim_floor(Offset(100), max, None, None), Offset(100));
        assert_eq!(trim_floor(Offset(100), Offset(50), None, None), Offset(50));
        assert_eq!(
            trim_floor(Offset(100), Offset(50), Some(Offset(30)), None),
            Offset(30)
        );
        assert_eq!(
            trim_floor(Offset(100), Offset(50), Some(Offset(70)), None),
            Offset(50)
        );
        assert_eq!(
            trim_floor(Offset(100), Offset(50), None, Some(Offset(20))),
            Offset(20)
        );
        assert_eq!(
            trim_floor(Offset(10), Offset(50), None, Some(Offset(20))),
            Offset(10)
        );
    }
}
