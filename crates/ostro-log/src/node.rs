//! `LogNode`: node-level open of every partition log over one shared `GroupCommitWal`.
//!
//! Order matters (research Pattern 3, W4, W10):
//! 1. recover **every** partition (verified scan, torn-tail cut) in parallel blocking tasks;
//! 2. only then construct the shared `GroupCommitWal` (inside the Tokio runtime, W10). The
//!    WAL opens a NEW segment on a partition's first append (W4); recovery must have finished
//!    cutting files before that can happen;
//! 3. register ONE commit callback that advances each partition's HW through
//!    [`compute_hw`] (monotonic, after fsync, W9);
//! 4. build an [`Rf1Log`] for every partition that recovered. Per-partition recovery errors
//!    are returned per slot; the other partitions still open.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ostro_types::envelope::DEFAULT_MAX_ENVELOPE_BYTES;
use ostro_types::{Offset, PartitionCount, PartitionId};
use ostro_wal::{GroupCommitConfig, GroupCommitWal};
use tokio::sync::watch;
use tracing::{error, info};

use crate::error::LogError;
use crate::hw::{ReplicaProgress, compute_hw};
use crate::recovery::{RecoveryReport, TOPIC, recover_partition};
use crate::rf1::Rf1Log;

/// Node-level log configuration.
#[derive(Clone, Debug)]
pub struct LogNodeConfig {
    /// WAL base directory (`<data>/wal`); partitions live under `<wal_dir>/p/<pid>`.
    pub wal_dir: PathBuf,
    /// Fixed partition count.
    pub partitions: PartitionCount,
    /// This node's id (the RF=1 ISR).
    pub node_id: u64,
    /// Group-commit WAL configuration.
    pub wal: GroupCommitConfig,
    /// Largest envelope an append may write (callers split larger batches).
    pub max_envelope_bytes: usize,
    /// Appends between enqueue and fsync, per partition (default 1024).
    pub max_inflight_appends: usize,
}

impl LogNodeConfig {
    /// Defaults: `GroupCommitConfig::default()`, `DEFAULT_MAX_ENVELOPE_BYTES`, 1024 in flight.
    pub fn new(wal_dir: PathBuf, partitions: PartitionCount, node_id: u64) -> Self {
        Self {
            wal_dir,
            partitions,
            node_id,
            wal: GroupCommitConfig::default(),
            max_envelope_bytes: DEFAULT_MAX_ENVELOPE_BYTES,
            max_inflight_appends: 1024,
        }
    }
}

/// Owns the node's shared `GroupCommitWal`.
pub struct LogNode {
    wal: Arc<GroupCommitWal>,
    commit_batches: Arc<AtomicU64>,
    partitions: PartitionCount,
}

impl std::fmt::Debug for LogNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogNode")
            .field("partitions", &self.partitions)
            .finish_non_exhaustive()
    }
}

/// Per-partition result of [`LogNode::open`].
pub type PartitionLogs = Vec<Result<Arc<Rf1Log>, LogError>>;

impl LogNode {
    /// Recover every partition, then open the shared WAL and build the logs.
    ///
    /// `floors[p]` is partition `p`'s durable floor (the state store's `applied_offset`, or
    /// `Offset(0)` when the state is being rebuilt); `floors.len()` must equal the partition
    /// count. Must be called inside a Tokio runtime.
    pub async fn open(
        cfg: LogNodeConfig,
        floors: &[Offset],
    ) -> Result<(LogNode, PartitionLogs), LogError> {
        let p = usize::from(cfg.partitions.get());
        if floors.len() != p {
            return Err(LogError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("floors.len() {} != partition count {p}", floors.len()),
            )));
        }
        std::fs::create_dir_all(&cfg.wal_dir)?;

        // (1) recovery for every partition, in parallel, before any WAL queue exists.
        let mut tasks = Vec::with_capacity(p);
        for pid in cfg.partitions.ids() {
            let dir = cfg.wal_dir.clone();
            let floor = floors[pid.index()];
            tasks.push(tokio::task::spawn_blocking(move || {
                recover_partition(&dir, pid, floor)
            }));
        }
        let mut reports: Vec<Result<RecoveryReport, LogError>> = Vec::with_capacity(p);
        for t in tasks {
            reports.push(match t.await {
                Ok(r) => r,
                Err(e) => Err(LogError::Failed(format!("recovery task failed: {e}"))),
            });
        }

        // (2) only now the shared WAL.
        let wal = Arc::new(GroupCommitWal::new(cfg.wal_dir.clone(), cfg.wal.clone()));

        // (3) one commit callback: partition -> HW sender, monotonic via compute_hw.
        let hws: Arc<[Option<Arc<watch::Sender<Offset>>>]> = reports
            .iter()
            .map(|r| {
                r.as_ref()
                    .ok()
                    .map(|rep| Arc::new(watch::channel(rep.leo).0))
            })
            .collect();
        let commit_batches = Arc::new(AtomicU64::new(0));
        {
            let hws = Arc::clone(&hws);
            let counter = Arc::clone(&commit_batches);
            let node = cfg.node_id;
            wal.set_commit_callback(Arc::new(
                move |topic: &str, partition: i32, _min: i64, max: i64| {
                    if topic != TOPIC {
                        return;
                    }
                    counter.fetch_add(1, Ordering::Relaxed);
                    let Some(Some(tx)) = usize::try_from(partition).ok().and_then(|i| hws.get(i))
                    else {
                        return;
                    };
                    let Some(durable_end) = max.checked_add(1).and_then(Offset::from_i64) else {
                        return;
                    };
                    tx.send_if_modified(|hw| {
                        let new = compute_hw(&[ReplicaProgress {
                            node,
                            leo: durable_end,
                            in_isr: true,
                        }]);
                        if new > *hw {
                            *hw = new;
                            true
                        } else {
                            false
                        }
                    });
                },
            ));
        }

        // (4) the logs.
        let logs = reports
            .into_iter()
            .zip(cfg.partitions.ids())
            .map(|(r, pid): (_, PartitionId)| match r {
                Ok(rep) => {
                    info!(
                        partition = pid.0,
                        leo = %rep.leo,
                        log_start = %rep.log_start,
                        records = rep.records,
                        torn_cut = ?rep.torn.as_ref().map(|t| (t.segment.display().to_string(), t.byte_pos)),
                        "partition log recovered"
                    );
                    let hw = hws[pid.index()]
                        .clone()
                        .expect("hw sender exists for a recovered partition");
                    Ok(Arc::new(Rf1Log::new(pid, &cfg, Arc::clone(&wal), hw, rep)))
                }
                Err(e) => {
                    error!(partition = pid.0, error = %e, "partition log recovery failed");
                    Err(e)
                }
            })
            .collect();

        Ok((
            LogNode {
                wal,
                commit_batches,
                partitions: cfg.partitions,
            },
            logs,
        ))
    }

    /// Number of WAL group commits (commit-callback invocations) since open, all partitions.
    pub fn commit_batches(&self) -> u64 {
        self.commit_batches.load(Ordering::Relaxed)
    }

    /// Partition count.
    pub fn partitions(&self) -> PartitionCount {
        self.partitions
    }

    /// Flush and seal every active segment (graceful shutdown).
    pub async fn shutdown(&self) {
        self.wal.shutdown().await;
    }
}
