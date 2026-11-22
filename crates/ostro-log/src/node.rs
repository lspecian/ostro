//! `LogNode`: node-level open of every partition log over one shared `GroupCommitWal`.
//!
//! Order matters (research Pattern 3, W4, W10):
//! 1. recover **every** partition (verified scan, torn-tail cut) in parallel blocking tasks;
//! 2. only then construct the shared `GroupCommitWal` (inside the Tokio runtime, W10). The
//!    WAL opens a NEW segment on a partition's first append (W4); recovery must have finished
//!    cutting files before that can happen;
//! 3. register ONE commit callback that reports each partition's durable end (after fsync,
//!    W9) to that partition's replica core, which advances the HW through its replica
//!    tracker and publishes it through [`HwVector::publish_hw`], the single HW publication
//!    point that also feeds the consistent cut ([`LogNode::cut`], W7). RF=1 takes exactly
//!    this path (D5, D-P12-15);
//! 4. build a [`PartitionLog`] for every partition that recovered. Per-partition recovery
//!    errors are returned per slot; the other partitions still open.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use ostro_types::envelope::DEFAULT_MAX_ENVELOPE_BYTES;
use ostro_types::{Offset, PartitionCount, PartitionId};
use ostro_wal::{GroupCommitConfig, GroupCommitWal};
use tracing::{error, info};

use crate::cut::HwVector;
use crate::error::LogError;
use crate::partition_log::{PartitionLog, ReplicaCore};
use crate::recovery::{RecoveryReport, TOPIC, recover_partition};

/// Logic time for the replica tracker (milliseconds). `ostro-partition` adapts its injected
/// `Clock` to this; the default is the system clock.
pub trait LogClock: Send + Sync + std::fmt::Debug {
    /// Milliseconds since an arbitrary fixed epoch (monotone enough for lag measurement).
    fn now_ms(&self) -> u64;
}

/// [`LogClock`] over the system wall clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemLogClock;

impl LogClock for SystemLogClock {
    // The log's only wall-clock read: replica lag measurement (never apply).
    #[allow(clippy::disallowed_methods)]
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }
}

/// Default `replica_lag_max` (D-P12-5: 10 s).
pub const DEFAULT_REPLICA_LAG_MAX_MS: u64 = 10_000;

/// Node-level log configuration.
#[derive(Clone, Debug)]
pub struct LogNodeConfig {
    /// WAL base directory (`<data>/wal`); partitions live under `<wal_dir>/p/<pid>`.
    pub wal_dir: PathBuf,
    /// Fixed partition count.
    pub partitions: PartitionCount,
    /// This node's id.
    pub node_id: u64,
    /// Group-commit WAL configuration.
    pub wal: GroupCommitConfig,
    /// Largest envelope an append may write (callers split larger batches).
    pub max_envelope_bytes: usize,
    /// Appends between enqueue and fsync, per partition (default 1024).
    pub max_inflight_appends: usize,
    /// Logic time for the replica tracker (default [`SystemLogClock`]).
    pub clock: Arc<dyn LogClock>,
    /// Follower lag bound for the measured in-sync verdict in
    /// [`ReplicaStatus`](crate::ReplicaStatus) (default [`DEFAULT_REPLICA_LAG_MAX_MS`]).
    pub replica_lag_max_ms: u64,
}

impl LogNodeConfig {
    /// Defaults: `GroupCommitConfig::default()`, `DEFAULT_MAX_ENVELOPE_BYTES`, 1024 in flight,
    /// the system clock, a 10 s replica lag bound.
    pub fn new(wal_dir: PathBuf, partitions: PartitionCount, node_id: u64) -> Self {
        Self {
            wal_dir,
            partitions,
            node_id,
            wal: GroupCommitConfig::default(),
            max_envelope_bytes: DEFAULT_MAX_ENVELOPE_BYTES,
            max_inflight_appends: 1024,
            clock: Arc::new(SystemLogClock),
            replica_lag_max_ms: DEFAULT_REPLICA_LAG_MAX_MS,
        }
    }
}

/// Owns the node's shared `GroupCommitWal`.
pub struct LogNode {
    wal: Arc<GroupCommitWal>,
    commit_batches: Arc<AtomicU64>,
    partitions: PartitionCount,
    hw_vector: Arc<HwVector>,
}

impl std::fmt::Debug for LogNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogNode")
            .field("partitions", &self.partitions)
            .finish_non_exhaustive()
    }
}

/// Per-partition result of [`LogNode::open`].
pub type PartitionLogs = Vec<Result<Arc<PartitionLog>, LogError>>;

impl LogNode {
    /// Recover every partition, then open the shared WAL and build the logs.
    ///
    /// `floors[p]` is partition `p`'s durable floor: the state store's `applied_offset`
    /// (`Offset(0)` when the state is being rebuilt, `Offset(u64::MAX)` when it is
    /// unreadable); `floors.len()` must equal the partition count. Must be called inside a
    /// Tokio runtime.
    ///
    /// **HW at open = the applied offset** (Phase 12 F4), clamped into `[log_start, LEO]`
    /// (the unreadable sentinel clamps to LEO), never simply the recovered LEO: the HW rises
    /// only through the replica tracker, to the durable LEO the moment the log
    /// [becomes leader](crate::ReplicatedLog::become_leader) with ISR `{self}`, or to
    /// `min(leader HW, durable end)` as a follower. Every log opens unassigned (role
    /// `Offline`): appends need [`become_leader`](crate::ReplicatedLog::become_leader).
    ///
    /// A partition whose log has no records opens **anchored at its floor** (Phase 12 F3):
    /// `log_start = LEO = HW = floor`, and its first append is assigned offset `floor`. Seed
    /// its epoch history afterwards with [`PartitionLog::seed_epochs`]. A floor above
    /// `i64::MAX` (the "state unreadable" sentinel) anchors at 0.
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

        // (3) one commit callback: partition -> its replica core (durable end -> tracker ->
        //     next_hw -> HwVector::publish_hw, vector then watch). HW starts at the applied
        //     offset (F4).
        let initial: Vec<Option<Offset>> = reports
            .iter()
            .zip(floors)
            .map(|(r, floor)| {
                r.as_ref()
                    .ok()
                    .map(|rep| (*floor).max(rep.log_start).min(rep.leo))
            })
            .collect();
        let hw_vector = Arc::new(HwVector::new(&initial));
        let commit_batches = Arc::new(AtomicU64::new(0));
        let cores: Arc<Vec<OnceLock<Arc<ReplicaCore>>>> =
            Arc::new((0..p).map(|_| OnceLock::new()).collect());
        {
            let counter = Arc::clone(&commit_batches);
            let cores = Arc::clone(&cores);
            wal.set_commit_callback(Arc::new(
                move |topic: &str, partition: i32, _min: i64, max: i64| {
                    if topic != TOPIC {
                        return;
                    }
                    counter.fetch_add(1, Ordering::Relaxed);
                    let Ok(pid) = u16::try_from(partition).map(PartitionId) else {
                        return;
                    };
                    let Some(durable_end) = max.checked_add(1).and_then(Offset::from_i64) else {
                        return;
                    };
                    if let Some(core) = cores.get(pid.index()).and_then(OnceLock::get) {
                        core.on_durable(durable_end);
                    }
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
                    let log = Arc::new(PartitionLog::new(
                        pid,
                        &cfg,
                        Arc::clone(&wal),
                        Arc::clone(&hw_vector),
                        rep,
                    ));
                    // Registered before the log is handed out, so before any append.
                    let _ = cores[pid.index()].set(log.core());
                    Ok(log)
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
                hw_vector,
            },
            logs,
        ))
    }

    /// Number of WAL group commits (commit-callback invocations) since open, all partitions.
    pub fn commit_batches(&self) -> u64 {
        self.commit_batches.load(Ordering::Relaxed)
    }

    /// The consistent cut: every partition's HW, read atomically under one lock (W7).
    ///
    /// Causally closed: an HW enters the cut before anyone can observe it through the HW
    /// watch (see [`HwVector`]), so if the cut contains a record that was appended after
    /// observing another partition's HW `x`, it also contains `x`. A partition whose
    /// recovery failed reports 0.
    pub fn cut(&self) -> Vec<Offset> {
        self.hw_vector.cut()
    }

    /// The node's [`HwVector`] (every partition's replica core publishes through it; tests
    /// use it for the negative control).
    pub fn hw_vector(&self) -> &Arc<HwVector> {
        &self.hw_vector
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
