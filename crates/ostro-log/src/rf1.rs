//! `Rf1Log`: the single-node (RF=1, ISR = {self}) [`ReplicatedLog`] (STOR-04, STOR-01, STOR-07,
//! STOR-08).
//!
//! - **Sequencer** (research Pattern 5, W8): one `tokio::sync::Mutex` per partition holds
//!   `next` and the current epoch. Under it: health check, stale-epoch check, envelope build,
//!   `append_enqueued(.., acks = 1)`; `next` advances **only** after the enqueue succeeded, so
//!   a rejected or failed enqueue never leaves a gap. The lock is released before the fsync,
//!   so concurrent appends share group commits. A `Semaphore(max_inflight_appends)` bounds
//!   the batches between enqueue and fsync.
//! - **HW** (W9): published by the node's WAL commit callback (after fsync) through
//!   [`compute_hw`](crate::compute_hw) and [`HwVector::publish_hw`] (the single HW
//!   publication point, which also feeds the consistent cut); see [`LogNode`](crate::LogNode).
//!   Truncation lowers it through [`HwVector::retract_hw`].
//! - **Fail-stop** (STOR-07): a WAL write/fsync error (the vendored WAL poisons the partition)
//!   turns health `Failed` and role `Offline` exactly once; waiters get an error, later appends
//!   fail, the HW never covers the failed batch. There is no retry and no reopen: the
//!   partition stays failed until the node restarts and recovery runs.
//! - **Reads** go through [`verify_record`] for every record (frame CRC + envelope CRC +
//!   continuity), from the WAL tail cache first, then the segment index. A batch the readers
//!   cannot produce below HW fails the partition; a read is never silently short.
//! - **Trim** deletes whole segments below a barrier-minted floor; **truncate** delegates to
//!   the WAL's quiescing suffix truncation (W13).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use ostro_types::envelope::{self, HEADER_LEN};
use ostro_types::{Epoch, Offset, PartitionId};
use ostro_wal::group_commit::fsync_dir_blocking;
use ostro_wal::segment_index::PartitionIndex;
use ostro_wal::truncate::{RECORD_HEADER_LEN, declared_record_size, first_record_range};
use ostro_wal::{GroupCommitWal, WalError, WalRecord};
use tokio::sync::{Mutex, RwLock, Semaphore, watch};
use tracing::{error, info, warn};

use crate::barrier::TrimPermit;
use crate::cut::HwVector;
use crate::epoch_cache::EpochCache;
use crate::error::LogError;
use crate::node::LogNodeConfig;
use crate::recovery::{RecoveryReport, TOPIC, list_segments, segment_dir};
use crate::replicated::{
    AppendReceipt, EpochHistory, Isr, LogHealth, RawBatch, ReplicatedLog, Role, push_raw,
};
use crate::verify::{VerifiedBatch, verify_record};

/// Sequencer state, guarded by the per-partition async mutex.
struct SeqState {
    /// Next offset to assign (the LEO).
    next: Offset,
    /// Highest epoch accepted so far.
    current_epoch: Epoch,
}

/// Health/role/truncation watches, shared with the per-append commit watchers.
struct Status {
    pid: PartitionId,
    health: watch::Sender<LogHealth>,
    role: watch::Sender<Role>,
    /// Bumped on every truncation, so waiters for truncated offsets wake up.
    truncations: watch::Sender<u64>,
    /// Serializes [`Status::fail`] and [`Status::set_role`], so a role update cannot slip in
    /// between the health check and the fail-stop (which would leave a failed partition
    /// `Leader`). Never held across an await.
    transition: std::sync::Mutex<()>,
}

impl Status {
    /// Role → `Offline`, then health → `Failed`, exactly once.
    ///
    /// Role goes first: anyone woken by health `Failed` must already observe role `Offline`
    /// (publishing health first let a health waiter read a stale `Leader`).
    fn fail(&self, reason: &str) {
        let _t = self.transition.lock().unwrap_or_else(|e| e.into_inner());
        if self.check().is_err() {
            return;
        }
        self.role.send_replace(Role::Offline);
        self.health.send_replace(LogHealth::Failed {
            reason: reason.to_string(),
        });
        error!(
            partition = self.pid.0,
            reason, "partition log failed (fail-stop until restart)"
        );
    }

    /// Set the role unless the partition has failed (it then stays `Offline`).
    fn set_role(&self, role: Role) {
        let _t = self.transition.lock().unwrap_or_else(|e| e.into_inner());
        if self.check().is_ok() {
            self.role.send_replace(role);
        }
    }

    fn check(&self) -> Result<(), LogError> {
        match &*self.health.borrow() {
            LogHealth::Healthy => Ok(()),
            LogHealth::Failed { reason } => Err(LogError::Failed(reason.clone())),
        }
    }
}

/// The RF=1 partition log. Created by [`LogNode::open`](crate::LogNode::open).
pub struct Rf1Log {
    pid: PartitionId,
    node_id: u64,
    wal: Arc<GroupCommitWal>,
    seg_dir: PathBuf,
    max_envelope_bytes: usize,
    max_inflight: u32,
    inflight: Arc<Semaphore>,
    seq: Mutex<SeqState>,
    /// Mirror of `seq.next` for lock-free `log_end`.
    leo: AtomicU64,
    log_start: AtomicU64,
    /// This partition's HW watch (read side; written only through `hw_vector`).
    hw: Arc<watch::Sender<Offset>>,
    /// The node's HW publication point (truncation lowers the HW through it).
    hw_vector: Arc<HwVector>,
    epochs: std::sync::Mutex<EpochCache>,
    status: Arc<Status>,
    /// Reads take it shared; trim and truncate take it exclusive (segment files change).
    layout: RwLock<()>,
    /// Lazily built segment index (reset when segments are deleted or cut).
    index: Mutex<Option<PartitionIndex>>,
    recovery: RecoveryReport,
}

impl std::fmt::Debug for Rf1Log {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rf1Log")
            .field("partition", &self.pid)
            .field("log_start", &self.log_start())
            .field("hw", &*self.hw.borrow())
            .field("leo", &self.log_end())
            .finish_non_exhaustive()
    }
}

impl Rf1Log {
    /// Build from a successful recovery. `hw_vector` holds the HW watch the node's commit
    /// callback advances; it starts at `recovery.leo` (recovery left every retained segment
    /// durable).
    /// The sequencer starts at `recovery.leo` too, so an empty log anchored at the floor
    /// (Phase 12 F3) assigns its first append exactly that offset.
    pub(crate) fn new(
        pid: PartitionId,
        cfg: &LogNodeConfig,
        wal: Arc<GroupCommitWal>,
        hw_vector: Arc<HwVector>,
        recovery: RecoveryReport,
    ) -> Self {
        let hw = hw_vector
            .sender(pid)
            .expect("hw watch exists for a recovered partition");
        let (node_id, max_envelope_bytes) = (cfg.node_id, cfg.max_envelope_bytes);
        let epochs = EpochCache::from_pairs(&recovery.epochs);
        let current_epoch = epochs.latest_epoch().unwrap_or_default();
        let max_inflight = u32::try_from(cfg.max_inflight_appends.max(1)).unwrap_or(u32::MAX);
        let max_inflight =
            max_inflight.min(u32::try_from(Semaphore::MAX_PERMITS).unwrap_or(u32::MAX));
        Self {
            pid,
            node_id,
            wal,
            seg_dir: segment_dir(&cfg.wal_dir, pid),
            max_envelope_bytes,
            max_inflight,
            inflight: Arc::new(Semaphore::new(max_inflight as usize)),
            seq: Mutex::new(SeqState {
                next: recovery.leo,
                current_epoch,
            }),
            leo: AtomicU64::new(recovery.leo.0),
            log_start: AtomicU64::new(recovery.log_start.0),
            hw,
            hw_vector,
            epochs: std::sync::Mutex::new(epochs),
            status: Arc::new(Status {
                pid,
                health: watch::channel(LogHealth::Healthy).0,
                role: watch::channel(Role::Offline).0,
                truncations: watch::channel(0).0,
                transition: std::sync::Mutex::new(()),
            }),
            layout: RwLock::new(()),
            index: Mutex::new(None),
            recovery,
        }
    }

    /// What recovery found at open.
    pub fn recovery(&self) -> &RecoveryReport {
        &self.recovery
    }

    /// Seed the epoch history of a log with **no records** (Phase 12 F3, plan 05-21).
    ///
    /// An empty log recovers anchored at the floor (`log_start = leo = applied_offset`) with
    /// no epoch history, because the history is derived from envelopes and there are none.
    /// The caller passes the state's applied `LeaderAnnounced` history
    /// (`CursorMeta.epoch_history`, `(epoch, first offset)` ascending), so that
    /// [`epochs`](ReplicatedLog::epochs) answers as if the log still held those epochs, the
    /// next term picks an epoch above them, and appends with an older epoch are refused
    /// ([`LogError::StaleEpoch`]). Pairs are kept verbatim (their offsets lie at or below
    /// `log_start`; Phase 12 divergence checks need the history below the start).
    ///
    /// Refused with [`LogError::Failed`] once the log holds records (its own envelopes are
    /// then the history) or when the history is not strictly ascending in epoch and
    /// non-decreasing in offset. An empty `history` is a no-op.
    pub async fn seed_epochs(&self, history: &[(Epoch, Offset)]) -> Result<(), LogError> {
        if history.is_empty() {
            return Ok(());
        }
        let ascending = history
            .windows(2)
            .all(|w| w[0].0 < w[1].0 && w[0].1 <= w[1].1);
        if !ascending {
            return Err(LogError::Failed(format!(
                "partition {}: epoch history to seed is not ascending: {history:?}",
                self.pid
            )));
        }
        let mut seq = self.seq.lock().await;
        let mut epochs = self.epochs_lock();
        if seq.next != self.log_start() || !epochs.is_empty() {
            return Err(LogError::Failed(format!(
                "partition {}: cannot seed epochs of a non-empty log (log start {}, end {})",
                self.pid,
                self.log_start(),
                seq.next
            )));
        }
        *epochs = EpochCache::from_pairs(history);
        if let Some(latest) = epochs.latest_epoch()
            && latest > seq.current_epoch
        {
            seq.current_epoch = latest;
        }
        Ok(())
    }

    /// Set the role (ignored once the partition has failed: it stays `Offline`).
    pub fn set_role(&self, role: Role) {
        self.status.set_role(role);
    }

    fn hw_now(&self) -> Offset {
        *self.hw.borrow()
    }

    /// Fail-stop the partition.
    fn fail(&self, reason: &str) {
        self.status.fail(reason);
    }

    /// Health first, then the WAL's poison flag (which may be set before our commit watcher
    /// has run).
    fn ensure_healthy(&self) -> Result<(), LogError> {
        self.status.check()?;
        if self.wal.is_poisoned(TOPIC, self.pid.as_i32()) {
            self.fail("wal partition poisoned by an I/O error");
            return self.status.check();
        }
        Ok(())
    }

    fn epochs_lock(&self) -> std::sync::MutexGuard<'_, EpochCache> {
        self.epochs.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A batch below HW could not be produced or failed verification: fail the partition.
    fn read_failure(&self, at: Offset, what: &str) -> LogError {
        let reason = format!(
            "partition {} read at offset {at} below HW {}: {what}",
            self.pid,
            self.hw_now()
        );
        self.fail(&reason);
        LogError::Failed(reason)
    }

    /// Records starting at `from`: tail cache first, then the segment index.
    async fn fetch(
        &self,
        from: Offset,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<Vec<WalRecord>, String> {
        if let Some(recs) = self
            .wal
            .read_tail(
                TOPIC,
                self.pid.as_i32(),
                from.as_i64(),
                max_records,
                max_bytes,
            )
            .await
        {
            return Ok(recs);
        }
        let mut idx = self.index.lock().await;
        let index =
            idx.get_or_insert_with(|| PartitionIndex::new(self.seg_dir.clone(), self.pid.as_i32()));
        index
            .read_from_bounded(from.as_i64(), max_records, max_bytes)
            .await
            .map_err(|e| format!("segment read failed: {e}"))
    }
}

impl Rf1Log {
    /// The verified read loop behind [`ReplicatedLog::read`] and
    /// [`ReplicatedLog::read_envelopes`]: whole batches inside `[from, min(upto, HW))`, each
    /// verified, then mapped through `f` together with its WAL record.
    async fn read_with<T>(
        &self,
        from: Offset,
        upto: Offset,
        max_bytes: usize,
        mut f: impl FnMut(VerifiedBatch, WalRecord) -> T,
    ) -> Result<Vec<T>, LogError> {
        let _layout = self.layout.read().await;
        if from < self.log_start() || from > self.log_end() {
            return Err(LogError::OutOfRange(from));
        }
        let upto = upto.min(self.hw_now());
        let mut out: Vec<T> = Vec::new();
        if from >= upto {
            return Ok(out);
        }
        let mut next = from;
        let mut bytes = 0usize;
        'fill: while next < upto {
            let want = usize::try_from(upto - next).unwrap_or(usize::MAX);
            let budget = max_bytes.saturating_sub(bytes).max(1);
            let recs = match self.fetch(next, want, budget).await {
                Ok(r) => r,
                Err(e) => return Err(self.read_failure(next, &e)),
            };
            let mut progressed = false;
            for rec in recs {
                let (base, last) = (rec.get_base_offset(), rec.get_last_offset());
                if last < next.as_i64() {
                    continue;
                }
                if base < next.as_i64() {
                    if next == from && out.is_empty() {
                        // Caller error: `from` is inside a batch.
                        return Err(LogError::OutOfRange(from));
                    }
                    return Err(self.read_failure(next, "overlapping batch"));
                }
                let len = match &rec {
                    WalRecord::V2 { canonical_data, .. } => canonical_data.len(),
                    WalRecord::V1 { .. } => 0,
                };
                let batch = match verify_record(&rec, next, self.pid) {
                    Ok(b) => b,
                    Err(c) => return Err(self.read_failure(next, &c.to_string())),
                };
                if batch.end() > upto {
                    break 'fill;
                }
                if !out.is_empty() && bytes.saturating_add(len) > max_bytes {
                    break 'fill;
                }
                bytes = bytes.saturating_add(len);
                next = batch.end();
                out.push(f(batch, rec));
                progressed = true;
                if bytes >= max_bytes || next >= upto {
                    break 'fill;
                }
            }
            if !progressed {
                return Err(self.read_failure(next, "no batch starts at this offset"));
            }
        }
        Ok(out)
    }
}

/// Envelope size for `cmds` (see `envelope::encode`).
fn envelope_len(cmds: &[Bytes]) -> usize {
    HEADER_LEN + cmds.iter().map(|c| 4 + c.len()).sum::<usize>()
}

/// Base offset of the first record of a segment file (`None` when empty or unparseable).
fn first_base(path: &Path) -> std::io::Result<Option<Offset>> {
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let mut header = [0u8; RECORD_HEADER_LEN];
    match f.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let Some(len) = declared_record_size(&header) else {
        return Ok(None);
    };
    let mut buf = vec![0u8; len];
    buf[..RECORD_HEADER_LEN].copy_from_slice(&header);
    match f.read_exact(&mut buf[RECORD_HEADER_LEN..]) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    Ok(first_record_range(&buf).and_then(|(base, _)| Offset::from_i64(base)))
}

impl ReplicatedLog for Rf1Log {
    fn partition(&self) -> PartitionId {
        self.pid
    }

    async fn append(&self, epoch: Epoch, cmds: Vec<Bytes>) -> Result<AppendReceipt, LogError> {
        if cmds.is_empty() {
            return Err(LogError::Wal("empty batch".into()));
        }
        let size = envelope_len(&cmds);
        if size > self.max_envelope_bytes {
            // Callers split batches; nothing touched.
            return Err(LogError::Wal(format!(
                "envelope too large: {size} bytes > max {}",
                self.max_envelope_bytes
            )));
        }
        let count = i32::try_from(cmds.len())
            .map_err(|_| LogError::Wal("envelope too large: too many commands".into()))?;
        self.ensure_healthy()?;
        let permit = Arc::clone(&self.inflight)
            .acquire_owned()
            .await
            .map_err(|_| LogError::Failed("append semaphore closed".into()))?;

        let (receipt, wait) = {
            let mut seq = self.seq.lock().await;
            self.ensure_healthy()?;
            if epoch < seq.current_epoch {
                return Err(LogError::StaleEpoch {
                    current: seq.current_epoch,
                });
            }
            let base = seq.next;
            let last = base + (cmds.len() as u64 - 1);
            let env = envelope::encode(self.pid, epoch, base, &cmds);
            debug_assert_eq!(env.len(), size);
            let rec = WalRecord::new_v2(
                TOPIC.to_string(),
                self.pid.as_i32(),
                env,
                base.as_i64(),
                last.as_i64(),
                count,
            );
            // acks = 1: the CommitWait resolves after fsync. Never acks = 0.
            let wait = match self
                .wal
                .append_enqueued(TOPIC.to_string(), self.pid.as_i32(), rec, 1)
                .await
            {
                Ok(w) => w,
                Err(WalError::Backpressure(m)) => {
                    return Err(LogError::Wal(format!("backpressure: {m}")));
                }
                Err(e) => {
                    let reason = format!("wal append failed: {e}");
                    self.fail(&reason);
                    return Err(LogError::Failed(reason));
                }
            };
            // Only now, with the record enqueued, does the sequencer advance.
            if epoch > seq.current_epoch {
                seq.current_epoch = epoch;
            }
            self.epochs_lock().assign(epoch, base);
            seq.next = last.next();
            self.leo.store(seq.next.0, Ordering::Release);
            (AppendReceipt { base, last, epoch }, wait)
        };

        // Watch the fsync outside the lock; the permit is held until it resolves.
        let status = Arc::clone(&self.status);
        let generation = *self.status.truncations.borrow();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) = wait.wait().await {
                let truncated = *status.truncations.borrow() != generation
                    && !matches!(e, WalError::Poisoned(_));
                if !truncated {
                    status.fail(&format!(
                        "wal commit failed for offsets {}..={}: {e}",
                        receipt.base, receipt.last
                    ));
                }
            }
        });
        Ok(receipt)
    }

    async fn wait_committed(&self, last: Offset) -> Result<(), LogError> {
        let mut hw = self.hw.subscribe();
        let mut health = self.status.health.subscribe();
        let mut trunc = self.status.truncations.subscribe();
        let mut truncated = false;
        loop {
            health.borrow_and_update();
            trunc.borrow_and_update();
            if *hw.borrow_and_update() > last {
                return Ok(());
            }
            self.ensure_healthy()?;
            if last >= self.log_end() {
                return Err(if truncated {
                    LogError::Truncated
                } else {
                    LogError::OutOfRange(last)
                });
            }
            let closed = || LogError::Failed("partition log closed".into());
            tokio::select! {
                biased;
                r = hw.changed() => r.map_err(|_| closed())?,
                r = health.changed() => r.map_err(|_| closed())?,
                r = trunc.changed() => {
                    r.map_err(|_| closed())?;
                    truncated = true;
                }
            }
        }
    }

    fn high_watermark(&self) -> watch::Receiver<Offset> {
        self.hw.subscribe()
    }

    fn log_end(&self) -> Offset {
        // The commit callback can publish HW for a batch an instant before the appending
        // task records the new `next`; a committed record is in the log, so LEO >= HW.
        Offset(self.leo.load(Ordering::Acquire)).max(self.hw_now())
    }

    fn log_start(&self) -> Offset {
        Offset(self.log_start.load(Ordering::Acquire))
    }

    async fn read(
        &self,
        from: Offset,
        upto: Offset,
        max_bytes: usize,
    ) -> Result<Vec<VerifiedBatch>, LogError> {
        self.read_with(from, upto, max_bytes, |batch, _| batch)
            .await
    }

    async fn read_envelopes(
        &self,
        from: Offset,
        max_bytes: usize,
    ) -> Result<Vec<RawBatch>, LogError> {
        let pieces = self
            .read_with(from, Offset(u64::MAX), max_bytes, |batch, rec| {
                let env = match rec {
                    WalRecord::V2 { canonical_data, .. } => Bytes::from(canonical_data),
                    // verify_record accepts V2 records only.
                    WalRecord::V1 { .. } => Bytes::new(),
                };
                (batch.epoch, batch.base, batch.end(), env)
            })
            .await?;
        let mut out = Vec::new();
        for (epoch, base, end, env) in pieces {
            push_raw(&mut out, epoch, base, end, env);
        }
        Ok(out)
    }

    fn epochs(&self) -> EpochHistory {
        EpochHistory(self.epochs_lock().to_pairs())
    }

    fn isr(&self) -> Isr {
        Isr(vec![self.node_id])
    }

    fn role(&self) -> watch::Receiver<Role> {
        self.status.role.subscribe()
    }

    fn health(&self) -> watch::Receiver<LogHealth> {
        self.status.health.subscribe()
    }

    async fn truncate_to(&self, end: Offset) -> Result<Offset, LogError> {
        self.ensure_healthy()?;
        let _layout = self.layout.write().await;
        let mut seq = self.seq.lock().await;
        let leo = seq.next;
        if end >= leo {
            return Ok(leo);
        }
        let start = self.log_start();
        if end < start {
            return Err(LogError::OutOfRange(end));
        }
        // Quiesce: wait until every enqueued batch has resolved (its commit callback has run),
        // so no late callback can raise HW above the cut. The sequencer lock keeps new
        // appends out meanwhile.
        let _quiet = self
            .inflight
            .acquire_many(self.max_inflight)
            .await
            .map_err(|_| LogError::Failed("append semaphore closed".into()))?;
        let outcome = match self
            .wal
            .truncate_to(TOPIC, self.pid.as_i32(), end.as_i64())
            .await
        {
            Ok(o) => o,
            Err(e) => {
                let reason = format!("wal truncation failed: {e}");
                self.fail(&reason);
                return Err(LogError::Failed(reason));
            }
        };
        let new = outcome
            .new_log_end_offset
            .and_then(Offset::from_i64)
            .unwrap_or(start)
            .max(start);
        seq.next = new;
        self.leo.store(new.0, Ordering::Release);
        self.hw_vector.retract_hw(self.pid, new);
        self.epochs_lock().truncate_from_end(new);
        *self.index.lock().await = None;
        self.status.truncations.send_modify(|g| *g += 1);
        info!(
            partition = self.pid.0,
            requested = %end,
            new_leo = %new,
            segments_deleted = outcome.segments_deleted,
            bytes_discarded = outcome.bytes_discarded,
            "truncated partition log suffix"
        );
        Ok(new)
    }

    async fn trim_prefix(&self, permit: TrimPermit) -> Result<Offset, LogError> {
        self.ensure_healthy()?;
        let _layout = self.layout.write().await;
        let hw = self.hw_now();
        let mut floor = permit.floor();
        if floor > hw {
            warn!(
                partition = self.pid.0,
                floor = %floor,
                hw = %hw,
                "trim floor above HW; clamped to HW"
            );
            floor = hw;
        }
        let pid = self.pid;
        let dir = self.seg_dir.clone();
        let wal = Arc::clone(&self.wal);
        let active = wal.get_segment_state(TOPIC, pid.as_i32()).map(|(id, _)| id);
        let (deleted, first_kept) = tokio::task::spawn_blocking(move || -> Result<_, LogError> {
            let mut non_empty = Vec::new();
            for (id, path) in list_segments(&dir, pid)? {
                if let Some(base) = first_base(&path)? {
                    non_empty.push((id, base));
                }
            }
            // A segment's last offset is (next non-empty segment's first base - 1). Delete a
            // prefix of whole segments with last < floor; never the last non-empty segment,
            // never the WAL's active segment.
            let mut deleted = Vec::new();
            let mut first_kept = non_empty.first().map(|&(_, b)| b);
            for w in non_empty.windows(2) {
                let ((id, _), (_, next_base)) = (w[0], w[1]);
                if next_base > floor || Some(id) == active {
                    break;
                }
                wal.delete_segment_file(TOPIC, pid.as_i32(), id)
                    .map_err(|e| LogError::Wal(format!("delete segment {id}: {e}")))?;
                deleted.push(id);
                first_kept = Some(next_base);
            }
            fsync_dir_blocking(&dir).map_err(|e| LogError::Wal(format!("dir fsync: {e}")))?;
            Ok((deleted, first_kept))
        })
        .await
        .map_err(|e| LogError::Wal(format!("trim task failed: {e}")))??;

        let old_start = self.log_start();
        let new_start = first_kept.unwrap_or(old_start).max(old_start);
        if new_start > old_start {
            self.log_start.store(new_start.0, Ordering::Release);
            self.epochs_lock().truncate_from_start(new_start);
        }
        if !deleted.is_empty() {
            *self.index.lock().await = None;
        }
        info!(
            partition = pid.0,
            floor = %floor,
            segments_deleted = deleted.len(),
            log_start = %new_start,
            "trim pass{}",
            if deleted.is_empty() { ": nothing to trim" } else { "" }
        );
        Ok(new_start)
    }
}
