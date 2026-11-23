//! `PartitionLog`: the one [`ReplicatedLog`] implementation (STOR-04, STOR-01, STOR-07,
//! STOR-08, CLUS-02, CLUS-04; D5, D-P12-15).
//!
//! RF=1 is configuration, not a second type: a single-node partition is a leader whose
//! replica set is `[self]` with `min_insync = 1` ([`ReplicaSet::solo`]). Every HW advance,
//! at any replication factor, goes through the same steps.
//!
//! - **Sequencer** (research Pattern 5, W8): one `tokio::sync::Mutex` per partition holds
//!   `next` and the current epoch. Under it: health check, role and `min_insync` gate,
//!   stale-epoch check, envelope build, `append_enqueued(.., acks = 1)`; `next` advances
//!   **only** after the enqueue succeeded, so a rejected or failed enqueue never leaves a gap.
//!   The lock is released before the fsync, so concurrent appends share group commits. A
//!   `Semaphore(max_inflight_appends)` bounds the batches between enqueue and fsync.
//! - **HW** (W9, D-P12-5, F12a): the node's WAL commit callback (after fsync) reports the
//!   local durable end to this log's replica core. A leader feeds it to its
//!   [`ReplicaTracker`] and recomputes the HW with [`next_hw`] (KIP-497 maximal ISR,
//!   `min_insync` freeze); a follower takes `min(leader HW, local durable end)`. Either way
//!   a moved HW is published through [`HwVector::publish_hw`], the single publication point
//!   that also feeds the consistent cut, and counted in `hw_publishes`. Truncation lowers the
//!   HW through [`HwVector::retract_hw`].
//! - **HW at open** (F4) is the caller's applied offset (clamped into `[log_start, LEO]`),
//!   never the recovered LEO: a follower must not apply entries that may be uncommitted. A
//!   leader whose ISR is `{self}` raises it to its durable LEO the moment it becomes leader.
//! - **Roles**: a log opens unassigned (role `Offline`). [`become_leader`] /
//!   [`become_follower`] take a meta-assigned epoch; `become_leader(e)` requires
//!   `e > latest_epoch()` (B2). `append` needs the leader role ([`LogError::NotLeader`]) and
//!   `|published ISR| >= min_insync` ([`LogError::NotEnoughReplicas`]).
//! - **Follower primitives**: [`read_replica`] serves raw envelopes up to the local durable
//!   end (not HW-capped); [`append_replica`] writes a leader's envelopes verbatim after
//!   verifying them; [`reset_to`] empties and re-anchors the log for a snapshot install.
//! - **Fail-stop** (STOR-07): a WAL write/fsync error (the vendored WAL poisons the partition)
//!   turns health `Failed` and role `Offline` exactly once; waiters get an error, later appends
//!   fail, the HW never covers the failed batch. There is no retry and no reopen: the
//!   partition stays failed until the node restarts and recovery runs.
//! - **Reads** go through [`verify_record`] for every record (frame CRC + envelope CRC +
//!   continuity), from the WAL's durable-gated tail cache first, then the sparse segment
//!   index. A batch the readers cannot produce below the read cap fails the partition; a
//!   read is never silently short.
//! - **Trim** deletes whole segments below a barrier-minted floor; **truncate** delegates to
//!   the WAL's quiescing suffix truncation (W13).
//!
//! [`become_leader`]: ReplicatedLog::become_leader
//! [`become_follower`]: ReplicatedLog::become_follower
//! [`read_replica`]: ReplicatedLog::read_replica
//! [`append_replica`]: ReplicatedLog::append_replica
//! [`reset_to`]: ReplicatedLog::reset_to

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
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore, watch};
use tracing::{error, info, warn};

use crate::barrier::TrimPermit;
use crate::cut::HwVector;
use crate::epoch_cache::EpochCache;
use crate::error::LogError;
use crate::mutants;
use crate::node::{LogClock, LogNodeConfig};
use crate::recovery::{RecoveryReport, TOPIC, list_segments, segment_dir};
use crate::replica::isr::{IsrState, PendingChange};
use crate::replica::tracker::ReplicaTracker;
use crate::replica::watermark::{append_allowed, next_hw};
use crate::replicated::{
    AppendReceipt, EpochHistory, FollowerProgress, Isr, LogHealth, RawBatch, ReplicaSet,
    ReplicaStatus, ReplicatedLog, Role, push_raw,
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

/// The replica role this log plays.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// Opened, no meta assignment applied yet (role `Offline`).
    Unassigned,
    /// Leading `epoch`.
    Leader { epoch: Epoch },
    /// Following `leader` at `epoch`.
    Follower { epoch: Epoch, leader: u64 },
}

impl Mode {
    fn epoch(self) -> Option<Epoch> {
        match self {
            Mode::Unassigned => None,
            Mode::Leader { epoch } | Mode::Follower { epoch, .. } => Some(epoch),
        }
    }
}

/// Replication state, guarded by [`ReplicaCore::state`] (a std mutex, never held across an
/// await; lock order: sequencer → replica state → HW vector).
struct ReplicaState {
    mode: Mode,
    /// Local fsynced log end (exclusive).
    durable_end: Offset,
    /// Leader only: measured progress of every replica.
    tracker: Option<ReplicaTracker>,
    /// The published ISR (leader) and its CAS version.
    isr: IsrState,
    min_insync: usize,
    /// Follower only: the newest leader HW received.
    leader_hw: Offset,
}

/// The part of a [`PartitionLog`] the WAL commit callback reaches: durable end, tracker,
/// HW publication. Holds no WAL reference, so the callback (owned by the WAL) never keeps
/// the WAL alive through it.
pub(crate) struct ReplicaCore {
    pid: PartitionId,
    node_id: u64,
    /// This partition's HW watch (read side; written only through `hw_vector`).
    hw: Arc<watch::Sender<Offset>>,
    hw_vector: Arc<HwVector>,
    durable: watch::Sender<Offset>,
    hw_publishes: AtomicU64,
    /// Largest number of envelopes one fetch response of this leader carried since open.
    max_envelopes_per_fetch: std::sync::atomic::AtomicU32,
    clock: Arc<dyn LogClock>,
    lag_max_ms: u64,
    state: std::sync::Mutex<ReplicaState>,
}

impl ReplicaCore {
    fn lock(&self) -> std::sync::MutexGuard<'_, ReplicaState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn hw_now(&self) -> Offset {
        *self.hw.borrow()
    }

    /// The WAL commit callback: the local log is durable up to `durable_end` (F12a).
    pub(crate) fn on_durable(&self, durable_end: Offset) {
        let mut st = self.lock();
        if durable_end > st.durable_end {
            st.durable_end = durable_end;
        }
        let d = st.durable_end;
        // The durable watch is written only under the replica lock, so this is race-free.
        if *self.durable.borrow() < d {
            self.durable.send_replace(d);
        }
        if crate::replica::mutant(mutants::RF1_BYPASS) {
            // Mutant: the RF=1 shortcut, straight from the WAL callback to the HW.
            self.hw_vector.publish_hw(self.pid, durable_end);
            return;
        }
        match st.mode {
            Mode::Leader { .. } => {
                if let Some(t) = st.tracker.as_mut() {
                    t.on_local_durable(d);
                }
                self.republish(&mut st);
            }
            Mode::Follower { .. } => self.republish(&mut st),
            Mode::Unassigned => {}
        }
    }

    /// Recompute the HW for the current role and publish it if it moved (the only caller of
    /// `publish_hw` for this partition, apart from [`reset_to`](ReplicatedLog::reset_to)).
    fn republish(&self, st: &mut ReplicaState) {
        let prev = self.hw_now();
        let next = match st.mode {
            Mode::Leader { .. } => {
                let Some(t) = st.tracker.as_ref() else {
                    return;
                };
                // Never above what this leader holds durably itself.
                next_hw(prev, &t.progress(), &st.isr, st.min_insync).min(st.durable_end)
            }
            Mode::Follower { .. } => st.leader_hw.min(st.durable_end),
            Mode::Unassigned => return,
        };
        if next > prev {
            self.publish(next);
        }
    }

    fn publish(&self, hw: Offset) {
        self.hw_vector.publish_hw(self.pid, hw);
        self.hw_publishes.fetch_add(1, Ordering::Relaxed);
    }

    /// After a suffix truncation to `new`: lower the durable end and the HW, and restart the
    /// leader's own progress at `new` (the tracker never lowers it by itself).
    fn on_truncated(&self, new: Offset) {
        let mut st = self.lock();
        if st.durable_end > new {
            st.durable_end = new;
        }
        if *self.durable.borrow() > new {
            self.durable.send_replace(new);
        }
        if let Some(t) = st.tracker.as_mut() {
            let replicas = t.replicas();
            *t = ReplicaTracker::new(self.node_id, &replicas, new, self.clock.now_ms());
        }
        if st.leader_hw > new {
            st.leader_hw = new;
        }
        self.hw_vector.retract_hw(self.pid, new);
    }

    /// After [`reset_to`](ReplicatedLog::reset_to): durable end = HW = `start`.
    fn on_reset(&self, start: Offset) {
        let mut st = self.lock();
        st.durable_end = start;
        st.leader_hw = start;
        self.durable.send_replace(start);
        let hw = self.hw_now();
        if start < hw {
            self.hw_vector.retract_hw(self.pid, start);
        } else if start > hw {
            self.publish(start);
        }
    }
}

/// The leader's ISR decision inputs ([`PartitionLog::isr_view`]).
#[derive(Clone, Debug)]
pub struct IsrView {
    /// The epoch this replica leads.
    pub epoch: Epoch,
    /// Measured progress of every replica.
    pub tracker: ReplicaTracker,
    /// Published ISR, its CAS version and the in-flight change.
    pub isr: IsrState,
    /// Minimum ISR size.
    pub min_insync: usize,
    /// Current HW.
    pub hw: Offset,
    /// First offset of the leader's epoch (KIP-101 expansion floor).
    pub epoch_start: Offset,
}

/// A partition log (leader or follower). Created by [`LogNode::open`](crate::LogNode::open).
pub struct PartitionLog {
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
    core: Arc<ReplicaCore>,
    epochs: std::sync::Mutex<EpochCache>,
    status: Arc<Status>,
    /// Reads take it shared; trim, truncate and reset take it exclusive (segment files change).
    layout: RwLock<()>,
    /// Lazily built segment index (reset when segments are deleted or cut).
    index: Mutex<Option<PartitionIndex>>,
    recovery: RecoveryReport,
}

impl std::fmt::Debug for PartitionLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionLog")
            .field("partition", &self.pid)
            .field("log_start", &self.log_start())
            .field("hw", &self.hw_now())
            .field("leo", &self.log_end())
            .finish_non_exhaustive()
    }
}

impl PartitionLog {
    /// Build from a successful recovery. `hw_vector` holds the HW watch, which starts at the
    /// caller's applied offset (F4, see [`LogNode::open`](crate::LogNode::open)); the durable
    /// end starts at `recovery.leo` (recovery left every retained segment durable).
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
        let core = Arc::new(ReplicaCore {
            pid,
            node_id,
            hw,
            hw_vector,
            durable: watch::channel(recovery.leo).0,
            hw_publishes: AtomicU64::new(0),
            max_envelopes_per_fetch: std::sync::atomic::AtomicU32::new(0),
            clock: Arc::clone(&cfg.clock),
            lag_max_ms: cfg.replica_lag_max_ms,
            state: std::sync::Mutex::new(ReplicaState {
                mode: Mode::Unassigned,
                durable_end: recovery.leo,
                tracker: None,
                isr: IsrState::new(Vec::new(), 0),
                min_insync: 1,
                leader_hw: Offset::ZERO,
            }),
        });
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
            core,
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

    /// The replica core the node's commit callback reports durability to.
    pub(crate) fn core(&self) -> Arc<ReplicaCore> {
        Arc::clone(&self.core)
    }

    /// What recovery found at open.
    pub fn recovery(&self) -> &RecoveryReport {
        &self.recovery
    }

    /// HW publications through the replica core since open (D-P12-15: every HW advance,
    /// RF=1 included, is one publication).
    pub fn hw_publishes(&self) -> u64 {
        self.core.hw_publishes.load(Ordering::Relaxed)
    }

    /// Record that one fetch response of this leader carried `envelopes` envelopes
    /// (`ReplicaStatus::max_envelopes_per_fetch` is the maximum since open).
    pub fn note_fetch_envelopes(&self, envelopes: usize) {
        let n = u32::try_from(envelopes).unwrap_or(u32::MAX);
        self.core
            .max_envelopes_per_fetch
            .fetch_max(n, Ordering::Relaxed);
    }

    /// The largest fetch response (in envelopes) this leader served since open.
    pub fn max_envelopes_per_fetch(&self) -> u32 {
        self.core.max_envelopes_per_fetch.load(Ordering::Relaxed)
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
        check_ascending(self.pid, history)?;
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

    /// Leader only: the inputs of the next ISR decision (12-11 ISR publisher): the
    /// tracker, the ISR state (published + pending), the HW and the start offset of the
    /// leader's epoch (its first record, or the LEO before one was written). `None` off the
    /// leader role.
    pub fn isr_view(&self) -> Option<IsrView> {
        let st = self.core.lock();
        let Mode::Leader { epoch } = st.mode else {
            return None;
        };
        let tracker = st.tracker.clone()?;
        let epoch_start = self
            .epochs_lock()
            .to_pairs()
            .iter()
            .find(|(e, _)| *e == epoch)
            .map_or_else(|| self.log_end(), |(_, s)| *s);
        Some(IsrView {
            epoch,
            tracker,
            isr: st.isr.clone(),
            min_insync: st.min_insync,
            hw: self.hw_now(),
            epoch_start,
        })
    }

    /// Leader of `epoch`: mark `change` as the one in-flight `AlterIsr` (KIP-497: from now
    /// on the HW counts the maximal ISR, a pending add included, a pending remove still
    /// counted). `Ok(false)` while another change is pending.
    pub fn propose_isr_change(
        &self,
        epoch: Epoch,
        change: PendingChange,
    ) -> Result<bool, LogError> {
        let mut st = self.core.lock();
        Self::check_leader_of(&st, epoch)?;
        let accepted = st.isr.propose(change);
        self.core.republish(&mut st);
        Ok(accepted)
    }

    /// Leader of `epoch`: drop the in-flight `AlterIsr` (meta rejected it, or its outcome
    /// became visible and [`set_published_isr`](ReplicatedLog::set_published_isr) adopted
    /// the result). The HW then follows the published ISR alone.
    pub fn clear_isr_change(&self, epoch: Epoch) -> Result<(), LogError> {
        let mut st = self.core.lock();
        Self::check_leader_of(&st, epoch)?;
        st.isr.reject();
        self.core.republish(&mut st);
        Ok(())
    }

    fn check_leader_of(st: &ReplicaState, epoch: Epoch) -> Result<(), LogError> {
        match st.mode {
            Mode::Leader { epoch: cur } if cur == epoch => Ok(()),
            Mode::Leader { epoch: cur } if cur > epoch => {
                Err(LogError::StaleEpoch { current: cur })
            }
            Mode::Follower { leader, .. } => Err(LogError::NotLeader {
                leader: Some(leader),
            }),
            _ => Err(LogError::NotLeader { leader: None }),
        }
    }

    fn hw_now(&self) -> Offset {
        self.core.hw_now()
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

    /// The leader gate of [`append`](ReplicatedLog::append): the leader epoch, or
    /// `NotLeader` / `NotEnoughReplicas`.
    fn leader_gate(&self) -> Result<Epoch, LogError> {
        let st = self.core.lock();
        let epoch = match st.mode {
            Mode::Leader { epoch } => epoch,
            Mode::Follower { leader, .. } => {
                return Err(LogError::NotLeader {
                    leader: Some(leader),
                });
            }
            Mode::Unassigned => return Err(LogError::NotLeader { leader: None }),
        };
        // The append half of `ignore_min_insync` (the HW half is in `next_hw`).
        let allowed = crate::replica::mutant(mutants::IGNORE_MIN_INSYNC)
            || append_allowed(&st.isr, st.min_insync);
        if !allowed {
            return Err(LogError::NotEnoughReplicas {
                isr: st.isr.published.len(),
                min: st.min_insync,
            });
        }
        Ok(epoch)
    }

    /// A batch below the read cap could not be produced or failed verification: fail the
    /// partition.
    fn read_failure(&self, at: Offset, what: &str) -> LogError {
        let reason = format!(
            "partition {} read at offset {at} below HW {}: {what}",
            self.pid,
            self.hw_now()
        );
        self.fail(&reason);
        LogError::Failed(reason)
    }

    /// Records starting at `from`: the durable-gated tail cache first, then the sparse
    /// segment index.
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

    /// Enqueue one record (`acks = 1`) and spawn its commit watcher; the caller holds the
    /// sequencer lock and `permit`. Returns `Err` with nothing touched in the log.
    async fn enqueue(
        &self,
        rec: WalRecord,
        permit: OwnedSemaphorePermit,
        range: (Offset, Offset),
    ) -> Result<(), LogError> {
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
                        range.0, range.1
                    ));
                }
            }
        });
        Ok(())
    }

    async fn permit(&self) -> Result<OwnedSemaphorePermit, LogError> {
        Arc::clone(&self.inflight)
            .acquire_owned()
            .await
            .map_err(|_| LogError::Failed("append semaphore closed".into()))
    }

    /// Verify one leader envelope for [`append_replica`](ReplicatedLog::append_replica) and
    /// wrap it, byte for byte, in a local WAL V2 frame.
    fn replica_record(
        &self,
        env: &Bytes,
        expect_base: Offset,
        batch_epoch: u32,
        min_epoch: Epoch,
        max_epoch: Epoch,
    ) -> Result<(WalRecord, VerifiedBatch), LogError> {
        let bad =
            |what: String| LogError::Wal(format!("replica envelope at {expect_base}: {what}"));
        if env.len() > self.max_envelope_bytes {
            return Err(bad(format!(
                "envelope too large: {} bytes > max {}",
                env.len(),
                self.max_envelope_bytes
            )));
        }
        let header = envelope::peek_header(env).map_err(|e| bad(e.to_string()))?;
        if header.cmd_count == 0 {
            return Err(bad("empty envelope".into()));
        }
        let count = i32::try_from(header.cmd_count).map_err(|_| bad("too many commands".into()))?;
        let last = header.base + (u64::from(header.cmd_count) - 1);
        let rec = WalRecord::new_v2(
            TOPIC.to_string(),
            self.pid.as_i32(),
            env.to_vec(),
            header.base.as_i64(),
            last.as_i64(),
            count,
        );
        // CRC32C, partition, base == LEO, header vs trailer.
        let batch = verify_record(&rec, expect_base, self.pid).map_err(|c| bad(c.to_string()))?;
        if batch.epoch.0 != batch_epoch {
            return Err(bad(format!(
                "envelope epoch {} inside a batch of epoch {batch_epoch}",
                batch.epoch.0
            )));
        }
        if batch.epoch < min_epoch {
            return Err(bad(format!(
                "epoch regression: {} after {}",
                batch.epoch.0, min_epoch.0
            )));
        }
        if batch.epoch > max_epoch {
            return Err(bad(format!(
                "epoch {} above the followed epoch {}",
                batch.epoch.0, max_epoch.0
            )));
        }
        Ok((rec, batch))
    }
}

/// `history` must be strictly ascending in epoch and non-decreasing in offset.
fn check_ascending(pid: PartitionId, history: &[(Epoch, Offset)]) -> Result<(), LogError> {
    let ascending = history
        .windows(2)
        .all(|w| w[0].0 < w[1].0 && w[0].1 <= w[1].1);
    if ascending {
        Ok(())
    } else {
        Err(LogError::Failed(format!(
            "partition {pid}: epoch history to seed is not ascending: {history:?}"
        )))
    }
}

impl PartitionLog {
    /// The verified read loop behind [`ReplicatedLog::read`], [`ReplicatedLog::read_envelopes`]
    /// and [`ReplicatedLog::read_replica`]: whole batches inside `[from, min(upto, cap))`,
    /// each verified, then mapped through `f` together with its WAL record. At least one
    /// batch is returned when one fits below the cap, even if it is larger than `max_bytes`.
    async fn read_with<T>(
        &self,
        from: Offset,
        upto: Offset,
        cap: impl FnOnce() -> Offset,
        max_bytes: usize,
        mut f: impl FnMut(VerifiedBatch, WalRecord) -> T,
    ) -> Result<Vec<T>, LogError> {
        let _layout = self.layout.read().await;
        if from < self.log_start() || from > self.log_end() {
            return Err(LogError::OutOfRange(from));
        }
        let upto = upto.min(cap());
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

    /// Raw envelopes of whole batches in `[from, min(cap))`, grouped by epoch.
    async fn read_raw(
        &self,
        from: Offset,
        cap: impl FnOnce() -> Offset,
        max_bytes: usize,
    ) -> Result<Vec<RawBatch>, LogError> {
        let pieces = self
            .read_with(from, Offset(u64::MAX), cap, max_bytes, |batch, rec| {
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

impl ReplicatedLog for PartitionLog {
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
        self.leader_gate()?;
        let permit = self.permit().await?;

        let mut seq = self.seq.lock().await;
        self.ensure_healthy()?;
        let leader_epoch = self.leader_gate()?;
        let current = seq.current_epoch.max(leader_epoch);
        if epoch < current {
            return Err(LogError::StaleEpoch { current });
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
        self.enqueue(rec, permit, (base, last)).await?;
        // Only now, with the record enqueued, does the sequencer advance.
        if epoch > seq.current_epoch {
            seq.current_epoch = epoch;
        }
        self.epochs_lock().assign(epoch, base);
        seq.next = last.next();
        self.leo.store(seq.next.0, Ordering::Release);
        Ok(AppendReceipt { base, last, epoch })
    }

    async fn wait_committed(&self, last: Offset) -> Result<(), LogError> {
        let mut hw = self.core.hw.subscribe();
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
        self.core.hw.subscribe()
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
        self.read_with(from, upto, || self.hw_now(), max_bytes, |batch, _| batch)
            .await
    }

    async fn read_envelopes(
        &self,
        from: Offset,
        max_bytes: usize,
    ) -> Result<Vec<RawBatch>, LogError> {
        self.read_raw(from, || self.hw_now(), max_bytes).await
    }

    fn epochs(&self) -> EpochHistory {
        EpochHistory(self.epochs_lock().to_pairs())
    }

    fn isr(&self) -> Isr {
        let st = self.core.lock();
        match st.mode {
            Mode::Leader { .. } => Isr(st.isr.published.clone()),
            // Not leading: this replica only knows itself.
            _ => Isr(vec![self.node_id]),
        }
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
        // A correct leader never asks a follower to drop committed data.
        if matches!(self.core.lock().mode, Mode::Follower { .. }) && end < self.hw_now() {
            return Err(LogError::Failed(format!(
                "truncate below HW: {end} < {}",
                self.hw_now()
            )));
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
        self.core.on_truncated(new);
        let latest = {
            let mut epochs = self.epochs_lock();
            epochs.truncate_from_end(new);
            epochs.latest_epoch()
        };
        // A follower that truncated a divergent suffix (KIP-279) accepts the leader's records
        // from the newest epoch it still holds: the dropped epochs no longer bound
        // `append_replica` (their records are gone). Leaders keep their fence.
        if matches!(self.core.lock().mode, Mode::Follower { .. }) {
            seq.current_epoch = latest.unwrap_or_default();
        }
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

    // ---- Phase 12 replica API (12-08). ----

    async fn read_replica(
        &self,
        from: Offset,
        max_bytes: usize,
    ) -> Result<Vec<RawBatch>, LogError> {
        self.ensure_healthy()?;
        // Capped at the local durable end, never at HW: a follower must be able to fetch
        // what the HW is waiting for.
        self.read_raw(from, || self.durable_end(), max_bytes).await
    }

    async fn append_replica(&self, batches: Vec<RawBatch>) -> Result<Offset, LogError> {
        self.ensure_healthy()?;
        let not_follower =
            || LogError::Failed("append_replica: this replica is not a follower".into());
        let followed = |log: &Self| match log.core.lock().mode {
            Mode::Follower { epoch, .. } => Some(epoch),
            _ => None,
        };
        followed(self).ok_or_else(not_follower)?;
        let mut seq = self.seq.lock().await;
        self.ensure_healthy()?;
        let max_epoch = followed(self).ok_or_else(not_follower)?;
        for batch in &batches {
            for env in &batch.envelopes {
                let min_epoch = self
                    .epochs_lock()
                    .latest_epoch()
                    .unwrap_or_default()
                    .max(seq.current_epoch);
                let (rec, verified) =
                    self.replica_record(env, seq.next, batch.epoch, min_epoch, max_epoch)?;
                let (base, last) = (verified.base, verified.last());
                let permit = self.permit().await?;
                self.enqueue(rec, permit, (base, last)).await?;
                if verified.epoch > seq.current_epoch {
                    seq.current_epoch = verified.epoch;
                }
                self.epochs_lock().assign(verified.epoch, base);
                seq.next = last.next();
                self.leo.store(seq.next.0, Ordering::Release);
            }
        }
        Ok(seq.next)
    }

    fn durable_end(&self) -> Offset {
        *self.core.durable.borrow()
    }

    fn durable_end_watch(&self) -> watch::Receiver<Offset> {
        self.core.durable.subscribe()
    }

    fn latest_epoch(&self) -> Option<Epoch> {
        self.epochs_lock().latest_epoch()
    }

    fn become_leader(&self, epoch: Epoch, set: ReplicaSet) -> Result<(), LogError> {
        self.status.check()?;
        if let Some(latest) = self.latest_epoch()
            && epoch <= latest
        {
            return Err(LogError::StaleEpoch { current: latest });
        }
        if !set.isr.contains(&self.node_id) {
            return Err(LogError::Failed(format!(
                "partition {}: leader {} is not in its published ISR {:?}",
                self.pid, self.node_id, set.isr
            )));
        }
        let mut st = self.core.lock();
        match st.mode {
            Mode::Leader { epoch: cur } if epoch <= cur => {
                return Err(LogError::StaleEpoch { current: cur });
            }
            Mode::Follower { epoch: cur, .. } if epoch < cur => {
                return Err(LogError::StaleEpoch { current: cur });
            }
            _ => {}
        }
        let mut replicas = set.replicas.clone();
        for r in &set.isr {
            if !replicas.contains(r) {
                replicas.push(*r);
            }
        }
        let now = self.core.clock.now_ms();
        st.tracker = Some(ReplicaTracker::new(
            self.node_id,
            &replicas,
            st.durable_end,
            now,
        ));
        st.isr = IsrState::new(set.isr, set.isr_version);
        st.min_insync = usize::from(set.min_insync.max(1));
        st.mode = Mode::Leader { epoch };
        self.status.set_role(Role::Leader { epoch });
        // ISR {self}: the HW rises to the durable LEO at once.
        self.core.republish(&mut st);
        info!(
            partition = self.pid.0,
            epoch = epoch.0,
            isr = ?st.isr.published,
            min_insync = st.min_insync,
            hw = %self.hw_now(),
            "partition log leads"
        );
        Ok(())
    }

    fn become_follower(&self, epoch: Epoch, leader: u64) -> Result<(), LogError> {
        self.status.check()?;
        if leader == self.node_id || leader == 0 {
            return Err(LogError::Failed(format!(
                "partition {}: cannot follow leader {leader}",
                self.pid
            )));
        }
        if let Some(latest) = self.latest_epoch()
            && epoch < latest
        {
            return Err(LogError::StaleEpoch { current: latest });
        }
        let mut st = self.core.lock();
        match st.mode {
            Mode::Leader { epoch: cur } if epoch <= cur => {
                return Err(LogError::StaleEpoch { current: cur });
            }
            Mode::Follower { epoch: cur, .. } if epoch < cur => {
                return Err(LogError::StaleEpoch { current: cur });
            }
            _ => {}
        }
        st.tracker = None;
        st.leader_hw = st.leader_hw.max(self.hw_now());
        st.mode = Mode::Follower { epoch, leader };
        self.status.set_role(Role::Follower { epoch });
        Ok(())
    }

    fn set_follower_hw(&self, leader_hw: Offset) {
        let mut st = self.core.lock();
        if !matches!(st.mode, Mode::Follower { .. }) {
            return;
        }
        st.leader_hw = st.leader_hw.max(leader_hw);
        self.core.republish(&mut st);
    }

    fn record_progress(&self, p: FollowerProgress) -> Result<(), LogError> {
        let mut st = self.core.lock();
        match st.mode {
            Mode::Leader { .. } => {}
            Mode::Follower { leader, .. } => {
                return Err(LogError::NotLeader {
                    leader: Some(leader),
                });
            }
            Mode::Unassigned => return Err(LogError::NotLeader { leader: None }),
        }
        if let Some(t) = st.tracker.as_mut() {
            t.on_fetch(
                p.replica,
                p.fetch_offset,
                p.durable_leo,
                p.leader_durable_end,
                p.now_ms,
            );
        }
        self.core.republish(&mut st);
        Ok(())
    }

    fn set_published_isr(&self, epoch: Epoch, set: ReplicaSet) -> Result<(), LogError> {
        let mut st = self.core.lock();
        let cur = match st.mode {
            Mode::Leader { epoch } => epoch,
            Mode::Follower { leader, .. } => {
                return Err(LogError::NotLeader {
                    leader: Some(leader),
                });
            }
            Mode::Unassigned => return Err(LogError::NotLeader { leader: None }),
        };
        if epoch < cur {
            return Err(LogError::StaleEpoch { current: cur });
        }
        if epoch > cur {
            return Err(LogError::NotLeader { leader: None });
        }
        if !set.isr.contains(&self.node_id) {
            return Err(LogError::Failed(format!(
                "partition {}: published ISR {:?} does not contain the leader {}",
                self.pid, set.isr, self.node_id
            )));
        }
        if set.isr_version < st.isr.version {
            // An older publication arriving late: the newer one stays.
            return Ok(());
        }
        st.isr.set_published(set.isr, set.isr_version);
        st.min_insync = usize::from(set.min_insync.max(1));
        self.core.republish(&mut st);
        Ok(())
    }

    async fn reset_to(&self, start: Offset, epochs: EpochHistory) -> Result<(), LogError> {
        self.ensure_healthy()?;
        if matches!(self.core.lock().mode, Mode::Leader { .. }) {
            return Err(LogError::Failed(format!(
                "partition {}: reset_to on a leader",
                self.pid
            )));
        }
        if i64::try_from(start.0).is_err() {
            return Err(LogError::OutOfRange(start));
        }
        check_ascending(self.pid, &epochs.0)?;
        let _layout = self.layout.write().await;
        let mut seq = self.seq.lock().await;
        let _quiet = self
            .inflight
            .acquire_many(self.max_inflight)
            .await
            .map_err(|_| LogError::Failed("append semaphore closed".into()))?;
        // Every record goes (the WAL also drops its tail cache and durable mark) ...
        if let Err(e) = self.wal.truncate_to(TOPIC, self.pid.as_i32(), 0).await {
            let reason = format!("wal reset failed: {e}");
            self.fail(&reason);
            return Err(LogError::Failed(reason));
        }
        // ... then every remaining (empty) segment file except the WAL's open one, and the
        // directory is fsynced: after a crash the log is the old one or empty, never half.
        let pid = self.pid;
        let dir = self.seg_dir.clone();
        let wal = Arc::clone(&self.wal);
        let active = wal.get_segment_state(TOPIC, pid.as_i32()).map(|(id, _)| id);
        let res = tokio::task::spawn_blocking(move || -> Result<usize, LogError> {
            let mut deleted = 0;
            for (id, _) in list_segments(&dir, pid)? {
                if Some(id) == active {
                    continue;
                }
                wal.delete_segment_file(TOPIC, pid.as_i32(), id)
                    .map_err(|e| LogError::Wal(format!("delete segment {id}: {e}")))?;
                deleted += 1;
            }
            if dir.exists() {
                fsync_dir_blocking(&dir).map_err(|e| LogError::Wal(format!("dir fsync: {e}")))?;
            }
            Ok(deleted)
        })
        .await
        .map_err(|e| LogError::Wal(format!("reset task failed: {e}")));
        let deleted = match res.and_then(|r| r) {
            Ok(n) => n,
            Err(e) => {
                let reason = format!("reset of partition {} failed: {e}", self.pid);
                self.fail(&reason);
                return Err(LogError::Failed(reason));
            }
        };
        let cache = EpochCache::from_pairs(&epochs.0);
        seq.current_epoch = cache.latest_epoch().unwrap_or_default();
        *self.epochs_lock() = cache;
        seq.next = start;
        self.leo.store(start.0, Ordering::Release);
        self.log_start.store(start.0, Ordering::Release);
        self.core.on_reset(start);
        *self.index.lock().await = None;
        self.status.truncations.send_modify(|g| *g += 1);
        info!(
            partition = self.pid.0,
            start = %start,
            epochs = epochs.0.len(),
            segment_files_deleted = deleted,
            "partition log reset (anchored for install)"
        );
        Ok(())
    }

    fn replica_status(&self) -> ReplicaStatus {
        let st = self.core.lock();
        let now = self.core.clock.now_ms();
        let leading = matches!(st.mode, Mode::Leader { .. });
        ReplicaStatus {
            role: self.status.role.borrow().clone(),
            epoch: st
                .mode
                .epoch()
                .or_else(|| self.latest_epoch())
                .unwrap_or_default(),
            hw: self.hw_now(),
            durable_end: st.durable_end,
            published_isr: if leading {
                st.isr.published.clone()
            } else {
                Vec::new()
            },
            isr_version: st.isr.version,
            min_insync: u8::try_from(st.min_insync).unwrap_or(u8::MAX),
            followers: st
                .tracker
                .as_ref()
                .map(|t| t.status(now, self.core.lag_max_ms))
                .unwrap_or_default(),
            hw_publishes: self.hw_publishes(),
            max_envelopes_per_fetch: self.max_envelopes_per_fetch(),
        }
    }
}
