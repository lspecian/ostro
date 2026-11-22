//! Per-partition state store on fjall 3.1.13.
//!
//! Durability design (STOR-05, adapted from S3 to fjall, see
//! docs/spikes/S3-redb-durability.md):
//! - One atomic `WriteBatch` per apply chunk carries the data together with
//!   `applied_offset`, `state_hash` and the cursor ([`ApplyBatch::commit`]).
//! - Commits use fjall's default buffered journal flush (`write(2)` per
//!   commit, no fsync).
//! - [`PartitionStore::checkpoint`] records `checkpointed_offset` and calls
//!   `persist(PersistMode::SyncAll)`. The caller runs it every 100 ms or 256
//!   batches.
//! - Recovery replays the WAL from the state's OWN `applied_offset`. After a
//!   process kill fjall may reopen ahead of the last checkpoint. That is safe:
//!   the state only ever applies WAL-durable commands.
//!
//! Each partition has its own fjall `Database` at `<data>/state/pNNNN` (see
//! [`config`]).

pub mod batch;
pub mod blobrefs;
pub mod config;
pub mod derived;
mod digest;
pub mod keys;
pub mod meta;
pub mod snapshot;
pub mod values;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

use fjall::{
    Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode, Readable, Snapshot,
};
use ostro_types::{Offset, PartitionCount, PartitionId};

pub use batch::{ApplyBatch, MetaUpdate};
pub use config::{StoreConfig, state_dir};
use values::CursorMeta;

/// Key/value rows in key order.
pub type Rows = Vec<(Vec<u8>, Vec<u8>)>;

/// The fixed keyspace set. Declaration order is the logical digest order. Ordinals and
/// on-disk names come only from `.planning/RESERVATIONS.md` §6 (placeholder rule included).
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Table {
    Meta,
    Runs,
    Events,
    Payloads,
    Ready,
    Timers,
    Inbox,
    Dedupe,
    Outbox,
    InboundSeen,
    Children,
    Dlq,
    Effects,
    /// Runs whose status is `Running`, keyed by run id; value `(epoch, n)` of the current
    /// attempt token (plan 04-12). Lets a new leader find in-flight runs without scanning
    /// `runs` (DISP-01).
    Running,
    /// Waits of runs (plan 05-02; RESERVATIONS §6 ordinal 14, name `waits`). Two key shapes:
    /// `0x01 ‖ run ‖ op` → `WaitRow` and the FIFO index `0x02 ‖ run ‖ name ‖ op` → `()`.
    Waits,
    /// Signal dedupe (D13; plan 05-02; RESERVATIONS §6 ordinal 15, name `signal_seen`):
    /// `run ‖ request_id` → `()`, deleted with the run.
    SignalSeen,
    /// Cron schedule state (Phase 8, P8-D10; RESERVATIONS §6 ordinal 16, name
    /// `schedule_state`), on the schedule's owner partition. Two key shapes:
    /// `0x01 ‖ name` → `ScheduleStateRow` (watermark) and the reverse index
    /// `0x02 ‖ run` → `ScheduleRunRow` (the run a fire started). Rows are written by 08-07.
    ScheduleState,
}

/// Every table, in fixed (declaration) order.
pub const TABLES: [Table; TABLE_COUNT] = [
    Table::Meta,
    Table::Runs,
    Table::Events,
    Table::Payloads,
    Table::Ready,
    Table::Timers,
    Table::Inbox,
    Table::Dedupe,
    Table::Outbox,
    Table::InboundSeen,
    Table::Children,
    Table::Dlq,
    Table::Effects,
    Table::Running,
    Table::Waits,
    Table::SignalSeen,
    Table::ScheduleState,
];

/// Number of keyspaces.
pub const TABLE_COUNT: usize = 17;

impl Table {
    /// fjall keyspace name.
    pub fn name(self) -> &'static str {
        match self {
            Table::Meta => "meta",
            Table::Runs => "runs",
            Table::Events => "events",
            Table::Payloads => "payloads",
            Table::Ready => "ready",
            Table::Timers => "timers",
            Table::Inbox => "inbox",
            Table::Dedupe => "dedupe",
            Table::Outbox => "outbox",
            Table::InboundSeen => "inbound_seen",
            Table::Children => "children",
            Table::Dlq => "dlq",
            Table::Effects => "effects",
            Table::Running => "running",
            Table::Waits => "waits",
            Table::SignalSeen => "signal_seen",
            Table::ScheduleState => "schedule_state",
        }
    }

    fn idx(self) -> usize {
        self as usize
    }
}

/// Decoded meta keyspace (plus `inbound_seen` rows).
#[derive(Clone, Debug, PartialEq)]
pub struct StoreMeta {
    pub partition: PartitionId,
    pub count: PartitionCount,
    pub applied: Offset,
    pub checkpointed: Offset,
    pub state_hash: [u8; 32],
    pub cursor: CursorMeta,
    pub inbound_seen: BTreeMap<u16, u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("fjall: {0}")]
    Fjall(#[from] fjall::Error),
    #[error("state codec: {0}")]
    Codec(String),
    #[error("unsupported state format version {found}")]
    FormatVersion { found: u32 },
    #[error("state store belongs to partition {found}, expected {expected}")]
    PartitionMismatch { expected: u16, found: u16 },
    #[error("state store was created with partition count {found}, configured {expected}")]
    PartitionCountMismatch { expected: u16, found: u16 },
    #[error("injected failure at {0}")]
    Injected(&'static str),
}

/// One partition's fjall-backed state. `Send + Sync`; the apply thread is
/// the only writer.
pub struct PartitionStore {
    db: Database,
    ks: [Keyspace; TABLE_COUNT],
    gate: CommitGate,
    /// The derived `x_run_index` keyspace (PF-11-2: not a [`Table`], not in [`TABLES`]).
    index: derived::RunIndex,
}

impl Drop for PartitionStore {
    fn drop(&mut self) {
        // A running index rebuild holds database handles: stop and join it first.
        self.index.stop();
    }
}

/// Orders fjall snapshot creation against batch commits (see [`StoreReader`]).
///
/// fjall 3.1 / lsm-tree 3.1 publish a batch's seqno only after its last item is in the
/// memtables, but a memtable flush or compaction of **any** keyspace that installs a new
/// tree version meanwhile allocates the next global seqno and raises the visible seqno past
/// it (`SuperVersions::upgrade_version_with_seqno`: `visible_seqno.fetch_max(seqno + 1)`),
/// i.e. past the batch still being inserted. A snapshot taken in that window sees a prefix
/// of the batch (the torn `runs`-without-`payloads` read of
/// `tests/store.rs::reader_never_sees_a_partial_batch`, reproduced on the musl CI leg).
/// Commits hold the write side and snapshot creation the read side, so a snapshot is only
/// ever taken while no batch is in flight: every seqno below the visible one then belongs
/// to a fully inserted batch (or to a version change, which has no items), and the next
/// batch gets a seqno at or above it. Reads themselves run outside the gate.
#[derive(Clone, Default)]
pub(crate) struct CommitGate(Arc<RwLock<()>>);

impl CommitGate {
    /// A fjall snapshot taken while no batch commit is in flight.
    pub(crate) fn snapshot(&self, db: &Database) -> Snapshot {
        let _g = self
            .0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        db.snapshot()
    }

    /// Commit `b` while no snapshot is being created.
    pub(crate) fn commit(&self, b: OwnedWriteBatch) -> Result<(), fjall::Error> {
        let _g = self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        b.commit()
    }
}

impl std::fmt::Debug for PartitionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionStore")
            .field("path", &self.ks[0].path())
            .finish_non_exhaustive()
    }
}

impl PartitionStore {
    /// Opens (creating if missing) the state store of partition `p`.
    ///
    /// A fresh store gets all meta keys in one batch, then
    /// `persist(SyncAll)`. An existing store must have format version 1 and
    /// the configured partition id and partition count (STOR-09).
    pub fn open(
        data_dir: &Path,
        p: PartitionId,
        count: PartitionCount,
        cfg: &StoreConfig,
    ) -> Result<(Self, StoreMeta), StoreError> {
        let db = Database::builder(state_dir(data_dir, p))
            .worker_threads(cfg.worker_threads)
            .max_journaling_size(cfg.max_journaling_size)
            .cache_size(cfg.cache_size)
            .open()?;
        let memtable = cfg.max_memtable_size;
        let mut handles = Vec::with_capacity(TABLES.len());
        for t in TABLES {
            handles.push(db.keyspace(t.name(), || {
                KeyspaceCreateOptions::default().max_memtable_size(memtable)
            })?);
        }
        let ks: [Keyspace; TABLE_COUNT] = handles
            .try_into()
            .unwrap_or_else(|_| unreachable!("one keyspace per table"));
        let store = Self::assemble(db, ks, memtable)?;

        match meta::read_format_version(store.ks(Table::Meta))? {
            None => {
                let mut b = store.db.batch();
                meta::write_initial(&mut b, store.ks(Table::Meta), p, count);
                // A fresh store has no runs: its (empty) run index is complete.
                b.insert(
                    &store.index.ks,
                    derived::MARKER_KEY,
                    derived::encode_marker(0),
                );
                store.gate.commit(b)?;
                store.persist()?;
            }
            Some(meta::FORMAT_VERSION) => {}
            Some(found) => return Err(StoreError::FormatVersion { found }),
        }
        let m = store.read_meta()?;
        if m.partition != p {
            return Err(StoreError::PartitionMismatch {
                expected: p.0,
                found: m.partition.0,
            });
        }
        if m.count != count {
            return Err(StoreError::PartitionCountMismatch {
                expected: count.get(),
                found: m.count.get(),
            });
        }
        derived::load_state(&store.index, m.applied.0)?;
        Ok((store, m))
    }

    /// A store over `db` with the table handles `ks`; opens the derived keyspace (no rows
    /// are written).
    pub(crate) fn assemble(
        db: Database,
        ks: [Keyspace; TABLE_COUNT],
        memtable: u64,
    ) -> Result<Self, StoreError> {
        let index = derived::RunIndex::new(derived::open_keyspace(&db, memtable)?);
        Ok(Self {
            db,
            ks,
            gate: CommitGate::default(),
            index,
        })
    }

    /// Whether partition `p` has a state directory under `data_dir`.
    pub fn exists(data_dir: &Path, p: PartitionId) -> bool {
        state_dir(data_dir, p).is_dir()
    }

    /// Starts an apply batch (overlay + read counter).
    pub fn batch(&self) -> ApplyBatch<'_> {
        ApplyBatch::new(self)
    }

    /// Records `checkpointed_offset = applied` and fsyncs everything
    /// (`persist(SyncAll)`).
    pub fn checkpoint(&self, applied: Offset) -> Result<(), StoreError> {
        self.ks(Table::Meta)
            .insert(meta::K_CHECKPOINTED, applied.0.to_be_bytes())?;
        self.persist()
    }

    fn persist(&self) -> Result<(), StoreError> {
        #[cfg(feature = "failpoints")]
        fail::fail_point!("state::persist", |_| Err(StoreError::Injected(
            "state::persist"
        )));
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }

    /// Read handle for leader services and tests.
    pub fn reader(&self) -> StoreReader {
        StoreReader {
            db: self.db.clone(),
            ks: self.ks.clone(),
            gate: self.gate.clone(),
            index_ks: self.index.ks.clone(),
            index: Arc::clone(&self.index.shared),
        }
    }

    pub fn read_meta(&self) -> Result<StoreMeta, StoreError> {
        meta::read(self.ks(Table::Meta), self.ks(Table::InboundSeen))
    }

    fn ks(&self, t: Table) -> &Keyspace {
        &self.ks[t.idx()]
    }
}

/// Cheap-to-clone read handle (cloned fjall database and keyspace handles).
///
/// Every read goes through a fresh fjall [`Snapshot`](fjall::Snapshot), i.e. at the
/// **published** sequence number, never at `SeqNo::MAX` (what a plain `Keyspace::get`
/// does). fjall's batch commit inserts the items into the memtables one by one and
/// publishes the batch seqno after the last one, so an unsnapshotted read can see a
/// prefix of a batch the apply thread is still committing: for example the terminal
/// `runs` row of a `CompleteRun` without its `payloads` result row (a long poll then
/// answered `completed: true, result: null`). Through a snapshot a batch becomes
/// visible all at once, and because published seqnos only grow, a later read sees at
/// least everything an earlier one saw. Snapshots are dropped at the end of each call.
/// The snapshot is created under the store's [`CommitGate`]: a concurrent flush can raise
/// fjall's visible seqno past a batch that is still being inserted, so a snapshot is never
/// created while a commit is in flight.
#[derive(Clone)]
pub struct StoreReader {
    db: Database,
    ks: [Keyspace; TABLE_COUNT],
    gate: CommitGate,
    index_ks: Keyspace,
    index: Arc<derived::Shared>,
}

impl StoreReader {
    pub fn get(&self, t: Table, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let snap = self.gate.snapshot(&self.db);
        Ok(snap.get(&self.ks[t.idx()], key)?.map(|v| v.to_vec()))
    }

    /// Up to `limit` rows whose key starts with `prefix`, in key order.
    pub fn prefix(&self, t: Table, prefix: &[u8], limit: usize) -> Result<Rows, StoreError> {
        collect(
            self.gate
                .snapshot(&self.db)
                .prefix(&self.ks[t.idx()], prefix),
            limit,
        )
    }

    /// Up to `limit` rows with `from <= key < to`, in key order.
    pub fn range(
        &self,
        t: Table,
        from: &[u8],
        to: &[u8],
        limit: usize,
    ) -> Result<Rows, StoreError> {
        collect(
            self.gate
                .snapshot(&self.db)
                .range(&self.ks[t.idx()], from..to),
            limit,
        )
    }
}

impl StoreReader {
    /// The run index state (see [`derived::IndexState`]).
    pub fn run_index_state(&self) -> derived::IndexState {
        self.index.state()
    }

    /// Up to `limit` `x_run_index` rows starting with `prefix` (strictly after `after` when
    /// given), in key order, from one snapshot. Rows are only meaningful while
    /// [`run_index_state`](Self::run_index_state) is `Ready`.
    pub fn run_index_rows(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Rows, StoreError> {
        let snap = self.gate.snapshot(&self.db);
        derived::scan(&snap, &self.index_ks, prefix, after, limit)
    }
}

fn collect(iter: fjall::Iter, limit: usize) -> Result<Rows, StoreError> {
    let mut out = Vec::new();
    for g in iter.take(limit) {
        let (k, v) = g.into_inner()?;
        out.push((k.to_vec(), v.to_vec()));
    }
    Ok(out)
}
