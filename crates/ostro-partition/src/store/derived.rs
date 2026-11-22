//! The derived run index `x_run_index` (Phase 11, OPS-05, D-P11-9; RESERVATIONS.md §6, PF-11-2).
//!
//! A keyspace in the partition's fjall `Database` that is **not** a [`Table`] and not in
//! [`TABLES`](super::TABLES): the digest, OSNP export/import and the blob-GC visitor never see
//! it, and older binaries ignore it. Its content is a pure function of the `runs` keyspace
//! ([`run_index_rows`]), so it can always be rebuilt from digest state.
//!
//! Key shapes (values are empty; they never hold user payloads):
//! - `kind ‖ value ‖ 0x00 ‖ sfx` for [`IndexKind::Tag`], [`IndexKind::Workflow`],
//!   [`IndexKind::Tenant`] (the value never contains `0x00`; a value that does gets no row);
//! - `kind ‖ status u8 ‖ sfx` for [`IndexKind::Status`] (terminal statuses only,
//!   [`status_code`]);
//! - `kind ‖ sfx` for [`IndexKind::All`];
//! - the version marker [`MARKER_KEY`] `0x00 'v'` → `index_version u32 BE ‖
//!   built_at_applied_offset u64 BE`.
//!
//! `sfx` = `!created_at_ms` (u64 BE, bit-inverted) ‖ `!run_uuid` (16 B, bit-inverted): a
//! forward scan returns runs newest first. Child run ids are uuid v8 (D-P9-7), so the uuid
//! alone does not order by time; `created_at_ms` does and the uuid breaks ties.
//!
//! Maintenance (see `apply/index.rs` and [`super::batch`]): apply writes the rows in the same
//! fjall batch as the state change, and every commit while the index is ready rewrites the
//! marker with the batch's `applied_offset`. At open, a missing marker, another version or
//! `built_at_applied_offset != applied_offset` marks the index stale ([`IndexState::Rebuilding`]);
//! a background rebuild ([`PartitionStore::start_run_index_rebuild`]) then clears the keyspace,
//! walks `runs` in batches of [`REBUILD_BATCH`] under the apply lock and finally writes the
//! marker.

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;

use fjall::{Database, Keyspace, Readable};
use ostro_types::RunId;
use tracing::{info, warn};

use super::meta::K_APPLIED;
use super::values::{self, RunRecord, RunStatus};
use super::{CommitGate, PartitionStore, Rows, StoreError, Table};

/// On-disk keyspace name (RESERVATIONS.md §6, PF-11-2).
pub const RUN_INDEX_KEYSPACE: &str = "x_run_index";
/// Version stored in the marker; a different one triggers a rebuild.
pub const RUN_INDEX_VERSION: u32 = 1;
/// The version marker row.
pub const MARKER_KEY: &[u8] = &[0x00, b'v'];
/// Rows (or keys) per rebuild batch; the apply lock is held for one batch at a time.
pub const REBUILD_BATCH: usize = 1_000;
/// Length of the ordering suffix.
pub const SUFFIX_LEN: usize = 8 + 16;

/// Index row kinds (first key byte). `0x00` is the marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexKind {
    Tag = 0x01,
    Workflow = 0x02,
    Status = 0x03,
    Tenant = 0x04,
    All = 0x05,
}

/// Whether the index can serve queries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexState {
    /// The marker matches: rows == f(runs).
    Ready,
    /// Missing or stale marker; a rebuild is pending or running. Callers use the scan.
    Rebuilding,
}

/// The status byte of a terminal status; `None` for Scheduled/Ready/Running (never indexed).
pub fn status_code(s: RunStatus) -> Option<u8> {
    match s {
        RunStatus::Scheduled | RunStatus::Ready | RunStatus::Running => None,
        RunStatus::Completed => Some(3),
        RunStatus::Failed => Some(4),
        RunStatus::Cancelled => Some(5),
        RunStatus::Terminated => Some(6),
        RunStatus::TimedOut => Some(7),
        RunStatus::ContinuedAsNew => Some(8),
    }
}

fn push_suffix(k: &mut Vec<u8>, created_at_ms: u64, run_uuid: &[u8; 16]) {
    k.extend_from_slice(&(!created_at_ms).to_be_bytes());
    k.extend(run_uuid.iter().map(|b| !b));
}

/// `index_prefix(kind, value) ‖ sfx`.
pub fn index_key(
    kind: IndexKind,
    value: &[u8],
    created_at_ms: u64,
    run_uuid: &[u8; 16],
) -> Vec<u8> {
    let mut k = index_prefix(kind, value);
    k.reserve(SUFFIX_LEN);
    push_suffix(&mut k, created_at_ms, run_uuid);
    k
}

/// The scan prefix of one index value: `kind ‖ value ‖ 0x00` (Tag/Workflow/Tenant),
/// `kind ‖ value` (Status: `value` is the one status byte), `kind` (All: `value` ignored).
pub fn index_prefix(kind: IndexKind, value: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 + value.len() + SUFFIX_LEN);
    k.push(kind as u8);
    match kind {
        IndexKind::Tag | IndexKind::Workflow | IndexKind::Tenant => {
            k.extend_from_slice(value);
            k.push(0x00);
        }
        IndexKind::Status => k.extend_from_slice(value),
        IndexKind::All => {}
    }
    k
}

/// `(created_at_ms, run uuid)` from the last [`SUFFIX_LEN`] bytes of an index key.
pub fn decode_suffix(key: &[u8]) -> Option<(u64, [u8; 16])> {
    if key.len() < 1 + SUFFIX_LEN || key[0] == 0x00 {
        return None;
    }
    let s = &key[key.len() - SUFFIX_LEN..];
    let mut c = [0u8; 8];
    c.copy_from_slice(&s[..8]);
    let mut u = [0u8; 16];
    for (d, b) in u.iter_mut().zip(&s[8..]) {
        *d = !b;
    }
    Some((!u64::from_be_bytes(c), u))
}

fn uuid_of(run: &RunId) -> [u8; 16] {
    let mut u = [0u8; 16];
    u.copy_from_slice(&run.as_bytes()[2..]);
    u
}

/// The rows a run carries at creation (every row but Status).
pub(crate) fn start_rows(run: &RunId, rec: &RunRecord) -> Vec<Vec<u8>> {
    let u = uuid_of(run);
    let at = rec.created_at_ms;
    let mut out = vec![index_key(IndexKind::All, &[], at, &u)];
    let ok = |v: &str| !v.as_bytes().contains(&0x00);
    for t in rec.tags.iter().filter(|t| ok(t)) {
        out.push(index_key(IndexKind::Tag, t.as_bytes(), at, &u));
    }
    if ok(&rec.workflow) {
        out.push(index_key(
            IndexKind::Workflow,
            rec.workflow.as_bytes(),
            at,
            &u,
        ));
    }
    if let Some(t) = rec.tenant.as_deref().filter(|t| ok(t)) {
        out.push(index_key(IndexKind::Tenant, t.as_bytes(), at, &u));
    }
    out
}

/// The Status row of a terminal run (`None` while it is live).
pub(crate) fn status_row(run: &RunId, rec: &RunRecord) -> Option<Vec<u8>> {
    status_code(rec.status)
        .map(|s| index_key(IndexKind::Status, &[s], rec.created_at_ms, &uuid_of(run)))
}

/// Every index row of `run` in state `rec`, sorted and deduplicated. The single source of
/// truth of hooks, rebuild and the property test.
pub fn run_index_rows(run: &RunId, rec: &RunRecord) -> Vec<Vec<u8>> {
    let mut out = start_rows(run, rec);
    out.extend(status_row(run, rec));
    out.sort();
    out.dedup();
    out
}

/// Marker value for `applied`.
pub fn encode_marker(applied: u64) -> [u8; 12] {
    let mut v = [0u8; 12];
    v[..4].copy_from_slice(&RUN_INDEX_VERSION.to_be_bytes());
    v[4..].copy_from_slice(&applied.to_be_bytes());
    v
}

/// `(index_version, built_at_applied_offset)`.
pub fn decode_marker(v: &[u8]) -> Option<(u32, u64)> {
    let v: &[u8; 12] = v.try_into().ok()?;
    let mut a = [0u8; 4];
    a.copy_from_slice(&v[..4]);
    let mut b = [0u8; 8];
    b.copy_from_slice(&v[4..]);
    Some((u32::from_be_bytes(a), u64::from_be_bytes(b)))
}

/// State shared by the store, its readers, apply commits and the rebuild worker.
#[derive(Default)]
pub(crate) struct Shared {
    ready: AtomicBool,
    /// Test switch: `false` simulates a binary without the hooks (no rows, no marker).
    hooks_off: AtomicBool,
    stop: AtomicBool,
    /// Taken by every apply commit and by each rebuild batch (never both at once).
    apply_lock: Mutex<()>,
}

impl Shared {
    pub(crate) fn ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub(crate) fn hooks(&self) -> bool {
        !self.hooks_off.load(Ordering::Acquire)
    }

    pub(crate) fn state(&self) -> IndexState {
        if self.ready() {
            IndexState::Ready
        } else {
            IndexState::Rebuilding
        }
    }

    pub(crate) fn lock_apply(&self) -> MutexGuard<'_, ()> {
        self.apply_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// The store's index handle.
pub(crate) struct RunIndex {
    pub(crate) ks: Keyspace,
    pub(crate) shared: Arc<Shared>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl RunIndex {
    pub(crate) fn new(ks: Keyspace) -> Self {
        Self {
            ks,
            shared: Arc::new(Shared::default()),
            worker: Mutex::new(None),
        }
    }

    fn worker(&self) -> MutexGuard<'_, Option<JoinHandle<()>>> {
        self.worker.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Stops a running rebuild after its current batch and waits for it.
    pub(crate) fn stop(&self) {
        self.shared.stop.store(true, Ordering::Release);
        if let Some(h) = self.worker().take() {
            let _ = h.join();
        }
    }
}

/// Everything the rebuild worker needs (cloned handles; the worker never holds the store).
struct Rebuild {
    db: Database,
    gate: CommitGate,
    runs: Keyspace,
    meta: Keyspace,
    ix: Keyspace,
    shared: Arc<Shared>,
}

impl Rebuild {
    fn stopped(&self) -> bool {
        self.shared.stop.load(Ordering::Acquire)
    }

    /// Up to [`REBUILD_BATCH`] rows of `ks` with key > `after`.
    fn next_rows(ks: &Keyspace, after: Option<&[u8]>) -> Result<Rows, StoreError> {
        let lo = match after {
            Some(a) => Bound::Excluded(a.to_vec()),
            None => Bound::Unbounded,
        };
        let mut out = Vec::new();
        let hi: Bound<Vec<u8>> = Bound::Unbounded;
        for g in ks.range((lo, hi)).take(REBUILD_BATCH) {
            let (k, v) = g.into_inner()?;
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }

    /// `Ok(false)` when stopped before the end.
    fn run(&self) -> Result<bool, StoreError> {
        {
            let _l = self.shared.lock_apply();
            self.shared.ready.store(false, Ordering::Release);
            let mut b = self.db.batch();
            b.remove(&self.ix, MARKER_KEY);
            self.gate.commit(b)?;
        }
        // 1. Clear every row. Hook rows written meanwhile below the cursor are correct deltas;
        // rows deleted above it belong to runs the walk below visits anyway.
        let mut after: Option<Vec<u8>> = None;
        loop {
            if self.stopped() {
                return Ok(false);
            }
            let _l = self.shared.lock_apply();
            let rows = Self::next_rows(&self.ix, after.as_deref())?;
            let Some((last, _)) = rows.last() else { break };
            after = Some(last.clone());
            let mut b = self.db.batch();
            for (k, _) in rows {
                b.remove(&self.ix, k);
            }
            self.gate.commit(b)?;
            drop(_l);
            std::thread::yield_now();
        }
        // 2. Walk `runs`: each batch reads the records at their current state and writes their
        // rows with no apply commit in between.
        let mut after: Option<Vec<u8>> = None;
        loop {
            if self.stopped() {
                return Ok(false);
            }
            let _l = self.shared.lock_apply();
            let rows = Self::next_rows(&self.runs, after.as_deref())?;
            let Some((last, _)) = rows.last() else { break };
            after = Some(last.clone());
            let mut b = self.db.batch();
            for (k, v) in rows {
                let run = RunId::from_bytes(&k)
                    .map_err(|e| StoreError::Codec(format!("runs key: {e}")))?;
                let rec: RunRecord = values::decode(&v)?;
                for key in run_index_rows(&run, &rec) {
                    b.insert(&self.ix, key, []);
                }
            }
            self.gate.commit(b)?;
            drop(_l);
            std::thread::yield_now();
        }
        // 3. The marker at the current applied offset; commits keep it current from now on.
        let _l = self.shared.lock_apply();
        let applied = self
            .meta
            .get(K_APPLIED)?
            .and_then(|v| <[u8; 8]>::try_from(&*v).ok())
            .map(u64::from_be_bytes)
            .ok_or_else(|| StoreError::Codec("meta key applied_offset missing".into()))?;
        let mut b = self.db.batch();
        b.insert(&self.ix, MARKER_KEY, encode_marker(applied));
        self.gate.commit(b)?;
        self.shared.ready.store(true, Ordering::Release);
        Ok(true)
    }
}

impl PartitionStore {
    /// The run index state ([`IndexState::Rebuilding`] until a rebuild wrote the marker).
    pub fn run_index_state(&self) -> IndexState {
        self.index.shared.state()
    }

    /// Starts the background rebuild of `x_run_index` if the index is stale and no rebuild
    /// is running; returns whether one was started. Never called in `StartMode::Inspect`.
    /// The worker is a plain thread (never the apply thread); dropping the store stops it
    /// after its current batch and joins it.
    pub fn start_run_index_rebuild(&self) -> bool {
        if self.index.shared.ready() {
            return false;
        }
        let mut w = self.index.worker();
        if w.as_ref().is_some_and(|h| !h.is_finished()) {
            return false;
        }
        if let Some(h) = w.take() {
            let _ = h.join();
        }
        self.index.shared.stop.store(false, Ordering::Release);
        let job = Rebuild {
            db: self.db.clone(),
            gate: self.gate.clone(),
            runs: self.ks(Table::Runs).clone(),
            meta: self.ks(Table::Meta).clone(),
            ix: self.index.ks.clone(),
            shared: Arc::clone(&self.index.shared),
        };
        let spawned = std::thread::Builder::new()
            .name("run-index-rebuild".into())
            .spawn(move || match job.run() {
                Ok(true) => info!("x_run_index rebuilt"),
                Ok(false) => info!("x_run_index rebuild stopped"),
                Err(e) => warn!(error = %e, "x_run_index rebuild failed; queries use the scan"),
            });
        match spawned {
            Ok(h) => {
                info!("x_run_index stale or missing: rebuilding from runs");
                *w = Some(h);
                true
            }
            Err(e) => {
                warn!(error = %e, "x_run_index rebuild thread not started");
                false
            }
        }
    }

    /// Waits for a running rebuild (if any) to finish.
    pub fn join_run_index_rebuild(&self) {
        if let Some(h) = self.index.worker().take() {
            let _ = h.join();
        }
    }

    /// Test switch: with `false`, apply writes no index rows and no marker (a binary without
    /// the hooks), and the index reports [`IndexState::Rebuilding`].
    #[doc(hidden)]
    pub fn set_run_index_hooks(&self, on: bool) {
        let _l = self.index.shared.lock_apply();
        self.index.shared.hooks_off.store(!on, Ordering::Release);
        if !on {
            self.index.shared.ready.store(false, Ordering::Release);
        }
    }

    /// Test helper: deletes the version marker (the next open rebuilds).
    #[doc(hidden)]
    pub fn delete_run_index_marker(&self) -> Result<(), StoreError> {
        let _l = self.index.shared.lock_apply();
        self.index.shared.ready.store(false, Ordering::Release);
        let mut b = self.db.batch();
        b.remove(&self.index.ks, MARKER_KEY);
        self.gate.commit(b)?;
        Ok(())
    }
}

/// Opens the derived keyspace of `db` (never a [`Table`]).
pub(crate) fn open_keyspace(db: &Database, memtable: u64) -> Result<Keyspace, StoreError> {
    Ok(db.keyspace(RUN_INDEX_KEYSPACE, || {
        fjall::KeyspaceCreateOptions::default().max_memtable_size(memtable)
    })?)
}

/// Sets the ready flag from the on-disk marker against `applied`.
pub(crate) fn load_state(ix: &RunIndex, applied: u64) -> Result<(), StoreError> {
    let ok = ix
        .ks
        .get(MARKER_KEY)?
        .and_then(|v| decode_marker(&v))
        .is_some_and(|m| m == (RUN_INDEX_VERSION, applied));
    ix.shared.ready.store(ok, Ordering::Release);
    Ok(())
}

/// Reader-side scan: up to `limit` rows starting with `prefix`, strictly after `after` when
/// given, in key order, from one fjall snapshot.
pub(crate) fn scan(
    snap: &fjall::Snapshot,
    ix: &Keyspace,
    prefix: &[u8],
    after: Option<&[u8]>,
    limit: usize,
) -> Result<Rows, StoreError> {
    let lo = match after {
        Some(a) if a >= prefix => Bound::Excluded(a.to_vec()),
        _ => Bound::Included(prefix.to_vec()),
    };
    let mut out = Vec::new();
    let hi: Bound<Vec<u8>> = Bound::Unbounded;
    for g in snap.range(ix, (lo, hi)) {
        if out.len() >= limit {
            break;
        }
        let (k, v) = g.into_inner()?;
        if !k.starts_with(prefix) {
            break;
        }
        out.push((k.to_vec(), v.to_vec()));
    }
    Ok(out)
}
