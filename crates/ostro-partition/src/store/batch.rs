//! `ApplyBatch`: the single write seam for apply.
//!
//! One `ApplyBatch` collects every write of one apply chunk. fjall's
//! `WriteBatch` has no read-your-writes, so the batch keeps an ordered overlay
//! (`BTreeMap`, never `HashMap`: determinism) and reads through it. Keyspace
//! reads are counted so apply can be held to its read budget (one point read
//! per command; S3 RMW margin over the 90k rule is only 1.19x).
//!
//! `commit` writes the overlay plus `applied_offset`, `state_hash`, `cursor`
//! and changed `inbound_seen` rows in ONE atomic fjall `WriteBatch`, with the
//! default buffered journal flush. Durability to disk comes from
//! `PartitionStore::checkpoint`. Any error is fatal for the partition (fjall
//! poisons the database anyway).

use std::collections::BTreeMap;

use ostro_types::Offset;

use super::derived::{MARKER_KEY, encode_marker};
use super::keys::inbound_key;
use super::meta::{K_APPLIED, K_CURSOR, K_STATE_HASH};
use super::values::{self, CursorMeta};
use super::{PartitionStore, Rows, StoreError, Table};

/// Meta written together with the data of one apply chunk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaUpdate {
    pub applied: Offset,
    pub state_hash: [u8; 32],
    pub cursor: CursorMeta,
    /// `(source partition, highest seen outbox seq)` rows to (over)write.
    pub inbound_seen_changed: Vec<(u16, u64)>,
}

/// Overlay entry: `Some(value)` = insert, `None` = tombstone.
type Overlay = BTreeMap<(Table, Vec<u8>), Option<Vec<u8>>>;

pub struct ApplyBatch<'a> {
    store: &'a PartitionStore,
    overlay: Overlay,
    /// `x_run_index` rows (`true` = insert with an empty value, `false` = remove). Kept apart
    /// from the table overlay: never read back by apply, never counted by [`len`](Self::len).
    derived: BTreeMap<Vec<u8>, bool>,
    reads: u64,
}

impl<'a> ApplyBatch<'a> {
    pub(super) fn new(store: &'a PartitionStore) -> Self {
        Self {
            store,
            overlay: BTreeMap::new(),
            derived: BTreeMap::new(),
            reads: 0,
        }
    }
}

impl ApplyBatch<'_> {
    /// Overlay first; a keyspace lookup increments [`reads`](Self::reads).
    pub fn get(&mut self, t: Table, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        if let Some(v) = self.overlay.get(&(t, key.to_vec())) {
            return Ok(v.clone());
        }
        self.reads += 1;
        Ok(self.store.ks(t).get(key)?.map(|v| v.to_vec()))
    }

    pub fn insert(&mut self, t: Table, key: Vec<u8>, value: Vec<u8>) {
        self.overlay.insert((t, key), Some(value));
    }

    pub fn remove(&mut self, t: Table, key: Vec<u8>) {
        self.overlay.insert((t, key), None);
    }

    /// Inserts an `x_run_index` row (empty value) in this batch (PF-11-2: outside the digest).
    pub(crate) fn insert_derived(&mut self, key: Vec<u8>) {
        self.derived.insert(key, true);
    }

    /// Removes an `x_run_index` row in this batch.
    pub(crate) fn remove_derived(&mut self, key: Vec<u8>) {
        self.derived.insert(key, false);
    }

    /// Number of `x_run_index` row writes (inserts + removes) pending.
    pub fn derived_len(&self) -> usize {
        self.derived.len()
    }

    /// Keyspace prefix scan merged with the overlay, in key order. Counts one
    /// read.
    pub fn range_prefix(&mut self, t: Table, prefix: &[u8]) -> Result<Rows, StoreError> {
        self.reads += 1;
        let mut rows = BTreeMap::new();
        for g in self.store.ks(t).prefix(prefix) {
            let (k, v) = g.into_inner()?;
            rows.insert(k.to_vec(), v.to_vec());
        }
        for ((_, k), v) in self
            .overlay
            .range((t, prefix.to_vec())..)
            .take_while(|((tt, k), _)| *tt == t && k.starts_with(prefix))
        {
            match v {
                Some(v) => rows.insert(k.clone(), v.clone()),
                None => rows.remove(k),
            };
        }
        Ok(rows.into_iter().collect())
    }

    /// Keyspace reads issued by this batch so far.
    pub fn reads(&self) -> u64 {
        self.reads
    }

    /// Number of overlay entries (writes + tombstones) pending.
    pub fn len(&self) -> usize {
        self.overlay.len()
    }

    pub fn is_empty(&self) -> bool {
        self.overlay.is_empty()
    }

    /// Writes all overlay entries plus the meta keys in ONE fjall batch.
    pub fn commit(self, meta: &MetaUpdate) -> Result<(), StoreError> {
        let store = self.store;
        let mut b = store.db.batch();
        for ((t, k), v) in self.overlay {
            let ks = store.ks(t);
            match v {
                Some(v) => b.insert(ks, k, v),
                None => b.remove(ks, k),
            }
        }
        let m = store.ks(Table::Meta);
        b.insert(m, K_APPLIED, meta.applied.0.to_be_bytes());
        b.insert(m, K_STATE_HASH, meta.state_hash);
        b.insert(m, K_CURSOR, values::encode(&meta.cursor));
        let inbound = store.ks(Table::InboundSeen);
        for &(src, seq) in &meta.inbound_seen_changed {
            b.insert(inbound, inbound_key(src), seq.to_be_bytes());
        }
        // x_run_index rows and marker, atomic with the data (PF-11-2). Under the apply lock, so
        // no rebuild batch interleaves and the ready flag cannot flip between check and commit.
        let shared = &store.index.shared;
        let _apply = shared.lock_apply();
        if shared.hooks() {
            let ix = &store.index.ks;
            for (k, ins) in self.derived {
                if ins {
                    b.insert(ix, k, []);
                } else {
                    b.remove(ix, k);
                }
            }
            if shared.ready() {
                b.insert(ix, MARKER_KEY, encode_marker(meta.applied.0));
            }
        }
        #[cfg(feature = "failpoints")]
        fail::fail_point!("state::commit", |_| Err(StoreError::Injected(
            "state::commit"
        )));
        store.gate.commit(b)?;
        Ok(())
    }
}
