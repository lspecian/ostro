//! Logical state digest (STOR-06).
//!
//! Compare logical digests, never fjall file bytes: SST/journal layout depends
//! on flush and compaction timing (02-RESEARCH Pattern 14, adapted to fjall).
//!
//! Layout fed to blake3, for each [`Table`] in [`TABLES`] order:
//! `u16 LE name_len ‖ name`, then every row in key order as
//! `u32 LE key_len ‖ key ‖ u32 LE value_len ‖ value`, then `u64 LE row_count`.
//! The only row skipped is the meta key `checkpointed_offset`, which depends
//! on wall-clock checkpoint timing. `applied_offset`, `state_hash` and
//! `cursor` are included.
//!
//! All keyspaces are read from ONE fjall snapshot, so the digest is
//! consistent across tables. The apply thread is the only writer: callers
//! take the digest while the partition is quiesced (between apply batches)
//! so that it corresponds to a single `applied_offset`.

use fjall::Readable;

use super::meta::K_CHECKPOINTED;
use super::{PartitionStore, StoreError, TABLES, Table};

fn len_u32(n: usize) -> [u8; 4] {
    u32::try_from(n)
        .expect("state key/value longer than u32::MAX")
        .to_le_bytes()
}

/// Incremental builder of the logical digest (layout in the module docs).
///
/// Shared by [`PartitionStore::digest`] and the OSNP snapshot writer/reader
/// (`store/snapshot.rs`), so a snapshot's trailer `logical_digest` is byte-identical to the
/// digest of the store it was exported from (and of the store it installs into). Callers
/// feed every table of [`TABLES`] in order, including empty ones, and skip the meta row
/// `checkpointed_offset` themselves.
pub(crate) struct DigestBuilder {
    h: blake3::Hasher,
    rows: u64,
}

impl DigestBuilder {
    pub(crate) fn new() -> Self {
        Self {
            h: blake3::Hasher::new(),
            rows: 0,
        }
    }

    /// `u16 LE name_len ‖ name`; resets the table's row count.
    pub(crate) fn table_begin(&mut self, name: &str) {
        let name = name.as_bytes();
        let name_len = u16::try_from(name.len()).expect("short table name");
        self.h.update(&name_len.to_le_bytes());
        self.h.update(name);
        self.rows = 0;
    }

    /// `u32 LE key_len ‖ key ‖ u32 LE value_len ‖ value`.
    pub(crate) fn row(&mut self, k: &[u8], v: &[u8]) {
        self.h.update(&len_u32(k.len()));
        self.h.update(k);
        self.h.update(&len_u32(v.len()));
        self.h.update(v);
        self.rows += 1;
    }

    /// `u64 LE row_count` of the current table; returns that count.
    pub(crate) fn table_end(&mut self) -> u64 {
        self.h.update(&self.rows.to_le_bytes());
        self.rows
    }

    pub(crate) fn finish(self) -> [u8; 32] {
        *self.h.finalize().as_bytes()
    }
}

impl PartitionStore {
    /// blake3 over the logical content of every table (see module docs).
    pub fn digest(&self) -> Result<[u8; 32], StoreError> {
        let snap = self.db.snapshot();
        let mut d = DigestBuilder::new();
        for t in TABLES {
            d.table_begin(t.name());
            for g in snap.iter(self.ks(t)) {
                let (k, v) = g.into_inner()?;
                if t == Table::Meta && &*k == K_CHECKPOINTED {
                    continue;
                }
                d.row(&k, &v);
            }
            d.table_end();
        }
        Ok(d.finish())
    }
}
