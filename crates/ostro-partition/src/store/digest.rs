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

impl PartitionStore {
    /// blake3 over the logical content of every table (see module docs).
    pub fn digest(&self) -> Result<[u8; 32], StoreError> {
        let snap = self.db.snapshot();
        let mut h = blake3::Hasher::new();
        for t in TABLES {
            let name = t.name().as_bytes();
            let name_len = u16::try_from(name.len()).expect("short table name");
            h.update(&name_len.to_le_bytes());
            h.update(name);
            let mut rows: u64 = 0;
            for g in snap.iter(self.ks(t)) {
                let (k, v) = g.into_inner()?;
                if t == Table::Meta && &*k == K_CHECKPOINTED {
                    continue;
                }
                h.update(&len_u32(k.len()));
                h.update(&k);
                h.update(&len_u32(v.len()));
                h.update(&v);
                rows += 1;
            }
            h.update(&rows.to_le_bytes());
        }
        Ok(*h.finalize().as_bytes())
    }
}
