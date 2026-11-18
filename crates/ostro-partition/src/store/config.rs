//! Store configuration and on-disk layout.
//!
//! Each partition owns one fjall `Database` at `<data>/state/pNNNN`:
//! - Deleting one partition's state (STOR-06 rebuild) is `rm -rf state/pNNNN`.
//! - fjall poisons a whole `Database` on an I/O error. Per-partition databases
//!   keep such failures partition-local (STOR-07).
//! - Journal replay on reopen is bounded per partition by
//!   `max_journaling_size`.
//!
//! The cost is P background worker pools, hence `worker_threads = 1` by default.

use std::path::{Path, PathBuf};

use ostro_types::PartitionId;

/// fjall tuning for one partition's state database.
#[derive(Clone, Debug)]
pub struct StoreConfig {
    /// fjall flush/compaction worker threads for this partition's database.
    pub worker_threads: usize,
    /// Maximum journal size before fjall forces memtable flushes (bounds the
    /// un-flushed journal replayed on reopen; 64 MiB is fjall's minimum).
    pub max_journaling_size: u64,
    /// Per-keyspace memtable size.
    pub max_memtable_size: u64,
    /// Block cache size for the database.
    pub cache_size: u64,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            worker_threads: 1,
            max_journaling_size: 64 << 20,
            max_memtable_size: 8 << 20,
            cache_size: 16 << 20,
        }
    }
}

/// `<data>/state/p0007` for partition 7.
pub fn state_dir(data_dir: &Path, p: PartitionId) -> PathBuf {
    data_dir.join("state").join(format!("p{:04}", p.0))
}
