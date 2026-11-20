//! Local snapshot service (P8-D1/P8-D2). Phase 8: implemented in 08-09.
//!
//! This file holds the configuration **contract** (plan 08-01); `[snapshots]` is parsed into it
//! by 08-04 (P8-D22). The service writes `snapshots/pNNNN/<applied 20d>.osnap`, keeps
//! [`SnapshotConfig::keep`] of them and publishes the local trim floor (the `applied_offset` of
//! the oldest retained snapshot) only after the file fsync, rename and directory fsync.

use std::time::Duration;

/// Local snapshot cadence and retention.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotConfig {
    /// Snapshots (and the trim they allow) are taken at all.
    pub enabled: bool,
    /// Take a snapshot at least this often while the partition changes (default 1 h).
    pub interval: Duration,
    /// Never snapshot more often than this (default 60 s).
    pub min_interval: Duration,
    /// Take a snapshot early once this many envelope bytes were appended since the last one
    /// (default 256 MiB).
    pub bytes_threshold: u64,
    /// Local snapshots retained per partition (default 2).
    pub keep: usize,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(3600),
            min_interval: Duration::from_secs(60),
            bytes_threshold: 256 * 1024 * 1024,
            keep: 2,
        }
    }
}
