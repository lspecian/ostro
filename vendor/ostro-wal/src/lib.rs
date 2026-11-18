// Vendored from chronik-stream@08715f4 crates/chronik-wal/src/lib.rs; rewritten by Ostro (see VENDORED.md).

//! Write-Ahead Log (WAL) subsystem, vendored from Chronik Stream.
//!
//! Provides group-commit durability, segment rotation, a sparse segment
//! index and suffix truncation. Ostro's partition journal (Phase 2) builds
//! on [`GroupCommitWal`].

#![allow(clippy::all)]

pub mod error;
pub mod manager;
pub mod record;
pub mod config;
pub mod buffer_pool;
pub mod group_commit;
pub mod io_priority; // I/O priority control for WAL writer threads
pub mod truncate; // Suffix truncation — discarding a divergent tail (RP-3.3)
pub mod segment_index; // Sparse offset → byte position, so reads seek (RP-9)

pub use error::{WalError, Result};
pub use manager::WalManager;
pub use record::WalRecord;
pub use config::{WalConfig, CompressionType, CheckpointConfig, RecoveryConfig, RotationConfig, FsyncConfig};
pub use group_commit::{GroupCommitWal, GroupCommitConfig, PartitionMetrics};
pub use truncate::TruncateOutcome;

// Re-export WalError as Error for compatibility
pub use WalError as Error;

use tracing::info;

/// Recover all WAL partitions from disk
pub async fn recover_all(config: &WalConfig) -> Result<RecoveryResult> {
    info!("Starting WAL recovery from {:?}", config.data_dir);

    let manager = WalManager::recover(config).await?;
    let result = manager.get_recovery_result();

    info!(
        "WAL recovery complete: {} records from {} partitions",
        result.total_records, result.partitions
    );

    Ok(result)
}

/// Recovery statistics
#[derive(Debug, Clone)]
pub struct RecoveryResult {
    pub total_records: usize,
    pub partitions: usize,
    pub corrupted_segments: usize,
    pub last_offsets: Vec<(String, i32, i64)>, // (topic, partition, offset)
}
