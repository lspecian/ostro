//! ostro-log: partition journal.
//!
//! Builds the CRC-verified envelope log on top of the vendored WAL (`ostro-wal`, a trimmed
//! Chronik Stream group-commit WAL; see VENDORED.md):
//! - [`verify`]: verified read of one WAL record (frame CRC + envelope CRC32C + trailer).
//! - [`recovery`]: full verified scan at open, torn-tail cut, LEO / epoch-history rebuild.

pub mod error;
pub mod recovery;
pub mod verify;

pub use error::LogError;
pub use ostro_wal;
pub use recovery::{RecoveryReport, TornCut, recover_partition};
pub use verify::{Corruption, VerifiedBatch, verify_record};

// Dependencies wired in plan 02-01 ahead of their consuming plans.
// The anchors keep cargo's per-crate `unused_dependencies` lint quiet.
use metrics as _;
use tokio as _;
use tracing as _;

#[cfg(test)]
mod dev_dep_anchors {
    use ostro_testkit as _;
    use proptest as _;
    use tempfile as _;
}
