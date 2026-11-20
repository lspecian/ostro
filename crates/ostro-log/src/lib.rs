//! ostro-log: partition journal.
//!
//! Builds the CRC-verified envelope log on top of the vendored WAL (`ostro-wal`, a trimmed
//! Chronik Stream group-commit WAL; see VENDORED.md):
//! - [`verify`]: verified read of one WAL record (frame CRC + envelope CRC32C + trailer).
//! - [`recovery`]: full verified scan at open, torn-tail cut, LEO / epoch-history rebuild.
//! - [`replicated`]: the [`ReplicatedLog`] trait (`ostro-partition` codes against it).
//! - [`rf1`] / [`node`]: the RF=1 implementation over one shared `GroupCommitWal` per node.
//! - [`cut`]: [`HwVector`], the consistent cut of every partition's HW (bottomless, W7).
//! - [`hw`]: high-watermark computation shared with the Phase 12 cluster.
//! - [`epoch_cache`]: leader-epoch history (ported Kafka semantics).
//! - [`barrier`]: readiness barrier and trim permits (STOR-08).

pub mod barrier;
pub mod cut;
pub mod epoch_cache;
pub mod error;
pub mod hw;
pub mod node;
pub mod recovery;
pub mod replicated;
pub mod rf1;
pub mod verify;

pub use barrier::{ReadyBarrier, TrimPermit};
pub use cut::HwVector;
pub use error::LogError;
pub use hw::{ReplicaProgress, compute_hw};
pub use node::{LogNode, LogNodeConfig, PartitionLogs};
pub use ostro_wal;
pub use recovery::{RecoveryReport, TornCut, recover_partition};
pub use replicated::{AppendReceipt, EpochHistory, Isr, LogHealth, RawBatch, ReplicatedLog, Role};
pub use rf1::Rf1Log;
pub use verify::{Corruption, VerifiedBatch, verify_record};

// Dependencies wired in plan 02-01 ahead of their consuming plans.
// The anchors keep cargo's per-crate `unused_dependencies` lint quiet.
use metrics as _;
// `fail` is used by the failpoint test suites only (feature `failpoints`).
#[cfg(feature = "failpoints")]
use fail as _;

#[cfg(test)]
mod dev_dep_anchors {
    use ostro_testkit as _;
    use proptest as _;
    use tempfile as _;
}
