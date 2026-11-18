//! ostro-partition: per-partition state store on fjall 3.1.13.
//!
//! fjall replaced redb after spike S3 (see docs/spikes/S3-redb-durability.md
//! and PROJECT.md Key Decisions).

pub mod store;

pub use store::{PartitionStore, StoreConfig, StoreError, StoreMeta, TABLES, Table};

// Dependencies wired in plan 02-01 ahead of their consuming plans.
// The anchors keep cargo's per-crate `unused_dependencies` lint quiet.
use bytes as _;
use metrics as _;
use ostro_blob as _;
use ostro_log as _;
use tokio as _;
use tracing as _;

#[cfg(test)]
mod tests {
    use ostro_testkit as _;
    use proptest as _;
    use proptest_state_machine as _;
    use tempfile as _;
}
