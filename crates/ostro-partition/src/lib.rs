//! ostro-partition: per-partition state store on fjall 3.1.13.
//!
//! fjall replaced redb after spike S3 (see docs/spikes/S3-redb-durability.md
//! and PROJECT.md Key Decisions).
//!
//! - [`apply`]: the pure deterministic apply core.
//! - [`machine`]: the per-partition apply thread (HW-capped chunks, checkpoints).
//! - [`acks`]: the submit/apply ack rendezvous.
//! - [`handle`]: [`PartitionHandle`], the submit path and read helpers.

pub mod acks;
pub mod apply;
pub mod handle;
pub mod machine;
pub mod store;

pub use apply::{Action, ApplyOutcome, Cursor, RejectReason, apply_chunk, apply_command};
pub use store::{PartitionStore, StoreConfig, StoreError, StoreMeta, TABLES, Table};

pub use acks::{ACK_RETAIN, PendingAcks};
pub use handle::{PartitionError, PartitionHandle};
pub use machine::{MachineConfig, MachineHandle, PartitionHealth};

#[cfg(test)]
mod tests {
    use ostro_testkit as _;
    use proptest as _;
    use proptest_state_machine as _;
    use tempfile as _;
}
