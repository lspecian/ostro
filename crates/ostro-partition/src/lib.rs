//! ostro-partition: per-partition state store on fjall 3.1.13.
//!
//! fjall replaced redb after spike S3 (see docs/spikes/S3-redb-durability.md
//! and PROJECT.md Key Decisions).
//!
//! - [`apply`]: the pure deterministic apply core.
//! - [`machine`]: the per-partition apply thread (HW-capped chunks, checkpoints).
//! - [`acks`]: the submit/apply ack rendezvous.
//! - [`handle`]: [`PartitionHandle`], the submit path and read helpers.
//! - [`query`]: typed read helpers (running runs, ready rows, journal, runs pages).
//! - [`node`]: [`Node`], the ordered startup of every partition ([`startup`]).
//! - [`leader`]: leader services (shuffler, timers, retention) started after readiness.
//! - [`leadership`], [`clock`], [`router`]: term epochs, wall clock, cross-partition delivery.

pub mod acks;
pub mod apply;
pub mod clock;
pub mod handle;
pub mod leader;
pub mod leadership;
pub mod machine;
pub mod node;
pub mod query;
pub mod router;
pub mod startup;
pub mod store;

pub use apply::{Action, ApplyOutcome, Cursor, RejectReason, apply_chunk, apply_command};
pub use store::{PartitionStore, StoreConfig, StoreError, StoreMeta, TABLES, Table};

pub use acks::{ACK_RETAIN, PendingAcks};
pub use handle::{PartitionError, PartitionHandle};
pub use machine::{MachineConfig, MachineHandle, PartitionHealth};

pub use clock::{Clock, ManualClock, SystemClock};
pub use leader::{LeaderConfig, LeaderCtx, LeaderTasks};
pub use leadership::{LeadershipSource, LocalLeadership, next_term_epoch};
pub use node::{Node, NodeConfig, NodeError, PartitionSlot, StartMode};
pub use router::{InProcessRouter, PartitionRouter, RouteFuture};
pub use startup::StartupEvent;

#[cfg(test)]
mod tests {
    use ostro_testkit as _;
    use proptest as _;
    use proptest_state_machine as _;
    use tempfile as _;
}
