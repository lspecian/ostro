// Vendored from chronik-stream@08715f4 crates/chronik-raft/src/lib.rs; modified by Ostro (see VENDORED.md).
//! Raft driver vendored from Chronik Stream: an event-driven [`RaftDriver`]
//! over raft-rs plus the durable [`RaftLogStore`]. Transport is supplied by the
//! caller through [`Outbox`].

pub mod driver;
pub mod log_store;

pub use driver::{Applied, DriverConfig, Outbox, ProposeError, RaftDriver, RaftHandle, RaftStatus, StateMachine};
pub use log_store::{LogStoreOptions, RaftLogStore};
/// Re-exported so consumers can name `raft::eraftpb::Message` without a direct dependency.
pub use raft;
