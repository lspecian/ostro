//! ostro-meta: the metadata state machine on the vendored Raft driver (META-01..03).
//!
//! - [`records`]: every metadata record type, wrapped in [`Versioned`].
//! - [`command`]: [`MetaKey`], [`MetaCommand`], [`MetaResult`] and their
//!   `[META_FORMAT] ‖ postcard` encoding.
//! - [`state`]: [`MetaState`] with deterministic, versioned (CAS) apply.

pub mod command;
pub mod records;
pub mod state;

pub use command::{
    DecodeError, META_FORMAT, MetaCommand, MetaKey, MetaKind, MetaRecord, MetaResult,
    decode_command, decode_result, encode_command, encode_result,
};
pub use records::{
    ActiveVersion, ApiToken, CLUSTER_FORMAT, ClusterInfo, Connection, EgressPolicy, Grant, Member,
    ModuleRecord, PartitionAssignment, QueueConfig, Schedule, SealedSecret, TOKEN_KEY_PLAINTEXT_ID,
    TOKEN_KEY_SECRET, Tenant, Versioned, WorkflowVersion,
};
pub use state::MetaState;

pub use ostro_raft_driver::{
    Applied, DriverConfig, Outbox, ProposeError, RaftDriver, RaftHandle, RaftStatus, StateMachine,
};

use ostro_raft_driver::raft::eraftpb::Message;
// Consumed by the handle (04-01 Task 3); anchored for the unused-dependency lint.
use tracing as _;

/// An [`Outbox`] that drops every message. Correct only for a single-voter
/// group, where raft never needs to send to a peer.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullOutbox;

impl Outbox for NullOutbox {
    fn send(&mut self, _messages: Vec<Message>) {}
}
