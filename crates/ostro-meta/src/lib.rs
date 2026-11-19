//! ostro-meta: the metadata state machine on the vendored Raft driver (META-01..03).
//!
//! - [`records`]: every metadata record type, wrapped in [`Versioned`].
//! - [`command`]: [`MetaKey`], [`MetaCommand`], [`MetaResult`] and their
//!   `[META_FORMAT] ‖ postcard` encoding.
//! - [`state`]: [`MetaState`] with deterministic, versioned (CAS) apply.
//! - [`ext`]: the TLV codec of the trailing `ext` bytes on every record.
//! - [`handle`]: [`MetaHandle`] (start + immediate campaign, propose, read, watch).
//! - [`sm`]: [`MetaSm`] (Raft `StateMachine`, postcard snapshots) and [`MetaView`].

pub mod command;
pub mod ext;
pub mod handle;
pub mod records;
pub mod sm;
pub mod state;
mod validate;

pub use command::{
    DecodeError, META_FORMAT, MetaCommand, MetaKey, MetaKind, MetaRecord, MetaResult,
    PRE_FREEZE_META_FORMAT, decode_command, decode_result, decode_snapshot, encode_command,
    encode_result, encode_snapshot,
};
pub use ext::{
    EXT_TAG_TEST, ExtEntry, ExtError, ext_entries, ext_get, ext_get_value, ext_put, ext_put_value,
    ext_remove, ext_validate,
};
pub use handle::{MetaError, MetaHandle};
pub use records::{
    ActiveVersion, ApiToken, CLUSTER_FORMAT, ClusterInfo, Connection, EgressPolicy, Grant, Member,
    ModuleRecord, PartitionAssignment, QueueConfig, QueuePolicy, RateSpec, Schedule, SealedSecret,
    SpillPolicy, TOKEN_KEY_PLAINTEXT_ID, TOKEN_KEY_SECRET, Tenant, Versioned, Webhook,
    WorkflowPolicy, WorkflowVersion,
};
pub use sm::{MetaSm, MetaView};
pub use state::MetaState;
/// D23 name grammar (the single source the API layer validates against).
pub use validate::{is_qualified_name, is_resource_name, is_tenant_id};

pub use ostro_raft_driver::{
    Applied, DriverConfig, Outbox, ProposeError, RaftDriver, RaftHandle, RaftStatus, StateMachine,
};

use ostro_raft_driver::raft::eraftpb::Message;

/// An [`Outbox`] that drops every message. Correct only for a single-voter
/// group, where raft never needs to send to a peer.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullOutbox;

impl Outbox for NullOutbox {
    fn send(&mut self, _messages: Vec<Message>) {}
}
