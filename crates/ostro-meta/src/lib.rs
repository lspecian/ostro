//! ostro-meta: metadata state machine on the vendored raft driver (single voter
//! today; transport/Outbox comes in Phase 4 META-01).

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
