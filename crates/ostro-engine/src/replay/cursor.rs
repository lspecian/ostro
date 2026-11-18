//! Journal cursor and command matching (stub; implemented in the GREEN step).

use std::sync::Arc;

use ostro_types::journal::{CommandHash, CommandKind, Event, EventBody, RunStarted};

use crate::env::{Clock, RandomStream};
use crate::error::{NonDeterminism, ProtocolViolation};

use super::rng::{ChaChaStream, RngStreams};

/// Result of a journaled guest command.
#[derive(Debug)]
pub enum CommandResult<'a> {
    Replayed(&'a Event),
    Live { seq: u64, ts_nanos: u64 },
}

/// Result of `begin_step`.
#[derive(Debug)]
pub enum StepBegin<'a> {
    Replayed(&'a Event),
    Live { seq: u64 },
}

/// Replay failure.
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error(transparent)]
    NonDeterminism(NonDeterminism),
    #[error(transparent)]
    Protocol(ProtocolViolation),
    #[error("bad journal: {0}")]
    BadJournal(String),
}

/// Replay state of one run.
pub struct ReplayState {
    pub(super) journal: Vec<Event>,
    pub(super) cursor: usize,
    pub(super) next_seq: u64,
    pub(super) pending: Vec<Event>,
    pub(super) run: RunStarted,
    pub(super) rng: RngStreams,
    pub(super) open_step: Option<u64>,
    pub(super) last_cmd_ts: u64,
    pub(super) last_cmd_seq: u64,
    pub(super) clock: Arc<dyn Clock>,
}

#[allow(unused_variables, clippy::needless_pass_by_value)]
impl ReplayState {
    pub fn new(journal: Vec<Event>, clock: Arc<dyn Clock>) -> Result<Self, ReplayError> {
        todo!()
    }
    pub fn run(&self) -> &RunStarted {
        todo!()
    }
    pub fn is_replaying(&self) -> bool {
        todo!()
    }
    pub fn logical_now_nanos(&self) -> u64 {
        todo!()
    }
    pub fn next_command_hash_ver(&self) -> Option<u8> {
        todo!()
    }
    pub fn command(
        &mut self,
        kind: CommandKind,
        hash: CommandHash,
        actual: &dyn Fn() -> String,
        live: impl FnOnce(u64) -> EventBody,
    ) -> Result<CommandResult<'_>, ReplayError> {
        todo!()
    }
    pub fn patched(&mut self, hash: CommandHash, change_id: &str) -> Result<bool, ReplayError> {
        todo!()
    }
    pub fn begin_step(
        &mut self,
        hash: CommandHash,
        actual: &dyn Fn() -> String,
    ) -> Result<StepBegin<'_>, ReplayError> {
        todo!()
    }
    pub fn complete_step(&mut self, seq: u64, body: EventBody) -> Result<(), ReplayError> {
        todo!()
    }
    pub fn abandon_step(&mut self, seq: u64) {
        todo!()
    }
    pub fn finish(&mut self) -> Result<(), ReplayError> {
        todo!()
    }
    pub fn take_pending(&mut self) -> Vec<Event> {
        todo!()
    }
    pub fn pending_len(&self) -> usize {
        todo!()
    }
    pub fn deliver_inputs(&mut self, events: Vec<Event>) -> Result<(), ReplayError> {
        todo!()
    }
    pub fn continue_as_new_suggested(&self, threshold_events: u64) -> bool {
        todo!()
    }
    pub fn rng(&mut self, stream: RandomStream) -> &mut ChaChaStream {
        todo!()
    }
    pub fn journal(&self) -> &[Event] {
        todo!()
    }
}
