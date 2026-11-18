//! Wait resolution and completion lookup (stub; implemented in the GREEN step).

use ostro_types::journal::{CommandHash, Event, OpId, WaitMode};

use super::cursor::{ReplayError, ReplayState};

/// Outcome of `wait`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitOutcome {
    Resolved(Vec<OpId>),
    Pending,
}

#[allow(unused_variables)]
impl ReplayState {
    pub fn wait(
        &mut self,
        ops: &[OpId],
        mode: WaitMode,
        hash: CommandHash,
    ) -> Result<WaitOutcome, ReplayError> {
        todo!()
    }
    pub fn completion_for(&self, op: OpId) -> Option<&Event> {
        todo!()
    }
    pub fn op_kind(&self, op: OpId) -> Option<&Event> {
        todo!()
    }
}
