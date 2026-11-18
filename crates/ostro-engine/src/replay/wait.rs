//! `effects.wait` resolution (live + replay) and completion lookup (WIT-03/WIT-05).
//!
//! Live: the completed subset of `ops` is ordered by the journal seq of each op's
//! completion event (never by the order of `ops`). Mode `Any` resolves once at least one
//! op completed, `All` once every op did; otherwise the wait is `Pending` and nothing is
//! appended (the activation is evicted and resumes when inputs arrive). Replay: the next
//! recorded guest command must be the matching `WaitResolved`, whose recorded list is
//! returned verbatim even if more completions exist by now.

use ostro_types::journal::{
    CANCELLATION_OP, CommandHash, CommandKind, CommandMeta, Event, EventBody, EventKind, HASH_VER,
    OpId, WaitMode, WaitResolved,
};

use super::cursor::{ReplayError, ReplayState};
use crate::error::ProtocolViolation;

/// Outcome of [`ReplayState::wait`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitOutcome {
    /// Completed ops in journal order of their completion events.
    Resolved(Vec<OpId>),
    /// Not satisfied yet; nothing was appended.
    Pending,
}

fn is_op_scheduling(kind: EventKind) -> bool {
    matches!(
        kind,
        EventKind::TimerStarted
            | EventKind::EffectScheduled
            | EventKind::SignalWaitStarted
            | EventKind::ChildStarted
            | EventKind::AwaitTokenCreated
    )
}

fn protocol(msg: String) -> ReplayError {
    ReplayError::Protocol(ProtocolViolation(msg))
}

impl ReplayState {
    /// `effects.wait(ops, mode)`.
    pub fn wait(
        &mut self,
        ops: &[OpId],
        mode: WaitMode,
        hash: CommandHash,
    ) -> Result<WaitOutcome, ReplayError> {
        if self.open_step.is_some() {
            return Err(protocol("journaled command inside an open step".into()));
        }
        if ops.is_empty() && mode == WaitMode::Any {
            return Err(protocol("wait(any) on an empty op list".into()));
        }
        for &op in ops {
            if op != CANCELLATION_OP && !self.is_issued_op(op) {
                return Err(protocol(format!("wait on unknown op {op}")));
            }
        }
        let actual = || format!("wait {ops:?} {mode:?}");
        if let Some(i) = self.match_next(CommandKind::Wait, hash, &actual)? {
            return match &self.journal[i].body {
                EventBody::WaitResolved(w) => Ok(WaitOutcome::Resolved(w.completed.clone())),
                other => Err(ReplayError::BadJournal(format!(
                    "wait matched {} at seq {i}",
                    other.summary()
                ))),
            };
        }

        // Live: completed ops ordered by their completion event's seq.
        let mut done: Vec<(u64, OpId)> = Vec::new();
        for &op in ops {
            if done.iter().any(|(_, o)| *o == op) {
                continue;
            }
            if let Some(ev) = self.completion_for(op) {
                done.push((ev.seq, op));
            }
        }
        done.sort_unstable();
        let distinct = {
            let mut v = ops.to_vec();
            v.sort_unstable();
            v.dedup();
            v.len()
        };
        let satisfied = match mode {
            WaitMode::Any => !done.is_empty(),
            WaitMode::All => done.len() == distinct,
        };
        if !satisfied {
            return Ok(WaitOutcome::Pending);
        }
        let completed: Vec<OpId> = done.into_iter().map(|(_, op)| op).collect();
        let seq = self.next_seq;
        self.append_live(
            seq,
            EventBody::WaitResolved(WaitResolved {
                meta: CommandMeta {
                    hash_ver: HASH_VER,
                    hash,
                },
                completed: completed.clone(),
            }),
        );
        Ok(WaitOutcome::Resolved(completed))
    }

    /// True if `op` names an op-scheduling guest command already issued by the guest.
    fn is_issued_op(&self, op: OpId) -> bool {
        usize::try_from(op).is_ok_and(|i| i < self.cursor)
            && self
                .op_kind(op)
                .is_some_and(|ev| is_op_scheduling(ev.body.kind()))
    }

    /// The input event completing `op` (first one in journal order), or the
    /// `CancelRequested` event for [`CANCELLATION_OP`].
    pub fn completion_for(&self, op: OpId) -> Option<&Event> {
        self.journal
            .iter()
            .find(|e| e.body.completes_op() == Some(op))
    }

    /// The event that scheduled `op` (op id == its seq).
    pub fn op_kind(&self, op: OpId) -> Option<&Event> {
        usize::try_from(op)
            .ok()
            .and_then(|i| self.journal.get(i))
            .filter(|e| e.seq == op && e.body.is_guest_command())
    }
}
