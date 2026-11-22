//! `effects.wait` resolution (live + replay) and completion lookup (WIT-03/WIT-05).
//!
//! Live: the completed subset of `ops` is ordered by the journal seq of each op's
//! completion event (never by the order of `ops`). Mode `Any` resolves once at least one
//! op completed, `All` once every op did; otherwise the wait is `Pending` and nothing is
//! appended (the activation is evicted and resumes when inputs arrive). Replay: the next
//! recorded guest command must be the matching `WaitResolved`, whose recorded list is
//! returned verbatim even if more completions exist by now.
//!
//! Completion rules (frozen with WIT 0.1, 05-03):
//! - An op is completed by the **first** event in journal order that targets it
//!   ([`completion_target`]): a committed input (`completes_op`) or the guest's own
//!   `OpCancelled{op}` command (D5). If an input completed the op at a lower seq, the
//!   input wins and the cancel is a no-op; if the cancel came first, every later input
//!   for that op is ignored.
//! - `OpCancelled` → `Err(cancelled)`, `WaitTimedOut` (70) → `Err(timeout)` (D4).
//! - `SignalMatched{op, payload_seq}` (97) → `Ok(signal(p))`, where `p` is the payload
//!   of the `SignalPayload` (96) event at `payload_seq` (D21). `SignalPayload` alone
//!   completes nothing. Legacy `SignalReceived` (66) still completes its op.
//!
//! Lookups go through the op → completion-seq index kept by the cursor, so they never
//! scan the journal.

use ostro_types::journal::{
    CANCELLATION_OP, CommandHash, CommandKind, CommandMeta, Event, EventBody, EventKind, Failure,
    HASH_VER, OpId, OpOutput, Payload, WaitMode, WaitResolved, decode_result,
};

use super::cursor::{ReplayError, ReplayState, protocol};

/// Outcome of [`ReplayState::wait`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitOutcome {
    /// Completed ops in journal order of their completion events.
    Resolved(Vec<OpId>),
    /// Not satisfied yet; nothing was appended.
    Pending,
}

/// The op `body` completes, if any: an input's `completes_op`, or the op named by a
/// guest `OpCancelled` (D5). `CancelRequested` targets [`CANCELLATION_OP`].
pub(super) fn completion_target(body: &EventBody) -> Option<OpId> {
    match body {
        EventBody::OpCancelled(c) => Some(c.op),
        other => other.completes_op(),
    }
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

impl ReplayState {
    /// `effects.wait(ops, mode)`.
    pub fn wait(
        &mut self,
        ops: &[OpId],
        mode: WaitMode,
        hash: CommandHash,
    ) -> Result<WaitOutcome, ReplayError> {
        self.check_no_open_step()?;
        if ops.is_empty() && mode == WaitMode::Any {
            return Err(protocol("wait(any) on an empty op list"));
        }
        for &op in ops {
            if op != CANCELLATION_OP && !self.is_issued_op(op) {
                return Err(protocol(format!("wait on unknown op {op}")));
            }
        }
        let actual = || format!("wait {ops:?} {mode:?}");
        if let Some(i) = self.match_next(CommandKind::Wait, hash, &actual)? {
            return match &self.journal[i].body {
                EventBody::WaitResolved(w) => {
                    let completed = w.completed.clone();
                    self.note_cancel_observed(&completed);
                    Ok(WaitOutcome::Resolved(completed))
                }
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
        )?;
        self.note_cancel_observed(&completed);
        Ok(WaitOutcome::Resolved(completed))
    }

    /// True if `op` names an op-scheduling guest command already issued by the guest.
    fn is_issued_op(&self, op: OpId) -> bool {
        usize::try_from(op).is_ok_and(|i| i < self.cursor)
            && self
                .op_kind(op)
                .is_some_and(|ev| is_op_scheduling(ev.body.kind()))
    }

    /// The event completing `op` (the first one in journal order: an input or the
    /// guest's `OpCancelled`), or the `CancelRequested` event for [`CANCELLATION_OP`].
    /// O(log n) through the completion index.
    pub fn completion_for(&self, op: OpId) -> Option<&Event> {
        let seq = *self.completions.get(&op)?;
        usize::try_from(seq).ok().and_then(|i| self.journal.get(i))
    }

    /// Seq of the event completing `op` (see [`completion_for`](Self::completion_for)).
    pub fn completion_seq(&self, op: OpId) -> Option<u64> {
        self.completions.get(&op).copied()
    }

    /// Guest-visible outcome of the resolved `op`, built only from its journaled
    /// completion event (see the module doc for the rules).
    ///
    /// A missing completion, an undecodable recorded outcome, or a `SignalMatched` whose
    /// `payload_seq` is not an earlier `SignalPayload` event is a bad journal (the host
    /// traps it as a protocol violation).
    pub fn completion_outcome(&self, op: OpId) -> Result<Result<OpOutput, Failure>, ReplayError> {
        let ev = self.completion_for(op).ok_or_else(|| {
            ReplayError::BadJournal(format!("resolved op {op} has no completion event"))
        })?;
        let bad =
            |e| ReplayError::BadJournal(format!("outcome of op {op} at seq {}: {e:?}", ev.seq));
        Ok(match &ev.body {
            EventBody::TimerFired(_) => Ok(OpOutput::Timer),
            EventBody::EffectCompleted(c) => decode_result(&c.outcome).map_err(bad)?,
            EventBody::ChildCompleted(c) => decode_result(&c.outcome).map_err(bad)?,
            EventBody::SignalReceived(s) => Ok(OpOutput::Signal(s.payload.clone())),
            EventBody::SignalMatched(m) => Ok(OpOutput::Signal(
                self.signal_payload(op, ev.seq, m.payload_seq)?.clone(),
            )),
            EventBody::TokenCompleted(t) => Ok(OpOutput::Token(t.payload.clone())),
            EventBody::CancelRequested(_) => Ok(OpOutput::CancelRequested),
            EventBody::WaitTimedOut(_) => Err(Failure::Timeout),
            EventBody::OpCancelled(_) => Err(Failure::Cancelled),
            other => {
                return Err(ReplayError::BadJournal(format!(
                    "{} at seq {} is not a completion",
                    other.summary(),
                    ev.seq
                )));
            }
        })
    }

    /// Payload of the `SignalPayload` event at `payload_seq` referenced by the
    /// `SignalMatched` at `matched_seq` (D21). It must exist and precede the match.
    fn signal_payload(
        &self,
        op: OpId,
        matched_seq: u64,
        payload_seq: u64,
    ) -> Result<&Payload, ReplayError> {
        let ev = (payload_seq < matched_seq)
            .then(|| usize::try_from(payload_seq).ok())
            .flatten()
            .and_then(|i| self.journal.get(i));
        match ev.map(|e| &e.body) {
            Some(EventBody::SignalPayload(p)) => Ok(&p.payload),
            Some(other) => Err(ReplayError::BadJournal(format!(
                "signal-matched op {op} at seq {matched_seq} references {} at seq {payload_seq}, \
                 not a signal payload",
                other.summary()
            ))),
            None => Err(ReplayError::BadJournal(format!(
                "signal-matched op {op} at seq {matched_seq} references payload seq \
                 {payload_seq}, which is not an earlier journal event"
            ))),
        }
    }

    /// The event that scheduled `op` (op id == its seq).
    pub fn op_kind(&self, op: OpId) -> Option<&Event> {
        usize::try_from(op)
            .ok()
            .and_then(|i| self.journal.get(i))
            .filter(|e| e.seq == op && e.body.is_guest_command())
    }
}
