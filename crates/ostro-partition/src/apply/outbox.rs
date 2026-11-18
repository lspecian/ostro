//! Outbox emission (STOR-12, source side).
//!
//! Every cross-partition message, including messages to this partition
//! (one code path, research Open Question 3), becomes an `outbox` row
//! `(target, seq) -> prost(OutboxMsg)` in the same batch as the command that
//! produced it. `seq` is dense per target and starts at 1; the counter lives
//! in [`CursorMeta::outbox_next_seq`](crate::store::values::CursorMeta).

use ostro_types::command::{OutboxMsg, OutboxTruncated, outbox_msg};
use ostro_types::{PartitionId, PayloadRef};
use prost::Message;

use super::cursor::Cursor;
use super::outcome::{Action, ApplyOutcome, InvalidCode, RejectReason};
use crate::store::keys::outbox_key;
use crate::store::{ApplyBatch, StoreError, Table};

/// Validates that `msg` names a target partition inside the partition count.
/// Apply calls this before its first write, so [`emit`] cannot fail on it.
pub(crate) fn msg_target(cur: &Cursor, msg: &OutboxMsg) -> Result<PartitionId, RejectReason> {
    match msg.target_partition() {
        Ok(p) if cur.count.contains(p) => Ok(p),
        _ => Err(RejectReason::Invalid(InvalidCode::BadRunId)),
    }
}

/// Whether any payload carried by `msg` is an oversize inline payload.
pub(crate) fn msg_oversize(msg: &OutboxMsg) -> bool {
    use outbox_msg::Kind;
    let big = |p: &Option<PayloadRef>| p.as_ref().is_some_and(PayloadRef::is_oversize_inline);
    match &msg.kind {
        None | Some(Kind::CancelRun(_)) => false,
        Some(Kind::StartChild(c)) => c.start.as_ref().is_some_and(|s| big(&s.input)),
        Some(Kind::CronStart(c)) => c.start.as_ref().is_some_and(|s| big(&s.input)),
        Some(Kind::ChildCompleted(c)) => big(&c.result),
        Some(Kind::Signal(s)) => big(&s.payload),
    }
}

/// Appends `msg` to the outbox of its target partition.
pub(crate) fn emit(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    msg: &OutboxMsg,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    let target = msg
        .target_partition()
        .map_err(|e| StoreError::Codec(format!("outbox message without a valid target: {e}")))?;
    let next = cur.meta.outbox_next_seq.entry(target.0).or_insert(1);
    let seq = *next;
    *next = seq + 1;
    b.insert(
        Table::Outbox,
        outbox_key(target.0, seq),
        msg.encode_to_vec(),
    );
    actions.push(Action::OutboxAppended { target, seq });
    Ok(())
}

/// `OutboxTruncated` apply. Stub until plan 02-10.
pub(crate) fn apply_outbox_truncated(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _t: &OutboxTruncated,
) -> Result<ApplyOutcome, StoreError> {
    Ok(ApplyOutcome::Rejected(RejectReason::Invalid(
        InvalidCode::NotImplemented,
    )))
}
