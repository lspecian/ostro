//! `InboundBatch` apply on the target partition (STOR-12, apply half).
//!
//! The shuffler (plan 02-13) copies a source partition's outbox rows for this
//! partition into `InboundBatch{src, msgs}`. Delivery is at-least-once; apply
//! makes it exactly-once with the per-source watermark `inbound_seen[src]`:
//!
//! - Leading messages with `seq <= seen` are duplicates and are skipped.
//! - The remaining seqs must be exactly `seen+1, seen+2, …`; any gap rejects
//!   the WHOLE command (`Rejected(SeqGap)`) with no writes.
//! - Nothing new → `Duplicate`.
//!
//! Structural problems (source outside the partition count, a message
//! without a body, a misrouted message, an oversize inline payload, a
//! malformed `ChildCompleted`) also reject the whole command before any
//! write. State-dependent misses (target run unknown or terminal, a child
//! start that is a duplicate, a cron start already claimed) are
//! deterministic per-message drops: the seq is still consumed.
//!
//! Inbound handlers may read more than once (one point read per message plus
//! a children scan for cancel, and the waits/inbox reads of `waits.rs` for
//! signals); they are not on the per-command hot path.
//!
//! Signals and cancel requests are handled in `waits.rs` (plan 05-08): a
//! signal journals its `SignalPayload` and matches the oldest wait or is
//! buffered; a `CancelRun` sets the flag, journals `CancelRequested` once and
//! cascades to the children, or is forwarded along a continue-as-new chain.
//!
//! Phase 9 children (plan 09-08, D-P9-24, D-P9-27):
//!
//! - **Duplicate StartChild (resume re-send, B3).** A `StartChild` for an existing run whose
//!   `RunRecord.parent` equals the incoming `ParentRef` is answered when the child is
//!   terminal: an event-less outbox `ChildCompleted{parent, child, op, status}` (the parent
//!   journals `Err(child_terminated)` / `Err(Timeout)` / `Err(Cancelled)`, D-P9-9). A
//!   `ContinuedAsNew` child forwards the duplicate to `continued_to` (start `run_id`
//!   rewritten, like the CancelRun forward of 05-08). A live child or a parent mismatch is a
//!   drop. One extra point read (the child record).
//! - **Row guard.** The parent journals a `ChildCompleted` only with a matching marked
//!   `children` row: the exact `(parent, child)` row, else (one prefix scan, only when
//!   `children > 0`) the row whose child uuid is `child_run_uuid(parent, op)`, i.e. the
//!   original child of a continue-as-new chain whose successor reports. The row is
//!   removed, so a re-completion never journals an op twice. A uuid match also sends
//!   `CancelRun{reason: "chain_done"}` to the original child. With no row at all the v0.1
//!   semantics are kept (legacy inbox row and wake): v0.1 commands, such as a completion
//!   for a parent that never recorded the child, apply as before, except that the counter
//!   is no longer decremented for a row that does not exist.
//! - **`chain_done`.** A `ContinuedAsNew` run receiving it forwards it to `continued_to` and
//!   clears `continued_to` (no cancel request); a record whose expiry was deferred
//!   (`retention.rs`) is deleted instead. Any other run drops it. "chain_done" is therefore a
//!   reserved CancelRun reason; a user cancel reason equal to it, cascaded to a child, is
//!   dropped by the child (documented limit).

use ostro_types::RunId;
use ostro_types::command::{
    CHILD_ROW_JOURNAL, CancelRun, ChildCompleted, CronStart, InboundBatch, InboundMsg, OutboxMsg,
    StartChild, outbox_msg,
};
use ostro_types::ids::child_run_uuid;
use ostro_types::journal::{self, EventBody, EventKind, Failure, OpOutput, encode_result};
use prost::Message;

use super::cursor::Cursor;
use super::inputs::{journal_encoded, journal_input};
use super::outbox::{emit, msg_oversize, msg_target};
use super::outcome::{Action, ApplyOutcome, InvalidCode, RejectReason};
use super::run::{enqueue_ready, load_run, local_run, start_run, store_run, terminal_status};
use super::{lifecycle, retention, waits};
use crate::store::keys::{children_key, children_prefix, inbox_key};
use crate::store::values::{self, InboxMsg, RunRecord, RunStatus};
use crate::store::{ApplyBatch, StoreError, Table};

/// Dedupe workflow namespace for cron starts: `(__cron/<schedule>, fire_time)`.
const CRON_DEDUPE_PREFIX: &str = "__cron/";

fn reject(r: RejectReason) -> Result<ApplyOutcome, StoreError> {
    Ok(ApplyOutcome::Rejected(r))
}

/// Structural check of one message (no state access).
fn check_msg(cur: &Cursor, m: &InboundMsg) -> Result<(), RejectReason> {
    use outbox_msg::Kind;
    let Some(msg) = m.msg.as_ref() else {
        return Err(RejectReason::Invalid(InvalidCode::MissingField));
    };
    let Some(kind) = &msg.kind else {
        return Err(RejectReason::Invalid(InvalidCode::MissingField));
    };
    match msg.target_partition() {
        Ok(p) if p == cur.partition => {}
        Ok(_) => return Err(RejectReason::WrongPartition),
        Err(_) => return Err(RejectReason::Invalid(InvalidCode::BadRunId)),
    }
    if msg_oversize(msg) {
        return Err(RejectReason::PayloadTooLarge);
    }
    if let Kind::ChildCompleted(c) = kind {
        RunId::from_bytes(&c.child_run)
            .map_err(|_| RejectReason::Invalid(InvalidCode::BadRunId))?;
        terminal_status(c.status).ok_or(RejectReason::Invalid(InvalidCode::BadIntent))?;
    }
    Ok(())
}

/// Returns the not-yet-seen suffix of `msgs`, or the first gap.
fn new_suffix(msgs: &[InboundMsg], seen: u64) -> Result<&[InboundMsg], RejectReason> {
    let skip = msgs.iter().take_while(|m| m.seq <= seen).count();
    let new = &msgs[skip..];
    let mut expected = seen.checked_add(1);
    for m in new {
        match expected {
            Some(e) if m.seq == e => expected = e.checked_add(1),
            // `None`: a message after seq u64::MAX can never be contiguous.
            _ => {
                return Err(RejectReason::SeqGap {
                    expected: expected.unwrap_or(u64::MAX),
                    found: m.seq,
                });
            }
        }
    }
    Ok(new)
}

pub(crate) fn apply_inbound_batch(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    ib: &InboundBatch,
    actions: &mut Vec<Action>,
) -> Result<ApplyOutcome, StoreError> {
    let src = match u16::try_from(ib.src) {
        Ok(s) if cur.count.contains(ostro_types::PartitionId(s)) => s,
        _ => return reject(RejectReason::Invalid(InvalidCode::BadSource)),
    };
    for m in &ib.msgs {
        if let Err(r) = check_msg(cur, m) {
            return reject(r);
        }
    }
    let seen = cur.inbound_seen.get(&src).copied().unwrap_or(0);
    let new = match new_suffix(&ib.msgs, seen) {
        Ok(n) => n,
        Err(r) => return reject(r),
    };
    let Some(last) = new.last() else {
        return Ok(ApplyOutcome::Duplicate { existing: None });
    };

    // All checks passed: write.
    for m in new {
        let Some(OutboxMsg { kind: Some(kind) }) = &m.msg else {
            continue; // validated by check_msg
        };
        apply_msg(b, cur, kind, actions)?;
    }
    cur.set_inbound_seen(src, last.seq);
    Ok(ApplyOutcome::Accepted)
}

fn apply_msg(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    kind: &outbox_msg::Kind,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    use outbox_msg::Kind;
    match kind {
        Kind::StartChild(c) => {
            // The child run id is unique: a resend hits the runs-row check
            // (Duplicate); 09-08 answers a duplicate for a terminal child (B3).
            if let Some(s) = &c.start
                && let ApplyOutcome::Duplicate {
                    existing: Some(child),
                } = start_run(b, cur, s, None, actions)?
            {
                duplicate_start_child(b, cur, c, &child, actions)?;
            }
        }
        Kind::CronStart(c) => cron_start(b, cur, c, actions)?,
        Kind::ChildCompleted(c) => child_completed(b, cur, c, actions)?,
        // Phase 5: signals go to `waits.rs` (05-08: wait matching, B1 pending slots),
        // continue-as-new successor starts to `lifecycle.rs` (05-09; consumed with no effect
        // until then, like the other per-message deterministic drops).
        Kind::Signal(s) => waits::inbound_signal(b, cur, s, actions)?,
        // Graceful cancel (plan 05-08): flag + journaled CancelRequested + children cascade,
        // forwarded along a continue-as-new chain (PF-F2).
        // 09-08 (D-P9-27): the chain bookkeeping reason is never a cancel request.
        Kind::CancelRun(c) if c.reason == REASON_CHAIN_DONE => chain_done(b, cur, c, actions)?,
        Kind::CancelRun(c) => waits::inbound_cancel(b, cur, c, actions)?,
        Kind::StartRun(s) => lifecycle::inbound_start_run(b, cur, s, actions)?,
    }
    Ok(())
}

/// Loads a live (non-terminal) target run; `None` = deterministic drop.
pub(crate) fn live_run(
    b: &mut ApplyBatch<'_>,
    cur: &Cursor,
    bytes: &[u8],
) -> Result<Option<(RunId, RunRecord)>, StoreError> {
    let Ok(run) = local_run(cur, bytes) else {
        return Ok(None);
    };
    Ok(load_run(b, &run)?
        .filter(|rec| !rec.status.is_terminal())
        .map(|rec| (run, rec)))
}

fn push_inbox(b: &mut ApplyBatch<'_>, run: &RunId, rec: &mut RunRecord, msg: &InboxMsg) {
    b.insert(
        Table::Inbox,
        inbox_key(run, rec.inbox_next),
        values::encode(msg),
    );
    rec.inbox_next += 1;
}

/// "Make ready": a Scheduled run becomes Ready (ready row + `EnqueueReady`)
/// at `ready_at_ms`. Ready and Running runs get no new ready row: Running
/// runs see the inbox on their next activation (wake-up policy: Phase 4).
pub(crate) fn make_ready(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &mut RunRecord,
    ready_at_ms: u64,
    actions: &mut Vec<Action>,
) {
    if rec.status == RunStatus::Scheduled {
        enqueue_ready(b, run, rec, ready_at_ms, actions);
    }
}

fn cron_start(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    c: &CronStart,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    let Some(s) = &c.start else { return Ok(()) };
    let dedupe = (
        format!("{CRON_DEDUPE_PREFIX}{}", c.schedule_id),
        c.fire_time_ms.to_be_bytes().to_vec(),
    );
    // A claimed (schedule, fire_time) yields Duplicate (ReturnExisting) or
    // IdempotencyConflict (Reject): either way exactly one run exists.
    start_run(b, cur, s, Some(dedupe), actions)?;
    Ok(())
}

/// `CancelRun` reason the parent sends to the original child of a finished continue-as-new
/// chain (09-08, D-P9-27).
pub(crate) const REASON_CHAIN_DONE: &str = "chain_done";

/// A `StartChild` whose run already exists (09-08, B3; module docs). Reads: the child record.
fn duplicate_start_child(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    c: &StartChild,
    child: &RunId,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    let Some(s) = &c.start else { return Ok(()) };
    let Some(p) = &s.parent else { return Ok(()) };
    let Some(rec) = load_run(b, child)? else {
        return Ok(());
    };
    let Ok(parent) = p.run_id() else {
        return Ok(());
    };
    if rec.parent != Some((*parent.as_bytes(), p.op)) || !rec.status.is_terminal() {
        return Ok(());
    }
    let msg = if rec.status == RunStatus::ContinuedAsNew {
        let Some(next) = rec.continued_to else {
            return Ok(());
        };
        let mut fwd = s.clone();
        fwd.run_id = next.as_bytes().to_vec();
        OutboxMsg {
            kind: Some(outbox_msg::Kind::StartChild(StartChild {
                start: Some(fwd),
            })),
        }
    } else {
        OutboxMsg {
            kind: Some(outbox_msg::Kind::ChildCompleted(ChildCompleted {
                parent_run: p.run_id.clone(),
                child_run: child.as_bytes().to_vec(),
                op: p.op,
                status: c_status(rec.status),
                result: None,
                event: None,
            })),
        }
    };
    // An unaddressable target (outside the partition count) is a drop, never a failure.
    if msg_target(cur, &msg).is_ok() {
        emit(b, cur, &msg, actions)?;
    }
    Ok(())
}

fn c_status(s: RunStatus) -> i32 {
    use ostro_types::command::TerminalStatus as T;
    (match s {
        RunStatus::Completed => T::Completed,
        RunStatus::Failed => T::Failed,
        RunStatus::Cancelled => T::Cancelled,
        RunStatus::Terminated => T::Terminated,
        RunStatus::TimedOut => T::TimedOut,
        _ => T::ContinuedAsNew,
    }) as i32
}

/// One `(key, value)` row.
type KeyValue = (Vec<u8>, Vec<u8>);

/// The `children` row of a continue-as-new chain's original child: the row of `parent`
/// whose child uuid is `child_run_uuid(parent, op)` (D-P9-27). One prefix scan, only when
/// `children > 0`.
fn chain_row(
    b: &mut ApplyBatch<'_>,
    parent: &RunId,
    rec: &RunRecord,
    op: u64,
) -> Result<Option<KeyValue>, StoreError> {
    if rec.children == 0 {
        return Ok(None);
    }
    let want = child_run_uuid(parent, op);
    Ok(b.range_prefix(Table::Children, &children_prefix(parent))?
        .into_iter()
        .find(|(k, _)| {
            k.get(RunId::LEN..)
                .and_then(|c| RunId::from_bytes(c).ok())
                .is_some_and(|c| c.uuid() == want)
        }))
}

/// `CancelRun{reason: "chain_done"}` (09-08, D-P9-27; module docs).
fn chain_done(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    c: &CancelRun,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    let Ok(run) = local_run(cur, &c.target_run) else {
        return Ok(());
    };
    let Some(mut rec) = load_run(b, &run)? else {
        return Ok(());
    };
    if rec.status != RunStatus::ContinuedAsNew {
        return Ok(());
    }
    let Some(next) = rec.continued_to else {
        return Ok(());
    };
    let msg = OutboxMsg {
        kind: Some(outbox_msg::Kind::CancelRun(CancelRun {
            target_run: next.as_bytes().to_vec(),
            reason: REASON_CHAIN_DONE.to_string(),
        })),
    };
    if msg_target(cur, &msg).is_ok() {
        emit(b, cur, &msg, actions)?;
    }
    if retention::is_deferred(&rec) {
        retention::delete_deferred(b, &run, &mut rec, actions)?;
    } else {
        rec.continued_to = None;
        store_run(b, &run, &rec);
    }
    Ok(())
}

fn child_completed(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    c: &ChildCompleted,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    let Some((parent, mut rec)) = live_run(b, cur, &c.parent_run)? else {
        return Ok(());
    };
    let child = RunId::from_bytes(&c.child_run)
        .map_err(|e| StoreError::Codec(format!("validated child run id: {e}")))?;
    let status = terminal_status(c.status)
        .ok_or_else(|| StoreError::Codec("validated terminal status".into()))?;
    // 09-08 row guard: the exact row, else the original child's row of a continue-as-new
    // chain (uuid match, D-P9-27). Journaling needs a marked row and consumes it, so a
    // re-completion is never journaled twice. No row at all keeps the v0.1 path unchanged
    // (legacy inbox row; format v0.1 frozen).
    let exact = children_key(&parent, &child);
    let (row, value, chained) = match b.get(Table::Children, &exact)? {
        Some(v) => (exact, Some(v), None),
        None => match chain_row(b, &parent, &rec, c.op)? {
            Some((k, v)) => {
                let original = k[RunId::LEN..].to_vec();
                (k, Some(v), Some(original))
            }
            None => (exact, None, None),
        },
    };
    // 09-05: a row marked by the opt-in `ChildJournal` intent journals the completion as
    // the parent's kind-67 event (D21); any other case keeps the v0.1 inbox row.
    let journaled = value.as_deref() == Some(CHILD_ROW_JOURNAL);
    if journaled {
        match &c.event {
            // Submitter-encoded by the child's derive (PF-F1): verbatim, never decoded or
            // fetched (BlobRef invariant). Its size was checked on the child's CompleteRun.
            Some(ev) => {
                journal_encoded(
                    b,
                    &parent,
                    &mut rec,
                    EventKind::ChildCompleted,
                    ev,
                    true,
                    actions,
                )?;
            }
            // Apply-built hard stops (and 09-08's re-completion): a no-user-data failure
            // encoded here (D21 permits it). The inbound message carries no time, so the
            // event ts is the parent's `created_at_ms`, like inbound CancelRequested.
            None => {
                let at = rec.created_at_ms;
                journal_input(
                    b,
                    &parent,
                    &mut rec,
                    at,
                    EventBody::ChildCompleted(journal::ChildCompleted {
                        op: c.op,
                        outcome: encode_result(&Err::<OpOutput, Failure>(hard_stop_failure(
                            status,
                        ))),
                    }),
                    actions,
                )?;
            }
        }
    } else {
        let result = c
            .result
            .as_ref()
            .map(Message::encode_to_vec)
            .unwrap_or_default();
        push_inbox(
            b,
            &parent,
            &mut rec,
            &InboxMsg::ChildCompleted {
                child: *child.as_bytes(),
                op: c.op,
                status,
                result,
            },
        );
        let at = rec.created_at_ms;
        make_ready(b, &parent, &mut rec, at, actions);
    }
    // 09-08: only a row that exists is consumed and counted (a stray re-completion must
    // not drift the counter the cascades rely on).
    if value.is_some() {
        b.remove(Table::Children, row);
        rec.children = rec.children.saturating_sub(1);
    }
    store_run(b, &parent, &rec);
    // 09-08 (D-P9-27): the chain is done; its predecessors stop being kept for forwarding.
    if let Some(original) = chained {
        let msg = OutboxMsg {
            kind: Some(outbox_msg::Kind::CancelRun(CancelRun {
                target_run: original,
                reason: REASON_CHAIN_DONE.to_string(),
            })),
        };
        if msg_target(cur, &msg).is_ok() {
            emit(b, cur, &msg, actions)?;
        }
    }
    Ok(())
}

/// Error kind of a child that was terminated (or whose result arrived while the parent was
/// terminal, D-P9-24).
pub(crate) const CHILD_TERMINATED_KIND: &str = "child_terminated";

/// The parent-side failure of an event-less `ChildCompleted` (D-P9-9, PF-W7): Terminated ->
/// non-retryable `child_terminated`, TimedOut -> `Timeout`, Cancelled -> `Cancelled`;
/// Completed/Failed (re-completion of a resumed parent, D-P9-24 B3) -> `child_terminated`.
pub(crate) fn hard_stop_failure(status: RunStatus) -> Failure {
    let message = match status {
        RunStatus::TimedOut => return Failure::Timeout,
        RunStatus::Cancelled => return Failure::Cancelled,
        RunStatus::Terminated => "child run was terminated",
        RunStatus::Completed | RunStatus::Failed => "child completed while the parent was terminal",
        _ => "child run ended without a result",
    };
    Failure::NonRetryable(journal::ErrorInfo {
        kind: CHILD_TERMINATED_KIND.to_string(),
        message: message.to_string(),
        details: None,
    })
}
