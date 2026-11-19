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
//! a children scan for cancel); they are not on the per-command hot path.

use ostro_types::RunId;
use ostro_types::command::{
    CancelRun, ChildCompleted, CronStart, InboundBatch, InboundMsg, OutboxMsg, Signal, outbox_msg,
};
use prost::Message;

use super::cursor::Cursor;
use super::outbox::{emit, msg_oversize};
use super::outcome::{Action, ApplyOutcome, InvalidCode, RejectReason};
use super::run::{enqueue_ready, load_run, local_run, start_run, store_run, terminal_status};
use crate::store::keys::{children_key, children_prefix, inbox_key};
use crate::store::values::{self, InboxMsg, RunRecord, RunStatus};
use crate::store::{ApplyBatch, StoreError, Table};

/// Dedupe workflow namespace for cron starts: `(__cron/<schedule>, fire_time)`.
const CRON_DEDUPE_PREFIX: &str = "__cron/";

const RUN_LEN: usize = 18;

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
            // (Duplicate) and is consumed like any other outcome.
            if let Some(s) = &c.start {
                start_run(b, cur, s, None, actions)?;
            }
        }
        Kind::CronStart(c) => cron_start(b, cur, c, actions)?,
        Kind::ChildCompleted(c) => child_completed(b, cur, c, actions)?,
        Kind::Signal(s) => signal(b, cur, s, actions)?,
        Kind::CancelRun(c) => cancel_run(b, cur, c, actions)?,
        // Phase 5: implemented in 05-09 (`lifecycle::inbound_start_run`). Until then the
        // message is consumed with no effect, like the other per-message deterministic drops
        // (a missing or terminal target run): the batch stays accepted and the source's
        // outbox still truncates.
        Kind::StartRun(_) => {}
    }
    Ok(())
}

/// Loads a live (non-terminal) target run; `None` = deterministic drop.
fn live_run(
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

fn child_completed(
    b: &mut ApplyBatch<'_>,
    cur: &Cursor,
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
    b.remove(Table::Children, children_key(&parent, &child));
    rec.children = rec.children.saturating_sub(1);
    let at = rec.created_at_ms;
    make_ready(b, &parent, &mut rec, at, actions);
    store_run(b, &parent, &rec);
    Ok(())
}

fn signal(
    b: &mut ApplyBatch<'_>,
    cur: &Cursor,
    s: &Signal,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    let Some((run, mut rec)) = live_run(b, cur, &s.target_run)? else {
        return Ok(());
    };
    let payload = s
        .payload
        .as_ref()
        .map(Message::encode_to_vec)
        .unwrap_or_default();
    push_inbox(
        b,
        &run,
        &mut rec,
        &InboxMsg::Signal {
            name: s.name.clone(),
            payload,
        },
    );
    let at = rec.created_at_ms;
    make_ready(b, &run, &mut rec, at, actions);
    store_run(b, &run, &rec);
    Ok(())
}

fn cancel_run(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    c: &CancelRun,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    let Some((run, mut rec)) = live_run(b, cur, &c.target_run)? else {
        return Ok(());
    };
    if rec.cancel_requested {
        return Ok(()); // already requested: no second cascade
    }
    rec.cancel_requested = true;
    push_inbox(
        b,
        &run,
        &mut rec,
        &InboxMsg::CancelRequested {
            reason: c.reason.clone(),
        },
    );
    let at = rec.created_at_ms;
    make_ready(b, &run, &mut rec, at, actions);
    store_run(b, &run, &rec);
    actions.push(Action::CancelRequested { run });

    // Cascade to every child, wherever it lives (one prefix scan).
    for (k, _) in b.range_prefix(Table::Children, &children_prefix(&run))? {
        let child_bytes = k
            .get(RUN_LEN..)
            .ok_or_else(|| StoreError::Codec("short children key".into()))?;
        let msg = OutboxMsg {
            kind: Some(outbox_msg::Kind::CancelRun(CancelRun {
                target_run: child_bytes.to_vec(),
                reason: c.reason.clone(),
            })),
        };
        emit(b, cur, &msg, actions)?;
    }
    Ok(())
}
