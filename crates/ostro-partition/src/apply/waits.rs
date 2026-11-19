//! Signals, await tokens, wait timeouts and graceful cancel (plan **05-08**; seams by 05-02).
//!
//! Semantics (05-RESEARCH "Signals and waits", "Await tokens", "Graceful cancel"; D4, D5, D13,
//! D21, B1, W5, PF-F2):
//!
//! - **Op registry.** The executor's derive step sends `RegisterWait{op, kind, name,
//!   timeout_due_ms, at_ms}` and `CancelWait{op}` intents, so apply never decodes a journal
//!   payload. A wait is the `waits` row `wait_key(run, op) -> WaitRow`, plus (signals only) the
//!   FIFO index row `wait_name_key(run, name, op)`, plus (with a timeout) the timer row
//!   `(timeout_due_ms, run, op)`. `RunRecord.open_waits` equals the number of live wait rows
//!   after every command (the terminal hook skips its scans when it is 0).
//! - **Signals (D21).** Every delivered signal journals the submitter-encoded `SignalPayload`
//!   event (kind 96) verbatim at `next_seq` with [`journal_encoded`]. If a wait for the name
//!   exists, apply then journals its own `SignalMatched{op, payload_seq}` (kind 97) for the
//!   **oldest** waiting op with [`journal_input`], removes that wait and its timer and wakes the
//!   run. Otherwise an inbox slot `SignalSlot::Journaled{payload_seq, sent_at_ms}` is stored at
//!   `signal_inbox_key(run, name, inbox_next)`, and the next `RegisterWait` for that name
//!   journals `SignalMatched` for the oldest slot (FIFO per name).
//! - **B1.** A signal that arrives while `next_seq == 0` (no `RunStarted` yet) journals nothing:
//!   it is stored as `SignalSlot::Pending{event, sent_at_ms}`. [`on_first_events`] journals the
//!   pending events right after the seq-0 batch's events and converts the slots to `Journaled`.
//! - **Wait timeouts (D4).** A wait-timeout timer that fires while the wait row exists journals
//!   `WaitTimedOut{op}` (kind 70), not `TimerFired`. The wait row is the token: whichever of
//!   signal/token/timeout/cancel applies first removes the row **and** the timer row, so the
//!   loser finds nothing (a later timer fire is `Duplicate` at the timer-row check, a later
//!   signal goes to the inbox, a later `CompleteToken` is `Duplicate`). There are never two
//!   completions for one op.
//! - **Tokens (D21).** `CompleteToken` journals the submitter-encoded `TokenCompleted` event
//!   (kind 68) verbatim. Tokens are never buffered.
//! - **Graceful cancel.** `RequestCancel` and inbound `CancelRun` set `cancel_requested` and
//!   `cancel_reason`, journal `CancelRequested{reason}` (kind 69) once, wake the run and cascade
//!   a graceful `CancelRun` to every child through the outbox. A run whose journal is still
//!   empty (`next_seq == 0`) gets the flag only: an event at seq 0 would precede `RunStarted`
//!   (the executor's RunStarted synthesis reads the flag, 05-11 journals `CancelRequested`
//!   right after it). A terminal `ContinuedAsNew` run with `continued_to` forwards the cancel to
//!   its successor (PF-F2), hop by hop along the chain.
//! - **W5.** Inline `PayloadRef`s over 256 KiB in `SignalRun.event`, `CompleteToken.event` and
//!   inbound `Signal.payload` are rejected `PayloadTooLarge` (the inbound case rejects the
//!   whole batch in `inbound::check_msg`). Blob refs of any size are journaled verbatim; apply
//!   has no blob store access at all, so it can never fetch them.
//!
//! **Timestamps.** `SignalMatched` gets `SignalRun.sent_at_ms` (immediate match) or
//! `RegisterWait.at_ms` (match at registration); an inbound outbox `Signal` carries no time, so
//! its immediate match uses `rec.created_at_ms`. `WaitTimedOut` gets `TimerFired.fired_at_ms`,
//! `CancelRequested` gets `RequestCancel.requested_at_ms` (inbound: `rec.created_at_ms`).
//!
//! **Read counts** (each documented on its function and asserted in `tests/apply_waits.rs`
//! with the `ApplyBatch` read counter; the overlay serves rows written earlier in the same
//! batch without a read):
//!
//! | Command / intent | Keyspace reads |
//! |---|---|
//! | `SignalRun` | run record; + `signal_seen` point read when `request_id` is set; + waits-by-name prefix when `next_seq > 0` and `open_waits > 0`; + the matched wait row on a match |
//! | `RegisterWait` | wait-row point read (idempotency) when `open_waits > 0`; + inbox-by-name prefix for a signal wait |
//! | `CancelWait` | wait-row point read when `open_waits > 0` |
//! | `CompleteToken` | run record; + wait-row point read when `open_waits > 0` |
//! | `RequestCancel` | run record; + children prefix when `children > 0` |
//! | `TimerFired` (guest op) | timer row + run record (timer.rs); + wait-row point read when `open_waits > 0` |
//! | `on_first_events` | inbox run prefix, only when `inbox_next > 0` |
//!
//! Every wait-row read and the waits-by-name prefix are skipped when `open_waits == 0` (exact
//! count, so no wait row can exist): a sleep timer of a run without waits keeps the 04-13
//! budget of two reads, and a signal to a run without waits costs one read (+ `signal_seen`).
//!
//! **Same-batch Suspend.** A `RegisterWait` that matches a buffered signal journals
//! `SignalMatched` past the executor's journal end, so the executor's later `Suspend` /
//! `EnqueueReady` (always a separate `EventsAppended` at its own journal end, `sink.rs`) fails
//! its `from_seq` compare-and-set and the executor re-activates (Pitfall 2). An executor must
//! not put `RegisterWait` and `Suspend` in one command.

use ostro_types::command::{
    CancelRun, CancelWait, CompleteToken, MAX_SIGNAL_NAME_LEN, OutboxMsg, RegisterWait,
    RequestCancel, Signal, SignalRun, TimerFired, WaitKind as CmdWaitKind, is_reserved_op,
    outbox_msg,
};
use ostro_types::journal::{self, EventBody, EventKind};
use ostro_types::{PayloadRef, RunId};

use super::cursor::Cursor;
use super::inbound::make_ready;
use super::inputs::{journal_encoded, journal_input};
use super::outbox::{emit, msg_target};
use super::outcome::{Action, ApplyOutcome, ApplyResult, InvalidCode, RejectReason};
use super::run::{load_run, local_run, store_run};
use crate::store::keys::{
    children_prefix, decode_signal_inbox_key, decode_wait_name_key, run_prefix, signal_inbox_key,
    signal_inbox_prefix, signal_seen_key, timer_key, wait_key, wait_name_key, wait_name_prefix,
};
use crate::store::values::{self, RunRecord, RunStatus, SignalSlot, WaitKind, WaitRow};
use crate::store::{ApplyBatch, StoreError, Table};

const RUN_LEN: usize = 18;

fn reject(r: RejectReason) -> ApplyResult {
    Ok(ApplyOutcome::Rejected(r))
}

fn invalid(c: InvalidCode) -> RejectReason {
    RejectReason::Invalid(c)
}

fn duplicate() -> ApplyResult {
    Ok(ApplyOutcome::Duplicate { existing: None })
}

// ------------------------------------------------------------------ wait rows

/// The wait row of `(run, op)`: one point read, or none at all when the run has no open wait
/// (`open_waits` is exact, so `0` proves the row is absent). This keeps a plain sleep timer at
/// timer.rs's two reads and makes wait intents free for runs without waits.
fn read_wait(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &RunRecord,
    op: u64,
) -> Result<Option<WaitRow>, StoreError> {
    if rec.open_waits == 0 {
        return Ok(None);
    }
    b.get(Table::Waits, &wait_key(run, op))?
        .map(|v| values::decode(&v))
        .transpose()
}

/// Inserts a new wait (row, FIFO index for signals, timeout timer) and counts it.
fn insert_wait(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &mut RunRecord,
    op: u64,
    row: &WaitRow,
    actions: &mut Vec<Action>,
) {
    b.insert(Table::Waits, wait_key(run, op), values::encode(row));
    if row.kind == WaitKind::Signal {
        b.insert(Table::Waits, wait_name_key(run, &row.name, op), Vec::new());
    }
    if row.timeout_due_ms > 0 {
        b.insert(
            Table::Timers,
            timer_key(row.timeout_due_ms, run, op),
            Vec::new(),
        );
        actions.push(Action::ScheduleTimer {
            due_at_ms: row.timeout_due_ms,
            run: *run,
            op,
        });
    }
    rec.open_waits = rec.open_waits.saturating_add(1);
}

/// Removes a live wait (row, FIFO index, timeout timer) and uncounts it. `fired_due` is the
/// due time of a timer that has just fired (its row is already gone): no `CancelTimer` for it.
fn remove_wait(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &mut RunRecord,
    op: u64,
    row: &WaitRow,
    fired_due: Option<u64>,
    actions: &mut Vec<Action>,
) {
    b.remove(Table::Waits, wait_key(run, op));
    if row.kind == WaitKind::Signal {
        b.remove(Table::Waits, wait_name_key(run, &row.name, op));
    }
    if row.timeout_due_ms > 0 && fired_due != Some(row.timeout_due_ms) {
        b.remove(Table::Timers, timer_key(row.timeout_due_ms, run, op));
        actions.push(Action::CancelTimer {
            due_at_ms: row.timeout_due_ms,
            run: *run,
            op,
        });
    }
    rec.open_waits = rec.open_waits.saturating_sub(1);
}

/// A `Running` run gets `InputAppended` for an input journaled without the waking helper.
fn input_appended(rec: &RunRecord, run: &RunId, seq: u64, actions: &mut Vec<Action>) {
    if rec.status == RunStatus::Running {
        actions.push(Action::InputAppended { run: *run, seq });
    }
}

// ------------------------------------------------------------------ signals

/// How a signal reached the partition.
#[derive(Clone, Copy)]
enum Via {
    /// `Cmd::SignalRun`: `sent_at_ms` from the submitter; an immediate match is stamped with it.
    Command { sent_at_ms: u64 },
    /// Inbound outbox `Signal`: no send time (`sent_at_ms` 0). A `Pending` slot still wakes a
    /// `Scheduled` run (the Phase 2 wake-up rule of inbound messages, kept by 05-02).
    Inbound,
}

/// Delivers a validated signal to the live run `rec` (record stored by the caller).
///
/// Reads: none while `next_seq == 0`; else the waits-by-name prefix when `open_waits > 0`,
/// plus the matched wait row on a match.
fn deliver_signal(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &mut RunRecord,
    name: &str,
    event: &PayloadRef,
    via: Via,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    let (sent_at_ms, match_ts_ms) = match via {
        Via::Command { sent_at_ms } => (sent_at_ms, sent_at_ms),
        Via::Inbound => (0, rec.created_at_ms),
    };
    let n = rec.inbox_next;

    // B1: nothing may precede RunStarted at seq 0. No wait can exist yet either (a wait is
    // registered for an op that is already journaled).
    if rec.next_seq == 0 {
        if matches!(via, Via::Inbound) {
            let at = rec.created_at_ms;
            make_ready(b, run, rec, at, actions);
        }
        b.insert(
            Table::Inbox,
            signal_inbox_key(run, name, n),
            values::encode(&SignalSlot::Pending {
                event: event.clone(),
                sent_at_ms,
            }),
        );
        rec.inbox_next = n.saturating_add(1);
        return Ok(());
    }

    // D21: the submitter-encoded SignalPayload, verbatim, always first.
    let payload_seq =
        journal_encoded(b, run, rec, EventKind::SignalPayload, event, false, actions)?;
    input_appended(rec, run, payload_seq, actions);

    let oldest = if rec.open_waits > 0 {
        b.range_prefix(Table::Waits, &wait_name_prefix(run, name))?
            .into_iter()
            .next()
    } else {
        None
    };
    if let Some((k, _)) = oldest {
        let (_, _, op) = decode_wait_name_key(&k)
            .ok_or_else(|| StoreError::Codec("malformed waits index key".into()))?;
        let row = read_wait(b, run, rec, op)?
            .ok_or_else(|| StoreError::Codec("waits index row without its wait row".into()))?;
        remove_wait(b, run, rec, op, &row, None, actions);
        journal_input(
            b,
            run,
            rec,
            match_ts_ms,
            EventBody::SignalMatched(journal::SignalMatched { op, payload_seq }),
            actions,
        )?;
    } else {
        // Nothing waits: buffer the slot; the run is not woken (a Running run already got
        // InputAppended so its journal view stays contiguous).
        b.insert(
            Table::Inbox,
            signal_inbox_key(run, name, n),
            values::encode(&SignalSlot::Journaled {
                payload_seq,
                sent_at_ms,
            }),
        );
        rec.inbox_next = n.saturating_add(1);
    }
    Ok(())
}

/// `Cmd::SignalRun` (tag 8).
///
/// Validation, in order: run id (`BadRunId` / `WrongPartition`), name and request id limits
/// (`FieldTooLong`), `event` present (`MissingField`), W5 inline size (`PayloadTooLarge`),
/// run exists (`RunNotFound`), run live (`RunTerminal`), `request_id` unseen (`Duplicate`).
/// Then the `signal_seen` row is written and the signal delivered (module doc): a `Pending`
/// slot while `next_seq == 0` (no wake), else `SignalPayload` verbatim and either
/// `SignalMatched` for the oldest wait (wake: Scheduled → Ready, Running → `InputAppended` for
/// both seqs) or a `Journaled` inbox slot.
///
/// **Reads** (asserted exception to the ≤ 1-read rule, W4): the run record (1); + the
/// `signal_seen` point read when `request_id` is set; + the waits-by-name prefix when
/// `next_seq > 0` and `open_waits > 0`; + the matched wait row's point read on a match (its
/// timer due locates the timer row). So 1..=4.
pub(crate) fn signal_run(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    s: &SignalRun,
    actions: &mut Vec<Action>,
) -> ApplyResult {
    let run = match local_run(cur, &s.run_id) {
        Ok(r) => r,
        Err(r) => return reject(r),
    };
    if s.check_limits().is_err() {
        return reject(invalid(InvalidCode::FieldTooLong));
    }
    let Some(event) = &s.event else {
        return reject(invalid(InvalidCode::MissingField));
    };
    if event.is_oversize_inline() {
        return reject(RejectReason::PayloadTooLarge);
    }
    let Some(mut rec) = load_run(b, &run)? else {
        return reject(RejectReason::RunNotFound);
    };
    if rec.status.is_terminal() {
        return reject(RejectReason::RunTerminal);
    }
    if !s.request_id.is_empty() {
        let k = signal_seen_key(&run, &s.request_id);
        if b.get(Table::SignalSeen, &k)?.is_some() {
            return duplicate();
        }
        b.insert(Table::SignalSeen, k, Vec::new());
    }
    deliver_signal(
        b,
        &run,
        &mut rec,
        &s.name,
        event,
        Via::Command {
            sent_at_ms: s.sent_at_ms,
        },
        actions,
    )?;
    store_run(b, &run, &rec);
    Ok(ApplyOutcome::Accepted)
}

/// Inbound outbox `Signal` (one message of an `InboundBatch`; state misses are deterministic
/// drops). `s.payload` is the sender-encoded `SignalPayload` event (D21). Oversize inline
/// payloads already rejected the whole batch (`inbound::check_msg`, W5); a message without a
/// payload or with a name over `MAX_SIGNAL_NAME_LEN` is dropped. Cross-partition dedupe is the
/// batch's `(src, seq)` watermark, so there is no `signal_seen` row.
///
/// Same delivery as [`signal_run`] with `sent_at_ms = 0` (the outbox message carries no time);
/// an immediate match is stamped `rec.created_at_ms`. While `next_seq == 0` the `Pending` slot
/// also wakes a `Scheduled` run (Phase 2 inbound wake-up, unchanged from 05-02).
///
/// Reads: the run record; + the waits-by-name prefix when `next_seq > 0` and
/// `open_waits > 0`; + the matched wait row on a match.
pub(crate) fn inbound_signal(
    b: &mut ApplyBatch<'_>,
    cur: &Cursor,
    s: &Signal,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    let Some(event) = &s.payload else {
        return Ok(());
    };
    if s.name.len() > MAX_SIGNAL_NAME_LEN {
        return Ok(());
    }
    let Some((run, mut rec)) = super::inbound::live_run(b, cur, &s.target_run)? else {
        return Ok(());
    };
    deliver_signal(b, &run, &mut rec, &s.name, event, Via::Inbound, actions)?;
    store_run(b, &run, &rec);
    Ok(())
}

/// B1: called by `run.rs` exactly once per run, after the batch that wrote seq 0 has written
/// its events and before its derived intents are applied (05-16 adds the call after
/// `StartRun.run_started`). Every `SignalSlot::Pending` of the run is journaled with
/// [`journal_encoded`] at `next_seq`, **in arrival order** (inbox counter `n`, so per-name FIFO
/// holds and the history shows send order), and its slot is rewritten to
/// `Journaled{payload_seq, sent_at_ms}`. Each journaled event wakes the run (`InputAppended`
/// for a Running run), so the executor's next append conflicts and re-activates (Pitfall 2).
/// A `RegisterWait` later in the same batch then matches a converted slot.
///
/// Reads: the inbox run prefix (1), only when `inbox_next > 0`; otherwise none.
pub(crate) fn on_first_events(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &mut RunRecord,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    #[cfg(test)]
    FIRST_EVENTS_CALLS.with(|c| c.set(c.get() + 1));
    if rec.inbox_next == 0 {
        return Ok(());
    }
    let mut pending = Vec::new();
    for (k, v) in b.range_prefix(Table::Inbox, &run_prefix(run))? {
        // Legacy InboxMsg rows (run ‖ 0xFFFF ‖ n) are not signal rows.
        let Some((_, _, n)) = decode_signal_inbox_key(&k) else {
            continue;
        };
        if let SignalSlot::Pending { event, sent_at_ms } = values::decode(&v)? {
            pending.push((n, k, event, sent_at_ms));
        }
    }
    pending.sort_by_key(|(n, ..)| *n);
    for (_, k, event, sent_at_ms) in pending {
        let payload_seq =
            journal_encoded(b, run, rec, EventKind::SignalPayload, &event, true, actions)?;
        b.insert(
            Table::Inbox,
            k,
            values::encode(&SignalSlot::Journaled {
                payload_seq,
                sent_at_ms,
            }),
        );
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    /// Number of `on_first_events` calls on this thread (seam test in `run.rs`).
    pub(crate) static FIRST_EVENTS_CALLS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

// ------------------------------------------------------------------ wait intents

/// Validation of a `RegisterWait` intent, before any write of its `EventsAppended`
/// (`run::check_intents`): name length (`FieldTooLong`), a known wait kind and an op outside the
/// apply-reserved range (`BadIntent`; a reserved op would collide with reserved timer routing).
///
/// The "op not journaled yet" rule cannot be checked here (no run record at this seam); see
/// [`register_wait`].
pub(crate) fn check_register_wait(_cur: &Cursor, w: &RegisterWait) -> Result<(), RejectReason> {
    if w.check_limits().is_err() {
        return Err(invalid(InvalidCode::FieldTooLong));
    }
    if is_reserved_op(w.op) || CmdWaitKind::try_from(w.kind).is_err() {
        return Err(invalid(InvalidCode::BadIntent));
    }
    Ok(())
}

/// Applies a validated `RegisterWait` intent (`run.rs` write loop; the run is the executor's
/// own, normally `Running`).
///
/// - `op >= rec.next_seq` (the op is not journaled): no effect. The seam cannot reject from
///   the write loop, so this is a deterministic drop.
/// - **Idempotent:** a wait row for `(run, op)` already exists → no effect (no second timer,
///   `open_waits` unchanged). 05-11 re-arms unresolved ops after a resume, and a repeat must
///   be harmless.
/// - Signal wait with a buffered slot for its name (oldest first): journals
///   `SignalMatched{op, payload_seq}` at `next_seq` with ts `at_ms` (a `Pending` slot, which
///   only a seq-0 path without [`on_first_events`] could leave, is journaled first), deletes
///   the slot and writes no wait or timer row; the run gets `InputAppended`.
/// - Otherwise inserts the wait row (+ FIFO index for signals) and, when
///   `timeout_due_ms > 0`, the timer row with `Action::ScheduleTimer`, even when the due time
///   is already past (the timer service fires it on its next tick → `WaitTimedOut`). Tokens
///   are never buffered. `open_waits` grows by one.
///
/// **Contract with the submitter.** Apply cannot tell a never-registered op from one whose
/// wait was already satisfied (signal, token, timeout or `CancelWait`): in both cases there is
/// no row and `op < next_seq`. A `RegisterWait` for a satisfied op would insert a fresh wait.
/// The executor (05-11 re-arm) must therefore only send ops that have no completion in the
/// journal (`ReplayState::completion_for(op) == None`, guest `OpCancelled` included).
///
/// **Reads** (asserted exception, W4): the wait-row point read, only when `open_waits > 0`,
/// plus the inbox-by-name prefix for a signal wait without an existing row. So 0..=2 on top
/// of the `EventsAppended` run-record read. A row written earlier in the same batch is served
/// by the overlay without a read.
pub(crate) fn register_wait(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &mut RunRecord,
    w: &RegisterWait,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    if w.op >= rec.next_seq {
        return Ok(());
    }
    let kind = match CmdWaitKind::try_from(w.kind) {
        Ok(CmdWaitKind::Signal) => WaitKind::Signal,
        Ok(CmdWaitKind::Token) => WaitKind::Token,
        Err(_) => return Err(StoreError::Codec("validated wait kind".into())),
    };
    if read_wait(b, run, rec, w.op)?.is_some() {
        return Ok(());
    }
    if kind == WaitKind::Signal {
        let oldest = b
            .range_prefix(Table::Inbox, &signal_inbox_prefix(run, &w.name))?
            .into_iter()
            .next();
        if let Some((k, v)) = oldest {
            let payload_seq = match values::decode(&v)? {
                SignalSlot::Journaled { payload_seq, .. } => payload_seq,
                SignalSlot::Pending { event, .. } => {
                    let seq = journal_encoded(
                        b,
                        run,
                        rec,
                        EventKind::SignalPayload,
                        &event,
                        false,
                        actions,
                    )?;
                    input_appended(rec, run, seq, actions);
                    seq
                }
            };
            b.remove(Table::Inbox, k);
            journal_input(
                b,
                run,
                rec,
                w.at_ms,
                EventBody::SignalMatched(journal::SignalMatched {
                    op: w.op,
                    payload_seq,
                }),
                actions,
            )?;
            return Ok(());
        }
    }
    let row = WaitRow {
        kind,
        name: if kind == WaitKind::Signal {
            w.name.clone()
        } else {
            String::new()
        },
        timeout_due_ms: w.timeout_due_ms,
    };
    insert_wait(b, run, rec, w.op, &row, actions);
    Ok(())
}

/// Validation of a `CancelWait` intent (`run::check_intents`): an op in the apply-reserved
/// range is `BadIntent`. An absent wait is not an error.
pub(crate) fn check_cancel_wait(_cur: &Cursor, w: &CancelWait) -> Result<(), RejectReason> {
    if is_reserved_op(w.op) {
        return Err(invalid(InvalidCode::BadIntent));
    }
    Ok(())
}

/// Applies a validated `CancelWait` intent (the guest cancelled a signal/token op; its
/// `OpCancelled` event is the completion, D5): removes the wait row, FIFO index row and timeout
/// timer (`Action::CancelTimer`) and decrements `open_waits`. An absent wait (already
/// satisfied, never registered) is a no-op inside the accepted `EventsAppended`.
///
/// Reads: the wait-row point read, only when `open_waits > 0`.
pub(crate) fn cancel_wait(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &mut RunRecord,
    w: &CancelWait,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    if let Some(row) = read_wait(b, run, rec, w.op)? {
        remove_wait(b, run, rec, w.op, &row, None, actions);
    }
    Ok(())
}

/// A fired guest-op timer on a live run (`timer.rs`, after the timer row was removed and the
/// record read). When the wait row of `op` exists, the timer is a wait timeout: journals
/// `WaitTimedOut{op}` (kind 70) at `fired_at_ms`, removes the wait row and FIFO index (and the
/// row's timer if it is not the one that fired), decrements `open_waits`, wakes the run,
/// stores the record and returns `Some(Accepted)`. No wait row → `None` (the ordinary
/// `TimerFired` path, e.g. a sleep).
///
/// A wait already satisfied by a signal, token or `CancelWait` took its timer row with it, so
/// its late fire is `Duplicate` at the timer-row check and never reaches this function.
///
/// Reads: the wait-row point read, only when `open_waits > 0`, on top of timer.rs's two.
pub(crate) fn try_wait_timeout(
    b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    run: &RunId,
    rec: &mut RunRecord,
    t: &TimerFired,
    actions: &mut Vec<Action>,
) -> Option<ApplyResult> {
    let row = match read_wait(b, run, rec, t.op) {
        Ok(row) => row?,
        Err(e) => return Some(Err(e)),
    };
    remove_wait(b, run, rec, t.op, &row, Some(t.due_at_ms), actions);
    if let Err(e) = journal_input(
        b,
        run,
        rec,
        t.fired_at_ms,
        EventBody::WaitTimedOut(journal::WaitTimedOut { op: t.op }),
        actions,
    ) {
        return Some(Err(e));
    }
    store_run(b, run, rec);
    Some(Ok(ApplyOutcome::Accepted))
}

// ------------------------------------------------------------------ tokens

/// `Cmd::CompleteToken` (tag 12).
///
/// Validation, in order: run id, `event` present (`MissingField`), W5 inline size
/// (`PayloadTooLarge`), run exists (`RunNotFound`), run live (`RunTerminal`). Then by the wait
/// row of `op`:
/// - a token wait → journals the submitter-encoded `TokenCompleted` event verbatim (D21; apply
///   validates `op` from the command field only; that the encoded event carries the same op
///   is the submitter's contract), removes the wait and its timer, wakes the run → `Accepted`;
/// - a signal wait → `Invalid(BadIntent)`;
/// - no row and `op < next_seq` → `Duplicate` (already completed or timed out);
/// - no row and `op >= next_seq` → `Invalid(BadIntent)` (no such op).
///
/// Reads: the run record; + the wait-row point read when `open_waits > 0`.
pub(crate) fn complete_token(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    c: &CompleteToken,
    actions: &mut Vec<Action>,
) -> ApplyResult {
    let run = match local_run(cur, &c.run_id) {
        Ok(r) => r,
        Err(r) => return reject(r),
    };
    let Some(event) = &c.event else {
        return reject(invalid(InvalidCode::MissingField));
    };
    if event.is_oversize_inline() {
        return reject(RejectReason::PayloadTooLarge);
    }
    let Some(mut rec) = load_run(b, &run)? else {
        return reject(RejectReason::RunNotFound);
    };
    if rec.status.is_terminal() {
        return reject(RejectReason::RunTerminal);
    }
    match read_wait(b, &run, &rec, c.op)? {
        Some(row) if row.kind == WaitKind::Token => {
            remove_wait(b, &run, &mut rec, c.op, &row, None, actions);
            journal_encoded(
                b,
                &run,
                &mut rec,
                EventKind::TokenCompleted,
                event,
                true,
                actions,
            )?;
            store_run(b, &run, &rec);
            Ok(ApplyOutcome::Accepted)
        }
        Some(_) => reject(invalid(InvalidCode::BadIntent)),
        None if c.op < rec.next_seq => duplicate(),
        None => reject(invalid(InvalidCode::BadIntent)),
    }
}

// ------------------------------------------------------------------ cancel

/// Result of the shared cancel path.
enum Cancel {
    /// Flag set (and `CancelRequested` journaled when `next_seq > 0`), children cascaded.
    Applied,
    /// Terminal `ContinuedAsNew` run: `CancelRun` sent on to `continued_to` (PF-F2).
    Forwarded,
    /// Cancel already requested.
    Repeat,
    /// Terminal run with no successor to forward to.
    Terminal,
}

/// The shared graceful-cancel path of [`request_cancel`] and [`inbound_cancel`]. `at_ms` is
/// the request time (`None` for inbound messages, which carry none: `rec.created_at_ms`).
///
/// Reads: + the children prefix when `children > 0`.
fn cancel(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    run: &RunId,
    mut rec: RunRecord,
    reason: &str,
    at_ms: Option<u64>,
    actions: &mut Vec<Action>,
) -> Result<Cancel, StoreError> {
    if rec.status.is_terminal() {
        if rec.status == RunStatus::ContinuedAsNew
            && let Some(next) = rec.continued_to
        {
            let msg = cancel_msg(next.as_bytes(), reason);
            // A successor outside the partition count cannot be addressed: treat as terminal
            // rather than fail the partition in `emit`.
            if msg_target(cur, &msg).is_ok() {
                emit(b, cur, &msg, actions)?;
                return Ok(Cancel::Forwarded);
            }
        }
        return Ok(Cancel::Terminal);
    }
    if rec.cancel_requested {
        return Ok(Cancel::Repeat);
    }
    rec.cancel_requested = true;
    rec.cancel_reason = reason.to_string();
    let at = at_ms.unwrap_or(rec.created_at_ms);
    if rec.next_seq == 0 {
        // Flag only: an event at seq 0 would precede RunStarted. The executor's RunStarted
        // synthesis reads `cancel_requested`; 05-11 journals CancelRequested right after
        // RunStarted. A Scheduled (delayed) run is woken so it observes the request soon.
        make_ready(b, run, &mut rec, at, actions);
    } else {
        journal_input(
            b,
            run,
            &mut rec,
            at,
            EventBody::CancelRequested(journal::CancelRequested {
                reason: reason.to_string(),
            }),
            actions,
        )?;
    }
    store_run(b, run, &rec);
    actions.push(Action::CancelRequested { run: *run });

    // Cascade a graceful cancel to every live child, wherever it lives (one prefix scan).
    if rec.children > 0 {
        for (k, _) in b.range_prefix(Table::Children, &children_prefix(run))? {
            let child = k
                .get(RUN_LEN..)
                .ok_or_else(|| StoreError::Codec("short children key".into()))?;
            emit(b, cur, &cancel_msg(child, reason), actions)?;
        }
    }
    Ok(Cancel::Applied)
}

fn cancel_msg(target: &[u8], reason: &str) -> OutboxMsg {
    OutboxMsg {
        kind: Some(outbox_msg::Kind::CancelRun(CancelRun {
            target_run: target.to_vec(),
            reason: reason.to_string(),
        })),
    }
}

/// `Cmd::RequestCancel` (tag 9).
///
/// Validation: run id, reason length (`FieldTooLong`), run exists (`RunNotFound`). Then:
/// - live run, first request → sets `cancel_requested` and `cancel_reason`, journals
///   `CancelRequested{reason}` at `requested_at_ms` (flag only while `next_seq == 0`), wakes
///   the run, emits `Action::CancelRequested` and cascades `CancelRun` to the children →
///   `Accepted`;
/// - live run, repeat → `Duplicate`;
/// - terminal `ContinuedAsNew` run with `continued_to` → `CancelRun` forwarded to the
///   successor through the outbox → `Accepted` (PF-F2; the chain is followed hop by hop);
/// - any other terminal run → `RunTerminal`.
///
/// Reads: the run record (1); + the children prefix when `children > 0`.
pub(crate) fn request_cancel(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    r: &RequestCancel,
    actions: &mut Vec<Action>,
) -> ApplyResult {
    let run = match local_run(cur, &r.run_id) {
        Ok(id) => id,
        Err(e) => return reject(e),
    };
    if r.check_limits().is_err() {
        return reject(invalid(InvalidCode::FieldTooLong));
    }
    let Some(rec) = load_run(b, &run)? else {
        return reject(RejectReason::RunNotFound);
    };
    match cancel(
        b,
        cur,
        &run,
        rec,
        &r.reason,
        Some(r.requested_at_ms),
        actions,
    )? {
        Cancel::Applied | Cancel::Forwarded => Ok(ApplyOutcome::Accepted),
        Cancel::Repeat => duplicate(),
        Cancel::Terminal => reject(RejectReason::RunTerminal),
    }
}

/// Inbound outbox `CancelRun` (cascade from a parent, or a forward along a continue-as-new
/// chain): the [`request_cancel`] path with the request time `rec.created_at_ms`. Every
/// outcome other than a write is a deterministic per-message drop (unknown or foreign run,
/// repeat, terminal run without successor).
pub(crate) fn inbound_cancel(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    c: &CancelRun,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    let Ok(run) = local_run(cur, &c.target_run) else {
        return Ok(());
    };
    let Some(rec) = load_run(b, &run)? else {
        return Ok(());
    };
    cancel(b, cur, &run, rec, &c.reason, None, actions)?;
    Ok(())
}
