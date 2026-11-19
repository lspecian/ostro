//! Signals, await tokens, wait timeouts and graceful cancel (owned by plan **05-08**).
//!
//! Plan 05-02 created this file with stub entry points so that 05-08 can fill it without
//! touching `mod.rs`, `run.rs` or `timer.rs`. Every call site already exists. 05-08 keeps the
//! names and may extend parameters within this file plus the single call site.
//!
//! Semantics 05-08 implements (05-RESEARCH "Signals and waits", D4, D5, D13, D21, B1):
//! - waits live in the `waits` keyspace (`wait_key` row + `wait_name_key` FIFO index) and
//!   `RunRecord.open_waits` counts them;
//! - a signal matches the oldest wait of its name, else it is buffered as a `SignalSlot` in
//!   the inbox (`Pending` while `next_seq == 0`, else `Journaled`);
//! - submitter-encoded event bytes are journaled verbatim with `inputs::journal_encoded`;
//!   apply-built events (`SignalMatched`, `WaitTimedOut`, `CancelRequested`) with
//!   `inputs::journal_input`.

use ostro_types::RunId;
use ostro_types::command::{
    CancelWait, CompleteToken, MAX_SIGNAL_NAME_LEN, RegisterWait, RequestCancel, Signal, SignalRun,
    TimerFired,
};
use ostro_types::journal::EventKind;

use super::cursor::Cursor;
use super::inbound::{live_run, make_ready};
use super::inputs::journal_encoded;
use super::outcome::{Action, ApplyOutcome, ApplyResult, InvalidCode, RejectReason};
use super::run::store_run;
use crate::store::keys::signal_inbox_key;
use crate::store::values::{self, RunRecord, SignalSlot};
use crate::store::{ApplyBatch, StoreError, Table};

fn not_implemented() -> ApplyResult {
    Ok(ApplyOutcome::Rejected(RejectReason::Invalid(
        InvalidCode::BadIntent,
    )))
}

/// `Cmd::SignalRun` (tag 8). 05-08: reads the run record, the waits-by-name prefix and (with a
/// `request_id`) one `signal_seen` point read; journals `SignalPayload` verbatim, then either
/// `SignalMatched` for the oldest wait or an inbox `SignalSlot` (`Pending` while
/// `next_seq == 0`, B1).
///
/// Stub (05-02): `Rejected(Invalid(BadIntent))`, as in 05-01.
pub(crate) fn signal_run(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _s: &SignalRun,
    _actions: &mut Vec<Action>,
) -> ApplyResult {
    not_implemented()
}

/// `Cmd::RequestCancel` (tag 9). 05-08: sets `cancel_requested` and `cancel_reason`, journals
/// `CancelRequested{reason}` once (not at seq 0), wakes the run and cascades `CancelRun` to the
/// children; forwards along `continued_to` (PF-F2).
///
/// Stub (05-02): `Rejected(Invalid(BadIntent))`, as in 05-01.
pub(crate) fn request_cancel(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _r: &RequestCancel,
    _actions: &mut Vec<Action>,
) -> ApplyResult {
    not_implemented()
}

/// `Cmd::CompleteToken` (tag 12). 05-08: a live token wait journals the submitter-encoded
/// `TokenCompleted` event verbatim (D21) and wakes the run; a completed op is `Duplicate`.
///
/// Stub (05-02): `Rejected(Invalid(BadIntent))`, as in 05-01.
pub(crate) fn complete_token(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _c: &CompleteToken,
    _actions: &mut Vec<Action>,
) -> ApplyResult {
    not_implemented()
}

/// Validation of a `RegisterWait` intent, before any write of its `EventsAppended`
/// (`run::check_intents`). 05-08: name length, wait kind.
///
/// Stub (05-02): `Invalid(BadIntent)`, as in 05-01.
pub(crate) fn check_register_wait(_cur: &Cursor, _w: &RegisterWait) -> Result<(), RejectReason> {
    Err(RejectReason::Invalid(InvalidCode::BadIntent))
}

/// Applies a validated `RegisterWait` intent (`run.rs` write loop). 05-08: inserts both wait
/// rows and the timeout timer (idempotent), bumps `open_waits`, and matches a buffered signal
/// of the same name (one inbox-by-name prefix read).
///
/// Stub (05-02): unreachable while [`check_register_wait`] rejects; no effect.
pub(crate) fn register_wait(
    _b: &mut ApplyBatch<'_>,
    _run: &RunId,
    _rec: &mut RunRecord,
    _w: &RegisterWait,
    _actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    Ok(())
}

/// Validation of a `CancelWait` intent (`run::check_intents`).
///
/// Stub (05-02): `Invalid(BadIntent)`, as in 05-01.
pub(crate) fn check_cancel_wait(_cur: &Cursor, _w: &CancelWait) -> Result<(), RejectReason> {
    Err(RejectReason::Invalid(InvalidCode::BadIntent))
}

/// Applies a validated `CancelWait` intent. 05-08: removes the wait rows and timer (absent =
/// no-op) and decrements `open_waits`.
///
/// Stub (05-02): unreachable while [`check_cancel_wait`] rejects; no effect.
pub(crate) fn cancel_wait(
    _b: &mut ApplyBatch<'_>,
    _run: &RunId,
    _rec: &mut RunRecord,
    _w: &CancelWait,
    _actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    Ok(())
}

/// Inbound outbox `Signal` (one message of an `InboundBatch`; state misses are deterministic
/// drops). `s.payload` is the sender-encoded `SignalPayload` event (D21); oversize inline
/// payloads already rejected the whole batch (`inbound::check_msg`).
///
/// 05-02 implements the **no-wait branch** (no wait can exist yet: `RegisterWait` is rejected
/// until 05-08): the signal is buffered as a `SignalSlot` at `signal_inbox_key(run, name,
/// inbox_next)`:
/// - `next_seq == 0` (B1): `Pending{event, sent_at_ms: 0}`, nothing journaled; a `Scheduled`
///   run is woken as before (Phase 2 wake-up);
/// - otherwise the event is journaled verbatim with `journal_encoded` (waking the run) and the
///   slot is `Journaled{payload_seq, sent_at_ms: 0}`.
///
/// The outbox `Signal` carries no send time, so `sent_at_ms` is 0. A message without an event
/// or with a name over `MAX_SIGNAL_NAME_LEN` is dropped. One read (the run record). 05-08 adds
/// the waits-by-name match in front of this branch.
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
    let Some((run, mut rec)) = live_run(b, cur, &s.target_run)? else {
        return Ok(());
    };
    let n = rec.inbox_next;
    let slot = if rec.next_seq == 0 {
        let at = rec.created_at_ms;
        make_ready(b, &run, &mut rec, at, actions);
        SignalSlot::Pending {
            event: event.clone(),
            sent_at_ms: 0,
        }
    } else {
        let payload_seq = journal_encoded(
            b,
            &run,
            &mut rec,
            EventKind::SignalPayload,
            event,
            true,
            actions,
        )?;
        SignalSlot::Journaled {
            payload_seq,
            sent_at_ms: 0,
        }
    };
    b.insert(
        Table::Inbox,
        signal_inbox_key(&run, &s.name, n),
        values::encode(&slot),
    );
    rec.inbox_next = n.saturating_add(1);
    store_run(b, &run, &rec);
    Ok(())
}

/// A fired guest-op timer on a live run (`timer.rs`, after the timer row was removed and the
/// record read). `Some(outcome)` when `op` is a wait timeout: 05-08 journals
/// `WaitTimedOut{op}` (kind 70), removes the wait rows, decrements `open_waits`, wakes the
/// run and stores the record. `None` = not a wait op → the ordinary `TimerFired` path.
///
/// Stub (05-02): always `None`.
pub(crate) fn try_wait_timeout(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _run: &RunId,
    _rec: &mut RunRecord,
    _t: &TimerFired,
    _actions: &mut Vec<Action>,
) -> Option<ApplyResult> {
    None
}

/// B1: called by `run.rs` exactly once per run, after the batch that wrote seq 0
/// (`EventsAppended` with `from_seq == 0` and at least one event) has written its events and
/// before its derived intents are applied, so a `RegisterWait` in the same batch sees the
/// converted slots. 05-08 journals every `SignalSlot::Pending` event of the run (one inbox
/// prefix scan, only when `inbox_next > 0`), converts the slots to `Journaled` and emits
/// `InputAppended`. 05-16 adds the same call after `StartRun.run_started` writes seq 0.
///
/// Stub (05-02): no effect (a test-only counter records the call).
pub(crate) fn on_first_events(
    _b: &mut ApplyBatch<'_>,
    _run: &RunId,
    _rec: &mut RunRecord,
    _actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    #[cfg(test)]
    FIRST_EVENTS_CALLS.with(|c| c.set(c.get() + 1));
    Ok(())
}

#[cfg(test)]
thread_local! {
    /// Number of `on_first_events` calls on this thread (seam test in `run.rs`).
    pub(crate) static FIRST_EVENTS_CALLS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}
