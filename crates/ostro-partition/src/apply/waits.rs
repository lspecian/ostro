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
use ostro_types::command::TimerFired;

use super::cursor::Cursor;
use super::outcome::{Action, ApplyResult};
use crate::store::values::RunRecord;
use crate::store::{ApplyBatch, StoreError};

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
