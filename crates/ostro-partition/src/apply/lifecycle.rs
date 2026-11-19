//! Run lifecycle: terminate, retry, run/schedule-to-start timeouts, continue-as-new (owned by
//! plan **05-09**).
//!
//! Plan 05-02 created this file with stub entry points so that 05-09 can fill it without
//! touching `mod.rs`, `run.rs` or `timer.rs`. Every call site already exists. 05-09 keeps the
//! names and may extend parameters within this file plus the single call site.
//!
//! Semantics 05-09 implements (D1, D2, D3, D9, D10, PF-F2):
//! - `RUN_TIMEOUT_OP` armed at start (`created_at_ms + run_timeout_ms`) and
//!   `SCHEDULE_TO_START_OP` (`s2s_due_ms`) deleted by the first grant; firing on a live run is
//!   a hard stop `TimedOut` with reason `run_timeout` / `schedule_to_start`;
//! - every terminal transition arms `EXPIRE_OP` at `completed_at_ms + retention_ms`;
//! - hard stops journal `RunTerminated{reason}` (kind 71) only when `next_seq > 0`, call
//!   `terminal::on_terminal` and emit `Action::RunCompleted`.

use ostro_types::RunId;
use ostro_types::command::{
    AttemptGranted, EventsAppended, RetryRun, StartRun, TerminateRun, TimerFired,
};

use super::cursor::Cursor;
use super::outcome::{Action, ApplyOutcome, ApplyResult, InvalidCode, RejectReason};
use crate::store::values::RunRecord;
use crate::store::{ApplyBatch, StoreError};

fn not_implemented() -> ApplyResult {
    Ok(ApplyOutcome::Rejected(RejectReason::Invalid(
        InvalidCode::BadIntent,
    )))
}

/// `Cmd::TerminateRun` (tag 10, D3). 05-09: hard stop `Terminated` with the reason, removes
/// the ready/running/waits rows, journals `RunTerminated` when `next_seq > 0`, notifies the
/// parent, cascades a graceful `CancelRun` to the children, emits `RunCompleted`.
///
/// Stub (05-02): `Rejected(Invalid(BadIntent))`, as in 05-01.
pub(crate) fn terminate_run(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _t: &TerminateRun,
    _actions: &mut Vec<Action>,
) -> ApplyResult {
    not_implemented()
}

/// `Cmd::RetryRun` (tag 11, D1). 05-09: resume-in-place for `resumable` terminal runs
/// (status Ready, `resumed_count += 1`, `attempt_failures = 0`, expiry timer removed);
/// `RunCompleted` (code 27) for Completed runs; restart-as-new is the server's job.
///
/// Stub (05-02): `Rejected(Invalid(BadIntent))`, as in 05-01.
pub(crate) fn retry_run(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _r: &RetryRun,
    _actions: &mut Vec<Action>,
) -> ApplyResult {
    not_implemented()
}

/// Inbound outbox `StartRun` (continue-as-new successor, tag 6). 05-09 starts the successor
/// run (`start_run` with `continued_from`).
///
/// Stub (05-02): consumed with no effect (a deterministic per-message drop; the batch stays
/// accepted), as in 05-01. Tripwire: `apply_contract_p5.rs`
/// `inbound_start_run_is_consumed_without_effect_until_implemented`.
pub(crate) fn inbound_start_run(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _s: &StartRun,
    _actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    Ok(())
}

/// After `StartRun` built the record (before it is stored). 05-09 arms `RUN_TIMEOUT_OP` and
/// `SCHEDULE_TO_START_OP` (setting `s2s_due_ms`).
///
/// Stub (05-02): no effect.
pub(crate) fn on_start(
    _b: &mut ApplyBatch<'_>,
    _run: &RunId,
    _rec: &mut RunRecord,
    _s: &StartRun,
    _actions: &mut Vec<Action>,
) {
}

/// After `AttemptGranted` accepted a grant (before the record is stored). 05-09 deletes the
/// schedule-to-start timer on the first grant (`s2s_due_ms`). The `after_failure` counter
/// (`attempt_failures`, F9) belongs to 05-22.
///
/// Stub (05-02): no effect.
pub(crate) fn on_attempt_granted(
    _b: &mut ApplyBatch<'_>,
    _run: &RunId,
    _rec: &mut RunRecord,
    _g: &AttemptGranted,
    _actions: &mut Vec<Action>,
) {
}

/// Called by `terminal::on_terminal` after the waits cleanup. 05-09 arms the expiry timer
/// and removes the run-timeout / schedule-to-start rows.
///
/// Stub (05-02): no effect.
pub(crate) fn on_terminal(
    _b: &mut ApplyBatch<'_>,
    _run: &RunId,
    _rec: &mut RunRecord,
    _at_ms: u64,
    _actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    Ok(())
}

/// `TimerFired` for `RUN_TIMEOUT_OP` or `SCHEDULE_TO_START_OP`. `timer.rs` already removed the
/// timer row and read the record (`None` = orphan row).
///
/// Stub (05-02): the row removal is the whole effect → `Accepted`.
pub(crate) fn on_reserved_timer(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _run: &RunId,
    _rec: Option<RunRecord>,
    _t: &TimerFired,
    _actions: &mut Vec<Action>,
) -> ApplyResult {
    Ok(ApplyOutcome::Accepted)
}

/// An `EventsAppended` whose last intent is `CompleteRun{ContinuedAsNew}`. `run.rs` already
/// checked the token, `from_seq` and event contiguity; 05-09 owns the rest of the command
/// (intent checks via `run::check_intents`, the successor `SendMessage(StartRun)`, no
/// `ChildCompleted` to the parent, `continued_to`, `terminal::on_terminal`).
///
/// Stub (05-02): rejected `Invalid(BadIntent)` as in 05-01, with no writes.
pub(crate) fn complete_continued_as_new(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _e: &EventsAppended,
    _run: &RunId,
    _rec: RunRecord,
    _actions: &mut Vec<Action>,
) -> ApplyResult {
    not_implemented()
}
