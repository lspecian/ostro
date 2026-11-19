//! Run retention: `EXPIRE_OP` deletes a terminal run's rows (owned by plan **05-09**, OPS-06).
//!
//! Plan 05-02 created this file with a stub so that 05-09 can fill it without touching
//! `timer.rs`. 05-09 deletes, in one batch: `runs`, `events`, `payloads`, `inbox`, `waits`,
//! `children`, `effects`, `signal_seen` (prefixes), `running`, the dedupe row rebuilt from
//! `(workflow, idempotency_key)` and any DLQ row. WAL bytes stay until Phase 8.

use ostro_types::RunId;
use ostro_types::command::TimerFired;

use super::cursor::Cursor;
use super::outcome::{Action, ApplyOutcome, ApplyResult};
use crate::store::ApplyBatch;
use crate::store::values::RunRecord;

/// `TimerFired` for `EXPIRE_OP`. `timer.rs` already removed the timer row and read the record
/// (`None` = orphan row); this runs before any status check, because expiry acts on terminal
/// runs.
///
/// Stub (05-02): the row removal is the whole effect → `Accepted`.
pub(crate) fn on_expire(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _run: &RunId,
    _rec: Option<RunRecord>,
    _t: &TimerFired,
    _actions: &mut Vec<Action>,
) -> ApplyResult {
    Ok(ApplyOutcome::Accepted)
}
