//! Phase 13: implemented in 13-17.
//!
//! `Cmd::RepinRun` (tag 43; CLUS-07, D-P13-21; RESERVATIONS §1 P13 block). Planned rules
//! (13-17): run missing / terminal → the existing `RunNotFound` / `RunTerminal`;
//! `module_sha != from_sha` → `ModuleMismatch` (23); `next_seq != expected_next_seq` →
//! `SeqMismatch`; a leased run → `RunBusy` (24); otherwise `RunRecord.module_sha = to_sha`.
//! Until then every `RepinRun` is rejected with `Invalid(NotImplemented)` (no reads, no
//! writes), which is deterministic and folded into the state hash like any rejection.

use ostro_types::command::RepinRun;

use super::cursor::Cursor;
use super::outcome::{Action, ApplyOutcome, ApplyResult, InvalidCode, RejectReason};
use crate::store::ApplyBatch;

/// `Cmd::RepinRun`: stub until 13-17.
pub(crate) fn apply_repin_run(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _cmd: &RepinRun,
    _actions: &mut Vec<Action>,
) -> ApplyResult {
    Ok(ApplyOutcome::Rejected(RejectReason::Invalid(
        InvalidCode::NotImplemented,
    )))
}
