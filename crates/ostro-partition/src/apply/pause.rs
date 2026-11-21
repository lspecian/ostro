//! Run pause / resume: `Cmd::PauseRun` (tag 20) and `Cmd::ResumeRun` (tag 21) (Phase 7,
//! OPS-01; RESERVATIONS.md P7 block, D-W18).
//!
//! Plan 07-03 lands the format contract only: both commands are routed here and rejected with
//! `Invalid(NotImplemented)`, a deterministic no-op that consumes its offset and folds outcome
//! code 26 into the rolling hash. Plan 07-04 implements the semantics (`RunRecord.pause`,
//! `RejectReason::RunPaused`, `Action::RunPaused` / `Action::RunResumed`); see
//! `docs/storage-format.md` "Phase 7 additions".

use ostro_types::command::{PauseRun, ResumeRun};

use super::cursor::Cursor;
use super::outcome::{Action, ApplyOutcome, ApplyResult, InvalidCode, RejectReason};
use crate::store::ApplyBatch;

/// `Cmd::PauseRun`. Phase 7: implemented in 07-04.
pub(crate) fn apply_pause_run(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _p: &PauseRun,
    _actions: &mut Vec<Action>,
) -> ApplyResult {
    // Phase 7: implemented in 07-04
    Ok(ApplyOutcome::Rejected(RejectReason::Invalid(
        InvalidCode::NotImplemented,
    )))
}

/// `Cmd::ResumeRun`. Phase 7: implemented in 07-04.
pub(crate) fn apply_resume_run(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _r: &ResumeRun,
    _actions: &mut Vec<Action>,
) -> ApplyResult {
    // Phase 7: implemented in 07-04
    Ok(ApplyOutcome::Rejected(RejectReason::Invalid(
        InvalidCode::NotImplemented,
    )))
}
