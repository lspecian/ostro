//! Run pause / resume: `Cmd::PauseRun` (tag 20) and `Cmd::ResumeRun` (tag 21) (Phase 7,
//! OPS-01; RESERVATIONS.md P7 block, D-W18). Format contract: 07-03; semantics: 07-04. See
//! `docs/storage-format.md` "Phase 7 additions".
//!
//! - **Pause** sets `RunRecord.pause` on a live run and blind-removes its ready row (via
//!   `rec.ready`). A `Running` run stays `Running` in the store (its `running` row stays too:
//!   the dispatcher revokes the activation on `Action::RunPaused`, 07-05). Emits
//!   `Action::RunPaused{run}`.
//! - **While paused**, `run::enqueue_ready` (the single place a ready row is created) moves
//!   the run to `Ready` without a ready row and without `EnqueueReady`; `EventsAppended` and
//!   `AttemptGranted` are rejected `RunPaused` (after their fencing checks); inputs are still
//!   journaled. Every terminal transition clears the mark (`terminal::on_terminal`), so a run
//!   that times out while paused is resumable with `RetryRun` (D1/D2).
//! - **Resume** clears the mark. A `Ready` or `Running` run goes through `enqueue_ready`
//!   (leave_running + ready row + `EnqueueReady`) at `requested_at_ms`; a `Scheduled` run is
//!   left alone (an input or timer wakes it later). Emits `Action::RunResumed{run}`.
//! - Both are idempotent (`Accepted`, no change, no action) and rejected `RunTerminal` on
//!   terminal runs. An auto-pause (`token` set) is fenced exactly like `EventsAppended`:
//!   `StaleEpoch` for a foreign epoch, `Fenced` unless the token is the current attempt.
//!
//! **Reads:** 1 each (the run record). No clocks: times come from the command.

use ostro_types::command::{
    PAUSE_REASON_OPERATOR, PAUSE_REASON_RETRIES_EXHAUSTED, PauseRun, ResumeRun,
};
use ostro_types::{AttemptToken, Epoch};

use super::cursor::Cursor;
use super::outcome::{Action, ApplyOutcome, ApplyResult, InvalidCode, RejectReason};
use super::run::{enqueue_ready, load_run, local_run, remove_ready, store_run};
use crate::store::ApplyBatch;
use crate::store::values::{PauseMark, RunStatus};

fn reject(r: RejectReason) -> ApplyResult {
    Ok(ApplyOutcome::Rejected(r))
}

fn invalid(c: InvalidCode) -> RejectReason {
    RejectReason::Invalid(c)
}

/// `Cmd::PauseRun`. Validation (run id, detail length, reason code) before the single read;
/// then `RunNotFound`, `RunTerminal`, auto-pause fencing, idempotence.
pub(crate) fn apply_pause_run(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    p: &PauseRun,
    actions: &mut Vec<Action>,
) -> ApplyResult {
    let run = match local_run(cur, &p.run_id) {
        Ok(r) => r,
        Err(r) => return reject(r),
    };
    if p.check_limits().is_err() {
        return reject(invalid(InvalidCode::FieldTooLong));
    }
    let reason = match p.reason {
        PAUSE_REASON_OPERATOR => 0u8,
        PAUSE_REASON_RETRIES_EXHAUSTED => 1u8,
        // An unknown enum value, like an unknown ConflictPolicy.
        _ => return reject(invalid(InvalidCode::Decode)),
    };
    let Some(mut rec) = load_run(b, &run)? else {
        return reject(RejectReason::RunNotFound);
    };
    if rec.status.is_terminal() {
        return reject(RejectReason::RunTerminal);
    }
    if let Some(t) = p.token() {
        // Same checks, same order as EventsAppended: only the current attempt may auto-pause.
        if t.epoch != cur.epoch() {
            return reject(RejectReason::StaleEpoch {
                current: cur.epoch(),
            });
        }
        let current = rec
            .current_attempt
            .map(|(e, n)| AttemptToken { epoch: Epoch(e), n });
        if Some(t) != current {
            return reject(RejectReason::Fenced { current });
        }
    }
    if rec.pause.is_some() {
        return Ok(ApplyOutcome::Accepted);
    }
    remove_ready(b, &run, &mut rec);
    rec.pause = Some(PauseMark {
        reason,
        at_ms: p.requested_at_ms,
        detail: p.detail.clone(),
    });
    store_run(b, &run, &rec);
    actions.push(Action::RunPaused { run });
    Ok(ApplyOutcome::Accepted)
}

/// `Cmd::ResumeRun`. One read; `RunNotFound`, `RunTerminal`, idempotent on a run that is not
/// paused.
pub(crate) fn apply_resume_run(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    r: &ResumeRun,
    actions: &mut Vec<Action>,
) -> ApplyResult {
    let run = match local_run(cur, &r.run_id) {
        Ok(r) => r,
        Err(r) => return reject(r),
    };
    let Some(mut rec) = load_run(b, &run)? else {
        return reject(RejectReason::RunNotFound);
    };
    if rec.status.is_terminal() {
        return reject(RejectReason::RunTerminal);
    }
    if rec.pause.take().is_none() {
        return Ok(ApplyOutcome::Accepted);
    }
    if matches!(rec.status, RunStatus::Ready | RunStatus::Running) {
        // The mark is cleared first, so the gate in enqueue_ready lets the row through.
        enqueue_ready(b, &run, &mut rec, r.requested_at_ms, actions);
    }
    store_run(b, &run, &rec);
    actions.push(Action::RunResumed { run });
    Ok(ApplyOutcome::Accepted)
}
