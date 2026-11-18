//! `TimerFired` apply. Stub until plan 02-10 (STOR-13).

use ostro_types::command::TimerFired;

use super::cursor::Cursor;
use super::outcome::{Action, ApplyOutcome, InvalidCode, RejectReason};
use crate::store::{ApplyBatch, StoreError};

pub(crate) fn apply_timer_fired(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _t: &TimerFired,
    _actions: &mut Vec<Action>,
) -> Result<ApplyOutcome, StoreError> {
    Ok(ApplyOutcome::Rejected(RejectReason::Invalid(
        InvalidCode::NotImplemented,
    )))
}
