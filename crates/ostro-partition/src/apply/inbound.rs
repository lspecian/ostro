//! `InboundBatch` apply. Stub until plan 02-10 (STOR-12).

use ostro_types::command::InboundBatch;

use super::cursor::Cursor;
use super::outcome::{Action, ApplyOutcome, InvalidCode, RejectReason};
use crate::store::{ApplyBatch, StoreError};

pub(crate) fn apply_inbound_batch(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _ib: &InboundBatch,
    _actions: &mut Vec<Action>,
) -> Result<ApplyOutcome, StoreError> {
    Ok(ApplyOutcome::Rejected(RejectReason::Invalid(
        InvalidCode::NotImplemented,
    )))
}
