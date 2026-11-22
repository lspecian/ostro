//! Dead-letter marks: `Cmd::DlqUpdate` (tag 45) (Phase 11, OPS-04; RESERVATIONS.md P11 block).
//! Format contract: 11-02; semantics: 11-08.
//!
//! Until 11-08 every `DlqUpdate` is rejected `Invalid(NotImplemented)`: a deterministic no-op
//! that consumes its offset and writes nothing.

use ostro_types::command::DlqUpdate;

use super::cursor::Cursor;
use super::outcome::{Action, ApplyOutcome, ApplyResult, InvalidCode, RejectReason};
use crate::store::ApplyBatch;

/// `Cmd::DlqUpdate`. Stub (11-02): rejects `Invalid(NotImplemented)`; implemented by 11-08.
pub(crate) fn apply_dlq_update(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _cmd: &DlqUpdate,
    _actions: &mut Vec<Action>,
) -> ApplyResult {
    Ok(ApplyOutcome::Rejected(RejectReason::Invalid(
        InvalidCode::NotImplemented,
    )))
}
