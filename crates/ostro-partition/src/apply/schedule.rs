//! `Cmd::ScheduleFire` (tag 27): cron fires with a durable per-schedule watermark in
//! keyspace 16 `schedule_state` (P8-D10).
//!
//! Phase 8: implemented in 08-07. Until then apply rejects every `ScheduleFire` with
//! `Rejected(Invalid(NotImplemented))`, a deterministic no-op folded into the state hash.

use ostro_types::command::ScheduleFire;

use super::cursor::Cursor;
use super::outcome::{Action, ApplyOutcome, ApplyResult, InvalidCode, RejectReason};
use crate::store::ApplyBatch;

/// Applies a `ScheduleFire`. Phase 8: implemented in 08-07.
pub(crate) fn apply_schedule_fire(
    _b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    _s: &ScheduleFire,
    _actions: &mut Vec<Action>,
) -> ApplyResult {
    Ok(ApplyOutcome::Rejected(RejectReason::Invalid(
        InvalidCode::NotImplemented,
    )))
}
