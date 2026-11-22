//! Rerun prefix: journals `StartRun.rerun_prefix` for a rerun (`rerun_from_seq != 0`)
//! (Phase 11, OPS-02; RESERVATIONS.md P11 StartRun 40/41). Contract: 11-02 (stub); implemented
//! and wired into `run::start_run` by 11-07.
//!
//! The rerun path is gated on `StartRun.rerun_from_seq != 0`; a StartRun without it (every
//! command written before Phase 11) never reaches this module.
// Called from `run::start_run` once 11-07 lands.
#![allow(dead_code)]

use ostro_types::RunId;
use ostro_types::command::StartRun;

use super::outcome::{ApplyOutcome, InvalidCode, RejectReason};
use crate::store::ApplyBatch;

/// Journals the rerun prefix of `s` for `run`. Stub (11-02): `Err(Invalid(NotImplemented))`;
/// not called yet. Implemented by 11-07.
pub(crate) fn journal_prefix(
    _b: &mut ApplyBatch<'_>,
    _run: &RunId,
    s: &StartRun,
) -> Result<(), ApplyOutcome> {
    debug_assert!(
        s.rerun_from_seq != 0,
        "the rerun path is gated on rerun_from_seq != 0"
    );
    Err(ApplyOutcome::Rejected(RejectReason::Invalid(
        InvalidCode::NotImplemented,
    )))
}
