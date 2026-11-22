//! Rerun prefix: journals `StartRun.rerun_prefix` for a rerun (`rerun_from_seq != 0`)
//! (Phase 11, OPS-02, D-P11-3/6; RESERVATIONS.md P11 StartRun 40/41, InvalidCode 29).
//! Contract: 11-02; implemented and wired into `run::start_run` by 11-07.
//!
//! The rerun path is gated on `StartRun.rerun_from_seq != 0` (W14): a non-zero field 41
//! with an empty field 40 is the valid `cut = 1` case (only the verbatim `RunStarted`). A
//! StartRun without fields 40/41 (every command written before Phase 11) never reaches
//! [`journal_prefix`]; [`check`] accepts it without looking further.
//!
//! [`check`] runs in `start_run`'s validation block, before the single point read and
//! before any write, so a rejected rerun leaves nothing in the overlay. It looks at kinds,
//! seqs, presence and inline lengths only; payloads are never decoded (blob refs are stored
//! as refs, never fetched). Every failure is `Invalid(BadRerunPrefix)`:
//!
//! - field 40 non-empty with field 41 == 0;
//! - `run_started` (23) missing or not a valid seq-0 `RunStarted` (`run::check_run_started`);
//! - prefix seqs not exactly `1, 2, .., len` or `rerun_from_seq != len + 1`;
//! - a prefix kind outside 16–127, or a terminal kind (26, 27, 28, 71);
//! - a prefix event without a payload or with an inline payload over the inline limit;
//! - not exactly one `__ostro/rerun_from=<n>` tag, or `<n>` (canonical decimal) != field 41.
//!
//! [`journal_prefix`] then writes events `0..cut` verbatim (`put_event`), sets
//! `next_seq = cut` and `status_seq` to the last `StatusPublished` (25) of the prefix.

use ostro_types::RunId;
use ostro_types::command::{JournalEvent, RERUN_FROM_TAG, StartRun};
use ostro_types::journal::EventKind;

use super::outcome::{InvalidCode, RejectReason};
use crate::store::ApplyBatch;
use crate::store::values::RunRecord;

const TERMINAL_KINDS: [EventKind; 4] = [
    EventKind::RunCompleted,
    EventKind::RunFailed,
    EventKind::RunContinuedAsNew,
    EventKind::RunTerminated,
];

fn bad() -> RejectReason {
    RejectReason::Invalid(InvalidCode::BadRerunPrefix)
}

/// Whether a copied prefix event is acceptable (kind and payload presence/size only).
fn prefix_event_ok(i: usize, ev: &JournalEvent) -> bool {
    let kind_ok =
        (16..=127).contains(&ev.kind) && !TERMINAL_KINDS.iter().any(|k| k.code() == ev.kind);
    let seq_ok = u64::try_from(i).is_ok_and(|i| ev.seq == i + 1);
    let payload_ok = ev.payload.as_ref().is_some_and(|p| !p.is_oversize_inline());
    kind_ok && seq_ok && payload_ok
}

/// Validates fields 40/41 (and the rerun tag) of `s`. `Ok` for a non-rerun StartRun. No
/// reads, no writes.
pub(crate) fn check(s: &StartRun) -> Result<(), RejectReason> {
    if s.rerun_from_seq == 0 {
        return if s.rerun_prefix.is_empty() {
            Ok(())
        } else {
            Err(bad())
        };
    }
    match &s.run_started {
        Some(ev) if super::run::check_run_started(ev).is_ok() => {}
        _ => return Err(bad()),
    }
    if u64::try_from(s.rerun_prefix.len())
        .ok()
        .and_then(|n| n.checked_add(1))
        != Some(s.rerun_from_seq)
    {
        return Err(bad());
    }
    if !s
        .rerun_prefix
        .iter()
        .enumerate()
        .all(|(i, ev)| prefix_event_ok(i, ev))
    {
        return Err(bad());
    }
    let mut tags = s.tags.iter().filter_map(|t| t.strip_prefix(RERUN_FROM_TAG));
    match (tags.next(), tags.next()) {
        (Some(v), None) if v == s.rerun_from_seq.to_string() => Ok(()),
        _ => Err(bad()),
    }
}

/// Journals events `0..cut` of a rerun verbatim (`run_started` at seq 0, then the prefix),
/// sets `next_seq = cut` and `status_seq`. Call only after [`check`] accepted `s` and only
/// when `s.rerun_from_seq != 0`; blind writes only.
pub(crate) fn journal_prefix(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &mut RunRecord,
    s: &StartRun,
) {
    debug_assert!(
        s.rerun_from_seq != 0,
        "the rerun path is gated on rerun_from_seq != 0"
    );
    if let Some(ev) = &s.run_started {
        super::run::put_event(b, run, ev);
    }
    for ev in &s.rerun_prefix {
        super::run::put_event(b, run, ev);
        if ev.kind == EventKind::StatusPublished.code() {
            rec.status_seq = Some(ev.seq);
        }
    }
    rec.next_seq = s.rerun_from_seq;
}
