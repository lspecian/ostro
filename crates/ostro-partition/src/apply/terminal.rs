//! The terminal-transition hook (plan 05-02).
//!
//! Every path that moves a run to a terminal status (executor `CompleteRun`; from 05-09 also
//! terminate, run timeout, schedule-to-start timeout and continue-as-new) calls
//! [`on_terminal`] after setting the status, before storing the record.
//!
//! It clears the run's waits (D4): every `waits` wait row (`0x01`), every FIFO index row
//! (`0x02`) and every pending wait-timeout timer row, so a stale wait timer can never fire
//! into a resumed run. Each removed timer also emits `Action::CancelTimer` for the timer
//! service. Then it calls `lifecycle::on_terminal` (05-09: retention timer, run-timeout row).
//!
//! Phase 6 (06-03, P-8): when the run is **not** `resumable` (callers set the flag before the
//! call), every `effects` row of the run is deleted (`effects::delete_run_effects`). Resumable
//! terminals keep them, so a resumed run still accepts the completion of an in-flight effect.
//!
//! Phase 8 (08-07): for a schedule-started run (idempotency key `__cron/…`), the hook then
//! calls `schedule::on_run_terminal`, which deletes the run's `schedule_state` reverse row and
//! starts a buffered `BufferOne` fire in the same batch. Other runs pay nothing for it.
//!
//! **Read budget:** the two `waits` prefix scans (2 reads) run only when
//! `RunRecord.open_waits > 0`; otherwise the hook reads nothing (asserted in
//! `tests/apply_seams.rs`). Documented exception, docs/storage-format.md §13. Non-resumable
//! terminals add 1 effects prefix scan by run (06-03; asserted in `tests/apply_effects.rs`).
//! Schedule-started runs add 1 (reverse row) to 3 reads (08-07; asserted in
//! `tests/apply_schedule.rs`).

use ostro_types::RunId;

use super::cursor::Cursor;
use super::outcome::Action;
use super::{effects, lifecycle, schedule};
use crate::store::keys::{WAIT_ROW, decode_wait_key, timer_key, wait_prefixes};
use crate::store::values::{self, RunRecord, WaitRow};
use crate::store::{ApplyBatch, StoreError, Table};

pub(crate) fn on_terminal(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    run: &RunId,
    rec: &mut RunRecord,
    at_ms: u64,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    if rec.open_waits > 0 {
        clear_waits(b, run, actions)?;
        rec.open_waits = 0;
    }
    // P-8 (06-03): a resumable terminal keeps its in-flight effects for the resume.
    if !rec.resumable {
        effects::delete_run_effects(b, run)?;
    }
    lifecycle::on_terminal(b, run, rec, at_ms, actions)?;
    // 08-07: schedule runs only (0 reads otherwise).
    if rec.idempotency_key.starts_with(schedule::CRON_KEY_PREFIX) {
        schedule::on_run_terminal(b, cur, run, rec.status, actions)?;
    }
    Ok(())
}

/// Deletes both `waits` key shapes of `run` and the wait-timeout timers. Two prefix reads.
fn clear_waits(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    for prefix in wait_prefixes(run) {
        for (k, v) in b.range_prefix(Table::Waits, &prefix)? {
            if k.first() == Some(&WAIT_ROW) {
                let (_, op) = decode_wait_key(&k)
                    .ok_or_else(|| StoreError::Codec("malformed waits key".into()))?;
                let row: WaitRow = values::decode(&v)?;
                if row.timeout_due_ms > 0 {
                    b.remove(Table::Timers, timer_key(row.timeout_due_ms, run, op));
                    actions.push(Action::CancelTimer {
                        due_at_ms: row.timeout_due_ms,
                        run: *run,
                        op,
                    });
                }
            }
            b.remove(Table::Waits, k);
        }
    }
    Ok(())
}
