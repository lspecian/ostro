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
//! Phase 7 (07-04): last, the hook clears `RunRecord.pause` (no read).
//!
//! Phase 9 (09-08, PF-F3, D-P9-11): **parent-close cascade.** For an executor or
//! continue-as-new terminal (Completed, Failed, Cancelled, ContinuedAsNew) of a run with
//! `children > 0`, the hook sends a graceful `CancelRun{reason: "parent_closed"}` to every
//! child in the `children` keyspace (one prefix scan), removes those rows and sets
//! `children = 0`. The hard stops (Terminated, TimedOut) keep 05-09's cascade in
//! `lifecycle::hard_stop`, which runs after the parent's `ChildCompleted` and carries the
//! hard-stop reason; from 09-08 it removes the rows too. Removing the rows at close is what
//! makes the journal-load repair re-send the StartChild after a RetryRun resume (D-P9-24).
//! A child that already ended drops the extra `CancelRun` (per-message drop at the target).
//!
//! **Read budget:** the two `waits` prefix scans (2 reads) run only when
//! `RunRecord.open_waits > 0`; otherwise the hook reads nothing (asserted in
//! `tests/apply_seams.rs`). Documented exception, docs/storage-format.md §13. Non-resumable
//! terminals add 1 effects prefix scan by run (06-03; asserted in `tests/apply_effects.rs`).
//! Schedule-started runs add 1 (reverse row) to 3 reads (08-07; asserted in
//! `tests/apply_schedule.rs`).
//! The parent-close cascade adds 1 children prefix scan, only when `RunRecord.children > 0`
//! (09-08; asserted in `tests/children_close.rs`).

use ostro_types::RunId;

use ostro_types::command::{CancelRun, OutboxMsg, outbox_msg};

use super::cursor::Cursor;
use super::outbox::emit;
use super::outcome::Action;
use super::{effects, lifecycle, schedule};
use crate::store::keys::{WAIT_ROW, children_prefix, decode_wait_key, timer_key, wait_prefixes};
use crate::store::values::{self, RunRecord, RunStatus, WaitRow};
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
    super::index::on_terminal(b, run, rec);
    // 08-07: schedule runs only (0 reads otherwise).
    if rec.idempotency_key.starts_with(schedule::CRON_KEY_PREFIX) {
        schedule::on_run_terminal(b, cur, run, rec.status, actions)?;
    }
    // 09-08 (PF-F3): parent-close cascade (hard stops cascade in `lifecycle::hard_stop`).
    if rec.children > 0 && !matches!(rec.status, RunStatus::Terminated | RunStatus::TimedOut) {
        close_children(b, cur, run, rec, REASON_PARENT_CLOSED, actions)?;
    }
    // 07-04 (B7): a terminal run is never paused, so a resumed (RetryRun) run is not stuck
    // behind the enqueue_ready gate. Callers store the record after the hook.
    rec.pause = None;
    Ok(())
}

/// `CancelRun` reason of the parent-close cascade (09-08, D-P9-11).
pub(crate) const REASON_PARENT_CLOSED: &str = "parent_closed";

/// Graceful `CancelRun{reason}` to every child row of `run`, then the rows are removed and
/// `rec.children` is 0 (09-08). One prefix scan; callers check `children > 0`.
pub(crate) fn close_children(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    run: &RunId,
    rec: &mut RunRecord,
    reason: &str,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    for (k, _) in b.range_prefix(Table::Children, &children_prefix(run))? {
        let child = k
            .get(RunId::LEN..)
            .ok_or_else(|| StoreError::Codec("short children key".into()))?;
        let msg = OutboxMsg {
            kind: Some(outbox_msg::Kind::CancelRun(CancelRun {
                target_run: child.to_vec(),
                reason: reason.to_string(),
            })),
        };
        emit(b, cur, &msg, actions)?;
        b.remove(Table::Children, k);
    }
    rec.children = 0;
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
