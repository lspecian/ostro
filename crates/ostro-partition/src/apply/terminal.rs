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
//! **Read budget:** the two `waits` prefix scans (2 reads) run only when
//! `RunRecord.open_waits > 0`; otherwise the hook reads nothing (asserted in
//! `tests/apply_seams.rs`). Documented exception, docs/storage-format.md §13.

use ostro_types::RunId;

use super::lifecycle;
use super::outcome::Action;
use crate::store::keys::{WAIT_ROW, decode_wait_key, timer_key, wait_prefixes};
use crate::store::values::{self, RunRecord, WaitRow};
use crate::store::{ApplyBatch, StoreError, Table};

pub(crate) fn on_terminal(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &mut RunRecord,
    at_ms: u64,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    if rec.open_waits > 0 {
        clear_waits(b, run, actions)?;
        rec.open_waits = 0;
    }
    lifecycle::on_terminal(b, run, rec, at_ms, actions)
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
