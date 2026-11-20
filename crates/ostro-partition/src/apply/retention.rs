//! Run retention: `EXPIRE_OP` deletes a terminal run's rows (plan **05-09**, D9, OPS-06).
//!
//! `lifecycle::on_terminal` arms `EXPIRE_OP` at `completed_at_ms + retention_ms` for every
//! terminal transition with `retention_ms > 0`. When it fires on a terminal run whose expiry
//! due time matches, [`on_expire`] deletes, in the one apply batch of the `TimerFired`:
//!
//! - `runs` (the record), `running`, the ready row (if any);
//! - `events` `run ‖ 0 .. next_seq` (blind deletes: every journal row has a seq below
//!   `next_seq`), `payloads` input + result (blind);
//! - prefix scans of `inbox` (legacy and signal rows), `waits` (both key shapes),
//!   `children`, `effects`, `signal_seen` and `dlq`, deleting every row found;
//! - the `dedupe` row rebuilt from `(workflow, idempotency_key)`, **only if it still points
//!   at this run** (so a key reused after an earlier expiry is untouched). After that a
//!   `StartRun` with the same key creates a new run.
//!
//! Not deleted (documented limits): guest timer rows still pending on a terminal run are
//! keyed `due ‖ run ‖ op` with no run prefix, so they stay until they fire (an orphan fire
//! is an accepted no-op); a cron run's dedupe row lives under `__cron/<schedule>` with the
//! fire time, which the record does not store, so it stays (it keeps that fire time
//! claimed). WAL bytes are not reclaimed until Phase 8 (`snapshot_floor` stays 0); blobs are
//! not garbage-collected in Phase 5.
//!
//! **Reads:** 2 in `timer.rs` (timer row, record) + 7 prefix scans + 1 dedupe point read =
//! at most 10, independent of the run's size. Writes are proportional to its rows (a 50k-event
//! run is measured in `tests/apply_retention.rs`).

use ostro_types::RunId;
use ostro_types::command::TimerFired;

use super::cursor::Cursor;
use super::effects;
use super::lifecycle::expire_due;
use super::outcome::{Action, ApplyOutcome, ApplyResult};
use super::run::{PAYLOAD_INPUT, PAYLOAD_RESULT, remove_ready};
use crate::store::keys::{
    children_prefix, dedupe_key, event_key, payload_key, run_key, run_prefix, running_key,
    wait_prefixes,
};
use crate::store::values::RunRecord;
use crate::store::{ApplyBatch, StoreError, Table};

/// `TimerFired` for `EXPIRE_OP`. `timer.rs` already removed the timer row and read the record
/// (`None` = the run is gone); this runs before any status check, because expiry acts on
/// terminal runs.
///
/// - unknown run → `Duplicate` (the timer row removal is the whole effect);
/// - non-terminal run (resumed by `RetryRun`) or a due time that is not the record's expiry
///   (a stale row) → `Accepted`, timer row only;
/// - terminal run → every run-scoped row deleted (module docs) → `Accepted`.
pub(crate) fn on_expire(
    b: &mut ApplyBatch<'_>,
    _cur: &mut Cursor,
    run: &RunId,
    rec: Option<RunRecord>,
    t: &TimerFired,
    _actions: &mut Vec<Action>,
) -> ApplyResult {
    let Some(mut rec) = rec else {
        return Ok(ApplyOutcome::Duplicate { existing: None });
    };
    if !rec.status.is_terminal() || expire_due(&rec) != Some(t.due_at_ms) {
        return Ok(ApplyOutcome::Accepted);
    }
    delete_run(b, run, &mut rec)?;
    Ok(ApplyOutcome::Accepted)
}

fn delete_prefix(b: &mut ApplyBatch<'_>, t: Table, prefix: &[u8]) -> Result<(), StoreError> {
    for (k, _) in b.range_prefix(t, prefix)? {
        b.remove(t, k);
    }
    Ok(())
}

fn delete_run(b: &mut ApplyBatch<'_>, run: &RunId, rec: &mut RunRecord) -> Result<(), StoreError> {
    // Dedupe first (the one point read), only if the row is still this run's.
    if !rec.idempotency_key.is_empty() && rec.workflow.len() <= usize::from(u16::MAX) {
        let dk = dedupe_key(&rec.workflow, &rec.idempotency_key);
        if b.get(Table::Dedupe, &dk)?.as_deref() == Some(run.as_bytes().as_slice()) {
            b.remove(Table::Dedupe, dk);
        }
    }
    b.remove(Table::Runs, run_key(run));
    b.remove(Table::Running, running_key(run));
    remove_ready(b, run, rec);
    for seq in 0..rec.next_seq {
        b.remove(Table::Events, event_key(run, seq));
    }
    b.remove(Table::Payloads, payload_key(run, PAYLOAD_INPUT));
    b.remove(Table::Payloads, payload_key(run, PAYLOAD_RESULT));
    let rp = run_prefix(run);
    delete_prefix(b, Table::Inbox, &rp)?;
    for p in wait_prefixes(run) {
        delete_prefix(b, Table::Waits, &p)?;
    }
    delete_prefix(b, Table::Children, &children_prefix(run))?;
    effects::delete_run_effects(b, run)?;
    delete_prefix(b, Table::SignalSeen, &rp)?;
    delete_prefix(b, Table::Dlq, &rp)?;
    Ok(())
}
