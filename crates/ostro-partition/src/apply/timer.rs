//! `TimerFired` apply (STOR-13, apply half).
//!
//! Idempotent by construction: the timer row `(due, run, op)` is the token.
//! The first `TimerFired` removes it; any later one (a re-fire by a new
//! leader, a fire after `CancelTimer`) finds no row and is a `Duplicate`
//! no-op. Wall clock never enters apply: `fired_at_ms` comes from the
//! command.
//!
//! Reads: one point read of the timer row; if present, one more of the run
//! record (allowed for `TimerFired`).
//!
//! **Timer inputs go to the journal (plan 04-13).** A run's timer op (not the
//! start-delay op) is journaled as a `TimerFired { op }` event at
//! `rec.next_seq` with `ts_nanos = fired_at_ms * 1_000_000`, and `next_seq`
//! moves by one; no inbox row is written (signals, child results and cancel
//! requests still use the inbox until Phase 5). The run is then woken by
//! status: `Scheduled` becomes `Ready` (`Action::EnqueueReady`); `Running`
//! stays Running and gets `Action::InputAppended { run, seq }` so the
//! dispatcher can forward the input to the hot activation (the `running` row
//! is not touched); `Ready` only gets the event. Because `next_seq` moves, any
//! `EventsAppended` built before the input was seen, a `Suspend` included, is
//! rejected `SeqMismatch`: the lost-wake-up window is closed by the existing
//! `from_seq` compare-and-set. The journaling and wake-up live in
//! [`super::inputs::journal_input`] since plan 05-02 (same behaviour).
//!
//! **Reserved-op routing (plan 05-02).** After the timer row is removed and the run record
//! read, apply routes by op before any status check (expiry acts on terminal runs):
//! `RUN_TIMEOUT_OP` / `SCHEDULE_TO_START_OP` → `lifecycle::on_reserved_timer`, `EXPIRE_OP` →
//! `retention::on_expire` (05-09), any other reserved op → no effect. `START_DELAY_OP` keeps
//! its Phase 2 path. Every other op is a guest op: on a live run `waits::try_wait_timeout`
//! (05-08) gets the first chance (`WaitTimedOut`), then the ordinary `TimerFired` journal path.

use ostro_types::command::{
    EXPIRE_OP, RUN_TIMEOUT_OP, SCHEDULE_TO_START_OP, TimerFired, is_reserved_op,
};
use ostro_types::journal::{self, EventBody};

use super::START_DELAY_OP;
use super::cursor::Cursor;
use super::inbound::make_ready;
use super::inputs::journal_input;
use super::outcome::{Action, ApplyOutcome};
use super::run::{load_run, local_run, store_run};
use super::{lifecycle, retention, waits};
use crate::store::keys::timer_key;
use crate::store::{ApplyBatch, StoreError, Table};

pub(crate) fn apply_timer_fired(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    t: &TimerFired,
    actions: &mut Vec<Action>,
) -> Result<ApplyOutcome, StoreError> {
    let run = match local_run(cur, &t.run_id) {
        Ok(r) => r,
        Err(r) => return Ok(ApplyOutcome::Rejected(r)),
    };
    let tk = timer_key(t.due_at_ms, &run, t.op);
    if b.get(Table::Timers, &tk)?.is_none() {
        return Ok(ApplyOutcome::Duplicate { existing: None });
    }
    b.remove(Table::Timers, tk);
    let rec = load_run(b, &run)?;

    // Reserved ops (apply-owned, never guest ops), routed before the status checks.
    match t.op {
        RUN_TIMEOUT_OP | SCHEDULE_TO_START_OP => {
            return lifecycle::on_reserved_timer(b, cur, &run, rec, t, actions);
        }
        EXPIRE_OP => return retention::on_expire(b, cur, &run, rec, t, actions),
        START_DELAY_OP => {}
        op if is_reserved_op(op) => return Ok(ApplyOutcome::Accepted),
        _ => {}
    }

    let Some(mut rec) = rec else {
        // Orphan timer row: removing it is the whole effect.
        return Ok(ApplyOutcome::Accepted);
    };
    if rec.status.is_terminal() {
        // Terminal transitions delete wait timers (`terminal::on_terminal`); any other
        // pending timer firing on a terminal run is a no-op beyond removing its row.
        return Ok(ApplyOutcome::Accepted);
    }
    if t.op == START_DELAY_OP {
        // Delayed start: Scheduled -> Ready at the requested ready time. A
        // run already woken early (by an inbound message) stays as it is.
        make_ready(b, &run, &mut rec, t.due_at_ms, actions);
    } else {
        if let Some(out) = waits::try_wait_timeout(b, cur, &run, &mut rec, t, actions) {
            return out;
        }
        journal_input(
            b,
            &run,
            &mut rec,
            t.fired_at_ms,
            EventBody::TimerFired(journal::TimerFired { op: t.op }),
            actions,
        )?;
    }
    store_run(b, &run, &rec);
    Ok(ApplyOutcome::Accepted)
}
