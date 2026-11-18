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

use ostro_types::command::TimerFired;

use super::START_DELAY_OP;
use super::cursor::Cursor;
use super::inbound::make_ready;
use super::outcome::{Action, ApplyOutcome};
use super::run::{load_run, local_run, store_run};
use crate::store::keys::{inbox_key, timer_key};
use crate::store::values::{self, InboxMsg};
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

    let Some(mut rec) = load_run(b, &run)? else {
        // Orphan timer row: removing it is the whole effect.
        return Ok(ApplyOutcome::Accepted);
    };
    if rec.status.is_terminal() {
        // CompleteRun does not delete pending timers; firing one is a no-op
        // beyond removing its row.
        return Ok(ApplyOutcome::Accepted);
    }
    if t.op == START_DELAY_OP {
        // Delayed start: Scheduled -> Ready at the requested ready time. A
        // run already woken early (by an inbound message) stays as it is.
        make_ready(b, &run, &mut rec, t.due_at_ms, actions);
    } else {
        b.insert(
            Table::Inbox,
            inbox_key(&run, rec.inbox_next),
            values::encode(&InboxMsg::TimerFired {
                op: t.op,
                fired_at_ms: t.fired_at_ms,
            }),
        );
        rec.inbox_next += 1;
        make_ready(b, &run, &mut rec, t.fired_at_ms, actions);
    }
    store_run(b, &run, &rec);
    Ok(ApplyOutcome::Accepted)
}
