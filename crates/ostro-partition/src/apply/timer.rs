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
//! `from_seq` compare-and-set.

use bytes::Bytes;
use ostro_types::PayloadRef;
use ostro_types::command::{JournalEvent, TimerFired};
use ostro_types::journal::{self, Event, EventBody, encode_event};

use super::START_DELAY_OP;
use super::cursor::Cursor;
use super::inbound::make_ready;
use super::outcome::{Action, ApplyOutcome};
use super::run::{load_run, local_run, put_event, store_run};
use crate::store::keys::timer_key;
use crate::store::values::RunStatus;
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
        let seq = rec.next_seq;
        let (kind, bytes) = encode_event(&Event {
            seq,
            ts_nanos: t.fired_at_ms.saturating_mul(1_000_000),
            body: EventBody::TimerFired(journal::TimerFired { op: t.op }),
        });
        put_event(
            b,
            &run,
            &JournalEvent {
                seq,
                kind,
                payload: Some(PayloadRef::inline(Bytes::from(bytes))),
            },
        );
        // u64 seqs cannot be exhausted in practice; saturating keeps apply
        // total (EventsAppended rejects at the same bound).
        rec.next_seq = seq.saturating_add(1);
        match rec.status {
            RunStatus::Scheduled => make_ready(b, &run, &mut rec, t.fired_at_ms, actions),
            RunStatus::Running => actions.push(Action::InputAppended { run, seq }),
            _ => {}
        }
    }
    store_run(b, &run, &rec);
    Ok(ApplyOutcome::Accepted)
}
