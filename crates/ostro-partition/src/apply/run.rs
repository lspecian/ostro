//! Run lifecycle apply: `StartRun`, `AttemptGranted`, `EventsAppended` with
//! Phase-2 `Derived` intents (STOR-05, STOR-10, STOR-14).
//!
//! Every handler validates completely before its first write, so a rejected
//! command leaves nothing in the overlay. Hot-path handlers issue at most one
//! keyspace point read (S3 RMW margin 1.19x): the run record (or the dedupe
//! row for a keyed start). All other writes are blind.

use ostro_types::command::{
    AttemptGranted, ChildCompleted, ConflictPolicy, EventsAppended, OutboxMsg, StartRun,
    TerminalStatus, derived::Intent, outbox_msg,
};
use ostro_types::{AttemptToken, Epoch, PayloadRef, RunId};
use prost::Message;

use super::START_DELAY_OP;
use super::cursor::Cursor;
use super::outbox::{emit, msg_oversize, msg_target};
use super::outcome::{Action, ApplyOutcome, InvalidCode, RejectReason};
use crate::store::keys::{
    children_key, dedupe_key, event_key, payload_key, ready_key, run_key, timer_key,
};
use crate::store::values::{self, ReadySlot, ReadyValue, RunRecord, RunStatus};
use crate::store::{ApplyBatch, StoreError, Table};

/// `payloads` kind of the run input.
pub(crate) const PAYLOAD_INPUT: u8 = 0;
/// `payloads` kind of the run result.
pub(crate) const PAYLOAD_RESULT: u8 = 1;

const MAX_KEY_STR: usize = u16::MAX as usize;

type Check<T> = Result<T, RejectReason>;

fn reject(r: RejectReason) -> Result<ApplyOutcome, StoreError> {
    Ok(ApplyOutcome::Rejected(r))
}

fn invalid(c: InvalidCode) -> RejectReason {
    RejectReason::Invalid(c)
}

/// Parses a run id that must live in this partition.
pub(crate) fn local_run(cur: &Cursor, bytes: &[u8]) -> Check<RunId> {
    let run = RunId::from_bytes(bytes).map_err(|_| invalid(InvalidCode::BadRunId))?;
    if run.partition() != cur.partition {
        return Err(RejectReason::WrongPartition);
    }
    Ok(run)
}

/// One point read of the run record.
pub(crate) fn load_run(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
) -> Result<Option<RunRecord>, StoreError> {
    b.get(Table::Runs, &run_key(run))?
        .map(|v| values::decode(&v))
        .transpose()
}

pub(crate) fn store_run(b: &mut ApplyBatch<'_>, run: &RunId, rec: &RunRecord) {
    b.insert(Table::Runs, run_key(run), values::encode(rec));
}

fn token_of(t: (u32, u32)) -> AttemptToken {
    AttemptToken {
        epoch: Epoch(t.0),
        n: t.1,
    }
}

fn oversize(p: &Option<PayloadRef>) -> bool {
    p.as_ref().is_some_and(PayloadRef::is_oversize_inline)
}

pub(crate) fn terminal_status(code: i32) -> Option<RunStatus> {
    match TerminalStatus::try_from(code).ok()? {
        TerminalStatus::Completed => Some(RunStatus::Completed),
        TerminalStatus::Failed => Some(RunStatus::Failed),
        TerminalStatus::Cancelled => Some(RunStatus::Cancelled),
    }
}

/// Blind-removes the run's ready row, if any.
pub(crate) fn remove_ready(b: &mut ApplyBatch<'_>, run: &RunId, rec: &mut RunRecord) {
    if let Some(slot) = rec.ready.take() {
        b.remove(
            Table::Ready,
            ready_key(&rec.queue, slot.priority, slot.ready_at_ms, run),
        );
    }
}

/// Inserts the ready row, marks the run Ready and emits `EnqueueReady`.
pub(crate) fn enqueue_ready(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &mut RunRecord,
    ready_at_ms: u64,
    actions: &mut Vec<Action>,
) {
    remove_ready(b, run, rec);
    b.insert(
        Table::Ready,
        ready_key(&rec.queue, rec.priority, ready_at_ms, run),
        values::encode(&ReadyValue {
            concurrency_key: rec.concurrency_key.clone(),
        }),
    );
    rec.ready = Some(ReadySlot {
        priority: rec.priority,
        ready_at_ms,
    });
    rec.status = RunStatus::Ready;
    actions.push(Action::EnqueueReady {
        run: *run,
        queue: rec.queue.clone(),
        priority: rec.priority,
        ready_at_ms,
        concurrency_key: rec.concurrency_key.clone(),
    });
}

// ------------------------------------------------------------------ StartRun

/// Creates a run. `dedupe` is the `(workflow, key)` idempotency row to check
/// and claim; `None` checks the run id instead. Exactly one point read.
pub(crate) fn start_run(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    s: &StartRun,
    dedupe: Option<(String, Vec<u8>)>,
    actions: &mut Vec<Action>,
) -> Result<ApplyOutcome, StoreError> {
    let run = match local_run(cur, &s.run_id) {
        Ok(r) => r,
        Err(r) => return reject(r),
    };
    if s.workflow.len() > MAX_KEY_STR
        || s.queue.len() > MAX_KEY_STR
        || dedupe.as_ref().is_some_and(|(w, _)| w.len() > MAX_KEY_STR)
    {
        return reject(invalid(InvalidCode::FieldTooLong));
    }
    let parent = match &s.parent {
        None => None,
        Some(p) => match p.run_id() {
            Ok(id) => Some((*id.as_bytes(), p.op)),
            Err(_) => return reject(invalid(InvalidCode::BadRunId)),
        },
    };
    let Ok(policy) = ConflictPolicy::try_from(s.conflict_policy) else {
        return reject(invalid(InvalidCode::Decode));
    };
    if oversize(&s.input) {
        return reject(RejectReason::PayloadTooLarge);
    }

    // The single point read.
    match dedupe {
        Some((wf, key)) => {
            let dk = dedupe_key(&wf, &key);
            if let Some(v) = b.get(Table::Dedupe, &dk)? {
                let existing = RunId::from_bytes(&v)
                    .map_err(|e| StoreError::Codec(format!("dedupe row: {e}")))?;
                return match policy {
                    ConflictPolicy::ReturnExisting => Ok(ApplyOutcome::Duplicate {
                        existing: Some(existing),
                    }),
                    ConflictPolicy::Reject => {
                        reject(RejectReason::IdempotencyConflict { existing })
                    }
                };
            }
            b.insert(Table::Dedupe, dk, run.as_bytes().to_vec());
        }
        None => {
            if b.get(Table::Runs, &run_key(&run))?.is_some() {
                return Ok(ApplyOutcome::Duplicate {
                    existing: Some(run),
                });
            }
        }
    }

    let ready_now = s.ready_at_ms <= s.requested_at_ms;
    let mut rec = RunRecord {
        workflow: s.workflow.clone(),
        workflow_version: s.workflow_version,
        queue: s.queue.clone(),
        status: RunStatus::Scheduled,
        current_attempt: None,
        next_seq: 0,
        ready: None,
        parent,
        cancel_requested: false,
        concurrency_key: s.concurrency_key.clone(),
        priority: s.priority,
        created_at_ms: s.requested_at_ms,
        rng_seed: s.rng_seed.clone(),
        inbox_next: 0,
        children: 0,
    };
    if let Some(input) = &s.input {
        b.insert(
            Table::Payloads,
            payload_key(&run, PAYLOAD_INPUT),
            input.encode_to_vec(),
        );
    }
    if ready_now {
        enqueue_ready(b, &run, &mut rec, s.ready_at_ms, actions);
    } else {
        b.insert(
            Table::Timers,
            timer_key(s.ready_at_ms, &run, START_DELAY_OP),
            Vec::new(),
        );
        actions.push(Action::ScheduleTimer {
            due_at_ms: s.ready_at_ms,
            run,
            op: START_DELAY_OP,
        });
    }
    store_run(b, &run, &rec);
    Ok(ApplyOutcome::Accepted)
}

// ------------------------------------------------------------ AttemptGranted

pub(crate) fn apply_attempt_granted(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    g: &AttemptGranted,
) -> Result<ApplyOutcome, StoreError> {
    let Some(t) = g.token.map(AttemptToken::from) else {
        return reject(invalid(InvalidCode::MissingToken));
    };
    let run = match local_run(cur, &g.run_id) {
        Ok(r) => r,
        Err(r) => return reject(r),
    };
    let Some(mut rec) = load_run(b, &run)? else {
        return reject(RejectReason::RunNotFound);
    };
    if rec.status.is_terminal() {
        return reject(RejectReason::RunTerminal);
    }
    if t.epoch != cur.epoch() {
        return reject(RejectReason::StaleEpoch {
            current: cur.epoch(),
        });
    }
    let current = rec.current_attempt.map(token_of);
    if Some(t) <= current {
        return reject(RejectReason::Fenced { current });
    }
    rec.current_attempt = Some((t.epoch.0, t.n));
    rec.status = RunStatus::Running;
    remove_ready(b, &run, &mut rec);
    store_run(b, &run, &rec);
    Ok(ApplyOutcome::Accepted)
}

// ------------------------------------------------------------ EventsAppended

/// Validates the Derived intents' structure; returns nothing on success.
fn check_intents(cur: &Cursor, e: &EventsAppended) -> Check<()> {
    let mut completed = false;
    for d in &e.derived {
        let Some(i) = &d.intent else {
            return Err(invalid(InvalidCode::MissingField));
        };
        if completed {
            return Err(invalid(InvalidCode::BadIntent));
        }
        match i {
            Intent::ScheduleTimer(t) if t.op == START_DELAY_OP => {
                return Err(invalid(InvalidCode::BadIntent));
            }
            Intent::CancelTimer(t) if t.op == START_DELAY_OP => {
                return Err(invalid(InvalidCode::BadIntent));
            }
            Intent::ScheduleTimer(_) | Intent::CancelTimer(_) | Intent::EnqueueReady(_) => {}
            Intent::SendMessage(m) => {
                let msg = m.msg.as_ref().ok_or(invalid(InvalidCode::MissingField))?;
                msg_target(cur, msg)?;
            }
            Intent::CompleteRun(c) => {
                terminal_status(c.status).ok_or(invalid(InvalidCode::BadIntent))?;
                completed = true;
            }
        }
    }
    Ok(())
}

fn any_oversize(e: &EventsAppended) -> bool {
    e.events.iter().any(|ev| oversize(&ev.payload))
        || e.derived.iter().any(|d| match &d.intent {
            Some(Intent::SendMessage(m)) => m.msg.as_ref().is_some_and(msg_oversize),
            Some(Intent::CompleteRun(c)) => oversize(&c.result),
            _ => false,
        })
}

pub(crate) fn apply_events_appended(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    e: &EventsAppended,
    actions: &mut Vec<Action>,
) -> Result<ApplyOutcome, StoreError> {
    let Ok(t) = e.token() else {
        return reject(invalid(InvalidCode::MissingToken));
    };
    let run = match local_run(cur, &e.run_id) {
        Ok(r) => r,
        Err(r) => return reject(r),
    };
    let Some(mut rec) = load_run(b, &run)? else {
        return reject(RejectReason::RunNotFound);
    };
    if rec.status.is_terminal() {
        return reject(RejectReason::RunTerminal);
    }
    if t.epoch != cur.epoch() {
        return reject(RejectReason::StaleEpoch {
            current: cur.epoch(),
        });
    }
    let current = rec.current_attempt.map(token_of);
    if Some(t) != current {
        return reject(RejectReason::Fenced { current });
    }
    if e.from_seq != rec.next_seq {
        return reject(RejectReason::SeqMismatch {
            expected: rec.next_seq,
        });
    }
    let n = e.events.len() as u64;
    let Some(new_next) = e.from_seq.checked_add(n) else {
        return reject(invalid(InvalidCode::EventsNotContiguous));
    };
    if !e.events_contiguous_from(e.from_seq) {
        return reject(invalid(InvalidCode::EventsNotContiguous));
    }
    if let Err(r) = check_intents(cur, e) {
        return reject(r);
    }
    if any_oversize(e) {
        return reject(RejectReason::PayloadTooLarge);
    }

    // All checks passed: write.
    for ev in &e.events {
        b.insert(Table::Events, event_key(&run, ev.seq), ev.encode_to_vec());
    }
    rec.next_seq = new_next;
    for d in &e.derived {
        let Some(i) = &d.intent else { continue };
        match i {
            Intent::ScheduleTimer(st) => {
                b.insert(
                    Table::Timers,
                    timer_key(st.due_at_ms, &run, st.op),
                    Vec::new(),
                );
                actions.push(Action::ScheduleTimer {
                    due_at_ms: st.due_at_ms,
                    run,
                    op: st.op,
                });
            }
            Intent::CancelTimer(ct) => {
                b.remove(Table::Timers, timer_key(ct.due_at_ms, &run, ct.op));
                actions.push(Action::CancelTimer {
                    due_at_ms: ct.due_at_ms,
                    run,
                    op: ct.op,
                });
            }
            Intent::SendMessage(m) => {
                let Some(msg) = &m.msg else { continue };
                if let Some(outbox_msg::Kind::StartChild(c)) = &msg.kind {
                    // msg_target validated the child's run id.
                    let child_bytes = c.start.as_ref().map_or(&[][..], |s| &s.run_id);
                    let child = RunId::from_bytes(child_bytes)
                        .map_err(|e| StoreError::Codec(format!("child run id: {e}")))?;
                    b.insert(Table::Children, children_key(&run, &child), Vec::new());
                    rec.children = rec.children.saturating_add(1);
                }
                emit(b, cur, msg, actions)?;
            }
            Intent::EnqueueReady(er) => {
                enqueue_ready(b, &run, &mut rec, er.ready_at_ms, actions);
            }
            Intent::CompleteRun(c) => {
                let status = terminal_status(c.status)
                    .ok_or_else(|| StoreError::Codec("validated terminal status".into()))?;
                rec.status = status;
                if let Some(res) = &c.result {
                    b.insert(
                        Table::Payloads,
                        payload_key(&run, PAYLOAD_RESULT),
                        res.encode_to_vec(),
                    );
                }
                remove_ready(b, &run, &mut rec);
                actions.push(Action::RunCompleted { run, status });
                if let Some((parent, op)) = rec.parent {
                    let msg = OutboxMsg {
                        kind: Some(outbox_msg::Kind::ChildCompleted(ChildCompleted {
                            parent_run: parent.to_vec(),
                            child_run: run.as_bytes().to_vec(),
                            op,
                            status: c.status,
                            result: c.result.clone(),
                        })),
                    };
                    emit(b, cur, &msg, actions)?;
                }
            }
        }
    }
    store_run(b, &run, &rec);
    Ok(ApplyOutcome::Accepted)
}
