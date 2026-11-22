//! Run lifecycle: terminate, retry, run/schedule-to-start timeouts, continue-as-new (plan
//! **05-09**; seams wired by 05-02).
//!
//! Semantics (D1, D2, D3, D9, D10, Phase 9 PF-F2):
//!
//! - **Timeouts (D2).** `StartRun.run_timeout_ms > 0` arms `RUN_TIMEOUT_OP` at
//!   `created_at_ms + run_timeout_ms`. `schedule_to_start_ms > 0` arms `SCHEDULE_TO_START_OP`
//!   at `max(ready_at_ms, requested_at_ms) + schedule_to_start_ms` and records the due time
//!   in `RunRecord.s2s_due_ms`; the first `AttemptGranted` deletes that row by key (the ready
//!   slot is gone by then, W13) and zeroes `s2s_due_ms`. A run timeout firing on a live run
//!   is a hard stop `TimedOut` with reason `run_timeout`; a schedule-to-start firing on a
//!   live, never-granted run is a hard stop `TimedOut` with reason `schedule_to_start`. A
//!   row that no longer matches the record (wrong due time, already granted, resumed run,
//!   terminal run) only loses its row.
//! - **Hard stop (D3)** ([`hard_stop`], shared by `TerminateRun` and both timeouts): status,
//!   `terminal_reason`, `completed_at_ms`, `resumable = true` (D1); ready/running rows
//!   removed; `RunTerminated{reason}` (kind 71) journaled **only when `next_seq > 0`** (so a
//!   never-started run keeps "exactly one RunStarted at seq 0") and without a wake;
//!   `terminal::on_terminal` (waits, timeout rows, expiry); `Action::RunCompleted`;
//!   `ChildCompleted` to the parent (no result, no event: a hard stop has neither); a
//!   graceful `CancelRun` to every child.
//! - **Retry (D1)**: see [`retry_run`]. Resume in place; **no new run timeout**.
//! - **Continue-as-new (D10, PF-F2)**: see [`complete_continued_as_new`].
//! - **Expiry (D9)**: every terminal transition arms `EXPIRE_OP` at
//!   `completed_at_ms + retention_ms` when `retention_ms > 0` ([`on_terminal`]);
//!   `retention::on_expire` does the delete.
//!
//! Every armed reserved timer also emits `Action::ScheduleTimer`, every deleted one
//! `Action::CancelTimer`, exactly like guest timers.
//!
//! **Read counts** (not hot path; all bounded, asserted in `tests/apply_lifecycle.rs`):
//! - `TerminateRun`: 1 (run record) + 1 children scan when `children > 0` + 2 waits scans
//!   when `open_waits > 0` → at most 4.
//! - `RetryRun`: 1 (run record).
//! - Timeout fire: 2 (timer row + record, in `timer.rs`) + the hard-stop scans above.
//! - `on_start` / `on_attempt_granted` / `on_terminal`: 0 (blind writes only).
//! - `complete_continued_as_new`: 0 beyond `EventsAppended`'s record read (+ the waits scans
//!   of `on_terminal` when `open_waits > 0`).
//!
//! Local helpers: `cascade_cancel` duplicates the children scan of `inbound::cancel_run`
//! (private there; owned by 05-08) and the CAN write loop duplicates the intent arms of
//! `run::apply_events_appended` it needs (05-02 left them inline). Candidates for a later
//! shared helper.

use ostro_types::RunId;
use ostro_types::command::{
    AttemptGranted, CancelRun, ChildCompleted, CompleteRun, EXPIRE_OP, EventsAppended,
    MAX_REASON_LEN, OutboxMsg, ParentRef, RUN_TIMEOUT_OP, RetryRun, SCHEDULE_TO_START_OP, StartRun,
    TerminalStatus, TerminateRun, TimerFired, derived::Intent, outbox_msg,
};
use ostro_types::journal::{self, EventBody, EventKind};
use prost::Message;

use super::cursor::Cursor;
use super::inputs::journal_input;
use super::outbox::{emit, msg_target};
use super::outcome::{Action, ApplyOutcome, ApplyResult, InvalidCode, RejectReason};
use super::run::{
    PAYLOAD_RESULT, any_oversize, check_intents, enqueue_ready, leave_running, load_run, local_run,
    put_event, remove_ready, start_run, store_run, terminal_status,
};
use super::{terminal, waits};
use crate::store::keys::{children_key, children_prefix, payload_key, timer_key};
use crate::store::values::{RunRecord, RunStatus};
use crate::store::{ApplyBatch, StoreError, Table};

/// Hard-stop reason of a run timeout (D2).
pub(crate) const REASON_RUN_TIMEOUT: &str = "run_timeout";
/// Hard-stop reason of a schedule-to-start timeout (D2).
pub(crate) const REASON_SCHEDULE_TO_START: &str = "schedule_to_start";

const RUN_LEN: usize = 18;

fn reject(r: RejectReason) -> ApplyResult {
    Ok(ApplyOutcome::Rejected(r))
}

fn invalid(c: InvalidCode) -> RejectReason {
    RejectReason::Invalid(c)
}

/// Due time of the run-timeout row of `rec`, if it has one (never after a resume: a resumed
/// run has no run timeout, D1).
fn run_timeout_due(rec: &RunRecord) -> Option<u64> {
    match rec.run_timeout_ms {
        Some(ms) if ms > 0 && rec.resumed_count == 0 => Some(rec.created_at_ms.saturating_add(ms)),
        _ => None,
    }
}

/// Due time of the expiry row of a terminal `rec`, if retention applies. One formula for
/// arming ([`on_terminal`]), disarming ([`retry_run`]) and matching (`retention::on_expire`).
pub(crate) fn expire_due(rec: &RunRecord) -> Option<u64> {
    (rec.retention_ms > 0).then(|| {
        rec.completed_at_ms
            .unwrap_or(0)
            .saturating_add(rec.retention_ms)
    })
}

fn arm(b: &mut ApplyBatch<'_>, run: &RunId, due: u64, op: u64, actions: &mut Vec<Action>) {
    b.insert(Table::Timers, timer_key(due, run, op), Vec::new());
    actions.push(Action::ScheduleTimer {
        due_at_ms: due,
        run: *run,
        op,
    });
}

fn disarm(b: &mut ApplyBatch<'_>, run: &RunId, due: u64, op: u64, actions: &mut Vec<Action>) {
    b.remove(Table::Timers, timer_key(due, run, op));
    actions.push(Action::CancelTimer {
        due_at_ms: due,
        run: *run,
        op,
    });
}

fn status_code(s: RunStatus) -> i32 {
    let t = match s {
        RunStatus::Completed => TerminalStatus::Completed,
        RunStatus::Failed => TerminalStatus::Failed,
        RunStatus::Cancelled => TerminalStatus::Cancelled,
        RunStatus::Terminated => TerminalStatus::Terminated,
        RunStatus::TimedOut => TerminalStatus::TimedOut,
        _ => TerminalStatus::ContinuedAsNew,
    };
    t as i32
}

// ------------------------------------------------------------------ hard stop

/// Graceful `CancelRun` to every child of `run` (one children prefix scan, only when
/// `children > 0`). Local equivalent of the cascade in `inbound::cancel_run`.
fn cascade_cancel(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    run: &RunId,
    rec: &RunRecord,
    reason: &str,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    if rec.children == 0 {
        return Ok(());
    }
    for (k, _) in b.range_prefix(Table::Children, &children_prefix(run))? {
        let child = k
            .get(RUN_LEN..)
            .ok_or_else(|| StoreError::Codec("short children key".into()))?;
        let msg = OutboxMsg {
            kind: Some(outbox_msg::Kind::CancelRun(CancelRun {
                target_run: child.to_vec(),
                reason: reason.to_string(),
            })),
        };
        emit(b, cur, &msg, actions)?;
    }
    Ok(())
}

/// What a hard stop sets: status (`Terminated` / `TimedOut`), reason, time.
struct Stop<'a> {
    status: RunStatus,
    reason: &'a str,
    at_ms: u64,
}

/// Hard stop of a live run (`Terminated` or `TimedOut`): see the module docs. Stores the
/// record.
fn hard_stop(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    run: &RunId,
    mut rec: RunRecord,
    stop: Stop<'_>,
    actions: &mut Vec<Action>,
) -> ApplyResult {
    let Stop {
        status,
        reason,
        at_ms,
    } = stop;
    leave_running(b, run, &rec);
    remove_ready(b, run, &mut rec);
    rec.status = status;
    rec.terminal_reason = reason.to_string();
    rec.completed_at_ms = (at_ms != 0).then_some(at_ms);
    rec.resumable = true;
    if rec.next_seq > 0 {
        // The status is already terminal, so `journal_input` wakes nothing.
        journal_input(
            b,
            run,
            &mut rec,
            at_ms,
            EventBody::RunTerminated(journal::RunTerminated {
                reason: reason.to_string(),
            }),
            actions,
        )?;
    }
    terminal::on_terminal(b, cur, run, &mut rec, at_ms, actions)?;
    actions.push(Action::RunCompleted { run: *run, status });
    if let Some((parent, op)) = rec.parent {
        let msg = OutboxMsg {
            kind: Some(outbox_msg::Kind::ChildCompleted(ChildCompleted {
                parent_run: parent.to_vec(),
                child_run: run.as_bytes().to_vec(),
                op,
                status: status_code(status),
                result: None,
                // A hard stop has no executor-built parent event; the parent side builds
                // its ChildCompleted journal entry from the status alone.
                event: None,
            })),
        };
        emit(b, cur, &msg, actions)?;
    }
    cascade_cancel(b, cur, run, &rec, reason, actions)?;
    store_run(b, run, &rec);
    Ok(ApplyOutcome::Accepted)
}

// --------------------------------------------------------------- TerminateRun

/// `Cmd::TerminateRun` (tag 10, D3): unknown run → `RunNotFound`, terminal run →
/// `RunTerminal`, reason over [`MAX_REASON_LEN`] → `Invalid(FieldTooLong)`; otherwise a hard
/// stop `Terminated` at `terminated_at_ms`. Reads: 1 + (children > 0) + 2·(open_waits > 0).
pub(crate) fn terminate_run(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    t: &TerminateRun,
    actions: &mut Vec<Action>,
) -> ApplyResult {
    let run = match local_run(cur, &t.run_id) {
        Ok(r) => r,
        Err(r) => return reject(r),
    };
    if t.reason.len() > MAX_REASON_LEN {
        return reject(invalid(InvalidCode::FieldTooLong));
    }
    let Some(rec) = load_run(b, &run)? else {
        return reject(RejectReason::RunNotFound);
    };
    if rec.status.is_terminal() {
        return reject(RejectReason::RunTerminal);
    }
    hard_stop(
        b,
        cur,
        &run,
        rec,
        Stop {
            status: RunStatus::Terminated,
            reason: &t.reason,
            at_ms: t.terminated_at_ms,
        },
        actions,
    )
}

// ------------------------------------------------------------------- RetryRun

/// `Cmd::RetryRun` (tag 11, D1): **resume in place**.
///
/// - unknown → `RunNotFound`; `Completed` → `RunCompleted` (409 `run_completed`);
///   non-terminal or non-resumable terminal → `Invalid(BadIntent)` (restart-as-new for a
///   guest terminal event is the server's job, 05-13, chosen from `RunRecord.resumable`).
/// - resumable terminal run: status `Ready` at `requested_at_ms` (ready row +
///   `EnqueueReady`), `terminal_reason` and `completed_at_ms` cleared, `resumable = false`,
///   `resumed_count += 1`, `attempt_failures = 0` (F9: a fresh attempt-failure budget), the
///   expiry row deleted by key. The journal, `current_attempt` (fencing) and
///   `cancel_requested` are kept.
/// - **A resumed run has no run timeout:** no `RUN_TIMEOUT_OP` is armed, and
///   `run_timeout_due` ignores `run_timeout_ms` once `resumed_count > 0`. Schedule-to-start
///   applies to the initial start only, so it is not re-armed either. Stale wait-timeout
///   rows were removed by `terminal::on_terminal`; 05-11 re-arms the unresolved timers and
///   waits from the replayed journal on the first activation after the resume (B2).
///
/// Reads: 1.
pub(crate) fn retry_run(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    r: &RetryRun,
    actions: &mut Vec<Action>,
) -> ApplyResult {
    let run = match local_run(cur, &r.run_id) {
        Ok(r) => r,
        Err(r) => return reject(r),
    };
    let Some(mut rec) = load_run(b, &run)? else {
        return reject(RejectReason::RunNotFound);
    };
    if rec.status == RunStatus::Completed {
        return reject(RejectReason::RunCompleted);
    }
    if !rec.status.is_terminal() || !rec.resumable {
        return reject(invalid(InvalidCode::BadIntent));
    }
    if let Some(due) = expire_due(&rec) {
        disarm(b, &run, due, EXPIRE_OP, actions);
    }
    rec.terminal_reason.clear();
    rec.completed_at_ms = None;
    rec.resumable = false;
    rec.resumed_count = rec.resumed_count.saturating_add(1);
    rec.attempt_failures = 0;
    enqueue_ready(b, &run, &mut rec, r.requested_at_ms, actions);
    store_run(b, &run, &rec);
    Ok(ApplyOutcome::Accepted)
}

// ------------------------------------------------------------- start / grant

/// Inbound outbox `StartRun` (tag 6): the continue-as-new successor. Starts it with
/// `start_run` and no idempotency row (the idempotency key belongs to the chain's first
/// run); a resend hits the run-id check and the `(src, seq)` watermark of `InboundBatch`
/// makes delivery exactly-once anyway. `continued_from` and `parent` are stored as sent
/// (`complete_continued_as_new` filled them in). The successor gets a **fresh** run-timeout
/// deadline (`requested_at_ms + run_timeout_ms` of its own StartRun) and its own retention.
pub(crate) fn inbound_start_run(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    s: &StartRun,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    // Rejections (malformed or misrouted successor) are deterministic per-message drops.
    start_run(b, cur, s, None, actions)?;
    Ok(())
}

/// After `StartRun` built the record (before it is stored): arms `RUN_TIMEOUT_OP` and
/// `SCHEDULE_TO_START_OP` (setting `s2s_due_ms`). Blind writes only.
///
/// The schedule-to-start clock starts at `max(ready_at_ms, requested_at_ms)`: a start with
/// `ready_at_ms` in the past (or 0) is ready at request time.
pub(crate) fn on_start(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &mut RunRecord,
    s: &StartRun,
    actions: &mut Vec<Action>,
) {
    if let Some(due) = run_timeout_due(rec) {
        arm(b, run, due, RUN_TIMEOUT_OP, actions);
    }
    if s.schedule_to_start_ms > 0 {
        let due = s
            .ready_at_ms
            .max(s.requested_at_ms)
            .saturating_add(s.schedule_to_start_ms);
        rec.s2s_due_ms = due;
        arm(b, run, due, SCHEDULE_TO_START_OP, actions);
    }
}

/// After `AttemptGranted` accepted a grant (before the record is stored): the first grant
/// (`s2s_due_ms != 0` only until then) deletes the schedule-to-start row by its stored due
/// time and zeroes `s2s_due_ms`. Blind writes only.
pub(crate) fn on_attempt_granted(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &mut RunRecord,
    _g: &AttemptGranted,
    actions: &mut Vec<Action>,
) {
    if rec.s2s_due_ms != 0 {
        disarm(b, run, rec.s2s_due_ms, SCHEDULE_TO_START_OP, actions);
        rec.s2s_due_ms = 0;
    }
}

/// Called by `terminal::on_terminal` after the waits cleanup, for every terminal
/// transition: deletes the run-timeout row (by `created_at_ms + run_timeout_ms`) and the
/// schedule-to-start row (by `s2s_due_ms`), then arms `EXPIRE_OP` at
/// `completed_at_ms + retention_ms` when `retention_ms > 0`. Blind writes only.
pub(crate) fn on_terminal(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &mut RunRecord,
    _at_ms: u64,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    if let Some(due) = run_timeout_due(rec) {
        disarm(b, run, due, RUN_TIMEOUT_OP, actions);
    }
    if rec.s2s_due_ms != 0 {
        disarm(b, run, rec.s2s_due_ms, SCHEDULE_TO_START_OP, actions);
        rec.s2s_due_ms = 0;
    }
    if let Some(due) = expire_due(rec) {
        arm(b, run, due, EXPIRE_OP, actions);
    }
    Ok(())
}

/// `TimerFired` for `RUN_TIMEOUT_OP` or `SCHEDULE_TO_START_OP`. `timer.rs` already removed the
/// timer row and read the record (`None` = orphan row, accepted).
///
/// - `RUN_TIMEOUT_OP` on a live run whose deadline equals `due_at_ms` → hard stop
///   `TimedOut` (`run_timeout`) at `fired_at_ms`.
/// - `SCHEDULE_TO_START_OP` on a live, never-granted run (`s2s_due_ms == due_at_ms`) → hard
///   stop `TimedOut` (`schedule_to_start`).
/// - Anything else (terminal, granted, resumed, stale due time): the row removal is the
///   whole effect.
pub(crate) fn on_reserved_timer(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    run: &RunId,
    rec: Option<RunRecord>,
    t: &TimerFired,
    actions: &mut Vec<Action>,
) -> ApplyResult {
    let Some(rec) = rec else {
        return Ok(ApplyOutcome::Accepted);
    };
    if rec.status.is_terminal() {
        return Ok(ApplyOutcome::Accepted);
    }
    let reason = match t.op {
        RUN_TIMEOUT_OP if run_timeout_due(&rec) == Some(t.due_at_ms) => REASON_RUN_TIMEOUT,
        SCHEDULE_TO_START_OP if rec.s2s_due_ms != 0 && rec.s2s_due_ms == t.due_at_ms => {
            REASON_SCHEDULE_TO_START
        }
        _ => return Ok(ApplyOutcome::Accepted),
    };
    hard_stop(
        b,
        cur,
        run,
        rec,
        Stop {
            status: RunStatus::TimedOut,
            reason,
            at_ms: t.fired_at_ms,
        },
        actions,
    )
}

// ------------------------------------------------------------- continue-as-new

/// The successor `StartRun` of a continue-as-new batch, completed with lineage: its
/// `continued_from` is set to `run` (or must already be `run`) and, when it names no parent,
/// it inherits the predecessor's (PF-F2; a different parent is rejected).
fn successor_start(run: &RunId, rec: &RunRecord, s: &StartRun) -> Result<StartRun, RejectReason> {
    let succ = RunId::from_bytes(&s.run_id).map_err(|_| invalid(InvalidCode::BadRunId))?;
    if succ == *run {
        return Err(invalid(InvalidCode::BadIntent));
    }
    if !s.continued_from.is_empty() && s.continued_from != run.as_bytes() {
        return Err(invalid(InvalidCode::BadIntent));
    }
    let inherited = rec.parent.map(|(p, op)| ParentRef {
        run_id: p.to_vec(),
        op,
    });
    match (&s.parent, &inherited) {
        (Some(sent), Some(own)) if sent != own => return Err(invalid(InvalidCode::BadIntent)),
        (Some(_), None) => return Err(invalid(InvalidCode::BadIntent)),
        _ => {}
    }
    let mut out = s.clone();
    out.continued_from = run.as_bytes().to_vec();
    out.parent = inherited;
    Ok(out)
}

/// Validation of a continue-as-new batch. Returns the completed successor start.
///
/// Shape: events, then any `SendMessage` (exactly one of them `StartRun`, the successor),
/// `ScheduleTimer` and `CancelTimer`, then `CompleteRun{ContinuedAsNew}` last. `EnqueueReady`,
/// `Suspend`, `RegisterWait`, `CancelWait` and a second `CompleteRun` make no sense in a
/// terminal batch → `BadIntent`. Every other intent goes through `run::check_intents`.
fn check_can(
    cur: &Cursor,
    e: &EventsAppended,
    run: &RunId,
    rec: &RunRecord,
) -> Result<(StartRun, CompleteRun), RejectReason> {
    let (last, rest) = e
        .derived
        .split_last()
        .ok_or(invalid(InvalidCode::BadIntent))?;
    let Some(Intent::CompleteRun(c)) = &last.intent else {
        return Err(invalid(InvalidCode::BadIntent));
    };
    let mut successor = None;
    let mut others = Vec::with_capacity(rest.len());
    for d in rest {
        match &d.intent {
            Some(Intent::SendMessage(m)) => {
                if let Some(OutboxMsg {
                    kind: Some(outbox_msg::Kind::StartRun(s)),
                }) = &m.msg
                {
                    if successor.is_some() {
                        return Err(invalid(InvalidCode::BadIntent));
                    }
                    let msg = m.msg.as_ref().ok_or(invalid(InvalidCode::MissingField))?;
                    msg_target(cur, msg)?;
                    successor = Some(successor_start(run, rec, s)?);
                    continue;
                }
                others.push(d.clone());
            }
            Some(
                Intent::EnqueueReady(_)
                | Intent::Suspend(_)
                | Intent::RegisterWait(_)
                | Intent::CancelWait(_)
                | Intent::CompleteRun(_),
            ) => return Err(invalid(InvalidCode::BadIntent)),
            _ => others.push(d.clone()),
        }
    }
    let successor = successor.ok_or(invalid(InvalidCode::BadIntent))?;
    // Structural checks of the remaining intents (reserved ops, targets, ...), on a copy
    // without events (check_intents looks at the intents only).
    let shell = EventsAppended {
        run_id: Vec::new(),
        token: None,
        from_seq: e.from_seq,
        events: Vec::new(),
        derived: others,
    };
    check_intents(cur, &shell, rec.status)?;
    Ok((successor, c.clone()))
}

/// An `EventsAppended` whose last intent is `CompleteRun{ContinuedAsNew}` (D10, PF-F2).
/// `run.rs` already checked the token, `from_seq` and event contiguity.
///
/// Validates the whole batch first ([`check_can`], `run::any_oversize`), then writes: the
/// events (`status_seq` follows the last `StatusPublished` like the ordinary path, 05-22
/// F10; the seq-0 hook runs like there), the intents, the successor `StartRun` into the
/// outbox (lineage filled in), `continued_to`, status `ContinuedAsNew` (not resumable), the
/// result payload, `terminal::on_terminal` and `Action::RunCompleted{ContinuedAsNew}`.
/// **No `ChildCompleted` is sent to a parent**: the successor inherits the parent link and
/// reports to it when the chain ends. Reads: none beyond the record read of `run.rs`.
pub(crate) fn complete_continued_as_new(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    e: &EventsAppended,
    run: &RunId,
    mut rec: RunRecord,
    actions: &mut Vec<Action>,
) -> ApplyResult {
    let (successor, c) = match check_can(cur, e, run, &rec) {
        Ok(x) => x,
        Err(r) => return reject(r),
    };
    if any_oversize(e) {
        return reject(RejectReason::PayloadTooLarge);
    }
    let succ_id = RunId::from_bytes(&successor.run_id)
        .map_err(|e| StoreError::Codec(format!("validated successor run id: {e}")))?;

    // All checks passed: write.
    let status_kind = EventKind::StatusPublished.code();
    for ev in &e.events {
        put_event(b, run, ev);
        if ev.kind == status_kind {
            rec.status_seq = Some(ev.seq);
        }
    }
    let n = e.events.len() as u64;
    rec.next_seq = e.from_seq.saturating_add(n);
    if e.from_seq == 0 && n > 0 {
        waits::on_first_events(b, run, &mut rec, actions)?;
    }
    for d in &e.derived {
        let Some(i) = &d.intent else { continue };
        match i {
            Intent::ScheduleTimer(st) => arm(b, run, st.due_at_ms, st.op, actions),
            Intent::CancelTimer(ct) => disarm(b, run, ct.due_at_ms, ct.op, actions),
            Intent::SendMessage(m) => {
                let Some(msg) = &m.msg else { continue };
                match &msg.kind {
                    Some(outbox_msg::Kind::StartRun(_)) => {
                        emit(b, cur, &OutboxMsg::start_run(successor.clone()), actions)?;
                    }
                    Some(outbox_msg::Kind::StartChild(ch)) => {
                        let child_bytes = ch.start.as_ref().map_or(&[][..], |s| &s.run_id);
                        let child = RunId::from_bytes(child_bytes)
                            .map_err(|e| StoreError::Codec(format!("child run id: {e}")))?;
                        b.insert(Table::Children, children_key(run, &child), Vec::new());
                        rec.children = rec.children.saturating_add(1);
                        emit(b, cur, msg, actions)?;
                    }
                    _ => emit(b, cur, msg, actions)?,
                }
            }
            // 09-05 opt-in marker (validated by check_intents through check_can).
            Intent::ChildJournal(j) => super::run::mark_child_journal(b, run, j)?,
            // Rejected by check_can / handled below.
            _ => {}
        }
    }
    let status = terminal_status(c.status)
        .ok_or_else(|| StoreError::Codec("validated terminal status".into()))?;
    leave_running(b, run, &rec);
    remove_ready(b, run, &mut rec);
    rec.status = status;
    rec.completed_at_ms = (c.completed_at_ms != 0).then_some(c.completed_at_ms);
    rec.resumable = false;
    rec.terminal_reason.clear();
    rec.continued_to = Some(succ_id);
    if let Some(res) = &c.result {
        b.insert(
            Table::Payloads,
            payload_key(run, PAYLOAD_RESULT),
            res.encode_to_vec(),
        );
    }
    terminal::on_terminal(b, cur, run, &mut rec, c.completed_at_ms, actions)?;
    actions.push(Action::RunCompleted { run: *run, status });
    store_run(b, run, &rec);
    Ok(ApplyOutcome::Accepted)
}
