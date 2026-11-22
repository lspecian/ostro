//! Run lifecycle apply: `StartRun`, `AttemptGranted`, `EventsAppended` with
//! `Derived` intents (STOR-05, STOR-10, STOR-14; Phase 4 additions: plan 04-12).
//!
//! Every handler validates completely before its first write, so a rejected
//! command leaves nothing in the overlay. Hot-path handlers issue at most one
//! keyspace point read (S3 RMW margin 1.19x): the run record (or the dedupe
//! row for a keyed start). All other writes are blind.
//!
//! **`running` index (04-12):** a row exists exactly while the run's status is
//! `Running`. `AttemptGranted` inserts (or overwrites) it with the new token;
//! every transition out of `Running` (`EnqueueReady`, `Suspend`, `CompleteRun`)
//! blind-removes it. Only `EventsAppended` moves a run out of `Running`
//! (inbound messages and timers only wake `Scheduled` runs). No reads added.
//!
//! **`Suspend` (04-12):** must be the last intent, cannot be combined with
//! `EnqueueReady` or `CompleteRun`, and needs status `Running`. It sets the
//! status to `Scheduled` (the token stays, for fencing) and removes the ready
//! and running rows. The `from_seq == next_seq` check that every
//! `EventsAppended` already passes is the compare-and-set: an input journaled
//! after the executor's last flush moves `next_seq` (04-13), so the Suspend is
//! rejected `SeqMismatch` and the executor re-activates.
//!
//! **Plan 05-22 (pre-freeze):**
//! - Ready rows and `Action::EnqueueReady` carry the run's workflow, tenant and
//!   first-activation flag (`current_attempt.is_none()`), so a dispatcher rebuild needs no
//!   run-record reads (Phase 7 F3/F7).
//! - `AttemptGranted{after_failure}` increments `RunRecord.attempt_failures` (F9).
//! - `EventsAppended` points `RunRecord.status_seq` at the batch's last `StatusPublished`
//!   event, read from `JournalEvent.kind` only (F10).
//! - `CompleteRun.parent_event` is forwarded verbatim into the outbox `ChildCompleted.event`
//!   (Phase 9 PF-F1, D21; never decoded).
//!
//! **Plan 05-16: `StartRun.run_started` (tag 23).** When set, the submitter's pre-encoded
//! `RunStarted` is journaled verbatim at seq 0 (opaque like `EventsAppended` events: only
//! `seq == 0`, `kind == RunStarted`, presence and the inline size limit are checked) and
//! `next_seq` starts at 1; the B1 first-events hook runs right after it (free for a fresh
//! run, `inbox_next == 0`). Signals and cancels sent before the first grant are then
//! journaled directly (`next_seq > 0`); the executor skips its own RunStarted append. The
//! first-activation flag stays `current_attempt.is_none()`. No extra read.

use ostro_types::command::{
    AttemptGranted, CHILD_ROW_JOURNAL, ChildCompleted, ChildJournal, ConflictPolicy,
    EventsAppended, JournalEvent, MAX_RUN_TAG_LEN, MAX_RUN_TAGS, MAX_TENANT_LEN,
    MAX_TRACEPARENT_LEN, MODULE_SHA_LEN, OutboxMsg, StartRun, TerminalStatus, derived::Intent,
    is_reserved_op, outbox_msg,
};
use ostro_types::journal::EventKind;
use ostro_types::{AttemptToken, Epoch, PayloadRef, RunId};
use prost::Message;

use super::START_DELAY_OP;
use super::cursor::Cursor;
use super::outbox::{emit, msg_oversize, msg_target};
use super::outcome::{Action, ApplyOutcome, InvalidCode, RejectReason};
use super::{effects, lifecycle, terminal, waits};
use crate::store::keys::{
    children_key, dedupe_key, event_key, payload_key, ready_key, run_key, running_key, timer_key,
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

/// Writes one journal row: key `run ‖ seq`, value the prost [`JournalEvent`]
/// (the format `query::journal_rows` decodes). Blind write.
pub(crate) fn put_event(b: &mut ApplyBatch<'_>, run: &RunId, ev: &JournalEvent) {
    b.insert(Table::Events, event_key(run, ev.seq), ev.encode_to_vec());
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
        TerminalStatus::Terminated => Some(RunStatus::Terminated),
        TerminalStatus::TimedOut => Some(RunStatus::TimedOut),
        TerminalStatus::ContinuedAsNew => Some(RunStatus::ContinuedAsNew),
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

/// Blind-removes the run's `running` row if the run is `Running`. Call before
/// changing the status away from `Running`.
pub(crate) fn leave_running(b: &mut ApplyBatch<'_>, run: &RunId, rec: &RunRecord) {
    if rec.status == RunStatus::Running {
        b.remove(Table::Running, running_key(run));
    }
}

/// Inserts the ready row, marks the run Ready and emits `EnqueueReady`. The row and the
/// action carry workflow, tenant and `first = current_attempt.is_none()` (F3/F7; not
/// `next_seq == 0`, which stops holding once StartRun writes RunStarted, 05-16).
pub(crate) fn enqueue_ready(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &mut RunRecord,
    ready_at_ms: u64,
    actions: &mut Vec<Action>,
) {
    let first = rec.current_attempt.is_none();
    leave_running(b, run, rec);
    remove_ready(b, run, rec);
    if rec.pause.is_some() {
        // Phase 7 (07-04) pause gate, the single choke point: a paused run may become Ready,
        // but gets no ready row and no EnqueueReady until ResumeRun re-enqueues it.
        rec.status = RunStatus::Ready;
        return;
    }
    b.insert(
        Table::Ready,
        ready_key(&rec.queue, rec.priority, ready_at_ms, run),
        values::encode(&ReadyValue {
            concurrency_key: rec.concurrency_key.clone(),
            workflow: rec.workflow.clone(),
            tenant: rec.tenant.clone(),
            first,
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
        workflow: rec.workflow.clone(),
        tenant: rec.tenant.clone(),
        first_activation: first,
    });
}

// ------------------------------------------------------------------ StartRun

/// Validated Phase 4/5 StartRun fields (14..22), as stored in [`RunRecord`].
struct StartExtras {
    module_sha: Option<[u8; 32]>,
    tenant: Option<String>,
    traceparent: Option<String>,
    run_timeout_ms: Option<u64>,
    retry_of: Option<RunId>,
    continued_from: Option<RunId>,
}

/// An optional run id field: empty = none, else exactly 18 valid bytes.
fn opt_run_id(b: &[u8]) -> Check<Option<RunId>> {
    if b.is_empty() {
        return Ok(None);
    }
    RunId::from_bytes(b)
        .map(Some)
        .map_err(|_| invalid(InvalidCode::BadRunId))
}

/// Checks fields 14..22 against the `ostro_types::command` limits. No reads.
fn check_start_extras(s: &StartRun) -> Check<StartExtras> {
    let module_sha = match s.module_sha.len() {
        0 => None,
        MODULE_SHA_LEN => {
            let mut a = [0u8; 32];
            a.copy_from_slice(&s.module_sha);
            Some(a)
        }
        _ => return Err(invalid(InvalidCode::BadLength)),
    };
    if s.tags.len() > MAX_RUN_TAGS
        || s.tags.iter().any(|t| t.len() > MAX_RUN_TAG_LEN)
        || s.tenant.len() > MAX_TENANT_LEN
        || s.traceparent.len() > MAX_TRACEPARENT_LEN
    {
        return Err(invalid(InvalidCode::FieldTooLong));
    }
    let retry_of = opt_run_id(&s.retry_of)?;
    let continued_from = opt_run_id(&s.continued_from)?;
    let non_empty = |v: &String| (!v.is_empty()).then(|| v.clone());
    Ok(StartExtras {
        module_sha,
        tenant: non_empty(&s.tenant),
        traceparent: non_empty(&s.traceparent),
        run_timeout_ms: (s.run_timeout_ms != 0).then_some(s.run_timeout_ms),
        retry_of,
        continued_from,
    })
}

/// Checks an optional pre-encoded `RunStarted` (`StartRun.run_started`, 05-16) without
/// decoding its payload.
fn check_run_started(ev: &JournalEvent) -> Check<()> {
    if ev.seq != 0 {
        return Err(invalid(InvalidCode::EventsNotContiguous));
    }
    if ev.kind != EventKind::RunStarted.code() {
        return Err(invalid(InvalidCode::BadIntent));
    }
    if ev.payload.is_none() {
        return Err(invalid(InvalidCode::MissingField));
    }
    if oversize(&ev.payload) {
        return Err(RejectReason::PayloadTooLarge);
    }
    Ok(())
}

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
    let extras = match check_start_extras(s) {
        Ok(x) => x,
        Err(r) => return reject(r),
    };
    if oversize(&s.input) {
        return reject(RejectReason::PayloadTooLarge);
    }
    if let Some(ev) = &s.run_started
        && let Err(r) = check_run_started(ev)
    {
        return reject(r);
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
        module_sha: extras.module_sha,
        tags: s.tags.clone(),
        tenant: extras.tenant,
        traceparent: extras.traceparent,
        run_timeout_ms: extras.run_timeout_ms,
        completed_at_ms: None,
        schedule_to_start_ms: s.schedule_to_start_ms,
        retention_ms: s.retention_ms,
        idempotency_key: s.idempotency_key.clone(),
        terminal_reason: String::new(),
        resumable: false,
        retry_of: extras.retry_of,
        continued_from: extras.continued_from,
        cancel_reason: String::new(),
        resumed_count: 0,
        open_waits: 0,
        s2s_due_ms: 0,
        attempt_failures: 0,
        status_seq: None,
        continued_to: None,
        pause: None,
    };
    if let Some(input) = &s.input {
        b.insert(
            Table::Payloads,
            payload_key(&run, PAYLOAD_INPUT),
            input.encode_to_vec(),
        );
    }
    if let Some(ev) = &s.run_started {
        // Validated above: seq 0, kind RunStarted, inline <= 256 KiB or a blob ref.
        put_event(b, &run, ev);
        rec.next_seq = 1;
        // B1 (05-08): pending signals follow the seq-0 events. A fresh run has none
        // (`inbox_next == 0`, no read), but the hook stays the single place for it.
        waits::on_first_events(b, &run, &mut rec, actions)?;
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
    lifecycle::on_start(b, &run, &mut rec, s, actions);
    super::index::on_start(b, &run, &rec);
    store_run(b, &run, &rec);
    Ok(ApplyOutcome::Accepted)
}

// ------------------------------------------------------------ AttemptGranted

pub(crate) fn apply_attempt_granted(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    g: &AttemptGranted,
    actions: &mut Vec<Action>,
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
    if rec.pause.is_some() {
        // Phase 7 (07-04): no grant for a paused run.
        return reject(RejectReason::RunPaused);
    }
    rec.current_attempt = Some((t.epoch.0, t.n));
    rec.status = RunStatus::Running;
    if g.after_failure {
        // F9: durable count of attempts granted after a retryable failure (05-11 reads it,
        // RetryRun resets it in 05-09). Same batch as the grant.
        rec.attempt_failures = rec.attempt_failures.saturating_add(1);
    }
    remove_ready(b, &run, &mut rec);
    // Blind insert (overwrites the previous attempt's row): no extra read.
    b.insert(
        Table::Running,
        running_key(&run),
        values::encode(&(t.epoch.0, t.n)),
    );
    lifecycle::on_attempt_granted(b, &run, &mut rec, g, actions);
    store_run(b, &run, &rec);
    Ok(ApplyOutcome::Accepted)
}

// ------------------------------------------------------------ EventsAppended

/// Validates the Derived intents' structure; returns nothing on success.
///
/// `CompleteRun` and `Suspend` must be last; `Suspend` cannot be combined with
/// `EnqueueReady` or `CompleteRun` and needs a `Running` run. `pub(crate)` for
/// `lifecycle::complete_continued_as_new` (05-09).
pub(crate) fn check_intents(cur: &Cursor, e: &EventsAppended, status: RunStatus) -> Check<()> {
    let mut completed = false;
    let mut suspended = false;
    let mut enqueued = false;
    // Child run id of the directly preceding `SendMessage(StartChild)` (09-05 ChildJournal).
    let mut prev_child: Option<&[u8]> = None;
    for d in &e.derived {
        let Some(i) = &d.intent else {
            return Err(invalid(InvalidCode::MissingField));
        };
        if completed || suspended {
            return Err(invalid(InvalidCode::BadIntent));
        }
        let prev = prev_child.take();
        match i {
            // 09-05: the opt-in marker must directly follow its child's StartChild.
            Intent::ChildJournal(j) => {
                if prev != Some(j.child_run.as_slice()) {
                    return Err(invalid(InvalidCode::BadIntent));
                }
            }
            // Apply-owned reserved timer ops (START_DELAY_OP, RUN_TIMEOUT_OP, ...) are never
            // guest ops (plan 05-01).
            Intent::ScheduleTimer(t) if is_reserved_op(t.op) => {
                return Err(invalid(InvalidCode::BadIntent));
            }
            Intent::CancelTimer(t) if is_reserved_op(t.op) => {
                return Err(invalid(InvalidCode::BadIntent));
            }
            // Phase 5 seams: validated by `waits.rs` (05-08; stubs reject until then).
            Intent::RegisterWait(w) => waits::check_register_wait(cur, w)?,
            Intent::CancelWait(w) => waits::check_cancel_wait(cur, w)?,
            // Phase 6 (06-03): `effects.rs`.
            Intent::RegisterEffect(r) => effects::check_register_effect(r)?,
            Intent::CancelEffect(_) => {}
            Intent::ScheduleTimer(_) | Intent::CancelTimer(_) => {}
            Intent::EnqueueReady(_) => enqueued = true,
            Intent::SendMessage(m) => {
                let msg = m.msg.as_ref().ok_or(invalid(InvalidCode::MissingField))?;
                // The continue-as-new successor start belongs to batches ending in
                // CompleteRun{ContinuedAsNew}, which `lifecycle::complete_continued_as_new`
                // (05-09) handles whole. Anywhere else it is rejected.
                if matches!(msg.kind, Some(outbox_msg::Kind::StartRun(_))) {
                    return Err(invalid(InvalidCode::BadIntent));
                }
                msg_target(cur, msg)?;
                // A detached child has no row to mark (09-08).
                if let Some(outbox_msg::Kind::StartChild(c)) = &msg.kind {
                    prev_child = c
                        .start
                        .as_ref()
                        .filter(|s| s.parent.is_some())
                        .map(|s| s.run_id.as_slice());
                }
            }
            Intent::CompleteRun(c) => {
                match terminal_status(c.status) {
                    Some(RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled) => {}
                    // Terminated/TimedOut are set by apply-side paths only (05-09); executors
                    // must never send them. ContinuedAsNew is valid only as the last intent,
                    // where `apply_events_appended` routes the whole command to 05-09.
                    _ => return Err(invalid(InvalidCode::BadIntent)),
                }
                completed = true;
            }
            Intent::Suspend(_) => {
                if enqueued {
                    return Err(invalid(InvalidCode::BadIntent));
                }
                suspended = true;
            }
        }
    }
    if suspended && status != RunStatus::Running {
        return Err(invalid(InvalidCode::NotRunning));
    }
    Ok(())
}

/// Any inline payload over 256 KiB in the events or intents (`CompleteRun.parent_event`
/// included, W5). `pub(crate)` for `lifecycle::complete_continued_as_new` (05-09).
pub(crate) fn any_oversize(e: &EventsAppended) -> bool {
    e.events.iter().any(|ev| oversize(&ev.payload))
        || e.derived.iter().any(|d| match &d.intent {
            Some(Intent::SendMessage(m)) => m.msg.as_ref().is_some_and(msg_oversize),
            Some(Intent::CompleteRun(c)) => oversize(&c.result) || oversize(&c.parent_event),
            _ => false,
        })
}

/// Whether the batch ends with `CompleteRun{ContinuedAsNew}` (routed to 05-09's
/// `lifecycle::complete_continued_as_new`).
fn ends_continued_as_new(e: &EventsAppended) -> bool {
    matches!(
        e.derived.last().and_then(|d| d.intent.as_ref()),
        Some(Intent::CompleteRun(c)) if terminal_status(c.status) == Some(RunStatus::ContinuedAsNew)
    )
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
    if rec.pause.is_some() {
        // Phase 7 (07-04): after fencing (a stale zombie still sees Fenced), before anything
        // else: nothing in the batch is applied while the run is paused.
        return reject(RejectReason::RunPaused);
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
    if ends_continued_as_new(e) {
        return lifecycle::complete_continued_as_new(b, cur, e, &run, rec, actions);
    }
    if let Err(r) = check_intents(cur, e, rec.status) {
        return reject(r);
    }
    if any_oversize(e) {
        return reject(RejectReason::PayloadTooLarge);
    }

    // All checks passed: write.
    let status_kind = EventKind::StatusPublished.code();
    for ev in &e.events {
        put_event(b, &run, ev);
        if ev.kind == status_kind {
            // F10: O(1) published-status pointer, from the kind code only (no decode).
            rec.status_seq = Some(ev.seq);
        }
    }
    rec.next_seq = new_next;
    if e.from_seq == 0 && n > 0 {
        // B1: this batch wrote seq 0. Pending signals are journaled after its events and
        // before its intents (05-08).
        waits::on_first_events(b, &run, &mut rec, actions)?;
    }
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
                    // 09-08: a detached child (no ParentRef) gets no children row: it is
                    // never cancelled with the parent and never reports to it (D-P9-10).
                    if c.start.as_ref().is_some_and(|s| s.parent.is_some()) {
                        b.insert(Table::Children, children_key(&run, &child), Vec::new());
                        rec.children = rec.children.saturating_add(1);
                    }
                }
                emit(b, cur, msg, actions)?;
            }
            Intent::EnqueueReady(er) => {
                enqueue_ready(b, &run, &mut rec, er.ready_at_ms, actions);
            }
            Intent::Suspend(_) => {
                // Validated: last intent, no EnqueueReady/CompleteRun, status Running.
                // Blind writes only (no read).
                leave_running(b, &run, &rec);
                remove_ready(b, &run, &mut rec);
                rec.status = RunStatus::Scheduled;
            }
            Intent::RegisterWait(w) => waits::register_wait(b, &run, &mut rec, w, actions)?,
            Intent::CancelWait(w) => waits::cancel_wait(b, &run, &mut rec, w, actions)?,
            // Phase 6 (06-03): blind effects-row writes, no reads.
            Intent::RegisterEffect(r) => effects::register_effect(b, &run, &rec, r),
            Intent::CancelEffect(c) => effects::cancel_effect(b, &run, c),
            Intent::ChildJournal(j) => mark_child_journal(b, &run, j)?,
            Intent::CompleteRun(c) => {
                let status = terminal_status(c.status)
                    .ok_or_else(|| StoreError::Codec("validated terminal status".into()))?;
                leave_running(b, &run, &rec);
                rec.status = status;
                rec.completed_at_ms = (c.completed_at_ms != 0).then_some(c.completed_at_ms);
                rec.resumable = c.resumable;
                // Executor completions carry no reason; clear one left by an earlier
                // hard stop of a resumed run.
                rec.terminal_reason.clear();
                if let Some(res) = &c.result {
                    b.insert(
                        Table::Payloads,
                        payload_key(&run, PAYLOAD_RESULT),
                        res.encode_to_vec(),
                    );
                }
                remove_ready(b, &run, &mut rec);
                terminal::on_terminal(b, cur, &run, &mut rec, c.completed_at_ms, actions)?;
                actions.push(Action::RunCompleted { run, status });
                if let Some((parent, op)) = rec.parent {
                    let msg = OutboxMsg {
                        kind: Some(outbox_msg::Kind::ChildCompleted(ChildCompleted {
                            parent_run: parent.to_vec(),
                            child_run: run.as_bytes().to_vec(),
                            op,
                            status: c.status,
                            result: c.result.clone(),
                            // PF-F1 / D21: the submitter-encoded ChildCompleted event bytes,
                            // forwarded verbatim (inline <= 256 KiB, checked by
                            // `any_oversize`, or a blob ref). Never decoded or fetched.
                            event: c.parent_event.clone(),
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

/// 09-05 opt-in marker: the `children` row of `j.child_run` (inserted by the directly
/// preceding `SendMessage(StartChild)`, `check_intents`) gets the journaling value. Blind
/// write; `rec.children` was already counted by the StartChild.
pub(crate) fn mark_child_journal(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    j: &ChildJournal,
) -> Result<(), StoreError> {
    let child = RunId::from_bytes(&j.child_run)
        .map_err(|e| StoreError::Codec(format!("validated child run id: {e}")))?;
    b.insert(
        Table::Children,
        children_key(run, &child),
        CHILD_ROW_JOURNAL.to_vec(),
    );
    Ok(())
}

#[cfg(test)]
mod seam_tests {
    use bytes::Bytes;
    use ostro_types::command::{
        AttemptGranted, AttemptTokenMsg, EventsAppended, JournalEvent, StartRun, encode_command,
    };
    use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};

    use crate::apply::{ApplyOutcome, Cursor, apply_command};
    use crate::store::{PartitionStore, StoreConfig};

    struct H {
        _dir: tempfile::TempDir,
        store: PartitionStore,
        cur: Cursor,
    }

    impl H {
        fn apply(&mut self, c: &Command) -> ApplyOutcome {
            let mut b = self.store.batch();
            let mut actions = Vec::new();
            let off = self.cur.next;
            let out = apply_command(
                &mut b,
                &mut self.cur,
                off,
                Epoch(1),
                &encode_command(c),
                &mut actions,
            )
            .expect("apply");
            let mu = self.cur.meta_update();
            b.commit(&mu).expect("commit");
            out
        }
    }

    fn rid(n: u8) -> RunId {
        let mut b = [0u8; 18];
        b[17] = n;
        RunId::from_bytes(&b).expect("run id")
    }

    fn append(r: &RunId, from: u64, n: u64) -> Command {
        Command::events_appended(EventsAppended {
            run_id: r.as_bytes().to_vec(),
            token: Some(AttemptTokenMsg { epoch: 1, n: 1 }),
            from_seq: from,
            events: (from..from + n)
                .map(|seq| JournalEvent {
                    seq,
                    kind: 16,
                    payload: Some(PayloadRef::inline(Bytes::from_static(b"e"))),
                    ..Default::default()
                })
                .collect(),
            derived: vec![],
        })
    }

    fn calls() -> u32 {
        super::waits::FIRST_EVENTS_CALLS.with(std::cell::Cell::get)
    }

    #[test]
    fn first_events_hook_runs_once_per_run_at_the_seq0_batch() {
        let dir = ostro_testkit::fs::tempdir();
        let (store, meta) = PartitionStore::open(
            dir.path(),
            PartitionId(0),
            PartitionCount::new(4).expect("non-zero"),
            &StoreConfig::default(),
        )
        .expect("open");
        let cur = Cursor::from_meta(&meta);
        let mut h = H {
            _dir: dir,
            store,
            cur,
        };
        let ok = ApplyOutcome::Accepted;
        assert_eq!(h.apply(&Command::leader_announced(1, 1, 0)), ok);
        let base = calls();
        for n in [1u8, 2] {
            let r = rid(n);
            let start = StartRun {
                run_id: r.as_bytes().to_vec(),
                workflow: "wf".into(),
                queue: "q".into(),
                ..Default::default()
            };
            assert_eq!(h.apply(&Command::start_run(start)), ok);
            let grant = AttemptGranted {
                run_id: r.as_bytes().to_vec(),
                token: Some(AttemptTokenMsg { epoch: 1, n: 1 }),
                after_failure: false,
            };
            assert_eq!(h.apply(&Command::attempt_granted(grant)), ok);
        }
        let (r1, r2) = (rid(1), rid(2));
        // An empty batch at from_seq 0 writes no seq 0: no call.
        assert_eq!(h.apply(&append(&r1, 0, 0)), ok);
        assert_eq!(calls(), base);
        // A rejected batch at from_seq 0 (seq mismatch elsewhere) never calls either.
        assert!(!h.apply(&append(&r1, 5, 1)).is_accepted());
        assert_eq!(calls(), base);
        // The seq-0 batch calls exactly once; later batches never do.
        assert_eq!(h.apply(&append(&r1, 0, 2)), ok);
        assert_eq!(calls(), base + 1);
        assert_eq!(h.apply(&append(&r1, 2, 1)), ok);
        assert_eq!(h.apply(&append(&r1, 3, 0)), ok);
        assert_eq!(calls(), base + 1);
        // Once per run.
        assert_eq!(h.apply(&append(&r2, 0, 1)), ok);
        assert_eq!(calls(), base + 2);
    }
}
