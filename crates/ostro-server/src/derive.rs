//! Journal events -> Phase 2 [`Derived`] storage intents (04-RESEARCH "Mapping journal
//! events to Derived intents", extended for Phase 5 in plan 05-11).
//!
//! # API shape (two phases, chosen in plan 04-15)
//!
//! [`derive`] is synchronous and pure: no I/O, no clock, no randomness. The parts that need
//! I/O are resolved by the caller ([`crate::sink::PartitionSink`]) before calling it:
//!
//! 1. The bytes of the run's result when the batch ends the run: [`terminal_result_bytes`]
//!    (`RunCompleted` output data, the failure JSON of `RunFailed`) or
//!    [`continued_as_new_json`] (`{"next_run_id"}` of a continue-as-new). The caller turns
//!    them into a [`PayloadRef`] with `PartitionHandle::prepare_payload`, which writes
//!    payloads over 256 KiB to the blob store **before** the append.
//! 2. `OpCancelled{op}` needs the event that started `op`. If it is not in the batch, the
//!    caller reads it from the journal and serves it through the `prior` lookup.
//! 3. `RunContinuedAsNew` needs the successor's `StartRun`, built by [`successor_start`] from
//!    the predecessor's record, the [`ModuleResolver`] and the [`PolicyResolver`], with a
//!    run id, seed and prepared input chosen by the caller. The caller generates the run id
//!    **once per flush** and logs it inside the command, so the successor id is part of the
//!    log and replay never re-derives it (derive only runs at a live flush, never during
//!    apply or replay).
//! 4. [`derive`] maps the batch with that [`DeriveCtx`].
//!
//! # Mapping
//!
//! | journal event at seq `s`, ts `t` | intent |
//! |---|---|
//! | `TimerStarted{fire_at_nanos}` | `ScheduleTimer{op: s, due_at_ms: ceil(fire_at / 1e6)}` |
//! | `SignalWaitStarted{name, timeout_ms}` | `RegisterWait{op: s, Signal, name, timeout_due_ms, at_ms: ceil(t / 1e6)}` |
//! | `AwaitTokenCreated{timeout_ms}` | `RegisterWait{op: s, Token, "", timeout_due_ms, at_ms}` |
//! | `OpCancelled{op}`, `op` a `TimerStarted` | `CancelTimer{op, due_at_ms}` (same rounding) |
//! | `OpCancelled{op}`, `op` a signal / token wait | `CancelWait{op}` |
//! | `EffectScheduled{request}` (06-14) | `RegisterEffect{op: s, kind: EffectKind::from_request, scheduled_at_ms: ceil(t / 1e6)}` |
//! | `OpCancelled{op}`, `op` an `EffectScheduled` (06-14) | `CancelEffect{op}` |
//! | `OpCancelled{op}`, `op` an attached `ChildStarted` (09-08) | `SendMessage(CancelRun{child_run_id(run, op, P), "parent_cancel_op"})` |
//! | `OpCancelled{op}`, `op` anything else (incl. a detached child) | none |
//! | `ChildStarted` at `s`, target resolved (09-05) | `SendMessage(StartChild(StartRun{child_run_id(run, s, P), …, parent: ParentRef{run, s}}))` + `ChildJournal{child}` |
//! | `ChildStarted` at `s`, detached, resolved (09-05) | `SendMessage(StartChild(StartRun{…, parent: None}))` |
//! | `ChildStarted`, target unresolvable | none (the sink journals `unknown_workflow`) |
//! | `RunCompleted{output}` | `CompleteRun{Completed, result, completed_at_ms: ceil(t / 1e6), parent_event}` |
//! | `RunFailed{Cancelled}` | `CompleteRun{Cancelled, result: failure JSON, parent_event}` |
//! | `RunFailed{failure}` | `CompleteRun{Failed, result: failure JSON, parent_event}` |
//! | `RunContinuedAsNew` | `SendMessage(StartRun(successor))` + `CompleteRun{ContinuedAsNew, {"next_run_id"}}` |
//! | `RunContinuedAsNew`, no successor (resolution failed) | `CompleteRun{Failed, result}` |
//! | everything else | none |
//!
//! `timeout_due_ms = ceil((t + timeout_ms * 1e6) / 1e6)` when a timeout is given, else 0.
//! Guest terminal events are never `resumable` (D1: a guest terminal is restarted as a new
//! linked run, not resumed).
//!
//! **Terminal batches** (the last event ends the run) carry no `RegisterWait`/`CancelWait`:
//! the terminal transition deletes every wait row and wait-timeout timer itself
//! (`terminal::on_terminal`), and apply rejects wait intents in a continue-as-new batch.
//! A continue-as-new batch has the shape apply accepts (05-09): events, `ScheduleTimer` /
//! `CancelTimer`, exactly one `SendMessage(StartRun)`, then `CompleteRun{ContinuedAsNew}`.
//!
//! `StepAttemptFailed` maps to nothing: the executor (04-16) submits its own `EnqueueReady`
//! after `Evicted{StepBackoff}`.
//!
//! **Children (09-05).** [`derive_with_children`] takes the child `StartRun`s the sink
//! resolved and prepared ([`ChildCtx::starts`], built with [`crate::children::child_start`])
//! and the child's own `parent_event` ([`ChildCtx::parent_event`], set only when the run
//! has a parent and ends with `RunCompleted` / `RunFailed`; a continue-as-new reports
//! through its successor, PF-F2). Every attached start is followed by the opt-in
//! `ChildJournal` marker (Derived tag 19), so apply journals the child's completion as a
//! kind-67 event; v0.1 commands without it keep their legacy inbox semantics.
//!
//! **Effects (06-14).** `RegisterEffect` records the in-flight `effects` row the bridge's
//! gate and the leader-start wake rely on; like waits it is left out of terminal batches
//! (the terminal transition cleans effect rows up and the gate abandons sends for terminal
//! runs). `CancelEffect` is emitted only for an op identified as an `EffectScheduled` (in
//! the batch or through `prior`); apply treats it as a blind delete of `(run, op)`, so a
//! repeat or a cancel of an already completed effect is harmless. A sink without a bridge
//! strips both intents ([`crate::sink`]).
//!
//! Units: commands carry milliseconds, journal events nanoseconds; due and completion
//! times round **up** (a timer never fires before its journaled time).
//!
//! `CompleteRun` is always the last intent (apply rejects anything after it).
//!
//! # Resume re-arm (B2)
//!
//! [`rearm_unresolved`] recomputes, from a replayed journal, the intents of every op that
//! is still open: the same `ScheduleTimer` / `RegisterWait` rows the original flush derived
//! (same due times), so a resumed run's waits and timers come back exactly as they were. An
//! op is open iff no input completes it (`completes_op() == Some(op)`) and the guest did not
//! cancel it (`OpCancelled{op}`), the rule of the engine's `ReplayState::completion_for`.

use ostro_partition::store::values::RunRecord;
use ostro_types::command::{
    CancelRun, CancelTimer, CompleteRun, Derived, OutboxMsg, ParentRef, ScheduleTimer, SendMessage,
    StartChild, StartRun, TerminalStatus, WaitKind, derived::Intent, outbox_msg,
};
use ostro_types::journal::{CANCELLATION_OP, Continuation, Event, EventBody, Failure};
use ostro_types::live::EffectKind;
use ostro_types::{PayloadRef, RunId};

/// Result-payload `reason_code` of a run that ended with `RunFailed`.
pub const REASON_WORKFLOW_FAILED: &str = "workflow_failed";
/// `reason_code` of features that are not supported yet (effects and children until
/// Phase 6/9).
pub const REASON_UNSUPPORTED: &str = "unsupported";
/// `reason_code` of a continue-as-new whose successor could not be resolved (unknown
/// workflow or version).
pub const REASON_CONTINUE_AS_NEW_FAILED: &str = "continue_as_new_failed";

/// Errors of the mapping. Each one is a bug in the engine or the caller, never user input.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeriveError {
    /// More than one terminal event, or a terminal event that is not the last event.
    #[error("terminal event at seq {seq} is not the last event of the batch")]
    TerminalNotLast { seq: u64 },
    /// A terminal event, but the caller passed no prepared result payload.
    #[error("terminal event at seq {seq} without a prepared result payload")]
    MissingResult { seq: u64 },
    /// A prepared result payload, but the batch has no terminal event.
    #[error("prepared result payload but no terminal event in the batch")]
    UnexpectedResult,
    /// A successor `StartRun`, but the batch does not end with `RunContinuedAsNew`.
    #[error("successor start but no continue-as-new in the batch")]
    UnexpectedSuccessor,
    /// `OpCancelled{op}` where `op` is neither in the batch nor known to the lookup.
    #[error("OpCancelled at seq {seq} names unknown op {op}")]
    UnknownOp { seq: u64, op: u64 },
    /// A prepared parent event, but the batch does not end with `RunCompleted` /
    /// `RunFailed` (09-05).
    #[error("parent event but no guest terminal in the batch")]
    UnexpectedParentEvent,
}

/// Nanoseconds -> milliseconds, rounded up.
pub fn ceil_ms(nanos: u64) -> u64 {
    nanos.div_ceil(1_000_000)
}

/// Due time (ms) of a wait timeout of `timeout_ms` started at `ts_nanos`; 0 = no timeout.
pub fn wait_timeout_due_ms(ts_nanos: u64, timeout_ms: Option<u64>) -> u64 {
    match timeout_ms {
        Some(ms) => ceil_ms(ts_nanos.saturating_add(ms.saturating_mul(1_000_000))),
        None => 0,
    }
}

/// JSON body of a failure result: `{"reason_code", "kind", "message"}` (plus `detail` when
/// given). Used for `RunFailed`, unsupported features and (04-16) attempt failures.
pub fn failure_json(reason_code: &str, kind: &str, message: &str) -> Vec<u8> {
    serde_json::json!({
        "reason_code": reason_code,
        "kind": kind,
        "message": message,
    })
    .to_string()
    .into_bytes()
}

/// Failure JSON of a workflow's own `RunFailed{failure}`.
pub fn run_failed_json(f: &Failure) -> Vec<u8> {
    match f {
        Failure::Retryable(i) | Failure::NonRetryable(i) => {
            failure_json(REASON_WORKFLOW_FAILED, &i.kind, &i.message)
        }
        Failure::Timeout => failure_json("timeout", "timeout", "workflow failed with a timeout"),
        Failure::Cancelled => failure_json("cancelled", "cancelled", "workflow was cancelled"),
    }
}

/// Result JSON of an unsupported feature (`{"reason_code": "unsupported", "detail"}` plus
/// `kind`/`message`, so every failure result has the same three keys).
pub fn unsupported_json(detail: &str) -> Vec<u8> {
    serde_json::json!({
        "reason_code": REASON_UNSUPPORTED,
        "kind": REASON_UNSUPPORTED,
        "message": detail,
        "detail": detail,
    })
    .to_string()
    .into_bytes()
}

/// Result JSON of a run that continued as new: `{"next_run_id": "<successor>"}`.
pub fn continued_as_new_json(next: &RunId) -> Vec<u8> {
    serde_json::json!({ "next_run_id": next.to_string() })
        .to_string()
        .into_bytes()
}

/// `(status, result bytes)` of a terminal event other than continue-as-new.
fn guest_terminal(ev: &Event) -> Option<(TerminalStatus, Vec<u8>)> {
    match &ev.body {
        EventBody::RunCompleted(c) => Some((TerminalStatus::Completed, c.output.data.clone())),
        EventBody::RunFailed(f) => {
            let status = match f.failure {
                Failure::Cancelled => TerminalStatus::Cancelled,
                _ => TerminalStatus::Failed,
            };
            Some((status, run_failed_json(&f.failure)))
        }
        _ => None,
    }
}

fn is_terminal(ev: &Event) -> bool {
    matches!(
        ev.body,
        EventBody::RunCompleted(_) | EventBody::RunFailed(_) | EventBody::RunContinuedAsNew(_)
    )
}

/// Bytes of the run result when `events` contains a `RunCompleted` / `RunFailed` event.
///
/// `RunCompleted` stores the output's raw `data` (the SDK's JSON); its content type stays in
/// the journal event. Failures store [`run_failed_json`]. A continue-as-new returns `None`:
/// its result is [`continued_as_new_json`] of the successor id chosen by the caller (or a
/// failure JSON when the successor cannot be resolved).
pub fn terminal_result_bytes(events: &[Event]) -> Option<Vec<u8>> {
    events.iter().find_map(guest_terminal).map(|(_, b)| b)
}

/// The `RunContinuedAsNew` continuation of the batch, if any.
pub fn continuation(events: &[Event]) -> Option<(&Event, &Continuation)> {
    events.iter().find_map(|e| match &e.body {
        EventBody::RunContinuedAsNew(c) => Some((e, &c.continuation)),
        _ => None,
    })
}

/// Inputs of [`derive`] resolved by the caller.
pub struct DeriveCtx<'a> {
    /// The body of the journal event at seq `op` when it is not in the batch (only
    /// consulted for `OpCancelled`).
    pub prior: &'a dyn Fn(u64) -> Option<EventBody>,
    /// Prepared result payload; required iff the batch has a terminal event.
    pub result: Option<PayloadRef>,
    /// The continue-as-new successor ([`successor_start`]); `None` for a continue-as-new
    /// whose successor could not be resolved (the run then completes `Failed` with
    /// `result`).
    pub successor: Option<StartRun>,
}

/// Map one appended batch to its intents (Phase 4 signature, kept for callers without a
/// continue-as-new successor: a `RunContinuedAsNew` then completes the run `Failed` with
/// `result`).
pub fn intents(
    events: &[Event],
    prior: &dyn Fn(u64) -> Option<EventBody>,
    result: Option<PayloadRef>,
) -> Result<Vec<Derived>, DeriveError> {
    derive(
        events,
        DeriveCtx {
            prior,
            result,
            successor: None,
        },
    )
}

fn complete(status: TerminalStatus, result: PayloadRef, ts_nanos: u64) -> Derived {
    Derived::new(Intent::CompleteRun(CompleteRun {
        status: status as i32,
        result: Some(result),
        completed_at_ms: ceil_ms(ts_nanos),
        resumable: false,
        parent_event: None,
    }))
}

/// The intent that registers the wait of a wait-starting event, if `ev` starts one.
fn register_wait(ev: &Event) -> Option<Derived> {
    let at_ms = ceil_ms(ev.ts_nanos);
    match &ev.body {
        EventBody::SignalWaitStarted(w) => Some(Derived::register_wait(
            ev.seq,
            WaitKind::Signal,
            w.name.clone(),
            wait_timeout_due_ms(ev.ts_nanos, w.timeout_ms),
            at_ms,
        )),
        EventBody::AwaitTokenCreated(t) => Some(Derived::register_wait(
            ev.seq,
            WaitKind::Token,
            String::new(),
            wait_timeout_due_ms(ev.ts_nanos, t.timeout_ms),
            at_ms,
        )),
        _ => None,
    }
}

fn schedule_timer(op: u64, fire_at_nanos: u64) -> Derived {
    Derived::new(Intent::ScheduleTimer(ScheduleTimer {
        op,
        due_at_ms: ceil_ms(fire_at_nanos),
    }))
}

/// Child inputs of [`derive_with_children`] resolved by the caller (09-05).
pub struct ChildCtx<'a> {
    /// The prepared child `StartRun` of the `ChildStarted` at seq `op`; `None` = the target
    /// could not be resolved (no intent; the sink journals `unknown_workflow`).
    pub starts: &'a dyn Fn(u64) -> Option<StartRun>,
    /// The prepared `CompleteRun.parent_event` of a child run's guest terminal (PF-F1).
    pub parent_event: Option<PayloadRef>,
    /// The deterministic child run id of op `op` (`children::child_run_id(run, op, P)`),
    /// for the CancelRun of a cancelled attached child op (09-08); `None` = no children
    /// context (no CancelRun).
    pub child_run: &'a dyn Fn(u64) -> Option<RunId>,
}

/// Map one appended batch to its intents (see the module docs), without children: every
/// `ChildStarted` maps to nothing.
pub fn derive(events: &[Event], ctx: DeriveCtx<'_>) -> Result<Vec<Derived>, DeriveError> {
    derive_with_children(
        events,
        ctx,
        ChildCtx {
            starts: &|_| None,
            parent_event: None,
            child_run: &|_| None,
        },
    )
}

/// Map one appended batch to its intents (see the module docs), children included (09-05).
pub fn derive_with_children(
    events: &[Event],
    ctx: DeriveCtx<'_>,
    kids: ChildCtx<'_>,
) -> Result<Vec<Derived>, DeriveError> {
    let DeriveCtx {
        prior,
        mut result,
        mut successor,
    } = ctx;
    let ChildCtx {
        starts,
        mut parent_event,
        child_run,
    } = kids;
    let terminal_batch = events.last().is_some_and(is_terminal);
    let mut out = Vec::new();
    let mut done = false;
    for (i, ev) in events.iter().enumerate() {
        if is_terminal(ev) {
            if done || i + 1 != events.len() {
                return Err(DeriveError::TerminalNotLast { seq: ev.seq });
            }
            let r = result
                .take()
                .ok_or(DeriveError::MissingResult { seq: ev.seq })?;
            match (guest_terminal(ev), successor.take()) {
                (Some(_), Some(_)) => return Err(DeriveError::UnexpectedSuccessor),
                (Some((status, _)), None) => {
                    let mut c = complete(status, r, ev.ts_nanos);
                    if let Some(Intent::CompleteRun(cr)) = &mut c.intent {
                        cr.parent_event = parent_event.take();
                    }
                    out.push(c);
                }
                (None, Some(start)) => {
                    out.push(Derived::new(Intent::SendMessage(SendMessage {
                        msg: Some(OutboxMsg::start_run(start)),
                    })));
                    out.push(complete(TerminalStatus::ContinuedAsNew, r, ev.ts_nanos));
                }
                (None, None) => out.push(complete(TerminalStatus::Failed, r, ev.ts_nanos)),
            }
            done = true;
            continue;
        }
        match &ev.body {
            EventBody::TimerStarted(t) => out.push(schedule_timer(ev.seq, t.fire_at_nanos)),
            EventBody::ChildStarted(_) => {
                if let Some(start) = starts(ev.seq) {
                    let attached = start.parent.is_some();
                    let child = RunId::from_bytes(&start.run_id).ok();
                    out.push(Derived::new(Intent::SendMessage(SendMessage {
                        msg: Some(OutboxMsg {
                            kind: Some(outbox_msg::Kind::StartChild(StartChild {
                                start: Some(start),
                            })),
                        }),
                    })));
                    if attached && let Some(child) = child {
                        out.push(Derived::child_journal(&child));
                    }
                }
            }
            EventBody::SignalWaitStarted(_) | EventBody::AwaitTokenCreated(_) => {
                if !terminal_batch && let Some(d) = register_wait(ev) {
                    out.push(d);
                }
            }
            EventBody::EffectScheduled(e) => {
                if !terminal_batch {
                    out.push(Derived::register_effect(
                        ev.seq,
                        u32::from(EffectKind::from_request(&e.request).as_u8()),
                        ceil_ms(ev.ts_nanos),
                    ));
                }
            }
            EventBody::OpCancelled(c) => {
                let started = events
                    .iter()
                    .find(|e| e.seq == c.op)
                    .map(|e| e.body.clone())
                    .or_else(|| prior(c.op))
                    .ok_or(DeriveError::UnknownOp {
                        seq: ev.seq,
                        op: c.op,
                    })?;
                match started {
                    EventBody::TimerStarted(t) => {
                        out.push(Derived::new(Intent::CancelTimer(CancelTimer {
                            op: c.op,
                            due_at_ms: ceil_ms(t.fire_at_nanos),
                        })));
                    }
                    EventBody::SignalWaitStarted(_) | EventBody::AwaitTokenCreated(_)
                        if !terminal_batch =>
                    {
                        out.push(Derived::cancel_wait(c.op));
                    }
                    EventBody::EffectScheduled(_) => out.push(Derived::cancel_effect(c.op)),
                    // 09-08 (D-P9-11): a graceful cancel of the attached child; the op itself
                    // completes Err(Cancelled) in the engine (D5). Detached: nothing.
                    EventBody::ChildStarted(ch) if !ch.options.detached => {
                        if let Some(child) = child_run(c.op) {
                            out.push(Derived::new(Intent::SendMessage(SendMessage {
                                msg: Some(OutboxMsg {
                                    kind: Some(outbox_msg::Kind::CancelRun(CancelRun {
                                        target_run: child.as_bytes().to_vec(),
                                        reason: crate::children::REASON_PARENT_CANCEL_OP
                                            .to_string(),
                                    })),
                                }),
                            })));
                        }
                    }
                    _ => {}
                }
            }
            // No intent: RunStarted (seq 0, written by the executor), StepCompleted,
            // StepAttemptFailed (the executor requeues itself, 04-16), PatchMarker,
            // StatusPublished, WaitResolved, and inputs (journaled by apply, or by the
            // executor's first-activation CancelRequested).
            _ => {}
        }
    }
    if result.is_some() {
        return Err(DeriveError::UnexpectedResult);
    }
    if successor.is_some() {
        return Err(DeriveError::UnexpectedSuccessor);
    }
    if parent_event.is_some() {
        return Err(DeriveError::UnexpectedParentEvent);
    }
    Ok(out)
}

/// Intents re-arming every unresolved op of `journal` (B2, see the module docs): a
/// `ScheduleTimer` per open `TimerStarted` and a `RegisterWait` per open `SignalWaitStarted`
/// / `AwaitTokenCreated`, with the original due times and `at_ms`. Effects, children and
/// [`CANCELLATION_OP`] are skipped. Empty for a journal that ends the run.
pub fn rearm_unresolved(journal: &[Event]) -> Vec<Derived> {
    if journal.iter().any(is_terminal) {
        return Vec::new();
    }
    let mut closed = std::collections::BTreeSet::new();
    for ev in journal {
        if let Some(op) = ev.body.completes_op() {
            closed.insert(op);
        }
        if let EventBody::OpCancelled(c) = &ev.body {
            closed.insert(c.op);
        }
    }
    journal
        .iter()
        .filter(|ev| ev.seq != CANCELLATION_OP && !closed.contains(&ev.seq))
        .filter_map(|ev| match &ev.body {
            EventBody::TimerStarted(t) => Some(schedule_timer(ev.seq, t.fire_at_nanos)),
            _ => register_wait(ev),
        })
        .collect()
}

// ------------------------------------------------------------------- continue-as-new

/// A workflow version as the continue-as-new successor runs it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedWorkflow {
    pub version: u32,
    pub module_sha: [u8; 32],
    /// The version's `describe()` run timeout (a fresh deadline for the successor).
    pub run_timeout_ms: Option<u64>,
}

/// Resolves `(qualified workflow, version)` to its module (meta lookup in the server).
pub trait ModuleResolver: Send + Sync {
    /// `version` `None` = the workflow's active version. `None` = unknown.
    fn resolve(&self, workflow: &str, version: Option<u32>) -> Option<ResolvedWorkflow>;

    /// The module-local default queue of `(workflow, version)` from its `describe()`
    /// (09-05 child starts); `None` = none known (the API default applies).
    fn default_queue(&self, _workflow: &str, _version: u32) -> Option<String> {
        None
    }

    /// Whether module `sha` has a metadata record (13-17, the executor's `module_gone`
    /// check): `Some(false)` = the record is gone (module GC deleted it), `None` = unknown
    /// (no metadata wired; the check is skipped).
    fn module_known(&self, _sha: &[u8; 32]) -> Option<bool> {
        None
    }

    /// A receiver that changes whenever the metadata view applies an entry (`None` = no
    /// metadata): the `module_gone` check re-reads on it before failing a run.
    fn changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        None
    }
}

/// Resolves a new run's `(retention_ms, schedule_to_start_ms)` for `(workflow, queue)`, both
/// in qualified form (D9; [`crate::retention`]).
pub trait PolicyResolver: Send + Sync {
    fn resolve(&self, workflow: &str, queue: &str) -> (u64, u64);
}

/// The resolver used when no metadata is wired (harnesses): knows nothing, so a successor
/// on the predecessor's own workflow and version reuses the predecessor's module and run
/// timeout, and any other successor fails to resolve.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoModules;

impl ModuleResolver for NoModules {
    fn resolve(&self, _workflow: &str, _version: Option<u32>) -> Option<ResolvedWorkflow> {
        None
    }
}

/// The policy used when no metadata is wired: the successor inherits the predecessor's
/// retention (see [`SuccessorParts`]) and has no schedule-to-start timeout.
#[derive(Clone, Copy, Debug, Default)]
pub struct InheritPolicy;

impl PolicyResolver for InheritPolicy {
    fn resolve(&self, _workflow: &str, _queue: &str) -> (u64, u64) {
        (0, 0)
    }
}

/// D23 storage form of a module-local `name` in `tenant` (bare for the default tenant).
pub fn qualify(tenant: Option<&str>, name: &str) -> String {
    match tenant {
        Some(t) if !t.is_empty() => format!("{t}/{name}"),
        _ => name.to_owned(),
    }
}

/// Everything about a successor that the caller chooses (I/O, randomness, clock).
#[derive(Clone, Debug)]
pub struct SuccessorParts<'a> {
    /// The predecessor and its record.
    pub pred_id: RunId,
    pub pred: &'a RunRecord,
    /// The successor's run id (generated once per flush by the caller).
    pub run_id: RunId,
    /// 32-byte RNG seed.
    pub rng_seed: [u8; 32],
    /// The prepared continuation input bytes.
    pub input: PayloadRef,
    /// `requested_at_ms` / `ready_at_ms` of the successor (the continue-as-new event time).
    pub at_ms: u64,
}

/// Why a successor could not be built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SuccessorError {
    #[error("continue-as-new into workflow {workflow:?} version {version:?}: not registered")]
    Unresolved {
        workflow: String,
        version: Option<u32>,
    },
}

/// Build the successor `StartRun` of a continue-as-new (D10, D23, Phase 14 F11, PF-F2).
///
/// - **Workflow (D23):** the continuation's module-local name qualified with the
///   predecessor's tenant, or the predecessor's (already qualified) workflow.
/// - **Version and module:** the continuation's version, else the active version when the
///   workflow changes, else the predecessor's version. Resolved through `modules`; an
///   unregistered target is [`SuccessorError::Unresolved`], except that the predecessor's
///   own workflow and version fall back to its pinned module and run timeout.
/// - **Copied (F11, PF-F2):** tenant, tags, traceparent, queue, priority, concurrency key and
///   the parent link. `continued_from` = the predecessor. No idempotency key.
/// - **Own deadlines:** `run_timeout_ms` from the version metadata (a fresh run timeout);
///   retention and schedule-to-start from `policy`, where a retention of 0 (no policy) keeps
///   the predecessor's retention.
pub fn successor_start(
    cont: &Continuation,
    parts: SuccessorParts<'_>,
    modules: &dyn ModuleResolver,
    policy: &dyn PolicyResolver,
) -> Result<StartRun, SuccessorError> {
    let pred = parts.pred;
    let workflow = match &cont.workflow {
        Some(name) => qualify(pred.tenant.as_deref(), name),
        None => pred.workflow.clone(),
    };
    let same_workflow = workflow == pred.workflow;
    let version = match cont.version {
        Some(v) => Some(v),
        None if same_workflow => Some(pred.workflow_version),
        None => None,
    };
    let resolved = match modules.resolve(&workflow, version) {
        Some(r) => r,
        None if same_workflow && version == Some(pred.workflow_version) => match pred.module_sha {
            Some(sha) => ResolvedWorkflow {
                version: pred.workflow_version,
                module_sha: sha,
                run_timeout_ms: pred.run_timeout_ms,
            },
            None => return Err(SuccessorError::Unresolved { workflow, version }),
        },
        None => return Err(SuccessorError::Unresolved { workflow, version }),
    };
    let (retention, s2s) = policy.resolve(&workflow, &pred.queue);
    let retention_ms = if retention == 0 {
        pred.retention_ms
    } else {
        retention
    };
    Ok(StartRun {
        run_id: parts.run_id.as_bytes().to_vec(),
        workflow,
        workflow_version: resolved.version,
        queue: pred.queue.clone(),
        input: Some(parts.input),
        idempotency_key: Vec::new(),
        conflict_policy: 0,
        concurrency_key: pred.concurrency_key.clone(),
        priority: pred.priority,
        ready_at_ms: parts.at_ms,
        requested_at_ms: parts.at_ms,
        parent: pred.parent.map(|(id, op)| ParentRef {
            run_id: id.to_vec(),
            op,
        }),
        rng_seed: parts.rng_seed.to_vec(),
        module_sha: resolved.module_sha.to_vec(),
        tags: pred.tags.clone(),
        tenant: pred.tenant.clone().unwrap_or_default(),
        traceparent: pred.traceparent.clone().unwrap_or_default(),
        run_timeout_ms: resolved.run_timeout_ms.unwrap_or(0),
        schedule_to_start_ms: s2s,
        retention_ms,
        retry_of: Vec::new(),
        continued_from: parts.pred_id.as_bytes().to_vec(),
        run_started: None,
        ..Default::default()
    })
}

/// [`ModuleResolver`] over the metadata state machine (workflow versions and the active
/// version, keyed by the qualified name).
#[derive(Clone)]
pub struct MetaModules(pub ostro_meta::MetaView);

impl std::fmt::Debug for MetaModules {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MetaModules")
    }
}

impl ModuleResolver for MetaModules {
    fn resolve(&self, workflow: &str, version: Option<u32>) -> Option<ResolvedWorkflow> {
        self.0.read(|s| {
            let v = match version {
                Some(v) => v,
                None => s.active(workflow)?.value.version,
            };
            s.workflow(workflow, v).map(|w| ResolvedWorkflow {
                version: w.value.version,
                module_sha: w.value.module_sha256,
                run_timeout_ms: w.value.run_timeout_ms,
            })
        })
    }

    fn default_queue(&self, workflow: &str, version: u32) -> Option<String> {
        self.0.read(|s| {
            s.workflow(workflow, version)
                .and_then(|w| w.value.default_queue.clone())
        })
    }

    fn module_known(&self, sha: &[u8; 32]) -> Option<bool> {
        Some(self.0.read(|s| s.module(sha).is_some()))
    }

    fn changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.0.subscribe())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use ostro_types::journal::{
        AttemptBackoff, ChildOptions, ChildStarted, CommandHash, CommandMeta, Continuation,
        EffectRequest, EffectScheduled, ErrorInfo, HttpRequest, OpCancelled, PatchMarker, Payload,
        RecordedResult, RunCompleted, RunContinuedAsNew, RunFailed, StatusPublished,
        StepAttemptFailed, StepCompleted, TimerStarted, WaitResolved,
    };

    use super::*;

    fn meta() -> CommandMeta {
        CommandMeta {
            hash_ver: 1,
            hash: CommandHash([0; 16]),
        }
    }

    fn ev(seq: u64, ts: u64, body: EventBody) -> Event {
        Event {
            seq,
            ts_nanos: ts,
            body,
        }
    }

    fn none(_: u64) -> Option<EventBody> {
        None
    }

    fn timer(fire_at: u64) -> EventBody {
        EventBody::TimerStarted(TimerStarted {
            meta: meta(),
            fire_at_nanos: fire_at,
        })
    }

    fn cancel(op: u64) -> EventBody {
        EventBody::OpCancelled(OpCancelled { meta: meta(), op })
    }

    fn payload(data: &[u8]) -> Payload {
        Payload {
            content_type: "application/json".into(),
            data: data.to_vec(),
        }
    }

    fn info(kind: &str, message: &str) -> ErrorInfo {
        ErrorInfo {
            kind: kind.into(),
            message: message.into(),
            details: None,
        }
    }

    fn step() -> EventBody {
        EventBody::StepCompleted(StepCompleted {
            meta: meta(),
            name: "s".into(),
            outcome: RecordedResult {
                codec_ver: 1,
                bytes: vec![],
            },
            fuel_used: 0,
        })
    }

    fn pref(b: &'static [u8]) -> PayloadRef {
        PayloadRef::inline(Bytes::from_static(b))
    }

    fn intent(d: &Derived) -> &Intent {
        d.intent.as_ref().expect("intent")
    }

    fn json(b: &[u8]) -> serde_json::Value {
        serde_json::from_slice(b).expect("json")
    }

    #[test]
    fn ceil_rounds_up() {
        assert_eq!(ceil_ms(0), 0);
        assert_eq!(ceil_ms(1), 1);
        assert_eq!(ceil_ms(1_000_000), 1);
        assert_eq!(ceil_ms(1_000_001), 2);
        assert_eq!(ceil_ms(u64::MAX), u64::MAX.div_ceil(1_000_000));
    }

    #[test]
    fn timer_started_schedules_timer_ceil_ms() {
        let d = intents(&[ev(5, 1, timer(1_500_000_001))], &none, None).unwrap();
        assert_eq!(
            d,
            vec![Derived::new(Intent::ScheduleTimer(ScheduleTimer {
                op: 5,
                due_at_ms: 1501
            }))]
        );
    }

    #[test]
    fn op_cancelled_timer_from_prior_journal() {
        let prior = |op: u64| (op == 5).then(|| timer(1_500_000_001));
        let d = intents(&[ev(9, 1, cancel(5))], &prior, None).unwrap();
        assert_eq!(
            d,
            vec![Derived::new(Intent::CancelTimer(CancelTimer {
                op: 5,
                due_at_ms: 1501
            }))]
        );
    }

    #[test]
    fn op_cancelled_timer_in_same_batch() {
        let d = intents(
            &[ev(3, 1, timer(2_000_000)), ev(4, 2, cancel(3))],
            &none,
            None,
        )
        .unwrap();
        assert_eq!(d.len(), 2);
        assert!(matches!(intent(&d[0]), Intent::ScheduleTimer(t) if t.op == 3 && t.due_at_ms == 2));
        assert!(matches!(intent(&d[1]), Intent::CancelTimer(t) if t.op == 3 && t.due_at_ms == 2));
    }

    #[test]
    fn op_cancelled_effect_cancels_effect() {
        let prior = |_: u64| {
            Some(EventBody::EffectScheduled(EffectScheduled {
                meta: meta(),
                request: EffectRequest::Http(HttpRequest {
                    method: "GET".into(),
                    url: "http://x".into(),
                    headers: vec![],
                    body: None,
                    timeout_ms: None,
                    idempotency_key: None,
                    redact: vec![],
                }),
            }))
        };
        assert_eq!(
            intents(&[ev(9, 1, cancel(2))], &prior, None).unwrap(),
            vec![Derived::cancel_effect(2)]
        );
    }

    fn http_effect() -> EventBody {
        EventBody::EffectScheduled(EffectScheduled {
            meta: meta(),
            request: EffectRequest::Http(HttpRequest {
                method: "POST".into(),
                url: "http://x/pay".into(),
                headers: vec![],
                body: None,
                timeout_ms: None,
                idempotency_key: None,
                redact: vec![],
            }),
        })
    }

    #[test]
    fn effect_scheduled_registers_effect_ceil_ms() {
        let sql = EventBody::EffectScheduled(EffectScheduled {
            meta: meta(),
            request: EffectRequest::Sql(ostro_types::journal::SqlRequest {
                connection: "db".into(),
                statement: "select 1".into(),
                params: vec![],
                exactly_once: false,
            }),
        });
        let d = intents(
            &[ev(4, 1_000_001, http_effect()), ev(5, 2_000_000, sql)],
            &none,
            None,
        )
        .unwrap();
        assert_eq!(
            d,
            vec![
                Derived::register_effect(4, 0, 2),
                Derived::register_effect(5, 1, 2)
            ]
        );
    }

    #[test]
    fn effect_scheduled_and_cancelled_in_same_batch() {
        let d = intents(&[ev(4, 1, http_effect()), ev(5, 2, cancel(4))], &none, None).unwrap();
        assert_eq!(
            d,
            vec![Derived::register_effect(4, 0, 1), Derived::cancel_effect(4)]
        );
    }

    #[test]
    fn terminal_batch_registers_no_effect() {
        let done = EventBody::RunCompleted(RunCompleted {
            meta: meta(),
            output: payload(b"1"),
        });
        let d = intents(
            &[ev(4, 1, http_effect()), ev(5, 2, done)],
            &none,
            Some(pref(b"1")),
        )
        .unwrap();
        assert_eq!(d.len(), 1);
        assert!(matches!(intent(&d[0]), Intent::CompleteRun(_)));
    }

    #[test]
    fn op_cancelled_unknown_op_is_error() {
        assert_eq!(
            intents(&[ev(9, 1, cancel(2))], &none, None),
            Err(DeriveError::UnknownOp { seq: 9, op: 2 })
        );
    }

    #[test]
    fn run_completed_is_last_complete_run() {
        let events = [
            ev(1, 10, step()),
            ev(
                2,
                11,
                EventBody::StatusPublished(StatusPublished {
                    meta: meta(),
                    json: "{}".into(),
                }),
            ),
            ev(
                3,
                2_000_000_001,
                EventBody::RunCompleted(RunCompleted {
                    meta: meta(),
                    output: payload(b"{\"ok\":1}"),
                }),
            ),
        ];
        assert_eq!(terminal_result_bytes(&events), Some(b"{\"ok\":1}".to_vec()));
        let d = intents(&events, &none, Some(pref(b"{\"ok\":1}"))).unwrap();
        assert_eq!(
            d,
            vec![Derived::new(Intent::CompleteRun(CompleteRun {
                status: TerminalStatus::Completed as i32,
                result: Some(pref(b"{\"ok\":1}")),
                completed_at_ms: 2001,
                resumable: false,
                parent_event: None,
            }))]
        );
    }

    #[test]
    fn complete_run_after_timer_is_last() {
        let events = [
            ev(1, 1, timer(5_000_000)),
            ev(
                2,
                3_000_000,
                EventBody::RunCompleted(RunCompleted {
                    meta: meta(),
                    output: payload(b"1"),
                }),
            ),
        ];
        let d = intents(&events, &none, Some(pref(b"1"))).unwrap();
        assert_eq!(d.len(), 2);
        assert!(matches!(intent(&d[0]), Intent::ScheduleTimer(_)));
        assert!(matches!(intent(&d[1]), Intent::CompleteRun(c) if c.completed_at_ms == 3));
    }

    #[test]
    fn run_failed_maps_to_failed_with_failure_json() {
        let events = [ev(
            4,
            7,
            EventBody::RunFailed(RunFailed {
                meta: meta(),
                failure: Failure::NonRetryable(info("bad_input", "nope")),
            }),
        )];
        let bytes = terminal_result_bytes(&events).unwrap();
        let v = json(&bytes);
        assert_eq!(v["reason_code"], "workflow_failed");
        assert_eq!(v["kind"], "bad_input");
        assert_eq!(v["message"], "nope");
        let d = intents(&events, &none, Some(PayloadRef::inline(bytes.into()))).unwrap();
        assert!(matches!(
            intent(&d[0]),
            Intent::CompleteRun(c) if c.status == TerminalStatus::Failed as i32 && c.completed_at_ms == 1
        ));
        // Timeout / Cancelled have their own reason codes.
        assert_eq!(
            json(&run_failed_json(&Failure::Timeout))["reason_code"],
            "timeout"
        );
        assert_eq!(
            json(&run_failed_json(&Failure::Cancelled))["reason_code"],
            "cancelled"
        );
    }

    #[test]
    fn continue_as_new_without_successor_completes_failed() {
        let events = [ev(
            2,
            1,
            EventBody::RunContinuedAsNew(RunContinuedAsNew {
                meta: meta(),
                continuation: Continuation {
                    input: payload(b"{}"),
                    workflow: None,
                    version: None,
                },
            }),
        )];
        // The result of a continue-as-new is chosen by the caller.
        assert_eq!(terminal_result_bytes(&events), None);
        assert!(continuation(&events).is_some());
        let d = intents(&events, &none, Some(pref(b"x"))).unwrap();
        assert_eq!(d.len(), 1);
        assert!(matches!(
            intent(&d[0]),
            Intent::CompleteRun(c) if c.status == TerminalStatus::Failed as i32 && !c.resumable
        ));
    }

    #[test]
    fn no_intent_events() {
        let bodies = vec![
            step(),
            EventBody::StepAttemptFailed(StepAttemptFailed {
                meta: meta(),
                name: "s".into(),
                attempt: 1,
                failure: Failure::Timeout,
                retry_at_nanos: 9_000_000_000,
                backoff: AttemptBackoff::Requeue,
                fuel_used: 0,
            }),
            EventBody::PatchMarker(PatchMarker {
                meta: meta(),
                change_id: "c".into(),
            }),
            EventBody::WaitResolved(WaitResolved {
                meta: meta(),
                completed: vec![1],
            }),
            // (EffectScheduled derives RegisterEffect since 06-14: see the effect tests.)
            EventBody::ChildStarted(ChildStarted {
                meta: meta(),
                workflow: "w".into(),
                input: payload(b"{}"),
                options: ChildOptions {
                    queue: None,
                    version: None,
                    detached: false,
                    run_timeout_ms: None,
                },
            }),
        ];
        let events: Vec<Event> = bodies
            .into_iter()
            .enumerate()
            .map(|(i, b)| ev(i as u64 + 1, 1, b))
            .collect();
        assert_eq!(terminal_result_bytes(&events), None);
        assert!(intents(&events, &none, None).unwrap().is_empty());
    }

    #[test]
    fn terminal_rules_are_enforced() {
        let done = || {
            EventBody::RunCompleted(RunCompleted {
                meta: meta(),
                output: payload(b"1"),
            })
        };
        // terminal not last
        assert_eq!(
            intents(
                &[ev(1, 1, done()), ev(2, 1, step())],
                &none,
                Some(pref(b"1"))
            ),
            Err(DeriveError::TerminalNotLast { seq: 1 })
        );
        // missing prepared result
        assert_eq!(
            intents(&[ev(1, 1, done())], &none, None),
            Err(DeriveError::MissingResult { seq: 1 })
        );
        // result without terminal
        assert_eq!(
            intents(&[ev(1, 1, step())], &none, Some(pref(b"1"))),
            Err(DeriveError::UnexpectedResult)
        );
    }
}
