//! `PartitionSink`: the real [`ActivationSink`] over a [`PartitionHandle`] (plan 04-15).
//!
//! One sink per activation. It holds the activation's [`AttemptToken`]; every engine flush
//! becomes exactly one fenced `EventsAppended` command:
//!
//! 1. the result payload of a terminal event is prepared (blob store first when > 256 KiB);
//!    a continue-as-new also gets its successor `StartRun` here (see below),
//! 2. intents come from the pure [`derive::derive`] mapping,
//! 3. every event is encoded (`encode_event`) and its bytes prepared (`prepare_payload`),
//! 4. the command is submitted and its outcome mapped:
//!
//! | apply outcome | `append` result |
//! |---|---|
//! | `Accepted` | `Ok(from_seq + n)`, lease renewed (`on_renew`) |
//! | `Rejected(Fenced \| StaleEpoch \| RunTerminal \| RunNotFound)` | `Err(Fenced)`: the activation must stop |
//! | `Rejected(SeqMismatch{expected})` | `Err(Conflict{expected})` |
//! | any other rejection, `Duplicate`, `PartitionError` | `Err(Unavailable)` |
//!
//! Fencing is entirely apply's: the sink never second-guesses the token beyond the
//! `attempt != token.n` sanity check (research "Key insight": no second fencing check).
//!
//! # Continue-as-new (05-11)
//!
//! A batch ending in `RunContinuedAsNew` reads the run's own record and builds the
//! successor with [`derive::successor_start`]: a fresh run id (uuid v7 on this run's
//! partition) and OS-random seed generated **once per flush** and logged inside the command
//! (so replay never re-derives them), the continuation input prepared like any payload, the
//! module from the [`ModuleResolver`] and retention / schedule-to-start from the
//! [`PolicyResolver`]. The run's result is `{"next_run_id"}`. An unresolvable successor
//! (unknown workflow or version) completes the run `Failed` with reason
//! `continue_as_new_failed` instead.
//!
//! # Effects (06-14)
//!
//! With [`PartitionSink::with_bridge`], the sink hands the engine the bridge's
//! [`EffectDriver`](ostro_engine::EffectDriver) ([`ActivationSink::effect_driver`], P-1):
//! `EffectScheduled` derives `RegisterEffect` (the in-flight `effects` row), an
//! `OpCancelled` of an effect derives `CancelEffect` and, once that append is accepted, the
//! bridge task is cancelled (best effort). The effect is completed by the bridge through
//! `Cmd::CompleteEffect`, never by this sink: [`PartitionSink::unsupported_input`] returns
//! `None` for `EffectScheduled` (B1), so neither the post-append path nor
//! [`PartitionSink::repair_unsupported`] (run on every journal load) fails an in-flight
//! effect as `unsupported`. Without a bridge (tests, bare nodes) `RegisterEffect` /
//! `CancelEffect` are stripped and effects keep the Phase 4 path below.
//!
//! # Children (09-05)
//!
//! Each `ChildStarted{seq s}` of a batch is resolved before the append
//! ([`children::resolve_child`] through the same [`ModuleResolver`] as continue-as-new):
//! the input is prepared (blob > 256 KiB), a fresh OS-random seed drawn, and the child
//! `StartRun` built with the deterministic id [`children::child_run_id`]`(run, s, P)`;
//! derive turns it into `SendMessage(StartChild)` + the opt-in `ChildJournal` marker. An
//! unresolvable target journals `ChildCompleted{s, Err(non-retryable unknown_workflow)}`
//! right after the flush (the completion mechanism below). A child run's terminal batch
//! (`RunRecord.parent`, read at construction) carries `CompleteRun.parent_event`: the
//! submitter-encoded kind-67 `ChildCompleted{op: parent op, outcome}`
//! ([`children::parent_event_bytes`], prepared like any payload). Children never complete
//! as `unsupported` any more. On journal load, [`PartitionSink::repair_unsupported`] also
//! applies the child repair rule ([`children::plan_child_repairs`], W1): a children row =
//! delivered, skip; else re-send the StartChild (intents-only `EventsAppended`) if the
//! target resolves now, or journal `unknown_workflow` once.
//!
//! # Children, part 2 (09-08)
//!
//! - `OpCancelled` of an attached child op derives `SendMessage(CancelRun{child,
//!   "parent_cancel_op"})` ([`derive::ChildCtx::child_run`]).
//! - A detached child's op is completed right after the flush that carried its StartChild:
//!   `ChildCompleted{op, Ok(Child({"run_id": id}))}` ([`children::detached_completion`]),
//!   journaled and sent on the hot-wait channel like the other post-flush completions.
//! - The journal-load repair executes every [`children::plan_child_repairs`] action (repair
//!   table in `children.rs`): attached re-sends (+ `ChildJournal`), detached re-sends + the
//!   `{run_id}` completion, `unknown_workflow`, and the W-b `child_terminated` completion
//!   of a parent resumed after its retention window.
//! - Failpoints (feature `failpoints`, `return` = `AppendError::Unavailable`, the
//!   executor re-activates the run): `child::start_child_after_flush` after the accepted
//!   flush of a batch with a `ChildStarted`, before any post-flush completion;
//!   `child::detached_after_flush` before a detached completion is journaled;
//!   `child::parent_event_before_flush` after a child's `parent_event` was prepared, before
//!   its terminal append.
//!
//! # Post-flush completions (Phase 4 mechanism)
//!
//! Without a bridge, effects are not executed. Instead of hanging, the run gets an
//! immediate non-retryable failure (await tokens are real since Phase 5: `RegisterWait`):
//!
//! - `EffectScheduled{seq s}` -> input `EffectCompleted{op: s, outcome: Err(NonRetryable{kind:
//!   "unsupported"})}`; an unresolvable `ChildStarted` -> `ChildCompleted{unknown_workflow}`.
//!   They are journaled with the **same token** in a second `EventsAppended` right after
//!   the scheduling append succeeded, and sent on the activation's `inputs` channel (hot
//!   wait). The engine's own next append then usually conflicts (`AppendConflict` ->
//!   re-activation), which is correct: the re-activation replays the completion.
//!
//! Completions are skipped when the batch itself ends the run. If journaling them fails
//! with an unavailable partition, `append` returns `Unavailable` although the scheduling
//! events are committed; the executor calls [`PartitionSink::repair_unsupported`] on the
//! journal it loads for the next activation so no op is left without a completion.
//!
//! # Guest logs (08-17)
//!
//! Live guest log lines ([`ActivationSink::guest_log`], already replay-filtered and
//! bounded per activation by the engine) are forwarded to the node's [`GuestLogSink`]
//! when one is attached ([`PartitionSink::with_guest_logs`]); they never touch the log.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;
use ostro_engine::sink::{ActivationOutcome, ActivationSink, AppendError, GuestLogRecord, RunRef};
use ostro_log::{ReplicatedLog, Rf1Log};
use ostro_partition::apply::InvalidCode;
use ostro_partition::store::keys::children_key;
use ostro_partition::{ApplyOutcome, PartitionHandle, RejectReason, Table};
use ostro_types::command::{
    AttemptTokenMsg, Derived, EventsAppended, JournalEvent, StartRun, derived::Intent,
};
use ostro_types::journal::{
    EffectCompleted, ErrorInfo, Event, EventBody, Failure, OpOutput, encode_event, encode_result,
};
use ostro_types::{AttemptToken, Command, PayloadRef, RunId};
use tokio::sync::mpsc::UnboundedSender;

use crate::bridge_services::BridgeServices;
use crate::children::{self, ChildRepair};
use crate::derive::{self, InheritPolicy, ModuleResolver, NoModules, PolicyResolver};
use crate::journal_io;
use crate::services::guest_logs::GuestLogSink;

/// Error kind of every Phase 4 unsupported completion.
pub const UNSUPPORTED_KIND: &str = "unsupported";
/// Message of the unsupported effect completion.
pub const EFFECTS_UNSUPPORTED: &str = "effects arrive in Phase 6";
/// Message of the former unsupported child completion (no longer produced since 09-05;
/// kept for journals written before).
pub const CHILDREN_UNSUPPORTED: &str = "child workflows arrive in Phase 6/9";

/// Attempts to journal unsupported completions when the journal end moves underneath
/// (an input such as `TimerFired` landed between the two appends).
const INPUT_RETRIES: usize = 4;

/// Evaluate a `child::` failpoint (09-08; module docs): `Ok` unless configured with `return`.
/// Without the feature `failpoints` it expands to `Ok(())`.
macro_rules! failpoint {
    ($name:literal) => {{
        #[cfg(feature = "failpoints")]
        let r = match fail::eval($name, |arg| arg) {
            None => Ok(()),
            Some(_) => Err(AppendError::Unavailable(format!("{} (injected)", $name))),
        };
        #[cfg(not(feature = "failpoints"))]
        let r = Ok::<(), AppendError>(());
        r
    }};
}

/// Callback run after every accepted append (lease renewal).
pub type RenewFn = Arc<dyn Fn() + Send + Sync>;

/// The real [`ActivationSink`]: one per activation (one run, one attempt token).
pub struct PartitionSink<L: ReplicatedLog = Rf1Log> {
    handle: PartitionHandle<L>,
    run: RunId,
    run_str: String,
    token: AttemptToken,
    inputs: Option<UnboundedSender<Event>>,
    on_renew: Option<RenewFn>,
    next_seq: AtomicU64,
    terminal: AtomicBool,
    outcome: Mutex<Option<ActivationOutcome>>,
    modules: Arc<dyn ModuleResolver>,
    policy: Arc<dyn PolicyResolver>,
    guest_logs: Option<Arc<GuestLogSink>>,
    /// The effects bridge (06-14); `None` keeps the Phase 4 unsupported path for effects.
    bridge: Option<Arc<BridgeServices>>,
    /// `RunRecord.parent` `(parent run, op)` at construction (09-05 parent_event).
    parent: Option<(RunId, u64)>,
}

impl<L: ReplicatedLog> std::fmt::Debug for PartitionSink<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionSink")
            .field("run", &self.run_str)
            .field("token", &self.token)
            .field("next_seq", &self.next_seq())
            .finish_non_exhaustive()
    }
}

fn unavailable(e: impl std::fmt::Display) -> AppendError {
    AppendError::Unavailable(e.to_string())
}

/// Non-retryable `unsupported` failure.
pub fn unsupported_failure(message: &str) -> Failure {
    Failure::NonRetryable(ErrorInfo {
        kind: UNSUPPORTED_KIND.to_string(),
        message: message.to_string(),
        details: None,
    })
}

/// The unsupported completion input for an event that schedules an op, if any
/// (`bridge`: effects are completed by the bridge, never as unsupported; B1).
fn unsupported_completion(ev: &Event, bridge: bool) -> Option<EventBody> {
    let out: Result<OpOutput, Failure>;
    match &ev.body {
        EventBody::EffectScheduled(_) if bridge => None,
        EventBody::EffectScheduled(_) => {
            out = Err(unsupported_failure(EFFECTS_UNSUPPORTED));
            Some(EventBody::EffectCompleted(EffectCompleted {
                op: ev.seq,
                outcome: encode_result(&out),
            }))
        }
        // Children are bridged since 09-05 (never `unsupported`).
        _ => None,
    }
}

impl PartitionSink<Rf1Log> {
    /// Route effects through the bridge (06-14; module docs). Registers this sink's
    /// partition handle with the bridge's partition table (the gate and the completion
    /// sink look partitions up there).
    pub fn with_bridge(mut self, bridge: Arc<BridgeServices>) -> Self {
        bridge.partitions().insert(self.handle.clone());
        self.bridge = Some(bridge);
        self
    }
}

impl<L: ReplicatedLog> PartitionSink<L> {
    /// A sink for one activation of `run` holding `token`.
    ///
    /// `inputs`: the engine side of the activation's hot-wait channel (unsupported
    /// completions are sent there). `on_renew`: called after every accepted append.
    /// [`next_seq`](Self::next_seq) starts at the run record's journal end.
    pub fn new(
        handle: PartitionHandle<L>,
        run: RunId,
        token: AttemptToken,
        inputs: Option<UnboundedSender<Event>>,
        on_renew: Option<RenewFn>,
    ) -> Self {
        let rec = handle.read_run(&run).ok().flatten();
        let next = rec.as_ref().map_or(0, |r| r.next_seq);
        let parent = rec
            .and_then(|r| r.parent)
            .and_then(|(id, op)| RunId::from_bytes(&id).ok().map(|id| (id, op)));
        Self {
            run_str: run.to_string(),
            handle,
            run,
            token,
            inputs,
            on_renew,
            next_seq: AtomicU64::new(next),
            terminal: AtomicBool::new(false),
            outcome: Mutex::new(None),
            modules: Arc::new(NoModules),
            policy: Arc::new(InheritPolicy),
            guest_logs: None,
            bridge: None,
            parent,
        }
    }

    /// The unsupported completion input for `ev`, if it schedules an op that gets one:
    /// `EffectScheduled` only without a bridge (B1); never `ChildStarted` (09-05).
    pub fn unsupported_input(&self, ev: &Event) -> Option<EventBody> {
        unsupported_completion(ev, self.bridge.is_some())
    }

    /// Forward live guest log lines to the node's guest log sink (08-17). Without it the
    /// engine writes them to the node log itself.
    pub fn with_guest_logs(mut self, sink: Arc<GuestLogSink>) -> Self {
        self.guest_logs = Some(sink);
        self
    }

    /// Resolve continue-as-new successors through `modules` / `policy` (the server wires
    /// the meta-backed ones). Without this, a successor on the run's own workflow and
    /// version reuses its module and retention, and any other successor fails the run.
    pub fn with_resolvers(
        mut self,
        modules: Arc<dyn ModuleResolver>,
        policy: Arc<dyn PolicyResolver>,
    ) -> Self {
        self.modules = modules;
        self.policy = policy;
        self
    }

    pub fn run(&self) -> RunId {
        self.run
    }

    pub fn token(&self) -> AttemptToken {
        self.token
    }

    pub fn handle(&self) -> &PartitionHandle<L> {
        &self.handle
    }

    /// The journal end as last observed: after the last accepted append (including
    /// unsupported completions), or the `expected` of the last `SeqMismatch`. The executor's
    /// `Suspend` / `EnqueueReady` compare-and-set uses it as `from_seq`.
    pub fn next_seq(&self) -> u64 {
        self.next_seq.load(Ordering::Acquire)
    }

    /// True once an append carrying `CompleteRun` was accepted through this sink.
    pub fn is_terminal(&self) -> bool {
        self.terminal.load(Ordering::Acquire)
    }

    fn lock_outcome(&self) -> MutexGuard<'_, Option<ActivationOutcome>> {
        self.outcome.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The outcome reported through [`ActivationSink::done`], if any.
    pub fn outcome(&self) -> Option<ActivationOutcome> {
        self.lock_outcome().clone()
    }

    /// Take the reported outcome.
    pub fn take_outcome(&self) -> Option<ActivationOutcome> {
        self.lock_outcome().take()
    }

    fn command(&self, from_seq: u64, events: Vec<JournalEvent>, derived: Vec<Derived>) -> Command {
        Command::events_appended(EventsAppended {
            run_id: self.run.as_bytes().to_vec(),
            token: Some(AttemptTokenMsg::from(self.token)),
            from_seq,
            events,
            derived,
        })
    }

    /// Submit and map the outcome; `Ok(())` means accepted. Updates `next_seq` on accept
    /// (`from_seq + n`) and on `SeqMismatch` (`expected`).
    async fn submit(
        &self,
        from_seq: u64,
        events: Vec<JournalEvent>,
        derived: Vec<Derived>,
    ) -> Result<u64, AppendError> {
        let n = events.len() as u64;
        let completes = derived
            .iter()
            .any(|d| matches!(d.intent, Some(Intent::CompleteRun(_))));
        let cmd = self.command(from_seq, events, derived);
        match self.handle.submit_one(cmd).await {
            Ok(ApplyOutcome::Accepted) => {
                let next = from_seq + n;
                self.next_seq.store(next, Ordering::Release);
                if completes {
                    self.terminal.store(true, Ordering::Release);
                }
                if let Some(f) = &self.on_renew {
                    f();
                }
                Ok(next)
            }
            Ok(ApplyOutcome::Rejected(r)) => Err(self.map_reject(r)),
            Ok(other) => Err(AppendError::Unavailable(format!(
                "unexpected outcome {other:?}"
            ))),
            // NotLeader, Failed, Log, AckLost, ...: the outcome is unknown or the
            // partition is gone; the engine evicts and the run is re-dispatched.
            Err(e) => Err(unavailable(e)),
        }
    }

    fn map_reject(&self, r: RejectReason) -> AppendError {
        match r {
            RejectReason::Fenced { .. }
            | RejectReason::StaleEpoch { .. }
            | RejectReason::RunTerminal
            | RejectReason::RunNotFound
            // Suspend on a run that is no longer Running (04-12): treat like Fenced.
            | RejectReason::Invalid(InvalidCode::NotRunning) => AppendError::Fenced,
            // 07-04 (W13): the run was paused; the dispatcher already revoked it, so stop.
            RejectReason::RunPaused => AppendError::Fenced,
            RejectReason::SeqMismatch { expected } => {
                self.next_seq.store(expected, Ordering::Release);
                AppendError::Conflict { expected }
            }
            other => AppendError::Unavailable(format!("rejected: {other:?}")),
        }
    }

    async fn prepare(&self, bytes: Vec<u8>) -> Result<PayloadRef, AppendError> {
        self.handle
            .prepare_payload(Bytes::from(bytes))
            .await
            .map_err(unavailable)
    }

    async fn encode(&self, events: &[Event]) -> Result<Vec<JournalEvent>, AppendError> {
        let mut out = Vec::with_capacity(events.len());
        for ev in events {
            let (kind, bytes) = encode_event(ev);
            out.push(JournalEvent {
                seq: ev.seq,
                kind,
                payload: Some(self.prepare(bytes).await?),
                ..Default::default()
            });
        }
        Ok(out)
    }

    /// Submit intents only (no events) at [`next_seq`](Self::next_seq): the executor's
    /// `Suspend` / `EnqueueReady` / failure `CompleteRun`. `Conflict{expected}` means the
    /// journal moved (re-activate); `Fenced` also covers `Invalid(NotRunning)`.
    pub async fn submit_intents(&self, derived: Vec<Derived>) -> Result<u64, AppendError> {
        self.submit(self.next_seq(), Vec::new(), derived).await
    }

    /// Submit intents only at an explicit `from_seq` (04-16). The executor's `Suspend` /
    /// `EnqueueReady` CAS uses the journal end **as the engine saw it**, not
    /// [`next_seq`](Self::next_seq): an unsupported completion journaled by this sink but
    /// never delivered to the engine must fail the CAS (`Conflict` -> re-activate), or the
    /// run would be suspended over an unconsumed input (04-13 lost wake-up rule).
    pub async fn submit_intents_at(
        &self,
        from_seq: u64,
        derived: Vec<Derived>,
    ) -> Result<u64, AppendError> {
        self.submit(from_seq, Vec::new(), derived).await
    }

    /// Journal input events at the journal end (renumbered from `next_seq`), retrying when
    /// the end moved; on accept each event is sent on `inputs`. Fenced / terminal runs are
    /// not an error here: the activation learns about it on its next append.
    async fn journal_inputs(
        &self,
        bodies: Vec<EventBody>,
        ts_nanos: u64,
    ) -> Result<(), AppendError> {
        if bodies.is_empty() {
            return Ok(());
        }
        for _ in 0..INPUT_RETRIES {
            let from = self.next_seq();
            let events: Vec<Event> = bodies
                .iter()
                .enumerate()
                .map(|(i, b)| Event {
                    seq: from + i as u64,
                    ts_nanos,
                    body: b.clone(),
                })
                .collect();
            let jevs = self.encode(&events).await?;
            match self.submit(from, jevs, Vec::new()).await {
                Ok(_) => {
                    if let Some(tx) = &self.inputs {
                        for ev in events {
                            // A closed channel (activation ended) leaves the input for the
                            // next activation, which reads it from the journal.
                            let _ = tx.send(ev);
                        }
                    }
                    return Ok(());
                }
                Err(AppendError::Conflict { .. }) => continue,
                Err(AppendError::Fenced) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
        Err(AppendError::Unavailable(format!(
            "journal end kept moving while journaling {} post-flush completion(s)",
            bodies.len()
        )))
    }

    /// Journal the unsupported completion of every effect / child in `journal` that has no
    /// completion yet (recovery after a failed second append; call on the journal loaded
    /// for an activation, before activating). No-op for terminal runs.
    pub async fn repair_unsupported(&self, journal: &[Event]) -> Result<(), AppendError> {
        let completed: std::collections::BTreeSet<u64> = journal
            .iter()
            .filter_map(|e| e.body.completes_op())
            .collect();
        let ended = journal.iter().any(|e| {
            matches!(
                e.body,
                EventBody::RunCompleted(_)
                    | EventBody::RunFailed(_)
                    | EventBody::RunContinuedAsNew(_)
            )
        });
        if ended {
            return Ok(());
        }
        let bodies: Vec<EventBody> = journal
            .iter()
            .filter(|e| !completed.contains(&e.seq))
            .filter_map(|e| self.unsupported_input(e))
            .collect();
        let ts = journal.iter().map(|e| e.ts_nanos).max().unwrap_or(0);
        self.journal_inputs(bodies, ts).await?;
        self.repair_children(journal).await
    }

    /// The child repair rule on journal load (09-05, W1; module docs).
    async fn repair_children(&self, journal: &[Event]) -> Result<(), AppendError> {
        if !journal
            .iter()
            .any(|e| matches!(e.body, EventBody::ChildStarted(_)))
        {
            return Ok(());
        }
        let rec = self.parent_record()?;
        let partitions = self.partitions()?;
        let reader = self.handle.reader();
        let has_row = |child: &RunId| {
            reader
                .get(Table::Children, &children_key(&self.run, child))
                .map(|r| r.is_some())
                // An unreadable row is treated as present: never re-decide on doubt.
                .unwrap_or(true)
        };
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        let plan = children::plan_child_repairs(
            &self.run,
            partitions,
            rec.tenant.as_deref(),
            journal,
            &has_row,
            &*self.modules,
            rec.retention_ms,
            now_ms,
        );
        let mut resend = Vec::new();
        // Post-repair completions by op (journaled in op order).
        let mut inputs = BTreeMap::new();
        for r in plan {
            match r {
                ChildRepair::Resend { op, target } | ChildRepair::Detached { op, target } => {
                    let Some(ev) = journal.iter().find(|e| e.seq == op) else {
                        continue;
                    };
                    let EventBody::ChildStarted(c) = &ev.body else {
                        continue;
                    };
                    let start = self.child_start(&rec, partitions, ev, c, &target).await?;
                    let child = children::child_run_id(&self.run, op, partitions);
                    resend.push(Derived::new(Intent::SendMessage(
                        ostro_types::command::SendMessage {
                            msg: Some(ostro_types::command::OutboxMsg {
                                kind: Some(ostro_types::command::outbox_msg::Kind::StartChild(
                                    ostro_types::command::StartChild { start: Some(start) },
                                )),
                            }),
                        },
                    )));
                    if c.options.detached {
                        // 09-08 (D-P9-10): the detached op completes with its run id.
                        inputs.insert(op, children::detached_completion(op, &child));
                    } else {
                        resend.push(Derived::child_journal(&child));
                    }
                }
                ChildRepair::UnknownWorkflow { op, message } => {
                    tracing::warn!(run = %self.run_str, op, %message, "child target unresolvable");
                    inputs.insert(op, children::unknown_workflow_completion(op, &message));
                }
                ChildRepair::Expired { op } => {
                    tracing::warn!(run = %self.run_str, op, "child not re-sent: parent resumed after its retention window");
                    inputs.insert(op, children::expired_child_completion(op));
                }
            }
        }
        let unknown: Vec<EventBody> = inputs.into_values().collect();
        if !resend.is_empty() {
            // Intents-only EventsAppended (apply accepts `events = []`, D-P9-25).
            match self.submit_intents(resend).await {
                Ok(_) | Err(AppendError::Fenced) => {}
                Err(e) => return Err(e),
            }
        }
        let ts = journal.iter().map(|e| e.ts_nanos).max().unwrap_or(0);
        self.journal_inputs(unknown, ts).await
    }

    /// The run's own record (children need its tenant, priority and traceparent).
    fn parent_record(&self) -> Result<ostro_partition::store::values::RunRecord, AppendError> {
        self.handle
            .read_run(&self.run)
            .map_err(unavailable)?
            .ok_or_else(|| unavailable(format!("run {} has no record", self.run_str)))
    }

    /// The node's partition count (child run ids route over it, D-P9-7).
    fn partitions(&self) -> Result<u16, AppendError> {
        Ok(self
            .handle
            .store()
            .read_meta()
            .map_err(unavailable)?
            .count
            .get())
    }

    /// The child `StartRun` of the `ChildStarted` `ev` (input prepared, fresh seed).
    async fn child_start(
        &self,
        rec: &ostro_partition::store::values::RunRecord,
        partitions: u16,
        ev: &Event,
        c: &ostro_types::journal::ChildStarted,
        target: &children::ChildTarget,
    ) -> Result<StartRun, AppendError> {
        let mut rng_seed = [0u8; 32];
        getrandom::fill(&mut rng_seed).map_err(unavailable)?;
        let input = self.prepare(c.input.data.clone()).await?;
        let parts = children::ChildParts {
            parent_id: self.run,
            parent: rec,
            op: ev.seq,
            ts_nanos: ev.ts_nanos,
            detached: c.options.detached,
            run_id: children::child_run_id(&self.run, ev.seq, partitions),
            rng_seed,
            input,
        };
        Ok(children::child_start(target, parts, &*self.policy))
    }

    /// Child starts of `events` (09-05): prepared `StartRun`s by op, and the
    /// `unknown_workflow` completions of unresolvable targets (by op).
    async fn prepare_children(
        &self,
        events: &[Event],
    ) -> Result<(BTreeMap<u64, StartRun>, BTreeMap<u64, EventBody>), AppendError> {
        let mut starts = BTreeMap::new();
        let mut unknown = BTreeMap::new();
        if !events
            .iter()
            .any(|e| matches!(e.body, EventBody::ChildStarted(_)))
        {
            return Ok((starts, unknown));
        }
        let rec = self.parent_record()?;
        let partitions = self.partitions()?;
        for ev in events {
            let EventBody::ChildStarted(c) = &ev.body else {
                continue;
            };
            match children::resolve_child(
                &*self.modules,
                rec.tenant.as_deref(),
                &c.workflow,
                &c.options,
            ) {
                Ok(target) => {
                    let start = self.child_start(&rec, partitions, ev, c, &target).await?;
                    starts.insert(ev.seq, start);
                }
                Err(e) => {
                    tracing::warn!(run = %self.run_str, op = ev.seq, error = %e, "child target unresolvable");
                    unknown.insert(
                        ev.seq,
                        children::unknown_workflow_completion(ev.seq, &e.to_string()),
                    );
                }
            }
        }
        Ok((starts, unknown))
    }

    /// The prepared `CompleteRun.parent_event` of a child run's guest terminal batch
    /// (PF-F1, D-P9-9); `None` for top-level runs, continue-as-new and other batches.
    async fn parent_event(&self, events: &[Event]) -> Result<Option<PayloadRef>, AppendError> {
        let Some((_, op)) = self.parent else {
            return Ok(None);
        };
        let Some((ev, outcome)) = events
            .last()
            .and_then(|ev| children::parent_outcome(ev).map(|o| (ev, o)))
        else {
            return Ok(None);
        };
        let bytes = children::parent_event_bytes(op, outcome, ev.ts_nanos);
        Ok(Some(self.prepare(bytes).await?))
    }

    /// The successor start and result bytes of a continue-as-new batch (module docs).
    async fn successor(
        &self,
        ev: &Event,
        cont: &ostro_types::journal::Continuation,
    ) -> Result<(Option<ostro_types::command::StartRun>, Vec<u8>), AppendError> {
        let pred = self
            .handle
            .read_run(&self.run)
            .map_err(unavailable)?
            .ok_or_else(|| unavailable(format!("run {} has no record", self.run_str)))?;
        let run_id = RunId::new_v7(self.run.partition());
        let mut rng_seed = [0u8; 32];
        getrandom::fill(&mut rng_seed).map_err(unavailable)?;
        let input = self.prepare(cont.input.data.clone()).await?;
        let parts = derive::SuccessorParts {
            pred_id: self.run,
            pred: &pred,
            run_id,
            rng_seed,
            input,
            at_ms: derive::ceil_ms(ev.ts_nanos),
        };
        match derive::successor_start(cont, parts, &*self.modules, &*self.policy) {
            Ok(start) => Ok((Some(start), derive::continued_as_new_json(&run_id))),
            Err(e) => {
                tracing::warn!(run = %self.run_str, error = %e, "continue-as-new failed");
                Ok((
                    None,
                    derive::failure_json(
                        derive::REASON_CONTINUE_AS_NEW_FAILED,
                        derive::REASON_CONTINUE_AS_NEW_FAILED,
                        &e.to_string(),
                    ),
                ))
            }
        }
    }

    /// Bodies of earlier journal events named by `OpCancelled` but not in `events`.
    async fn prior_events(
        &self,
        events: &[Event],
    ) -> Result<BTreeMap<u64, EventBody>, AppendError> {
        let mut out = BTreeMap::new();
        for ev in events {
            if let EventBody::OpCancelled(c) = &ev.body
                && !events.iter().any(|e| e.seq == c.op)
                && !out.contains_key(&c.op)
                && let Some(p) = journal_io::read_event(&self.handle, &self.run, c.op)
                    .await
                    .map_err(unavailable)?
            {
                out.insert(c.op, p.body);
            }
        }
        Ok(out)
    }
}

#[async_trait::async_trait]
impl<L: ReplicatedLog> ActivationSink for PartitionSink<L> {
    async fn append(
        &self,
        run: &RunRef,
        attempt: u32,
        from_seq: u64,
        events: Vec<Event>,
    ) -> Result<u64, AppendError> {
        if attempt != self.token.n {
            return Err(AppendError::Fenced);
        }
        if run.run_id != self.run_str || run.partition != self.run.partition().0 {
            return Err(AppendError::Unavailable(format!(
                "sink for run {} got an append for {}/{}",
                self.run_str, run.partition, run.run_id
            )));
        }
        // 1. Terminal result payload (blob store first when large), and the
        //    continue-as-new successor.
        let (successor, bytes) = match derive::continuation(&events) {
            Some((ev, cont)) => {
                let (s, b) = self.successor(ev, cont).await?;
                (s, Some(b))
            }
            None => (None, derive::terminal_result_bytes(&events)),
        };
        let result = match bytes {
            Some(b) => Some(self.prepare(b).await?),
            None => None,
        };
        // Children (09-05): resolved/prepared starts, unresolvable targets, parent event.
        let (starts, mut unknown_children) = self.prepare_children(&events).await?;
        let parent_event = self.parent_event(&events).await?;
        if parent_event.is_some() {
            failpoint!("child::parent_event_before_flush")?;
        }
        // 09-08: child ids of cancelled child ops (the partition count is read only then),
        // and the `{run_id}` completions of detached starts (journaled after the flush).
        let partitions = if events
            .iter()
            .any(|e| matches!(e.body, EventBody::OpCancelled(_)))
        {
            Some(self.partitions()?)
        } else {
            None
        };
        let mut detached: BTreeMap<u64, EventBody> = starts
            .iter()
            .filter(|(_, s)| s.parent.is_none())
            .filter_map(|(op, s)| {
                let child = RunId::from_bytes(&s.run_id).ok()?;
                Some((*op, children::detached_completion(*op, &child)))
            })
            .collect();
        let has_child_start = events
            .iter()
            .any(|e| matches!(e.body, EventBody::ChildStarted(_)));
        // 2. Intents (pure).
        let prior = self.prior_events(&events).await?;
        let derived = derive::derive_with_children(
            &events,
            derive::DeriveCtx {
                prior: &|op| prior.get(&op).cloned(),
                result,
                successor,
            },
            derive::ChildCtx {
                starts: &|op| starts.get(&op).cloned(),
                parent_event,
                child_run: &|op| partitions.map(|p| children::child_run_id(&self.run, op, p)),
            },
        )
        .map_err(unavailable)?;
        // Effect intents exist only with a bridge (without one nothing would ever complete
        // or clean up the rows; the unsupported path below completes the effect instead).
        let derived: Vec<Derived> = if self.bridge.is_some() {
            derived
        } else {
            derived
                .into_iter()
                .filter(|d| {
                    !matches!(
                        d.intent,
                        Some(Intent::RegisterEffect(_) | Intent::CancelEffect(_))
                    )
                })
                .collect()
        };
        let cancelled: Vec<u64> = derived
            .iter()
            .filter_map(|d| match &d.intent {
                Some(Intent::CancelEffect(c)) => Some(c.op),
                _ => None,
            })
            .collect();
        let max_ts = events.iter().map(|e| e.ts_nanos).max().unwrap_or(0);
        // 3. Events.
        let jevs = self.encode(&events).await?;
        // 4. Submit.
        let next = self.submit(from_seq, jevs, derived).await?;
        // CancelEffect committed: stop the bridge task (best effort; the gate's effects-row
        // check abandons it anyway).
        if let Some(b) = &self.bridge {
            for op in cancelled {
                b.bridge().cancel(run.partition, &run.run_id, op);
            }
        }
        if has_child_start {
            failpoint!("child::start_child_after_flush")?;
        }
        // Post-flush completions after the scheduling append (same token): unsupported
        // effects without a bridge, then unresolvable children (09-05) and detached
        // children (09-08).
        if !self.is_terminal() {
            if !detached.is_empty() {
                failpoint!("child::detached_after_flush")?;
            }
            // In op order, like the scheduling events.
            let bodies: Vec<EventBody> = events
                .iter()
                .filter_map(|e| {
                    self.unsupported_input(e)
                        .or_else(|| unknown_children.remove(&e.seq))
                        .or_else(|| detached.remove(&e.seq))
                })
                .collect();
            self.journal_inputs(bodies, max_ts).await?;
        }
        Ok(next)
    }

    async fn fetch_blob(&self, sha: &[u8; 32]) -> Result<Vec<u8>, AppendError> {
        let blobs = Arc::clone(self.handle.blobs());
        let sha = *sha;
        tokio::task::spawn_blocking(move || {
            let probe = ostro_blob::BlobRef {
                sha256: sha,
                len: 0,
            };
            let len = std::fs::metadata(blobs.path_of(&probe))
                .map_err(|e| unavailable(format!("blob {}: {e}", ostro_blob::hex(&sha))))?
                .len();
            blobs
                .get(&ostro_blob::BlobRef { sha256: sha, len })
                .map_err(unavailable)
        })
        .await
        .map_err(unavailable)?
    }

    async fn done(&self, _run: &RunRef, _attempt: u32, outcome: ActivationOutcome) {
        *self.lock_outcome() = Some(outcome);
    }

    fn handles_guest_logs(&self) -> bool {
        self.guest_logs.is_some()
    }

    fn effect_driver(&self) -> Option<Arc<dyn ostro_engine::EffectDriver>> {
        self.bridge
            .as_ref()
            .map(|b| Arc::clone(b.driver()) as Arc<dyn ostro_engine::EffectDriver>)
    }

    fn guest_log(&self, rec: GuestLogRecord) {
        if let Some(g) = &self.guest_logs {
            g.emit(rec);
        }
    }
}
