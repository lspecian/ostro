//! `PartitionSink`: the real [`ActivationSink`] over a [`PartitionHandle`] (plan 04-15).
//!
//! One sink per activation. It holds the activation's [`AttemptToken`]; every engine flush
//! becomes exactly one fenced `EventsAppended` command:
//!
//! 1. the result payload of a terminal event is prepared (blob store first when > 256 KiB),
//! 2. intents come from the pure [`derive::intents`] mapping,
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
//! # Phase 4 "unsupported" completions (locked decision; Phase 6 replaces this)
//!
//! Effects, child workflows and await tokens are not implemented in Phase 4. Instead of
//! hanging, the run gets an immediate non-retryable failure:
//!
//! - `EffectScheduled{seq s}` -> input `EffectCompleted{op: s, outcome: Err(NonRetryable{kind:
//!   "unsupported"})}`; `ChildStarted` -> `ChildCompleted` likewise. They are journaled with
//!   the **same token** in a second `EventsAppended` right after the scheduling append
//!   succeeded, and sent on the activation's `inputs` channel (hot wait). The engine's own
//!   next append then usually conflicts (`AppendConflict` -> re-activation), which is
//!   correct: the re-activation replays the completion.
//! - `AwaitTokenCreated` -> `CompleteRun{Failed, reason unsupported}` in the **same**
//!   append (atomic; `TokenCompleted` has no failure variant). Later appends are rejected
//!   `RunTerminal` and map to `Fenced`.
//!
//! Completions are skipped when the batch itself ends the run. If journaling them fails
//! with an unavailable partition, `append` returns `Unavailable` although the scheduling
//! events are committed; the executor calls [`PartitionSink::repair_unsupported`] on the
//! journal it loads for the next activation so no effect is left without a completion.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;
use ostro_engine::sink::{ActivationOutcome, ActivationSink, AppendError, RunRef};
use ostro_log::{ReplicatedLog, Rf1Log};
use ostro_partition::apply::InvalidCode;
use ostro_partition::{ApplyOutcome, PartitionHandle, RejectReason};
use ostro_types::command::{
    AttemptTokenMsg, CompleteRun, Derived, EventsAppended, JournalEvent, TerminalStatus,
    derived::Intent,
};
use ostro_types::journal::{
    ChildCompleted, EffectCompleted, ErrorInfo, Event, EventBody, Failure, OpOutput, encode_event,
    encode_result,
};
use ostro_types::{AttemptToken, Command, PayloadRef, RunId};
use tokio::sync::mpsc::UnboundedSender;

use crate::derive;
use crate::journal_io;

/// Error kind of every Phase 4 unsupported completion.
pub const UNSUPPORTED_KIND: &str = "unsupported";
/// Message of the unsupported effect completion.
pub const EFFECTS_UNSUPPORTED: &str = "effects arrive in Phase 6";
/// Message of the unsupported child completion.
pub const CHILDREN_UNSUPPORTED: &str = "child workflows arrive in Phase 6/9";
/// Detail of the run failure after `AwaitTokenCreated`.
pub const TOKENS_UNSUPPORTED: &str = "await tokens arrive in Phase 5";

/// Attempts to journal unsupported completions when the journal end moves underneath
/// (an input such as `TimerFired` landed between the two appends).
const INPUT_RETRIES: usize = 4;

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

/// The unsupported completion input for an event that schedules an op, if any.
fn unsupported_input(ev: &Event) -> Option<EventBody> {
    let out: Result<OpOutput, Failure>;
    match &ev.body {
        EventBody::EffectScheduled(_) => {
            out = Err(unsupported_failure(EFFECTS_UNSUPPORTED));
            Some(EventBody::EffectCompleted(EffectCompleted {
                op: ev.seq,
                outcome: encode_result(&out),
            }))
        }
        EventBody::ChildStarted(_) => {
            out = Err(unsupported_failure(CHILDREN_UNSUPPORTED));
            Some(EventBody::ChildCompleted(ChildCompleted {
                op: ev.seq,
                outcome: encode_result(&out),
            }))
        }
        _ => None,
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
        let next = handle
            .read_run(&run)
            .ok()
            .flatten()
            .map_or(0, |r| r.next_seq);
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
        }
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
            "journal end kept moving while journaling {} unsupported completion(s)",
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
            .filter_map(unsupported_input)
            .collect();
        let ts = journal.iter().map(|e| e.ts_nanos).max().unwrap_or(0);
        self.journal_inputs(bodies, ts).await
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
        // 1. Terminal result payload (blob store first when large).
        let result = match derive::terminal_result_bytes(&events) {
            Some(b) => Some(self.prepare(b).await?),
            None => None,
        };
        // 2. Intents (pure).
        let prior = self.prior_events(&events).await?;
        let mut derived =
            derive::intents(&events, &|op| prior.get(&op).cloned(), result).map_err(unavailable)?;
        let ends_run = derived
            .iter()
            .any(|d| matches!(d.intent, Some(Intent::CompleteRun(_))));
        let max_ts = events.iter().map(|e| e.ts_nanos).max().unwrap_or(0);
        // Phase 4: an await token fails the run in the same append.
        if !ends_run
            && let Some(tok) = events
                .iter()
                .find(|e| matches!(e.body, EventBody::AwaitTokenCreated(_)))
        {
            let r = self
                .prepare(derive::unsupported_json(TOKENS_UNSUPPORTED))
                .await?;
            derived.push(Derived::new(Intent::CompleteRun(CompleteRun {
                status: TerminalStatus::Failed as i32,
                result: Some(r),
                completed_at_ms: derive::ceil_ms(tok.ts_nanos),
            })));
        }
        // 3. Events.
        let jevs = self.encode(&events).await?;
        // 4. Submit.
        let next = self.submit(from_seq, jevs, derived).await?;
        // Phase 4: unsupported completions of effects / children, after the scheduling
        // append (same token).
        if !self.is_terminal() {
            let bodies: Vec<EventBody> = events.iter().filter_map(unsupported_input).collect();
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
}
