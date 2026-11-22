//! Journal cursor, command matching and nondeterminism detection (ENG-03/ENG-04).
//!
//! The journal is the run's full event list: `journal[0]` is `RunStarted` and seqs are
//! contiguous from 0, so `journal[seq] .seq == seq`. Guest-command events (codes 16–63)
//! are matched one by one against the commands the guest issues; input events (timer
//! fired, effect completed, ...) are skipped by the cursor and never compared. Once no
//! recorded guest command is left, commands run live: each gets the next seq, is stamped
//! with the clock, and is appended to both the journal (so op lookups see it) and the
//! pending batch the caller hands to the sink.
//!
//! Step retry (WF-02, plan 04-04): a step whose attempts failed and were retried is
//! journaled as a chain of `StepAttemptFailed` events (same command hash as the step)
//! followed by its `StepCompleted`. [`ReplayState::begin_step`] consumes the whole chain;
//! if the chain is not yet closed by a completion the step goes live at attempt `k + 1`,
//! so the attempt count always comes from the journal and never resets on restart.
//!
//! Retry logical clock (D22, plan 05-23): an attempt that re-runs after a
//! `StepAttemptFailed` observes `now = max(last journaled ts, retry_at_nanos)` of that
//! failure. Live, [`ReplayState::record_attempt_failed`] raises the clock right after
//! appending the failure (the host sleeps until `retry_at` before the next in-activation
//! attempt). Resuming a requeued attempt, [`ReplayState::begin_step`] raises it after
//! consuming the chain, but only once the attempt is due (`retry_at <= live now`); a
//! not-yet-due attempt is handed back by the host without running, so the backoff stays
//! visible. Every later live event is stamped at least the raised value, so the clock
//! stays monotonic, and the value comes only from kind 29's `retry_at_nanos`, so record
//! and replay agree. A fully recorded chain is consumed through its `StepCompleted`,
//! whose ts then defines `now` exactly as it was recorded.
//!
//! Cancellation point at a step retry (D25, plan 05-27): the guest is "handed" the run's
//! cancellation when a wait it issued resolves with [`CANCELLATION_OP`] in its completed list
//! (live or replayed `WaitResolved`) or when a step resolves with `Err(cancelled)` after the
//! `CancelRequested`. [`ReplayState::cancel_pending`] is true while a `CancelRequested` is in
//! the journal and the guest has not been handed it yet; the host then resolves a step that
//! is about to be retried as `Err(cancelled)` instead of running another attempt. Both inputs
//! of the decision (the `CancelRequested` seq and the guest's own command sequence up to the
//! retry point) come from the journal, so record and replay agree.
//!
//! Abandoned live steps (09-07): a live `begin_step` reserves a seq but journals nothing
//! until the step completes or fails an attempt; a step the guest drops or leaves open
//! when its run function returns is abandoned and its seq released, so the journal has
//! no trace of it. On replay the same guest issues the same `begin_step` and finds the
//! next recorded guest command is not this step (the following command, or the
//! terminal). That alone is not a divergence: the step is opened *unrecorded* at the seq
//! of that next event (the seq the live run reserved), its body runs exactly as it did
//! live (the open-step rule still applies, so it cannot issue journaled commands), and
//! the divergence is reported only if the guest then records an outcome for it
//! (`complete_step` / `record_attempt_failed`), which the live run never did. Dropping it
//! or returning matches the live run, and the next command is compared as usual.
//!
//! Completion index (05-03): `completions` maps every op to the seq of the first event
//! that completes it ([`completion_target`](super::wait::completion_target): an input
//! with `completes_op`, or the guest's own `OpCancelled`). It is filled when the journal
//! is loaded, in [`ReplayState::deliver_inputs`] and in every live append, so
//! `completion_for` is a map lookup instead of a journal scan. First completion in
//! journal order wins (D5), so later events for the same op never change it.

use std::collections::BTreeMap;
use std::sync::Arc;

use ostro_types::journal::{
    CANCELLATION_OP, CommandHash, CommandKind, CommandMeta, Event, EventBody, EventKind, HASH_VER,
    OpId, PatchMarker, RunStarted,
};

use crate::env::{Clock, RandomStream};
use crate::error::{NonDeterminism, ProtocolViolation};
use crate::run_limits::{LimitBreach, RunLimits, check_live_append};

use super::rng::{ChaChaStream, RngStreams, insecure_seed, step_seed, uuid_v7};
use super::wait::completion_target;

const OPEN_STEP: &str = "journaled command inside an open step";
const RUN_RETURNED: &str = "run returned";

/// Result of a journaled guest command.
#[derive(Debug)]
pub enum CommandResult<'a> {
    /// The command matched the recorded event; its recorded result is authoritative.
    Replayed(&'a Event),
    /// The command ran live and its event was appended at `seq` (`seq` is also the op id).
    Live { seq: u64, ts_nanos: u64 },
}

/// Result of [`ReplayState::begin_step`].
#[derive(Debug)]
pub enum StepBegin<'a> {
    /// The step completed in a previous activation: the recorded `StepCompleted`.
    Replayed(&'a Event),
    /// The step runs live; `seq` is reserved for its `StepCompleted` (or the next
    /// `StepAttemptFailed`). `attempt` is 1-based: `1 + ` the number of recorded failed
    /// attempts consumed. `last_retry_at_nanos` is the `retry_at_nanos` of the last
    /// recorded failed attempt, so the host can re-evict if it is still in the future.
    Live {
        seq: u64,
        attempt: u32,
        last_retry_at_nanos: Option<u64>,
    },
}

/// Replay failure.
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    /// The guest diverged from its journal.
    #[error(transparent)]
    NonDeterminism(NonDeterminism),
    /// The guest (or adapter) misused the protocol.
    #[error(transparent)]
    Protocol(ProtocolViolation),
    /// The journal or a delivered input batch is malformed.
    #[error("bad journal: {0}")]
    BadJournal(String),
    /// A live append would exceed a run limit (11-06; never raised on replay).
    #[error(transparent)]
    LimitExceeded(LimitBreach),
}

pub(super) fn protocol(msg: impl Into<String>) -> ReplayError {
    ReplayError::Protocol(ProtocolViolation(msg.into()))
}

/// First 4 hash bytes as 8 lowercase hex chars.
pub(super) fn hash_hex8(hash: &CommandHash) -> String {
    hash.0[..4].iter().map(|b| format!("{b:02x}")).collect()
}

/// Whether a recorded event kind belongs to the command family of `cmd`.
///
/// Families are coarse (e.g. one `EffectScheduled` for http/sql/object); the command
/// hash (plan 03-02) covers the precise `CommandKind` byte, so Sleep vs SleepUntil or
/// Http vs Sql is still caught by the hash comparison.
pub(super) fn family_matches(ev: EventKind, cmd: CommandKind) -> bool {
    use CommandKind as C;
    use EventKind as E;
    matches!(
        (ev, cmd),
        (E::StepCompleted | E::StepAttemptFailed, C::Step)
            | (E::TimerStarted, C::Sleep | C::SleepUntil)
            | (E::EffectScheduled, C::Http | C::Sql | C::Object)
            | (E::SignalWaitStarted, C::WaitSignal)
            | (E::ChildStarted, C::StartChild)
            | (E::AwaitTokenCreated, C::CreateAwaitToken)
            | (E::OpCancelled, C::CancelOp)
            | (E::WaitResolved, C::Wait)
            | (E::PatchMarker, C::Patch)
            | (E::StatusPublished, C::SetStatus)
            | (
                E::RunCompleted | E::RunFailed | E::RunContinuedAsNew,
                C::Terminal
            )
    )
}

fn is_terminal(kind: EventKind) -> bool {
    matches!(
        kind,
        EventKind::RunCompleted | EventKind::RunFailed | EventKind::RunContinuedAsNew
    )
}

/// Replay state of one run (one activation's view of its journal).
pub struct ReplayState {
    pub(super) journal: Vec<Event>,
    /// Index of the first journal event not yet consumed by a guest command.
    pub(super) cursor: usize,
    pub(super) next_seq: u64,
    pub(super) pending: Vec<Event>,
    pub(super) run: RunStarted,
    pub(super) rng: RngStreams,
    /// Run-level streams saved while a live step's step-local streams are active.
    pub(super) outer_rng: Option<RngStreams>,
    /// Seq reserved by a live `begin_step` that has not completed yet.
    pub(super) open_step: Option<u64>,
    /// The open step is an unrecorded replay of an abandoned live step (see the module
    /// doc): recording an outcome for it is this divergence.
    pub(super) unrecorded_step: Option<NonDeterminism>,
    /// Ts of the most recent consumed/appended guest command (`accepted_at` before any),
    /// raised to `retry_at` for a re-run step attempt (D22, see the module doc).
    pub(super) last_cmd_ts: u64,
    /// Seq of the most recent consumed/appended guest command (0 before any).
    pub(super) last_cmd_seq: u64,
    /// Max ts over every event in the journal (recorded, delivered inputs and live
    /// appends). Live events are stamped at least this, so journal ts never decrease
    /// across a live append and `now()` after a wait is never earlier than the input
    /// (e.g. `TimerFired`) that resolved it, whatever the node's wall clock says.
    pub(super) max_ts: u64,
    /// Op -> seq of its first completion event (see the module doc).
    pub(super) completions: BTreeMap<OpId, u64>,
    /// `patched(change_id)` memo: the first evaluation per id decides (D6).
    pub(super) patched: BTreeMap<String, bool>,
    /// The guest has been handed the run's cancellation (D25, see the module doc).
    pub(super) cancel_observed: bool,
    pub(super) clock: Arc<dyn Clock>,
    /// Run limits checked on every live append (11-06; default [`RunLimits::default`]).
    pub(super) limits: RunLimits,
    /// `ChildStarted` events in the journal (recorded and live), for the children limit.
    pub(super) children: u64,
}

impl std::fmt::Debug for ReplayState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayState")
            .field("journal_len", &self.journal.len())
            .field("cursor", &self.cursor)
            .field("next_seq", &self.next_seq)
            .field("pending", &self.pending.len())
            .field("open_step", &self.open_step)
            .field("last_cmd_seq", &self.last_cmd_seq)
            .finish_non_exhaustive()
    }
}

impl ReplayState {
    /// Load a run's journal. `journal[0]` must be `RunStarted` and seqs contiguous from 0.
    pub fn new(journal: Vec<Event>, clock: Arc<dyn Clock>) -> Result<Self, ReplayError> {
        let run = match journal.first() {
            Some(Event {
                body: EventBody::RunStarted(rs),
                ..
            }) => rs.clone(),
            Some(ev) => {
                return Err(ReplayError::BadJournal(format!(
                    "first event is {}, not run started",
                    ev.body.summary()
                )));
            }
            None => return Err(ReplayError::BadJournal("empty journal".into())),
        };
        for (i, ev) in journal.iter().enumerate() {
            if ev.seq != i as u64 {
                return Err(ReplayError::BadJournal(format!(
                    "seq gap: event at index {i} has seq {}",
                    ev.seq
                )));
            }
            if i > 0 && matches!(ev.body, EventBody::RunStarted(_)) {
                return Err(ReplayError::BadJournal(format!(
                    "second run started at seq {i}"
                )));
            }
        }
        let max_ts = journal.iter().map(|e| e.ts_nanos).max().unwrap_or(0);
        let children = journal
            .iter()
            .filter(|e| matches!(e.body, EventBody::ChildStarted(_)))
            .count() as u64;
        let mut completions = BTreeMap::new();
        for ev in &journal {
            if let Some(op) = completion_target(&ev.body) {
                completions.entry(op).or_insert(ev.seq);
            }
        }
        Ok(Self {
            max_ts,
            completions,
            patched: BTreeMap::new(),
            cancel_observed: false,
            next_seq: journal.len() as u64,
            cursor: 1,
            pending: Vec::new(),
            rng: RngStreams::new(&run.rng_seed),
            outer_rng: None,
            open_step: None,
            unrecorded_step: None,
            last_cmd_ts: run.accepted_at_nanos,
            last_cmd_seq: 0,
            run,
            journal,
            clock,
            limits: RunLimits::default(),
            children,
        })
    }

    /// Set the run limits checked on live appends (11-06). Replayed events are never
    /// checked, so any limit is safe for any journal.
    pub fn set_run_limits(&mut self, limits: RunLimits) {
        self.limits = limits;
    }

    /// The run's `RunStarted` record.
    pub fn run(&self) -> &RunStarted {
        &self.run
    }

    /// Index of the next recorded guest-command event at or after the cursor.
    pub(super) fn next_guest_command(&self) -> Option<usize> {
        self.journal[self.cursor.min(self.journal.len())..]
            .iter()
            .position(|e| e.body.is_guest_command())
            .map(|p| self.cursor + p)
    }

    /// True while recorded guest commands remain to be matched.
    pub fn is_replaying(&self) -> bool {
        self.next_guest_command().is_some()
    }

    /// Logical "now": ts of the most recent guest-command event at or before the cursor
    /// (`accepted_at` before the first). Input events never move it directly, but every
    /// live command is stamped at least the max ts of all journaled events (inputs
    /// included), so `now()` after the wait that consumed an input is >= that input's ts.
    /// Inside a step attempt that re-runs after a `StepAttemptFailed` it is
    /// `max(that ts, retry_at_nanos)` (D22).
    pub fn logical_now_nanos(&self) -> u64 {
        self.last_cmd_ts
    }

    /// Live "now": the timestamp a live event appended now would get at least
    /// (`max(clock, last command ts, max journal ts)`). Reads the clock; used only for
    /// live decisions whose result is journaled (step retry `retry_at`, WF-02).
    pub fn live_now_nanos(&self) -> u64 {
        self.clock
            .now_unix_nanos()
            .max(self.last_cmd_ts)
            .max(self.max_ts)
    }

    /// Hash version of the next recorded guest command, so the adapter can hash with the
    /// version that was recorded. `None` when live.
    pub fn next_command_hash_ver(&self) -> Option<u8> {
        self.next_guest_command()
            .and_then(|i| self.journal[i].body.command_meta())
            .map(|m| m.hash_ver)
    }

    pub(super) fn check_no_open_step(&self) -> Result<(), ReplayError> {
        match self.open_step {
            Some(_) => Err(protocol(OPEN_STEP)),
            None => Ok(()),
        }
    }

    /// Consume the recorded guest command at journal index `i`.
    fn consume(&mut self, i: usize) {
        self.cursor = i + 1;
        self.last_cmd_ts = self.journal[i].ts_nanos;
        self.last_cmd_seq = self.journal[i].seq;
    }

    /// Match the next recorded guest command against `(kind, hash)`.
    ///
    /// `Ok(Some(i))`: matched journal index `i` (cursor advanced). `Ok(None)`: no recorded
    /// command left (live). `Err`: divergence.
    pub(super) fn match_next(
        &mut self,
        kind: CommandKind,
        hash: CommandHash,
        actual: &dyn Fn() -> String,
    ) -> Result<Option<usize>, ReplayError> {
        // A recorded marker can only match `patched`; any other command skips the
        // markers the code no longer queries (D6).
        if kind != CommandKind::Patch {
            self.skip_unqueried_markers(None);
        }
        let Some(i) = self.next_guest_command() else {
            return Ok(None);
        };
        let ev = &self.journal[i];
        let meta = ev.body.command_meta().copied();
        match meta {
            Some(m) if family_matches(ev.body.kind(), kind) && m.hash == hash => {
                self.consume(i);
                Ok(Some(i))
            }
            _ => Err(ReplayError::NonDeterminism(NonDeterminism {
                seq: ev.seq,
                expected: expected_text(ev),
                actual: format!("{} #{}", actual(), hash_hex8(&hash)),
            })),
        }
    }

    /// Append a live guest-command event at the next seq; returns `(seq, ts)`.
    ///
    /// Every live append funnels through here, and only live appends do, so this is
    /// where the run limits are checked (11-06): a breach appends nothing.
    pub(super) fn append_live(
        &mut self,
        seq: u64,
        body: EventBody,
    ) -> Result<(u64, u64), ReplayError> {
        debug_assert_eq!(seq, self.next_seq);
        check_live_append(&self.limits, seq, self.children, &body)
            .map_err(ReplayError::LimitExceeded)?;
        if matches!(body, EventBody::ChildStarted(_)) {
            self.children += 1;
        }
        // Logical time never moves backwards, even if the wall clock does or lags the
        // timestamps of committed inputs (timer fire time, a skewed leader's commit ts).
        let ts = self
            .clock
            .now_unix_nanos()
            .max(self.last_cmd_ts)
            .max(self.max_ts);
        let ev = Event {
            seq,
            ts_nanos: ts,
            body,
        };
        self.index_completion(&ev);
        self.journal.push(ev.clone());
        self.pending.push(ev);
        self.next_seq = seq + 1;
        self.cursor = self.journal.len();
        self.max_ts = ts;
        self.last_cmd_ts = ts;
        self.last_cmd_seq = seq;
        Ok((seq, ts))
    }

    /// Record `ev` in the completion index if it completes an op not completed yet.
    fn index_completion(&mut self, ev: &Event) {
        if let Some(op) = completion_target(&ev.body) {
            self.completions.entry(op).or_insert(ev.seq);
        }
    }

    fn check_live_body(kind: CommandKind, body: &EventBody) -> Result<(), ReplayError> {
        if body.command_meta().is_some() && family_matches(body.kind(), kind) {
            Ok(())
        } else {
            Err(protocol(format!(
                "live event {} does not belong to command {kind:?}",
                body.summary()
            )))
        }
    }

    /// Issue a journaled guest command.
    ///
    /// On replay the next recorded guest command must have the same kind family and
    /// hash; its event is returned and nothing is appended. When live, `live(seq)` builds
    /// the event, which is appended at `seq` (the op id).
    pub fn command(
        &mut self,
        kind: CommandKind,
        hash: CommandHash,
        actual: &dyn Fn() -> String,
        live: impl FnOnce(u64) -> EventBody,
    ) -> Result<CommandResult<'_>, ReplayError> {
        self.check_no_open_step()?;
        if let Some(i) = self.match_next(kind, hash, actual)? {
            return Ok(CommandResult::Replayed(&self.journal[i]));
        }
        let seq = self.next_seq;
        let body = live(seq);
        Self::check_live_body(kind, &body)?;
        let (seq, ts_nanos) = self.append_live(seq, body)?;
        Ok(CommandResult::Live { seq, ts_nanos })
    }

    /// `patched(change_id)`, memoised per change-id (D6: "Memoised per change-id;
    /// unqueried `PatchMarker`s are skipped").
    ///
    /// The first evaluation of a change-id in this run decides, and every later call
    /// returns the memo with no journal effect (one marker per id per run):
    /// - live → `true` and a `PatchMarker` is appended;
    /// - replay with this id's marker next (after skipping markers of other ids that
    ///   the code no longer queries) → `true`, the marker is consumed;
    /// - replay where the next guest command is anything else → `false`, nothing is
    ///   consumed, no error. A journal recorded before the patch existed takes the old
    ///   path.
    pub fn patched(&mut self, hash: CommandHash, change_id: &str) -> Result<bool, ReplayError> {
        self.check_no_open_step()?;
        if let Some(&memo) = self.patched.get(change_id) {
            return Ok(memo);
        }
        self.skip_unqueried_markers(Some(hash));
        let active = match self.next_guest_command() {
            Some(i) => {
                let ev = &self.journal[i];
                let matches = ev.body.kind() == EventKind::PatchMarker
                    && ev.body.command_meta().is_some_and(|m| m.hash == hash);
                if matches {
                    self.consume(i);
                }
                matches
            }
            None => {
                let seq = self.next_seq;
                let body = EventBody::PatchMarker(PatchMarker {
                    meta: CommandMeta {
                        hash_ver: HASH_VER,
                        hash,
                    },
                    change_id: change_id.to_owned(),
                });
                self.append_live(seq, body)?;
                true
            }
        };
        self.patched.insert(change_id.to_owned(), active);
        Ok(active)
    }

    /// Unqueried-marker skip (D6, the 03-RESEARCH "patch removed" rule): consume every
    /// recorded `PatchMarker` at the cursor that the code is not querying now (all of
    /// them, or all but the one with hash `keep` when called from
    /// [`patched`](Self::patched)). A module that dropped a `patched(id)` call, or a
    /// journal with a duplicate marker from before the memo, therefore replays without
    /// divergence. Markers have no side effects, so skipping one changes nothing else.
    fn skip_unqueried_markers(&mut self, keep: Option<CommandHash>) {
        while let Some(i) = self.next_guest_command() {
            let ev = &self.journal[i];
            let skip = ev.body.kind() == EventKind::PatchMarker
                && keep.is_none_or(|k| ev.body.command_meta().is_none_or(|m| m.hash != k));
            if !skip {
                break;
            }
            self.consume(i);
        }
    }

    /// Start a step. Replay: consumes any recorded `StepAttemptFailed` chain for this
    /// step, then returns the recorded `StepCompleted` and opens no guard. Live (no
    /// completion recorded): reserves `seq = next_seq`, opens the step guard and reports
    /// the attempt number; nothing is appended until
    /// [`complete_step`](Self::complete_step) or
    /// [`record_attempt_failed`](Self::record_attempt_failed). A step the recorded run
    /// abandoned (the next recorded command is something else) is opened unrecorded at
    /// that command's seq; recording an outcome for it reports the divergence (see the
    /// module doc).
    pub fn begin_step(
        &mut self,
        hash: CommandHash,
        actual: &dyn Fn() -> String,
    ) -> Result<StepBegin<'_>, ReplayError> {
        self.check_no_open_step()?;
        let mut failed: u32 = 0;
        let mut last_retry_at_nanos = None;
        loop {
            let i = match self.match_next(CommandKind::Step, hash, actual) {
                Ok(Some(i)) => i,
                Ok(None) => break,
                // The live run abandoned this step (see the module doc): open it
                // unrecorded at the seq it reserved, the seq of the next recorded event.
                Err(ReplayError::NonDeterminism(nd)) => {
                    let seq = nd.seq;
                    if let Some(at) = last_retry_at_nanos
                        && at <= self.live_now_nanos()
                    {
                        self.raise_to_retry_at(at);
                    }
                    self.open_live_step(seq);
                    self.unrecorded_step = Some(nd);
                    return Ok(StepBegin::Live {
                        seq,
                        attempt: failed.saturating_add(1),
                        last_retry_at_nanos,
                    });
                }
                Err(e) => return Err(e),
            };
            let ev = &self.journal[i];
            match &ev.body {
                EventBody::StepAttemptFailed(f) => {
                    let expected = failed.saturating_add(1);
                    if f.attempt != expected {
                        return Err(ReplayError::BadJournal(format!(
                            "step attempt {} at seq {} where attempt {expected} was expected",
                            f.attempt, ev.seq
                        )));
                    }
                    failed = expected;
                    last_retry_at_nanos = Some(f.retry_at_nanos);
                }
                _ => return Ok(StepBegin::Replayed(&self.journal[i])),
            }
        }
        // D22: a due re-run attempt observes max(last ts, retry_at). A not-yet-due one
        // is handed back by the host without running, so its clock is left alone (the
        // host compares retry_at against `live_now_nanos`).
        if let Some(at) = last_retry_at_nanos
            && at <= self.live_now_nanos()
        {
            self.raise_to_retry_at(at);
        }
        let seq = self.next_seq;
        self.open_live_step(seq);
        Ok(StepBegin::Live {
            seq,
            attempt: failed.saturating_add(1),
            last_retry_at_nanos,
        })
    }

    /// Open the step guard at `seq` with step-local randomness seeded from `seq`. The
    /// run-level streams are saved once (on the first attempt) and restored on close.
    fn open_live_step(&mut self, seq: u64) {
        self.open_step = Some(seq);
        // The closure runs live only: give it step-local randomness so it never moves
        // the run-level streams (which replay, skipping the closure, would not move).
        let local = RngStreams::new(&step_seed(&self.run.rng_seed, seq));
        let previous = std::mem::replace(&mut self.rng, local);
        if self.outer_rng.is_none() {
            self.outer_rng = Some(previous);
        }
    }

    /// Journal a failed attempt of the open step: appends `body` (a
    /// `StepAttemptFailed`) at the step's reserved `seq`, keeps the step open at the next
    /// seq (with a fresh step-local RNG seeded from the NEW seq) and returns that seq,
    /// which is reserved for attempt `k + 1`.
    pub fn record_attempt_failed(&mut self, seq: u64, body: EventBody) -> Result<u64, ReplayError> {
        if self.open_step != Some(seq) {
            return Err(protocol(format!(
                "attempt-failed for step {seq} that is not open"
            )));
        }
        self.check_recorded_step()?;
        let EventBody::StepAttemptFailed(f) = &body else {
            return Err(protocol(format!(
                "attempt-failed with {} instead of a step attempt failure",
                body.summary()
            )));
        };
        let retry_at = f.retry_at_nanos;
        self.append_live(seq, body)?;
        // D22: the next attempt (in-activation after the host's sleep, or requeued and
        // resumed through `begin_step`) observes max(this failure's ts, retry_at).
        self.raise_to_retry_at(retry_at);
        let next = self.next_seq;
        self.open_live_step(next);
        Ok(next)
    }

    /// D22: raise logical time to a failed attempt's `retry_at` (never lowers it).
    fn raise_to_retry_at(&mut self, retry_at_nanos: u64) {
        self.last_cmd_ts = self.last_cmd_ts.max(retry_at_nanos);
    }

    /// Close the open step (if any) and restore the run-level random streams.
    fn close_step(&mut self) {
        self.open_step = None;
        self.unrecorded_step = None;
        if let Some(outer) = self.outer_rng.take() {
            self.rng = outer;
        }
    }

    /// An outcome for an unrecorded (abandoned-live) step: the live run recorded none.
    fn check_recorded_step(&self) -> Result<(), ReplayError> {
        match &self.unrecorded_step {
            Some(nd) => Err(ReplayError::NonDeterminism(nd.clone())),
            None => Ok(()),
        }
    }

    /// Append the `StepCompleted` for the open step at its reserved seq and close the guard.
    pub fn complete_step(&mut self, seq: u64, body: EventBody) -> Result<(), ReplayError> {
        if self.open_step != Some(seq) {
            return Err(protocol(format!(
                "complete-step for step {seq} that is not open"
            )));
        }
        self.check_recorded_step()?;
        if !matches!(body, EventBody::StepCompleted(_)) {
            return Err(protocol(format!(
                "complete-step with {} instead of a step completion",
                body.summary()
            )));
        }
        self.append_live(seq, body)?;
        self.close_step();
        Ok(())
    }

    /// Drop an open step without journaling it; its reserved seq is released.
    pub fn abandon_step(&mut self, seq: u64) {
        if self.open_step == Some(seq) {
            self.close_step();
        }
    }

    /// Seq of the run's `CancelRequested`, if one is in the journal (the completion of
    /// [`CANCELLATION_OP`]).
    pub fn cancel_requested_seq(&self) -> Option<u64> {
        self.completions.get(&CANCELLATION_OP).copied()
    }

    /// D25: a `CancelRequested` is journaled and the guest has not been handed it yet (no
    /// wait of the guest resolved with [`CANCELLATION_OP`], no step resolved as cancelled
    /// after it). While true, a step retry is a cancellation point.
    pub fn cancel_pending(&self) -> bool {
        !self.cancel_observed && self.cancel_requested_seq().is_some()
    }

    /// Record that the guest was handed the cancellation (a step resolved `Err(cancelled)`
    /// at `seq`). A `cancelled` step outcome before the `CancelRequested` is not a hand-off.
    pub fn note_step_cancelled(&mut self, seq: u64) {
        if self.cancel_requested_seq().is_some_and(|c| c < seq) {
            self.cancel_observed = true;
        }
    }

    /// Record a resolved wait: the cancellation is handed over when its completed list
    /// contains [`CANCELLATION_OP`].
    pub(super) fn note_cancel_observed(&mut self, completed: &[OpId]) {
        if completed.contains(&CANCELLATION_OP) {
            self.cancel_observed = true;
        }
    }

    /// Deliver committed input events that arrived while the open live step waits out an
    /// in-activation retry backoff (D25). Same rules as
    /// [`deliver_inputs`](Self::deliver_inputs), except that a step must be open at
    /// `next_seq` with nothing pending (its failed attempt is already flushed and its next
    /// attempt has not started): the step is re-opened at the new `next_seq`, which is
    /// returned (the seq reserved for the next attempt).
    pub fn deliver_inputs_in_backoff(&mut self, events: Vec<Event>) -> Result<u64, ReplayError> {
        let Some(seq) = self.open_step else {
            return Err(protocol("backoff inputs delivered with no open step"));
        };
        if seq != self.next_seq {
            return Err(protocol(format!(
                "backoff inputs delivered while step {seq} is not at the journal end {}",
                self.next_seq
            )));
        }
        self.open_step = None;
        let res = self.deliver_inputs(events);
        // Re-open the step at the (possibly new) journal end, with step-local randomness
        // seeded from that seq, exactly as a resumed attempt opens it in `begin_step`.
        let next = self.next_seq;
        self.open_live_step(next);
        res.map(|()| next)
    }

    /// The guest's run function returned (called before its terminal command is
    /// issued). Unconsumed recorded guest commands are a divergence, except unqueried
    /// patch markers (skipped, D6) and a trailing terminal event, which is compared
    /// through `command(Terminal, ..)`. An open step is abandoned (it never completed,
    /// so nothing was journaled).
    pub fn finish(&mut self) -> Result<(), ReplayError> {
        self.close_step();
        self.skip_unqueried_markers(None);
        match self.next_guest_command() {
            Some(i) if !is_terminal(self.journal[i].body.kind()) => {
                let ev = &self.journal[i];
                Err(ReplayError::NonDeterminism(NonDeterminism {
                    seq: ev.seq,
                    expected: expected_text(ev),
                    actual: RUN_RETURNED.to_owned(),
                }))
            }
            _ => Ok(()),
        }
    }

    /// Drain the live events appended since the last call (the journal keeps them).
    pub fn take_pending(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.pending)
    }

    /// Number of live events not yet taken.
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Append committed input events. They must continue `next_seq` contiguously, be
    /// input events, and arrive only when no live events are pending and no step is open.
    ///
    /// Input events are the completing kinds (`completes_op` is `Some`: timer fired,
    /// effect/child/token completed, signal received/matched, wait timed out, cancel
    /// requested) plus the apply-written kinds that complete nothing: `SignalPayload`
    /// (96, a delivered signal that a later `SignalMatched` references, D21) and
    /// `RunTerminated` (71, a history marker, D3). Guest commands are rejected.
    pub fn deliver_inputs(&mut self, events: Vec<Event>) -> Result<(), ReplayError> {
        if !self.pending.is_empty() {
            return Err(ReplayError::BadJournal(format!(
                "inputs delivered with {} uncommitted live events",
                self.pending.len()
            )));
        }
        if let Some(seq) = self.open_step {
            return Err(ReplayError::BadJournal(format!(
                "inputs delivered while step {seq} is open"
            )));
        }
        let mut expect = self.next_seq;
        for ev in &events {
            if ev.seq != expect {
                return Err(ReplayError::BadJournal(format!(
                    "input seq {} where {expect} was expected",
                    ev.seq
                )));
            }
            if !is_input(&ev.body) {
                return Err(ReplayError::BadJournal(format!(
                    "delivered event {} at seq {} is not an input",
                    ev.body.summary(),
                    ev.seq
                )));
            }
            expect += 1;
        }
        if let Some(m) = events.iter().map(|e| e.ts_nanos).max() {
            self.max_ts = self.max_ts.max(m);
        }
        for ev in &events {
            self.index_completion(ev);
        }
        self.journal.extend(events);
        self.next_seq = expect;
        Ok(())
    }

    /// True iff `last guest command seq + 1 >= threshold_events`. Depends only on the
    /// guest's own command position, so it is identical at the same program point in
    /// record and replay.
    pub fn continue_as_new_suggested(&self, threshold_events: u64) -> bool {
        self.last_cmd_seq.saturating_add(1) >= threshold_events
    }

    /// A per-run deterministic random stream.
    pub fn rng(&mut self, stream: RandomStream) -> &mut ChaChaStream {
        self.rng.stream(stream)
    }

    /// `wasi:random/insecure-seed` value for this run.
    pub fn insecure_seed(&self) -> (u64, u64) {
        insecure_seed(&self.run.rng_seed)
    }

    /// UUIDv7 embedding logical-now milliseconds, randomness from the Uuid stream.
    pub fn new_uuid_v7(&mut self) -> String {
        let now_ms = self.last_cmd_ts / 1_000_000;
        uuid_v7(now_ms, self.rng.stream(RandomStream::Uuid))
    }

    /// The full journal (recorded events plus everything appended or delivered).
    pub fn journal(&self) -> &[Event] {
        &self.journal
    }
}

/// Whether `body` may be delivered as a committed input (see
/// [`ReplayState::deliver_inputs`]).
fn is_input(body: &EventBody) -> bool {
    body.completes_op().is_some()
        || matches!(
            body,
            EventBody::SignalPayload(_) | EventBody::RunTerminated(_)
        )
}

fn expected_text(ev: &Event) -> String {
    match ev.body.command_meta() {
        Some(m) => format!("{} #{}", ev.body.summary(), hash_hex8(&m.hash)),
        None => ev.body.summary(),
    }
}
