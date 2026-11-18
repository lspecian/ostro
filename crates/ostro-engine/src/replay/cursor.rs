//! Journal cursor, command matching and nondeterminism detection (ENG-03/ENG-04).
//!
//! The journal is the run's full event list: `journal[0]` is `RunStarted` and seqs are
//! contiguous from 0, so `journal[seq] .seq == seq`. Guest-command events (codes 16–63)
//! are matched one by one against the commands the guest issues; input events (timer
//! fired, effect completed, ...) are skipped by the cursor and never compared. Once no
//! recorded guest command is left, commands run live: each gets the next seq, is stamped
//! with the clock, and is appended to both the journal (so op lookups see it) and the
//! pending batch the caller hands to the sink.

use std::sync::Arc;

use ostro_types::journal::{
    CommandHash, CommandKind, CommandMeta, Event, EventBody, EventKind, HASH_VER, PatchMarker,
    RunStarted,
};

use crate::env::{Clock, RandomStream};
use crate::error::{NonDeterminism, ProtocolViolation};

use super::rng::{ChaChaStream, RngStreams, insecure_seed, uuid_v7};

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
    /// The step runs live; `seq` is reserved for its `StepCompleted`.
    Live { seq: u64 },
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
}

fn protocol(msg: impl Into<String>) -> ReplayError {
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
        (E::StepCompleted, C::Step)
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
    /// Seq reserved by a live `begin_step` that has not completed yet.
    pub(super) open_step: Option<u64>,
    /// Ts of the most recent consumed/appended guest command (`accepted_at` before any).
    pub(super) last_cmd_ts: u64,
    /// Seq of the most recent consumed/appended guest command (0 before any).
    pub(super) last_cmd_seq: u64,
    pub(super) clock: Arc<dyn Clock>,
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
        Ok(Self {
            next_seq: journal.len() as u64,
            cursor: 1,
            pending: Vec::new(),
            rng: RngStreams::new(&run.rng_seed),
            open_step: None,
            last_cmd_ts: run.accepted_at_nanos,
            last_cmd_seq: 0,
            run,
            journal,
            clock,
        })
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
    /// (`accepted_at` before the first). Input events never move it.
    pub fn logical_now_nanos(&self) -> u64 {
        self.last_cmd_ts
    }

    /// Hash version of the next recorded guest command, so the adapter can hash with the
    /// version that was recorded. `None` when live.
    pub fn next_command_hash_ver(&self) -> Option<u8> {
        self.next_guest_command()
            .and_then(|i| self.journal[i].body.command_meta())
            .map(|m| m.hash_ver)
    }

    fn check_no_open_step(&self) -> Result<(), ReplayError> {
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
    pub(super) fn append_live(&mut self, seq: u64, body: EventBody) -> (u64, u64) {
        debug_assert_eq!(seq, self.next_seq);
        // Logical time never moves backwards, even if the wall clock does.
        let ts = self.clock.now_unix_nanos().max(self.last_cmd_ts);
        let ev = Event {
            seq,
            ts_nanos: ts,
            body,
        };
        self.journal.push(ev.clone());
        self.pending.push(ev);
        self.next_seq = seq + 1;
        self.cursor = self.journal.len();
        self.last_cmd_ts = ts;
        self.last_cmd_seq = seq;
        (seq, ts)
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
        let (seq, ts_nanos) = self.append_live(seq, body);
        Ok(CommandResult::Live { seq, ts_nanos })
    }

    /// `patched(change_id)`: live → `true` and a `PatchMarker` is appended; replay with a
    /// matching marker next → `true` (consumed); replay where the next guest command is
    /// anything else → `false`, nothing consumed, no error.
    pub fn patched(&mut self, hash: CommandHash, change_id: &str) -> Result<bool, ReplayError> {
        self.check_no_open_step()?;
        match self.next_guest_command() {
            Some(i) => {
                let ev = &self.journal[i];
                let matches = ev.body.kind() == EventKind::PatchMarker
                    && ev.body.command_meta().is_some_and(|m| m.hash == hash);
                if matches {
                    self.consume(i);
                }
                Ok(matches)
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
                self.append_live(seq, body);
                Ok(true)
            }
        }
    }

    /// Start a step. Replay: returns the recorded `StepCompleted` and opens no guard.
    /// Live: reserves `seq = next_seq` and opens the step guard; nothing is appended
    /// until [`complete_step`](Self::complete_step).
    pub fn begin_step(
        &mut self,
        hash: CommandHash,
        actual: &dyn Fn() -> String,
    ) -> Result<StepBegin<'_>, ReplayError> {
        self.check_no_open_step()?;
        if let Some(i) = self.match_next(CommandKind::Step, hash, actual)? {
            return Ok(StepBegin::Replayed(&self.journal[i]));
        }
        let seq = self.next_seq;
        self.open_step = Some(seq);
        Ok(StepBegin::Live { seq })
    }

    /// Append the `StepCompleted` for the open step at its reserved seq and close the guard.
    pub fn complete_step(&mut self, seq: u64, body: EventBody) -> Result<(), ReplayError> {
        if self.open_step != Some(seq) {
            return Err(protocol(format!(
                "complete-step for step {seq} that is not open"
            )));
        }
        if !matches!(body, EventBody::StepCompleted(_)) {
            return Err(protocol(format!(
                "complete-step with {} instead of a step completion",
                body.summary()
            )));
        }
        self.open_step = None;
        self.append_live(seq, body);
        Ok(())
    }

    /// Drop an open step without journaling it; its reserved seq is released.
    pub fn abandon_step(&mut self, seq: u64) {
        if self.open_step == Some(seq) {
            self.open_step = None;
        }
    }

    /// The guest's run function returned (called before its terminal command is
    /// issued). Unconsumed recorded guest commands are a divergence, except a trailing
    /// terminal event, which is compared through `command(Terminal, ..)`. An open step is
    /// abandoned (it never completed, so nothing was journaled).
    pub fn finish(&mut self) -> Result<(), ReplayError> {
        self.open_step = None;
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
            if ev.body.completes_op().is_none() {
                return Err(ReplayError::BadJournal(format!(
                    "delivered event {} at seq {} is not an input",
                    ev.body.summary(),
                    ev.seq
                )));
            }
            expect += 1;
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

fn expected_text(ev: &Event) -> String {
    match ev.body.command_meta() {
        Some(m) => format!("{} #{}", ev.body.summary(), hash_hex8(&m.hash)),
        None => ev.body.summary(),
    }
}
