//! Per-store `HostState`: the thin host adapter over the replay core (plan 03-07).
//!
//! `HostState` owns one activation's [`ReplayState`] and implements every
//! `ostro:runtime@0.1.0` import (see `context.rs`, `journal.rs`, `effects.rs`, `log.rs`)
//! plus [`GuestEnv`] for the deterministic WASI shim and [`HasLimits`] for the store
//! limiter. Imports are `async fn` per the bindgen flags but never await anything: the
//! only awaiting method is [`HostState::flush`], called by the activation driver (03-08)
//! outside guest code.
//!
//! [`build_linker`] assembles the single production linker: ostro bindings + WASI shim +
//! the store-aware per-step fuel functions (`begin-step`, `[method]step.complete`).

use std::sync::Arc;
use std::time::Instant;

use ostro_types::journal::{
    CommandArgs, CommandHash, CommandKind, CommandMeta, Continuation, Event, EventBody, Failure,
    HASH_VER, Payload, RunCompleted, RunContinuedAsNew, RunFailed, command_hash,
};
use wasmtime::component::{HasSelf, Linker, Resource, ResourceTable};
use wasmtime::{Engine, StoreContextMut};

use crate::env::{Clock, GuestEnv, RandomStream, StdStream};
use crate::error::{NonDeterminism, ProtocolViolation};
use crate::host::bindings::Workflow;
use crate::host::bindings::exports::ostro::runtime::workflows as wit_workflows;
use crate::host::bindings::ostro::runtime::journal as wit_journal;
use crate::host::bindings::ostro::runtime::types as wit_types;
use crate::host::bindings::{StepRep, ostro::runtime::types::StepOptions};
use crate::limits::{HasLimits, OstroLimiter};
use crate::replay::{CommandResult, ReplayError, ReplayState};
use crate::sink::{ActivationSink, AppendError, RunRef};

/// Per-stream guest stdio budget per activation.
pub const STDIO_LIMIT: usize = 64 * 1024;
/// Max log lines kept per activation.
pub const LOG_LINE_LIMIT: usize = 1_000;
/// Max log bytes (messages + fields) kept per activation.
pub const LOG_BYTE_LIMIT: usize = 1024 * 1024;
/// Bytes of stderr returned by [`HostState::stderr_tail`].
const STDERR_TAIL: usize = 4 * 1024;

/// Everything needed to build a [`HostState`] for one activation.
pub struct HostInit {
    pub run: RunRef,
    pub attempt: u32,
    /// Hex SHA-256 of the component (reported in `run-info.module-hash`).
    pub module_hash_hex: String,
    /// Committed journal; `journal[0]` is `RunStarted`.
    pub journal: Vec<Event>,
    /// Real clock, used only to stamp new events.
    pub clock: Arc<dyn Clock>,
    pub limiter: OstroLimiter,
    pub wall_deadline: Instant,
    /// Node key for minting await tokens (live only; replay returns recorded tokens).
    pub token_key: [u8; 32],
    /// `engine.continue_as_new_events`.
    pub continue_as_new_events: u64,
    pub sink: Option<Arc<dyn ActivationSink>>,
}

/// A bounded byte buffer: excess bytes are dropped and counted.
#[derive(Debug, Default)]
pub struct BoundedBuf {
    pub bytes: Vec<u8>,
    pub dropped: u64,
}

impl BoundedBuf {
    fn push(&mut self, data: &[u8]) {
        let room = STDIO_LIMIT.saturating_sub(self.bytes.len());
        let n = room.min(data.len());
        self.bytes.extend_from_slice(&data[..n]);
        self.dropped += (data.len() - n) as u64;
    }

    fn remaining(&self) -> u64 {
        STDIO_LIMIT.saturating_sub(self.bytes.len()) as u64
    }
}

/// One guest log line (live activations only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogLine {
    pub level: &'static str,
    pub message: String,
    pub fields: Vec<(String, String)>,
}

/// Bounded per-activation guest log (1000 lines or 1 MiB; excess counted).
#[derive(Debug, Default)]
pub struct BoundedLog {
    pub lines: Vec<LogLine>,
    pub bytes: usize,
    pub dropped: u64,
}

impl BoundedLog {
    /// Keep `line` if within budget; returns whether it was kept.
    pub(crate) fn push(&mut self, line: LogLine) -> bool {
        let size = line.message.len()
            + line
                .fields
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>();
        if self.lines.len() >= LOG_LINE_LIMIT || self.bytes + size > LOG_BYTE_LIMIT {
            self.dropped += 1;
            return false;
        }
        self.bytes += size;
        self.lines.push(line);
        true
    }
}

/// Successful terminal result of `run` (journal mirror types).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunReturn {
    Completed(Payload),
    ContinueAsNew(Continuation),
}

/// Per-step fuel bookkeeping for the single open live step.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StepFuel {
    pub seq: u64,
    /// Fuel the step started with (after a step budget was applied).
    pub start: u64,
    /// Outer activation fuel to restore at completion when a budget was applied.
    pub outer: Option<u64>,
}

/// Store data of one activation.
pub struct HostState {
    pub(crate) replay: ReplayState,
    table: ResourceTable,
    limiter: OstroLimiter,
    wall_deadline: Instant,
    pub(crate) run: RunRef,
    pub(crate) attempt: u32,
    pub(crate) module_hash_hex: String,
    pub(crate) token_key: [u8; 32],
    pub(crate) can_events: u64,
    sink: Option<Arc<dyn ActivationSink>>,
    /// Status published by the guest, as journaled (on replay: the recorded json).
    pub latest_status: Option<String>,
    /// Guest log lines of this activation (empty while replaying).
    pub logs: BoundedLog,
    stdout: BoundedBuf,
    stderr: BoundedBuf,
    insecure_seed: (u64, u64),
    /// Seqs of steps completed in this activation (double-complete detection).
    pub(crate) completed_steps: std::collections::HashSet<u64>,
    /// Fuel bookkeeping of the open live step.
    pub(crate) step_fuel: Option<StepFuel>,
    /// A step with a fuel budget was dropped open; outer fuel to restore lazily.
    pub(crate) fuel_restore: Option<u64>,
}

impl std::fmt::Debug for HostState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostState")
            .field("run", &self.run)
            .field("attempt", &self.attempt)
            .field("replay", &self.replay)
            .finish_non_exhaustive()
    }
}

impl HostState {
    /// Build the state for one activation; fails on a malformed journal.
    pub fn new(init: HostInit) -> Result<Self, ReplayError> {
        let replay = ReplayState::new(init.journal, init.clock)?;
        let insecure_seed = replay.insecure_seed();
        Ok(Self {
            replay,
            table: ResourceTable::new(),
            limiter: init.limiter,
            wall_deadline: init.wall_deadline,
            run: init.run,
            attempt: init.attempt,
            module_hash_hex: init.module_hash_hex,
            token_key: init.token_key,
            can_events: init.continue_as_new_events,
            sink: init.sink,
            latest_status: None,
            logs: BoundedLog::default(),
            stdout: BoundedBuf::default(),
            stderr: BoundedBuf::default(),
            insecure_seed,
            completed_steps: std::collections::HashSet::new(),
            step_fuel: None,
            fuel_restore: None,
        })
    }

    pub fn run_ref(&self) -> &RunRef {
        &self.run
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The replay core (read-only).
    pub fn replay(&self) -> &ReplayState {
        &self.replay
    }

    /// The full journal as seen by this activation (committed + live).
    pub fn journal(&self) -> &[Event] {
        self.replay.journal()
    }

    pub fn is_replaying(&self) -> bool {
        self.replay.is_replaying()
    }

    /// Drain the live events appended since the last call.
    pub fn take_pending(&mut self) -> Vec<Event> {
        self.replay.take_pending()
    }

    /// Deliver committed input events (03-08 hot wait).
    pub fn deliver_inputs(&mut self, events: Vec<Event>) -> Result<(), ReplayError> {
        self.replay.deliver_inputs(events)
    }

    /// Captured guest stdout (live only, bounded).
    pub fn stdout(&self) -> &BoundedBuf {
        &self.stdout
    }

    /// Captured guest stderr (live only, bounded).
    pub fn stderr(&self) -> &BoundedBuf {
        &self.stderr
    }

    /// Last 4 KiB of guest stderr, lossily decoded (e.g. a Rust panic message).
    pub fn stderr_tail(&self) -> String {
        let b = &self.stderr.bytes;
        let start = b.len().saturating_sub(STDERR_TAIL);
        String::from_utf8_lossy(&b[start..]).into_owned()
    }

    /// Append the pending live events through the sink (`from_seq` = first pending seq).
    ///
    /// On success the events stay in the replay journal as committed. Without a sink
    /// this is a no-op and the events stay pending.
    pub async fn flush(&mut self) -> Result<(), AppendError> {
        let Some(sink) = self.sink.clone() else {
            return Ok(());
        };
        let pending = self.replay.take_pending();
        let Some(first) = pending.first() else {
            return Ok(());
        };
        let from_seq = first.seq;
        sink.append(&self.run, self.attempt, from_seq, pending)
            .await
            .map(|_| ())
    }

    /// Hash a guest command with the recorded hash version (replay) or [`HASH_VER`].
    pub(crate) fn hash(&self, args: &CommandArgs<'_>) -> wasmtime::Result<CommandHash> {
        let ver = self.replay.next_command_hash_ver().unwrap_or(HASH_VER);
        command_hash(ver, args).map_err(|e| {
            wasmtime::Error::new(ProtocolViolation(format!(
                "bad journal: cannot hash command with version {ver}: {e:?}"
            )))
        })
    }

    /// Journal the run's terminal command after `run` returned.
    ///
    /// Calls `finish()` (unconsumed recorded commands diverge), then issues the
    /// kind-only `Terminal` command. On replay the recorded terminal must be the same
    /// kind (completed / failed / continued-as-new); output differences never diverge.
    pub fn record_terminal(&mut self, r: &Result<RunReturn, Failure>) -> Result<(), ReplayError> {
        self.replay.finish()?;
        let ver = self.replay.next_command_hash_ver().unwrap_or(HASH_VER);
        let hash = command_hash(ver, &CommandArgs::Terminal)
            .map_err(|e| ReplayError::BadJournal(format!("terminal hash: {e:?}")))?;
        let meta = CommandMeta {
            hash_ver: HASH_VER,
            hash,
        };
        let (want, actual) = match r {
            Ok(RunReturn::Completed(_)) => (
                ostro_types::journal::EventKind::RunCompleted,
                "run completed",
            ),
            Ok(RunReturn::ContinueAsNew(_)) => (
                ostro_types::journal::EventKind::RunContinuedAsNew,
                "continue-as-new",
            ),
            Err(_) => (ostro_types::journal::EventKind::RunFailed, "run failed"),
        };
        let body = match r {
            Ok(RunReturn::Completed(p)) => EventBody::RunCompleted(RunCompleted {
                meta,
                output: p.clone(),
            }),
            Ok(RunReturn::ContinueAsNew(c)) => EventBody::RunContinuedAsNew(RunContinuedAsNew {
                meta,
                continuation: c.clone(),
            }),
            Err(f) => EventBody::RunFailed(RunFailed {
                meta,
                failure: f.clone(),
            }),
        };
        let res =
            self.replay
                .command(CommandKind::Terminal, hash, &|| actual.to_owned(), |_| body)?;
        if let CommandResult::Replayed(ev) = res
            && ev.body.kind() != want
        {
            return Err(ReplayError::NonDeterminism(NonDeterminism {
                seq: ev.seq,
                expected: ev.body.summary(),
                actual: actual.to_owned(),
            }));
        }
        Ok(())
    }
}

/// Convert the guest's `run` result into journal mirror types.
pub fn run_return_from_wit(
    r: Result<wit_workflows::RunResult, wit_types::Failure>,
) -> Result<RunReturn, Failure> {
    use crate::host::effects::convert;
    match r {
        Ok(wit_workflows::RunResult::Completed(p)) => {
            Ok(RunReturn::Completed(convert::payload_from_wit(p)))
        }
        Ok(wit_workflows::RunResult::ContinueAsNew(c)) => {
            Ok(RunReturn::ContinueAsNew(Continuation {
                input: convert::payload_from_wit(c.input),
                workflow: c.workflow,
                version: c.version,
            }))
        }
        Err(f) => Err(convert::failure_from_wit(f)),
    }
}

/// Map a replay error to a guest trap carrying the typed engine error.
pub(crate) fn trap(e: ReplayError) -> wasmtime::Error {
    match e {
        ReplayError::NonDeterminism(nd) => wasmtime::Error::new(nd),
        ReplayError::Protocol(p) => wasmtime::Error::new(p),
        ReplayError::BadJournal(m) => {
            wasmtime::Error::new(ProtocolViolation(format!("bad journal: {m}")))
        }
    }
}

/// A protocol-violation trap.
pub(crate) fn violation(msg: impl Into<String>) -> wasmtime::Error {
    wasmtime::Error::new(ProtocolViolation(msg.into()))
}

impl GuestEnv for HostState {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }

    fn wall_now(&self) -> (u64, u32) {
        let n = self.replay.logical_now_nanos();
        (n / 1_000_000_000, (n % 1_000_000_000) as u32)
    }

    fn monotonic_now_ns(&self) -> u64 {
        self.replay
            .logical_now_nanos()
            .saturating_sub(self.replay.run().accepted_at_nanos)
    }

    fn fill_random(&mut self, stream: RandomStream, buf: &mut [u8]) {
        self.replay.rng(stream).fill(buf);
    }

    fn insecure_seed(&self) -> (u64, u64) {
        self.insecure_seed
    }

    fn write_stdio(&mut self, stream: StdStream, bytes: &[u8]) {
        if self.replay.is_replaying() {
            return;
        }
        let (buf, name) = match stream {
            StdStream::Stdout => (&mut self.stdout, "stdout"),
            StdStream::Stderr => (&mut self.stderr, "stderr"),
        };
        buf.push(bytes);
        tracing::debug!(
            target: "ostro::guest",
            run = %self.run.run_id,
            attempt = self.attempt,
            stream = name,
            text = %String::from_utf8_lossy(bytes),
        );
    }

    fn stdio_budget(&self, stream: StdStream) -> u64 {
        match stream {
            StdStream::Stdout => self.stdout.remaining(),
            StdStream::Stderr => self.stderr.remaining(),
        }
    }
}

impl HasLimits for HostState {
    fn limiter(&mut self) -> &mut OstroLimiter {
        &mut self.limiter
    }

    fn wall_deadline(&self) -> Instant {
        self.wall_deadline
    }
}

const JOURNAL_IFACE: &str = "ostro:runtime/journal@0.1.0";

/// Apply a lazily pending outer-fuel restore (a budgeted step was dropped open).
fn apply_fuel_restore(store: &mut StoreContextMut<'_, HostState>) -> wasmtime::Result<()> {
    if let Some(f) = store.data_mut().fuel_restore.take() {
        store.set_fuel(f)?;
    }
    Ok(())
}

/// Build the production linker: `ostro:runtime@0.1.0` imports, the deterministic WASI
/// shim, and store-aware `begin-step` / `[method]step.complete` for per-step fuel.
///
/// Wasmtime 50's bindgen `store` flag needs the component-model-async `Accessor`
/// machinery, which Ostro does not enable, so the two fuel-aware functions are defined
/// with `func_wrap` (shadowing the bindgen ones) and receive `StoreContextMut`.
pub fn build_linker(engine: &Engine) -> wasmtime::Result<Linker<HostState>> {
    let mut linker: Linker<HostState> = Linker::new(engine);
    Workflow::add_to_linker::<_, HasSelf<_>>(&mut linker, |h| h)?;
    crate::host::wasi::add_to_linker(&mut linker)?;

    linker.allow_shadowing(true);
    {
        let mut inst = linker.instance(JOURNAL_IFACE)?;
        inst.func_wrap(
            "begin-step",
            |mut store: StoreContextMut<'_, HostState>,
             (name, opts): (String, StepOptions)|
             -> wasmtime::Result<(wit_journal::StepStart,)> {
                apply_fuel_restore(&mut store)?;
                let fuel = store.get_fuel().ok();
                let (start, set) = store.data_mut().begin_step_impl(name, opts, fuel)?;
                if let Some(f) = set {
                    store.set_fuel(f)?;
                }
                Ok((start,))
            },
        )?;
        inst.func_wrap(
            "[method]step.complete",
            |mut store: StoreContextMut<'_, HostState>,
             (step, outcome): (
                Resource<StepRep>,
                Result<wit_types::Payload, wit_types::Failure>,
            )|
             -> wasmtime::Result<(wit_journal::CompletionAck,)> {
                let fuel = store.get_fuel().ok();
                let (ack, restore) = store.data_mut().complete_step_impl(step, outcome, fuel)?;
                if let Some(f) = restore {
                    store.set_fuel(f)?;
                }
                Ok((ack,))
            },
        )?;
    }
    linker.allow_shadowing(false);
    Ok(linker)
}
