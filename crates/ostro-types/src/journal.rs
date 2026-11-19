//! Journal event contract (owned by Phase 3).
//!
//! Phase 3 owns the meaning of `JournalEvent.kind: u32` and its payload bytes; Phase 2
//! carries them opaquely inside `EventsAppended` (02-RESEARCH Pitfall 12). Phase 2 stores
//! events as `command::JournalEvent { seq, kind, payload }` and carries the run's
//! [`RngSeed`] in `StartRun.rng_seed` / `RunRecord.rng_seed`. The submitter (the Phase 4
//! engine host) derives Phase 2's `Derived` intents (ScheduleTimer, SendMessage,
//! CompleteRun, ...) from these journal events.
//!
//! The types here mirror the WIT 0.1 types but are Ostro-owned, so WIT bindgen types
//! never leak into the on-disk journal.
//!
//! # Event codec (v1, [`EVENT_CODEC_VER`])
//!
//! [`encode_event`] produces `(kind, bytes)` where `kind = EventKind as u32` and
//! `bytes = [EVENT_CODEC_VER] ‖ postcard((ts_nanos, <variant payload struct>))`. `seq` is
//! NOT inside the bytes: Phase 2's `JournalEvent { seq, kind, payload }` carries it.
//! [`decode_event`] rejects unknown kinds, unknown versions, truncated input and trailing
//! bytes; it never skips anything.
//!
//! # Recorded-result codec (v1, [`RESULT_CODEC_VER`])
//!
//! [`encode_result`] stores `postcard(value)` with `codec_ver = RESULT_CODEC_VER`; used for
//! `Result<Payload, Failure>` (steps) and `Result<OpOutput, Failure>` (effects, children).
//! On replay these exact bytes are returned to the guest (ENG-04).
//!
//! Postcard is used only for the Ostro-owned mirror and payload structs in this module.
//! Its layout depends on field order and enum variant order, so **adding, removing or
//! reordering a field or variant of any type reachable from an event or result requires
//! bumping [`EVENT_CODEC_VER`] / [`RESULT_CODEC_VER`]** (and a decoder for the old version).
//! The golden vectors in `tests/golden/journal_v1.txt` fail CI on any drift.
//!
//! **Appending a new event kind is additive and does NOT bump [`EVENT_CODEC_VER`]**
//! (Phase 4 decision): the kind code lives outside the bytes and every existing kind's
//! encoding is unchanged, so old journals decode identically. A new kind gets a new code,
//! a new payload struct, a new `EventBody` variant appended at the END of the enum, and a
//! new golden line appended to `journal_v1.txt`; existing golden lines are never
//! re-blessed. Bump the version only when an existing kind's encoding changes. Example:
//! [`EventKind::StepAttemptFailed`] (29, plan 04-04, step retry WF-02).
//!
//! # Phase 5 kinds (plan 05-01; codes from `.planning/RESERVATIONS.md` §4)
//!
//! - [`EventKind::WaitTimedOut`] (70, D4): a signal/token wait timed out; completes its op
//!   with `Err(Failure::Timeout)`. Encoded by apply.
//! - [`EventKind::RunTerminated`] (71, D3): history marker of a hard stop (`Terminated`, or
//!   `TimedOut` with reason `run_timeout`/`schedule_to_start`). Non-completing; apply-encoded.
//! - [`EventKind::SignalPayload`] (96, D21): a delivered signal's name and payload, encoded by
//!   the **submitter** and journaled verbatim by apply. Non-completing on its own.
//! - [`EventKind::SignalMatched`] (97, D21): apply matched a wait `op` with the signal at
//!   `payload_seq`; carries no user data. The op completes with the payload of the
//!   `SignalPayload` event at `payload_seq`.
//!
//! [`EventKind::SignalReceived`] (66) stays decodable for old journals, but Phase 5+ apply
//! never produces it (D21: the matched op is chosen in apply, so the submitter cannot
//! pre-encode it).
//!
//! # Command hash (v1, [`HASH_VER`])
//!
//! `command_hash = sha256(b"ostro/cmd" ‖ [hash_ver] ‖ [CommandKind as u8] ‖ canonical_args)[..16]`.
//! The canonical argument layout is hand-specified (no serde), so additive WIT changes
//! and dependency bumps never change existing hashes. Primitive encodings:
//!
//! | primitive | encoding |
//! |-----------|----------|
//! | string | u32 LE byte length ‖ UTF-8 bytes |
//! | digested blob | 32-byte sha256 of the raw bytes (no length prefix) |
//! | inline blob (SQL bytea) | u32 LE length ‖ bytes |
//! | u64 / u32 / i64 | little-endian |
//! | u8 / bool | one byte (bool: 0 / 1) |
//! | f64 | LE IEEE-754 bits; every NaN canonicalised to `0x7ff8000000000000` |
//! | list | u32 LE count ‖ items in guest order |
//!
//! Per-kind hashed fields, in order:
//!
//! | kind (code) | hashed fields | not hashed |
//! |-------------|---------------|------------|
//! | Step (1) | name: string | |
//! | Http (2) | method: string, url: string, sha256(body), header names: list<string> ASCII-lower-cased and byte-sorted | header values (may hold `{{secret:..}}` templates), timeout, idempotency key, redact list, retry policy |
//! | Sql (3) | connection: string, statement: string, sha256(params) where params = list of (tag u8 ‖ value) | exactly_once |
//! | Object (4) | connection: string, op: string, key: string, sha256(body) | |
//! | Sleep (5) | duration_ms: u64 | |
//! | SleepUntil (6) | seconds: u64, nanoseconds: u32 | |
//! | WaitSignal (7) | name: string | timeout |
//! | StartChild (8) | workflow: string, sha256(input bytes) | queue, version, detached, run timeout |
//! | CreateAwaitToken (9) | nothing (kind only) | timeout |
//! | CancelOp (10) | op: u64 | |
//! | Wait (11) | ops: list<u64>, mode: u8 (0 All, 1 Any) | |
//! | Patch (12) | change_id: string | |
//! | SetStatus (13) | nothing (kind only: status text changes never break replay) | status JSON |
//! | Terminal (14) | nothing (kind only: output changes never break replay) | output / failure |
//!
//! SQL param tags: 0 Null (no value), 1 Boolean (u8), 2 Int8 (i64 LE), 3 Float8 (f64),
//! 4 Numeric (string), 5 Text (string), 6 Bytea (inline blob), 7 Json (string),
//! 8 Timestamptz (seconds u64 ‖ nanoseconds u32). An absent HTTP/object body hashes as
//! the empty body.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Version of the command-hash spec stored in every [`CommandMeta`].
pub const HASH_VER: u8 = 1;
/// Version of the codec used for [`RecordedResult`] bytes.
pub const RESULT_CODEC_VER: u8 = 1;
/// Version of the journal event codec.
pub const EVENT_CODEC_VER: u8 = 1;

/// Operation id allocated deterministically by the guest's effect calls.
pub type OpId = u64;

/// Reserved op id for `effects.cancellation`.
pub const CANCELLATION_OP: OpId = u64::MAX;

/// Per-run RNG seed.
///
/// Generated by the partition leader at run start and journaled in [`RunStarted`];
/// Phase 2's start-run command must carry it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RngSeed(pub [u8; 32]);

/// Truncated hash of a guest command's arguments (see [`CommandArgs`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommandHash(pub [u8; 16]);

// ---------------------------------------------------------------------------
// WIT mirrors
// ---------------------------------------------------------------------------

/// Mirror of WIT `types.payload`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Payload {
    pub content_type: String,
    pub data: Vec<u8>,
}

/// Mirror of WIT `types.error-info`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub kind: String,
    pub message: String,
    pub details: Option<Payload>,
}

/// Mirror of WIT `types.failure`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Failure {
    Retryable(ErrorInfo),
    NonRetryable(ErrorInfo),
    Timeout,
    Cancelled,
}

/// Mirror of WIT `effects.header`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Header {
    pub name: String,
    pub value: String,
}

/// Mirror of WIT `effects.http-request` (retry policy is applied by the host, not journaled).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<Header>,
    pub body: Option<Vec<u8>>,
    pub timeout_ms: Option<u64>,
    pub idempotency_key: Option<String>,
    pub redact: Vec<String>,
}

/// Mirror of WIT `effects.http-response`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<Header>,
    pub body: Vec<u8>,
}

/// Mirror of WIT `effects.sql-value`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SqlValue {
    Null,
    Boolean(bool),
    Int8(i64),
    Float8(f64),
    Numeric(String),
    Text(String),
    Bytea(Vec<u8>),
    Json(String),
    Timestamptz { seconds: u64, nanoseconds: u32 },
}

/// Mirror of WIT `effects.sql-request`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlRequest {
    pub connection: String,
    pub statement: String,
    pub params: Vec<SqlValue>,
    pub exactly_once: bool,
}

/// Mirror of WIT `effects.sql-result`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub rows_affected: u64,
}

/// Mirror of WIT `effects.object-request`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectRequest {
    pub connection: String,
    pub op: String,
    pub key: String,
    pub body: Option<Vec<u8>>,
}

/// A host-executed effect request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EffectRequest {
    Http(HttpRequest),
    Sql(SqlRequest),
    Object(ObjectRequest),
}

/// Mirror of WIT `effects.op-output`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OpOutput {
    Http(HttpResponse),
    Sql(SqlResult),
    Object(Vec<u8>),
    Timer,
    Signal(Payload),
    Child(Payload),
    Token(Payload),
    CancelRequested,
}

/// Mirror of WIT `workflows.continuation`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Continuation {
    pub input: Payload,
    pub workflow: Option<String>,
    pub version: Option<u32>,
}

/// Mirror of WIT `effects.child-options`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChildOptions {
    pub queue: Option<String>,
    pub version: Option<u32>,
    pub detached: bool,
    pub run_timeout_ms: Option<u64>,
}

/// Mirror of WIT `effects.wait-mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WaitMode {
    All,
    Any,
}

// ---------------------------------------------------------------------------
// Journal records
// ---------------------------------------------------------------------------

/// A recorded result (ENG-04): the exact canonical bytes returned to the guest on
/// replay; never re-derived.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RecordedResult {
    pub codec_ver: u8,
    pub bytes: Vec<u8>,
}

/// Command identity carried by every guest-command event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommandMeta {
    pub hash_ver: u8,
    pub hash: CommandHash,
}

/// Kind of guest command (input to the command hash).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum CommandKind {
    Step = 1,
    Http = 2,
    Sql = 3,
    Object = 4,
    Sleep = 5,
    SleepUntil = 6,
    WaitSignal = 7,
    StartChild = 8,
    CreateAwaitToken = 9,
    CancelOp = 10,
    Wait = 11,
    Patch = 12,
    SetStatus = 13,
    Terminal = 14,
}

/// Stable journal event kind codes (`JournalEvent.kind`).
///
/// Ranges: 1–15 run record, 16–63 guest commands, 64–127 inputs, 128+ reserved.
/// Codes are frozen with the on-disk format at the end of Phase 5.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u32)]
pub enum EventKind {
    RunStarted = 1,
    // guest commands
    StepCompleted = 16,
    TimerStarted = 17,
    EffectScheduled = 18,
    SignalWaitStarted = 19,
    ChildStarted = 20,
    AwaitTokenCreated = 21,
    OpCancelled = 22,
    WaitResolved = 23,
    PatchMarker = 24,
    StatusPublished = 25,
    RunCompleted = 26,
    RunFailed = 27,
    RunContinuedAsNew = 28,
    /// A step attempt failed and will be retried (WF-02, additive in Phase 4).
    StepAttemptFailed = 29,
    // inputs
    TimerFired = 64,
    EffectCompleted = 65,
    SignalReceived = 66,
    ChildCompleted = 67,
    TokenCompleted = 68,
    CancelRequested = 69,
    /// A wait timed out (D4, Phase 5).
    WaitTimedOut = 70,
    /// A run was hard-stopped (D3, Phase 5).
    RunTerminated = 71,
    // RESERVATIONS.md: reserved P6 = 72..79, P7 = 80..87, P8 = 88..95.
    /// A delivered signal, submitter-encoded (D21, Phase 5).
    SignalPayload = 96,
    /// Apply matched a wait with a journaled signal (D21, Phase 5).
    SignalMatched = 97,
    // RESERVATIONS.md: reserved P9 = 98..105.
}

impl EventKind {
    /// Numeric code stored in `JournalEvent.kind`.
    pub fn code(self) -> u32 {
        self as u32
    }
}

/// First event of every run.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RunStarted {
    pub workflow: String,
    pub workflow_version: u32,
    pub input: Payload,
    pub rng_seed: RngSeed,
    pub accepted_at_nanos: u64,
    pub deadline_nanos: Option<u64>,
    pub module_hash: String,
    pub tenant: Option<String>,
    pub parent_run_id: Option<String>,
    pub continued_from: Option<String>,
}

/// A step completed; `outcome` is postcard `Result<Payload, Failure>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StepCompleted {
    pub meta: CommandMeta,
    pub name: String,
    pub outcome: RecordedResult,
    pub fuel_used: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimerStarted {
    pub meta: CommandMeta,
    pub fire_at_nanos: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectScheduled {
    pub meta: CommandMeta,
    pub request: EffectRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SignalWaitStarted {
    pub meta: CommandMeta,
    pub name: String,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChildStarted {
    pub meta: CommandMeta,
    pub workflow: String,
    pub input: Payload,
    pub options: ChildOptions,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AwaitTokenCreated {
    pub meta: CommandMeta,
    pub token: String,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OpCancelled {
    pub meta: CommandMeta,
    pub op: OpId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WaitResolved {
    pub meta: CommandMeta,
    pub completed: Vec<OpId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PatchMarker {
    pub meta: CommandMeta,
    pub change_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StatusPublished {
    pub meta: CommandMeta,
    pub json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RunCompleted {
    pub meta: CommandMeta,
    pub output: Payload,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RunFailed {
    pub meta: CommandMeta,
    pub failure: Failure,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RunContinuedAsNew {
    pub meta: CommandMeta,
    pub continuation: Continuation,
}

/// Where the wait before the next attempt of a failed step happens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AttemptBackoff {
    /// Short delay: the host waits inside the activation and retries in place.
    InActivation,
    /// Long delay: the run is evicted and re-queued at `retry_at_nanos`.
    Requeue,
}

/// A step attempt failed and the retry policy decided to retry it (WF-02).
///
/// Carries the same command hash as the step's eventual [`StepCompleted`]; on replay the
/// cursor consumes a chain of these before the completion. `attempt` is 1-based (the
/// attempt that failed), so the live attempt after a chain of `n` is `n + 1`. The final
/// attempt (success, or failure that is not retried) is recorded as `StepCompleted`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StepAttemptFailed {
    pub meta: CommandMeta,
    pub name: String,
    pub attempt: u32,
    pub failure: Failure,
    pub retry_at_nanos: u64,
    pub backoff: AttemptBackoff,
    pub fuel_used: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimerFired {
    pub op: OpId,
}

/// An effect completed; `outcome` is postcard `Result<OpOutput, Failure>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EffectCompleted {
    pub op: OpId,
    pub outcome: RecordedResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SignalReceived {
    pub op: OpId,
    pub payload: Payload,
}

/// A child run completed; `outcome` is postcard `Result<OpOutput, Failure>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChildCompleted {
    pub op: OpId,
    pub outcome: RecordedResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TokenCompleted {
    pub op: OpId,
    pub payload: Payload,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CancelRequested {
    pub reason: String,
}

/// A signal/token wait timed out (kind 70, D4). Completes `op` with `Err(Failure::Timeout)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WaitTimedOut {
    pub op: OpId,
}

/// History marker of a hard stop (kind 71, D3); never completes an op.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RunTerminated {
    pub reason: String,
}

/// A delivered signal (kind 96, D21). Encoded by the submitter, journaled verbatim by apply;
/// never completes an op on its own (see [`SignalMatched`]).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SignalPayload {
    pub name: String,
    pub payload: Payload,
}

/// Apply matched wait `op` with the [`SignalPayload`] event at `payload_seq` (kind 97, D21).
/// Carries no user data; the op's completion payload is read from that event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SignalMatched {
    pub op: OpId,
    pub payload_seq: u64,
}

/// Body of a journal event; one variant per [`EventKind`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EventBody {
    RunStarted(RunStarted),
    StepCompleted(StepCompleted),
    TimerStarted(TimerStarted),
    EffectScheduled(EffectScheduled),
    SignalWaitStarted(SignalWaitStarted),
    ChildStarted(ChildStarted),
    AwaitTokenCreated(AwaitTokenCreated),
    OpCancelled(OpCancelled),
    WaitResolved(WaitResolved),
    PatchMarker(PatchMarker),
    StatusPublished(StatusPublished),
    RunCompleted(RunCompleted),
    RunFailed(RunFailed),
    RunContinuedAsNew(RunContinuedAsNew),
    TimerFired(TimerFired),
    EffectCompleted(EffectCompleted),
    SignalReceived(SignalReceived),
    ChildCompleted(ChildCompleted),
    TokenCompleted(TokenCompleted),
    CancelRequested(CancelRequested),
    // Appended after the original Phase 3 variants (additive, see module docs).
    StepAttemptFailed(StepAttemptFailed),
    // Phase 5 (plan 05-01), appended in this order. RESERVATIONS.md: P6 = 72..79,
    // P7 = 80..87, P8 = 88..95, P9 = 98..105.
    WaitTimedOut(WaitTimedOut),
    RunTerminated(RunTerminated),
    SignalPayload(SignalPayload),
    SignalMatched(SignalMatched),
}

impl EventBody {
    /// Stable kind code of this event.
    pub fn kind(&self) -> EventKind {
        match self {
            Self::RunStarted(_) => EventKind::RunStarted,
            Self::StepCompleted(_) => EventKind::StepCompleted,
            Self::TimerStarted(_) => EventKind::TimerStarted,
            Self::EffectScheduled(_) => EventKind::EffectScheduled,
            Self::SignalWaitStarted(_) => EventKind::SignalWaitStarted,
            Self::ChildStarted(_) => EventKind::ChildStarted,
            Self::AwaitTokenCreated(_) => EventKind::AwaitTokenCreated,
            Self::OpCancelled(_) => EventKind::OpCancelled,
            Self::WaitResolved(_) => EventKind::WaitResolved,
            Self::PatchMarker(_) => EventKind::PatchMarker,
            Self::StatusPublished(_) => EventKind::StatusPublished,
            Self::RunCompleted(_) => EventKind::RunCompleted,
            Self::RunFailed(_) => EventKind::RunFailed,
            Self::RunContinuedAsNew(_) => EventKind::RunContinuedAsNew,
            Self::TimerFired(_) => EventKind::TimerFired,
            Self::EffectCompleted(_) => EventKind::EffectCompleted,
            Self::SignalReceived(_) => EventKind::SignalReceived,
            Self::ChildCompleted(_) => EventKind::ChildCompleted,
            Self::TokenCompleted(_) => EventKind::TokenCompleted,
            Self::CancelRequested(_) => EventKind::CancelRequested,
            Self::StepAttemptFailed(_) => EventKind::StepAttemptFailed,
            Self::WaitTimedOut(_) => EventKind::WaitTimedOut,
            Self::RunTerminated(_) => EventKind::RunTerminated,
            Self::SignalPayload(_) => EventKind::SignalPayload,
            Self::SignalMatched(_) => EventKind::SignalMatched,
        }
    }

    /// True for events produced by guest commands (codes 16–63).
    pub fn is_guest_command(&self) -> bool {
        self.command_meta().is_some()
    }

    /// Command identity, for guest-command events.
    pub fn command_meta(&self) -> Option<&CommandMeta> {
        match self {
            Self::StepCompleted(e) => Some(&e.meta),
            Self::TimerStarted(e) => Some(&e.meta),
            Self::EffectScheduled(e) => Some(&e.meta),
            Self::SignalWaitStarted(e) => Some(&e.meta),
            Self::ChildStarted(e) => Some(&e.meta),
            Self::AwaitTokenCreated(e) => Some(&e.meta),
            Self::OpCancelled(e) => Some(&e.meta),
            Self::WaitResolved(e) => Some(&e.meta),
            Self::PatchMarker(e) => Some(&e.meta),
            Self::StatusPublished(e) => Some(&e.meta),
            Self::RunCompleted(e) => Some(&e.meta),
            Self::RunFailed(e) => Some(&e.meta),
            Self::RunContinuedAsNew(e) => Some(&e.meta),
            Self::StepAttemptFailed(e) => Some(&e.meta),
            Self::RunStarted(_)
            | Self::TimerFired(_)
            | Self::EffectCompleted(_)
            | Self::SignalReceived(_)
            | Self::ChildCompleted(_)
            | Self::TokenCompleted(_)
            | Self::CancelRequested(_)
            | Self::WaitTimedOut(_)
            | Self::RunTerminated(_)
            | Self::SignalPayload(_)
            | Self::SignalMatched(_) => None,
        }
    }

    /// The op an input event completes (`CancelRequested` completes [`CANCELLATION_OP`]).
    /// `RunTerminated` and `SignalPayload` complete nothing.
    pub fn completes_op(&self) -> Option<OpId> {
        match self {
            Self::WaitTimedOut(e) => Some(e.op),
            Self::SignalMatched(e) => Some(e.op),
            Self::TimerFired(e) => Some(e.op),
            Self::EffectCompleted(e) => Some(e.op),
            Self::SignalReceived(e) => Some(e.op),
            Self::ChildCompleted(e) => Some(e.op),
            Self::TokenCompleted(e) => Some(e.op),
            Self::CancelRequested(_) => Some(CANCELLATION_OP),
            _ => None,
        }
    }

    /// Short human text for nondeterminism messages. Never includes payload bytes.
    pub fn summary(&self) -> String {
        fn ops(v: &[OpId]) -> String {
            let parts: Vec<String> = v.iter().map(u64::to_string).collect();
            format!("[{}]", parts.join(","))
        }
        match self {
            Self::RunStarted(e) => format!("run started {:?} v{}", e.workflow, e.workflow_version),
            Self::StepCompleted(e) => format!("step {:?}", e.name),
            Self::TimerStarted(_) => "sleep".to_string(),
            Self::EffectScheduled(e) => match &e.request {
                EffectRequest::Http(r) => format!("http {} {}", r.method, r.url),
                EffectRequest::Sql(r) => format!("sql {:?}", r.connection),
                EffectRequest::Object(r) => format!("object {} {:?}", r.op, r.key),
            },
            Self::SignalWaitStarted(e) => format!("wait-signal {:?}", e.name),
            Self::ChildStarted(e) => format!("start-child {:?}", e.workflow),
            Self::AwaitTokenCreated(_) => "create-await-token".to_string(),
            Self::OpCancelled(e) => format!("cancel-op {}", e.op),
            Self::WaitResolved(e) => format!("wait-resolved {}", ops(&e.completed)),
            Self::PatchMarker(e) => format!("patched {:?}", e.change_id),
            Self::StatusPublished(_) => "set-status".to_string(),
            Self::RunCompleted(_) => "run completed".to_string(),
            Self::RunFailed(_) => "run failed".to_string(),
            Self::RunContinuedAsNew(_) => "continue-as-new".to_string(),
            Self::TimerFired(e) => format!("timer fired op {}", e.op),
            Self::EffectCompleted(e) => format!("effect completed op {}", e.op),
            Self::SignalReceived(e) => format!("signal received op {}", e.op),
            Self::ChildCompleted(e) => format!("child completed op {}", e.op),
            Self::TokenCompleted(e) => format!("token completed op {}", e.op),
            Self::CancelRequested(_) => "cancel requested".to_string(),
            Self::StepAttemptFailed(e) => {
                format!("step {:?} attempt {} failed", e.name, e.attempt)
            }
            Self::WaitTimedOut(e) => format!("wait timed out op {}", e.op),
            Self::RunTerminated(e) => format!("run terminated ({:?})", e.reason),
            Self::SignalPayload(e) => format!("signal payload {:?}", e.name),
            Self::SignalMatched(e) => {
                format!("signal matched op {} (payload seq {})", e.op, e.payload_seq)
            }
        }
    }
}

/// A journal event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub seq: u64,
    pub ts_nanos: u64,
    pub body: EventBody,
}

/// Arguments of a guest command, input to the per-kind command-hash spec (see module docs).
///
/// Build effect args with [`command_args`], which keeps only hashed fields (e.g. header
/// names, never header values).
#[derive(Debug, Clone, PartialEq)]
pub enum CommandArgs<'a> {
    Step {
        name: &'a str,
    },
    Http {
        method: &'a str,
        url: &'a str,
        body: &'a [u8],
        header_names: Vec<&'a str>,
    },
    Sql {
        connection: &'a str,
        statement: &'a str,
        params: &'a [SqlValue],
    },
    Object {
        connection: &'a str,
        op: &'a str,
        key: &'a str,
        body: &'a [u8],
    },
    Sleep {
        duration_ms: u64,
    },
    SleepUntil {
        seconds: u64,
        nanoseconds: u32,
    },
    WaitSignal {
        name: &'a str,
    },
    StartChild {
        workflow: &'a str,
        input: &'a [u8],
    },
    CreateAwaitToken,
    CancelOp {
        op: OpId,
    },
    Wait {
        ops: &'a [OpId],
        mode: WaitMode,
    },
    Patch {
        change_id: &'a str,
    },
    SetStatus,
    Terminal,
}

impl CommandArgs<'_> {
    /// Command kind of these arguments.
    pub fn kind(&self) -> CommandKind {
        match self {
            Self::Step { .. } => CommandKind::Step,
            Self::Http { .. } => CommandKind::Http,
            Self::Sql { .. } => CommandKind::Sql,
            Self::Object { .. } => CommandKind::Object,
            Self::Sleep { .. } => CommandKind::Sleep,
            Self::SleepUntil { .. } => CommandKind::SleepUntil,
            Self::WaitSignal { .. } => CommandKind::WaitSignal,
            Self::StartChild { .. } => CommandKind::StartChild,
            Self::CreateAwaitToken => CommandKind::CreateAwaitToken,
            Self::CancelOp { .. } => CommandKind::CancelOp,
            Self::Wait { .. } => CommandKind::Wait,
            Self::Patch { .. } => CommandKind::Patch,
            Self::SetStatus => CommandKind::SetStatus,
            Self::Terminal => CommandKind::Terminal,
        }
    }
}

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

/// Journal codec / hash error. Unknown kinds and versions are errors, never skipped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// `JournalEvent.kind` is not a known [`EventKind`] code.
    UnknownKind(u32),
    /// Codec or hash version not supported by this node (written by a newer node?).
    UnsupportedVersion(u8),
    /// Bytes do not decode (truncated, trailing bytes, invalid contents).
    Malformed(String),
    /// The payload does not match the event kind it was stored under.
    KindMismatch { kind: u32 },
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownKind(k) => write!(f, "unknown journal event kind {k}"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported journal codec/hash version {v}"),
            Self::Malformed(m) => write!(f, "malformed journal bytes: {m}"),
            Self::KindMismatch { kind } => {
                write!(f, "journal payload does not match event kind {kind}")
            }
        }
    }
}

impl std::error::Error for CodecError {}

impl EventKind {
    /// Kind for a stored code; `None` for unknown codes.
    pub fn from_code(code: u32) -> Option<Self> {
        Some(match code {
            1 => Self::RunStarted,
            16 => Self::StepCompleted,
            17 => Self::TimerStarted,
            18 => Self::EffectScheduled,
            19 => Self::SignalWaitStarted,
            20 => Self::ChildStarted,
            21 => Self::AwaitTokenCreated,
            22 => Self::OpCancelled,
            23 => Self::WaitResolved,
            24 => Self::PatchMarker,
            25 => Self::StatusPublished,
            26 => Self::RunCompleted,
            27 => Self::RunFailed,
            28 => Self::RunContinuedAsNew,
            29 => Self::StepAttemptFailed,
            64 => Self::TimerFired,
            65 => Self::EffectCompleted,
            66 => Self::SignalReceived,
            67 => Self::ChildCompleted,
            68 => Self::TokenCompleted,
            69 => Self::CancelRequested,
            70 => Self::WaitTimedOut,
            71 => Self::RunTerminated,
            96 => Self::SignalPayload,
            97 => Self::SignalMatched,
            _ => return None,
        })
    }
}

fn postcard_bytes<T: Serialize + ?Sized>(v: &T) -> Vec<u8> {
    // Every type serialised here is an Ostro-owned struct/enum of sized primitives,
    // strings, vecs and options: postcard serialisation into a Vec cannot fail.
    postcard::to_allocvec(v).expect("postcard serialisation of journal types is infallible")
}

/// Postcard-decode exactly one value; trailing bytes are an error.
fn postcard_exact<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    let (v, rest) =
        postcard::take_from_bytes::<T>(bytes).map_err(|e| CodecError::Malformed(e.to_string()))?;
    if !rest.is_empty() {
        return Err(CodecError::Malformed(format!(
            "{} trailing bytes",
            rest.len()
        )));
    }
    Ok(v)
}

/// Encode an event as `(kind, bytes)`; see the module docs for the layout.
pub fn encode_event(ev: &Event) -> (u32, Vec<u8>) {
    fn enc<T: Serialize>(ts: u64, p: &T) -> Vec<u8> {
        let mut out = vec![EVENT_CODEC_VER];
        out.extend_from_slice(&postcard_bytes(&(ts, p)));
        out
    }
    let ts = ev.ts_nanos;
    let bytes = match &ev.body {
        EventBody::RunStarted(p) => enc(ts, p),
        EventBody::StepCompleted(p) => enc(ts, p),
        EventBody::TimerStarted(p) => enc(ts, p),
        EventBody::EffectScheduled(p) => enc(ts, p),
        EventBody::SignalWaitStarted(p) => enc(ts, p),
        EventBody::ChildStarted(p) => enc(ts, p),
        EventBody::AwaitTokenCreated(p) => enc(ts, p),
        EventBody::OpCancelled(p) => enc(ts, p),
        EventBody::WaitResolved(p) => enc(ts, p),
        EventBody::PatchMarker(p) => enc(ts, p),
        EventBody::StatusPublished(p) => enc(ts, p),
        EventBody::RunCompleted(p) => enc(ts, p),
        EventBody::RunFailed(p) => enc(ts, p),
        EventBody::RunContinuedAsNew(p) => enc(ts, p),
        EventBody::TimerFired(p) => enc(ts, p),
        EventBody::EffectCompleted(p) => enc(ts, p),
        EventBody::SignalReceived(p) => enc(ts, p),
        EventBody::ChildCompleted(p) => enc(ts, p),
        EventBody::TokenCompleted(p) => enc(ts, p),
        EventBody::CancelRequested(p) => enc(ts, p),
        EventBody::StepAttemptFailed(p) => enc(ts, p),
        EventBody::WaitTimedOut(p) => enc(ts, p),
        EventBody::RunTerminated(p) => enc(ts, p),
        EventBody::SignalPayload(p) => enc(ts, p),
        EventBody::SignalMatched(p) => enc(ts, p),
    };
    (ev.body.kind().code(), bytes)
}

/// Decode an event stored as `(seq, kind, bytes)`.
pub fn decode_event(seq: u64, kind: u32, bytes: &[u8]) -> Result<Event, CodecError> {
    let k = EventKind::from_code(kind).ok_or(CodecError::UnknownKind(kind))?;
    let (&ver, rest) = bytes
        .split_first()
        .ok_or_else(|| CodecError::Malformed("empty event bytes".into()))?;
    if ver != EVENT_CODEC_VER {
        return Err(CodecError::UnsupportedVersion(ver));
    }
    fn dec<T: DeserializeOwned>(
        rest: &[u8],
        wrap: fn(T) -> EventBody,
    ) -> Result<(u64, EventBody), CodecError> {
        let (ts, p) = postcard_exact::<(u64, T)>(rest)?;
        Ok((ts, wrap(p)))
    }
    let (ts_nanos, body) = match k {
        EventKind::RunStarted => dec(rest, EventBody::RunStarted),
        EventKind::StepCompleted => dec(rest, EventBody::StepCompleted),
        EventKind::TimerStarted => dec(rest, EventBody::TimerStarted),
        EventKind::EffectScheduled => dec(rest, EventBody::EffectScheduled),
        EventKind::SignalWaitStarted => dec(rest, EventBody::SignalWaitStarted),
        EventKind::ChildStarted => dec(rest, EventBody::ChildStarted),
        EventKind::AwaitTokenCreated => dec(rest, EventBody::AwaitTokenCreated),
        EventKind::OpCancelled => dec(rest, EventBody::OpCancelled),
        EventKind::WaitResolved => dec(rest, EventBody::WaitResolved),
        EventKind::PatchMarker => dec(rest, EventBody::PatchMarker),
        EventKind::StatusPublished => dec(rest, EventBody::StatusPublished),
        EventKind::RunCompleted => dec(rest, EventBody::RunCompleted),
        EventKind::RunFailed => dec(rest, EventBody::RunFailed),
        EventKind::RunContinuedAsNew => dec(rest, EventBody::RunContinuedAsNew),
        EventKind::TimerFired => dec(rest, EventBody::TimerFired),
        EventKind::EffectCompleted => dec(rest, EventBody::EffectCompleted),
        EventKind::SignalReceived => dec(rest, EventBody::SignalReceived),
        EventKind::ChildCompleted => dec(rest, EventBody::ChildCompleted),
        EventKind::TokenCompleted => dec(rest, EventBody::TokenCompleted),
        EventKind::CancelRequested => dec(rest, EventBody::CancelRequested),
        EventKind::StepAttemptFailed => dec(rest, EventBody::StepAttemptFailed),
        EventKind::WaitTimedOut => dec(rest, EventBody::WaitTimedOut),
        EventKind::RunTerminated => dec(rest, EventBody::RunTerminated),
        EventKind::SignalPayload => dec(rest, EventBody::SignalPayload),
        EventKind::SignalMatched => dec(rest, EventBody::SignalMatched),
    }?;
    Ok(Event {
        seq,
        ts_nanos,
        body,
    })
}

/// Encode a recorded result (ENG-04).
pub fn encode_result<T: Serialize>(v: &T) -> RecordedResult {
    RecordedResult {
        codec_ver: RESULT_CODEC_VER,
        bytes: postcard_bytes(v),
    }
}

/// Decode a recorded result; unknown version or trailing bytes are errors.
pub fn decode_result<T: DeserializeOwned>(r: &RecordedResult) -> Result<T, CodecError> {
    if r.codec_ver != RESULT_CODEC_VER {
        return Err(CodecError::UnsupportedVersion(r.codec_ver));
    }
    postcard_exact(&r.bytes)
}

// ---------------------------------------------------------------------------
// Command hash
// ---------------------------------------------------------------------------

const HASH_DOMAIN: &[u8] = b"ostro/cmd";
const CANONICAL_NAN: u64 = 0x7ff8_0000_0000_0000;

fn sha256(b: &[u8]) -> [u8; 32] {
    Sha256::digest(b).into()
}

/// Canonical writer: the single place primitive layouts are defined (module docs table).
#[derive(Default)]
struct Canon(Vec<u8>);

impl Canon {
    fn len(&mut self, n: usize) {
        // Guest memory is wasm32 (< 4 GiB), so lengths always fit; overflow is a bug.
        self.u32(u32::try_from(n).expect("canonical length exceeds u32"));
    }
    fn str(&mut self, s: &str) {
        self.len(s.len());
        self.0.extend_from_slice(s.as_bytes());
    }
    fn blob(&mut self, b: &[u8]) {
        self.len(b.len());
        self.0.extend_from_slice(b);
    }
    fn bytes_digest(&mut self, b: &[u8]) {
        self.0.extend_from_slice(&sha256(b));
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn i64(&mut self, v: i64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn f64(&mut self, v: f64) {
        self.u64(if v.is_nan() {
            CANONICAL_NAN
        } else {
            v.to_bits()
        });
    }
    fn list<T>(&mut self, items: &[T], mut f: impl FnMut(&mut Self, &T)) {
        self.len(items.len());
        for it in items {
            f(self, it);
        }
    }
    fn sql_value(&mut self, v: &SqlValue) {
        match v {
            SqlValue::Null => self.u8(0),
            SqlValue::Boolean(b) => {
                self.u8(1);
                self.u8(u8::from(*b));
            }
            SqlValue::Int8(i) => {
                self.u8(2);
                self.i64(*i);
            }
            SqlValue::Float8(f) => {
                self.u8(3);
                self.f64(*f);
            }
            SqlValue::Numeric(s) => {
                self.u8(4);
                self.str(s);
            }
            SqlValue::Text(s) => {
                self.u8(5);
                self.str(s);
            }
            SqlValue::Bytea(b) => {
                self.u8(6);
                self.blob(b);
            }
            SqlValue::Json(s) => {
                self.u8(7);
                self.str(s);
            }
            SqlValue::Timestamptz {
                seconds,
                nanoseconds,
            } => {
                self.u8(8);
                self.u64(*seconds);
                self.u32(*nanoseconds);
            }
        }
    }
}

/// Hand-specified canonical argument bytes (hash v1; see module docs).
pub fn canonical_args(args: &CommandArgs<'_>) -> Vec<u8> {
    let mut c = Canon::default();
    match args {
        CommandArgs::Step { name } => c.str(name),
        CommandArgs::Http {
            method,
            url,
            body,
            header_names,
        } => {
            c.str(method);
            c.str(url);
            c.bytes_digest(body);
            let mut names: Vec<String> = header_names
                .iter()
                .map(|n| n.to_ascii_lowercase())
                .collect();
            names.sort_unstable();
            c.list(&names, |c, n| c.str(n));
        }
        CommandArgs::Sql {
            connection,
            statement,
            params,
        } => {
            c.str(connection);
            c.str(statement);
            let mut p = Canon::default();
            p.list(params, Canon::sql_value);
            c.bytes_digest(&p.0);
        }
        CommandArgs::Object {
            connection,
            op,
            key,
            body,
        } => {
            c.str(connection);
            c.str(op);
            c.str(key);
            c.bytes_digest(body);
        }
        CommandArgs::Sleep { duration_ms } => c.u64(*duration_ms),
        CommandArgs::SleepUntil {
            seconds,
            nanoseconds,
        } => {
            c.u64(*seconds);
            c.u32(*nanoseconds);
        }
        CommandArgs::WaitSignal { name } => c.str(name),
        CommandArgs::StartChild { workflow, input } => {
            c.str(workflow);
            c.bytes_digest(input);
        }
        CommandArgs::CancelOp { op } => c.u64(*op),
        CommandArgs::Wait { ops, mode } => {
            c.list(ops, |c, op| c.u64(*op));
            c.u8(match mode {
                WaitMode::All => 0,
                WaitMode::Any => 1,
            });
        }
        CommandArgs::Patch { change_id } => c.str(change_id),
        CommandArgs::CreateAwaitToken | CommandArgs::SetStatus | CommandArgs::Terminal => {}
    }
    c.0
}

/// Command hash: `sha256(b"ostro/cmd" ‖ [hash_ver] ‖ [kind] ‖ canonical_args)[..16]`.
///
/// Only [`HASH_VER`] is supported; any other version (e.g. a journal written by a newer
/// node) is [`CodecError::UnsupportedVersion`].
pub fn command_hash(hash_ver: u8, args: &CommandArgs<'_>) -> Result<CommandHash, CodecError> {
    if hash_ver != HASH_VER {
        return Err(CodecError::UnsupportedVersion(hash_ver));
    }
    let mut h = Sha256::new();
    h.update(HASH_DOMAIN);
    h.update([hash_ver, args.kind() as u8]);
    h.update(canonical_args(args));
    let d: [u8; 32] = h.finalize().into();
    let mut out = [0u8; 16];
    out.copy_from_slice(&d[..16]);
    Ok(CommandHash(out))
}

/// Hash arguments for an effect request; keeps only hashed fields (header names, not values).
pub fn command_args(req: &EffectRequest) -> CommandArgs<'_> {
    match req {
        EffectRequest::Http(r) => CommandArgs::Http {
            method: &r.method,
            url: &r.url,
            body: r.body.as_deref().unwrap_or(&[]),
            header_names: r.headers.iter().map(|h| h.name.as_str()).collect(),
        },
        EffectRequest::Sql(r) => CommandArgs::Sql {
            connection: &r.connection,
            statement: &r.statement,
            params: &r.params,
        },
        EffectRequest::Object(r) => CommandArgs::Object {
            connection: &r.connection,
            op: &r.op,
            key: &r.key,
            body: r.body.as_deref().unwrap_or(&[]),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> CommandMeta {
        CommandMeta {
            hash_ver: HASH_VER,
            hash: CommandHash([7; 16]),
        }
    }

    fn payload() -> Payload {
        Payload {
            content_type: "application/json".into(),
            data: b"{}".to_vec(),
        }
    }

    fn rr() -> RecordedResult {
        RecordedResult {
            codec_ver: RESULT_CODEC_VER,
            bytes: vec![0],
        }
    }

    /// One instance of every EventBody variant with its expected kind.
    fn all_bodies() -> Vec<(EventBody, EventKind)> {
        let m = meta();
        vec![
            (
                EventBody::RunStarted(RunStarted {
                    workflow: "wf".into(),
                    workflow_version: 1,
                    input: payload(),
                    rng_seed: RngSeed([1; 32]),
                    accepted_at_nanos: 1,
                    deadline_nanos: None,
                    module_hash: "abc".into(),
                    tenant: None,
                    parent_run_id: None,
                    continued_from: None,
                }),
                EventKind::RunStarted,
            ),
            (
                EventBody::StepCompleted(StepCompleted {
                    meta: m,
                    name: "charge".into(),
                    outcome: rr(),
                    fuel_used: 5,
                }),
                EventKind::StepCompleted,
            ),
            (
                EventBody::TimerStarted(TimerStarted {
                    meta: m,
                    fire_at_nanos: 9,
                }),
                EventKind::TimerStarted,
            ),
            (
                EventBody::EffectScheduled(EffectScheduled {
                    meta: m,
                    request: EffectRequest::Object(ObjectRequest {
                        connection: "s3".into(),
                        op: "get".into(),
                        key: "k".into(),
                        body: None,
                    }),
                }),
                EventKind::EffectScheduled,
            ),
            (
                EventBody::SignalWaitStarted(SignalWaitStarted {
                    meta: m,
                    name: "approve".into(),
                    timeout_ms: Some(10),
                }),
                EventKind::SignalWaitStarted,
            ),
            (
                EventBody::ChildStarted(ChildStarted {
                    meta: m,
                    workflow: "child".into(),
                    input: payload(),
                    options: ChildOptions {
                        queue: None,
                        version: None,
                        detached: false,
                        run_timeout_ms: None,
                    },
                }),
                EventKind::ChildStarted,
            ),
            (
                EventBody::AwaitTokenCreated(AwaitTokenCreated {
                    meta: m,
                    token: "t".into(),
                    timeout_ms: None,
                }),
                EventKind::AwaitTokenCreated,
            ),
            (
                EventBody::OpCancelled(OpCancelled { meta: m, op: 3 }),
                EventKind::OpCancelled,
            ),
            (
                EventBody::WaitResolved(WaitResolved {
                    meta: m,
                    completed: vec![3, 5],
                }),
                EventKind::WaitResolved,
            ),
            (
                EventBody::PatchMarker(PatchMarker {
                    meta: m,
                    change_id: "v2".into(),
                }),
                EventKind::PatchMarker,
            ),
            (
                EventBody::StatusPublished(StatusPublished {
                    meta: m,
                    json: "{}".into(),
                }),
                EventKind::StatusPublished,
            ),
            (
                EventBody::RunCompleted(RunCompleted {
                    meta: m,
                    output: payload(),
                }),
                EventKind::RunCompleted,
            ),
            (
                EventBody::RunFailed(RunFailed {
                    meta: m,
                    failure: Failure::Timeout,
                }),
                EventKind::RunFailed,
            ),
            (
                EventBody::RunContinuedAsNew(RunContinuedAsNew {
                    meta: m,
                    continuation: Continuation {
                        input: payload(),
                        workflow: None,
                        version: None,
                    },
                }),
                EventKind::RunContinuedAsNew,
            ),
            (
                EventBody::TimerFired(TimerFired { op: 1 }),
                EventKind::TimerFired,
            ),
            (
                EventBody::EffectCompleted(EffectCompleted {
                    op: 2,
                    outcome: rr(),
                }),
                EventKind::EffectCompleted,
            ),
            (
                EventBody::SignalReceived(SignalReceived {
                    op: 3,
                    payload: payload(),
                }),
                EventKind::SignalReceived,
            ),
            (
                EventBody::ChildCompleted(ChildCompleted {
                    op: 4,
                    outcome: rr(),
                }),
                EventKind::ChildCompleted,
            ),
            (
                EventBody::TokenCompleted(TokenCompleted {
                    op: 5,
                    payload: payload(),
                }),
                EventKind::TokenCompleted,
            ),
            (
                EventBody::CancelRequested(CancelRequested {
                    reason: "user".into(),
                }),
                EventKind::CancelRequested,
            ),
            (
                EventBody::StepAttemptFailed(StepAttemptFailed {
                    meta: meta(),
                    name: "charge".into(),
                    attempt: 1,
                    failure: Failure::Timeout,
                    retry_at_nanos: 9,
                    backoff: AttemptBackoff::InActivation,
                    fuel_used: 0,
                }),
                EventKind::StepAttemptFailed,
            ),
            (
                EventBody::WaitTimedOut(WaitTimedOut { op: 3 }),
                EventKind::WaitTimedOut,
            ),
            (
                EventBody::RunTerminated(RunTerminated {
                    reason: "ops".into(),
                }),
                EventKind::RunTerminated,
            ),
            (
                EventBody::SignalPayload(SignalPayload {
                    name: "approve".into(),
                    payload: payload(),
                }),
                EventKind::SignalPayload,
            ),
            (
                EventBody::SignalMatched(SignalMatched {
                    op: 4,
                    payload_seq: 7,
                }),
                EventKind::SignalMatched,
            ),
        ]
    }

    #[test]
    fn kind_round_trips_for_every_variant() {
        let bodies = all_bodies();
        assert_eq!(bodies.len(), 25);
        for (body, kind) in &bodies {
            assert_eq!(body.kind(), *kind);
        }
    }

    #[test]
    fn guest_commands_are_exactly_codes_16_to_29() {
        for (body, kind) in all_bodies() {
            let code = kind.code();
            assert_eq!(
                body.is_guest_command(),
                (16..=29).contains(&code),
                "code {code}"
            );
            // Inputs 64..=70 and SignalMatched (97) complete an op; RunTerminated (71) and
            // SignalPayload (96) do not.
            assert_eq!(
                body.completes_op().is_some(),
                (64..=70).contains(&code) || code == 97,
                "code {code}"
            );
        }
    }

    #[test]
    fn event_kind_codes_are_pinned() {
        let expected: [(EventKind, u32); 25] = [
            (EventKind::RunStarted, 1),
            (EventKind::StepCompleted, 16),
            (EventKind::TimerStarted, 17),
            (EventKind::EffectScheduled, 18),
            (EventKind::SignalWaitStarted, 19),
            (EventKind::ChildStarted, 20),
            (EventKind::AwaitTokenCreated, 21),
            (EventKind::OpCancelled, 22),
            (EventKind::WaitResolved, 23),
            (EventKind::PatchMarker, 24),
            (EventKind::StatusPublished, 25),
            (EventKind::RunCompleted, 26),
            (EventKind::RunFailed, 27),
            (EventKind::RunContinuedAsNew, 28),
            (EventKind::StepAttemptFailed, 29),
            (EventKind::TimerFired, 64),
            (EventKind::EffectCompleted, 65),
            (EventKind::SignalReceived, 66),
            (EventKind::ChildCompleted, 67),
            (EventKind::TokenCompleted, 68),
            (EventKind::CancelRequested, 69),
            (EventKind::WaitTimedOut, 70),
            (EventKind::RunTerminated, 71),
            (EventKind::SignalPayload, 96),
            (EventKind::SignalMatched, 97),
        ];
        for (kind, code) in expected {
            assert_eq!(EventKind::from_code(code), Some(kind), "{kind:?}");
        }
        for (kind, code) in expected {
            assert_eq!(kind as u32, code, "{kind:?}");
        }
    }

    #[test]
    fn command_kind_codes_are_pinned() {
        assert_eq!(CommandKind::Step as u8, 1);
        assert_eq!(CommandKind::Terminal as u8, 14);
        assert_eq!(CommandArgs::Step { name: "a" }.kind(), CommandKind::Step);
        assert_eq!(CommandArgs::Terminal.kind(), CommandKind::Terminal);
        assert_eq!(
            CommandArgs::Wait {
                ops: &[1],
                mode: WaitMode::All
            }
            .kind(),
            CommandKind::Wait
        );
    }

    #[test]
    fn summary_has_no_payload_bytes() {
        for (body, _) in all_bodies() {
            assert!(!body.summary().contains("{}"), "{}", body.summary());
        }
        let s = EventBody::StepCompleted(StepCompleted {
            meta: meta(),
            name: "charge".into(),
            outcome: rr(),
            fuel_used: 0,
        })
        .summary();
        assert_eq!(s, "step \"charge\"");
    }
}
