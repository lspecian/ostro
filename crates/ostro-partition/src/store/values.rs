//! postcard value types. Every encoded value starts with a format byte (1).

use std::collections::BTreeMap;

use ostro_types::{PayloadRef, RunId};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::StoreError;

/// Format byte prefixed to every stored value.
pub const VALUE_FORMAT: u8 = 1;

/// Leader/fencing cursor persisted in meta alongside `applied_offset`.
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CursorMeta {
    pub epoch: u32,
    /// `(epoch, first offset of that epoch)`.
    pub epoch_history: Vec<(u32, u64)>,
    /// Next outbox sequence number per target partition.
    pub outbox_next_seq: BTreeMap<u16, u64>,
    /// Highest truncated outbox seq per target: rows up to and including this seq have
    /// been deleted (absent = 0, nothing truncated; seqs start at 1).
    pub outbox_truncated: BTreeMap<u16, u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunStatus {
    /// `Default` only so `RunRecord` can derive it (literal sites use `..Default::default()`).
    #[default]
    Scheduled,
    Ready,
    Running,
    Completed,
    Failed,
    Cancelled,
    // Phase 5 (plan 05-01), appended so the postcard indices of the variants above are
    // unchanged; mirrors TerminalStatus 3..=5 (RESERVATIONS.md §5).
    /// Hard-stopped by an operator (D3).
    Terminated,
    /// Run timeout or schedule-to-start timeout (D2).
    TimedOut,
    /// Continued as a new run (D10).
    ContinuedAsNew,
}

impl RunStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            RunStatus::Completed
                | RunStatus::Failed
                | RunStatus::Cancelled
                | RunStatus::Terminated
                | RunStatus::TimedOut
                | RunStatus::ContinuedAsNew
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadySlot {
    pub priority: i32,
    pub ready_at_ms: u64,
}

/// The run record (`runs` keyspace). Positional postcard: fields are only ever appended.
///
/// Derives `Default` so literal sites (tests, fixtures) write `..Default::default()` and keep
/// compiling when fields are appended (plan 05-02). Apply never builds a record from
/// `Default` without setting every field it owns.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecord {
    pub workflow: String,
    pub workflow_version: u32,
    pub queue: String,
    pub status: RunStatus,
    /// `(epoch, n)` of the current attempt token.
    pub current_attempt: Option<(u32, u32)>,
    pub next_seq: u64,
    pub ready: Option<ReadySlot>,
    /// `(parent run id bytes, op)`.
    pub parent: Option<([u8; 18], u64)>,
    pub cancel_requested: bool,
    pub concurrency_key: String,
    pub priority: i32,
    pub created_at_ms: u64,
    pub rng_seed: Vec<u8>,
    pub inbox_next: u64,
    pub children: u32,
    // ---- Phase 4 additions (plan 04-12). Appended at the end: postcard is positional, so
    // this changes the encoding of every stored RunRecord. Pre-freeze format change, no
    // migration (dev data dirs created before Phase 4 must be deleted). Phase 5 freezes it.
    /// sha256 of the module the run is pinned to (`StartRun.module_sha`; empty = none).
    pub module_sha: Option<[u8; 32]>,
    /// `StartRun.tags`, in submission order.
    pub tags: Vec<String>,
    /// `StartRun.tenant` (empty = none).
    pub tenant: Option<String>,
    /// `StartRun.traceparent` (empty = none).
    pub traceparent: Option<String>,
    /// `StartRun.run_timeout_ms` (0 = none).
    pub run_timeout_ms: Option<u64>,
    /// `CompleteRun.completed_at_ms` of the terminal transition (0 = none).
    pub completed_at_ms: Option<u64>,
    // ---- Phase 5 additions (plan 05-02). Appended at the end: pre-freeze positional postcard
    // change with no VALUE_FORMAT bump (delete pre-Phase-5 dev data dirs). After the 05-20
    // freeze every change here needs a row migrator (amended D18, docs/storage-format.md).
    /// `StartRun.schedule_to_start_ms` (0 = none). Initial start only (D2).
    pub schedule_to_start_ms: u64,
    /// `StartRun.retention_ms` of the terminal run (0 = keep forever; D9).
    pub retention_ms: u64,
    /// `StartRun.idempotency_key` (empty = none). Stored so expiry can delete the dedupe row
    /// `(workflow, idempotency_key)` (05-09). Cron starts dedupe under `__cron/<schedule>`
    /// with the fire time and store their own (usually empty) key here.
    pub idempotency_key: Vec<u8>,
    /// Reason of the terminal transition (`run_timeout`, `schedule_to_start`, an operator's
    /// terminate reason; empty for executor completions).
    pub terminal_reason: String,
    /// The terminal run may be resumed in place by `RetryRun` (D1; `CompleteRun.resumable`).
    pub resumable: bool,
    /// `StartRun.retry_of`: the run this run retries as a new linked run (D1).
    #[serde(with = "opt_run_id")]
    pub retry_of: Option<RunId>,
    /// `StartRun.continued_from`: the predecessor this run continues (D10).
    #[serde(with = "opt_run_id")]
    pub continued_from: Option<RunId>,
    /// Reason of the first graceful cancel request (`RequestCancel` / inbound `CancelRun`).
    pub cancel_reason: String,
    /// Number of resume-in-place retries (D1).
    pub resumed_count: u32,
    /// Live waits of the run (`waits` rows of shape 0x01). Terminal cleanup skips both
    /// `waits` prefix scans when it is 0.
    pub open_waits: u32,
    /// Due time of the pending `SCHEDULE_TO_START_OP` timer (0 = none), so the first grant
    /// deletes it with one point delete (D2).
    pub s2s_due_ms: u64,
    /// Durable count of retryable attempt failures (pre-freeze F9; incremented on
    /// `AttemptGranted.after_failure` by 05-22, reset by RetryRun in 05-09).
    pub attempt_failures: u32,
    /// Seq of the last `StatusPublished` journal event (pre-freeze F10; set by 05-22).
    pub status_seq: Option<u64>,
    /// The successor of a run that continued as new (Phase 9 PF-F2; set by 05-09).
    #[serde(with = "opt_run_id")]
    pub continued_to: Option<RunId>,
    /// Pause marker (Phase 7 F1). Stored only; Phase 5 never sets it.
    pub pause: Option<PauseMark>,
}

/// Why and when a run was paused (Phase 7 F1, stored only in Phase 5).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PauseMark {
    /// Pause reason code (Phase 7 assigns the values).
    pub reason: u8,
    /// Time the pause took effect (ms).
    pub at_ms: u64,
    /// Human-readable detail.
    pub detail: String,
}

/// Value of a `ready` row. Derives `Default` (literal sites use `..Default::default()`).
///
/// Carries everything the dispatcher needs to queue the run, so a leader rebuild over the
/// `ready` keyspace needs no run-record point reads (Phase 7 F3/F7, plan 05-22).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadyValue {
    pub concurrency_key: String,
    /// `RunRecord.workflow` (F3).
    pub workflow: String,
    /// `RunRecord.tenant` (F3; `None` = no tenant).
    pub tenant: Option<String>,
    /// `true` when no attempt was ever granted (`RunRecord.current_attempt.is_none()` at
    /// enqueue time), i.e. the next activation is the run's first (F7).
    pub first: bool,
}

/// Value of an `effects` row (`effect_key(run, op)`, keyspace #12, RESERVATIONS §6): one
/// in-flight http/sql/object effect of the run (Phase 6, plan 06-03). Written by the
/// `RegisterEffect` intent, deleted by `CancelEffect`, by the first `CompleteEffect`, by
/// non-resumable terminal transitions (P-8) and by retention expiry. Derived value (D18,
/// additive: a new value type in an existing keyspace that had no rows before Phase 6).
/// Stored as `VALUE_FORMAT ‖ postcard`. Carries no payload (no `BlobRef`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectRow {
    /// `ostro_types::live::EffectKind` as `u8` (Http 0, Sql 1, Object 2).
    pub kind: u8,
    /// `RegisterEffect.scheduled_at_ms` (time of the `EffectScheduled` event).
    pub scheduled_at_ms: u64,
}

/// What a `waits` row waits for (mirrors `ostro_types::command::WaitKind`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WaitKind {
    /// A named signal.
    Signal,
    /// An await-token completion.
    Token,
}

/// Value of a `waits` wait row (`wait_key(run, op)`, shape 0x01). The FIFO index row
/// (`wait_name_key`, shape 0x02) has an empty value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitRow {
    pub kind: WaitKind,
    /// Signal name (empty for tokens).
    pub name: String,
    /// Due time of the wait-timeout timer `(timeout_due_ms, run, op)` (0 = none).
    pub timeout_due_ms: u64,
}

/// Value of a signal inbox row (`signal_inbox_key(run, name, n)`): a buffered signal no wait
/// has consumed yet (D21). Inbox rows never hold raw signal payload bytes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SignalSlot {
    /// The signal's `SignalPayload` (kind 96) event is already journaled at `payload_seq`;
    /// a later match journals `SignalMatched{op, payload_seq}`.
    Journaled { payload_seq: u64, sent_at_ms: u64 },
    /// The signal arrived before the run's seq 0 existed (B1): `event` holds the
    /// submitter-encoded `SignalPayload` event bytes (inline or blob; stored as the prost
    /// `PayloadRef` encoding, so a `BlobRef` here is covered by the BlobRef invariant). The
    /// seq-0 batch journals it and converts the slot to `Journaled`.
    Pending {
        #[serde(with = "payload_ref_bytes")]
        event: PayloadRef,
        sent_at_ms: u64,
    },
}

/// Watermark row of one schedule in `schedule_state` (Phase 8, P8-D10; key
/// `schedule_state_key(name)`). Written by 08-07's `ScheduleFire` apply. Positional postcard:
/// fields are only ever appended.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleStateRow {
    /// The meta `Schedule.created_at_ms` of the schedule incarnation this row belongs to (W4):
    /// a re-created schedule with the same name starts from a fresh row.
    pub schedule_created_at_ms: u64,
    /// Highest nominal fire time applied (`Fire` or `Advance`), ms. Fires at or below it are
    /// duplicates.
    pub watermark_ms: u64,
    /// The run started by the latest fire (overlap policies look at it).
    #[serde(with = "opt_run_id")]
    pub last_run: Option<RunId>,
    /// A fire held back by `BufferOne` overlap, started when `last_run` ends.
    pub buffered: Option<BufferedFire>,
    /// Fires that started a run.
    pub fired: u64,
    /// Fires skipped (overlap `Skip` and catch-up skips).
    pub skipped: u64,
    /// Duplicate fires (at or below the watermark, or a repeated manual `request_id`).
    pub duplicates: u64,
}

/// A buffered schedule fire ([`ScheduleStateRow::buffered`]).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BufferedFire {
    /// Nominal fire time (ms).
    pub fire_time_ms: u64,
    /// The prost-encoded `StartRun` to submit when the previous run ends.
    pub start: Vec<u8>,
}

/// Reverse-index row `schedule_run_key(run)` in `schedule_state` (Phase 8): which schedule
/// (incarnation) started the run, so its terminal transition can update the schedule row.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleRunRow {
    /// Schedule name.
    pub schedule: String,
    /// The schedule's `created_at_ms` (incarnation, W4).
    pub schedule_created_at_ms: u64,
}

/// serde for `Option<RunId>` as `Option<[u8; 18]>`.
mod opt_run_id {
    use ostro_types::RunId;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(v: &Option<RunId>, s: S) -> Result<S::Ok, S::Error> {
        v.map(|r| *r.as_bytes()).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<RunId>, D::Error> {
        Option::<[u8; 18]>::deserialize(d)?
            .map(|b| RunId::from_bytes(&b).map_err(serde::de::Error::custom))
            .transpose()
    }
}

/// serde for a prost `PayloadRef` as its prost encoding (a byte string).
mod payload_ref_bytes {
    use ostro_types::PayloadRef;
    use prost::Message;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(v: &PayloadRef, s: S) -> Result<S::Ok, S::Error> {
        v.encode_to_vec().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<PayloadRef, D::Error> {
        let b = <Vec<u8>>::deserialize(d)?;
        PayloadRef::decode(b.as_slice()).map_err(serde::de::Error::custom)
    }
}

/// Value of a legacy inbox row (`inbox_key(run, n)`, name-length sentinel `0xFFFF`).
///
/// Signals are [`SignalSlot`] rows keyed by name since plan 05-02 (the `Signal` variant was
/// removed). Inbound `CancelRun` still writes `CancelRequested` until 05-08 journals it, and
/// inbound `ChildCompleted` keeps writing `ChildCompleted` until Phase 9 journals it as an
/// input. `TimerFired` is never written (04-13); it stays because the 04-13 regression test
/// (`tests/timers.rs`) matches on it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InboxMsg {
    ChildCompleted {
        child: [u8; 18],
        op: u64,
        status: RunStatus,
        result: Vec<u8>,
    },
    CancelRequested {
        reason: String,
    },
    /// No longer written since plan 04-13 (timer inputs are journal events).
    /// Kept so the postcard variant indices stay stable.
    TimerFired {
        op: u64,
        fired_at_ms: u64,
    },
}

/// `VALUE_FORMAT ‖ postcard(v)`.
pub fn encode<T: Serialize>(v: &T) -> Vec<u8> {
    let mut out = vec![VALUE_FORMAT];
    // postcard serialisation into a Vec only fails for types serde cannot
    // represent (none of ours); treat it as a programming error.
    let body = postcard::to_allocvec(v).expect("postcard encode of a store value");
    out.extend_from_slice(&body);
    out
}

/// Checks the format byte and decodes the postcard body.
pub fn decode<T: DeserializeOwned>(b: &[u8]) -> Result<T, StoreError> {
    match b.split_first() {
        Some((&VALUE_FORMAT, body)) => {
            postcard::from_bytes(body).map_err(|e| StoreError::Codec(e.to_string()))
        }
        Some((&f, _)) => Err(StoreError::Codec(format!("unknown value format byte {f}"))),
        None => Err(StoreError::Codec("empty value".into())),
    }
}
