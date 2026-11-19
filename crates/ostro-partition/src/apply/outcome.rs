//! Apply outcomes, rejection reasons and post-commit actions (STOR-11).
//!
//! # Frozen outcome codes
//!
//! [`ApplyOutcome::code`] is folded into the rolling state hash, so the
//! table below is part of the on-disk format and must never change. New
//! reasons get new codes appended at the end.
//!
//! | outcome                              | code |
//! |--------------------------------------|------|
//! | `Accepted`                           | 0    |
//! | `Duplicate`                          | 1    |
//! | `Rejected(StaleEpoch)`               | 16   |
//! | `Rejected(Fenced)`                   | 17   |
//! | `Rejected(SeqMismatch)`              | 18   |
//! | `Rejected(SeqGap)`                   | 19   |
//! | `Rejected(RunNotFound)`              | 20   |
//! | `Rejected(RunTerminal)`              | 21   |
//! | `Rejected(RunExists)`                | 22   |
//! | `Rejected(IdempotencyConflict)`      | 23   |
//! | `Rejected(PayloadTooLarge)`          | 24   |
//! | `Rejected(WrongPartition)`           | 25   |
//! | `Rejected(Invalid(_))` (any code)    | 26   |
//!
//! The [`InvalidCode`] detail is not part of the hash code (the command
//! bytes are folded anyway, and they determine the detail). `InvalidCode`
//! variants are append-only as well (02-09 added `BadIntent`,
//! `FieldTooLong`; 02-10 added `BadSource`, `TruncateBeyondEnd`; 04-12 added `BadLength`,
//! `NotRunning`).

use ostro_types::{AttemptToken, Epoch, PartitionId, RunId};

use crate::store::StoreError;
use crate::store::values::RunStatus;

/// Result of an apply handler: the outcome, or a fatal store error.
pub(crate) type ApplyResult = Result<ApplyOutcome, StoreError>;

/// Result of applying one command. Rejections are deterministic no-ops that
/// still consume their offset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    Accepted,
    Duplicate { existing: Option<RunId> },
    Rejected(RejectReason),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RejectReason {
    StaleEpoch { current: Epoch },
    Fenced { current: Option<AttemptToken> },
    SeqMismatch { expected: u64 },
    SeqGap { expected: u64, found: u64 },
    RunNotFound,
    RunTerminal,
    RunExists,
    IdempotencyConflict { existing: RunId },
    PayloadTooLarge,
    WrongPartition,
    Invalid(InvalidCode),
}

/// Detail of a `Rejected(Invalid)` outcome.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum InvalidCode {
    /// The command bytes do not decode (or carry an unknown enum value).
    Decode,
    /// `Command { cmd: None }`.
    Empty,
    /// A run id is not 18 bytes or names a partition outside the count.
    BadRunId,
    /// An attempt token is absent.
    MissingToken,
    /// `events[i].seq != from_seq + i`.
    EventsNotContiguous,
    /// A required message field is absent.
    MissingField,
    /// The command kind is not implemented yet.
    NotImplemented,
    /// A Derived intent is malformed (unknown status, intent after
    /// `CompleteRun`).
    BadIntent,
    /// A string that becomes part of a key (workflow, queue) is longer than
    /// 65535 bytes, or a StartRun field exceeds its `ostro_types::command`
    /// limit (tags, tenant, traceparent; 04-12).
    FieldTooLong,
    /// `InboundBatch.src` is outside the partition count.
    BadSource,
    /// `OutboxTruncated.upto_seq` names a seq that was never emitted.
    TruncateBeyondEnd,
    /// A fixed-length byte field has the wrong length (`StartRun.module_sha` must be empty or
    /// 32 bytes). Added by 04-12.
    BadLength,
    /// A `Suspend` intent for a run whose status is not `Running`. Added by 04-12.
    NotRunning,
}

impl RejectReason {
    fn index(&self) -> u8 {
        match self {
            RejectReason::StaleEpoch { .. } => 0,
            RejectReason::Fenced { .. } => 1,
            RejectReason::SeqMismatch { .. } => 2,
            RejectReason::SeqGap { .. } => 3,
            RejectReason::RunNotFound => 4,
            RejectReason::RunTerminal => 5,
            RejectReason::RunExists => 6,
            RejectReason::IdempotencyConflict { .. } => 7,
            RejectReason::PayloadTooLarge => 8,
            RejectReason::WrongPartition => 9,
            RejectReason::Invalid(_) => 10,
        }
    }
}

impl ApplyOutcome {
    /// Stable outcome code (see the module table). Frozen: it is folded into
    /// the rolling hash.
    pub fn code(&self) -> u8 {
        match self {
            ApplyOutcome::Accepted => 0,
            ApplyOutcome::Duplicate { .. } => 1,
            ApplyOutcome::Rejected(r) => 16 + r.index(),
        }
    }

    pub fn is_accepted(&self) -> bool {
        matches!(self, ApplyOutcome::Accepted)
    }
}

/// Side effects for leader services, emitted only by accepted commands and
/// acted upon after the batch commits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    EnqueueReady {
        run: RunId,
        queue: String,
        priority: i32,
        ready_at_ms: u64,
        concurrency_key: String,
    },
    ScheduleTimer {
        due_at_ms: u64,
        run: RunId,
        op: u64,
    },
    CancelTimer {
        due_at_ms: u64,
        run: RunId,
        op: u64,
    },
    OutboxAppended {
        target: PartitionId,
        seq: u64,
    },
    RunCompleted {
        run: RunId,
        status: RunStatus,
    },
    CancelRequested {
        run: RunId,
    },
    /// An input (signal, timer, child result, ...) was journaled at `seq` for a run that is
    /// `Running`; the dispatcher forwards it to the hot activation. Added by 04-12; emitted
    /// by 04-13.
    InputAppended {
        run: RunId,
        seq: u64,
    },
}
