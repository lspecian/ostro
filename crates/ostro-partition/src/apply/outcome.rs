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
//! | `Rejected(RunCompleted)`             | 27   |
//! | `Rejected(ReservedP6_12..14)`        | 28–30 (never emitted) |
//! | `Rejected(RunPaused)`                | 31   |
//!
//! Indices come only from `.planning/RESERVATIONS.md` §7 (`RunCompleted` = 11, Phase 5;
//! `RunPaused` = 15, Phase 7). `RejectReason` is declaration-indexed like [`InvalidCode`], so
//! 07-03 declared the canonical `#[doc(hidden)]` placeholders `ReservedP6_12..14` for Phase 6's
//! unused slots (P-15 rule); they are never emitted, so they never reach the state hash. A
//! phase claiming one of them renames it in place.
//!
//! The [`InvalidCode`] detail is not part of the hash code (the command
//! bytes are folded anyway, and they determine the detail). `InvalidCode`
//! variants are append-only as well (02-09 added `BadIntent`,
//! `FieldTooLong`; 02-10 added `BadSource`, `TruncateBeyondEnd`; 04-12 added `BadLength`,
//! `NotRunning`; 08-01 added Phase 8's `BadScheduleFire` (22) and `ScheduleOwnerMismatch` (23);
//! 06-02 renamed the placeholder at 15 to Phase 6's `BadEffectCompletion`; 11-02 added Phase 11's
//! `BadRerunPrefix` (29) and `BadDlqUpdate` (30) above the placeholders `ReservedP8_24`,
//! `ReservedP9_25..28`).
//!
//! `InvalidCode` is declaration-indexed, so a phase landing above an absent lower slot declares
//! `#[doc(hidden)]`, never-emitted placeholders named `Reserved<Phase>_<index>`
//! (RESERVATIONS §8 placeholder rule). A phase claiming its own slot renames the placeholder in
//! place.

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
    StaleEpoch {
        current: Epoch,
    },
    Fenced {
        current: Option<AttemptToken>,
    },
    SeqMismatch {
        expected: u64,
    },
    SeqGap {
        expected: u64,
        found: u64,
    },
    RunNotFound,
    RunTerminal,
    RunExists,
    IdempotencyConflict {
        existing: RunId,
    },
    PayloadTooLarge,
    WrongPartition,
    Invalid(InvalidCode),
    /// The run completed successfully and cannot be retried (`RetryRun` on a `Completed` run,
    /// D1; the API maps it to 409 `run_completed`). Phase 5 (05-02), index 11.
    RunCompleted,
    // RESERVATIONS.md §7: placeholders for Phase 6's unused slots 12..=14 (never emitted;
    // declared by 07-03, P-15 convention).
    #[doc(hidden)]
    ReservedP6_12,
    #[doc(hidden)]
    ReservedP6_13,
    #[doc(hidden)]
    ReservedP6_14,
    /// The run is paused (`RunRecord.pause` is set): executor commands (`EventsAppended`,
    /// `AttemptGranted`) are refused until `ResumeRun`. Phase 7 (07-03; emitted from 07-04),
    /// index 15, outcome code 31.
    // RESERVATIONS.md: P7 = RejectReason 15..17
    RunPaused,
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
    // RESERVATIONS.md §8: placeholders for other phases' slots 13..=21 (never emitted);
    // 06-02 renamed ReservedP6_15 in place (P-15).
    #[doc(hidden)]
    ReservedP5_13,
    #[doc(hidden)]
    ReservedP5_14,
    /// A malformed `CompleteEffect`: missing or wrong-kind outcome, or an op that does not
    /// match a registered effect. Phase 6 (06-02; emitted from 06-03), index 15.
    BadEffectCompletion,
    #[doc(hidden)]
    ReservedP6_16,
    #[doc(hidden)]
    ReservedP6_17,
    #[doc(hidden)]
    ReservedP7_18,
    #[doc(hidden)]
    ReservedP7_19,
    #[doc(hidden)]
    ReservedP7_20,
    #[doc(hidden)]
    ReservedP7_21,
    /// A malformed `ScheduleFire`: `Fire`/`Manual` without `start`, `Advance` with one,
    /// `request_id` on a non-`Manual` fire or too long, an unknown kind or overlap value.
    /// Phase 8 (08-01), index 22.
    BadScheduleFire,
    /// A `ScheduleFire` applied on a partition that is not `schedule_owner(name, P)`.
    /// Phase 8 (08-01), index 23.
    ScheduleOwnerMismatch,
    // RESERVATIONS.md §8: placeholders for the unclaimed slots 24..=28 (never emitted; 11-02,
    // P-15 canonical names).
    #[doc(hidden)]
    ReservedP8_24,
    #[doc(hidden)]
    ReservedP9_25,
    #[doc(hidden)]
    ReservedP9_26,
    #[doc(hidden)]
    ReservedP9_27,
    #[doc(hidden)]
    ReservedP9_28,
    /// A malformed rerun `StartRun` (`rerun_prefix` / `rerun_from_seq` inconsistent).
    /// Phase 11 (11-02; emitted from 11-07), index 29.
    // RESERVATIONS.md: P11 = InvalidCode 29..31
    BadRerunPrefix,
    /// A malformed `DlqUpdate` (missing op, bad ids, reason too long, unknown mode).
    /// Phase 11 (11-02; emitted from 11-08), index 30.
    BadDlqUpdate,
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
            RejectReason::RunCompleted => 11,
            RejectReason::ReservedP6_12 => 12,
            RejectReason::ReservedP6_13 => 13,
            RejectReason::ReservedP6_14 => 14,
            RejectReason::RunPaused => 15,
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
        /// `ReadyValue.workflow` (Phase 7 F3).
        workflow: String,
        /// `ReadyValue.tenant` (Phase 7 F3).
        tenant: Option<String>,
        /// `ReadyValue.first` (Phase 7 F7).
        first_activation: bool,
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
    /// The run was paused (`PauseRun` accepted on a non-paused run). In memory only (not a
    /// RESERVATIONS space). Phase 7: declared by 07-03, emitted by 07-04; the dispatcher's
    /// wildcard arm ignores it until 07-05.
    RunPaused {
        run: RunId,
    },
    /// The run was resumed (`ResumeRun` accepted on a paused run). In memory only. Phase 7:
    /// declared by 07-03, emitted by 07-04, consumed by 07-05.
    RunResumed {
        run: RunId,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every outcome code is frozen (folded into the rolling hash); see the module table.
    #[test]
    fn outcome_codes_are_pinned() {
        let run = RunId::from_bytes(&[0; 18]).expect("run id");
        let cases = [
            (ApplyOutcome::Accepted, 0),
            (ApplyOutcome::Duplicate { existing: None }, 1),
            (
                ApplyOutcome::Duplicate {
                    existing: Some(run),
                },
                1,
            ),
            (
                ApplyOutcome::Rejected(RejectReason::StaleEpoch { current: Epoch(1) }),
                16,
            ),
            (
                ApplyOutcome::Rejected(RejectReason::Fenced { current: None }),
                17,
            ),
            (
                ApplyOutcome::Rejected(RejectReason::SeqMismatch { expected: 1 }),
                18,
            ),
            (
                ApplyOutcome::Rejected(RejectReason::SeqGap {
                    expected: 1,
                    found: 2,
                }),
                19,
            ),
            (ApplyOutcome::Rejected(RejectReason::RunNotFound), 20),
            (ApplyOutcome::Rejected(RejectReason::RunTerminal), 21),
            (ApplyOutcome::Rejected(RejectReason::RunExists), 22),
            (
                ApplyOutcome::Rejected(RejectReason::IdempotencyConflict { existing: run }),
                23,
            ),
            (ApplyOutcome::Rejected(RejectReason::PayloadTooLarge), 24),
            (ApplyOutcome::Rejected(RejectReason::WrongPartition), 25),
            (
                ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::Decode)),
                26,
            ),
            (
                ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::NotRunning)),
                26,
            ),
            (ApplyOutcome::Rejected(RejectReason::RunCompleted), 27),
            (ApplyOutcome::Rejected(RejectReason::ReservedP6_12), 28),
            (ApplyOutcome::Rejected(RejectReason::ReservedP6_13), 29),
            (ApplyOutcome::Rejected(RejectReason::ReservedP6_14), 30),
            (ApplyOutcome::Rejected(RejectReason::RunPaused), 31),
        ];
        for (o, code) in cases {
            assert_eq!(o.code(), code, "{o:?}");
        }
        // InvalidCode declaration order (RESERVATIONS §8); NotImplemented exists at 6.
        let order = [
            InvalidCode::Decode,
            InvalidCode::Empty,
            InvalidCode::BadRunId,
            InvalidCode::MissingToken,
            InvalidCode::EventsNotContiguous,
            InvalidCode::MissingField,
            InvalidCode::NotImplemented,
            InvalidCode::BadIntent,
            InvalidCode::FieldTooLong,
            InvalidCode::BadSource,
            InvalidCode::TruncateBeyondEnd,
            InvalidCode::BadLength,
            InvalidCode::NotRunning,
            InvalidCode::ReservedP5_13,
            InvalidCode::ReservedP5_14,
            InvalidCode::BadEffectCompletion,
            InvalidCode::ReservedP6_16,
            InvalidCode::ReservedP6_17,
            InvalidCode::ReservedP7_18,
            InvalidCode::ReservedP7_19,
            InvalidCode::ReservedP7_20,
            InvalidCode::ReservedP7_21,
            InvalidCode::BadScheduleFire,
            InvalidCode::ScheduleOwnerMismatch,
            InvalidCode::ReservedP8_24,
            InvalidCode::ReservedP9_25,
            InvalidCode::ReservedP9_26,
            InvalidCode::ReservedP9_27,
            InvalidCode::ReservedP9_28,
            InvalidCode::BadRerunPrefix,
            InvalidCode::BadDlqUpdate,
        ];
        for (i, c) in order.into_iter().enumerate() {
            assert_eq!(c as usize, i, "{c:?}");
        }
        // Phase 8 (RESERVATIONS §8): 22 BadScheduleFire, 23 ScheduleOwnerMismatch.
        assert_eq!(InvalidCode::BadScheduleFire as usize, 22);
        // Phase 6 (RESERVATIONS §8, P-15): 15 BadEffectCompletion.
        assert_eq!(InvalidCode::BadEffectCompletion as usize, 15);
        assert_eq!(
            ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::BadEffectCompletion)).code(),
            26
        );
        assert_eq!(InvalidCode::ScheduleOwnerMismatch as usize, 23);
        // Phase 11 (RESERVATIONS §8): 29 BadRerunPrefix, 30 BadDlqUpdate.
        assert_eq!(InvalidCode::BadRerunPrefix as usize, 29);
        assert_eq!(InvalidCode::BadDlqUpdate as usize, 30);
        assert_eq!(
            ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::BadDlqUpdate)).code(),
            26
        );
        assert_eq!(
            ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::BadScheduleFire)).code(),
            26
        );
    }
}
