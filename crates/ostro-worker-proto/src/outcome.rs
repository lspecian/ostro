//! [`WireOutcome`]: the postcard mirror of [`ActivationOutcome`] (D-W19).
//!
//! The worker encodes the engine outcome with [`WireOutcome::encode`] into `Done.outcome`;
//! the leader decodes it with [`WireOutcome::decode`] and maps it back with
//! [`WireOutcome::into_engine`]. Every conversion is an exhaustive `match` without a
//! wildcard arm, so adding an engine variant fails to compile here until the mirror follows.
//! Payloads, failures and continuations reuse the `ostro_types::journal` serde types
//! (already the journal's frozen encoding).

use ostro_engine::error::{AttemptFailure, EvictReason, NonDeterminism};
use ostro_engine::run_limits::{LIMIT_CHILDREN, LIMIT_HISTORY, LIMIT_PAYLOAD};
use ostro_engine::sink::ActivationOutcome;
use ostro_types::journal::{Continuation, Failure, Payload};
use serde::{Deserialize, Serialize};

/// Mirror of [`EvictReason`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireEvictReason {
    WaitUnsatisfied,
    HotWaitElapsed,
    BudgetPressure,
    External,
    AppendConflict,
    StepBackoff { retry_at_nanos: u64 },
    StepInterrupted,
}

/// Mirror of [`AttemptFailure`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireAttemptFailure {
    NonDeterminism {
        seq: u64,
        expected: String,
        actual: String,
    },
    FuelExhausted,
    Timeout,
    MemoryExceeded,
    StackOverflow,
    GuestExit(u32),
    ProtocolViolation(String),
    Trap(String),
    /// Run-limit breach (11-06, D-P11-13). Appended last: earlier variant indices, and so
    /// every existing encoding, are unchanged.
    LimitExceeded {
        limit: String,
        max: u64,
        actual: u64,
    },
}

/// Mirror of [`ActivationOutcome`]; the `Done.outcome` bytes are `postcard(WireOutcome)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WireOutcome {
    Completed { output: Payload },
    Failed { failure: Failure },
    ContinuedAsNew { continuation: Continuation },
    Evicted { reason: WireEvictReason },
    AttemptFailed(WireAttemptFailure),
    Fenced,
}

/// A `Done.outcome` that does not decode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireOutcomeError(pub String);

impl std::fmt::Display for WireOutcomeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid worker outcome: {}", self.0)
    }
}

impl std::error::Error for WireOutcomeError {}

impl From<&EvictReason> for WireEvictReason {
    fn from(r: &EvictReason) -> Self {
        match *r {
            EvictReason::WaitUnsatisfied => Self::WaitUnsatisfied,
            EvictReason::HotWaitElapsed => Self::HotWaitElapsed,
            EvictReason::BudgetPressure => Self::BudgetPressure,
            EvictReason::External => Self::External,
            EvictReason::AppendConflict => Self::AppendConflict,
            EvictReason::StepBackoff { retry_at_nanos } => Self::StepBackoff { retry_at_nanos },
            EvictReason::StepInterrupted => Self::StepInterrupted,
        }
    }
}

impl WireEvictReason {
    /// Back to the engine type.
    pub fn into_engine(self) -> EvictReason {
        match self {
            Self::WaitUnsatisfied => EvictReason::WaitUnsatisfied,
            Self::HotWaitElapsed => EvictReason::HotWaitElapsed,
            Self::BudgetPressure => EvictReason::BudgetPressure,
            Self::External => EvictReason::External,
            Self::AppendConflict => EvictReason::AppendConflict,
            Self::StepBackoff { retry_at_nanos } => EvictReason::StepBackoff { retry_at_nanos },
            Self::StepInterrupted => EvictReason::StepInterrupted,
        }
    }
}

impl From<&AttemptFailure> for WireAttemptFailure {
    fn from(f: &AttemptFailure) -> Self {
        match f {
            AttemptFailure::NonDeterminism(nd) => Self::NonDeterminism {
                seq: nd.seq,
                expected: nd.expected.clone(),
                actual: nd.actual.clone(),
            },
            AttemptFailure::FuelExhausted => Self::FuelExhausted,
            AttemptFailure::Timeout => Self::Timeout,
            AttemptFailure::MemoryExceeded => Self::MemoryExceeded,
            AttemptFailure::StackOverflow => Self::StackOverflow,
            AttemptFailure::GuestExit(code) => Self::GuestExit(*code),
            AttemptFailure::ProtocolViolation(m) => Self::ProtocolViolation(m.clone()),
            AttemptFailure::Trap(m) => Self::Trap(m.clone()),
            AttemptFailure::LimitExceeded { limit, max, actual } => Self::LimitExceeded {
                limit: (*limit).to_owned(),
                max: *max,
                actual: *actual,
            },
        }
    }
}

impl WireAttemptFailure {
    /// Back to the engine type.
    pub fn into_engine(self) -> AttemptFailure {
        match self {
            Self::NonDeterminism {
                seq,
                expected,
                actual,
            } => AttemptFailure::NonDeterminism(NonDeterminism {
                seq,
                expected,
                actual,
            }),
            Self::FuelExhausted => AttemptFailure::FuelExhausted,
            Self::Timeout => AttemptFailure::Timeout,
            Self::MemoryExceeded => AttemptFailure::MemoryExceeded,
            Self::StackOverflow => AttemptFailure::StackOverflow,
            Self::GuestExit(code) => AttemptFailure::GuestExit(code),
            Self::ProtocolViolation(m) => AttemptFailure::ProtocolViolation(m),
            Self::Trap(m) => AttemptFailure::Trap(m),
            // The engine names limits with `&'static str` constants; a name this leader does
            // not know (a newer worker) cannot be carried and is reported as a violation.
            Self::LimitExceeded { limit, max, actual } => {
                match [LIMIT_HISTORY, LIMIT_CHILDREN, LIMIT_PAYLOAD]
                    .into_iter()
                    .find(|l| *l == limit)
                {
                    Some(limit) => AttemptFailure::LimitExceeded { limit, max, actual },
                    None => AttemptFailure::ProtocolViolation(format!(
                        "worker reported unknown limit {limit:?} ({actual} > {max})"
                    )),
                }
            }
        }
    }
}

impl From<&ActivationOutcome> for WireOutcome {
    fn from(o: &ActivationOutcome) -> Self {
        match o {
            ActivationOutcome::Completed { output } => Self::Completed {
                output: output.clone(),
            },
            ActivationOutcome::Failed { failure } => Self::Failed {
                failure: failure.clone(),
            },
            ActivationOutcome::ContinuedAsNew { continuation } => Self::ContinuedAsNew {
                continuation: continuation.clone(),
            },
            ActivationOutcome::Evicted { reason } => Self::Evicted {
                reason: reason.into(),
            },
            ActivationOutcome::AttemptFailed(f) => Self::AttemptFailed(f.into()),
            ActivationOutcome::Fenced => Self::Fenced,
        }
    }
}

impl WireOutcome {
    /// Back to the engine type.
    pub fn into_engine(self) -> ActivationOutcome {
        match self {
            Self::Completed { output } => ActivationOutcome::Completed { output },
            Self::Failed { failure } => ActivationOutcome::Failed { failure },
            Self::ContinuedAsNew { continuation } => {
                ActivationOutcome::ContinuedAsNew { continuation }
            }
            Self::Evicted { reason } => ActivationOutcome::Evicted {
                reason: reason.into_engine(),
            },
            Self::AttemptFailed(f) => ActivationOutcome::AttemptFailed(f.into_engine()),
            Self::Fenced => ActivationOutcome::Fenced,
        }
    }

    /// `postcard(self)`: the `Done.outcome` bytes.
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("WireOutcome serialises")
    }

    /// Decode `Done.outcome` (untrusted worker bytes: errors, never panics).
    pub fn decode(bytes: &[u8]) -> Result<Self, WireOutcomeError> {
        postcard::from_bytes(bytes).map_err(|e| WireOutcomeError(e.to_string()))
    }
}
