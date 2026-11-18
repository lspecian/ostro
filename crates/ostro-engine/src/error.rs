//! Engine error taxonomy: eviction, nondeterminism and attempt failures.

/// Why an activation was evicted (not a failure: the run resumes later from its journal).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictReason {
    /// The guest waited on ops that are not yet complete.
    WaitUnsatisfied,
    /// The activation was parked hot and its hot-wait window elapsed.
    HotWaitElapsed,
    /// Evicted to free instances/memory for other runs.
    BudgetPressure,
    /// Evicted by the caller (cancellation token).
    External,
    /// `append` reported a sequence conflict.
    AppendConflict,
    /// A live step failed retryably and its next attempt is due later than
    /// `engine.hot_wait` (or a re-activation found the last attempt's `retry_at` still in
    /// the future): the run should be re-dispatched at `retry_at_nanos` (WF-02).
    StepBackoff {
        /// Unix nanos at which the next attempt of the step is due.
        retry_at_nanos: u64,
    },
}

impl EvictReason {
    /// Stable metrics label.
    pub fn code(&self) -> &'static str {
        match self {
            Self::WaitUnsatisfied => "wait_unsatisfied",
            Self::HotWaitElapsed => "hot_wait_elapsed",
            Self::BudgetPressure => "budget_pressure",
            Self::External => "external",
            Self::AppendConflict => "append_conflict",
            Self::StepBackoff { .. } => "step_backoff",
        }
    }
}

/// Activation evicted; carried as a trap payload out of host calls.
#[derive(Debug, thiserror::Error)]
#[error("activation evicted: {reason:?}")]
pub struct Evict {
    pub reason: EvictReason,
}

/// The guest issued a command that does not match the journal at `seq`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("nondeterminism at seq {seq}: expected {expected}, actual {actual}")]
pub struct NonDeterminism {
    pub seq: u64,
    pub expected: String,
    pub actual: String,
}

/// The guest called `wasi:cli/exit`.
#[derive(Debug, thiserror::Error)]
#[error("guest exited with code {0}")]
pub struct GuestExit(pub u32);

/// A memory grow exceeded the store limit.
#[derive(Debug, thiserror::Error)]
#[error("memory limit exceeded: requested {requested} bytes, limit {limit}")]
pub struct MemoryExceeded {
    pub requested: usize,
    pub limit: usize,
}

/// The guest misused the WIT protocol (e.g. completing a step twice).
#[derive(Debug, thiserror::Error)]
#[error("protocol violation: {0}")]
pub struct ProtocolViolation(pub String);

/// The sink fenced this attempt (a newer attempt or leader owns the run).
#[derive(Debug, thiserror::Error)]
#[error("activation fenced")]
pub struct Fenced;

/// A failed run attempt (the attempt ends; the run may be retried or dead-lettered).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptFailure {
    NonDeterminism(NonDeterminism),
    FuelExhausted,
    Timeout,
    MemoryExceeded,
    StackOverflow,
    GuestExit(u32),
    ProtocolViolation(String),
    Trap(String),
}

impl AttemptFailure {
    /// Stable metrics/DLQ reason code.
    pub fn reason_code(&self) -> &'static str {
        match self {
            Self::NonDeterminism(_) => "nondeterminism",
            Self::FuelExhausted => "fuel_exhausted",
            Self::Timeout => "timeout",
            Self::MemoryExceeded => "memory_exceeded",
            Self::StackOverflow => "stack_overflow",
            Self::GuestExit(_) => "guest_exit",
            Self::ProtocolViolation(_) => "protocol_violation",
            Self::Trap(_) => "trap",
        }
    }

    /// Whether a new attempt may succeed. Nondeterminism and protocol violations are
    /// properties of the code and never fixed by retrying.
    pub fn retryable(&self) -> bool {
        !matches!(self, Self::NonDeterminism(_) | Self::ProtocolViolation(_))
    }

    /// Human-readable dead-letter reason.
    pub fn dlq_reason(&self) -> String {
        match self {
            Self::NonDeterminism(nd) => format!(
                "nondeterminism: seq={} expected={} actual={}",
                nd.seq, nd.expected, nd.actual
            ),
            Self::GuestExit(code) => format!("guest_exit: code={code}"),
            Self::ProtocolViolation(msg) => format!("protocol_violation: {msg}"),
            Self::Trap(msg) => format!("trap: {msg}"),
            other => other.reason_code().to_string(),
        }
    }
}

/// Engine-level (non-attempt) errors.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("pooling allocator reservation failed: {message}")]
    PoolReservation { message: String },
    #[error("component validation failed: {}", .0.join("; "))]
    Validation(Vec<String>),
    #[error("AOT cache error: {0}")]
    Cache(String),
    #[error(transparent)]
    Wasmtime(#[from] wasmtime::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reason_codes_and_retryability() {
        let nd = AttemptFailure::NonDeterminism(NonDeterminism {
            seq: 3,
            expected: "step \"a\"".into(),
            actual: "sleep".into(),
        });
        assert_eq!(nd.reason_code(), "nondeterminism");
        assert!(!nd.retryable());
        assert_eq!(
            nd.dlq_reason(),
            "nondeterminism: seq=3 expected=step \"a\" actual=sleep"
        );
        assert!(!AttemptFailure::ProtocolViolation("x".into()).retryable());
        assert_eq!(
            EvictReason::StepBackoff { retry_at_nanos: 7 }.code(),
            "step_backoff"
        );
        assert_eq!(EvictReason::WaitUnsatisfied.code(), "wait_unsatisfied");
        for (f, code) in [
            (AttemptFailure::FuelExhausted, "fuel_exhausted"),
            (AttemptFailure::Timeout, "timeout"),
            (AttemptFailure::MemoryExceeded, "memory_exceeded"),
            (AttemptFailure::StackOverflow, "stack_overflow"),
            (AttemptFailure::GuestExit(1), "guest_exit"),
            (AttemptFailure::Trap("t".into()), "trap"),
        ] {
            assert_eq!(f.reason_code(), code);
            assert!(f.retryable());
        }
    }
}
