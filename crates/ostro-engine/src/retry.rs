//! Step retry policy evaluation (WF-02, plan 04-04).
//!
//! [`decide`] is a pure, deterministic function of `(policy, attempt, failure, now)`: no
//! clocks, no randomness, no jitter in v1. The host (plan 04-07) calls it when a live
//! step attempt fails and journals a
//! [`StepAttemptFailed`](ostro_types::journal::StepAttemptFailed) for every
//! [`RetryDecision::Retry`], so the attempt number is recomputed from the journal on
//! replay and restarts never reset or exceed `max_attempts` (Pitfall 8).
//!
//! # Semantics
//!
//! - `max_attempts` counts **total attempts including the first**: `0` and `1` both mean
//!   "no retry". (This differs from Temporal, where `0` means unlimited.)
//! - Retryable: `Failure::Retryable(e)` whose `e.kind` is not in `non_retryable_kinds`,
//!   and `Failure::Timeout`. Never retried: `Failure::NonRetryable`, `Failure::Cancelled`,
//!   and listed kinds.
//! - Delay before attempt `k + 1` (after attempt `k` failed, `k >= 1`):
//!   `min(initial_ms * coeff^(k-1), max_ms)` milliseconds, where a NaN, infinite or `< 1`
//!   coefficient is treated as `1`, `max_ms < initial_ms` is raised to `initial_ms`, and
//!   overflow saturates to `max_ms`. The power is computed by repeated IEEE multiplication
//!   (exactly reproducible on every platform), stopping as soon as the cap is reached.
//! - [`AttemptBackoff::InActivation`] if the delay is `<= hot_wait`, otherwise
//!   [`AttemptBackoff::Requeue`] (the run is evicted and re-queued at `retry_at_nanos`).

use std::time::Duration;

use ostro_types::journal::{AttemptBackoff, Failure};

/// Plain Rust mirror of WIT `types.retry-policy` (04-07 converts from the WIT record).
#[derive(Debug, Clone, PartialEq)]
pub struct RetryPolicy {
    /// Total attempts including the first; `0` and `1` mean no retry.
    pub max_attempts: u32,
    pub initial_interval_ms: u64,
    pub max_interval_ms: u64,
    pub backoff_coefficient: f64,
    /// `ErrorInfo.kind` values that are never retried.
    pub non_retryable_kinds: Vec<String>,
}

/// What to do with a failed step attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    /// Record the failure as the step's final `StepCompleted` (no retry).
    Record,
    /// Journal `StepAttemptFailed` and run the next attempt at `retry_at_nanos`.
    Retry {
        retry_at_nanos: u64,
        delay: Duration,
        backoff: AttemptBackoff,
    },
}

/// Whether `failure` may be retried under `policy` (ignoring the attempt budget).
pub fn is_retryable(policy: &RetryPolicy, failure: &Failure) -> bool {
    match failure {
        Failure::Retryable(e) => !policy.non_retryable_kinds.contains(&e.kind),
        Failure::Timeout => true,
        Failure::NonRetryable(_) | Failure::Cancelled => false,
    }
}

/// Delay in milliseconds before attempt `attempt + 1`, after attempt `attempt` failed.
pub fn delay_ms(policy: &RetryPolicy, attempt: u32) -> u64 {
    let initial = policy.initial_interval_ms;
    let cap = policy.max_interval_ms.max(initial);
    let coeff = policy.backoff_coefficient;
    let coeff = if coeff.is_finite() && coeff >= 1.0 {
        coeff
    } else {
        1.0
    };
    // Exact in u64 while no multiplication happens; f64 only once growth starts.
    // initial == 0 never grows: return early so huge attempt numbers cannot spin.
    if coeff == 1.0 || attempt <= 1 || initial == 0 {
        return initial.min(cap);
    }
    let cap_f = cap as f64;
    let mut d = initial as f64;
    for _ in 1..attempt {
        if d >= cap_f {
            return cap;
        }
        d *= coeff;
    }
    if d.is_nan() || d >= cap_f {
        cap
    } else {
        // `as` truncates toward zero and saturates; d is in [0, cap).
        d as u64
    }
}

/// Decide whether attempt `attempt` (1-based) of a step that failed with `failure` is
/// retried. `now_nanos` is the run's logical now; `hot_wait` bounds in-activation waits.
pub fn decide(
    policy: Option<&RetryPolicy>,
    attempt: u32,
    failure: &Failure,
    now_nanos: u64,
    hot_wait: Duration,
) -> RetryDecision {
    let Some(policy) = policy else {
        return RetryDecision::Record;
    };
    if attempt >= policy.max_attempts || !is_retryable(policy, failure) {
        return RetryDecision::Record;
    }
    let ms = delay_ms(policy, attempt.max(1));
    let delay = Duration::from_millis(ms);
    let retry_at_nanos = now_nanos.saturating_add(ms.saturating_mul(1_000_000));
    let backoff = if delay <= hot_wait {
        AttemptBackoff::InActivation
    } else {
        AttemptBackoff::Requeue
    };
    RetryDecision::Retry {
        retry_at_nanos,
        delay,
        backoff,
    }
}

#[cfg(test)]
mod tests {
    use ostro_types::journal::ErrorInfo;

    use super::*;

    const HOT: Duration = Duration::from_millis(500);
    const NOW: u64 = 1_000_000_000_000;

    fn policy(max: u32) -> RetryPolicy {
        RetryPolicy {
            max_attempts: max,
            initial_interval_ms: 200,
            max_interval_ms: 1_000,
            backoff_coefficient: 2.0,
            non_retryable_kinds: vec!["validation".into()],
        }
    }

    fn info(kind: &str) -> ErrorInfo {
        ErrorInfo {
            kind: kind.into(),
            message: "m".into(),
            details: None,
        }
    }

    fn retryable() -> Failure {
        Failure::Retryable(info("io"))
    }

    fn delay_of(d: RetryDecision) -> Option<u64> {
        match d {
            RetryDecision::Retry { delay, .. } => Some(delay.as_millis() as u64),
            RetryDecision::Record => None,
        }
    }

    #[test]
    fn no_policy_records() {
        assert_eq!(
            decide(None, 1, &retryable(), NOW, HOT),
            RetryDecision::Record
        );
    }

    #[test]
    fn max_attempts_zero_and_one_never_retry() {
        for max in [0, 1] {
            assert_eq!(
                decide(Some(&policy(max)), 1, &retryable(), NOW, HOT),
                RetryDecision::Record,
                "max {max}"
            );
        }
    }

    #[test]
    fn retries_until_max_attempts_total() {
        let p = policy(3);
        assert!(delay_of(decide(Some(&p), 1, &retryable(), NOW, HOT)).is_some());
        assert!(delay_of(decide(Some(&p), 2, &retryable(), NOW, HOT)).is_some());
        assert_eq!(
            decide(Some(&p), 3, &retryable(), NOW, HOT),
            RetryDecision::Record
        );
        assert_eq!(
            decide(Some(&p), 4, &retryable(), NOW, HOT),
            RetryDecision::Record
        );
    }

    #[test]
    fn failure_classification() {
        let p = policy(5);
        assert!(delay_of(decide(Some(&p), 1, &Failure::Timeout, NOW, HOT)).is_some());
        for f in [
            Failure::NonRetryable(info("io")),
            Failure::Cancelled,
            Failure::Retryable(info("validation")),
        ] {
            assert_eq!(
                decide(Some(&p), 1, &f, NOW, HOT),
                RetryDecision::Record,
                "{f:?}"
            );
        }
    }

    #[test]
    fn exponential_delays_capped() {
        let p = policy(10);
        let got: Vec<u64> = (1..=5)
            .map(|k| delay_of(decide(Some(&p), k, &retryable(), NOW, HOT)).unwrap())
            .collect();
        assert_eq!(got, [200, 400, 800, 1_000, 1_000]);
    }

    #[test]
    fn retry_at_and_backoff_mode() {
        let p = policy(10);
        match decide(Some(&p), 2, &retryable(), NOW, HOT) {
            RetryDecision::Retry {
                retry_at_nanos,
                delay,
                backoff,
            } => {
                assert_eq!(delay, Duration::from_millis(400));
                assert_eq!(retry_at_nanos, NOW + 400_000_000);
                assert_eq!(backoff, AttemptBackoff::InActivation);
            }
            RetryDecision::Record => panic!("expected retry"),
        }
        match decide(Some(&p), 3, &retryable(), NOW, HOT) {
            RetryDecision::Retry { backoff, .. } => assert_eq!(backoff, AttemptBackoff::Requeue),
            RetryDecision::Record => panic!("expected retry"),
        }
        // Boundary: delay == hot_wait stays in the activation.
        match decide(Some(&p), 1, &retryable(), NOW, Duration::from_millis(200)) {
            RetryDecision::Retry { backoff, .. } => {
                assert_eq!(backoff, AttemptBackoff::InActivation)
            }
            RetryDecision::Record => panic!("expected retry"),
        }
    }

    #[test]
    fn bad_coefficients_clamp_to_one() {
        for c in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.5, 0.0, -3.0] {
            let p = RetryPolicy {
                backoff_coefficient: c,
                ..policy(10)
            };
            for k in 1..=6 {
                assert_eq!(delay_ms(&p, k), 200, "coeff {c} attempt {k}");
            }
        }
    }

    #[test]
    fn max_below_initial_raises_to_initial() {
        let p = RetryPolicy {
            max_interval_ms: 50,
            ..policy(10)
        };
        for k in 1..=4 {
            assert_eq!(delay_ms(&p, k), 200);
        }
    }

    #[test]
    fn overflow_saturates() {
        let p = RetryPolicy {
            max_attempts: u32::MAX,
            initial_interval_ms: u64::MAX / 2,
            max_interval_ms: u64::MAX,
            backoff_coefficient: 1e300,
            non_retryable_kinds: vec![],
        };
        assert_eq!(delay_ms(&p, 1), u64::MAX / 2);
        assert_eq!(delay_ms(&p, 2), u64::MAX);
        assert_eq!(delay_ms(&p, u32::MAX - 1), u64::MAX);
        match decide(Some(&p), 3, &retryable(), u64::MAX - 5, HOT) {
            RetryDecision::Retry { retry_at_nanos, .. } => assert_eq!(retry_at_nanos, u64::MAX),
            RetryDecision::Record => panic!("expected retry"),
        }
        // Huge attempt numbers terminate quickly (loop stops at the cap).
        let q = RetryPolicy {
            max_attempts: u32::MAX,
            ..policy(10)
        };
        assert_eq!(delay_ms(&q, u32::MAX - 1), 1_000);
        let z = RetryPolicy {
            initial_interval_ms: 0,
            ..q
        };
        assert_eq!(delay_ms(&z, u32::MAX - 1), 0);
    }

    #[test]
    fn decide_is_pure() {
        let p = policy(4);
        let a = decide(Some(&p), 2, &Failure::Timeout, NOW, HOT);
        let b = decide(Some(&p), 2, &Failure::Timeout, NOW, HOT);
        assert_eq!(a, b);
    }
}
