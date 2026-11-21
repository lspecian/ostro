//! In-attempt retry classification and backoff (FX-01, FX-03).
//!
//! | Class | Conditions | Retried |
//! |-------|------------|---------|
//! | [`Class::NeverLeft`] | DNS failure, connection refused, TLS handshake failure (reqwest `is_connect`) | always (within the attempt budget) |
//! | [`Class::AfterSend`] | timeout awaiting the response, reset mid-response, body stream failure | only with a policy, when a key header is sent (or the method is idempotent) and the window allows |
//! | [`Class::RetryStatus`] | 408, 425, 429, 5xx, and 409 whose body or headers say the same key is in progress | only with a policy |
//! | [`Class::Final`] | anything else | never: the response is the outcome |
//!
//! With no guest policy: up to 3 attempts, `NeverLeft` only (200 ms initial, ×2, max 5 s).
//! With a policy: `max_attempts` attempts in total (at least 1), the policy's backoff.
//! `Retry-After` (delta seconds) replaces the computed delay, capped at the policy's
//! `max_interval`. A guest `non_retryable_kinds` entry of `http_transport` disables retries of
//! transport failures (statuses are not failures, so it does not affect them).
//!
//! A final status (even a 5xx after the budget runs out) is a **successful** completion: the
//! guest decides what a status means.
//!
//! **Re-drive in progress (07-21 gap closure).** A re-driven effect (FX-03: replayed without a
//! completion) whose idempotency key header was sent and whose response is a 409 the
//! provider marks as "same key still in progress" ([`is_redrive_in_progress`]) is retried
//! **with or without a guest policy**, outside the attempt budget, with the bounded backoff
//! of [`in_progress_delay`] (200 ms, x2, max 5 s; `Retry-After` honoured up to 5 s). It stops
//! at the provider's final answer or when the idempotency window closes
//! ([`super::window::may_await_in_progress`]), which ends the effect
//! `effect_outcome_unknown`. Such a 409 is not the provider's answer to the request: the
//! original send (from a killed leader) is still running, and its result is what the run
//! must see. A first (live) attempt keeps the rules above: no policy, no 409 retry.
//!
//! Detection is a heuristic: status 409 plus [`is_in_progress_conflict`] (a `Retry-After`
//! header, or an "in progress"/"concurrent" marker in the first 4 KiB of the body: the
//! MockProvider's `{"error":"in_progress"}`, Stripe's "executing concurrently"). A 409 with
//! any other body (e.g. "duplicate order") stays final.

use std::time::Duration;

use ostro_types::live::EffectRetry;
use reqwest::header::{HeaderMap, RETRY_AFTER};

use crate::kinds::HTTP_TRANSPORT;

/// What an attempt observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    /// The request never left this node: always safe to retry.
    NeverLeft,
    /// The request may have reached the server; the outcome is unknown.
    AfterSend,
    /// A status that asks for a retry.
    RetryStatus,
    /// Final.
    Final,
}

/// A transport failure, reduced to what classification needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    /// Connecting failed (DNS, refused, TLS handshake, connect timeout).
    Connect,
    /// The per-attempt deadline passed after the connection may have been used.
    Timeout,
    /// Any other failure (reset mid-response, protocol error, body stream error).
    Other,
}

impl Transport {
    /// Reduces a reqwest error.
    pub fn from_error(err: &reqwest::Error) -> Self {
        if err.is_connect() {
            Self::Connect
        } else if err.is_timeout() {
            Self::Timeout
        } else {
            Self::Other
        }
    }

    /// The class of this failure.
    pub fn class(self) -> Class {
        match self {
            Self::Connect => Class::NeverLeft,
            Self::Timeout | Self::Other => Class::AfterSend,
        }
    }
}

/// Whether a 409 response says a request with the same key is still being processed
/// (Stripe: "conflicts with another request that's executing concurrently").
pub fn is_in_progress_conflict(headers: &HeaderMap, body: &[u8]) -> bool {
    if headers.contains_key(RETRY_AFTER) {
        return true;
    }
    let lower = String::from_utf8_lossy(&body[..body.len().min(4096)]).to_ascii_lowercase();
    [
        "in_progress",
        "in progress",
        "in-progress",
        "concurrent",
        "currently being processed",
    ]
    .iter()
    .any(|n| lower.contains(n))
}

/// The class of a response status.
pub fn classify_status(status: u16, headers: &HeaderMap, body: &[u8]) -> Class {
    match status {
        408 | 425 | 429 | 500..=599 => Class::RetryStatus,
        409 if is_in_progress_conflict(headers, body) => Class::RetryStatus,
        _ => Class::Final,
    }
}

/// Whether a re-drive response says the original request (same key) is still being
/// processed by the provider: 409 + [`is_in_progress_conflict`].
pub fn is_redrive_in_progress(status: u16, headers: &HeaderMap, body: &[u8]) -> bool {
    status == 409 && is_in_progress_conflict(headers, body)
}

/// First re-drive in-progress backoff.
pub const IN_PROGRESS_INITIAL: Duration = Duration::from_millis(200);
/// Re-drive in-progress backoff cap (also caps `Retry-After`).
pub const IN_PROGRESS_MAX: Duration = Duration::from_secs(5);

/// The delay before re-drive in-progress retry number `retry` (1-based): 200 ms doubling up to
/// 5 s; `Retry-After` replaces it, capped at 5 s.
pub fn in_progress_delay(retry: u32, retry_after: Option<Duration>) -> Duration {
    if let Some(ra) = retry_after {
        return ra.min(IN_PROGRESS_MAX);
    }
    let shift = retry.saturating_sub(1).min(16);
    IN_PROGRESS_INITIAL
        .saturating_mul(1u32 << shift)
        .min(IN_PROGRESS_MAX)
}

/// `Retry-After` in delta seconds (HTTP dates are ignored).
pub fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let v = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    v.parse::<u64>().ok().map(Duration::from_secs)
}

/// The retry budget and backoff of one effect.
#[derive(Clone, Debug, PartialEq)]
pub struct Schedule {
    /// Total attempts including the first.
    pub max_attempts: u32,
    /// First backoff interval.
    pub initial: Duration,
    /// Backoff cap.
    pub max_interval: Duration,
    /// Multiplier.
    pub coefficient: f64,
    /// `true` when the guest supplied a policy.
    pub from_policy: bool,
    /// Transport failures are never retried (`http_transport` in `non_retryable_kinds`).
    pub transport_non_retryable: bool,
}

impl Schedule {
    /// Attempts without a guest policy.
    pub const DEFAULT_ATTEMPTS: u32 = 3;

    /// The schedule of `policy` (or the default one).
    pub fn new(policy: Option<&EffectRetry>) -> Self {
        match policy {
            None => Self {
                max_attempts: Self::DEFAULT_ATTEMPTS,
                initial: Duration::from_millis(200),
                max_interval: Duration::from_secs(5),
                coefficient: 2.0,
                from_policy: false,
                transport_non_retryable: false,
            },
            Some(p) => Self {
                max_attempts: p.max_attempts.max(1),
                initial: Duration::from_millis(p.initial_interval_ms),
                max_interval: Duration::from_millis(p.max_interval_ms.max(p.initial_interval_ms)),
                coefficient: if p.backoff_coefficient.is_finite() && p.backoff_coefficient >= 1.0 {
                    p.backoff_coefficient
                } else {
                    1.0
                },
                from_policy: true,
                transport_non_retryable: p.non_retryable_kinds.iter().any(|k| k == HTTP_TRANSPORT),
            },
        }
    }

    /// Whether attempt number `attempt` (1-based, just finished with `class`) may be followed
    /// by another. `after_send_ok`: a key header was sent (or the method is idempotent) and
    /// the window still allows a re-send.
    pub fn should_retry(&self, class: Class, attempt: u32, after_send_ok: bool) -> bool {
        if attempt >= self.max_attempts {
            return false;
        }
        match class {
            Class::Final => false,
            Class::NeverLeft => !self.transport_non_retryable,
            Class::AfterSend => self.from_policy && !self.transport_non_retryable && after_send_ok,
            Class::RetryStatus => self.from_policy,
        }
    }

    /// The delay before retry number `retry` (1-based), honouring `Retry-After`.
    pub fn delay(&self, retry: u32, retry_after: Option<Duration>) -> Duration {
        if let Some(ra) = retry_after {
            return ra.min(self.max_interval);
        }
        let exp = i32::try_from(retry.saturating_sub(1)).unwrap_or(i32::MAX);
        let secs = self.initial.as_secs_f64() * self.coefficient.powi(exp);
        if !secs.is_finite() || secs >= self.max_interval.as_secs_f64() {
            self.max_interval
        } else {
            Duration::from_secs_f64(secs)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    fn h(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut m = HeaderMap::new();
        for (k, v) in pairs {
            m.insert(*k, HeaderValue::from_static(v));
        }
        m
    }

    #[test]
    fn status_classes() {
        let none = HeaderMap::new();
        for s in [408, 425, 429, 500, 502, 503, 504, 599] {
            assert_eq!(classify_status(s, &none, b""), Class::RetryStatus, "{s}");
        }
        for s in [200, 201, 204, 301, 302, 400, 401, 403, 404, 409, 410, 422] {
            assert_eq!(classify_status(s, &none, b"{}"), Class::Final, "{s}");
        }
        assert_eq!(
            classify_status(409, &none, br#"{"error":"in_progress"}"#),
            Class::RetryStatus
        );
        assert_eq!(
            classify_status(409, &none, b"request is currently being processed"),
            Class::RetryStatus
        );
        assert_eq!(
            classify_status(409, &h(&[("retry-after", "1")]), b""),
            Class::RetryStatus
        );
        assert_eq!(
            classify_status(409, &none, br#"{"error":"duplicate order"}"#),
            Class::Final
        );
    }

    #[test]
    fn redrive_in_progress_detection_and_backoff() {
        let none = HeaderMap::new();
        assert!(is_redrive_in_progress(
            409,
            &none,
            br#"{"error":"in_progress"}"#
        ));
        assert!(is_redrive_in_progress(
            409,
            &h(&[("retry-after", "1")]),
            b""
        ));
        assert!(!is_redrive_in_progress(409, &none, b"{}"));
        assert!(!is_redrive_in_progress(
            409,
            &none,
            br#"{"error":"duplicate order"}"#
        ));
        // Only 409: a 503/429 saying "in progress" follows the normal policy rules.
        assert!(!is_redrive_in_progress(503, &none, b"in progress"));
        assert!(!is_redrive_in_progress(200, &none, b"in_progress"));

        assert_eq!(in_progress_delay(1, None), Duration::from_millis(200));
        assert_eq!(in_progress_delay(2, None), Duration::from_millis(400));
        assert_eq!(in_progress_delay(5, None), Duration::from_millis(3200));
        assert_eq!(in_progress_delay(6, None), IN_PROGRESS_MAX);
        assert_eq!(in_progress_delay(u32::MAX, None), IN_PROGRESS_MAX);
        assert_eq!(
            in_progress_delay(1, Some(Duration::from_secs(2))),
            Duration::from_secs(2)
        );
        assert_eq!(
            in_progress_delay(1, Some(Duration::from_secs(600))),
            IN_PROGRESS_MAX
        );
        // The default schedule still does not retry a RetryStatus (first attempts).
        assert!(!Schedule::new(None).should_retry(Class::RetryStatus, 1, true));
    }

    #[test]
    fn transport_classes() {
        assert_eq!(Transport::Connect.class(), Class::NeverLeft);
        assert_eq!(Transport::Timeout.class(), Class::AfterSend);
        assert_eq!(Transport::Other.class(), Class::AfterSend);
    }

    #[test]
    fn default_schedule_retries_never_left_only() {
        let s = Schedule::new(None);
        assert_eq!(s.max_attempts, 3);
        assert!(s.should_retry(Class::NeverLeft, 1, false));
        assert!(s.should_retry(Class::NeverLeft, 2, false));
        assert!(!s.should_retry(Class::NeverLeft, 3, false));
        assert!(!s.should_retry(Class::AfterSend, 1, true));
        assert!(!s.should_retry(Class::RetryStatus, 1, true));
        assert!(!s.should_retry(Class::Final, 1, true));
        assert_eq!(s.delay(1, None), Duration::from_millis(200));
        assert_eq!(s.delay(2, None), Duration::from_millis(400));
        assert_eq!(s.delay(10, None), Duration::from_secs(5));
    }

    #[test]
    fn policy_schedule() {
        let p = EffectRetry {
            max_attempts: 4,
            initial_interval_ms: 100,
            max_interval_ms: 1_000,
            backoff_coefficient: 3.0,
            non_retryable_kinds: vec![],
        };
        let s = Schedule::new(Some(&p));
        assert!(s.should_retry(Class::RetryStatus, 1, false));
        assert!(s.should_retry(Class::AfterSend, 3, true));
        assert!(!s.should_retry(Class::AfterSend, 1, false));
        assert!(!s.should_retry(Class::RetryStatus, 4, true));
        assert_eq!(s.delay(1, None), Duration::from_millis(100));
        assert_eq!(s.delay(2, None), Duration::from_millis(300));
        assert_eq!(s.delay(3, None), Duration::from_millis(900));
        assert_eq!(s.delay(4, None), Duration::from_secs(1));
        // Retry-After honoured up to max_interval.
        assert_eq!(
            s.delay(1, Some(Duration::from_millis(500))),
            Duration::from_millis(500)
        );
        assert_eq!(
            s.delay(1, Some(Duration::from_secs(60))),
            Duration::from_secs(1)
        );

        let one = Schedule::new(Some(&EffectRetry {
            max_attempts: 0,
            ..p.clone()
        }));
        assert_eq!(one.max_attempts, 1);
        assert!(!one.should_retry(Class::NeverLeft, 1, true));

        let no_transport = Schedule::new(Some(&EffectRetry {
            non_retryable_kinds: vec![HTTP_TRANSPORT.into()],
            ..p
        }));
        assert!(!no_transport.should_retry(Class::NeverLeft, 1, true));
        assert!(!no_transport.should_retry(Class::AfterSend, 1, true));
        assert!(no_transport.should_retry(Class::RetryStatus, 1, true));
    }

    #[test]
    fn retry_after_parses_delta_seconds_only() {
        assert_eq!(
            retry_after(&h(&[("retry-after", "3")])),
            Some(Duration::from_secs(3))
        );
        assert_eq!(
            retry_after(&h(&[("retry-after", "Wed, 21 Oct 2015 07:28:00 GMT")])),
            None
        );
        assert_eq!(retry_after(&HeaderMap::new()), None);
    }
}
