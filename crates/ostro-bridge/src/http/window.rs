//! The unknown-outcome window (FX-03, D-FX3).
//!
//! Applied **only at re-drive time** (`LiveEffect.redrive == true`): replay found an
//! `EffectScheduled` without a completion, so the request may already have been sent. A live
//! first dispatch always sends.
//!
//! ```text
//! elapsed = wall_now - EffectScheduled.ts          (conservative: ts <= first send)
//! idempotent method (GET HEAD OPTIONS PUT DELETE) and !force_window  -> Resend
//! resend == never                                                    -> Unknown
//! elapsed + safety_margin < idempotency_window                       -> Resend (same key)
//! otherwise                                                          -> Unknown
//! ```
//!
//! `Unknown` completes the effect `NonRetryable{effect_outcome_unknown}` without sending.
//! Method names are case-sensitive (RFC 9110 §9.1): `get` is not `GET`, so it gets the
//! conservative, non-idempotent treatment.

use ostro_types::journal::Failure;

use crate::kinds::EFFECT_OUTCOME_UNKNOWN;
use crate::outcome::non_retryable;

use super::target::{HttpTarget, ResendMode};

/// The message of an [`unknown`] failure.
pub const UNKNOWN_MESSAGE: &str = "in-flight effect older than idempotency window; not re-sent";

/// The re-drive decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Send again (with the same key).
    Resend,
    /// Do not send: the outcome is unknown.
    Unknown,
}

/// RFC 9110 idempotent methods.
pub fn is_idempotent(method: &str) -> bool {
    matches!(method, "GET" | "HEAD" | "OPTIONS" | "PUT" | "DELETE")
}

/// Whether a request `elapsed_nanos` after its scheduling may be (re-)sent to `target`.
pub fn decide(method: &str, elapsed_nanos: u64, target: &HttpTarget) -> Decision {
    if is_idempotent(method) && !target.force_window {
        return Decision::Resend;
    }
    if target.resend == ResendMode::Never {
        return Decision::Unknown;
    }
    if elapsed_nanos.saturating_add(target.margin_nanos()) < target.window_nanos() {
        Decision::Resend
    } else {
        Decision::Unknown
    }
}

/// Whether a re-drive may keep waiting for (retrying into) a request the provider reports as
/// still in progress, `elapsed_nanos` after scheduling: `elapsed + safety_margin < window`.
///
/// This is the time part of [`decide`] applied to **every** method (idempotent ones are
/// exempt from the window in `decide`, but in-progress retries must never run forever), and
/// it ignores `resend` (a re-drive that reached the provider has already passed `decide`).
/// `false` ends the effect with [`unknown`].
pub fn may_await_in_progress(elapsed_nanos: u64, target: &HttpTarget) -> bool {
    elapsed_nanos.saturating_add(target.margin_nanos()) < target.window_nanos()
}

/// `NonRetryable{effect_outcome_unknown}` with details `{"op", "key", "elapsed_ms"}`.
pub fn unknown(op: u64, key: &str, elapsed_nanos: u64) -> Failure {
    non_retryable(
        EFFECT_OUTCOME_UNKNOWN,
        UNKNOWN_MESSAGE,
        Some(serde_json::json!({
            "op": op,
            "key": key,
            "elapsed_ms": elapsed_nanos / 1_000_000,
        })),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::egress::EgressConfig;

    const H: u64 = 3_600_000_000_000;
    const M: u64 = 60_000_000_000;

    fn target(resend: ResendMode, force: bool) -> HttpTarget {
        let mut t = HttpTarget::defaults(&EgressConfig::default());
        t.idempotency_window = Duration::from_secs(24 * 3600);
        t.safety_margin = Duration::from_secs(300);
        t.resend = resend;
        t.force_window = force;
        t
    }

    #[test]
    fn decision_table() {
        use Decision::{Resend, Unknown};
        let w = target(ResendMode::Window, false);
        let never = target(ResendMode::Never, false);
        let forced = target(ResendMode::Window, true);
        let boundary = 24 * H - 5 * M; // elapsed + margin == window
        let rows: &[(&str, u64, &HttpTarget, Decision)] = &[
            ("POST", 0, &w, Resend),
            ("POST", boundary - 1, &w, Resend),
            ("POST", boundary, &w, Unknown),
            ("POST", boundary + 1, &w, Unknown),
            ("PATCH", 25 * H, &w, Unknown),
            ("POST", 0, &never, Unknown),
            ("GET", 100 * H, &w, Resend),
            ("HEAD", 100 * H, &w, Resend),
            ("OPTIONS", 100 * H, &w, Resend),
            ("PUT", 100 * H, &w, Resend),
            ("DELETE", 100 * H, &w, Resend),
            ("GET", 100 * H, &never, Resend),
            ("get", 100 * H, &w, Unknown),
            ("GET", 100 * H, &forced, Unknown),
            ("GET", boundary - 1, &forced, Resend),
            ("POST", u64::MAX, &w, Unknown),
        ];
        for (method, elapsed, t, want) in rows {
            assert_eq!(
                decide(method, *elapsed, t),
                *want,
                "{method} {elapsed} {:?}/{}",
                t.resend,
                t.force_window
            );
        }
    }

    #[test]
    fn in_progress_wait_is_bounded_by_the_window_for_every_method() {
        let w = target(ResendMode::Window, false);
        let boundary = 24 * H - 5 * M;
        assert!(may_await_in_progress(0, &w));
        assert!(may_await_in_progress(boundary - 1, &w));
        assert!(!may_await_in_progress(boundary, &w));
        assert!(!may_await_in_progress(u64::MAX, &w));
        // Unlike decide(): no exemption for idempotent methods, no dependence on resend.
        assert_eq!(decide("GET", 100 * H, &w), Decision::Resend);
        assert!(!may_await_in_progress(100 * H, &w));
        let never = target(ResendMode::Never, false);
        assert!(may_await_in_progress(0, &never));
    }

    #[test]
    fn unknown_failure_shape() {
        match unknown(7, "ostro-abc", 90_061_000_000_000) {
            Failure::NonRetryable(i) => {
                assert_eq!(i.kind, EFFECT_OUTCOME_UNKNOWN);
                assert_eq!(i.message, UNKNOWN_MESSAGE);
                let d: serde_json::Value =
                    serde_json::from_slice(&i.details.expect("details").data).unwrap();
                assert_eq!(
                    d,
                    serde_json::json!({"op": 7, "key": "ostro-abc", "elapsed_ms": 90_061_000u64})
                );
            }
            other => panic!("{other:?}"),
        }
    }
}
