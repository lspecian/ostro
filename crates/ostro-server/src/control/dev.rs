//! Dev clock endpoints (SDK-06, D-P10-6): `GET /v1/dev/time` and
//! `POST /v1/dev/time/skip`, delegated from [`ServerControlPlane`].
//!
//! Plan 10-10: both read the [`DevClock`](crate::devclock::DevClock) that exists only when
//! time skipping is on (`ostro dev --ephemeral --time-skipping`); with it off they answer
//! 409 `time_skipping_disabled`.
//!
//! Plan 10-13: `idle` / `next_due_ms` come from the [`DevTime`](crate::devtime::DevTime)
//! idle snapshot. A manual skip (either mode) is lease-safe (D-W26): it first waits up to
//! [`SKIP_BUSY_WAIT`] for zero activations (leases or grants) and zero mutating API
//! requests, else answers 503 `busy: N activations in flight; retry`; then it skips through
//! [`DevClock::skip_guarded`](crate::devclock::DevClock::skip_guarded) and re-checks the
//! activations under the barrier's write guard (atomic loads only). A re-check that finds
//! work goes back to waiting within the same deadline.

use std::time::Duration;

use ostro_api::ApiError;
use ostro_api::domain::{DevSkip, DevSkipResult, DevTimeInfo, time_skipping_disabled};

use super::ServerControlPlane;

/// How long a manual skip waits for in-flight activations to finish.
pub const SKIP_BUSY_WAIT: Duration = Duration::from_secs(5);
/// Poll interval of that wait.
const SKIP_BUSY_POLL: Duration = Duration::from_millis(10);

/// The dev clock: `{now_ms, offset_ms, mode, idle, next_due_ms}`.
pub(super) async fn dev_time(cp: &ServerControlPlane) -> Result<DevTimeInfo, ApiError> {
    let dev = cp.dev.as_ref().ok_or_else(time_skipping_disabled)?;
    let snap = dev.time().map(|t| t.snapshot()).unwrap_or_default();
    let (now_ms, offset_ms, mode) = dev.info();
    Ok(DevTimeInfo {
        now_ms,
        offset_ms,
        mode,
        idle: snap.idle,
        next_due_ms: snap.next_due_ms,
    })
}

/// Skip the dev clock forward (never backwards; a target in the past skips 0 ms).
pub(super) async fn dev_skip(
    cp: &ServerControlPlane,
    s: DevSkip,
) -> Result<DevSkipResult, ApiError> {
    let dev = cp.dev.as_ref().ok_or_else(time_skipping_disabled)?;
    let time = dev.time();
    let deadline = tokio::time::Instant::now() + SKIP_BUSY_WAIT; // clock-exempt: busy wait bound (real time)
    let skipped_ms = loop {
        if let Some(t) = &time {
            loop {
                let c = t.counters();
                if c.busy_activations == 0 && c.api_writes == 0 {
                    break;
                }
                let expired = tokio::time::Instant::now() >= deadline; // clock-exempt: busy wait bound (real time)
                if expired {
                    let mut msg =
                        format!("busy: {} activations in flight; retry", c.busy_activations);
                    if c.api_writes > 0 {
                        msg = format!(
                            "busy: {} activations in flight, {} API writes in flight; retry",
                            c.busy_activations, c.api_writes
                        );
                    }
                    return Err(ApiError::unavailable(msg));
                }
                tokio::time::sleep(SKIP_BUSY_POLL).await;
            }
        }
        let mut refused = false;
        let skipped = dev
            .skip_guarded(|now| {
                if time
                    .as_ref()
                    .is_some_and(|t| t.counters().busy_activations != 0)
                {
                    refused = true;
                    return None;
                }
                Some(match s {
                    DevSkip::By { ms } => now.saturating_add(ms),
                    DevSkip::To { ms } => ms,
                })
            })
            .await;
        if !refused {
            break skipped;
        }
    };
    let (now_ms, offset_ms, _) = dev.info();
    Ok(DevSkipResult {
        now_ms,
        offset_ms,
        skipped_ms,
    })
}
