//! Dev clock endpoints (SDK-06, D-P10-6): `GET /v1/dev/time` and
//! `POST /v1/dev/time/skip`, delegated from [`ServerControlPlane`].
//!
//! Plan 10-10: both read the [`DevClock`](crate::devclock::DevClock) that exists only when
//! time skipping is on (`ostro dev --ephemeral --time-skipping`); with it off they answer
//! 409 `time_skipping_disabled`.
//!
//! A skip goes through [`DevClock::skip_guarded`](crate::devclock::DevClock::skip_guarded)
//! (the write guard of the skip barrier, so no grant interleaves it). In 10-10 the check
//! always returns the requested target; 10-13 adds the busy check and fills `idle` /
//! `next_due_ms`.

use ostro_api::ApiError;
use ostro_api::domain::{DevSkip, DevSkipResult, DevTimeInfo, time_skipping_disabled};

use super::ServerControlPlane;

/// The dev clock: `{now_ms, offset_ms, mode, idle, next_due_ms}`.
pub(super) async fn dev_time(cp: &ServerControlPlane) -> Result<DevTimeInfo, ApiError> {
    let dev = cp.dev.as_ref().ok_or_else(time_skipping_disabled)?;
    let (now_ms, offset_ms, mode) = dev.info();
    Ok(DevTimeInfo {
        now_ms,
        offset_ms,
        mode,
        // 10-13 (idle detector) fills these.
        idle: false,
        next_due_ms: None,
    })
}

/// Skip the dev clock forward (never backwards; a target in the past skips 0 ms).
pub(super) async fn dev_skip(
    cp: &ServerControlPlane,
    s: DevSkip,
) -> Result<DevSkipResult, ApiError> {
    let dev = cp.dev.as_ref().ok_or_else(time_skipping_disabled)?;
    let skipped_ms = dev
        .skip_guarded(|now| {
            Some(match s {
                DevSkip::By { ms } => now.saturating_add(ms),
                DevSkip::To { ms } => ms,
            })
        })
        .await;
    let (now_ms, offset_ms, _) = dev.info();
    Ok(DevSkipResult {
        now_ms,
        offset_ms,
        skipped_ms,
    })
}
