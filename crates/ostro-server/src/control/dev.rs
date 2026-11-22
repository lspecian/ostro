//! Dev clock endpoints (SDK-06, D-P10-6): `GET /v1/dev/time` and
//! `POST /v1/dev/time/skip`, delegated from [`ServerControlPlane`].
//!
//! Stub (10-05): time skipping does not exist yet, so both answer 409
//! `time_skipping_disabled`. Plan 10-10 replaces these bodies with the `DevClock`
//! (enabled only by `ostro dev --ephemeral --time-skipping`); with time skipping off they
//! keep answering this 409.

use ostro_api::ApiError;
use ostro_api::domain::{DevSkip, DevSkipResult, DevTimeInfo, time_skipping_disabled};

use super::ServerControlPlane;

/// The dev clock. Stub: always 409 `time_skipping_disabled` (10-10 implements).
pub(super) async fn dev_time(cp: &ServerControlPlane) -> Result<DevTimeInfo, ApiError> {
    let _ = cp;
    Err(time_skipping_disabled())
}

/// Skip the dev clock forward. Stub: always 409 `time_skipping_disabled` (10-10
/// implements).
pub(super) async fn dev_skip(
    cp: &ServerControlPlane,
    s: DevSkip,
) -> Result<DevSkipResult, ApiError> {
    let _ = (cp, s);
    Err(time_skipping_disabled())
}
