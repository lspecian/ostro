//! Server-side replay (CLI-07, D-P10-7): `POST /v1/modules/{sha}/replay`, delegated
//! from [`ServerControlPlane`].
//!
//! Stub (10-05): answers 501 `unimplemented`. Plan 10-11 implements it: prepare the
//! module once, select the runs (explicit ids or the newest N per workflow), read each
//! run's journal from its partition (`cp.partitions`) and check it with
//! `ostro_engine::replay_check` on `cp.runtime` (which holds the cluster token key), on a
//! bounded pool. The token key and the journals never leave the server.

use ostro_api::ApiError;
use ostro_api::domain::{ReplayRequest, ReplayResult};

use super::ServerControlPlane;

/// Replay stored journals against a module. Stub: 501 (10-11 implements).
pub(super) async fn replay_module(
    cp: &ServerControlPlane,
    r: ReplayRequest,
) -> Result<ReplayResult, ApiError> {
    let _ = (cp, r);
    Err(ApiError::unimplemented("replay_module"))
}
