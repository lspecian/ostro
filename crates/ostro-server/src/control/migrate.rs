//! Phase 13: implemented in 13-17 (`POST /v1/runs/migrate`, CLUS-07 / D-P13-21): replay-check
//! the selected runs against the target version and repin the ones that pass
//! (`Cmd::RepinRun`, expected next seq).
//!
//! Fixed signature. Stub: `Unimplemented`.

use ostro_api::ApiError;
use ostro_api::phase13::{MigrateRunsReport, MigrateRunsRequest};

use crate::p13::{P13Ctx, lands_in};

/// Migrate runs of one workflow version to another. Stub.
pub async fn migrate_runs(
    ctx: &P13Ctx,
    r: MigrateRunsRequest,
) -> Result<MigrateRunsReport, ApiError> {
    let _ = (ctx, r);
    Err(lands_in("migrate_runs", "13-17"))
}
