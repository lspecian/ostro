//! Phase 7: implemented in 07-09.
//!
//! The `ostro.worker.v1` Execute service served on `[cluster] listen` (mTLS, client
//! certificate always required, D-W16): sessions, journal/blob fetch, append, bridge-back
//! effects (D-W3), renewals.

use tokio::task::JoinHandle;

use crate::p7::{P7Ctx, P7Error};

/// Start the cluster listener and the Execute service; `None` when nothing runs.
///
/// Phase 7 stub (07-02): `Ok(None)`.
pub fn start(ctx: &P7Ctx) -> Result<Option<JoinHandle<()>>, P7Error> {
    let _ = ctx;
    Ok(None)
}
