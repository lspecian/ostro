//! Phase 14: implemented in 14-03 (admin bootstrap, online and offline).

use std::path::PathBuf;

use super::{P14Ctx, P14Error};

/// `ostro auth bootstrap` arguments (the `ostro` binary intercepts the CLI variant).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BootstrapArgs {
    /// The stopped node's data directory (`--data-dir`).
    pub data_dir: PathBuf,
    /// The server configuration file (`--config`).
    pub config: Option<PathBuf>,
    /// Token lifetime (`--expires`, a friendly duration); `None` = no expiry.
    pub expires: Option<String>,
}

/// Online: make sure one system-admin token exists (first start). Stub: nothing to do.
pub async fn ensure_admin(_ctx: &P14Ctx) -> Result<(), P14Error> {
    Ok(())
}

/// Offline `ostro auth bootstrap --data-dir` on a stopped single node: creates a system-admin
/// token and prints the bearer once. Stub: not implemented.
pub fn offline(_args: BootstrapArgs) -> Result<(), P14Error> {
    Err(P14Error::new(
        "ostro auth bootstrap: not implemented until 14-03",
    ))
}
