//! Phase 14: implemented in 14-04 (offline chain verification).

use std::path::PathBuf;

use crate::p14::P14Error;

/// `ostro audit verify` arguments (the `ostro` binary intercepts the CLI variant).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VerifyArgs {
    /// The node's data directory (`--data-dir`); the journal is `<data>/audit` unless
    /// `[audit] dir` says otherwise.
    pub data_dir: PathBuf,
    /// The server configuration file (`--config`).
    pub config: Option<PathBuf>,
}

/// Verify the journal under `args.data_dir` and print the report. Stub: not implemented.
pub fn offline(_args: VerifyArgs) -> Result<(), P14Error> {
    Err(P14Error::new(
        "ostro audit verify: not implemented until 14-04",
    ))
}
