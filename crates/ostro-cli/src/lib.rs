//! ostro-cli: the CLI-06 client verbs of the `ostro` binary over the REST API.
//!
//! - [`client`]: typed REST client (reqwest, JSON, h2c; no TLS until SEC-01).
//! - [`output`]: `--json` / human output and the exit-code mapping
//!   (0 ok, 1 API/server error, 2 usage, 3 `--wait` ended not completed).
//! - [`commands`]: clap verbs, embedded in the `ostro` clap tree with
//!   `#[command(flatten)]`, and their dispatch.

pub mod client;
pub mod commands;
pub mod output;

pub use commands::{ClientCommand, run};
pub use output::{Outcome, exit_code};

// Phase 8 (plan 08-01): `tar` for `ostro backup --to` (08-12/08-18), anchored until it is used.
use tar as _;

#[cfg(test)]
mod dev_dep_anchors {
    // FakeControlPlane-backed CLI tests (08-12).
    use ostro_api as _;
}
