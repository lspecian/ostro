//! ostro-cli: the CLI-06 client verbs of the `ostro` binary over the REST API.
//!
//! - [`client`]: typed REST client (reqwest, JSON, h2c; no TLS until SEC-01).
//! - [`output`]: `--json` / human output and the exit-code mapping
//!   (0 ok, 1 API/server error, 2 usage, 3 `--wait` ended not completed).
//! - [`commands`]: clap verbs, embedded in the `ostro` clap tree with
//!   `#[command(flatten)]`, and their dispatch. Phase 6 (06-16) adds the bridge admin
//!   verbs `secret`, `grant`, `connection` and `egress` ([`commands::bridge`]).
//!   Phase 7 (07-15) adds `queue`, `workflow pause|resume`, `run pause|resume`
//!   ([`commands::queues`], [`commands::pause`]) and `worker token`
//!   ([`commands::worker_tokens`]).

pub mod client;
pub mod commands;
pub mod output;
pub mod watch;

pub use commands::{ClientCommand, run};
pub use output::{Outcome, exit_code};

// Phase 8 (plan 08-01): `tar` for `ostro backup --to` (08-12/08-18), anchored until it is used.
use tar as _;

// Phase 10 (plan 10-02): dependencies of 10-09 (ostro-client) and 10-12/10-13 (ostro-build,
// ostro-engine), anchored until they are used.
use ostro_build as _;
use ostro_client as _;
use ostro_engine as _;

#[cfg(test)]
mod dev_dep_anchors {
    // FakeControlPlane-backed CLI tests (08-12, tests/phase8_verbs.rs).
    use axum as _;
    use ostro_api as _;
    // Phase 10 (10-02): --json snapshots of the Phase 10 verbs.
    use insta as _;
}
