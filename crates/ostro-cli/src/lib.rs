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
