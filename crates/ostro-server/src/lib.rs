//! ostro-server: single-node server glue (layered config, logging, node wiring).

pub mod config;
pub mod logging;

// Dependency anchors wired ahead of use (04-02); removed by the consuming plans.
use getrandom as _; // 04-17: OS RNG for run rng_seed / token key
use tempfile as _; // 04-17/04-19: `--ephemeral` TempDir data dir
use tokio as _; // 04-19: server runtime
use tracing as _; // 04-17+: server events
