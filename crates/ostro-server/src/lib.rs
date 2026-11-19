//! ostro-server: single-node server glue (layered config, logging, node wiring).
//!
//! The storage <-> engine seam (04-15):
//! - [`derive`]: pure mapping of journal events to Phase 2 `Derived` intents.
//! - [`run_started`]: `RunStarted` (seq 0) synthesis from a `RunRecord`.
//! - [`journal_io`]: reading a run's journal back as engine events.

pub mod config;
pub mod derive;
pub mod journal_io;
pub mod logging;
pub mod run_started;

// Dependency anchors wired ahead of use (04-02); removed by the consuming plans.
use getrandom as _; // 04-17: OS RNG for run rng_seed / token key
use tempfile as _; // 04-17/04-19: `--ephemeral` TempDir data dir
use tracing as _; // 04-17+: server events

#[cfg(test)]
mod dev_dep_anchors {
    use ostro_testkit as _;
}
