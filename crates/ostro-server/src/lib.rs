//! ostro-server: single-node server glue (layered config, logging, node wiring).
//!
//! The storage <-> engine seam (04-15):
//! - [`derive`]: pure mapping of journal events to Phase 2 `Derived` intents.
//! - [`sink`]: [`sink::PartitionSink`], the real `ActivationSink` over a partition handle.
//! - [`run_started`]: `RunStarted` (seq 0) synthesis from a `RunRecord`.
//! - [`journal_io`]: reading a run's journal back as engine events.
//!
//! Execution (04-16): [`executor::EngineExecutor`], the dispatcher's `Executor` running
//! activations on the engine (lease heartbeat, self-fence, outcome state machine).

pub mod config;
pub mod derive;
pub mod executor;
pub mod journal_io;
pub mod logging;
pub mod run_started;
pub mod sink;

// Dependency anchors wired ahead of use (04-02); removed by the consuming plans.
use getrandom as _; // 04-17: OS RNG for run rng_seed / token key
use tempfile as _; // 04-17/04-19: `--ephemeral` TempDir data dir

#[cfg(test)]
mod dev_dep_anchors {
    use ostro_testkit as _;
}
