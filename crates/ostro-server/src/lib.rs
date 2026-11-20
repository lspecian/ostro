//! ostro-server: single-node server glue (layered config, logging, node wiring).
//!
//! The storage <-> engine seam (04-15):
//! - [`derive`]: pure mapping of journal events to Phase 2 `Derived` intents.
//! - [`sink`]: [`sink::PartitionSink`], the real `ActivationSink` over a partition handle.
//! - [`run_started`]: `RunStarted` (seq 0) synthesis from a `RunRecord`.
//! - [`journal_io`]: reading a run's journal back as engine events.
//!
//! Phase 5 (05-11): [`derive`] maps waits, tokens, cancel-op, the Cancelled status and
//! continue-as-new (successor `StartRun` via a [`derive::ModuleResolver`]) and re-arms
//! unresolved ops after a resume; [`retention`] resolves retention / schedule-to-start
//! (request > `WorkflowPolicy` > `QueuePolicy` > `[runs]` config).
//!
//! Execution (04-16): [`executor::EngineExecutor`], the dispatcher's `Executor` running
//! activations on the engine (lease heartbeat, self-fence, outcome state machine).
//!
//! Server (04-17): [`server`] boots meta -> runtime -> node (+ dispatcher) -> API on one
//! port ([`Server::start`] -> [`ServerHandle`]); [`leadership::MetaLeadership`] supplies
//! partition epoch floors from meta; [`control::ServerControlPlane`] is the real
//! `ostro_api::ControlPlane` (modules/workflows here, runs in 04-18).

pub mod config;
pub mod control;
pub mod derive;
pub mod executor;
pub mod journal_io;
pub mod leadership;
pub mod logging;
pub mod retention;
pub mod run_started;
pub mod server;
pub mod sink;

pub use server::{LOCK_FILE, Server, ServerError, ServerHandle, StartupTimings, start};

#[cfg(test)]
mod dev_dep_anchors {
    use ostro_testkit as _;
}

// Phase 8 dependencies declared once by plan 08-01 (P8-D21) for later Phase 8 plans
// (08-04 telemetry/metrics, 08-11 spans, 08-13/08-18 bottomless, 08-14/08-15 triggers).
// Anchored here so cargo's unused-dependency lint stays quiet until the owning modules
// consume them; harmless afterwards.
mod phase8_dep_anchors {
    use hmac as _;
    use metrics_exporter_prometheus as _;
    use opentelemetry as _;
    use opentelemetry_appender_tracing as _;
    use opentelemetry_otlp as _;
    use opentelemetry_sdk as _;
    use ostro_backup as _;
    use ostro_objstore as _;
    use ostro_triggers as _;
    use tracing_opentelemetry as _;
}
