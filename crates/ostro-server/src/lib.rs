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
//!
//! Phase 8 (08-04): the [`services`] seam (one place every node-level background service
//! starts and stops, P8-D21), the Phase 8 config sections in [`config`], and
//! [`telemetry`] (Prometheus `/metrics`, OTLP traces/logs, `OstroIdGenerator`).
//!
//! Phase 6 (06-14): [`bridge_services::BridgeServices`] builds the effects bridge once per
//! server over [`meta_view::MetaHandleView`]; [`effects`] adapts it to the engine's
//! `EffectDriver` (completions through `Cmd::CompleteEffect`, the pre-send gate).
//!
//! Phase 8 (08-21): [`effect_trace::TraceEffectHooks`] fills the effect hooks with outbound
//! `traceparent` propagation (live request copy only) and `effect:<kind>` spans.
//!
//! Phase 7 (07-02): [`p7`] is the single wiring seam (D-W17) called from [`server`]; the
//! Phase 7 modules [`tls`] (client-listener TLS, cluster CA), [`flow`] (node flow control)
//! and [`workers`] (registry, worker protocol, placement, spill, demand) are stubs with
//! fixed signatures until their plans (07-06 .. 07-16) fill them.
//!
//! Phase 11 (11-04): [`p11`] is the single wiring seam (D-P11-23) called once from
//! [`server`]: the `P11Ctx`, the `ApiExtensions::{ops, inventory}` slots, the UI router
//! and the emission gate [`p11::p11_feature_enabled`]; [`rerun`] (11-07) and
//! [`onfailure`] (11-12) are stubs until their plans.
//!
//! Phase 12 (12-02): [`cluster`] is the cluster wiring seam (12-14: transport, meta peers,
//! controller, fetchers, replication listener); `[replication]` and the `[cluster]` peer
//! keys live in [`config`]; [`ServerOptions::cluster_transport`] is the transport hook.

pub mod bridge_services;
pub mod children;
pub mod cluster;
pub mod config;
pub mod control;
pub mod derive;
pub mod effect_trace;
pub mod effects;
pub mod executor;
pub mod flow;
pub mod journal_io;
pub mod leadership;
pub mod logging;
pub mod meta_view;
pub mod onfailure;
pub mod p11;
pub mod p7;
pub mod rerun;
pub mod retention;
pub mod run_started;
pub mod server;
pub mod services;
pub mod sink;
pub mod telemetry;
pub mod tls;
pub mod workers;

pub use server::{
    LOCK_FILE, Server, ServerError, ServerHandle, ServerOptions, StartupTimings, start,
};

#[cfg(test)]
mod dev_dep_anchors {
    use ostro_testkit as _;
}

// Phase 8 dependencies declared once by plan 08-01 (P8-D21) for later Phase 8 plans
// (08-04 telemetry/metrics, 08-11 spans, 08-13/08-18 bottomless, 08-14/08-15 triggers).
// Anchored here so cargo's unused-dependency lint stays quiet until the owning modules
// consume them; harmless afterwards.
// (08-04 consumes the OTel/Prometheus crates and ostro-backup's config in `telemetry` and
// `config`.)
mod phase8_dep_anchors {
    use hmac as _;
    use ostro_objstore as _;
    use ostro_triggers as _;
}

// Phase 6 dependency declared once by plan 06-02 for 06-16 (admin impl); anchored like the
// Phase 8 ones above (ostro-bridge itself is consumed by 06-14's `bridge_services`).
mod phase6_dep_anchors {
    use base64 as _;
}

// Phase 7 dependencies declared once by plan 07-02 (D-W17) for 07-06 (CA, TLS listener),
// 07-09/07-12 (worker protocol), 07-10 (ACME) and 07-13 (getrusage); anchored like the
// Phase 6/8 ones above until the owning modules consume them.
mod phase7_dep_anchors {
    use arc_swap as _;
    use axum_server as _;
    use instant_acme as _;
    use libc as _;
    use ostro_worker_proto as _;
    use rcgen as _;
    use rustls as _;
    use rustls_pki_types as _;
    use tokio_rustls as _;
    use x509_parser as _;
}
