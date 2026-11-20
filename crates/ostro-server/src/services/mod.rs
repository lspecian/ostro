//! The ostro-server services seam (P8-D21, plan 08-04).
//!
//! Every Phase 8 node-level background service lives in its own submodule and is started
//! from one place, [`Services::start`], so later plans fill a submodule without touching
//! `server.rs`:
//!
//! | Submodule | Service | Implemented by |
//! |---|---|---|
//! | [`run_spans`] | run / activation span emission | 08-11 |
//! | [`uploader`] | bottomless uploader (consistent cut, seal, upload) | 08-13 |
//! | [`metrics_poll`] | gauge poller (queues, lag, HW, ISR) | 08-16 |
//! | [`guest_logs`] | guest log sink (rate limits, ring buffer, OTLP) | 08-17 |
//! | [`blob_gc`] | local blob garbage collection | 08-19 |
//!
//! # Lifecycle
//!
//! `server.rs` builds one [`ServiceCtx`] after `Node::open` returned (every partition's
//! readiness barrier is open, the leader services run) and the partition leaders are
//! recorded in meta, and calls [`Services::start`] before the API starts serving.
//! Shutdown order: the API stops, then [`Services::shutdown`] (cancel + join), then the
//! node, then meta, then the telemetry providers are flushed.
//!
//! # Time
//!
//! Every time read that feeds service logic goes through [`ServiceCtx::clock`], the same
//! `Arc<dyn Clock>` instance the node uses (Phase 10 F5: one injectable clock).

pub mod blob_gc;
pub mod guest_logs;
pub mod metrics_poll;
pub mod run_spans;
pub mod uploader;

use std::path::PathBuf;
use std::sync::Arc;

use ostro_blob::BlobStore;
use ostro_meta::MetaHandle;
use ostro_partition::{Clock, Node};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::config::ServerConfig;

/// What every service gets. Cheap to clone (all shared handles).
#[derive(Clone)]
pub struct ServiceCtx {
    /// The partition node (every slot started; barriers open).
    pub node: Arc<Node>,
    /// The metadata handle (shared with the control plane).
    pub meta: Arc<MetaHandle>,
    /// The node-wide content-addressed blob store.
    pub blobs: Arc<BlobStore>,
    /// The effective server configuration.
    pub config: Arc<ServerConfig>,
    /// The resolved data directory (a temp dir in ephemeral mode).
    pub data_dir: PathBuf,
    /// Cancelled by [`Services::shutdown`]; every service exits promptly once it fires.
    pub shutdown: CancellationToken,
    /// The one injectable clock (shared with `NodeConfig.clock`).
    pub clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for ServiceCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceCtx")
            .field("data_dir", &self.data_dir)
            .field("cancelled", &self.shutdown.is_cancelled())
            .finish_non_exhaustive()
    }
}

/// The running services.
pub struct Services {
    shutdown: CancellationToken,
    tasks: Vec<(&'static str, JoinHandle<()>)>,
    guest_logs: Arc<guest_logs::GuestLogSink>,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for Services {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Services")
            .field(
                "tasks",
                &self.tasks.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

impl Services {
    /// Start every service. `guest_logs` is the sink the executor was built with (it
    /// exists before the node opens, because the executor is part of `NodeConfig`).
    pub fn start(ctx: ServiceCtx, guest_logs: Arc<guest_logs::GuestLogSink>) -> Self {
        let mut tasks = Vec::new();
        let spawned = [
            ("uploader", uploader::spawn(&ctx)),
            ("blob_gc", blob_gc::spawn(&ctx)),
            ("metrics_poll", metrics_poll::spawn(&ctx)),
            ("run_spans", run_spans::spawn(&ctx)),
        ];
        for (name, task) in spawned {
            if let Some(task) = task {
                tasks.push((name, task));
            }
        }
        Self {
            shutdown: ctx.shutdown.clone(),
            tasks,
            guest_logs,
            clock: Arc::clone(&ctx.clock),
        }
    }

    /// Services that never start a task (no partition started, so there is no node-wide
    /// blob store to hand out); only the guest log sink exists.
    pub fn idle(guest_logs: Arc<guest_logs::GuestLogSink>, clock: Arc<dyn Clock>) -> Self {
        Self {
            shutdown: CancellationToken::new(),
            tasks: Vec::new(),
            guest_logs,
            clock,
        }
    }

    /// The guest log sink (also held by the executor).
    pub fn guest_log_sink(&self) -> Arc<guest_logs::GuestLogSink> {
        Arc::clone(&self.guest_logs)
    }

    /// The clock every service reads.
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    /// Names of the running service tasks.
    pub fn running(&self) -> Vec<&'static str> {
        self.tasks.iter().map(|(n, _)| *n).collect()
    }

    /// Cancel every service and wait for it to exit. Releases every `ServiceCtx` clone
    /// held by the tasks (so the node and meta handles are unique again afterwards).
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        for (name, task) in self.tasks {
            if let Err(e) = task.await {
                warn!(service = name, error = %e, "service task failed");
            }
        }
    }
}
