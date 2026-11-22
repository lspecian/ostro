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
//! | [`dlq_hook`] | `on_failure` hook leader service (Phase 11) | 11-12 |
//! | [`bulk`] | bulk job registry and runner (Phase 11) | 11-13 |
//!
//! The Phase 11 services need the `P11Ctx`, which exists only once the API is wired:
//! `p11::wire` calls [`Services::attach_p11`], which starts them with a clone of the
//! [`ServiceCtx`] kept by [`Services::start`]; [`Services::shutdown`] joins them with the
//! others.
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
pub mod bulk;
pub mod dlq_hook;
pub mod guest_logs;
pub mod metrics_poll;
pub mod run_spans;
pub mod uploader;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use ostro_blob::BlobStore;
use ostro_meta::MetaHandle;
use ostro_partition::{Clock, Node};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::config::ServerConfig;
use crate::p11::P11Ctx;

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
    /// The context the services started with (`None` when idle); used by
    /// [`Services::attach_p11`].
    ctx: Option<ServiceCtx>,
    /// Tasks started by [`Services::attach_p11`] (Phase 11).
    p11_tasks: Mutex<Vec<(&'static str, JoinHandle<()>)>>,
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
            ctx: Some(ctx),
            p11_tasks: Mutex::new(Vec::new()),
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
            ctx: None,
            p11_tasks: Mutex::new(Vec::new()),
        }
    }

    /// Start the Phase 11 services ([`dlq_hook`], [`bulk`]) over `p11` (called once by
    /// `p11::wire`). Idle services start nothing.
    pub fn attach_p11(&self, p11: &Arc<P11Ctx>) {
        let Some(ctx) = &self.ctx else {
            return;
        };
        let spawned = [
            ("dlq_hook", dlq_hook::spawn(ctx, p11)),
            ("bulk", bulk::spawn(ctx, p11)),
        ];
        let mut tasks = self.p11_tasks.lock().unwrap_or_else(|p| p.into_inner());
        for (name, task) in spawned {
            if let Some(task) = task {
                tasks.push((name, task));
            }
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
        let p11 = self.p11_tasks.lock().unwrap_or_else(|p| p.into_inner());
        self.tasks
            .iter()
            .chain(p11.iter())
            .map(|(n, _)| *n)
            .collect()
    }

    /// Cancel every service and wait for it to exit. Releases every `ServiceCtx` clone
    /// held by the tasks (so the node and meta handles are unique again afterwards).
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        drop(self.ctx);
        let p11 = self
            .p11_tasks
            .into_inner()
            .unwrap_or_else(|p| p.into_inner());
        for (name, task) in self.tasks.into_iter().chain(p11) {
            if let Err(e) = task.await {
                warn!(service = name, error = %e, "service task failed");
            }
        }
    }
}
