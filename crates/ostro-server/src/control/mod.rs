//! The real [`ControlPlane`] (plans 04-17 / 04-18): every REST and gRPC adapter in
//! `ostro-api` calls into [`ServerControlPlane`], which reads and writes the metadata
//! state machine, the partitions and the engine runtime.
//!
//! - [`modules`]: module upload (validate + AOT + register workflows), module lookup,
//!   workflow listing and activation (API-02).
//! - [`runs`]: start (API-03) and get (API-04 / WF-01) (04-18).
//! - [`history`]: a run's decoded journal (API-04) (04-18).
//! - [`list`]: filtered, paginated run listing across partitions (API-04) (04-18).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ostro_api::ApiError;
use ostro_api::domain::{
    ActivateWorkflow, ControlPlane, HistoryPage, HistoryRequest, ListRunsRequest, ModuleInfo,
    RunInfo, RunPage, StartRunRequest, StartRunResult, UploadModule, UploadModuleResult,
    WorkflowInfo,
};
use ostro_blob::BlobStore;
use ostro_engine::runtime::Runtime;
use ostro_log::Rf1Log;
use ostro_meta::MetaHandle;
use ostro_partition::{PartitionHandle, PartitionHealth};
use ostro_types::{PartitionCount, PartitionId};

use crate::config::ServerConfig;

pub mod history;
pub mod list;
pub mod modules;
pub mod runs;

/// Shared state behind every API call.
pub struct ServerControlPlane {
    pub(crate) meta: Arc<MetaHandle>,
    /// One entry per partition: `Some` when the slot started (`PartitionSlot::Ready`).
    pub(crate) partitions: Vec<Option<PartitionHandle<Rf1Log>>>,
    pub(crate) count: PartitionCount,
    /// The node-wide blob store (shared by every partition); `None` if no partition started.
    pub(crate) blobs: Option<Arc<BlobStore>>,
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) cfg: Arc<ServerConfig>,
    /// Cleared when shutdown begins (`/readyz` turns 503 before the listener closes).
    pub(crate) serving: AtomicBool,
}

impl std::fmt::Debug for ServerControlPlane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerControlPlane")
            .field("partitions", &self.count.get())
            .field("serving", &self.serving.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl ServerControlPlane {
    pub(crate) fn new(
        meta: Arc<MetaHandle>,
        partitions: Vec<Option<PartitionHandle<Rf1Log>>>,
        count: PartitionCount,
        runtime: Arc<Runtime>,
        cfg: Arc<ServerConfig>,
    ) -> Self {
        let blobs = partitions
            .iter()
            .flatten()
            .next()
            .map(|h| Arc::clone(h.blobs()));
        Self {
            meta,
            partitions,
            count,
            blobs,
            runtime,
            cfg,
            serving: AtomicBool::new(true),
        }
    }

    /// The metadata handle.
    pub fn meta(&self) -> &MetaHandle {
        &self.meta
    }

    /// The engine runtime.
    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    /// The effective server configuration.
    pub fn config(&self) -> &ServerConfig {
        &self.cfg
    }

    /// The fixed partition count.
    pub fn partition_count(&self) -> PartitionCount {
        self.count
    }

    /// Partition `p`'s handle, or `Unavailable` when its slot failed to start.
    pub fn partition(&self, p: PartitionId) -> Result<&PartitionHandle<Rf1Log>, ApiError> {
        self.partitions
            .get(p.index())
            .and_then(Option::as_ref)
            .ok_or_else(|| ApiError::unavailable(format!("partition {} is not available", p.0)))
    }

    /// The node-wide blob store.
    pub fn blobs(&self) -> Result<&Arc<BlobStore>, ApiError> {
        self.blobs
            .as_ref()
            .ok_or_else(|| ApiError::unavailable("no partition is available (blob store)"))
    }

    /// Wall-clock ms since the Unix epoch (proposers supply every metadata timestamp).
    pub fn now_ms(&self) -> u64 {
        self.runtime.clock().now_unix_nanos() / 1_000_000
    }

    pub(crate) fn stop_serving(&self) {
        self.serving.store(false, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl ControlPlane for ServerControlPlane {
    async fn upload_module(&self, req: UploadModule) -> Result<UploadModuleResult, ApiError> {
        modules::upload_module(self, req).await
    }

    async fn get_module(&self, sha256: String) -> Result<ModuleInfo, ApiError> {
        modules::get_module(self, &sha256)
    }

    async fn list_workflows(&self) -> Result<Vec<WorkflowInfo>, ApiError> {
        Ok(modules::list_workflows(self))
    }

    async fn activate_workflow(&self, req: ActivateWorkflow) -> Result<WorkflowInfo, ApiError> {
        modules::activate_workflow(self, req).await
    }

    async fn start_run(&self, req: StartRunRequest) -> Result<StartRunResult, ApiError> {
        runs::start_run(self, req).await
    }

    async fn get_run(&self, run_id: String) -> Result<RunInfo, ApiError> {
        runs::get_run(self, run_id).await
    }

    async fn run_history(&self, req: HistoryRequest) -> Result<HistoryPage, ApiError> {
        history::run_history(self, req).await
    }

    async fn list_runs(&self, req: ListRunsRequest) -> Result<RunPage, ApiError> {
        list::list_runs(self, req).await
    }

    /// Serving, meta has a leader, and every partition slot is `Ready` and not failed.
    fn ready(&self) -> bool {
        self.serving.load(Ordering::SeqCst)
            && self.meta.is_leader()
            && self.partitions.iter().all(|slot| {
                slot.as_ref().is_some_and(|h| {
                    !matches!(
                        *h.machine().health().borrow(),
                        PartitionHealth::Failed { .. }
                    )
                })
            })
    }
}
