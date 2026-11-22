//! Node-local bulk operation jobs (Phase 11, OPS-03, D-P11-20). Declared by 11-04
//! (D-P11-23); implemented by **11-13** (job files, pinned index cursor, GCRA pacing,
//! adaptive backpressure pause, checkpoints, resume after restart).
//!
//! The one [`BulkRegistry`] per node is built by `p11::wire` and held as
//! `P11Ctx::bulk`; the bulk controllers (`control::p11_bulk`) and the service task
//! started here ([`spawn`], through `Services::attach_p11`) share that `Arc`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ostro_partition::Clock;
use tokio::task::JoinHandle;

use super::ServiceCtx;
use crate::config::BulkSection;
use crate::p11::P11Ctx;

/// The node's bulk jobs. Internals land in 11-13; the constructor signature is fixed.
pub struct BulkRegistry {
    data_dir: PathBuf,
    cfg: BulkSection,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for BulkRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BulkRegistry")
            .field("data_dir", &self.data_dir)
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

impl BulkRegistry {
    /// Over the node data directory (`data_dir`; 11-13 picks its subdirectory), `[bulk]`
    /// and the injected clock. Touches no disk.
    pub fn new(data_dir: PathBuf, cfg: BulkSection, clock: Arc<dyn Clock>) -> Arc<Self> {
        Arc::new(Self {
            data_dir,
            cfg,
            clock,
        })
    }

    /// The node data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// `[bulk]`.
    pub fn config(&self) -> &BulkSection {
        &self.cfg
    }

    /// The injected clock (every pacing / checkpoint time read goes through it).
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }
}

/// The bulk service task (resumes persisted jobs at start; exits on
/// `ServiceCtx::shutdown`). Stub: starts no task (11-13).
pub fn spawn(svc: &ServiceCtx, p11: &Arc<P11Ctx>) -> Option<JoinHandle<()>> {
    let _ = (svc, p11);
    None
}
