//! `impl ModuleLifecycleApi` (D-P13-24): pure delegation. Final after 13-02.
//!
//! | method | hook (owner) |
//! |--------|--------------|
//! | `retire_module` | [`crate::control::module_retire::retire`] (13-15) |
//! | `run_module_gc` | [`super::module_gc::run_now`] (13-15) |
//! | `migrate_runs` | [`crate::control::migrate::migrate_runs`] (13-17) |

use std::sync::Arc;

use ostro_api::ApiError;
use ostro_api::phase13::{
    GcRunReport, MigrateRunsReport, MigrateRunsRequest, ModuleLifecycleApi, RetireModuleResponse,
};

use super::P13Ctx;

/// The module lifecycle operations of this node.
#[derive(Debug)]
pub struct P13ModuleLifecycle {
    ctx: Arc<P13Ctx>,
}

impl P13ModuleLifecycle {
    /// Over `ctx`.
    pub fn new(ctx: Arc<P13Ctx>) -> Self {
        Self { ctx }
    }
}

#[async_trait::async_trait]
impl ModuleLifecycleApi for P13ModuleLifecycle {
    async fn retire_module(&self, sha: String) -> Result<RetireModuleResponse, ApiError> {
        crate::control::module_retire::retire(&self.ctx, sha).await
    }

    async fn run_module_gc(&self) -> Result<GcRunReport, ApiError> {
        super::module_gc::run_now(&self.ctx).await
    }

    async fn migrate_runs(&self, r: MigrateRunsRequest) -> Result<MigrateRunsReport, ApiError> {
        crate::control::migrate::migrate_runs(&self.ctx, r).await
    }
}
