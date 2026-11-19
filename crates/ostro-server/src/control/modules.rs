//! Modules and workflows (API-02). Implemented in 04-17 Task 2.

use ostro_api::ApiError;
use ostro_api::domain::{
    ActivateWorkflow, ModuleInfo, UploadModule, UploadModuleResult, WorkflowInfo,
};

use super::ServerControlPlane;

pub(crate) async fn upload_module(
    _cp: &ServerControlPlane,
    _req: UploadModule,
) -> Result<UploadModuleResult, ApiError> {
    Err(ApiError::unavailable("module upload not implemented yet"))
}

pub(crate) fn get_module(_cp: &ServerControlPlane, sha: &str) -> Result<ModuleInfo, ApiError> {
    Err(ApiError::not_found(format!("module {sha} not found")))
}

pub(crate) fn list_workflows(_cp: &ServerControlPlane) -> Vec<WorkflowInfo> {
    Vec::new()
}

pub(crate) async fn activate_workflow(
    _cp: &ServerControlPlane,
    req: ActivateWorkflow,
) -> Result<WorkflowInfo, ApiError> {
    Err(ApiError::not_found(format!(
        "workflow {} not found",
        req.name
    )))
}
