//! The `on_failure` hook leader service (Phase 11, D-P11-12): on
//! `RunCompleted{Failed|TimedOut}` resolve the failure policy ([`crate::onfailure`]),
//! start the hook run (dedupe `__onfail/<run>/<resumed_count>`) and record
//! `DlqUpdate HookStarted`; at leader start re-drive failed runs newer than
//! `[dlq] hook_catchup`. Declared by 11-04 (D-P11-23); implemented by **11-12**.
//!
//! Started through `Services::attach_p11` by `p11::wire`; exits on
//! `ServiceCtx::shutdown`.

use std::sync::Arc;

use tokio::task::JoinHandle;

use super::ServiceCtx;
use crate::p11::P11Ctx;

/// The hook service task. Stub: starts no task (11-12).
pub fn spawn(svc: &ServiceCtx, p11: &Arc<P11Ctx>) -> Option<JoinHandle<()>> {
    let _ = (svc, p11);
    None
}
