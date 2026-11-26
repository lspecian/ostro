//! Phase 14: implemented in 14-18 (audit capture, D-P14-11/12/13): reserve-before-act
//! permits for audited methods, the post-call record, worker events.

use ostro_api::ApiError;
use ostro_api::auth::{Method, Principal};
use ostro_types::audit::{AuditOutcome, AuditPermit};

use super::{P14Ctx, WorkerAuditEvent};

/// Before an audited call: a writer permit (fail-closed: 503 `audit_unavailable`).
/// `Ok(None)` for methods that are not audited. Stub: `Ok(None)`.
pub fn reserve(_ctx: &P14Ctx, _p: &Principal, _m: Method) -> Result<Option<AuditPermit>, ApiError> {
    Ok(None)
}

/// After the call: commit the record. Stub: `Ok(())`.
pub async fn after(
    _ctx: &P14Ctx,
    _permit: Option<AuditPermit>,
    _p: &Principal,
    _m: Method,
    _target: &str,
    _outcome: AuditOutcome,
) -> Result<(), ApiError> {
    Ok(())
}

/// A worker join / renew / token revoke ([`super::worker_event`]). Stub: no-op.
pub fn worker_event(_ctx: &P14Ctx, _ev: WorkerAuditEvent) {}
