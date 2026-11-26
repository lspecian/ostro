//! Phase 14: implemented in 14-19 (signed modules: upload / version / activation gates,
//! evidence in ext 113, D-P14-14/15, B1a-c).

use ostro_api::ApiError;

use super::{P14Ctx, UploadEvidence};

/// Stub: no evidence, allowed.
pub async fn upload_gate(
    _ctx: &P14Ctx,
    _tenant: Option<&str>,
    _sha: [u8; 32],
    _bytes: &[u8],
    _sigs: &[Vec<u8>],
) -> Result<UploadEvidence, ApiError> {
    Ok(UploadEvidence::default())
}

/// Stub: no-op.
pub fn evidence_ext(_evidence: &UploadEvidence, _ext: &mut Vec<u8>) {}

/// Stub: no-op.
pub async fn merge_evidence(
    _ctx: &P14Ctx,
    _sha: [u8; 32],
    _evidence: UploadEvidence,
) -> Result<(), ApiError> {
    Ok(())
}

/// Stub: allowed.
pub async fn version_gate(
    _ctx: &P14Ctx,
    _tenant: Option<&str>,
    _workflow: &str,
    _version: u32,
    _sha: [u8; 32],
) -> Result<(), ApiError> {
    Ok(())
}

/// Stub: allowed.
pub async fn activation_gate(
    _ctx: &P14Ctx,
    _tenant: Option<&str>,
    _sha: [u8; 32],
) -> Result<(), ApiError> {
    Ok(())
}
