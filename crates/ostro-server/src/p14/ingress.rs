//! Phase 14: implemented in 14-20 (ingress quotas: suspended tenants, event-rate budgets;
//! Kafka starts are charged like webhook deliveries; dedupe hits are not charged).

use ostro_api::ApiError;

use super::{IngressKind, P14Ctx};

/// Stub: admits.
pub async fn admit(
    _ctx: &P14Ctx,
    _kind: IngressKind,
    _tenant: Option<&str>,
) -> Result<(), ApiError> {
    Ok(())
}
