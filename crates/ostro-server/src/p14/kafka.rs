//! Phase 14: implemented in 14-32 (Kafka topic-consumer triggers, D-P14-43): validates
//! `[[triggers.kafka]]` and runs the owner-leader consumers.

use std::sync::Arc;

use ostro_api::ControlPlane;
use ostro_partition::PartitionRoles;
use tokio::task::JoinHandle;

use super::{P14Ctx, P14Error};

/// Stub: `Ok(None)` without `[[triggers.kafka]]`, a startup error otherwise.
pub async fn start(
    ctx: &Arc<P14Ctx>,
    _routed: Arc<dyn ControlPlane>,
    _roles: Arc<dyn PartitionRoles>,
) -> Result<Option<JoinHandle<()>>, P14Error> {
    if ctx.cfg.triggers.kafka.is_empty() {
        Ok(None)
    } else {
        Err(P14Error::new(
            "[[triggers.kafka]] is configured, but Kafka triggers land in 14-32",
        ))
    }
}
