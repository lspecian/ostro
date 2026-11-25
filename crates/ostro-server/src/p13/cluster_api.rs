//! `impl ClusterApi` (D-P13-24): pure delegation to the hook that owns each operation.
//! Final after 13-02.
//!
//! | method | hook (owner) |
//! |--------|--------------|
//! | `init` | [`super::promote::init`] (13-16) |
//! | `status` | [`super::status::status`] (13-09) |
//! | `join`, `create_node_token` | [`super::join`] (13-09) |
//! | `remove`, `drain` | [`super::membership`] (13-09) |
//! | `reassign`, `rebalance` | `ostro_cluster::placement::controller::{request_reassign, request_rebalance}` (13-10), then the status |
//! | `upgrade_finalize/hold/release` | `ostro_cluster::upgrade::{finalize, hold, release}` (13-04), then the upgrade status |
//! | `upgrade_status` | `ostro_cluster::upgrade::upgrade_view` (13-04) over the local meta view |

use std::sync::Arc;

use ostro_api::ApiError;
use ostro_api::phase13::MemberLevel;
use ostro_api::phase13::{
    ClusterApi, ClusterStatus, InitRequest, JoinRequest, JoinResponse, NodeTokenRequest,
    NodeTokenResponse, ReassignRequest, UpgradeStatus,
};
use ostro_cluster::placement::controller::{self, PlacementError};
use ostro_cluster::upgrade::{self, UpgradeError};
use ostro_meta::ext::MEMBER_REMOVED;

use super::{P13Ctx, lands_in};

/// The cluster operations of this node.
#[derive(Debug)]
pub struct P13ClusterApi {
    ctx: Arc<P13Ctx>,
}

impl P13ClusterApi {
    /// Over `ctx`.
    pub fn new(ctx: Arc<P13Ctx>) -> Self {
        Self { ctx }
    }
}

fn upgrade_error(method: &str, e: UpgradeError) -> ApiError {
    match e {
        UpgradeError::NotImplemented => lands_in(method, "13-04"),
        UpgradeError::NotReady(m) => ApiError::precondition_failed(m),
        UpgradeError::Meta(m) => ApiError::unavailable(m),
    }
}

fn placement_error(method: &str, e: PlacementError) -> ApiError {
    match e {
        PlacementError::NotImplemented => lands_in(method, "13-10"),
        PlacementError::Invalid(m) => ApiError::invalid_argument(m),
        // 409, as every gate (13-18, D-P13-17).
        PlacementError::FeatureNotEnabled(f) => crate::control::module_retire::feature_error(f),
        PlacementError::Conflict(m) => ApiError::conflict(m),
        PlacementError::Meta(m) => ApiError::unavailable(m),
    }
}

/// `GET /v1/cluster/upgrade` from the local meta view through 13-04's `upgrade_view`
/// (absent ClusterVersion = `LEVEL_MIN`; a member without NodeVersion counts as
/// `[LEVEL_MIN, LEVEL_MIN]`), Removed members left out.
pub(crate) fn upgrade_status_of(ctx: &P13Ctx) -> UpgradeStatus {
    let universe = ctx
        .meta
        .view()
        .read(|s| s.members().map(|m| m.value.node_id).collect());
    let live = ctx.cluster.liveness().lock().live_set(&universe);
    let view = ctx.meta.view().read(|s| upgrade::upgrade_view(s, &live));
    UpgradeStatus {
        finalized: view.finalized,
        hold: view.hold,
        common_level_max: view.level_range.1,
        auto_finalize: ctx.cfg.cluster.auto_finalize,
        members: view
            .members
            .iter()
            .filter(|m| m.state != MEMBER_REMOVED)
            .map(|m| MemberLevel {
                node_id: m.node_id,
                level_min: m
                    .version
                    .as_ref()
                    .map_or(ostro_types::compat::LEVEL_MIN, |v| v.level_min),
                level_max: m
                    .version
                    .as_ref()
                    .map_or(ostro_types::compat::LEVEL_MIN, |v| v.level_max),
                live: m.live,
            })
            .collect(),
    }
}

async fn upgrade_status(ctx: &P13Ctx) -> Result<UpgradeStatus, ApiError> {
    Ok(upgrade_status_of(ctx))
}

async fn reassign(ctx: &P13Ctx, r: ReassignRequest) -> Result<ClusterStatus, ApiError> {
    controller::request_reassign(
        &ctx.meta,
        r.partition,
        r.replicas,
        r.cancel,
        ctx.level_max,
        ctx.clock.now_ms(),
    )
    .await
    .map_err(|e| placement_error("reassign", e))?;
    super::status::status(ctx).await
}

async fn rebalance(ctx: &P13Ctx) -> Result<ClusterStatus, ApiError> {
    controller::request_rebalance(&ctx.meta, &ctx.placement_trigger, ctx.level_max)
        .await
        .map_err(|e| placement_error("rebalance", e))?;
    super::status::status(ctx).await
}

#[async_trait::async_trait]
impl ClusterApi for P13ClusterApi {
    async fn init(&self, r: InitRequest) -> Result<ClusterStatus, ApiError> {
        super::promote::init(&self.ctx, r).await
    }

    async fn join(&self, r: JoinRequest) -> Result<JoinResponse, ApiError> {
        super::join::handle_join(&self.ctx, r).await
    }

    async fn status(&self) -> Result<ClusterStatus, ApiError> {
        super::status::status(&self.ctx).await
    }

    async fn create_node_token(&self, r: NodeTokenRequest) -> Result<NodeTokenResponse, ApiError> {
        super::join::create_node_token(&self.ctx, r).await
    }

    async fn remove(&self, id: u64, force: bool) -> Result<ClusterStatus, ApiError> {
        super::membership::request_remove(&self.ctx, id, force).await
    }

    async fn drain(&self, id: u64) -> Result<ClusterStatus, ApiError> {
        super::membership::request_drain(&self.ctx, id).await
    }

    async fn reassign(&self, r: ReassignRequest) -> Result<ClusterStatus, ApiError> {
        reassign(&self.ctx, r).await
    }

    async fn rebalance(&self) -> Result<ClusterStatus, ApiError> {
        rebalance(&self.ctx).await
    }

    async fn upgrade_status(&self) -> Result<UpgradeStatus, ApiError> {
        upgrade_status(&self.ctx).await
    }

    async fn upgrade_finalize(&self) -> Result<UpgradeStatus, ApiError> {
        upgrade::finalize(&self.ctx.meta, self.ctx.clock.now_ms())
            .await
            .map_err(|e| upgrade_error("upgrade_finalize", e))?;
        upgrade_status(&self.ctx).await
    }

    async fn upgrade_hold(&self) -> Result<UpgradeStatus, ApiError> {
        upgrade::hold(&self.ctx.meta, self.ctx.clock.now_ms())
            .await
            .map_err(|e| upgrade_error("upgrade_hold", e))?;
        upgrade_status(&self.ctx).await
    }

    async fn upgrade_release(&self) -> Result<UpgradeStatus, ApiError> {
        upgrade::release(&self.ctx.meta, self.ctx.clock.now_ms())
            .await
            .map_err(|e| upgrade_error("upgrade_release", e))?;
        upgrade_status(&self.ctx).await
    }
}
