//! Phase 13: implemented in 13-09 (membership: the meta leader's reconcile loop over
//! `ostro_cluster::membership::MembershipService`, remove / drain requests, the
//! `ostro.cluster.v1.Membership` server and the removed-node handler; D-P13-8).
//!
//! Fixed signatures (called by [`super::start`], [`super::cluster_listener_services`] and
//! [`super::cluster_api`]). Stubs: the 13-01 loop (idle), no listener service, operator
//! calls answer `Unimplemented`.

use std::sync::Arc;

use ostro_api::ApiError;
use ostro_api::phase13::ClusterStatus;
use ostro_cluster::membership::service::{MembershipConfig, MembershipService};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::service::Routes;

use super::{P13Ctx, lands_in};

/// Start the membership tasks (every node; acts as meta leader).
pub fn start(ctx: &Arc<P13Ctx>, stop: CancellationToken) -> Vec<JoinHandle<()>> {
    let c = &ctx.cfg.cluster;
    let svc = MembershipService::new(
        Arc::clone(&ctx.meta),
        MembershipConfig {
            promote_lag: c.promote_lag,
            join_orphan_timeout: c.join_orphan_timeout.get(),
            ..MembershipConfig::default()
        },
    );
    vec![svc.start(stop)]
}

/// The `ostro.cluster.v1.Membership` service on the cluster listener. Stub: none (the proto
/// lands in 13-03).
pub fn service(ctx: &Arc<P13Ctx>) -> Option<Routes> {
    let _ = ctx;
    None
}

/// `POST /v1/cluster/members/{id}/remove`. Stub.
pub async fn request_remove(ctx: &P13Ctx, id: u64, force: bool) -> Result<ClusterStatus, ApiError> {
    let _ = (ctx, id, force);
    Err(lands_in("remove", "13-09"))
}

/// `POST /v1/cluster/members/{id}/drain`. Stub.
pub async fn request_drain(ctx: &P13Ctx, id: u64) -> Result<ClusterStatus, ApiError> {
    let _ = (ctx, id);
    Err(lands_in("drain", "13-09"))
}
