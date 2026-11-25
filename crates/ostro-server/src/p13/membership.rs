//! Membership on the server (13-09, D-P13-8): the meta leader's driver
//! ([`ostro_cluster::membership::service::MembershipService`]) wired to this node's liveness,
//! clock and transport; the dynamic address book follower; the cluster CA secret on meta
//! leadership; remove / drain requests; the `ostro.cluster.v1.Membership` server and the
//! removed-node stop.
//!
//! - [`start`] (every node): the driver (acts only as meta leader), `AddressBook::follow`
//!   over the meta view (TCP transport only), `join::ensure_cluster_ca` whenever this node
//!   becomes meta leader, and the self-removal watch: once this node's Member record is
//!   Removed and it is no longer an incoming voter or learner, it fail-stops
//!   ([`P13Ctx::fatal`]: meta participation ends, the binary exits non-zero).
//! - [`request_drain`] sets MemberState Draining; [`request_remove`] checks the live quorum
//!   (`membership::remove::check_remove`, never skipped), then sets Draining (the driver
//!   writes Removed once the node holds no replica and leads nothing) or, with `force` on a
//!   dead node, Removed directly (its replicas are evacuated by 13-10, plan check W7). A
//!   voter cannot be drained / removed while a conf change is pending or the configuration
//!   is joint, or when no other live Active voter would remain (13-06 handover).
//! - [`service`]: `Membership.Removed` (node identities only): stops this node when the
//!   notice names it and the local view confirms it (Removed, or out of a non-empty
//!   configuration).

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use ostro_api::ApiError;
use ostro_api::phase13::ClusterStatus;
use ostro_api::proto::cluster::v1::membership_client::MembershipClient;
use ostro_api::proto::cluster::v1::membership_server::{Membership, MembershipServer};
use ostro_api::proto::cluster::v1::{RemovedAck, RemovedNotice};
use ostro_cluster::authz;
use ostro_cluster::membership::remove::check_remove;
use ostro_cluster::membership::service::{
    LiveSource, MembershipConfig, MembershipService, RemovedNotifier, StateWriteError,
    member_state_of, set_member_state,
};
use ostro_meta::ext::{MEMBER_ACTIVE, MEMBER_DRAINING, MEMBER_REMOVED};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::service::Routes;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use super::P13Ctx;

/// Which of `universe` are live: members of the meta configuration that the liveness
/// accumulator (fed by the meta leader's samples over voters ∪ learners) does not consider
/// dead. A member outside the configuration has had no contact and is not live.
fn live_source(ctx: &Arc<P13Ctx>) -> LiveSource {
    let weak = Arc::downgrade(ctx);
    Arc::new(move |universe: &BTreeSet<u64>| {
        let Some(ctx) = weak.upgrade() else {
            return BTreeSet::new();
        };
        let st = ctx.meta.raft_detail();
        let conf: BTreeSet<u64> = universe
            .iter()
            .copied()
            .filter(|id| {
                st.voters.contains(id)
                    || st.learners.contains(id)
                    || st.voters_outgoing.contains(id)
                    || st.learners_next.contains(id)
            })
            .collect();
        ctx.cluster.liveness().lock().live_set(&conf)
    })
}

fn removed_notifier(ctx: &Arc<P13Ctx>) -> RemovedNotifier {
    let weak = Arc::downgrade(ctx);
    Arc::new(move |id: u64| {
        let weak = weak.clone();
        Box::pin(async move {
            let Some(ctx) = weak.upgrade() else {
                return;
            };
            let Ok(ch) = ctx.cluster.transport().channel(id) else {
                return;
            };
            let notice = RemovedNotice {
                node_id: id,
                cluster_id: ctx.cluster.cluster_id().to_owned(),
                reason: "removed from the cluster".to_owned(),
            };
            let mut client = MembershipClient::new(ch);
            let call = client.removed(notice);
            match tokio::time::timeout(Duration::from_secs(2), call).await {
                Ok(Ok(_)) => info!(node = id, "removed node acknowledged its removal"),
                Ok(Err(e)) => info!(node = id, error = %e, "removed-node notice not delivered"),
                Err(_) => info!(node = id, "removed-node notice timed out"),
            }
        })
    })
}

/// This node is Removed in the local view and no longer an incoming voter / learner.
fn self_removed(ctx: &P13Ctx) -> bool {
    let me = ctx.node_id();
    let removed = ctx.meta.view().read(|s| {
        s.member(me)
            .is_some_and(|m| member_state_of(&m.value) == MEMBER_REMOVED)
    });
    if !removed {
        return false;
    }
    let st = ctx.meta.raft_detail();
    !(st.voters.contains(&me) || st.learners.contains(&me) || st.learners_next.contains(&me))
}

/// Start the membership tasks (every node; the driver acts as meta leader only).
pub fn start(ctx: &Arc<P13Ctx>, stop: CancellationToken) -> Vec<JoinHandle<()>> {
    let c = &ctx.cfg.cluster;
    let mut tasks = Vec::new();
    let svc = MembershipService::new(
        Arc::clone(&ctx.meta),
        MembershipConfig {
            promote_lag: c.promote_lag,
            join_orphan_timeout: c.join_orphan_timeout.get(),
            ..MembershipConfig::default()
        },
    )
    .with_clock(Arc::clone(&ctx.clock))
    .with_live_source(live_source(ctx))
    .with_removed_notifier(removed_notifier(ctx));
    tasks.push(svc.start(stop.clone()));
    if let Some(book) = ctx.cluster.address_book() {
        tasks.push(book.follow(ctx.meta.view().clone(), Some(ctx.node_id()), stop.clone()));
    }
    // The cluster CA secret on meta leadership; the self-removal watch.
    let weak = Arc::downgrade(ctx);
    let meta = Arc::clone(&ctx.meta);
    tasks.push(tokio::spawn(async move {
        let mut raft = meta.raft().subscribe();
        let mut applied = meta.view().subscribe();
        let mut was_leader = false;
        loop {
            let Some(ctx) = weak.upgrade() else {
                return;
            };
            let leader = ctx.meta.is_leader();
            if leader && !was_leader {
                if let Err(e) = super::join::ensure_cluster_ca(&ctx).await {
                    warn!(error = %e, "storing the cluster CA failed; retried on the next change");
                } else {
                    was_leader = true;
                }
            } else if !leader {
                was_leader = false;
            }
            if self_removed(&ctx) {
                ctx.fatal(format!(
                    "node {} was removed from the cluster; stopping (its id is never reused)",
                    ctx.node_id()
                ));
                return;
            }
            drop(ctx);
            tokio::select! {
                _ = stop.cancelled() => return,
                r = raft.changed() => if r.is_err() { return },
                r = applied.changed() => if r.is_err() { return },
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }));
    tasks
}

/// The `ostro.cluster.v1.Membership` server.
#[derive(Clone)]
struct MembershipSvc {
    ctx: std::sync::Weak<P13Ctx>,
}

#[tonic::async_trait]
impl Membership for MembershipSvc {
    async fn removed(&self, req: Request<RemovedNotice>) -> Result<Response<RemovedAck>, Status> {
        let from = authz::require_node(&req)?;
        let n = req.into_inner();
        let ctx = self
            .ctx
            .upgrade()
            .ok_or_else(|| Status::unavailable("shutting down"))?;
        if n.node_id != ctx.node_id() || n.cluster_id != ctx.cluster.cluster_id() {
            return Err(Status::invalid_argument("the notice names another node"));
        }
        info!(from, reason = %n.reason, "Membership.Removed received");
        if self_removed(&ctx) {
            ctx.fatal(format!(
                "node {} was removed from the cluster (notified by node {from}); stopping",
                ctx.node_id()
            ));
        }
        Ok(Response::new(RemovedAck {}))
    }
}

/// The `ostro.cluster.v1.Membership` service on the cluster listener.
pub fn service(ctx: &Arc<P13Ctx>) -> Option<Routes> {
    Some(Routes::new(MembershipServer::new(MembershipSvc {
        ctx: Arc::downgrade(ctx),
    })))
}

fn state_error(e: StateWriteError) -> ApiError {
    match e {
        StateWriteError::NotMember(id) => ApiError::not_found(format!("node {id} is not a member")),
        StateWriteError::Removed(id) => {
            ApiError::precondition_failed(format!("node {id} was removed (tombstone)"))
        }
        StateWriteError::Meta(e) => ApiError::unavailable(e.to_string()),
    }
}

/// The 13-06 guards for draining / removing a voter.
fn voter_guards(ctx: &P13Ctx, id: u64, live: &BTreeSet<u64>) -> Result<(), ApiError> {
    let st = ctx.meta.raft_detail();
    let is_voter = st.voters.contains(&id) || st.voters_outgoing.contains(&id);
    if !is_voter {
        return Ok(());
    }
    if st.pending_conf || !st.voters_outgoing.is_empty() {
        return Err(ApiError::unavailable(
            "a metadata membership change is in progress; retry in a moment",
        ));
    }
    let others_active_live = ctx.meta.view().read(|s| {
        st.voters
            .iter()
            .filter(|v| **v != id && live.contains(v))
            .any(|v| {
                s.member(*v)
                    .is_some_and(|m| member_state_of(&m.value) == MEMBER_ACTIVE)
            })
    });
    if !others_active_live {
        return Err(ApiError::precondition_failed(format!(
            "node {id} is the only live active metadata voter; add a node first"
        )));
    }
    Ok(())
}

fn member_state(ctx: &P13Ctx, id: u64) -> Result<u8, ApiError> {
    ctx.meta
        .view()
        .read(|s| s.member(id).map(|m| member_state_of(&m.value)))
        .ok_or_else(|| ApiError::not_found(format!("node {id} is not a member")))
}

fn live_inline(ctx: &P13Ctx) -> BTreeSet<u64> {
    let st = ctx.meta.raft_detail();
    let conf: BTreeSet<u64> = st
        .voters
        .iter()
        .chain(&st.learners)
        .chain(&st.voters_outgoing)
        .chain(&st.learners_next)
        .copied()
        .collect();
    let mut live = ctx.cluster.liveness().lock().live_set(&conf);
    live.insert(ctx.node_id());
    live
}

/// `POST /v1/cluster/members/{id}/drain`: MemberState Draining.
pub async fn request_drain(ctx: &P13Ctx, id: u64) -> Result<ClusterStatus, ApiError> {
    match member_state(ctx, id)? {
        MEMBER_REMOVED => {
            return Err(ApiError::precondition_failed(format!(
                "node {id} was removed (tombstone)"
            )));
        }
        MEMBER_DRAINING => return super::status::status(ctx).await,
        _ => {}
    }
    voter_guards(ctx, id, &live_inline(ctx))?;
    set_member_state(&ctx.meta, id, MEMBER_DRAINING, ctx.clock.now_ms())
        .await
        .map_err(state_error)?;
    info!(node = id, "drain requested");
    super::status::status(ctx).await
}

/// `POST /v1/cluster/members/{id}/remove`: quorum-checked; Draining, or Removed directly with
/// `force` on a dead node.
pub async fn request_remove(ctx: &P13Ctx, id: u64, force: bool) -> Result<ClusterStatus, ApiError> {
    if member_state(ctx, id)? == MEMBER_REMOVED {
        return super::status::status(ctx).await;
    }
    let live = live_inline(ctx);
    voter_guards(ctx, id, &live)?;
    let st = ctx.meta.raft_detail();
    let voters: Vec<u64> = st
        .voters
        .iter()
        .chain(&st.voters_outgoing)
        .copied()
        .collect::<BTreeSet<u64>>()
        .into_iter()
        .collect();
    let live_v: Vec<u64> = live.iter().copied().collect();
    check_remove(id, &voters, &live_v, force)
        .map_err(|e| ApiError::precondition_failed(e.to_string()))?;
    let now = ctx.clock.now_ms();
    let dead = !live.contains(&id);
    if force && dead {
        warn!(
            node = id,
            "force-removing a dead node (its replicas are re-placed later)"
        );
        set_member_state(&ctx.meta, id, MEMBER_REMOVED, now)
            .await
            .map_err(state_error)?;
    } else {
        set_member_state(&ctx.meta, id, MEMBER_DRAINING, now)
            .await
            .map_err(state_error)?;
        info!(node = id, force, "removal requested (draining first)");
    }
    super::status::status(ctx).await
}
