//! `GET /v1/cluster/status` (13-09): the cluster document from the local meta view, read
//! behind the ReadIndex barrier (`MetaHandle::read_index_barrier`, so a follower answers
//! with everything committed before the request).
//!
//! Members (tombstones included) with addresses, MemberState, NodeVersion, liveness and meta
//! Raft role (`voter` / `learner` / `outgoing` / `none`); the configuration (voters,
//! learners, outgoing voters) and meta leader as this node applied them; the finalized
//! level, hold and level range (13-04's `upgrade_view`); the replication policy (ext 99);
//! in-flight reassignments (ext 100).

use std::collections::BTreeSet;

use ostro_api::ApiError;
use ostro_api::phase13::{
    ClusterMember, ClusterStatus, NodeVersionDto, ReassignmentDto, ReplicationPolicyDto,
};
use ostro_cluster::membership::service::{member_state_of, state_name};
use ostro_meta::ext::{get_node_version, get_reassignment, get_replication_policy};

use super::P13Ctx;

/// The cluster status document.
pub async fn status(ctx: &P13Ctx) -> Result<ClusterStatus, ApiError> {
    ctx.meta
        .read_index_barrier()
        .await
        .map_err(|e| ApiError::unavailable(format!("metadata read barrier: {e}")))?;
    Ok(status_now(ctx))
}

/// The document from the local view, without the barrier.
pub fn status_now(ctx: &P13Ctx) -> ClusterStatus {
    let st = ctx.meta.raft_detail();
    let upgrade = super::cluster_api::upgrade_status_of(ctx);
    let conf: BTreeSet<u64> = st
        .voters
        .iter()
        .chain(&st.learners)
        .chain(&st.voters_outgoing)
        .copied()
        .collect();
    let live = ctx.cluster.liveness().lock().live_set(&conf);
    let me = ctx.node_id();
    ctx.meta.view().read(|s| {
        let members = s
            .members()
            .map(|m| {
                let v = &m.value;
                let id = v.node_id;
                let raft_role = if st.voters.contains(&id) {
                    "voter"
                } else if st.voters_outgoing.contains(&id) {
                    "outgoing"
                } else if st.learners.contains(&id) || st.learners_next.contains(&id) {
                    "learner"
                } else {
                    "none"
                };
                ClusterMember {
                    node_id: id,
                    addr: v.addr.clone(),
                    cluster_addr: v.cluster_addr.clone(),
                    state: state_name(member_state_of(v)).to_owned(),
                    version: get_node_version(&v.ext)
                        .ok()
                        .flatten()
                        .map(|n| NodeVersionDto {
                            ostro_version: n.ostro_version,
                            level_min: n.level_min,
                            level_max: n.level_max,
                            wasmtime: n.wasmtime,
                            event_codec_ver: n.event_codec_ver,
                            wit_lines: n.wit_lines,
                        }),
                    live: id == me || live.contains(&id),
                    raft_role: raft_role.to_owned(),
                }
            })
            .collect();
        let policy = s
            .cluster()
            .and_then(|c| get_replication_policy(&c.value.ext).ok().flatten())
            .map(|p| ReplicationPolicyDto {
                rf: p.rf,
                min_insync: p.min_insync,
                max_voters: p.max_voters,
            });
        let reassignments = s
            .partitions()
            .filter_map(|p| {
                get_reassignment(&p.value.ext)
                    .ok()
                    .flatten()
                    .map(|r| ReassignmentDto {
                        partition: p.value.partition,
                        target: r.target,
                        adding: r.adding,
                        removing: r.removing,
                        started_at_ms: r.started_at_ms,
                    })
            })
            .collect();
        ClusterStatus {
            cluster_id: ctx.cluster.cluster_id().to_owned(),
            members,
            voters: st.voters.clone(),
            learners: st.learners.clone(),
            voters_outgoing: st.voters_outgoing.clone(),
            meta_leader: st.leader_id.unwrap_or(0),
            finalized: upgrade.finalized,
            hold: upgrade.hold,
            level_min: upgrade
                .members
                .iter()
                .map(|m| m.level_min)
                .min()
                .unwrap_or(ostro_types::compat::LEVEL_MIN),
            level_max: upgrade.common_level_max,
            policy,
            reassignments,
        }
    })
}
