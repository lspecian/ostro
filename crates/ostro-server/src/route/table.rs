//! Route classes per API method (D-P13-14, the table recorded in 13-01-SUMMARY), owner
//! resolution and the partition of a call (13-12).
//!
//! Fixed signature: [`pinned_partition`], read by the start path (`control/runs.rs`).
//!
//! | class | where the call runs |
//! |-------|---------------------|
//! | [`RouteClass::RunPartition`] | the leader of the run id's partition |
//! | [`RouteClass::KeyPartition`] | the leader of the key's partition (keyed start, schedule owner); `start_run` without a key is [`RouteClass::UnkeyedStart`] |
//! | [`RouteClass::TokenPartition`] | the leader of the await token's partition (`ot1_` + BE partition) |
//! | [`RouteClass::UnkeyedStart`] | a locally led partition (zero hops), else a uniform partition forwarded with `ostro-partition` |
//! | [`RouteClass::FanoutPartitions`] | one call per leader node with `ostro-partitions`, merged |
//! | [`RouteClass::FanoutNodes`] | node-local registries: merged across members / first hit wins |
//! | [`RouteClass::MetaRead`] | this node, behind the meta read hook |
//! | [`RouteClass::MetaWrite`] | this node (meta proposals forward to the meta leader), token after the write |
//! | [`RouteClass::MetaLeader`] | the meta leader (`upgrade_finalize`, `run_module_gc`) |
//! | [`RouteClass::Local`] | this node (uploads, presign, dev tooling, worker join, backup, limits, hook delivery whose start is routed) |
//!
//! A forwarded-in call (`RequestMeta.hop >= 1`) always runs locally: one hop at most.

use ostro_api::request_meta::RequestMeta;
use ostro_partition::roles::PartitionAssignmentView;
use ostro_types::{Epoch, PartitionCount, PartitionId, RunId};

/// How a call is routed (D-P13-14).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RouteClass {
    /// Partition of a run id.
    RunPartition,
    /// Partition of an idempotency key (keyed start) or a schedule's owner partition.
    KeyPartition,
    /// Partition of an await token.
    TokenPartition,
    /// A start without an idempotency key.
    UnkeyedStart,
    /// One call per partition leader node, merged.
    FanoutPartitions,
    /// Node-local registries, merged across members.
    FanoutNodes,
    /// A metadata read on this node.
    MetaRead,
    /// A metadata write from this node.
    MetaWrite,
    /// Runs on the meta leader.
    MetaLeader,
    /// Runs on this node.
    Local,
}

impl RouteClass {
    /// The metric label.
    pub fn as_str(self) -> &'static str {
        match self {
            RouteClass::RunPartition => "run_partition",
            RouteClass::KeyPartition => "key_partition",
            RouteClass::TokenPartition => "token_partition",
            RouteClass::UnkeyedStart => "unkeyed_start",
            RouteClass::FanoutPartitions => "fanout_partitions",
            RouteClass::FanoutNodes => "fanout_nodes",
            RouteClass::MetaRead => "meta_read",
            RouteClass::MetaWrite => "meta_write",
            RouteClass::MetaLeader => "meta_leader",
            RouteClass::Local => "local",
        }
    }
}

use RouteClass::{
    FanoutNodes as FN, FanoutPartitions as FP, KeyPartition as KP, Local as LO, MetaLeader as ML,
    MetaRead as MR, MetaWrite as MW, RunPartition as RP, TokenPartition as TP,
};

/// Every routed trait method: `(trait, method, class)`. The `every_trait_method_classified`
/// test parses the trait sources and fails on a method missing here.
pub const ROUTES: &[(&str, &str, RouteClass)] = &[
    // ControlPlane (ostro_api::domain).
    ("ControlPlane", "upload_module", LO),
    ("ControlPlane", "get_module", MR),
    ("ControlPlane", "list_workflows", MR),
    ("ControlPlane", "activate_workflow", MW),
    ("ControlPlane", "start_run", KP),
    ("ControlPlane", "get_run", RP),
    ("ControlPlane", "run_history", RP),
    ("ControlPlane", "list_runs", FP),
    ("ControlPlane", "ready", LO),
    ("ControlPlane", "signal_run", RP),
    ("ControlPlane", "cancel_run", RP),
    ("ControlPlane", "terminate_run", RP),
    ("ControlPlane", "retry_run", RP),
    ("ControlPlane", "await_result", RP),
    ("ControlPlane", "complete_token", TP),
    ("ControlPlane", "set_workflow_policy", MW),
    ("ControlPlane", "get_workflow_policy", MR),
    ("ControlPlane", "set_queue_policy", MW),
    ("ControlPlane", "get_queue_policy", MR),
    ("ControlPlane", "put_secret", MW),
    ("ControlPlane", "list_secrets", MR),
    ("ControlPlane", "delete_secret", MW),
    ("ControlPlane", "put_grant", MW),
    ("ControlPlane", "list_grants", MR),
    ("ControlPlane", "delete_grant", MW),
    ("ControlPlane", "put_connection", MW),
    ("ControlPlane", "get_connection", MR),
    ("ControlPlane", "list_connections", MR),
    ("ControlPlane", "delete_connection", MW),
    ("ControlPlane", "put_egress", MW),
    ("ControlPlane", "get_egress", MR),
    ("ControlPlane", "list_egress", MR),
    ("ControlPlane", "delete_egress", MW),
    ("ControlPlane", "presigned_object", LO),
    ("ControlPlane", "get_queue_flow", MR),
    ("ControlPlane", "put_queue_flow", MW),
    ("ControlPlane", "pause_queue", MW),
    ("ControlPlane", "resume_queue", MW),
    ("ControlPlane", "pause_workflow", MW),
    ("ControlPlane", "resume_workflow", MW),
    ("ControlPlane", "pause_run", RP),
    ("ControlPlane", "resume_run", RP),
    ("ControlPlane", "queue_demand", FP),
    ("ControlPlane", "list_workers", FN),
    ("ControlPlane", "drain_worker", FN),
    ("ControlPlane", "create_worker_token", MW),
    ("ControlPlane", "list_worker_tokens", MR),
    ("ControlPlane", "revoke_worker_token", MW),
    ("ControlPlane", "join_worker", LO),
    ("ControlPlane", "dev_time", LO),
    ("ControlPlane", "dev_skip", LO),
    ("ControlPlane", "replay_module", LO),
    // Phase 8 (ostro_api::phase8).
    ("ScheduleApi", "list_schedules", MR),
    ("ScheduleApi", "get_schedule", MR),
    ("ScheduleApi", "create_schedule", MW),
    ("ScheduleApi", "update_schedule", MW),
    ("ScheduleApi", "delete_schedule", MW),
    ("ScheduleApi", "pause_schedule", MW),
    ("ScheduleApi", "resume_schedule", MW),
    ("ScheduleApi", "trigger_schedule", KP),
    ("WebhookApi", "list_webhooks", MR),
    ("WebhookApi", "get_webhook", MR),
    ("WebhookApi", "create_webhook", MW),
    ("WebhookApi", "update_webhook", MW),
    ("WebhookApi", "delete_webhook", MW),
    // The delivery's start / signal / token completion go through the routed control
    // plane (13-02 switched `control/webhooks.rs` to it), so the start lands on its
    // key's (or an unkeyed) partition exactly as `partition(key) / unkeyed_start` says.
    ("HookIngress", "deliver", LO),
    ("RunLogsApi", "run_logs", RP),
    ("AdminApi", "backup", LO),
    // Phase 11 (ostro_api::phase11).
    ("OpsApi", "rerun_run", RP),
    ("OpsApi", "analyze_rerun", RP),
    ("OpsApi", "list_dlq", FP),
    ("OpsApi", "requeue_dlq", RP),
    ("OpsApi", "dismiss_dlq", RP),
    ("OpsApi", "create_bulk", LO),
    ("OpsApi", "list_bulk", FN),
    ("OpsApi", "get_bulk", FN),
    ("OpsApi", "cancel_bulk", FN),
    ("OpsApi", "limits", LO),
    ("OpsApi", "get_failure_policy", MR),
    ("OpsApi", "set_failure_policy", MW),
    ("InventoryApi", "list_modules", MR),
    ("InventoryApi", "list_queues", MR),
    ("InventoryApi", "list_partitions", FN),
    // Phase 13 (ostro_api::phase13).
    ("ClusterApi", "init", MW),
    ("ClusterApi", "join", MW),
    ("ClusterApi", "status", MR),
    ("ClusterApi", "create_node_token", MW),
    ("ClusterApi", "remove", MW),
    ("ClusterApi", "drain", MW),
    ("ClusterApi", "reassign", MW),
    ("ClusterApi", "rebalance", MW),
    ("ClusterApi", "upgrade_status", MR),
    ("ClusterApi", "upgrade_finalize", ML),
    ("ClusterApi", "upgrade_hold", MW),
    ("ClusterApi", "upgrade_release", MW),
    ("ModuleLifecycleApi", "retire_module", MW),
    ("ModuleLifecycleApi", "run_module_gc", ML),
    ("ModuleLifecycleApi", "migrate_runs", FP),
];

/// The class of `trait_name::method` (`None`: not in [`ROUTES`]).
pub fn class_of(trait_name: &str, method: &str) -> Option<RouteClass> {
    ROUTES
        .iter()
        .find(|(t, m, _)| *t == trait_name && *m == method)
        .map(|(_, _, c)| *c)
}

/// The class a routed method uses; a method missing from [`ROUTES`] is a programming error
/// caught by the classification test, and routes locally here.
pub(crate) fn class(trait_name: &str, method: &str) -> RouteClass {
    class_of(trait_name, method).unwrap_or(RouteClass::Local)
}

/// Where a partition-scoped call goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    /// This node leads (or is taking over) the partition.
    Local,
    /// Another node leads it.
    Node(u64),
    /// No leader (offline: no eligible in-sync replica).
    Offline(PartitionId),
}

impl Target {
    /// The metric label.
    pub fn as_str(self) -> &'static str {
        match self {
            Target::Local => "local",
            Target::Node(_) => "forwarded",
            Target::Offline(_) => "offline",
        }
    }
}

/// The owner of partition `p` as this node sees it, with the assignment epoch it routed on:
/// led here (the led set) → [`Target::Local`]; the published leader is this node (taking
/// over) → `Local` (the local call answers `not_leader` until the takeover completes, and
/// the router waits and re-routes); leader 0 → [`Target::Offline`]; another leader →
/// [`Target::Node`]. A partition without an assignment row routes locally (the local call
/// answers).
pub fn owner(
    p: PartitionId,
    local: u64,
    led: &[(PartitionId, Epoch)],
    view: &[PartitionAssignmentView],
) -> (Target, Epoch) {
    let row = view.iter().find(|a| a.partition == p);
    if let Some(t) = stale_view(local, row) {
        return (t, row.map(|r| r.epoch).unwrap_or(Epoch(0)));
    }
    if let Some((_, e)) = led.iter().find(|(q, _)| *q == p) {
        return (Target::Local, *e);
    }
    let Some(row) = row else {
        return (Target::Local, Epoch(0));
    };
    let t = match row.leader {
        0 => Target::Offline(p),
        l if l == local => Target::Local,
        l => Target::Node(l),
    };
    (t, row.epoch)
}

/// Failpoint `route::stale_view` (tier-B `hop_limit_no_loop`): every node believes another
/// replica (not itself, not the real leader) leads the partition, so two nodes forward to
/// each other. Without the `failpoints` feature: never.
#[allow(unused_variables)]
fn stale_view(local: u64, row: Option<&PartitionAssignmentView>) -> Option<Target> {
    #[cfg(feature = "failpoints")]
    {
        let on = (|| {
            fail::fail_point!("route::stale_view", |_| true);
            false
        })();
        if on {
            let row = row?;
            let other = row
                .replicas
                .iter()
                .chain(row.isr.iter())
                .copied()
                .find(|n| *n != local && *n != row.leader && *n != 0)
                .or_else(|| {
                    row.replicas
                        .iter()
                        .copied()
                        .find(|n| *n != local && *n != 0)
                })?;
            return Some(Target::Node(other));
        }
    }
    None
}

/// The partition a forwarded unkeyed start is pinned to (`ostro-partition`, honoured only
/// from node identities: `RequestMeta::from_node` is set by the cluster listener, or by the
/// router itself for a locally placed start). A client-sent `ostro-partition` header is
/// ignored.
pub fn pinned_partition() -> Option<PartitionId> {
    let meta = RequestMeta::current()?;
    meta.from_node?;
    meta.partition_pin.map(PartitionId)
}

/// The partitions a listing on this node scans: the `ostro-partitions` of a fan-out leg
/// (honoured only from node identities), else every partition.
pub fn listed_partitions(count: PartitionCount) -> Vec<PartitionId> {
    let leg = RequestMeta::current()
        .filter(|m| m.from_node.is_some())
        .and_then(|m| m.partitions);
    match leg {
        Some(mut ps) => {
            ps.retain(|p| *p < count.get());
            ps.sort_unstable();
            ps.dedup();
            ps.into_iter().map(PartitionId).collect()
        }
        None => count.ids().collect(),
    }
}

/// The partition of a run id (`None`: malformed or outside the count; the local call then
/// answers 400 / 404).
pub fn run_partition(run_id: &str, count: PartitionCount) -> Option<PartitionId> {
    let run: RunId = run_id.parse().ok()?;
    count.contains(run.partition()).then(|| run.partition())
}

/// The partition of an await token (`None`: malformed; the local call answers).
pub fn token_partition(token: &str, count: PartitionCount) -> Option<PartitionId> {
    let parts = ostro_engine::token::parse(token).ok()?;
    let p = PartitionId(parts.partition);
    count.contains(p).then_some(p)
}

/// The partition of a keyed start: `partition_for_key(qualified workflow, key, P)`, the
/// D23-qualified workflow exactly as the start path computes it (`None`: invalid names; the
/// local call answers 400).
pub fn key_partition(
    workflow: &str,
    tenant: Option<&str>,
    key: &str,
    count: PartitionCount,
) -> Option<PartitionId> {
    let tenant = crate::control::names::request_tenant(tenant).ok()?;
    let wf = crate::control::names::qualify_request_name("workflow", tenant, workflow).ok()?;
    Some(ostro_types::routing::partition_for_key(
        &wf,
        key.as_bytes(),
        count,
    ))
}
