//! Phase 13 API contract (13-01; D-P13-24): cluster membership / upgrades ([`ClusterApi`]) and
//! module lifecycle ([`ModuleLifecycleApi`]: retire, GC, migrate-runs).
//!
//! Like Phases 8 and 11, the operations live in extension traits held in optional slots of
//! [`crate::ApiExtensions`] (`cluster`, `module_lifecycle`), so [`crate::ControlPlane`] is
//! untouched; an absent slot answers 404 once the routes land (13-03). The traits take and
//! return wire DTOs directly (serde + utoipa derives). Every method has a default body
//! answering `Unimplemented` and naming the plan that implements it.
//!
//! # Routes (13-03; RESERVATIONS §11 P13)
//!
//! | Method | Path | Trait method |
//! |---|---|---|
//! | POST | `/v1/cluster/init` | [`ClusterApi::init`] (13-16) |
//! | POST | `/v1/cluster/join` | [`ClusterApi::join`] (13-09; client listener, token auth) |
//! | GET | `/v1/cluster/status` | [`ClusterApi::status`] (13-09) |
//! | POST | `/v1/cluster/tokens` | [`ClusterApi::create_node_token`] (13-09) |
//! | POST | `/v1/cluster/members/{id}/remove` | [`ClusterApi::remove`] (13-09) |
//! | POST | `/v1/cluster/members/{id}/drain` | [`ClusterApi::drain`] (13-11) |
//! | POST | `/v1/cluster/reassign`, `/v1/cluster/rebalance` | [`ClusterApi::reassign`] / [`ClusterApi::rebalance`] (13-10) |
//! | GET | `/v1/cluster/upgrade` | [`ClusterApi::upgrade_status`] (13-04) |
//! | POST | `/v1/cluster/upgrade/{finalize,hold,release}` | [`ClusterApi::upgrade_finalize`] / `upgrade_hold` / `upgrade_release` (13-04) |
//! | DELETE | `/v1/modules/{sha}` | [`ModuleLifecycleApi::retire_module`] (13-15) |
//! | POST | `/v1/modules/gc` | [`ModuleLifecycleApi::run_module_gc`] (13-15) |
//! | POST | `/v1/runs/migrate` | [`ModuleLifecycleApi::migrate_runs`] (13-17) |

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::{ApiError, ErrorCode};

fn lands_in(method: &str, plan: &str) -> ApiError {
    ApiError::new(
        ErrorCode::Unimplemented,
        format!("{method} lands in {plan}"),
    )
    .with_details(serde_json::json!({ "method": method }))
}

// ---------------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------------

/// `POST /v1/cluster/init`: promote a running single node to a cluster (D-P13-11).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InitRequest {
    /// Cluster listener address peers dial (`[cluster] advertise` when absent).
    #[serde(default)]
    pub advertise: Option<String>,
    /// Target replication factor (default 3).
    #[serde(default)]
    pub rf: Option<u8>,
    /// Target min_insync (default 2).
    #[serde(default)]
    pub min_insync: Option<u8>,
}

/// The addresses a joining node advertises.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Advertise {
    /// Client API address.
    pub api: String,
    /// Cluster listener address.
    pub cluster: String,
    /// Separate replication listener, if any.
    #[serde(default)]
    pub repl: Option<String>,
}

/// What a node's binary supports (wire form of meta ext 96).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct NodeVersionDto {
    /// Ostro version.
    pub ostro_version: String,
    /// Lowest supported feature level.
    pub level_min: u32,
    /// Highest emitted feature level.
    pub level_max: u32,
    /// Wasmtime version.
    pub wasmtime: String,
    /// Event codec version.
    pub event_codec_ver: u32,
    /// WIT lines linked by the engine.
    pub wit_lines: Vec<String>,
}

/// `POST /v1/cluster/join` (D-P13-9).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct JoinRequest {
    /// Node join token (`njt_…`).
    pub token: String,
    /// PEM CSR for the node certificate.
    pub csr_pem: String,
    /// Advertised addresses.
    pub advertise: Advertise,
    /// The joiner's binary.
    pub node_version: NodeVersionDto,
    /// Resume a still-Joining id from the join marker.
    #[serde(default)]
    pub resume_node_id: Option<u64>,
}

/// One seed in a [`JoinResponse`] (written to `<data>/cluster-seeds.toml`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SeedDto {
    /// Node id.
    pub id: u64,
    /// Cluster listener address.
    pub addr: String,
    /// Separate replication listener, if any.
    #[serde(default)]
    pub repl_addr: Option<String>,
}

/// Answer of a join.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct JoinResponse {
    /// The allocated (or resumed) node id.
    pub node_id: u64,
    /// The cluster id.
    pub cluster_id: String,
    /// Partition count.
    pub partitions: u16,
    /// Cluster CA certificate (PEM).
    pub ca_cert_pem: String,
    /// Signed node certificate (PEM).
    pub node_cert_pem: String,
    /// Seeds (current members' cluster addresses).
    pub seeds: Vec<SeedDto>,
    /// The cluster's finalized feature level.
    pub finalized_level: u32,
}

/// `POST /v1/cluster/tokens`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct NodeTokenRequest {
    /// Token lifetime in seconds (server default when absent).
    #[serde(default)]
    pub ttl_secs: Option<u64>,
}

/// A created node join token (shown once).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct NodeTokenResponse {
    /// Token id (`njt_…`).
    pub id: String,
    /// The secret token.
    pub token: String,
    /// Expiry (Unix ms).
    pub expires_at_ms: u64,
    /// `sha256:<hex>` of the cluster CA (the joiner's pin).
    pub ca_hash: String,
}

/// One member in [`ClusterStatus`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ClusterMember {
    /// Node id.
    pub node_id: u64,
    /// Client API address.
    pub addr: String,
    /// Cluster listener address.
    #[serde(default)]
    pub cluster_addr: Option<String>,
    /// `joining`, `active`, `draining` or `removed`.
    pub state: String,
    /// Reported binary, if any.
    #[serde(default)]
    pub version: Option<NodeVersionDto>,
    /// Live per the meta leader's liveness view.
    pub live: bool,
    /// Meta Raft role: `voter`, `learner`, `outgoing` or `none`.
    pub raft_role: String,
}

/// Replication policy (wire form of meta ext 99).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ReplicationPolicyDto {
    /// Target replication factor.
    pub rf: u8,
    /// Target min_insync.
    pub min_insync: u8,
    /// Maximum meta voters.
    pub max_voters: u8,
}

/// One in-flight reassignment (wire form of meta ext 100).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ReassignmentDto {
    /// Partition.
    pub partition: u16,
    /// Target replica set.
    pub target: Vec<u64>,
    /// Replicas being added.
    pub adding: Vec<u64>,
    /// Replicas being removed.
    pub removing: Vec<u64>,
    /// Start time (Unix ms).
    pub started_at_ms: u64,
}

/// `GET /v1/cluster/status`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ClusterStatus {
    /// Cluster id.
    pub cluster_id: String,
    /// Every member (tombstones included).
    pub members: Vec<ClusterMember>,
    /// Meta voters.
    pub voters: Vec<u64>,
    /// Meta learners.
    pub learners: Vec<u64>,
    /// Outgoing voters (non-empty while joint).
    pub voters_outgoing: Vec<u64>,
    /// Meta leader (0 = unknown).
    pub meta_leader: u64,
    /// Finalized feature level.
    pub finalized: u32,
    /// Operator hold.
    #[serde(default)]
    pub hold: Option<u32>,
    /// Lowest `level_min` over members.
    pub level_min: u32,
    /// Lowest `level_max` over members.
    pub level_max: u32,
    /// Replication policy.
    #[serde(default)]
    pub policy: Option<ReplicationPolicyDto>,
    /// In-flight reassignments.
    pub reassignments: Vec<ReassignmentDto>,
}

/// `POST /v1/cluster/reassign`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ReassignRequest {
    /// Partition.
    pub partition: u16,
    /// Target replica set (ignored when `cancel`).
    #[serde(default)]
    pub replicas: Vec<u64>,
    /// Cancel an in-flight reassignment (allowed before LeaderMove).
    #[serde(default)]
    pub cancel: bool,
}

/// One member's supported levels in [`UpgradeStatus`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct MemberLevel {
    /// Node id.
    pub node_id: u64,
    /// Lowest supported level.
    pub level_min: u32,
    /// Highest emitted level.
    pub level_max: u32,
    /// Live.
    pub live: bool,
}

/// `GET /v1/cluster/upgrade`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UpgradeStatus {
    /// Finalized feature level.
    pub finalized: u32,
    /// Operator hold.
    #[serde(default)]
    pub hold: Option<u32>,
    /// The highest level every member supports.
    pub common_level_max: u32,
    /// `[cluster] auto_finalize`.
    pub auto_finalize: bool,
    /// Per-member levels.
    pub members: Vec<MemberLevel>,
}

/// The source of a migration: a workflow version or a module sha.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum VersionOrSha {
    /// A workflow version number.
    Version(u32),
    /// A module sha256 (hex).
    Sha(String),
}

/// `POST /v1/runs/migrate` (D-P13-21).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct MigrateRunsRequest {
    /// Workflow name.
    pub workflow: String,
    /// Source version or module.
    pub from: VersionOrSha,
    /// Target workflow version.
    pub to_version: u32,
    /// Optional run filter (tag expression, as `ostro ls --tag`).
    #[serde(default)]
    pub filter: Option<String>,
    /// Maximum runs to process.
    #[serde(default)]
    pub limit: Option<u32>,
    /// Replay-check only; submit no `RepinRun`.
    #[serde(default)]
    pub dry_run: bool,
}

/// A run that failed the replay check.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct MigrateFailure {
    /// Run id.
    pub run_id: String,
    /// Journal seq of the divergence (0 = error before replay).
    pub seq: u64,
    /// Divergence / error detail.
    pub detail: String,
}

/// Result of a migrate-runs call (run ids per outcome).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct MigrateRunsReport {
    /// Runs whose journal replayed cleanly against the target.
    pub passed: Vec<String>,
    /// Runs repinned (`RepinRun` accepted; empty on a dry run).
    pub repinned: Vec<String>,
    /// Runs that diverged or errored.
    pub failed: Vec<MigrateFailure>,
    /// Runs refused as `RunBusy` (leased).
    pub busy: Vec<String>,
    /// Runs refused as `SeqMismatch` (progressed since the check; dry-run again).
    pub seq_mismatch: Vec<String>,
}

/// One workflow version named by a retirement.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowVersionRef {
    /// Workflow name.
    pub name: String,
    /// Version.
    pub version: u32,
}

/// `DELETE /v1/modules/{sha}`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RetireModuleResponse {
    /// Module sha256 (hex).
    pub sha256: String,
    /// Retirement time (Unix ms).
    pub retired_at_ms: u64,
    /// Workflow versions retired with it.
    pub versions: Vec<WorkflowVersionRef>,
}

/// `POST /v1/modules/gc`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct GcRunReport {
    /// Retired modules considered.
    pub candidates: Vec<String>,
    /// Modules deleted this round.
    pub deleted: Vec<String>,
    /// Modules still referenced.
    pub referenced: Vec<String>,
    /// The round was postponed (uncovered partitions, single scan so far, ...).
    pub postponed: bool,
    /// Why it was postponed.
    #[serde(default)]
    pub reason: Option<String>,
}

// ---------------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------------

/// Cluster membership, placement and upgrades.
#[async_trait::async_trait]
pub trait ClusterApi: Send + Sync + 'static {
    /// Promote this single node to a cluster.
    async fn init(&self, r: InitRequest) -> Result<ClusterStatus, ApiError> {
        let _ = r;
        Err(lands_in("init", "13-16"))
    }
    /// Admit a joining node.
    async fn join(&self, r: JoinRequest) -> Result<JoinResponse, ApiError> {
        let _ = r;
        Err(lands_in("join", "13-09"))
    }
    /// Cluster status.
    async fn status(&self) -> Result<ClusterStatus, ApiError> {
        Err(lands_in("status", "13-09"))
    }
    /// Create a node join token.
    async fn create_node_token(&self, r: NodeTokenRequest) -> Result<NodeTokenResponse, ApiError> {
        let _ = r;
        Err(lands_in("create_node_token", "13-09"))
    }
    /// Remove a member (`force` skips waiting on a dead node, never the quorum check).
    async fn remove(&self, id: u64, force: bool) -> Result<ClusterStatus, ApiError> {
        let _ = (id, force);
        Err(lands_in("remove", "13-09"))
    }
    /// Drain a member (hand off its partitions).
    async fn drain(&self, id: u64) -> Result<ClusterStatus, ApiError> {
        let _ = id;
        Err(lands_in("drain", "13-11"))
    }
    /// Start or cancel a partition reassignment.
    async fn reassign(&self, r: ReassignRequest) -> Result<ClusterStatus, ApiError> {
        let _ = r;
        Err(lands_in("reassign", "13-10"))
    }
    /// Rebalance replicas and preferred leaders.
    async fn rebalance(&self) -> Result<ClusterStatus, ApiError> {
        Err(lands_in("rebalance", "13-10"))
    }
    /// Feature levels and finalization state.
    async fn upgrade_status(&self) -> Result<UpgradeStatus, ApiError> {
        Err(lands_in("upgrade_status", "13-04"))
    }
    /// Finalize the highest common level.
    async fn upgrade_finalize(&self) -> Result<UpgradeStatus, ApiError> {
        Err(lands_in("upgrade_finalize", "13-04"))
    }
    /// Hold the current level (keep the downgrade window open).
    async fn upgrade_hold(&self) -> Result<UpgradeStatus, ApiError> {
        Err(lands_in("upgrade_hold", "13-04"))
    }
    /// Release a hold.
    async fn upgrade_release(&self) -> Result<UpgradeStatus, ApiError> {
        Err(lands_in("upgrade_release", "13-04"))
    }
}

/// Module retirement, module GC and run migration.
#[async_trait::async_trait]
pub trait ModuleLifecycleApi: Send + Sync + 'static {
    /// Retire a module and its workflow versions (refused while a version is active).
    async fn retire_module(&self, sha: String) -> Result<RetireModuleResponse, ApiError> {
        let _ = sha;
        Err(lands_in("retire_module", "13-15"))
    }
    /// Run one module GC round now.
    async fn run_module_gc(&self) -> Result<GcRunReport, ApiError> {
        Err(lands_in("run_module_gc", "13-15"))
    }
    /// Replay-check and repin runs to another workflow version.
    async fn migrate_runs(&self, r: MigrateRunsRequest) -> Result<MigrateRunsReport, ApiError> {
        let _ = r;
        Err(lands_in("migrate_runs", "13-17"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct Empty;
    impl ClusterApi for Empty {}
    impl ModuleLifecycleApi for Empty {}

    #[tokio::test]
    async fn default_bodies_are_unimplemented_and_name_the_plan() {
        let e = Arc::new(Empty);
        let c: Arc<dyn ClusterApi> = e.clone();
        let err = c.status().await.expect_err("default");
        assert_eq!(err.code, ErrorCode::Unimplemented);
        assert!(err.message.contains("13-09"), "{}", err.message);
        assert!(
            c.upgrade_finalize()
                .await
                .unwrap_err()
                .message
                .contains("13-04")
        );
        assert!(
            c.init(InitRequest::default())
                .await
                .unwrap_err()
                .message
                .contains("13-16")
        );
        let m: Arc<dyn ModuleLifecycleApi> = e;
        assert!(
            m.run_module_gc()
                .await
                .unwrap_err()
                .message
                .contains("13-15")
        );
        let req = MigrateRunsRequest {
            workflow: "w".into(),
            from: VersionOrSha::Version(1),
            to_version: 2,
            filter: None,
            limit: None,
            dry_run: true,
        };
        assert!(
            m.migrate_runs(req)
                .await
                .unwrap_err()
                .message
                .contains("13-17")
        );
    }

    #[test]
    fn wire_shapes() {
        let v: VersionOrSha = serde_json::from_str(r#"{"version":3}"#).expect("version");
        assert_eq!(v, VersionOrSha::Version(3));
        let s: VersionOrSha = serde_json::from_str(r#"{"sha":"ab"}"#).expect("sha");
        assert_eq!(s, VersionOrSha::Sha("ab".into()));
        let j: JoinRequest = serde_json::from_str(
            r#"{"token":"njt_x","csr_pem":"c","advertise":{"api":"a","cluster":"b"},
                "node_version":{"ostro_version":"0.1.0","level_min":1,"level_max":2,
                "wasmtime":"50","event_codec_ver":1,"wit_lines":["0.1"]}}"#,
        )
        .expect("join");
        assert_eq!(j.resume_node_id, None);
        assert_eq!(j.advertise.repl, None);
    }
}
