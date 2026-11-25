//! Phase 13 client surface (13-16; D-P13-23 REST paths): cluster membership, placement,
//! upgrades, run migration and module lifecycle.
//!
//! - Raw methods on [`Client`] return the response body as JSON ([`Response`]), so the CLI's
//!   `--json` prints it verbatim; non-2xx answers are [`CallError::Api`] with the server's
//!   error body (the Phase 10 error mapping; [`crate::ClientError::reason`] carries
//!   `details.reason`, e.g. `feature_not_enabled`, `module_active`, `not_supported_in_profile`).
//! - Typed methods on [`TypedClient`] take the request structs below and decode the answers
//!   into the types below (the wire shapes of `ostro_api::phase13`, 13-01). Every answer type
//!   keeps unknown fields in `extra`, so additive server changes never break a client.
//!
//! | method | path | raw | typed |
//! |--------|------|-----|-------|
//! | POST | `/v1/cluster/init` | [`Client::cluster_init`] | [`TypedClient::cluster_init`] |
//! | POST | `/v1/cluster/join` | [`Client::cluster_join`] | (the joiner, `ostro cluster join`) |
//! | GET | `/v1/cluster/status` | [`Client::cluster_status`] | [`TypedClient::cluster_status`] |
//! | POST | `/v1/cluster/tokens` | [`Client::create_node_token`] | [`TypedClient::create_node_token`] |
//! | POST | `/v1/cluster/members/{id}/remove` | [`Client::cluster_remove`] | [`TypedClient::cluster_remove`] |
//! | POST | `/v1/cluster/members/{id}/drain` | [`Client::cluster_drain`] | [`TypedClient::cluster_drain`] |
//! | POST | `/v1/cluster/reassign` | [`Client::cluster_reassign`] | [`TypedClient::cluster_reassign`] |
//! | POST | `/v1/cluster/rebalance` | [`Client::cluster_rebalance`] | [`TypedClient::cluster_rebalance`] |
//! | GET | `/v1/cluster/upgrade` | [`Client::upgrade_status`] | [`TypedClient::upgrade_status`] |
//! | POST | `/v1/cluster/upgrade/{finalize,hold,release}` | [`Client::upgrade_finalize`] … | [`TypedClient::upgrade_finalize`] … |
//! | POST | `/v1/runs/migrate` | [`Client::migrate_runs`] | [`TypedClient::migrate_runs`] |
//! | DELETE | `/v1/modules/{sha}` | [`Client::retire_module`] | [`TypedClient::retire_module`] |
//! | POST | `/v1/modules/gc` | [`Client::run_module_gc`] | [`TypedClient::run_module_gc`] |

use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::client::{CallError, Client, Response};
use crate::error::ClientError;
use crate::typed::{Extra, TypedClient, decode};

// ------------------------------------------------------------------------------- requests

/// `POST /v1/cluster/init` (unset fields take the server defaults: `[cluster] advertise`,
/// rf 3, min_insync `rf / 2 + 1`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct InitRequest {
    /// The cluster listener address peers dial.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub advertise: Option<String>,
    /// Target replication factor (1, 3 or 5).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rf: Option<u8>,
    /// Target min_insync (1..=rf).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_insync: Option<u8>,
}

/// `POST /v1/cluster/tokens`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct NodeTokenRequest {
    /// Token lifetime in seconds (server default when unset).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u64>,
}

/// `POST /v1/cluster/reassign`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ReassignRequest {
    /// Partition.
    pub partition: u16,
    /// Target replica set (ignored when `cancel`).
    pub replicas: Vec<u64>,
    /// Cancel the in-flight reassignment (before its leader move).
    pub cancel: bool,
}

/// The source of a migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionOrSha {
    /// A workflow version.
    Version(u32),
    /// A module sha256 (hex).
    Sha(String),
}

impl VersionOrSha {
    /// A number is a version, anything else a module sha (the CLI's `--from`).
    pub fn parse(s: &str) -> Self {
        match s.parse::<u32>() {
            Ok(v) => VersionOrSha::Version(v),
            Err(_) => VersionOrSha::Sha(s.to_owned()),
        }
    }
}

/// `POST /v1/runs/migrate` (D-P13-21).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MigrateRunsRequest {
    /// Workflow name.
    pub workflow: String,
    /// Source version or module.
    pub from: VersionOrSha,
    /// Target version.
    pub to_version: u32,
    /// Tag filter (comma-separated tags, all required).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    /// At most this many runs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Replay-check only.
    pub dry_run: bool,
}

// -------------------------------------------------------------------------------- answers

/// A member's binary.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct NodeVersion {
    /// Ostro version.
    #[serde(default)]
    pub ostro_version: String,
    /// Lowest supported feature level.
    #[serde(default)]
    pub level_min: u32,
    /// Highest emitted feature level.
    #[serde(default)]
    pub level_max: u32,
    /// Wasmtime version.
    #[serde(default)]
    pub wasmtime: String,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// One member of [`ClusterStatus`].
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ClusterMember {
    /// Node id.
    pub node_id: u64,
    /// Client API address.
    #[serde(default)]
    pub addr: String,
    /// Cluster listener address.
    #[serde(default)]
    pub cluster_addr: Option<String>,
    /// `joining`, `active`, `draining` or `removed`.
    #[serde(default)]
    pub state: String,
    /// Reported binary.
    #[serde(default)]
    pub version: Option<NodeVersion>,
    /// Live per the liveness view.
    #[serde(default)]
    pub live: bool,
    /// Meta Raft role: `voter`, `learner`, `outgoing` or `none`.
    #[serde(default)]
    pub raft_role: String,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// The replication policy.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ReplicationPolicy {
    /// Target replication factor.
    pub rf: u8,
    /// Target min_insync.
    pub min_insync: u8,
    /// Maximum meta voters.
    pub max_voters: u8,
}

/// One in-flight reassignment.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Reassignment {
    /// Partition.
    pub partition: u16,
    /// Target replica set.
    #[serde(default)]
    pub target: Vec<u64>,
    /// Replicas being added.
    #[serde(default)]
    pub adding: Vec<u64>,
    /// Replicas being removed.
    #[serde(default)]
    pub removing: Vec<u64>,
    /// Start (Unix ms).
    #[serde(default)]
    pub started_at_ms: u64,
}

/// `GET /v1/cluster/status` (also the answer of init, remove, drain, reassign, rebalance).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ClusterStatus {
    /// Cluster id.
    #[serde(default)]
    pub cluster_id: String,
    /// Every member (tombstones included).
    #[serde(default)]
    pub members: Vec<ClusterMember>,
    /// Meta voters.
    #[serde(default)]
    pub voters: Vec<u64>,
    /// Meta learners.
    #[serde(default)]
    pub learners: Vec<u64>,
    /// Outgoing voters (non-empty while joint).
    #[serde(default)]
    pub voters_outgoing: Vec<u64>,
    /// Meta leader (0 = unknown).
    #[serde(default)]
    pub meta_leader: u64,
    /// Finalized feature level.
    #[serde(default)]
    pub finalized: u32,
    /// Operator hold.
    #[serde(default)]
    pub hold: Option<u32>,
    /// Lowest `level_min` over members.
    #[serde(default)]
    pub level_min: u32,
    /// Lowest `level_max` over members.
    #[serde(default)]
    pub level_max: u32,
    /// Replication policy (`None` until `cluster init`).
    #[serde(default)]
    pub policy: Option<ReplicationPolicy>,
    /// In-flight reassignments.
    #[serde(default)]
    pub reassignments: Vec<Reassignment>,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// A created node join token (the secret is shown once).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct NodeToken {
    /// Token id (`njt_…`).
    pub id: String,
    /// The secret token.
    pub token: String,
    /// Expiry (Unix ms).
    #[serde(default)]
    pub expires_at_ms: u64,
    /// `sha256:<hex>` of the cluster CA (the joiner's `--ca-hash`).
    #[serde(default)]
    pub ca_hash: String,
}

/// One member's levels in [`UpgradeStatus`].
#[derive(Debug, Clone, Default, Deserialize)]
pub struct MemberLevel {
    /// Node id.
    pub node_id: u64,
    /// Lowest supported level.
    #[serde(default)]
    pub level_min: u32,
    /// Highest emitted level.
    #[serde(default)]
    pub level_max: u32,
    /// Live.
    #[serde(default)]
    pub live: bool,
}

/// `GET /v1/cluster/upgrade`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct UpgradeStatus {
    /// Finalized feature level.
    #[serde(default)]
    pub finalized: u32,
    /// Operator hold.
    #[serde(default)]
    pub hold: Option<u32>,
    /// The highest level every member supports.
    #[serde(default)]
    pub common_level_max: u32,
    /// `[cluster] auto_finalize`.
    #[serde(default)]
    pub auto_finalize: bool,
    /// Per-member levels.
    #[serde(default)]
    pub members: Vec<MemberLevel>,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// A run that failed the replay check.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct MigrateFailure {
    /// Run id.
    pub run_id: String,
    /// Journal seq of the divergence (0 = error before replay).
    #[serde(default)]
    pub seq: u64,
    /// Detail.
    #[serde(default)]
    pub detail: String,
}

/// `POST /v1/runs/migrate`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct MigrateRunsReport {
    /// Replayed cleanly.
    #[serde(default)]
    pub passed: Vec<String>,
    /// Repinned (empty on a dry run).
    #[serde(default)]
    pub repinned: Vec<String>,
    /// Diverged or errored.
    #[serde(default)]
    pub failed: Vec<MigrateFailure>,
    /// Leased (`RunBusy`).
    #[serde(default)]
    pub busy: Vec<String>,
    /// Progressed since the check (`SeqMismatch`).
    #[serde(default)]
    pub seq_mismatch: Vec<String>,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// One workflow version retired with a module.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WorkflowVersionRef {
    /// Workflow name.
    pub name: String,
    /// Version.
    pub version: u32,
}

/// `DELETE /v1/modules/{sha}`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RetiredModule {
    /// Module sha256 (hex).
    pub sha256: String,
    /// Retirement time (Unix ms).
    #[serde(default)]
    pub retired_at_ms: u64,
    /// Workflow versions retired with it.
    #[serde(default)]
    pub versions: Vec<WorkflowVersionRef>,
}

/// `POST /v1/modules/gc`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GcReport {
    /// Retired modules considered.
    #[serde(default)]
    pub candidates: Vec<String>,
    /// Deleted this round.
    #[serde(default)]
    pub deleted: Vec<String>,
    /// Still referenced.
    #[serde(default)]
    pub referenced: Vec<String>,
    /// The round was postponed.
    #[serde(default)]
    pub postponed: bool,
    /// Why.
    #[serde(default)]
    pub reason: Option<String>,
}

// ------------------------------------------------------------------------------ raw client

impl Client {
    /// `POST /v1/cluster/init` `{advertise, rf, min_insync}`.
    pub async fn cluster_init(&self, body: &Value) -> Result<Response, CallError> {
        self.post(&["v1", "cluster", "init"], body).await
    }

    /// `POST /v1/cluster/join` `{token, csr_pem, advertise, node_version, resume_node_id}`.
    pub async fn cluster_join(&self, body: &Value) -> Result<Response, CallError> {
        self.post(&["v1", "cluster", "join"], body).await
    }

    /// `GET /v1/cluster/status`.
    pub async fn cluster_status(&self) -> Result<Response, CallError> {
        self.get(&["v1", "cluster", "status"], &[]).await
    }

    /// `POST /v1/cluster/tokens` `{ttl_secs}` (a `njt_` node join token).
    pub async fn create_node_token(&self, body: &Value) -> Result<Response, CallError> {
        self.post(&["v1", "cluster", "tokens"], body).await
    }

    /// `POST /v1/cluster/members/{id}/remove` `{force}`.
    pub async fn cluster_remove(&self, id: u64, body: &Value) -> Result<Response, CallError> {
        self.post(
            &["v1", "cluster", "members", &id.to_string(), "remove"],
            body,
        )
        .await
    }

    /// `POST /v1/cluster/members/{id}/drain`.
    pub async fn cluster_drain(&self, id: u64) -> Result<Response, CallError> {
        self.post(
            &["v1", "cluster", "members", &id.to_string(), "drain"],
            &json!({}),
        )
        .await
    }

    /// `POST /v1/cluster/reassign` `{partition, replicas, cancel}`.
    pub async fn cluster_reassign(&self, body: &Value) -> Result<Response, CallError> {
        self.post(&["v1", "cluster", "reassign"], body).await
    }

    /// `POST /v1/cluster/rebalance`.
    pub async fn cluster_rebalance(&self) -> Result<Response, CallError> {
        self.post(&["v1", "cluster", "rebalance"], &json!({})).await
    }

    /// `GET /v1/cluster/upgrade`.
    pub async fn upgrade_status(&self) -> Result<Response, CallError> {
        self.get(&["v1", "cluster", "upgrade"], &[]).await
    }

    /// `POST /v1/cluster/upgrade/finalize`.
    pub async fn upgrade_finalize(&self) -> Result<Response, CallError> {
        self.post(&["v1", "cluster", "upgrade", "finalize"], &json!({}))
            .await
    }

    /// `POST /v1/cluster/upgrade/hold`.
    pub async fn upgrade_hold(&self) -> Result<Response, CallError> {
        self.post(&["v1", "cluster", "upgrade", "hold"], &json!({}))
            .await
    }

    /// `POST /v1/cluster/upgrade/release`.
    pub async fn upgrade_release(&self) -> Result<Response, CallError> {
        self.post(&["v1", "cluster", "upgrade", "release"], &json!({}))
            .await
    }

    /// `POST /v1/runs/migrate` `{workflow, from, to_version, filter, limit, dry_run}`.
    pub async fn migrate_runs(&self, body: &Value) -> Result<Response, CallError> {
        self.post(&["v1", "runs", "migrate"], body).await
    }

    /// `DELETE /v1/modules/{sha}` (retire a module; idempotent).
    pub async fn retire_module(&self, sha: &str) -> Result<Response, CallError> {
        self.send(self.request(Method::DELETE, self.url(&["v1", "modules", sha], &[])))
            .await
    }

    /// `POST /v1/modules/gc` (one module GC round now).
    pub async fn run_module_gc(&self) -> Result<Response, CallError> {
        self.post(&["v1", "modules", "gc"], &json!({})).await
    }
}

// ---------------------------------------------------------------------------- typed client

fn body<T: Serialize>(req: &T, what: &str) -> Result<Value, ClientError> {
    serde_json::to_value(req).map_err(|e| ClientError {
        status: None,
        code: "usage".to_owned(),
        message: format!("cannot encode the {what} request: {e}"),
        details: Value::Null,
    })
}

impl TypedClient {
    /// Promote the server (a running single node) to a cluster.
    pub async fn cluster_init(&self, req: &InitRequest) -> Result<ClusterStatus, ClientError> {
        let b = body(req, "init")?;
        decode(self.raw().cluster_init(&b).await, "cluster init")
    }

    /// The cluster status.
    pub async fn cluster_status(&self) -> Result<ClusterStatus, ClientError> {
        decode(self.raw().cluster_status().await, "cluster status")
    }

    /// Create a node join token.
    pub async fn create_node_token(
        &self,
        req: &NodeTokenRequest,
    ) -> Result<NodeToken, ClientError> {
        let b = body(req, "token")?;
        decode(self.raw().create_node_token(&b).await, "node token")
    }

    /// Remove member `id` (`force` skips waiting on a dead node, never the quorum check).
    pub async fn cluster_remove(&self, id: u64, force: bool) -> Result<ClusterStatus, ClientError> {
        decode(
            self.raw()
                .cluster_remove(id, &json!({ "force": force }))
                .await,
            "cluster remove",
        )
    }

    /// Drain member `id`.
    pub async fn cluster_drain(&self, id: u64) -> Result<ClusterStatus, ClientError> {
        decode(self.raw().cluster_drain(id).await, "cluster drain")
    }

    /// Start or cancel a partition reassignment.
    pub async fn cluster_reassign(
        &self,
        req: &ReassignRequest,
    ) -> Result<ClusterStatus, ClientError> {
        let b = body(req, "reassign")?;
        decode(self.raw().cluster_reassign(&b).await, "cluster reassign")
    }

    /// Rebalance replicas and preferred leaders now.
    pub async fn cluster_rebalance(&self) -> Result<ClusterStatus, ClientError> {
        decode(self.raw().cluster_rebalance().await, "cluster rebalance")
    }

    /// Feature levels and finalization state.
    pub async fn upgrade_status(&self) -> Result<UpgradeStatus, ClientError> {
        decode(self.raw().upgrade_status().await, "upgrade status")
    }

    /// Finalize the highest common level.
    pub async fn upgrade_finalize(&self) -> Result<UpgradeStatus, ClientError> {
        decode(self.raw().upgrade_finalize().await, "upgrade finalize")
    }

    /// Hold the current level.
    pub async fn upgrade_hold(&self) -> Result<UpgradeStatus, ClientError> {
        decode(self.raw().upgrade_hold().await, "upgrade hold")
    }

    /// Release a hold.
    pub async fn upgrade_release(&self) -> Result<UpgradeStatus, ClientError> {
        decode(self.raw().upgrade_release().await, "upgrade release")
    }

    /// Replay-check and repin runs to another workflow version.
    pub async fn migrate_runs(
        &self,
        req: &MigrateRunsRequest,
    ) -> Result<MigrateRunsReport, ClientError> {
        let b = body(req, "migrate-runs")?;
        decode(self.raw().migrate_runs(&b).await, "migrate-runs")
    }

    /// Retire a module (idempotent).
    pub async fn retire_module(&self, sha: &str) -> Result<RetiredModule, ClientError> {
        decode(self.raw().retire_module(sha).await, "module retire")
    }

    /// Run one module GC round now.
    pub async fn run_module_gc(&self) -> Result<GcReport, ClientError> {
        decode(self.raw().run_module_gc().await, "module gc")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_shapes() {
        assert_eq!(
            serde_json::to_value(InitRequest::default()).unwrap(),
            json!({})
        );
        assert_eq!(
            serde_json::to_value(InitRequest {
                advertise: Some("h:1".into()),
                rf: Some(3),
                min_insync: None,
            })
            .unwrap(),
            json!({ "advertise": "h:1", "rf": 3 })
        );
        assert_eq!(VersionOrSha::parse("3"), VersionOrSha::Version(3));
        assert_eq!(VersionOrSha::parse("ab"), VersionOrSha::Sha("ab".into()));
        assert_eq!(
            serde_json::to_value(MigrateRunsRequest {
                workflow: "w".into(),
                from: VersionOrSha::Version(1),
                to_version: 2,
                filter: None,
                limit: Some(5),
                dry_run: true,
            })
            .unwrap(),
            json!({ "workflow": "w", "from": { "version": 1 }, "to_version": 2, "limit": 5, "dry_run": true })
        );
    }

    #[test]
    fn answers_keep_unknown_fields() {
        let s: ClusterStatus = serde_json::from_value(json!({
            "cluster_id": "c", "members": [{"node_id": 1, "state": "active", "x": 1}],
            "future": true
        }))
        .unwrap();
        assert_eq!(s.members[0].node_id, 1);
        assert_eq!(s.members[0].extra["x"], json!(1));
        assert_eq!(s.extra["future"], json!(true));
        assert!(s.policy.is_none());
    }
}
