//! Phase 13 adapters (13-03; D-P13-23, D-P13-24): the REST routes and the
//! `ostro.api.v1.{Cluster, ModuleLifecycle}` gRPC services over the
//! [`crate::phase13::ClusterApi`] / [`crate::phase13::ModuleLifecycleApi`] slots of
//! [`crate::ApiExtensions`], following the Phase 11 adapter pattern (D-P11-22).
//!
//! An absent `cluster` / `module_lifecycle` slot answers 404 `not_found` on REST, and its
//! gRPC service is not mounted (`UNIMPLEMENTED`). Request validation that needs no server
//! state (ids, sha syntax, replica lists, migrate source) runs here for both transports
//! before the trait is called.
//!
//! `POST /v1/cluster/join` is served by [`join_router`], which [`crate::router_with`] mounts
//! outside the API middleware stack: the body's node join token is the credential, so the
//! Phase 14 client authentication added around the API routes never applies to it (the
//! gRPC twin `Cluster.Join` likewise). Its responses, like the node-token response, carry
//! `Cache-Control: no-store`.
//!
//! The proto <-> domain conversions live here in both directions: the server uses
//! `pb -> domain` for requests and `domain -> pb` for responses, the production client
//! ([`crate::grpc_client::GrpcControlPlane`]) the inverse.

use std::sync::Arc;

use axum::Json;
use axum::extract::rejection::{BytesRejection, JsonRejection};
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::{cluster_server, module_lifecycle_server};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::error::{ApiError, ErrorBody};
use crate::phase8::{ApiExtensions, ExtState};
use crate::phase13::*;
use crate::rest::{json_body, optional_json_body};

// ---------------------------------------------------------------------------------
// Validation (both transports)
// ---------------------------------------------------------------------------------

/// Stateless request checks shared by REST and gRPC.
pub mod check {
    use super::*;

    /// A module sha256: 64 lowercase hex digits.
    pub fn sha256(field: &str, sha: &str) -> Result<(), ApiError> {
        if sha.len() == 64 && sha.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            Ok(())
        } else {
            Err(
                ApiError::invalid_argument(format!("{field} must be 64 lowercase hex digits"))
                    .with_details(serde_json::json!({ "field": field })),
            )
        }
    }

    /// A member id: non-zero.
    pub fn node_id(id: u64) -> Result<(), ApiError> {
        if id == 0 {
            return Err(ApiError::invalid_argument("node id must be > 0")
                .with_details(serde_json::json!({ "field": "id" })));
        }
        Ok(())
    }

    /// A reassignment: a non-empty, duplicate-free replica set of non-zero ids unless
    /// cancelling.
    pub fn reassign(r: &ReassignRequest) -> Result<(), ApiError> {
        if r.cancel {
            return Ok(());
        }
        let field = || serde_json::json!({ "field": "replicas" });
        if r.replicas.is_empty() {
            return Err(
                ApiError::invalid_argument("replicas is required unless cancel")
                    .with_details(field()),
            );
        }
        let mut seen = std::collections::BTreeSet::new();
        for id in &r.replicas {
            if *id == 0 || !seen.insert(*id) {
                return Err(ApiError::invalid_argument(format!(
                    "replicas: {id} is zero or repeated"
                ))
                .with_details(field()));
            }
        }
        Ok(())
    }

    /// A migration: a workflow, a valid source, a positive limit when given.
    pub fn migrate(r: &MigrateRunsRequest) -> Result<(), ApiError> {
        if r.workflow.is_empty() {
            return Err(ApiError::invalid_argument("workflow is required")
                .with_details(serde_json::json!({ "field": "workflow" })));
        }
        if let VersionOrSha::Sha(s) = &r.from {
            sha256("from.sha", s)?;
        }
        if r.limit == Some(0) {
            return Err(ApiError::invalid_argument("limit must be > 0")
                .with_details(serde_json::json!({ "field": "limit" })));
        }
        Ok(())
    }
}

fn absent(what: &str) -> ApiError {
    ApiError::not_found(format!("the {what} API is not enabled on this server"))
}

fn cluster(ext: &ApiExtensions) -> Result<&dyn ClusterApi, ApiError> {
    ext.cluster.as_deref().ok_or_else(|| absent("cluster"))
}

fn lifecycle(ext: &ApiExtensions) -> Result<&dyn ModuleLifecycleApi, ApiError> {
    ext.module_lifecycle
        .as_deref()
        .ok_or_else(|| absent("module lifecycle"))
}

fn parse_id(s: &str) -> Result<u64, ApiError> {
    let id = s.parse::<u64>().map_err(|_| {
        ApiError::invalid_argument(format!("node id {s:?} is not a number"))
            .with_details(serde_json::json!({ "field": "id" }))
    })?;
    check::node_id(id)?;
    Ok(id)
}

fn no_store<T: IntoResponse>(status: StatusCode, body: T) -> Response {
    let mut res = (status, body).into_response();
    res.headers_mut().insert(
        header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    res
}

// ---------------------------------------------------------------------------------
// REST
// ---------------------------------------------------------------------------------

/// `POST /v1/cluster/members/{id}/remove` body.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RemoveMemberBody {
    /// Do not wait for a dead node (never skips the quorum check).
    #[serde(default)]
    pub force: bool,
}

/// Promote this single node to a cluster.
#[utoipa::path(
    post,
    path = "/v1/cluster/init",
    tag = "cluster",
    request_body(content = InitRequest, description = "Optional; an empty body takes the defaults"),
    responses(
        (status = 200, description = "Cluster status after promotion", body = ClusterStatus),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 404, description = "Cluster API not enabled", body = ErrorBody),
        (status = 409, description = "Already a cluster", body = ErrorBody),
    )
)]
async fn init(
    State(ext): State<ExtState>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<ClusterStatus>, ApiError> {
    let api = cluster(&ext)?;
    let req: InitRequest = optional_json_body(body)?;
    Ok(Json(api.init(req).await?))
}

/// Admit a joining node. Authenticated by the node join token in the body (no client
/// identity; mounted outside the API middleware).
#[utoipa::path(
    post,
    path = "/v1/cluster/join",
    tag = "cluster",
    request_body = JoinRequest,
    responses(
        (status = 200, description = "Admitted (Cache-Control: no-store)", body = JoinResponse),
        (status = 400, description = "Invalid request or token (details.reason)", body = ErrorBody),
        (status = 404, description = "Cluster API not enabled", body = ErrorBody),
        (status = 409, description = "feature_not_enabled / incompatible binary (details.reason)", body = ErrorBody),
    )
)]
async fn join(
    State(ext): State<ExtState>,
    body: Result<Json<JoinRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let api = cluster(&ext)?;
    let req = json_body(body)?;
    Ok(no_store(StatusCode::OK, Json(api.join(req).await?)))
}

/// Cluster status: members, meta voters / learners, levels, policy, reassignments.
#[utoipa::path(
    get,
    path = "/v1/cluster/status",
    tag = "cluster",
    responses(
        (status = 200, description = "Cluster status", body = ClusterStatus),
        (status = 404, description = "Cluster API not enabled", body = ErrorBody),
    )
)]
async fn status(State(ext): State<ExtState>) -> Result<Json<ClusterStatus>, ApiError> {
    Ok(Json(cluster(&ext)?.status().await?))
}

/// Create a node join token (`njt_…`). The secret is in this response only.
#[utoipa::path(
    post,
    path = "/v1/cluster/tokens",
    tag = "cluster",
    request_body(content = NodeTokenRequest, description = "Optional; an empty body means the default lifetime"),
    responses(
        (status = 201, description = "Created (Cache-Control: no-store)", body = NodeTokenResponse),
        (status = 400, description = "Invalid ttl_secs", body = ErrorBody),
        (status = 404, description = "Cluster API not enabled", body = ErrorBody),
    )
)]
async fn create_node_token(
    State(ext): State<ExtState>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Response, ApiError> {
    let api = cluster(&ext)?;
    let req: NodeTokenRequest = optional_json_body(body)?;
    Ok(no_store(
        StatusCode::CREATED,
        Json(api.create_node_token(req).await?),
    ))
}

/// Remove a member (`force` skips waiting for a dead node, never the quorum check).
#[utoipa::path(
    post,
    path = "/v1/cluster/members/{id}/remove",
    tag = "cluster",
    params(("id" = u64, Path, description = "Node id")),
    request_body(content = RemoveMemberBody, description = "Optional"),
    responses(
        (status = 200, description = "Cluster status", body = ClusterStatus),
        (status = 400, description = "Invalid id", body = ErrorBody),
        (status = 404, description = "Unknown member, or cluster API not enabled", body = ErrorBody),
        (status = 409, description = "Would lose quorum (details.reason)", body = ErrorBody),
    )
)]
async fn remove(
    State(ext): State<ExtState>,
    Path(id): Path<String>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<ClusterStatus>, ApiError> {
    let api = cluster(&ext)?;
    let id = parse_id(&id)?;
    let b: RemoveMemberBody = optional_json_body(body)?;
    Ok(Json(api.remove(id, b.force).await?))
}

/// Drain a member: hand off its partition leaderships and replicas.
#[utoipa::path(
    post,
    path = "/v1/cluster/members/{id}/drain",
    tag = "cluster",
    params(("id" = u64, Path, description = "Node id")),
    responses(
        (status = 200, description = "Cluster status", body = ClusterStatus),
        (status = 400, description = "Invalid id", body = ErrorBody),
        (status = 404, description = "Unknown member, or cluster API not enabled", body = ErrorBody),
    )
)]
async fn drain(
    State(ext): State<ExtState>,
    Path(id): Path<String>,
) -> Result<Json<ClusterStatus>, ApiError> {
    let api = cluster(&ext)?;
    Ok(Json(api.drain(parse_id(&id)?).await?))
}

/// Start or cancel a partition reassignment.
#[utoipa::path(
    post,
    path = "/v1/cluster/reassign",
    tag = "cluster",
    request_body = ReassignRequest,
    responses(
        (status = 200, description = "Cluster status", body = ClusterStatus),
        (status = 400, description = "Invalid replica set", body = ErrorBody),
        (status = 404, description = "Cluster API not enabled", body = ErrorBody),
        (status = 409, description = "Reassignment in progress / not cancellable (details.reason)", body = ErrorBody),
    )
)]
async fn reassign(
    State(ext): State<ExtState>,
    body: Result<Json<ReassignRequest>, JsonRejection>,
) -> Result<Json<ClusterStatus>, ApiError> {
    let api = cluster(&ext)?;
    let req = json_body(body)?;
    check::reassign(&req)?;
    Ok(Json(api.reassign(req).await?))
}

/// Rebalance replicas and preferred leaders.
#[utoipa::path(
    post,
    path = "/v1/cluster/rebalance",
    tag = "cluster",
    responses(
        (status = 200, description = "Cluster status", body = ClusterStatus),
        (status = 404, description = "Cluster API not enabled", body = ErrorBody),
    )
)]
async fn rebalance(State(ext): State<ExtState>) -> Result<Json<ClusterStatus>, ApiError> {
    Ok(Json(cluster(&ext)?.rebalance().await?))
}

/// Feature levels and finalization state.
#[utoipa::path(
    get,
    path = "/v1/cluster/upgrade",
    tag = "cluster",
    responses(
        (status = 200, description = "Upgrade status", body = UpgradeStatus),
        (status = 404, description = "Cluster API not enabled", body = ErrorBody),
    )
)]
async fn upgrade_status(State(ext): State<ExtState>) -> Result<Json<UpgradeStatus>, ApiError> {
    Ok(Json(cluster(&ext)?.upgrade_status().await?))
}

/// Finalize the highest common feature level.
#[utoipa::path(
    post,
    path = "/v1/cluster/upgrade/finalize",
    tag = "cluster",
    responses(
        (status = 200, description = "Upgrade status", body = UpgradeStatus),
        (status = 404, description = "Cluster API not enabled", body = ErrorBody),
        (status = 409, description = "Not ready / held (details.reason)", body = ErrorBody),
    )
)]
async fn upgrade_finalize(State(ext): State<ExtState>) -> Result<Json<UpgradeStatus>, ApiError> {
    Ok(Json(cluster(&ext)?.upgrade_finalize().await?))
}

/// Hold the current feature level (keep the downgrade window open).
#[utoipa::path(
    post,
    path = "/v1/cluster/upgrade/hold",
    tag = "cluster",
    responses(
        (status = 200, description = "Upgrade status", body = UpgradeStatus),
        (status = 404, description = "Cluster API not enabled", body = ErrorBody),
    )
)]
async fn upgrade_hold(State(ext): State<ExtState>) -> Result<Json<UpgradeStatus>, ApiError> {
    Ok(Json(cluster(&ext)?.upgrade_hold().await?))
}

/// Release a hold.
#[utoipa::path(
    post,
    path = "/v1/cluster/upgrade/release",
    tag = "cluster",
    responses(
        (status = 200, description = "Upgrade status", body = UpgradeStatus),
        (status = 404, description = "Cluster API not enabled", body = ErrorBody),
    )
)]
async fn upgrade_release(State(ext): State<ExtState>) -> Result<Json<UpgradeStatus>, ApiError> {
    Ok(Json(cluster(&ext)?.upgrade_release().await?))
}

/// Retire a module and its workflow versions (refused while a version is active).
#[utoipa::path(
    delete,
    path = "/v1/modules/{sha256}",
    tag = "modules",
    params(("sha256" = String, Path, description = "Module sha256 (64 lowercase hex digits)")),
    responses(
        (status = 200, description = "Retired", body = RetireModuleResponse),
        (status = 400, description = "Invalid sha", body = ErrorBody),
        (status = 404, description = "Unknown module, or module lifecycle API not enabled", body = ErrorBody),
        (status = 409, description = "A version is active (details.reason)", body = ErrorBody),
    )
)]
async fn retire_module(
    State(ext): State<ExtState>,
    Path(sha): Path<String>,
) -> Result<Json<RetireModuleResponse>, ApiError> {
    let api = lifecycle(&ext)?;
    check::sha256("sha256", &sha)?;
    Ok(Json(api.retire_module(sha).await?))
}

/// Run one module GC round now.
#[utoipa::path(
    post,
    path = "/v1/modules/gc",
    tag = "modules",
    responses(
        (status = 200, description = "Round report", body = GcRunReport),
        (status = 404, description = "Module lifecycle API not enabled", body = ErrorBody),
    )
)]
async fn run_module_gc(State(ext): State<ExtState>) -> Result<Json<GcRunReport>, ApiError> {
    Ok(Json(lifecycle(&ext)?.run_module_gc().await?))
}

/// Replay-check runs against another workflow version and repin the clean ones.
#[utoipa::path(
    post,
    path = "/v1/runs/migrate",
    tag = "runs",
    request_body = MigrateRunsRequest,
    responses(
        (status = 200, description = "Per-run outcome", body = MigrateRunsReport),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 404, description = "Unknown workflow / version, or module lifecycle API not enabled", body = ErrorBody),
        (status = 409, description = "Target retired (details.reason)", body = ErrorBody),
    )
)]
async fn migrate_runs(
    State(ext): State<ExtState>,
    body: Result<Json<MigrateRunsRequest>, JsonRejection>,
) -> Result<Json<MigrateRunsReport>, ApiError> {
    let api = lifecycle(&ext)?;
    let req = json_body(body)?;
    check::migrate(&req)?;
    Ok(Json(api.migrate_runs(req).await?))
}

/// The Phase 13 REST routes except join, with their OpenAPI description, without state.
pub fn api_routes() -> OpenApiRouter<ExtState> {
    OpenApiRouter::new()
        .routes(routes!(init))
        .routes(routes!(status))
        .routes(routes!(create_node_token))
        .routes(routes!(remove))
        .routes(routes!(drain))
        .routes(routes!(reassign))
        .routes(routes!(rebalance))
        .routes(routes!(upgrade_status))
        .routes(routes!(upgrade_finalize))
        .routes(routes!(upgrade_hold))
        .routes(routes!(upgrade_release))
        .routes(routes!(retire_module))
        .routes(routes!(run_module_gc))
        .routes(routes!(migrate_runs))
}

/// `POST /v1/cluster/join` alone (mounted outside the API middleware).
pub fn join_routes() -> OpenApiRouter<ExtState> {
    OpenApiRouter::new().routes(routes!(join))
}

/// The Phase 13 REST router over `ext`, join excluded (merged by [`crate::router_with`]).
pub fn router(ext: ApiExtensions) -> axum::Router {
    let (r, _doc) = api_routes().split_for_parts();
    r.with_state(Arc::new(ext))
}

/// The join route over `ext` (merged by [`crate::router_with`] outside the API middleware).
pub fn join_router(ext: ApiExtensions) -> axum::Router {
    let (r, _doc) = join_routes().split_for_parts();
    r.with_state(Arc::new(ext))
}

// ---------------------------------------------------------------------------------
// Conversions (both directions)
// ---------------------------------------------------------------------------------

fn small(field: &str, v: u32) -> Result<u8, ApiError> {
    u8::try_from(v).map_err(|_| {
        ApiError::invalid_argument(format!("{field} = {v} is out of range (max 255)"))
            .with_details(serde_json::json!({ "field": field }))
    })
}

fn partition_u16(field: &str, v: u32) -> Result<u16, ApiError> {
    u16::try_from(v).map_err(|_| {
        ApiError::invalid_argument(format!("{field} = {v} is out of range (max 65535)"))
            .with_details(serde_json::json!({ "field": field }))
    })
}

fn required<T>(field: &str, v: Option<T>) -> Result<T, ApiError> {
    v.ok_or_else(|| {
        ApiError::invalid_argument(format!("{field} is required"))
            .with_details(serde_json::json!({ "field": field }))
    })
}

/// `pb::InitClusterRequest` -> domain.
pub fn init_from_proto(r: pb::InitClusterRequest) -> Result<InitRequest, ApiError> {
    Ok(InitRequest {
        advertise: r.advertise,
        rf: r.rf.map(|v| small("rf", v)).transpose()?,
        min_insync: r.min_insync.map(|v| small("min_insync", v)).transpose()?,
    })
}

/// Domain -> `pb::InitClusterRequest`.
pub fn init_to_proto(r: InitRequest) -> pb::InitClusterRequest {
    pb::InitClusterRequest {
        advertise: r.advertise,
        rf: r.rf.map(u32::from),
        min_insync: r.min_insync.map(u32::from),
    }
}

impl From<NodeVersionDto> for pb::NodeVersion {
    fn from(v: NodeVersionDto) -> Self {
        pb::NodeVersion {
            ostro_version: v.ostro_version,
            level_min: v.level_min,
            level_max: v.level_max,
            wasmtime: v.wasmtime,
            event_codec_ver: v.event_codec_ver,
            wit_lines: v.wit_lines,
        }
    }
}

impl From<pb::NodeVersion> for NodeVersionDto {
    fn from(v: pb::NodeVersion) -> Self {
        NodeVersionDto {
            ostro_version: v.ostro_version,
            level_min: v.level_min,
            level_max: v.level_max,
            wasmtime: v.wasmtime,
            event_codec_ver: v.event_codec_ver,
            wit_lines: v.wit_lines,
        }
    }
}

impl From<Advertise> for pb::NodeAdvertise {
    fn from(a: Advertise) -> Self {
        pb::NodeAdvertise {
            api: a.api,
            cluster: a.cluster,
            repl: a.repl,
        }
    }
}

impl From<pb::NodeAdvertise> for Advertise {
    fn from(a: pb::NodeAdvertise) -> Self {
        Advertise {
            api: a.api,
            cluster: a.cluster,
            repl: a.repl,
        }
    }
}

impl From<JoinRequest> for pb::JoinClusterRequest {
    fn from(r: JoinRequest) -> Self {
        pb::JoinClusterRequest {
            token: r.token,
            csr_pem: r.csr_pem,
            advertise: Some(r.advertise.into()),
            node_version: Some(r.node_version.into()),
            resume_node_id: r.resume_node_id,
        }
    }
}

impl TryFrom<pb::JoinClusterRequest> for JoinRequest {
    type Error = ApiError;

    fn try_from(r: pb::JoinClusterRequest) -> Result<Self, ApiError> {
        Ok(JoinRequest {
            token: r.token,
            csr_pem: r.csr_pem,
            advertise: required("advertise", r.advertise)?.into(),
            node_version: required("node_version", r.node_version)?.into(),
            resume_node_id: r.resume_node_id,
        })
    }
}

impl From<SeedDto> for pb::SeedNode {
    fn from(s: SeedDto) -> Self {
        pb::SeedNode {
            id: s.id,
            addr: s.addr,
            repl_addr: s.repl_addr,
        }
    }
}

impl From<pb::SeedNode> for SeedDto {
    fn from(s: pb::SeedNode) -> Self {
        SeedDto {
            id: s.id,
            addr: s.addr,
            repl_addr: s.repl_addr,
        }
    }
}

impl From<JoinResponse> for pb::JoinClusterResponse {
    fn from(r: JoinResponse) -> Self {
        pb::JoinClusterResponse {
            node_id: r.node_id,
            cluster_id: r.cluster_id,
            partitions: u32::from(r.partitions),
            ca_cert_pem: r.ca_cert_pem,
            node_cert_pem: r.node_cert_pem,
            seeds: r.seeds.into_iter().map(Into::into).collect(),
            finalized_level: r.finalized_level,
        }
    }
}

impl TryFrom<pb::JoinClusterResponse> for JoinResponse {
    type Error = ApiError;

    fn try_from(r: pb::JoinClusterResponse) -> Result<Self, ApiError> {
        Ok(JoinResponse {
            node_id: r.node_id,
            cluster_id: r.cluster_id,
            partitions: partition_u16("partitions", r.partitions)?,
            ca_cert_pem: r.ca_cert_pem,
            node_cert_pem: r.node_cert_pem,
            seeds: r.seeds.into_iter().map(Into::into).collect(),
            finalized_level: r.finalized_level,
        })
    }
}

impl From<NodeTokenResponse> for pb::NodeTokenCreated {
    fn from(t: NodeTokenResponse) -> Self {
        pb::NodeTokenCreated {
            id: t.id,
            token: t.token,
            expires_at_ms: t.expires_at_ms,
            ca_hash: t.ca_hash,
        }
    }
}

impl From<pb::NodeTokenCreated> for NodeTokenResponse {
    fn from(t: pb::NodeTokenCreated) -> Self {
        NodeTokenResponse {
            id: t.id,
            token: t.token,
            expires_at_ms: t.expires_at_ms,
            ca_hash: t.ca_hash,
        }
    }
}

impl From<ClusterMember> for pb::ClusterMember {
    fn from(m: ClusterMember) -> Self {
        pb::ClusterMember {
            node_id: m.node_id,
            addr: m.addr,
            cluster_addr: m.cluster_addr,
            state: m.state,
            version: m.version.map(Into::into),
            live: m.live,
            raft_role: m.raft_role,
        }
    }
}

impl From<pb::ClusterMember> for ClusterMember {
    fn from(m: pb::ClusterMember) -> Self {
        ClusterMember {
            node_id: m.node_id,
            addr: m.addr,
            cluster_addr: m.cluster_addr,
            state: m.state,
            version: m.version.map(Into::into),
            live: m.live,
            raft_role: m.raft_role,
        }
    }
}

impl From<ClusterStatus> for pb::ClusterStatus {
    fn from(s: ClusterStatus) -> Self {
        pb::ClusterStatus {
            cluster_id: s.cluster_id,
            members: s.members.into_iter().map(Into::into).collect(),
            voters: s.voters,
            learners: s.learners,
            voters_outgoing: s.voters_outgoing,
            meta_leader: s.meta_leader,
            finalized: s.finalized,
            hold: s.hold,
            level_min: s.level_min,
            level_max: s.level_max,
            policy: s.policy.map(|p| pb::ClusterReplicationPolicy {
                rf: u32::from(p.rf),
                min_insync: u32::from(p.min_insync),
                max_voters: u32::from(p.max_voters),
            }),
            reassignments: s
                .reassignments
                .into_iter()
                .map(|r| pb::PartitionReassignment {
                    partition: u32::from(r.partition),
                    target: r.target,
                    adding: r.adding,
                    removing: r.removing,
                    started_at_ms: r.started_at_ms,
                })
                .collect(),
        }
    }
}

impl TryFrom<pb::ClusterStatus> for ClusterStatus {
    type Error = ApiError;

    fn try_from(s: pb::ClusterStatus) -> Result<Self, ApiError> {
        Ok(ClusterStatus {
            cluster_id: s.cluster_id,
            members: s.members.into_iter().map(Into::into).collect(),
            voters: s.voters,
            learners: s.learners,
            voters_outgoing: s.voters_outgoing,
            meta_leader: s.meta_leader,
            finalized: s.finalized,
            hold: s.hold,
            level_min: s.level_min,
            level_max: s.level_max,
            policy: s
                .policy
                .map(|p| {
                    Ok::<_, ApiError>(ReplicationPolicyDto {
                        rf: small("policy.rf", p.rf)?,
                        min_insync: small("policy.min_insync", p.min_insync)?,
                        max_voters: small("policy.max_voters", p.max_voters)?,
                    })
                })
                .transpose()?,
            reassignments: s
                .reassignments
                .into_iter()
                .map(|r| {
                    Ok(ReassignmentDto {
                        partition: partition_u16("reassignments.partition", r.partition)?,
                        target: r.target,
                        adding: r.adding,
                        removing: r.removing,
                        started_at_ms: r.started_at_ms,
                    })
                })
                .collect::<Result<_, ApiError>>()?,
        })
    }
}

impl From<ReassignRequest> for pb::ReassignPartitionRequest {
    fn from(r: ReassignRequest) -> Self {
        pb::ReassignPartitionRequest {
            partition: u32::from(r.partition),
            replicas: r.replicas,
            cancel: r.cancel,
        }
    }
}

impl TryFrom<pb::ReassignPartitionRequest> for ReassignRequest {
    type Error = ApiError;

    fn try_from(r: pb::ReassignPartitionRequest) -> Result<Self, ApiError> {
        Ok(ReassignRequest {
            partition: partition_u16("partition", r.partition)?,
            replicas: r.replicas,
            cancel: r.cancel,
        })
    }
}

impl From<UpgradeStatus> for pb::UpgradeStatus {
    fn from(u: UpgradeStatus) -> Self {
        pb::UpgradeStatus {
            finalized: u.finalized,
            hold: u.hold,
            common_level_max: u.common_level_max,
            auto_finalize: u.auto_finalize,
            members: u
                .members
                .into_iter()
                .map(|m| pb::MemberLevel {
                    node_id: m.node_id,
                    level_min: m.level_min,
                    level_max: m.level_max,
                    live: m.live,
                })
                .collect(),
        }
    }
}

impl From<pb::UpgradeStatus> for UpgradeStatus {
    fn from(u: pb::UpgradeStatus) -> Self {
        UpgradeStatus {
            finalized: u.finalized,
            hold: u.hold,
            common_level_max: u.common_level_max,
            auto_finalize: u.auto_finalize,
            members: u
                .members
                .into_iter()
                .map(|m| MemberLevel {
                    node_id: m.node_id,
                    level_min: m.level_min,
                    level_max: m.level_max,
                    live: m.live,
                })
                .collect(),
        }
    }
}

impl From<RetireModuleResponse> for pb::RetireModuleResponse {
    fn from(r: RetireModuleResponse) -> Self {
        pb::RetireModuleResponse {
            sha256: r.sha256,
            retired_at_ms: r.retired_at_ms,
            versions: r
                .versions
                .into_iter()
                .map(|v| pb::WorkflowVersionRef {
                    name: v.name,
                    version: v.version,
                })
                .collect(),
        }
    }
}

impl From<pb::RetireModuleResponse> for RetireModuleResponse {
    fn from(r: pb::RetireModuleResponse) -> Self {
        RetireModuleResponse {
            sha256: r.sha256,
            retired_at_ms: r.retired_at_ms,
            versions: r
                .versions
                .into_iter()
                .map(|v| WorkflowVersionRef {
                    name: v.name,
                    version: v.version,
                })
                .collect(),
        }
    }
}

impl From<GcRunReport> for pb::GcRunReport {
    fn from(r: GcRunReport) -> Self {
        pb::GcRunReport {
            candidates: r.candidates,
            deleted: r.deleted,
            referenced: r.referenced,
            postponed: r.postponed,
            reason: r.reason,
        }
    }
}

impl From<pb::GcRunReport> for GcRunReport {
    fn from(r: pb::GcRunReport) -> Self {
        GcRunReport {
            candidates: r.candidates,
            deleted: r.deleted,
            referenced: r.referenced,
            postponed: r.postponed,
            reason: r.reason,
        }
    }
}

impl From<MigrateRunsRequest> for pb::MigrateRunsRequest {
    fn from(r: MigrateRunsRequest) -> Self {
        use pb::migrate_runs_request::From as F;
        pb::MigrateRunsRequest {
            workflow: r.workflow,
            from: Some(match r.from {
                VersionOrSha::Version(v) => F::FromVersion(v),
                VersionOrSha::Sha(s) => F::FromSha(s),
            }),
            to_version: r.to_version,
            filter: r.filter,
            limit: r.limit,
            dry_run: r.dry_run,
        }
    }
}

impl TryFrom<pb::MigrateRunsRequest> for MigrateRunsRequest {
    type Error = ApiError;

    fn try_from(r: pb::MigrateRunsRequest) -> Result<Self, ApiError> {
        use pb::migrate_runs_request::From as F;
        Ok(MigrateRunsRequest {
            workflow: r.workflow,
            from: match required("from", r.from)? {
                F::FromVersion(v) => VersionOrSha::Version(v),
                F::FromSha(s) => VersionOrSha::Sha(s),
            },
            to_version: r.to_version,
            filter: r.filter,
            limit: r.limit,
            dry_run: r.dry_run,
        })
    }
}

impl From<MigrateRunsReport> for pb::MigrateRunsReport {
    fn from(r: MigrateRunsReport) -> Self {
        pb::MigrateRunsReport {
            passed: r.passed,
            repinned: r.repinned,
            failed: r
                .failed
                .into_iter()
                .map(|f| pb::MigrateFailure {
                    run_id: f.run_id,
                    seq: f.seq,
                    detail: f.detail,
                })
                .collect(),
            busy: r.busy,
            seq_mismatch: r.seq_mismatch,
            already_on_target: r.already_on_target,
        }
    }
}

impl From<pb::MigrateRunsReport> for MigrateRunsReport {
    fn from(r: pb::MigrateRunsReport) -> Self {
        MigrateRunsReport {
            passed: r.passed,
            repinned: r.repinned,
            failed: r
                .failed
                .into_iter()
                .map(|f| MigrateFailure {
                    run_id: f.run_id,
                    seq: f.seq,
                    detail: f.detail,
                })
                .collect(),
            busy: r.busy,
            seq_mismatch: r.seq_mismatch,
            already_on_target: r.already_on_target,
        }
    }
}

// ---------------------------------------------------------------------------------
// gRPC: ostro.api.v1.{Cluster, ModuleLifecycle}
// ---------------------------------------------------------------------------------

type GrpcResult<T> = Result<tonic::Response<T>, tonic::Status>;

fn ok<T>(v: T) -> GrpcResult<T> {
    Ok(tonic::Response::new(v))
}

fn st(e: ApiError) -> tonic::Status {
    crate::grpc::to_status(e)
}

/// `ostro.api.v1.Cluster` over a [`ClusterApi`] (same validation as REST).
#[derive(Clone)]
pub struct GrpcCluster(pub Arc<dyn ClusterApi>);

/// `ostro.api.v1.ModuleLifecycle` over a [`ModuleLifecycleApi`].
#[derive(Clone)]
pub struct GrpcModuleLifecycle(pub Arc<dyn ModuleLifecycleApi>);

#[tonic::async_trait]
impl cluster_server::Cluster for GrpcCluster {
    async fn init(
        &self,
        r: tonic::Request<pb::InitClusterRequest>,
    ) -> GrpcResult<pb::ClusterStatus> {
        let req = init_from_proto(r.into_inner()).map_err(st)?;
        ok(self.0.init(req).await.map_err(st)?.into())
    }

    async fn join(
        &self,
        r: tonic::Request<pb::JoinClusterRequest>,
    ) -> GrpcResult<pb::JoinClusterResponse> {
        let req = JoinRequest::try_from(r.into_inner()).map_err(st)?;
        ok(self.0.join(req).await.map_err(st)?.into())
    }

    async fn status(
        &self,
        _r: tonic::Request<pb::ClusterStatusRequest>,
    ) -> GrpcResult<pb::ClusterStatus> {
        ok(self.0.status().await.map_err(st)?.into())
    }

    async fn create_node_token(
        &self,
        r: tonic::Request<pb::CreateNodeTokenRequest>,
    ) -> GrpcResult<pb::NodeTokenCreated> {
        let req = NodeTokenRequest {
            ttl_secs: r.into_inner().ttl_secs,
        };
        ok(self.0.create_node_token(req).await.map_err(st)?.into())
    }

    async fn remove(
        &self,
        r: tonic::Request<pb::RemoveMemberRequest>,
    ) -> GrpcResult<pb::ClusterStatus> {
        let r = r.into_inner();
        check::node_id(r.node_id).map_err(st)?;
        ok(self.0.remove(r.node_id, r.force).await.map_err(st)?.into())
    }

    async fn drain(
        &self,
        r: tonic::Request<pb::DrainMemberRequest>,
    ) -> GrpcResult<pb::ClusterStatus> {
        let id = r.into_inner().node_id;
        check::node_id(id).map_err(st)?;
        ok(self.0.drain(id).await.map_err(st)?.into())
    }

    async fn reassign(
        &self,
        r: tonic::Request<pb::ReassignPartitionRequest>,
    ) -> GrpcResult<pb::ClusterStatus> {
        let req = ReassignRequest::try_from(r.into_inner()).map_err(st)?;
        check::reassign(&req).map_err(st)?;
        ok(self.0.reassign(req).await.map_err(st)?.into())
    }

    async fn rebalance(
        &self,
        _r: tonic::Request<pb::RebalanceRequest>,
    ) -> GrpcResult<pb::ClusterStatus> {
        ok(self.0.rebalance().await.map_err(st)?.into())
    }

    async fn upgrade_status(
        &self,
        _r: tonic::Request<pb::UpgradeStatusRequest>,
    ) -> GrpcResult<pb::UpgradeStatus> {
        ok(self.0.upgrade_status().await.map_err(st)?.into())
    }

    async fn upgrade_finalize(
        &self,
        _r: tonic::Request<pb::UpgradeFinalizeRequest>,
    ) -> GrpcResult<pb::UpgradeStatus> {
        ok(self.0.upgrade_finalize().await.map_err(st)?.into())
    }

    async fn upgrade_hold(
        &self,
        _r: tonic::Request<pb::UpgradeHoldRequest>,
    ) -> GrpcResult<pb::UpgradeStatus> {
        ok(self.0.upgrade_hold().await.map_err(st)?.into())
    }

    async fn upgrade_release(
        &self,
        _r: tonic::Request<pb::UpgradeReleaseRequest>,
    ) -> GrpcResult<pb::UpgradeStatus> {
        ok(self.0.upgrade_release().await.map_err(st)?.into())
    }
}

#[tonic::async_trait]
impl module_lifecycle_server::ModuleLifecycle for GrpcModuleLifecycle {
    async fn retire_module(
        &self,
        r: tonic::Request<pb::RetireModuleRequest>,
    ) -> GrpcResult<pb::RetireModuleResponse> {
        let sha = r.into_inner().sha256;
        check::sha256("sha256", &sha).map_err(st)?;
        ok(self.0.retire_module(sha).await.map_err(st)?.into())
    }

    async fn run_module_gc(
        &self,
        _r: tonic::Request<pb::RunModuleGcRequest>,
    ) -> GrpcResult<pb::GcRunReport> {
        ok(self.0.run_module_gc().await.map_err(st)?.into())
    }

    async fn migrate_runs(
        &self,
        r: tonic::Request<pb::MigrateRunsRequest>,
    ) -> GrpcResult<pb::MigrateRunsReport> {
        let req = MigrateRunsRequest::try_from(r.into_inner()).map_err(st)?;
        check::migrate(&req).map_err(st)?;
        ok(self.0.migrate_runs(req).await.map_err(st)?.into())
    }
}

/// Mount `Cluster` / `ModuleLifecycle` for the slots that are present (absent = not
/// mounted, so clients see `UNIMPLEMENTED`, as for Phases 8 and 11).
pub(crate) fn add_grpc_services(
    mut routes: tonic::service::Routes,
    ext: &ApiExtensions,
    max_request_bytes: usize,
) -> tonic::service::Routes {
    if let Some(c) = &ext.cluster {
        routes = routes.add_service(
            cluster_server::ClusterServer::new(GrpcCluster(Arc::clone(c)))
                .max_decoding_message_size(max_request_bytes),
        );
    }
    if let Some(m) = &ext.module_lifecycle {
        routes = routes.add_service(
            module_lifecycle_server::ModuleLifecycleServer::new(GrpcModuleLifecycle(Arc::clone(m)))
                .max_decoding_message_size(max_request_bytes),
        );
    }
    routes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status() -> ClusterStatus {
        ClusterStatus {
            cluster_id: "c".into(),
            members: vec![ClusterMember {
                node_id: 1,
                addr: "a:1".into(),
                cluster_addr: Some("a:2".into()),
                state: "active".into(),
                version: Some(NodeVersionDto {
                    ostro_version: "0.2.0".into(),
                    level_min: 1,
                    level_max: 2,
                    wasmtime: "50".into(),
                    event_codec_ver: 1,
                    wit_lines: vec!["0.1".into()],
                }),
                live: true,
                raft_role: "voter".into(),
            }],
            voters: vec![1],
            learners: vec![2],
            voters_outgoing: vec![],
            meta_leader: 1,
            finalized: 1,
            hold: Some(1),
            level_min: 1,
            level_max: 2,
            policy: Some(ReplicationPolicyDto {
                rf: 3,
                min_insync: 2,
                max_voters: 5,
            }),
            reassignments: vec![ReassignmentDto {
                partition: 9,
                target: vec![1, 2],
                adding: vec![2],
                removing: vec![3],
                started_at_ms: 5,
            }],
        }
    }

    #[test]
    fn conversions_round_trip() {
        let s = status();
        assert_eq!(
            ClusterStatus::try_from(pb::ClusterStatus::from(s.clone())).expect("status"),
            s
        );
        let j = JoinRequest {
            token: "njt_x".into(),
            csr_pem: "csr".into(),
            advertise: Advertise {
                api: "a".into(),
                cluster: "b".into(),
                repl: Some("c".into()),
            },
            node_version: NodeVersionDto::default(),
            resume_node_id: Some(4),
        };
        assert_eq!(
            JoinRequest::try_from(pb::JoinClusterRequest::from(j.clone())).expect("join"),
            j
        );
        for from in [VersionOrSha::Version(2), VersionOrSha::Sha("ab".into())] {
            let m = MigrateRunsRequest {
                workflow: "w".into(),
                from,
                to_version: 3,
                filter: Some("t".into()),
                limit: Some(5),
                dry_run: true,
            };
            assert_eq!(
                MigrateRunsRequest::try_from(pb::MigrateRunsRequest::from(m.clone()))
                    .expect("migrate"),
                m
            );
        }
        let i = InitRequest {
            advertise: Some("x".into()),
            rf: Some(3),
            min_insync: None,
        };
        assert_eq!(init_from_proto(init_to_proto(i.clone())).expect("init"), i);
    }

    #[test]
    fn out_of_range_and_missing_fields_are_invalid() {
        let e = init_from_proto(pb::InitClusterRequest {
            rf: Some(300),
            ..Default::default()
        })
        .unwrap_err();
        assert_eq!(e.code, crate::ErrorCode::InvalidArgument);
        assert!(
            ReassignRequest::try_from(pb::ReassignPartitionRequest {
                partition: 70_000,
                ..Default::default()
            })
            .is_err()
        );
        assert!(MigrateRunsRequest::try_from(pb::MigrateRunsRequest::default()).is_err());
        assert!(JoinRequest::try_from(pb::JoinClusterRequest::default()).is_err());
    }

    #[test]
    fn checks() {
        assert!(check::sha256("s", &"a".repeat(64)).is_ok());
        assert!(check::sha256("s", &"A".repeat(64)).is_err());
        assert!(check::sha256("s", "ab").is_err());
        assert!(check::node_id(0).is_err());
        let mut r = ReassignRequest {
            partition: 1,
            replicas: vec![],
            cancel: false,
        };
        assert!(check::reassign(&r).is_err());
        r.replicas = vec![1, 1];
        assert!(check::reassign(&r).is_err());
        r.replicas = vec![1, 2];
        assert!(check::reassign(&r).is_ok());
        r.replicas = vec![];
        r.cancel = true;
        assert!(check::reassign(&r).is_ok());
    }
}
