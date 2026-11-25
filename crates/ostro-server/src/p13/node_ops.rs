//! The `ostro.cluster.v1.NodeOps` service (D-P13-23) on the cluster listener: `HandOff`
//! (13-11's [`HandoffExecutor`]), `ModuleRefs` (13-15's [`ModuleRefsSource`]) and
//! `StartRestart` (p13-follow, [`RestartServer`]: a keyed restart's new run on the
//! partition this node leads, `p13::keyed_start`) and `DeliverInbound` (p13-bugs2,
//! [`InboundServer`]: an outbox batch for a partition this node leads, `p13::inbound`),
//! node identities only (`authz::require_node`).
//!
//! 13-02 wrote the delegation; 13-03 swapped its stand-in message shapes for the generated
//! `ostro.cluster.v1` types (`ostro/cluster/v1/membership.proto`) and mounts the tonic
//! server ([`NodeOpsService::routes`], attached through `cluster_listener_services`).

use std::sync::{Arc, Weak};

use ostro_cluster::authz;
use ostro_cluster::contracts::{HandoffError, HandoffExecutor, HandoffOutcome, ModuleRefsSource};
use ostro_types::{Epoch, PartitionId};
use tonic::service::Routes;
use tonic::{Request, Response, Status};

use ostro_api::proto::cluster::v1::node_ops_server::{NodeOps, NodeOpsServer};
pub use ostro_api::proto::cluster::v1::{
    DeliverInboundRequest, DeliverInboundResponse, HandOffRequest, HandOffResponse,
    ModuleRefsRequest, ModuleRefsResponse, PartitionEpoch, StartRestartRequest,
    StartRestartResponse,
};

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// `NodeOps.StartRestart` behind the service (p13-follow).
pub trait RestartServer: Send + Sync + 'static {
    /// Serve one request on this node.
    fn start_restart(
        &self,
        req: StartRestartRequest,
    ) -> BoxFuture<'_, Result<StartRestartResponse, ostro_api::ApiError>>;
}

/// `NodeOps.DeliverInbound` behind the service (p13-bugs2, `p13::inbound`).
pub trait InboundServer: Send + Sync + 'static {
    /// Serve one request on this node; `Status` answers make the sender resend later.
    fn deliver_inbound(
        &self,
        req: DeliverInboundRequest,
    ) -> BoxFuture<'_, Result<DeliverInboundResponse, Status>>;
}

/// Production [`RestartServer`]: the partition-write check (leading + the lease, as a
/// routed write), then `keyed_start::serve` on the local control plane.
pub struct NodeRestarts {
    ctx: Weak<super::P13Ctx>,
}

impl NodeRestarts {
    /// Over `ctx` (weak: the cluster listener outlives the node).
    pub fn new(ctx: &Arc<super::P13Ctx>) -> Self {
        Self {
            ctx: Arc::downgrade(ctx),
        }
    }
}

impl RestartServer for NodeRestarts {
    fn start_restart(
        &self,
        req: StartRestartRequest,
    ) -> BoxFuture<'_, Result<StartRestartResponse, ostro_api::ApiError>> {
        Box::pin(async move {
            let ctx = self
                .ctx
                .upgrade()
                .ok_or_else(|| ostro_api::ApiError::unavailable("the node is shutting down"))?;
            if let Ok(p) = u16::try_from(req.partition) {
                crate::route::consistency::before_partition_write(&ctx, PartitionId(p)).await?;
            }
            super::keyed_start::serve(&ctx.local_cp, req).await
        })
    }
}

fn partition(p: u32) -> Result<PartitionId, Status> {
    u16::try_from(p)
        .map(PartitionId)
        .map_err(|_| Status::invalid_argument(format!("partition {p} out of range")))
}

fn handoff_status(e: HandoffError) -> Status {
    match e {
        HandoffError::NotLeader { .. } | HandoffError::TargetNotInIsr { .. } => {
            Status::failed_precondition(e.to_string())
        }
        HandoffError::Transport(_) | HandoffError::Meta(_) => Status::unavailable(e.to_string()),
    }
}

/// The NodeOps service: delegation only.
#[derive(Clone)]
pub struct NodeOpsService {
    handoff: Arc<dyn HandoffExecutor>,
    refs: Arc<dyn ModuleRefsSource>,
    restarts: Option<Arc<dyn RestartServer>>,
    inbound: Option<Arc<dyn InboundServer>>,
    max_message_bytes: Option<usize>,
}

impl std::fmt::Debug for NodeOpsService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeOpsService").finish_non_exhaustive()
    }
}

impl NodeOpsService {
    /// A service over the local handoff executor and module-refs source.
    pub fn new(handoff: Arc<dyn HandoffExecutor>, refs: Arc<dyn ModuleRefsSource>) -> Self {
        Self {
            handoff,
            refs,
            restarts: None,
            inbound: None,
            max_message_bytes: None,
        }
    }

    /// p13-follow: serve `StartRestart` through `restarts`; messages up to `max` bytes (a
    /// StartRun with inline payloads). Without it `StartRestart` is `unimplemented`.
    pub fn with_restarts(mut self, restarts: Arc<dyn RestartServer>, max: usize) -> Self {
        self.restarts = Some(restarts);
        self.max_message_bytes = Some(max);
        self
    }

    /// p13-bugs2: serve `DeliverInbound` through `inbound` (messages up to the
    /// [`NodeOpsService::with_restarts`] bound). Without it `DeliverInbound` is
    /// `unimplemented` (the sender keeps resending).
    pub fn with_inbound(mut self, inbound: Arc<dyn InboundServer>) -> Self {
        self.inbound = Some(inbound);
        self
    }

    /// `NodeOps.DeliverInbound`: an outbox batch for a partition this node leads.
    pub async fn deliver_inbound(
        &self,
        req: Request<DeliverInboundRequest>,
    ) -> Result<Response<DeliverInboundResponse>, Status> {
        authz::require_node(&req)?;
        let Some(i) = &self.inbound else {
            return Err(Status::unimplemented(
                "DeliverInbound is not served by this node",
            ));
        };
        i.deliver_inbound(req.into_inner()).await.map(Response::new)
    }

    /// The tonic routes of the `NodeOps` server.
    pub fn routes(self) -> Option<Routes> {
        let max = self.max_message_bytes;
        let mut server = NodeOpsServer::new(self);
        if let Some(m) = max {
            server = server
                .max_decoding_message_size(m)
                .max_encoding_message_size(m);
        }
        Some(Routes::new(server))
    }

    /// `NodeOps.StartRestart`: a keyed restart's new run on the partition this node leads
    /// (`p13::keyed_start`). Errors keep the API error (`not_leader` with its details).
    pub async fn start_restart(
        &self,
        req: Request<StartRestartRequest>,
    ) -> Result<Response<StartRestartResponse>, Status> {
        authz::require_node(&req)?;
        let Some(r) = &self.restarts else {
            return Err(Status::unimplemented(
                "StartRestart is not served by this node",
            ));
        };
        r.start_restart(req.into_inner())
            .await
            .map(Response::new)
            .map_err(ostro_api::grpc::to_status)
    }

    /// `NodeOps.HandOff`: this node (the partition leader) hands `partition` to `to`.
    pub async fn hand_off(
        &self,
        req: Request<HandOffRequest>,
    ) -> Result<Response<HandOffResponse>, Status> {
        authz::require_node(&req)?;
        let r = req.into_inner();
        let out = self
            .handoff
            .execute(partition(r.partition)?, r.to, Epoch(r.epoch))
            .await
            .map_err(handoff_status)?;
        Ok(Response::new(match out {
            HandoffOutcome::Moved { new_epoch } => HandOffResponse {
                moved: true,
                new_epoch: new_epoch.0,
                abort_reason: String::new(),
            },
            HandoffOutcome::Aborted { reason } => HandOffResponse {
                moved: false,
                new_epoch: 0,
                abort_reason: reason,
            },
        }))
    }

    /// `NodeOps.ModuleRefs`: which `shas` the partitions this node leads still reference.
    pub async fn module_refs(
        &self,
        req: Request<ModuleRefsRequest>,
    ) -> Result<Response<ModuleRefsResponse>, Status> {
        authz::require_node(&req)?;
        let r = req.into_inner();
        let shas = r
            .shas
            .iter()
            .map(|s| {
                <[u8; 32]>::try_from(s.as_slice())
                    .map_err(|_| Status::invalid_argument("module sha must be 32 bytes"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let parts = r
            .partitions
            .iter()
            .map(|pe| Ok((partition(pe.partition)?, Epoch(pe.epoch))))
            .collect::<Result<Vec<_>, Status>>()?;
        let answer = self
            .refs
            .referenced(shas, parts, r.protect_resumable)
            .await
            .map_err(Status::internal)?;
        Ok(Response::new(ModuleRefsResponse {
            referenced: answer.referenced.iter().map(|s| s.to_vec()).collect(),
            uncovered: answer
                .uncovered
                .iter()
                .map(|(p, e)| PartitionEpoch {
                    partition: u32::from(p.0),
                    epoch: e.0,
                })
                .collect(),
        }))
    }
}

#[tonic::async_trait]
impl NodeOps for NodeOpsService {
    async fn hand_off(
        &self,
        req: Request<HandOffRequest>,
    ) -> Result<Response<HandOffResponse>, Status> {
        NodeOpsService::hand_off(self, req).await
    }

    async fn module_refs(
        &self,
        req: Request<ModuleRefsRequest>,
    ) -> Result<Response<ModuleRefsResponse>, Status> {
        NodeOpsService::module_refs(self, req).await
    }

    async fn start_restart(
        &self,
        req: Request<StartRestartRequest>,
    ) -> Result<Response<StartRestartResponse>, Status> {
        NodeOpsService::start_restart(self, req).await
    }

    async fn deliver_inbound(
        &self,
        req: Request<DeliverInboundRequest>,
    ) -> Result<Response<DeliverInboundResponse>, Status> {
        NodeOpsService::deliver_inbound(self, req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_cluster::contracts::RefsAnswer;

    type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

    struct Moves;
    impl HandoffExecutor for Moves {
        fn execute(
            &self,
            p: PartitionId,
            to: u64,
            epoch: Epoch,
        ) -> BoxFuture<'_, Result<HandoffOutcome, HandoffError>> {
            Box::pin(async move {
                if to == 9 {
                    return Err(HandoffError::TargetNotInIsr {
                        partition: p.0,
                        target: to,
                    });
                }
                Ok(HandoffOutcome::Moved {
                    new_epoch: Epoch(epoch.0 + 1),
                })
            })
        }
    }

    struct Refs;
    impl ModuleRefsSource for Refs {
        fn referenced(
            &self,
            shas: Vec<[u8; 32]>,
            partitions: Vec<(PartitionId, Epoch)>,
            _protect_resumable: bool,
        ) -> BoxFuture<'_, Result<RefsAnswer, String>> {
            Box::pin(async move {
                Ok(RefsAnswer {
                    referenced: shas,
                    uncovered: partitions,
                })
            })
        }
    }

    fn svc() -> NodeOpsService {
        NodeOpsService::new(Arc::new(Moves), Arc::new(Refs))
    }

    /// Echoes the partition into the run id; partition 7 is led elsewhere.
    struct Restarts;
    impl RestartServer for Restarts {
        fn start_restart(
            &self,
            req: StartRestartRequest,
        ) -> BoxFuture<'_, Result<StartRestartResponse, ostro_api::ApiError>> {
            Box::pin(async move {
                if req.partition == 7 {
                    return Err(
                        ostro_api::ApiError::unavailable("partition 7: not the leader")
                            .with_details(
                                serde_json::json!({ "reason": "not_leader", "partition": 7 }),
                            ),
                    );
                }
                Ok(StartRestartResponse {
                    run_id: vec![u8::try_from(req.partition).unwrap_or(0)],
                    existing: req.lookup_only,
                    rejected: String::new(),
                })
            })
        }
    }

    #[tokio::test]
    async fn start_restart_needs_a_node_and_keeps_the_api_error() {
        let plain = svc();
        let e = plain
            .start_restart(as_node(StartRestartRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::Unimplemented, "not wired: {e:?}");
        let s = svc().with_restarts(Arc::new(Restarts), 1 << 20);
        let e = s
            .start_restart(Request::new(StartRestartRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::Unauthenticated);
        let r = s
            .start_restart(as_node(StartRestartRequest {
                partition: 3,
                lookup_only: true,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!((r.run_id, r.existing), (vec![3], true));
        // A not_leader refusal crosses the wire with its reason, so the caller re-routes.
        let st = s
            .start_restart(as_node(StartRestartRequest {
                partition: 7,
                ..Default::default()
            }))
            .await
            .unwrap_err();
        let e = ostro_api::grpc_client::status_to_error(&st);
        assert_eq!(e.reason(), Some("not_leader"), "{e:?}");
        assert!(crate::route::retryable(&e, true));
        assert!(s.routes().is_some());
    }

    fn as_node<T>(m: T) -> Request<T> {
        let mut r = Request::new(m);
        r.extensions_mut()
            .insert(ostro_cluster::transport::PeerNode(2));
        r
    }

    #[tokio::test]
    async fn requires_a_node_identity() {
        let e = svc()
            .hand_off(Request::new(HandOffRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::Unauthenticated);
        let e = svc()
            .module_refs(Request::new(ModuleRefsRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::Unauthenticated);
        assert!(svc().routes().is_some(), "mounted since 13-03");
    }

    #[tokio::test]
    async fn delegates_hand_off_and_module_refs() {
        let r = svc()
            .hand_off(as_node(HandOffRequest {
                partition: 3,
                to: 2,
                epoch: 4,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!((r.moved, r.new_epoch), (true, 5));
        let e = svc()
            .hand_off(as_node(HandOffRequest {
                partition: 3,
                to: 9,
                epoch: 4,
            }))
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::FailedPrecondition);
        let e = svc()
            .hand_off(as_node(HandOffRequest {
                partition: 70_000,
                to: 2,
                epoch: 4,
            }))
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument);
        let r = svc()
            .module_refs(as_node(ModuleRefsRequest {
                shas: vec![vec![7; 32]],
                partitions: vec![PartitionEpoch {
                    partition: 1,
                    epoch: 2,
                }],
                protect_resumable: true,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r.referenced, vec![vec![7u8; 32]]);
        assert_eq!(
            r.uncovered,
            vec![PartitionEpoch {
                partition: 1,
                epoch: 2
            }]
        );
        let e = svc()
            .module_refs(as_node(ModuleRefsRequest {
                shas: vec![vec![1; 3]],
                ..ModuleRefsRequest::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument);
    }
}
