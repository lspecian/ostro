//! The `ostro.cluster.v1.NodeOps` service (D-P13-23) on the cluster listener: `HandOff`
//! (13-11's [`HandoffExecutor`]) and `ModuleRefs` (13-15's [`ModuleRefsSource`]), node
//! identities only (`authz::require_node`).
//!
//! 13-02 wrote the delegation; 13-03 swapped its stand-in message shapes for the generated
//! `ostro.cluster.v1` types (`ostro/cluster/v1/membership.proto`) and mounts the tonic
//! server ([`NodeOpsService::routes`], attached through `cluster_listener_services`).

use std::sync::Arc;

use ostro_cluster::authz;
use ostro_cluster::contracts::{HandoffError, HandoffExecutor, HandoffOutcome, ModuleRefsSource};
use ostro_types::{Epoch, PartitionId};
use tonic::service::Routes;
use tonic::{Request, Response, Status};

use ostro_api::proto::cluster::v1::node_ops_server::{NodeOps, NodeOpsServer};
pub use ostro_api::proto::cluster::v1::{
    HandOffRequest, HandOffResponse, ModuleRefsRequest, ModuleRefsResponse, PartitionEpoch,
};

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
}

impl std::fmt::Debug for NodeOpsService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeOpsService").finish_non_exhaustive()
    }
}

impl NodeOpsService {
    /// A service over the local handoff executor and module-refs source.
    pub fn new(handoff: Arc<dyn HandoffExecutor>, refs: Arc<dyn ModuleRefsSource>) -> Self {
        Self { handoff, refs }
    }

    /// The tonic routes of the `NodeOps` server.
    pub fn routes(self) -> Option<Routes> {
        Some(Routes::new(NodeOpsServer::new(self)))
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
