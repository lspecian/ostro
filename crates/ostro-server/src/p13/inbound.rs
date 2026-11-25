//! Cross-node delivery of outbox batches (p13-bugs2 bug 3): the network half of
//! `ostro_partition::router::PartitionRouter` that `router.rs` promised for Phase 13.
//!
//! A child run's `StartChild`, its parent's `ChildStarted` / `ChildCompleted` and every
//! other cross-partition message travel through the source partition's outbox; the
//! shuffler hands each batch to the node's router. Until p13-bugs2 that router was only
//! [`InProcessRouter`]: a target partition led by another node answered `NotLeader`
//! forever, so the child never started or the parent never heard back.
//!
//! [`ClusterRouter`] wraps the node's [`InProcessRouter`] (`NodeConfig::router_wrapper`):
//!
//! - no Phase 13 context yet, or the routing view says this node leads the target (or
//!   nobody does): the in-process router, exactly the single-node path (no copy, no hop);
//! - another node leads it: `NodeOps.DeliverInbound` on that node (cluster listener, node
//!   identities only) with the batch as a frozen partition `Command`. The leader runs the partition-write
//!   check and delivers through **its own in-process router only** (never forwarded again,
//!   so no loops), and answers the apply outcome code.
//!
//! Exactly-once is unchanged: the target dedupes `(src, seq)` against its replicated
//! `inbound_seen` and rejects gaps whichever path delivered, and the shuffler truncates its
//! outbox only after `Accepted` / `Duplicate`. A lost answer (timeout, transport error,
//! leader moved) is a failed round: the shuffler resends the same rows from its table, and
//! the target answers `Duplicate` for what it already applied. A rejection on the target
//! (a shuffler bug or a malformed row) is logged here and reported as a failed delivery.

use std::sync::{Arc, Weak};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use ostro_api::proto::cluster::v1::node_ops_client::NodeOpsClient;
use ostro_partition::node::RouterWrapper;
use ostro_partition::router::{InProcessRouter, PartitionRouter, RouteFuture};
use ostro_partition::{ApplyOutcome, PartitionError};
use ostro_types::Command;
use ostro_types::PartitionId;
use ostro_types::command::{Cmd, InboundBatch, decode_command, encode_command};
use tonic::Status;
use tracing::{debug, error};

use super::P13Ctx;
use super::node_ops::{DeliverInboundRequest, DeliverInboundResponse, InboundServer};
use crate::route::table::Target;

/// Bound on one `NodeOps.DeliverInbound` call (a failed round; the shuffler resends).
pub const CALL_TIMEOUT: Duration = Duration::from_secs(10);
/// Counter of forwarded batches by `result` (`accepted`, `duplicate`, `rejected`,
/// `refused`, `timeout`, `unreachable`).
pub const METRIC: &str = "ostro_route_inbound_forwards_total";

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

fn count(result: &'static str) {
    metrics::counter!(METRIC, "result" => result).increment(1);
}

/// The late-bound Phase 13 context of [`ClusterRouter`]: the node opens (and its shufflers
/// start) before the context exists. Weak only: the node owns the router, and the context
/// reaches the node.
#[derive(Clone, Default)]
pub struct InboundRoute(Arc<ArcSwapOption<Weak<P13Ctx>>>);

impl std::fmt::Debug for InboundRoute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InboundRoute")
            .field("installed", &self.ctx().is_some())
            .finish()
    }
}

impl InboundRoute {
    /// The `NodeConfig::router_wrapper` that puts a [`ClusterRouter`] over the node's
    /// in-process router.
    pub fn wrapper(&self) -> RouterWrapper {
        let late = self.clone();
        Arc::new(move |local| {
            Arc::new(ClusterRouter {
                local,
                late: late.clone(),
            }) as Arc<dyn PartitionRouter>
        })
    }

    /// Route through `ctx` from now on (after the routed control plane exists).
    pub fn install(&self, ctx: &Arc<P13Ctx>) {
        self.0.store(Some(Arc::new(Arc::downgrade(ctx))));
    }

    fn ctx(&self) -> Option<Arc<P13Ctx>> {
        self.0.load().as_ref().and_then(|w| w.upgrade())
    }
}

/// [`PartitionRouter`] over the local [`InProcessRouter`] and `NodeOps.DeliverInbound`.
pub struct ClusterRouter {
    local: Arc<InProcessRouter>,
    late: InboundRoute,
}

impl PartitionRouter for ClusterRouter {
    fn deliver(&self, target: PartitionId, batch: InboundBatch) -> RouteFuture<'_> {
        let remote =
            self.late
                .ctx()
                .and_then(|ctx| match crate::route::route_env(&ctx).owner(target).0 {
                    Target::Node(n) if n != ctx.node_id() => Some((ctx, n)),
                    _ => None,
                });
        match remote {
            None => self.local.deliver(target, batch),
            Some((ctx, node)) => Box::pin(forward(ctx, node, target, batch)),
        }
    }
}

/// `NodeOps.DeliverInbound` of `batch` on `node`, the leader of `target`.
async fn forward(
    ctx: Arc<P13Ctx>,
    node: u64,
    target: PartitionId,
    batch: InboundBatch,
) -> Result<ApplyOutcome, PartitionError> {
    let ch = match ctx.cluster.transport().channel(node) {
        Ok(ch) => ch,
        Err(e) => {
            debug!(node, partition = target.0, error = %e, "inbound forward: node unreachable");
            count("unreachable");
            return Err(PartitionError::NotLeader);
        }
    };
    let max = super::keyed_start::max_message_bytes(&ctx);
    drop(ctx);
    let req = DeliverInboundRequest {
        partition: u32::from(target.0),
        batch: encode_command(&Command::inbound_batch(batch)).to_vec(),
    };
    let call = async move {
        NodeOpsClient::new(ch)
            .max_encoding_message_size(max)
            .max_decoding_message_size(max)
            .deliver_inbound(req)
            .await
    };
    match tokio::time::timeout(CALL_TIMEOUT, call).await {
        Err(_) => {
            debug!(node, partition = target.0, "inbound forward timed out");
            count("timeout");
            Err(PartitionError::NotLeader)
        }
        Ok(Err(s)) => {
            debug!(node, partition = target.0, code = ?s.code(), msg = s.message(),
                "inbound forward refused");
            count("refused");
            Err(PartitionError::NotLeader)
        }
        Ok(Ok(r)) => outcome(node, target, r.into_inner()),
    }
}

/// The target leader's answer as the shuffler's delivery result.
fn outcome(
    node: u64,
    target: PartitionId,
    r: DeliverInboundResponse,
) -> Result<ApplyOutcome, PartitionError> {
    match r.outcome {
        0 => {
            count("accepted");
            Ok(ApplyOutcome::Accepted)
        }
        1 => {
            count("duplicate");
            Ok(ApplyOutcome::Duplicate { existing: None })
        }
        code => {
            count("rejected");
            error!(node, partition = target.0, code, rejected = %r.rejected,
                "inbound forward: the target partition rejected an outbox batch");
            Err(PartitionError::Failed(format!(
                "node {node} rejected the outbox batch for partition {}: {}",
                target.0, r.rejected
            )))
        }
    }
}

/// The served half: the partition-write check, then this node's in-process router only.
pub struct NodeInbound {
    ctx: Weak<P13Ctx>,
}

impl NodeInbound {
    /// Over `ctx` (weak: the cluster listener outlives the node).
    pub fn new(ctx: &Arc<P13Ctx>) -> Self {
        Self {
            ctx: Arc::downgrade(ctx),
        }
    }
}

/// The answer of a local delivery.
pub fn response(o: &ApplyOutcome) -> DeliverInboundResponse {
    DeliverInboundResponse {
        outcome: u32::from(o.code()),
        rejected: match o {
            ApplyOutcome::Rejected(r) => format!("{r:?}"),
            _ => String::new(),
        },
    }
}

fn status(p: PartitionId, e: &PartitionError) -> Status {
    match e {
        PartitionError::NotLeader => {
            Status::failed_precondition(format!("partition {}: not the leader", p.0))
        }
        e => Status::unavailable(format!("partition {}: {e}", p.0)),
    }
}

impl InboundServer for NodeInbound {
    fn deliver_inbound(
        &self,
        req: DeliverInboundRequest,
    ) -> BoxFuture<'_, Result<DeliverInboundResponse, Status>> {
        Box::pin(async move {
            let ctx = self
                .ctx
                .upgrade()
                .ok_or_else(|| Status::unavailable("the node is shutting down"))?;
            let p = u16::try_from(req.partition).map(PartitionId).map_err(|_| {
                Status::invalid_argument(format!("partition {} out of range", req.partition))
            })?;
            let batch = match decode_command(&req.batch)
                .map_err(|e| Status::invalid_argument(format!("batch: {e}")))?
                .cmd
            {
                Some(Cmd::InboundBatch(b)) => b,
                _ => {
                    return Err(Status::invalid_argument(
                        "batch: not an InboundBatch command",
                    ));
                }
            };
            crate::route::consistency::before_partition_write(&ctx, p)
                .await
                .map_err(ostro_api::grpc::to_status)?;
            let router = ctx
                .node()
                .map(|n| n.router())
                .ok_or_else(|| Status::unavailable("the node is shutting down"))?;
            drop(ctx);
            match router.deliver(p, batch).await {
                Ok(o) => Ok(response(&o)),
                Err(e) => Err(status(p, &e)),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_partition::RejectReason;

    #[test]
    fn outcomes_round_trip_the_shuffler_classes() {
        let p = PartitionId(3);
        assert!(matches!(
            outcome(2, p, response(&ApplyOutcome::Accepted)),
            Ok(ApplyOutcome::Accepted)
        ));
        assert!(matches!(
            outcome(2, p, response(&ApplyOutcome::Duplicate { existing: None })),
            Ok(ApplyOutcome::Duplicate { existing: None })
        ));
        let gap = ApplyOutcome::Rejected(RejectReason::SeqGap {
            expected: 4,
            found: 9,
        });
        let r = response(&gap);
        assert!(r.outcome >= 16 && r.rejected.contains("SeqGap"), "{r:?}");
        assert!(matches!(outcome(2, p, r), Err(PartitionError::Failed(m)) if m.contains("SeqGap")));
    }

    #[test]
    fn not_leader_is_a_precondition_failure() {
        let s = status(PartitionId(1), &PartitionError::NotLeader);
        assert_eq!(s.code(), tonic::Code::FailedPrecondition);
        let s = status(PartitionId(1), &PartitionError::LeaderChanged);
        assert_eq!(s.code(), tonic::Code::Unavailable);
    }
}
