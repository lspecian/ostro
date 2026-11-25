//! Graceful leader handoff (13-11, D-P13-13, research Pattern 6).
//!
//! [`LocalHandoff`] executes a handoff of a partition this node leads (through
//! `ostro_partition::HandoffControl`); [`NodeOpsHandoffClient`] asks the current leader over
//! `NodeOps.HandOff`, or calls the local executor when the leader is this node.
//!
//! # Sequence (`LocalHandoff::execute(p, to, epoch)`)
//!
//! 1. **Refuse** unless meta names this node leader of `p` at `epoch`
//!    ([`HandoffError::NotLeader`]) and `to` is in the **published** ISR
//!    ([`HandoffError::TargetNotInIsr`]).
//! 2. `prepare_handoff(p, handoff_grace)`: admission off, hot waits evicted, in-flight
//!    activations get up to the grace to reach a flush/suspend point (none is cancelled).
//! 3. `freeze_appends()` (W11): new submissions are refused with the retryable
//!    `leader_changed`, so the log end stops moving.
//! 4. **Catch-up:** wait until the local log is fully durable and `to`'s durable LEO (the
//!    leader's follower progress) reached the log end, bounded by `handoff_catchup_timeout`.
//!    The assignment is re-read on every check: a changed term or `to` leaving the ISR aborts.
//! 5. Failpoint `handoff::before_elect`, then propose
//!    `ElectLeader{epoch + 1, leader: to, isr: published ISR unchanged, expected_isr_version}`
//!    (meta apply re-checks `to ∈ ISR` and the CAS). The target leads only at an epoch above
//!    its log's latest (12-14): if not, its role driver requests a fresh term itself, which
//!    still counts as moved. The proposal is bounded by [`ELECT_TIMEOUT`]. Failpoint
//!    `handoff::after_elect`.
//! 6. Applied → `complete()` (the fence stays until the Phase 12 role lifecycle demotes this
//!    node on the assignment change) and [`HandoffOutcome::Moved`]. Any failure (catch-up
//!    timeout, `StaleVersion`, `Invalid`, meta error, `to` out of the ISR, failpoint) →
//!    `abort()` (admission and appends back on) and [`HandoffOutcome::Aborted`]: this node
//!    keeps leading. A handoff never replaces `AssignmentSource::resign` for a locally
//!    failed partition, never elects outside the published ISR and is never unclean.
//!
//! Timing uses tokio time only (`handoff_grace`, `handoff_catchup_timeout`; no wall-clock
//! read): the election timestamp is the partition clock's value from the handoff guard.
//!
//! The production meta ([`MetaHandle`]) and log ([`PartitionLogHandle`]) reach the sequence
//! through the small [`HandoffMeta`] / [`HandoffLog`] views, so tests drive it with fakes
//! ([`LocalHandoff::with_parts`]).

use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_log::ReplicatedLog;
use ostro_meta::ext::{ELECTION_REASON_RESIGN, ElectionInfo};
use ostro_meta::{MetaCommand, MetaHandle, MetaResult, PartitionAssignment};
use ostro_partition::roles::PartitionLogHandle;
use ostro_partition::{HandoffControl, HandoffGuard, PartitionError};
use ostro_proto::cluster::v1::HandOffRequest;
use ostro_proto::cluster::v1::node_ops_client::NodeOpsClient;
use ostro_types::{Epoch, Offset, PartitionId};
use tracing::{info, warn};

use crate::contracts::{HandoffClient, HandoffError, HandoffExecutor, HandoffOutcome};
use crate::transport::ClusterTransport;

/// Looks up the local log of a partition (for the leader LEO and the follower progress).
pub type LogLookup = Arc<dyn Fn(PartitionId) -> Option<Arc<PartitionLogHandle>> + Send + Sync>;

/// Test seam: the log view of a partition ([`HandoffLog`]).
pub type HandoffLogLookup = Arc<dyn Fn(PartitionId) -> Option<Arc<dyn HandoffLog>> + Send + Sync>;

/// Handoff timing (`[cluster]` keys, D-P13-26).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandoffConfig {
    /// `handoff_grace`: wait for in-flight activations to reach a flush/suspend point.
    pub grace: Duration,
    /// `handoff_catchup_timeout`: wait for the target's durable LEO to equal ours.
    pub catchup_timeout: Duration,
}

impl Default for HandoffConfig {
    fn default() -> Self {
        Self {
            grace: Duration::from_secs(2),
            catchup_timeout: Duration::from_secs(10),
        }
    }
}

/// How often the catch-up wait re-reads the follower progress and the assignment.
const CATCHUP_POLL: Duration = Duration::from_millis(10);

/// The catch-up aborts early when the target is behind and its durable LEO did not move for
/// this long (it is not fetching: dead, isolated or stopping), instead of waiting out
/// `handoff_catchup_timeout`. A fetching follower reports progress on every fetch round.
pub const CATCHUP_STALL: Duration = Duration::from_secs(2);

/// Bound on the `ElectLeader` proposal: a metadata group that cannot commit within it (its
/// leader stopping, no quorum) aborts the handoff instead of holding the partition frozen
/// for the whole proposal deadline.
pub const ELECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Margin on top of `grace + catchup_timeout` for a remote `NodeOps.HandOff` call
/// (propose + apply + transport).
const REMOTE_MARGIN: Duration = Duration::from_secs(20);

/// The meta operations a handoff needs.
pub trait HandoffMeta: Send + Sync {
    /// This node's id.
    fn node_id(&self) -> u64;
    /// The partition's current assignment (local applied view).
    fn assignment(&self, partition: u16) -> Option<PartitionAssignment>;
    /// Propose `cmd` and wait for its result.
    fn propose(&self, cmd: MetaCommand) -> BoxFuture<'_, Result<MetaResult, String>>;
}

impl HandoffMeta for MetaHandle {
    fn node_id(&self) -> u64 {
        MetaHandle::node_id(self)
    }

    fn assignment(&self, partition: u16) -> Option<PartitionAssignment> {
        self.view()
            .read(|s| s.partition(partition).map(|v| v.value.clone()))
    }

    fn propose(&self, cmd: MetaCommand) -> BoxFuture<'_, Result<MetaResult, String>> {
        Box::pin(async move {
            MetaHandle::propose(self, cmd)
                .await
                .map_err(|e| e.to_string())
        })
    }
}

/// The log operations a handoff needs (the leader's view of its partition log).
pub trait HandoffLog: Send + Sync {
    /// Appended log end (exclusive).
    fn log_end(&self) -> Offset;
    /// Fsynced log end (exclusive).
    fn durable_end(&self) -> Offset;
    /// `node`'s last reported durable LEO (leader only; `None` = no progress known).
    fn follower_durable(&self, node: u64) -> Option<Offset>;
}

impl HandoffLog for PartitionLogHandle {
    fn log_end(&self) -> Offset {
        ReplicatedLog::log_end(self)
    }

    fn durable_end(&self) -> Offset {
        ReplicatedLog::durable_end(self)
    }

    fn follower_durable(&self, node: u64) -> Option<Offset> {
        self.replica_status()
            .followers
            .iter()
            .find(|f| f.replica == node)
            .map(|f| f.durable_leo)
    }
}

/// Failpoint `handoff::before_elect`: `return` aborts the handoff before the proposal.
#[cfg(feature = "failpoints")]
fn before_elect() -> Result<(), String> {
    fail::fail_point!("handoff::before_elect", |_| Err(
        "injected failure at handoff::before_elect".to_string()
    ));
    Ok(())
}

#[cfg(not(feature = "failpoints"))]
#[inline(always)]
fn before_elect() -> Result<(), String> {
    Ok(())
}

/// Failpoint `handoff::after_elect`: the election is applied (sleep / pause / panic widen or
/// kill the window between the election and the old leader's demotion; `return` is ignored).
#[cfg(feature = "failpoints")]
fn after_elect() {
    fail::fail_point!("handoff::after_elect");
}

#[cfg(not(feature = "failpoints"))]
#[inline(always)]
fn after_elect() {}

/// Node-local handoff executor.
pub struct LocalHandoff {
    control: Arc<dyn HandoffControl>,
    meta: Arc<dyn HandoffMeta>,
    logs: HandoffLogLookup,
    cfg: HandoffConfig,
}

impl std::fmt::Debug for LocalHandoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalHandoff")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

impl LocalHandoff {
    /// An executor driving `control`, proposing through `meta`, reading progress from `logs`.
    pub fn new(
        control: Arc<dyn HandoffControl>,
        meta: Arc<MetaHandle>,
        logs: LogLookup,
        cfg: HandoffConfig,
    ) -> Self {
        let logs: HandoffLogLookup = Arc::new(move |p| logs(p).map(|l| l as Arc<dyn HandoffLog>));
        Self::with_parts(control, meta, logs, cfg)
    }

    /// The same executor over any [`HandoffMeta`] / [`HandoffLog`] (tests use fakes).
    pub fn with_parts(
        control: Arc<dyn HandoffControl>,
        meta: Arc<dyn HandoffMeta>,
        logs: HandoffLogLookup,
        cfg: HandoffConfig,
    ) -> Self {
        Self {
            control,
            meta,
            logs,
            cfg,
        }
    }

    /// This node's id.
    pub fn node_id(&self) -> u64 {
        self.meta.node_id()
    }

    /// The timing in use.
    pub fn config(&self) -> &HandoffConfig {
        &self.cfg
    }

    async fn run(
        &self,
        p: PartitionId,
        to: u64,
        epoch: Epoch,
    ) -> Result<HandoffOutcome, HandoffError> {
        let me = self.meta.node_id();
        let not_leader = HandoffError::NotLeader {
            partition: p.0,
            epoch: epoch.0,
        };
        let a = self
            .meta
            .assignment(p.0)
            .ok_or_else(|| not_leader.clone())?;
        if a.leader != me || a.epoch != epoch.0 {
            return Err(not_leader);
        }
        if to == me {
            return Ok(aborted(p, to, "the target already leads the partition"));
        }
        if !a.isr.contains(&to) {
            return Err(HandoffError::TargetNotInIsr {
                partition: p.0,
                target: to,
            });
        }
        let log = (self.logs)(p).ok_or_else(|| not_leader.clone())?;
        let guard = match self.control.prepare_handoff(p, self.cfg.grace).await {
            Ok(g) => g,
            Err(PartitionError::NotLeader) => return Err(not_leader),
            Err(e) => return Ok(aborted(p, to, &format!("prepare: {e}"))),
        };
        let report = guard.report();
        guard.freeze_appends();
        let a = match self.catch_up(p, to, epoch, log.as_ref()).await {
            Ok(a) => a,
            Err(reason) => return Ok(abort(guard, p, to, &reason)),
        };
        if let Err(reason) = before_elect() {
            return Ok(abort(guard, p, to, &reason));
        }
        let new_epoch = Epoch(epoch.0.saturating_add(1));
        let cmd = MetaCommand::ElectLeader {
            partition: p.0,
            epoch: new_epoch.0,
            leader: to,
            isr: a.isr.clone(),
            expected_isr_version: a.isr_version,
            info: ElectionInfo {
                reason: ELECTION_REASON_RESIGN,
                elected_at_ms: report.started_at_ms,
                prev_leader: me,
            },
        };
        let proposed = match tokio::time::timeout(ELECT_TIMEOUT, self.meta.propose(cmd)).await {
            Ok(r) => r,
            Err(_) => Err(format!(
                "no answer within {ELECT_TIMEOUT:?} (outcome unknown; if the election applies \
                 later it is a valid in-ISR election and the role lifecycle demotes this node)"
            )),
        };
        match proposed {
            Ok(MetaResult::Ok { .. }) => {
                after_elect();
                guard.complete();
                info!(
                    partition = p.0,
                    from = me,
                    to,
                    epoch = new_epoch.0,
                    leased_left = report.leased_left,
                    evicted = report.evicted,
                    "handoff: leadership moved"
                );
                Ok(HandoffOutcome::Moved { new_epoch })
            }
            Ok(MetaResult::StaleVersion { current }) => {
                // The target may already have requested its own fresh term above ours.
                if let Some(now) = self.meta.assignment(p.0)
                    && now.leader == to
                    && now.epoch > epoch.0
                {
                    guard.complete();
                    return Ok(HandoffOutcome::Moved {
                        new_epoch: Epoch(now.epoch),
                    });
                }
                Ok(abort(
                    guard,
                    p,
                    to,
                    &format!("ElectLeader stale (current version {current})"),
                ))
            }
            Ok(other) => Ok(abort(
                guard,
                p,
                to,
                &format!("ElectLeader not applied: {other:?}"),
            )),
            Err(e) => Ok(abort(guard, p, to, &format!("meta: {e}"))),
        }
    }

    /// Wait until the log is fully durable and `to` reached its end. Returns the assignment
    /// read last (its ISR and CAS token go into the election).
    async fn catch_up(
        &self,
        p: PartitionId,
        to: u64,
        epoch: Epoch,
        log: &dyn HandoffLog,
    ) -> Result<PartitionAssignment, String> {
        let me = self.meta.node_id();
        let stall = CATCHUP_STALL.min(self.cfg.catchup_timeout);
        let wait = async {
            let mut last = log.follower_durable(to);
            let mut still = Duration::ZERO;
            loop {
                let Some(a) = self.meta.assignment(p.0) else {
                    return Err("the partition assignment disappeared".to_string());
                };
                if a.leader != me || a.epoch != epoch.0 {
                    return Err(format!(
                        "the assignment moved (leader {} epoch {})",
                        a.leader, a.epoch
                    ));
                }
                if !a.isr.contains(&to) {
                    return Err(format!("target {to} left the ISR {:?}", a.isr));
                }
                let end = log.log_end();
                let at = log.follower_durable(to);
                if log.durable_end() >= end && at.is_some_and(|d| d >= end) {
                    return Ok(a);
                }
                if at == last {
                    still += CATCHUP_POLL;
                    if still >= stall {
                        return Err(format!(
                            "target {to} is not catching up (durable LEO {:?} for {stall:?}, leader end {})",
                            at.map(|o| o.0),
                            end.0
                        ));
                    }
                } else {
                    last = at;
                    still = Duration::ZERO;
                }
                tokio::time::sleep(CATCHUP_POLL).await;
            }
        };
        match tokio::time::timeout(self.cfg.catchup_timeout, wait).await {
            Ok(r) => r,
            Err(_) => Err(format!(
                "catch-up timeout after {:?}: target at {:?}, leader end {} (durable {})",
                self.cfg.catchup_timeout,
                log.follower_durable(to).map(|o| o.0),
                log.log_end().0,
                log.durable_end().0
            )),
        }
    }
}

fn aborted(p: PartitionId, to: u64, reason: &str) -> HandoffOutcome {
    warn!(partition = p.0, to, %reason, "handoff aborted");
    HandoffOutcome::Aborted {
        reason: reason.to_string(),
    }
}

/// Release the guard (admission and appends back on) and report the abort.
fn abort(guard: HandoffGuard, p: PartitionId, to: u64, reason: &str) -> HandoffOutcome {
    guard.abort();
    aborted(p, to, reason)
}

impl HandoffExecutor for LocalHandoff {
    fn execute(
        &self,
        p: PartitionId,
        to: u64,
        epoch: Epoch,
    ) -> BoxFuture<'_, Result<HandoffOutcome, HandoffError>> {
        Box::pin(self.run(p, to, epoch))
    }
}

/// Controller-side client: `NodeOps.HandOff` to the leader, or the local executor.
pub struct NodeOpsHandoffClient {
    transport: Arc<dyn ClusterTransport>,
    local: Arc<LocalHandoff>,
}

impl std::fmt::Debug for NodeOpsHandoffClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeOpsHandoffClient")
            .finish_non_exhaustive()
    }
}

impl NodeOpsHandoffClient {
    /// A client dialing leaders over `transport`, short-circuiting to `local`.
    pub fn new(transport: Arc<dyn ClusterTransport>, local: Arc<LocalHandoff>) -> Self {
        Self { transport, local }
    }

    async fn remote(
        &self,
        leader: u64,
        p: PartitionId,
        to: u64,
        epoch: Epoch,
    ) -> Result<HandoffOutcome, HandoffError> {
        let channel = self
            .transport
            .channel(leader)
            .map_err(|e| HandoffError::Transport(e.to_string()))?;
        let mut client = NodeOpsClient::new(channel);
        let req = HandOffRequest {
            partition: u32::from(p.0),
            to,
            epoch: epoch.0,
        };
        let cfg = self.local.config();
        let bound = cfg.grace + cfg.catchup_timeout + REMOTE_MARGIN;
        let resp = match tokio::time::timeout(bound, client.hand_off(req)).await {
            Ok(r) => r,
            Err(_) => {
                return Err(HandoffError::Transport(format!(
                    "NodeOps.HandOff to node {leader}: no answer within {bound:?}"
                )));
            }
        };
        match resp {
            Ok(r) => {
                let r = r.into_inner();
                Ok(if r.moved {
                    HandoffOutcome::Moved {
                        new_epoch: Epoch(r.new_epoch),
                    }
                } else {
                    HandoffOutcome::Aborted {
                        reason: r.abort_reason,
                    }
                })
            }
            Err(s) => Err(status_error(&s, p, to, epoch)),
        }
    }
}

/// The server maps `NotLeader` / `TargetNotInIsr` to FAILED_PRECONDITION with the error's
/// text (ostro-server `p13/node_ops.rs`) and transport / meta failures to UNAVAILABLE.
fn status_error(s: &tonic::Status, p: PartitionId, to: u64, epoch: Epoch) -> HandoffError {
    let msg = s.message();
    match s.code() {
        tonic::Code::FailedPrecondition if msg.contains("does not lead partition") => {
            HandoffError::NotLeader {
                partition: p.0,
                epoch: epoch.0,
            }
        }
        tonic::Code::FailedPrecondition if msg.contains("is not in the ISR") => {
            HandoffError::TargetNotInIsr {
                partition: p.0,
                target: to,
            }
        }
        tonic::Code::FailedPrecondition | tonic::Code::InvalidArgument => {
            HandoffError::Meta(msg.to_string())
        }
        _ => HandoffError::Transport(format!("{:?}: {msg}", s.code())),
    }
}

impl HandoffClient for NodeOpsHandoffClient {
    fn hand_off(
        &self,
        leader: u64,
        p: PartitionId,
        to: u64,
        epoch: Epoch,
    ) -> BoxFuture<'_, Result<HandoffOutcome, HandoffError>> {
        Box::pin(async move {
            if leader == self.local.node_id() {
                self.local.execute(p, to, epoch).await
            } else {
                self.remote(leader, p, to, epoch).await
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_errors_map_back() {
        let p = PartitionId(3);
        let e = Epoch(7);
        let s = tonic::Status::failed_precondition(
            HandoffError::NotLeader {
                partition: 3,
                epoch: 7,
            }
            .to_string(),
        );
        assert_eq!(
            status_error(&s, p, 2, e),
            HandoffError::NotLeader {
                partition: 3,
                epoch: 7
            }
        );
        let s = tonic::Status::failed_precondition(
            HandoffError::TargetNotInIsr {
                partition: 3,
                target: 2,
            }
            .to_string(),
        );
        assert_eq!(
            status_error(&s, p, 2, e),
            HandoffError::TargetNotInIsr {
                partition: 3,
                target: 2
            }
        );
        assert!(matches!(
            status_error(&tonic::Status::unavailable("x"), p, 2, e),
            HandoffError::Transport(_)
        ));
    }
}
