//! Leader side of `Replication.Fetch` (12-11, research Pattern 3, CLUS-02).
//!
//! [`handle_fetch`] evaluates a follower's fetch in a fixed order:
//!
//! 1. **Replica identity:** the calling node (mTLS `PeerNode`) must equal `replica_id` and be
//!    one of the partition's assigned replicas, else `NOT_REPLICA`.
//! 2. **Epoch fencing (KIP-320):** `leader_epoch` below the newest epoch this node knows for
//!    the partition → `FENCED_LEADER_EPOCH`; above it → `UNKNOWN_LEADER_EPOCH` (this node is
//!    behind on meta). The `repl::mutant::skip_epoch_check` failpoint skips this step.
//! 3. **Leadership:** the local log must lead exactly that epoch, else `NOT_LEADER`.
//! 4. **Divergence (KIP-595):** [`divergence::check`] over the leader's epoch history:
//!    `diverging{epoch, end}` (no error code, as in Kafka) or `SNAPSHOT_REQUIRED`.
//! 5. **Progress:** only after the check returned `Ok`, and before any wait:
//!    `record_progress{durable_leo: min(durable_leo, fetch_offset), fetch_offset, now_ms,
//!    leader_durable_end}`, where `leader_durable_end` is the durable end sampled **before**
//!    any read (12-04 W3 / B5): it is the caught-up target of the replica's next fetch.
//! 6. **Read or long-poll:** with data below the durable end the read starts at once,
//!    bounded only by `fetch_read_timeout`; otherwise the handler waits on the durable-end
//!    watch for up to `max_wait_ms` and then reads. The two timeouts never mix (RP-12): a
//!    read in progress is never cut short by `max_wait`, and a read that outlives
//!    `fetch_read_timeout` fails the fetch (it never answers "no data").
//!
//! Reads come from [`ReplicatedLog::read_replica`] (verified, durable-capped, never HW-capped)
//! and are cut at the durable end sampled before the read, so the response never carries data
//! beyond its own `leader_durable_end`. Envelopes are forwarded verbatim (F1), whole batches
//! only, at least one when one is available, at most about `max_bytes` (never more than one
//! maximum-size envelope above it). Both gRPC sides use [`fetch_decode_limit`].

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_log::replica::divergence::{self, FetchCheck};
use ostro_log::{FollowerProgress, LogError, ReplicatedLog, Role};
use ostro_partition::Clock;
use ostro_partition::roles::{PartitionAssignmentView, PartitionLogHandle};
use ostro_proto::cluster::v1::{DivergingEpoch, FetchError, FetchRequest, FetchResponse};
use ostro_types::{Epoch, Offset, PartitionId};
use tokio::sync::watch;
use tonic::{Request, Response, Status};

/// Extra room on top of the largest response payload for the protobuf framing of a fetch
/// response (field tags, lengths, the scalar fields).
pub const FETCH_DECODE_SLACK: usize = 64 * 1024;

/// Default `fetch_max_wait` (D-P12-10).
pub const DEFAULT_FETCH_MAX_WAIT: Duration = Duration::from_millis(500);
/// Default `fetch_read_timeout` (D-P12-10).
pub const DEFAULT_FETCH_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Default `fetch_max_bytes` (D-P12-10).
pub const DEFAULT_FETCH_MAX_BYTES: usize = 8 << 20;

/// The gRPC encode/decode limit for `Fetch` on both sides: the largest response payload
/// (`max(fetch_max_bytes, max_envelope_bytes)`, since a response may exceed `max_bytes` by
/// at most one envelope that is alone in it) plus [`FETCH_DECODE_SLACK`]. With
/// `fetch_max_bytes <= max_envelope_bytes` this is `max_envelope_bytes + 64 KiB`, so a
/// maximum-size envelope always replicates.
pub fn fetch_decode_limit(max_envelope_bytes: usize, fetch_max_bytes: usize) -> usize {
    max_envelope_bytes.max(fetch_max_bytes) + FETCH_DECODE_SLACK
}

/// Leader-side fetch tuning (`[replication]`, derived by 12-14's config).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FetchConfig {
    /// Upper bound on a request's `max_wait_ms` (`fetch_max_wait`).
    pub max_wait: Duration,
    /// Bound on one read of existing data (`fetch_read_timeout`), independent of `max_wait`.
    pub read_timeout: Duration,
    /// Upper bound on a request's `max_bytes` (`fetch_max_bytes`).
    pub max_bytes: usize,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            max_wait: DEFAULT_FETCH_MAX_WAIT,
            read_timeout: DEFAULT_FETCH_READ_TIMEOUT,
            max_bytes: DEFAULT_FETCH_MAX_BYTES,
        }
    }
}

/// A test hook run between the durable-end snapshot and the read (`#[doc(hidden)]`).
pub type ReadHook = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

/// Everything the leader needs to answer fetches for one partition.
pub struct FetchCtx {
    partition: PartitionId,
    log: Arc<PartitionLogHandle>,
    assignments: watch::Receiver<Arc<Vec<PartitionAssignmentView>>>,
    clock: Arc<dyn Clock>,
    cfg: FetchConfig,
    before_read: Option<ReadHook>,
}

impl fmt::Debug for FetchCtx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FetchCtx")
            .field("partition", &self.partition)
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

impl FetchCtx {
    /// The fetch context of `log`'s partition. `assignments` is the meta-published
    /// assignment watch (`AssignmentSource::subscribe`); `clock` is the node's injected
    /// clock (the same one that drives the log's replica tracker).
    pub fn new(
        log: Arc<PartitionLogHandle>,
        assignments: watch::Receiver<Arc<Vec<PartitionAssignmentView>>>,
        clock: Arc<dyn Clock>,
        cfg: FetchConfig,
    ) -> Self {
        Self {
            partition: log.partition(),
            log,
            assignments,
            clock,
            cfg,
            before_read: None,
        }
    }

    /// Test hook: run `hook` after the durable end was sampled and before `read_replica`.
    #[doc(hidden)]
    pub fn with_read_hook(mut self, hook: ReadHook) -> Self {
        self.before_read = Some(hook);
        self
    }

    /// The partition.
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// The partition log.
    pub fn log(&self) -> &Arc<PartitionLogHandle> {
        &self.log
    }

    /// The configuration.
    pub fn config(&self) -> &FetchConfig {
        &self.cfg
    }

    fn assignment(&self) -> Option<PartitionAssignmentView> {
        self.assignments
            .borrow()
            .iter()
            .find(|a| a.partition == self.partition)
            .cloned()
    }
}

/// Whether the mutant failpoint `name` is enabled (`fail::cfg(name, "return")`).
#[cfg(feature = "failpoints")]
pub(crate) fn mutant(name: &str) -> bool {
    fail::eval(name, |_| ()).is_some()
}

/// Whether the mutant failpoint `name` is enabled: never, without the `failpoints` feature.
#[cfg(not(feature = "failpoints"))]
#[inline(always)]
pub(crate) fn mutant(_name: &str) -> bool {
    false
}

fn error_response(error: FetchError, epoch: Epoch) -> FetchResponse {
    FetchResponse {
        error: error as i32,
        leader_epoch: epoch.0,
        ..FetchResponse::default()
    }
}

fn log_status(e: &LogError) -> Status {
    match e {
        LogError::NotLeader { .. } | LogError::StaleEpoch { .. } => {
            Status::unavailable(format!("fetch: {e}"))
        }
        _ => Status::internal(format!("fetch: {e}")),
    }
}

/// The local log's role epoch, if it leads.
fn leading_epoch(log: &PartitionLogHandle) -> Option<Epoch> {
    match &*log.role().borrow() {
        Role::Leader { epoch } => Some(*epoch),
        _ => None,
    }
}

/// `Replication.Fetch` behind `authz::require_node` (what 12-14's service calls).
pub async fn serve_fetch(
    ctx: &FetchCtx,
    req: Request<FetchRequest>,
) -> Result<Response<FetchResponse>, Status> {
    let peer = crate::authz::require_node(&req)?;
    handle_fetch(ctx, peer, req.into_inner())
        .await
        .map(Response::new)
}

/// Answer one fetch from node `peer` (see the module docs for the evaluation order).
pub async fn handle_fetch(
    ctx: &FetchCtx,
    peer: u64,
    req: FetchRequest,
) -> Result<FetchResponse, Status> {
    let log = &ctx.log;
    if req.partition != u32::from(ctx.partition.0) {
        return Err(Status::invalid_argument(format!(
            "fetch for partition {} sent to the context of partition {}",
            req.partition, ctx.partition.0
        )));
    }
    let assignment = ctx.assignment();
    let role_epoch = match &*log.role().borrow() {
        Role::Leader { epoch } | Role::Follower { epoch } => Some(*epoch),
        Role::Offline => None,
    };
    let current = assignment
        .as_ref()
        .map(|a| a.epoch)
        .into_iter()
        .chain(role_epoch)
        .max()
        .unwrap_or_default();

    // 1. Replica identity.
    let is_replica = assignment
        .as_ref()
        .is_some_and(|a| a.replicas.contains(&peer));
    if peer != req.replica_id || !is_replica {
        return Ok(error_response(FetchError::NotReplica, current));
    }

    // 2. Epoch fencing (KIP-320).
    let requested = Epoch(req.leader_epoch);
    if !mutant(ostro_log::mutants::SKIP_EPOCH_CHECK) {
        if requested < current {
            return Ok(error_response(FetchError::FencedLeaderEpoch, current));
        }
        if requested > current {
            return Ok(error_response(FetchError::UnknownLeaderEpoch, current));
        }
    }

    // 3. Leadership of exactly that epoch (under the mutant: of any epoch).
    let Some(leader_epoch) = leading_epoch(log) else {
        return Ok(error_response(FetchError::NotLeader, current));
    };
    if leader_epoch != current && !mutant(ostro_log::mutants::SKIP_EPOCH_CHECK) {
        return Ok(error_response(FetchError::NotLeader, current));
    }

    // 4. Divergence (KIP-595 / KIP-279; UNDEFINED → snapshot, never truncate-to-HW).
    let fetch_offset = Offset(req.fetch_offset);
    let last_fetched = (req.last_fetched_epoch != 0).then_some(Epoch(req.last_fetched_epoch));
    let hw = *log.high_watermark().borrow();
    let log_start = log.log_start();
    match divergence::check(
        &log.epochs(),
        log_start,
        log.log_end(),
        fetch_offset,
        last_fetched,
    ) {
        FetchCheck::Ok => {}
        FetchCheck::Diverging { epoch, end } => {
            return Ok(FetchResponse {
                error: FetchError::None as i32,
                leader_epoch: leader_epoch.0,
                high_watermark: hw.0,
                log_start: log_start.0,
                leader_durable_end: log.durable_end().0,
                diverging: Some(DivergingEpoch {
                    epoch: epoch.0,
                    end_offset: end.0,
                }),
                envelopes: Vec::new(),
            });
        }
        FetchCheck::SnapshotRequired => {
            let mut r = error_response(FetchError::SnapshotRequired, leader_epoch);
            r.high_watermark = hw.0;
            r.log_start = log_start.0;
            r.leader_durable_end = log.durable_end().0;
            return Ok(r);
        }
    }

    // 5. Progress, before any wait. The durable end sampled here is the replica's next
    //    caught-up target (B5) and, when data is available now, the read's cap (W3).
    let mut durable_rx = log.durable_end_watch();
    let mut snapshot = *durable_rx.borrow_and_update();
    log.record_progress(FollowerProgress {
        replica: peer,
        durable_leo: Offset(req.durable_leo).min(fetch_offset),
        fetch_offset,
        now_ms: ctx.clock.now_ms(),
        leader_durable_end: snapshot,
    })
    .map_err(|e| log_status(&e))?;

    // 6. Long-poll for new data (max_wait), then read (fetch_read_timeout).
    if fetch_offset >= snapshot {
        let max_wait = Duration::from_millis(u64::from(req.max_wait_ms)).min(ctx.cfg.max_wait);
        if !max_wait.is_zero() {
            let _ =
                tokio::time::timeout(max_wait, durable_rx.wait_for(|d| *d > fetch_offset)).await;
        }
        if leading_epoch(log) != Some(leader_epoch) {
            return Ok(error_response(FetchError::NotLeader, current));
        }
        snapshot = log.durable_end();
    }
    let mut out = FetchResponse {
        error: FetchError::None as i32,
        leader_epoch: leader_epoch.0,
        high_watermark: log.high_watermark().borrow().0,
        log_start: log.log_start().0,
        leader_durable_end: snapshot.0,
        diverging: None,
        envelopes: Vec::new(),
    };
    if fetch_offset >= snapshot {
        return Ok(out);
    }
    let budget = if req.max_bytes == 0 {
        ctx.cfg.max_bytes
    } else {
        usize::try_from(req.max_bytes)
            .unwrap_or(usize::MAX)
            .min(ctx.cfg.max_bytes)
    }
    .max(1);
    let read = async {
        if let Some(hook) = &ctx.before_read {
            hook().await;
        }
        log.read_replica(fetch_offset, budget).await
    };
    let batches = match tokio::time::timeout(ctx.cfg.read_timeout, read).await {
        Ok(Ok(b)) => b,
        Ok(Err(e)) => return Err(log_status(&e)),
        Err(_) => {
            return Err(Status::deadline_exceeded(format!(
                "fetch read of partition {} from {fetch_offset} exceeded {:?}",
                ctx.partition.0, ctx.cfg.read_timeout
            )));
        }
    };
    // Whole envelopes below the snapshot, at least one, about `budget` bytes.
    let mut total = 0usize;
    'batches: for b in batches {
        let mut base = b.base;
        for env in b.envelopes {
            let Ok(h) = ostro_types::envelope::peek_header(&env) else {
                return Err(Status::internal("fetch: unreadable stored envelope header"));
            };
            let end = h.base + u64::from(h.cmd_count);
            if h.base != base || end > snapshot {
                break 'batches;
            }
            if !out.envelopes.is_empty() && total + env.len() > budget {
                break 'batches;
            }
            total += env.len();
            out.envelopes.push(env.to_vec());
            base = end;
        }
    }
    log.note_fetch_envelopes(out.envelopes.len());
    Ok(out)
}
