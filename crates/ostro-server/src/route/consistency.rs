//! Read-your-writes from any node (API-08 second half, D-P13-15/16, plan 13-13): partition
//! reads under a leader lease (plus a full barrier with `[api] strict_reads`), metadata
//! reads behind a raft ReadIndex barrier, and the `ostro-consistency` tokens.
//!
//! The four hooks have fixed signatures and fixed call sites (the 13-12 router's local
//! branch, for direct and forwarded-in calls). Their rules live in the testable core
//! ([`check_partition_read`], [`check_meta_read`], [`partition_token`], [`meta_token`]) over a
//! [`ReadEnv`]; the hooks run the core over the node ([`NodeReadEnv`]).
//!
//! # Partition reads (the lease)
//!
//! A partition read is served only when **all** hold:
//!
//! 1. this node leads `p`: role `Leader{epoch}` (set only after its `LeaderAnnounced{epoch}`
//!    applied) and the handle stamps the same epoch;
//! 2. no graceful handoff froze `p` (`HandoffControl::in_handoff`, 13-11; stays true after
//!    the handoff completes until the demotion);
//! 3. the metadata lease holds: `now − meta_lease_anchor < session_timeout × (1 − lease_margin)`.
//!    The anchor (13-05, monotonic) is the send instant of this node's newest
//!    quorum-confirmed lease round: on a meta follower a ReadIndex round trip through the meta
//!    leader, on the meta leader its own quorum-acked heartbeat round, `now` on a one-voter
//!    leader. It freezes when the node is cut off and is reset by a meta term change.
//!
//! Otherwise the read answers `unavailable` reason `not_leader` (with the partition and the
//! published leader as a hint), which the router re-routes once.
//!
//! With `[api] strict_reads = true` the leader additionally passes
//! `MetaHandle::read_index_barrier` and checks the meta assignment (leader == self at its
//! epoch) before serving. A read carrying `ostro-read-after: p<P>:<epoch>:<offset>` that the
//! local state covers skips that confirmation (never the lease).
//!
//! The controller side of the lease (W2) is in `ostro_cluster::controller`: a failover
//! `ElectLeader` waits until the controller has not heard from the old leader for
//! `session_timeout`, and a new meta leader waits `session_timeout` before any failover, so a
//! lease holder has always stopped serving before a successor exists. Zombie safety still
//! rests on the Phase 12 fencing too (epochs, ISR, the log); the lease only closes the read
//! window.
//!
//! # Metadata reads (the barrier)
//!
//! Every meta read passes `read_index_barrier` (bounded by the request deadline and by
//! 2 × `session_timeout`) before serving locally, so a write acknowledged on any node is
//! visible on every node. A cut-off old meta leader cannot pass it (it never gets the quorum
//! acknowledgement): this closes the 12-18 follow-up (stale plain meta reads for about 2.6 s
//! on a cut-off old controller), because the gate is the barrier, not `is_leader` or the
//! contact age. A read carrying `ostro-read-after: m:<index>` that the local applied index
//! covers is served without the barrier.
//!
//! # Tokens
//!
//! - Partition write: `p<P>:<epoch>:<offset>`, `offset` = the partition's applied cursor
//!   (`cursor.next`, everything below is applied) read after the write returned. A write is
//!   acknowledged after it applied, so the cursor is an upper bound of the write's offset.
//!   A node covers the token when its handle epoch ≥ `epoch` and its applied cursor ≥ `offset`.
//! - Meta write: `m:<index>`, the local meta applied index after the write returned
//!   (proposals return once applied locally). Covered when the local applied index ≥ `index`.
//!
//! Correctness never depends on tokens: without one, the lease / barrier rules alone hold.
//! A malformed `ostro-read-after` answers 400.
//!
//! # Mutant
//!
//! `route::mutant::serve_without_lease` (failpoints build) skips the lease condition (3);
//! the zombie RYW scenarios must then fail.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use ostro_api::ApiError;
use ostro_api::request_meta::{HEADER_READ_AFTER, RequestMeta};
use ostro_meta::MetaHandle;
use ostro_types::{Epoch, PartitionId};

use super::BoxFuture;
use crate::p13::P13Ctx;

/// Failpoint name of the mutant that serves partition reads without the lease.
pub const MUTANT_SERVE_WITHOUT_LEASE: &str = "route::mutant::serve_without_lease";

/// Lease refusals (counter).
pub const LEASE_REJECTIONS_TOTAL: &str = "ostro_route_lease_rejections_total";
/// Barrier latency (histogram, seconds; meta reads and strict partition reads).
pub const READ_BARRIER_SECONDS: &str = "ostro_route_read_barrier_seconds";

/// How often a read waiting for a fresh lease anchor looks again.
const ANCHOR_POLL: Duration = Duration::from_millis(5);
/// Upper bound of that wait (also bounded by a quarter of the lease and the deadline).
const ANCHOR_WAIT_MAX: Duration = Duration::from_millis(500);
/// How often the keeper asks for the anchor so the meta follower's lease rounds keep
/// running (13-05 runs them only while asked for within the last 10 s).
const KEEPER_PERIOD: Duration = Duration::from_secs(1);

// ---------------------------------------------------------------------------------
// Probes
// ---------------------------------------------------------------------------------

static BARRIERS: AtomicU64 = AtomicU64::new(0);
static LEASE_REFUSALS: AtomicU64 = AtomicU64::new(0);
static TOKEN_SKIPS: AtomicU64 = AtomicU64::new(0);

/// Process-wide counters of the hooks (test probes; the metrics carry the same).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConsistencyStats {
    /// Read barriers taken (meta reads and strict partition reads).
    pub barriers: u64,
    /// Partition reads refused (`not_leader`).
    pub lease_refusals: u64,
    /// Barriers skipped because the request's token was covered.
    pub token_skips: u64,
}

/// The current [`ConsistencyStats`].
pub fn stats() -> ConsistencyStats {
    ConsistencyStats {
        barriers: BARRIERS.load(Ordering::Relaxed),
        lease_refusals: LEASE_REFUSALS.load(Ordering::Relaxed),
        token_skips: TOKEN_SKIPS.load(Ordering::Relaxed),
    }
}

// ---------------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------------

/// A parsed consistency token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Token {
    /// `p<P>:<epoch>:<offset>`.
    Partition {
        /// The partition.
        p: PartitionId,
        /// The leader epoch of the write.
        epoch: Epoch,
        /// The applied cursor after the write.
        offset: u64,
    },
    /// `m:<index>`.
    Meta {
        /// The meta applied index after the write.
        index: u64,
    },
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::Partition { p, epoch, offset } => write!(f, "p{}:{}:{offset}", p.0, epoch.0),
            Token::Meta { index } => write!(f, "m:{index}"),
        }
    }
}

fn malformed(s: &str) -> ApiError {
    ApiError::invalid_argument(format!(
        "malformed {HEADER_READ_AFTER} {s:?} (expected p<partition>:<epoch>:<offset> or m:<index>)"
    ))
    .with_details(serde_json::json!({ "header": HEADER_READ_AFTER }))
}

/// Parse a token (`ostro-read-after`); malformed → 400 `invalid_argument`.
pub fn parse_token(s: &str) -> Result<Token, ApiError> {
    let t = s.trim();
    if let Some(rest) = t.strip_prefix("m:") {
        let index = rest.parse::<u64>().map_err(|_| malformed(s))?;
        return Ok(Token::Meta { index });
    }
    let Some(rest) = t.strip_prefix('p') else {
        return Err(malformed(s));
    };
    let mut it = rest.split(':');
    let (Some(p), Some(e), Some(o), None) = (it.next(), it.next(), it.next(), it.next()) else {
        return Err(malformed(s));
    };
    let p = p.parse::<u16>().map_err(|_| malformed(s))?;
    let e = e.parse::<u32>().map_err(|_| malformed(s))?;
    let o = o.parse::<u64>().map_err(|_| malformed(s))?;
    Ok(Token::Partition {
        p: PartitionId(p),
        epoch: Epoch(e),
        offset: o,
    })
}

fn read_after(meta: &RequestMeta) -> Result<Option<Token>, ApiError> {
    meta.read_after.as_deref().map(parse_token).transpose()
}

// ---------------------------------------------------------------------------------
// The node as the rules see it
// ---------------------------------------------------------------------------------

/// What the consistency rules read from a node. [`NodeReadEnv`] is the production
/// implementation; tests plug in fakes (fake time, roles, anchors and barriers) or a real
/// metadata group.
pub trait ReadEnv: Send + Sync {
    /// This node's id.
    fn node_id(&self) -> u64;
    /// Monotonic now (the lease is never measured in wall time).
    fn now(&self) -> Instant;
    /// `[replication] session_timeout`.
    fn session_timeout(&self) -> Duration;
    /// `[api] lease_margin` (0 < m < 0.5).
    fn lease_margin(&self) -> f64;
    /// `[api] strict_reads`.
    fn strict_reads(&self) -> bool;
    /// `Some(epoch)` when this node leads `p` at `epoch`: role `Leader{epoch}` (its
    /// `LeaderAnnounced` applied) and the handle stamps `epoch`.
    fn leading(&self, p: PartitionId) -> Option<Epoch>;
    /// A graceful handoff froze `p` (13-11).
    fn in_handoff(&self, p: PartitionId) -> bool;
    /// The metadata lease anchor (13-05; asking also keeps a follower's rounds running).
    fn meta_lease_anchor(&self) -> Option<Instant>;
    /// The published leader of `p` (a hint for the refusal).
    fn leader_hint(&self, p: PartitionId) -> Option<u64>;
    /// `(handle epoch, applied cursor)` of `p` when it runs here.
    fn applied(&self, p: PartitionId) -> Option<(Epoch, u64)>;
    /// The local meta applied index.
    fn meta_applied(&self) -> u64;
    /// `MetaHandle::read_index_barrier_until(deadline)`.
    fn read_index_barrier(&self, deadline: Instant) -> BoxFuture<'_, Result<u64, String>>;
    /// `(leader, epoch)` of `p` in the local meta view.
    fn assignment(&self, p: PartitionId) -> Option<(u64, Epoch)>;
    /// Sleep `d` (fake time advances its clock).
    fn sleep(&self, d: Duration) -> BoxFuture<'_, ()>;
}

/// `session_timeout × (1 − margin)`: how long a lease anchor stays good.
pub fn lease_limit(session_timeout: Duration, margin: f64) -> Duration {
    let m = if margin.is_finite() {
        margin.clamp(0.0, 0.99)
    } else {
        0.2
    };
    session_timeout.mul_f64(1.0 - m)
}

/// Whether `anchor` grants a lease at `now`.
pub fn lease_holds(anchor: Option<Instant>, now: Instant, limit: Duration) -> bool {
    anchor.is_some_and(|a| now.saturating_duration_since(a) < limit)
}

#[cfg(feature = "failpoints")]
fn serve_without_lease() -> bool {
    (|| {
        fail::fail_point!("route::mutant::serve_without_lease", |_| true);
        false
    })()
}

#[cfg(not(feature = "failpoints"))]
#[inline(always)]
fn serve_without_lease() -> bool {
    false
}

fn not_leader(env: &dyn ReadEnv, p: PartitionId, why: &str) -> ApiError {
    LEASE_REFUSALS.fetch_add(1, Ordering::Relaxed);
    metrics::counter!(LEASE_REJECTIONS_TOTAL).increment(1);
    let hint = env
        .leader_hint(p)
        .filter(|l| *l != 0 && *l != env.node_id());
    ApiError::unavailable(format!("partition {} cannot serve reads here: {why}", p.0)).with_details(
        serde_json::json!({
            "reason": "not_leader",
            "partition": p.0,
            "leader": hint,
        }),
    )
}

fn barrier_deadline(env: &dyn ReadEnv, meta: &RequestMeta) -> Instant {
    let cap = env.now() + env.session_timeout() * 2;
    meta.deadline.map_or(cap, |d| d.min(cap))
}

async fn barrier(env: &dyn ReadEnv, deadline: Instant) -> Result<u64, String> {
    BARRIERS.fetch_add(1, Ordering::Relaxed);
    let t0 = env.now();
    let r = env.read_index_barrier(deadline).await;
    metrics::histogram!(READ_BARRIER_SECONDS)
        .record(env.now().saturating_duration_since(t0).as_secs_f64());
    r
}

/// The lease check (condition 3), waiting briefly for a fresh anchor: the first ask on a
/// meta follower starts its lease rounds and may answer `None`, and an anchor that went
/// stale while nobody asked refreshes within one heartbeat round. A cut-off node never
/// gets one and is refused after the wait.
async fn lease(env: &dyn ReadEnv, meta: &RequestMeta) -> bool {
    let limit = lease_limit(env.session_timeout(), env.lease_margin());
    if lease_holds(env.meta_lease_anchor(), env.now(), limit) {
        return true;
    }
    let mut until = env.now() + (limit / 4).min(ANCHOR_WAIT_MAX);
    if let Some(d) = meta.deadline {
        until = until.min(d);
    }
    while env.now() < until {
        env.sleep(ANCHOR_POLL).await;
        if lease_holds(env.meta_lease_anchor(), env.now(), limit) {
            return true;
        }
    }
    false
}

/// `leading(p)`, waiting (bounded by `session_timeout` and the deadline) while the
/// metadata already names this node as `p`'s leader and its takeover (the
/// `LeaderAnnounced` of the new term) is still running: the router re-routes only once, and
/// a call routed (or forwarded) here by the published assignment would otherwise answer
/// `not_leader` for the few milliseconds of the takeover.
async fn leading_or_takeover(
    env: &dyn ReadEnv,
    p: PartitionId,
    meta: &RequestMeta,
) -> Option<Epoch> {
    if let Some(e) = env.leading(p) {
        return Some(e);
    }
    if env.leader_hint(p) != Some(env.node_id()) {
        return None;
    }
    let mut until = env.now() + env.session_timeout();
    if let Some(d) = meta.deadline {
        until = until.min(d);
    }
    while env.now() < until {
        env.sleep(ANCHOR_POLL).await;
        if let Some(e) = env.leading(p) {
            return Some(e);
        }
        if env.leader_hint(p) != Some(env.node_id()) {
            return None;
        }
    }
    None
}

fn covers_partition(env: &dyn ReadEnv, p: PartitionId, tok: Option<Token>) -> Option<bool> {
    match tok {
        Some(Token::Partition {
            p: q,
            epoch,
            offset,
        }) if q == p => Some(
            env.applied(p)
                .is_some_and(|(e, o)| e >= epoch && o >= offset),
        ),
        _ => None,
    }
}

/// The partition-read rules (see the module docs).
pub async fn check_partition_read(
    env: &dyn ReadEnv,
    p: PartitionId,
    meta: &RequestMeta,
) -> Result<(), ApiError> {
    let tok = read_after(meta)?;
    let Some(epoch) = leading_or_takeover(env, p, meta).await else {
        return Err(not_leader(env, p, "this node does not lead it"));
    };
    if env.in_handoff(p) {
        return Err(not_leader(env, p, "a leadership handoff is in progress"));
    }
    if !serve_without_lease() && !lease(env, meta).await {
        return Err(not_leader(env, p, "no metadata lease (meta contact lost)"));
    }
    let covered = covers_partition(env, p, tok);
    if covered == Some(true) {
        if env.strict_reads() {
            TOKEN_SKIPS.fetch_add(1, Ordering::Relaxed);
        }
        return Ok(());
    }
    if !env.strict_reads() && covered.is_none() {
        return Ok(());
    }
    // Strict reads, or a token this leader does not cover yet: confirm with meta.
    let deadline = barrier_deadline(env, meta);
    if let Err(e) = barrier(env, deadline).await {
        return Err(not_leader(env, p, &format!("metadata barrier failed: {e}")));
    }
    if env.assignment(p) != Some((env.node_id(), epoch)) || env.leading(p) != Some(epoch) {
        return Err(not_leader(env, p, "the metadata assignment moved"));
    }
    if covered == Some(false) {
        // Leader at the current epoch: the token's (committed) offset is at most the high
        // watermark; wait for the apply to reach it.
        while covers_partition(env, p, tok) != Some(true) {
            if env.now() >= deadline {
                return Err(not_leader(env, p, "the token is ahead of this leader"));
            }
            env.sleep(ANCHOR_POLL).await;
        }
    }
    Ok(())
}

/// The meta-read rules (see the module docs).
pub async fn check_meta_read(env: &dyn ReadEnv, meta: &RequestMeta) -> Result<(), ApiError> {
    if let Some(Token::Meta { index }) = read_after(meta)?
        && env.meta_applied() >= index
    {
        TOKEN_SKIPS.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    let deadline = barrier_deadline(env, meta);
    barrier(env, deadline).await.map(|_| ()).map_err(|e| {
        ApiError::unavailable(format!("metadata read barrier did not complete: {e}")).with_details(
            serde_json::json!({
                "reason": "meta_unavailable",
                "retry_after_ms": super::RETRY_AFTER_MS,
            }),
        )
    })
}

/// The token after a partition write.
pub fn partition_token(env: &dyn ReadEnv, p: PartitionId) -> Option<String> {
    let (epoch, offset) = env.applied(p)?;
    Some(Token::Partition { p, epoch, offset }.to_string())
}

/// The token after a meta write.
pub fn meta_token(env: &dyn ReadEnv) -> Option<String> {
    Some(
        Token::Meta {
            index: env.meta_applied(),
        }
        .to_string(),
    )
}

// ---------------------------------------------------------------------------------
// The node
// ---------------------------------------------------------------------------------

/// [`ReadEnv`] over a node's [`P13Ctx`].
pub struct NodeReadEnv<'a>(pub &'a P13Ctx);

impl std::fmt::Debug for NodeReadEnv<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("NodeReadEnv")
            .field(&self.0.node_id())
            .finish()
    }
}

impl ReadEnv for NodeReadEnv<'_> {
    fn node_id(&self) -> u64 {
        self.0.node_id()
    }

    fn now(&self) -> Instant {
        // clock-exempt: the lease is measured on the monotonic clock (D-P13-15), like the
        // 13-05 anchor it is compared with.
        Instant::now()
    }

    fn session_timeout(&self) -> Duration {
        self.0.cfg.replication.session_timeout.get()
    }

    fn lease_margin(&self) -> f64 {
        self.0.cfg.api.lease_margin
    }

    fn strict_reads(&self) -> bool {
        self.0.cfg.api.strict_reads
    }

    fn leading(&self, p: PartitionId) -> Option<Epoch> {
        let node = self.0.node()?;
        let role = *node.role(p)?.borrow();
        let ostro_partition::roles::PartitionRole::Leader { epoch } = role else {
            return None;
        };
        (node.handle(p)?.epoch() == epoch).then_some(epoch)
    }

    fn in_handoff(&self, p: PartitionId) -> bool {
        use ostro_partition::HandoffControl as _;
        self.0.node().is_some_and(|n| n.in_handoff(p))
    }

    fn meta_lease_anchor(&self) -> Option<Instant> {
        self.0.meta.lease_anchor()
    }

    fn leader_hint(&self, p: PartitionId) -> Option<u64> {
        self.0
            .cluster
            .assignment_watch()
            .borrow()
            .iter()
            .find(|a| a.partition == p)
            .map(|a| a.leader)
    }

    fn applied(&self, p: PartitionId) -> Option<(Epoch, u64)> {
        let h = self.0.node()?.handle(p)?;
        let applied = h.machine().applied().borrow().0;
        Some((h.epoch(), applied))
    }

    fn meta_applied(&self) -> u64 {
        self.0.meta.raft_status().applied_index
    }

    fn read_index_barrier(&self, deadline: Instant) -> BoxFuture<'_, Result<u64, String>> {
        Box::pin(async move {
            self.0
                .meta
                .read_index_barrier_until(tokio::time::Instant::from_std(deadline))
                .await
                .map_err(|e| e.to_string())
        })
    }

    fn assignment(&self, p: PartitionId) -> Option<(u64, Epoch)> {
        self.0.meta.view().read(|s| {
            s.partitions()
                .find(|a| a.value.partition == p.0)
                .map(|a| (a.value.leader, Epoch(a.value.epoch)))
        })
    }

    fn sleep(&self, d: Duration) -> BoxFuture<'_, ()> {
        Box::pin(tokio::time::sleep(d))
    }
}

/// Keep the meta follower's lease rounds running while the node lives (13-05: rounds run
/// only while the anchor was asked for within the last 10 s; the first ask may answer
/// `None`). One task per metadata handle, started on the first hook call; it holds the
/// handle weakly and ends with it.
fn ensure_keeper(meta: &Arc<MetaHandle>) {
    static KEPT: Mutex<Vec<Weak<MetaHandle>>> = Mutex::new(Vec::new());
    let mut kept = KEPT.lock().unwrap_or_else(|e| e.into_inner());
    kept.retain(|w| w.strong_count() > 0);
    if kept
        .iter()
        .any(|w| std::ptr::eq(w.as_ptr(), Arc::as_ptr(meta)))
    {
        return;
    }
    let Ok(rt) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let weak = Arc::downgrade(meta);
    kept.push(weak.clone());
    drop(kept);
    rt.spawn(async move {
        loop {
            tokio::time::sleep(KEEPER_PERIOD).await;
            let Some(m) = weak.upgrade() else { break };
            let _ = m.lease_anchor();
            if Arc::strong_count(&m) == 1 {
                // The last owner went away meanwhile: dropping the handle joins the
                // driver thread, which must not happen on a runtime worker.
                tokio::task::spawn_blocking(move || drop(m));
                break;
            }
        }
    });
}

// ---------------------------------------------------------------------------------
// The hooks (fixed signatures, 13-02 / 13-12 call sites)
// ---------------------------------------------------------------------------------

fn current_meta() -> RequestMeta {
    RequestMeta::current().unwrap_or_default()
}

/// Before a partition read on this node: the lease (and with `strict_reads` the barrier +
/// epoch check).
pub async fn before_partition_read(ctx: &P13Ctx, p: PartitionId) -> Result<(), ApiError> {
    ensure_keeper(&ctx.meta);
    check_partition_read(&NodeReadEnv(ctx), p, &current_meta()).await
}

/// Before a meta read on this node: the ReadIndex barrier (skipped for a covered token).
pub async fn before_meta_read(ctx: &P13Ctx) -> Result<(), ApiError> {
    ensure_keeper(&ctx.meta);
    check_meta_read(&NodeReadEnv(ctx), &current_meta()).await
}

/// The consistency token after a partition write (`p<P>:<epoch>:<offset>`).
pub fn token_after_partition_write(ctx: &P13Ctx, p: PartitionId) -> Option<String> {
    partition_token(&NodeReadEnv(ctx), p)
}

/// The consistency token after a meta write (`m:<index>`).
pub fn token_after_meta_write(ctx: &P13Ctx) -> Option<String> {
    meta_token(&NodeReadEnv(ctx))
}
