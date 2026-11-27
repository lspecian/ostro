//! Phase 13 routing (API-08, D-P13-14, plan 13-12); the consistency hooks in
//! [`consistency`] are 13-13's.
//!
//! The seam ([`crate::p13`]) calls three fixed entry points: [`wrap_control_plane`] at the
//! single control-plane construction site (every API, UI and in-process consumer gets the
//! result), [`wrap_extensions`] over the filled `ApiExtensions` slots and
//! [`node_api_services`] for the cluster listener. The start path reads the forwarded
//! partition pin through [`table::pinned_partition`].
//!
//! | file | contents |
//! |------|----------|
//! | [`table`] | route classes per method, owner resolution, the partition of a call, the honoured `ostro-partition` pin |
//! | [`peer`] | `PeerControlPlane` over the 13-03 gRPC client and the cluster transport |
//! | [`fanout`] | partition / node fan-out merging (complete-or-error) |
//! | [`consistency`] | lease / barrier / token hooks (13-13) |
//!
//! # Rules ([`RouteCore`])
//!
//! - **Local** (this node leads, or the call was forwarded in): partition reads call
//!   `before_partition_read(p)`, partition writes `before_partition_write(p)` (p13-fixes)
//!   and meta reads `before_meta_read()` before the inner call;
//!   partition writes publish `token_after_partition_write(p)` and meta writes
//!   `token_after_meta_write()` after a successful inner call (plan check B4).
//! - **Forward** (another node leads and `RequestMeta.hop == 0`): the call goes to that
//!   node's cluster listener inside a `RequestMeta` scope with `hop = 1`; the peer serves it
//!   locally whatever its view says (one hop at most, never a loop).
//! - **Retry once:** a `not_leader` or `leader_changed` answer (handoff fence, plan check
//!   W13), from the peer or the local inner call, or a transport failure to the peer, waits
//!   until the local assignment view moves past the epoch the call was routed on (bounded
//!   by min(remaining deadline, 2 × `[replication] session_timeout`)), re-routes once, then
//!   answers 503 `unavailable` + `Retry-After: 1` (gRPC `UNAVAILABLE`). An offline
//!   partition answers the same 503 immediately.
//! - **Unkeyed start:** `[api] unkeyed_start_placement = "local"` picks uniformly among
//!   the partitions this node leads (zero hops); with none led (or `"uniform"`) a uniform
//!   partition, forwarded with `ostro-partition` when another node leads it. The start
//!   path honours the pin only from node identities ([`table::pinned_partition`]).
//!
//! Metrics: `ostro_route_requests_total{class, target=local|forwarded|offline, result}`
//! and `ostro_route_forward_seconds`.

pub mod consistency;
pub mod fanout;
pub mod peer;
pub mod table;

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use ostro_api::domain::*;
use ostro_api::phase7::{
    CreateWorkerTokenRequest, DemandSnapshot, DrainResult, JoinRequest, JoinResponse, PauseState,
    QueueFlow, QueueFlowInfo, RunPauseState, WorkerInfo, WorkerTokenCreated, WorkerTokenInfo,
};
use ostro_api::request_meta::{self, RequestMeta};
use ostro_api::{ApiError, ApiExtensions, ControlPlane, ErrorCode};
use ostro_partition::roles::PartitionAssignmentView;
use ostro_types::{Epoch, PartitionCount, PartitionId};
use tokio::sync::watch;
use tonic::service::Routes;

use crate::control::ServerControlPlane;
use crate::p13::P13Ctx;
use peer::{PeerApi, PeerControlPlane};
use table::{RouteClass, Target};

/// `BoxFuture` without a `futures` dependency in this crate.
pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A routed call's result with its type erased: the routing logic ([`RouteCore`]) is
/// compiled once instead of once per routed method (binary size, docs/binary-size.md).
type AnyOut = Box<dyn std::any::Any + Send>;

/// A type-erased routed call.
type AnyFut<'a> = BoxFuture<'a, Result<AnyOut, ApiError>>;

/// The local branch of a type-erased routed call.
/// (`'s`: the borrow of the branch itself, `'f`: what its futures borrow.)
type LocalFn<'s, 'f> = dyn Fn() -> AnyFut<'f> + Send + Sync + 's;

/// The forwarding branch of a type-erased routed call.
type RemoteFn<'s, 'f> = dyn Fn(PeerApi) -> AnyFut<'f> + Send + Sync + 's;

/// Erase a routed call's result type.
fn erase<'a, T, F>(fut: F) -> AnyFut<'a>
where
    T: Send + 'static,
    F: Future<Output = Result<T, ApiError>> + Send + 'a,
{
    Box::pin(async move { fut.await.map(|v| Box::new(v) as AnyOut) })
}

/// Restore the result type [`erase`] removed (the same call, so it always matches).
fn unerase<T: 'static>(r: Result<AnyOut, ApiError>) -> Result<T, ApiError> {
    match r?.downcast::<T>() {
        Ok(v) => Ok(*v),
        Err(_) => Err(ApiError::internal("route: result type mismatch")),
    }
}

/// How long a refused call waits for the view to move, as a multiple of
/// `[replication] session_timeout` (the remaining deadline bounds it too).
pub const RETRY_WAIT_SESSIONS: u32 = 2;
/// `Retry-After` of the router's 503 answers, in milliseconds (`details.retry_after_ms`).
pub const RETRY_AFTER_MS: u64 = 1000;

// ---------------------------------------------------------------------------------
// What the router needs from its node
// ---------------------------------------------------------------------------------

/// The node as the router sees it. [`NodeEnv`] is the production implementation over
/// [`P13Ctx`]; the unit tests plug in fakes (assignment view, peers, recording hooks).
pub trait RouteEnv: Send + Sync + 'static {
    /// This node's id.
    fn local_node(&self) -> u64;
    /// The fixed partition count.
    fn partition_count(&self) -> PartitionCount;
    /// The owner of `p` and the assignment epoch it was resolved on ([`table::owner`]).
    fn owner(&self, p: PartitionId) -> (Target, Epoch);
    /// The partitions this node leads now.
    fn led(&self) -> Vec<PartitionId>;
    /// Every other member node (node fan-out).
    fn members(&self) -> Vec<u64>;
    /// The meta leader, when known.
    fn meta_leader(&self) -> Option<u64>;
    /// `[api] unkeyed_start_placement = "local"`.
    fn unkeyed_local(&self) -> bool;
    /// A uniform index in `0..n` (`n > 0`).
    fn uniform(&self, n: usize) -> usize;
    /// 2 × `[replication] session_timeout`.
    fn retry_wait(&self) -> Duration;
    /// Wait until [`RouteEnv::owner`] of `p` differs from `seen`, at most `limit`.
    fn wait_change(
        &self,
        p: PartitionId,
        seen: (Target, Epoch),
        limit: Duration,
    ) -> BoxFuture<'_, ()>;
    /// `node`'s API over the cluster listener.
    fn peer(&self, node: u64) -> Result<PeerApi, ApiError>;
    /// Drop `node`'s cached client after a transport failure.
    fn forget_peer(&self, node: u64);
    /// 13-13: before a partition read on this node.
    fn before_partition_read(&self, p: PartitionId) -> BoxFuture<'_, Result<(), ApiError>>;
    /// p13-fixes: before a partition write on this node (leading + the lease).
    fn before_partition_write(&self, p: PartitionId) -> BoxFuture<'_, Result<(), ApiError>>;
    /// 13-13: before a meta read on this node.
    fn before_meta_read(&self) -> BoxFuture<'_, Result<(), ApiError>>;
    /// 13-13: the token after a partition write on this node.
    fn token_after_partition_write(&self, p: PartitionId) -> Option<String>;
    /// 13-13: the token after a meta write on this node.
    fn token_after_meta_write(&self) -> Option<String>;
    /// 13-18: the gate of a submitting `migrate_runs`, decided **once at the entry node**
    /// before any leg runs (13-17 deviation 8: each leg checked after its own barrier, so a
    /// finalization committing during the fan-out could refuse one leg after another had
    /// repinned). After this node's barrier saw the cluster finalized, every leg's barrier
    /// sees it too (finalization is monotonic). Default: open (unit-test environments).
    fn migrate_gate(&self) -> BoxFuture<'_, Result<(), ApiError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Per-node routing counters (test probes; the metrics carry the same information).
#[derive(Debug, Default)]
pub struct RouteStats {
    /// Calls served by this node's inner control plane (including forwarded-in calls).
    pub local: AtomicU64,
    /// Calls forwarded to a peer (each re-route counts again; fan-out legs included).
    pub forwarded: AtomicU64,
    /// Calls answered 503 because the partition is offline.
    pub offline: AtomicU64,
    /// Re-routes after a refused call.
    pub rerouted: AtomicU64,
    /// 503 answers after the one re-route.
    pub exhausted: AtomicU64,
    /// Calls that arrived forwarded (`hop >= 1`).
    pub forwarded_in: AtomicU64,
}

impl RouteStats {
    /// `forwarded` now.
    pub fn forwarded(&self) -> u64 {
        self.forwarded.load(Ordering::Relaxed)
    }
}

/// What a local call does around the inner call (plan check B4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    /// A partition read: `before_partition_read(p)` first.
    PartitionRead(PartitionId),
    /// A partition write: `before_partition_write(p)` first (leading + the lease), and
    /// `token_after_partition_write(p)` after success.
    PartitionWrite(PartitionId),
    /// A meta read: `before_meta_read()` first.
    MetaRead,
    /// A meta write: `token_after_meta_write()` after success.
    MetaWrite,
    /// A meta write that validates against metadata first (13-18: activation's
    /// `module_retired` check): `before_meta_read()` first, `token_after_meta_write()` after
    /// success.
    MetaReadWrite,
    /// Neither (uploads, tooling, calls whose partition is unknown).
    None,
}

/// The shared routing engine of the control plane and every extension decorator.
pub struct RouteCore {
    env: Arc<dyn RouteEnv>,
    stats: Arc<RouteStats>,
}

impl std::fmt::Debug for RouteCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouteCore")
            .field("node", &self.env.local_node())
            .finish_non_exhaustive()
    }
}

fn now() -> Instant {
    // clock-exempt: request deadlines and forward latency are wall-time budgets.
    Instant::now()
}

fn current_meta() -> RequestMeta {
    RequestMeta::current().unwrap_or_default()
}

fn remaining(meta: &RequestMeta) -> Option<Duration> {
    meta.deadline.map(|d| d.saturating_duration_since(now()))
}

fn record(class: RouteClass, target: &'static str, ok: bool) {
    metrics::counter!(
        "ostro_route_requests_total",
        "class" => class.as_str(),
        "target" => target,
        "result" => if ok { "ok" } else { "error" }
    )
    .increment(1);
}

/// Whether a refused call is re-routed: `not_leader` / `leader_changed` (handoff fence, W13)
/// from anywhere, and a reason-less `unavailable` (transport failure) from a peer.
pub fn retryable(e: &ApiError, from_peer: bool) -> bool {
    if e.code != ErrorCode::Unavailable {
        return false;
    }
    match e.reason() {
        Some("not_leader" | "leader_changed" | "peer_unreachable") => true,
        None => from_peer,
        Some(_) => false,
    }
}

fn with_retry_after(mut details: serde_json::Value, p: Option<PartitionId>) -> serde_json::Value {
    if !details.is_object() {
        details = serde_json::json!({});
    }
    if let Some(o) = details.as_object_mut() {
        o.insert("retry_after_ms".into(), RETRY_AFTER_MS.into());
        if let Some(p) = p {
            o.entry("partition").or_insert(p.0.into());
        }
    }
    details
}

/// The 503 for an offline partition (no eligible in-sync replica). Keeps the D-P12-21
/// leader fields (`leader` / `leader_addr` null: no node leads it).
pub fn offline_error(p: PartitionId) -> ApiError {
    ApiError::unavailable(format!(
        "partition {} has no leader (offline: no in-sync replica can lead it); retry later",
        p.0
    ))
    .with_details(with_retry_after(
        serde_json::json!({
            "reason": "partition_offline",
            "partition": p.0,
            "leader": null,
            "leader_addr": null,
        }),
        Some(p),
    ))
}

/// The 503 after the one re-route: the last refusal's details plus `retry_after_ms`.
fn exhausted_error(p: PartitionId, last: ApiError) -> ApiError {
    let details = last
        .details
        .clone()
        .unwrap_or_else(|| serde_json::json!({ "reason": "not_leader" }));
    ApiError::unavailable(format!(
        "partition {}: the leader did not answer after one re-route ({}); retry later",
        p.0, last.message
    ))
    .with_details(with_retry_after(details, Some(p)))
}

fn deadline_error() -> ApiError {
    ApiError::unavailable("the request deadline passed while routing; retry").with_details(
        with_retry_after(serde_json::json!({ "reason": "deadline" }), None),
    )
}

fn no_slot(what: &str) -> ApiError {
    ApiError::unavailable(format!("the peer does not serve {what}"))
}

/// Poll every future of `futs` to completion on this task (no `futures` dependency); the
/// results keep the input order.
pub(crate) async fn join_all<T>(futs: Vec<BoxFuture<'_, T>>) -> Vec<T> {
    let mut futs: Vec<Option<BoxFuture<'_, T>>> = futs.into_iter().map(Some).collect();
    let mut out: Vec<Option<T>> = futs.iter().map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut pending = false;
        for (i, slot) in futs.iter_mut().enumerate() {
            if let Some(f) = slot {
                match f.as_mut().poll(cx) {
                    std::task::Poll::Ready(v) => {
                        out[i] = Some(v);
                        *slot = None;
                    }
                    std::task::Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(())
        }
    })
    .await;
    out.into_iter().flatten().collect()
}

impl RouteCore {
    /// A core over `env` with fresh counters.
    pub fn new(env: Arc<dyn RouteEnv>) -> Self {
        Self::with_stats(env, Arc::new(RouteStats::default()))
    }

    /// A core over `env` counting into `stats`.
    pub fn with_stats(env: Arc<dyn RouteEnv>, stats: Arc<RouteStats>) -> Self {
        Self { env, stats }
    }

    /// The counters.
    pub fn stats(&self) -> &Arc<RouteStats> {
        &self.stats
    }

    /// The node.
    pub fn env(&self) -> &Arc<dyn RouteEnv> {
        &self.env
    }

    fn count(&self) -> PartitionCount {
        self.env.partition_count()
    }

    /// The local branch: hooks around the inner call (direct and forwarded-in calls).
    pub async fn local<T, F>(&self, access: Access, fut: F) -> Result<T, ApiError>
    where
        F: Future<Output = Result<T, ApiError>>,
    {
        match access {
            Access::PartitionRead(p) => self.env.before_partition_read(p).await?,
            Access::PartitionWrite(p) => self.env.before_partition_write(p).await?,
            Access::MetaRead | Access::MetaReadWrite => self.env.before_meta_read().await?,
            _ => {}
        }
        self.stats.local.fetch_add(1, Ordering::Relaxed);
        let out = fut.await?;
        let token = match access {
            Access::PartitionWrite(p) => self.env.token_after_partition_write(p),
            Access::MetaWrite | Access::MetaReadWrite => self.env.token_after_meta_write(),
            _ => None,
        };
        if let Some(t) = token {
            request_meta::set_consistency(t);
        }
        Ok(out)
    }

    /// Forward to `node` with `hop = 1` (plus `edit` of the outgoing metadata); the peer's
    /// consistency token is copied into the caller's out-slot.
    async fn forward<T, R, RF>(
        &self,
        node: u64,
        meta: &RequestMeta,
        edit: impl FnOnce(&mut RequestMeta),
        remote: &R,
    ) -> Result<T, ApiError>
    where
        R: Fn(PeerApi) -> RF + ?Sized,
        RF: Future<Output = Result<T, ApiError>>,
    {
        let peer = match self.env.peer(node) {
            Ok(p) => p,
            Err(e) => {
                self.env.forget_peer(node);
                return Err(e);
            }
        };
        let mut out = RequestMeta {
            hop: 1,
            partition_pin: None,
            partitions: None,
            ..meta.clone()
        };
        edit(&mut out);
        // Phase 14 (14-15): the caller's principal travels as `ostro-principal`.
        peer::stamp_principal(&mut out);
        self.stats.forwarded.fetch_add(1, Ordering::Relaxed);
        let t0 = now();
        let (res, token) = request_meta::scope_with_consistency(out, remote(peer)).await;
        metrics::histogram!("ostro_route_forward_seconds").record(t0.elapsed().as_secs_f64());
        if let Some(t) = token {
            request_meta::set_consistency(t);
        }
        if let Err(e) = &res
            && e.code == ErrorCode::Unavailable
            && matches!(e.reason(), None | Some("peer_unreachable"))
        {
            self.env.forget_peer(node);
        }
        res
    }

    /// A partition-scoped call (run id, key, token, pinned unkeyed start). `p = None`
    /// (malformed id) runs locally so the inner call answers. `pin` forwards `p` as
    /// `ostro-partition`.
    ///
    /// Generic shim over [`Self::partition_dyn`]: the routing logic is compiled once, not
    /// once per call site (binary size, docs/binary-size.md).
    pub async fn partition<T, L, LF, R, RF>(
        &self,
        class: RouteClass,
        p: Option<PartitionId>,
        write: bool,
        pin: bool,
        local: L,
        remote: R,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        L: Fn() -> LF + Send + Sync,
        LF: Future<Output = Result<T, ApiError>> + Send,
        R: Fn(PeerApi) -> RF + Send + Sync,
        RF: Future<Output = Result<T, ApiError>> + Send,
    {
        let local = || erase(local());
        let remote = |peer: PeerApi| erase(remote(peer));
        unerase(
            self.partition_dyn(class, p, write, pin, &local, &remote)
                .await,
        )
    }

    /// [`Self::partition`] over type-erased calls.
    fn partition_dyn<'s, 'f: 's>(
        &'s self,
        class: RouteClass,
        p: Option<PartitionId>,
        write: bool,
        pin: bool,
        local: &'s LocalFn<'s, 'f>,
        remote: &'s RemoteFn<'s, 'f>,
    ) -> BoxFuture<'s, Result<AnyOut, ApiError>> {
        Box::pin(async move {
            let meta = current_meta();
            let Some(p) = p else {
                let r = self.local(Access::None, local()).await;
                record(class, "local", r.is_ok());
                return r;
            };
            let access = if write {
                Access::PartitionWrite(p)
            } else {
                Access::PartitionRead(p)
            };
            if meta.hop >= 1 {
                // Forwarded in: served here whatever the view says (one hop at most).
                self.stats.forwarded_in.fetch_add(1, Ordering::Relaxed);
                let r = self.local(access, local()).await;
                record(class, "local", r.is_ok());
                return r;
            }
            let mut rerouted = false;
            loop {
                let (target, epoch) = self.env.owner(p);
                let res = match target {
                    Target::Local => self.local(access, local()).await,
                    Target::Node(n) => {
                        self.forward(
                            n,
                            &meta,
                            |m| {
                                if pin {
                                    m.partition_pin = Some(p.0);
                                }
                            },
                            remote,
                        )
                        .await
                    }
                    Target::Offline(_) => {
                        self.stats.offline.fetch_add(1, Ordering::Relaxed);
                        record(class, "offline", false);
                        return Err(offline_error(p));
                    }
                };
                let e = match res {
                    Ok(v) => {
                        record(class, target.as_str(), true);
                        return Ok(v);
                    }
                    Err(e) => e,
                };
                if !retryable(&e, matches!(target, Target::Node(_))) {
                    record(class, target.as_str(), false);
                    return Err(e);
                }
                record(class, target.as_str(), false);
                if rerouted {
                    self.stats.exhausted.fetch_add(1, Ordering::Relaxed);
                    return Err(exhausted_error(p, e));
                }
                let budget = self.env.retry_wait();
                let budget = match remaining(&meta) {
                    Some(r) if r.is_zero() => return Err(deadline_error()),
                    Some(r) => r.min(budget),
                    None => budget,
                };
                rerouted = true;
                self.stats.rerouted.fetch_add(1, Ordering::Relaxed);
                self.env.wait_change(p, (target, epoch), budget).await;
                if remaining(&meta).is_some_and(|r| r.is_zero()) {
                    self.stats.exhausted.fetch_add(1, Ordering::Relaxed);
                    return Err(exhausted_error(p, e));
                }
            }
        })
    }

    /// A metadata read (local, behind the read hook).
    pub async fn meta_read<T, F>(&self, class: RouteClass, fut: F) -> Result<T, ApiError>
    where
        F: Future<Output = Result<T, ApiError>>,
    {
        let r = self.local(Access::MetaRead, fut).await;
        record(class, "local", r.is_ok());
        r
    }

    /// A metadata write (local; meta proposals forward to the meta leader themselves).
    pub async fn meta_write<T, F>(&self, class: RouteClass, fut: F) -> Result<T, ApiError>
    where
        F: Future<Output = Result<T, ApiError>>,
    {
        let r = self.local(Access::MetaWrite, fut).await;
        record(class, "local", r.is_ok());
        r
    }

    /// A metadata write that validates against metadata first ([`Access::MetaReadWrite`]).
    pub async fn meta_read_write<T, F>(&self, class: RouteClass, fut: F) -> Result<T, ApiError>
    where
        F: Future<Output = Result<T, ApiError>>,
    {
        let r = self.local(Access::MetaReadWrite, fut).await;
        record(class, "local", r.is_ok());
        r
    }

    /// A call served by this node only.
    pub async fn local_only<T, F>(&self, class: RouteClass, fut: F) -> Result<T, ApiError>
    where
        F: Future<Output = Result<T, ApiError>>,
    {
        let r = self.local(Access::None, fut).await;
        record(class, "local", r.is_ok());
        r
    }

    /// A call that runs on the meta leader (`upgrade_finalize`, `run_module_gc`): forwarded
    /// once when another node leads meta; a meta write here.
    pub async fn meta_leader<T, L, LF, R, RF>(
        &self,
        class: RouteClass,
        local: L,
        remote: R,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        L: Fn() -> LF + Send + Sync,
        LF: Future<Output = Result<T, ApiError>> + Send,
        R: Fn(PeerApi) -> RF + Send + Sync,
        RF: Future<Output = Result<T, ApiError>> + Send,
    {
        let local = || erase(local());
        let remote = |peer: PeerApi| erase(remote(peer));
        unerase(self.meta_leader_dyn(class, &local, &remote).await)
    }

    /// [`Self::meta_leader`] over type-erased calls.
    fn meta_leader_dyn<'s, 'f: 's>(
        &'s self,
        class: RouteClass,
        local: &'s LocalFn<'s, 'f>,
        remote: &'s RemoteFn<'s, 'f>,
    ) -> BoxFuture<'s, Result<AnyOut, ApiError>> {
        Box::pin(async move {
            let meta = current_meta();
            let me = self.env.local_node();
            match self.env.meta_leader() {
                Some(l) if l != me && meta.hop == 0 => {
                    let r = self.forward(l, &meta, |_| {}, remote).await;
                    record(class, "forwarded", r.is_ok());
                    r
                }
                _ => self.meta_write(class, local()).await,
            }
        })
    }

    /// An unkeyed start: a locally led partition (zero hops) with `"local"` placement,
    /// else a uniform partition, pinned (`ostro-partition`) wherever it runs.
    pub async fn unkeyed_start<T, L, LF, R, RF>(&self, local: L, remote: R) -> Result<T, ApiError>
    where
        T: Send + 'static,
        L: Fn() -> LF + Send + Sync,
        LF: Future<Output = Result<T, ApiError>> + Send,
        R: Fn(PeerApi) -> RF + Send + Sync,
        RF: Future<Output = Result<T, ApiError>> + Send,
    {
        let local = || erase(local());
        let remote = |peer: PeerApi| erase(remote(peer));
        unerase(self.unkeyed_start_dyn(&local, &remote).await)
    }

    /// [`Self::unkeyed_start`] over type-erased calls.
    fn unkeyed_start_dyn<'s, 'f: 's>(
        &'s self,
        local: &'s LocalFn<'s, 'f>,
        remote: &'s RemoteFn<'s, 'f>,
    ) -> BoxFuture<'s, Result<AnyOut, ApiError>> {
        Box::pin(async move {
            let meta = current_meta();
            if meta.hop >= 1 {
                // Forwarded in: the pin (if any) is honoured by the start path.
                self.stats.forwarded_in.fetch_add(1, Ordering::Relaxed);
                let r = self.local(Access::None, local()).await;
                record(RouteClass::UnkeyedStart, "local", r.is_ok());
                return r;
            }
            let led = self.env.led();
            let p = if self.env.unkeyed_local() && !led.is_empty() {
                led[self.env.uniform(led.len())]
            } else {
                let n = usize::from(self.count().get());
                PartitionId(u16::try_from(self.env.uniform(n)).unwrap_or(0))
            };
            let me = self.env.local_node();
            let pinned_local = move || {
                let scoped = RequestMeta {
                    partition_pin: Some(p.0),
                    from_node: Some(me),
                    ..current_meta()
                };
                let fut = local();
                let pinned: AnyFut<'f> = Box::pin(async move {
                    let (r, token) = request_meta::scope_with_consistency(scoped, fut).await;
                    if let Some(t) = token {
                        request_meta::set_consistency(t);
                    }
                    r
                });
                pinned
            };
            self.partition_dyn(
                RouteClass::UnkeyedStart,
                Some(p),
                true,
                true,
                &pinned_local,
                remote,
            )
            .await
        })
    }
}

// ---------------------------------------------------------------------------------
// The routed control plane
// ---------------------------------------------------------------------------------

/// The routed control plane over a local one (D-P13-14).
pub struct RoutedControlPlane {
    core: Arc<RouteCore>,
    inner: Arc<dyn ControlPlane>,
    _keep: Option<Arc<dyn std::any::Any + Send + Sync>>,
}

impl std::fmt::Debug for RoutedControlPlane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutedControlPlane")
            .field("core", &self.core)
            .finish_non_exhaustive()
    }
}

impl RoutedControlPlane {
    /// Route `inner` (this node's control plane) over `core`.
    pub fn new(core: Arc<RouteCore>, inner: Arc<dyn ControlPlane>) -> Self {
        Self {
            core,
            inner,
            _keep: None,
        }
    }

    /// Also keep `v` alive as long as this control plane (the per-node routing state).
    fn keep_alive(mut self, v: Arc<dyn std::any::Any + Send + Sync>) -> Self {
        self._keep = Some(v);
        self
    }

    /// The routing engine.
    pub fn core(&self) -> &Arc<RouteCore> {
        &self.core
    }

    fn run_p(&self, run_id: &str) -> Option<PartitionId> {
        table::run_partition(run_id, self.core.count())
    }
}

const CP: &str = "ControlPlane";

macro_rules! by_run {
    ($self:ident, $m:ident, $run:expr, $write:expr, $arg:ident) => {{
        let p = $self.run_p(&$run);
        $self
            .core
            .partition(
                table::class(CP, stringify!($m)),
                p,
                $write,
                false,
                || $self.inner.$m($arg.clone()),
                |peer: PeerApi| {
                    let a = $arg.clone();
                    async move { peer.cp.$m(a).await }
                },
            )
            .await
    }};
}

macro_rules! meta_r {
    ($self:ident, $m:ident $(, $a:expr)*) => {
        $self
            .core
            .meta_read(table::class(CP, stringify!($m)), $self.inner.$m($($a),*))
            .await
    };
}

macro_rules! meta_w {
    ($self:ident, $m:ident $(, $a:expr)*) => {
        $self
            .core
            .meta_write(table::class(CP, stringify!($m)), $self.inner.$m($($a),*))
            .await
    };
}

macro_rules! local_cp {
    ($self:ident, $m:ident $(, $a:expr)*) => {
        $self
            .core
            .local_only(table::class(CP, stringify!($m)), $self.inner.$m($($a),*))
            .await
    };
}

#[async_trait::async_trait]
impl ControlPlane for RoutedControlPlane {
    async fn upload_module(&self, req: UploadModule) -> Result<UploadModuleResult, ApiError> {
        local_cp!(self, upload_module, req)
    }
    async fn get_module(&self, sha256: String) -> Result<ModuleInfo, ApiError> {
        meta_r!(self, get_module, sha256)
    }
    async fn list_workflows(&self) -> Result<Vec<WorkflowInfo>, ApiError> {
        meta_r!(self, list_workflows)
    }
    async fn activate_workflow(&self, req: ActivateWorkflow) -> Result<WorkflowInfo, ApiError> {
        // 13-18: the barrier before the `module_retired` check (13-15 / 13-17 handover).
        self.core
            .meta_read_write(
                table::class(CP, "activate_workflow"),
                self.inner.activate_workflow(req),
            )
            .await
    }
    async fn start_run(&self, req: StartRunRequest) -> Result<StartRunResult, ApiError> {
        let key = req.idempotency_key.as_deref().filter(|k| !k.is_empty());
        let remote = |peer: PeerApi| {
            let r = req.clone();
            async move { peer.cp.start_run(r).await }
        };
        // 13-18: a start resolves its workflow version and checks `module_retired` on the
        // local metadata view. A start carrying `ostro-read-after: m:<index>` (e.g. after a
        // retire or an activation on another node) passes `before_meta_read` first, so it
        // sees that write; a start without a meta token adds no barrier (a start must not
        // wait on, or fail with, a metadata round trip: availability under a meta-quorum
        // loss and start latency, decision D-13-18-2).
        let local = || async {
            if consistency::carries_meta_token(&current_meta()) {
                self.core.env().before_meta_read().await?;
            }
            self.inner.start_run(req.clone()).await
        };
        match key {
            Some(k) => {
                let p = table::key_partition(
                    &req.workflow,
                    req.tenant.as_deref(),
                    k,
                    self.core.count(),
                );
                self.core
                    .partition(RouteClass::KeyPartition, p, true, false, local, remote)
                    .await
            }
            None => self.core.unkeyed_start(local, remote).await,
        }
    }
    async fn get_run(&self, run_id: String) -> Result<RunInfo, ApiError> {
        by_run!(self, get_run, run_id, false, run_id)
    }
    async fn run_history(&self, req: HistoryRequest) -> Result<HistoryPage, ApiError> {
        by_run!(self, run_history, req.run_id, false, req)
    }
    async fn list_runs(&self, req: ListRunsRequest) -> Result<RunPage, ApiError> {
        fanout::list_runs(&self.core, &self.inner, req).await
    }
    fn ready(&self) -> bool {
        self.inner.ready()
    }
    async fn signal_run(&self, r: SignalRequest) -> Result<SignalResponse, ApiError> {
        by_run!(self, signal_run, r.run_id, true, r)
    }
    async fn cancel_run(&self, r: CancelRequest) -> Result<CancelResponse, ApiError> {
        by_run!(self, cancel_run, r.run_id, true, r)
    }
    async fn terminate_run(&self, r: TerminateRequest) -> Result<TerminateResponse, ApiError> {
        by_run!(self, terminate_run, r.run_id, true, r)
    }
    async fn retry_run(&self, r: RetryRequest) -> Result<RetryResponse, ApiError> {
        by_run!(self, retry_run, r.run_id, true, r)
    }
    async fn await_result(&self, r: AwaitResultRequest) -> Result<AwaitResultResponse, ApiError> {
        by_run!(self, await_result, r.run_id, false, r)
    }
    async fn complete_token(
        &self,
        r: CompleteTokenRequest,
    ) -> Result<CompleteTokenResponse, ApiError> {
        let p = table::token_partition(&r.token, self.core.count());
        self.core
            .partition(
                RouteClass::TokenPartition,
                p,
                true,
                false,
                || self.inner.complete_token(r.clone()),
                |peer: PeerApi| {
                    let r = r.clone();
                    async move { peer.cp.complete_token(r).await }
                },
            )
            .await
    }
    async fn set_workflow_policy(
        &self,
        workflow: String,
        p: RunPolicy,
    ) -> Result<RunPolicy, ApiError> {
        meta_w!(self, set_workflow_policy, workflow, p)
    }
    async fn get_workflow_policy(&self, workflow: String) -> Result<RunPolicy, ApiError> {
        meta_r!(self, get_workflow_policy, workflow)
    }
    async fn set_queue_policy(&self, queue: String, p: RunPolicy) -> Result<RunPolicy, ApiError> {
        meta_w!(self, set_queue_policy, queue, p)
    }
    async fn get_queue_policy(&self, queue: String) -> Result<RunPolicy, ApiError> {
        meta_r!(self, get_queue_policy, queue)
    }
    async fn put_secret(&self, req: PutSecret) -> Result<SecretInfo, ApiError> {
        meta_w!(self, put_secret, req)
    }
    async fn list_secrets(&self, tenant: Option<String>) -> Result<Vec<SecretInfo>, ApiError> {
        meta_r!(self, list_secrets, tenant)
    }
    async fn delete_secret(&self, name: String, tenant: Option<String>) -> Result<(), ApiError> {
        meta_w!(self, delete_secret, name, tenant)
    }
    async fn put_grant(&self, g: GrantInfo) -> Result<GrantInfo, ApiError> {
        meta_w!(self, put_grant, g)
    }
    async fn list_grants(&self, module: Option<String>) -> Result<Vec<GrantInfo>, ApiError> {
        meta_r!(self, list_grants, module)
    }
    async fn delete_grant(&self, id: String) -> Result<(), ApiError> {
        meta_w!(self, delete_grant, id)
    }
    async fn put_connection(&self, c: ConnectionInfo) -> Result<ConnectionInfo, ApiError> {
        meta_w!(self, put_connection, c)
    }
    async fn get_connection(&self, name: String) -> Result<ConnectionInfo, ApiError> {
        meta_r!(self, get_connection, name)
    }
    async fn list_connections(&self) -> Result<Vec<ConnectionInfo>, ApiError> {
        meta_r!(self, list_connections)
    }
    async fn delete_connection(&self, name: String) -> Result<(), ApiError> {
        meta_w!(self, delete_connection, name)
    }
    async fn put_egress(&self, e: EgressInfo) -> Result<EgressInfo, ApiError> {
        meta_w!(self, put_egress, e)
    }
    async fn get_egress(&self, module: String) -> Result<EgressInfo, ApiError> {
        meta_r!(self, get_egress, module)
    }
    async fn list_egress(&self) -> Result<Vec<EgressInfo>, ApiError> {
        meta_r!(self, list_egress)
    }
    async fn delete_egress(&self, module: String) -> Result<(), ApiError> {
        meta_w!(self, delete_egress, module)
    }
    async fn presigned_object(
        &self,
        req: PresignedObjectRequest,
    ) -> Result<PresignedObjectResponse, ApiError> {
        local_cp!(self, presigned_object, req)
    }
    async fn get_queue_flow(&self, queue: String) -> Result<QueueFlowInfo, ApiError> {
        meta_r!(self, get_queue_flow, queue)
    }
    async fn put_queue_flow(
        &self,
        queue: String,
        flow: QueueFlow,
        expected_version: Option<u64>,
    ) -> Result<QueueFlowInfo, ApiError> {
        meta_w!(self, put_queue_flow, queue, flow, expected_version)
    }
    async fn pause_queue(&self, queue: String) -> Result<PauseState, ApiError> {
        meta_w!(self, pause_queue, queue)
    }
    async fn resume_queue(&self, queue: String) -> Result<PauseState, ApiError> {
        meta_w!(self, resume_queue, queue)
    }
    async fn pause_workflow(&self, workflow: String) -> Result<PauseState, ApiError> {
        meta_w!(self, pause_workflow, workflow)
    }
    async fn resume_workflow(&self, workflow: String) -> Result<PauseState, ApiError> {
        meta_w!(self, resume_workflow, workflow)
    }
    async fn pause_run(
        &self,
        run_id: String,
        detail: Option<String>,
    ) -> Result<RunPauseState, ApiError> {
        let p = self.run_p(&run_id);
        self.core
            .partition(
                RouteClass::RunPartition,
                p,
                true,
                false,
                || self.inner.pause_run(run_id.clone(), detail.clone()),
                |peer: PeerApi| {
                    let (r, d) = (run_id.clone(), detail.clone());
                    async move { peer.cp.pause_run(r, d).await }
                },
            )
            .await
    }
    async fn resume_run(&self, run_id: String) -> Result<RunPauseState, ApiError> {
        by_run!(self, resume_run, run_id, true, run_id)
    }
    async fn queue_demand(&self, queue: String) -> Result<DemandSnapshot, ApiError> {
        fanout::queue_demand(&self.core, &self.inner, queue).await
    }
    async fn list_workers(&self) -> Result<Vec<WorkerInfo>, ApiError> {
        fanout::list_workers(&self.core, &self.inner).await
    }
    async fn drain_worker(&self, worker_id: String) -> Result<DrainResult, ApiError> {
        fanout::drain_worker(&self.core, &self.inner, worker_id).await
    }
    async fn create_worker_token(
        &self,
        req: CreateWorkerTokenRequest,
    ) -> Result<WorkerTokenCreated, ApiError> {
        meta_w!(self, create_worker_token, req)
    }
    async fn list_worker_tokens(&self) -> Result<Vec<WorkerTokenInfo>, ApiError> {
        meta_r!(self, list_worker_tokens)
    }
    async fn revoke_worker_token(&self, token_id: String) -> Result<(), ApiError> {
        meta_w!(self, revoke_worker_token, token_id)
    }
    async fn join_worker(&self, req: JoinRequest) -> Result<JoinResponse, ApiError> {
        local_cp!(self, join_worker, req)
    }
    async fn dev_time(&self) -> Result<DevTimeInfo, ApiError> {
        local_cp!(self, dev_time)
    }
    async fn dev_skip(&self, s: DevSkip) -> Result<DevSkipResult, ApiError> {
        local_cp!(self, dev_skip, s)
    }
    async fn replay_module(&self, r: ReplayRequest) -> Result<ReplayResult, ApiError> {
        local_cp!(self, replay_module, r)
    }
}

// ---------------------------------------------------------------------------------
// Production environment over the Phase 13 context
// ---------------------------------------------------------------------------------

/// The node's led-set watch.
type LedRx = watch::Receiver<Arc<Vec<(PartitionId, Epoch)>>>;

/// [`RouteEnv`] over a node's [`P13Ctx`].
pub struct NodeEnv {
    ctx: Arc<P13Ctx>,
    count: PartitionCount,
    view: watch::Receiver<Arc<Vec<PartitionAssignmentView>>>,
    led: Option<LedRx>,
    peers: PeerControlPlane,
}

impl std::fmt::Debug for NodeEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeEnv")
            .field("node", &self.ctx.node_id())
            .finish_non_exhaustive()
    }
}

impl NodeEnv {
    /// The environment of `ctx`'s node.
    pub fn new(ctx: &Arc<P13Ctx>) -> Self {
        let max = usize::try_from(ctx.cfg.api.max_module_bytes)
            .unwrap_or(usize::MAX)
            .saturating_add(64 * 1024);
        Self {
            count: ctx.local_cp.partition_count(),
            view: ctx.cluster.assignment_watch(),
            led: ctx
                .node()
                .map(|n| ostro_partition::roles::PartitionRoles::led(&*n)),
            peers: PeerControlPlane::new(Arc::clone(ctx.cluster.transport()), max),
            ctx: Arc::clone(ctx),
        }
    }
}

impl RouteEnv for NodeEnv {
    fn local_node(&self) -> u64 {
        self.ctx.node_id()
    }

    fn partition_count(&self) -> PartitionCount {
        self.count
    }

    fn owner(&self, p: PartitionId) -> (Target, Epoch) {
        let led: Vec<(PartitionId, Epoch)> = self
            .led
            .as_ref()
            .map(|r| r.borrow().as_ref().clone())
            .unwrap_or_default();
        let view = Arc::clone(&self.view.borrow());
        table::owner(p, self.local_node(), &led, &view)
    }

    fn led(&self) -> Vec<PartitionId> {
        self.led
            .as_ref()
            .map(|r| r.borrow().iter().map(|(p, _)| *p).collect())
            .unwrap_or_default()
    }

    fn members(&self) -> Vec<u64> {
        let me = self.local_node();
        self.ctx.meta.view().read(|s| {
            s.members()
                .filter(|m| {
                    m.value.node_id != me
                        && m.value.node_id != 0
                        && !matches!(
                            ostro_meta::ext::get_member_state(&m.value.ext),
                            Ok(Some(st)) if st.state == ostro_meta::ext::MEMBER_REMOVED
                        )
                })
                .map(|m| m.value.node_id)
                .collect()
        })
    }

    fn meta_leader(&self) -> Option<u64> {
        self.ctx.meta.raft().status().leader_id.filter(|l| *l != 0)
    }

    fn unkeyed_local(&self) -> bool {
        self.ctx.cfg.api.unkeyed_start_placement == crate::config::UnkeyedStartPlacement::Local
    }

    fn uniform(&self, n: usize) -> usize {
        let mut b = [0u8; 8];
        if getrandom::fill(&mut b).is_err() {
            return 0;
        }
        usize::try_from(u64::from_le_bytes(b) % u64::try_from(n.max(1)).unwrap_or(1)).unwrap_or(0)
    }

    fn retry_wait(&self) -> Duration {
        self.ctx.cfg.replication.session_timeout.get() * RETRY_WAIT_SESSIONS
    }

    fn wait_change(
        &self,
        p: PartitionId,
        seen: (Target, Epoch),
        limit: Duration,
    ) -> BoxFuture<'_, ()> {
        let mut view = self.view.clone();
        let mut led = self.led.clone();
        Box::pin(async move {
            // A self-demoted node (meta contact lost) masks its view: nothing will move it
            // until contact returns, so the re-route happens at once.
            if self.ctx.cluster.self_demoted() {
                return;
            }
            let led_at_start = self.led().contains(&p);
            let wait = async {
                loop {
                    // The owner moved, or this node gained / lost the partition (a takeover
                    // completing keeps the same target and epoch).
                    if self.owner(p) != seen || self.led().contains(&p) != led_at_start {
                        return;
                    }
                    match led.as_mut() {
                        Some(led) => tokio::select! {
                            r = view.changed() => if r.is_err() { return },
                            r = led.changed() => if r.is_err() { return },
                        },
                        None => {
                            if view.changed().await.is_err() {
                                return;
                            }
                        }
                    }
                }
            };
            // clock-exempt: bounded re-route wait (a wall-time budget of the caller).
            let _ = tokio::time::timeout(limit, wait).await;
        })
    }

    fn peer(&self, node: u64) -> Result<PeerApi, ApiError> {
        self.peers.api(node)
    }

    fn forget_peer(&self, node: u64) {
        self.peers.forget(node);
    }

    fn before_partition_read(&self, p: PartitionId) -> BoxFuture<'_, Result<(), ApiError>> {
        Box::pin(consistency::before_partition_read(&self.ctx, p))
    }

    fn before_partition_write(&self, p: PartitionId) -> BoxFuture<'_, Result<(), ApiError>> {
        Box::pin(consistency::before_partition_write(&self.ctx, p))
    }

    fn before_meta_read(&self) -> BoxFuture<'_, Result<(), ApiError>> {
        Box::pin(consistency::before_meta_read(&self.ctx))
    }

    fn token_after_partition_write(&self, p: PartitionId) -> Option<String> {
        consistency::token_after_partition_write(&self.ctx, p)
    }

    fn token_after_meta_write(&self) -> Option<String> {
        consistency::token_after_meta_write(&self.ctx)
    }

    fn migrate_gate(&self) -> BoxFuture<'_, Result<(), ApiError>> {
        Box::pin(crate::control::migrate::entry_gate(&self.ctx))
    }
}

// ---------------------------------------------------------------------------------
// Per-node shared state (one core per node; the seam calls each entry point once)
// ---------------------------------------------------------------------------------

/// What [`wrap_control_plane`], [`wrap_extensions`] and [`node_api_services`] of one node
/// share. Held strongly only by the routed control plane, the routed extensions and the
/// cluster listener's API service, so it goes away with them (it holds the context).
struct NodeShared {
    core: Arc<RouteCore>,
    /// The node API slot of the cluster listener ([`node_api_services`]); weak, because the
    /// router stored in it holds this state.
    late_api: Mutex<Weak<LateApi>>,
}

type Registry = Vec<(Weak<P13Ctx>, Weak<NodeShared>)>;

static REGISTRY: Mutex<Registry> = Mutex::new(Vec::new());

fn lookup(ctx: &Arc<P13Ctx>) -> Option<Arc<NodeShared>> {
    let mut reg = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    reg.retain(|(c, s)| c.strong_count() > 0 && s.strong_count() > 0);
    reg.iter()
        .find(|(c, _)| c.upgrade().is_some_and(|c| Arc::ptr_eq(&c, ctx)))
        .and_then(|(_, s)| s.upgrade())
}

fn shared(ctx: &Arc<P13Ctx>) -> Arc<NodeShared> {
    if let Some(s) = lookup(ctx) {
        return s;
    }
    let s = Arc::new(NodeShared {
        core: Arc::new(RouteCore::new(Arc::new(NodeEnv::new(ctx)))),
        late_api: Mutex::new(Weak::new()),
    });
    REGISTRY
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push((Arc::downgrade(ctx), Arc::downgrade(&s)));
    s
}

/// The routing environment of `ctx`'s node (p13-follow: `p13::keyed_start` resolves a
/// keyed restart's target leader with the router's view).
pub(crate) fn route_env(ctx: &Arc<P13Ctx>) -> Arc<dyn RouteEnv> {
    Arc::clone(shared(ctx).core.env())
}

/// The routing counters of `ctx`'s node (`None` before [`wrap_control_plane`] or after the
/// routed control plane is gone). Test probe.
pub fn stats(ctx: &Arc<P13Ctx>) -> Option<Arc<RouteStats>> {
    lookup(ctx).map(|s| Arc::clone(&s.core.stats))
}

/// The routed control plane over `local` (D-P13-14).
pub fn wrap_control_plane(
    ctx: &Arc<P13Ctx>,
    local: Arc<ServerControlPlane>,
) -> Arc<dyn ControlPlane> {
    let s = shared(ctx);
    // p13-follow: keyed restarts reach their target partition's leader
    // (`p13::keyed_start`, NodeOps.StartRestart).
    local.set_remote_start(Arc::new(crate::p13::keyed_start::NodeRemoteStart::new(ctx)));
    Arc::new(
        RoutedControlPlane::new(Arc::clone(&s.core), local as Arc<dyn ControlPlane>).keep_alive(s),
    )
}

/// Route every filled extension slot (Phase 7/8/11/13 traits) with the node's core: the
/// same table as the control plane ([`table::ROUTES`]).
pub fn wrap_extensions(ctx: &Arc<P13Ctx>, ext: &mut ApiExtensions) {
    let s = shared(ctx);
    let routed =
        RoutedExtensions::wrap(Arc::clone(&s.core), ext.clone(), Some(Arc::clone(&s) as _));
    routed.install(ext);
    // The node API of the cluster listener serves the same routed surface to forwarded
    // calls (`hop = 1`: always local there).
    let late = s
        .late_api
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .upgrade();
    if let Some(late) = late {
        let cp: Arc<dyn ControlPlane> = Arc::new(
            RoutedControlPlane::new(
                Arc::clone(&s.core),
                Arc::clone(&ctx.local_cp) as Arc<dyn ControlPlane>,
            )
            .keep_alive(Arc::clone(&s) as _),
        );
        let router = node_api_router(cp, ext.clone(), api_config(ctx), ctx.cluster.cluster_id());
        late.0.store(Some(Arc::new(router)));
    }
}

/// The API limits of the node API (the client listener's, `server.rs`).
fn api_config(ctx: &P13Ctx) -> ostro_api::ApiConfig {
    let c = &ctx.cfg;
    ostro_api::ApiConfig {
        max_module_bytes: usize::try_from(c.api.max_module_bytes).unwrap_or(usize::MAX),
        max_request_bytes: usize::try_from(c.api.max_request_bytes).unwrap_or(usize::MAX),
        max_payload_bytes: usize::try_from(c.limits.max_payload_bytes.get()).unwrap_or(usize::MAX),
        root_redirect: None,
        // Phase 14 (14-01b): the node API is reached by node identities only; the forwarded
        // principal (14-15) is not a bearer.
        ..ostro_api::ApiConfig::default()
    }
}

// ---------------------------------------------------------------------------------
// The node API on the cluster listener
// ---------------------------------------------------------------------------------

/// The node API router, set by [`wrap_extensions`] once the extension slots exist (the
/// cluster listener's services attach before them).
#[derive(Default)]
struct LateApi(arc_swap::ArcSwapOption<axum::Router>);

/// The node identity of a cluster-listener caller: the transport's [`PeerNode`] (in-memory
/// transport) or the verified peer certificate (`ostro://<cluster>/node/<id>`); worker
/// certificates and other clusters' nodes are `PERMISSION_DENIED`, no certificate is
/// `UNAUTHENTICATED` (`authz::require_node`).
///
/// [`PeerNode`]: ostro_cluster::transport::PeerNode
pub fn trusted_node(
    extensions: &axum::http::Extensions,
    cluster_id: &str,
) -> Result<u64, tonic::Status> {
    let mut probe = axum::http::Request::new(());
    *probe.extensions_mut() = extensions.clone();
    let mut req = tonic::Request::from_http(probe);
    ostro_cluster::transport::tcp::PeerIdentity::new(cluster_id).identify(&mut req);
    ostro_cluster::authz::require_node(&req)
}

/// The node API: `ostro_api::router_with` (with its request-metadata middleware) behind a
/// node-identity layer that rejects every other caller and inserts
/// [`request_meta::TrustedNode`], so `ostro-partition` / `ostro-partitions` /
/// `ostro-principal` count and `RequestMeta::from_node` is set. Unknown paths answer gRPC
/// `UNIMPLEMENTED`. Phase 14 (14-15): the call runs inside `PRINCIPAL.scope` of the
/// forwarded principal (`p14::principal_fwd::ingest`; none = an internal call as `System`),
/// for node-local lookups only: the node API is not guarded and never re-authorizes.
pub fn node_api_router(
    cp: Arc<dyn ControlPlane>,
    ext: ApiExtensions,
    cfg: ostro_api::ApiConfig,
    cluster_id: &str,
) -> axum::Router {
    let cluster_id: Arc<str> = Arc::from(cluster_id);
    ostro_api::router_with(cp, cfg, ext)
        .fallback(|| async {
            tonic::Status::unimplemented("not served on the cluster listener")
                .into_http::<axum::body::Body>()
        })
        .layer(axum::middleware::from_fn(
            move |mut req: axum::extract::Request, next: axum::middleware::Next| {
                let cluster_id = Arc::clone(&cluster_id);
                async move {
                    match trusted_node(req.extensions(), &cluster_id) {
                        Ok(id) => {
                            req.extensions_mut().insert(request_meta::TrustedNode(id));
                            match forwarded_principal(req.headers()) {
                                Ok((p, fwd)) => {
                                    let call = async move {
                                        trace_principal(id, fwd);
                                        next.run(req).await
                                    };
                                    ostro_api::auth::PRINCIPAL.scope(p, call).await
                                }
                                Err(e) => ostro_api::grpc::to_status(e).into_http(),
                            }
                        }
                        Err(status) => status.into_http::<axum::body::Body>(),
                    }
                }
            },
        ))
}

/// Phase 14 (14-15): the principal of a node-API call (its `ostro-principal` header;
/// repeated = malformed) and whether one was forwarded.
fn forwarded_principal(
    headers: &axum::http::HeaderMap,
) -> Result<(ostro_api::auth::Principal, bool), ApiError> {
    let mut values = headers.get_all(request_meta::HEADER_PRINCIPAL).iter();
    let first = values.next();
    let bad = || ApiError::invalid_argument("header ostro-principal: repeated or not ASCII");
    if values.next().is_some() {
        return Err(bad());
    }
    let header = first.map(|v| v.to_str().map_err(|_| bad())).transpose()?;
    Ok((crate::p14::principal_fwd::ingest(header)?, header.is_some()))
}

/// The principal a node-API call from node `from` runs as, read back from the task-local
/// (what node-local lookups see).
fn trace_principal(from: u64, forwarded: bool) {
    let p = ostro_api::auth::current();
    tracing::debug!(
        target: crate::p14::principal_fwd::TRACE_TARGET,
        from_node = from,
        principal = %p.token_id,
        kind = p.kind.as_str(),
        tenant = p.tenant.as_deref().unwrap_or("*"),
        forwarded,
        "node API call principal"
    );
}

/// The `ostro.api.v1.*` services the cluster listener serves to node identities (forwarded
/// calls and fan-out legs). Until [`wrap_extensions`] built the router, calls answer
/// `UNAVAILABLE`.
pub fn node_api_services(ctx: &Arc<P13Ctx>) -> Vec<Routes> {
    let s = shared(ctx);
    let late = Arc::new(LateApi::default());
    *s.late_api.lock().unwrap_or_else(|e| e.into_inner()) = Arc::downgrade(&late);
    let svc = LateApiService(late);
    vec![Routes::from(axum::Router::new().fallback_service(svc))]
}

#[derive(Clone)]
struct LateApiService(Arc<LateApi>);

impl tonic::codegen::Service<axum::extract::Request> for LateApiService {
    type Response = axum::response::Response;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: axum::extract::Request) -> Self::Future {
        let router = self.0.0.load_full();
        Box::pin(async move {
            match router {
                Some(r) => {
                    let mut r = (*r).clone();
                    tonic::codegen::Service::call(&mut r, req).await
                }
                None => Ok(tonic::Status::unavailable("the node API is starting")
                    .into_http::<axum::body::Body>()),
            }
        })
    }
}

// ---------------------------------------------------------------------------------
// The routed extension traits
// ---------------------------------------------------------------------------------

use ostro_api::phase8::{
    AdminApi, BackupStream, HookIngress, HookRequest, HookResponse, RunLogsApi, RunLogsRequest,
    RunLogsResponse, ScheduleApi, ScheduleDto, WebhookApi, WebhookDto,
};
use ostro_api::phase11::{
    BulkCreateRequest, BulkCreateResponse, BulkJobView, DlqDismissRequest, DlqEntry,
    DlqListRequest, DlqPage, DlqRequeueRequest, DlqRequeueResponse, FailurePolicy, InventoryApi,
    LimitsView, ListModulesRequest, ModulePage, OpsApi, PartitionView, PolicyScope, QueueView,
    RerunCutsView, RerunRequest, RerunResponse,
};
use ostro_api::phase13::{
    ClusterApi, ClusterStatus, GcRunReport, InitRequest, JoinRequest as ClusterJoinRequest,
    JoinResponse as ClusterJoinResponse, MigrateRunsReport, MigrateRunsRequest, ModuleLifecycleApi,
    NodeTokenRequest, NodeTokenResponse, ReassignRequest, RetireModuleResponse, UpgradeStatus,
};

/// Every extension trait routed over one core; `inner` holds this node's slots (a slot is
/// wrapped only when it is filled).
pub struct RoutedExtensions {
    core: Arc<RouteCore>,
    inner: ApiExtensions,
    _keep: Option<Arc<dyn std::any::Any + Send + Sync>>,
}

impl std::fmt::Debug for RoutedExtensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutedExtensions").finish_non_exhaustive()
    }
}

fn slot<T: ?Sized>(s: &Option<Arc<T>>, what: &str) -> Result<Arc<T>, ApiError> {
    s.clone()
        .ok_or_else(|| ApiError::unimplemented(&format!("{what} (slot not filled)")))
}

impl RoutedExtensions {
    /// Route `inner`'s slots over `core` (`keep` lives as long as the decorator).
    pub fn wrap(
        core: Arc<RouteCore>,
        inner: ApiExtensions,
        keep: Option<Arc<dyn std::any::Any + Send + Sync>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            core,
            inner,
            _keep: keep,
        })
    }

    /// Replace every filled slot of `ext` with this decorator.
    pub fn install(self: &Arc<Self>, ext: &mut ApiExtensions) {
        let me = Arc::clone(self);
        if ext.schedules.is_some() {
            ext.schedules = Some(me.clone());
        }
        if ext.webhooks.is_some() {
            ext.webhooks = Some(me.clone());
        }
        if ext.hooks.is_some() {
            ext.hooks = Some(me.clone());
        }
        if ext.run_logs.is_some() {
            ext.run_logs = Some(me.clone());
        }
        if ext.admin.is_some() {
            ext.admin = Some(me.clone());
        }
        if ext.ops.is_some() {
            ext.ops = Some(me.clone());
        }
        if ext.inventory.is_some() {
            ext.inventory = Some(me.clone());
        }
        if ext.cluster.is_some() {
            ext.cluster = Some(me.clone());
        }
        if ext.module_lifecycle.is_some() {
            ext.module_lifecycle = Some(me);
        }
    }

    fn run_p(&self, run_id: &str) -> Option<PartitionId> {
        table::run_partition(run_id, self.core.count())
    }
}

macro_rules! ext_local {
    ($self:ident, $slot:ident, $tr:literal, $m:ident, $how:ident $(, $a:expr)*) => {{
        let s = slot(&$self.inner.$slot, stringify!($m))?;
        $self
            .core
            .$how(table::class($tr, stringify!($m)), s.$m($($a),*))
            .await
    }};
}

macro_rules! ext_run {
    ($self:ident, $slot:ident, $tr:literal, $m:ident, $run:expr, $write:expr, $arg:ident) => {{
        let s = slot(&$self.inner.$slot, stringify!($m))?;
        let p = $self.run_p(&$run);
        $self
            .core
            .partition(
                table::class($tr, stringify!($m)),
                p,
                $write,
                false,
                || s.$m($arg.clone()),
                |peer: PeerApi| {
                    let a = $arg.clone();
                    async move { slot(&peer.ext.$slot, stringify!($slot))?.$m(a).await }
                },
            )
            .await
    }};
}

#[async_trait::async_trait]
impl ScheduleApi for RoutedExtensions {
    async fn list_schedules(&self) -> Result<Vec<ScheduleDto>, ApiError> {
        ext_local!(self, schedules, "ScheduleApi", list_schedules, meta_read)
    }
    async fn get_schedule(&self, name: String) -> Result<ScheduleDto, ApiError> {
        ext_local!(
            self,
            schedules,
            "ScheduleApi",
            get_schedule,
            meta_read,
            name
        )
    }
    async fn create_schedule(&self, s: ScheduleDto) -> Result<ScheduleDto, ApiError> {
        ext_local!(
            self,
            schedules,
            "ScheduleApi",
            create_schedule,
            meta_write,
            s
        )
    }
    async fn update_schedule(&self, s: ScheduleDto) -> Result<ScheduleDto, ApiError> {
        ext_local!(
            self,
            schedules,
            "ScheduleApi",
            update_schedule,
            meta_write,
            s
        )
    }
    async fn delete_schedule(
        &self,
        name: String,
        version_tag: Option<u64>,
    ) -> Result<(), ApiError> {
        ext_local!(
            self,
            schedules,
            "ScheduleApi",
            delete_schedule,
            meta_write,
            name,
            version_tag
        )
    }
    async fn pause_schedule(&self, name: String) -> Result<ScheduleDto, ApiError> {
        ext_local!(
            self,
            schedules,
            "ScheduleApi",
            pause_schedule,
            meta_write,
            name
        )
    }
    async fn resume_schedule(&self, name: String) -> Result<ScheduleDto, ApiError> {
        ext_local!(
            self,
            schedules,
            "ScheduleApi",
            resume_schedule,
            meta_write,
            name
        )
    }
    async fn trigger_schedule(
        &self,
        name: String,
        request_id: Option<String>,
    ) -> Result<String, ApiError> {
        let s = slot(&self.inner.schedules, "trigger_schedule")?;
        // The schedule's owner partition (`schedule_owner(name)`), as the local impl uses.
        let p = Some(ostro_types::routing::schedule_owner(
            &name,
            self.core.count(),
        ));
        self.core
            .partition(
                RouteClass::KeyPartition,
                p,
                true,
                false,
                || s.trigger_schedule(name.clone(), request_id.clone()),
                |peer: PeerApi| {
                    let (n, r) = (name.clone(), request_id.clone());
                    async move {
                        slot(&peer.ext.schedules, "schedules")?
                            .trigger_schedule(n, r)
                            .await
                    }
                },
            )
            .await
    }
}

#[async_trait::async_trait]
impl WebhookApi for RoutedExtensions {
    async fn list_webhooks(&self) -> Result<Vec<WebhookDto>, ApiError> {
        ext_local!(self, webhooks, "WebhookApi", list_webhooks, meta_read)
    }
    async fn get_webhook(&self, name: String) -> Result<WebhookDto, ApiError> {
        ext_local!(self, webhooks, "WebhookApi", get_webhook, meta_read, name)
    }
    async fn create_webhook(&self, w: WebhookDto) -> Result<WebhookDto, ApiError> {
        ext_local!(self, webhooks, "WebhookApi", create_webhook, meta_write, w)
    }
    async fn update_webhook(&self, w: WebhookDto) -> Result<WebhookDto, ApiError> {
        ext_local!(self, webhooks, "WebhookApi", update_webhook, meta_write, w)
    }
    async fn delete_webhook(&self, name: String, version_tag: Option<u64>) -> Result<(), ApiError> {
        ext_local!(
            self,
            webhooks,
            "WebhookApi",
            delete_webhook,
            meta_write,
            name,
            version_tag
        )
    }
}

#[async_trait::async_trait]
impl HookIngress for RoutedExtensions {
    async fn deliver(&self, req: HookRequest) -> Result<HookResponse, ApiError> {
        // Local: the delivery's start / signal / completion go through the routed control
        // plane (`control/webhooks.rs`), so they reach their partition's leader.
        ext_local!(self, hooks, "HookIngress", deliver, local_only, req)
    }
}

#[async_trait::async_trait]
impl RunLogsApi for RoutedExtensions {
    async fn run_logs(&self, req: RunLogsRequest) -> Result<RunLogsResponse, ApiError> {
        ext_run!(
            self,
            run_logs,
            "RunLogsApi",
            run_logs,
            req.run_id,
            false,
            req
        )
    }
}

#[async_trait::async_trait]
impl AdminApi for RoutedExtensions {
    async fn backup(&self) -> Result<BackupStream, ApiError> {
        ext_local!(self, admin, "AdminApi", backup, local_only)
    }
}

#[async_trait::async_trait]
impl OpsApi for RoutedExtensions {
    async fn rerun_run(&self, req: RerunRequest) -> Result<RerunResponse, ApiError> {
        ext_run!(self, ops, "OpsApi", rerun_run, req.run_id, true, req)
    }
    async fn analyze_rerun(&self, run_id: String) -> Result<RerunCutsView, ApiError> {
        ext_run!(self, ops, "OpsApi", analyze_rerun, run_id, false, run_id)
    }
    async fn list_dlq(&self, req: DlqListRequest) -> Result<DlqPage, ApiError> {
        let s = slot(&self.inner.ops, "list_dlq")?;
        fanout::list_dlq(&self.core, &s, req).await
    }
    async fn requeue_dlq(&self, req: DlqRequeueRequest) -> Result<DlqRequeueResponse, ApiError> {
        ext_run!(self, ops, "OpsApi", requeue_dlq, req.run_id, true, req)
    }
    async fn dismiss_dlq(&self, req: DlqDismissRequest) -> Result<DlqEntry, ApiError> {
        ext_run!(self, ops, "OpsApi", dismiss_dlq, req.run_id, true, req)
    }
    async fn create_bulk(&self, req: BulkCreateRequest) -> Result<BulkCreateResponse, ApiError> {
        ext_local!(self, ops, "OpsApi", create_bulk, local_only, req)
    }
    async fn list_bulk(&self) -> Result<Vec<BulkJobView>, ApiError> {
        let s = slot(&self.inner.ops, "list_bulk")?;
        fanout::list_bulk(&self.core, &s).await
    }
    async fn get_bulk(&self, job_id: String) -> Result<BulkJobView, ApiError> {
        let s = slot(&self.inner.ops, "get_bulk")?;
        self.core
            .first_hit(
                RouteClass::FanoutNodes,
                || s.get_bulk(job_id.clone()),
                |peer: PeerApi| {
                    let j = job_id.clone();
                    async move { slot(&peer.ext.ops, "ops")?.get_bulk(j).await }
                },
            )
            .await
    }
    async fn cancel_bulk(&self, job_id: String) -> Result<BulkJobView, ApiError> {
        let s = slot(&self.inner.ops, "cancel_bulk")?;
        self.core
            .first_hit(
                RouteClass::FanoutNodes,
                || s.cancel_bulk(job_id.clone()),
                |peer: PeerApi| {
                    let j = job_id.clone();
                    async move { slot(&peer.ext.ops, "ops")?.cancel_bulk(j).await }
                },
            )
            .await
    }
    async fn limits(&self) -> Result<LimitsView, ApiError> {
        ext_local!(self, ops, "OpsApi", limits, local_only)
    }
    async fn get_failure_policy(&self, scope: PolicyScope) -> Result<FailurePolicy, ApiError> {
        ext_local!(self, ops, "OpsApi", get_failure_policy, meta_read, scope)
    }
    async fn set_failure_policy(
        &self,
        scope: PolicyScope,
        p: FailurePolicy,
    ) -> Result<FailurePolicy, ApiError> {
        ext_local!(
            self,
            ops,
            "OpsApi",
            set_failure_policy,
            meta_write,
            scope,
            p
        )
    }
}

#[async_trait::async_trait]
impl InventoryApi for RoutedExtensions {
    async fn list_modules(&self, req: ListModulesRequest) -> Result<ModulePage, ApiError> {
        ext_local!(
            self,
            inventory,
            "InventoryApi",
            list_modules,
            meta_read,
            req
        )
    }
    async fn list_queues(&self) -> Result<Vec<QueueView>, ApiError> {
        ext_local!(self, inventory, "InventoryApi", list_queues, meta_read)
    }
    async fn list_partitions(&self) -> Result<Vec<PartitionView>, ApiError> {
        let s = slot(&self.inner.inventory, "list_partitions")?;
        fanout::list_partitions(&self.core, &s).await
    }
}

#[async_trait::async_trait]
impl ClusterApi for RoutedExtensions {
    async fn init(&self, r: InitRequest) -> Result<ClusterStatus, ApiError> {
        ext_local!(self, cluster, "ClusterApi", init, meta_write, r)
    }
    async fn join(&self, r: ClusterJoinRequest) -> Result<ClusterJoinResponse, ApiError> {
        ext_local!(self, cluster, "ClusterApi", join, meta_write, r)
    }
    async fn status(&self) -> Result<ClusterStatus, ApiError> {
        ext_local!(self, cluster, "ClusterApi", status, meta_read)
    }
    async fn create_node_token(&self, r: NodeTokenRequest) -> Result<NodeTokenResponse, ApiError> {
        ext_local!(
            self,
            cluster,
            "ClusterApi",
            create_node_token,
            meta_write,
            r
        )
    }
    async fn remove(&self, id: u64, force: bool) -> Result<ClusterStatus, ApiError> {
        ext_local!(self, cluster, "ClusterApi", remove, meta_write, id, force)
    }
    async fn drain(&self, id: u64) -> Result<ClusterStatus, ApiError> {
        ext_local!(self, cluster, "ClusterApi", drain, meta_write, id)
    }
    async fn reassign(&self, r: ReassignRequest) -> Result<ClusterStatus, ApiError> {
        ext_local!(self, cluster, "ClusterApi", reassign, meta_write, r)
    }
    async fn rebalance(&self) -> Result<ClusterStatus, ApiError> {
        ext_local!(self, cluster, "ClusterApi", rebalance, meta_write)
    }
    async fn upgrade_status(&self) -> Result<UpgradeStatus, ApiError> {
        ext_local!(self, cluster, "ClusterApi", upgrade_status, meta_read)
    }
    async fn upgrade_finalize(&self) -> Result<UpgradeStatus, ApiError> {
        let s = slot(&self.inner.cluster, "upgrade_finalize")?;
        // `upgrade::finalize` runs on the meta leader (13-04).
        self.core
            .meta_leader(
                RouteClass::MetaLeader,
                || s.upgrade_finalize(),
                |peer: PeerApi| async move {
                    slot(&peer.ext.cluster, "cluster")?.upgrade_finalize().await
                },
            )
            .await
    }
    async fn upgrade_hold(&self) -> Result<UpgradeStatus, ApiError> {
        ext_local!(self, cluster, "ClusterApi", upgrade_hold, meta_write)
    }
    async fn upgrade_release(&self) -> Result<UpgradeStatus, ApiError> {
        ext_local!(self, cluster, "ClusterApi", upgrade_release, meta_write)
    }
}

#[async_trait::async_trait]
impl ModuleLifecycleApi for RoutedExtensions {
    async fn retire_module(&self, sha: String) -> Result<RetireModuleResponse, ApiError> {
        ext_local!(
            self,
            module_lifecycle,
            "ModuleLifecycleApi",
            retire_module,
            meta_write,
            sha
        )
    }
    async fn run_module_gc(&self) -> Result<GcRunReport, ApiError> {
        let s = slot(&self.inner.module_lifecycle, "run_module_gc")?;
        self.core
            .meta_leader(
                RouteClass::MetaLeader,
                || s.run_module_gc(),
                |peer: PeerApi| async move {
                    slot(&peer.ext.module_lifecycle, "module lifecycle")?
                        .run_module_gc()
                        .await
                },
            )
            .await
    }
    async fn migrate_runs(&self, r: MigrateRunsRequest) -> Result<MigrateRunsReport, ApiError> {
        let s = slot(&self.inner.module_lifecycle, "migrate_runs")?;
        fanout::migrate_runs(&self.core, &s, r).await
    }
}
