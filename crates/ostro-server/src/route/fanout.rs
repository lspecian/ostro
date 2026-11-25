//! Partition and node fan-out (13-12), complete-or-error.
//!
//! # Partition fan-out (`list_runs`, `list_dlq`, `migrate_runs`)
//!
//! Partitions are grouped by their leader ([`RouteCore::fanout_partitions`]): one leg per
//! leader node with `ostro-partitions` (the local leg runs in a scope with the same set and
//! this node as `from_node`, so [`super::table::listed_partitions`] restricts the scan). A
//! node that leads every partition answers alone, exactly as a single node does. A
//! partition with no leader makes the whole call 503 `partition_offline`
//! (`details.partitions`, `retry_after_ms`): the listing APIs have no partial-result
//! convention, so the router never returns a silently incomplete page. A leg refused with
//! `not_leader` / `leader_changed` (or a transport failure) re-plans once after the view
//! moves, then 503.
//!
//! Pages merge by the existing cursor order: the scan order `(uuid, partition)` for `c1.`
//! tokens, the index order `(created_at desc, uuid desc, partition)` for `i1.` tokens.
//! Every leg examined everything up to its own token's position, so the merged page keeps
//! the entries at or before the earliest leg position, cut at `limit`; its token is that
//! leg's token, or the position of the last entry returned when the cut is the limit.
//! `list_runs` is exact (`RunSummary` carries `created_at_ms`). `list_dlq` entries carry no
//! creation time: its newest-first merge uses the run id's uuid v7 timestamp in its place,
//! which equals the index key except within one millisecond of clock skew between the
//! uuid and `created_at_ms` (recorded in 13-12-SUMMARY).
//!
//! # Node fan-out (worker registry, bulk jobs, inventory, demand)
//!
//! Node-local registries are merged across every member ([`RouteCore::all_nodes`]) or
//! looked up member by member, first hit wins ([`RouteCore::first_hit`]).

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ostro_api::domain::{ListOrder, ListRunsRequest, RunPage, effective_limit};
use ostro_api::phase7::{DemandSnapshot, DrainResult, WorkerInfo};
use ostro_api::phase11::{BulkJobView, DlqListRequest, DlqPage, PartitionView};
use ostro_api::phase13::{MigrateRunsReport, MigrateRunsRequest};
use ostro_api::request_meta::{self, RequestMeta};
use ostro_api::{ApiError, ControlPlane, ErrorCode};
use ostro_types::{PartitionId, RunId};

use super::peer::PeerApi;
use super::table::{RouteClass, Target};
use super::{
    Access, AnyOut, BoxFuture, LocalFn, RemoteFn, RouteCore, current_meta, erase, join_all, record,
    retryable, unerase,
};

/// One fan-out leg: `None` = this node, else the leader node.
type Leg = (Option<u64>, Vec<u16>);

/// A finished leg.
type LegResult<T> = (Option<u64>, Vec<u16>, Result<T, ApiError>);

/// One page of rows to merge: `(run, created_at_ms, row)` and the leg's token.
type PageRows<T> = (Vec<(RunId, u64, T)>, Option<String>);

impl RouteCore {
    /// The legs of a partition fan-out, or the offline 503.
    fn plan_partitions(&self) -> Result<Vec<Leg>, ApiError> {
        let mut groups: BTreeMap<Option<u64>, Vec<u16>> = BTreeMap::new();
        let mut offline = Vec::new();
        for p in self.count().ids() {
            match self.env.owner(p).0 {
                Target::Local => groups.entry(None).or_default().push(p.0),
                Target::Node(n) => groups.entry(Some(n)).or_default().push(p.0),
                Target::Offline(_) => offline.push(p.0),
            }
        }
        if !offline.is_empty() {
            return Err(ApiError::unavailable(format!(
                "partitions {offline:?} have no leader (offline: no in-sync replica can lead \
                 them); the listing would be incomplete; retry later"
            ))
            .with_details(serde_json::json!({
                "reason": "partition_offline",
                "partitions": offline,
                "retry_after_ms": super::RETRY_AFTER_MS,
            })));
        }
        Ok(groups.into_iter().collect())
    }

    /// One call per partition leader (module docs); the per-leg results in leg order. A
    /// forwarded-in leg (or a node leading every partition) answers alone.
    pub async fn fanout_partitions<T, L, LF, R, RF>(
        &self,
        class: RouteClass,
        local: L,
        remote: R,
    ) -> Result<Vec<T>, ApiError>
    where
        T: Send + 'static,
        L: Fn() -> LF + Send + Sync,
        LF: std::future::Future<Output = Result<T, ApiError>> + Send,
        R: Fn(PeerApi) -> RF + Send + Sync,
        RF: std::future::Future<Output = Result<T, ApiError>> + Send,
    {
        let local = || erase(local());
        let remote = |peer: PeerApi| erase(remote(peer));
        self.fanout_partitions_dyn(class, &local, &remote)
            .await?
            .into_iter()
            .map(|v| unerase(Ok(v)))
            .collect()
    }

    /// [`Self::fanout_partitions`] over type-erased calls (compiled once; binary size).
    fn fanout_partitions_dyn<'s, 'f: 's>(
        &'s self,
        class: RouteClass,
        local: &'s LocalFn<'s, 'f>,
        remote: &'s RemoteFn<'s, 'f>,
    ) -> BoxFuture<'s, Result<Vec<AnyOut>, ApiError>> {
        Box::pin(async move {
            let meta = current_meta();
            if meta.hop >= 1 {
                self.stats
                    .forwarded_in
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let parts = super::table::listed_partitions(self.count());
                for p in &parts {
                    self.env.before_partition_read(*p).await?;
                }
                let r = self.local(Access::None, local()).await;
                record(class, "local", r.is_ok());
                return r.map(|v| vec![v]);
            }
            let mut replanned = false;
            loop {
                let legs = match self.plan_partitions() {
                    Ok(l) => l,
                    Err(e) => {
                        self.stats
                            .offline
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        record(class, "offline", false);
                        return Err(e);
                    }
                };
                if let [(None, _)] = legs.as_slice() {
                    // Every partition is led here: the single-node path, unchanged.
                    for p in self.count().ids() {
                        self.env.before_partition_read(p).await?;
                    }
                    let r = self.local(Access::None, local()).await;
                    record(class, "local", r.is_ok());
                    return r.map(|v| vec![v]);
                }
                let me = self.env.local_node();
                let futs: Vec<BoxFuture<'_, LegResult<AnyOut>>> = legs
                    .iter()
                    .cloned()
                    .map(|(node, parts)| {
                        let meta = meta.clone();
                        let local = &local;
                        let remote = &remote;
                        let f: BoxFuture<'_, _> = Box::pin(async move {
                            let r = match node {
                                None => {
                                    let scoped = RequestMeta {
                                        partitions: Some(parts.clone()),
                                        from_node: Some(me),
                                        ..meta.clone()
                                    };
                                    let ps = parts.clone();
                                    let (r, token) =
                                        request_meta::scope_with_consistency(scoped, async move {
                                            for p in &ps {
                                                self.env
                                                    .before_partition_read(PartitionId(*p))
                                                    .await?;
                                            }
                                            self.local(Access::None, local()).await
                                        })
                                        .await;
                                    if let Some(t) = token {
                                        request_meta::set_consistency(t);
                                    }
                                    r
                                }
                                Some(n) => {
                                    let ps = parts.clone();
                                    self.forward(n, &meta, move |m| m.partitions = Some(ps), remote)
                                        .await
                                }
                            };
                            (node, parts, r)
                        });
                        f
                    })
                    .collect();
                let results = join_all(futs).await;
                let mut out = Vec::with_capacity(results.len());
                let mut refused: Option<(ApiError, PartitionId, Option<u64>)> = None;
                let mut failed: Option<ApiError> = None;
                for (node, parts, r) in results {
                    match r {
                        Ok(v) => out.push(v),
                        Err(e) if refused.is_none() && retryable(&e, node.is_some()) => {
                            let p = e
                                .details
                                .as_ref()
                                .and_then(|d| d.get("partition"))
                                .and_then(serde_json::Value::as_u64)
                                .and_then(|p| u16::try_from(p).ok())
                                .unwrap_or(parts[0]);
                            refused = Some((e, PartitionId(p), node));
                        }
                        Err(e) => {
                            failed.get_or_insert(e);
                        }
                    }
                }
                if let Some(e) = failed {
                    record(class, "forwarded", false);
                    return Err(e);
                }
                let Some((e, p, _)) = refused else {
                    record(class, "forwarded", true);
                    return Ok(out);
                };
                record(class, "forwarded", false);
                if replanned {
                    self.stats
                        .exhausted
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Err(super::exhausted_error(p, e));
                }
                replanned = true;
                self.stats
                    .rerouted
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let seen = self.env.owner(p);
                let budget = match super::remaining(&meta) {
                    Some(r) if r.is_zero() => return Err(super::deadline_error()),
                    Some(r) => r.min(self.env.retry_wait()),
                    None => self.env.retry_wait(),
                };
                self.env.wait_change(p, seen, budget).await;
            }
        })
    }

    /// Every member's answer (this node first, then the members in id order); any failure
    /// fails the call. A forwarded-in call answers alone.
    pub async fn all_nodes<T, L, LF, R, RF>(
        &self,
        class: RouteClass,
        local: L,
        remote: R,
    ) -> Result<Vec<T>, ApiError>
    where
        T: Send + 'static,
        L: Fn() -> LF + Send + Sync,
        LF: std::future::Future<Output = Result<T, ApiError>> + Send,
        R: Fn(PeerApi) -> RF + Send + Sync,
        RF: std::future::Future<Output = Result<T, ApiError>> + Send,
    {
        let local = || erase(local());
        let remote = |peer: PeerApi| erase(remote(peer));
        self.all_nodes_dyn(class, &local, &remote)
            .await?
            .into_iter()
            .map(|v| unerase(Ok(v)))
            .collect()
    }

    /// [`Self::all_nodes`] over type-erased calls (compiled once; binary size).
    fn all_nodes_dyn<'s, 'f: 's>(
        &'s self,
        class: RouteClass,
        local: &'s LocalFn<'s, 'f>,
        remote: &'s RemoteFn<'s, 'f>,
    ) -> BoxFuture<'s, Result<Vec<AnyOut>, ApiError>> {
        Box::pin(async move {
            let meta = current_meta();
            if meta.hop >= 1 {
                let r = self.local(Access::None, local()).await;
                record(class, "local", r.is_ok());
                return r.map(|v| vec![v]);
            }
            let mut members = self.env.members();
            members.sort_unstable();
            members.dedup();
            let mut futs: Vec<BoxFuture<'_, Result<AnyOut, ApiError>>> = Vec::new();
            futs.push(Box::pin(self.local(Access::None, local())));
            for n in members {
                let meta = meta.clone();
                let remote = &remote;
                futs.push(Box::pin(async move {
                    self.forward(n, &meta, |_| {}, remote).await
                }));
            }
            let mut out = Vec::new();
            for r in join_all(futs).await {
                match r {
                    Ok(v) => out.push(v),
                    Err(e) => {
                        record(class, "forwarded", false);
                        return Err(e);
                    }
                }
            }
            record(class, "forwarded", true);
            Ok(out)
        })
    }

    /// A lookup in a node-local registry: this node first, then every member in id order
    /// until one answers anything but `not_found`. `not_found` everywhere is the local
    /// answer; a member that could not be asked makes a final `not_found` a 503 instead.
    pub async fn first_hit<T, L, LF, R, RF>(
        &self,
        class: RouteClass,
        local: L,
        remote: R,
    ) -> Result<T, ApiError>
    where
        T: Send + 'static,
        L: Fn() -> LF + Send + Sync,
        LF: std::future::Future<Output = Result<T, ApiError>> + Send,
        R: Fn(PeerApi) -> RF + Send + Sync,
        RF: std::future::Future<Output = Result<T, ApiError>> + Send,
    {
        let local = || erase(local());
        let remote = |peer: PeerApi| erase(remote(peer));
        unerase(self.first_hit_dyn(class, &local, &remote).await)
    }

    /// [`Self::first_hit`] over type-erased calls (compiled once; binary size).
    fn first_hit_dyn<'s, 'f: 's>(
        &'s self,
        class: RouteClass,
        local: &'s LocalFn<'s, 'f>,
        remote: &'s RemoteFn<'s, 'f>,
    ) -> BoxFuture<'s, Result<AnyOut, ApiError>> {
        Box::pin(async move {
            let meta = current_meta();
            let first = self.local(Access::None, local()).await;
            let miss = match first {
                Err(e) if e.code == ErrorCode::NotFound && meta.hop == 0 => e,
                r => {
                    record(class, "local", r.is_ok());
                    return r;
                }
            };
            let mut members = self.env.members();
            members.sort_unstable();
            members.dedup();
            let mut unreachable: Option<ApiError> = None;
            for n in members {
                match self.forward(n, &meta, |_| {}, &remote).await {
                    Err(e) if e.code == ErrorCode::NotFound => {}
                    Err(e) if e.code == ErrorCode::Unavailable => {
                        unreachable.get_or_insert(e);
                    }
                    r => {
                        record(class, "forwarded", r.is_ok());
                        return r;
                    }
                }
            }
            record(class, "forwarded", false);
            Err(unreachable.unwrap_or(miss))
        })
    }
}

// ---------------------------------------------------------------------------------
// Merging
// ---------------------------------------------------------------------------------

/// A merge position: the scan order or the index order (both ascending).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Key {
    Scan(u128, u16),
    Index(Reverse<u64>, Reverse<u128>, u16),
}

/// Which order a set of pages is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Order {
    Scan,
    Index(char),
}

fn parse_run(s: &str) -> Option<RunId> {
    s.parse().ok()
}

/// The position and order of a page token (`c1.<run>` / `i1.<kind>.<created>.<run>`).
fn token_key(t: &str) -> Option<(Order, Key)> {
    if let Some(run) = t.strip_prefix("c1.") {
        let r = parse_run(run)?;
        return Some((Order::Scan, Key::Scan(r.uuid().as_u128(), r.partition().0)));
    }
    let rest = t.strip_prefix("i1.")?;
    let mut parts = rest.splitn(3, '.');
    let kind = parts.next()?.chars().next()?;
    let created: u64 = parts.next()?.parse().ok()?;
    let r = parse_run(parts.next()?)?;
    Some((
        Order::Index(kind),
        Key::Index(
            Reverse(created),
            Reverse(r.uuid().as_u128()),
            r.partition().0,
        ),
    ))
}

fn entry_key(order: Order, run: &RunId, created_at_ms: u64) -> Key {
    match order {
        Order::Scan => Key::Scan(run.uuid().as_u128(), run.partition().0),
        Order::Index(_) => Key::Index(
            Reverse(created_at_ms),
            Reverse(run.uuid().as_u128()),
            run.partition().0,
        ),
    }
}

fn token_at(order: Order, run: &RunId, created_at_ms: u64) -> String {
    match order {
        Order::Scan => {
            crate::control::list::encode_token((run.uuid().as_u128(), run.partition().0))
        }
        Order::Index(k) => crate::control::p11_search::encode_token(k, created_at_ms, run),
    }
}

/// The uuid v7 timestamp of a run id in ms (the `list_dlq` stand-in for `created_at_ms`).
fn uuid_ms(run: &RunId) -> u64 {
    run.uuid()
        .get_timestamp()
        .map(|t| {
            let (s, n) = t.to_unix();
            s.saturating_mul(1000)
                .saturating_add(u64::from(n / 1_000_000))
        })
        .unwrap_or(0)
}

/// Merge pages of `(run, created_at_ms, item)` rows (module docs).
fn merge_pages<T>(
    pages: Vec<PageRows<T>>,
    limit: usize,
    default_order: Order,
    index_ms: impl Fn(&RunId, u64) -> u64,
) -> Result<(Vec<T>, Option<String>), ApiError> {
    let mut order: Option<Order> = None;
    let mut cut: Option<(Key, String)> = None;
    for (_, tok) in &pages {
        let Some(t) = tok else { continue };
        let (o, k) = token_key(t).ok_or_else(|| {
            ApiError::internal(format!(
                "fan-out: a leg returned an unknown page token {t:?}"
            ))
        })?;
        match order {
            Some(prev) if prev != o => {
                return Err(ApiError::unavailable(
                    "the run index is rebuilding on some partitions; restart the listing \
                     without a page token",
                )
                .with_details(serde_json::json!({
                    "reason": "index_rebuilding",
                    "retry_after_ms": super::RETRY_AFTER_MS,
                })));
            }
            _ => order = Some(o),
        }
        if cut.as_ref().is_none_or(|(c, _)| k < *c) {
            cut = Some((k, t.clone()));
        }
    }
    let order = order.unwrap_or(default_order);
    let mut rows: Vec<(Key, RunId, u64, T)> = Vec::new();
    let mut seen = BTreeSet::new();
    for (items, _) in pages {
        for (run, created, item) in items {
            let ms = index_ms(&run, created);
            let k = entry_key(order, &run, ms);
            if cut.as_ref().is_some_and(|(c, _)| k > *c) || !seen.insert(run) {
                continue;
            }
            rows.push((k, run, ms, item));
        }
    }
    rows.sort_by_key(|r| r.0);
    if rows.len() > limit {
        rows.truncate(limit);
        let (_, run, ms, _) = rows.last().expect("limit > 0");
        let token = token_at(order, run, *ms);
        return Ok((rows.into_iter().map(|r| r.3).collect(), Some(token)));
    }
    Ok((rows.into_iter().map(|r| r.3).collect(), cut.map(|(_, t)| t)))
}

/// The index token kind of a `list_runs` request (`p11_search` plan: tag, workflow, dlq,
/// terminal status, all).
fn index_kind(req: &ListRunsRequest) -> char {
    use ostro_api::domain::RunStatus as S;
    if req
        .tags
        .first()
        .or(req.tag.as_ref())
        .is_some_and(|t| !t.contains('\0'))
    {
        return 't';
    }
    if req.workflow.as_ref().is_some_and(|w| !w.contains('\0')) {
        return 'w';
    }
    if req.dlq {
        return 'd';
    }
    match req.status {
        Some(S::Scheduled | S::Ready | S::Running) | None => 'a',
        Some(_) => 's',
    }
}

/// Merge `list_runs` legs.
pub fn merge_run_pages(req: &ListRunsRequest, pages: Vec<RunPage>) -> Result<RunPage, ApiError> {
    let mut pages = pages;
    if pages.len() == 1 {
        return Ok(pages.pop().expect("one page"));
    }
    let default = match req.order {
        ListOrder::Oldest => Order::Scan,
        ListOrder::Newest => Order::Index(index_kind(req)),
    };
    let rows = pages
        .into_iter()
        .map(|p| {
            let items = p
                .runs
                .into_iter()
                .filter_map(|r| {
                    let id = parse_run(&r.run_id)?;
                    Some((id, r.created_at_ms, r))
                })
                .collect();
            (items, p.next_page_token)
        })
        .collect();
    let (runs, next_page_token) =
        merge_pages(rows, effective_limit(req.limit) as usize, default, |_, c| c)?;
    Ok(RunPage {
        runs,
        next_page_token,
    })
}

/// Merge `list_dlq` legs (newest first by the index; the uuid timestamp stands in for the
/// creation time, module docs).
pub fn merge_dlq_pages(req: &DlqListRequest, pages: Vec<DlqPage>) -> Result<DlqPage, ApiError> {
    let mut pages = pages;
    if pages.len() == 1 {
        return Ok(pages.pop().expect("one page"));
    }
    let rows = pages
        .into_iter()
        .map(|p| {
            let items = p
                .entries
                .into_iter()
                .filter_map(|e| {
                    let id = parse_run(&e.run_id)?;
                    Some((id, 0, e))
                })
                .collect();
            (items, p.next_page_token)
        })
        .collect();
    let (entries, next_page_token) = merge_pages(
        rows,
        effective_limit(req.limit) as usize,
        Order::Index('d'),
        |r, _| uuid_ms(r),
    )?;
    Ok(DlqPage {
        entries,
        next_page_token,
    })
}

// ---------------------------------------------------------------------------------
// The fan-out methods
// ---------------------------------------------------------------------------------

/// `list_runs`: one leg per leader node, merged by the cursor order.
pub async fn list_runs(
    core: &RouteCore,
    inner: &Arc<dyn ControlPlane>,
    req: ListRunsRequest,
) -> Result<RunPage, ApiError> {
    let pages = core
        .fanout_partitions(
            RouteClass::FanoutPartitions,
            || inner.list_runs(req.clone()),
            |peer: PeerApi| {
                let r = req.clone();
                async move { peer.cp.list_runs(r).await }
            },
        )
        .await?;
    merge_run_pages(&req, pages)
}

/// `list_dlq` over the local `OpsApi`.
pub async fn list_dlq(
    core: &RouteCore,
    inner: &Arc<dyn ostro_api::phase11::OpsApi>,
    req: DlqListRequest,
) -> Result<DlqPage, ApiError> {
    let pages = core
        .fanout_partitions(
            RouteClass::FanoutPartitions,
            || inner.list_dlq(req.clone()),
            |peer: PeerApi| {
                let r = req.clone();
                async move {
                    match peer.ext.ops {
                        Some(o) => o.list_dlq(r).await,
                        None => Err(super::no_slot("ops")),
                    }
                }
            },
        )
        .await?;
    merge_dlq_pages(&req, pages)
}

/// `migrate_runs`: one leg per leader node, reports concatenated and sorted by run id.
pub async fn migrate_runs(
    core: &RouteCore,
    inner: &Arc<dyn ostro_api::phase13::ModuleLifecycleApi>,
    req: MigrateRunsRequest,
) -> Result<MigrateRunsReport, ApiError> {
    // 13-18: a submitting migrate's feature gate is decided once, here at the entry node,
    // before any leg (RouteEnv::migrate_gate); forwarded legs (hop 1) never re-decide it here.
    let entry = current_meta().hop == 0;
    if !req.dry_run && entry {
        core.env().migrate_gate().await?;
    }
    let legs = core
        .fanout_partitions(
            RouteClass::FanoutPartitions,
            || inner.migrate_runs(req.clone()),
            |peer: PeerApi| {
                let r = req.clone();
                async move {
                    match peer.ext.module_lifecycle {
                        Some(m) => m.migrate_runs(r).await,
                        None => Err(super::no_slot("module lifecycle")),
                    }
                }
            },
        )
        .await?;
    let mut out = MigrateRunsReport::default();
    for r in legs {
        out.passed.extend(r.passed);
        out.repinned.extend(r.repinned);
        out.failed.extend(r.failed);
        out.busy.extend(r.busy);
        out.seq_mismatch.extend(r.seq_mismatch);
    }
    // 13-17: sorted by run id, so the report does not depend on the leg order (which
    // depends on the receiving node).
    crate::control::migrate::sort_report(&mut out);
    Ok(out)
}

/// `queue_demand`: demand, backlog and worker slots are node-local (each node counts its
/// led partitions' queues and its own workers), so every member is asked and the counts
/// add up; placement is this node's.
pub async fn queue_demand(
    core: &RouteCore,
    inner: &Arc<dyn ControlPlane>,
    queue: String,
) -> Result<DemandSnapshot, ApiError> {
    let mut legs = core
        .all_nodes(
            RouteClass::FanoutPartitions,
            || inner.queue_demand(queue.clone()),
            |peer: PeerApi| {
                let q = queue.clone();
                async move { peer.cp.queue_demand(q).await }
            },
        )
        .await?
        .into_iter();
    let mut out = legs
        .next()
        .ok_or_else(|| ApiError::internal("no local leg"))?;
    for d in legs {
        out.demand = out.demand.saturating_add(d.demand);
        out.backlog = out.backlog.saturating_add(d.backlog);
        out.running_on_workers = out.running_on_workers.saturating_add(d.running_on_workers);
        out.workers = out.workers.saturating_add(d.workers);
        out.worker_slots_free = out.worker_slots_free.saturating_add(d.worker_slots_free);
        out.spilling |= d.spilling;
    }
    Ok(out)
}

/// `list_workers`: every member's sessions (a worker connects to one node).
pub async fn list_workers(
    core: &RouteCore,
    inner: &Arc<dyn ControlPlane>,
) -> Result<Vec<WorkerInfo>, ApiError> {
    let legs = core
        .all_nodes(
            RouteClass::FanoutNodes,
            || inner.list_workers(),
            |peer: PeerApi| async move { peer.cp.list_workers().await },
        )
        .await?;
    let mut seen = BTreeSet::new();
    Ok(legs
        .into_iter()
        .flatten()
        .filter(|w| seen.insert(w.worker_id.clone()))
        .collect())
}

/// `drain_worker`: the node holding the worker's session.
pub async fn drain_worker(
    core: &RouteCore,
    inner: &Arc<dyn ControlPlane>,
    worker_id: String,
) -> Result<DrainResult, ApiError> {
    core.first_hit(
        RouteClass::FanoutNodes,
        || inner.drain_worker(worker_id.clone()),
        |peer: PeerApi| {
            let w = worker_id.clone();
            async move { peer.cp.drain_worker(w).await }
        },
    )
    .await
}

/// `list_bulk`: every member's jobs (bulk jobs are node-local).
pub async fn list_bulk(
    core: &RouteCore,
    inner: &Arc<dyn ostro_api::phase11::OpsApi>,
) -> Result<Vec<BulkJobView>, ApiError> {
    let legs = core
        .all_nodes(
            RouteClass::FanoutNodes,
            || inner.list_bulk(),
            |peer: PeerApi| async move {
                match peer.ext.ops {
                    Some(o) => o.list_bulk().await,
                    None => Err(super::no_slot("ops")),
                }
            },
        )
        .await?;
    let mut seen = BTreeSet::new();
    Ok(legs
        .into_iter()
        .flatten()
        .filter(|j| seen.insert(j.job_id.clone()))
        .collect())
}

/// `list_partitions`: the assignment rows plus per-node role data; each partition's row
/// comes from the node that leads it (else this node's).
pub async fn list_partitions(
    core: &RouteCore,
    inner: &Arc<dyn ostro_api::phase11::InventoryApi>,
) -> Result<Vec<PartitionView>, ApiError> {
    let legs = core
        .all_nodes(
            RouteClass::FanoutNodes,
            || inner.list_partitions(),
            |peer: PeerApi| async move {
                match peer.ext.inventory {
                    Some(i) => i.list_partitions().await,
                    None => Err(super::no_slot("inventory")),
                }
            },
        )
        .await?;
    let mut legs = legs.into_iter();
    let mut rows: BTreeMap<u16, PartitionView> = legs
        .next()
        .unwrap_or_default()
        .into_iter()
        .map(|v| (v.id, v))
        .collect();
    for leg in legs {
        for v in leg {
            if v.role == "leader" && rows.get(&v.id).is_none_or(|r| r.role != "leader") {
                rows.insert(v.id, v);
            } else {
                rows.entry(v.id).or_insert(v);
            }
        }
    }
    Ok(rows.into_values().collect())
}
