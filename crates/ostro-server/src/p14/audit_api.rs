//! Phase 14 (14-18): `AuditApi`, the cluster-wide audit query (`GET /v1/audit`, D-P14-11).
//!
//! Every node keeps its own journal (`<data>/audit`, 14-04); a record lives on the node that
//! served the audited request (the ingress node). A query therefore asks every member:
//!
//! 1. [`install`] fills `ApiExtensions.audit` with this node's [`NodeAudit`] (a scan of the
//!    local journal). The Phase 13 slot routing (`route::RoutedExtensions`) wraps it and
//!    calls [`fan_out`], which runs the scan here and on every other member through the
//!    node fan-out (`RouteCore::each_node`, forwarded calls carry the caller's principal);
//!    a forwarded-in call (hop 1) only scans locally.
//! 2. Each node answers its oldest matching records after the cursor position for that
//!    node, in journal (`seq`) order, at most `limit`.
//! 3. The entry node merges the pages by `(ts_ms, node, seq)` (a k-way merge of the
//!    per-node lists, so every node's returned records are a `seq` prefix), cuts at
//!    `limit`, and encodes the next cursor.
//!
//! **Cursor:** opaque to clients; `a1.` followed by `<node>-<seq>` pairs joined by `.`, one
//! per node seen so far: the last `seq` returned from that node (a node absent from the
//! cursor starts at the beginning). `None` once no node has more matching records.
//!
//! **Partial results:** a member that does not answer (stopped, unreachable, timing out
//! after [`LEG_TIMEOUT`]) is listed in `AuditPage.partial_nodes`; its cursor entry is left
//! as it was, so the next page asks it again from the same position.
//!
//! **Visibility:** system admins see everything; for a tenant principal the guard's
//! `tenant_scope` (14-16) forces `q.tenant` to its own tenant before the call and filters
//! the answer again. `tenant = "_"` selects records with no tenant (system scope / the
//! default tenant).
//!
//! **Filters:** `since_ms` (inclusive), `until_ms` (exclusive), `tenant`, `actor` (exact),
//! `action` (exact or `prefix.*`), `limit` (default [`DEFAULT_LIMIT`], at most
//! [`MAX_LIMIT`]). The journal's control records (`segment_open`, `audit_recovered`,
//! `retention_trim`) are only listed when the action filter names them. Coalesced records
//! carry `count`, `first_ts_ms` and `last_ts_ms` in `details`.

use std::collections::{BTreeMap, VecDeque};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::Duration;

use ostro_api::phase14::{AuditApi, AuditPage, AuditQuery, AuditRecordView};
use ostro_api::{ApiError, ApiExtensions};
use ostro_types::audit::AuditOutcome;

use super::P14Ctx;
use crate::audit::journal::{AUDIT_RECOVERED, RETENTION_TRIM, Record, SEGMENT_OPEN};
use crate::audit::segment::list_segments;
use crate::audit::verify::journal_dir;
use crate::route::RouteCore;
use crate::route::peer::PeerApi;
use crate::route::table::RouteClass;

/// Page size when the query names none.
pub const DEFAULT_LIMIT: u32 = 100;
/// Largest page size.
pub const MAX_LIMIT: u32 = 1_000;
/// How long the entry node waits for one member's page.
pub const LEG_TIMEOUT: Duration = Duration::from_secs(10);
/// Version prefix of the cursor.
const CURSOR_PREFIX: &str = "a1";

/// Fill `ApiExtensions.audit` with this node's journal scan.
pub fn install(ctx: &Arc<P14Ctx>, ext: &mut ApiExtensions) {
    ext.audit = Some(Arc::new(NodeAudit {
        ctx: Arc::downgrade(ctx),
    }));
}

/// The node-local audit query (this node's journal only); [`fan_out`] merges the members.
pub struct NodeAudit {
    ctx: Weak<P14Ctx>,
}

impl std::fmt::Debug for NodeAudit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeAudit").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl AuditApi for NodeAudit {
    async fn query(&self, q: AuditQuery) -> Result<AuditPage, ApiError> {
        let ctx = self
            .ctx
            .upgrade()
            .ok_or_else(|| ApiError::unavailable("the server is shutting down"))?;
        let dir = journal_dir(&ctx.cfg, &ctx.data_dir);
        let node = ctx.node_id;
        drop(ctx);
        let after = decode_cursor(q.cursor.as_deref())?
            .get(&node)
            .copied()
            .unwrap_or(0);
        let filter = Filter::new(&q)?;
        let limit = limit_of(&q);
        let (records, more) =
            tokio::task::spawn_blocking(move || scan(&dir, &filter, after, limit))
                .await
                .map_err(|e| ApiError::internal(format!("audit scan task failed: {e}")))?
                .map_err(|e| ApiError::unavailable(format!("reading the audit journal: {e}")))?;
        let cursor = match (more, records.last()) {
            (true, Some(last)) => Some(encode_cursor(&BTreeMap::from([(node, last.seq)]))),
            _ => None,
        };
        Ok(AuditPage {
            records,
            cursor,
            partial_nodes: Vec::new(),
        })
    }
}

/// The page size of `q` (default [`DEFAULT_LIMIT`], 1..=[`MAX_LIMIT`]).
fn limit_of(q: &AuditQuery) -> usize {
    q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT) as usize
}

/// The query filters.
#[derive(Clone, Debug)]
struct Filter {
    since: Option<u64>,
    until: Option<u64>,
    tenant: Option<String>,
    actor: Option<String>,
    action: Option<String>,
}

impl Filter {
    fn new(q: &AuditQuery) -> Result<Self, ApiError> {
        if let (Some(s), Some(u)) = (q.since_ms, q.until_ms)
            && u < s
        {
            return Err(ApiError::invalid_argument("until must not be before since"));
        }
        Ok(Self {
            since: q.since_ms,
            until: q.until_ms,
            tenant: q.tenant.clone().filter(|t| !t.is_empty()),
            actor: q.actor.clone().filter(|a| !a.is_empty()),
            action: q.action.clone().filter(|a| !a.is_empty()),
        })
    }

    fn action_ok(&self, a: &str) -> bool {
        let control = matches!(a, SEGMENT_OPEN | AUDIT_RECOVERED | RETENTION_TRIM);
        match &self.action {
            None => !control,
            Some(f) => match f.strip_suffix('*') {
                Some(prefix) => a.starts_with(prefix) && !control,
                None => a == f,
            },
        }
    }

    fn matches(&self, r: &Record) -> bool {
        self.since.is_none_or(|s| r.ts_ms >= s)
            && self.until.is_none_or(|u| r.ts_ms < u)
            && match self.tenant.as_deref() {
                None => true,
                Some("_") => r.tenant.is_none(),
                Some(t) => r.tenant.as_deref() == Some(t),
            }
            && self.actor.as_ref().is_none_or(|a| &r.actor == a)
            && self.action_ok(&r.action)
    }
}

/// The served form of a record.
fn view(r: Record) -> AuditRecordView {
    let outcome = match &r.outcome {
        AuditOutcome::Ok => "ok".to_owned(),
        AuditOutcome::Denied(why) => format!("denied:{why}"),
        AuditOutcome::Failed(why) => format!("failed:{why}"),
    };
    let mut details = r.details;
    if let Some(c) = r.count {
        details.insert("count".into(), c.to_string());
    }
    if let Some(t) = r.first_ts_ms {
        details.insert("first_ts_ms".into(), t.to_string());
    }
    if let Some(t) = r.last_ts_ms {
        details.insert("last_ts_ms".into(), t.to_string());
    }
    AuditRecordView {
        seq: r.seq,
        ts_ms: r.ts_ms,
        node: r.node,
        actor: r.actor,
        actor_kind: r.actor_kind.as_str().to_owned(),
        tenant: r.tenant,
        action: r.action,
        target: r.target,
        outcome,
        request_id: r.request_id,
        details,
        hash: r.hash,
    }
}

/// The parsed records of one segment, in file order (a torn or unparsable line ends it:
/// the writer may be appending to the newest segment right now).
fn read_records(path: &Path) -> std::io::Result<Vec<Record>> {
    let f = std::fs::File::open(path)?;
    let mut out = Vec::new();
    for line in BufReader::new(f).split(b'\n') {
        match serde_json::from_slice::<Record>(&line?) {
            Ok(r) => out.push(r),
            Err(_) => break,
        }
    }
    Ok(out)
}

/// The `ts_ms` of a segment's first record (its `segment_open`).
fn first_ts(path: &Path) -> Option<u64> {
    let f = std::fs::File::open(path).ok()?;
    let mut line = Vec::new();
    BufReader::new(f).read_until(b'\n', &mut line).ok()?;
    serde_json::from_slice::<Record>(&line)
        .ok()
        .map(|r| r.ts_ms)
}

/// The oldest `limit` records of the journal in `dir` with `seq > after` that pass
/// `filter`, in `seq` order, and whether more exist. Segments entirely at or before `after`,
/// or entirely before `since` (judged by the next segment's opening time), are skipped
/// without being read.
fn scan(
    dir: &Path,
    filter: &Filter,
    after: u64,
    limit: usize,
) -> std::io::Result<(Vec<AuditRecordView>, bool)> {
    let segs: Vec<(u64, PathBuf)> = match list_segments(dir) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e),
    };
    let mut out = Vec::new();
    for (i, (_, path)) in segs.iter().enumerate() {
        if let Some((next_first, next_path)) = segs.get(i + 1) {
            if *next_first <= after.saturating_add(1) {
                continue;
            }
            if let Some(since) = filter.since
                && first_ts(next_path).is_some_and(|t| t < since)
            {
                continue;
            }
        }
        let records = match read_records(path) {
            Ok(r) => r,
            // Removed by retention between the listing and the read.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for r in records {
            if r.seq <= after || !filter.matches(&r) {
                continue;
            }
            if out.len() == limit {
                return Ok((out, true));
            }
            out.push(view(r));
        }
    }
    Ok((out, false))
}

/// `a1.<node>-<seq>[.<node>-<seq>]…`.
pub fn encode_cursor(m: &BTreeMap<u64, u64>) -> String {
    let mut s = String::from(CURSOR_PREFIX);
    for (n, seq) in m {
        s.push_str(&format!(".{n}-{seq}"));
    }
    s
}

/// The per-node positions of a cursor (empty for none); 400 for a malformed one.
pub fn decode_cursor(c: Option<&str>) -> Result<BTreeMap<u64, u64>, ApiError> {
    let mut m = BTreeMap::new();
    let Some(c) = c.filter(|c| !c.is_empty()) else {
        return Ok(m);
    };
    let bad = || ApiError::invalid_argument("malformed audit cursor");
    let mut parts = c.split('.');
    if parts.next() != Some(CURSOR_PREFIX) {
        return Err(bad());
    }
    for p in parts {
        let (n, seq) = p.split_once('-').ok_or_else(bad)?;
        let n: u64 = n.parse().map_err(|_| bad())?;
        let seq: u64 = seq.parse().map_err(|_| bad())?;
        if m.insert(n, seq).is_some() {
            return Err(bad());
        }
    }
    Ok(m)
}

/// Merge per-node pages (each in `seq` order) into one page of at most `limit` records
/// ordered by `(ts_ms, node, seq)`. `cursor` is the query's cursor; a page's flag says
/// whether its node had more records than it returned.
fn merge(
    pages: Vec<(Vec<AuditRecordView>, bool)>,
    cursor: &BTreeMap<u64, u64>,
    limit: usize,
    mut partial_nodes: Vec<u64>,
) -> AuditPage {
    let mut lists: Vec<VecDeque<AuditRecordView>> = Vec::new();
    let mut more = false;
    for (records, leg_more) in pages {
        more |= leg_more;
        lists.push(records.into());
    }
    let mut out = Vec::new();
    while out.len() < limit {
        let best = lists
            .iter()
            .enumerate()
            .filter_map(|(i, l)| l.front().map(|r| ((r.ts_ms, r.node, r.seq), i)))
            .min();
        let Some((_, i)) = best else { break };
        out.extend(lists[i].pop_front());
    }
    more |= lists.iter().any(|l| !l.is_empty());
    let mut next = cursor.clone();
    for r in &out {
        next.insert(r.node, r.seq);
    }
    partial_nodes.sort_unstable();
    partial_nodes.dedup();
    AuditPage {
        records: out,
        cursor: more.then(|| encode_cursor(&next)),
        partial_nodes,
    }
}

/// The routed query (`route::RoutedExtensions`): this node's page plus every member's,
/// merged ([`merge`]); members that do not answer go to `partial_nodes`.
pub async fn fan_out(
    core: &RouteCore,
    local: Arc<dyn AuditApi>,
    q: AuditQuery,
) -> Result<AuditPage, ApiError> {
    let cursor = decode_cursor(q.cursor.as_deref())?;
    Filter::new(&q)?;
    let limit = limit_of(&q);
    let legs = core
        .each_node(
            RouteClass::FanoutNodes,
            || local.query(q.clone()),
            |peer: PeerApi| {
                let q = q.clone();
                async move {
                    let Some(a) = peer.ext.audit else {
                        return Err(ApiError::unavailable("the member serves no audit query"));
                    };
                    // A per-leg wait bound (tokio timer), not a timestamp.
                    match tokio::time::timeout(LEG_TIMEOUT, a.query(q)).await {
                        Ok(r) => r,
                        Err(_) => Err(ApiError::unavailable("the member did not answer in time")),
                    }
                }
            },
        )
        .await;
    let mut pages = Vec::new();
    let mut partial = Vec::new();
    for (node, r) in legs {
        match (node, r) {
            (_, Ok(p)) => {
                pages.push((p.records, p.cursor.is_some()));
                partial.extend(p.partial_nodes);
            }
            // This node's own journal failed: the whole query fails.
            (None, Err(e)) => return Err(e),
            (Some(n), Err(e)) => {
                tracing::warn!(node = n, error = %e, "audit query: a member did not answer");
                partial.push(n);
            }
        }
    }
    Ok(merge(pages, &cursor, limit, partial))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(node: u64, seq: u64, ts: u64) -> AuditRecordView {
        AuditRecordView {
            seq,
            ts_ms: ts,
            node,
            action: "run.cancel".into(),
            ..AuditRecordView::default()
        }
    }

    #[test]
    fn cursor_round_trips_and_rejects_garbage() {
        let m = BTreeMap::from([(1, 42), (3, 7)]);
        let c = encode_cursor(&m);
        assert_eq!(c, "a1.1-42.3-7");
        assert_eq!(decode_cursor(Some(&c)).unwrap(), m);
        assert!(decode_cursor(None).unwrap().is_empty());
        for bad in ["x", "a2.1-2", "a1.1", "a1.1-x", "a1.1-2.1-3", "a1.-1"] {
            assert!(decode_cursor(Some(bad)).is_err(), "{bad}");
        }
    }

    #[test]
    fn merge_orders_by_time_node_seq_and_pages() {
        let a = vec![rec(1, 1, 10), rec(1, 2, 30), rec(1, 3, 50)];
        let b = vec![rec(2, 1, 20), rec(2, 2, 30)];
        let page = merge(
            vec![(a.clone(), false), (b.clone(), false)],
            &BTreeMap::new(),
            3,
            vec![],
        );
        let got: Vec<_> = page.records.iter().map(|r| (r.node, r.seq)).collect();
        assert_eq!(got, [(1, 1), (2, 1), (1, 2)]);
        assert_eq!(page.cursor.as_deref(), Some("a1.1-2.2-1"));
        let page = merge(
            vec![(a, false), (b, false)],
            &BTreeMap::new(),
            10,
            vec![3, 3],
        );
        assert_eq!(page.records.len(), 5);
        assert_eq!(page.cursor, None);
        assert_eq!(page.partial_nodes, [3]);
    }

    #[test]
    fn filters_select_tenant_actor_action_and_time() {
        let q = AuditQuery {
            tenant: Some("_".into()),
            action: Some("run.*".into()),
            since_ms: Some(10),
            until_ms: Some(20),
            ..AuditQuery::default()
        };
        let f = Filter::new(&q).unwrap();
        let mut r: Record = serde_json::from_value(serde_json::json!({
            "v": 1, "seq": 1, "ts_ms": 10, "node": 1, "incarnation": 1, "actor": "dev",
            "actor_kind": "dev", "tenant": null, "action": "run.cancel", "target": "r",
            "outcome": {"result": "ok"}, "request_id": null, "details": {},
            "prev_hash": "", "hash": ""
        }))
        .unwrap();
        assert!(f.matches(&r));
        r.ts_ms = 20;
        assert!(!f.matches(&r));
        r.ts_ms = 15;
        r.tenant = Some("acme".into());
        assert!(!f.matches(&r));
        r.tenant = None;
        r.action = SEGMENT_OPEN.into();
        assert!(!f.matches(&r));
        let all = Filter::new(&AuditQuery::default()).unwrap();
        assert!(!all.matches(&r), "control records are hidden by default");
        let named = Filter::new(&AuditQuery {
            action: Some(SEGMENT_OPEN.into()),
            ..AuditQuery::default()
        })
        .unwrap();
        assert!(named.matches(&r));
        assert!(
            Filter::new(&AuditQuery {
                since_ms: Some(5),
                until_ms: Some(4),
                ..AuditQuery::default()
            })
            .is_err()
        );
    }
}
