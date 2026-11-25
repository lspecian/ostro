//! Indexed run search (Phase 11, D-P11-9/10, plan 11-11): `ListRunsRequest` served from
//! the partition run index `x_run_index` instead of the scan in [`super::list`].
//!
//! # Which requests come here
//!
//! - `order = newest`: served by the index, newest first (`created_at_ms` descending, ties
//!   by run uuid descending, then partition ascending).
//! - `dlq = true` in the default (oldest) order: the budgeted scan with the DLQ membership
//!   predicate ([`p11_dlq::is_dlq_member`]); the index cannot serve oldest first.
//! - Everything else returns `None`, and the caller takes the unchanged scan
//!   ([`super::list::list_runs`]); its in-memory filter honours every `tags` entry (AND).
//!
//! # Prefix choice
//!
//! First tag -> `Tag`; else workflow -> `Workflow`; else `dlq` -> `Status(Failed)` and
//! `Status(TimedOut)` (two prefixes merged); else a terminal status -> `Status`; else
//! `All` (`0x05`, every run). The remaining filters (other tags, queue, non-terminal
//! status, creation range, DLQ membership) apply in memory on the decoded `RunRecord`.
//!
//! # Merge and page token
//!
//! One stream per `(prefix, partition)`, read with [`query::index_page`] [`REFILL`] rows at
//! a time, merged by `(!created_at_ms, !uuid, partition)`. The token is
//! `i1.<kind>.<created_at_ms>.<run id>`: the last run **examined** (matching or not), with
//! `kind` one of `t` (tag), `w` (workflow), `d` (dlq), `s` (status), `a` (all). Because
//! every prefix sorts by the same suffix, one position resumes every stream: partition `q`
//! continues strictly after the position when `q <= p`, and at the position itself when
//! `q > p` (the predecessor key is used as the exclusive start). A token whose kind does
//! not match the request, or that is malformed, is 400.
//!
//! # Budget
//!
//! Like [`super::list`]: a call stops after `limit` matches or after examining the budget
//! ([`list::SCAN_BUDGET`]) of index rows, and returns a token unless every stream is
//! exhausted.
//!
//! # Fallback while the index is rebuilding
//!
//! When a partition's index is `Rebuilding` (missing / stale marker, D-P11-9), a newest
//! request without a token (or with a scan token `c1.`) is answered by the budgeted
//! oldest-first scan with the same filters; its pages carry scan tokens, so a client
//! paging through the fallback stays on the scan. A request carrying an index token while
//! the index is rebuilding is 503 `unavailable` (`details.reason = "index_rebuilding"`):
//! the position cannot be resumed by the scan. The REST response does not yet flag the
//! oldest-order fallback (the `X-Ostro-Order-Fallback` header needs a `RunPage` field in
//! ostro-api; recorded in 11-11-SUMMARY).

use std::cmp::Reverse;
use std::collections::VecDeque;

use ostro_api::ApiError;
use ostro_api::domain::{ListOrder, ListRunsRequest, RunPage, RunStatus, effective_limit};
use ostro_partition::query::{self, IndexError};
use ostro_partition::store::StoreReader;
use ostro_partition::store::derived::{IndexKind, SUFFIX_LEN, index_prefix};
use ostro_partition::store::values::RunRecord;
use ostro_types::{PartitionCount, RunId};

use super::list::{self, matches, summary};
use super::p11_dlq;
use super::runs::{store_error, task_error};
use crate::p11::P11Ctx;

/// Index rows read from one stream per refill (at most the budget).
pub const REFILL: usize = 128;
/// Index page token prefix (format version 1).
const TOKEN_PREFIX: &str = "i1.";
/// Scan page token prefix ([`super::list`]).
const SCAN_TOKEN_PREFIX: &str = "c1.";

/// Merge position: `(Reverse(created_at_ms), Reverse(uuid), partition)`, ascending =
/// newest first.
type Pos = (Reverse<u64>, Reverse<u128>, u16);

fn pos(run: &RunId, created_at_ms: u64) -> Pos {
    (
        Reverse(created_at_ms),
        Reverse(run.uuid().as_u128()),
        run.partition().0,
    )
}

/// The index prefixes serving `req` and the token kind.
fn plan(req: &ListRunsRequest) -> (char, Vec<Vec<u8>>) {
    // Values containing 0x00 have no index row (store::derived): filter them in memory.
    if let Some(t) = req.tags.first().or(req.tag.as_ref())
        && !t.contains('\0')
    {
        return ('t', vec![index_prefix(IndexKind::Tag, t.as_bytes())]);
    }
    if let Some(w) = req.workflow.as_ref().filter(|w| !w.contains('\0')) {
        return ('w', vec![index_prefix(IndexKind::Workflow, w.as_bytes())]);
    }
    if req.dlq {
        return (
            'd',
            vec![
                index_prefix(
                    IndexKind::Status,
                    &[status_code(RunStatus::Failed).unwrap_or(4)],
                ),
                index_prefix(
                    IndexKind::Status,
                    &[status_code(RunStatus::TimedOut).unwrap_or(7)],
                ),
            ],
        );
    }
    if let Some(code) = req.status.and_then(status_code) {
        return ('s', vec![index_prefix(IndexKind::Status, &[code])]);
    }
    ('a', vec![index_prefix(IndexKind::All, &[])])
}

/// The status byte of a terminal API status (`store::derived::status_code`); `None` for
/// the statuses that have no status row.
fn status_code(s: RunStatus) -> Option<u8> {
    match s {
        RunStatus::Scheduled | RunStatus::Ready | RunStatus::Running => None,
        RunStatus::Completed => Some(3),
        RunStatus::Failed => Some(4),
        RunStatus::Cancelled => Some(5),
        RunStatus::Terminated => Some(6),
        RunStatus::TimedOut => Some(7),
        RunStatus::ContinuedAsNew => Some(8),
    }
}

/// Encode a merge position as an index page token.
pub fn encode_token(kind: char, created_at_ms: u64, run: &RunId) -> String {
    format!("{TOKEN_PREFIX}{kind}.{created_at_ms}.{run}")
}

/// Decode an index page token for a request of `kind` (400 when malformed, of another
/// kind, or naming a partition outside `count`).
pub fn decode_token(t: &str, kind: char, count: PartitionCount) -> Result<Pos, ApiError> {
    let bad = || ApiError::invalid_argument("page_token: malformed");
    let rest = t.strip_prefix(TOKEN_PREFIX).ok_or_else(bad)?;
    let mut parts = rest.splitn(3, '.');
    let k = parts.next().ok_or_else(bad)?;
    let created: u64 = parts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
    let run: RunId = parts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
    if k.len() != 1 || !k.starts_with(kind) {
        return Err(ApiError::invalid_argument(
            "page_token: belongs to a different query",
        ));
    }
    if !count.contains(run.partition()) {
        return Err(bad());
    }
    Ok(pos(&run, created))
}

/// The ordering suffix of a position (`!created_at_ms ‖ !uuid`).
fn suffix(p: &Pos) -> [u8; SUFFIX_LEN] {
    let mut s = [0u8; SUFFIX_LEN];
    s[..8].copy_from_slice(&(!p.0.0).to_be_bytes());
    s[8..].copy_from_slice(&(!p.1.0).to_be_bytes());
    s
}

/// The suffix as a 24-byte big-endian integer minus one; `None` at zero.
fn predecessor(mut s: [u8; SUFFIX_LEN]) -> Option<[u8; SUFFIX_LEN]> {
    for b in s.iter_mut().rev() {
        if *b == 0 {
            *b = 0xff;
        } else {
            *b -= 1;
            return Some(s);
        }
    }
    None
}

/// Where stream `(prefix, q)` resumes after position `cur` (exclusive start key).
fn resume_after(prefix: &[u8], q: u16, cur: Option<&Pos>) -> Option<Vec<u8>> {
    let cur = cur?;
    let sfx = if q <= cur.2 {
        suffix(cur)
    } else {
        // Rows of q at exactly this position are not examined yet: start just below it.
        predecessor(suffix(cur))?
    };
    let mut k = prefix.to_vec();
    k.extend_from_slice(&sfx);
    Some(k)
}

struct Stream {
    reader: StoreReader,
    prefix: Vec<u8>,
    after: Option<Vec<u8>>,
    /// `(position, run, record)`; the record is `None` when the run vanished between the
    /// index read and the record read (expiry), and such rows are skipped.
    buf: VecDeque<(Pos, RunId, Option<RunRecord>)>,
    done: bool,
}

/// A failed indexed read: the index is rebuilding (fall back), or a real error.
enum SearchError {
    Rebuilding,
    Api(ApiError),
}

impl From<IndexError> for SearchError {
    fn from(e: IndexError) -> Self {
        match e {
            IndexError::Rebuilding => SearchError::Rebuilding,
            IndexError::Store(e) => SearchError::Api(store_error(e)),
        }
    }
}

impl From<ApiError> for SearchError {
    fn from(e: ApiError) -> Self {
        SearchError::Api(e)
    }
}

/// Refill every empty, unfinished stream (one blocking task for all of them).
async fn refill(streams: &mut Vec<Stream>, per: usize) -> Result<(), SearchError> {
    if !streams.iter().any(|s| s.buf.is_empty() && !s.done) {
        return Ok(());
    }
    let taken = std::mem::take(streams);
    let back = tokio::task::spawn_blocking(move || {
        let mut taken = taken;
        for s in taken.iter_mut().filter(|s| s.buf.is_empty() && !s.done) {
            let page = query::index_page(&s.reader, &s.prefix, s.after.as_deref(), per, per + 1)?;
            for (run, created) in page.runs {
                let rec = query::run_record(&s.reader, &run)?;
                s.buf.push_back((pos(&run, created), run, rec));
            }
            match page.next {
                Some(n) => s.after = Some(n),
                None => s.done = true,
            }
        }
        Ok::<_, IndexError>(taken)
    })
    .await
    .map_err(task_error)??;
    *streams = back;
    Ok(())
}

fn search_error(e: SearchError) -> ApiError {
    match e {
        SearchError::Rebuilding => ApiError::unavailable(
            "the run index is rebuilding; restart the listing without a page token",
        )
        .with_details(serde_json::json!({ "reason": "index_rebuilding" })),
        SearchError::Api(e) => e,
    }
}

/// `Some(page)` when this module serves `req` (see the module docs); `None` falls back to
/// the existing scan ([`super::list::list_runs`]). Called first by
/// `ServerControlPlane::list_runs` once the P11 context is set.
pub async fn list_runs_indexed(
    ctx: &P11Ctx,
    req: &ListRunsRequest,
) -> Option<Result<RunPage, ApiError>> {
    list_runs_indexed_with_budget(ctx, req, list::SCAN_BUDGET).await
}

/// [`list_runs_indexed`] with an explicit budget (tests exercise the budget path with a
/// small one; production always uses [`list::SCAN_BUDGET`]).
#[doc(hidden)]
pub async fn list_runs_indexed_with_budget(
    ctx: &P11Ctx,
    req: &ListRunsRequest,
    budget: usize,
) -> Option<Result<RunPage, ApiError>> {
    let scan_token = req
        .page_token
        .as_deref()
        .is_some_and(|t| t.starts_with(SCAN_TOKEN_PREFIX));
    match (req.order, req.dlq) {
        (ListOrder::Newest, _) if !scan_token => Some(match indexed(ctx, req, budget).await {
            Err(SearchError::Rebuilding) if !has_token(req) => scan(ctx, req, budget).await,
            r => r.map_err(search_error),
        }),
        (ListOrder::Newest, _) | (ListOrder::Oldest, true) => Some(scan(ctx, req, budget).await),
        (ListOrder::Oldest, false) => None,
    }
}

fn has_token(req: &ListRunsRequest) -> bool {
    req.page_token.as_deref().is_some_and(|t| !t.is_empty())
}

/// The DLQ membership predicate when `req.dlq`.
fn keep(
    ctx: &P11Ctx,
    req: &ListRunsRequest,
    run: &RunId,
    rec: &RunRecord,
) -> Result<bool, ApiError> {
    if req.dlq {
        p11_dlq::is_dlq_member(ctx, run, rec)
    } else {
        Ok(true)
    }
}

/// The budgeted oldest-first scan with the same filters (fallback and oldest DLQ).
async fn scan(ctx: &P11Ctx, req: &ListRunsRequest, budget: usize) -> Result<RunPage, ApiError> {
    let req2 = req.clone();
    let ctx2 = ctx.clone();
    list::list_runs_scan(&ctx.cc.partitions, req.clone(), budget, &move |run, rec| {
        keep(&ctx2, &req2, run, rec)
    })
    .await
}

/// The index-served newest-first page.
async fn indexed(
    ctx: &P11Ctx,
    req: &ListRunsRequest,
    budget: usize,
) -> Result<RunPage, SearchError> {
    let cp = &ctx.cc.partitions;
    let budget = budget.max(1);
    let count = cp.partition_count();
    let limit = effective_limit(req.limit) as usize;
    let (kind, prefixes) = plan(req);
    let cursor = match req.page_token.as_deref().filter(|t| !t.is_empty()) {
        Some(t) => Some(decode_token(t, kind, count)?),
        None => None,
    };

    let mut streams = Vec::with_capacity(prefixes.len() * usize::from(count.get()));
    for prefix in &prefixes {
        // Phase 13 (13-12): a fan-out leg scans only its `ostro-partitions` (from node
        // identities); everything else scans every partition.
        for p in crate::route::table::listed_partitions(count) {
            let reader = cp.partition(p)?.reader();
            streams.push(Stream {
                after: resume_after(prefix, p.0, cursor.as_ref()),
                reader,
                prefix: prefix.clone(),
                buf: VecDeque::new(),
                done: false,
            });
        }
    }

    let per = REFILL.min(budget);
    let mut runs = Vec::new();
    let mut last: (u64, RunId);
    let mut scanned = 0usize;
    loop {
        refill(&mut streams, per).await?;
        let next = streams
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.buf.front().map(|(p, _, _)| (*p, i)))
            .min();
        let Some((p, i)) = next else {
            return Ok(RunPage {
                runs,
                next_page_token: None,
            });
        };
        let (_, run, rec) = streams[i].buf.pop_front().expect("front exists");
        scanned += 1;
        last = (p.0.0, run);
        if let Some(rec) = rec
            && matches(req, &rec)
            && keep(ctx, req, &run, &rec)?
        {
            runs.push(summary(run, rec));
        }
        if runs.len() >= limit || scanned >= budget {
            break;
        }
    }

    // Stopped on the limit or the budget: a token only if anything is left.
    refill(&mut streams, per).await?;
    let more = streams.iter().any(|s| !s.buf.is_empty());
    Ok(RunPage {
        runs,
        next_page_token: more.then(|| encode_token(kind, last.0, &last.1)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_types::PartitionId;

    fn count(n: u16) -> PartitionCount {
        PartitionCount::new(n).expect("count")
    }

    #[test]
    fn token_round_trips_and_checks_kind() {
        let run = RunId::new(PartitionId(3), uuid::Uuid::now_v7());
        let t = encode_token('t', 1_700_000_000_123, &run);
        assert!(t.starts_with("i1.t.1700000000123."), "{t}");
        assert_eq!(
            decode_token(&t, 't', count(4)).expect("ok"),
            pos(&run, 1_700_000_000_123)
        );
        assert!(decode_token(&t, 'a', count(4)).is_err(), "other kind");
        assert!(
            decode_token(&t, 't', count(2)).is_err(),
            "partition outside"
        );
        for bad in ["", "i1.", "i1.t", "i1.t.x.y", "c1.abc", "i1.tt.1.x"] {
            assert!(decode_token(bad, 't', count(4)).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn suffix_matches_the_index_key_and_predecessor_steps_back() {
        let run = RunId::new(PartitionId(1), uuid::Uuid::now_v7());
        let p = pos(&run, 42);
        let key = ostro_partition::store::derived::index_key(
            IndexKind::All,
            &[],
            42,
            run.uuid().as_bytes(),
        );
        assert_eq!(&key[1..], &suffix(&p)[..]);
        let mut one = [0u8; SUFFIX_LEN];
        one[SUFFIX_LEN - 1] = 1;
        assert_eq!(predecessor(one), Some([0u8; SUFFIX_LEN]));
        let mut carry = [0u8; SUFFIX_LEN];
        carry[SUFFIX_LEN - 2] = 1;
        let mut want = [0u8; SUFFIX_LEN];
        want[SUFFIX_LEN - 1] = 0xff;
        assert_eq!(predecessor(carry), Some(want));
        assert_eq!(predecessor([0u8; SUFFIX_LEN]), None);
    }

    #[test]
    fn resume_positions_follow_merge_order() {
        let run = RunId::new(PartitionId(2), uuid::Uuid::now_v7());
        let p = pos(&run, 7);
        let prefix = index_prefix(IndexKind::All, &[]);
        assert_eq!(resume_after(&prefix, 0, None), None);
        let at = resume_after(&prefix, 2, Some(&p)).expect("after");
        assert_eq!(&at[1..], &suffix(&p)[..]);
        let below = resume_after(&prefix, 3, Some(&p)).expect("after");
        assert!(below < at, "q > p resumes at the position itself");
    }

    #[test]
    fn prefix_choice() {
        let mut r = ListRunsRequest {
            tags: vec!["blue".into(), "big".into()],
            workflow: Some("w".into()),
            ..Default::default()
        };
        assert_eq!(plan(&r).0, 't');
        r.tags.clear();
        assert_eq!(plan(&r).0, 'w');
        r.workflow = None;
        r.dlq = true;
        let (k, p) = plan(&r);
        assert_eq!((k, p.len()), ('d', 2));
        r.dlq = false;
        r.status = Some(RunStatus::Completed);
        assert_eq!(plan(&r).0, 's');
        r.status = Some(RunStatus::Running);
        assert_eq!(plan(&r).0, 'a');
        r.tags = vec!["nul\0tag".into()];
        assert_eq!(plan(&r).0, 'a', "NUL tags have no rows");
    }
}
