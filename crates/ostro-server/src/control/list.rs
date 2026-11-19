//! `list_runs` (API-04): a filtered, paginated listing over every partition.
//!
//! # Order
//!
//! Runs are listed **oldest first**, in `(uuid v7, partition)` order. Within a partition the
//! `runs` keyspace is already in uuid v7 order (creation time, see `query::runs_page`), so
//! the listing is a k-way merge of P sorted streams. Oldest-first is what the store scans
//! natively (there is no reverse scan) and it gives stable pagination: runs started after a
//! page was served have newer uuids and show up on later pages, never shifting earlier ones.
//!
//! # Page token
//!
//! The token is `c1.<run id>`: the last run **examined** (matching or not) in merge order.
//! Because the order is global, one position is enough to resume every partition:
//! partition `q` continues strictly after `(q, uuid)` when `q <= p`, and at `uuid` itself
//! when `q > p` (those rows sort after the cursor). The token is opaque to clients; a
//! malformed one (wrong prefix, bad id, partition outside the count) is 400.
//!
//! # Budget
//!
//! A call stops after `limit` matches (`effective_limit`: default 100, max 1000) or after
//! examining [`SCAN_BUDGET`] rows, whichever comes first, and returns a continuation token
//! unless every partition is exhausted. A filter that matches nothing therefore returns
//! empty pages with tokens until the scan reaches the end. Reads happen in
//! `spawn_blocking`, [`REFILL`] rows per partition at a time.
//!
//! Filters are applied in memory: workflow, status, queue, tag (the run carries it) and the
//! creation range `created_after_ms <= created_at_ms < created_before_ms`.

use std::collections::VecDeque;

use ostro_api::ApiError;
use ostro_api::domain::{ListRunsRequest, RunPage, RunSummary, effective_limit};
use ostro_partition::query;
use ostro_partition::store::StoreReader;
use ostro_partition::store::values::RunRecord;
use ostro_types::{PartitionId, RunId};

use super::ServerControlPlane;
use super::runs::{status, store_error, task_error};

/// Rows examined per call at most.
pub const SCAN_BUDGET: usize = 10_000;
/// Rows read from one partition per refill.
pub const REFILL: usize = 128;
/// Page token prefix (format version 1).
const TOKEN_PREFIX: &str = "c1.";

/// Merge position: `(uuid, partition)`.
type Key = (u128, u16);

fn key(r: &RunId) -> Key {
    (r.uuid().as_u128(), r.partition().0)
}

/// Encode the merge position as a page token.
pub fn encode_token(last: Key) -> String {
    let run = RunId::new(PartitionId(last.1), uuid::Uuid::from_u128(last.0));
    format!("{TOKEN_PREFIX}{run}")
}

/// Decode a page token (400 when malformed or naming a partition outside `count`).
pub fn decode_token(t: &str, count: ostro_types::PartitionCount) -> Result<Key, ApiError> {
    let bad = || ApiError::invalid_argument("page_token: malformed");
    let run: RunId = t
        .strip_prefix(TOKEN_PREFIX)
        .ok_or_else(bad)?
        .parse()
        .map_err(|_| bad())?;
    if !count.contains(run.partition()) {
        return Err(bad());
    }
    Ok(key(&run))
}

/// Where partition `q` resumes after merge position `cur`: `runs_page(after)`.
fn resume_after(q: u16, cur: Option<Key>) -> Option<RunId> {
    let (uuid, p) = cur?;
    if q <= p {
        Some(RunId::new(PartitionId(q), uuid::Uuid::from_u128(uuid)))
    } else {
        // Rows of q with this uuid sort after the cursor: start just below it.
        uuid.checked_sub(1)
            .map(|u| RunId::new(PartitionId(q), uuid::Uuid::from_u128(u)))
    }
}

struct Stream {
    reader: StoreReader,
    after: Option<RunId>,
    buf: VecDeque<(RunId, RunRecord)>,
    done: bool,
}

fn matches(req: &ListRunsRequest, rec: &RunRecord) -> bool {
    req.workflow.as_ref().is_none_or(|w| *w == rec.workflow)
        && req.status.is_none_or(|s| s == status(rec.status))
        && req.queue.as_ref().is_none_or(|q| *q == rec.queue)
        && req.tag.as_ref().is_none_or(|t| rec.tags.contains(t))
        && req.created_after_ms.is_none_or(|a| rec.created_at_ms >= a)
        && req.created_before_ms.is_none_or(|b| rec.created_at_ms < b)
}

fn summary(run: RunId, rec: RunRecord) -> RunSummary {
    RunSummary {
        run_id: run.to_string(),
        workflow: rec.workflow,
        version: rec.workflow_version,
        status: status(rec.status),
        queue: rec.queue,
        created_at_ms: rec.created_at_ms,
        tags: rec.tags,
    }
}

/// Refill every empty, unfinished stream (one blocking task for all of them).
async fn refill(streams: &mut Vec<Stream>) -> Result<(), ApiError> {
    if !streams.iter().any(|s| s.buf.is_empty() && !s.done) {
        return Ok(());
    }
    let taken = std::mem::take(streams);
    let back = tokio::task::spawn_blocking(move || {
        let mut taken = taken;
        for s in taken.iter_mut().filter(|s| s.buf.is_empty() && !s.done) {
            let rows = query::runs_page(&s.reader, s.after, REFILL)?;
            s.done = rows.len() < REFILL;
            if let Some((last, _)) = rows.last() {
                s.after = Some(*last);
            }
            s.buf.extend(rows);
        }
        Ok::<_, ostro_partition::StoreError>(taken)
    })
    .await
    .map_err(task_error)?
    .map_err(store_error)?;
    *streams = back;
    Ok(())
}

pub(crate) async fn list_runs(
    cp: &ServerControlPlane,
    req: ListRunsRequest,
) -> Result<RunPage, ApiError> {
    list_runs_with_budget(cp, req, SCAN_BUDGET).await
}

/// [`list_runs`] with an explicit scan budget (tests exercise the budget path with a small
/// one; production always uses [`SCAN_BUDGET`]).
#[doc(hidden)]
pub async fn list_runs_with_budget(
    cp: &ServerControlPlane,
    req: ListRunsRequest,
    budget: usize,
) -> Result<RunPage, ApiError> {
    let budget = budget.max(1);
    let count = cp.partition_count();
    let limit = effective_limit(req.limit) as usize;
    let cursor = req
        .page_token
        .as_deref()
        .filter(|t| !t.is_empty())
        .map(|t| decode_token(t, count))
        .transpose()?;

    let mut streams = Vec::with_capacity(usize::from(count.get()));
    for p in count.ids() {
        streams.push(Stream {
            reader: cp.partition(p)?.reader(),
            after: resume_after(p.0, cursor),
            buf: VecDeque::new(),
            done: false,
        });
    }

    let mut runs = Vec::new();
    let mut last: Key;
    let mut scanned = 0usize;
    loop {
        refill(&mut streams).await?;
        let next = streams
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.buf.front().map(|(r, _)| (key(r), i)))
            .min();
        let Some((k, i)) = next else {
            // Every partition is exhausted.
            return Ok(RunPage {
                runs,
                next_page_token: None,
            });
        };
        let (run, rec) = streams[i].buf.pop_front().expect("front exists");
        scanned += 1;
        last = k;
        if matches(&req, &rec) {
            runs.push(summary(run, rec));
        }
        if runs.len() >= limit || scanned >= budget {
            break;
        }
    }

    // Stopped on the limit or the budget: a token only if anything is left.
    refill(&mut streams).await?;
    let more = streams.iter().any(|s| !s.buf.is_empty());
    Ok(RunPage {
        runs,
        next_page_token: more.then(|| encode_token(last)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(n: u16) -> ostro_types::PartitionCount {
        ostro_types::PartitionCount::new(n).expect("count")
    }

    #[test]
    fn token_round_trips_and_rejects_garbage() {
        let k = (uuid::Uuid::now_v7().as_u128(), 5);
        let t = encode_token(k);
        assert!(t.starts_with(TOKEN_PREFIX));
        assert_eq!(decode_token(&t, count(16)).expect("ok"), k);
        // Partition outside the count.
        assert!(decode_token(&t, count(4)).is_err());
        for bad in ["", "garbage", "c1.", "c2.0000", &t[TOKEN_PREFIX.len()..]] {
            assert!(decode_token(bad, count(16)).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn resume_positions_follow_merge_order() {
        let u = uuid::Uuid::now_v7().as_u128();
        assert_eq!(resume_after(3, None), None);
        // q <= p: strictly after (q, u).
        let a = resume_after(2, Some((u, 2))).expect("after");
        assert_eq!(key(&a), (u, 2));
        // q > p: rows of q with uuid u are not yet examined.
        let b = resume_after(3, Some((u, 2))).expect("after");
        assert_eq!(key(&b), (u - 1, 3));
        assert_eq!(resume_after(3, Some((0, 2))), None);
    }
}
