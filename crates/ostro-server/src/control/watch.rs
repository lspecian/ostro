//! Long-poll result waiters (`GET /v1/runs/{id}/result?wait=`, `Runs.AwaitResult`; plan
//! 05-13).
//!
//! [`RunWatchers`] holds **waiters only, never state**: per partition a
//! `Mutex<BTreeMap<RunId, Vec<(id, oneshot::Sender<()>)>>>`. One task per partition
//! forwards the apply machine's action broadcast (`MachineHandle::subscribe_actions`):
//! every `Action::RunCompleted{run, ..}` (every terminal transition, hard stops and
//! continue-as-new included) wakes and removes that run's waiters. On `Lagged` the task
//! cannot know which completions it missed, so it wakes **every** waiter of the partition
//! (they re-read and either return or re-register). On `Closed` (node shutdown) it wakes
//! everyone and exits.
//!
//! A [`Waiter`] removes its own entry when dropped, so a client that disconnects (its
//! request future is dropped) leaves nothing behind; empty vectors are removed.
//!
//! # Lost wake-ups
//!
//! Callers **register first, then read the run record** ([`await_result`]). A completion
//! applied before the read is seen by the read; one applied after the registration fires
//! the waiter. Actions are broadcast after the chunk is committed, so a woken waiter's
//! re-read sees the terminal record. The read itself may race the commit (the
//! register-then-read case): it relies on `StoreReader` reading at fjall's published
//! seqno, so a `CompleteRun` batch is seen whole (terminal record **and** result payload)
//! or not at all, never as a terminal record without its result.
//!
//! # Leadership
//!
//! On this single-node (RF=1) server each partition's apply machine, and with it the action
//! broadcast sender, lives as long as the node: the subscription taken at bootstrap stays
//! valid across term changes, and actions are emitted whenever the partition leads
//! (catch-up discards them, but nothing can wait on a partition that is not serving).
//! Clustered leadership changes (Phase 12) must re-subscribe on the new leader.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use ostro_api::ApiError;
use ostro_api::domain::{AwaitResultRequest, AwaitResultResponse, RunStatus, effective_wait_ms};
use ostro_partition::Action;
use ostro_partition::query;
use ostro_partition::store::values::{self, RunRecord};
use ostro_types::{PartitionId, RunId};
use tokio::sync::{broadcast, oneshot};
use tokio::task::JoinHandle;

use super::ServerControlPlane;
use super::runs::{not_found, parse_run_id, partition_error, status, store_error, task_error};

/// Poll interval while a followed continuation's successor is not created yet.
const SUCCESSOR_POLL: Duration = Duration::from_millis(10);

type Waiters = BTreeMap<RunId, Vec<(u64, oneshot::Sender<()>)>>;

struct Inner {
    parts: Vec<Mutex<Waiters>>,
    next_id: AtomicU64,
}

impl Inner {
    fn part(&self, p: PartitionId) -> MutexGuard<'_, Waiters> {
        self.parts[p.index()]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn wake_run(&self, run: &RunId) {
        if let Some(ws) = self.part(run.partition()).remove(run) {
            for (_, tx) in ws {
                let _ = tx.send(());
            }
        }
    }

    fn wake_all(&self, p: PartitionId) {
        let all = std::mem::take(&mut *self.part(p));
        for (_, tx) in all.into_values().flatten() {
            let _ = tx.send(());
        }
    }
}

/// Per-partition long-poll waiters fed by `Action::RunCompleted` (see the module docs).
pub struct RunWatchers {
    inner: Arc<Inner>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl std::fmt::Debug for RunWatchers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunWatchers")
            .field("partitions", &self.inner.parts.len())
            .field("waiters", &self.waiters())
            .finish()
    }
}

impl RunWatchers {
    /// Empty hub for `partitions` partitions (no forwarding task yet).
    pub fn new(partitions: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                parts: (0..partitions)
                    .map(|_| Mutex::new(BTreeMap::new()))
                    .collect(),
                next_id: AtomicU64::new(0),
            }),
            tasks: Mutex::new(Vec::new()),
        }
    }

    /// Forward partition `p`'s action broadcast to the waiters (one task per partition).
    pub fn watch_partition(
        &self,
        p: PartitionId,
        mut actions: broadcast::Receiver<Arc<Vec<Action>>>,
    ) {
        let inner = Arc::clone(&self.inner);
        let task = tokio::spawn(async move {
            loop {
                match actions.recv().await {
                    Ok(batch) => {
                        for a in batch.iter() {
                            if let Action::RunCompleted { run, .. } = a {
                                inner.wake_run(run);
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => inner.wake_all(p),
                    Err(broadcast::error::RecvError::Closed) => {
                        inner.wake_all(p);
                        break;
                    }
                }
            }
        });
        self.tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(task);
    }

    /// Register a waiter for `run` (do this **before** reading the run).
    pub fn register(&self, run: RunId) -> Waiter {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.inner
            .part(run.partition())
            .entry(run)
            .or_default()
            .push((id, tx));
        Waiter {
            inner: Arc::clone(&self.inner),
            run,
            id,
            rx,
        }
    }

    /// Registered waiters across all partitions (test probe).
    pub fn waiters(&self) -> usize {
        (0..self.inner.parts.len())
            .map(|i| {
                self.inner
                    .part(PartitionId(i as u16))
                    .values()
                    .map(Vec::len)
                    .sum::<usize>()
            })
            .sum()
    }

    /// Runs with at least one waiter (test probe: empty vectors are removed).
    pub fn watched_runs(&self) -> usize {
        (0..self.inner.parts.len())
            .map(|i| self.inner.part(PartitionId(i as u16)).len())
            .sum()
    }

    /// Wake every waiter of partition `p` (also what a `Lagged` receiver does).
    pub fn wake_partition(&self, p: PartitionId) {
        self.inner.wake_all(p);
    }

    /// Stop the forwarding tasks (shutdown).
    pub fn stop(&self) {
        for t in self
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
        {
            t.abort();
        }
    }
}

/// One registered waiter. Dropping it removes the registration.
pub struct Waiter {
    inner: Arc<Inner>,
    run: RunId,
    id: u64,
    rx: oneshot::Receiver<()>,
}

impl std::fmt::Debug for Waiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Waiter")
            .field("run", &self.run)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl Waiter {
    /// Resolves when the run completes (or the partition's waiters are woken).
    pub async fn notified(&mut self) {
        let _ = (&mut self.rx).await;
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        let mut part = self.inner.part(self.run.partition());
        if let Some(ws) = part.get_mut(&self.run) {
            ws.retain(|(id, _)| *id != self.id);
            if ws.is_empty() {
                part.remove(&self.run);
            }
        }
    }
}

// ---------------------------------------------------------------------------------
// await_result
// ---------------------------------------------------------------------------------

/// JSON value of `bytes`, or the text as a JSON string when it is not JSON (the fake's
/// rule too).
fn json_or_string(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(bytes).into_owned()))
}

/// The record of `run` plus its result payload bytes when terminal.
async fn read_outcome(
    cp: &ServerControlPlane,
    run: RunId,
) -> Result<(RunRecord, Option<bytes::Bytes>), ApiError> {
    let handle = cp.partition(run.partition())?;
    let reader = handle.reader();
    let (rec, payload) = tokio::task::spawn_blocking(move || {
        let Some(rec) = query::run_record(&reader, &run)? else {
            return Ok((None, None));
        };
        let payload = if rec.status.is_terminal() {
            query::run_payload(&reader, &run, query::PAYLOAD_RESULT)?
        } else {
            None
        };
        Ok::<_, ostro_partition::StoreError>((Some(rec), payload))
    })
    .await
    .map_err(task_error)?
    .map_err(store_error)?;
    let rec = rec.ok_or_else(|| not_found(&run.to_string()))?;
    let bytes = match payload {
        Some(p) => Some(handle.fetch_payload(&p).await.map_err(partition_error)?),
        None => None,
    };
    Ok((rec, bytes))
}

/// The response for `run` in `rec`'s state. A completed run's `result` is its output; a
/// continued-as-new run's `result` is `{"next_run_id"}`; a failed or cancelled run's
/// `failure` is its failure JSON (`{"reason_code", "kind", "message"}`); a terminated or
/// timed-out run without a failure payload gets `{"reason_code": <terminal reason>}`.
fn response(run: RunId, rec: &RunRecord, payload: Option<&[u8]>) -> AwaitResultResponse {
    let st = status(rec.status);
    let (result, failure) = match (st, payload) {
        (RunStatus::Completed | RunStatus::ContinuedAsNew, Some(b)) => {
            (Some(json_or_string(b)), None)
        }
        (s, Some(b)) if s.is_terminal() => (None, Some(json_or_string(b))),
        (RunStatus::Terminated | RunStatus::TimedOut, None) => (
            None,
            Some(serde_json::json!({ "reason_code": rec.terminal_reason })),
        ),
        _ => (None, None),
    };
    AwaitResultResponse {
        run_id: run.to_string(),
        status: st,
        completed: st.is_terminal(),
        result,
        failure,
    }
}

/// Long-poll: register, read, wait up to `min(wait, 60 s)` for a completion, re-read.
/// With `follow_continuations`, a `ContinuedAsNew` run is followed to its successor
/// (`RunRecord.continued_to`) within the same deadline. The successor is created by the
/// predecessor's outbox `StartRun`, so it may not exist for a moment: it is polled every
/// [`SUCCESSOR_POLL`] until it appears (or the deadline answers the predecessor). Always
/// `Ok` for a known run.
pub(crate) async fn await_result(
    cp: &ServerControlPlane,
    r: AwaitResultRequest,
) -> Result<AwaitResultResponse, ApiError> {
    let wait = Duration::from_millis(effective_wait_ms(r.wait_ms));
    let deadline = tokio::time::Instant::now() + wait;
    let mut run = parse_run_id(cp, &r.run_id)?;
    // Bounds the chain walk (a continue-as-new chain never cycles; stay total anyway).
    let mut hops = 0u32;
    // The last continued-as-new run followed (answered if its successor never appears).
    let mut pred: Option<(RunId, RunRecord, Option<bytes::Bytes>)> = None;
    loop {
        // Register FIRST, then read (lost-wakeup rule).
        let mut waiter = cp.watchers().register(run);
        let (rec, payload) = match read_outcome(cp, run).await {
            Ok(v) => v,
            // The successor is created by the predecessor's outbox `StartRun`, delivered
            // after `continued_to` is set: poll briefly until it exists.
            Err(e) if e.code == ostro_api::ErrorCode::NotFound && pred.is_some() => {
                drop(waiter);
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    let (p, rec, payload) = pred.take().expect("checked");
                    return Ok(response(p, &rec, payload.as_deref()));
                }
                tokio::time::sleep(SUCCESSOR_POLL.min(deadline - now)).await;
                continue;
            }
            Err(e) => return Err(e),
        };
        if rec.status.is_terminal() {
            if r.follow_continuations
                && rec.status == values::RunStatus::ContinuedAsNew
                && hops < 10_000
                && let Some(next) = rec.continued_to
                && cp.partition_count().contains(next.partition())
            {
                hops += 1;
                pred = Some((run, rec, payload));
                run = next;
                continue;
            }
            return Ok(response(run, &rec, payload.as_deref()));
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(response(run, &rec, None));
        }
        let woken = tokio::time::timeout_at(deadline, waiter.notified())
            .await
            .is_ok();
        drop(waiter);
        if !woken {
            // Deadline: one last read, then answer whatever it says.
            let (rec, payload) = read_outcome(cp, run).await?;
            if !(rec.status.is_terminal()
                && r.follow_continuations
                && rec.status == values::RunStatus::ContinuedAsNew)
            {
                return Ok(response(run, &rec, payload.as_deref()));
            }
            // A continuation completed right at the deadline: follow it with no wait
            // left (the loop answers at once).
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(p: u16) -> RunId {
        RunId::new(PartitionId(p), uuid::Uuid::now_v7())
    }

    #[tokio::test]
    async fn completion_wakes_and_drop_unregisters() {
        let w = RunWatchers::new(2);
        let (tx, rx) = broadcast::channel(4);
        w.watch_partition(PartitionId(1), rx);
        let r = run(1);
        let other = run(1);
        let mut a = w.register(r);
        let b = w.register(r);
        let _c = w.register(other);
        assert_eq!(w.waiters(), 3);
        drop(b);
        assert_eq!(w.waiters(), 2);
        tx.send(Arc::new(vec![Action::RunCompleted {
            run: r,
            status: values::RunStatus::Completed,
        }]))
        .expect("send");
        tokio::time::timeout(Duration::from_secs(5), a.notified())
            .await
            .expect("woken");
        drop(a);
        assert_eq!(w.waiters(), 1);
        assert_eq!(w.watched_runs(), 1);
        drop(_c);
        assert_eq!(w.watched_runs(), 0, "empty vectors are removed");
        w.stop();
    }

    #[tokio::test]
    async fn lagged_wakes_the_whole_partition() {
        let w = RunWatchers::new(1);
        let (tx, rx) = broadcast::channel(1);
        let r = run(0);
        let mut a = w.register(r);
        // Overflow the channel before the task runs: the receiver lags.
        for _ in 0..3 {
            tx.send(Arc::new(Vec::new())).expect("send");
        }
        w.watch_partition(PartitionId(0), rx);
        tokio::time::timeout(Duration::from_secs(5), a.notified())
            .await
            .expect("lagged wakes everyone");
        drop(a);
        // Closed wakes everyone too, and the task exits.
        let mut b = w.register(r);
        drop(tx);
        tokio::time::timeout(Duration::from_secs(5), b.notified())
            .await
            .expect("closed wakes everyone");
    }
}
