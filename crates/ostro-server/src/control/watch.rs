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
//! A partition's apply machine, and with it the action broadcast sender, lives until the
//! partition restarts (a snapshot install or a failed-partition restart replaces the
//! machine, 12-16). Each partition's forwarding task therefore follows the node's
//! **current** machine ([`next_actions`]): when the old broadcast closes it wakes every
//! waiter of the partition and subscribes to the new machine. Actions are emitted whenever
//! the partition leads (catch-up discards them, but nothing can wait on a partition that is
//! not serving).
//!
//! # Per-term subscriptions (13-13, the 05-13 hand-off)
//!
//! The task also watches the node's led set (`PartitionRoles::led()`): on every new term of
//! the partition here (newly led, or a new epoch) it re-subscribes to the current machine
//! (the new receiver is taken before the old one is drained and dropped, so nothing in
//! between is lost). When the partition stops being led here (demotion, a graceful
//! handoff, a failover elsewhere) its waiters are woken with [`WaitOutcome::LeaderChanged`]
//! and the old term's subscription is dropped; [`await_result`] then answers `unavailable`
//! reason `leader_changed`, which the routed control plane (13-12) re-routes once to the new
//! leader with the call's remaining deadline. A new epoch on the same node wakes waiters
//! with [`WaitOutcome::Retry`] (they re-read here). `Lagged` / `Closed` still wake the whole
//! partition with `Retry`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use ostro_api::ApiError;
use ostro_api::domain::{AwaitResultRequest, AwaitResultResponse, RunStatus, effective_wait_ms};
use ostro_partition::Action;
use ostro_partition::query;
use ostro_partition::store::values::{self, RunRecord};
use ostro_types::{Epoch, PartitionId, RunId};
use tokio::sync::{broadcast, oneshot, watch};
use tokio::task::JoinHandle;

use super::ServerControlPlane;
use super::runs::{not_found, parse_run_id, partition_error, status, store_error, task_error};

/// Poll interval while a followed continuation's successor is not created yet.
const SUCCESSOR_POLL: Duration = Duration::from_millis(10);

/// Why a [`Waiter`] was woken.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitOutcome {
    /// The run completed (`Action::RunCompleted`).
    Completed,
    /// The partition's waiters were woken without knowing which run completed (`Lagged`,
    /// `Closed`, a new term here, [`RunWatchers::wake_partition`]): re-read.
    Retry,
    /// This node stopped leading the partition: re-route to the new leader.
    LeaderChanged,
}

type Waiters = BTreeMap<RunId, Vec<(u64, oneshot::Sender<WaitOutcome>)>>;

/// The led set of a node (`PartitionRoles::led()`).
pub type LedRx = watch::Receiver<Arc<Vec<(PartitionId, Epoch)>>>;

/// What [`ActionSource::subscribe`] found.
pub enum Subscription {
    /// A receiver of the partition's current apply machine.
    Rx(broadcast::Receiver<Arc<Vec<Action>>>),
    /// The partition is not running now (quiesced, restarting): ask again later.
    NotRunning,
    /// The node is gone: stop.
    Gone,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Subscription::Rx(_) => "Rx",
            Subscription::NotRunning => "NotRunning",
            Subscription::Gone => "Gone",
        })
    }
}

/// Where a partition's action broadcasts come from (the node; tests plug in fakes).
pub trait ActionSource: Send + Sync + 'static {
    /// Subscribe to `p`'s current apply machine.
    fn subscribe(&self, p: PartitionId) -> Subscription;
}

/// [`ActionSource`] over a node, held weakly (the handle used to subscribe is dropped at
/// once, so a receiver never pins the partition's store, 12-16).
struct NodeActions(std::sync::Weak<ostro_partition::Node>);

impl ActionSource for NodeActions {
    fn subscribe(&self, p: PartitionId) -> Subscription {
        let Some(node) = self.0.upgrade() else {
            return Subscription::Gone;
        };
        match node.handle(p) {
            Some(h) => Subscription::Rx(h.machine().subscribe_actions()),
            None => Subscription::NotRunning,
        }
    }
}

fn term_of(led: &[(PartitionId, Epoch)], p: PartitionId) -> Option<Epoch> {
    led.iter().find(|(q, _)| *q == p).map(|(_, e)| *e)
}

struct Inner {
    parts: Vec<Mutex<Waiters>>,
    next_id: AtomicU64,
    /// Result long-polls in progress (plan 10-13, [`RunWatchers::pending_waiters`]).
    pending: AtomicUsize,
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
                let _ = tx.send(WaitOutcome::Completed);
            }
        }
    }

    fn wake_all(&self, p: PartitionId) {
        self.wake_all_with(p, WaitOutcome::Retry);
    }

    fn wake_all_with(&self, p: PartitionId, why: WaitOutcome) {
        let all = std::mem::take(&mut *self.part(p));
        for (_, tx) in all.into_values().flatten() {
            let _ = tx.send(why);
        }
    }

    fn complete(&self, batch: &[Action]) {
        for a in batch {
            if let Action::RunCompleted { run, .. } = a {
                self.wake_run(run);
            }
        }
    }

    /// Hand every batch still queued in `rx` to the waiters (before it is dropped).
    fn drain(&self, p: PartitionId, rx: &mut broadcast::Receiver<Arc<Vec<Action>>>) {
        loop {
            match rx.try_recv() {
                Ok(batch) => self.complete(&batch),
                Err(broadcast::error::TryRecvError::Lagged(_)) => self.wake_all(p),
                Err(_) => break,
            }
        }
    }
}

/// A led-set change for partition `p`: a new term here re-subscribes (new receiver first,
/// then the old one drained and dropped); losing the partition wakes its waiters with
/// [`WaitOutcome::LeaderChanged`]; a new epoch here wakes them with `Retry`.
fn on_led(
    inner: &Inner,
    p: PartitionId,
    term: &mut Option<Epoch>,
    led: &mut Option<LedRx>,
    actions: &mut Option<broadcast::Receiver<Arc<Vec<Action>>>>,
    source: &dyn ActionSource,
) {
    let Some(rx) = led.as_mut() else { return };
    let now = term_of(&rx.borrow_and_update(), p);
    if now == *term {
        return;
    }
    let fresh = match source.subscribe(p) {
        Subscription::Rx(r) => Some(r),
        _ => None,
    };
    if let Some(mut old) = actions.take() {
        inner.drain(p, &mut old);
    }
    *actions = fresh;
    match (*term, now) {
        (Some(_), None) => inner.wake_all_with(p, WaitOutcome::LeaderChanged),
        (Some(_), Some(_)) => inner.wake_all_with(p, WaitOutcome::Retry),
        (None, _) => {}
    }
    *term = now;
}

/// Resolves when `led` changes (`true`) or its sender is gone (`false`); never without one.
async fn led_changed(led: &mut Option<LedRx>) -> bool {
    match led {
        Some(rx) => rx.changed().await.is_ok(),
        None => std::future::pending().await,
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
                pending: AtomicUsize::new(0),
            }),
            tasks: Mutex::new(Vec::new()),
        }
    }

    /// Forward partition `p`'s action broadcasts to the waiters for as long as `node` lives,
    /// re-subscribing whenever the partition's apply machine is replaced and on every new
    /// term of the partition here, and waking its waiters with
    /// [`WaitOutcome::LeaderChanged`] when the node stops leading it (one task per
    /// partition; the node's handles are never held between subscriptions).
    pub fn follow_partition(&self, node: std::sync::Weak<ostro_partition::Node>, p: PartitionId) {
        let led = node
            .upgrade()
            .map(|n| ostro_partition::roles::PartitionRoles::led(&*n));
        self.follow(p, led, Arc::new(NodeActions(node)));
    }

    /// [`RunWatchers::follow_partition`] over any led set and action source (`led = None`:
    /// no leadership tracking, the Phase 12 behaviour).
    pub fn follow(&self, p: PartitionId, led: Option<LedRx>, source: Arc<dyn ActionSource>) {
        let inner = Arc::clone(&self.inner);
        let mut led = led;
        let mut term = led
            .as_mut()
            .and_then(|r| term_of(&r.borrow_and_update(), p));
        // The first subscription is taken now, so nothing applied after this call is missed.
        let first = match source.subscribe(p) {
            Subscription::Rx(rx) => Some(rx),
            _ => None,
        };
        let task = tokio::spawn(async move {
            let mut actions = first;
            loop {
                if actions.is_none() {
                    match source.subscribe(p) {
                        Subscription::Rx(rx) => actions = Some(rx),
                        Subscription::Gone => break,
                        Subscription::NotRunning => {
                            tokio::select! {
                                () = tokio::time::sleep(RESUBSCRIBE_POLL) => {}
                                alive = led_changed(&mut led) => {
                                    if alive {
                                        on_led(&inner, p, &mut term, &mut led, &mut actions, &*source);
                                    } else {
                                        led = None;
                                    }
                                }
                            }
                            continue;
                        }
                    }
                }
                let Some(rx) = actions.as_mut() else { continue };
                tokio::select! {
                    r = rx.recv() => match r {
                        Ok(batch) => inner.complete(&batch),
                        Err(broadcast::error::RecvError::Lagged(_)) => inner.wake_all(p),
                        Err(broadcast::error::RecvError::Closed) => {
                            inner.wake_all(p);
                            actions = None;
                        }
                    },
                    alive = led_changed(&mut led) => {
                        if alive {
                            on_led(&inner, p, &mut term, &mut led, &mut actions, &*source);
                        } else {
                            led = None;
                        }
                    }
                }
            }
        });
        self.tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(task);
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
                    Ok(batch) => inner.complete(&batch),
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
        // (A waiter woken by a dropped sender, e.g. at shutdown, reads `Retry`.)
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

    /// Result long-polls in progress: [`await_result`] calls that have not answered yet
    /// (plan 10-13, the dev clock's auto-skip trigger). One atomic load.
    pub fn pending_waiters(&self) -> usize {
        self.inner.pending.load(Ordering::SeqCst)
    }

    /// Count one long-poll in [`Self::pending_waiters`] until the guard drops (also when
    /// the request future is dropped, e.g. on client disconnect).
    pub fn pending_guard(&self) -> PendingWaiter {
        self.inner.pending.fetch_add(1, Ordering::SeqCst);
        PendingWaiter(Arc::clone(&self.inner))
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

/// One long-poll counted in [`RunWatchers::pending_waiters`] (RAII).
pub struct PendingWaiter(Arc<Inner>);

impl std::fmt::Debug for PendingWaiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingWaiter").finish_non_exhaustive()
    }
}

impl Drop for PendingWaiter {
    fn drop(&mut self) {
        self.0.pending.fetch_sub(1, Ordering::SeqCst);
    }
}

/// One registered waiter. Dropping it removes the registration.
pub struct Waiter {
    inner: Arc<Inner>,
    run: RunId,
    id: u64,
    rx: oneshot::Receiver<WaitOutcome>,
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
    /// Resolves when the run completes (or the partition's waiters are woken), with why.
    pub async fn notified(&mut self) -> WaitOutcome {
        (&mut self.rx).await.unwrap_or(WaitOutcome::Retry)
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
    let handle = &cp.partition(run.partition())?;
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

/// Wait until the metadata names a leader of `p` other than this node (at most
/// 2 × `session_timeout`, and never past `deadline`).
async fn wait_successor(cp: &ServerControlPlane, p: PartitionId, deadline: tokio::time::Instant) {
    let me = cp.cfg.node.node_id;
    // clock-exempt: bounded wait inside a long-poll deadline.
    let until =
        (tokio::time::Instant::now() + cp.cfg.replication.session_timeout.get() * 2).min(deadline);
    loop {
        let leader = cp
            .meta
            .view()
            .read(|s| s.partition(p.0).map(|a| a.value.leader))
            .unwrap_or(0);
        if leader != 0 && leader != me {
            return;
        }
        // clock-exempt: bounded wait inside a long-poll deadline.
        if tokio::time::Instant::now() >= until {
            return;
        }
        tokio::time::sleep(SUCCESSOR_POLL).await;
    }
}

/// `unavailable` reason `leader_changed` for a long-poll whose partition moved away.
fn leader_changed(run: RunId) -> ApiError {
    ApiError::unavailable(format!(
        "partition {} moved to another leader while waiting for run {run}",
        run.partition().0
    ))
    .with_details(serde_json::json!({
        "reason": "leader_changed",
        "partition": run.partition().0,
    }))
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
    // A waiting client (10-13): with `wait = 0` the call answers at once and is no waiter.
    let _pending = (!wait.is_zero()).then(|| cp.watchers().pending_guard());
    let deadline = tokio::time::Instant::now() + wait; // clock-exempt: long-poll deadline
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
                let now = tokio::time::Instant::now(); // clock-exempt: long-poll deadline
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
        let now = tokio::time::Instant::now(); // clock-exempt: long-poll deadline
        if now >= deadline {
            return Ok(response(run, &rec, None));
        }
        let outcome = tokio::time::timeout_at(deadline, waiter.notified())
            .await
            .ok();
        drop(waiter);
        if outcome == Some(WaitOutcome::LeaderChanged) {
            // 13-13: this node no longer leads the run's partition; the routed control
            // plane re-routes once to the new leader with the remaining deadline. Answer
            // once the metadata names a successor (bounded), so that one re-route finds it
            // instead of the leader-0 window of a resignation.
            wait_successor(cp, run.partition(), deadline).await;
            return Err(leader_changed(run));
        }
        let woken = outcome.is_some();
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

/// How often [`next_actions`] looks for a partition that is not running (quiesced for a
/// snapshot install, restarting).
const RESUBSCRIBE_POLL: Duration = Duration::from_millis(100);

/// A subscription to partition `p`'s **current** apply machine on `node`, waiting while the
/// partition is not running; `None` once the node is gone or `stop` fired. The handle used to
/// subscribe is dropped before returning, so a caller holding only the receiver never pins
/// the partition's state store (a snapshot install must be able to close it, 12-16); the
/// receiver reports `Closed` once that machine is gone, and the caller subscribes again.
pub(crate) async fn next_actions(
    node: &std::sync::Weak<ostro_partition::Node>,
    p: PartitionId,
    stop: Option<&tokio_util::sync::CancellationToken>,
) -> Option<broadcast::Receiver<Arc<Vec<Action>>>> {
    loop {
        if stop.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
            return None;
        }
        let rx = {
            let node = node.upgrade()?;
            node.handle(p).map(|h| h.machine().subscribe_actions())
        };
        if let Some(rx) = rx {
            return Some(rx);
        }
        match stop {
            Some(stop) => {
                tokio::select! {
                    () = stop.cancelled() => return None,
                    () = tokio::time::sleep(RESUBSCRIBE_POLL) => {}
                }
            }
            None => tokio::time::sleep(RESUBSCRIBE_POLL).await,
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
        // Pending long-polls (10-13) are counted by guard, not by registration.
        assert_eq!(w.pending_waiters(), 0);
        let g1 = w.pending_guard();
        let g2 = w.pending_guard();
        assert_eq!(w.pending_waiters(), 2);
        drop(g1);
        assert_eq!(w.pending_waiters(), 1);
        drop(g2);
        assert_eq!(w.pending_waiters(), 0);
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
