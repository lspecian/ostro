//! Dispatcher (DISP-01, DISP-02, DISP-07): the per-partition leader service that turns
//! `Ready` runs into executor jobs, under soft leases.
//!
//! The dispatcher is generic over [`Executor`], so this crate does not depend on the engine.
//! The server's `EngineExecutor` (plan 04-16) implements it.
//!
//! # State (in memory only, rebuilt on leadership)
//!
//! - Ready queues: `queue -> {(Reverse(priority), ready_at, run)}`, plus a delay set for
//!   entries whose `ready_at` is still in the future. Every entry carries the run's
//!   concurrency key (DISP-02; not enforced until Phase 7). Queues are served round-robin.
//! - Leases: `run -> Lease{generation, token, deadline, revoked, inputs, slot permit}`.
//! - Consecutive retryable failures per run (`Job::failures`).
//!
//! # Rebuild, wake-ups, no polling
//!
//! [`spawn`] subscribes to the apply machine's actions **before** scanning, then rebuilds
//! from `query::ready_entries` (every queue) and `query::running_runs` (in-flight runs).
//! After a leadership change every `Running` run holds a token of an older epoch, so it is
//! re-granted and re-dispatched immediately (the old process is gone; no `lease_ttl` wait).
//!
//! The loop then wakes only on: actions (`EnqueueReady`, `InputAppended`, `RunCompleted`),
//! the next deadline (earliest delayed `ready_at` or lease check), a free executor slot, a
//! finished grant/job, and shutdown. There is no interval. A lagged action stream triggers a
//! full rescan ([`DispatcherStats::rescans`]); the startup rebuild is not counted.
//!
//! Delayed `ready_at` values are wall-clock ms from [`LeaderCtx::clock`]; the sleep is
//! computed from them once per loop turn. A [`ManualClock`](crate::clock::ManualClock) that
//! is advanced does not wake the loop by itself (the timer service polls; this does not).
//!
//! # Dispatch
//!
//! Dispatch = take a slot permit, record a lease in phase *Granting*, and run
//! [`PartitionHandle::grant_attempt`] (a logged `AttemptGranted` with `n + 1` in the current
//! epoch) plus a record read in a task, so concurrent grants share log appends. When the
//! grant comes back, the loop first drains the action stream: every action applied before
//! the grant is in the channel by then (the apply thread commits, acks, then fans out, one
//! chunk at a time). An `EnqueueReady` seen while the lease is still *Granting* is therefore
//! stale (it predates the grant) and ignored; one seen while *Executing* means the executor
//! requeued the run itself, so the lease is dropped and the entry queued.
//!
//! # Soft leases and fencing
//!
//! The lease is not logged. Correctness comes from the logged `AttemptGranted` plus apply's
//! token check: once `n + 1` is granted, a zombie's late append carries token `n` and is
//! rejected `Fenced`. The lease only decides when to stop waiting for an executor:
//!
//! - The executor renews through [`LeaseRenewer::renew`] (on every append and on a
//!   heartbeat). A lease not renewed within `lease_ttl` is revoked: its `revoked` signal
//!   turns true, its slot is freed, and the run is re-granted as attempt `n + 1` and
//!   re-dispatched ([`DispatcherStats::lease_expired`]).
//! - An outcome from a job whose lease is gone or was replaced (generation mismatch) is
//!   ignored.
//! - Self-fencing is the executor's job (plan 04-16, Pitfall 14): stop when `revoked` fires
//!   or when its own renewal deadline ([`LeaseRenewer::expires_at`]) passes, because a fence
//!   on the journal does not stop the zombie's other I/O.
//!
//! Slot permits belong to leases, not to executor futures: a revoked zombie does not keep a
//! slot, so `slots` bounds the leased jobs. A future that keeps running after revocation
//! (or after its run was requeued) briefly overlaps with its successor.
//!
//! Shutdown revokes every lease and waits for executor futures for at most
//! `min(lease_ttl, 5 s)`, then aborts the rest.
//!
//! # Hot inputs
//!
//! `InputAppended { run, seq }` for a leased run sends `seq` to the job's `inputs` channel.
//! Inputs for runs without a lease are ignored (the next activation reads the journal).

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ostro_log::Rf1Log;
use ostro_types::{AttemptToken, PartitionId, RunId};
use tokio::sync::broadcast::{
    self,
    error::{RecvError, TryRecvError},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::{Id as TaskId, JoinError, JoinSet};
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use super::LeaderCtx;
use crate::apply::{Action, RejectReason};
use crate::handle::{PartitionError, PartitionHandle};
use crate::query;
use crate::store::values::RunRecord;

/// Default lease TTL.
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);
/// Default number of executor slots of [`DispatcherConfig::new`].
pub const DEFAULT_SLOTS: usize = 64;
/// Upper bound of the shutdown grace period.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// Delay before re-dispatching after a non-fatal grant error (ms).
const GRANT_RETRY_MS: u64 = 100;
/// Backoff after an executor future panicked (ms).
const PANIC_RETRY_MS: u64 = 1_000;

// ------------------------------------------------------------------- public contract

/// One leased activation handed to the [`Executor`].
pub struct Job {
    pub partition: PartitionId,
    pub run: RunId,
    pub token: AttemptToken,
    /// Read right after the grant (re-read on [`ExecOutcome::Reactivate`]).
    pub record: RunRecord,
    pub handle: PartitionHandle<Rf1Log>,
    /// Seqs of inputs journaled while leased (`InputAppended`).
    pub inputs: mpsc::UnboundedReceiver<u64>,
    /// `true` = lease lost (expiry / shutdown / requeue): stop now.
    pub revoked: watch::Receiver<bool>,
    /// `renew() -> bool` (false once revoked).
    pub lease: LeaseRenewer,
    /// Consecutive `Retry` outcomes of this run (in memory).
    pub failures: u32,
}

impl std::fmt::Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("partition", &self.partition)
            .field("run", &self.run)
            .field("token", &self.token)
            .field("failures", &self.failures)
            .finish_non_exhaustive()
    }
}

/// What an executor reports when a job ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecOutcome {
    /// Terminal, suspended or requeued by the executor's own command: drop the lease.
    Released,
    /// Run again now with the SAME token (hot-wait elapsed, append conflict, budget pressure).
    Reactivate,
    /// The attempt failed retryably: grant a NEW attempt after the delay.
    Retry { after_ms: u64 },
    /// A newer attempt owns the run.
    Fenced,
}

/// Runs jobs. Implemented by the server's engine executor (plan 04-16) and by test fakes.
pub trait Executor: Send + Sync + 'static {
    fn execute(&self, job: Job) -> Pin<Box<dyn Future<Output = ExecOutcome> + Send + 'static>>;
}

/// The executor's side of a soft lease.
#[derive(Clone, Debug)]
pub struct LeaseRenewer {
    /// Lease deadline in ms since `base`.
    deadline: Arc<AtomicU64>,
    base: Instant,
    ttl: Duration,
    revoked: watch::Receiver<bool>,
}

impl LeaseRenewer {
    /// Extend the lease to `now + lease_ttl`. Returns `false` once the lease was revoked
    /// (then the caller must stop).
    pub fn renew(&self) -> bool {
        if self.is_revoked() {
            return false;
        }
        let at = ms_since(self.base, Instant::now() + self.ttl);
        self.deadline.fetch_max(at, Ordering::AcqRel);
        !self.is_revoked()
    }

    pub fn is_revoked(&self) -> bool {
        *self.revoked.borrow()
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Current lease deadline (for the executor's self-fence timer).
    pub fn expires_at(&self) -> Instant {
        self.base + Duration::from_millis(self.deadline.load(Ordering::Acquire))
    }
}

/// Dispatcher configuration (one per node, cloned into every partition's dispatcher).
#[derive(Clone)]
pub struct DispatcherConfig {
    pub executor: Arc<dyn Executor>,
    pub lease_ttl: Duration,
    /// Node-wide executor slots (shared by every partition).
    pub slots: Arc<Semaphore>,
    pub stats: Option<Arc<DispatcherStats>>,
}

impl DispatcherConfig {
    /// `lease_ttl` 30 s, 64 slots, no stats.
    pub fn new(executor: Arc<dyn Executor>) -> Self {
        Self {
            executor,
            lease_ttl: DEFAULT_LEASE_TTL,
            slots: Arc::new(Semaphore::new(DEFAULT_SLOTS)),
            stats: None,
        }
    }
}

impl std::fmt::Debug for DispatcherConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DispatcherConfig")
            .field("lease_ttl", &self.lease_ttl)
            .field("slots_available", &self.slots.available_permits())
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

/// Counters for tests and introspection (monotonic).
#[derive(Debug, Default)]
pub struct DispatcherStats {
    /// Jobs handed to the executor after a grant (not counting reactivations).
    pub dispatched: AtomicU64,
    /// Full rescans after a lagged action stream (the startup rebuild is not counted).
    pub rescans: AtomicU64,
    /// Leases revoked because they were not renewed within `lease_ttl`.
    pub lease_expired: AtomicU64,
    /// Jobs re-run with the same token after [`ExecOutcome::Reactivate`].
    pub reactivated: AtomicU64,
}

/// Start the dispatcher of `ctx.partition`. Runs until `ctx.shutdown` turns true or the
/// apply machine goes away.
pub fn spawn(ctx: LeaderCtx, cfg: DispatcherConfig) -> tokio::task::JoinHandle<()> {
    // Subscribe before the rebuild scan so nothing committed after the scan is missed.
    let actions = ctx.handle.machine().subscribe_actions();
    tokio::spawn(async move {
        let d = Dispatcher::new(ctx, cfg);
        d.run(actions).await;
    })
}

// ------------------------------------------------------------------- internal state

fn ms_since(base: Instant, t: Instant) -> u64 {
    u64::try_from(t.saturating_duration_since(base).as_millis()).unwrap_or(u64::MAX)
}

/// A ready (or to-be-regranted) run waiting for a slot.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Entry {
    queue: String,
    priority: i32,
    ready_at_ms: u64,
    concurrency_key: String,
}

impl Entry {
    fn from_record(r: &RunRecord, ready_at_ms: u64) -> Self {
        Self {
            queue: r.queue.clone(),
            priority: r.priority,
            ready_at_ms,
            concurrency_key: r.concurrency_key.clone(),
        }
    }

    fn at(&self, ready_at_ms: u64) -> Self {
        Self {
            ready_at_ms,
            ..self.clone()
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// `AttemptGranted` submitted, outcome not yet seen.
    Granting,
    /// Granted; a job with this token is (or was last) handed to the executor.
    Executing(AttemptToken),
}

struct Lease {
    generation: u64,
    phase: Phase,
    entry: Entry,
    /// Deadline in ms since `State::base` (shared with the job's [`LeaseRenewer`]).
    deadline: Arc<AtomicU64>,
    /// Key of this lease in `State::checks`.
    check_at: u64,
    revoked: watch::Sender<bool>,
    inputs: mpsc::UnboundedSender<u64>,
    /// Receiver for the next job (taken when the job is built).
    pending_inputs: Option<mpsc::UnboundedReceiver<u64>>,
    _permit: OwnedSemaphorePermit,
}

/// Result of a full scan (startup rebuild or lagged rescan).
struct Scan {
    ready: Vec<query::ReadyEntryRow>,
    /// Running runs with their queue entry (None = record missing).
    running: Vec<(RunId, Option<Entry>)>,
}

/// The dispatcher's in-memory state, separated from I/O so the action handling is
/// unit-testable.
struct State {
    queues: BTreeMap<String, BTreeSet<(Reverse<i32>, u64, RunId)>>,
    /// Every queued or delayed entry.
    entries: BTreeMap<RunId, Entry>,
    /// Entries with a future `ready_at`: `(ready_at_ms, run)`.
    delayed: BTreeSet<(u64, RunId)>,
    leases: BTreeMap<RunId, Lease>,
    /// Lease checks `(deadline ms since base, run)`.
    checks: BTreeSet<(u64, RunId)>,
    failures: BTreeMap<RunId, u32>,
    next_generation: u64,
    /// Last queue served (round-robin cursor).
    last_queue: Option<String>,
    base: Instant,
    ttl: Duration,
}

impl State {
    fn new(ttl: Duration) -> Self {
        Self {
            queues: BTreeMap::new(),
            entries: BTreeMap::new(),
            delayed: BTreeSet::new(),
            leases: BTreeMap::new(),
            checks: BTreeSet::new(),
            failures: BTreeMap::new(),
            next_generation: 0,
            last_queue: None,
            base: Instant::now(),
            ttl,
        }
    }

    fn now_rel(&self) -> u64 {
        ms_since(self.base, Instant::now())
    }

    fn ttl_ms(&self) -> u64 {
        u64::try_from(self.ttl.as_millis()).unwrap_or(u64::MAX)
    }

    fn has_ready(&self) -> bool {
        !self.queues.is_empty()
    }

    /// Queue `run` (replacing any previous entry): due entries go to their queue, future
    /// ones to the delay set.
    fn enqueue(&mut self, run: RunId, e: Entry, now_ms: u64) {
        self.unqueue(&run);
        if e.ready_at_ms > now_ms {
            self.delayed.insert((e.ready_at_ms, run));
        } else {
            self.queues.entry(e.queue.clone()).or_default().insert((
                Reverse(e.priority),
                e.ready_at_ms,
                run,
            ));
        }
        self.entries.insert(run, e);
    }

    fn unqueue(&mut self, run: &RunId) -> Option<Entry> {
        let e = self.entries.remove(run)?;
        if !self.delayed.remove(&(e.ready_at_ms, *run))
            && let Some(set) = self.queues.get_mut(&e.queue)
        {
            set.remove(&(Reverse(e.priority), e.ready_at_ms, *run));
            if set.is_empty() {
                self.queues.remove(&e.queue);
            }
        }
        Some(e)
    }

    /// Move delayed entries with `ready_at <= now` into their queues.
    fn promote_due(&mut self, now_ms: u64) {
        while let Some(&(at, run)) = self.delayed.first() {
            if at > now_ms {
                break;
            }
            self.delayed.pop_first();
            if let Some(e) = self.entries.get(&run) {
                self.queues.entry(e.queue.clone()).or_default().insert((
                    Reverse(e.priority),
                    e.ready_at_ms,
                    run,
                ));
            }
        }
    }

    /// Next run to dispatch: queues round-robin, per queue highest priority, then earliest
    /// ready time, then run id.
    fn pop_next(&mut self) -> Option<(RunId, Entry)> {
        let q = match &self.last_queue {
            Some(last) => self
                .queues
                .range::<String, _>((Bound::Excluded(last), Bound::Unbounded))
                .next()
                .or_else(|| self.queues.iter().next()),
            None => self.queues.iter().next(),
        }
        .map(|(k, _)| k.clone())?;
        let set = self.queues.get_mut(&q)?;
        let (_, _, run) = set.pop_first()?;
        if set.is_empty() {
            self.queues.remove(&q);
        }
        self.last_queue = Some(q);
        let e = self.entries.remove(&run)?;
        Some((run, e))
    }

    /// Remove `run`'s lease, signalling `revoked` (harmless when the job already ended).
    fn drop_lease(&mut self, run: &RunId) -> Option<Lease> {
        let l = self.leases.remove(run)?;
        self.checks.remove(&(l.check_at, *run));
        l.revoked.send_replace(true);
        Some(l)
    }

    fn revoke_all(&mut self) {
        let runs: Vec<RunId> = self.leases.keys().copied().collect();
        for r in runs {
            self.drop_lease(&r);
        }
    }

    /// Record a new lease in phase Granting; returns its generation.
    fn new_lease(&mut self, run: RunId, entry: Entry, permit: OwnedSemaphorePermit) -> u64 {
        self.next_generation += 1;
        let generation = self.next_generation;
        let at = self.now_rel().saturating_add(self.ttl_ms());
        let (tx, rx) = mpsc::unbounded_channel();
        let lease = Lease {
            generation,
            phase: Phase::Granting,
            entry,
            deadline: Arc::new(AtomicU64::new(at)),
            check_at: at,
            revoked: watch::channel(false).0,
            inputs: tx,
            pending_inputs: Some(rx),
            _permit: permit,
        };
        self.checks.insert((at, run));
        self.leases.insert(run, lease);
        generation
    }

    /// Reset `run`'s lease deadline to `now + ttl` (job start).
    fn refresh_lease(&mut self, run: &RunId) {
        let at = self.now_rel().saturating_add(self.ttl_ms());
        if let Some(l) = self.leases.get_mut(run) {
            l.deadline.store(at, Ordering::Release);
            self.checks.remove(&(l.check_at, *run));
            l.check_at = at;
            self.checks.insert((at, *run));
        }
    }

    /// Revoke leases whose deadline passed (leases still Granting are not expired; their
    /// grant completes or fails on its own). Returns the expired runs, re-queued for an
    /// immediate re-grant.
    fn expire_leases(&mut self, now_ms: u64) -> Vec<RunId> {
        let now_rel = self.now_rel();
        let mut expired = Vec::new();
        while let Some(&(at, run)) = self.checks.first() {
            if at > now_rel {
                break;
            }
            self.checks.pop_first();
            let ttl = self.ttl_ms();
            let Some(l) = self.leases.get_mut(&run) else {
                continue;
            };
            let deadline = l.deadline.load(Ordering::Acquire);
            let next = if matches!(l.phase, Phase::Granting) {
                Some(now_rel.saturating_add(ttl))
            } else if deadline > now_rel {
                Some(deadline)
            } else {
                None
            };
            match next {
                Some(n) => {
                    l.check_at = n;
                    self.checks.insert((n, run));
                }
                None => {
                    if let Some(l) = self.drop_lease(&run) {
                        // Ready time 0: re-dispatch ahead of same-priority work.
                        self.enqueue(run, l.entry.at(0), now_ms);
                        expired.push(run);
                    }
                }
            }
        }
        expired
    }

    /// Apply a batch of committed actions.
    fn on_actions(&mut self, batch: &[Action], now_ms: u64) {
        for a in batch {
            match a {
                Action::EnqueueReady {
                    run,
                    queue,
                    priority,
                    ready_at_ms,
                    concurrency_key,
                } => {
                    if let Some(l) = self.leases.get(run) {
                        if l.phase == Phase::Granting {
                            // Applied before our grant, which supersedes it.
                            continue;
                        }
                        // The executor requeued the run: its lease ends here.
                        self.drop_lease(run);
                    }
                    let e = Entry {
                        queue: queue.clone(),
                        priority: *priority,
                        ready_at_ms: *ready_at_ms,
                        concurrency_key: concurrency_key.clone(),
                    };
                    self.enqueue(*run, e, now_ms);
                }
                Action::InputAppended { run, seq } => {
                    if let Some(l) = self.leases.get(run) {
                        let _ = l.inputs.send(*seq);
                    }
                }
                Action::RunCompleted { run, .. } => {
                    self.drop_lease(run);
                    self.unqueue(run);
                    self.failures.remove(run);
                }
                _ => {}
            }
        }
    }

    /// Reconcile with a full scan. Leased runs keep their lease, except that a ready row for
    /// a run whose grant was applied means the executor requeued it. Unleased running runs
    /// are queued for an immediate re-grant (pending retries keep their delay).
    fn apply_scan(&mut self, scan: Scan, now_ms: u64) {
        let running: BTreeMap<RunId, Option<Entry>> = scan.running.into_iter().collect();
        // Keep only pending re-grants (retry backoff) of runs that are still Running.
        let old: Vec<(RunId, Entry)> = std::mem::take(&mut self.entries).into_iter().collect();
        self.queues.clear();
        self.delayed.clear();
        for (run, e) in old {
            if running.contains_key(&run) && !self.leases.contains_key(&run) {
                self.enqueue(run, e, now_ms);
            }
        }
        for row in scan.ready {
            if let Some(l) = self.leases.get(&row.run) {
                if l.phase == Phase::Granting {
                    continue;
                }
                self.drop_lease(&row.run);
            }
            let e = Entry {
                queue: row.queue,
                priority: row.priority,
                ready_at_ms: row.ready_at_ms,
                concurrency_key: row.concurrency_key,
            };
            self.enqueue(row.run, e, now_ms);
        }
        for (run, e) in running {
            if self.leases.contains_key(&run) || self.entries.contains_key(&run) {
                continue;
            }
            if let Some(e) = e {
                // Ready time 0: re-dispatch ahead of same-priority work.
                self.enqueue(run, e.at(0), now_ms);
            }
        }
    }
}

// ------------------------------------------------------------------- the service

enum Done {
    Granted {
        run: RunId,
        generation: u64,
        res: Result<(AttemptToken, Box<RunRecord>), PartitionError>,
    },
    Executed {
        run: RunId,
        generation: u64,
        outcome: ExecOutcome,
    },
}

struct Dispatcher {
    ctx: LeaderCtx,
    cfg: DispatcherConfig,
    st: State,
    stats: Arc<DispatcherStats>,
    joins: JoinSet<Done>,
    /// Task id -> (run, generation), to attribute a panicked task.
    tasks: BTreeMap<TaskId, (RunId, u64)>,
    m_dispatched: metrics::Counter,
    m_expired: metrics::Counter,
}

/// Loop control after handling an event.
enum Flow {
    Continue,
    Stop,
}

impl Dispatcher {
    fn new(ctx: LeaderCtx, cfg: DispatcherConfig) -> Self {
        let label = ctx.partition.0.to_string();
        Self {
            st: State::new(cfg.lease_ttl),
            stats: cfg.stats.clone().unwrap_or_default(),
            joins: JoinSet::new(),
            tasks: BTreeMap::new(),
            m_dispatched: metrics::counter!("ostro_dispatcher_dispatched_total", "partition" => label.clone()),
            m_expired: metrics::counter!("ostro_dispatcher_lease_expired_total", "partition" => label),
            ctx,
            cfg,
        }
    }

    fn now_ms(&self) -> u64 {
        self.ctx.clock.now_ms()
    }

    fn p(&self) -> u16 {
        self.ctx.partition.0
    }

    /// Scan `ready` and `running` (blocking reads, off the runtime threads).
    async fn scan(&self) -> Result<Scan, String> {
        let reader = self.ctx.handle.reader();
        tokio::task::spawn_blocking(move || {
            let ready = query::ready_entries(&reader, usize::MAX).map_err(|e| e.to_string())?;
            let running = query::running_runs(&reader)
                .map_err(|e| e.to_string())?
                .into_iter()
                .map(|(run, _)| {
                    let rec = query::run_record(&reader, &run)?;
                    Ok((run, rec.map(|r| Entry::from_record(&r, 0))))
                })
                .collect::<Result<Vec<_>, crate::store::StoreError>>()
                .map_err(|e| e.to_string())?;
            Ok(Scan { ready, running })
        })
        .await
        .map_err(|e| format!("scan task: {e}"))?
    }

    /// Scan and reconcile. `Err` = partition failing (stop).
    async fn rescan(&mut self) -> Result<(), ()> {
        loop {
            match self.scan().await {
                Ok(s) => {
                    let now = self.now_ms();
                    self.st.apply_scan(s, now);
                    return Ok(());
                }
                Err(e) => {
                    if self.ctx.handle.machine().check_healthy().is_err() {
                        error!(partition = self.p(), error = %e, "dispatcher scan failed; partition failing, dispatcher stops");
                        return Err(());
                    }
                    warn!(partition = self.p(), error = %e, "dispatcher scan failed; retrying");
                    let mut sd = self.ctx.shutdown.clone();
                    tokio::select! {
                        _ = sd.wait_for(|v| *v) => return Err(()),
                        () = tokio::time::sleep(Duration::from_millis(GRANT_RETRY_MS)) => {}
                    }
                }
            }
        }
    }

    /// Drain every action already in the channel. `Stop` if the machine is gone.
    async fn drain(&mut self, actions: &mut broadcast::Receiver<Arc<Vec<Action>>>) -> Flow {
        loop {
            match actions.try_recv() {
                Ok(b) => {
                    let now = self.now_ms();
                    self.st.on_actions(&b, now);
                }
                Err(TryRecvError::Lagged(n)) => {
                    if self.lagged(n).await.is_err() {
                        return Flow::Stop;
                    }
                }
                Err(TryRecvError::Empty) => return Flow::Continue,
                Err(TryRecvError::Closed) => return Flow::Stop,
            }
        }
    }

    async fn lagged(&mut self, skipped: u64) -> Result<(), ()> {
        debug!(
            partition = self.p(),
            skipped, "dispatcher actions lagged; rescanning"
        );
        self.stats.rescans.fetch_add(1, Ordering::Relaxed);
        self.rescan().await
    }

    /// Start grants while there is ready work and a free slot.
    fn dispatch_available(&mut self) {
        while self.st.has_ready() {
            let Ok(permit) = Arc::clone(&self.cfg.slots).try_acquire_owned() else {
                return;
            };
            self.dispatch(permit);
        }
    }

    fn dispatch(&mut self, permit: OwnedSemaphorePermit) {
        let Some((run, entry)) = self.st.pop_next() else {
            return; // the permit drops
        };
        let generation = self.st.new_lease(run, entry, permit);
        let h = self.ctx.handle.clone();
        let id = self
            .joins
            .spawn(async move {
                let res = async {
                    let token = h.grant_attempt(run).await?;
                    let rec = h
                        .read_run(&run)?
                        .ok_or(PartitionError::Rejected(RejectReason::RunNotFound))?;
                    Ok((token, Box::new(rec)))
                }
                .await;
                Done::Granted {
                    run,
                    generation,
                    res,
                }
            })
            .id();
        self.tasks.insert(id, (run, generation));
    }

    /// Hand a job for `run` (lease in phase Executing) to the executor.
    fn start_job(&mut self, run: RunId, record: RunRecord) {
        self.st.refresh_lease(&run);
        let failures = self.st.failures.get(&run).copied().unwrap_or(0);
        let base = self.st.base;
        let ttl = self.st.ttl;
        let Some(l) = self.st.leases.get_mut(&run) else {
            return;
        };
        let Phase::Executing(token) = l.phase else {
            return;
        };
        let inputs = match l.pending_inputs.take() {
            Some(rx) => rx,
            None => {
                let (tx, rx) = mpsc::unbounded_channel();
                l.inputs = tx;
                rx
            }
        };
        let revoked = l.revoked.subscribe();
        let job = Job {
            partition: self.ctx.partition,
            run,
            token,
            record,
            handle: self.ctx.handle.clone(),
            inputs,
            revoked: revoked.clone(),
            lease: LeaseRenewer {
                deadline: Arc::clone(&l.deadline),
                base,
                ttl,
                revoked,
            },
            failures,
        };
        let generation = l.generation;
        let fut = self.cfg.executor.execute(job);
        let id = self
            .joins
            .spawn(async move {
                let outcome = fut.await;
                Done::Executed {
                    run,
                    generation,
                    outcome,
                }
            })
            .id();
        self.tasks.insert(id, (run, generation));
    }

    /// Is `run`'s current lease the one of `generation`?
    fn current(&self, run: &RunId, generation: u64) -> bool {
        self.st
            .leases
            .get(run)
            .is_some_and(|l| l.generation == generation)
    }

    fn on_done(&mut self, d: Done) -> Flow {
        match d {
            Done::Granted {
                run,
                generation,
                res,
            } => self.on_granted(run, generation, res),
            Done::Executed {
                run,
                generation,
                outcome,
            } => {
                self.on_executed(run, generation, outcome);
                Flow::Continue
            }
        }
    }

    fn on_granted(
        &mut self,
        run: RunId,
        generation: u64,
        res: Result<(AttemptToken, Box<RunRecord>), PartitionError>,
    ) -> Flow {
        if !self.current(&run, generation) {
            debug!(partition = self.p(), %run, "grant result for a dropped lease ignored");
            return Flow::Continue;
        }
        match res {
            Ok((token, record)) => {
                if let Some(l) = self.st.leases.get_mut(&run) {
                    l.phase = Phase::Executing(token);
                }
                self.stats.dispatched.fetch_add(1, Ordering::Relaxed);
                self.m_dispatched.increment(1);
                debug!(partition = self.p(), %run, epoch = token.epoch.0, n = token.n, "dispatching");
                self.start_job(run, *record);
                Flow::Continue
            }
            Err(PartitionError::Rejected(r)) => {
                // RunTerminal / RunNotFound: nothing to run. Fenced / StaleEpoch: another
                // grant or term owns the run.
                debug!(partition = self.p(), %run, reason = ?r, "grant rejected; entry dropped");
                self.st.drop_lease(&run);
                Flow::Continue
            }
            Err(e) => {
                if self.ctx.handle.machine().check_healthy().is_err() {
                    error!(partition = self.p(), %run, error = %e, "grant failed; partition failing, dispatcher stops");
                    return Flow::Stop;
                }
                warn!(partition = self.p(), %run, error = %e, "grant failed; retrying");
                if let Some(l) = self.st.drop_lease(&run) {
                    let now = self.now_ms();
                    let at = now.saturating_add(GRANT_RETRY_MS);
                    self.st.enqueue(run, l.entry.at(at), now);
                }
                Flow::Continue
            }
        }
    }

    fn on_executed(&mut self, run: RunId, generation: u64, outcome: ExecOutcome) {
        if !self.current(&run, generation) {
            debug!(partition = self.p(), %run, ?outcome, "stale job outcome ignored");
            return;
        }
        match outcome {
            ExecOutcome::Released => {
                self.st.drop_lease(&run);
                self.st.failures.remove(&run);
            }
            ExecOutcome::Fenced => {
                self.st.drop_lease(&run);
            }
            ExecOutcome::Retry { after_ms } => {
                if let Some(l) = self.st.drop_lease(&run) {
                    *self.st.failures.entry(run).or_default() += 1;
                    let now = self.now_ms();
                    self.st
                        .enqueue(run, l.entry.at(now.saturating_add(after_ms)), now);
                }
            }
            ExecOutcome::Reactivate => match self.ctx.handle.read_run(&run) {
                Ok(Some(rec)) if !rec.status.is_terminal() => {
                    self.stats.reactivated.fetch_add(1, Ordering::Relaxed);
                    self.start_job(run, rec);
                }
                Ok(_) => {
                    self.st.drop_lease(&run);
                }
                Err(e) => {
                    warn!(partition = self.p(), %run, error = %e, "reactivate: run read failed; re-granting");
                    if let Some(l) = self.st.drop_lease(&run) {
                        let now = self.now_ms();
                        self.st
                            .enqueue(run, l.entry.at(now.saturating_add(GRANT_RETRY_MS)), now);
                    }
                }
            },
        }
    }

    fn on_join_error(&mut self, e: &JoinError) {
        let Some((run, generation)) = self.tasks.remove(&e.id()) else {
            return;
        };
        if e.is_cancelled() || !self.current(&run, generation) {
            return;
        }
        error!(partition = self.p(), %run, error = %e, "dispatcher task panicked; retrying the run");
        if let Some(l) = self.st.drop_lease(&run) {
            let now = self.now_ms();
            self.st
                .enqueue(run, l.entry.at(now.saturating_add(PANIC_RETRY_MS)), now);
        }
    }

    fn on_expired(&mut self) {
        let now = self.now_ms();
        for run in self.st.expire_leases(now) {
            self.stats.lease_expired.fetch_add(1, Ordering::Relaxed);
            self.m_expired.increment(1);
            warn!(
                partition = self.p(),
                %run,
                "lease not renewed within lease_ttl; revoked, re-granting"
            );
        }
    }

    /// Earliest wake-up: next delayed ready time or next lease check.
    fn next_deadline(&self) -> Option<Instant> {
        let ready = self.st.delayed.first().map(|&(at, _)| {
            Instant::now() + Duration::from_millis(at.saturating_sub(self.now_ms()))
        });
        let lease = self
            .st
            .checks
            .first()
            .map(|&(at, _)| self.st.base + Duration::from_millis(at));
        match (ready, lease) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    async fn run(mut self, mut actions: broadcast::Receiver<Arc<Vec<Action>>>) {
        let mut shutdown = self.ctx.shutdown.clone();
        if self.rescan().await.is_err() {
            return;
        }
        info!(
            partition = self.p(),
            queued = self.st.entries.len(),
            lease_ttl_ms = self.st.ttl_ms(),
            "dispatcher started"
        );
        loop {
            if *shutdown.borrow() {
                break;
            }
            let now = self.now_ms();
            self.on_expired();
            self.st.promote_due(now);
            self.dispatch_available();

            let deadline = self.next_deadline();
            let want_slot = self.st.has_ready();
            let slots = Arc::clone(&self.cfg.slots);
            let flow = tokio::select! {
                biased;
                r = shutdown.changed() => match r {
                    Ok(()) => Flow::Continue, // checked at the top of the loop
                    Err(_) => Flow::Stop,
                },
                r = actions.recv() => match r {
                    Ok(b) => {
                        let now = self.now_ms();
                        self.st.on_actions(&b, now);
                        Flow::Continue
                    }
                    Err(RecvError::Lagged(n)) => match self.lagged(n).await {
                        Ok(()) => Flow::Continue,
                        Err(()) => Flow::Stop,
                    },
                    Err(RecvError::Closed) => {
                        info!(partition = self.p(), "apply machine gone; dispatcher stops");
                        Flow::Stop
                    }
                },
                Some(j) = self.joins.join_next_with_id() => {
                    // Every action applied before this grant/outcome is in the channel:
                    // handle those first (see module docs).
                    match self.drain(&mut actions).await {
                        Flow::Stop => Flow::Stop,
                        Flow::Continue => match j {
                            Ok((id, d)) => {
                                self.tasks.remove(&id);
                                self.on_done(d)
                            }
                            Err(e) => {
                                self.on_join_error(&e);
                                Flow::Continue
                            }
                        },
                    }
                },
                p = slots.acquire_owned(), if want_slot => {
                    if let Ok(p) = p {
                        self.dispatch(p);
                    }
                    Flow::Continue
                },
                () = sleep_until_opt(deadline), if deadline.is_some() => Flow::Continue,
            };
            if matches!(flow, Flow::Stop) {
                break;
            }
        }
        self.stop().await;
    }

    /// Revoke every lease, then give executor futures a bounded grace period.
    async fn stop(mut self) {
        self.st.revoke_all();
        let grace = self.cfg.lease_ttl.min(SHUTDOWN_GRACE);
        let drained = tokio::time::timeout(grace, async {
            while self.joins.join_next().await.is_some() {}
        })
        .await;
        if drained.is_err() {
            warn!(
                partition = self.p(),
                left = self.joins.len(),
                "executor jobs still running after the shutdown grace; aborting"
            );
            self.joins.shutdown().await;
        }
        info!(partition = self.p(), "dispatcher stopped");
    }
}

async fn sleep_until_opt(at: Option<Instant>) {
    match at {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run() -> RunId {
        RunId::new_v7(PartitionId(0))
    }

    fn permit() -> OwnedSemaphorePermit {
        Arc::new(Semaphore::new(1))
            .try_acquire_owned()
            .expect("permit")
    }

    fn entry(q: &str, priority: i32, at: u64, ck: &str) -> Entry {
        Entry {
            queue: q.into(),
            priority,
            ready_at_ms: at,
            concurrency_key: ck.into(),
        }
    }

    fn enqueue_action(run: RunId, q: &str, priority: i32, at: u64, ck: &str) -> Action {
        Action::EnqueueReady {
            run,
            queue: q.into(),
            priority,
            ready_at_ms: at,
            concurrency_key: ck.into(),
        }
    }

    /// A lease in phase Executing; returns the job's inputs receiver.
    fn leased(st: &mut State, r: RunId) -> mpsc::UnboundedReceiver<u64> {
        st.new_lease(r, entry("q", 0, 0, ""), permit());
        let l = st.leases.get_mut(&r).expect("lease");
        l.phase = Phase::Executing(AttemptToken {
            epoch: ostro_types::Epoch(1),
            n: 1,
        });
        l.pending_inputs.take().expect("rx")
    }

    #[tokio::test]
    async fn input_appended_is_forwarded_to_the_leased_job_only() {
        let mut st = State::new(Duration::from_secs(30));
        let (a, b) = (run(), run());
        let mut rx = leased(&mut st, a);
        st.on_actions(
            &[
                Action::InputAppended { run: a, seq: 4 },
                Action::InputAppended { run: b, seq: 9 },
                Action::InputAppended { run: a, seq: 5 },
            ],
            0,
        );
        assert_eq!(rx.try_recv().ok(), Some(4));
        assert_eq!(rx.try_recv().ok(), Some(5));
        assert!(rx.try_recv().is_err());
        assert!(st.entries.is_empty());
    }

    #[tokio::test]
    async fn enqueue_ready_order_delay_and_concurrency_key() {
        let mut st = State::new(Duration::from_secs(30));
        let (a, b, c, d) = (run(), run(), run(), run());
        st.on_actions(
            &[
                enqueue_action(a, "q", 0, 10, "k-a"),
                enqueue_action(b, "q", 5, 20, "k-b"),
                enqueue_action(c, "q", 0, 5, ""),
                enqueue_action(d, "q", 9, 500, "k-d"),
            ],
            100,
        );
        assert_eq!(st.delayed.len(), 1, "d is in the future");
        assert_eq!(st.entries[&a].concurrency_key, "k-a");
        let order: Vec<RunId> = std::iter::from_fn(|| st.pop_next().map(|(r, _)| r)).collect();
        assert_eq!(order, vec![b, c, a]);
        st.promote_due(500);
        let (r, e) = st.pop_next().expect("d due");
        assert_eq!((r, e.concurrency_key.as_str()), (d, "k-d"));
    }

    #[tokio::test]
    async fn enqueue_ready_while_granting_is_stale_while_executing_requeues() {
        let mut st = State::new(Duration::from_secs(30));
        let (a, b) = (run(), run());
        st.new_lease(a, entry("q", 0, 0, ""), permit());
        st.on_actions(&[enqueue_action(a, "q", 0, 1, "")], 10);
        assert!(st.leases.contains_key(&a), "granting lease kept");
        assert!(st.entries.is_empty());

        let _rx = leased(&mut st, b);
        let revoked = st.leases[&b].revoked.subscribe();
        st.on_actions(&[enqueue_action(b, "q", 0, 1, "ck")], 10);
        assert!(!st.leases.contains_key(&b));
        assert!(*revoked.borrow());
        assert_eq!(st.entries[&b].concurrency_key, "ck");
    }

    #[tokio::test]
    async fn run_completed_drops_lease_entry_and_failures() {
        let mut st = State::new(Duration::from_secs(30));
        let (a, b) = (run(), run());
        let _rx = leased(&mut st, a);
        st.failures.insert(a, 2);
        st.enqueue(b, entry("q", 0, 0, ""), 0);
        st.on_actions(
            &[
                Action::RunCompleted {
                    run: a,
                    status: crate::store::values::RunStatus::Completed,
                },
                Action::RunCompleted {
                    run: b,
                    status: crate::store::values::RunStatus::Completed,
                },
            ],
            0,
        );
        assert!(st.leases.is_empty() && st.entries.is_empty() && st.failures.is_empty());
        assert!(!st.has_ready());
    }

    #[tokio::test]
    async fn scan_carries_concurrency_key_and_regrants_unleased_running() {
        let mut st = State::new(Duration::from_secs(30));
        let (a, b) = (run(), run());
        st.apply_scan(
            Scan {
                ready: vec![query::ReadyEntryRow {
                    queue: "q".into(),
                    priority: 1,
                    ready_at_ms: 7,
                    run: a,
                    concurrency_key: "ck-ready".into(),
                }],
                running: vec![(b, Some(entry("q2", 3, 55, "ck-run")))],
            },
            100,
        );
        assert_eq!(st.entries[&a].concurrency_key, "ck-ready");
        assert_eq!(st.entries[&b], entry("q2", 3, 0, "ck-run"));
    }

    #[tokio::test]
    async fn round_robin_across_queues() {
        let mut st = State::new(Duration::from_secs(30));
        let runs: Vec<RunId> = (0..4).map(|_| run()).collect();
        st.enqueue(runs[0], entry("a", 0, 1, ""), 10);
        st.enqueue(runs[1], entry("a", 0, 2, ""), 10);
        st.enqueue(runs[2], entry("b", 0, 1, ""), 10);
        st.enqueue(runs[3], entry("b", 0, 2, ""), 10);
        let order: Vec<RunId> = std::iter::from_fn(|| st.pop_next().map(|(r, _)| r)).collect();
        assert_eq!(order, vec![runs[0], runs[2], runs[1], runs[3]]);
    }
}
