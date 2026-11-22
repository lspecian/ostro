//! Dispatcher (DISP-01, DISP-02, DISP-07): the per-partition leader service that turns
//! `Ready` runs into executor jobs, under soft leases.
//!
//! The dispatcher is generic over [`Executor`], so this crate does not depend on the engine.
//! The server's `EngineExecutor` (plan 04-16) implements it.
//!
//! # State (in memory only, rebuilt on leadership)
//!
//! - Ready entries in [`FairQueues`](super::fair::FairQueues) (plan 07-05, D-W10: weighted
//!   DRR across queues, strict priority bands, round robin across concurrency keys), plus a
//!   delay set for entries whose `ready_at` is still in the future. Every entry carries the
//!   run's concurrency key (DISP-02), workflow, tenant and first-activation flag (Phase 7
//!   F3/F7), which admission and placement see as an [`EntryView`].
//! - Leases: `run -> Lease{generation, token, deadline, revoked, inputs, flow permit,
//!   reservation}`.
//!
//! Retryable attempt failures are counted durably, not here (F9, plan 05-11): on
//! [`ExecOutcome::Retry`] the dispatcher keeps the lease (phase *Granting*), grants the next
//! attempt **at once** with `AttemptGranted{after_failure: true}` (apply increments
//! `RunRecord.attempt_failures` in the same batch), and starts the job only after the retry
//! delay. The count is therefore on disk the moment the failure is known, so a restart
//! during the backoff cannot lose it; [`Job::failures`] is the record's
//! `attempt_failures`. The slot stays taken during the backoff (bounded by
//! [`ExecOutcome::Retry`]'s delay); a hard stop or shutdown during it revokes the lease.
//!
//! **No capacity (test hook, 05-11):** a [`DispatcherConfig::slots`] semaphore with 0
//! permits (`executor.slots = 0`) dispatches nothing: every run stays `Ready` and
//! un-granted (schedule-to-start tests); `add_permits` later releases them.
//!
//! # Admission, placement and fair selection (plan 07-05)
//!
//! A dispatch round asks [`FairQueues::next_candidate`](super::fair::FairQueues) for the
//! next due entry, then:
//!
//! 1. [`FlowControl::admit`] (`now_ms` from the injected clock): `Yes(permit)` continues;
//!    a **throttle** block (`QueueBlocked{Some(until)}` / `KeyBlocked{Some(until)}` on a
//!    first activation; throttles apply only to first activations, D-W13) gates only the
//!    first activations of that queue (key) (`FairQueues::gate_first` /
//!    `gate_first_key`) until `until` or the next [`FlowControl::changed`] bump, and the
//!    queue's re-activations are still granted in the same round (plan 07-23: a skipped
//!    queue made a re-activation behind a throttled first activation wait for `until`);
//!    any other `QueueBlocked{until}` (concurrency) and `Paused{Queue}` skip the queue for
//!    the rest of the round (`until` joins the loop's deadline); any other
//!    `KeyBlocked{until}` parks the key (other keys and queues go on) until `until` or the
//!    next [`FlowControl::changed`] bump;
//!    `Paused{Workflow}` holds back that entry alone (`FairQueues::hold`) until the next
//!    [`FlowControl::changed`] bump, and the rest of the queue (other workflows, even on
//!    the same concurrency key) is still served in the same round. A held entry is not
//!    probed again until that bump, so a queue whose every entry is paused costs nothing
//!    per loop turn and never spins. (If the flow control's `changed()` channel is
//!    closed, held entries are released at every round instead.)
//! 2. [`Placement::reserve`]: `Granted(r)` commits the flow permit (W2: the charge becomes
//!    final only now), takes the entry and starts the grant with the permit and the
//!    reservation in the lease; `Blocked` drops (rolls back) the permit and parks the queue
//!    until [`Placement::changed`] moves, and the round **goes on with the other queues**
//!    (B2: a worker-only queue without credit does not starve an embedded queue).
//!
//! The round ends when `next_candidate` returns `None`. Both happen before
//! `AttemptGranted`, so a blocked entry never burns an attempt number. `first_activation`
//! comes only from F7 (the ready row on rebuild, `Action::EnqueueReady` live); re-grants
//! after a lease expiry or [`ExecOutcome::Abandoned`] and rebuilt `Running` runs are
//! `false`, so first-activation throttles never hold a re-activation back (B1).
//!
//! Defaults: `placement: None` is a [`SemaphorePlacement`] over [`DispatcherConfig::slots`]
//! (04-14 behaviour); `flow: None` is [`NoFlow`]. Because the default placement is built
//! per dispatcher while the slot semaphore is node-wide, a dispatcher using it also waits
//! on the semaphore itself while a queue is parked (a slot freed by another partition or
//! by `add_permits` wakes it; the acquired permit is used for the next reservation), as in
//! 04-14. A configured placement wakes the loop through `changed()` alone.
//!
//! **Reservation in the lease.** The lease owns the [`FlowPermit`] and the [`Reservation`]
//! (behind an `Arc`); dropping the lease (release, requeue, revoke, expiry, pause,
//! shutdown) releases both **before** `revoked` is signalled, so a revoked zombie holds no
//! capacity. The [`Job`] carries a cheap view: a [`Reservation`] of the same kind whose
//! inner value is a [`LeaseReservation`] (a weak handle). Executors reach the placement's
//! guard with [`Job::with_reservation`] (e.g. 07-12's worker session), which yields `None`
//! once the lease is gone.
//!
//! **Gauges.** With [`DispatcherConfig::gauges`] set, every loop turn (one batch of state
//! changes, not per entry) publishes this partition's [`QueueGauge`] per queue when it
//! changed: `ready` (due entries), `ready_unblocked` (minus entries of skipped queues,
//! parked keys in the last round and held entries), `oldest_ready_at_ms`, and the leases by
//! [`ReservationKind`]. Shutdown removes the partition's rows. The rebuild records
//! `ostro_flow_rebuild_entries` (gauge) and `ostro_flow_rebuild_seconds` (histogram).
//!
//! **Pause (OPS-01).** `Action::RunPaused` revokes the run's lease and drops its queued or
//! delayed entry; a grant read that finds the run paused is rejected `RunPaused` without
//! logging, and a paused run is never re-queued after a rejected grant. `Action::RunResumed`
//! needs no in-memory reset (retryable failures are durable since F9); the `EnqueueReady`
//! that resume emits queues the run again.
//!
//! # Rebuild, wake-ups, no polling
//!
//! [`spawn`] subscribes to the apply machine's actions **before** scanning, then rebuilds
//! from `query::ready_entries` (every queue) and `query::running_runs` (in-flight runs).
//! After a leadership change every `Running` run holds a token of an older epoch, so it is
//! re-granted and re-dispatched immediately (the old process is gone; no `lease_ttl` wait).
//!
//! The loop then wakes only on: actions (`EnqueueReady`, `InputAppended`, `RunCompleted`,
//! `RunPaused`), the next deadline (earliest delayed `ready_at`, lease check, flow-control
//! `until`), a placement or flow-control `changed()` bump, a free executor slot (default
//! placement), a finished grant/job, and shutdown. There is no interval. A lagged action stream triggers a
//! full rescan ([`DispatcherStats::rescans`]); the startup rebuild is not counted.
//!
//! Delayed `ready_at` values are wall-clock ms from [`LeaderCtx::clock`]; the sleep is
//! computed from them once per loop turn. A [`ManualClock`](crate::clock::ManualClock) that
//! is advanced does not wake the loop by itself (the timer service polls; this does not).
//!
//! # Dispatch (pipelined grant, plan 05-16)
//!
//! Dispatch = take a slot permit, record a lease in phase *Granting*, and in a task: read
//! the run record once, compute the next token `n + 1` of the current epoch from it, and
//! **append** `AttemptGranted` to the log without waiting for its ack. The append returns
//! once the sequencer fixed the grant's offset (the PartitionLog sequencer assigns offsets under
//! one lock in submission order), so the job is handed to the executor right away, with the
//! record read before the grant (patched with the new token, `Running`, no ready slot) and
//! no second record read. Every executor append is submitted after the grant returned, so
//! it lands at a higher offset: apply sees the grant first. The grant's fsync is shared
//! with the executor's first append instead of being waited for on its own.
//!
//! - **The apply token check stays the authority.** A separately spawned ack task waits
//!   until apply passed the grant's offset and reads the record: the grant was accepted iff
//!   `current_attempt` equals its token. A rejected grant drops the lease (revoking the
//!   job); the job's appends were or will be rejected by apply (`Fenced` / `StaleEpoch` /
//!   `RunTerminal`), so nothing it did counts. A rejected grant of a run that is still
//!   live is re-queued after `GRANT_RETRY_MS`.
//! - **Never two concurrent grants per run.** While a grant of a run is not yet applied
//!   (`Dispatcher::unacked`), a new grant of the same run (re-dispatch after a lease drop,
//!   a retry re-grant) first waits until apply passed the earlier grant, so it reads a record
//!   that already holds the earlier token and grants a strictly higher one. After a
//!   rejected grant (whose job may still be alive with that token) the next grant of the
//!   run also goes above the rejected token (`Dispatcher::rejected`), so the zombie stays
//!   fenced.
//! - **Stale `EnqueueReady`.** While the lease is *Granting* an `EnqueueReady` predates the
//!   grant and is ignored. While the job runs but the grant is not yet acknowledged, an
//!   `EnqueueReady` may predate the grant (same apply chunk) or be the executor's own
//!   requeue; the dispatcher reads the record (every action is broadcast after its chunk
//!   committed): `current_attempt == token` and status `Ready` means a requeue after the
//!   grant (lease dropped, entry queued), anything else is stale. Once acknowledged, an
//!   `EnqueueReady` while *Executing* is always the executor's requeue.
//! - Retry re-grants ([`ExecOutcome::Retry`]) still await their ack (the job starts only
//!   after the backoff anyway).
//!
//! When a grant or job result comes back, the loop first drains the action stream: every
//! action of an earlier chunk is in the channel by then (the apply thread commits, acks,
//! then fans out, one chunk at a time).
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
//! Reservations (slot permits) belong to leases, not to executor futures: a revoked zombie
//! does not keep a slot, so `slots` bounds the leased jobs. A future that keeps running after revocation
//! (or after its run was requeued) briefly overlaps with its successor.
//!
//! Shutdown revokes every lease and waits for executor futures for at most
//! `min(lease_ttl, 5 s)`, then aborts the rest.
//!
//! # Hot inputs
//!
//! `InputAppended { run, seq }` for a leased run sends `seq` to the job's `inputs` channel.
//! Inputs for runs without a lease are ignored (the next activation reads the journal).
//!
//! # Failpoints (plan 05-18, feature `failpoints`)
//!
//! - `dispatcher::before_grant`: in the grant task, after the record read and right before
//!   `AttemptGranted` is appended. `return` fails the grant with EIO (`return(enospc)`:
//!   ENOSPC) before anything is logged; the run is re-queued after `GRANT_RETRY_MS`.
//!   `sleep(ms)` delays the grant (it blocks one runtime worker thread).
//! - `dispatcher::before_ack`: in the ack task of a pipelined grant, before it waits for
//!   apply. The job has already started; the grant may not be applied yet. `return` makes
//!   the ack fail with EIO / ENOSPC (outcome unknown): the lease is dropped (the job is
//!   revoked) and the run is re-granted above the token. `sleep(ms)` holds the ack back
//!   while the job runs.
//!
//! `panic` at either site panics the task; the dispatcher handles it like any task panic
//! (`on_join_error`: the lease is dropped and the run retried).
//!
//! # Metrics (OBS-02, plan 08-16)
//!
//! - `ostro_queue_ready{queue}` (gauge): due entries waiting for a slot. Every partition's
//!   dispatcher moves the shared per-queue series by its own deltas (as with the lease
//!   gauge), so the value is the node total; a stopped dispatcher removes its share.
//! - `ostro_queue_lag_seconds{queue}` (histogram): `ready_at` → granted, recorded when a
//!   grant succeeds (regrants of running runs, whose ready time is unknown, are skipped).
//! - `ostro_runs_started_total{workflow}` (counter): first grant of a run (its first
//!   activation).
//! - `ostro_queue_worker_demand{queue}` is only **described** here; Phase 7's
//!   `workers/demand.rs` owns its value (07-DECISIONS).

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use ostro_log::{PartitionLog, ReplicatedLog};
use ostro_types::command::{AttemptGranted, AttemptTokenMsg, encode_command};
use ostro_types::{AttemptToken, Command, Offset, PartitionId, RunId};
use tokio::sync::broadcast::{
    self,
    error::{RecvError, TryRecvError},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::{Id as TaskId, JoinError, JoinSet};
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use super::LeaderCtx;
use super::fair::{FairEntry, FairQueues};
use super::flow::{Admit, FlowControl, FlowPermit, NoFlow, PauseScope, QueueGauge, QueueGauges};
use super::placement::{
    EntryView, Placement, Reservation, ReservationKind, Reserve, SemaphorePlacement,
};
use crate::apply::{Action, ApplyOutcome, RejectReason};
use crate::handle::{PartitionError, PartitionHandle};
use crate::query;
use crate::store::values::{RunRecord, RunStatus};

/// Evaluate a failpoint: `failpoint!("name")` is a `Result<(), PartitionError>`. Without the
/// feature `failpoints` it expands to `Ok(())` (no name, no call).
macro_rules! failpoint {
    ($name:literal) => {{
        #[cfg(feature = "failpoints")]
        let r = failpoint_eval($name);
        #[cfg(not(feature = "failpoints"))]
        let r = Ok::<(), PartitionError>(());
        r
    }};
}

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
    pub handle: PartitionHandle<PartitionLog>,
    /// Seqs of inputs journaled while leased (`InputAppended`).
    pub inputs: mpsc::UnboundedReceiver<u64>,
    /// `true` = lease lost (expiry / shutdown / requeue): stop now.
    pub revoked: watch::Receiver<bool>,
    /// `renew() -> bool` (false once revoked).
    pub lease: LeaseRenewer,
    /// Durable count of retryable attempt failures (`record.attempt_failures`, F9).
    pub failures: u32,
    /// View of the lease's reservation (plan 07-05): same [`ReservationKind`], inner value a
    /// [`LeaseReservation`]. Use [`Job::with_reservation`] to reach the placement's guard.
    pub reservation: Reservation,
}

impl Job {
    /// Where this job runs (the reservation's kind).
    pub fn reservation_kind(&self) -> ReservationKind {
        self.reservation.kind()
    }

    /// Run `f` on the placement's guard of this job's reservation, if the lease still holds
    /// it and it is a `T`. `None` once the lease was dropped (revoked jobs hold nothing).
    /// Do not keep the borrow across awaits: the lease owns the capacity.
    pub fn with_reservation<T: Any, R>(&self, f: impl FnOnce(&T) -> R) -> Option<R> {
        self.reservation.downcast_ref::<LeaseReservation>()?.with(f)
    }
}

/// The [`Job`]'s weak handle on its lease's [`Reservation`] (the lease owns the capacity;
/// see the module docs).
#[derive(Clone, Debug)]
pub struct LeaseReservation(Weak<Reservation>);

impl LeaseReservation {
    /// Run `f` on the guard if the lease still holds the reservation and it is a `T`.
    pub fn with<T: Any, R>(&self, f: impl FnOnce(&T) -> R) -> Option<R> {
        let r = self.0.upgrade()?;
        r.downcast_ref::<T>().map(f)
    }

    /// Whether the lease still holds the reservation.
    pub fn is_live(&self) -> bool {
        self.0.strong_count() > 0
    }
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
    /// The job was lost without an outcome (its worker disconnected, plan 07-05): drop the
    /// lease and re-grant `n + 1` at once. Not an attempt failure (no failure counted, no
    /// delay).
    Abandoned,
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
        let at = ms_since(self.base, Instant::now() + self.ttl); // clock-exempt: lease TTL (real time by design)
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
    /// Node-wide embedded executor slots (shared by every partition). Used by the default
    /// placement and by a node placement for its embedded capacity.
    pub slots: Arc<Semaphore>,
    pub stats: Option<Arc<DispatcherStats>>,
    /// Capacity source (plan 07-05); `None` = [`SemaphorePlacement`] over `slots`.
    pub placement: Option<Arc<dyn Placement>>,
    /// Admission control (plan 07-05); `None` = [`NoFlow`].
    pub flow: Option<Arc<dyn FlowControl>>,
    /// Per-(partition, queue) gauges to publish (plan 07-05); `None` = not published.
    pub gauges: Option<Arc<QueueGauges>>,
}

impl DispatcherConfig {
    /// `lease_ttl` 30 s, 64 slots, no stats, default placement and flow, no gauges.
    pub fn new(executor: Arc<dyn Executor>) -> Self {
        Self {
            executor,
            lease_ttl: DEFAULT_LEASE_TTL,
            slots: Arc::new(Semaphore::new(DEFAULT_SLOTS)),
            stats: None,
            placement: None,
            flow: None,
            gauges: None,
        }
    }
}

impl std::fmt::Debug for DispatcherConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DispatcherConfig")
            .field("lease_ttl", &self.lease_ttl)
            .field("slots_available", &self.slots.available_permits())
            .field("stats", &self.stats)
            .field("placement", &self.placement.is_some())
            .field("flow", &self.flow.is_some())
            .field("gauges", &self.gauges.is_some())
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
    /// Gauge: leases currently held (*Granting* or *Executing*) over every dispatcher sharing
    /// these stats, refreshed every loop turn (plan 05-16; the WF-04 "no lease per sleeper"
    /// probe).
    pub leases: AtomicU64,
    /// Pipelined grants whose ack showed apply rejected them (lease dropped, job revoked).
    pub grants_rejected: AtomicU64,
    /// Test hook (plan 05-16): when it returns `true` for a run, the next pipelined grant is
    /// submitted with a stale epoch so apply rejects it (`StaleEpoch`).
    #[doc(hidden)]
    pub grant_fault: GrantFault,
}

/// `run -> inject a grant rejection` (see [`DispatcherStats::grant_fault`]).
pub type GrantFaultFn = Arc<dyn Fn(&RunId) -> bool + Send + Sync>;

/// Holder of the optional [`GrantFaultFn`] test hook.
#[doc(hidden)]
#[derive(Default)]
pub struct GrantFault(std::sync::Mutex<Option<GrantFaultFn>>);

impl std::fmt::Debug for GrantFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let set = self.0.lock().map(|g| g.is_some()).unwrap_or(true);
        f.debug_tuple("GrantFault").field(&set).finish()
    }
}

impl GrantFault {
    /// Install (`Some`) or clear (`None`) the hook.
    pub fn set(&self, f: Option<GrantFaultFn>) {
        *self.0.lock().unwrap_or_else(|p| p.into_inner()) = f;
    }

    fn fires(&self, run: &RunId) -> bool {
        let f = self.0.lock().unwrap_or_else(|p| p.into_inner()).clone();
        f.is_some_and(|f| f(run))
    }
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
    /// Phase 7 F3/F7 placement inputs, from the ready row / action (no record read).
    workflow: String,
    tenant: Option<String>,
    first_activation: bool,
}

impl Entry {
    fn from_record(r: &RunRecord, ready_at_ms: u64) -> Self {
        Self {
            queue: r.queue.clone(),
            priority: r.priority,
            ready_at_ms,
            concurrency_key: r.concurrency_key.clone(),
            workflow: r.workflow.clone(),
            tenant: r.tenant.clone(),
            first_activation: r.current_attempt.is_none(),
        }
    }

    fn from_ready_row(row: query::ReadyEntryRow) -> Self {
        Self {
            queue: row.queue,
            priority: row.priority,
            ready_at_ms: row.ready_at_ms,
            concurrency_key: row.concurrency_key,
            workflow: row.workflow,
            tenant: row.tenant,
            first_activation: row.first_activation,
        }
    }

    fn at(&self, ready_at_ms: u64) -> Self {
        Self {
            ready_at_ms,
            ..self.clone()
        }
    }

    /// The entry of an immediate re-grant of a started run (lease expiry, `Abandoned`):
    /// ready time 0 (ahead of same-priority work) and not a first activation (B1).
    fn regrant(&self) -> Self {
        Self {
            ready_at_ms: 0,
            first_activation: false,
            ..self.clone()
        }
    }

    fn view(&self) -> EntryView<'_> {
        EntryView {
            queue: &self.queue,
            workflow: &self.workflow,
            tenant: self.tenant.as_deref(),
            concurrency_key: &self.concurrency_key,
            priority: self.priority,
            first_activation: self.first_activation,
        }
    }

    fn fair(&self, run: RunId) -> FairEntry {
        FairEntry {
            queue: self.queue.clone(),
            priority: self.priority,
            key: self.concurrency_key.clone(),
            ready_at_ms: self.ready_at_ms,
            run,
            first_activation: self.first_activation,
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
    /// Offset of the pipelined grant while its ack is outstanding.
    grant_pending: Option<Offset>,
    entry: Entry,
    /// Deadline in ms since `State::base` (shared with the job's [`LeaseRenewer`]).
    deadline: Arc<AtomicU64>,
    /// Key of this lease in `State::checks`.
    check_at: u64,
    revoked: watch::Sender<bool>,
    inputs: mpsc::UnboundedSender<u64>,
    /// Receiver for the next job (taken when the job is built).
    pending_inputs: Option<mpsc::UnboundedReceiver<u64>>,
    /// Committed flow charge (released when the lease ends).
    flow: Option<FlowPermit>,
    /// The capacity of this job (the job sees a weak view).
    reservation: Option<Arc<Reservation>>,
    kind: ReservationKind,
}

impl Lease {
    /// Give the flow charge and the capacity back (idempotent).
    fn release_capacity(&mut self) {
        drop(self.flow.take());
        drop(self.reservation.take());
    }
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
    /// Due entries, selected fairly (D-W10).
    fair: FairQueues,
    /// Every queued or delayed entry.
    entries: BTreeMap<RunId, Entry>,
    /// Entries with a future `ready_at`: `(ready_at_ms, run)`.
    delayed: BTreeSet<(u64, RunId)>,
    leases: BTreeMap<RunId, Lease>,
    /// Lease checks `(deadline ms since base, run)`.
    checks: BTreeSet<(u64, RunId)>,
    next_generation: u64,
    base: Instant,
    ttl: Duration,
}

impl State {
    fn new(ttl: Duration) -> Self {
        Self {
            fair: FairQueues::new(),
            entries: BTreeMap::new(),
            delayed: BTreeSet::new(),
            leases: BTreeMap::new(),
            checks: BTreeSet::new(),
            next_generation: 0,
            base: Instant::now(), // clock-exempt: lease TTL base
            ttl,
        }
    }

    fn now_rel(&self) -> u64 {
        ms_since(self.base, Instant::now()) // clock-exempt: lease TTL
    }

    fn ttl_ms(&self) -> u64 {
        u64::try_from(self.ttl.as_millis()).unwrap_or(u64::MAX)
    }

    fn has_ready(&self) -> bool {
        !self.fair.is_empty()
    }

    /// Queue `run` (replacing any previous entry): due entries go to their queue, future
    /// ones to the delay set.
    fn enqueue(&mut self, run: RunId, e: Entry, now_ms: u64) {
        self.unqueue(&run);
        if e.ready_at_ms > now_ms {
            self.delayed.insert((e.ready_at_ms, run));
        } else {
            self.fair.insert(e.fair(run));
        }
        self.entries.insert(run, e);
    }

    fn unqueue(&mut self, run: &RunId) -> Option<Entry> {
        let e = self.entries.remove(run)?;
        if !self.delayed.remove(&(e.ready_at_ms, *run)) {
            self.fair.remove(run);
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
                self.fair.insert(e.fair(run));
            }
        }
    }

    /// Next run in fair order, ignoring admission and placement (unit tests).
    #[cfg(test)]
    fn pop_next(&mut self) -> Option<(RunId, Entry)> {
        let c = self.fair.next_candidate(u64::MAX)?;
        self.fair.take(&c)?;
        let e = self.entries.remove(&c.run)?;
        Some((c.run, e))
    }

    /// Remove `run`'s lease: release its flow charge and capacity, then signal `revoked`
    /// (harmless when the job already ended).
    fn drop_lease(&mut self, run: &RunId) -> Option<Lease> {
        let mut l = self.leases.remove(run)?;
        self.checks.remove(&(l.check_at, *run));
        l.release_capacity();
        l.revoked.send_replace(true);
        Some(l)
    }

    fn revoke_all(&mut self) {
        let runs: Vec<RunId> = self.leases.keys().copied().collect();
        for r in runs {
            self.drop_lease(&r);
        }
    }

    /// Record a new lease in phase Granting, owning the committed flow charge and the
    /// reservation; returns its generation.
    fn new_lease(
        &mut self,
        run: RunId,
        entry: Entry,
        flow: FlowPermit,
        reservation: Reservation,
    ) -> u64 {
        self.next_generation += 1;
        let generation = self.next_generation;
        let at = self.now_rel().saturating_add(self.ttl_ms());
        let (tx, rx) = mpsc::unbounded_channel();
        let lease = Lease {
            generation,
            phase: Phase::Granting,
            grant_pending: None,
            entry,
            deadline: Arc::new(AtomicU64::new(at)),
            check_at: at,
            revoked: watch::channel(false).0,
            inputs: tx,
            pending_inputs: Some(rx),
            flow: Some(flow),
            kind: reservation.kind(),
            reservation: Some(Arc::new(reservation)),
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
                        // Ready time 0: re-dispatch ahead of same-priority work; a started
                        // run is never a first activation (B1).
                        self.enqueue(run, l.entry.regrant(), now_ms);
                        expired.push(run);
                    }
                }
            }
        }
        expired
    }

    /// Whether a ready entry / `EnqueueReady` for `run` is stale with respect to its lease:
    /// always while *Granting*; while the pipelined grant is unacknowledged, unless
    /// `is_requeue` (a record read) shows a requeue after the grant (see module docs).
    fn stale_for_lease(
        &self,
        run: &RunId,
        is_requeue: &mut dyn FnMut(&RunId, AttemptToken) -> bool,
    ) -> Option<bool> {
        let l = self.leases.get(run)?;
        Some(match l.phase {
            Phase::Granting => true,
            Phase::Executing(token) if l.grant_pending.is_some() => !is_requeue(run, token),
            Phase::Executing(_) => false,
        })
    }

    /// Apply a batch of committed actions. `is_requeue(run, token)` is consulted only for an
    /// `EnqueueReady` of a run whose pipelined grant is not yet acknowledged.
    fn on_actions(
        &mut self,
        batch: &[Action],
        now_ms: u64,
        is_requeue: &mut dyn FnMut(&RunId, AttemptToken) -> bool,
    ) {
        for a in batch {
            match a {
                Action::EnqueueReady {
                    run,
                    queue,
                    priority,
                    ready_at_ms,
                    concurrency_key,
                    workflow,
                    tenant,
                    first_activation,
                } => {
                    match self.stale_for_lease(run, is_requeue) {
                        // Applied before our grant, which supersedes it.
                        Some(true) => continue,
                        // The executor requeued the run: its lease ends here.
                        Some(false) => {
                            self.drop_lease(run);
                        }
                        None => {}
                    }
                    let e = Entry {
                        queue: queue.clone(),
                        priority: *priority,
                        ready_at_ms: *ready_at_ms,
                        concurrency_key: concurrency_key.clone(),
                        workflow: workflow.clone(),
                        tenant: tenant.clone(),
                        first_activation: *first_activation,
                    };
                    self.enqueue(*run, e, now_ms);
                }
                Action::InputAppended { run, seq } => {
                    if let Some(l) = self.leases.get(run) {
                        let _ = l.inputs.send(*seq);
                    }
                }
                Action::RunCompleted { run, .. } => {
                    // Any terminal status, hard stops (Terminated / TimedOut) included:
                    // revoking the lease cancels a live activation (05-09/05-11).
                    self.drop_lease(run);
                    self.unqueue(run);
                }
                Action::RunPaused { run } => {
                    // OPS-01: stop the live activation, forget queued / delayed work.
                    self.drop_lease(run);
                    self.unqueue(run);
                }
                // Nothing in memory to reset (failures are durable, F9); the EnqueueReady
                // emitted with the resume queues the run.
                Action::RunResumed { .. } => {}
                _ => {}
            }
        }
    }

    /// Reconcile with a full scan. Leased runs keep their lease, except that a ready row for
    /// a run whose grant was applied means the executor requeued it. Unleased running runs
    /// are queued for an immediate re-grant (pending retries keep their delay).
    fn apply_scan(
        &mut self,
        scan: Scan,
        now_ms: u64,
        is_requeue: &mut dyn FnMut(&RunId, AttemptToken) -> bool,
    ) {
        let running: BTreeMap<RunId, Option<Entry>> = scan.running.into_iter().collect();
        // Keep only pending re-grants (retry backoff) of runs that are still Running.
        let old: Vec<(RunId, Entry)> = std::mem::take(&mut self.entries).into_iter().collect();
        self.fair.clear();
        self.delayed.clear();
        for (run, e) in old {
            if running.contains_key(&run) && !self.leases.contains_key(&run) {
                self.enqueue(run, e, now_ms);
            }
        }
        for row in scan.ready {
            match self.stale_for_lease(&row.run, is_requeue) {
                Some(true) => continue,
                Some(false) => {
                    self.drop_lease(&row.run);
                }
                None => {}
            }
            let run = row.run;
            self.enqueue(run, Entry::from_ready_row(row), now_ms);
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

/// A grant handed back to the loop: the token, the record the job runs with, and the
/// grant's log offset while its ack is outstanding (pipelined grants only).
struct Grant {
    token: AttemptToken,
    record: Box<RunRecord>,
    pending: Option<Offset>,
}

enum Done {
    Granted {
        run: RunId,
        generation: u64,
        res: Result<Grant, PartitionError>,
    },
    /// Apply passed a pipelined grant: `Ok(accepted)`.
    GrantAcked {
        run: RunId,
        generation: u64,
        offset: Offset,
        token: AttemptToken,
        res: Result<bool, PartitionError>,
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
    /// This dispatcher's share of [`DispatcherStats::leases`] (the stats are shared by every
    /// partition's dispatcher, so the gauge moves by deltas).
    reported_leases: u64,
    /// Pipelined grants appended but not yet acknowledged: `run -> grant offset`. A new grant
    /// of the run waits until apply passed this offset (never two concurrent grants).
    unacked: BTreeMap<RunId, Offset>,
    /// Tokens of rejected pipelined grants whose job may still be alive: the next grant of
    /// the run uses a strictly higher `n`, so the zombie stays fenced.
    rejected: BTreeMap<RunId, AttemptToken>,
    m_dispatched: metrics::Counter,
    m_expired: metrics::Counter,
    /// This dispatcher's share of `ostro_queue_ready{queue}`: queue -> (gauge, reported).
    ready_gauges: BTreeMap<String, (metrics::Gauge, usize)>,
    /// Capacity source (configured, or the default [`SemaphorePlacement`] over `slots`).
    placement: Arc<dyn Placement>,
    /// Admission control (configured, or [`NoFlow`]).
    flow: Arc<dyn FlowControl>,
    /// `true` = `placement` is the default one: also wait on the slot semaphore itself.
    default_placement: bool,
    /// A slot permit taken by the semaphore arm (default placement), used by the next
    /// reservation of the round.
    held_slot: Option<OwnedSemaphorePermit>,
    place_rx: watch::Receiver<u64>,
    flow_rx: watch::Receiver<u64>,
    /// The watch senders are alive (a closed channel disables its arm).
    place_live: bool,
    flow_live: bool,
    /// Earliest `QueueBlocked{until}` of the last round (clock ms).
    queue_until: Option<u64>,
    /// Last published gauges per queue.
    published: BTreeMap<String, QueueGauge>,
}

/// Loop control after handling an event.
enum Flow {
    Continue,
    Stop,
}

impl Dispatcher {
    fn new(ctx: LeaderCtx, cfg: DispatcherConfig) -> Self {
        let label = ctx.partition.0.to_string();
        describe_queue_metrics();
        let default_placement = cfg.placement.is_none();
        let placement: Arc<dyn Placement> = match &cfg.placement {
            Some(p) => Arc::clone(p),
            None => Arc::new(SemaphorePlacement::new(Arc::clone(&cfg.slots))),
        };
        let flow: Arc<dyn FlowControl> = match &cfg.flow {
            Some(f) => Arc::clone(f),
            None => Arc::new(NoFlow::new()),
        };
        Self {
            place_rx: placement.changed(),
            flow_rx: flow.changed(),
            placement,
            flow,
            default_placement,
            held_slot: None,
            place_live: true,
            flow_live: true,
            queue_until: None,
            published: BTreeMap::new(),
            st: State::new(cfg.lease_ttl),
            stats: cfg.stats.clone().unwrap_or_default(),
            joins: JoinSet::new(),
            tasks: BTreeMap::new(),
            unacked: BTreeMap::new(),
            rejected: BTreeMap::new(),
            reported_leases: 0,
            m_dispatched: metrics::counter!("ostro_dispatcher_dispatched_total", "partition" => label.clone()),
            m_expired: metrics::counter!("ostro_dispatcher_lease_expired_total", "partition" => label),
            ready_gauges: BTreeMap::new(),
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
                    // A paused Running run is not re-granted (OPS-01): no entry.
                    Ok((
                        run,
                        rec.filter(|r| r.pause.is_none())
                            .map(|r| Entry::from_record(&r, 0)),
                    ))
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
                    let h = &self.ctx.handle;
                    self.st
                        .apply_scan(s, now, &mut |r, t| requeued_after_grant(h, r, t));
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
                Ok(b) => self.actions(&b),
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

    /// One dispatch round (module docs): fair candidate -> admission -> reservation ->
    /// grant, until no candidate is left (every queue skipped, parked or empty).
    fn dispatch_available(&mut self) {
        self.queue_until = None;
        self.st.fair.begin_round();
        if !self.flow_live {
            // No `changed()` bump can release held entries any more: probe them per round.
            self.st.fair.release_held();
        }
        if !self.st.has_ready() {
            drop(self.held_slot.take());
            return;
        }
        for q in self.st.fair.queue_names() {
            let w = self.flow.queue_weight(&q);
            self.st.fair.set_weight(&q, w);
        }
        let now = self.now_ms();
        while let Some(c) = self.st.fair.next_candidate(now) {
            let Some(e) = self.st.entries.get(&c.run) else {
                // Not reachable (fair holds only queued entries); never spin on it.
                self.st.fair.remove(&c.run);
                continue;
            };
            let view = e.view();
            let first = e.first_activation;
            let mut permit = match self.flow.admit(&view, now) {
                Admit::Yes(p) => p,
                Admit::QueueBlocked {
                    until_ms: Some(until),
                } if first => {
                    // A throttle (first activations only, D-W13): gate the queue's first
                    // activations until `until` (or `changed()`); its re-activations are
                    // still served this round (07-23). `until` is in the future, so the
                    // gate's timer never spins.
                    self.st.fair.gate_first(&c.queue, until.max(now + 1));
                    continue;
                }
                Admit::KeyBlocked {
                    until_ms: Some(until),
                } if first => {
                    // A key throttle: only this key's first activations wait.
                    self.st
                        .fair
                        .gate_first_key(&c.queue, &c.key, until.max(now + 1));
                    continue;
                }
                Admit::QueueBlocked { until_ms } => {
                    self.st.fair.skip_queue(&c.queue);
                    self.queue_until = match (self.queue_until, until_ms) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (a, b) => a.or(b),
                    };
                    continue;
                }
                Admit::KeyBlocked { until_ms } => {
                    self.st.fair.park_key(&c.queue, &c.key, until_ms);
                    continue;
                }
                Admit::Paused {
                    scope: PauseScope::Queue,
                } => {
                    self.st.fair.skip_queue(&c.queue);
                    continue;
                }
                Admit::Paused {
                    scope: PauseScope::Workflow,
                } => {
                    // Only this workflow waits; the rest of the queue goes on (07-05 fix).
                    self.st.fair.hold(&c);
                    continue;
                }
            };
            let reserved = match self.held_slot.take() {
                Some(slot) => Reserve::Granted(Reservation::new(ReservationKind::Embedded, slot)),
                None => self.placement.reserve(&view),
            };
            let reservation = match reserved {
                Reserve::Granted(r) => r,
                Reserve::Blocked => {
                    drop(permit); // rolls the tentative charge back
                    self.st.fair.park_queue(&c.queue);
                    continue;
                }
            };
            permit.commit();
            self.st.fair.take(&c);
            if let Some(entry) = self.st.entries.remove(&c.run) {
                self.dispatch(c.run, entry, permit, reservation);
            }
        }
        // An unused semaphore permit goes back (another partition may need it).
        drop(self.held_slot.take());
    }

    /// Move the shared lease gauge by this dispatcher's change since the last report.
    fn report_leases(&mut self) {
        let now = self.st.leases.len() as u64;
        if now > self.reported_leases {
            self.stats
                .leases
                .fetch_add(now - self.reported_leases, Ordering::Relaxed);
        } else if now < self.reported_leases {
            self.stats
                .leases
                .fetch_sub(self.reported_leases - now, Ordering::Relaxed);
        }
        self.reported_leases = now;
    }

    /// Move `ostro_queue_ready{queue}` by this dispatcher's change since the last report
    /// (one count per queue; no lock).
    fn report_queues(&mut self) {
        let counts = self.st.fair.per_queue_counts();
        for (q, (g, reported)) in &mut self.ready_gauges {
            let now = counts.get(q).copied().unwrap_or(0);
            move_gauge(g, reported, now);
        }
        for (q, n) in counts {
            if let std::collections::btree_map::Entry::Vacant(v) = self.ready_gauges.entry(q) {
                let g = ready_gauge(v.key());
                let mut reported = 0;
                move_gauge(&g, &mut reported, n);
                v.insert((g, reported));
            }
        }
    }

    /// Make `queue`'s ready series visible (at this dispatcher's share 0) even when its
    /// entries are always granted before a report sees them queued.
    fn track_queue(&mut self, queue: &str) {
        if !self.ready_gauges.contains_key(queue) {
            self.ready_gauges
                .insert(queue.to_owned(), (ready_gauge(queue), 0));
        }
    }

    /// Queue lag and run-start metrics of a successful grant.
    fn record_grant(&mut self, run: &RunId) {
        let Some(l) = self.st.leases.get(run) else {
            return;
        };
        let (ready_at_ms, queue) = (l.entry.ready_at_ms, l.entry.queue.clone());
        let workflow = l.entry.first_activation.then(|| l.entry.workflow.clone());
        self.track_queue(&queue);
        if ready_at_ms > 0 {
            let lag_ms = self.now_ms().saturating_sub(ready_at_ms);
            metrics::histogram!("ostro_queue_lag_seconds", "queue" => queue)
                .record(lag_ms as f64 / 1000.0);
        }
        if let Some(workflow) = workflow {
            metrics::counter!("ostro_runs_started_total", "workflow" => workflow).increment(1);
        }
    }

    /// Apply one committed action batch to the state.
    fn actions(&mut self, b: &[Action]) {
        let now = self.now_ms();
        let h = &self.ctx.handle;
        self.st
            .on_actions(b, now, &mut |r, t| requeued_after_grant(h, r, t));
    }

    fn dispatch(&mut self, run: RunId, entry: Entry, flow: FlowPermit, res: Reservation) {
        let generation = self.st.new_lease(run, entry, flow, res);
        let h = self.ctx.handle.clone();
        let after = self.unacked.get(&run).copied();
        let floor = self.rejected.get(&run).copied();
        let fault = self.stats.grant_fault.fires(&run);
        let id = self
            .joins
            .spawn(async move {
                let res = pipelined_grant(&h, run, after, floor, fault).await;
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
        let failures = record.attempt_failures;
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
        let view = LeaseReservation(
            l.reservation
                .as_ref()
                .map_or_else(Weak::new, Arc::downgrade),
        );
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
            reservation: Reservation::new(l.kind, view),
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
            Done::GrantAcked {
                run,
                generation,
                offset,
                token,
                res,
            } => self.on_grant_acked(run, generation, offset, token, res),
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
        res: Result<Grant, PartitionError>,
    ) -> Flow {
        // A pipelined grant is in the log whether or not its lease is still current: track
        // it so the next grant of this run waits for it.
        if let Ok(Grant {
            pending: Some(off), ..
        }) = &res
        {
            self.unacked.insert(run, *off);
            self.spawn_ack(run, generation, *off, res.as_ref().map(|g| g.token).ok());
        }
        if !self.current(&run, generation) {
            debug!(partition = self.p(), %run, "grant result for a dropped lease ignored");
            return Flow::Continue;
        }
        match res {
            Ok(Grant {
                token,
                record,
                pending,
            }) => {
                if let Some(l) = self.st.leases.get_mut(&run) {
                    l.phase = Phase::Executing(token);
                    l.grant_pending = pending;
                }
                self.record_grant(&run);
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

    /// Watch a pipelined grant until apply passed it; the outcome comes back as
    /// [`Done::GrantAcked`].
    fn spawn_ack(
        &mut self,
        run: RunId,
        generation: u64,
        offset: Offset,
        token: Option<AttemptToken>,
    ) {
        let Some(token) = token else { return };
        let h = self.ctx.handle.clone();
        let id = self
            .joins
            .spawn(async move {
                let res = async {
                    failpoint!("dispatcher::before_ack")?;
                    h.machine().wait_applied(offset).await?;
                    let accepted = h
                        .read_run(&run)?
                        .is_some_and(|r| r.current_attempt == Some((token.epoch.0, token.n)));
                    Ok(accepted)
                }
                .await;
                Done::GrantAcked {
                    run,
                    generation,
                    offset,
                    token,
                    res,
                }
            })
            .id();
        self.tasks.insert(id, (run, generation));
    }

    fn on_grant_acked(
        &mut self,
        run: RunId,
        generation: u64,
        offset: Offset,
        token: AttemptToken,
        res: Result<bool, PartitionError>,
    ) -> Flow {
        if self.unacked.get(&run) == Some(&offset) {
            self.unacked.remove(&run);
        }
        let current = self.current(&run, generation);
        if res.as_ref().is_ok_and(|accepted| *accepted) {
            // The record now holds this token: later grants read past it.
            self.rejected.remove(&run);
            if current
                && let Some(l) = self.st.leases.get_mut(&run)
                && l.grant_pending == Some(offset)
            {
                l.grant_pending = None;
            }
            return Flow::Continue;
        }
        // Rejected, or the outcome is unknown: its job (if any) keeps the token, so later
        // grants must go past it.
        let floor = self.rejected.entry(run).or_insert(token);
        *floor = (*floor).max(token);
        match &res {
            Ok(_) => {
                // Apply rejected the grant (RunTerminal / RunNotFound / Fenced / StaleEpoch):
                // the job's appends are rejected too.
                self.stats.grants_rejected.fetch_add(1, Ordering::Relaxed);
                debug!(partition = self.p(), %run, "pipelined grant rejected by apply; lease dropped");
            }
            Err(e) => {
                if self.ctx.handle.machine().check_healthy().is_err() {
                    error!(partition = self.p(), %run, error = %e, "grant ack failed; partition failing, dispatcher stops");
                    return Flow::Stop;
                }
                warn!(partition = self.p(), %run, error = %e, "grant ack failed; re-granting");
            }
        }
        // Stop the job of this grant. Its lease may already be gone: a job fenced by apply
        // can report `Fenced` before this ack is handled.
        let entry = if current {
            self.st.drop_lease(&run).map(|l| l.entry)
        } else {
            None
        };
        if self.st.leases.contains_key(&run) || self.st.entries.contains_key(&run) {
            return Flow::Continue; // a newer lease or a queued entry owns the run
        }
        match self.ctx.handle.read_run(&run) {
            Ok(Some(rec)) if rec.pause.is_some() && !rec.status.is_terminal() => {
                // Paused (OPS-01): the resume's EnqueueReady queues it again.
                debug!(partition = self.p(), %run, "grant rejected for a paused run; not re-queued");
            }
            Ok(Some(rec)) if !rec.status.is_terminal() => {
                // Still live: re-grant (above the rejected token) after a short delay.
                let now = self.now_ms();
                let e = entry.unwrap_or_else(|| Entry::from_record(&rec, 0));
                self.st
                    .enqueue(run, e.at(now.saturating_add(GRANT_RETRY_MS)), now);
            }
            Ok(_) => {
                // Terminal or gone: no later grant, nothing to fence above.
                self.rejected.remove(&run);
            }
            Err(e) => {
                warn!(partition = self.p(), %run, error = %e, "run read after a rejected grant failed; re-granting");
                let now = self.now_ms();
                if let Some(e) = entry {
                    self.st
                        .enqueue(run, e.at(now.saturating_add(GRANT_RETRY_MS)), now);
                }
            }
        }
        Flow::Continue
    }

    fn on_executed(&mut self, run: RunId, generation: u64, outcome: ExecOutcome) {
        if !self.current(&run, generation) {
            debug!(partition = self.p(), %run, ?outcome, "stale job outcome ignored");
            return;
        }
        match outcome {
            ExecOutcome::Released | ExecOutcome::Fenced => {
                self.st.drop_lease(&run);
            }
            ExecOutcome::Abandoned => {
                // Worker lost (07-05): re-grant n + 1 now; not an attempt failure.
                debug!(partition = self.p(), %run, "job abandoned; re-granting");
                if let Some(l) = self.st.drop_lease(&run) {
                    let now = self.now_ms();
                    self.st.enqueue(run, l.entry.regrant(), now);
                }
            }
            ExecOutcome::Retry { after_ms } => self.regrant_after_failure(run, after_ms),
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

    /// F9 (see the module docs): grant the next attempt now with `after_failure = true`,
    /// then hand the job over after `after_ms`, keeping the lease (phase *Granting*, so it
    /// does not expire). A revoke (hard stop, shutdown) ends the wait early; the result is
    /// then ignored like any result of a dropped lease.
    fn regrant_after_failure(&mut self, run: RunId, after_ms: u64) {
        let Some(l) = self.st.leases.get_mut(&run) else {
            return;
        };
        l.phase = Phase::Granting;
        l.grant_pending = None;
        let generation = l.generation;
        let mut revoked = l.revoked.subscribe();
        let h = self.ctx.handle.clone();
        let after = self.unacked.get(&run).copied();
        let id = self
            .joins
            .spawn(async move {
                let res = async {
                    if let Some(off) = after {
                        h.machine().wait_applied(off).await?;
                    }
                    let token = grant_after_failure(&h, run).await?;
                    tokio::select! {
                        () = tokio::time::sleep(Duration::from_millis(after_ms)) => {}
                        _ = revoked.wait_for(|v| *v) => {}
                    }
                    let rec = h
                        .read_run(&run)?
                        .ok_or(PartitionError::Rejected(RejectReason::RunNotFound))?;
                    Ok(Grant {
                        token,
                        record: Box::new(rec),
                        pending: None,
                    })
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

    /// Earliest wake-up: next delayed ready time, next lease check, or the earliest
    /// flow-control `until` (a `QueueBlocked` of the last round, a parked key or a
    /// first-activation gate).
    fn next_deadline(&self) -> Option<Instant> {
        let at_clock = [
            self.st.delayed.first().map(|&(at, _)| at),
            self.queue_until,
            self.st.fair.next_unpark_at(),
        ]
        .into_iter()
        .flatten()
        .min();
        // Clock ms -> a tokio deadline (the sleep waits for a deadline computed from the
        // injected clock).
        let ready = at_clock
            .map(|at| Instant::now() + Duration::from_millis(at.saturating_sub(self.now_ms()))); // clock-exempt: logic-time follow-up (10-10)
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

    /// Publish this partition's per-queue gauges (when configured and changed).
    fn publish_gauges(&mut self) {
        let Some(gauges) = self.cfg.gauges.clone() else {
            return;
        };
        let mut now: BTreeMap<String, QueueGauge> = BTreeMap::new();
        let unblocked = self.st.fair.per_queue_unblocked();
        let oldest = self.st.fair.per_queue_oldest();
        for (q, n) in self.st.fair.per_queue_counts() {
            let g = now.entry(q.clone()).or_default();
            g.ready = n as u64;
            g.ready_unblocked = unblocked.get(&q).copied().unwrap_or(0) as u64;
            g.oldest_ready_at_ms = oldest.get(&q).copied();
        }
        for l in self.st.leases.values() {
            let g = now.entry(l.entry.queue.clone()).or_default();
            match l.kind {
                ReservationKind::Embedded => g.running_embedded += 1,
                ReservationKind::Worker => g.running_on_workers += 1,
            }
        }
        let p = self.ctx.partition;
        for (q, g) in &now {
            if self.published.get(q) != Some(g) {
                gauges.set(p, q, *g);
            }
        }
        // Queues that went idle: publish zeros once, then forget them.
        for q in self.published.keys() {
            if !now.contains_key(q) {
                gauges.set(p, q, QueueGauge::default());
            }
        }
        self.published = now;
    }

    async fn run(mut self, mut actions: broadcast::Receiver<Arc<Vec<Action>>>) {
        let mut shutdown = self.ctx.shutdown.clone();
        #[allow(clippy::disallowed_methods)]
        let started = std::time::Instant::now(); // clock-exempt: rebuild duration metric only
        if self.rescan().await.is_err() {
            return;
        }
        let label = self.p().to_string();
        metrics::gauge!("ostro_flow_rebuild_entries", "partition" => label.clone())
            .set((self.st.entries.len() + self.st.leases.len()) as f64);
        metrics::histogram!("ostro_flow_rebuild_seconds", "partition" => label)
            .record(started.elapsed().as_secs_f64());
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
            self.st.fair.unpark_due(now);
            self.dispatch_available();
            self.report_leases();
            self.report_queues();
            self.publish_gauges();

            let deadline = self.next_deadline();
            // Default placement: a parked queue waits for a node-wide slot (module docs).
            let want_slot = self.default_placement && self.st.fair.has_parked_queues();
            let slots = Arc::clone(&self.cfg.slots);
            let (place_live, flow_live) = (self.place_live, self.flow_live);
            let flow = tokio::select! {
                biased;
                r = shutdown.changed() => match r {
                    Ok(()) => Flow::Continue, // checked at the top of the loop
                    Err(_) => Flow::Stop,
                },
                r = actions.recv() => match r {
                    Ok(b) => {
                        self.actions(&b);
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
                r = self.place_rx.changed(), if place_live => {
                    match r {
                        Ok(()) => self.st.fair.unpark_queues(),
                        Err(_) => self.place_live = false,
                    }
                    Flow::Continue
                },
                r = self.flow_rx.changed(), if flow_live => {
                    match r {
                        Ok(()) => {
                            self.st.fair.unpark_keys();
                            self.st.fair.ungate_all();
                            self.st.fair.release_held();
                        }
                        Err(_) => self.flow_live = false,
                    }
                    Flow::Continue
                },
                p = slots.acquire_owned(), if want_slot => {
                    if let Ok(p) = p {
                        self.held_slot = Some(p);
                        self.st.fair.unpark_queues();
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
        self.report_leases();
        // Leadership ends: remove this dispatcher's share of the ready gauges.
        for (g, reported) in self.ready_gauges.values_mut() {
            move_gauge(g, reported, 0);
        }
        if let Some(g) = &self.cfg.gauges {
            g.remove_partition(self.ctx.partition);
        }
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

/// The `ostro_queue_ready{queue}` handle (registered, so the series renders at 0).
fn ready_gauge(queue: &str) -> metrics::Gauge {
    let g = metrics::gauge!("ostro_queue_ready", "queue" => queue.to_owned());
    g.increment(0.0);
    g
}

/// Move a shared gauge from this owner's `reported` share to `now`.
fn move_gauge(g: &metrics::Gauge, reported: &mut usize, now: usize) {
    if now > *reported {
        g.increment((now - *reported) as f64);
    } else if now < *reported {
        g.decrement((*reported - now) as f64);
    }
    *reported = now;
}

/// Help text of the queue series (idempotent). `ostro_queue_worker_demand` is described
/// only: Phase 7 (`workers/demand.rs`) owns its value.
fn describe_queue_metrics() {
    metrics::describe_gauge!(
        "ostro_flow_rebuild_entries",
        "Entries (queued, delayed and leased) after a dispatcher rebuild, per partition."
    );
    metrics::describe_histogram!(
        "ostro_flow_rebuild_seconds",
        metrics::Unit::Seconds,
        "Duration of a dispatcher rebuild scan, per partition."
    );
    metrics::describe_gauge!(
        "ostro_queue_ready",
        "Ready (due) runs waiting for an executor slot, per queue."
    );
    metrics::describe_histogram!(
        "ostro_queue_lag_seconds",
        metrics::Unit::Seconds,
        "Time from a run's ready_at to its attempt grant, per queue."
    );
    metrics::describe_counter!(
        "ostro_runs_started_total",
        "Runs granted their first activation, per workflow."
    );
    metrics::describe_gauge!(
        "ostro_queue_worker_demand",
        "Worker demand per queue (set by Phase 7 workers/demand.rs; KEDA input)."
    );
}

/// The pipelined grant (see the module docs): wait for an earlier unacknowledged grant of
/// `run` (`after`), read the record once, append `AttemptGranted{n + 1}` and return as soon
/// as the log fixed its offset. The returned record is the one read, patched as apply will
/// patch it (token, `Running`, no ready slot), so the job needs no second read. `floor` is
/// the token of an earlier rejected grant of the run (its job may still be alive): the new
/// token is above it. `fault` (test hook) stamps the token with a stale epoch so apply
/// rejects the grant.
async fn pipelined_grant(
    h: &PartitionHandle<PartitionLog>,
    run: RunId,
    after: Option<Offset>,
    floor: Option<AttemptToken>,
    fault: bool,
) -> Result<Grant, PartitionError> {
    if let Some(off) = after {
        h.machine().wait_applied(off).await?;
    }
    let mut rec = h
        .read_run(&run)?
        .ok_or(PartitionError::Rejected(RejectReason::RunNotFound))?;
    if rec.status.is_terminal() {
        return Err(PartitionError::Rejected(RejectReason::RunTerminal));
    }
    if rec.pause.is_some() {
        // Apply would reject it `RunPaused`; log nothing (OPS-01).
        return Err(PartitionError::Rejected(RejectReason::RunPaused));
    }
    let epoch = h.epoch();
    let n = match rec.current_attempt {
        Some((e, n)) if e == epoch.0 => n.saturating_add(1),
        _ => 1,
    };
    // Above a rejected grant's token whose job may still run (same epoch).
    let n = match floor {
        Some(f) if f.epoch == epoch => n.max(f.n.saturating_add(1)),
        _ => n,
    };
    let stamped = if fault {
        epoch.0.wrapping_sub(1)
    } else {
        epoch.0
    };
    let cmd = Command::attempt_granted(AttemptGranted {
        run_id: run.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: stamped, n }),
        after_failure: false,
    });
    h.machine().check_healthy()?;
    failpoint!("dispatcher::before_grant")?;
    let receipt = h.log().append(epoch, vec![encode_command(&cmd)]).await?;
    rec.current_attempt = Some((epoch.0, n));
    rec.status = RunStatus::Running;
    rec.ready = None;
    Ok(Grant {
        token: AttemptToken { epoch, n },
        record: Box::new(rec),
        pending: Some(receipt.last),
    })
}

/// A dispatcher failpoint (module docs): `Ok` unless it is configured with `return`, which
/// yields an EIO (or, with `return(enospc)`, ENOSPC) log I/O error.
#[cfg(feature = "failpoints")]
fn failpoint_eval(name: &str) -> Result<(), PartitionError> {
    // POSIX errno values (identical on Linux and macOS).
    const EIO: i32 = 5;
    const ENOSPC: i32 = 28;
    match fail::eval(name, |arg| arg) {
        None => Ok(()),
        Some(arg) => {
            let errno = if arg.as_deref() == Some("enospc") {
                ENOSPC
            } else {
                EIO
            };
            Err(PartitionError::Log(ostro_log::LogError::Io(
                std::io::Error::from_raw_os_error(errno),
            )))
        }
    }
}

/// Read check for an `EnqueueReady` seen while `run`'s pipelined grant `token` is not yet
/// acknowledged: `true` = the run was requeued after the grant (record holds the token and
/// is `Ready`); a read error also counts as a requeue (the lease is dropped, which is safe).
fn requeued_after_grant(
    h: &PartitionHandle<PartitionLog>,
    run: &RunId,
    token: AttemptToken,
) -> bool {
    match h.read_run(run) {
        Ok(Some(r)) => {
            r.current_attempt == Some((token.epoch.0, token.n)) && r.status == RunStatus::Ready
        }
        Ok(None) | Err(_) => true,
    }
}

/// [`PartitionHandle::grant_attempt`] with `after_failure = true` (F9): the next attempt
/// `n + 1` of the current epoch, counted in `RunRecord.attempt_failures` by apply.
async fn grant_after_failure(
    h: &PartitionHandle<PartitionLog>,
    run: RunId,
) -> Result<AttemptToken, PartitionError> {
    let epoch = h.epoch();
    let n = match h.read_run(&run)?.and_then(|r| r.current_attempt) {
        Some((e, n)) if e == epoch.0 => n.saturating_add(1),
        _ => 1,
    };
    let cmd = Command::attempt_granted(AttemptGranted {
        run_id: run.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: epoch.0, n }),
        after_failure: true,
    });
    match h.submit_one(cmd).await? {
        ApplyOutcome::Accepted => Ok(AttemptToken { epoch, n }),
        ApplyOutcome::Rejected(r) => Err(PartitionError::Rejected(r)),
        ApplyOutcome::Duplicate { .. } => Err(PartitionError::Rejected(RejectReason::Fenced {
            current: None,
        })),
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

    /// `is_requeue` for tests without unacknowledged pipelined grants.
    fn never(_: &RunId, _: AttemptToken) -> bool {
        false
    }

    fn res() -> Reservation {
        Reservation::new(ReservationKind::Embedded, ())
    }

    fn entry(q: &str, priority: i32, at: u64, ck: &str) -> Entry {
        Entry {
            queue: q.into(),
            priority,
            ready_at_ms: at,
            concurrency_key: ck.into(),
            workflow: String::new(),
            tenant: None,
            first_activation: false,
        }
    }

    fn enqueue_action(run: RunId, q: &str, priority: i32, at: u64, ck: &str) -> Action {
        Action::EnqueueReady {
            run,
            queue: q.into(),
            priority,
            ready_at_ms: at,
            concurrency_key: ck.into(),
            workflow: String::new(),
            tenant: None,
            first_activation: false,
        }
    }

    /// A lease in phase Executing; returns the job's inputs receiver.
    fn leased(st: &mut State, r: RunId) -> mpsc::UnboundedReceiver<u64> {
        st.new_lease(r, entry("q", 0, 0, ""), FlowPermit::none(), res());
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
            &mut never,
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
            &mut never,
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
        st.new_lease(a, entry("q", 0, 0, ""), FlowPermit::none(), res());
        st.on_actions(&[enqueue_action(a, "q", 0, 1, "")], 10, &mut never);
        assert!(st.leases.contains_key(&a), "granting lease kept");
        assert!(st.entries.is_empty());

        let _rx = leased(&mut st, b);
        let revoked = st.leases[&b].revoked.subscribe();
        st.on_actions(&[enqueue_action(b, "q", 0, 1, "ck")], 10, &mut never);
        assert!(!st.leases.contains_key(&b));
        assert!(*revoked.borrow());
        assert_eq!(st.entries[&b].concurrency_key, "ck");
    }

    #[tokio::test]
    async fn run_completed_drops_lease_and_entry() {
        let mut st = State::new(Duration::from_secs(30));
        let (a, b) = (run(), run());
        let _rx = leased(&mut st, a);
        let revoked = st.leases[&a].revoked.subscribe();
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
            &mut never,
        );
        assert!(st.leases.is_empty() && st.entries.is_empty());
        assert!(*revoked.borrow(), "a completed run's lease is revoked");
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
                    ..Default::default()
                }],
                running: vec![(b, Some(entry("q2", 3, 55, "ck-run")))],
            },
            100,
            &mut never,
        );
        assert_eq!(st.entries[&a].concurrency_key, "ck-ready");
        assert_eq!(st.entries[&b], entry("q2", 3, 0, "ck-run"));
    }

    /// Phase 7 F3/F7: the rebuild takes workflow, tenant and first activation from the ready
    /// rows alone (`apply_scan` gets no store handle, so it cannot read a run record), and
    /// `EnqueueReady` actions carry them the same way.
    #[tokio::test]
    async fn ready_rows_and_actions_carry_workflow_tenant_and_first_activation() {
        let mut st = State::new(Duration::from_secs(30));
        let (a, b, c) = (run(), run(), run());
        st.apply_scan(
            Scan {
                ready: vec![
                    query::ReadyEntryRow {
                        queue: "q".into(),
                        run: a,
                        workflow: "wf-a".into(),
                        tenant: Some("acme".into()),
                        first_activation: true,
                        ..Default::default()
                    },
                    query::ReadyEntryRow {
                        queue: "q".into(),
                        run: b,
                        workflow: "wf-b".into(),
                        ..Default::default()
                    },
                ],
                running: vec![],
            },
            100,
            &mut never,
        );
        let ea = &st.entries[&a];
        assert_eq!(
            (
                ea.workflow.as_str(),
                ea.tenant.as_deref(),
                ea.first_activation
            ),
            ("wf-a", Some("acme"), true)
        );
        let eb = &st.entries[&b];
        assert_eq!(
            (
                eb.workflow.as_str(),
                eb.tenant.as_deref(),
                eb.first_activation
            ),
            ("wf-b", None, false)
        );
        st.on_actions(
            &[Action::EnqueueReady {
                run: c,
                queue: "q".into(),
                priority: 0,
                ready_at_ms: 0,
                concurrency_key: String::new(),
                workflow: "wf-c".into(),
                tenant: Some("t".into()),
                first_activation: true,
            }],
            100,
            &mut never,
        );
        let ec = &st.entries[&c];
        assert_eq!(
            (
                ec.workflow.as_str(),
                ec.tenant.as_deref(),
                ec.first_activation
            ),
            ("wf-c", Some("t"), true)
        );
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
