//! `EngineExecutor` (plan 04-16): the [`ostro_partition::Executor`] that runs dispatcher jobs
//! as Wasm activations on the engine [`Runtime`].
//!
//! # One job = one activation
//!
//! For every [`Job`] (one leased attempt token; a `Reactivate` is a new job with the same
//! token):
//!
//! 1. **Module:** `record.module_sha` is required. The prepared module comes from the
//!    runtime's L2 cache ([`Runtime::prepared`]) or from the blob store +
//!    [`Runtime::prepare_cached`]. No pin, a missing blob or an invalid component fails the
//!    run (`CompleteRun{Failed}`, JSON reason) because no retry can fix it.
//! 2. **Sink:** one [`PartitionSink`] holding the job's token; every accepted append renews
//!    the lease.
//! 3. **First activation:** when the journal is empty (`next_seq == 0`) the executor
//!    appends `RunStarted` (seq 0) built from the run record (`build_run_started`).
//! 4. **Journal:** read from the store on every job (inputs sent between jobs are not
//!    replayed by the dispatcher), then `repair_unsupported`.
//! 5. **Inputs (ordered bridge):** the dispatcher's `InputAppended` seqs and the sink's own
//!    unsupported completions are both only *wake-ups*. One bridge task reads the journal
//!    strictly in seq order from the end of the loaded journal and forwards every input
//!    event (`completes_op()`) to `Activate.inputs`, exactly once and in order. This closes
//!    the 04-15 race (a dispatcher-forwarded `TimerFired` and a sink completion sent out of
//!    seq order made the engine report `BadJournal`), and skips inputs that are already in
//!    the loaded journal (journaled between `AttemptGranted` and the load).
//! 6. **Lease:** a watcher task bridges `job.revoked` to the activation's
//!    [`CancellationToken`], renews on a heartbeat (`heartbeat`), and self-fences
//!    (Pitfall 14): when the local deadline `min(lease.expires_at(), last renewal +
//!    lease_ttl) - self_fence_margin` passes without a renewal it cancels the activation
//!    itself. Once cancelled, the executor does no more I/O for the job: engine appends are
//!    refused locally (`AppendError::Fenced`) and no `Suspend`/`EnqueueReady`/failure is
//!    submitted. A self-fenced job then waits for the dispatcher to revoke the lease (it
//!    is no longer renewed, so that happens within the dispatcher's TTL) before returning;
//!    outcomes of revoked leases are ignored by the dispatcher, which re-grants `n + 1`.
//!
//! # Outcome state machine
//!
//! | activation outcome | executor action | `ExecOutcome` |
//! |---|---|---|
//! | any, after revoke / self-fence | nothing more is submitted | `Released` (ignored) |
//! | a terminal `CompleteRun` was accepted (`sink.is_terminal()`) | - | `Released` |
//! | `Evicted{WaitUnsatisfied}` | `Suspend` CAS at the engine's journal end | `Released`; `Conflict` -> `Reactivate` |
//! | `Evicted{StepBackoff{retry_at}}` | `EnqueueReady{ceil_ms(retry_at)}` CAS likewise | `Released`; `Conflict` -> `Reactivate` |
//! | `Evicted{HotWaitElapsed \| BudgetPressure \| AppendConflict \| StepInterrupted}` | - | `Reactivate` |
//! | `Evicted{External}` without revoke (flush `Unavailable`) | short pause | `Reactivate` |
//! | `AttemptFailed(f)`, `f.retryable()` and `failures + 1 < max_attempt_failures` | - | `Retry{after_ms}` |
//! | `AttemptFailed(f)` otherwise (incl. NonDeterminism / ProtocolViolation) | `CompleteRun{Failed, failure JSON}` | `Released` |
//! | `Fenced` | - | `Fenced` |
//!
//! The CAS `from_seq` is the journal end as the **engine** saw it (initially the loaded
//! journal's end, then the `next` of each accepted engine append), never the sink's
//! `next_seq`: an input journaled after the engine's last append (a `TimerFired`, or this
//! sink's own unsupported completion) makes the CAS fail with `SeqMismatch`, and the run is
//! re-activated instead of suspended over an unconsumed input (04-13). A `Suspend`
//! answered `SeqMismatch` is never re-submitted at the updated end.
//!
//! `max_attempt_failures` counts failed attempts including the last one: with 3, attempts
//! 1 and 2 are retried and the third failure completes the run `Failed` (no journal event;
//! the reason is the run result `{"reason_code", "kind", "message"}`).
//! The retry delay is `retry_backoff * 2^failures`, capped at [`MAX_RETRY_BACKOFF`].

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use ostro_engine::error::{AttemptFailure, EvictReason};
use ostro_engine::runtime::{Prepared, Runtime};
use ostro_engine::sink::{Activate, ActivationOutcome, ActivationSink, AppendError, RunRef};
use ostro_partition::{ExecOutcome, Executor, Job, LeaseRenewer, PartitionHandle, query};
use ostro_types::command::{CompleteRun, Derived, EnqueueReady, TerminalStatus, derived::Intent};
use ostro_types::journal::Event;
use ostro_types::{AttemptToken, RunId};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::derive;
use crate::journal_io;
use crate::run_started::{build_run_started, input_payload};
use crate::sink::{PartitionSink, RenewFn};

/// Upper bound of the attempt retry delay.
pub const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(60);
/// Attempts to submit the failure `CompleteRun` when the journal end keeps moving.
const FAIL_RETRIES: usize = 4;
/// Pause before re-activating after the partition was unavailable.
const UNAVAILABLE_PAUSE: Duration = Duration::from_millis(100);

/// Reason codes of run failures decided by the executor (not by an attempt failure).
pub const REASON_NO_MODULE: &str = "no_module";
pub const REASON_MODULE_NOT_FOUND: &str = "module_not_found";
pub const REASON_MODULE_INVALID: &str = "module_invalid";
pub const REASON_BAD_RECORD: &str = "bad_run_record";

/// Executor tuning.
#[derive(Clone, Debug)]
pub struct ExecutorConfig {
    /// Lease renewal interval while a job is alive (embedded mode: keeps long activations
    /// leased; `lease_ttl` then only catches stuck executors).
    pub heartbeat: Duration,
    /// The executor's own view of the lease TTL (normally the dispatcher's).
    pub lease_ttl: Duration,
    /// The executor self-fences this long before its lease would expire.
    pub self_fence_margin: Duration,
    /// Failed attempts (including the last) before the run is completed `Failed`.
    pub max_attempt_failures: u32,
    /// Base delay of the attempt retry backoff.
    pub retry_backoff: Duration,
}

impl ExecutorConfig {
    /// Heartbeat `ttl / 3`, margin `ttl / 10`, 3 attempts, 1 s backoff.
    pub fn for_lease_ttl(lease_ttl: Duration) -> Self {
        Self {
            heartbeat: lease_ttl / 3,
            lease_ttl,
            self_fence_margin: lease_ttl / 10,
            max_attempt_failures: 3,
            retry_backoff: Duration::from_secs(1),
        }
    }

    /// Delay of the retry after `failures` earlier consecutive failed attempts.
    pub fn retry_delay(&self, failures: u32) -> Duration {
        let factor = 1u32.checked_shl(failures.min(31)).unwrap_or(u32::MAX);
        self.retry_backoff
            .saturating_mul(factor)
            .min(MAX_RETRY_BACKOFF)
    }
}

impl Default for ExecutorConfig {
    /// For the dispatcher's default 30 s lease.
    fn default() -> Self {
        Self::for_lease_ttl(ostro_partition::leader::dispatcher::DEFAULT_LEASE_TTL)
    }
}

/// What the executor saw of one finished job (tests).
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct JobRecord {
    pub run: RunId,
    pub token: AttemptToken,
    /// `None` when no activation ran (module failure, RunStarted fenced, ...).
    pub activation: Option<ActivationOutcome>,
    pub outcome: ExecOutcome,
    pub revoked: bool,
    pub self_fenced: bool,
}

/// Test hooks (not part of the product surface).
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct ExecutorHooks {
    /// Activations started.
    pub activations: AtomicU64,
    /// Self-fences (local deadline passed without renewal).
    pub self_fences: AtomicU64,
    /// When set, every engine append first takes (and returns) one permit; a gate with 0
    /// permits stalls appends. A cancelled job stops waiting (its append is refused).
    pub pause_before_append: Mutex<Option<Arc<Semaphore>>>,
    /// Skip heartbeat renewals.
    pub suppress_heartbeat: AtomicBool,
    /// Restrict `pause_before_append` / `suppress_heartbeat` to this attempt number
    /// (0 = every attempt).
    pub only_attempt: AtomicU32,
    /// One record per finished job.
    pub jobs: Mutex<Vec<JobRecord>>,
}

impl ExecutorHooks {
    fn applies(&self, n: u32) -> bool {
        let only = self.only_attempt.load(Ordering::Acquire);
        only == 0 || only == n
    }

    fn gate(&self, n: u32) -> Option<Arc<Semaphore>> {
        if !self.applies(n) {
            return None;
        }
        self.pause_before_append
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    fn heartbeat_suppressed(&self, n: u32) -> bool {
        self.applies(n) && self.suppress_heartbeat.load(Ordering::Acquire)
    }

    /// Records of finished jobs.
    pub fn jobs(&self) -> MutexGuard<'_, Vec<JobRecord>> {
        self.jobs.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// The engine-backed dispatcher executor.
#[derive(Clone)]
pub struct EngineExecutor {
    runtime: Arc<Runtime>,
    cfg: ExecutorConfig,
    hooks: Arc<ExecutorHooks>,
}

impl std::fmt::Debug for EngineExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineExecutor")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

impl EngineExecutor {
    pub fn new(runtime: Arc<Runtime>, cfg: ExecutorConfig) -> Self {
        Self {
            runtime,
            cfg,
            hooks: Arc::new(ExecutorHooks::default()),
        }
    }

    /// Share `hooks` (tests).
    #[doc(hidden)]
    pub fn with_hooks(mut self, hooks: Arc<ExecutorHooks>) -> Self {
        self.hooks = hooks;
        self
    }

    #[doc(hidden)]
    pub fn hooks(&self) -> &Arc<ExecutorHooks> {
        &self.hooks
    }

    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    pub fn config(&self) -> &ExecutorConfig {
        &self.cfg
    }
}

impl Executor for EngineExecutor {
    fn execute(&self, job: Job) -> Pin<Box<dyn Future<Output = ExecOutcome> + Send + 'static>> {
        let me = self.clone();
        Box::pin(async move { me.run_job(job).await })
    }
}

// ------------------------------------------------------------------- lease watcher

/// Lease state shared by the watcher, the renew callback and the job.
struct LeaseState {
    lease: LeaseRenewer,
    base: Instant,
    /// Last successful renewal, ms since `base`.
    last_renew_ms: AtomicU64,
    ttl: Duration,
    margin: Duration,
    self_fenced: AtomicBool,
}

impl LeaseState {
    fn renew(&self) -> bool {
        if self.lease.renew() {
            let ms = u64::try_from(self.base.elapsed().as_millis()).unwrap_or(u64::MAX);
            self.last_renew_ms.fetch_max(ms, Ordering::AcqRel);
            true
        } else {
            false
        }
    }

    /// Local self-fence deadline.
    fn deadline(&self) -> Instant {
        let local = self.base
            + Duration::from_millis(self.last_renew_ms.load(Ordering::Acquire))
            + self.ttl;
        let at = local.min(self.lease.expires_at());
        at.checked_sub(self.margin).unwrap_or(at)
    }
}

/// Bridge revoke -> cancel, heartbeat renewals, local self-fence.
async fn watch_lease(
    st: Arc<LeaseState>,
    mut revoked: watch::Receiver<bool>,
    cancel: CancellationToken,
    heartbeat: Duration,
    suppress: bool,
    hooks: Arc<ExecutorHooks>,
) {
    let heartbeat = heartbeat.max(Duration::from_millis(1));
    let mut next_hb = Instant::now() + heartbeat;
    loop {
        let deadline = st.deadline();
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            _ = revoked.wait_for(|v| *v) => {
                // Revoked, or the dispatcher dropped the lease sender: stop either way.
                cancel.cancel();
                return;
            }
            _ = tokio::time::sleep_until(deadline) => {
                if Instant::now() >= st.deadline() {
                    st.self_fenced.store(true, Ordering::Release);
                    hooks.self_fences.fetch_add(1, Ordering::Relaxed);
                    debug!("executor self-fenced: lease not renewed before the local deadline");
                    cancel.cancel();
                    return;
                }
            }
            _ = tokio::time::sleep_until(next_hb) => {
                next_hb = Instant::now() + heartbeat;
                if !suppress && !st.renew() {
                    cancel.cancel();
                    return;
                }
            }
        }
    }
}

/// Aborts the spawned helper tasks of a job when dropped.
struct TaskGuard(Vec<JoinHandle<()>>);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        for t in &self.0 {
            t.abort();
        }
    }
}

// ------------------------------------------------------------------- input bridge

/// Forward input events to the engine strictly in journal order. `seqs` (dispatcher
/// `InputAppended`) and `sink_inputs` (the sink's unsupported completions) are wake-ups
/// only: on each, the journal is read from `next` to its end and every input event in it
/// is forwarded once. Public for the ordering test only.
#[doc(hidden)]
pub async fn input_bridge(
    handle: PartitionHandle<ostro_log::Rf1Log>,
    run: RunId,
    mut next: u64,
    mut seqs: mpsc::UnboundedReceiver<u64>,
    mut sink_inputs: mpsc::UnboundedReceiver<Event>,
    tx: mpsc::UnboundedSender<Event>,
) {
    let (mut seqs_open, mut sink_open) = (true, true);
    while seqs_open || sink_open {
        let hint = tokio::select! {
            s = seqs.recv(), if seqs_open => match s {
                Some(s) => s,
                None => { seqs_open = false; continue; }
            },
            e = sink_inputs.recv(), if sink_open => match e {
                Some(e) => e.seq,
                None => { sink_open = false; continue; }
            },
        };
        if hint < next {
            continue;
        }
        match journal_io::read_journal_from(&handle, &run, next).await {
            Ok(events) => {
                for ev in events {
                    next = ev.seq + 1;
                    if ev.body.completes_op().is_some() && tx.send(ev).is_err() {
                        return;
                    }
                }
            }
            Err(e) => {
                // The engine's next append conflicts and the run is re-activated.
                warn!(%run, error = %e, "input bridge: journal read failed; inputs stop");
                return;
            }
        }
    }
}

// ------------------------------------------------------------------- engine sink wrapper

/// The sink the engine sees: refuses appends once the job is cancelled (self-fence /
/// revoke: no I/O after the fence), applies the test gate, and tracks the journal end as
/// the engine saw it.
struct EngineSink {
    inner: Arc<PartitionSink>,
    cancel: CancellationToken,
    gate: Option<Arc<Semaphore>>,
    engine_end: AtomicU64,
}

#[async_trait::async_trait]
impl ActivationSink for EngineSink {
    async fn append(
        &self,
        run: &RunRef,
        attempt: u32,
        from_seq: u64,
        events: Vec<Event>,
    ) -> Result<u64, AppendError> {
        if let Some(g) = &self.gate {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return Err(AppendError::Fenced),
                p = g.acquire() => drop(p),
            }
        }
        if self.cancel.is_cancelled() {
            return Err(AppendError::Fenced);
        }
        let r = self.inner.append(run, attempt, from_seq, events).await;
        if let Ok(next) = r {
            self.engine_end.store(next, Ordering::Release);
        }
        r
    }

    async fn fetch_blob(&self, sha: &[u8; 32]) -> Result<Vec<u8>, AppendError> {
        self.inner.fetch_blob(sha).await
    }

    async fn done(&self, run: &RunRef, attempt: u32, outcome: ActivationOutcome) {
        self.inner.done(run, attempt, outcome).await;
    }
}

// ------------------------------------------------------------------- the job

/// Why `RunStarted` could not be built.
enum StartErr {
    /// The record cannot produce one (bad seed / parent): fail the run.
    Fatal(String),
    /// Store or payload read failure: re-activate.
    Transient(String),
}

/// Why a job could not reach the engine.
enum Prep {
    Ready(Arc<Prepared>, [u8; 32]),
    /// Fail the run: (reason_code, message).
    Fail(&'static str, String),
}

impl EngineExecutor {
    async fn run_job(&self, job: Job) -> ExecOutcome {
        let Job {
            run,
            token,
            record,
            handle,
            inputs: seqs,
            revoked,
            lease,
            failures,
            ..
        } = job;
        if record.status.is_terminal() {
            return ExecOutcome::Released;
        }
        let cancel = CancellationToken::new();
        let st = Arc::new(LeaseState {
            lease,
            base: Instant::now(),
            last_renew_ms: AtomicU64::new(0),
            ttl: self.cfg.lease_ttl,
            margin: self.cfg.self_fence_margin,
            self_fenced: AtomicBool::new(false),
        });
        let mut guard = TaskGuard(vec![tokio::spawn(watch_lease(
            Arc::clone(&st),
            revoked.clone(),
            cancel.clone(),
            self.cfg.heartbeat,
            self.hooks.heartbeat_suppressed(token.n),
            Arc::clone(&self.hooks),
        ))]);
        let renew_st = Arc::clone(&st);
        let on_renew: RenewFn = Arc::new(move || {
            renew_st.renew();
        });
        let (sink_tx, sink_rx) = mpsc::unbounded_channel();
        let sink = Arc::new(PartitionSink::new(
            handle.clone(),
            run,
            token,
            Some(sink_tx),
            Some(on_renew),
        ));
        let rref = RunRef {
            partition: run.partition().0,
            run_id: run.to_string(),
        };

        let (activation, outcome) = self
            .drive(
                &handle, run, token, &record, &sink, &rref, &cancel, seqs, sink_rx, failures,
                &mut guard,
            )
            .await;
        drop(guard);

        let is_revoked = *revoked.borrow();
        let self_fenced = st.self_fenced.load(Ordering::Acquire);
        let outcome = if sink.is_terminal() {
            ExecOutcome::Released
        } else if is_revoked || self_fenced || cancel.is_cancelled() {
            if !is_revoked {
                // Self-fenced: the lease is no longer renewed, so the dispatcher revokes it
                // within its TTL and re-grants n + 1 (the value returned here is ignored).
                let mut r = revoked.clone();
                let _ = r.wait_for(|v| *v).await;
            }
            ExecOutcome::Released
        } else {
            outcome
        };
        self.hooks.jobs().push(JobRecord {
            run,
            token,
            activation,
            outcome,
            revoked: *revoked.borrow(),
            self_fenced,
        });
        outcome
    }

    #[allow(clippy::too_many_arguments)]
    async fn drive(
        &self,
        handle: &PartitionHandle<ostro_log::Rf1Log>,
        run: RunId,
        token: AttemptToken,
        record: &ostro_partition::store::values::RunRecord,
        sink: &Arc<PartitionSink>,
        rref: &RunRef,
        cancel: &CancellationToken,
        seqs: mpsc::UnboundedReceiver<u64>,
        sink_rx: mpsc::UnboundedReceiver<Event>,
        failures: u32,
        guard: &mut TaskGuard,
    ) -> (Option<ActivationOutcome>, ExecOutcome) {
        // 1. Module.
        let (prepared, sha) = match self.module(record, sink).await {
            Prep::Ready(p, sha) => (p, sha),
            Prep::Fail(code, msg) => return (None, self.fail_run(sink, code, code, &msg).await),
        };
        if cancel.is_cancelled() {
            return (None, ExecOutcome::Released);
        }
        // 2. RunStarted on the first activation.
        if sink.next_seq() == 0 {
            match self.run_started(handle, run, record, &sha).await {
                Ok(ev) => match sink.append(rref, token.n, 0, vec![ev]).await {
                    Ok(_) => {}
                    Err(AppendError::Fenced) => return (None, ExecOutcome::Fenced),
                    Err(AppendError::Conflict { .. }) => return (None, ExecOutcome::Reactivate),
                    Err(AppendError::Unavailable(e)) => {
                        warn!(%run, error = %e, "RunStarted append failed; re-activating");
                        tokio::time::sleep(UNAVAILABLE_PAUSE).await;
                        return (None, ExecOutcome::Reactivate);
                    }
                },
                Err(StartErr::Fatal(msg)) => {
                    let out = self
                        .fail_run(sink, REASON_BAD_RECORD, REASON_BAD_RECORD, &msg)
                        .await;
                    return (None, out);
                }
                Err(StartErr::Transient(msg)) => {
                    warn!(%run, error = %msg, "RunStarted input read failed; re-activating");
                    tokio::time::sleep(UNAVAILABLE_PAUSE).await;
                    return (None, ExecOutcome::Reactivate);
                }
            }
        }
        // 3. Journal (+ repair of unsupported completions).
        let journal = match self.load_journal(handle, run, sink).await {
            Ok(j) => j,
            Err(e) => {
                warn!(%run, error = %e, "journal load failed");
                tokio::time::sleep(UNAVAILABLE_PAUSE).await;
                return (None, ExecOutcome::Reactivate);
            }
        };
        if cancel.is_cancelled() {
            return (None, ExecOutcome::Released);
        }
        let end = journal.len() as u64;
        // 4. Ordered input bridge.
        let (eng_tx, eng_rx) = mpsc::unbounded_channel();
        guard.0.push(tokio::spawn(input_bridge(
            handle.clone(),
            run,
            end,
            seqs,
            sink_rx,
            eng_tx,
        )));
        // 5. Activate.
        let esink = Arc::new(EngineSink {
            inner: Arc::clone(sink),
            cancel: cancel.clone(),
            gate: self.hooks.gate(token.n),
            engine_end: AtomicU64::new(end),
        });
        self.hooks.activations.fetch_add(1, Ordering::Relaxed);
        let act = Activate {
            run: rref.clone(),
            attempt: token.n,
            module_sha: sha,
            journal,
            inputs: Some(eng_rx),
            cancel: cancel.clone(),
        };
        let outcome = self
            .runtime
            .activate(
                &prepared,
                act,
                Arc::clone(&esink) as Arc<dyn ActivationSink>,
            )
            .await;
        if sink.is_terminal() || cancel.is_cancelled() {
            return (Some(outcome), ExecOutcome::Released);
        }
        let engine_end = esink.engine_end.load(Ordering::Acquire);
        let exec = self.map_outcome(sink, &outcome, engine_end, failures).await;
        (Some(outcome), exec)
    }

    async fn map_outcome(
        &self,
        sink: &PartitionSink,
        outcome: &ActivationOutcome,
        engine_end: u64,
        failures: u32,
    ) -> ExecOutcome {
        match outcome {
            // The terminal append went through the sink; is_terminal() is checked first, so
            // reaching here means the run ended some other way (e.g. already terminal).
            ActivationOutcome::Completed { .. }
            | ActivationOutcome::Failed { .. }
            | ActivationOutcome::ContinuedAsNew { .. } => ExecOutcome::Released,
            ActivationOutcome::Fenced => ExecOutcome::Fenced,
            ActivationOutcome::Evicted { reason } => match reason {
                EvictReason::WaitUnsatisfied => {
                    self.cas(sink, engine_end, Derived::suspend()).await
                }
                EvictReason::StepBackoff { retry_at_nanos } => {
                    let intent = Intent::EnqueueReady(EnqueueReady {
                        ready_at_ms: derive::ceil_ms(*retry_at_nanos),
                    });
                    self.cas(sink, engine_end, Derived::new(intent)).await
                }
                EvictReason::HotWaitElapsed
                | EvictReason::BudgetPressure
                | EvictReason::AppendConflict
                | EvictReason::StepInterrupted => ExecOutcome::Reactivate,
                // Not revoked (checked by the caller): a flush hit an unavailable partition.
                EvictReason::External => {
                    tokio::time::sleep(UNAVAILABLE_PAUSE).await;
                    ExecOutcome::Reactivate
                }
            },
            ActivationOutcome::AttemptFailed(f) => self.attempt_failed(sink, f, failures).await,
        }
    }

    /// `Suspend` / `EnqueueReady` compare-and-set at the engine's journal end.
    async fn cas(&self, sink: &PartitionSink, from_seq: u64, d: Derived) -> ExecOutcome {
        match sink.submit_intents_at(from_seq, vec![d]).await {
            // EnqueueReady: the dispatcher revokes this lease right after the append and
            // queues the entry; Suspend: the run waits for an input. Either way: Released.
            Ok(_) => ExecOutcome::Released,
            // An input landed after the engine's view: never re-submit at the new end
            // (lost wake-up); re-activate so the input is replayed.
            Err(AppendError::Conflict { .. }) => ExecOutcome::Reactivate,
            Err(AppendError::Fenced) => ExecOutcome::Fenced,
            Err(AppendError::Unavailable(e)) => {
                warn!(run = %sink.run(), error = %e, "suspend/requeue submit failed; re-activating");
                tokio::time::sleep(UNAVAILABLE_PAUSE).await;
                ExecOutcome::Reactivate
            }
        }
    }

    async fn attempt_failed(
        &self,
        sink: &PartitionSink,
        f: &AttemptFailure,
        failures: u32,
    ) -> ExecOutcome {
        if f.retryable() && failures.saturating_add(1) < self.cfg.max_attempt_failures {
            let after = self.cfg.retry_delay(failures);
            return ExecOutcome::Retry {
                after_ms: u64::try_from(after.as_millis()).unwrap_or(u64::MAX),
            };
        }
        self.fail_run(sink, f.reason_code(), f.reason_code(), &f.dlq_reason())
            .await
    }

    /// Complete the run `Failed` with a JSON reason (no journal event).
    async fn fail_run(
        &self,
        sink: &PartitionSink,
        reason_code: &str,
        kind: &str,
        message: &str,
    ) -> ExecOutcome {
        let bytes = derive::failure_json(reason_code, kind, message);
        let result = match sink
            .handle()
            .prepare_payload(bytes::Bytes::from(bytes))
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(run = %sink.run(), error = %e, "failure payload; re-activating");
                tokio::time::sleep(UNAVAILABLE_PAUSE).await;
                return ExecOutcome::Reactivate;
            }
        };
        let completed_at_ms = derive::ceil_ms(self.runtime.clock().now_unix_nanos());
        for _ in 0..FAIL_RETRIES {
            let d = Derived::new(Intent::CompleteRun(CompleteRun {
                status: TerminalStatus::Failed as i32,
                result: Some(result.clone()),
                completed_at_ms,
                resumable: false,
                parent_event: None,
            }));
            match sink.submit_intents(vec![d]).await {
                Ok(_) => return ExecOutcome::Released,
                // next_seq followed `expected`; an input does not change the verdict.
                Err(AppendError::Conflict { .. }) => continue,
                Err(AppendError::Fenced) => return ExecOutcome::Fenced,
                Err(AppendError::Unavailable(e)) => {
                    warn!(run = %sink.run(), error = %e, "failure completion; re-activating");
                    tokio::time::sleep(UNAVAILABLE_PAUSE).await;
                    return ExecOutcome::Reactivate;
                }
            }
        }
        ExecOutcome::Reactivate
    }

    async fn module(
        &self,
        record: &ostro_partition::store::values::RunRecord,
        sink: &PartitionSink,
    ) -> Prep {
        let Some(sha) = record.module_sha else {
            return Prep::Fail(REASON_NO_MODULE, "no module pinned".to_string());
        };
        if let Some(p) = self.runtime.prepared(&sha) {
            return Prep::Ready(p, sha);
        }
        let wasm = match sink.fetch_blob(&sha).await {
            Ok(w) => w,
            Err(e) => {
                return Prep::Fail(
                    REASON_MODULE_NOT_FOUND,
                    format!("module {} not found: {e}", ostro_blob::hex(&sha)),
                );
            }
        };
        match self.runtime.prepare_cached(&wasm).await {
            Ok(p) => Prep::Ready(p, sha),
            Err(e) => Prep::Fail(
                REASON_MODULE_INVALID,
                format!("module {}: {e}", ostro_blob::hex(&sha)),
            ),
        }
    }

    async fn run_started(
        &self,
        handle: &PartitionHandle<ostro_log::Rf1Log>,
        run: RunId,
        record: &ostro_partition::store::values::RunRecord,
        sha: &[u8; 32],
    ) -> Result<Event, StartErr> {
        let reader = handle.reader();
        let pref = tokio::task::spawn_blocking(move || {
            query::run_payload(&reader, &run, query::PAYLOAD_INPUT)
        })
        .await
        .map_err(|e| StartErr::Transient(format!("input read task: {e}")))?
        .map_err(|e| StartErr::Transient(format!("input read: {e}")))?;
        let bytes = match pref {
            Some(p) => handle
                .fetch_payload(&p)
                .await
                .map_err(|e| StartErr::Transient(format!("input payload: {e}")))?
                .to_vec(),
            None => Vec::new(),
        };
        build_run_started(record, input_payload(bytes), &ostro_blob::hex(sha))
            .map_err(|e| StartErr::Fatal(e.to_string()))
    }

    async fn load_journal(
        &self,
        handle: &PartitionHandle<ostro_log::Rf1Log>,
        run: RunId,
        sink: &PartitionSink,
    ) -> Result<Vec<Event>, String> {
        let journal = journal_io::read_journal(handle, &run)
            .await
            .map_err(|e| e.to_string())?;
        let before = sink.next_seq();
        sink.repair_unsupported(&journal)
            .await
            .map_err(|e| format!("repair: {e}"))?;
        if sink.next_seq() != before {
            return journal_io::read_journal(handle, &run)
                .await
                .map_err(|e| e.to_string());
        }
        Ok(journal)
    }
}
