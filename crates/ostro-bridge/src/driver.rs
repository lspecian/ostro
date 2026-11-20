//! The bridge driver core (06-07): the [`Bridge`] facade that owns effect task lifecycle,
//! dedupe, rate limits and completion submission. Connectors (06-10 http, 06-11 sql,
//! 06-12 object) only implement [`Connector::target`] and [`Connector::execute`]; the server
//! (06-14) adapts `Bridge` to the engine's `EffectDriver` seam (06-06), because ostro-bridge
//! depends on neither ostro-engine nor ostro-partition.
//!
//! # Task pipeline
//!
//! [`Bridge::start`] spawns at most one node-owned tokio task per `(partition, run_id, op)`
//! (a second start for a running key attaches and starts nothing). The task outlives the
//! caller (activation eviction does not stop it) and runs:
//!
//! 1. `connector.target()` (pure);
//! 2. [`RateLimiter::acquire`] with `max_wait = min(rate_max_wait, http timeout_ms)`;
//! 3. [`SendGate::check`]: `Abandon` ends the task with no execution and no completion;
//! 4. `connector.execute(ctx, effect, gate)` raced against `ctx.cancel`. If the token is
//!    cancelled first, or by the time execute returns, the task ends without a completion
//!    (connectors cancel the token to signal a late `Abandon` from their own gate re-check);
//! 5. [`encode_outcome`] once;
//! 6. failpoint `bridge::effect::before_complete_submit`;
//! 7. [`CompletionSink::complete`], retrying `Unavailable` with exponential backoff
//!    (100 ms doubling to 5 s) until cancelled; `Dropped` (run gone or already completed,
//!    including a run that turned terminal meanwhile, whose response is discarded) is logged
//!    at debug and ends the task.
//!
//! The task removes its registry entry when it ends.
//!
//! A panic anywhere in the pipeline (a connector bug) is caught through the task's
//! `JoinHandle`: the supervisor logs `op`/`kind`, completes the effect
//! `NonRetryable{kind: "internal_error", message: "bridge connector panicked"}` (unless a
//! completion was already being submitted) so the run never waits forever (W11), and removes the
//! entry. It never respawns: a deterministic panic would loop.
//!
//! # At-least-once boundary
//!
//! Registry dedupe only prevents *same-node* duplicates while a task is live. The gate is
//! checked by the driver before execute and re-checked by the connector immediately before
//! secret resolution and the send (Pitfall 7), but a node that loses its lease between that
//! re-check and the send, or a crash after the send and before the completion commits, means
//! the request can reach the destination again when the new leader's replay re-drives the
//! effect. The idempotency key (HTTP), the exactly-once token row (SQL) and conditional puts
//! (objects) are the only protection on the external side (PITFALLS #14).
//!
//! # Logging
//!
//! The driver logs only `partition`, `run_id`, `op`, `kind`, the rate-limit target and the
//! outcome kind: never request headers, bodies, SQL parameters or results.
//!
//! # Metrics
//!
//! `ostro_effect_inflight` (gauge), `ostro_effect_duration_seconds{kind}` (histogram),
//! `ostro_effect_completed_total{kind,outcome}` (outcome = `ok` or the failure kind),
//! `ostro_effect_attach_total` and (from [`crate::ratelimit`])
//! `ostro_effect_rate_limited_total{target}`. All names use the reserved `ostro_effect_` prefix.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use ostro_types::journal::{EffectRequest, Failure, OpOutput, RecordedResult};
use ostro_types::live::{EffectKind, LiveEffect};
use tokio_util::sync::CancellationToken;

use crate::config::ConnectorsConfig;
use crate::connector::{
    CompletionError, CompletionSink, Connector, EffectCtx, GateDecision, SendGate,
};
use crate::kinds::UNSUPPORTED_OP;
use crate::outcome::{
    CONNECTOR_PANICKED, INTERNAL_ERROR, encode_outcome, non_retryable, outcome_label,
};
use crate::ratelimit::RateLimiter;
use crate::registry::{EffectKey, EffectRegistry, Insert};

/// Histogram of effect execution time (start to outcome), by kind.
pub const DURATION_METRIC: &str = "ostro_effect_duration_seconds";
/// Counter of produced outcomes, by kind and outcome.
pub const COMPLETED_METRIC: &str = "ostro_effect_completed_total";
/// Counter of starts that attached to a running task.
pub const ATTACH_METRIC: &str = "ostro_effect_attach_total";
/// The failpoint evaluated right before the completion is submitted.
pub const FP_BEFORE_COMPLETE_SUBMIT: &str = "bridge::effect::before_complete_submit";

/// First completion retry delay.
const RETRY_INITIAL: Duration = Duration::from_millis(100);
/// Completion retry delay cap.
const RETRY_MAX: Duration = Duration::from_secs(5);

/// Run-level context of an effect, built by the server from the run (06-14).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectBase {
    /// Cluster id.
    pub cluster_id: String,
    /// Partition of the run.
    pub partition: u16,
    /// Run id (string form).
    pub run_id: String,
    /// Activation attempt that scheduled the effect.
    pub attempt: u32,
    /// Tenant-qualified deployed module name `<tenant>/<name>` (from `WorkflowVersion.module`).
    pub module: String,
    /// Tenant of the run, if any.
    pub tenant: Option<String>,
}

/// The metric label of an effect kind.
pub const fn kind_label(kind: EffectKind) -> &'static str {
    match kind {
        EffectKind::Http => "http",
        EffectKind::Sql => "sql",
        EffectKind::Object => "object",
    }
}

struct Shared {
    cfg: ConnectorsConfig,
    rate_max_wait: Duration,
    gate: Arc<dyn SendGate>,
    sink: Arc<dyn CompletionSink>,
    limiter: Arc<RateLimiter>,
    connectors: RwLock<BTreeMap<EffectKind, Arc<dyn Connector>>>,
    registry: EffectRegistry,
    closed: AtomicBool,
    runtime: Option<tokio::runtime::Handle>,
}

/// The effects bridge facade (one per node).
pub struct Bridge {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for Bridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bridge")
            .field("rate_max_wait", &self.shared.rate_max_wait)
            .field("inflight", &self.inflight())
            .finish_non_exhaustive()
    }
}

fn describe_metrics() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        metrics::describe_gauge!(crate::registry::INFLIGHT_METRIC, "Effect tasks in flight");
        metrics::describe_histogram!(
            DURATION_METRIC,
            metrics::Unit::Seconds,
            "Effect execution time from start to outcome"
        );
        metrics::describe_counter!(COMPLETED_METRIC, "Effect outcomes by kind and outcome");
        metrics::describe_counter!(ATTACH_METRIC, "Effect starts attached to a running task");
        metrics::describe_counter!(
            crate::ratelimit::RATE_LIMITED_METRIC,
            "Effects failed rate_limited after max_wait"
        );
    });
}

impl Bridge {
    /// A bridge without connectors. `rate_max_wait` comes from `EgressConfig.rate_max_wait`
    /// (06-04, W1).
    pub fn new(
        cfg: ConnectorsConfig,
        rate_max_wait: Duration,
        gate: Arc<dyn SendGate>,
        sink: Arc<dyn CompletionSink>,
        limiter: Arc<RateLimiter>,
    ) -> Self {
        describe_metrics();
        Self {
            shared: Arc::new(Shared {
                cfg,
                rate_max_wait,
                gate,
                sink,
                limiter,
                connectors: RwLock::new(BTreeMap::new()),
                registry: EffectRegistry::new(),
                closed: AtomicBool::new(false),
                runtime: tokio::runtime::Handle::try_current().ok(),
            }),
        }
    }

    /// Registers the connector of its [`Connector::kind`] (one per kind; a later one replaces
    /// an earlier one).
    pub fn register(&mut self, connector: Arc<dyn Connector>) {
        let kind = connector.kind();
        self.shared
            .connectors
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(kind, connector);
    }

    /// The `[connectors]` config the bridge was built with.
    pub fn config(&self) -> &ConnectorsConfig {
        &self.shared.cfg
    }

    /// The rate limiter shared by the connectors.
    pub fn limiter(&self) -> &Arc<RateLimiter> {
        &self.shared.limiter
    }

    /// Starts effect `effect.op` of `base.run_id` (non-blocking). Idempotent per
    /// `(partition, run_id, op)`: a start for a running key attaches and starts nothing.
    pub fn start(&self, base: EffectBase, effect: LiveEffect) {
        let sh = &self.shared;
        let kind = effect.kind();
        if sh.closed.load(Ordering::Acquire) {
            tracing::debug!(
                partition = base.partition,
                run_id = %base.run_id,
                op = effect.op,
                kind = kind_label(kind),
                "bridge shut down; effect not started"
            );
            return;
        }
        let Some(rt) = tokio::runtime::Handle::try_current()
            .ok()
            .or_else(|| sh.runtime.clone())
        else {
            tracing::error!(
                op = effect.op,
                kind = kind_label(kind),
                "Bridge::start outside a tokio runtime; effect not started"
            );
            return;
        };
        let key: EffectKey = (base.partition, base.run_id.clone(), effect.op);
        let (id, cancel) = match sh.registry.try_insert(key.clone(), kind) {
            Insert::Attached => {
                metrics::counter!(ATTACH_METRIC).increment(1);
                tracing::debug!(
                    partition = base.partition,
                    run_id = %base.run_id,
                    op = effect.op,
                    kind = kind_label(kind),
                    "effect already running; attached"
                );
                return;
            }
            Insert::New { id, cancel } => (id, cancel),
        };
        let sh = Arc::clone(sh);
        rt.spawn(supervise(sh, key, id, base, effect, cancel));
    }

    /// Cancels the task of `(partition, run_id, op)` (best effort: the side effect may already
    /// have happened). No completion is submitted for a cancelled task.
    pub fn cancel(&self, partition: u16, run_id: &str, op: u64) {
        self.shared.registry.cancel(partition, run_id, op);
    }

    /// Cancels every task of `run_id` on `partition`.
    pub fn cancel_run(&self, partition: u16, run_id: &str) {
        self.shared.registry.cancel_run(partition, run_id);
    }

    /// Number of live tasks.
    pub fn inflight(&self) -> usize {
        self.shared.registry.len()
    }

    /// Stops accepting starts, waits up to `grace` for the live tasks to finish, then cancels
    /// the rest and waits (briefly) for them to exit.
    pub async fn shutdown(&self, grace: Duration) {
        let sh = &self.shared;
        sh.closed.store(true, Ordering::Release);
        let deadline = Instant::now() + grace;
        while !sh.registry.is_empty() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let cancelled = sh.registry.cancel_all();
        if cancelled > 0 {
            tracing::info!(cancelled, "bridge shutdown cancelled in-flight effects");
        }
        let exit_deadline = Instant::now() + Duration::from_secs(1);
        while !sh.registry.is_empty() && Instant::now() < exit_deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

/// Runs the effect task and turns a panic into an `internal_error` completion (W11).
async fn supervise(
    sh: Arc<Shared>,
    key: EffectKey,
    id: u64,
    base: EffectBase,
    effect: LiveEffect,
    cancel: CancellationToken,
) {
    let op = effect.op;
    let kind = effect.kind();
    let submitting = Arc::new(AtomicBool::new(false));
    let worker = tokio::spawn(run_effect(
        Arc::clone(&sh),
        base,
        effect,
        cancel.clone(),
        Arc::clone(&submitting),
    ));
    match worker.await {
        Ok(()) => {}
        Err(e) if e.is_panic() => {
            tracing::error!(
                partition = key.0,
                run_id = %key.1,
                op,
                kind = kind_label(kind),
                "bridge connector panicked; completing internal_error"
            );
            if !submitting.load(Ordering::Acquire) && !cancel.is_cancelled() {
                let r: Result<OpOutput, Failure> =
                    Err(non_retryable(INTERNAL_ERROR, CONNECTOR_PANICKED, None));
                metrics::counter!(
                    COMPLETED_METRIC,
                    "kind" => kind_label(kind),
                    "outcome" => INTERNAL_ERROR
                )
                .increment(1);
                submit(&sh, &key, encode_outcome(&r), &cancel, kind).await;
            }
        }
        Err(_) => {} // aborted: the runtime is shutting down
    }
    sh.registry.remove(&key, id);
}

/// The gate -> rate limit -> execute -> encode -> complete pipeline of one effect.
async fn run_effect(
    sh: Arc<Shared>,
    base: EffectBase,
    effect: LiveEffect,
    cancel: CancellationToken,
    submitting: Arc<AtomicBool>,
) {
    let kind = effect.kind();
    let op = effect.op;
    let started = Instant::now();
    let ctx = EffectCtx {
        cluster_id: base.cluster_id,
        partition: base.partition,
        run_id: base.run_id,
        attempt: base.attempt,
        module: base.module,
        tenant: base.tenant,
        cancel: cancel.clone(),
    };
    let key: EffectKey = (ctx.partition, ctx.run_id.clone(), op);

    let Some(result) = produce(&sh, &ctx, &effect, kind).await else {
        tracing::debug!(
            partition = ctx.partition,
            run_id = %ctx.run_id,
            op,
            kind = kind_label(kind),
            "effect abandoned or cancelled; no completion"
        );
        return;
    };

    let outcome = outcome_label(&result);
    metrics::histogram!(DURATION_METRIC, "kind" => kind_label(kind))
        .record(started.elapsed().as_secs_f64());
    metrics::counter!(
        COMPLETED_METRIC,
        "kind" => kind_label(kind),
        "outcome" => outcome.clone()
    )
    .increment(1);
    tracing::debug!(
        partition = ctx.partition,
        run_id = %ctx.run_id,
        op,
        kind = kind_label(kind),
        outcome = %outcome,
        "effect outcome"
    );

    let encoded = encode_outcome(&result);
    drop(result);

    if let Some(action) = crate::bridge_fail!(FP_BEFORE_COMPLETE_SUBMIT) {
        let applied = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            r = crate::failpoints::apply(action) => r,
        };
        if applied.is_err() {
            // `return(error)`: simulate a completion lost before submission (recovered by the
            // leader-start re-drive).
            tracing::debug!(
                op,
                "failpoint {FP_BEFORE_COMPLETE_SUBMIT}: completion dropped"
            );
            return;
        }
    }

    submitting.store(true, Ordering::Release);
    submit(&sh, &key, encoded, &cancel, kind).await;
}

/// Steps 1-4: `None` = abandoned or cancelled (no completion).
async fn produce(
    sh: &Shared,
    ctx: &EffectCtx,
    effect: &LiveEffect,
    kind: EffectKind,
) -> Option<Result<OpOutput, Failure>> {
    let connector = sh
        .connectors
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .get(&kind)
        .cloned();
    let cancel = &ctx.cancel;

    let Some(connector) = connector else {
        if !gate_proceeds(sh, ctx, effect.op).await {
            return None;
        }
        return Some(Err(non_retryable(
            UNSUPPORTED_OP,
            format!("no connector registered for {} effects", kind_label(kind)),
            None,
        )));
    };

    // 1-2. rate limit (target resolution is pure).
    if let Some(target) = connector.target(ctx, effect) {
        let mut max_wait = sh.rate_max_wait;
        if let EffectRequest::Http(h) = &effect.request
            && let Some(ms) = h.timeout_ms
        {
            max_wait = max_wait.min(Duration::from_millis(ms));
        }
        let acquired = tokio::select! {
            biased;
            () = cancel.cancelled() => return None,
            r = sh.limiter.acquire(&target, max_wait) => r,
        };
        if let Err(f) = acquired {
            tracing::debug!(
                op = effect.op,
                kind = kind_label(kind),
                target = %target.key,
                "effect rate limited"
            );
            if !gate_proceeds(sh, ctx, effect.op).await {
                return None;
            }
            return Some(Err(f));
        }
    }

    // 3. gate.
    if !gate_proceeds(sh, ctx, effect.op).await {
        return None;
    }

    // 4. execute, raced against cancellation.
    let result = tokio::select! {
        biased;
        () = cancel.cancelled() => return None,
        r = connector.execute(ctx, effect, sh.gate.as_ref()) => r,
    };
    if cancel.is_cancelled() {
        return None;
    }
    Some(result)
}

async fn gate_proceeds(sh: &Shared, ctx: &EffectCtx, op: u64) -> bool {
    let decision = tokio::select! {
        biased;
        () = ctx.cancel.cancelled() => return false,
        d = sh.gate.check(ctx.partition, &ctx.run_id, op) => d,
    };
    decision == GateDecision::Proceed && !ctx.cancel.is_cancelled()
}

/// Step 7: submit, retrying `Unavailable` with backoff until cancelled.
async fn submit(
    sh: &Shared,
    key: &EffectKey,
    outcome: RecordedResult,
    cancel: &CancellationToken,
    kind: EffectKind,
) {
    let (partition, run_id, op) = (key.0, key.1.as_str(), key.2);
    let mut delay = RETRY_INITIAL;
    loop {
        let r = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            r = sh.sink.complete(partition, run_id, op, outcome.clone()) => r,
        };
        match r {
            Ok(()) => return,
            Err(CompletionError::Dropped(why)) => {
                tracing::debug!(
                    partition,
                    run_id,
                    op,
                    kind = kind_label(kind),
                    reason = %why,
                    "effect completion dropped"
                );
                return;
            }
            Err(CompletionError::Unavailable(why)) => {
                tracing::debug!(
                    partition,
                    run_id,
                    op,
                    kind = kind_label(kind),
                    reason = %why,
                    retry_in_ms = delay.as_millis() as u64,
                    "effect completion unavailable; retrying"
                );
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(delay) => {}
                }
                delay = (delay * 2).min(RETRY_MAX);
            }
        }
    }
}
