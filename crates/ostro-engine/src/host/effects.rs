//! Host impl of `ostro:runtime/effects` (WIT-02/03/05): durable effects, timers,
//! signals, children, await tokens, cancellation and `wait`.
//!
//! Every scheduling call hashes its `CommandArgs` (03-02 `command_hash`), issues one
//! replay-core command and returns the op id = the seq of its scheduling event (on
//! replay: the recorded event's seq). `wait` builds completions only from journaled
//! input events; effect/child outcomes are decoded from the exact recorded bytes
//! (ENG-04).
//!
//! An unsatisfied live wait either evicts or parks hot (ENG-09, plan 03-11). It parks
//! only when every op it still waits on can resolve within `engine.hot_wait` (timers
//! due within the window, effects in flight) and an input channel exists. Parking
//! first flushes the pending intents (so the leader can schedule the timers/effects),
//! registers with the [`ParkedRegistry`](crate::limits::ParkedRegistry), then waits for
//! committed inputs, delivering them to the replay core and re-checking the wait. It traps with
//! [`Evict`] `HotWaitElapsed` / `BudgetPressure` / `External` when the window elapses,
//! budget pressure picks it, or the activation is cancelled. Otherwise an unsatisfied
//! wait traps with `WaitUnsatisfied` and appends nothing. The guest sees the same
//! completions either way (a hot-waited and an evict-and-replay run journal the same
//! events). Only channel receives and the flush await happen inside the import, which
//! keeps it thin on the guest's fiber stack.

use ostro_types::journal::{
    AwaitTokenCreated, CANCELLATION_OP, ChildStarted, CommandArgs, CommandHash, CommandKind,
    CommandMeta, EffectRequest, EffectScheduled, EventBody, EventKind, HASH_VER, OpCancelled, OpId,
    SignalWaitStarted, TimerStarted, command_args,
};

use crate::error::{Evict, EvictReason, Fenced};
use crate::host::bindings::ostro::runtime::effects::{
    self as wit, AwaitToken, ChildOptions, Completion, Host, HttpRequest, ObjectRequest,
    SqlRequest, WaitMode,
};
use crate::host::bindings::ostro::runtime::types::{Datetime, Payload};
use crate::host::state::{HostState, HotWait, trap, violation};
use crate::replay::{CommandResult, WaitOutcome, token};
use crate::sink::AppendError;
use ostro_types::live::{EffectRetry, LiveEffect};

fn evict<T>(reason: EvictReason) -> wasmtime::Result<T> {
    Err(wasmtime::Error::new(Evict { reason }))
}

/// Trap for a failed pre-park flush, mapped like the driver's own flush errors (03-08).
pub(crate) fn flush_trap(e: AppendError) -> wasmtime::Error {
    match e {
        AppendError::Fenced => wasmtime::Error::new(Fenced),
        AppendError::Conflict { .. } => wasmtime::Error::new(Evict {
            reason: EvictReason::AppendConflict,
        }),
        AppendError::Unavailable(_) => wasmtime::Error::new(Evict {
            reason: EvictReason::External,
        }),
    }
}

fn meta(hash: CommandHash) -> CommandMeta {
    CommandMeta {
        hash_ver: HASH_VER,
        hash,
    }
}

impl HostState {
    /// Issue a scheduling command; returns its op id (= event seq).
    fn issue(
        &mut self,
        kind: CommandKind,
        hash: CommandHash,
        actual: &dyn Fn() -> String,
        live: impl FnOnce(u64) -> EventBody,
    ) -> wasmtime::Result<OpId> {
        match self
            .replay
            .command(kind, hash, actual, live)
            .map_err(trap)?
        {
            CommandResult::Replayed(ev) => Ok(ev.seq),
            CommandResult::Live { seq, .. } => Ok(seq),
        }
    }

    /// Schedule an effect (06-06).
    ///
    /// `live` is the guest's full request (real header values); `journaled` is the copy
    /// written to the journal (values of `redact` headers replaced, see
    /// [`convert::redact_http_request`]). Header values are not hashed, so both hash the
    /// same; the hash is computed over `live`.
    ///
    /// With an effect driver: a live command keeps a [`LiveEffect`] in
    /// `pending_effects` for `flush` to start after commit (FX-01). A replayed command
    /// whose op has no completion in the loaded journal is re-driven at once
    /// (`redrive = true`, F1): its `EffectScheduled` is already durable.
    fn schedule_effect(
        &mut self,
        live: EffectRequest,
        journaled: EffectRequest,
        http_retry: Option<EffectRetry>,
    ) -> wasmtime::Result<OpId> {
        let args = command_args(&live);
        let kind = args.kind();
        let hash = self.hash(&args)?;
        let actual = match &live {
            EffectRequest::Http(r) => format!("http {} {}", r.method, r.url),
            EffectRequest::Sql(r) => format!("sql {:?}", r.connection),
            EffectRequest::Object(r) => format!("object {} {:?}", r.op, r.key),
        };
        let res = self
            .replay
            .command(kind, hash, &|| actual.clone(), |_| {
                EventBody::EffectScheduled(EffectScheduled {
                    meta: meta(hash),
                    request: journaled,
                })
            })
            .map_err(trap)?;
        let (op, ts, redrive) = match res {
            CommandResult::Replayed(ev) => (ev.seq, ev.ts_nanos, true),
            CommandResult::Live { seq, ts_nanos } => (seq, ts_nanos, false),
        };
        let Some(driver) = self.driver.clone() else {
            return Ok(op);
        };
        let effect = LiveEffect {
            op,
            request: live,
            http_retry,
            scheduled_ts_nanos: ts,
            redrive,
        };
        if !redrive {
            self.pending_effects.insert(op, effect);
        } else if self.replay.completion_for(op).is_none() {
            driver.start(&self.run, self.attempt, effect);
        }
        Ok(op)
    }

    /// Schedule an HTTP effect: journal the redacted copy, keep the full request.
    fn schedule_http(&mut self, req: HttpRequest) -> wasmtime::Result<OpId> {
        let http_retry = req.retry.clone().map(convert::effect_retry_from_wit);
        let live = convert::http_request_from_wit(req);
        let journaled = convert::redact_http_request(&live);
        self.schedule_effect(
            EffectRequest::Http(live),
            EffectRequest::Http(journaled),
            http_retry,
        )
    }

    fn schedule_timer(
        &mut self,
        args: CommandArgs<'_>,
        fire_at_nanos: u64,
    ) -> wasmtime::Result<OpId> {
        let kind = args.kind();
        let hash = self.hash(&args)?;
        self.issue(kind, hash, &|| "sleep".to_owned(), |_| {
            EventBody::TimerStarted(TimerStarted {
                meta: meta(hash),
                fire_at_nanos,
            })
        })
    }

    /// Build the guest-visible completion of a resolved op from its journaled
    /// completion event (rules in `replay::wait`: first completion wins, `OpCancelled`
    /// → `Err(cancelled)`, `WaitTimedOut` → `Err(timeout)`, `SignalMatched` → the payload
    /// of the referenced `SignalPayload`). A bad journal traps as a protocol violation.
    fn completion(&self, op: OpId) -> wasmtime::Result<Completion> {
        let outcome = self.replay.completion_outcome(op).map_err(trap)?;
        Ok(Completion {
            op,
            outcome: match outcome {
                Ok(o) => Ok(convert::op_output_to_wit(o)),
                Err(f) => Err(convert::failure_to_wit(f)),
            },
        })
    }

    fn completions(&self, ops: Vec<OpId>) -> wasmtime::Result<Vec<Completion>> {
        ops.into_iter().map(|op| self.completion(op)).collect()
    }

    /// Whether an unsatisfied wait on `ops` may park hot: hot-wait wiring present and
    /// every still-open op (except the cancellation op) a timer due within `hot_wait`
    /// of logical now or an in-flight effect (see [`hot_waitable_op`]).
    ///
    /// **Signal and token waits always evict** (frozen semantics, W12): an open
    /// `wait-signal` or `create-await-token` op is never hot-waitable, whatever its
    /// timeout, because its completion depends on an external party. Parking would hold
    /// guest memory for an unbounded time.
    ///
    /// The node budget is deliberately not checked here: parking over budget registers
    /// this activation as the most recently parked and wakes the LRU, which evicts
    /// older parked activations first (refusing to park the newest would invert LRU
    /// and race the eviction loop).
    fn hot_waitable(&self, ops: &[OpId]) -> bool {
        let Some(hot) = &self.hot else {
            return false;
        };
        let now = self.replay.logical_now_nanos();
        let window = u64::try_from(hot.hot_wait.as_nanos()).unwrap_or(u64::MAX);
        let mut open = ops
            .iter()
            .copied()
            .filter(|&op| op != CANCELLATION_OP && self.replay.completion_for(op).is_none())
            .peekable();
        if open.peek().is_none() {
            return false;
        }
        open.all(|op| hot_waitable_op(self.replay.op_kind(op).map(|e| &e.body), now, window))
    }

    /// Unsatisfied live wait: park hot if possible, else evict (`WaitUnsatisfied`).
    /// Waits on signal or token ops always evict (see [`hot_waitable`](Self::hot_waitable)).
    async fn park_or_evict(
        &mut self,
        ops: &[OpId],
        mode: ostro_types::journal::WaitMode,
        hash: CommandHash,
    ) -> wasmtime::Result<Vec<Completion>> {
        if !self.hot_waitable(ops) {
            return evict(EvictReason::WaitUnsatisfied);
        }
        // Intents must be committed before parking so the leader can schedule them.
        self.flush().await.map_err(flush_trap)?;
        let Some(mut hot) = self.hot.take() else {
            return evict(EvictReason::WaitUnsatisfied);
        };
        let r = self.parked(&mut hot, ops, mode, hash).await;
        self.hot = Some(hot);
        r
    }

    async fn parked(
        &mut self,
        hot: &mut HotWait,
        ops: &[OpId],
        mode: ostro_types::journal::WaitMode,
        hash: CommandHash,
    ) -> wasmtime::Result<Vec<Completion>> {
        let guard = hot.parked.park();
        let deadline = tokio::time::Instant::now() + hot.hot_wait;
        let mut inputs_open = true;
        loop {
            tokio::select! {
                biased;
                _ = hot.cancel.cancelled() => return evict(EvictReason::External),
                _ = guard.token().cancelled() => return evict(EvictReason::BudgetPressure),
                ev = hot.inputs.recv(), if inputs_open => {
                    let Some(ev) = ev else {
                        // No more inputs can arrive; wait out the window (or a cancel).
                        inputs_open = false;
                        continue;
                    };
                    let mut batch = vec![ev];
                    while let Ok(ev) = hot.inputs.try_recv() {
                        batch.push(ev);
                    }
                    if let Err(e) = self.replay.deliver_inputs(batch) {
                        // The delivered inputs do not continue this activation's
                        // journal: re-dispatch replays the committed one.
                        tracing::warn!(error = %e, "hot wait: undeliverable inputs");
                        return evict(EvictReason::AppendConflict);
                    }
                    if let WaitOutcome::Resolved(list) =
                        self.replay.wait(ops, mode, hash).map_err(trap)?
                    {
                        drop(guard);
                        return self.completions(list);
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return evict(EvictReason::HotWaitElapsed);
                }
            }
        }
    }
}

/// Whether one still-open op (its scheduling event) can resolve within `window` of
/// logical `now`: a timer due within the window or an in-flight effect. Signal waits
/// and await tokens are never hot-waitable, with or without a timeout (W12, frozen);
/// children and unknown ops are not either.
fn hot_waitable_op(scheduled: Option<&EventBody>, now: u64, window: u64) -> bool {
    match scheduled {
        Some(EventBody::TimerStarted(t)) => t.fire_at_nanos.saturating_sub(now) <= window,
        Some(EventBody::EffectScheduled(_)) => true,
        Some(EventBody::SignalWaitStarted(_) | EventBody::AwaitTokenCreated(_)) => false,
        _ => false,
    }
}

fn nanos(at: &Datetime) -> u64 {
    at.seconds
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::from(at.nanoseconds))
}

impl Host for HostState {
    async fn http_fetch(&mut self, req: HttpRequest) -> wasmtime::Result<OpId> {
        self.schedule_http(req)
    }

    async fn sql_query(&mut self, req: SqlRequest) -> wasmtime::Result<OpId> {
        let r = EffectRequest::Sql(convert::sql_request_from_wit(req));
        self.schedule_effect(r.clone(), r, None)
    }

    async fn object_store(&mut self, req: ObjectRequest) -> wasmtime::Result<OpId> {
        let r = EffectRequest::Object(convert::object_request_from_wit(req));
        self.schedule_effect(r.clone(), r, None)
    }

    async fn sleep(&mut self, duration_ms: u64) -> wasmtime::Result<OpId> {
        // Deterministic: computed from logical now before the command is issued.
        let fire_at = self
            .replay
            .logical_now_nanos()
            .saturating_add(duration_ms.saturating_mul(1_000_000));
        self.schedule_timer(CommandArgs::Sleep { duration_ms }, fire_at)
    }

    async fn sleep_until(&mut self, at: Datetime) -> wasmtime::Result<OpId> {
        self.schedule_timer(
            CommandArgs::SleepUntil {
                seconds: at.seconds,
                nanoseconds: at.nanoseconds,
            },
            nanos(&at),
        )
    }

    async fn wait_signal(
        &mut self,
        name: String,
        timeout_ms: Option<u64>,
    ) -> wasmtime::Result<OpId> {
        let hash = self.hash(&CommandArgs::WaitSignal { name: &name })?;
        let actual = format!("wait-signal {name:?}");
        self.issue(CommandKind::WaitSignal, hash, &|| actual.clone(), |_| {
            EventBody::SignalWaitStarted(SignalWaitStarted {
                meta: meta(hash),
                name,
                timeout_ms,
            })
        })
    }

    async fn start_child(
        &mut self,
        workflow: String,
        input: Payload,
        opts: ChildOptions,
    ) -> wasmtime::Result<OpId> {
        let hash = self.hash(&CommandArgs::StartChild {
            workflow: &workflow,
            input: &input.data,
        })?;
        let actual = format!("start-child {workflow:?}");
        self.issue(CommandKind::StartChild, hash, &|| actual.clone(), |_| {
            EventBody::ChildStarted(ChildStarted {
                meta: meta(hash),
                workflow,
                input: convert::payload_from_wit(input),
                options: convert::child_options_from_wit(opts),
            })
        })
    }

    async fn create_await_token(
        &mut self,
        timeout_ms: Option<u64>,
    ) -> wasmtime::Result<AwaitToken> {
        let hash = self.hash(&CommandArgs::CreateAwaitToken)?;
        let partition = self.run.partition;
        let run_id = self.run.run_id.clone();
        let key = self.token_key;
        let op = self.issue(
            CommandKind::CreateAwaitToken,
            hash,
            &|| "create-await-token".to_owned(),
            |seq| {
                EventBody::AwaitTokenCreated(AwaitTokenCreated {
                    meta: meta(hash),
                    token: token::mint(partition, &run_id, seq, &key),
                    timeout_ms,
                })
            },
        )?;
        // Live or replayed, the token is the journaled one (replay never needs the key).
        match self.replay.journal().get(op as usize).map(|e| &e.body) {
            Some(EventBody::AwaitTokenCreated(t)) => Ok(AwaitToken {
                op,
                token: t.token.clone(),
            }),
            _ => Err(violation(format!(
                "bad journal: no await token at seq {op}"
            ))),
        }
    }

    async fn cancellation(&mut self) -> wasmtime::Result<OpId> {
        Ok(CANCELLATION_OP)
    }

    async fn cancel_op(&mut self, op: OpId) -> wasmtime::Result<()> {
        let schedulable = self.replay.op_kind(op).is_some_and(|ev| {
            matches!(
                ev.body.kind(),
                EventKind::TimerStarted
                    | EventKind::EffectScheduled
                    | EventKind::SignalWaitStarted
                    | EventKind::ChildStarted
                    | EventKind::AwaitTokenCreated
            )
        });
        if !schedulable {
            return Err(violation(format!("cancel-op on unknown op {op}")));
        }
        let hash = self.hash(&CommandArgs::CancelOp { op })?;
        self.issue(
            CommandKind::CancelOp,
            hash,
            &|| format!("cancel-op {op}"),
            |_| {
                EventBody::OpCancelled(OpCancelled {
                    meta: meta(hash),
                    op,
                })
            },
        )?;
        Ok(())
    }

    async fn wait(&mut self, ops: Vec<OpId>, mode: WaitMode) -> wasmtime::Result<Vec<Completion>> {
        if ops.is_empty() {
            return Err(violation("wait on an empty op list"));
        }
        let mode = convert::wait_mode_from_wit(mode);
        let hash = self.hash(&CommandArgs::Wait { ops: &ops, mode })?;
        match self.replay.wait(&ops, mode, hash).map_err(trap)? {
            WaitOutcome::Pending => self.park_or_evict(&ops, mode, hash).await,
            WaitOutcome::Resolved(list) => self.completions(list),
        }
    }
}

/// WIT ⇄ `ostro_types::journal` mirror conversions (shared by context/journal/state).
pub(crate) mod convert {
    use ostro_types::journal as j;

    use super::wit;
    use crate::host::bindings::ostro::runtime::types as t;

    pub(crate) fn payload_from_wit(p: t::Payload) -> j::Payload {
        j::Payload {
            content_type: p.content_type,
            data: p.data,
        }
    }

    pub(crate) fn payload_to_wit(p: j::Payload) -> t::Payload {
        t::Payload {
            content_type: p.content_type,
            data: p.data,
        }
    }

    fn error_info_from_wit(e: t::ErrorInfo) -> j::ErrorInfo {
        j::ErrorInfo {
            kind: e.kind,
            message: e.message,
            details: e.details.map(payload_from_wit),
        }
    }

    fn error_info_to_wit(e: j::ErrorInfo) -> t::ErrorInfo {
        t::ErrorInfo {
            kind: e.kind,
            message: e.message,
            details: e.details.map(payload_to_wit),
        }
    }

    pub(crate) fn failure_from_wit(f: t::Failure) -> j::Failure {
        match f {
            t::Failure::Retryable(e) => j::Failure::Retryable(error_info_from_wit(e)),
            t::Failure::NonRetryable(e) => j::Failure::NonRetryable(error_info_from_wit(e)),
            t::Failure::Timeout => j::Failure::Timeout,
            t::Failure::Cancelled => j::Failure::Cancelled,
        }
    }

    pub(crate) fn failure_to_wit(f: j::Failure) -> t::Failure {
        match f {
            j::Failure::Retryable(e) => t::Failure::Retryable(error_info_to_wit(e)),
            j::Failure::NonRetryable(e) => t::Failure::NonRetryable(error_info_to_wit(e)),
            j::Failure::Timeout => t::Failure::Timeout,
            j::Failure::Cancelled => t::Failure::Cancelled,
        }
    }

    pub(crate) fn step_outcome_from_wit(
        o: Result<t::Payload, t::Failure>,
    ) -> Result<j::Payload, j::Failure> {
        o.map(payload_from_wit).map_err(failure_from_wit)
    }

    pub(crate) fn step_outcome_to_wit(
        o: Result<j::Payload, j::Failure>,
    ) -> Result<t::Payload, t::Failure> {
        o.map(payload_to_wit).map_err(failure_to_wit)
    }

    fn header_from_wit(h: wit::Header) -> j::Header {
        j::Header {
            name: h.name,
            value: h.value,
        }
    }

    fn header_to_wit(h: j::Header) -> wit::Header {
        wit::Header {
            name: h.name,
            value: h.value,
        }
    }

    /// The value journaled for a header named in `http-request.redact` (W3, F2).
    pub(crate) const REDACTED: &str = "[redacted]";

    /// The journaled copy of a live HTTP request: the value of every header whose name
    /// is in `redact` (ASCII case-insensitive) becomes [`REDACTED`]. Other values,
    /// `{{secret:..}}` templates included (they hold no secret), are kept verbatim.
    /// Header values are not hashed, so the command hash is unchanged.
    pub(crate) fn redact_http_request(r: &j::HttpRequest) -> j::HttpRequest {
        let mut out = r.clone();
        for h in &mut out.headers {
            if r.redact.iter().any(|n| n.eq_ignore_ascii_case(&h.name)) {
                h.value = REDACTED.to_owned();
            }
        }
        out
    }

    /// The guest's `retry-policy` for an HTTP effect (applied by the bridge, never
    /// journaled).
    pub(crate) fn effect_retry_from_wit(p: t::RetryPolicy) -> ostro_types::live::EffectRetry {
        ostro_types::live::EffectRetry {
            max_attempts: p.max_attempts,
            initial_interval_ms: p.initial_interval_ms,
            max_interval_ms: p.max_interval_ms,
            backoff_coefficient: p.backoff_coefficient,
            non_retryable_kinds: p.non_retryable_kinds,
        }
    }

    /// The full (live) request: real header values. The journaled copy is
    /// [`redact_http_request`] of this. The retry policy is mapped separately
    /// ([`effect_retry_from_wit`]); the journal mirror has no field for it (F1).
    pub(crate) fn http_request_from_wit(r: wit::HttpRequest) -> j::HttpRequest {
        j::HttpRequest {
            method: r.method,
            url: r.url,
            headers: r.headers.into_iter().map(header_from_wit).collect(),
            body: r.body,
            timeout_ms: r.timeout_ms,
            idempotency_key: r.idempotency_key,
            redact: r.redact,
        }
    }

    fn sql_value_from_wit(v: wit::SqlValue) -> j::SqlValue {
        match v {
            wit::SqlValue::Null => j::SqlValue::Null,
            wit::SqlValue::Boolean(b) => j::SqlValue::Boolean(b),
            wit::SqlValue::Int8(i) => j::SqlValue::Int8(i),
            wit::SqlValue::Float8(f) => j::SqlValue::Float8(f),
            wit::SqlValue::Numeric(s) => j::SqlValue::Numeric(s),
            wit::SqlValue::Text(s) => j::SqlValue::Text(s),
            wit::SqlValue::Bytea(b) => j::SqlValue::Bytea(b),
            wit::SqlValue::Json(s) => j::SqlValue::Json(s),
            wit::SqlValue::Timestamptz(d) => j::SqlValue::Timestamptz {
                seconds: d.seconds,
                nanoseconds: d.nanoseconds,
            },
        }
    }

    fn sql_value_to_wit(v: j::SqlValue) -> wit::SqlValue {
        match v {
            j::SqlValue::Null => wit::SqlValue::Null,
            j::SqlValue::Boolean(b) => wit::SqlValue::Boolean(b),
            j::SqlValue::Int8(i) => wit::SqlValue::Int8(i),
            j::SqlValue::Float8(f) => wit::SqlValue::Float8(f),
            j::SqlValue::Numeric(s) => wit::SqlValue::Numeric(s),
            j::SqlValue::Text(s) => wit::SqlValue::Text(s),
            j::SqlValue::Bytea(b) => wit::SqlValue::Bytea(b),
            j::SqlValue::Json(s) => wit::SqlValue::Json(s),
            j::SqlValue::Timestamptz {
                seconds,
                nanoseconds,
            } => wit::SqlValue::Timestamptz(t::Datetime {
                seconds,
                nanoseconds,
            }),
        }
    }

    pub(crate) fn sql_request_from_wit(r: wit::SqlRequest) -> j::SqlRequest {
        j::SqlRequest {
            connection: r.connection,
            statement: r.statement,
            params: r.params.into_iter().map(sql_value_from_wit).collect(),
            exactly_once: r.exactly_once,
        }
    }

    pub(crate) fn object_request_from_wit(r: wit::ObjectRequest) -> j::ObjectRequest {
        j::ObjectRequest {
            connection: r.connection,
            op: r.op,
            key: r.key,
            body: r.body,
        }
    }

    pub(crate) fn child_options_from_wit(o: wit::ChildOptions) -> j::ChildOptions {
        j::ChildOptions {
            queue: o.queue,
            version: o.version,
            detached: o.detached,
            run_timeout_ms: o.run_timeout_ms,
        }
    }

    pub(crate) fn wait_mode_from_wit(m: wit::WaitMode) -> j::WaitMode {
        match m {
            wit::WaitMode::All => j::WaitMode::All,
            wit::WaitMode::Any => j::WaitMode::Any,
        }
    }

    pub(crate) fn op_output_to_wit(o: j::OpOutput) -> wit::OpOutput {
        match o {
            j::OpOutput::Http(r) => wit::OpOutput::Http(wit::HttpResponse {
                status: r.status,
                headers: r.headers.into_iter().map(header_to_wit).collect(),
                body: r.body,
            }),
            j::OpOutput::Sql(r) => wit::OpOutput::Sql(wit::SqlResult {
                columns: r.columns,
                rows: r
                    .rows
                    .into_iter()
                    .map(|row| row.into_iter().map(sql_value_to_wit).collect())
                    .collect(),
                rows_affected: r.rows_affected,
            }),
            j::OpOutput::Object(b) => wit::OpOutput::Object(b),
            j::OpOutput::Timer => wit::OpOutput::Timer,
            j::OpOutput::Signal(p) => wit::OpOutput::Signal(payload_to_wit(p)),
            j::OpOutput::Child(p) => wit::OpOutput::Child(payload_to_wit(p)),
            j::OpOutput::Token(p) => wit::OpOutput::Token(payload_to_wit(p)),
            j::OpOutput::CancelRequested => wit::OpOutput::CancelRequested,
        }
    }
}

#[cfg(test)]
mod tests {
    use ostro_types::journal::{AwaitTokenCreated, SignalWaitStarted, TimerStarted};

    use super::*;

    const MS: u64 = 1_000_000;

    fn m() -> CommandMeta {
        meta(CommandHash([1; 16]))
    }

    /// W12: signal and token waits always evict, whatever their timeout; timers due
    /// within the window and in-flight effects stay hot-waitable.
    #[test]
    fn signal_and_token_waits_are_never_hot_waitable() {
        let window = 2_000 * MS;
        let now = 10_000 * MS;
        let signal = EventBody::SignalWaitStarted(SignalWaitStarted {
            meta: m(),
            name: "go".into(),
            timeout_ms: Some(1),
        });
        assert!(!hot_waitable_op(Some(&signal), now, window));
        let signal_no_timeout = EventBody::SignalWaitStarted(SignalWaitStarted {
            meta: m(),
            name: "go".into(),
            timeout_ms: None,
        });
        assert!(!hot_waitable_op(Some(&signal_no_timeout), now, window));
        for timeout_ms in [None, Some(1)] {
            let token = EventBody::AwaitTokenCreated(AwaitTokenCreated {
                meta: m(),
                token: "ot1_x".into(),
                timeout_ms,
            });
            assert!(!hot_waitable_op(Some(&token), now, window));
        }
        let due = EventBody::TimerStarted(TimerStarted {
            meta: m(),
            fire_at_nanos: now + MS,
        });
        assert!(hot_waitable_op(Some(&due), now, window));
        let far = EventBody::TimerStarted(TimerStarted {
            meta: m(),
            fire_at_nanos: now + window + 1,
        });
        assert!(!hot_waitable_op(Some(&far), now, window));
        assert!(!hot_waitable_op(None, now, window));
    }
}
