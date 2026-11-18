//! Host impl of `ostro:runtime/effects` (WIT-02/03/05): durable effects, timers,
//! signals, children, await tokens, cancellation and `wait`.
//!
//! Every scheduling call hashes its `CommandArgs` (03-02 `command_hash`), issues one
//! replay-core command and returns the op id = the seq of its scheduling event (on
//! replay: the recorded event's seq). `wait` builds completions only from journaled
//! input events; effect/child outcomes are decoded from the exact recorded bytes
//! (ENG-04). An unsatisfied live wait traps with [`Evict`] `WaitUnsatisfied` and
//! appends nothing.

use ostro_types::journal::{
    AwaitTokenCreated, CANCELLATION_OP, ChildStarted, CommandArgs, CommandHash, CommandKind,
    CommandMeta, EffectRequest, EffectScheduled, EventBody, EventKind, Failure, HASH_VER,
    OpCancelled, OpId, OpOutput, SignalWaitStarted, TimerStarted, command_args, decode_result,
};

use crate::error::{Evict, EvictReason};
use crate::host::bindings::ostro::runtime::effects::{
    self as wit, AwaitToken, ChildOptions, Completion, Host, HttpRequest, ObjectRequest,
    SqlRequest, WaitMode,
};
use crate::host::bindings::ostro::runtime::types::{Datetime, Payload};
use crate::host::state::{HostState, trap, violation};
use crate::replay::{CommandResult, WaitOutcome, token};

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

    fn schedule_effect(&mut self, req: EffectRequest) -> wasmtime::Result<OpId> {
        let args = command_args(&req);
        let kind = args.kind();
        let hash = self.hash(&args)?;
        let actual = match &req {
            EffectRequest::Http(r) => format!("http {} {}", r.method, r.url),
            EffectRequest::Sql(r) => format!("sql {:?}", r.connection),
            EffectRequest::Object(r) => format!("object {} {:?}", r.op, r.key),
        };
        self.issue(kind, hash, &|| actual.clone(), |_| {
            EventBody::EffectScheduled(EffectScheduled {
                meta: meta(hash),
                request: req,
            })
        })
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

    /// Build the guest-visible completion of a resolved op from its journaled input.
    fn completion(&self, op: OpId) -> wasmtime::Result<Completion> {
        let ev = self.replay.completion_for(op).ok_or_else(|| {
            violation(format!(
                "bad journal: resolved op {op} has no completion event"
            ))
        })?;
        let bad = |e| {
            violation(format!(
                "bad journal: outcome of op {op} at seq {}: {e:?}",
                ev.seq
            ))
        };
        let outcome: Result<OpOutput, Failure> = match &ev.body {
            EventBody::TimerFired(_) => Ok(OpOutput::Timer),
            EventBody::EffectCompleted(c) => decode_result(&c.outcome).map_err(bad)?,
            EventBody::ChildCompleted(c) => decode_result(&c.outcome).map_err(bad)?,
            EventBody::SignalReceived(s) => Ok(OpOutput::Signal(s.payload.clone())),
            EventBody::TokenCompleted(t) => Ok(OpOutput::Token(t.payload.clone())),
            EventBody::CancelRequested(_) => Ok(OpOutput::CancelRequested),
            other => {
                return Err(violation(format!(
                    "bad journal: {} is not a completion",
                    other.summary()
                )));
            }
        };
        Ok(Completion {
            op,
            outcome: match outcome {
                Ok(o) => Ok(convert::op_output_to_wit(o)),
                Err(f) => Err(convert::failure_to_wit(f)),
            },
        })
    }
}

fn nanos(at: &Datetime) -> u64 {
    at.seconds
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::from(at.nanoseconds))
}

impl Host for HostState {
    async fn http_fetch(&mut self, req: HttpRequest) -> wasmtime::Result<OpId> {
        self.schedule_effect(EffectRequest::Http(convert::http_request_from_wit(req)))
    }

    async fn sql_query(&mut self, req: SqlRequest) -> wasmtime::Result<OpId> {
        self.schedule_effect(EffectRequest::Sql(convert::sql_request_from_wit(req)))
    }

    async fn object_store(&mut self, req: ObjectRequest) -> wasmtime::Result<OpId> {
        self.schedule_effect(EffectRequest::Object(convert::object_request_from_wit(req)))
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
            WaitOutcome::Pending => Err(wasmtime::Error::new(Evict {
                reason: EvictReason::WaitUnsatisfied,
            })),
            WaitOutcome::Resolved(list) => list.into_iter().map(|op| self.completion(op)).collect(),
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

    /// Header values may hold `{{secret:..}}` templates: journaled, never hashed.
    /// The retry policy is applied by the host and not journaled.
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
