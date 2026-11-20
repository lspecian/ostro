//! Run traces (OBS-01, P8-D14, plans 08-01 / 08-11). Uses the OpenTelemetry **API** crate
//! only; the SDK, exporter and id generator are configured by ostro-server.
//!
//! # Deterministic ids
//!
//! - Trace id: the run's stored W3C `traceparent` trace id, else
//!   `blake3("ostro/trace" ‖ run_id)[..16]` ([`trace_id_for`]).
//! - Span id: `blake3("ostro/span" ‖ run_id ‖ kind ‖ n_le)[..8]` ([`span_id`]), with
//!   `kind` = `run` (n = 0), `activation` (n = `attempt << 32 | counter`), `step`
//!   (n = journal seq) and `effect` (n = op id).
//!
//! The server's custom `IdGenerator` (08-04) calls [`take_next_ids`]; code that starts a
//! span calls [`set_next_ids`] immediately before `build_with_context`, on the same thread,
//! so the override is consumed by exactly that span (any leftover is cleared right after).
//!
//! # Live-only emission
//!
//! - `activation` spans are emitted by every activation (each has its own counter).
//! - `step` spans are emitted only for steps journaled live, and only after the append
//!   that carries them committed ([`ActivationTrace::collect_steps`] /
//!   [`ActivationTrace::emit_steps`]). Replayed steps emit nothing, so an evicted and
//!   re-activated run has exactly one span per `(run, seq)`.
//! - `workflow.run` is emitted by ostro-server from the post-apply `Action::RunCompleted`.
//! - `effect` spans are emitted by the effect driver (outside the guest) through
//!   [`effect_span`] / [`effect_span_context`].
//!
//! Telemetry never feeds back into guest-visible state or the journal: it reads the
//! journal, never the injected clock (timestamps come from journaled `ts_nanos` or from
//! `SystemTime`, telemetry only), and with no global tracer provider installed (the no-op
//! tracer) nothing beyond the activation span builder runs.

use std::cell::Cell;
use std::collections::HashMap;
use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::propagation::Injector;
use opentelemetry::trace::{
    Span, SpanContext, SpanId, SpanKind, Status, TraceContextExt, TraceFlags, TraceId, TraceState,
    Tracer,
};
use opentelemetry::{Context, KeyValue, global};
use ostro_types::journal::{Event, EventBody, Failure, Payload, decode_result};

use crate::sink::ActivationOutcome;

/// Instrumentation scope of the engine's spans.
pub const TRACER_NAME: &str = "ostro-engine";
/// Span-id kind of the `workflow.run` span (n = 0).
pub const KIND_RUN: &str = "run";
/// Span-id kind of an `activation` span (n = [`activation_n`]).
pub const KIND_ACTIVATION: &str = "activation";
/// Span-id kind of a `step` span (n = journal seq).
pub const KIND_STEP: &str = "step";
/// Span-id kind of an `effect` span (n = op id).
pub const KIND_EFFECT: &str = "effect";
/// Name of the run span.
pub const RUN_SPAN_NAME: &str = "workflow.run";
/// Name of the activation span.
pub const ACTIVATION_SPAN_NAME: &str = "activation";

thread_local! {
    static NEXT_IDS: Cell<Option<([u8; 16], [u8; 8])>> = const { Cell::new(None) };
}

/// Sets the `(trace id, span id)` the next span started on this thread must use.
pub fn set_next_ids(trace: [u8; 16], span: [u8; 8]) {
    NEXT_IDS.with(|c| c.set(Some((trace, span))));
}

/// Takes (and clears) this thread's pending override, if any.
pub fn take_next_ids() -> Option<([u8; 16], [u8; 8])> {
    NEXT_IDS.with(Cell::take)
}

// ------------------------------------------------------------------ ids

/// The run's trace id: the `traceparent`'s trace id when it parses, else
/// `blake3("ostro/trace" ‖ run_id)[..16]`.
pub fn trace_id_for(run_id: &str, traceparent: Option<&str>) -> [u8; 16] {
    if let Some(tp) = traceparent.and_then(parse_traceparent) {
        return tp.trace_id;
    }
    let mut h = blake3::Hasher::new();
    h.update(b"ostro/trace");
    h.update(run_id.as_bytes());
    let mut out = [0u8; 16];
    out.copy_from_slice(&h.finalize().as_bytes()[..16]);
    out
}

/// `blake3("ostro/span" ‖ run_id ‖ kind ‖ n_le)[..8]`.
pub fn span_id(run_id: &str, kind: &str, n: u64) -> [u8; 8] {
    let mut h = blake3::Hasher::new();
    h.update(b"ostro/span");
    h.update(run_id.as_bytes());
    h.update(kind.as_bytes());
    h.update(&n.to_le_bytes());
    let mut out = [0u8; 8];
    out.copy_from_slice(&h.finalize().as_bytes()[..8]);
    out
}

/// `n` of an activation span: `attempt << 32 | counter`.
pub fn activation_n(attempt: u32, counter: u32) -> u64 {
    (u64::from(attempt) << 32) | u64::from(counter)
}

/// A parsed W3C `traceparent` (version `00`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceParent {
    pub trace_id: [u8; 16],
    pub parent_span_id: [u8; 8],
    pub flags: u8,
}

fn hex_into(s: &str, out: &mut [u8]) -> Option<()> {
    let b = s.as_bytes();
    if b.len() != out.len() * 2 {
        return None;
    }
    fn nib(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            _ => None,
        }
    }
    for (i, o) in out.iter_mut().enumerate() {
        *o = (nib(b[2 * i])? << 4) | nib(b[2 * i + 1])?;
    }
    Some(())
}

/// Parse a W3C `traceparent` (`00-<32 hex>-<16 hex>-<2 hex>`, lowercase). All-zero trace
/// or span ids and version `ff` are invalid.
pub fn parse_traceparent(s: &str) -> Option<TraceParent> {
    let mut parts = s.trim().split('-');
    let (v, t, p, f) = (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
    let mut ver = [0u8; 1];
    hex_into(v, &mut ver)?;
    if ver[0] == 0xff || (ver[0] == 0 && parts.next().is_some()) {
        return None;
    }
    let mut trace_id = [0u8; 16];
    let mut parent_span_id = [0u8; 8];
    let mut flags = [0u8; 1];
    hex_into(t, &mut trace_id)?;
    hex_into(p, &mut parent_span_id)?;
    hex_into(f, &mut flags)?;
    if trace_id == [0; 16] || parent_span_id == [0; 8] {
        return None;
    }
    Some(TraceParent {
        trace_id,
        parent_span_id,
        flags: flags[0],
    })
}

fn hex(b: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push(H[usize::from(x >> 4)] as char);
        s.push(H[usize::from(x & 0xf)] as char);
    }
    s
}

/// `nanos since the epoch` → `SystemTime`.
pub fn time_from_nanos(nanos: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(nanos)
}

/// `millis since the epoch` → `SystemTime`.
pub fn time_from_millis(ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms)
}

// ------------------------------------------------------------------ run context

/// The trace identity of one run: its id, trace id, sampling flags and the caller's span
/// (from the stored `traceparent`), all derived without I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunTrace {
    pub run_id: String,
    pub trace_id: [u8; 16],
    pub flags: TraceFlags,
    /// The caller's span id (the parent of `workflow.run`), when a traceparent was given.
    pub caller_span: Option<[u8; 8]>,
}

impl RunTrace {
    /// Build from the run id and its stored traceparent (an unparsable one is ignored).
    pub fn new(run_id: &str, traceparent: Option<&str>) -> Self {
        let tp = traceparent.and_then(parse_traceparent);
        Self {
            run_id: run_id.to_string(),
            trace_id: trace_id_for(run_id, traceparent),
            flags: tp.map_or(TraceFlags::SAMPLED, |t| TraceFlags::new(t.flags & 0x01)),
            caller_span: tp.map(|t| t.parent_span_id),
        }
    }

    /// A span context of this run's trace with a deterministic span id.
    pub fn span_context(&self, kind: &str, n: u64, remote: bool) -> SpanContext {
        SpanContext::new(
            TraceId::from_bytes(self.trace_id),
            SpanId::from_bytes(span_id(&self.run_id, kind, n)),
            self.flags,
            remote,
            TraceState::default(),
        )
    }

    /// The `workflow.run` span context (it may be exported later than its children).
    pub fn run_span_context(&self) -> SpanContext {
        self.span_context(KIND_RUN, 0, true)
    }

    /// The parent context of `workflow.run`: the caller's span when a traceparent was given,
    /// else empty (a root span; the id override carries the derived trace id).
    pub fn run_parent_context(&self) -> Context {
        match self.caller_span {
            Some(p) => Context::new().with_remote_span_context(SpanContext::new(
                TraceId::from_bytes(self.trace_id),
                SpanId::from_bytes(p),
                self.flags,
                true,
                TraceState::default(),
            )),
            None => Context::new(),
        }
    }
}

/// Parent context with `sc` as its (non-recording) span.
fn parent(sc: SpanContext) -> Context {
    Context::new().with_remote_span_context(sc)
}

/// Start a span with deterministic ids: the override is set right before
/// `build_with_context` and any leftover (a tracer that did not consume it) is cleared.
pub fn start_span<T: Tracer>(
    tracer: &T,
    builder: opentelemetry::trace::SpanBuilder,
    trace_id: [u8; 16],
    span: [u8; 8],
    parent_cx: &Context,
) -> T::Span {
    set_next_ids(trace_id, span);
    let s = tracer.build_with_context(builder, parent_cx);
    let _ = take_next_ids();
    s
}

/// Emit the `workflow.run` span (ostro-server's `RunCompleted` consumer): explicit start
/// and end, parent = the caller's span, deterministic id `span_id(run, "run", 0)`.
pub fn emit_run_span(
    run: &RunTrace,
    workflow: &str,
    start: SystemTime,
    end: SystemTime,
    status: &str,
    ok: bool,
) {
    let tracer = global::tracer(TRACER_NAME);
    let builder = tracer
        .span_builder(RUN_SPAN_NAME)
        .with_kind(SpanKind::Internal)
        .with_start_time(start);
    let mut span = start_span(
        &tracer,
        builder,
        run.trace_id,
        span_id(&run.run_id, KIND_RUN, 0),
        &run.run_parent_context(),
    );
    if span.is_recording() {
        span.set_attributes([
            KeyValue::new("ostro.run_id", run.run_id.clone()),
            KeyValue::new("ostro.workflow", workflow.to_string()),
            KeyValue::new("ostro.status", status.to_string()),
        ]);
        span.set_status(if ok {
            Status::Ok
        } else {
            Status::error(status.to_string())
        });
    }
    span.end_with_timestamp(end.max(start));
}

// ------------------------------------------------------------------ effects (Phase 6 hook)

/// The deterministic `effect` span context of `op`: trace id per [`trace_id_for`], span id
/// `span_id(run, "effect", op)`. Needs no live activation (the effect driver runs outside
/// the guest), so ostro-server's effect driver can parent and propagate it.
pub fn effect_span_context(run_id: &str, traceparent: Option<&str>, op: u64) -> SpanContext {
    RunTrace::new(run_id, traceparent).span_context(KIND_EFFECT, op, false)
}

/// Emit the `effect` span of `op` (name `effect:<kind>`, parent = the run span) with
/// explicit start and end. `outcome` is `Ok(())` or `Err(failure kind)`. Call only for live
/// I/O (never for a replayed completion).
pub fn effect_span(
    run: &RunTrace,
    op: u64,
    kind: &str,
    start: SystemTime,
    end: SystemTime,
    outcome: Result<(), &str>,
) {
    let tracer = global::tracer(TRACER_NAME);
    let builder = tracer
        .span_builder(format!("effect:{kind}"))
        .with_kind(SpanKind::Client)
        .with_start_time(start);
    let mut span = start_span(
        &tracer,
        builder,
        run.trace_id,
        span_id(&run.run_id, KIND_EFFECT, op),
        &parent(run.run_span_context()),
    );
    if span.is_recording() {
        span.set_attributes([
            KeyValue::new("ostro.run_id", run.run_id.clone()),
            KeyValue::new("ostro.op", op as i64),
            KeyValue::new("ostro.effect_kind", kind.to_string()),
            KeyValue::new("ostro.outcome", outcome.err().unwrap_or("ok").to_string()),
        ]);
        if let Err(k) = outcome {
            span.set_status(Status::error(k.to_string()));
        }
    }
    span.end_with_timestamp(end.max(start));
}

// ------------------------------------------------------------------ propagation

struct VecInjector<'a>(&'a mut Vec<(String, String)>);

impl Injector for VecInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.push((key.to_string(), value));
    }
}

/// The W3C `traceparent` value of `cx` (`00-<trace>-<span>-<flags>`).
pub fn format_traceparent(cx: &SpanContext) -> String {
    format!(
        "00-{}-{}-{:02x}",
        hex(&cx.trace_id().to_bytes()),
        hex(&cx.span_id().to_bytes()),
        cx.trace_flags().to_u8()
    )
}

/// Outbound propagation headers for `cx` through the global propagator (the server installs
/// `TraceContextPropagator`). Without a propagator that emits `traceparent`, the W3C header
/// is formatted directly, so outbound HTTP always carries one. Empty for an invalid `cx`.
pub fn traceparent_headers(cx: &SpanContext) -> Vec<(String, String)> {
    if !cx.is_valid() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let ctx = parent(cx.clone());
    global::get_text_map_propagator(|p| p.inject_context(&ctx, &mut VecInjector(&mut out)));
    if !out
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("traceparent"))
    {
        out.push(("traceparent".to_string(), format_traceparent(cx)));
    }
    out
}

// ------------------------------------------------------------------ activation seed

tokio::task_local! {
    static RUN_TRACEPARENT: Option<String>;
}

/// Run `f` (an activation) with the run's stored traceparent visible to the engine.
/// ostro-server's executor wraps `Runtime::activate` in this; without it the trace id is
/// derived from the run id.
pub async fn with_traceparent<F: Future>(traceparent: Option<String>, f: F) -> F::Output {
    RUN_TRACEPARENT.scope(traceparent, f).await
}

/// The traceparent set by [`with_traceparent`] for the current task, if any.
pub fn current_traceparent() -> Option<String> {
    RUN_TRACEPARENT.try_with(Clone::clone).ok().flatten()
}

// ------------------------------------------------------------------ activation + steps

/// A live step recorded by an append, waiting for the append to commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepRecord {
    pub seq: u64,
    pub name: String,
    pub attempt: u32,
    pub start: SystemTime,
    pub end: SystemTime,
    /// `ok`, `retry` (a failed attempt that is retried), `cancelled`, `timeout` or `error`.
    pub outcome: &'static str,
}

/// Per-activation trace state kept in the host state.
#[derive(Debug, Clone)]
pub struct ActivationTrace {
    pub run: RunTrace,
    /// The activation span's id (the parent of every step span).
    pub span_id: [u8; 8],
    /// Whether the activation span records (false with the no-op tracer: steps are skipped).
    pub recording: bool,
    /// Start time and attempt of live step attempts, by seq.
    step_starts: HashMap<u64, (SystemTime, u32)>,
}

impl ActivationTrace {
    /// The activation span context (parent for Phase 6/7 work done inside the activation).
    pub fn span_context(&self) -> SpanContext {
        SpanContext::new(
            TraceId::from_bytes(self.run.trace_id),
            SpanId::from_bytes(self.span_id),
            self.run.flags,
            false,
            TraceState::default(),
        )
    }

    /// A live step attempt started at `seq` (telemetry timestamp only).
    pub fn step_started(&mut self, seq: u64, attempt: u32) {
        if self.recording {
            let now = SystemTime::now(); // clock-exempt: span timestamp only
            self.step_starts.insert(seq, (now, attempt));
        }
    }

    /// The step records of a batch about to be appended (empty when not recording).
    pub fn collect_steps(&mut self, batch: &[Event]) -> Vec<StepRecord> {
        if !self.recording {
            return Vec::new();
        }
        let mut out = Vec::new();
        for ev in batch {
            let (name, attempt, outcome) = match &ev.body {
                EventBody::StepCompleted(sc) => {
                    let outcome = match decode_result::<Result<Payload, Failure>>(&sc.outcome) {
                        Ok(Ok(_)) => "ok",
                        Ok(Err(Failure::Cancelled)) => "cancelled",
                        Ok(Err(Failure::Timeout)) => "timeout",
                        _ => "error",
                    };
                    (sc.name.clone(), None, outcome)
                }
                EventBody::StepAttemptFailed(f) => (f.name.clone(), Some(f.attempt), "retry"),
                _ => continue,
            };
            let end = time_from_nanos(ev.ts_nanos);
            let (start, started_attempt) = self
                .step_starts
                .remove(&ev.seq)
                .map_or((end, None), |(s, a)| (s, Some(a)));
            out.push(StepRecord {
                seq: ev.seq,
                name,
                attempt: attempt.or(started_attempt).unwrap_or(1),
                start: start.min(end),
                end,
                outcome,
            });
        }
        out
    }

    /// Emit the step spans of a committed append.
    pub fn emit_steps(&self, steps: Vec<StepRecord>) {
        if steps.is_empty() {
            return;
        }
        let tracer = global::tracer(TRACER_NAME);
        let parent_cx = parent(self.span_context());
        for st in steps {
            let builder = tracer
                .span_builder(st.name.clone())
                .with_kind(SpanKind::Internal)
                .with_start_time(st.start);
            let mut span = start_span(
                &tracer,
                builder,
                self.run.trace_id,
                span_id(&self.run.run_id, KIND_STEP, st.seq),
                &parent_cx,
            );
            if span.is_recording() {
                span.set_attributes([
                    KeyValue::new("ostro.run_id", self.run.run_id.clone()),
                    KeyValue::new("ostro.seq", st.seq as i64),
                    KeyValue::new("ostro.attempt", i64::from(st.attempt)),
                    KeyValue::new("ostro.outcome", st.outcome),
                ]);
                if st.outcome != "ok" {
                    span.set_status(Status::error(st.outcome));
                }
            }
            span.end_with_timestamp(st.end);
        }
    }
}

impl crate::host::state::HostState {
    /// The current activation's span context (parent for Phase 6/7 work started inside the
    /// activation); `None` outside the activation driver.
    pub fn activation_span_context(&self) -> Option<SpanContext> {
        self.trace.as_ref().map(ActivationTrace::span_context)
    }
}

/// The live `activation` span of one activation.
pub struct ActivationSpan {
    span: global::BoxedSpan,
    trace: ActivationTrace,
}

impl std::fmt::Debug for ActivationSpan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActivationSpan")
            .field("trace", &self.trace)
            .finish_non_exhaustive()
    }
}

impl ActivationSpan {
    /// Start the span `activation` with id `span_id(run, "activation", attempt << 32 |
    /// counter)`, parent = the (deterministic, possibly not yet exported) run span.
    pub fn start(run: RunTrace, attempt: u32, counter: u32, now: SystemTime) -> Self {
        let tracer = global::tracer(TRACER_NAME);
        let id = span_id(&run.run_id, KIND_ACTIVATION, activation_n(attempt, counter));
        let builder = tracer
            .span_builder(ACTIVATION_SPAN_NAME)
            .with_kind(SpanKind::Internal)
            .with_start_time(now);
        let mut span = start_span(
            &tracer,
            builder,
            run.trace_id,
            id,
            &parent(run.run_span_context()),
        );
        let recording = span.is_recording();
        if recording {
            span.set_attributes([
                KeyValue::new("ostro.run_id", run.run_id.clone()),
                KeyValue::new("ostro.attempt", i64::from(attempt)),
            ]);
        }
        Self {
            span,
            trace: ActivationTrace {
                run,
                span_id: id,
                recording,
                step_starts: HashMap::new(),
            },
        }
    }

    /// The per-activation trace state (handed to the host state).
    pub fn trace(&self) -> &ActivationTrace {
        &self.trace
    }

    /// Record the workflow name and the number of journal events this activation replays.
    pub fn set_details(&mut self, workflow: &str, replayed_events: u64) {
        if self.trace.recording {
            self.span.set_attributes([
                KeyValue::new("ostro.workflow", workflow.to_string()),
                KeyValue::new("ostro.replayed_events", replayed_events as i64),
            ]);
        }
    }

    /// End the span with the activation's outcome (Error on a failed attempt: trap, fuel,
    /// nondeterminism, ...).
    pub fn end(mut self, outcome: &ActivationOutcome, code: &'static str, now: SystemTime) {
        if self.trace.recording {
            self.span
                .set_attribute(KeyValue::new("ostro.outcome", code.to_string()));
            if let ActivationOutcome::AttemptFailed(f) = outcome {
                self.span.set_status(Status::error(f.reason_code()));
            }
        }
        self.span.end_with_timestamp(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn override_is_taken_once_and_thread_local() {
        assert_eq!(take_next_ids(), None);
        set_next_ids([1; 16], [2; 8]);
        std::thread::spawn(|| assert_eq!(take_next_ids(), None))
            .join()
            .expect("thread");
        assert_eq!(take_next_ids(), Some(([1; 16], [2; 8])));
        assert_eq!(take_next_ids(), None);
        set_next_ids([3; 16], [4; 8]);
        set_next_ids([5; 16], [6; 8]);
        assert_eq!(take_next_ids(), Some(([5; 16], [6; 8])), "last set wins");
    }

    /// Pinned vectors (P8-D14): changing the derivation changes every exported id.
    #[test]
    fn telemetry_id_vectors_are_pinned() {
        assert_eq!(
            hex(&trace_id_for("run-1", None)),
            "86ca30530a6d71a97854d22c0272e94b",
            "derived trace id"
        );
        assert_eq!(hex(&span_id("run-1", KIND_RUN, 0)), "d3ede864a16f6334");
        assert_eq!(
            hex(&span_id("run-1", KIND_ACTIVATION, activation_n(1, 1))),
            "9bc352849fc38bc5"
        );
        assert_eq!(hex(&span_id("run-1", KIND_STEP, 3)), "a6b04adb4033b468");
        assert_eq!(hex(&span_id("run-1", KIND_EFFECT, 7)), "a4b3d0fc243cbad8");
    }

    #[test]
    fn telemetry_ids_follow_the_formula() {
        let mut h = blake3::Hasher::new();
        h.update(b"ostro/spanrun-1step");
        h.update(&3u64.to_le_bytes());
        assert_eq!(span_id("run-1", KIND_STEP, 3), h.finalize().as_bytes()[..8]);
        let mut h = blake3::Hasher::new();
        h.update(b"ostro/tracerun-1");
        assert_eq!(trace_id_for("run-1", None), h.finalize().as_bytes()[..16]);
        assert_ne!(
            span_id("run-1", KIND_STEP, 3),
            span_id("run-2", KIND_STEP, 3)
        );
        assert_ne!(
            span_id("run-1", KIND_STEP, 3),
            span_id("run-1", KIND_EFFECT, 3)
        );
        assert_eq!(activation_n(2, 5), (2 << 32) | 5);
    }

    #[test]
    fn telemetry_traceparent_parsing() {
        let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let p = parse_traceparent(tp).expect("valid");
        assert_eq!(hex(&p.trace_id), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(hex(&p.parent_span_id), "00f067aa0ba902b7");
        assert_eq!(p.flags, 1);
        assert_eq!(trace_id_for("any", Some(tp)), p.trace_id);
        for bad in [
            "",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-x",
        ] {
            assert_eq!(parse_traceparent(bad), None, "{bad:?}");
        }
        // An invalid traceparent falls back to the derived id.
        assert_eq!(
            trace_id_for("run-1", Some("garbage")),
            trace_id_for("run-1", None)
        );
        let rt = RunTrace::new("run-1", Some(tp));
        assert_eq!(rt.caller_span, Some(p.parent_span_id));
        assert_eq!(rt.flags, TraceFlags::SAMPLED);
        let rt = RunTrace::new("run-1", None);
        assert_eq!(rt.caller_span, None);
        assert_eq!(rt.trace_id, trace_id_for("run-1", None));
    }

    #[test]
    fn telemetry_traceparent_headers_fallback_and_effect_context() {
        let cx = effect_span_context("run-1", None, 7);
        assert_eq!(cx.trace_id().to_bytes(), trace_id_for("run-1", None));
        assert_eq!(cx.span_id().to_bytes(), span_id("run-1", KIND_EFFECT, 7));
        // No global propagator in this unit test binary: the W3C header is formatted.
        let h = traceparent_headers(&cx);
        assert_eq!(h.len(), 1);
        assert_eq!(h[0].0, "traceparent");
        let p = parse_traceparent(&h[0].1).expect("round-trips");
        assert_eq!(p.trace_id, trace_id_for("run-1", None));
        assert_eq!(p.parent_span_id, span_id("run-1", KIND_EFFECT, 7));
        assert_eq!(p.flags, 1);
        assert!(traceparent_headers(&SpanContext::empty_context()).is_empty());
    }
}
