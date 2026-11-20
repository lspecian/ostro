//! OBS-01 engine spans (plan 08-11): deterministic activation and live-only step spans.
//!
//! A recording tracer built on the OpenTelemetry **API** crate only (ostro-engine never
//! depends on the SDK) is installed as the global provider. It consumes the engine's id
//! override (`take_next_ids`) exactly like ostro-server's `OstroIdGenerator`, so the
//! recorded ids are the exported ids.
//!
//! One test function: the global provider is process-wide, and the first leg runs
//! before it is installed (the no-op tracer path, which must journal identically).

mod common;

use std::borrow::Cow;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use opentelemetry::trace::{
    Span, SpanBuilder, SpanContext, SpanId, Status, TraceContextExt, TraceFlags, TraceId,
    TraceState, Tracer, TracerProvider,
};
use opentelemetry::{Context, InstrumentationScope, KeyValue, global};
use ostro_engine::config::EngineConfig;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::telemetry::{
    self, KIND_ACTIVATION, KIND_RUN, KIND_STEP, RunTrace, activation_n, span_id, trace_id_for,
};
use ostro_engine::testkit::{DEFAULT_ACCEPTED_AT, FakeClock, FakeSink, RunSpec};
use ostro_types::journal::{Event, EventBody, Payload, encode_event};
use tokio_util::sync::CancellationToken;

// ------------------------------------------------------------------ recording tracer

#[derive(Debug, Clone)]
struct Rec {
    name: String,
    trace: [u8; 16],
    span: [u8; 8],
    parent: Option<[u8; 8]>,
    attrs: Vec<KeyValue>,
    start: Option<SystemTime>,
    end: SystemTime,
    status: Status,
}

impl Rec {
    fn attr(&self, k: &str) -> Option<String> {
        self.attrs
            .iter()
            .find(|kv| kv.key.as_str() == k)
            .map(|kv| kv.value.to_string())
    }
}

type Sink = Arc<Mutex<Vec<Rec>>>;

#[derive(Clone, Default)]
struct RecProvider(Sink);

struct RecTracer(Sink);

struct RecSpan {
    cx: SpanContext,
    rec: Rec,
    sink: Sink,
    ended: bool,
}

impl TracerProvider for RecProvider {
    type Tracer = RecTracer;
    fn tracer_with_scope(&self, _scope: InstrumentationScope) -> RecTracer {
        RecTracer(self.0.clone())
    }
}

impl Tracer for RecTracer {
    type Span = RecSpan;
    fn build_with_context(&self, b: SpanBuilder, parent_cx: &Context) -> RecSpan {
        // Same order as the SDK + OstroIdGenerator: the override is consumed here.
        let (trace, span) = telemetry::take_next_ids().expect("id override set before build");
        let p = parent_cx.span().span_context().clone();
        let parent = p.is_valid().then(|| {
            assert_eq!(
                p.trace_id().to_bytes(),
                trace,
                "child inherits the trace id"
            );
            p.span_id().to_bytes()
        });
        RecSpan {
            cx: SpanContext::new(
                TraceId::from_bytes(trace),
                SpanId::from_bytes(span),
                TraceFlags::SAMPLED,
                false,
                TraceState::default(),
            ),
            rec: Rec {
                name: b.name.to_string(),
                trace,
                span,
                parent,
                attrs: b.attributes.unwrap_or_default(),
                start: b.start_time,
                end: SystemTime::UNIX_EPOCH,
                status: Status::Unset,
            },
            sink: self.0.clone(),
            ended: false,
        }
    }
}

impl Span for RecSpan {
    fn add_event_with_timestamp<T: Into<Cow<'static, str>>>(
        &mut self,
        _name: T,
        _timestamp: SystemTime,
        _attributes: Vec<KeyValue>,
    ) {
    }
    fn span_context(&self) -> &SpanContext {
        &self.cx
    }
    fn is_recording(&self) -> bool {
        !self.ended
    }
    fn set_attribute(&mut self, attribute: KeyValue) {
        self.rec.attrs.push(attribute);
    }
    fn set_status(&mut self, status: Status) {
        self.rec.status = status;
    }
    fn update_name<T: Into<Cow<'static, str>>>(&mut self, new_name: T) {
        self.rec.name = new_name.into().into_owned();
    }
    fn add_link(&mut self, _span_context: SpanContext, _attributes: Vec<KeyValue>) {}
    fn end_with_timestamp(&mut self, timestamp: SystemTime) {
        if !self.ended {
            self.ended = true;
            self.rec.end = timestamp;
            self.sink.lock().expect("sink").push(self.rec.clone());
        }
    }
}

// ------------------------------------------------------------------ harness

fn runtime() -> Arc<Runtime> {
    Arc::new(
        Runtime::new(
            EngineConfig {
                hot_wait: Duration::ZERO,
                ..EngineConfig::default()
            },
            RuntimeOptions {
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 1_000)),
                token_key: [7; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime"),
    )
}

fn spec(run_id: &str, workflow: &str, input: &str) -> RunSpec {
    let mut s = RunSpec::new(
        RunRef {
            partition: 2,
            run_id: run_id.to_string(),
        },
        workflow,
        input.as_bytes(),
    );
    s.input = Payload {
        content_type: "application/json".to_string(),
        data: input.as_bytes().to_vec(),
    };
    s
}

async fn activate(
    rt: &Runtime,
    p: &Prepared,
    sink: &Arc<FakeSink>,
    s: &RunSpec,
    traceparent: Option<&str>,
) -> ActivationOutcome {
    let act = Activate {
        run: s.run.clone(),
        attempt: 1,
        module_sha: p.sha,
        journal: sink.journal(&s.run),
        inputs: None,
        cancel: CancellationToken::new(),
    };
    let fut = rt.activate(p, act, sink.clone());
    telemetry::with_traceparent(traceparent.map(str::to_string), fut).await
}

fn seed(sink: &FakeSink, s: &RunSpec, p: &Prepared) {
    sink.seed(
        s.run.clone(),
        vec![s.run_started_event(&p.module_hash_hex)],
        1,
    );
}

fn bytes(journal: &[Event]) -> Vec<(u32, Vec<u8>)> {
    journal.iter().map(encode_event).collect()
}

fn of_run(recs: &Sink, run: &str) -> Vec<Rec> {
    recs.lock()
        .expect("sink")
        .iter()
        .filter(|r| r.attr("ostro.run_id").as_deref() == Some(run))
        .cloned()
        .collect()
}

fn step_seqs(journal: &[Event]) -> Vec<(u64, String)> {
    journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::StepCompleted(sc) => Some((e.seq, sc.name.clone())),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn telemetry_spans_live_only_deterministic() {
    let rt = runtime();
    let steps = Arc::new(
        rt.prepare(&common::guest_wasm("trivial-steps"))
            .expect("prepare trivial-steps"),
    );

    // 1. No global provider (no-op tracer): the reference journal.
    let s0 = spec("tel-run-a", "trivial-steps", r#"{"n":3}"#);
    let sink0 = Arc::new(FakeSink::new());
    seed(&sink0, &s0, &steps);
    let out = activate(&rt, &steps, &sink0, &s0, None).await;
    assert!(
        matches!(out, ActivationOutcome::Completed { .. }),
        "{out:?}"
    );
    let reference = sink0.journal(&s0.run);
    assert_eq!(telemetry::take_next_ids(), None, "no override left behind");

    // 2. Install the recorder; the same run journals byte-identically.
    let recs: Sink = Arc::default();
    global::set_tracer_provider(RecProvider(recs.clone()));
    // A fresh runtime (fresh FakeClock: identical timestamps); modules are per engine.
    let rt = runtime();
    let steps = Arc::new(
        rt.prepare(&common::guest_wasm("trivial-steps"))
            .expect("prepare trivial-steps"),
    );
    let sink = Arc::new(FakeSink::new());
    seed(&sink, &s0, &steps);
    let out = activate(&rt, &steps, &sink, &s0, None).await;
    assert!(
        matches!(out, ActivationOutcome::Completed { .. }),
        "{out:?}"
    );
    let journal = sink.journal(&s0.run);
    assert_eq!(
        bytes(&journal),
        bytes(&reference),
        "telemetry never changes the journal"
    );

    let run = "tel-run-a";
    let trace = trace_id_for(run, None);
    let run_span = span_id(run, KIND_RUN, 0);
    let act1 = span_id(run, KIND_ACTIVATION, activation_n(1, 1));
    let first = of_run(&recs, run);
    let acts: Vec<&Rec> = first.iter().filter(|r| r.name == "activation").collect();
    let stepr: Vec<&Rec> = first.iter().filter(|r| r.name != "activation").collect();
    assert_eq!(acts.len(), 1, "{first:#?}");
    assert_eq!(acts[0].span, act1, "activation id: attempt 1, counter 1");
    assert_eq!(acts[0].trace, trace);
    assert_eq!(
        acts[0].parent,
        Some(run_span),
        "activation's parent is the run"
    );
    assert_eq!(
        acts[0].attr("ostro.workflow").as_deref(),
        Some("trivial-steps")
    );
    assert_eq!(acts[0].attr("ostro.attempt").as_deref(), Some("1"));
    assert_eq!(acts[0].attr("ostro.replayed_events").as_deref(), Some("1"));
    assert!(acts[0].start.is_some());
    let seqs = step_seqs(&journal);
    assert_eq!(seqs.len(), 3);
    assert_eq!(stepr.len(), 3, "{first:#?}");
    for ((seq, name), r) in seqs.iter().zip(&stepr) {
        assert_eq!(&r.name, name);
        assert_eq!(r.span, span_id(run, KIND_STEP, *seq), "step id = seq {seq}");
        assert_eq!(r.trace, trace);
        assert_eq!(r.parent, Some(act1), "step's parent is the activation");
        assert_eq!(r.attr("ostro.seq"), Some(seq.to_string()));
        assert_eq!(r.attr("ostro.attempt").as_deref(), Some("1"));
        assert_eq!(r.attr("ostro.outcome").as_deref(), Some("ok"));
        let start = r.start.expect("explicit start");
        assert!(start <= r.end, "start <= end");
        let ev = journal.iter().find(|e| e.seq == *seq).expect("event");
        assert_eq!(
            r.end,
            telemetry::time_from_nanos(ev.ts_nanos),
            "end = completion ts"
        );
    }
    let first_step_ids: Vec<[u8; 8]> = stepr.iter().map(|r| r.span).collect();

    // 3. Replay the completed journal in a fresh activation: 1 activation span (new
    //    counter), 0 step spans, nothing appended.
    recs.lock().expect("sink").clear();
    let appends = sink.append_count();
    let out = activate(&rt, &steps, &sink, &s0, None).await;
    assert!(
        matches!(out, ActivationOutcome::Completed { .. }),
        "{out:?}"
    );
    assert_eq!(
        sink.append_count(),
        appends,
        "a full replay appends nothing"
    );
    let second = of_run(&recs, run);
    assert_eq!(second.len(), 1, "only the activation span: {second:#?}");
    let n = journal.len() as u32;
    assert_eq!(
        second[0].span,
        span_id(run, KIND_ACTIVATION, activation_n(1, n))
    );
    assert_ne!(second[0].span, act1);
    assert_eq!(
        second[0].attr("ostro.replayed_events"),
        Some(journal.len().to_string())
    );
    // The step ids are a pure function of (run, seq): a re-activation that ran them live
    // would reproduce the same ids.
    let again: Vec<[u8; 8]> = seqs
        .iter()
        .map(|(seq, _)| span_id(run, KIND_STEP, *seq))
        .collect();
    assert_eq!(again, first_step_ids);

    // 4. With a caller traceparent every span is in the caller's trace.
    let tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let s1 = spec("tel-run-b", "trivial-steps", r#"{"n":2}"#);
    seed(&sink, &s1, &steps);
    let out = activate(&rt, &steps, &sink, &s1, Some(tp)).await;
    assert!(
        matches!(out, ActivationOutcome::Completed { .. }),
        "{out:?}"
    );
    let b = of_run(&recs, "tel-run-b");
    assert_eq!(b.len(), 3, "{b:#?}");
    let caller_trace = RunTrace::new("tel-run-b", Some(tp)).trace_id;
    for r in &b {
        assert_eq!(r.trace, caller_trace, "{}", r.name);
    }

    // 5. A step then a durable sleep (det-basic) with hot_wait 0: the first activation
    //    runs the step live and evicts; the second replays it and evicts again. Exactly
    //    one step span per (run, seq) across both.
    let det = Arc::new(
        rt.prepare(&common::guest_wasm("det-basic"))
            .expect("prepare det-basic"),
    );
    let mut s2 = spec("tel-run-c", "det", "");
    s2.input = Payload {
        content_type: "application/octet-stream".to_string(),
        data: Vec::new(),
    };
    seed(&sink, &s2, &det);
    for _ in 0..2 {
        let out = activate(&rt, &det, &sink, &s2, None).await;
        assert!(matches!(out, ActivationOutcome::Evicted { .. }), "{out:?}");
    }
    let c = of_run(&recs, "tel-run-c");
    let c_steps: Vec<&Rec> = c.iter().filter(|r| r.name != "activation").collect();
    let c_acts: Vec<&Rec> = c.iter().filter(|r| r.name == "activation").collect();
    assert_eq!(c_acts.len(), 2, "{c:#?}");
    assert_ne!(c_acts[0].span, c_acts[1].span, "distinct activation ids");
    let journal_c = sink.journal(&s2.run);
    let live_steps = step_seqs(&journal_c);
    assert_eq!(c_steps.len(), live_steps.len(), "{c:#?}");
    assert!(!c_steps.is_empty());
    for ((seq, _), r) in live_steps.iter().zip(&c_steps) {
        assert_eq!(r.span, span_id("tel-run-c", KIND_STEP, *seq));
        assert_eq!(
            r.parent,
            Some(c_acts[0].span),
            "emitted by the live activation"
        );
    }

    assert_eq!(telemetry::take_next_ids(), None, "no override left behind");
}
