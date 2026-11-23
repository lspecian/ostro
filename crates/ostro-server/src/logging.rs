//! Tracing subscriber initialisation (plan 08-04 registry composition).
//!
//! The global subscriber is one registry:
//!
//! 1. [`OtelSlotLayer`]: the OpenTelemetry layers, installed **later** by
//!    [`crate::telemetry::Telemetry::init`] (the OTLP providers must be built inside the
//!    server's Tokio runtime, while the binary installs the subscriber before it). The
//!    slot holds the `tracing-opentelemetry` span layer and the
//!    `opentelemetry-appender-tracing` log bridge; empty, it costs one atomic load per
//!    callback.
//! 2. The fmt layer: pretty, compact or JSON ([`LogFormat`]).
//! 3. `EnvFilter` (`RUST_LOG`, else `[log] level`), a global filter for every layer.
//!
//! The log bridge never exports events whose target starts with one of
//! [`BRIDGE_EXCLUDED_TARGETS`]: the OTel SDK and its transport (`opentelemetry*`, `tonic`,
//! `h2`, `hyper`, `tower`, `reqwest`) would feed their own export back into the exporter,
//! and `ostro::guest` lines are exported as OTLP LogRecords directly by the guest log sink
//! (08-17), so bridging them would export every guest line twice (W4).
//!
//! The span layer likewise never sees spans or events of the crates in
//! [`SPAN_EXCLUDED_TARGETS`] (the OTel SDK and the OTLP gRPC transport: `tonic`, `h2`,
//! `tower`; matched at a crate boundary, so `tower_http` stays visible). At `trace` level the exporter's own gRPC spans would otherwise be exported
//! by the exporter they describe, a feedback loop that floods the batch queue (08-21:
//! 120k spans in 40 s). The outbound HTTP path of the bridge (`hyper`, `reqwest`) stays
//! visible to the span layer.

use std::any::TypeId;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{PoisonError, RwLock};

use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_sdk::logs::{SdkLogger, SdkLoggerProvider};
use opentelemetry_sdk::trace::SdkTracer;
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Subscriber};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt as _};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{EnvFilter, Registry, fmt};

use crate::config::{LogFormat, LogSection};

/// Targets (prefixes) the OTLP log bridge never exports (feedback loop / double export).
pub const BRIDGE_EXCLUDED_TARGETS: &[&str] = &[
    "opentelemetry",
    "opentelemetry_sdk",
    "opentelemetry_otlp",
    "tonic",
    "h2",
    "hyper",
    "tower",
    "reqwest",
    "ostro::guest",
];

/// Whether the OTLP log bridge skips events of `target`.
pub fn bridge_excludes(target: &str) -> bool {
    BRIDGE_EXCLUDED_TARGETS
        .iter()
        .any(|p| target.starts_with(p))
}

/// Crates the OTLP span layer never sees (exporter feedback loop, 08-21). Matched at a crate
/// boundary (`tower` excludes `tower::buffer`, not `tower_http`, whose `request` span is the
/// REST request span).
pub const SPAN_EXCLUDED_TARGETS: &[&str] = &[
    "opentelemetry",
    "opentelemetry_sdk",
    "opentelemetry_otlp",
    "tonic",
    "h2",
    "tower",
];

/// Whether the OTLP span layer skips spans and events of `target`.
pub fn span_layer_excludes(target: &str) -> bool {
    SPAN_EXCLUDED_TARGETS.iter().any(|krate| {
        target
            .strip_prefix(krate)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with("::"))
    })
}

type SpanLayer = OpenTelemetryLayer<Registry, SdkTracer>;
type LogBridge = OpenTelemetryTracingBridge<SdkLoggerProvider, SdkLogger>;

/// One generation of installed OTel layers.
struct OtelLayers {
    generation: u64,
    spans: Option<SpanLayer>,
    logs: Option<LogBridge>,
}

/// The installed layers. Each generation is leaked (`&'static`): `downcast_raw` hands
/// out raw pointers into the span layer (tracing-opentelemetry's `WithContext`), which
/// must stay valid even after a later [`Telemetry::init`](crate::telemetry::Telemetry)
/// replaced or cleared the slot. A process installs one generation per server start
/// (one in production), so the leak is bounded.
static SLOT: RwLock<Option<&'static OtelLayers>> = RwLock::new(None);
/// Fast path: false while the slot is empty.
static ACTIVE: AtomicBool = AtomicBool::new(false);
static GENERATION: AtomicU64 = AtomicU64::new(0);
/// Set when [`init_tracing`] installed the global subscriber (so the slot is wired).
static OURS: AtomicBool = AtomicBool::new(false);

fn current() -> Option<&'static OtelLayers> {
    if !ACTIVE.load(Ordering::Acquire) {
        return None;
    }
    *SLOT.read().unwrap_or_else(PoisonError::into_inner)
}

/// Install the OTel layers built by [`crate::telemetry::Telemetry::init`]; returns the
/// generation to pass to [`clear_otel_layers`].
pub(crate) fn install_otel_layers(
    spans: Option<SdkTracer>,
    logs: Option<&SdkLoggerProvider>,
) -> u64 {
    let generation = GENERATION.fetch_add(1, Ordering::Relaxed) + 1;
    let layers: &'static OtelLayers = Box::leak(Box::new(OtelLayers {
        generation,
        spans: spans.map(|t| tracing_opentelemetry::layer().with_tracer(t)),
        logs: logs.map(OpenTelemetryTracingBridge::new),
    }));
    *SLOT.write().unwrap_or_else(PoisonError::into_inner) = Some(layers);
    ACTIVE.store(true, Ordering::Release);
    generation
}

/// Empty the slot if it still holds `generation` (a later server's layers stay).
pub(crate) fn clear_otel_layers(generation: u64) {
    let mut slot = SLOT.write().unwrap_or_else(PoisonError::into_inner);
    if slot.is_some_and(|l| l.generation == generation) {
        *slot = None;
        ACTIVE.store(false, Ordering::Release);
    }
}

/// Whether the global subscriber is the one [`init_tracing`] installed (only then do the
/// OTel layers see anything).
pub fn subscriber_is_ours() -> bool {
    OURS.load(Ordering::Acquire)
}

/// The late-bound OTel layer slot (first layer on the registry).
#[derive(Clone, Copy, Debug, Default)]
pub struct OtelSlotLayer;

impl Layer<Registry> for OtelSlotLayer {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, Registry>) {
        if span_layer_excludes(attrs.metadata().target()) {
            // No OtelData is attached, so the span layer ignores every later callback for
            // this span id (record, enter, exit, close) and exports nothing for it.
            return;
        }
        if let Some(SpanLayerRef(l)) = current().and_then(spans) {
            l.on_new_span(attrs, id, ctx);
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, Registry>) {
        if let Some(SpanLayerRef(l)) = current().and_then(spans) {
            l.on_record(id, values, ctx);
        }
    }

    fn on_follows_from(&self, id: &Id, follows: &Id, ctx: Context<'_, Registry>) {
        if let Some(SpanLayerRef(l)) = current().and_then(spans) {
            l.on_follows_from(id, follows, ctx);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, Registry>) {
        let Some(layers) = current() else { return };
        if let Some(l) = &layers.spans
            && !span_layer_excludes(event.metadata().target())
        {
            l.on_event(event, ctx.clone());
        }
        if let Some(b) = &layers.logs
            && !bridge_excludes(event.metadata().target())
        {
            b.on_event(event, ctx);
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, Registry>) {
        if let Some(SpanLayerRef(l)) = current().and_then(spans) {
            l.on_enter(id, ctx);
        }
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, Registry>) {
        if let Some(SpanLayerRef(l)) = current().and_then(spans) {
            l.on_exit(id, ctx);
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, Registry>) {
        if let Some(SpanLayerRef(l)) = current().and_then(spans) {
            l.on_close(id, ctx);
        }
    }

    // SAFETY (trait contract): pointers returned point into a leaked `&'static OtelLayers`
    // (see `SLOT`), so they stay valid for the rest of the process.
    unsafe fn downcast_raw(&self, id: TypeId) -> Option<*const ()> {
        if id == TypeId::of::<Self>() {
            return Some(self as *const Self as *const ());
        }
        let SpanLayerRef(l) = current().and_then(spans)?;
        // SAFETY: forwarded to the inner layer's own implementation; see above.
        unsafe { l.downcast_raw(id) }
    }
}

struct SpanLayerRef(&'static SpanLayer);

fn spans(l: &'static OtelLayers) -> Option<SpanLayerRef> {
    l.spans.as_ref().map(SpanLayerRef)
}

/// The fmt layer for `format`, writing to `writer`.
pub fn fmt_layer<S, W>(format: LogFormat, writer: W) -> Box<dyn Layer<S> + Send + Sync>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: for<'w> MakeWriter<'w> + Send + Sync + 'static,
{
    let base = fmt::layer().with_writer(writer);
    match format {
        LogFormat::Pretty => base.pretty().boxed(),
        LogFormat::Compact => base.compact().boxed(),
        LogFormat::Json => base.json().boxed(),
    }
}

/// The fmt layer, on **stderr**: stdout carries only machine-readable output (the
/// frozen ready line, `--json`, `--print-config`), so a script reading stdout never has
/// to skip log lines. ANSI colours only when stderr is a terminal (07-22: a piped or
/// redirected `ostro server` / `ostro worker` writes plain text).
fn stderr_layer<S>(format: LogFormat) -> Box<dyn Layer<S> + Send + Sync>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    use std::io::IsTerminal as _;
    let base = fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal());
    match format {
        LogFormat::Pretty => base.pretty().boxed(),
        LogFormat::Compact => base.compact().boxed(),
        LogFormat::Json => base.json().boxed(),
    }
}

/// Install the global subscriber: `RUST_LOG` wins over `cfg.level`; pretty, compact or
/// JSON output on stderr per `cfg.format`; the OTel slot is wired for a later
/// [`crate::telemetry::Telemetry::init`]. Idempotent: a second call (or one after another
/// subscriber was installed, e.g. in tests) is a no-op and returns `false`.
pub fn init_tracing(cfg: &LogSection) -> bool {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&cfg.level))
        .unwrap_or_else(|_| EnvFilter::new("info"));
    let installed = tracing_subscriber::registry()
        .with(OtelSlotLayer)
        .with(stderr_layer(cfg.format))
        .with(filter)
        .try_init()
        .is_ok();
    if installed {
        OURS.store(true, Ordering::Release);
    }
    installed
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use super::*;

    #[test]
    fn init_is_idempotent() {
        let cfg = LogSection::default();
        let _first = init_tracing(&cfg);
        assert!(!init_tracing(&cfg), "second init must be a no-op");
    }

    #[test]
    fn span_layer_exclusions() {
        for t in [
            "opentelemetry_sdk::trace",
            "opentelemetry_otlp::exporter",
            "tonic::transport::channel",
            "h2::proto::connection",
            "tower::buffer::worker",
            "tonic",
        ] {
            assert!(span_layer_excludes(t), "{t} must not reach the span layer");
        }
        for t in [
            "ostro_server::server",
            "ostro_engine::activation",
            "hyper_util::client",
            "reqwest::connect",
            "ostro::guest",
            "tower_http::trace::on_request",
            "tonic_health",
            "h2c",
        ] {
            assert!(!span_layer_excludes(t), "{t} must reach the span layer");
        }
    }

    #[test]
    fn bridge_exclusions() {
        for t in [
            "opentelemetry_sdk::trace",
            "opentelemetry",
            "opentelemetry_otlp::exporter",
            "tonic::transport",
            "h2::proto",
            "hyper_util::client",
            "tower::buffer",
            "reqwest::connect",
            "ostro::guest",
        ] {
            assert!(bridge_excludes(t), "{t} must be excluded");
        }
        for t in [
            "ostro_server::server",
            "ostro_partition",
            "ostro::api",
            "ostro",
        ] {
            assert!(!bridge_excludes(t), "{t} must be exported");
        }
    }

    #[derive(Clone, Default)]
    struct Buf(Arc<Mutex<Vec<u8>>>);

    impl Write for Buf {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("buf").extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Buf {
        type Writer = Buf;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn json_format_emits_one_object_per_line() {
        let buf = Buf::default();
        let sub = tracing_subscriber::registry().with(fmt_layer(LogFormat::Json, buf.clone()));
        tracing::subscriber::with_default(sub, || {
            tracing::info!(target: "ostro::test", answer = 42, "first line");
            tracing::warn!(target: "ostro::test", "second line");
        });
        let out = String::from_utf8(buf.0.lock().expect("buf").clone()).expect("utf8");
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2, "{out}");
        for line in &lines {
            let v: serde_json::Value = serde_json::from_str(line).expect("one JSON object");
            for key in ["timestamp", "level", "target", "fields"] {
                assert!(v.get(key).is_some(), "{key} missing in {line}");
            }
            assert_eq!(v["target"], "ostro::test");
        }
        let first: serde_json::Value = serde_json::from_str(lines[0]).expect("json");
        assert_eq!(first["level"], "INFO");
        assert_eq!(first["fields"]["message"], "first line");
        assert_eq!(first["fields"]["answer"], 42);
    }
}
