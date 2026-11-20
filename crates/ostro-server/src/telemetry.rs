//! Telemetry foundation (plan 08-04; P8-D13/D14/D15): Prometheus `/metrics`, OTLP traces
//! and logs over the locked tonic, the deterministic-id [`OstroIdGenerator`] and the
//! provider lifecycle ([`TelemetryGuard`]).
//!
//! # Metrics
//!
//! [`metrics_handle`] installs the `metrics-exporter-prometheus` recorder once per process
//! (explicit [`SECONDS_BUCKETS`] for every `*_seconds` histogram, so they render as
//! `_bucket` lines rather than summaries). Every crate already records through the
//! `metrics` facade, so the existing `ostro_*` series appear without further wiring.
//! [`metrics_router`] serves `GET /metrics`; each [`TelemetryGuard`] runs the recorder's
//! upkeep every [`UPKEEP_INTERVAL`]. Labels are bounded: never `run_id`.
//!
//! # Traces and logs
//!
//! With `[telemetry] otlp_endpoint` set, [`Telemetry::init`] builds an
//! `SdkTracerProvider` (batch exporter, [`OstroIdGenerator`],
//! `ParentBased(TraceIdRatioBased(sample_ratio))`) and an `SdkLoggerProvider` (batch
//! exporter), both over `opentelemetry-otlp`'s tonic exporter, with the resource
//! attributes `service.name`, `service.version` and `service.instance.id`. It installs
//! them into the subscriber's late-bound slot ([`crate::logging::OtelSlotLayer`]), sets
//! the global propagator to `TraceContextPropagator` and the global tracer provider.
//! The batch processors run on SDK-owned threads (no `rt-tokio`); the tonic channel is
//! created inside the server's runtime, which drives its I/O.
//!
//! # Shutdown
//!
//! [`TelemetryGuard::shutdown`] (idempotent) runs after the services and the node have
//! stopped: the slot is cleared, then both providers are flushed and shut down on a
//! blocking thread (the exporter needs the runtime's workers to make progress).

use std::cell::Cell;
use std::sync::OnceLock;
use std::time::Duration;

use axum::Router;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::routing::get;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use opentelemetry::KeyValue;
use opentelemetry::trace::{SpanId, TraceId, TracerProvider as _};
use opentelemetry_otlp::tonic_types::metadata::MetadataMap;
use opentelemetry_otlp::{WithExportConfig as _, WithTonicConfig as _};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{IdGenerator, RandomIdGenerator, Sampler, SdkTracerProvider};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::config::{ServerConfig, TelemetrySection};
use crate::logging;

/// Explicit buckets for every `*_seconds` histogram (P8-D15).
pub const SECONDS_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0];
/// How often the recorder's upkeep (histogram/summary maintenance) runs.
pub const UPKEEP_INTERVAL: Duration = Duration::from_secs(5);
/// The Prometheus text exposition content type.
pub const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4";
/// OTLP export timeout per batch.
pub const OTLP_TIMEOUT: Duration = Duration::from_secs(5);
/// Instrumentation scope of node-internal spans.
pub const TRACER_NAME: &str = "ostro";

static RECORDER: OnceLock<Option<PrometheusHandle>> = OnceLock::new();

/// The process-wide Prometheus handle, installing the recorder on first use. `None` when
/// another global `metrics` recorder was installed first (logged once).
pub fn metrics_handle() -> Option<PrometheusHandle> {
    RECORDER
        .get_or_init(|| {
            let builder = match PrometheusBuilder::new()
                .set_buckets_for_metric(Matcher::Suffix("_seconds".into()), SECONDS_BUCKETS)
            {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = %e, "prometheus buckets rejected; /metrics disabled");
                    return None;
                }
            };
            match builder.install_recorder() {
                Ok(h) => Some(h),
                Err(e) => {
                    warn!(error = %e, "cannot install the prometheus recorder; /metrics disabled");
                    None
                }
            }
        })
        .clone()
}

/// `GET /metrics`: the Prometheus text exposition (503 when the recorder is unavailable).
pub fn metrics_router() -> Router {
    Router::new().route(
        "/metrics",
        get(|| async {
            match metrics_handle() {
                Some(h) => (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
                    h.render(),
                ),
                None => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    [(header::CONTENT_TYPE, "text/plain")],
                    "metrics recorder unavailable\n".to_owned(),
                ),
            }
        }),
    )
}

thread_local! {
    /// The trace id half of an override consumed by `new_span_id` (the SDK asks for the
    /// span id first and the trace id afterwards, only for root spans).
    static PENDING_TRACE: Cell<Option<[u8; 16]>> = const { Cell::new(None) };
}

/// Deterministic span ids (P8-D14): uses `ostro_engine::telemetry::take_next_ids()` when
/// the code starting a span set an override on this thread, otherwise random ids.
///
/// The SDK's tracer calls `new_span_id` first and then, for a root span only,
/// `new_trace_id`. `new_span_id` therefore consumes the override and parks its trace id
/// for the immediately following `new_trace_id`; a child span (trace id inherited from the
/// parent) clears it again on the next `new_span_id`.
#[derive(Clone, Debug, Default)]
pub struct OstroIdGenerator {
    random: RandomIdGenerator,
}

impl IdGenerator for OstroIdGenerator {
    fn new_trace_id(&self) -> TraceId {
        match PENDING_TRACE.with(Cell::take) {
            Some(t) => TraceId::from_bytes(t),
            None => self.random.new_trace_id(),
        }
    }

    fn new_span_id(&self) -> SpanId {
        match ostro_engine::telemetry::take_next_ids() {
            Some((trace, span)) => {
                PENDING_TRACE.with(|c| c.set(Some(trace)));
                SpanId::from_bytes(span)
            }
            None => {
                PENDING_TRACE.with(|c| c.set(None));
                self.random.new_span_id()
            }
        }
    }
}

/// Why telemetry could not start.
#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("telemetry.otlp_headers: {0}")]
    Header(String),
    #[error("OTLP exporter: {0}")]
    Exporter(String),
}

/// Entry point ([`Telemetry::init`]).
#[derive(Debug, Clone, Copy)]
pub struct Telemetry;

impl Telemetry {
    /// Start the Prometheus upkeep (when `[telemetry] metrics`) and, with an OTLP endpoint,
    /// the trace and log pipelines. Must run inside the server's Tokio runtime.
    pub fn init(cfg: &ServerConfig) -> Result<TelemetryGuard, TelemetryError> {
        let t = &cfg.telemetry;
        let upkeep = if t.metrics {
            metrics_handle().map(|h| {
                tokio::spawn(async move {
                    // clock-exempt: recorder maintenance cadence, no logic depends on it.
                    let mut tick = tokio::time::interval(UPKEEP_INTERVAL);
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        tick.tick().await;
                        h.run_upkeep();
                    }
                })
            })
        } else {
            None
        };
        let mut guard = TelemetryGuard {
            tracer: None,
            logger: None,
            generation: None,
            upkeep,
        };
        let Some(endpoint) = t.otlp_endpoint.as_deref() else {
            return Ok(guard);
        };
        if !(t.traces || t.logs) {
            return Ok(guard);
        }
        let metadata = metadata(t)?;
        let resource = resource(t, cfg.node.node_id);
        if t.traces {
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .with_timeout(OTLP_TIMEOUT)
                .with_metadata(metadata.clone())
                .build()
                .map_err(|e| TelemetryError::Exporter(e.to_string()))?;
            guard.tracer = Some(
                SdkTracerProvider::builder()
                    .with_id_generator(OstroIdGenerator::default())
                    .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                        t.sample_ratio,
                    ))))
                    .with_batch_exporter(exporter)
                    .with_resource(resource.clone())
                    .build(),
            );
        }
        if t.logs {
            let exporter = opentelemetry_otlp::LogExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .with_timeout(OTLP_TIMEOUT)
                .with_metadata(metadata)
                .build()
                .map_err(|e| TelemetryError::Exporter(e.to_string()))?;
            guard.logger = Some(
                SdkLoggerProvider::builder()
                    .with_batch_exporter(exporter)
                    .with_resource(resource)
                    .build(),
            );
        }
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        if let Some(p) = &guard.tracer {
            opentelemetry::global::set_tracer_provider(p.clone());
        }
        // The subscriber is normally installed by the binary already (a no-op then).
        logging::init_tracing(&cfg.effective_log_section());
        if !logging::subscriber_is_ours() {
            warn!(
                "a foreign tracing subscriber is installed; node spans and logs are not \
                 exported over OTLP"
            );
        }
        guard.generation = Some(logging::install_otel_layers(
            guard.tracer.as_ref().map(|p| p.tracer(TRACER_NAME)),
            guard.logger.as_ref(),
        ));
        info!(
            endpoint,
            traces = guard.tracer.is_some(),
            logs = guard.logger.is_some(),
            "OTLP export enabled"
        );
        Ok(guard)
    }
}

fn metadata(t: &TelemetrySection) -> Result<MetadataMap, TelemetryError> {
    let mut headers = HeaderMap::new();
    for (name, value) in &t.otlp_headers {
        let n = HeaderName::try_from(name.as_str())
            .map_err(|e| TelemetryError::Header(format!("{name}: {e}")))?;
        let mut v = HeaderValue::try_from(value.expose())
            .map_err(|e| TelemetryError::Header(format!("{name}: {e}")))?;
        v.set_sensitive(true);
        headers.insert(n, v);
    }
    Ok(MetadataMap::from_headers(headers))
}

fn resource(t: &TelemetrySection, node_id: u64) -> Resource {
    Resource::builder_empty()
        .with_service_name(t.service_name.clone())
        .with_attributes([
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("service.instance.id", node_id.to_string()),
        ])
        .build()
}

/// Owns the OTel providers and the upkeep task of one server.
pub struct TelemetryGuard {
    tracer: Option<SdkTracerProvider>,
    logger: Option<SdkLoggerProvider>,
    generation: Option<u64>,
    upkeep: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for TelemetryGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelemetryGuard")
            .field("traces", &self.tracer.is_some())
            .field("logs", &self.logger.is_some())
            .field("upkeep", &self.upkeep.is_some())
            .finish()
    }
}

impl TelemetryGuard {
    /// A guard that owns nothing (telemetry off).
    pub fn disabled() -> Self {
        Self {
            tracer: None,
            logger: None,
            generation: None,
            upkeep: None,
        }
    }

    /// The tracer provider (OTLP traces on).
    pub fn tracer_provider(&self) -> Option<&SdkTracerProvider> {
        self.tracer.as_ref()
    }

    /// The logger provider (OTLP logs on); the guest log sink (08-17) emits through it.
    pub fn logger_provider(&self) -> Option<&SdkLoggerProvider> {
        self.logger.as_ref()
    }

    /// Export everything buffered so far (tests; also part of shutdown).
    pub async fn force_flush(&self) {
        let tracer = self.tracer.clone();
        let logger = self.logger.clone();
        let res = tokio::task::spawn_blocking(move || {
            if let Some(p) = &tracer
                && let Err(e) = p.force_flush()
            {
                warn!(error = %e, "flushing OTLP traces");
            }
            if let Some(p) = &logger
                && let Err(e) = p.force_flush()
            {
                warn!(error = %e, "flushing OTLP logs");
            }
        })
        .await;
        if let Err(e) = res {
            warn!(error = %e, "OTLP flush task failed");
        }
    }

    /// Stop the upkeep task, detach the layers, then flush and shut down both providers.
    /// Idempotent.
    pub async fn shutdown(&mut self) {
        if let Some(task) = self.upkeep.take() {
            task.abort();
        }
        if let Some(generation) = self.generation.take() {
            logging::clear_otel_layers(generation);
        }
        let tracer = self.tracer.take();
        let logger = self.logger.take();
        if tracer.is_none() && logger.is_none() {
            return;
        }
        let res = tokio::task::spawn_blocking(move || {
            if let Some(p) = tracer {
                let _ = p.force_flush();
                if let Err(e) = p.shutdown() {
                    warn!(error = %e, "OTLP tracer provider shutdown");
                }
            }
            if let Some(p) = logger {
                let _ = p.force_flush();
                if let Err(e) = p.shutdown() {
                    warn!(error = %e, "OTLP logger provider shutdown");
                }
            }
        })
        .await;
        if let Err(e) = res {
            warn!(error = %e, "OTLP shutdown task failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::trace::{Span as _, TraceContextExt as _, Tracer as _};

    use super::*;

    #[test]
    fn telemetry_id_generator_override_applies_to_exactly_one_span() {
        let provider = SdkTracerProvider::builder()
            .with_id_generator(OstroIdGenerator::default())
            .build();
        let tracer = provider.tracer("test");
        let trace = [7u8; 16];
        let span = [9u8; 8];

        ostro_engine::telemetry::set_next_ids(trace, span);
        let mut first = tracer.start("first");
        let ctx = first.span_context().clone();
        assert_eq!(ctx.trace_id(), TraceId::from_bytes(trace));
        assert_eq!(ctx.span_id(), SpanId::from_bytes(span));
        first.end();

        let mut second = tracer.start("second");
        let ctx2 = second.span_context().clone();
        assert_ne!(ctx2.trace_id(), TraceId::from_bytes(trace), "random after");
        assert_ne!(ctx2.span_id(), SpanId::from_bytes(span));
        assert!(ctx2.is_valid());
        second.end();
    }

    #[test]
    fn telemetry_id_override_on_child_keeps_parent_trace() {
        let provider = SdkTracerProvider::builder()
            .with_id_generator(OstroIdGenerator::default())
            .build();
        let tracer = provider.tracer("test");
        let parent = tracer.start("parent");
        let parent_trace = parent.span_context().trace_id();
        let cx = opentelemetry::Context::current_with_span(parent);
        ostro_engine::telemetry::set_next_ids([1; 16], [2; 8]);
        let child = tracer.start_with_context("child", &cx);
        assert_eq!(child.span_context().span_id(), SpanId::from_bytes([2; 8]));
        assert_eq!(child.span_context().trace_id(), parent_trace, "inherited");
        // The parked trace id of the child's override does not leak into the next root.
        let root = tracer.start("root");
        assert_ne!(root.span_context().trace_id(), TraceId::from_bytes([1; 16]));
    }

    #[test]
    fn telemetry_buckets_are_sorted() {
        assert!(SECONDS_BUCKETS.windows(2).all(|w| w[0] < w[1]));
    }
}
