# Observability

Ostro exposes three signals:

- **Metrics:** a Prometheus scrape endpoint, `GET /metrics`.
- **Traces:** OTLP over gRPC.
- **Logs:** node logs on stdout (pretty, compact or JSON), optionally also exported as
  OTLP LogRecords.

The pipes are set up in plan 08-04 (`crates/ostro-server/src/telemetry.rs` and
`logging.rs`). The span model and guest logs are filled in by 08-11 and 08-17.

## Configuration keys

`[telemetry]` in `ostro.toml`. Each key can also be set with `OSTRO_TELEMETRY__<KEY>`.

| Key | Default | Meaning |
|---|---|---|
| `log_format` | unset | Overrides `[log] format` for the node log: `pretty`, `compact` or `json`. |
| `otlp_endpoint` | unset | OTLP collector URL, for example `http://otel-collector:4317`. When unset, nothing is exported over OTLP. |
| `otlp_protocol` | `grpc` | Only `grpc` is supported in Phase 8. It runs over the workspace's tonic; no HTTP exporter is built. |
| `otlp_headers` | `{}` | gRPC metadata sent with every export, for example `{ authorization = "env:OTLP_TOKEN" }`. A value may be an `env:NAME` reference, which is resolved at load. Values are never printed. |
| `traces` | `true` | Export traces when `otlp_endpoint` is set. |
| `logs` | `true` | Export node logs as OTLP LogRecords when `otlp_endpoint` is set. |
| `sample_ratio` | `1.0` | Sampling ratio for root spans: `ParentBased(TraceIdRatioBased(ratio))`. A sampled parent's decision (for example from an inbound `traceparent`) is always honoured. Must be within `0.0..=1.0`. |
| `service_name` | `ostro` | The `service.name` resource attribute. |
| `metrics` | `true` | Serve `GET /metrics`. |

Resource attributes:
- `service.name` (from the config);
- `service.version` (the binary version);
- `service.instance.id` (`[node] node_id`).

The other Phase 8 sections (`[snapshots]`, `[backup]`, `[blob_gc]`, `[triggers]`,
`[guest_logs]`) are documented with their features. Their defaults are in
`crates/ostro-server/src/config.rs`.

Durations in every section are written in `h`/`m`/`s`/`ms`. Days are rejected, so write
`24h`, not `1d`.

## Scraping `/metrics`

`GET /metrics` is served on the API port and returns `200` with
`Content-Type: text/plain; version=0.0.4`:

```yaml
scrape_configs:
  - job_name: ostro
    static_configs:
      - targets: ["ostro:7700"]
```

How it works:
- The exporter is `metrics-exporter-prometheus`. Its recorder is installed once per process,
  before the partitions open.
- Every crate records through the `metrics` facade, so all existing `ostro_*` series appear.
  Examples: `ostro_wal_*`, `ostro_apply_*`, `ostro_activation_*`, `ostro_timer*`,
  `ostro_shuffler_*`, `ostro_outbox_rows`, `ostro_aot_cache_tamper_total`.
- Every `*_seconds` histogram has explicit buckets (`0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1,
  5, 30` seconds), so histograms render as `_bucket{...,le="..."}` lines.
- Recorder upkeep runs every 5 s.
- Labels are bounded: partition, queue, workflow, outcome. A run id is **never** a label.

When `[telemetry] metrics = false`, `/metrics` is not routed.

## OTLP endpoint and `OTEL_EXPORTER_OTLP_*`

Set `[telemetry] otlp_endpoint`, or `OSTRO_TELEMETRY__OTLP_ENDPOINT`. The node then exports
over OTLP/gRPC:
- **Traces:** node-internal `tracing` spans, through `tracing-opentelemetry` (for example,
  tower-http's `request` span at `debug` level for REST requests), plus the run, activation,
  step and effect spans of the span model below (08-11, 08-21).
- **Logs:** node `tracing` events, through `opentelemetry-appender-tracing`. The event target
  becomes the instrumentation scope name.

Exports are batched on SDK-owned threads with a 5 s timeout. At shutdown, buffered spans and
logs are flushed after the node has stopped.

`opentelemetry-otlp` also reads the standard `OTEL_EXPORTER_OTLP_*` environment variables.
The useful one is `OTEL_EXPORTER_OTLP_HEADERS` (or `OTEL_EXPORTER_OTLP_TRACES_HEADERS` /
`OTEL_EXPORTER_OTLP_LOGS_HEADERS`), whose headers are merged with `otlp_headers`.

Compression is not compiled in: the `zstd` and `gzip` features are off. Do not set
`OTEL_EXPORTER_OTLP_COMPRESSION`, or the server fails at startup with an OTLP exporter
error.

Ostro sets the endpoint and timeout itself, so `otlp_endpoint` is required to turn export on.

**No feedback loop.** These targets are never bridged into LogRecords, because they would log
about their own exports:
- the OpenTelemetry crates: `opentelemetry*`;
- their transports: `tonic`, `h2`, `hyper`, `tower`, `reqwest`.

`ostro::guest` events are excluded too: the guest log sink exports each guest line as its own
LogRecord (08-17), so bridging would export it twice.

The span layer has the same guard (08-22): spans and events of the crates
`opentelemetry`, `opentelemetry_sdk`, `opentelemetry_otlp`, `tonic`, `h2` and `tower` never
reach `tracing-opentelemetry` (`logging::SPAN_EXCLUDED_TARGETS`, matched at the crate
boundary, so tower-http's `request` span is still exported). At `trace` level the exporter's own gRPC spans would
otherwise be exported by the exporter they describe (08-21 measured 120k spans in 40 s and
dropped batch records). The outbound HTTP path of the effects bridge (`hyper`, `reqwest`)
stays visible to the span layer.

The log level is the node's `[log] level` / `RUST_LOG` filter, which applies to stdout and
OTLP alike.

## JSON logs

```toml
[log]
format = "json"   # or [telemetry] log_format = "json"
```

The output is one JSON object per line, for example:

```json
{"timestamp":"2026-11-20T09:00:00.000000Z","level":"INFO","fields":{"message":"ostro server started","addr":"0.0.0.0:7700"},"target":"ostro_server::server"}
```

The keys are `timestamp`, `level`, `target`, `fields` (with `message`), and `span`/`spans` when
inside a span. `[log] format = "json"` takes effect in the binary today.
`[telemetry] log_format` applies wherever the subscriber is built from the full config: the
server's own fallback initialisation, and the binary once its startup reads it (08-18 owns
`main.rs`).

## Deterministic span ids

The tracer provider uses `OstroIdGenerator`:
- When the code that starts a span has called
  `ostro_engine::telemetry::set_next_ids(trace, span)` on the same thread, that span gets
  exactly those ids.
- Otherwise ids are random.

Run, activation, step and effect spans (08-11, see the span model below) use this, so a replayed run never produces
different ids for the same span.

The global propagator is the W3C `TraceContextPropagator`.

## Span model

Every run produces one trace (OBS-01, P8-D14). The engine and run spans use the
instrumentation scope `ostro-engine`; node-internal `tracing` spans (API requests, the
`activation` tracing span that carries log context) are separate and have random ids.

| Span | Emitted by | Parent | Span id (`kind`, `n`) | Start / end | Attributes |
|---|---|---|---|---|---|
| `workflow.run` | ostro-server `services/run_spans.rs`, on the post-apply `Action::RunCompleted` | the caller's span from the run's `traceparent`; none (root) otherwise | `run`, 0 | `created_at_ms` / `completed_at_ms` of the run record | `ostro.run_id`, `ostro.workflow`, `ostro.status`; status Ok for `completed` / `continued_as_new`, Error otherwise |
| `activation` | the engine, once per activation | `workflow.run` | `activation`, `attempt << 32 \| counter` (counter = committed journal length at activation start) | activation start / end (wall clock) | `ostro.run_id`, `ostro.workflow`, `ostro.attempt`, `ostro.replayed_events`, `ostro.outcome`; status Error on a failed attempt (trap, fuel, nondeterminism, ...) |
| `<step name>` | the engine, after the append carrying the step's `StepCompleted` / `StepAttemptFailed` committed | the activation that ran the step live | `step`, journal seq | attempt start / the journaled completion ts | `ostro.run_id`, `ostro.seq`, `ostro.attempt`, `ostro.outcome` (`ok`, `retry`, `cancelled`, `timeout`, `error`) |
| `effect:<kind>` (`http`, `sql`, `object`) | ostro-server `effect_trace.rs` (`TraceEffectHooks`, installed on `ServerEffectDriver` when OTLP traces are on), once per completed effect | `workflow.run` | `effect`, op id | effect I/O start / end | `ostro.run_id`, `ostro.op`, `ostro.effect_kind`, `ostro.outcome` (`ok` or the bounded failure kind: `ErrorInfo.kind`, `timeout`, `cancelled`, `undecodable`) |

### Id derivation

- Trace id: the trace id of the run's stored W3C `traceparent` (the `traceparent` header
  or body field at start), else `blake3("ostro/trace" ‖ run_id)[..16]`.
- Span id: `blake3("ostro/span" ‖ run_id ‖ kind ‖ n as u64 little-endian)[..8]`.
- `run_id` is the run id's string form. Pinned vectors (`run-1`): trace
  `86ca30530a6d71a97854d22c0272e94b`; `run`/0 `d3ede864a16f6334`; `activation`/(1, 1)
  `9bc352849fc38bc5`; `step`/3 `a6b04adb4033b468`; `effect`/7 `a4b3d0fc243cbad8`.

### Live-only rule

- Replayed steps never emit spans: after an eviction and re-activation there is exactly one
  span per `(run, seq)`. A step whose append failed (fenced, conflict) emits nothing; its
  re-execution later emits the span once, with the same id.
- `workflow.run` comes from post-apply actions, which are off during startup catch-up, so a
  restart never emits a second run span.
- Telemetry never feeds guest-visible state or the journal: step start times use the wall
  clock only for the span, ends use the journaled `ts_nanos`, and the journal bytes are
  identical with and without a tracer provider (tested).
- Without an OTLP endpoint the global tracer is the no-op tracer and the engine skips all
  step bookkeeping.

### Late parents

Activation spans reference the `workflow.run` span id long before the run span is
exported (it is emitted when the run completes, possibly by another node incarnation).
OTLP backends accept a late parent; until the run completes, a trace view shows the
activations under a missing parent.

### Hooks for Phase 6/7 (ostro-engine `telemetry`)

```rust
pub fn effect_span_context(run_id: &str, traceparent: Option<&str>, op: u64) -> SpanContext;
pub fn effect_span(run: &RunTrace, op: u64, kind: &str, start: SystemTime, end: SystemTime,
                   outcome: Result<(), &str>);
pub fn traceparent_headers(cx: &SpanContext) -> Vec<(String, String)>;
impl HostState { pub fn activation_span_context(&self) -> Option<SpanContext>; }
pub async fn with_traceparent<F: Future>(traceparent: Option<String>, f: F) -> F::Output;
```

`traceparent_headers` injects through the global propagator (`TraceContextPropagator`) and
formats the W3C header itself when no propagator is installed, so outbound HTTP always
carries `traceparent`. The executor wraps every activation in `with_traceparent(record
.traceparent)`.

### Outbound `traceparent` (B2, plan 08-21)

With OTLP traces on, `TraceEffectHooks` (`crates/ostro-server/src/effect_trace.rs`)
decorates the **live** copy of every outbound HTTP effect request with
`traceparent: 00-<run trace id>-<span_id(run, "effect", op)>-01`, after the intent was
journaled, so the journal never contains it (journal bytes are the same with and without
telemetry). A guest-supplied `traceparent` header (any case) wins and is sent unchanged; the
effect span is still emitted. A crash re-drive sends the identical header. With traces off
the driver keeps the no-op hooks and the outbound request equals the journaled intent.
Evidence: `crates/ostro-server/tests/outbound_traceparent.rs`.

## Guest logs

Workflows log through `ostro:runtime/log` (Rust SDK: `ctx.log_with(level, message,
&[(key, value), ..])`). Decision P8-D16, plan 08-17 (OBS-03).

**Never journaled, never replayed.** A guest line is dropped while the activation is
replaying, so a run that is evicted, re-activated or resumed after a restart emits each
line once, when its code first runs live. Logs are not part of the run's durable state.

### Pipeline

1. **Engine** (`ostro-engine/src/host/log.rs`): replay check, then the per-activation
   bound of **1,000 lines / 1 MiB** (message + keys + values). Lines over it are dropped
   with reason `activation_bound`. A kept line goes to the activation sink with the run
   id, workflow, attempt, fields, timestamp and the activation span's trace/span ids.
2. **Node sink** (`ostro-server/src/services/guest_logs.rs`, `GuestLogSink`), in this
   order:
   1. node token bucket, `node_rate_per_sec` lines/s (burst: one second's worth). Drop
      reason `node_rate`;
   2. per-run token bucket, `rate_per_sec` lines/s with `burst`. Drop reason `run_rate`;
   3. per-run lifetime cap, `run_lines_cap` lines or `run_bytes_cap` bytes. Drop reason
      `run_cap`. The node logs one `warn` per run when the cap is first hit.

   A line is charged against the buckets only when it passes all three checks, so a
   capped run does not drain the node bucket.
3. An accepted line goes to three places:
   - the run's ring buffer;
   - the node log;
   - one OTLP LogRecord.

### `[guest_logs]`

| Key | Default | Meaning |
|---|---|---|
| `rate_per_sec` | `50` | Per-run refill (lines/s). |
| `burst` | `500` | Per-run bucket size. |
| `run_lines_cap` | `10000` | Lines accepted per run over its lifetime. |
| `run_bytes_cap` | `10485760` (10 MiB) | Bytes accepted per run over its lifetime. |
| `ring_lines` | `1000` | Lines kept per run for `GET /v1/runs/{id}/logs`. |
| `ring_budget_bytes` | `67108864` (64 MiB) | Node-wide budget for all ring buffers. |
| `node_rate_per_sec` | `5000` | Node-wide bucket (lines/s). |

**Caps reset on restart.** The limiter state and the lifetime counters live in memory
only (P8-D16, best effort). After a restart a run starts with a fresh bucket and a fresh
cap. A run's state is also forgotten after 1 h without a line once its ring is empty, and
the node tracks at most 100,000 runs (least recently seen go first).

### Metrics

| Series | Labels |
|---|---|
| `ostro_guest_log_lines_total` | `level` (`trace` .. `error`) |
| `ostro_guest_log_dropped_total` | `reason`: `activation_bound`, `node_rate`, `run_rate`, `run_cap` |

The labels never carry a run id (P8-D15). Per-run drop counts are in the endpoint's
`dropped` field.

### `GET /v1/runs/{id}/logs`

Query `?limit=N` (default 100, max 1000) and `&level=` (minimum level). gRPC:
`ostro.api.v1.RunLogs/GetRunLogs`.

```json
{"run_id":"…","lines":[{"ts_ms":1763629200000,"level":"info","message":"charged",
  "fields":{"order":"42"},"attempt":1}],"dropped":0}
```

- Lines are returned oldest first: the last `N` accepted lines at or above `level`.
- The ring keeps the last `ring_lines` lines per run. All rings share `ring_budget_bytes`
  (line bytes + 64 bytes of overhead per line). Over budget, the **least recently written
  run loses its whole ring**. A single run larger than the budget loses its oldest lines
  instead.
- The ring is non-durable and best effort. An unknown run, a forgotten run, a run whose
  ring was evicted, or any run after a restart answers **200 with `lines: []`**, never
  404.
- `dropped` counts this run's lines lost to `node_rate`, `run_rate`, `run_cap` and ring
  eviction since the node started. `activation_bound` drops happen in the engine and are
  only in the metric. A non-zero `dropped` with fewer lines than asked for marks a
  truncated or evicted ring.

### Node log line

The node log line uses target `ostro::guest` at the guest's level, with the fields
`run_id`, `workflow`, `attempt` and `guest.fields`. `tracing` field names are static, so
the guest's dynamic keys travel as **one JSON object string** under `guest.fields`.
Under `format = "json"` a line looks like this:

```json
{"timestamp":"…","level":"WARN","fields":{"message":"payment failed","run_id":"…","workflow":"billing","attempt":1,"guest.fields":"{\"order\":\"42\",\"reason\":\"card declined\"}"},"target":"ostro::guest"}
```

To get real keys, parse `fields["guest.fields"]` a second time. tracing-subscriber's
JSON formatter writes string fields as strings. Flattening them needs `valuable`, which
requires the unstable `tracing_unstable` cfg, so it is not used. The OTLP LogRecord has
the fields as real attributes.

### OTLP LogRecord

When `[telemetry] otlp_endpoint` is set and `logs = true`, each accepted line becomes
exactly one LogRecord. The OTLP log bridge excludes `ostro::guest`, so the node log line
is not exported a second time.

| Part | Value |
|---|---|
| Instrumentation scope | `ostro.guest` |
| Body | the message |
| Severity | `TRACE` / `DEBUG` / `INFO` / `WARN` / `ERROR` (number and text) |
| Timestamp | the activation's live clock at emission; observed timestamp = node clock |
| Attributes | every guest field (string), plus `run.id`, `workflow`, `attempt` (int) |
| Trace context | trace id = the run's trace (its `traceparent`, else the id derived from the run id); span id = the emitting `activation` span (08-11); flags sampled |

The node's `[log] level` / `RUST_LOG` filter applies only to the node log line. OTLP
export and the ring buffer see every accepted line, whatever its level.
