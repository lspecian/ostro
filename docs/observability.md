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
  tower-http's `request` span at `debug` level for REST requests). Run, activation, step and
  effect spans follow in 08-11.
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

Run, activation, step and effect spans (08-11) use this, so a replayed run never produces
different ids for the same span.

The global propagator is the W3C `TraceContextPropagator`.

## Span model

*Filled by 08-11:*
- `workflow.run` → `activation` → `step` / `effect:http` hierarchy;
- live-only emission;
- trace id taken from the run's `traceparent`;
- deterministic span ids.

## Guest logs

*Filled by 08-17:*
- rate limits per run and per node, and lifetime caps (`[guest_logs]`);
- the per-run ring buffer at `GET /v1/runs/{id}/logs`;
- the OTLP LogRecord attributes (`run.id`, `workflow`, `attempt`, trace/span ids).
