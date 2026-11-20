---
phase: 08-triggers-bottomless-observability
plan: 04
subsystem: observability
tags: [config, telemetry, opentelemetry, otlp, prometheus, tracing, services-seam, clock]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: 08-01 SnapshotConfig / CronConfig / BackupConfig contracts, NodeConfig.{snapshots,cron,backup_enabled}, ostro_engine::telemetry::{set_next_ids,take_next_ids}, every OTel/Prometheus dependency declared
provides:
  - "ServerConfig sections [telemetry], [snapshots], [backup], [blob_gc], [triggers], [guest_logs] (P8-D22 defaults, deny_unknown_fields, env:NAME secrets via SecretString, to_snapshot_config / to_cron_config / to_backup_config)"
  - "LogFormat::Json; ServerConfig::effective_log_format / effective_log_section"
  - "services seam: ServiceCtx { node, meta, blobs, config, data_dir, shutdown, clock }, Services::start / idle / shutdown / guest_log_sink / clock / running; stub spawns uploader, blob_gc, metrics_poll, run_spans; GuestLogSink stub with emit()"
  - "ServerOptions { clock, hooks } + server::start_with_options: one Arc<dyn Clock> reaching InitCluster, NodeConfig.clock and ServiceCtx.clock"
  - "telemetry: metrics_handle() (OnceLock recorder, _seconds buckets), metrics_router() (GET /metrics), Telemetry::init -> TelemetryGuard (OTLP tracer + logger providers, upkeep, force_flush, idempotent shutdown), OstroIdGenerator"
  - "logging: registry composition (late-bound OtelSlotLayer + fmt pretty/compact/json + EnvFilter), BRIDGE_EXCLUDED_TARGETS, fmt_layer(), subscriber_is_ours()"
  - "ostro-testkit::otlp::Collector (TraceServiceServer + LogsServiceServer on 127.0.0.1:0; spans(), logs(), trace_requests(), log_requests(), all_bytes(), wait_for())"
  - "docs/observability.md"
affects: [08-09, 08-10, 08-11, 08-13, 08-14, 08-16, 08-17, 08-18, 08-19, 08-20, 08-21, 08-22]

tech-stack:
  added: []
  patterns:
    - "Late-bound OTel layers: the binary installs the subscriber before the runtime exists; Telemetry::init later fills a static slot (leaked &'static per generation so downcast_raw pointers stay valid)"
    - "Services seam: later plans fill services/<name>.rs; server.rs calls Services::start once"
    - "Guard shutdown on a blocking thread (spawn_blocking) so the tonic exporter's runtime keeps making progress"

key-files:
  created:
    - crates/ostro-server/src/telemetry.rs
    - crates/ostro-server/src/services/mod.rs
    - crates/ostro-server/src/services/uploader.rs
    - crates/ostro-server/src/services/blob_gc.rs
    - crates/ostro-server/src/services/metrics_poll.rs
    - crates/ostro-server/src/services/run_spans.rs
    - crates/ostro-server/src/services/guest_logs.rs
    - crates/ostro-server/tests/metrics_endpoint.rs
    - crates/ostro-server/tests/otlp_export.rs
    - crates/ostro-testkit/src/otlp.rs
    - docs/observability.md
  modified:
    - crates/ostro-server/src/config.rs
    - crates/ostro-server/src/logging.rs
    - crates/ostro-server/src/server.rs
    - crates/ostro-server/src/lib.rs
    - crates/ostro-server/src/executor.rs
    - crates/ostro-server/tests/config.rs
    - crates/ostro-testkit/src/lib.rs

key-decisions:
  - "OTel layers are late-bound through a static slot (OtelSlotLayer), because ostro/src/main.rs installs the subscriber before the runtime and the providers must be built inside it (Pitfall 11); main.rs is unchanged"
  - "ServiceCtx.meta is Arc<MetaHandle> (MetaHandle is not Clone; the control plane already holds an Arc), ServerHandle holds Arc<Node> and unwraps it after Services::shutdown"
  - "The GuestLogSink is built before Node::open (the executor is part of NodeConfig) and passed to Services::start(ctx, sink)"
  - "[snapshots] byte threshold key is `bytes` (P8-D22), mapped to SnapshotConfig.bytes_threshold"
  - "Unknown paths, including /metrics with [telemetry] metrics = false, hit tonic's gRPC fallback (HTTP 200 + grpc-status 12), not a 404"

requirements-completed: []

duration: ~1h
completed: 2026-11-20
---

# Phase 8 Plan 04: Telemetry foundation Summary

**The six Phase 8 config sections now parse with the P8-D22 defaults. ostro-server has a services seam and one injectable clock. `GET /metrics` serves the existing `ostro_*` series, with `_bucket` lines. With `otlp_endpoint` set, node spans and logs reach an OTLP gRPC collector over the locked tonic, with no feedback loop. Node logs can be written as JSON. `OstroIdGenerator` honours the engine's id override.**

## Services seam map (file → owning plan)

| File | Service | Owner | Now |
|---|---|---|---|
| `services/mod.rs` | `ServiceCtx`, `Services::{start, idle, shutdown, guest_log_sink, clock, running}` | 08-04 | complete |
| `services/run_spans.rs` | run / activation spans from post-apply actions | 08-11 | `spawn` returns `None` |
| `services/uploader.rs` | bottomless uploader | 08-13 | `spawn` returns `None` |
| `services/metrics_poll.rs` | gauge poller | 08-16 | `spawn` returns `None` |
| `services/guest_logs.rs` | `GuestLogSink` (`new`, `config`, `emit`) and `GuestLine` | 08-17 | `emit` is a no-op |
| `services/blob_gc.rs` | local blob GC | 08-19 | `spawn` returns `None` |

Signature for the later plans to fill: `pub fn spawn(ctx: &ServiceCtx) -> Option<JoinHandle<()>>`. A task must exit when `ctx.shutdown` is cancelled, and must read time only through `ctx.clock`.

## server.rs wiring point

The order inside `start_with_options`:

1. `Telemetry::init(&cfg)` runs first. It installs the Prometheus recorder before any partition caches its metric handles, and builds the OTLP providers inside the runtime.
2. The data dir is resolved and locked, and the listener is bound.
3. `MetaHandle::start`.
4. `init_cluster(.., clock.now_ms())`. The private `now_ms()` that read `SystemClock` is removed.
5. `Runtime::new`.
6. The `GuestLogSink` is built, then `EngineExecutor::with_guest_log_sink`.
7. `NodeConfig`:
   - `.clock` is the same `Arc<dyn Clock>`;
   - `.snapshots` comes from `[snapshots]`, `.cron` from `[triggers]`;
   - `.backup_enabled = [backup] url.is_some()`;
   - `schedule_source` stays `None` (08-14 sets it).
8. `Node::open`. It returns only once every barrier is open.
9. `record_partition_leaders`.
10. **`Services::start(ServiceCtx { .. }, guest_logs)`.** This is the leader-services point: after the leader records, before the control plane and API. `blobs` comes from the first ready partition handle. If no partition started, `Services::idle` is used instead.
11. The control plane and `ostro_api::router`, plus `telemetry::metrics_router()` when `[telemetry] metrics` is on.
12. Serve.

If any step after `Telemetry::init` fails, the guard is shut down before the error is returned.

Shutdown order:
1. `/readyz` returns 503, and the API drains.
2. **`Services::shutdown`** (cancel, then join). This releases every `Arc<Node>` / `Arc<MetaHandle>` clone.
3. `Arc::try_unwrap(node)`, then `Node::shutdown`.
4. Meta stops.
5. The data-dir lock is released, and the ephemeral dir is removed.
6. **`TelemetryGuard::shutdown`**: the slot is cleared, then force-flush and provider shutdown run on `spawn_blocking`.

## OTel feature spike outcome

The spike checked opentelemetry_sdk 0.33.1 and its siblings:

- **No `rt-tokio` needed.** The batch span and log processors run on SDK-owned threads. The tonic channel (lazy connect) is created inside the server runtime, which drives its I/O. `force_flush`/`shutdown` block, so they run on `spawn_blocking`.
- **`InMemorySpanExporter`** is available under the `testing` feature, which 08-01 made a server dev-dependency (`opentelemetry_sdk::trace::InMemorySpanExporter`). It was not needed here: the IdGenerator tests read `span_context()` directly.
- **`SdkTracer`** is re-exported as `opentelemetry_sdk::trace::Tracer` (for tracing-opentelemetry 0.34). Logger: `opentelemetry_sdk::logs::{SdkLogger, SdkLoggerProvider}`.
- **gRPC metadata** goes through `opentelemetry_otlp::tonic_types::metadata::MetadataMap::from_headers(axum::http::HeaderMap)`. ostro-server has no normal tonic dependency, so no Cargo change was needed. `OTEL_EXPORTER_OTLP_HEADERS` is merged in by the exporter.
- **Id-generation order** in the SDK tracer (`tracer.rs`): `new_span_id()` runs first, and `new_trace_id()` runs only for root spans. So `OstroIdGenerator::new_span_id` consumes the override and parks its trace id for the immediately following `new_trace_id`. The next `new_span_id` clears the parked trace id. Both cases are tested: an override on a root span, and an override on a child span (the trace id is inherited and does not leak).
- **Appender scope name.** `opentelemetry-appender-tracing` sets the log record target, and the OTLP transform uses it as the instrumentation scope name. The collector's `LogView.scope` is therefore the tracing target.
- **`cargo tree -d -e normal`** shows no duplicate tonic, tonic-prost, prost, prost-types, hyper, http, axum, opentelemetry or opentelemetry_sdk. Cargo.toml and Cargo.lock are untouched.

## Default config table (P8-D22 / P8-D16)

| Section | Key | Default |
|---|---|---|
| `[telemetry]` | `log_format` | unset (uses `[log] format`) |
| | `otlp_endpoint` | unset (no OTLP) |
| | `otlp_protocol` | `grpc` (the only value) |
| | `otlp_headers` | `{}` (values may be `env:NAME`; redacted) |
| | `traces` / `logs` / `metrics` | `true` / `true` / `true` |
| | `sample_ratio` | `1.0` (0..=1 validated) |
| | `service_name` | `ostro` |
| `[snapshots]` | `enabled` | `true` |
| | `interval` / `min_interval` | `1h` / `60s` |
| | `bytes` | `268435456` |
| | `keep` | `2` (≥ 1 validated) |
| `[backup]` | `url` | unset (backup off; the dev profile is off too) |
| | `seal_interval` | `1s` (non-zero) |
| | `region` / `endpoint` | unset |
| | `url_style` | `path` (`virtual`) |
| | `access_key_id` / `secret_access_key` / `session_token` | unset (`env:NAME` resolved at load; `<redacted>` in Debug and `--print-config`) |
| | `allow_http` / `takeover` | `false` / `false` |
| | `tls_roots` | `platform` (`webpki` or a PEM path) |
| | `remote_snapshots_keep` | `3` |
| `[blob_gc]` | `enabled` / `interval` / `grace` | `true` / `1h` / `1h` |
| `[triggers]` | `webhook_body_limit` | `1048576` |
| | `webhook_tolerance` | `300s` |
| | `cron_catchup_cap` | `100` |
| `[guest_logs]` | `rate_per_sec` / `burst` | `50` / `500` |
| | `run_lines_cap` / `run_bytes_cap` | `10000` / `10485760` |
| | `ring_lines` / `ring_budget_bytes` | `1000` / `67108864` |
| | `node_rate_per_sec` | `5000` |

Durations take h/m/s/ms. A value such as `"1d"` fails with a message that names the key, quotes the value and suggests `"24h"`.

## Task Commits

1. **Task 1: config contract + services seam + clock** – `4884073`
2. **Task 2: telemetry, logging, testkit collector, tests, docs** – `672910f`

## Verification

All runs were on the worktree, rebased onto main 913187c before the Task 2 commit. `CARGO_TARGET_DIR=target/wt-08-04` throughout.

Plan checks:
- `cargo test -p ostro-server --test config --locked`: **ok, 29 passed** (14 are the new Phase 8 config tests).
- `cargo test -p ostro-server --test metrics_endpoint --test otlp_export --locked`: **ok, 2 passed + 1 passed**.
- `cargo test -p ostro-server --lib --locked`: **ok, 48 passed**, including:
  - `telemetry::tests::{telemetry_id_generator_override_applies_to_exactly_one_span, telemetry_id_override_on_child_keeps_parent_trace, telemetry_buckets_are_sorted}`;
  - `logging::tests::{init_is_idempotent, bridge_exclusions, json_format_emits_one_object_per_line}`;
  - `server::tests::injected_clock_reaches_cluster_init_and_services`.
- `cargo tree --workspace -d -e normal --locked`: none of tonic, tonic-prost, prost, prost-types, hyper, http, axum, opentelemetry or opentelemetry_sdk is duplicated.
- `bash scripts/forbidden-crates.sh`: "no chronik / tokio-uring / prometheus in cargo tree".
- `cargo deny check`: advisories ok, bans ok, licenses ok, sources ok.

Wider checks:
- `cargo test -p ostro-server -p ostro-testkit --locked`: **all ok**. Every ostro-server integration target passed: bootstrap 5, config 29, data_dir_lock 4, derive_phase5 16, executor 8, executor_phase5 12, leases 4, lifecycle_api 13, metrics_endpoint 2, modules_api 3, otlp_export 1, resume_rearm 4, runs_api 2, sdk_e2e 9, semantics_e2e 18, sink 15, start_run_api 11, tenancy_api 3. The testkit lib has 10, including `otlp::tests::collector_records_log_exports`.
- `cargo test -p ostro --locked` (the binary's spawned-server tests, which use `init_tracing`): all ok (5, 7, 1, 4, 3, 4, 4 passed).
- `cargo test -p ostro-server --features failpoints --test failpoints_phase5 -- --test-threads=1`: **ok, 5 passed**.
- `cargo build --workspace --all-targets --locked`: warning-free, both before and after the rebase.
- Clippy with `-D warnings` is clean in all three forms:
  - the ci-local form (`--workspace --all-targets`, raft/wal excluded);
  - `-p ostro-server --features failpoints --all-targets`;
  - `-p ostro-server -p ostro-testkit --all-targets` after the rebase.
- `cargo fmt --all --check`: clean.

**Not run:** the full `bash scripts/ci-local.sh --no-musl` (about 50 min). Other executors were running in parallel and the brief caps concurrent builds. The steps above cover the crates this plan touched: clippy, build, the ostro-server, testkit and ostro tests, the server failpoint suite, deny and forbidden-crates.

## Deviations from Plan

### Auto-fixed / adapted

**1. [Rule 3 - Blocking] OTel layers are late-bound (`logging::OtelSlotLayer`)**
- **Problem:** `crates/ostro/src/main.rs` (not in this plan's files; 08-18 owns it) calls `init_tracing(&cfg.log)` before the runtime exists, and a global subscriber can be installed only once. The providers must be built inside the runtime (Pitfall 11).
- **Fix:** the registry gets a first layer that forwards every callback, including `downcast_raw` for tracing-opentelemetry's `WithContext`, to OTel layers installed later by `Telemetry::init`.
  - Each generation is leaked as `&'static` (one per server start), so raw `downcast_raw` pointers stay valid.
  - A generation check stops one test server from clearing another's layers.
  - Log-target exclusion is done inside the slot's `on_event`, not with a per-layer `Targets` filter: per-layer filters cannot be swapped in later.
  - If a foreign subscriber is already installed, `Telemetry::init` logs a warning and the export is inert.

**2. [Rule 3] `ServiceCtx.meta: Arc<MetaHandle>`, `ServerHandle.node: Option<Arc<Node>>`**
- `MetaHandle` is not `Clone`, and `Node::shutdown(self)` consumes the node.
- Shutdown unwraps both after `Services::shutdown` has joined every task. A leaked clone is an `Invariant` error, not a silent hang.

**3. [Rule 3] `Services::start(ctx, guest_logs)` takes the sink as a parameter**
- The executor, and so the sink, is built before `Node::open`, while the services start after it.
- `Services::guest_log_sink()` returns the same `Arc`.

**4. [Deviation] `[telemetry] log_format` versus the binary**
- `LogFormat::Json` works through `[log] format = "json"` in the binary today.
- `[telemetry] log_format` overrides it in `ServerConfig::effective_log_section()`, which the server uses when it installs the subscriber itself.
- The binary still passes `&cfg.log`. **Follow-up for 08-18 (owner of main.rs):** call `init_tracing(&cfg.effective_log_section())`.

**5. [Deviation] `Collector::all_bytes()` returns the decoded requests' `Debug` text, not protobuf wire bytes**
- ostro-testkit has no `prost` dependency, and Cargo.toml must not change in wave 2.
- String fields appear verbatim, so 08-20/08-21 canary scans of raw, hex, base64 and percent-encoded values still work.
- If wire bytes are needed later, add `prost` to the testkit in a wave that may touch the lock.

**6. [Deviation] the "API request span" is tower-http's existing `request` span**
- ostro-api already wraps REST routes in `TraceLayer::new_for_http()`, which creates a DEBUG-level span. The test raises `tower_http=debug`.
- No new server middleware was added. gRPC routes have no request span yet, and none was needed for this plan.

**7. [Rule 1] Task 1's config.rs was committed before `cargo fmt`**
- Task 2's commit carries the rustfmt-only reflow.

**8. [Deviation] the clock test is a lib unit test**
- It lives in `server.rs` (`server::tests::injected_clock_reaches_cluster_init_and_services`), not in an integration test file.
- It uses `ostro_partition::ManualClock`, the existing shared clock. No second trait was added.

## Self-Check: PASSED

- Every file listed under key-files exists. That covers telemetry.rs, the 6 `services/*.rs` files, the two new test files, `ostro-testkit/src/otlp.rs` and `docs/observability.md`.
- Commits 4884073 and 672910f are on main.
