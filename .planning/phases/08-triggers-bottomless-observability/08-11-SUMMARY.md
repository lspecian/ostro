---
phase: 08-triggers-bottomless-observability
plan: 11
subsystem: observability
tags: [traces, opentelemetry, obs-01, deterministic-ids, live-only, run-spans]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: "08-01 set_next_ids/take_next_ids contract; 08-04 OstroIdGenerator, global TraceContextPropagator, services seam (run_spans.rs), testkit OTLP collector"
  - phase: 06-effects-bridge-connectors-secrets
    provides: "06-06 HostState::flush append site (step spans are emitted after the same Ok)"
provides:
  - "ostro_engine::telemetry: trace_id_for, span_id, activation_n, parse_traceparent, RunTrace, ActivationSpan, ActivationTrace (collect_steps / emit_steps), emit_run_span, effect_span, effect_span_context, traceparent_headers, format_traceparent, with_traceparent / current_traceparent, HostState::activation_span_context"
  - "ostro-server services::run_spans: workflow.run span per Action::RunCompleted (post-apply, never during catch-up)"
  - "EngineExecutor runs every activation inside with_traceparent(record.traceparent)"
  - "docs/observability.md span model"
affects: [08-17, 08-21, 08-22, Phase 7 workers]

tech-stack:
  added: []
  patterns:
    - "Deterministic span ids: set_next_ids right before build_with_context, leftover cleared right after (start_span)"
    - "Step spans collected from the batch before append, emitted only after the append returns Ok"
    - "Run seed via tokio task_local (with_traceparent) instead of a new Activate field"

key-files:
  created:
    - crates/ostro-engine/tests/telemetry_spans.rs
    - crates/ostro-server/tests/tracing_spans.rs
  modified:
    - crates/ostro-engine/src/telemetry.rs
    - crates/ostro-engine/src/activation.rs
    - crates/ostro-engine/src/host/journal.rs
    - crates/ostro-engine/src/host/state.rs
    - crates/ostro-engine/Cargo.toml
    - Cargo.lock
    - crates/ostro-server/src/services/run_spans.rs
    - crates/ostro-server/src/executor.rs
    - docs/observability.md

key-decisions:
  - "Activation counter = committed journal length at activation start (deterministic, survives restarts, distinct per progressing activation); no executor-side counter"
  - "Step spans are emitted after the append that carries them commits (HostState::flush), so a fenced/conflicted append emits nothing and the re-execution emits the span once with the same id"
  - "The run's traceparent reaches the engine through a tokio task_local (telemetry::with_traceparent) wrapped around Runtime::activate; Activate (24 construction sites) is unchanged"
  - "effect spans are parented to workflow.run (the effect driver runs outside any activation)"
  - "ostro-engine gains blake3 (already in Cargo.lock via ostro-partition, pure feature); the lock diff is one dependency-list line"
  - "Engine test uses an API-only recording TracerProvider that consumes take_next_ids like OstroIdGenerator, so ostro-engine needs no opentelemetry_sdk dev-dependency"

requirements-completed: []

duration: ~1h15m
completed: 2026-11-20
---

# Phase 8 Plan 11: Traces Summary

**A run now produces `workflow.run` -> `activation` -> `step` spans with blake3-derived span ids. They sit in the caller's `traceparent` trace, or in a trace id derived from the run id. Step spans are emitted only for live steps, and only after their append commits, so an evicted and re-activated run has exactly one span per `(run, seq)`. `workflow.run` comes from the post-apply `Action::RunCompleted`, so a restart never re-emits it. `effect_span`, `effect_span_context` and `traceparent_headers` are ready for 08-21's `ServerEffectDriver` hooks. ostro-engine still depends only on the OpenTelemetry API crate.**

## Span table

| Span | Emitter | Parent | id kind / n | Start / end |
|---|---|---|---|---|
| `workflow.run` | `services/run_spans.rs` on `Action::RunCompleted` | caller span (traceparent) or root | `run` / 0 | `created_at_ms` / `completed_at_ms` |
| `activation` | engine `run_activation` | `workflow.run` | `activation` / `attempt << 32 \| committed journal len` | activation wall clock |
| `<step name>` | engine `HostState::flush` after the append is Ok | the live activation | `step` / journal seq | attempt start (wall clock) / journaled `ts_nanos` |
| `effect:<kind>` | effect driver (08-21) via `effect_span` | `workflow.run` | `effect` / op id | effect I/O |

All of them use scope `ostro-engine`. Attributes: `ostro.run_id`, plus `ostro.workflow`, `ostro.attempt`, `ostro.replayed_events`, `ostro.outcome`, `ostro.seq`, `ostro.status`, `ostro.op` and `ostro.effect_kind`, depending on the span.

## Id vectors (pinned in `telemetry::tests::telemetry_id_vectors_are_pinned`)

`trace = blake3("ostro/trace" ‖ run_id)[..16]`, `span = blake3("ostro/span" ‖ run_id ‖ kind ‖ n_le)[..8]`. For `run_id = "run-1"`:

| Id | Value |
|---|---|
| trace (derived) | `86ca30530a6d71a97854d22c0272e94b` |
| `run` / 0 | `d3ede864a16f6334` |
| `activation` / (1, 1) | `9bc352849fc38bc5` |
| `step` / 3 | `a6b04adb4033b468` |
| `effect` / 7 | `a4b3d0fc243cbad8` |

`telemetry_ids_follow_the_formula` also re-derives these with blake3 directly.

## Action-stream mechanism

`run_spans::spawn` returns `None` unless `[telemetry] otlp_endpoint` is set and `traces = true`. Otherwise it calls `MachineHandle::subscribe_actions()` on every `PartitionSlot::Ready` handle; this is the broadcast that the dispatcher, timers and shuffler already consume. It subscribes before returning, then runs one JoinSet task per partition.

For each `Action::RunCompleted` it reads the record with `PartitionHandle::read_run` and calls `emit_run_span`. If the receiver lags, it logs a warning and continues, and the skipped spans are lost. The machine publishes actions only once its `emit` flag is set, which happens after catch-up, so a restart never re-emits a span. The only clock read is `ServiceCtx.clock` as the fallback when a record has no `completed_at_ms`.

## Phase 6/7 hook signatures (ostro_engine::telemetry)

```rust
pub fn effect_span_context(run_id: &str, traceparent: Option<&str>, op: u64) -> SpanContext;
pub fn effect_span(run: &RunTrace, op: u64, kind: &str, start: SystemTime, end: SystemTime,
                   outcome: Result<(), &str>);
pub fn traceparent_headers(cx: &SpanContext) -> Vec<(String, String)>;   // global propagator, W3C fallback
pub fn format_traceparent(cx: &SpanContext) -> String;
impl RunTrace { pub fn new(run_id: &str, traceparent: Option<&str>) -> Self; }
impl HostState { pub fn activation_span_context(&self) -> Option<SpanContext>; }
pub async fn with_traceparent<F: Future>(traceparent: Option<String>, f: F) -> F::Output;
pub fn emit_run_span(run: &RunTrace, workflow: &str, start: SystemTime, end: SystemTime,
                     status: &str, ok: bool);
```

For 08-21: in `decorate_live_request(&RunRecord, op, req)`, append `traceparent_headers(&effect_span_context(&run_id, rec.traceparent.as_deref(), op))` to the live copy. In `on_effect_complete`, call `effect_span(&RunTrace::new(&run_id, rec.traceparent.as_deref()), op, kind, started, ended, outcome)`.

## Task Commits

1. **Task 1: engine telemetry (ids, activation and live-only step spans, effect hook, traceparent helper)**: `d06448c`
2. **Task 2: run span on RunCompleted, executor seed, server span tests, docs**: `e32039f`

## Verification

Worktree rebased onto main before each commit. `CARGO_TARGET_DIR=target/wt-08-11`.

Plan checks:
- `cargo test -p ostro-engine --test telemetry_spans --test determinism --locked`: ok, 1 passed and 10 passed.
- `cargo test -p ostro-engine --lib --locked -- telemetry`: ok, 5 passed.
- `cargo tree -p ostro-engine -e normal --locked --prefix none` has no `opentelemetry_sdk` line ("no-sdk").
- `cargo test -p ostro-server --test tracing_spans --locked`: ok, 1 passed. This was re-run after both rebases, including after 08-10 changed `tests/common/mod.rs`.

Wider checks:
- `cargo test -p ostro-engine --locked`: every binary ok.
  - lib 88, determinism 10, replay_corpus 5, effects_driver 12, wit_surface 17, telemetry_spans 1, and the rest.
  - The replay corpus files and runtime.wit are untouched.
- `cargo test -p ostro-server --locked`: every target ok.
  - lib 48, executor 8, executor_phase5 12, otlp_export 1, sdk_e2e 9, semantics_e2e 18, start_run_api 11, tracing_spans 1, and the rest.
  - perf and semantics_sleepers are ignored, as they were before.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: clean before the Task 2 rebase. After each rebase, `cargo clippy -p ostro-server -p ostro-engine --all-targets --locked -- -D warnings` was clean.
- `cargo fmt --all --check`: clean.

**Not run:** the full `bash scripts/ci-local.sh --no-musl`. Parallel executors were building and the brief caps concurrent builds. The workspace clippy and the engine and server suites cover the touched crates.

## Deviations from Plan

### Auto-fixed / adapted

**1. [Rule 3 - Blocking] `blake3` added to ostro-engine (Cargo.toml + one Cargo.lock line)**
- P8-D14 fixes blake3 for the id derivation, but ostro-engine had no blake3 dependency, while the plan said "No Cargo.toml/Cargo.lock changes".
- `blake3` (workspace pin, `pure` feature) was already in the lock through ostro-partition. The lock diff is `"blake3"` added to ostro-engine's dependency list, and no new crates are pulled in.
- None of the parallel wave-3 plans touch Cargo files.
- Commit `d06448c`.

**2. [Rule 3] `host/state.rs` touched (not in files_modified)**
- It gets one `trace: Option<ActivationTrace>` field and a 6-line hook in `flush`: collect step records before the append, emit them after `r?`.
- Emitting at `complete_step` time in journal.rs would have produced spans for steps whose append later failed. The re-execution would then produce a duplicate span id, which breaks "exactly one span per (run, seq)".
- journal.rs still records the live attempt start times through `telemetry::`.

**3. [Design] Run seed via task-local, not a `RunCtx` field on `Activate`**
- `Activate` has 24 construction sites across engine and server tests, none of them in this plan.
- `telemetry::with_traceparent(record.traceparent, runtime.activate(..))` carries the traceparent instead. Without the wrapper (engine tests, other callers) the trace id is derived from the run id.

**4. [Design] Activation counter = committed journal length at activation start**
- No executor-side counter exists, and an in-memory one would reset on restart and collide.
- The journal length is deterministic and grows with every activation that makes progress.
- Two activations of the same attempt over an identical journal (for example a re-dispatch after `AppendConflict` / `External` with no new events) share an activation span id. This is rare and only affects activation spans; step ids are unaffected.

**5. [Test design] Engine test uses an API-only recording tracer**
- The engine test uses a small `TracerProvider` written on the `opentelemetry` API instead of the SDK's `InMemorySpanExporter`, so ostro-engine needs no `opentelemetry_sdk` dev-dependency.
- It consumes `take_next_ids()` like `OstroIdGenerator`, and asserts that children inherit the trace id.
- The end-to-end SDK + OTLP path is covered by `tracing_spans.rs`.

**6. [Test design] tracing_spans builds its server locally**
- 08-10's `tests/common/phase8.rs` landed during this plan. The test uses `common::TestServer::start_with` with an OTLP endpoint and `engine.hot_wait = 1ms` (sleeps evict).
- The caller trace is passed with the `traceparent` **header**, which 08-10's inbound support now accepts.

**7. [Note] Two spans named `activation`**
- The pre-existing `tracing::info_span!("activation")` in `run_activation` (log context) is still exported by the tracing-opentelemetry layer, with random ids and a different scope.
- The OBS-01 spans use scope `ostro-engine`, and the tests filter on it. docs/observability.md documents this.

## Self-Check: PASSED

- FOUND: crates/ostro-engine/src/telemetry.rs, crates/ostro-engine/tests/telemetry_spans.rs, crates/ostro-server/src/services/run_spans.rs, crates/ostro-server/tests/tracing_spans.rs, docs/observability.md
- FOUND: d06448c and e32039f on main
