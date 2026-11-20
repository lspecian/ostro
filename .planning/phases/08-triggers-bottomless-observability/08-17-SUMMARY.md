---
phase: 08-triggers-bottomless-observability
plan: 17
subsystem: observability
tags: [guest-logs, obs-03, rate-limit, ring-buffer, otlp, logs, json]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: "08-04 GuestLogSink stub + [guest_logs] config + services seam + OTLP logger provider + bridge exclusion of ostro::guest; 08-10 RunLogsApi + run_logs controller stub + tests/common/phase8.rs; 08-11 HostState::activation_span_context + trace_id_for"
provides:
  - "ostro_engine::sink::{GuestLogRecord, ActivationSink::guest_log / handles_guest_logs (default no-op / false), guest_log_metrics}"
  - "services::guest_logs::GuestLogSink: node bucket -> run bucket -> lifetime cap, ring LRU, node log line, OTLP LogRecord, metrics; run_logs / run_stats / forget_run"
  - "control::run_logs::RunLogsController (GET /v1/runs/{id}/logs, gRPC RunLogs/GetRunLogs)"
  - "guests/log-spammer test guest"
  - "docs/observability.md guest logs section"
affects: [08-21, 08-22, Phase 7 workers]

tech-stack:
  added: []
  patterns:
    - "Default trait methods on ActivationSink for new engine->server hand-offs (other implementors compile unchanged)"
    - "Check all limits first, charge buckets only on accept (a capped run does not drain the node bucket)"

key-files:
  created:
    - crates/ostro-server/tests/guest_logs.rs
    - guests/log-spammer/Cargo.toml
    - guests/log-spammer/src/lib.rs
  modified:
    - crates/ostro-engine/src/host/log.rs
    - crates/ostro-engine/src/sink.rs
    - crates/ostro-engine/src/host/state.rs
    - crates/ostro-server/src/sink.rs
    - crates/ostro-server/src/executor.rs
    - crates/ostro-server/src/server.rs
    - crates/ostro-server/src/services/guest_logs.rs
    - crates/ostro-server/src/control/run_logs.rs
    - crates/ostro-server/tests/phase8_smoke.rs
    - docs/observability.md

key-decisions:
  - "The OTLP LogRecord goes through the server's own SdkLoggerProvider (TelemetryGuard::logger_provider), not a global one: opentelemetry 0.33 has no global logger provider, and a per-server logger stays correct with several servers in one process"
  - "Guest fields in the node log are one JSON object string under guest.fields, also under LogFormat::Json (tracing field names are static; valuable needs tracing_unstable). The OTLP LogRecord has them as real attributes"
  - "Unknown/forgotten/evicted runs: 200 with lines [] (no truncated/evicted flag: RunLogsResponse is 08-10's frozen DTO); `dropped` counts rate/cap/eviction losses per run"
  - "Guest log metric names and counters live in ostro_engine::sink::guest_log_metrics, since the engine counts activation_bound itself"
  - "Bucket checks are all-or-nothing: a line is charged to the node and run buckets only when all three checks pass"

requirements-completed: [OBS-03]

duration: ~1h15m
completed: 2026-11-20
---

# Phase 8 Plan 17: Guest logs Summary

**Every live guest log line now goes from the engine through the activation sink to a node-level `GuestLogSink`. The sink applies a node bucket (5,000/s), a per-run bucket (50/s, burst 500) and a per-run lifetime cap (10,000 lines / 10 MiB). Each accepted line lands in a per-run ring buffer, an LRU under a node byte budget, served at `GET /v1/runs/{id}/logs`. It also becomes one `ostro::guest` node log line and exactly one OTLP LogRecord carrying `run.id`/`workflow`/`attempt`, the guest fields and the activation's trace context. Replay emits nothing, and logs are never journaled.**

## Measured limiter numbers (tests/guest_logs.rs and the unit tests)

| Scenario | Result |
|---|---|
| (a) 20,000 lines in 20 activations of 1,000 (rates lifted) | accepted **10,000**, `dropped_run_cap` **10,000**, rate drops 0; `/metrics` `ostro_guest_log_dropped_total{reason="run_cap"}` delta ≥ 10,000 |
| (b) 2,000 lines in one activation, default limits | 1,000 reach the sink (activation bound); accepted **500**, `run_rate` 500. The run took about 27 ms, so it got no measurable refill. Same on 3 runs. |
| (c) `?limit=100` after (a) | seq 9900..9999 in order; `dropped` = 10,000; `limit=1000` returns 1,000; `level=info` on debug lines returns `[]` |
| (d) restart during a 5 s sleep, 2 chunks of 1,500 | before the restart: 1,000 OTLP records (seq < 1500). After: the new sink accepted exactly 1,000 (chunk 1 only); seq < 1500 still 1,000, seq ≥ 1500 = 1,000 |
| unit: run bucket (ManualClock) | 500 burst, then 50 after +1 s, 5 after +100 ms |
| unit: node bucket 100/s | 100 across two runs, then 50 after +500 ms, all `node_rate` |
| unit: byte cap 1,000 B, 100 B lines | 10 accepted |

## How JSON fields render

`tracing::event!(target: "ostro::guest", <level>, run_id, workflow, attempt, guest.fields = <json>, "{message}")`. Under `LogFormat::Json`:

```json
{"timestamp":"…","level":"WARN","fields":{"message":"payment failed","run_id":"run-json","workflow":"wf","attempt":3,"guest.fields":"{\"order\":\"42\",\"reason\":\"card \\\"declined\\\"\"}"},"target":"ostro::guest"}
```

`run_id`, `workflow` and `attempt` are real JSON keys. The guest's own keys are a JSON object **string** under `fields["guest.fields"]`, which takes a second parse. Test (e) asserts exactly this. Real keys would need `valuable`, which requires the unstable `tracing_unstable` cfg. Without a consuming sink (engine tests), the engine's fallback event has the same shape.

## OTLP attribute set

| Part | Value |
|---|---|
| scope | `ostro.guest` |
| body | message |
| severity | number + text `TRACE`/`DEBUG`/`INFO`/`WARN`/`ERROR` |
| timestamp | engine live clock (`live_now_nanos`); observed = injected node clock |
| attributes | each guest field (string) + `run.id` (string) + `workflow` (string) + `attempt` (int) |
| trace_id / span_id | the activation span context from 08-11 (`trace_id_for(run, traceparent)` / the activation span id), flags sampled |

Test (f) asserts all of this against `trace_id_for(run, None)`. Test (g) asserts that 50 accepted lines produce exactly 50 records with that body prefix across **all** scopes, i.e. no bridged copy.

## Task Commits

1. **Task 1: engine forwarding + GuestLogSink**: `b5dd14c`
2. **Task 2: run-logs endpoint, log-spammer guest, integration tests, docs**: `9e58109`

## Verification

`CARGO_TARGET_DIR=target/wt-08-17`. The worktree was rebased onto main before each commit.

- `cargo test -p ostro-engine --locked`: every binary ok (lib 89 with the new `guest_log_record_fields_json_and_size`, determinism 10, and the rest).
- `cargo test -p ostro-server --lib --locked -- guest_logs`: 9 passed. The run_logs unit tests pass too (lib total 66).
- `cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml -p log-spammer`: ok. The full guests build (ci-local `check_guests`) is warning-free.
- `cargo test -p ostro-server --test guest_logs --locked`: 4 passed. Ran 3 more times, all green, about 5.5 s each.
- `grep -q 'run_cap' docs/observability.md`: ok.
- `cargo test -p ostro-server --locked --no-fail-fast`: every target ok. Counts:

  | Target | Passed | Ignored |
  |---|---|---|
  | lib | 66 | |
  | executor | 8 | |
  | executor_phase5 | 12 | |
  | guest_logs | 4 | |
  | phase8_smoke | 1 | |
  | sdk_e2e | 9 | |
  | semantics_e2e | 18 | |
  | sink | 15 | |
  | tracing_spans | 1 | |
  | webhooks | 5 | |
  | perf | | 1 (as before) |
  | semantics_sleepers | | 2 (as before) |

- `cargo clippy --workspace --all-targets --locked -- -D warnings`: clean.
- `cargo fmt --all --check`: clean.

**Not run:** the full `bash scripts/ci-local.sh --no-musl`. Parallel executors were building (load average about 15), and the brief caps concurrent builds. The workspace clippy, fmt, the engine and server suites and the guests build cover the touched code.

**Flake seen once, under load:** the first `cargo test -p ostro-server` run failed all 8 `tests/executor.rs` tests at `tests/common/mod.rs:186` (`Node::open` inside `timeout(T, ..)`), while the load average was about 15. Re-running `--test executor` alone gave 8 passed, and the full `--no-fail-fast` run was green. These tests do not involve guest logs, and the failure is in the node-open timeout of the shared harness.

## Deviations from Plan

### Auto-fixed / adapted

**1. [Rule 3] `server.rs`: the sink gets the clock and the logger provider (one statement)**
- opentelemetry 0.33 has no `global::logger_provider()`, so the plan's `global::logger_provider().logger("ostro.guest")` cannot be written.
- The sink is built in `server.rs` as `GuestLogSink::new(..).with_clock(clock).with_logger_provider(telemetry.logger_provider())`. That also wires the one injectable clock (F5).
- No running plan touches `server.rs`. 08-14 (wave 4, not running) will rebase over a 4-line hunk.

**2. [Rule 3] `host/state.rs`: `sink` field made `pub(crate)`**
- `host/log.rs` must reach the activation sink, and the field was private to `state.rs`. This is a one-word change.

**3. [Rule 3] `tests/phase8_smoke.rs`: the run-logs stub assertions replaced**
- REST now returns 200 `{"run_id":"run-1","lines":[],"dropped":0}`, and gRPC returns OK with empty lines. 08-15 made the same kind of edit for webhooks.

**4. [Design] `GuestLogRecord.run_id: String` instead of `RunId`**
- ostro-engine's sink seam uses string run ids (`RunRef`). The record also carries `workflow` from the run's `RunStarted`.

**5. [Design] `handles_guest_logs()` default method next to `guest_log()`**
- The plan said the engine falls back to its own tracing event only when no sink is configured.
- With only a no-op `guest_log`, any other sink implementor (the testkit fake, future workers) would swallow lines silently. The engine therefore falls back whenever the sink does not consume guest logs. The server's `PartitionSink` and `EngineSink` return `true`.

**6. [Design] No `truncated`/`evicted` flag in the response**
- `RunLogsResponse` is 08-10's DTO, with an OpenAPI snapshot and proto behind it, outside this plan.
- An unknown, forgotten or evicted run answers 200 with `lines: []`, and `dropped` includes ring-eviction losses. `GuestLogSink::run_logs` returns `unknown`/`evicted` flags internally, so a later API revision can expose them.

**7. [Design] Metric helpers live in ostro-engine (`sink::guest_log_metrics`)**
- The engine counts `activation_bound` itself. When Task 1 started, ostro-server had no `metrics` dependency, and Cargo files were off limits.
- 08-15 has since added `metrics` to ostro-server. The helpers stay in the engine so all four reasons share one definition.

**8. [Note] `guests/Cargo.lock` is not committed**
- It is gitignored by design: the guests workspace pins versions exactly in `guests/Cargo.toml`. The new member adds no dependencies beyond sdk-basic's.

**9. [Note] Limiter state lifecycle**
- The sink has no run-completion signal, so states age out instead: after 1 h without a line once the ring is empty, swept every 4,096 emits and on every new run.
- A hard bound of 100,000 tracked runs evicts the least recently seen. `forget_run` exists for an explicit cleanup hook. All of this is documented.

## Self-Check: PASSED

- FOUND: crates/ostro-server/src/services/guest_logs.rs (`struct GuestLogSink`, 850 lines), guests/log-spammer/src/lib.rs, crates/ostro-server/tests/guest_logs.rs, docs/observability.md (`run_cap`)
- FOUND: b5dd14c and 9e58109 on main
