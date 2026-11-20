---
phase: 08-triggers-bottomless-observability
plan: 21
subsystem: observability
tags: [obs-01, obs-02, obs-03, traceparent, effect-spans, sc5, otlp, canary, d-fx6]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: "08-11 effect_span / effect_span_context / traceparent_headers / RunTrace; 08-14 schedules; 08-15 webhooks; 08-16 metrics; 08-17 guest logs; 08-18 restore; 08-10 tests/common/phase8.rs"
  - phase: 06-effects-bridge-connectors-secrets
    provides: "06-14 EffectHooks + ServerEffectDriver::set_hooks + start_server_with_bridge/crash; 06-19 canary harness (register_sink, Report, spawn_quiet)"
provides:
  - "ostro_server::effect_trace::TraceEffectHooks (EffectHooks impl): outbound traceparent on the live HTTP copy, effect:<kind> span per completed effect"
  - "server installs TraceEffectHooks when OTLP traces are on"
  - "tests/outbound_traceparent.rs (B2 evidence), tests/observability_e2e.rs (SC5), tests/otlp_canary.rs (D-FX6 OTLP leg + Phase 6 fx-canary leg)"
affects: [08-22]

tech-stack:
  added: []
  patterns:
    - "Effect hooks read the run's stored traceparent from the partition table at completion time (on_effect_complete only gets a RunRef)"
    - "OTLP canary scans the collector through the Phase 6 harness sink (register_sink(\"otlp\"))"

key-files:
  created:
    - crates/ostro-server/src/effect_trace.rs
    - crates/ostro-server/tests/outbound_traceparent.rs
    - crates/ostro-server/tests/observability_e2e.rs
    - crates/ostro-server/tests/otlp_canary.rs
  modified:
    - crates/ostro-server/src/effects.rs
    - crates/ostro-server/src/lib.rs
    - crates/ostro-server/src/server.rs

key-decisions:
  - "B2 wired (06-14 landed): TraceEffectHooks implements EffectHooks; no BLOCKING follow-up is needed and 08-22 may count the outbound evidence for OBS-01"
  - "EffectHooks::decorate_live_request gains a leading `run: &RunRef` argument: RunRecord carries no run id, and the effect span id needs it"
  - "Hooks are installed only with OTLP traces on; otherwise the driver keeps NoopEffectHooks and the outbound request equals the journaled intent"
  - "A guest-supplied traceparent header wins (left untouched); the effect span is still emitted"
  - "OTLP canary TRACE level keeps h2/tonic/tower and ostro_wal::group_commit at INFO: at TRACE the exporter's own gRPC spans feed back into the span exporter (120k spans in 40 s) and the batch queues drop records"

requirements-completed: []

duration: ~45m
completed: 2026-11-20
---

# Phase 8 Plan 21: SC5 observability e2e, outbound traceparent, OTLP canary Summary

**Outbound HTTP effects now carry the run's W3C `traceparent` (trace id = the run's trace, parent = `span_id(run, "effect", op)`) on the live request copy only, and every completed effect exports an `effect:<kind>` span under `workflow.run`. A crash re-drive sends the same header, and the journaled intent never contains it. SC5 holds end to end on one server: the span tree, bounded guest logs with exactly one LogRecord per accepted line, the SC5 metric families, and no re-emission after a restart. The OTLP canary leg of D-FX6 scans 0 hits, including the Phase 6 fx-canary scenario through the harness sink.**

## B2 outcome: wired, with evidence

06-14 had landed (`EffectHooks`, `NoopEffectHooks`, `ServerEffectDriver::set_hooks`), so Task 1 took the "landed" branch. No BLOCKING follow-up was written to STATE.md.

- `crates/ostro-server/src/effect_trace.rs`: `TraceEffectHooks::new(Arc<PartitionTable>)`.
  - `decorate_live_request(run, rec, op, req)` appends `traceparent_headers(effect_span_context(run, rec.traceparent, op))`, unless the request already has a `traceparent` (any case). The guest's header wins.
  - `on_effect_complete(run, op, kind, started, ended, outcome)` reads the run's stored traceparent through the partition table. If the record is gone, it falls back to the derived trace id. It then calls `effect_span(RunTrace::new(..), op, "http"|"sql"|"object", started, ended, outcome)`. The outcome is `ok`, or the bounded failure kind (`ErrorInfo.kind`, `timeout`, `cancelled`, `undecodable`).
- `server.rs` (2c, right after `BridgeServices::build`): `bridge.driver().set_hooks(Arc::new(TraceEffectHooks::new(bridge.partitions().clone())))`, only when `TelemetryGuard::tracer_provider()` is `Some`.

### Cross-phase edit (effects.rs, Phase 6 file)

The landed signature differs from the plan's interface: `decorate_live_request(&RunRecord, op, &mut HttpRequest)` cannot build the effect span context, because `RunRecord` has no run id. The trait method now takes `run: &RunRef` first, and the one call site in `ServerEffectDriver::start` passes the `run` it already holds. The no-op unit test was updated. No other implementor exists. Intent journaling, idempotency keys, redirects, egress checks and `send.rs` are untouched. The main checkout shows `D crates/ostro-server/src/effects.rs` in its stale index. That is the known stale-tree artifact, not uncommitted Phase 6 work; `git show main:` had the 06-14 file.

### tests/outbound_traceparent.rs (1 test, recording axum mock + OTLP collector)

| Case | Asserted |
|---|---|
| (a) caller `traceparent: 00-4bf9…-00f0…-01`, 2 GETs | the mock got exactly one `traceparent` per request = `00-<caller trace>-<span_id(run,"effect",op)>-01`; the journaled header names are the guest's only (`["x-req"]`, `[]`); the `EffectScheduled` bytes contain no `traceparent` |
| (b) no caller header | trace id = `trace_id_for(run, None)` |
| (c) guest sets its own `traceparent` | sent verbatim, once; the effect span is still exported |
| effect spans | exactly one `effect:http` per op (a=2, b=1, c=1), id = `span_id(run,"effect",op)`, trace = the run's, parent = `span_id(run,"run",0)`, `ostro.outcome = ok` |
| (d) re-drive: `/hold` hangs, `crash()`, `restart()` | 2 sends of `/hold`, both with the identical traceparent (`00-4bf9…-c3f33b7fad355fcf-01` in one run); one intent; one effect span after the restart; no traceparent in the journal |
| (e) a second server without OTLP | the mock gets no traceparent; the journal is as in (a) |

Printed: `BRIDGE_TRACEPARENT_LEG: wired`.

The plan compares "journal bytes with and without telemetry". Two runs never share journal bytes (run id, timestamps), so the test asserts the equivalent instead: the journaled request in both configurations holds exactly the guest's headers and no `traceparent` bytes, and only the live copy differs.

## SC5 (tests/observability_e2e.rs, 1 test)

One `Phase8Server` with the collector and `/metrics`, `hot_wait = 50ms`. log-spammer runs 1,200 lines in 3 chunks of 400, with `mark` steps and 200 ms evicting sleeps, under `traceparent 00-0af7…-b7ad…-01`. Measured in one run (another run gave the same span counts):

| Item | Count |
|---|---|
| `workflow.run` | 1, id `span_id(run,"run",0)`, parent = caller span `b7ad6b7169203331`, trace = caller trace |
| `activation` | 3 (two evictions), all parented to the run span |
| step spans | 2 (`mark(1)`, `mark(2)`) = journal `StepCompleted` count, ids `span_id(run,"step",seq)`, parent = an activation |
| guest lines | 524 accepted, 676 dropped `run_rate` (default 50/s, burst 500); the bound is 500 + 50/s × elapsed + 10 |
| OTLP LogRecords for the run | **524 = accepted**, all distinct seqs, scope `ostro.guest`, caller trace id, span id = one of the run's activation spans |
| `/metrics` | `ostro_queue_ready`, `ostro_queue_lag_seconds_bucket`, `ostro_partition_apply_lag`, `ostro_steps_total`, `ostro_step_duration_seconds_bucket`, `ostro_replay_events_total`, `ostro_replay_duration_seconds_bucket`, `ostro_fuel_consumed_total`, `ostro_instance_memory_bytes`, `ostro_wasm_memory_budget_bytes`, `ostro_dlq_total{reason="nondeterminism"}` ≥ 1, `ostro_guest_log_dropped_total{reason="run_rate"}` delta ≥ 676; no run id in the scrape |
| restart | `Counts { run: 1, activations: 3, steps: 2, logs: 524 }` unchanged, the dead-lettered run's span count unchanged, and a fresh run after the restart is exported (run span + 3 LogRecords) |

The DLQ series is produced the same way as in `metrics_endpoint.rs`: a patch-v1 run is replayed on patch-v3 through `hooks.module_override`.

## OTLP canary (tests/otlp_canary.rs, 1 test)

- **Canary:** `ostro_testkit::canary::Canary::new()` (32 random base62 characters, 10 deduplicated needles: raw, base64 std/url ± padding plus 3 alignment middles, hex lower/upper, percent forms, JSON escapes). The plan said 32 random bytes as hex; the harness canary is used so the scan is the same as Phase 6's.
- **Where the canary is used:**
  - sealed meta secret `canary-key` (REST);
  - Stripe webhook `secret_ref = canary-key`, driven with valid (200, run completed), redelivery (200), tampered (401), unsigned (401) and stale −3600 s (401) deliveries;
  - the running server's `[backup] secret_access_key` (LocalFs URL; inline, because `bridge_server_config` resolves env refs against an empty map);
  - `env:CANARY` in a restore TOML loaded with `load_layered(.., EnvSource::Map{CANARY})`. It resolves to the canary, its `Debug`/`--print-config` renderings are logged through `tracing`, and it drives a real `ostro_backup::restore` of the server's backup prefix;
  - fx-canary with `X-Api-Key: {{secret:canary-key}}`. The mock (under `spawn_quiet`) received exactly the canary, and the echo is `[REDACTED]`. With traces on, this also exercises `TraceEffectHooks`.
- **Schedule CRUD:** create, list, get, pause, resume, update with version_tag, delete.
- **Level:** `[log] level = "trace,h2=info,tonic=info,tower=info,ostro_wal::group_commit=info,<06-19 compiler targets>=warn"`. `RUST_LOG` is not set from the test (edition 2024 makes `set_var` unsafe), so the config directive does the same thing.
- **Non-vacuity:** the scanner finds the raw value; the export holds the hook run id, the fx-canary run id, an `effect:http` span and the "restore config loaded"/"restore done" log lines; spans > 0 and logs > 100.
- **Scan result (last run):**

| Location | Bytes | Hits |
|---|---|---|
| `sink:otlp` (the collector's raw `Debug` export, through `register_sink("otlp")` + `Report::sinks()`) | 32.8 MB (13,627 spans, 12,816 log records) | 0 |
| decoded span/log strings (names, bodies, attribute keys/values, resources) | 9.4 MB | 0 |
| backup prefix (13 files) | 343 KB | 0 |
| restored data dir (169 files) | 275 KB | 0 |

**Phase 6 canary leg: run.** The collector is registered as the harness sink, and the fx-canary scenario runs inside this test with OTLP on. Printed: `PHASE6_CANARY_LEG: run (fx-canary through the harness sink "otlp"): clean`. `tests/fx_canary.rs` itself (CaptureLayer, no OTLP) is unchanged and still passes. It cannot export OTLP, because CaptureLayer and the server's OTLP subscriber are both process-global.

## Task Commits

1. **Task 1: TraceEffectHooks, install, effects.rs signature, outbound_traceparent test**: `a5fa6a1`
2. **Task 2: SC5 observability e2e**: `da79788`
3. **Task 3: OTLP canary leg**: `cc5fef8`

## Verification

Worktree wt-08-21 detached at main, `CARGO_TARGET_DIR=target/wt-08-21`, test runs sequential.

- `cargo test -p ostro-server --test outbound_traceparent --locked`: **test result: ok. 1 passed** (15 s).
- `cargo test -p ostro-server --test observability_e2e --locked`: **ok. 1 passed**, run twice (4 s each).
- `cargo test -p ostro-server --test otlp_canary --locked`: **ok. 1 passed**, 3 green runs with the final level filter. The first full-TRACE run failed its non-vacuity wait (deviation 5).
- `cargo test -p ostro-server --lib --locked -- effect`: ok. 8 passed (3 new `effect_trace` unit tests + the updated no-op hooks test).
- `cargo test -p ostro-server --locked --no-fail-fast` on cc5fef8 (load average 13-21, next to 06-20's ci-local and 08-20): **every target ok, 298 passed, 0 failed, 3 ignored** (perf 1, semantics_sleepers 2, as before), 38 targets. This includes fx_canary 4, tracing_spans 1, guest_logs 4, metrics_endpoint 4, webhooks 5, schedules 4, effects_seam 6, fx_http_e2e 4 and sink 19.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: clean.
- `cargo fmt --all --check`: only `crates/ostro-server/tests/phase8_smoke.rs` (pre-existing, 08-18; not touched here). The files of this plan are formatted.
- No Cargo.toml / Cargo.lock change.
- **Not run:** `bash scripts/ci-local.sh --no-musl`. 06-20 was running its final ci-local gate and 08-20 was building at the same time, and the brief keeps load modest. The ostro-server suite and workspace clippy cover the touched crate; the full gate is 08-22's.

## Deviations from Plan

**1. [Rule 3 - Blocking] `EffectHooks::decorate_live_request` takes `run: &RunRef`** (effects.rs, Phase 6 file: trait, one call site, unit test). The record has no run id. See "Cross-phase edit".

**2. [Rule 3] `server.rs` edited (not in files_modified)**: an 11-line install block after `BridgeServices::build`. The plan says to install "where the server builds ServerEffectDriver", which is server.rs. No wave-6 plan touches server.rs.

**3. [Test design] The journal comparison uses names and bytes, not identical journals** (see B2 above).

**4. [Test design] Canary = harness `Canary::new()`** instead of 32 random bytes as hex. The backup key is inline in the server config, and `env:CANARY` is covered by the restore-config load.

**5. [Test design] TRACE minus the OTLP transport and WAL tick targets.** The first full-TRACE run exported 120,768 spans and 171,156 log records (514 MB) in 42 s. One run failed its non-vacuity wait because the batch queues dropped records. The exporter's own h2/tonic spans feed back into the span exporter at TRACE (the log bridge excludes these targets; the span layer does not). This is recorded for 08-22 as an observation; with the default `info` level it does not happen. The outbound HTTP path of the bridge (hyper, reqwest) stays at TRACE.

## Notes for 08-22

- OBS-01 outbound evidence exists (B2 wired). There is no blocking follow-up.
- `docs/observability.md` already lists `effect:<kind>` (08-11). The install condition (traces on) and the "guest traceparent wins" rule are documented in `effect_trace.rs`. 08-22 may copy them into the docs.
- Possible hardening: filter `h2`/`tonic`/`tower`/`opentelemetry*` out of the tracing-opentelemetry span layer as well, matching the log bridge's exclusions.
- `cargo fmt --all --check` reports a diff in `crates/ostro-server/tests/phase8_smoke.rs` (from 08-18, aeacc41). It is not touched here (out of scope).

## Self-Check: PASSED

- FOUND: crates/ostro-server/src/effect_trace.rs, crates/ostro-server/tests/{outbound_traceparent,observability_e2e,otlp_canary}.rs
- FOUND: a5fa6a1, da79788, cc5fef8 on main
