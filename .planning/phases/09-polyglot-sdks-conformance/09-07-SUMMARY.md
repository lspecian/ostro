---
phase: 09-polyglot-sdks-conformance
plan: 07
subsystem: engine
tags: [engine, validation, componentize-py, pooling, admission, config, replay]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-02 wasmparser pin, build-guest.sh, ostro-conformance polyglot build/inspect, py-min/ts-min fixtures"
  - phase: 09-polyglot-sdks-conformance
    provides: "09-04 S9: pool minimum per component (py 38, QJS 13, SM 3), fuel numbers, SM open-step replay divergence"
provides:
  - "validate.rs: exact-shape allowance for componentize-py's `exports` export (ENG-1), count_core_instances, render_type, componentize_py_init_signature"
  - "core_budget.rs: CoreBudget weighted FIFO semaphore + CorePermit (ENG-2)"
  - "Runtime: calibrated per-module admission weight (Runtime::core_weight), permit held around run_activation, BudgetPressure eviction on timeout, describe/instantiate_probe budgeted"
  - "LanguageClass::Mixed + FromStr/as_str, PoolConfig.total_core_instances, EngineConfig::effective_total_core_instances"
  - "server [engine] class (rust|python|typescript|mixed) and pool_total_core_instances (TOML/env/--print-config)"
  - "replay fix: a step the live run abandoned (dropped, or open at return) replays without NonDeterminism"
  - "guests exports-shape-ok (also abandon-open/abandon-drop regression workflows) and exports-shape-bad"
affects: [09-08, 09-12, 09-13, 09-14, 09-16, 09-20]
tech-stack:
  added: []
  patterns:
    - "structural component-type rendering compared against a canonical signature string"
    - "static core-instance count as a lower bound, raised once per prepared module by a calibration instantiation under a StoreLimits instance limit"
    - "unrecorded replay step: begin-step that does not match the journal opens the step at the next event's seq; recording an outcome reports the divergence"
key-files:
  created:
    - crates/ostro-engine/src/core_budget.rs
    - crates/ostro-engine/tests/exports_shape.rs
    - crates/ostro-engine/tests/core_budget.rs
    - crates/ostro-server/tests/engine_class.rs
    - crates/ostro-conformance/tests/polyglot_engine.rs
    - guests/exports-shape-ok/{Cargo.toml,wit/world.wit,src/lib.rs}
    - guests/exports-shape-bad/{Cargo.toml,wit/world.wit,src/lib.rs}
  modified:
    - crates/ostro-engine/src/validate.rs
    - crates/ostro-engine/src/config.rs
    - crates/ostro-engine/src/runtime.rs
    - crates/ostro-engine/src/lib.rs
    - crates/ostro-engine/src/replay/cursor.rs
    - crates/ostro-engine/src/replay/tests.rs
    - crates/ostro-server/src/config.rs
    - crates/ostro-conformance/src/engine_leg.rs
key-decisions:
  - "Budget timeout in Runtime::activate evicts with EvictReason::BudgetPressure (the executor re-activates it); a cancel while waiting evicts External"
  - "Admission weight = calibrated count: the static wasmparser count (Instantiate entries, nested components flattened) is exact for Rust (3) and Python (38) but QuickJS needs 13, not 11 (Wasmtime's fused adapter modules are not in the binary), so each prepared module is calibrated once by instantiating it under a growing StoreLimits instance limit"
  - "Abandoned live steps: replay tolerates them (the step is opened unrecorded) instead of failing the live run that returns with an open step; no journal or WIT change"
  - "Server `class` is a typed enum with a custom Deserialize so an unknown value names all four allowed values (the config crate's enum error does not)"
requirements-completed: [SDK-02, SDK-03]
duration: ~2h 30min
completed: 2026-11-22
---

# Phase 9 Plan 07: Engine polyglot support (ENG-1, ENG-2) Summary

**Real componentize-py and QuickJS components now pass deploy validation and run on the real engine. Every instantiation is admitted against a weighted core-instance budget, so a full pool waits instead of failing. `[engine] class` and `pool_total_core_instances` are operator settings. A replay bug for steps the live run abandoned is fixed.**

## Commits

- `5197264` fix(09-07): replay a step the live run abandoned without NonDeterminism
- `b6c9ab7` feat(09-07): ENG-1 exact-shape allowance for componentize-py's exports export
- `6092921` feat(09-07): ENG-2 core-instance count, weighted admission, engine class config

## ENG-1: the `exports` export

componentize-py 0.25.1's extra export, taken from `jco wit` of a real `py-min` build:

```wit
export exports: interface {
  record function { protocol: string, name: string }
  record %constructor { module: string, protocol: string }
  record %static { module: string, protocol: string, name: string }
  variant function-export-kind { freestanding(function), %constructor(%constructor), method(string), %static(%static) }
  variant return-style { none, normal, %result }
  record function-export { kind: function-export-kind, return-style: return-style }
  record %resource { %package: string, name: string }
  record %record { %package: string, name: string, fields: list<string> }
  record %flags { %package: string, name: string, u32-count: u32 }
  record %tuple { count: u32 }
  record case { name: string, has-payload: bool }
  record %variant { %package: string, name: string, cases: list<case> }
  record %enum { %package: string, name: string, count: u32 }
  enum option-kind { non-nesting, nesting }
  record result-record { has-ok: bool, has-err: bool }
  record symbols { exports: list<function-export>, resources: list<%resource>, records: list<%record>, %flags: list<%flags>, tuples: list<%tuple>, variants: list<%variant>, enums: list<%enum>, options: list<option-kind>, results: list<result-record> }
  init: func(app-name: string, symbols: symbols, stub-wasi: bool) -> result<_, string>;
}
```

**The rule.** A component may carry exactly one extra bare export named `exports`, and only under three conditions:
- `ostro:runtime/workflows@0.1.*` is also exported.
- Its only function is `init`.
- `init`'s structural type matches the signature above exactly.

Checking the type:
- Type exports inside the instance are ignored.
- Record field names and variant/enum case names are compared.
- Type names are not compared (they are not part of a component type).

What gets rejected:
- The same name with another shape: `export exports has an unexpected type ... (found functions [...])`.
- Any other extra export, or `exports` without the workflows export: the unchanged `export X is not allowed` message.

`guests/exports-shape-ok` has a WIT byte-identical to the real build's interface (diffed with `jco wit`). `guests/exports-shape-bad` changes `stub-wasi` to `u32`.

## ENG-2: core instances and admission

**Measured core instances per component instance** (`count_core_instances` is the static count; the weight is the calibrated count; "exact" means the pool needs `n` and fails at `n - 1`):

| Guest | static count | admission weight (calibrated) | exact vs pool |
|---|---|---|---|
| Rust (`smoke`, `det-basic`, `exports-shape-ok`) | 3 | 3 | yes |
| py-min (componentize-py 0.25.1) | 38 | 38 | yes |
| ts-min (jco QuickJS) | 11 | 13 | yes (S9: 13) |

- **Static count:** core `instantiate` entries only (from-exports bags allocate nothing). Nested components are flattened the way Wasmtime inlines them, following the component index space: nested definitions, imports, outer aliases and re-exports.
- **Calibration:** done once per `Prepared`, lazily, in the first async budgeted call (`activate`, `describe`, `instantiate_probe`, `core_weight`). It instantiates the module in a throwaway store whose `StoreLimits::instances` starts at the static count and grows until instantiation succeeds. It runs under a permit of `static + CALIBRATION_MARGIN (16)`.
  - If it cannot run, the conservative `static + 16` is used and the calibration is retried next time.
  - With the on-demand allocator the weight is the static count and the budget is unbounded.
- **Validation:** rejects a module whose static count exceeds the pool's core-instance total (it could never instantiate).
- **Budget:** `CoreBudget` is a FIFO weighted semaphore (tokio) whose capacity is `EngineConfig::effective_total_core_instances()`. That is the override if set, otherwise `total_instances × class.core_instances_per_instance()`.
- **`Runtime::activate`:** acquires the weight before `run_activation` and drops the permit after it returns. Hot-wait parking happens inside `run_activation` (03-11), so a parked activation keeps its permit. The test `exhausted_budget_evicts_with_budget_pressure` parks det-basic and observes `in_use == 3` while parked.
  - **On timeout** (the activation deadline, `activation_timeout`), `run_activation` is not called and `sink.done(Evicted { BudgetPressure })` is sent exactly once. The executor maps `BudgetPressure` to `Reactivate`.
  - **If the activation's cancel token fires while it waits**, the outcome is `Evicted { External }`.
- **`describe` / `instantiate_probe`:** take a permit too, waiting up to `BUDGET_PROBE_TIMEOUT` (5 s), and fail with `EngineError::Wasmtime("core-instance budget: ...")`.
- **Defaults are unchanged.** `class = rust` with no override gives the same pool sizing. The `wasmtime_config_debug` snapshot is unchanged (the `engine_config` test passes, and the server test compares `describe_wasmtime_config` against the default).
- **Server config:** `[engine] class = "rust|python|typescript|mixed"` and `[engine] pool_total_core_instances = N` work in TOML and through `OSTRO_ENGINE__CLASS` / `OSTRO_ENGINE__POOL_TOTAL_CORE_INSTANCES`, and round-trip through `--print-config` (an unset override is not printed). An unknown value gives `unknown engine class "go": expected one of rust, python, typescript, mixed`.

**Fuel (PF-W6).** The doc comment on `EngineConfig::activation_fuel` now quotes S9:
- Python hello: 185k / 417k fuel, which is 0.0018% / 0.0042% of the default.
- QuickJS hello: 0.78M. StarlingMonkey hello: 2.54M. A Python `describe`: about 120k.

Python is far below 10% of the default, so **there is no `[USER REVIEW]` candidate**. Defaults are not changed.

## Replay bug: abandoned steps (coordinator addition; S9's StarlingMonkey observation)

**Root cause.** A live `begin-step` reserves a seq but journals nothing until `complete` / `StepAttemptFailed`. If the guest drops the handle, or returns while the step is still open, the reservation is released (`abandon_step` / `finish`) and the journal has no trace of the step. Replaying that journal, the guest makes the same `begin-step`, `ReplayState::begin_step` compares it with the next recorded guest command, and gets `NonDeterminism`.
- Begin, then return: the next recorded command is `RunCompleted`.
- Begin, drop, then other commands: the next recorded command is the following one.

**Fix** (`replay/cursor.rs`; no journal-format, codec or WIT change):
- When `begin_step` hits that mismatch, it opens the step **unrecorded** at the mismatching event's seq. That is the seq the live run reserved, so step-local RNG matches.
- The open-step rule still applies.
- If the guest then records an outcome for it (`complete_step`, `record_attempt_failed`), the original `NonDeterminism` (same seq/expected/actual) is reported, because the live run recorded none.
- Dropping it or returning matches the live run.
- **Detection change:** a genuinely divergent `begin-step` is now reported when it records its outcome, not at begin. Step bodies cannot issue journaled commands, so running the body first has no durable effect.

Tests:
- Unit tests `abandoned_step_then_return_replays` and `abandoned_step_then_command_replays`. `step_guard` and `attempt_chain_divergence` were updated for the deferred detection.
- Integration tests `abandoned_open_step_replays` and `abandoned_dropped_step_replays` (exports-shape-ok workflows `abandon-open` with a leaked handle, `abandon-drop` with drop-then-step). Both failed with `NonDeterminism` before the fix: verified by running them against the pre-fix `cursor.rs`.

## Tests

- `cargo test -p ostro-engine --locked` (full suite) green, including:
  - lib: 93 tests;
  - `exports_shape`: 7;
  - `core_budget`: 8;
  - `validate`;
  - `engine_config` snapshot.
- `cargo test -p ostro-server --locked --test config --test engine_class` green (38 + 5).
- `cargo test -p ostro-conformance --features polyglot --locked -- --test-threads=1` green. `polyglot_engine` has 5 tests:
  - py-min and ts-min validate, describe, run `hello` to `Completed` with `[RunStarted, StepCompleted("greet"), RunCompleted]`, and replay with 0 appended;
  - the calibrated weights are 38 / 13;
  - `class = python`: 100 parked live Python instances, `in_use = 3800`;
  - default class: 100 concurrent sleeping Python runs all complete, peak 3000/3000 slots, no `AttemptFailed`.
- `cargo clippy -p ostro-engine -p ostro-server -p ostro-conformance --features ostro-conformance/polyglot --all-targets --locked -- -D warnings` clean.

## Deviations from Plan

1. **[Rule 1 - Bug] The static count underestimates QuickJS** (11 vs 13): Wasmtime's adapter modules are invisible in the binary. A wasmparser-only weight would let the pool run out while the budget still admits. **Fix:** the per-module calibration described above. `count_core_instances` stays as the validation lower bound and the `ValidatedComponent.core_instances` field. Files: runtime.rs, validate.rs.
2. **[Rule 3 - Blocking] `crates/ostro-conformance/src/engine_leg.rs`** (not in files_modified) matches `LanguageClass` exhaustively. One line was changed so `Mixed` shares the Python runtime.
3. **[Coordinator] Replay fix** in `crates/ostro-engine/src/replay/cursor.rs` and `replay/tests.rs` (not in files_modified). Its regression workflows live in the reserved `exports-shape-ok` guest, so no new guest name was needed.
4. **`validate_loaded` signature** gained `bytes: &[u8], max_core_instances: Option<u32>`. `runtime.rs` was its only caller. `Runtime::prepare` now goes through `validate_loaded` so the pool cap applies. `validate_component` keeps its signature (no cap).
5. **Server `class`** is a typed `EngineClass` enum with a custom `Deserialize` instead of a `String`. The validation error comes from deserialisation, so `resolve_and_validate` is untouched (append-only rule).

## Open issues

- **QuickJS needs 3 memories and 3 tables per instance (S9).** The pool sizes `total_memories = total_instances` and `total_tables = 2 × total_instances`, so with `class = typescript` a node holds at most about 333 QJS instances by memories. Past that, instantiation fails with "memory stripe" / table limits. The core budget does not cover memories or tables. Candidate for 09-20 or a follow-up: weigh memories and tables too, or size them per class. `LanguageClass::TypeScript::min_memory_bytes` is still the StarlingMonkey figure.
- The new metrics `ostro_core_budget_evictions_total` and `ostro_module_core_instances` are not in the observability docs yet.
- S9 asked 09-07/09-13 to decide whether a run returning with an open step should fail live. 09-07 chose replay tolerance instead, so live behaviour is unchanged. The SDKs should still always close their steps.

## Self-Check: PASSED
