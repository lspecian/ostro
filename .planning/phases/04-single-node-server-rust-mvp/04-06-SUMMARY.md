---
phase: 04-single-node-server-rust-mvp
plan: 06
subsystem: sdk
tags: [rust-sdk, proc-macro, trybuild, wasm32-wasip2, guests, determinism]
requires:
  - phase: 04
    provides: 04-05 ostro-sdk runtime (Workflow trait, run_step, StepOptions, export!, join!/select!)
  - phase: 03
    provides: engine testkit (drive_to_completion, replay_journal), Runtime::prepare_cached/describe
provides:
  - sdks/rust/ostro-sdk-macros (#[workflow], #[step]) re-exported from ostro_sdk (root + prelude)
  - trybuild UI suite (1 pass, 2 compile-fail with stderr snapshots) + parser unit tests
  - guests/sdk-basic (sdk-basic v1 + sdk-echo), guests/sdk-basic-v2 (sdk-basic v2), guests/sdk-retry, guests/sdk-native
  - crates/ostro-engine/tests/sdk_guests.rs (validate + describe + testkit completion + replay)
affects: [04-07 retry e2e, 04-20 server e2e, activation/version tests, WF-07 verification]
tech-stack:
  added: [syn =2.0.119, quote =1.0.47, proc-macro2 =1.0.107, trybuild =1.0.125 (dev)]
  patterns:
    - "#[workflow] keeps the fn and emits `<UpperCamelFnName>Workflow` unit struct implementing ostro_sdk::Workflow; run() calls the fn and maps E via Into<WorkflowError>"
    - "#[step] rewrites the fn body to run_step(name, &StepOptions{..}, || -> Ret { body }); options are compile-time literals"
    - "Generated code uses ::ostro_sdk / ::core / ::std paths only"
key-files:
  created:
    - sdks/rust/ostro-sdk-macros/Cargo.toml
    - sdks/rust/ostro-sdk-macros/src/lib.rs
    - sdks/rust/ostro-sdk-macros/tests/trybuild.rs
    - sdks/rust/ostro-sdk-macros/tests/ui/pass_basic.rs
    - sdks/rust/ostro-sdk-macros/tests/ui/fail_missing_name.rs
    - sdks/rust/ostro-sdk-macros/tests/ui/fail_missing_name.stderr
    - sdks/rust/ostro-sdk-macros/tests/ui/fail_bad_retry.rs
    - sdks/rust/ostro-sdk-macros/tests/ui/fail_bad_retry.stderr
    - guests/sdk-basic/{Cargo.toml,src/lib.rs}
    - guests/sdk-basic-v2/{Cargo.toml,src/lib.rs}
    - guests/sdk-retry/{Cargo.toml,src/lib.rs}
    - guests/sdk-native/{Cargo.toml,src/lib.rs}
    - crates/ostro-engine/tests/sdk_guests.rs
  modified:
    - sdks/rust/Cargo.toml
    - sdks/rust/ostro-sdk/Cargo.toml
    - sdks/rust/ostro-sdk/src/lib.rs
key-decisions:
  - "Workflow struct name = fn name in UpperCamelCase + `Workflow` suffix (fn order -> OrderWorkflow); the suffix avoids clashing with the input type (fn order(input: Order))"
  - "Steps are called without ctx: `charge(&order)`; the body becomes an FnMut closure (may borrow params, must not move out of them)"
  - "Partial retry(...) defaults: max_attempts 3, initial 1s, max 60s, backoff 2.0, non_retryable []; no retry(...) = host default policy (None)"
  - "Durations: integer + ms|s|m|h string literals, validated at compile time with the error spanned on the literal"
  - "Guests pin serde/serde_json explicitly in their own manifests (serde derive needs a direct serde dep); guests/Cargo.toml untouched (members glob)"
  - "sdk-retry step names: before, flaky (exhaust), recover (recover_hot), fatal (non_retryable), slow (long_backoff), flaky_slow (exhaust_slow)"
requirements-completed: []
duration: 30min
completed: 2026-11-18
---

# Phase 4 Plan 06: #[workflow]/#[step] macros + SDK test guests Summary

**`ostro-sdk-macros` provides `#[workflow(name, version, queue, run_timeout)]` and `#[step(name, retry(...), timeout, fuel)]`, both trybuild-tested on the host with clear compile errors. Four SDK-built guests (`sdk-basic`, `sdk-basic-v2`, `sdk-retry`, `sdk-native`) build with plain cargo for wasm32-wasip2, pass production linker validation, describe themselves, and run on the engine testkit. `sdk-native` (std time, HashMap and rand) replays byte-identically.**

## Performance

- **Duration:** ~30 min
- **Tasks:** 2 (Task 1 TDD: RED + GREEN commits)
- **Files:** 23 (20 created, 3 modified)

## Accomplishments

- **Macros.** `#[workflow]` checks the signature: no async, no generics, exactly `(ctx, input)`, and a return type of `Result<Out, E>`. It then emits a `Workflow` impl with `Input`/`Output` taken from the signature. `#[step]` turns the body into a `run_step` call with literal `StepOptions`. Every misuse produces a spanned error: missing name or version gives `#[workflow] requires name = "..." and version = N`, and there are errors for unknown or duplicate keys, bad durations (pointing at the literal), negative fuel, backoff < 1.0, and a non-array `non_retryable`.
- **trybuild.**
  - `pass_basic` compiles and runs on the host. It covers the full retry syntax, `name =` override, a bare `#[step]`, the queue/run_timeout consts, `export!` with two generated structs, and an error type other than `WorkflowError`.
  - The two compile-fail suites have 11 cases between them.
  - 7 parser/expansion unit tests.
  - The `sdks/rust` workspace still builds for wasm32-wasip2, and clippy is clean.
- **Guests.**
  - `sdk-basic`: two steps, `join!` over two sleeps, `select!` over 5 ms vs 1 h (fails the run if the wrong op wins), `set_status`, and a second workflow `sdk-echo` in the same component.
  - `sdk-basic-v2`: `sdk-basic` v2; the output adds `"version":2`.
  - `sdk-retry`: five modes plus a `before` replay probe; see the module docs.
  - `sdk-native`: SystemTime, a 50-key HashMap order and `rand::random`, sampled both outside a step and inside one.
- **`tests/sdk_guests.rs`.** All 7 tests pass:
  - describe() lists for all 4 modules.
  - sdk-basic output is `{"greeting":"hello, ostro","sum":55,"steps":2}`, with status `{"stage":"done"}`, 2 StepCompleted and 4 TimerStarted, and it replays with zero appends.
  - sdk-echo dispatches by name.
  - sdk-basic v2 output is checked.
  - Bad input fails with a non-retryable `invalid_json`.
  - sdk-retry `non_retryable` fails with `fatal` after one attempt, and an unknown mode fails with `bad_mode`.
  - sdk-native replay is identical, and a second independent run with the same seed gives the same output.
- `wit_surface` (17 tests) still passes with the new guests in the `guests/*` workspace.

## Task Commits

1. **Task 1 RED: failing trybuild UI tests + macro crate stub:** `2c8e9ea` (test)
2. **Task 1 GREEN: #[workflow]/#[step] implementation + stderr snapshots:** `9b44362` (feat)
3. **Task 2: SDK guests + engine suite:** `985bf79` (feat)

## Decisions Made

See frontmatter key-decisions.

## Deviations from Plan

**1. [Rule 3 - Blocking] Engine tests run in a temporary git worktree of HEAD.**
- Plan 04-07 was editing `crates/ostro-engine/src/host/*` at the same time, and its uncommitted changes left the engine lib uncompilable (non-Send future in `retry_sleep`).
- `cargo test -p ostro-engine --test sdk_guests --test wit_surface --locked` was therefore run in a detached worktree of HEAD (which includes the 04-07 RED commit and this plan's files). Both suites passed. The worktree was removed afterwards.
- No engine source was touched.

**2. Extra test coverage beyond the plan:** sdk-echo dispatch, the v2 output, invalid input JSON, and sdk-retry `non_retryable`/bad-mode failures on the testkit.

## Issues Encountered

- The scratchpad directory is shared with other agents. An existing `wt/` directory briefly received copies of the guest dirs; they were removed right away, and nothing else in it was changed.

## Next Phase Readiness

- 04-07 / server e2e can use `guest_wasm("sdk-retry")`. Count attempts from `StepAttemptFailed` in the journal; `before` must have exactly one StepCompleted.
- Activation/version tests can deploy `sdk-basic` (v1) together with `sdk-basic-v2`.
- Requirements SDK-01, WF-01 and WF-07 are **not** marked complete. SDK-01 needs the server e2e (04-20), WF-01 needs deploy/start over the API, and WF-07 also covers the Python/TS SDKs.

## Self-Check: PASSED
