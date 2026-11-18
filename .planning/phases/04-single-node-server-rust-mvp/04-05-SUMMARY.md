---
phase: 04-single-node-server-rust-mvp
plan: 05
subsystem: sdk
tags: [rust-sdk, wit-bindgen, wasm32-wasip2, durable-steps, replay]
requires:
  - phase: 03
    provides: WIT ostro:runtime@0.1.0 (wit/ostro-runtime-0.1.0/runtime.wit), resource-step + retry-now completion-ack
provides:
  - sdks/rust cargo workspace with the ostro-sdk runtime crate
  - run_step<T> typed JSON step runner with replay + retry-now loop
  - Ctx (run info, deterministic now/random/uuid, patched, set_status, log, ops)
  - Op<T> handles + join!/select! macros
  - Workflow trait + explicit export! registration (describe/run dispatch)
affects: [04-06 macros + trybuild + SDK test guests, 04-20 e2e, Phase 6 effects bridging]
tech-stack:
  added: [wit-bindgen =0.63.0 (SDK), serde =1.0.229, serde_json =1.0.154]
  patterns:
    - "wit-bindgen pub_export_macro + default_bindings_module; export! expands to $crate::bindings::__export_world!(T with_types_in $crate::bindings)"
    - "StepHandle trait seam over the WIT step resource so the step state machine is host-testable"
    - "macro_rules hygiene accumulator (@acc) binds each join!/select! argument once, in order"
key-files:
  created:
    - sdks/rust/Cargo.toml
    - sdks/rust/README.md
    - sdks/rust/ostro-sdk/Cargo.toml
    - sdks/rust/ostro-sdk/src/lib.rs
    - sdks/rust/ostro-sdk/src/bindings.rs
    - sdks/rust/ostro-sdk/src/ctx.rs
    - sdks/rust/ostro-sdk/src/error.rs
    - sdks/rust/ostro-sdk/src/ops.rs
    - sdks/rust/ostro-sdk/src/step.rs
    - sdks/rust/ostro-sdk/src/registry.rs
  modified:
    - .gitignore
key-decisions:
  - "run_step returns the value decoded from the journaled JSON bytes (not the live T), so live execution and replay return identical values"
  - "Empty input/recorded payload decodes as JSON null (unit/Option inputs need no body)"
  - "StepError kinds 'timeout'/'cancelled' (non-retryable) are reserved and map to/from WIT failure::timeout/cancelled"
  - "sdks/rust has no [workspace.dependencies] (cargo warns on unused ones since members use explicit pins); versions documented in a comment"
  - "Host-side unit tests work: wit-bindgen 0.63 emits unreachable!() import stubs on non-wasm32 targets, so the SDK compiles and tests natively as long as tests never call imports (relevant for 04-06 trybuild)"
patterns-established:
  - "select! borrows its op arguments (losers can be waited on/cancelled); returns (index, Completion) decoded via Op::decode"
requirements-completed: []
duration: 25min
completed: 2026-11-18
---

# Phase 4 Plan 05: Rust SDK runtime crate Summary

**`ostro-sdk` in a separate `sdks/rust` workspace: wit-bindgen 0.63 bindings with a public export macro, a typed JSON step runner (replay without running the body, retry-now loop), deterministic `Ctx`, `Op<T>` handles with `join!`/`select!`, and explicit `export!` registration — builds warning-free for wasm32-wasip2 and has 21 host-side unit tests.**

## Performance

- **Duration:** ~25 min
- **Tasks:** 2 (Task 2 TDD: RED + GREEN commits)
- **Files:** 11 (10 created, 1 modified)

## Accomplishments

- `sdks/rust` is its own cargo workspace (resolver 3, `members = ["ostro-sdk"]`). The root workspace is untouched (`cargo metadata --locked` at root does not list ostro-sdk). The root `exclude` does not list `sdks` yet (04-02 adds it); that is not needed because cargo finds `sdks/rust/Cargo.toml` first.
- `bindings.rs`: `generate!` with `pub_export_macro: true`, `export_macro_name: "__export_world"`, `default_bindings_module: "ostro_sdk::bindings"`, path `../../../wit/ostro-runtime-0.1.0`.
- `run_step<T>(name, &StepOptions, FnMut body)` behaves as specified. `StepOptions::builder()` sets retry, timeout and fuel and converts to WIT `step-options`.
- `Ctx` covers info (typed `RunInfo` with a `SystemTime` deadline), now, random_u64, random_bytes, uuid_v7, is_replaying, patched, `set_status<T: Serialize>`, `log`/`log_with` (suppressed on replay), `info!`/`warn!` macros, continue_as_new_suggested, and the ops `sleep`, `sleep_until`, `http_fetch`, `wait_signal<T>`, `cancellation` and `cancel`.
- `Workflow` trait and `export!(A, B, ...)`: `describe()` lists the metadata and `run(name, input)` dispatches through a `(name, fn)` table. An unknown name returns a non-retryable `unknown_workflow` failure. Bad input JSON returns a non-retryable `invalid_json` failure.
- End-to-end check, done in the scratchpad and not committed: the README example plus a second workflow using `join!`/`select!` built as a downstream `cdylib` with zero warnings. The resulting component imports `ostro:runtime/{types,context,journal,effects,log}@0.1.0` plus 15 `wasi:*@0.2.12` interfaces (no `wasi:random` when HashMap is unused) and exports `ostro:runtime/workflows@0.1.0`. The downstream crate does **not** need a direct `wit-bindgen` dependency.

## Task Commits

1. **Task 1: SDK workspace, bindings, Ctx / ops / errors**: `45dd880` (feat)
2. **Task 2 RED: failing step-runner / registry tests**: `7bb65a3` (test)
3. **Task 2 GREEN: step runner, Workflow trait, export!**: `a675989` (feat)

## Decisions Made

See frontmatter key-decisions. Most notable: the live step result is decoded from the journaled bytes, so live and replay return the same value even for lossy serde types such as floats.

## Deviations from Plan

**1. [Rule 3 - Blocking] step.rs created in Task 1 (codec helpers only)**
- ops.rs (signal decoding) needs the JSON payload codec, so `step.rs` was committed in Task 1 with only the helpers. Task 2 then added the runner. Temporary `#[allow(dead_code)]` markers were removed in Task 2.

**2. [Rule 3 - Blocking] No `[workspace.dependencies]` in sdks/rust/Cargo.toml**
- The plan said to declare them, but members must use explicit versions and not inherit them. cargo 1.100 warns about unused workspace dependencies (`cargo::unused_workspace_dependencies`), so the table was replaced by a comment listing the pins.

**3. [Rule 2] Added `wait_signal<T>` and `cancellation()` ops beyond the plan's list.** These are small additions over the same `Op` decode path, and `select!` needs a realistic second op kind.

## Findings for 04-06

- On native targets, wit-bindgen 0.63 generates `unsafe extern "C" fn ... { unreachable!() }` import stubs. The SDK therefore compiles, links and runs unit tests on the host. Only calling an import panics. trybuild UI tests on the host target should work.
- The `__export_world` macro takes `$ty:ident`, so `export!` defines a local `struct __OstroComponent`. The macro can be called only once per crate, which is correct for one component.
- The SDK has not yet been run on the engine. 04-06 owns the SDK test guests validated on the engine.

## Issues Encountered

- The scratchpad guest build needs `cargo +1.100.0` outside the repo, because the default toolchain has no wasm32-wasip2 target.

## Next Phase Readiness

- 04-06 can add `ostro-sdk-macros` to `members` and generate `Workflow` impls plus `run_step` calls.
- SDK-01 is **not** marked complete. It also needs the `#[workflow]`/`#[step]` macros (04-06) and the e2e run (04-20).

## Self-Check: PASSED
