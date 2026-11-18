---
phase: 03-deterministic-wasm-engine-wit-0-1
plan: 01
subsystem: engine
tags: [wit, wasmtime, bindgen, wit-bindgen, journal, contracts, scaffold]
requires: []
provides:
  - "wit/ostro-runtime-0.1.0/runtime.wit (ostro:runtime@0.1.0 world `workflow`)"
  - "guests/ workspace + smoke guest + tests/common guest_wasm() helper"
  - "ostro-engine module skeleton (all Phase 3 modules declared once in lib.rs)"
  - "ostro_engine::{error, sink, env, host::bindings} contract types"
  - "ostro_types::journal event contract types and kind codes"
affects: [03-02, 03-03, 03-04, 03-05, 03-06, 03-07, 03-08, phase-2-ostro-types, phase-4]
tech-stack:
  added: [wit-bindgen 0.63.0 (guests), serde, postcard, sha2 0.11, hmac 0.13, chacha20 =0.10.2, async-trait, thiserror, tokio-util, tracing, metrics, proptest 1.12, insta 1, cargo-nextest 0.9.152]
  patterns:
    - "Separate guests/ cargo workspace built by a nested cargo from integration tests (OnceLock, target/guests)"
    - "`use <dep> as _;` anchors keep cargo's per-crate unused_dependencies lint quiet for deps wired ahead of their consuming plan"
key-files:
  created:
    - wit/ostro-runtime-0.1.0/runtime.wit
    - guests/Cargo.toml
    - guests/smoke/Cargo.toml
    - guests/smoke/src/lib.rs
    - .config/nextest.toml
    - crates/ostro-engine/src/{config,engine,env,error,limits,cache,validate,activation,runtime,sink,testkit}.rs
    - crates/ostro-engine/src/replay/mod.rs
    - crates/ostro-engine/src/host/{mod,bindings,state,context,journal,effects,log}.rs
    - crates/ostro-engine/src/host/wasi/mod.rs
    - crates/ostro-engine/tests/common/mod.rs
    - crates/ostro-engine/tests/scaffold.rs
    - crates/ostro-types/src/journal.rs
  modified:
    - Cargo.toml
    - Cargo.lock
    - .gitignore
    - crates/ostro-engine/Cargo.toml
    - crates/ostro-engine/src/lib.rs
    - crates/ostro-types/Cargo.toml
    - crates/ostro-types/src/lib.rs
decisions:
  - "guests/Cargo.lock is gitignored; guest deps pinned exactly in guests/Cargo.toml"
  - "Cargo (1.100 toolchain) has a per-crate `unused_dependencies` lint; deps wired early are anchored with `use x as _;` (engine lib.rs: chacha20, hmac, metrics, tracing; ostro-types journal.rs: postcard, sha2)"
  - "Wasmtime 50 bindgen record types do not derive PartialEq; tests compare fields"
metrics:
  duration: 25min
  completed: 2026-11-18
---

# Phase 3 Plan 01: Contracts + Scaffold Summary

This plan adds the full `ostro:runtime@0.1.0` WIT world, and it compiles on both sides. The host uses Wasmtime 50.0.2 `bindgen!` with async|trappable imports and a `StepRep` resource. The guest uses wit-bindgen 0.63 on wasm32-wasip2. The plan also adds the ostro-engine module skeleton, the error/sink/env contracts, and the `ostro_types::journal` event types with pinned kind codes. Wave-2 plans can now work on separate files in parallel.

## What was built

- **WIT 0.1** (`wit/ostro-runtime-0.1.0/runtime.wit`) has these interfaces: types, context (including set-status and continue-as-new-suggested), journal (`resource step`, `step-start`, `completion-ack`), effects (http/sql/object/timers/signals/children/await tokens/`cancellation`/`cancel-op`/`wait` and every `op-output` case), log, and workflows (`run-result` with `continue-as-new`). The `failure` type has `non-retryable`, `timeout` and `cancelled` cases. A header comment records the Phase 5 freeze rule.
- **guests/ workspace** is excluded from the root workspace. Its release profile is opt-level s, LTO and strip. The `smoke` guest echoes its input through `run("smoke")` and calls no imports. `smoke.wasm` is 51.8 KB.
- **`tests/common::guest_wasm(name)`** builds guests/ once per process. It runs a nested `$CARGO build --target wasm32-wasip2 --target-dir target/guests` with `RUSTFLAGS`, `CARGO_ENCODED_RUSTFLAGS`, `CARGO_BUILD_TARGET`, `CARGO_TARGET_DIR` and `RUSTC_WRAPPER` removed. `test_engine()` wraps `config::engine_config()`.
- **ostro-engine skeleton:** lib.rs declares all 12 modules plus the testkit (behind a feature flag). Each stub file names the plan that owns it. `engine_config()` and its test moved unchanged into `config.rs`.
- **Contracts:**
  - `error.rs`: `EvictReason`, `Evict`, `NonDeterminism`, `GuestExit`, `MemoryExceeded`, `ProtocolViolation`, `Fenced`. `AttemptFailure` has `reason_code`, `retryable` and `dlq_reason`; `EngineError` is also here.
  - `sink.rs`: `RunRef`, `Activate`, `ActivationOutcome`, `AppendError`, and the async `ActivationSink`.
  - `env.rs`: `Clock`, `SystemClock`, `RandomStream`, `StdStream`, `GuestEnv`.
- **`ostro_types::journal`** contains only types:
  - Constants, `OpId` / `CANCELLATION_OP`, `RngSeed`, `CommandHash`.
  - Mirrors of the WIT types.
  - `RecordedResult`, `CommandMeta`, `CommandKind`.
  - `EventKind` codes: 1, 16–28 and 64–69.
  - Payload structs for every event, then `EventBody` (`kind`, `is_guest_command`, `command_meta`, `completes_op`, `summary`), `Event` and `CommandArgs`.
  - Unit tests pin every kind code.
- **nextest:** `.config/nextest.toml` sets a 60 s slow-timeout (terminate after 5 periods) in the default profile and adds a `ci` profile. cargo-nextest 0.9.152 is installed.
- **Dev profile:** opt-level 3 for cranelift-codegen, regalloc2, wasmtime-internal-cranelift and wasmparser.

## Verification

- `cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml --target-dir target/guests` builds `smoke.wasm`. smoke is not part of the root workspace metadata.
- `cargo test -p ostro-engine -p ostro-types` passes: 2 unit tests, the scaffold integration test, and 6 ostro-types tests.
- `cargo clippy -p ostro-types -p ostro-engine --all-targets -- -D warnings` is clean, and so is `cargo build` of both crates.
- `cargo tree -i digest@0.11.3` shows hmac 0.13.0 and sha2 0.11.1 on one digest. Wasmtime's sha2 0.10 duplicate is accepted.
- I ran `bash scripts/ci-local.sh --no-musl` in a clean worktree at 5ebd743, so Phase 2's uncommitted work was not included.
  - **Passed:** toolchain, fmt, clippy -D warnings, warning-free workspace build, cargo deny, deny fixtures, forbidden crates, workflow validation.
  - **Failed (both from Phase 2's TDD RED commit 06344f6, not this plan):**
    - `cargo test`: the new ostro-wal `dir_fsync`/`poison` tests reference APIs that don't exist yet. That is intentional RED.
    - `check-vendored.sh`: `vendor/ostro-wal/tests/{dir_fsync,poison}.rs` have no VENDORED.md rows yet. Phase 2 plan 02-02 adds them.
  - `cargo test --workspace --locked --exclude ostro-wal` passes in that worktree. The scaffold test passed there with a cold nested guest build.
- `grep -c "pub mod" crates/ostro-engine/src/lib.rs` = 13 (12 modules plus testkit).

## Self-Check: PASSED

All key files exist, and commits ec4bef3, 6ae0f63 and 5ebd743 are present.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Cargo's per-crate `unused_dependencies` lint**
- **Found during:** Task 2
- **Issue:** The plan only expected the unused-*workspace*-dependency lint. This toolchain also warns about member deps that a crate doesn't use yet (chacha20, hmac, metrics, tracing in ostro-engine; postcard, sha2 in ostro-types). That breaks ci-local's warning-free build.
- **Fix:** Added `use <dep> as _;` anchors with comments: in ostro-engine `lib.rs` (they stay harmless after later plans use the deps), and in the Phase 3-owned `journal.rs` for postcard/sha2. Plan 03-02 can drop the journal.rs anchors once its codec and hash use the crates.
- **Commits:** 6ae0f63, 5ebd743

**2. [Rule 1 - Bug] Bindgen `Payload` has no PartialEq**
- **Found during:** Task 2 (scaffold test)
- **Fix:** The scaffold test compares `content_type` and `data` field by field.
- **Commit:** 5ebd743

**3. Commit order: Task 3 before Task 2**
- `sink.rs` (Task 2) imports `ostro_types::journal` (Task 3), so the journal types and workspace pins went in first (6ae0f63) and the engine skeleton second (5ebd743). The intermediate commit 6ae0f63 builds, but it shows unused-dependency warnings for pins that 5ebd743 then uses.

### Concurrency notes
- Phase 2 commit 06344f6 added `fail` to `vendor/ostro-wal/Cargo.toml` but did not commit Cargo.lock. My Cargo.lock commit (6ae0f63) includes the regenerated `fail` entry so that `--locked` works.
- Phase 2 had uncommitted WIP in `vendor/ostro-wal` during this plan. I did not touch or stage it.

## Known Stubs

The stub module files are intentional. Each contains one `//!` line that names the plan that owns it:
- 03-03: engine, limits, config
- 03-04: host/wasi, validate
- 03-05: replay
- 03-06: cache
- 03-07: host/{state, context, journal, effects, log}
- 03-08: activation, runtime, testkit

## Commits

- ec4bef3 feat(03-01): WIT 0.1 contract, guests workspace and smoke guest
- 6ae0f63 feat(03-01): ostro-types journal contract types and Phase 3 workspace pins
- 5ebd743 feat(03-01): ostro-engine module skeleton, contract types and host bindings
