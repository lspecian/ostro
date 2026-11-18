---
phase: 03-deterministic-wasm-engine-wit-0-1
plan: 07
subsystem: engine
tags: [wasmtime, wit, host-adapter, replay, linker, fuel, guests]
requires:
  - "03-02: journal::{command_hash, command_args, encode_result, decode_result}"
  - "03-03: OstroEngine, configure_store, HasLimits, OstroLimiter"
  - "03-04: host::wasi::add_to_linker, validate_component"
  - "03-05: ReplayState (command/patched/begin_step/complete_step/abandon_step/wait/finish), token::mint"
provides:
  - "ostro_engine::host::state::{HostState, HostInit, RunReturn, build_linker, run_return_from_wit, BoundedLog, BoundedBuf, LogLine}"
  - "HostState::{new, take_pending, deliver_inputs, flush, record_terminal, stderr_tail, journal, latest_status, logs}"
  - "Every ostro:runtime@0.1.0 import implemented (context, journal + resource step, effects, log)"
  - "guests det-basic (shared workflow.rs + DIVERGE modes) and wit-surface (12 named workflows)"
affects: [03-08, 03-09, 03-10, 03-11, 03-12]
tech-stack:
  added: ["guests: sha2 =0.10.9 (wit-surface digests)"]
  patterns:
    - "Store-aware imports via linker.allow_shadowing + func_wrap over the bindgen-generated functions (bindgen `store` flag needs component-model-async)"
    - "Thin imports: every Host method is sync work over ReplayState; only HostState::flush awaits"
key-files:
  created:
    - crates/ostro-engine/tests/host_adapter.rs
    - guests/det-basic/{Cargo.toml,src/lib.rs,src/workflow.rs}
    - guests/wit-surface/{Cargo.toml,src/lib.rs}
  modified:
    - crates/ostro-engine/src/host/state.rs
    - crates/ostro-engine/src/host/context.rs
    - crates/ostro-engine/src/host/log.rs
    - crates/ostro-engine/src/host/journal.rs
    - crates/ostro-engine/src/host/effects.rs
    - guests/Cargo.toml
key-decisions:
  - "Per-step fuel: begin-step and [method]step.complete are shadowed in build_linker with sync func_wrap closures that get StoreContextMut (bindgen's per-function `store` flag requires the component-model-async Accessor, not enabled)"
  - "A budgeted step dropped without completion charges its whole budget (conservative); outer fuel is restored at the next begin-step"
  - "record_terminal rejects a replayed terminal of a different kind (completed/failed/continued-as-new) as NonDeterminism; output differences never diverge"
  - "cancel-op and wait([]) are protocol violations (cancel-op needs an issued op-scheduling seq)"
requirements-completed: [ENG-04]
duration: 40min
completed: 2026-11-18
---

# Phase 3 Plan 07: Host Adapter + Production Linker + Guests Summary

Every `ostro:runtime@0.1.0` import now runs as a thin adapter over the 03-05 replay core. `build_linker` puts the ostro bindings, the deterministic WASI shim and the fuel-aware step functions into a single production linker. The new det-basic and wit-surface guests validate against that linker and run live, then evict, resume and replay, with byte-identical output and zero appends on replay.

## What was built

- **`host/state.rs`**
  - `HostInit` / `HostState` own the `ReplayState` and also hold:
    - a bounded stdout and stderr buffer (64 KiB each; the excess is counted);
    - a bounded log (1000 lines / 1 MiB);
    - `latest_status`;
    - the precomputed insecure seed;
    - step and fuel bookkeeping.
  - `flush()` appends pending events through `ActivationSink`, with `from_seq` set to the first pending seq. `stderr_tail()` returns the last 4 KiB.
  - `record_terminal()` calls `finish()` and then issues the kind-only `Terminal` command.
  - `run_return_from_wit()` converts the guest's `run` result into journal mirror types (03-08 uses it).
  - Trait impls:
    - `GuestEnv`: wall time is logical now; monotonic time is now − accepted_at; randomness comes from the run's ChaCha streams; stdio is dropped while replaying.
    - `HasLimits`.
  - `trap()` / `violation()` map replay errors to typed traps: `NonDeterminism`, or `ProtocolViolation` (with a "bad journal:" prefix for journal errors).
- **`host/context.rs`**
  - info, now, random-u64, random-bytes (≤ 65,536), uuid-v7, is-replaying and continue-as-new-suggested journal nothing.
  - `patched` hashes `CommandArgs::Patch`.
  - `set-status` (≤ 65,536) is journaled with a kind-only hash, and `latest_status` is always the journaled JSON (on replay, the recorded JSON).
- **`host/log.rs`**: nothing is logged while replaying. Live lines are bounded, stored in `logs`, and emitted as `tracing` events (target `ostro::guest`) with run and attempt fields.
- **`host/journal.rs`**
  - `begin_step_impl` / `complete_step_impl` take the store's fuel.
  - A replayed step decodes `Result<Payload, Failure>` from the recorded bytes.
  - A live step pushes a `StepRep` and applies `min(opts.fuel, remaining)`. On completion it records `fuel_used` and restores the outer fuel.
  - Completing a step twice is a protocol violation, and `retry-now` is never returned.
  - `drop` abandons an open step and increments `ostro_step_abandoned_total`.
- **`host/effects.rs`**
  - All 12 effects imports:
    - op id = the seq of the scheduling event;
    - `sleep` fire_at = logical now + ms (computed before the command);
    - an await token is minted when live; on replay the recorded token is returned;
    - `cancellation()` returns `CANCELLATION_OP`.
  - `wait` builds completions only from journaled inputs. Effect and child outcomes are decoded from their exact recorded bytes. An unsatisfied wait traps with `Evict{WaitUnsatisfied}`.
  - A `convert` module holds the WIT ⇄ mirror conversions.
- **Guests**
  - `det-basic`: `lib.rs` = `const DIVERGE: bool = false; include!("workflow.rs")`. All five divergence modes are implemented now, so 03-09 only adds a crate.
  - `wit-surface`: the workflows surface, echo-bytes, cancel-cleanup, can, token, status, non-retryable, step-fail, nested-step, info, panic and exit.

## Verification

- `cargo test -p ostro-engine --test host_adapter`: 7 pass.
  - `host_validate_real_linker`: det-basic, wit-surface and smoke validate against `build_linker`; wasi-forbidden is still rejected.
  - `host_live_then_replay_det_basic`:
    - Activation 1: pending = [StepCompleted, TimerStarted], hashes = `command_hash(1, ..)`, `fuel_used > 0` (which proves the shadowed `complete` ran), and fire_at = s1 ts + 5 s.
    - Activation 2: 8 live events, then `RunCompleted`.
    - Activation 3: identical output and zero appends. A terminal of the wrong kind is reported as nondeterminism.
  - `host_seed_controls_native_randomness`: the same seed gives identical output, and a different seed changes the HashMap order and rand values. The wall clock equals accepted_at.
  - `host_replay_exact_bytes`: 256 non-UTF-8 bytes replay byte-identically, and the `RecordedResult` re-encodes identically.
  - `host_nested_step_protocol`: `ProtocolViolation("journaled command inside an open step")`.
  - `host_surface_journals_every_command`: 12 events, one of each kind as specified. The token parses to (partition, run, op = seq) and verifies with the key. The secret template header value is journaled verbatim. Logs are captured.
  - `host_surface_resume_and_replay_effects` (added, see deviations): completions arrive in journal order (sql, http, object). The HTTP header order and body sha come out as recorded, and a failure decodes as `timeout`. A full replay is byte-identical, with no appends and no logs.
- `cargo test -p ostro-engine`: everything passes (lib 41, cache 12, engine_config 3, host_adapter 7, pool 6 + 1 ignored, scaffold 1, validate 7, wasi_shim 8).
- `cargo clippy -p ostro-engine --all-targets -- -D warnings` and `cargo build --workspace --locked` are clean. Guest clippy (wasm32-wasip2, `-D warnings`) is clean for det-basic and wit-surface, and `cargo fmt --check` is clean.

## Deviations from Plan

### Auto-fixed Issues / design choices

**1. [Rule 3 - Blocking] Per-step fuel via shadowed `func_wrap`, not the bindgen `store` flag**
- In Wasmtime 50, a `store`-flagged import takes `Access`/`Accessor`, which comes from the component-model-async machinery, and Ostro does not enable it. `build_linker` therefore re-opens `ostro:runtime/journal@0.1.0` with `allow_shadowing(true)` and defines `begin-step` and `[method]step.complete` with sync `func_wrap` closures that receive `StoreContextMut<HostState>`. The plan said `func_wrap_async`, but nothing awaits, so sync matches the thin-import rule. The bindgen trait methods remain as store-less fallbacks.
- **Commit:** ae66fcf / 3144078

**2. [Rule 2 - Correctness] Dropped budgeted step charges the full budget**
- `HostStep::drop` has no store access, so when a step that had a fuel budget is dropped open, the whole budget is charged and outer fuel is restored at the next `begin-step`. This is conservative and deterministic, and it closes a fuel-refund hole.
- **Commit:** 3144078

**3. [Rule 2 - Correctness] Terminal kind check, cancel-op validation, empty wait**
- `record_terminal` returns `NonDeterminism` if the replayed terminal is a different kind. The family/hash match alone would accept RunCompleted vs RunFailed, because the hash covers only the kind.
- `cancel-op` on an op that was never issued as op-scheduling is a protocol violation.
- `wait([], all)` is rejected by the adapter, as the plan specifies. The core itself allows it.
- **Commit:** ae66fcf / 3144078

**4. [Test coverage] Added `host_surface_resume_and_replay_effects`**
- The plan's tests covered exact bytes only for steps. This test also proves ENG-04 for effect completions end to end, which is why ENG-04 is marked complete.
- **Commit:** f4d5dab

**5. Commit granularity**
- Tasks 1 and 2 are separate commits, but `build_linker` (Task 1) needs the journal/effects `Host` impls (Task 2) to compile. Only the pair builds.

### Concurrency notes
- In the middle of the plan, the main tree failed to build because a Phase 2 executor had in-progress changes in `ostro-types/src/command.rs`. I verified Tasks 1 to 3 in a detached scratch worktree (`wt-0307`, now removed). After those changes landed, the main tree built and `--test host_adapter` passed there too.
- `guests/Cargo.toml` gained `sha2 = "=0.10.9"` (guests workspace only). guests/Cargo.lock is gitignored.

## Known Stubs

None. `flush` is not exercised yet because no sink is wired; 03-08's FakeSink drives it.

## Requirements

- ENG-04: complete. Exact recorded bytes reach the guest for both steps and effect completions.
- ENG-03, WIT-02, WIT-03, WIT-04, WIT-05: the host paths now exist, but closing them needs 03-08 (DLQ / activation driver: nondeterminism into the DLQ, continue-as-new run start) and 03-12 (behaviour tests), plus Phase 4 for API-readable status and token completion. Left Pending.

## Commits

- ae66fcf feat(03-07): HostState, GuestEnv/HasLimits, context + log imports, build_linker
- 3144078 feat(03-07): journal step resource and effects imports over the replay core
- ad0566d feat(03-07): det-basic and wit-surface guests + host adapter integration tests
- f4d5dab test(03-07): resume wit-surface with effect completions and replay byte-identically

## Self-Check: PASSED

All listed files exist; commits ae66fcf, 3144078, ad0566d, f4d5dab are in git log.
