---
phase: 03-deterministic-wasm-engine-wit-0-1
plan: 10
subsystem: engine
tags: [wasmtime, sandbox, fuel, epoch, memory-limit, stack-overflow, fiber-stack, isolation]
requires:
  - "03-08: Runtime, drive_to_completion, FakeClock, classify reason codes"
  - "03-07: per-step fuel (func_wrap shadowing), host import caps"
  - "03-03: EngineConfig (async_stack_size 4 MiB, max_wasm_stack 512 KiB), limiter, node budget"
provides:
  - "guests/sandbox: loop, bomb, deep, deep-call (depth ‖ selector), step-fuel workflows"
  - "crates/ostro-engine/tests/sandbox.rs: ENG-05 tests incl. sandbox_isolation"
  - "crates/ostro-engine/tests/stack_depth.rs: ENG-06 binary-search depth test + subprocess negative control"
affects: [03-11, phase-04]
tech-stack:
  added: []
  patterns:
    - "Process-crash hazards are tested by re-executing the test binary (current_exe + --exact child --ignored) and inspecting ExitStatusExt::signal()"
    - "Guests that probe the native Wasm stack need a shadow stack larger than max_wasm_stack (build.rs -zstack-size)"
key-files:
  created:
    - guests/sandbox/Cargo.toml
    - guests/sandbox/build.rs
    - guests/sandbox/src/lib.rs
    - crates/ostro-engine/tests/sandbox.rs
    - crates/ostro-engine/tests/stack_depth.rs
  modified: []
key-decisions:
  - "sandbox guest links with a 16 MiB shadow stack (build.rs); with the default 1 MiB, 4,664 frames × 272 B of shadow stack would run out before the 512 KiB native Wasm stack and surface as an OOB-memory trap, not stack_overflow"
  - "sandbox_isolation runs on ONE Runtime, so fuel/timeout are shared: the 'fuel loop' is the step-fuel workflow (loop stopped by its step budget) and the plain loop is stopped by a 1 s epoch deadline (activation fuel u64::MAX/2)"
  - "Isolation/solo comparison uses FakeClock::with_step(_, 0) so concurrent runs' clock calls do not change det-basic's output"
requirements-completed: [ENG-05, ENG-06]
duration: 25min
completed: 2026-11-18
---

# Phase 3 Plan 10: Sandbox + Max-Depth Host-Call Stack Test Summary

This plan proves roadmap success criterion 2.
- **ENG-05:** an infinite loop, a memory bomb and unbounded recursion each end only their own attempt, with the right reason code. A per-step fuel budget trips only inside its step. Healthy det-basic runs on the same Runtime are unaffected.
- **ENG-06:** at the deepest Wasm stack depth that does not overflow, a guest can call every host import with maximum-size arguments and the process survives. A subprocess negative control shows that, with only 16 KiB of host headroom, the same calls do abort the process.

No engine bugs were found. `ostro-engine/src` was not touched.

## What was built

**`guests/sandbox`** (cdylib, world `workflow`) provides five workflows:
- `loop`: completes step `before`, then spins on `black_box(())` without calling any import.
- `bomb`: allocates 1 MiB chunks forever, touching every 4 KiB page and keeping each chunk alive.
- `deep`: unbounded recursion with a `black_box([u8; 256])` frame. Its exit condition is opaque to the optimiser.
- `deep-call`: the input is `depth u32 LE ‖ selector u8`. It recurses exactly `depth` frames (same frame shape as `deep`), then calls imports according to the selector:

  | Selector | Imports called at the bottom |
  |---|---|
  | 0 | none |
  | 1 | step with a 64 KiB name, completed with a 1 MiB payload |
  | 2 | `random-bytes(65536)` |
  | 3 | `set-status` with exactly 65,536 bytes of JSON |
  | 4 | `log.emit` with a 512 KiB message and 64 fields |
  | 5 | `http-fetch` with a 1 MiB body and 256 headers |
  | 6 | 4,096 sleeps, then `wait(all)` (evicts, then completes after timers fire) |
  | 7 | `uuid-v7` + `now` + `info` |
  | 8 | WASI `println!` of 64 KiB + `SystemTime::now` + `HashMap` insert |
  | 255 | all of the above in sequence |

  It returns `completed`.
- `step-fuel`: step `ok`, then step `hot` with `fuel: Some(1_000_000)`, whose body spins forever.

`build.rs` raises the shadow stack to 16 MiB (see Deviations).

**`tests/sandbox.rs`** (6 tests, multi-thread tokio):

| Test | Setup | Result |
|---|---|---|
| `sandbox_loop_fuel` | 100 M fuel | `FuelExhausted`; journal holds StepCompleted(`before`) |
| `sandbox_loop_epoch` | fuel `u64::MAX/2`, timeout 200 ms | `Timeout` in under 2 s |
| `sandbox_memory_bomb` | 64 MiB cap | `MemoryExceeded`; `budget().used()` returns to baseline |
| `sandbox_deep_recursion` | defaults | `StackOverflow` |
| `sandbox_step_fuel` | huge activation fuel, 60 s timeout | `FuelExhausted`; journal holds `ok` but not `hot` |
| `sandbox_isolation` | one Runtime | see below |

In `sandbox_isolation`, four hostile runs are spawned concurrently with det-basic: step-fuel, loop/epoch, bomb and deep. det-basic completes with output byte-identical to a solo run on a fresh Runtime. Each hostile run fails with its expected code. A second det-basic run on the same Runtime afterwards also completes.

**`tests/stack_depth.rs`** (2 tests plus 1 ignored child):
- **`stack_depth_every_import_at_max_depth`** uses the default config (4 MiB fiber stack, 512 KiB Wasm stack). For each selector it finds the maximum depth (probe from 1,000 doubling, then binary search; each probe is a fresh activation). It then runs at d−1, d and d+1, and every outcome is one of Completed, Evicted or StackOverflow.
- **`stack_depth_negative_control`** (`#[cfg(unix)]`) re-execs `current_exe()` with `--exact stack_depth_child --ignored --nocapture --test-threads=1` and `OSTRO_STACK_CHILD=1`.
  - The child uses `async_stack_size = max_wasm_stack + 16 KiB` (540,672 B), which Wasmtime accepted. It finds the max depth for selector 0, then binary-searches selector 255.
  - The parent accepts death by SIGABRT, SIGSEGV or SIGBUS.
  - If the child survives, the parent prints `NEGATIVE CONTROL DID NOT ABORT` and fails only under `OSTRO_STACK_STRICT=1`.
- The file header documents why the fiber stack is 4 MiB, the Research measurement (with a 2 MiB fiber and 512 KiB Wasm stack, a host import using 1.4 MiB survived and one using 1.7 MiB aborted), and the rule that host imports must stay thin.

## Measurements (debug host build, Wasmtime 50.0.2)

**Max non-overflowing `deep-call` depth with default config:**

| selector | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 255 |
|---|---|---|---|---|---|---|---|---|---|---|
| max depth | 4664 | 4660 | 4662 | 4652 | 4654 | 4657 | 4661 | 4662 | 4648 | 4641 |

That works out to about 112 B of native Wasm stack per frame (512 KiB / 4,664). Every selector survived at d−1, d and d+1.

**Negative control:**
- With 16 KiB of headroom, the child printed `CHILD selector=0 max_depth=4664`.
- During the selector-255 search it died with **SIGABRT** (wait status 134), and Wasmtime printed `execution on async fiber has overflowed its stack`.
- This is the abort mode the 4 MiB default guards against, and the test does detect it.

## Verification

- `cargo test -p ostro-engine --test sandbox`: 6 passed (1.3 s).
- `cargo test -p ostro-engine --test stack_depth -- --test-threads=1 --nocapture`: 2 passed, 1 ignored (17 s).
- `cargo nextest run -p ostro-engine --test stack_depth --test sandbox`: 8 passed, 1 skipped.
- `cargo clippy -p ostro-engine --test sandbox --test stack_depth -- -D warnings` is clean, and rustfmt is clean on all new files.
- `bash scripts/ci-local.sh --no-musl` passes: fmt, clippy, build, test, failpoints, deny, vendored, workflow validation.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Guest shadow stack raised via `guests/sandbox/build.rs`**
- **Found during:** Task 1
- **Issue:** Rust wasm32 guests keep the `[u8; 256]` frame in a 1 MiB linear-memory shadow stack. At the measured ~4,660 frames that needs about 1.27 MiB, so the shadow stack would run out before the 512 KiB native Wasm stack. That produces an out-of-bounds memory trap (`Trap`), not `StackOverflow`, and ENG-06 would never reach the native limit.
- **Fix:** `build.rs` emits `cargo:rustc-link-arg-cdylib=-zstack-size=16777216`. This file was not in the plan's `files_modified`.
- **Commit:** e8f1325

**2. [Plan adaptation] Isolation "fuel loop" uses the step-fuel workflow**
- **Issue:** `activation_fuel` and `activation_timeout` are per Runtime, and the plan requires ONE Runtime. A fuel-bounded loop and an epoch-bounded loop cannot both be distinguished on the same config.
- **Fix:** The shared Runtime has effectively unlimited activation fuel and a 1 s deadline. The fuel case is `step-fuel` (a loop stopped by its step's fuel budget, giving `FuelExhausted`), and the plain `loop` gives `Timeout`. Both reason codes are still exercised concurrently with det-basic.
- **Commit:** e8f1325

## Known Stubs

None.

## Commits

- e8f1325 feat(03-10): sandbox guest and ENG-05 resource-governance tests
- ff0ef4e test(03-10): ENG-06 max-depth host-call test with subprocess negative control

## Self-Check: PASSED

All five created files exist; commits e8f1325 and ff0ef4e are in git log.
