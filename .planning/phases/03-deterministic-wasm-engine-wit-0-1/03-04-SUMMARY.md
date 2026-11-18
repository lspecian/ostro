---
phase: 03-deterministic-wasm-engine-wit-0-1
plan: 04
subsystem: engine
tags: [wasi, shim, determinism, validation, wit, insta]
requires:
  - "03-01: GuestEnv / RandomStream / StdStream (env.rs), GuestExit + EngineError::Validation (error.rs), host::bindings, tests/common::guest_wasm"
provides:
  - "ostro_engine::host::wasi::add_to_linker<T: GuestEnv> (18 wasi:*@0.2.12 interfaces, sync+trappable)"
  - "ostro_engine::validate::{validate_component, ValidatedComponent, ALLOWED_OSTRO, ALLOWED_WASI, REQUIRED_EXPORT}"
  - "crates/ostro-engine/wit-wasi/ (vendored WASI 0.2.12 WIT + shim.wit)"
  - "guests: wasi-probe, wasi-forbidden, cli-command"
affects: [03-07, 03-08, phase-4-deploy, phase-9-sdks]
tech-stack:
  added: []
  patterns:
    - "Shim Host traits implemented on a local `WasiImpl<'a, T>` view with a custom HasData (avoids collision with bindgen's `impl Host for &mut _T`)"
    - "Per-store one-time warning tracked by a marker value in the store's ResourceTable"
key-files:
  created:
    - crates/ostro-engine/wit-wasi/shim.wit
    - crates/ostro-engine/wit-wasi/deps/{io,clocks,random,cli}.wit
    - crates/ostro-engine/wit-wasi/README.md
    - crates/ostro-engine/src/host/wasi/{clocks,random,io,cli}.rs
    - crates/ostro-engine/tests/wasi_shim.rs
    - crates/ostro-engine/tests/validate.rs
    - crates/ostro-engine/tests/snapshots/validate__wasi_probe_imports.snap
    - guests/wasi-probe/{Cargo.toml,src/lib.rs}
    - guests/wasi-forbidden/{Cargo.toml,src/lib.rs}
    - guests/cli-command/{Cargo.toml,src/main.rs}
  modified:
    - crates/ostro-engine/src/host/wasi/mod.rs
    - crates/ostro-engine/src/validate.rs
    - VENDORED.md
    - NOTICE
decisions:
  - "Shim imports are sync + trappable (nothing awaits); they coexist with async ostro imports in one Linker"
  - "Shim Host impls live on `WasiImpl<'_, T: GuestEnv>` (custom HasData), not a blanket `impl Host for T`"
  - "cli.wit: only the `world imports`/`world command` blocks are dropped (they import filesystem/sockets); clocks.wit timezone (@unstable) kept unchanged"
  - "Validation runs instantiate_pre only when the allow-list checks are clean; all violations are reported together"
  - "wasi:random requests are capped at 1 MiB per call (traps above), guarding host memory"
metrics:
  duration: 35min
  completed: 2026-11-18
---

# Phase 3 Plan 04: Deterministic WASI Shim + Deploy-time Validation Summary

Ostro now has its own WASI 0.2.12 shim. It implements 18 interfaces with sync, trappable Wasmtime bindgen imports and takes clocks, randomness and stdio only from `GuestEnv`. Timers never sleep in real time and `exit` becomes a typed `GuestExit` trap. Alongside it, `validate_component` checks a component at deploy time: imports and exports against the allow-list, then `instantiate_pre`. A committed insta snapshot pins the exact WASI imports of the probe guest. `wasmtime-wasi` is not used (`cargo tree -p ostro-engine -e normal | grep -c wasmtime-wasi` = 0).

## What was built

- **Vendored WIT** (`crates/ostro-engine/wit-wasi/`): `io`, `clocks`, `random` and `cli` 0.2.12, copied from the `wasip2 1.0.4+wasi-0.2.12` crate.
  - `cli.wit` lost lines 140–233 (the `world imports` and `world command` blocks), because they import filesystem and sockets.
  - `shim.wit` declares world `ostro:wasi-shim/shim` with the 18 interfaces.
  - `README.md` gives provenance and the re-vendoring steps.
  - VENDORED.md has a new "Third-party WIT (not under vendor/)" table (appended), and NOTICE has a WASI paragraph (appended).
- **Shim** (`src/host/wasi/`):
  - wall clock = `wall_now`, resolution 1 µs. Monotonic clock = `monotonic_now_ns`, resolution 1 ns.
  - `subscribe-*`, `poll`, `ready` and `block` all return immediately, with one `tracing::warn!` per store.
  - random → `WasiRandom`, insecure → `WasiInsecure`, insecure-seed → `insecure_seed()`.
  - env, args and cwd are empty. stdin is at EOF (`Closed`) and terminals are `None`.
  - stdout/stderr call `write_stdio`, truncated to `stdio_budget`. `check-write` returns at least 4096 so the guest keeps making progress, and the excess is dropped without trapping.
  - `exit` → `GuestExit(0|1)` and `exit-with-code` → `GuestExit(code)`.
- **`validate.rs`** runs four steps and collects every violation:
  1. Core modules are rejected ("not a component: ..."), detected from the version bytes.
  2. Each import is checked against `ALLOWED_OSTRO@0.1.*` / `ALLOWED_WASI@0.2.*`. The error gives a reason by family: filesystem, sockets/http, or other.
  3. The only allowed export is `ostro:runtime/workflows@0.1.*`. Any other export is rejected, and a missing workflows export is reported too.
  4. `instantiate_pre` runs only if steps 2 and 3 are clean.
- **Guests:**
  - `wasi-probe` has the probes time, hashmap, rand, print, print-big, env, sleep, exit and stdin.
  - `wasi-forbidden` uses `std::fs`.
  - `cli-command` is a `[[bin]]` and exports `wasi:cli/run`.

Real rejection messages:
```
import wasi:filesystem/types@0.2.12 is not allowed: Ostro guests have no filesystem
import wasi:filesystem/preopens@0.2.12 is not allowed: Ostro guests have no filesystem
export wasi:cli/run@0.2.12 is not allowed: Ostro workflows export ostro:runtime/workflows, not a command/HTTP handler
missing required export ostro:runtime/workflows@0.1.*
```

The import snapshot lists `ostro:runtime/types@0.1.0` and 17 `wasi:*@0.2.12` interfaces: the 16 from S2 plus `random/random` from `rand`.

## Verification

- `cargo test -p ostro-engine --test wasi_shim --test validate`: 8 + 7 pass. This was run in an isolated worktree and again in the main repo after the other executors' code built. The 2 `validate` unit tests in the lib also pass.
- `cargo clippy -p ostro-engine --lib --test validate --test wasi_shim -- -D warnings` is clean, and so is guest clippy for the three new guests (wasm32-wasip2, `-D warnings`).
- `bash scripts/check-vendored.sh`: all checks pass.
- `cargo tree -p ostro-engine -e normal | grep -c wasmtime-wasi` = 0.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `wasi_shim_env_empty` expectation for cwd**
- **Found during:** Task 1
- **Issue:** The shim returns `initial-cwd = None` as specified. wasi-libc then falls back to `/`, so `std::env::current_dir()` returns `Ok("/")`, and a guest cannot observe "no cwd" directly.
- **Fix:** The probe now reports the cwd string, and the test asserts `"/"` (the libc default when the host provides none). The shim itself is unchanged.
- **Commit:** 4e35881

**2. [Rule 1 - Bug] Sleep timing measured around compilation**
- **Found during:** Task 1
- **Issue:** With 8 tests compiling in parallel in a debug build, the timing measured around compilation is noisy.
- **Fix:** `run_probe` now times only `call_run`, and the test asserts that call takes < 1 s. An isolated run measured about 35–50 ms in total, including compilation.
- **Commit:** 4e35881

**3. [Design] `WasiImpl` view instead of `HasSelf<T>`**
- The plan suggested `HasSelf<T>` with `impl<T: GuestEnv> Host for T`. That clashes with bindgen's generated `impl<_T: Host> Host for &mut _T`, because `&mut` is fundamental. I used a local `WasiImpl<'a, T>` with a custom `HasData`, following the wasmtime-wasi pattern. The public API is unchanged: `add_to_linker<T: GuestEnv>(&mut Linker<T>)`.

**4. [Test design] The test "full linker" only adds traps for allow-listed guests**
- The `define_unknown_imports_as_traps` fallback is applied only when every import is `ostro:runtime/*` or allow-listed WASI, so a forbidden WASI import is never papered over with traps. The current guests import only `ostro:runtime/types`, which has no functions. 03-07 re-runs validation against the real linker.

**5. TDD commits:** each task's tests and implementation were committed together rather than as separate RED/GREEN commits. Both tasks were verified red→green locally.

### Concurrency notes
- During Task 1 the main tree failed to build because 03-05's replay modules were mid-edit, so I verified in a detached worktree (scratchpad `wt-0304`, since removed) with only my files synced in. After commit, the main repo's `cargo test -p ostro-engine --test wasi_shim --test validate` also passed.
- In that worktree, `cargo clippy --all-targets` failed on 03-06's RED test `tests/cache.rs`, which is not mine, so I scoped clippy to `--lib --test validate --test wasi_shim`.
- I appended to VENDORED.md and NOTICE only, after re-reading them immediately before the edit.

## Known Stubs

None. The `validate` integration tests use a stand-in full linker (shim plus traps) until 03-07 provides the real ostro import linker.

## Commits

- 4e35881 feat(03-04): deterministic WASI 0.2.12 shim on GuestEnv with vendored WIT
- d64b59a feat(03-04): deploy-time component validation and WASI import snapshot

## Self-Check: PASSED

All listed created files exist, and commits 4e35881 and d64b59a are in `git log`.
