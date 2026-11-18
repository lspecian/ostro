---
phase: 03-deterministic-wasm-engine-wit-0-1
plan: 03
subsystem: engine
tags: [wasmtime, pooling-allocator, determinism, nan-canonicalization, resource-limiter, epoch]
requires: ["03-01"]
provides:
  - "ostro_engine::config::{EngineConfig, PoolConfig, LanguageClass, Fallback, build_wasmtime_config, build_on_demand_config, pooling_allocation_config, describe_wasmtime_config, engine_config}"
  - "ostro_engine::limits::{OstroLimiter, NodeMemoryBudget, Breach, HasLimits}"
  - "ostro_engine::engine::{OstroEngine, PoolMode, TickerProbe, configure_store}"
  - "guests/nan-probe guest"
affects: [03-04, 03-05, 03-06, 03-07, 03-08]
tech-stack:
  added: []
  patterns:
    - "Every guest-visible resource refusal is a trap (Err), never Ok(false)"
    - "Soft node memory budget: grows always allowed, pressure signalled through tokio Notify (notify_one, single consumer)"
    - "One epoch ticker thread per engine (EngineWeak, park_timeout, unpark+join on drop)"
key-files:
  created:
    - crates/ostro-engine/tests/engine_config.rs
    - crates/ostro-engine/tests/snapshots/engine_config__wasmtime_config_debug.snap
    - crates/ostro-engine/tests/pool.rs
    - guests/nan-probe/Cargo.toml
    - guests/nan-probe/src/lib.rs
  modified:
    - crates/ostro-engine/src/config.rs
    - crates/ostro-engine/src/limits.rs
    - crates/ostro-engine/src/engine.rs
decisions:
  - "The snapshot pins describe_wasmtime_config(): wasmtime::Config's Debug omits max_wasm_stack, async_stack_size, backtrace details and the allocator, so these are appended after the Debug output. No host-dependent values appear, so no insta filters are needed"
  - "WasmBacktraceDetails::Disable is set in the shared base config, so engine_config(), the pooling config and the on-demand config never read WASMTIME_BACKTRACE_DETAILS. The snapshot and an assertion pin it (this resolves the 03-06 orchestrator note)"
  - "Memory/table limit = min(wasm-declared maximum, configured cap); memory cap = max(engine.pool.max_memory_bytes, class floor)"
  - "The GC struct-type module is rejected, so the GC case is tested rather than skipped. Debug shows wasm_gc: true but wasm_gc_types: false"
metrics:
  duration: 35min
  completed: 2026-11-18
---

# Phase 3 Plan 03: Deterministic Engine, Pooling, Limiter, Epoch Ticker Summary

This plan builds the Wasmtime 50.0.2 engine that all activations will run on:

- **Config and snapshot.** The flags are deterministic: NaN canonicalisation, deterministic relaxed SIMD, fuel, epochs, a 512 KiB wasm stack, a 4 MiB fiber stack and no backtrace details. An insta snapshot pins the config.
- **Pool sizing.** The pooling allocator is sized per language class (core instances per instance: Rust 3, Python 38, TS 3; plus memory floors).
- **Pool failures.** A reservation failure produces an error that names `engine.pool.total_instances`, the GiB figure, `ulimit -v` and `vm.overcommit_memory`. By default the engine then falls back to the on-demand allocator. Pool exhaustion gets an explanation that names the same key.
- **Limiter.** The resource limiter traps on every breach and records which kind it was. It charges a soft node memory budget.
- **Epoch ticker.** It runs on a thread for as long as the engine lives and stops when the engine is dropped.

## What was built

- **config.rs**
  - `LanguageClass`, `Fallback`, `PoolConfig` and `EngineConfig`. Field names are as specified in the plan, and each field's doc names its `engine.*` key.
  - `effective_max_memory()`.
  - `pooling_allocation_config`, `build_wasmtime_config` (pooling when `engine.pool.enabled`) and `build_on_demand_config`.
  - `describe_wasmtime_config`. 03-06 can use it as compat-hash input if useful.
  - `engine_config()` is kept as a thin wrapper.
- **limits.rs**
  - `NodeMemoryBudget` (charge, release, used, limit, `is_over`, `pressure`).
  - `Breach`.
  - `OstroLimiter`, which implements `wasmtime::ResourceLimiter`. A memory breach returns `Err(MemoryExceeded)` and a table breach returns `Err`; both record the breach. `*_grow_failed` also traps. `Drop` releases exactly what was charged.
  - `HasLimits`.
- **engine.rs**
  - `OstroEngine::new` (pooling, wrapped `PoolReservation`, warn and on-demand fallback, or `Fail`).
  - Accessors: `engine`, `config`, `pool_mode`, `budget`, `new_limiter`, `pooling_metrics` and `release_idle_pool_memory`.
  - `ticker_probe()` (tick count and exited flag).
  - `explain_instantiate_error`, which detects `PoolConcurrencyLimitError` or the "maximum concurrent limit" text.
  - `configure_store`: limiter, fuel, `set_epoch_deadline(1)`, and a callback that yields every tick and interrupts once the wall deadline has passed.
- **guests/nan-probe**: `run("nan", 8 bytes)` returns the bits of `x+1.0` and `x*2.0`.

## Verification

- `cargo test -p ostro-engine --lib` passes: 9 tests, including 5 limiter tests, 2 config tests and the reservation-message test.
- `cargo test -p ostro-engine --test engine_config` passes 3 tests:
  - snapshot
  - shared-memory and GC-struct modules rejected
  - NaN probe returns 0x7ff8000000000000 for both operations
- `cargo test -p ostro-engine --test pool -- --test-threads=1` passes 6 tests (1 ignored child):
  - The 40,000-slot reservation failed on the first try (mmap ENOMEM for ~161,250 GiB).
  - The prlimit 8 GiB child reports `pool_mode=OnDemand`.
  - Exhaustion is reported as `instance pool exhausted (engine.pool.total_instances=2): maximum concurrent limit of 2 for component instances reached`.
  - **Warm instantiate in a debug build: p50 = 159 µs, p99 = 213 µs**, against a 1 ms target.
  - The ticker advanced by at least 5 ticks in 100 ms and exited within 30 ms of drop.
- `cargo test -p ostro-engine --test scaffold` passes. The 03-01 helper still works with the now-pooling `engine_config()`.
- `cargo clippy -p ostro-engine --lib --test pool --test engine_config --test scaffold -- -D warnings` is clean, and `cargo build -p ostro-engine` gives 0 warnings.

### Verification scope (concurrency)

The shared tree did not compile while I worked:
- 03-04's in-progress `host/wasi/mod.rs` and 03-06's `cache.rs` broke the lib build in the main checkout.
- 03-06's committed RED test `tests/cache.rs` breaks `--all-targets`.

I therefore ran every verification in a detached scratch worktree at HEAD with only this plan's files copied in, and scoped clippy to `--lib` plus this plan's tests rather than `--all-targets`. `cargo build --workspace --locked` was not run, because the workspace was mid-edit by Phase 2 and 03-04/03-06.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Config Debug omits stack sizes**
- **Found during:** Task 1
- **Issue:** `wasmtime::Config`'s `Debug` output has no `max_wasm_stack`, `async_stack_size` or allocator fields, so a snapshot of `format!("{:#?}", config)` could not pin them, even though the plan requires that.
- **Fix:** Added `config::describe_wasmtime_config`, which appends these settings and the `PoolingAllocationConfig` Debug, and snapshotted that. The pool's `stack_size: 2097152` in that Debug is overwritten with `async_stack_size` when the Wasmtime engine is built (config.rs:2951 in wasmtime 50.0.2).
- **Commit:** 25af7ac

**2. [Rule 3 - Blocking] `AtomicU64::fetch_update` is deprecated on toolchain 1.100**
- **Fix:** Used `try_update`.
- **Commit:** 25af7ac

**3. Fallback warning assertion**
- The workspace has no `tracing-subscriber`, so `pool_reservation_fallback` installs a minimal hand-written `tracing::Subscriber` that captures WARN events.

### Unused-dep anchors
- The `use tracing as _;` anchor in lib.rs had already been removed by 03-06 (which uses tracing in `cache`), so lib.rs did not need editing. The other anchors (chacha20, hmac, metrics) are not consumed by this plan.

## Known Stubs

None. `HasLimits` has no implementor yet; `HostState` implements it in 03-07, as planned.

## Commits

- 25af7ac feat(03-03): EngineConfig, deterministic Wasmtime config builder, resource limiter
- f0c0bc0 feat(03-03): OstroEngine with pool-failure wrapping, on-demand fallback, epoch ticker

## Self-Check: PASSED
