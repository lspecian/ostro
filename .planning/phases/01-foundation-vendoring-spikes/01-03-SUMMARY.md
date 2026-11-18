---
phase: 01-foundation-vendoring-spikes
plan: 03
subsystem: spikes
tags: [wasm, wasmtime, component-model, wit, componentize-py, jco, wit-bindgen, spike-s2]
requires: []
provides:
  - "spikes/wit/spike.wit: shared S2 WIT (resource step + u64 fallback, context, probes, run-reseeded, run-dropped-step)"
  - "spikes/s2-guests/build.sh: reproducible py/ts/rust guest builds into spikes/out/"
  - "spikes/s2-harness: wasmtime 50.0.2 harness, subcommands compile|instantiate|memory|probes|imports|cold (one JSON line each)"
affects: [01-05, phase-3-engine, phase-4-deploy-validation, phase-9-10-sdks]
tech-stack:
  added: [wasmtime 50.0.2 (spike only), componentize-py 0.25.1, "@bytecodealliance/jco 1.42.0", "@bytecodealliance/componentize-js 0.23.0", wit-bindgen 0.63, clap 4, flate2 1]
  patterns: [define_unknown_imports_as_traps instead of wasmtime-wasi, deserialize_file + InstancePre, ResourceTable-backed resource step, traps-as-data JSON]
key-files:
  created:
    - spikes/wit/spike.wit
    - spikes/s2-guests/build.sh
    - spikes/s2-guests/python/app.py
    - spikes/s2-guests/ts/package.json
    - spikes/s2-guests/ts/package-lock.json
    - spikes/s2-guests/ts/src/index.ts
    - spikes/s2-guests/rust/Cargo.toml
    - spikes/s2-guests/rust/Cargo.lock
    - spikes/s2-guests/rust/src/lib.rs
    - spikes/s2-harness/Cargo.toml
    - spikes/s2-harness/Cargo.lock
    - spikes/s2-harness/src/main.rs
  modified: []
decisions:
  - "S2 harness defines unknown imports as traps (no wasmtime-wasi); WASI-touching probes report the trap as a finding"
  - "Rust guest built with toolchain 1.100.0 after `rustup target add --toolchain 1.100.0 wasm32-wasip2` (fallback toolchain 1.100 not needed)"
  - "AOT artefacts compiled with the on-demand engine config deserialize fine under the pooling engine (no separate pooling compile needed)"
metrics:
  duration: ~12min
  completed: 2026-11-18
---

# Phase 1 Plan 03: S2 Spike Apparatus Summary

One shared WIT builds into Python (componentize-py 0.25.1 `--stub-wasi`), TypeScript (jco 1.42.0 `--disable all`) and Rust (wit-bindgen 0.63, wasm32-wasip2) guests. A standalone Wasmtime 50.0.2 harness compiles, instantiates (on-demand or pooling, from an mmap'd `deserialize_file` artefact), measures memory, runs the probes and measures cold start. Each measurement is its own subcommand and prints one JSON line.

## Tasks

| # | Task | Commit |
|---|------|--------|
| 1 | Shared WIT, three guests, build.sh | 8f01c8a |
| 2 | Wasmtime 50.0.2 measurement harness | 78acdd1 |

## What was built

- **`spikes/wit/spike.wit`**: the research WIT, plus `run-reseeded(seed-from-host: bool)` and `run-dropped-step()`.
- **Guests**: all three implement every export. `run` does a resource step round trip (begin-step, seq, complete) and then the u64 round trip (begin-step-u64, complete-u64). The Rust guest's `run`, `run-reseeded` and `run-dropped-step` make no WASI calls.
- **`build.sh <py|ts|rust|all> [suffix]`**: writes `spikes/out/<lang><suffix>.wasm` and prints sha256 and byte size. It runs `npm ci` before the jco build. `PYTHONHASHSEED` passes through from the caller.
- **`s2-harness`**: `compile` (compile_ms, wasm/gzip/cwasm bytes, VmHWM), `instantiate` (deserialize ms, first instantiate, first `run` call, then n× p50/p99/max for instantiate, per-instance first call and steady same-instance call), `memory` (max declared linear memory via `ResourceLimiter`, VmRSS before, with n live instances, and after drop), `probes` (two fresh instances per probe, run-reseeded with no reseed ×2 and seeds A/B/A, run-dropped-step with `step_drops` counted), `imports` (non-`ostro:spike/*` imports listed separately) and `cold`. Every pooling override is a CLI flag and is echoed back in `allocator.overrides`.

## Smoke-run observations (not the formal 01-05 measurements)

- **Imports:** py and ts import only `ostro:spike/*`. The Rust wasip2 guest imports 16 `wasi:*@0.2.12` interfaces (io, clocks, cli/*, random/insecure-seed). Python also exports componentize-py's internal `exports` interface.
- **Resource step:** works in all three languages. `HostStep::drop` is observed for a step dropped without `complete` in py, ts (via `Symbol.dispose`) and rust. **In TS `run`, the completed step is never dropped (`step_drops: 0`)** without an explicit dispose, so the handle leaks until GC. This matters for the SDK design.
- **Probes:** py `time.time()` and `uuid.uuid4()` trap (`unreachable` stub in wasi-libc). TS returns a frozen build timestamp and fixed crypto bytes. In Rust, `HashMap::new()`/`RandomState` and `SystemTime::now()` trap on the undefined `wasi:random/insecure-seed` and `wasi:clocks/wall-clock` imports. Even a plain std `HashMap` needs a WASI random shim. Random values are identical across instances for py and ts. Host-seeded `run-reseeded` is deterministic per seed and differs between seeds, in all three languages.
- **Indicative numbers (on-demand, n=20):** instantiate p50: py about 490 µs, ts about 20 µs, rust about 18 µs. RSS per live instance: py about 6.6 MB, ts about 2.6 MB, rust about 40 KB. Cold start from main to first `run`: py 5.0 ms, ts 2.3 ms, rust 0.5 ms.

## Deviations from Plan

**1. [Rule 2 - Observability] Richer trap reporting.** Each trap is reported as `{"trap": <root cause>, "trap_code", "frames": [first 4 backtrace frames]}`, not just the first line of the message. The outer error is always "error while executing at wasm backtrace", so the root cause is what tells an `unreachable` stub apart from an unknown-import trap. Part of commit 78acdd1.

**2. Minor additions.** The probes subcommand also runs `run` with completion and drop counts, plus a no-reseed baseline for `run-reseeded`. `instantiate` reports per-instance first-call stats alongside steady same-instance stats. The TS `run-dropped-step` calls `Symbol.dispose` when it exists and reports whether it did.

Otherwise the plan was executed as written. Neither spike package is a root-workspace member: the root `Cargo.toml` excludes `spikes`, and each spike has its own empty `[workspace]`.

## Requirements

FOUND-06 is NOT marked complete: this plan supplies only the S2 apparatus. The S2 measurements and report (01-05) and the S3 results (01-02) are still needed before FOUND-06 is satisfied.

## Self-Check: PASSED
