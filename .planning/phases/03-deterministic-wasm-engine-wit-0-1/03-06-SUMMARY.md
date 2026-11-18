---
phase: 03-deterministic-wasm-engine-wit-0-1
plan: 06
subsystem: engine
tags: [wasmtime, aot, cache, hmac, sha256, compile-pool, single-flight]
requires: ["03-01"]
provides:
  - "ostro_engine::cache::{AotCache, DiskCache, CacheStats, CacheStatsSnapshot}"
  - "ostro_engine::cache::{cache_key, compat_digest, module_sha, load_or_create_key}"
affects: [03-11, phase-4-deploy]
tech-stack:
  added: []
  patterns:
    - "Wasmtime `impl Hash` compat hash fed into a SHA-256 `Hasher` adapter (never std's SipHash)"
    - "Artefact = cwasm || HMAC-SHA256(node key); verify on in-memory bytes, then Component::deserialize from the same bytes"
    - "Single-flight via Mutex<HashMap<key, Arc<tokio OnceCell>>>; flight covers disk lookup + compile"
key-files:
  created:
    - crates/ostro-engine/tests/cache.rs
  modified:
    - crates/ostro-engine/src/cache.rs
    - crates/ostro-engine/src/lib.rs
decisions:
  - "Cache key = hex(sha256(module)) + '-' + first 16 hex chars of sha256(compat hash, wasmtime=50.0.2, ostro version, arch, os, target_env), parts length-prefixed"
  - "A whole flight (disk lookup + compile) is single-flighted, so concurrent callers share one result; hits/misses count once per flight"
  - "Artefact persist failure is logged, not fatal (the compiled component is still returned)"
  - "Wasmtime deserialize does NOT reject a different cranelift opt level (it does reject tunables such as memory_guard_size); the compat digest still separates them by key"
metrics:
  duration: 20min
  completed: 2026-11-18
---

# Phase 3 Plan 06: AOT Cache + Compile Pool Summary

ENG-07 is in place. `AotCache` is an HMAC-SHA256-authenticated on-disk artefact cache with a semaphore-bounded, single-flight compile pool. Keys are sha256(module) plus a SHA-256 digest of Wasmtime's compatibility hash, the Wasmtime and Ostro versions, and the target triple.

## What was built

- **`compat_digest` / `cache_key` / `module_sha`.** A `Sha256Hasher` adapter takes `engine.precompile_compatibility_hash()`. The Wasmtime version, Ostro version, arch, OS and target_env are then appended, each with a length prefix. A small `hex` helper is used instead of a hex crate.
- **`load_or_create_key(dir)`.** Creates `dir/aot.key` with `create_new` and mode 0600: 32 bytes from `/dev/urandom`, then fsync of the file and its directory. If `/dev/urandom` is unavailable, it falls back to SHA-256 over several `RandomState` hashes, the wall clock and the pid. If two processes race to create the key, the loser reads the winner's file. A key file that is not 32 bytes is reported as `InvalidData`.
- **`DiskCache`.**
  - `get` reads the whole file and checks the trailer with `verify_slice` (constant time). It then calls `Component::deserialize` on those same in-memory bytes, with a SAFETY comment explaining why that is sound.
  - A short or tampered file is deleted and adds 1 to `tamper_total`. It also emits the `ostro_aot_cache_tamper_total` metric and a warn log.
  - If deserialize fails, the file is deleted and `incompatible_total` goes up.
  - `put` writes to a tmp file, calls `sync_all`, renames it into place and fsyncs the directory.
- **`AotCache`.** `component(sha, wasm)` puts the disk lookup and compile for one key into a single flight. On a miss, it takes a semaphore permit and runs `precompile_component` plus `put` in `spawn_blocking`, then deserializes the fresh in-memory artefact. The flight entry is removed afterwards using a `ptr_eq` check. Also provided: `max_concurrent_compiles()` (test hook), `stats()` and `disk()`. Doc comment: plan 03-11's L2 `InstancePre` LRU builds on this, and Phase 4 deploy calls `component()` at upload time.
- **lib.rs.** Removed the `use hmac/metrics/tracing as _;` anchors from 03-01, since `cache` now uses those crates. `chacha20` stays.

## Verification

- `cargo test -p ostro-engine --test cache` passes all 12 tests, and stayed green in 3 repeated runs:
  - cache_key_stable, compat_digest_env_independent, hmac_key_created_0600
  - cache_roundtrip, cache_tamper_detected_{body,trailer}, cache_truncated
  - compile_pool_bounded, compile_single_flight, cache_hit_after_compile
  - tamper_then_recompile, compat_miss_recompiles
- `cargo clippy -p ostro-engine --all-targets -- -D warnings` is clean, the build has no warnings, and `cargo test -p ostro-engine` passes.
- `grep -n "deserialize_file\|DefaultHasher" crates/ostro-engine/src/cache.rs` returns nothing.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Test bug] Opt level alone does not make an artefact incompatible**
- **Found during:** Task 2 (compat_miss_recompiles)
- **Issue:** Wasmtime 50 deserialized engine A's artefact in an engine B that differed only in `cranelift_opt_level(None)`. The test assumed it would be rejected.
- **Fix:** The test's `other_engine()` now also sets `memory_guard_size(64 KiB)`, a tunable that Wasmtime's `check_tunables` does reject. The engines' keys still differ either way.
- **Commit:** f9b9714

**2. [Rule 3 - Blocking] Verification in a scratch worktree**
- Other executors' uncommitted 03-04 WIP in the main checkout (`host/wasi` modules) stopped the `ostro-engine` lib from compiling. I ran every test and clippy check in a detached git worktree containing only my files, sharing `target/`. That worktree has since been removed.

**3. lib.rs edited (not listed in files_modified)**
- I removed the unused-dependency anchors, as the orchestrator asked. lib.rs had no uncommitted changes from anyone else when I did this.

### Notes for 03-03
- `common::test_engine()` / `engine_config()` leaves `wasm_backtrace_details` at its default (`Environment`). The tests use a local config pinned to `Disable`. `engine_config()` should set `WasmBacktraceDetails::Disable` explicitly so that the env var cannot affect production engines.

## Known Stubs

None.

## Commits

- ac47cc5 test(03-06): add failing tests for AOT cache key, HMAC key file and disk cache
- 45bb292 feat(03-06): AOT cache key, node-local HMAC key and authenticated disk cache
- 696760c test(03-06): add failing tests for bounded single-flight compile pool
- f9b9714 feat(03-06): bounded single-flight AOT compile pool (AotCache)

## Self-Check: PASSED

All four commits are present, and `crates/ostro-engine/src/cache.rs` and `crates/ostro-engine/tests/cache.rs` exist.
