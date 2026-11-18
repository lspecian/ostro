---
phase: 03-deterministic-wasm-engine-wit-0-1
verified: 2026-11-18T00:00:00Z
status: passed
score: 5/5 must-haves verified
---

# Phase 3: Deterministic Wasm Engine & WIT 0.1 Verification Report

**Phase Goal:** Wasm components run in a deterministic, sandboxed host that replays them exactly and detects divergence, built against a fake ActivationSink. The full ostro:runtime@0.1.0 surface is defined.
**Verified:** 2026-11-18
**Status:** passed
**Re-verification:** No, initial verification

Verification ran in a detached worktree at HEAD, because uncommitted Phase 2 work is in the tree. The worktree was removed afterwards.

- `cargo test -p ostro-engine -p ostro-types` passed with 0 failures.
  - ostro-engine: 51 unit tests plus 15 integration test files.
  - ostro-types: 7 unit tests plus command, envelope, ids and journal_golden tests.
- `cargo clippy -p ostro-engine -p ostro-types --all-targets -- -D warnings` was clean.
- The ignored tests are child processes spawned by the prlimit and negative-control tests. They are not skipped coverage.

## Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Determinism guest gives identical journals over 1,000 replays. A modified module fails with `nondeterminism` at the right seq, with expected vs actual. | VERIFIED | tests/determinism.rs (10 tests pass, default 1000 iterations). Divergence tests pass. Golden journal test in ostro-types. |
| 2 | Infinite-loop, memory-bomb and deep-recursion guests are stopped in isolation. Max-depth host call does not abort the node. | VERIFIED | tests/sandbox.rs (6 tests) and tests/stack_depth.rs (2 pass, plus a child-process negative control). |
| 3 | Deploy-time import validation. Bounded AOT compile pool. Cache keyed by sha256 + compat hash + version. Tampered artefact fails HMAC and is recompiled. | VERIFIED | tests/validate.rs (7), tests/cache.rs (12), tests/wasi_shim.rs (8). |
| 4 | Warm instantiation via InstancePre and pooling with p50 under 1 ms. Clear pool misconfiguration error. Evicted activations replay within the node budget. | VERIFIED | runtime_warm_instantiate_p50 asserts p50 < 1 ms. tests/pool.rs (pool_exhaustion_clear_error, prlimit reservation). tests/hot_wait.rs (7), tests/runtime_cache.rs (5). |
| 5 | WIT world covers context, journal, effects/wait, log and the workflows export. It also covers cancel vs terminate, continue-as-new, await tokens, set-status, run timeouts and the non-retryable kind. | VERIFIED | tests/wit_surface.rs (17 pass), tests/host_adapter.rs (7), tests/scaffold.rs. |

**Score:** 5/5

## Requirements Coverage

| Requirement | Status | Evidence |
|---|---|---|
| ENG-01 | SATISFIED | tests/engine_config.rs |
| ENG-02 | SATISFIED | tests/wasi_shim.rs and tests/validate.rs. Own shim with 16-17 wasip2 imports, accepted deviation. |
| ENG-03 | SATISFIED | tests/determinism.rs |
| ENG-04 | SATISFIED | Replay returns journaled canonical bytes. journal_golden and determinism tests pass. |
| ENG-05 | SATISFIED | tests/sandbox.rs |
| ENG-06 | SATISFIED | tests/stack_depth.rs |
| ENG-07 | SATISFIED | tests/cache.rs |
| ENG-08 | SATISFIED | tests/pool.rs and tests/runtime_cache.rs |
| ENG-09 | SATISFIED | tests/hot_wait.rs |
| WIT-01 | SATISFIED | tests/wit_surface.rs |
| WIT-02 | SATISFIED | tests/wit_surface.rs |
| WIT-03 | SATISFIED | tests/wit_surface.rs |
| WIT-04 | SATISFIED | Engine side only. Client side is API-07 in Phase 4, accepted. |
| WIT-05 | SATISFIED | Engine side only. API side is API-04 in Phase 4, accepted. |
| WIT-06 | SATISFIED | tests/wit_surface.rs |

All 15 IDs are mapped to Phase 3 in REQUIREMENTS.md. None are orphaned. WIT-07 is Phase 5 and out of scope.

## Anti-Patterns

None blocking. I did not scan every source file for TODO or stub markers. The clippy and test evidence above is what the verdict rests on.

## Accepted Deviations

- Logical time is monotonic over all journal events.
- RNG streams are step-local.
- wasmtime is pinned at =50.0.2.
- The WASI shim is Ostro-owned.

## Human Verification

None required. The p50 benchmark runs in a debug profile test and passes. Release-profile NFR numbers could be re-checked in CI, but this does not block the phase.

## Gaps Summary

No gaps. The phase goal is achieved.

_Verified: 2026-11-18_
_Verifier: Claude (gsd-verifier)_
