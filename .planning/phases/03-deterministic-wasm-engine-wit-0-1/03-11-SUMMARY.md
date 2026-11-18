---
phase: 03-deterministic-wasm-engine-wit-0-1
plan: 11
subsystem: engine
tags: [hot-wait, eviction, lru, node-budget, aot-cache, instance-pre, logical-time, rng, determinism]
requires:
  - "03-06: AotCache (L1 disk, HMAC, single-flight compile pool), load_or_create_key, module_sha"
  - "03-08: Runtime, activation driver, FakeSink testkit, Activate.inputs/cancel"
  - "03-05: ReplayState (wait/deliver_inputs/logical time/RNG streams)"
  - "03-09/03-10/03-12: determinism, sandbox, stack-depth and WIT suites (regression gate)"
provides:
  - "Hot wait in effects.wait: park after flush, resume in-activation on inputs; Evict{HotWaitElapsed|BudgetPressure|External}"
  - "ostro_engine::limits::{ParkedRegistry, ParkGuard}; NodeMemoryBudget::released()"
  - "ostro_engine::host::state::HotWait; HostInit.hot; Runtime::parked()"
  - "Runtime::{prepare_cached, cache_stats, instance_pre_len, instantiate_probe}; RuntimeOptions { cache_dir, instance_pre_capacity }; DEFAULT_INSTANCE_PRE_CAPACITY"
  - "validate::{validate_loaded, reject_core_module}"
  - "activation::Classified::Fenced"
  - "testkit: Script::live(delay), FakeSink::{open_live, close_live}"
  - "Replay core: logical time monotonic over all journaled events; step-local RNG streams (rng::step_seed)"
affects: [phase-04, phase-05 QA-02 replay corpus]
tech-stack:
  added: []
  patterns:
    - "Hot wait = flush intents, register ParkGuard, tokio::select!{cancel, budget token, inputs.recv, sleep_until(deadline)} inside the wait import (thin: only channel receives + the flush await)"
    - "Budget LRU = one tokio task on NodeMemoryBudget::pressure; evicts min park-tick, then waits (<=250 ms) for released() before re-checking"
    - "L2 InstancePre LRU keyed by module sha in front of the L1 AOT cache; validation after every load"
    - "Live step closures draw from step-local ChaCha streams keyed by (run seed, step seq)"
key-files:
  created:
    - crates/ostro-engine/tests/hot_wait.rs
    - crates/ostro-engine/tests/runtime_cache.rs
  modified:
    - crates/ostro-engine/src/host/effects.rs
    - crates/ostro-engine/src/host/state.rs
    - crates/ostro-engine/src/limits.rs
    - crates/ostro-engine/src/activation.rs
    - crates/ostro-engine/src/runtime.rs
    - crates/ostro-engine/src/validate.rs
    - crates/ostro-engine/src/testkit.rs
    - crates/ostro-engine/src/replay/cursor.rs
    - crates/ostro-engine/src/replay/rng.rs
    - crates/ostro-engine/src/replay/tests.rs
    - crates/ostro-engine/tests/snapshots/determinism__golden_det_basic_journal.snap
    - crates/ostro-engine/tests/{activation,determinism,sandbox,stack_depth,wit_surface,host_adapter}.rs
key-decisions:
  - "Logical time: live events are stamped max(clock, last command ts, max ts of every journal event); now() after a wait is >= the input that resolved it"
  - "Step-local randomness: a live step's closure draws from streams seeded sha256(\"ostro/step-rng/v1\" || seed || seq LE); run-level streams restored on complete/abandon/finish"
  - "Parking does not check the node budget: an over-budget park registers as most recently parked and wakes the LRU, so older parked activations go first (the plan's !is_over() precondition raced the eviction task and inverted LRU)"
  - "Pre-park flush errors map like driver flushes: Fenced -> Classified::Fenced -> ActivationOutcome::Fenced; Conflict -> Evict{AppendConflict}; Unavailable -> Evict{External}; undeliverable inputs -> Evict{AppendConflict}"
  - "StepCompleted.fuel_used is a per-execution measurement and may differ between hot and evict runs (guest heap differs); equivalence compares everything else"
requirements-completed: [ENG-09]
duration: 45min
completed: 2026-11-18
---

# Phase 3 Plan 11: Hot Wait, LRU Budget Eviction, Cached Prepare Summary

Waits that can resolve within `engine.hot_wait` now park in-activation and resume when the input arrives. Over the node memory budget, the least recently parked activation is evicted first. `Runtime::prepare_cached` serves modules from an L2 `InstancePre` LRU over the HMAC-verified AOT disk cache, and a warm det-basic instantiates with the production linker in 272 µs p50 (debug build). Two replay-core determinism bugs were fixed on the way, each with a deliberate golden re-bless: logical time is now monotonic over input events, and RNG draws inside live step closures no longer leak into the run-level streams.

## What was built

- **Hot wait (`host/effects.rs`)**
  - On `WaitOutcome::Pending`, the wait is hot-waitable when:
    - an input channel is present;
    - at least one awaited op (other than `CANCELLATION_OP`) is still open;
    - every open op is either a `TimerStarted` with `fire_at - logical_now <= hot_wait` or an `EffectScheduled`.
  - Otherwise the wait traps `Evict{WaitUnsatisfied}`, as before.
  - When hot-waitable, it first runs `flush()` so the leader can schedule the intents. Flush errors are mapped to typed traps (see decisions).
  - It then takes a `ParkGuard` and loops on a biased `select!` over four branches:
    1. External cancel → `Evict{External}`.
    2. Budget token → `Evict{BudgetPressure}`.
    3. `inputs.recv()` → drain the channel, `deliver_inputs`, re-run `replay.wait`. `Resolved` returns the completions; `Pending` keeps waiting. A closed channel only disables this branch.
    4. `sleep_until(deadline)` → `Evict{HotWaitElapsed}`.
- **`limits.rs`**
  - `ParkedRegistry`: a map of park tick → `CancellationToken`, plus one eviction task on `pressure()`. While the budget is over, the task cancels the minimum-tick entry, increments `ostro_activation_budget_evictions_total`, and waits up to 250 ms for `released()`.
  - The task is spawned when a runtime is current, otherwise lazily by `park()`, and is restarted if it has finished. It holds only a `Weak` reference to the registry.
  - `park()` also notifies pressure when the budget is already over.
  - `NodeMemoryBudget::release` now calls `notify_waiters` on a new `released` Notify.
- **State/driver**
  - `HostInit.hot: Option<HotWait { inputs, cancel, parked, hot_wait }>`.
  - The driver builds `HotWait` from `Activate.inputs`.
  - `Runtime` owns the registry (`parked()`).
  - New shared `Runtime::host_init` is used by the driver and by `instantiate_probe`.
  - `classify` handles `Fenced` (new `Classified::Fenced`).
- **Testkit**
  - `Script::live(delay)`: per activation, `FakeSink::open_live` atomically snapshots the journal and registers a channel.
  - Each accepted `TimerStarted`/`EffectScheduled` spawns a delayed `commit_live`. It skips ops that are already answered, otherwise commits at seq = journal length and sends to the open activation.
  - Scripted inputs after an eviction are committed atomically (`commit_scripted`).
  - The stuck check also counts live completions.
- **Cached prepare (`runtime.rs`, `validate.rs`)**
  - `RuntimeOptions { cache_dir, instance_pre_capacity = 256 }`.
  - `prepare_cached` steps:
    1. L2 lookup by `module_sha`. A hit returns the same `Arc` and bumps its LRU tick.
    2. On a miss: `reject_core_module`.
    3. Load the component via `AotCache::component`, or `Component::from_binary` when there is no cache dir.
    4. `validate_loaded` with the production linker (on every load).
    5. `WorkflowPre`, then insert into L2, evicting the minimum tick.
  - Also new: `cache_stats()` and `instance_pre_len()`.
- **Replay core**
  - `max_ts` covers every journal event (loaded journal, delivered inputs, live appends). `append_live` stamps `max(clock, last_cmd_ts, max_ts)`.
  - `begin_step` (live) swaps in `RngStreams::new(step_seed(seed, seq))`, and `close_step` restores them.

## Verification

- `tests/hot_wait.rs`: 7 tests pass, stable across 5 consecutive runs (~0.5 s).
  - **One activation:** det-basic with hot_wait 10 s and live delivery at 50 ms completes in 1 activation and 2 append batches.
  - **Cold evict:** with hot_wait 1 s, the first activation returns `WaitUnsatisfied` in under 200 ms; the re-dispatch completes in 2 activations.
  - **Elapsed:** wit-surface `surface` with hot_wait 300 ms gives `HotWaitElapsed`, then the run completes; total time is between 250 ms and 2 s.
  - **Equivalence:** the hot run (1 activation) and the evict run (2 activations) have equal outputs, equal `(seq, kind, hash, encoded bytes)` with ts and fuel_used zeroed, and equal timestamps.
  - **Budget LRU:** the budget is set to 3.5× one instance's charge (1,114,112 B). With 3 runs parked, a 4th run causes the first to be evicted with `BudgetPressure`. The other two survive, and the 4th is never budget-evicted.
  - **External cancel:** cancelling a parked activation gives `External`.
  - **Bounded budget:** 50 concurrent runs under a 10× budget all complete with outputs equal to solo runs. 13 of them ran hot, and 37 budget evictions happened. `used()` returns to 0.
- `tests/runtime_cache.rs`: 5 tests pass.
  - L2 hit returns the same `Arc` (1 compile).
  - After a restart, 0 compiles, 1 hit, and the run output is identical.
  - wasi-forbidden is rejected both on compile and on the L1 hit, and never enters L2. A core module is rejected before any lookup.
  - LRU at capacity 2: smoke is evicted and comes back as an L1 hit.
  - Warm instantiate over 1,000 samples (pooling): **p50 272 µs, p99 473 µs** in a debug build. One-step activation p50 483 µs (upper bound for time to first begin-step).
- Regression gate: `cargo test -p ostro-engine` passes in every binary (lib 51, activation 11, cache 12, determinism 10, engine_config 3, host_adapter 7, hot_wait 7, pool 6, runtime_cache 5, sandbox 6, scaffold 1, stack_depth 2, validate 7, wasi_shim 8, wit_surface 17). `is-replaying == (activations > 1)` still holds.
- clippy `-D warnings` and fmt are clean for ostro-engine.
- Full `bash scripts/ci-local.sh` including musl was run in a scratch worktree at 62b3b97, with `CARGO_TARGET_DIR` pointing at the main tree's target. That isolates it from concurrent Phase 2 uncommitted files.
  - PASS: toolchain, fmt, clippy, warning-free build, `cargo test --workspace` (96 s), failpoint suites, deny, vendored, deny fixtures, banned deps, workflow validation, zig present, and `cargo-zigbuild test` on x86_64-musl (engine tests + nested wasm32-wasip2 guest build, including hot_wait and runtime_cache). No `tests/common/mod.rs` env scrubbing change was needed.
  - The "musl release" step printed FAIL only because its post-build check reads the binary from the relative `target/` path, which the override moved. The release build itself finished. Checked by hand: `file` reports "statically linked", and `build-info` reports `target=x86_64-unknown-linux-musl allocator=mimalloc`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Orchestrator-required fix] Logical time monotonic over all journaled events**
- **Found during:** pre-task (STATE.md Pending Todo, from the 03-09 observation)
- **Issue:** Live events were stamped `max(clock, last command ts)`, which ignores inputs. With a frozen clock, events after `TimerFired` were stamped before the fire time. With clock skew, `now()` after `sleep` could read earlier than the timer's fire time.
- **Fix:** `max_ts` over every journal event, included in the live stamp.
- **Tests:** unit test `time_monotonic_over_inputs` (frozen clock far behind the fire ts: journal ts never decrease, `now()` after the timer is >= the fire ts, on record, re-activation and replay). The golden test now asserts non-decreasing ts. `time_rule` was updated.
- **Re-bless:** `determinism__golden_det_basic_journal.snap` events 4–12 move from ts 1700000000000000000 to 1700000005000000000 (the TimerFired ts). Kinds, hashes and lengths are unchanged. Digests change because ts is encoded and s2/RunCompleted embed logical now.
- **Commit:** aa409ff

**2. [Rule 1 - Bug] Randomness inside live step closures leaked into the run-level streams**
- **Found during:** Task 1 (`hot_wait_journal_equivalence`)
- **Issue:** A step's closure runs only live and is skipped on replay. RNG it consumed (ctx random, uuid-v7, WASI random) advanced the run-level streams live but not on replay, so every later draw depended on activation boundaries. As a result, hot and evict runs journaled different loop-step bytes. It is also a latent replay-divergence hazard: a command derived from an out-of-step draw that follows a live step would diverge after eviction.
- **Fix:** step-local streams seeded `sha256("ostro/step-rng/v1" || seed || seq LE)` while a live step is open; the run-level streams are restored on complete/abandon/finish. Unit test `step_closure_randomness_does_not_leak`.
- **Re-bless:** only events whose bytes come from in-step randomness change digest: s1 (seq 1), s2 (5), loop-0..2 (6–8) and RunCompleted (12). Every kind, hash, ts and length is unchanged.
- **Commit:** e262a12

**3. [Rule 1 - Bug] Budget check at park time raced the LRU**
- **Found during:** Task 1 (`hot_wait_budget_lru`)
- **Issue:** With the plan's `!budget.is_over()` hot-waitability rule, the over-budget 4th run reached its wait before the eviction task ran. It evicted itself with `WaitUnsatisfied` and no parked run was evicted. This is racy, and it inverts LRU (the newest goes first).
- **Fix:** parking ignores the budget. `park()` wakes the LRU when the budget is over, so the oldest parked go first, and the newcomer goes only if nothing older is left.
- **Commit:** 8d1e354

**4. [Rule 3] Test adjustments**
- `hot_wait_elapsed_evicts` uses wit-surface `surface` (three in-flight effects), because det-basic's 5 s sleep is not hot-waitable at hot_wait = 300 ms. "Live delivery disabled" is a 1 h live delay. The run completes in 2 activations, because the re-dispatch fires every open timer.
- The equivalence compare zeroes `StepCompleted.fuel_used` (1054 vs 1048: the guest heap differs when s1 ran live in the same instance). The recorded outcome bytes are identical.
- Adding `RuntimeOptions` fields required `..RuntimeOptions::default()` in 6 test files. Adding `HostInit.hot` required `hot: None` in host_adapter.rs and wit_surface.rs.

## Known Stubs

None. Phase 4 provides the real input channel (the partition leader commits inputs and forwards them to `Activate.inputs`) and calls `prepare_cached` at deploy and activation.

## Requirements

- **ENG-09:** complete. Hot wait for short effects/timers, evict otherwise, replay on re-dispatch, and a bounded node budget with LRU eviction of parked activations (50 runs under a 10× budget).
- **ENG-07/ENG-08** were already complete. This plan wires them into the Runtime (`prepare_cached`) and re-proves warm instantiation with the full production linker (< 1 ms p50).

## Commits

- aa409ff fix(03-11): logical time monotonic over all journaled events, inputs included
- e262a12 fix(03-11): step-local randomness so live step closures never move run-level RNG
- 8d1e354 feat(03-11): hot wait, ParkedRegistry LRU budget eviction, live testkit inputs
- 62b3b97 feat(03-11): Runtime::prepare_cached (AOT L1 + InstancePre L2 LRU), warm p50

## Self-Check: PASSED

hot_wait.rs, runtime_cache.rs and this SUMMARY exist; commits aa409ff, e262a12, 8d1e354, 62b3b97 are in git log.
