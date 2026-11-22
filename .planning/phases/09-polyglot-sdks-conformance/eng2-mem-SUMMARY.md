---
phase: 09-polyglot-sdks-conformance
plan: eng2-mem
subsystem: engine
tags: [eng-2, admission, pooling, quickjs, memories, tables]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-07 ENG-2 CoreBudget; 09-19 QuickJS memory-ceiling measurement and open issues 1-2"
provides:
  - "ostro_engine::core_budget::{PoolBudget, PoolWeights, PoolResource, PoolPermit, PoolBudgetError}"
  - "Runtime::pool_budget(), Runtime::admission_weights(), Prepared.{memories, tables, min_memory_bytes}"
  - "EngineConfig::effective_total_memories/effective_total_tables"
affects: [09-20]
key-files:
  modified:
    - crates/ostro-engine/src/core_budget.rs
    - crates/ostro-engine/src/runtime.rs
    - crates/ostro-engine/src/config.rs
    - crates/ostro-engine/tests/core_budget.rs
    - crates/ostro-conformance/tests/polyglot_engine.rs
    - crates/ostro-conformance/tests/resources.rs
    - docs/sdk-resources.md
key-decisions:
  - "Fix in admission (coordinator decision), pool reservation unchanged: one weighted FIFO budget per pooled resource (component instances, core instances, memories, tables), acquired in that fixed order with one deadline. Chosen over a single max-of-fractions budget because it keeps core_budget() and its slot numbers intact and is exact, not conservative"
  - "Memories/tables are static and exact via wasmtime Component::resources_required (walks every statically instantiated core module, adapters included); no calibration needed. Fallback when None (core-module imports, ruled out by the allow-list): 1 memory, class tables_per_instance"
  - "Component-instance slots (weight 1) are budgeted too: under class=mixed the core budget alone admitted 12.7x more Rust instances than total_instances; also bounds total_stacks"
  - "Python/Mixed min_memory_bytes = 16,711,680 (SDK guest, 255 pages)"
duration: ~1h
completed: 2026-11-22
---

# Plan eng2-mem: admission over every pooled resource

**ENG-2 admission now charges component-instance slots, core instances, linear memories and tables. With `class = mixed, total_instances = 60`, 30 QuickJS runs give 20 parked and 10 waiting, 0 `AttemptFailed` (09-19 measured 19–20 parked and the rest failing with "memory stripe 0"). The default config snapshot is unchanged, and Rust-only defaults admit exactly as before (all four budgets bind at 1,000).**

## Design

- `PoolBudget` holds one `CoreBudget` (weighted FIFO tokio semaphore) per `PoolResource`:
  - instances, capacity `total_instances`;
  - core instances, capacity `effective_total_core_instances`;
  - memories, capacity `effective_total_memories = total_instances`;
  - tables, capacity `effective_total_tables = total_instances × 2`.
- `acquire(PoolWeights, deadline)`:
  - checks `TooLarge` for every resource first, so nothing is held when it fails;
  - then acquires in the fixed `ORDER` with one deadline;
  - partial permits drop on timeout or cancellation.
  - Deadlock-free: every waiter acquires in the same order.
- `Runtime::activate`, `describe`, `instantiate_probe` and the core-instance calibration all go through `admit(p, deadline)` with `PoolWeights {1, calibrated core weight, p.memories, p.tables}`. The existing semantics carry over to every resource:
  - deadline: `Evicted{BudgetPressure}`;
  - cancel: `Evicted{External}`, exactly one `done`;
  - describe/probe: 5 s wait, then `EngineError::Wasmtime("<resource> budget: ...")`.
- `pre_instantiate` reads `Component::resources_required` and stores `memories`, `tables` and `min_memory_bytes` on `Prepared`. A module needing more memories or tables than the pool holds is a validation error.
- `Runtime::core_budget()` is the core-instance sub-budget, so existing callers and slot numbers are unchanged.
- `BudgetError` messages lost their "core-instance budget:" prefix, which now comes from `PoolBudgetError` ("core-instance budget: no 3 free slots ...", "memory budget: ...").

## Measured shapes (SDK conformance guests)

| | Rust | Python | QuickJS | StarlingMonkey |
|---|---|---|---|---|
| weights (inst / core / mem / tables) | 1 / 3 / 1 / 2 | 1 / 38 / 1 / 2 | 1 / 13 / 3 / 3 | 1 / 3 / 1 / 2 |
| declared memory floor | 1,114,112 | 16,711,680 | 2,228,224 | 9,502,720 |

## Verification

- `cargo test -p ostro-engine`: all green. `core_budget` has 12 tests, 4 of them new:
  - `pool_budget_admits_only_when_every_resource_fits`;
  - `pool_budget_rejects_too_large_without_holding_anything`;
  - `pool_slots_not_core_instances_bound_admission`;
  - `exhausted_pool_slots_evict_with_budget_pressure`.
- `engine_config` (snapshot unchanged), `exports_shape`, `pool`, `activation`, `hot_wait` and the lib tests (97) pass.
- `OSTRO_POLYGLOT_CACHE=target/polyglot-guests cargo test -p ostro-conformance --features polyglot --test polyglot_engine`: 7/7, including the new tests:
  - `quickjs_memories_wait_for_admission`;
  - `sdk_guest_shapes_pin_class_memory_floors`.
- **Regression check:** with the memories, tables and instances weights temporarily zeroed (the old admission), `quickjs_memories_wait_for_admission` fails with `AttemptFailed(Trap("instance pool exhausted (engine.pool.total_instances=60): maximum concurrent limit of 60 for memory stripe 0 reached"))`.
- `resources.rs pool_mixed` (ignored; run once in debug, JSONL line discarded):
  - QuickJS probe: 20 parked, 10 waiting, 0 failed;
  - mixed case: 4,750 / 38,000 core slots in use, 0 failures;
  - default class: 100 Python runs complete.
- `cargo clippy -p ostro-engine --all-targets -D warnings`, `cargo clippy -p ostro-conformance --features polyglot --tests -D warnings` and `cargo fmt --all --check` are clean.

## Open issues

- 09-19 open issue 3 is not changed: `class = typescript` sizes core instances for StarlingMonkey (230 QuickJS instances at defaults). The docs now cap the override guidance at the memories limit (`13 × total_instances / 3`).
- The published `docs/data/sdk-resources.jsonl` still holds the 09-19 pre-fix QuickJS records. The doc table states both results. A release re-run on the perf machine would add post-fix lines.
