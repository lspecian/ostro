---
phase: 07-workers-placement-flow-control
plan: 23
subsystem: dispatcher fair selection (ostro-partition leader/fair.rs, leader/dispatcher.rs)
tags: [gap-closure, dispatcher, throttle, flow-control, fairness, d-w13]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-05 FairQueues + dispatcher seams (incl. fix-0705 hold/release_held); 07-08 NodeFlow throttle (Block::QueueThrottle -> QueueBlocked{Some(until)}); 07-22 gate finding G1"
provides:
  - "FairQueues::gate_first(queue, until_ms) / gate_first_key(queue, key, until_ms) / ungate_all / is_first_gated: first-activation gates"
  - "FairEntry.first_activation"
  - "Dispatcher: throttle block on a first activation gates instead of skip_queue / park_key"
affects: [07-VERIFICATION, 16-*]
tech-stack:
  added: []
  patterns:
    - "Second per-band order over re-activation heads (react_order): a queue gate costs O(log n) to set or lift, no per-entry moves"
key-files:
  created: []
  modified:
    - crates/ostro-partition/src/leader/fair.rs
    - crates/ostro-partition/src/leader/dispatcher.rs
    - crates/ostro-partition/src/leader/flow.rs
    - crates/ostro-partition/tests/dispatcher_flow.rs
    - docs/flow-control.md
key-decisions:
  - "Throttle detection in the dispatcher = QueueBlocked/KeyBlocked with until_ms Some on a first activation (NodeFlow returns None for concurrency blocks and throttles only first activations); documented on the Admit contract, no new Admit variant"
  - "Key throttles (KeyBlocked{Some} on a first activation) get the same treatment as queue throttles: the key's re-activations stay servable (same D-W13 violation at key scope)"
  - "Gates lift at until_ms (via next_unpark_at -> existing deadline arm) or on any flow changed() bump (ungate_all next to unpark_keys/release_held); the gate deadline is clamped to >= now + 1 so it can never spin"
requirements-completed: [DISP-05]
metrics:
  duration: "~1h"
  completed: 2026-11-21
---

# Phase 7 Plan 23: Gap closure G1 (throttled first activation held back re-activations) Summary

**A throttle that blocks a first activation now holds back only first activations of that queue (or key), until `until_ms` or the next flow-control `changed()` bump. Re-activations queued behind it on the same partition, queue and key are granted at once, in their usual fair order. Concurrency blocks still skip the queue or park the key, as before.**

## Commits

| Hash | Message |
|------|---------|
| 272ff34 | fix(07-23): a throttled first activation no longer holds back re-activations (G1) |
| (docs commit) | docs(07-23): G1 gap closure summary, VALIDATION/STATE/ROADMAP |

## Cause

`NodeFlow::admit` returns `Admit::QueueBlocked { until_ms: Some(..) }` for `Block::QueueThrottle` (first activations only). The dispatcher answered every `QueueBlocked` with `fair.skip_queue`. A re-activation in the same queue on that partition (FIFO by `ready_at`, same key) was then never a candidate before `until_ms` (about 10 minutes in `flow_e2e::throttle_vs_rate_limit`). This broke D-W13 ("re-activations are never throttled"). `KeyBlocked{Some}` from a key throttle parked the whole key, which broke D-W13 in the same way at key scope.

## Design

- **`FairEntry.first_activation`** (F7 flag, also stored in the index `Loc`). `KeyState` keeps `reacts` (the re-activation subset of `items`) and an `rslot`. Each `Band` keeps a second order, `react_order`, by `(rank, head re-activation ready_at, run)`.
- **Queue gate** (`gate_first(queue, until)`): while it holds, `queue_candidate` searches `react_order` instead of `order`. Setting or lifting the gate is one map insert or remove, with no per-entry moves. Ranks, the band round and the queue turn are shared by both orders, so weighted DRR, strict bands and key round robin are unchanged. Gating charges nothing.
- **Key gate** (`gate_first_key(queue, key, until)`): the key's slot in `order` is filed by its first re-activation (or dropped if it has none). Its re-activations and every other key stay servable. A key park still wins over a key gate.
- **Lifting a gate**:
  - `unpark_due(now)` lifts expired gates. `next_unpark_at` includes the gate deadlines, so the existing `sleep_until_opt(deadline)` select arm wakes the dispatcher at `until_ms`. There is no polling and no new timer.
  - `ungate_all()` runs in the flow `changed()` arm, next to `unpark_keys` / `release_held`, and in `unpark_all`.
  - Emptying a queue or key drops its gate. `clear` drops everything.
- **No busy loop**: a gated first activation is probed again only on its deadline or a `changed()` bump (O(1) per event). The deadline is clamped to `until.max(now + 1)`. The dispatcher_flow tests check that nothing is re-probed during a quiet window.
- **No attempt consumed**: gating happens before reserve/grant, like every other block. The tests assert that the throttled run has `current_attempt == None`.
- **Gauges**: gated first activations count in `len` / `per_queue_counts` / `per_queue_oldest` and are left out of `per_queue_unblocked` (which feeds `ready_unblocked` and worker demand).
- The `QueueBlocked{None}` (concurrency), `Paused{Queue}`, `KeyBlocked{None}` and `Paused{Workflow}` paths are unchanged.

## Verification

| Command | Result |
|---------|--------|
| `cargo test -p ostro-partition --lib -- leader::fair` | 17 passed (2 new: `queue_gate_holds_only_first_activations`, `key_gate_holds_only_the_keys_first_activations`; the proptest now includes gate/ungate ops and checks that no gated first activation is ever proposed); proptest also at `PROPTEST_CASES=5000`: ok |
| `cargo test -p ostro-partition --lib --test dispatcher --test dispatcher_flow --test p7_contract -- --test-threads=1` | lib 39 passed (1 ignored), dispatcher 17, dispatcher_flow 15 (13 + 2 new), p7_contract 6 |
| `cargo test -p ostro-partition --features failpoints --test dispatcher_failpoints -- --test-threads=1` | 4 passed |
| Negative control: the new dispatcher_flow tests against the main-blob `fair.rs`/`dispatcher.rs`/`flow.rs` | both FAILED: `timed out waiting for 2 jobs; saw 1` (`dispatcher_flow.rs:106`) |
| `cargo test -p ostro-server --test flow_control --test auto_pause --test executor -- --test-threads=1` | 10 / 2 / 8 passed |
| `cargo test -p ostro-server --lib -- flow::` | 18 passed |
| `cargo test -p ostro --test flow_e2e -- --test-threads=1 --exact throttle_vs_rate_limit`, 15 sequential runs | **15 passed, 0 failed** (6.8–8.6 s each; before the fix: 3 of 7 failed) |
| `cargo test -p ostro --test flow_e2e` (full, default parallel) | 11 passed (128 s) |
| `cargo clippy -p ostro-partition -p ostro-server -p ostro --all-targets --locked -- -D warnings`; also `-p ostro-partition --features failpoints` | clean |
| `cargo fmt --all --check` | clean |

## Deviations from Plan

1. **[Rule 2 - Missing critical] Key throttle too.** `KeyBlocked{Some}` on a first activation parked the whole key, so a re-activation on that key waited for `until_ms` (the same D-W13 violation). It is fixed with the same gate mechanism at key scope and has its own dispatcher_flow test.
2. **[Docs] `docs/flow-control.md`** §3/§4 now say that a throttle holds back only first activations and that re-activations behind it are still granted.

## Issues

None. The first private-index commit's worktree `git checkout` step aborted after the CAS had already succeeded (the worktree index was stale). `main` was correct. The worktree was moved with a mixed `git reset` (files identical), and the commit script now uses that.

## Self-Check: PASSED
- FOUND: crates/ostro-partition/src/leader/{fair,dispatcher,flow}.rs, tests/dispatcher_flow.rs changes in 272ff34
- FOUND: commit 272ff34 on main
