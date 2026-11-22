---
phase: 09-polyglot-sdks-conformance
plan: 16
subsystem: conformance
tags: [conformance, cross-language, children, saga, effects, determinism, wf-06, sdk-05, wave-6]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-13 server leg (run_spec with positional roles, wf:/wf2: refs, MockProvider); 09-14 Python guests; 09-15 TS guests (QuickJS + StarlingMonkey); 09-11 engine leg + C-VAL-01"
provides:
  - "conformance/spec/server/C-CHILD-08.json: TS root -> Python child -> Rust grandchild, nested result + cancel-all-levels variant"
  - "conformance/spec/server/C-SAGA-03.json: conf_saga_http (HTTP bookings + reverse HTTP compensations) in every language"
  - "crates/ostro-conformance/tests/polyglot_cross.rs: cross_chain, saga_http, values_cross_language, full_table"
affects: [09-17, 09-18, 09-20]
tech-stack:
  added: []
  patterns:
    - "Cross-language chains are plain server specs run with roles [Ts, Py, Rust]; the test checks each level's module through the recorded child edges"
    - "Checks that depend on the run id (per-key charges, effect key order) live in the test, next to the spec, because the spec schema cannot express them"
key-files:
  created:
    - conformance/spec/server/C-CHILD-08.json
    - conformance/spec/server/C-SAGA-03.json
    - crates/ostro-conformance/tests/polyglot_cross.rs
  modified: []
key-decisions:
  - "C-CHILD-08's cancel case is a `variants` entry of the same spec (grandchild conf_leaf_sleep), mirroring C-CHILD-09's expectations, so one spec covers result propagation and cancellation"
  - "C-SAGA-03's per-key checks (a, b, refund-b, refund-a in journal order; 1 request + 1 charge per key) are in polyglot_cross.rs::saga_checks: the keys are idempotency_key_for(root run id, name) and the spec's fx_pay extra only handles one key of a completed result"
  - "values_cross_language adds two extra (seed, run id) inputs to the pinned seed 42, and asserts they give distinct values, so equality is not an artefact of one input"
requirements-completed: [SDK-05, SDK-04, WF-06, WF-10, SDK-02, SDK-03]
duration: ~40 min
completed: 2026-11-22
---

# Phase 9 Plan 16: Cross-language conformance Summary

**A TypeScript parent starts a Python child, which starts a Rust grandchild. The nested result `{"child":{"child":{"echo":{"next":[]}}}}` reaches the TS root, and cancelling the TS root cancels all three levels, with the Rust grandchild's cleanup step run. The HTTP saga (C-SAGA-03) compensates in reverse order with exactly one MockProvider charge per key in Rust, Python and TypeScript. The C-VAL-01 values are byte-identical across Rust, Python, TS on QuickJS and TS on StarlingMonkey for three seed and run-id inputs. In the full scenario × language table, all 135 cells pass across 43 scenarios. No SDK, engine or server deviation was found, so no product code changed.**

## Commits

| Task | Commit | Content |
|------|--------|---------|
| 1 | `e9dd7a2` | C-CHILD-08 + C-SAGA-03 specs, `polyglot_cross.rs` (`cross_chain`, `saga_http`) |
| 2 | `3045fb0` | `polyglot_cross.rs` (`values_cross_language`, `full_table`) |

## C-CHILD-08: TS → Python → Rust

How it runs:
- `ServerHarness::run_spec(spec, &[Ts, Py, Rust])` runs `ts_conf_parent_chain` with `next = [wf:conf_parent_chain, wf2:conf_leaf_echo]`. That resolves to `py_conf_parent_chain`, then `rs_conf_leaf_echo`.
- The cancel variant swaps the grandchild for `rs_conf_leaf_sleep`. Once the grandchild's `timer#1` exists, it cancels the TS root with reason `conformance`.

What the spec and test check:
- **Result:** root status `completed`. The root, child and grandchild results are all asserted, along with their projections and parent links.
- **Cancellation:** all three levels end `cancelled`. The grandchild's projection is `timer#1, step:cleanup, terminal:cancelled`.
- **Comparison with Rust:** the same spec runs all-Rust on a separate harness. Status, failure kind and root facts are compared.
- **Language per level:** every recorded child edge is checked to run in its level's module. Level 1 must be `py_conf_parent_chain`. Level 2 must be an `rs_` workflow. That gives 4 edges: two levels for each of the two root runs.
- **One run per op:** `check_children_unique` finds exactly one child run per (parent, op).

## C-SAGA-03: HTTP saga in every language

The spec covers status `failed`, failure kind `book_c_failed`, and the projection `effect:http#1, effect:http#2, step:book_c, effect:http#3, effect:http#4, terminal:failed`.

`saga_checks` adds two checks for each language:
- The journal's HTTP effects carry `idempotency_key_for(root, "a" | "b" | "refund-b" | "refund-a")` in exactly that order, so `refund-b` runs before `refund-a`.
- The mock saw exactly 1 request and 1 charge per key.

Across the whole harness, `/pay` received 12 requests in total, 4 per language. Python and TS are also compared with Rust on status, failure kind and facts.

## C-VAL-01: value probes

The values are canonical JSON and identical across all four backends: `rust`, `py`, `ts` (QuickJS) and `ts-sm` (StarlingMonkey).

| seed | run id | value (all 4 backends) |
|------|--------|------------------------|
| 42 (spec) | `01890000-0000-7000-8000-000000000001` | `{"idempotency_key":"ostro-43ff999e924f58fa3b089b017dab68df","random_f64":0.8693496500947742,"random_u64":"16357943517170225811","uuid_v7":"018bcfe5-6800-7e6d-b89c-fc7a01337ce6"}` |
| 7 | `01890000-0000-7000-8000-0000000000aa` | `{"idempotency_key":"ostro-8b2afe375bc2b9cda74f7fdc2cb37b2a","random_f64":0.7998196682576197,"random_u64":"6751122108365557993","uuid_v7":"018bcfe5-6800-7b5c-9856-6c0a31437f9b"}` |
| 18446744073709551557 | `0193a1b2-c3d4-7e5f-9a0b-1c2d3e4f5a6b` | `{"idempotency_key":"ostro-cf3274d7d2bbb59f8050486024b5848d","random_f64":0.9444927643041874,"random_u64":"14888874563485712985","uuid_v7":"018bcfe5-6800-7f4c-8c44-dae0d4b248a0"}` |

Every engine-leg strict check ran for each backend: two executions are byte-identical, and replay appends 0 events.

## Scenario × language table (SC1 evidence)

This table is the output of `full_table`. Every cell is a pass of the runner's strict checks and the spec expectations, plus a comparison with Rust:
- **Engine leg:** status, canonical outcome and projection. C-NATIVE-01 compares facts only.
- **Server leg:** status, failure kind and root facts.

"-" means the catalogue does not list that language for the scenario. ts-sm is listed only for the D-P9-1 smoke subset. C-VAL-01 on ts-sm is covered by `values_cross_language` instead.

The server column for a language runs every role in that language. The last column is the cross-language chain. Overall, 135/135 cells pass in 51 s with cached guests. One server harness ran all 16 server scenarios, and it found exactly one child run per (parent, op) across 58 child starts.

| Scenario | Leg | Status | rust | py | ts | ts-sm | ts->py->rust |
|---|---|---|---|---|---|---|---|
| C-STEP-01 | engine | completed | pass | pass | pass | pass | - |
| C-STEP-02 | engine | completed | pass | pass | pass | - | - |
| C-STEP-03 | engine | completed | pass | pass | pass | - | - |
| C-STEP-04 | engine | completed | pass | pass | pass | - | - |
| C-NATIVE-01 | engine | completed | pass | pass | pass | - | - |
| C-RETRY-01 | engine | completed | pass | pass | pass | - | - |
| C-RETRY-02 | engine | failed | pass | pass | pass | - | - |
| C-TIMER-01 | engine | completed | pass | pass | pass | - | - |
| C-TIMER-02 | engine | completed | pass | pass | pass | - | - |
| C-SIG-01 | engine | completed | pass | pass | pass | pass | - |
| C-SIG-02 | engine | completed | pass | pass | pass | - | - |
| C-RACE-01 | engine | completed | pass | pass | pass | pass | - |
| C-RACE-02 | engine | completed | pass | pass | pass | - | - |
| C-CANCEL-01 | engine | cancelled | pass | pass | pass | pass | - |
| C-CANCEL-02 | engine | cancelled | pass | pass | pass | - | - |
| C-CANCEL-03 | engine | completed | pass | pass | pass | - | - |
| C-PATCH-01 | engine | completed | pass | pass | pass | - | - |
| C-PATCH-02 | engine | completed | pass | pass | pass | - | - |
| C-NDET-01 | engine | non_determinism | pass | pass | pass | - | - |
| C-RAND-01 | engine | completed | pass | pass | pass | pass | - |
| C-RAND-02 | engine | completed | pass | pass | pass | - | - |
| C-VAL-01 | engine | completed | pass | pass | pass | - (see values_cross_language) | - |
| C-CAN-01 | engine | completed | pass | pass | pass | - | - |
| C-TOKEN-01 | engine | completed | pass | pass | pass | - | - |
| C-STATUS-01 | engine | completed | pass | pass | pass | - | - |
| C-SAGA-01 | engine | failed | pass | pass | pass | - | - |
| C-SAGA-02 | engine | cancelled | pass | pass | pass | - | - |
| C-CHILD-01 | server | completed | pass | pass | pass | - | - |
| C-CHILD-02 | server | completed | pass | pass | pass | - | - |
| C-CHILD-03 | server | completed | pass | pass | pass | - | - |
| C-CHILD-04 | server | completed | pass | pass | pass | - | - |
| C-CHILD-05 | server | completed | pass | pass | pass | - | - |
| C-CHILD-06 | server | completed | pass | pass | pass | - | - |
| C-CHILD-07 | server | completed | pass | pass | pass | - | - |
| C-CHILD-09 | server | cancelled | pass | pass | pass | - | - |
| C-CHILD-10 | server | completed | pass | pass | pass | - | - |
| C-CHILD-11 | server | completed | pass | pass | pass | - | - |
| C-SIG-API-01 | server | completed | pass | pass | pass | - | - |
| C-TOKEN-API-01 | server | completed | pass | pass | pass | - | - |
| C-CAN-02 | server | completed | pass | pass | pass | - | - |
| C-FX-01 | server | completed | pass | pass | pass | - | - |
| C-CHILD-08 | server | completed | pass | pass | pass | - | pass |
| C-SAGA-03 | server | failed | pass | pass | pass | - | - |

Some catalogue entries are not in the table:
- C-REBUILD-01 (kind rebuild) belongs to 09-17.
- C-CRASH-01/02 (crash leg) belong to 09-18.

## SDK deviation log

None. All four tests passed on their first run. No file under `sdks/python/**` or `sdks/ts/**` changed, so `test_regressions.py` and `regressions.test.ts` were not created.

## Spec changes

None to existing specs. The two new specs are the C-CHILD-08 and C-SAGA-03 files.

## Deviations from Plan

1. **C-SAGA-03 per-key assertions live in the test, not the spec.** The keys depend on the run id, and the spec schema has no multi-key extra. The 09-13 `fx_pay` extra handles a single key on a completed result. `crates/ostro-conformance/src/**` is off-limits, so no new extra was added. The spec pins status, failure kind and the effect/step order. `saga_checks` pins the key order and the one-charge-per-key rule. `full_table` and `saga_http` both run it.
2. **C-CHILD-08 cancellation is a variant of the spec**, not a second spec file, because a second file would need an ordinal or name the catalogue does not have. The coverage is the same.
3. **`cross_chain` uses two harnesses:** one for the all-Rust reference and one for TS→Py→Rust. This lets the per-level language check see only cross-language edges.
4. **Not run:** `scripts/ci-local.sh` and the whole-workspace suite. This follows the load rule of this session; the 09-17/09-20 gates own them. The plan changes only new spec files and a new test file.

## Verification

- `cargo test -p ostro-conformance --features polyglot --locked --test polyglot_cross -- --test-threads=2`: **test result: ok. 4 passed**, 51 s (cached guests). No `FAILED`.
- Filtered `-- cross_chain saga_http`: **ok. 2 passed**, 7.5 s.
- `cargo clippy -p ostro-conformance --features polyglot --all-targets --locked -- -D warnings`: clean. `cargo fmt -p ostro-conformance -- --check`: clean.
- Regression: `cargo test -p ostro-conformance --locked --test server_leg --test spec_catalogue`: ok (7 and 4 passed). The new specs parse and belong to the catalogue. `full_table` re-runs every Python and TS engine and server scenario against Rust, which covers the `polyglot_python`/`polyglot_ts` legs.
- Builds ran with `CARGO_TARGET_DIR=target/slot-3`, `nice -n 10` and `CARGO_BUILD_JOBS=4`. Guests came from `OSTRO_POLYGLOT_CACHE`.

## Open issues

- None blocking.
- 09-17 (ci-local polyglot block) should add `--test polyglot_cross` next to `polyglot_python` and `polyglot_ts`. `full_table` overlaps them, so CI can run only `polyglot_cross` if time matters.

## Self-Check: PASSED

- FOUND: conformance/spec/server/C-CHILD-08.json, conformance/spec/server/C-SAGA-03.json, crates/ostro-conformance/tests/polyglot_cross.rs.
- FOUND: e9dd7a2, 3045fb0 on main.
