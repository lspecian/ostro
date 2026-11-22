---
phase: 09-polyglot-sdks-conformance
plan: 18
subsystem: conformance-crash-leg
tags: [crash-leg, kill-9, failpoints, children, cancel, wf-06, sdk-05, sc3, wave-8]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-08 child:: failpoints in sink.rs; 09-14/09-15 Python/TS conformance guests; 09-16 C-CHILD-08 chain; 09-17 build recipe (released componentize-py wheel)"
  - phase: 05-workflow-semantics-core-hardening
    provides: "05-18 failpoint binary recipe (cargo build --release -p ostro --features failpoints --target-dir target/failpoints); 05-19 spawned-server pattern (ready line, kill -9, OSTRO_FAILPOINT_BIN)"
provides:
  - "crates/ostro-conformance/tests/crash_leg.rs: spawned failpoint `ostro dev` crash leg (crash_chain_completes = C-CRASH-01, crash_cancel_propagates = C-CRASH-02), #[ignore]"
  - "conformance/spec/crash/C-CRASH-01.json, C-CRASH-02.json (catalogue entries no longer pending)"
affects: [09-19, 09-20]
tech-stack:
  added: []
  patterns:
    - "Failpoint crash marker in a spawned process: `pause` + journal observation (child_started) for a post-flush site; `1*print(marker)` through the log->tracing bridge for sites without a journal-visible marker"
    - "Kill delays draw half of their samples in the measured in-flight window so random kills actually land mid-chain"
key-files:
  created:
    - crates/ostro-conformance/tests/crash_leg.rs
    - conformance/spec/crash/C-CRASH-01.json
    - conformance/spec/crash/C-CRASH-02.json
  modified: []
key-decisions:
  - "Failpoint crash points use `pause` (start_child_after_flush, marker: root journal shows child_started) and `1*print(crash-leg hit <site>)` (parent_event_before_flush, all-three mode) instead of `1*return`: the child:: sites' `return` is mapped by the engine to Evicted{External} without logging, so it gives no marker in a spawned process"
  - "child::detached_after_flush is unreachable from an attached chain; it is armed only in the all-three mode and its coverage stays with 09-08's children_failpoints"
  - "Half of the random kill delays fall in [0, 80] ms (chain) / [0, 40] ms (cancel cascade): with the plan's uniform [0, 1500] / [0, 500] alone almost no kill lands while the runs are in flight (0 of 21 in a 10-iteration trial)"
requirements-completed: [WF-06, SDK-05]
duration: ~30 min of work (12:37 to ~13:10 CET), including a 3m45s 100-iteration gate run
completed: 2026-11-22
---

# Phase 9 Plan 18: Crash leg SC3 Summary

**A TS → Python → Rust child chain on a spawned failpoint `ostro dev` was run 100 times under 202 kill -9 (50 of them at `child::` failpoint sites) and completed exactly once per level every time. A further 100 iterations killed the server around a root cancel: the cancel reached the Python child and the Rust grandchild every time, and the Rust leaf's cleanup step was journaled exactly once. 0 violations.**

## Accomplishments

### Harness (`crates/ostro-conformance/tests/crash_leg.rs`)
- `#![cfg(feature = "polyglot")]`. Both tests are `#[ignore]` because they spawn processes and are slow.
- **Server binary:** uses `OSTRO_FAILPOINT_BIN` if set, otherwise builds with the 05-18 recipe. It asserts that `build-info` reports `failpoints=on`.
- **Spawn:** runs `ostro dev --data-dir target/crash-leg/<test>-<seed> --listen 127.0.0.1:0`.
  - Inherited `OSTRO_*`, `RUST_LOG` and `FAILPOINTS` are stripped.
  - The address comes from the ready line, then the harness waits for `/readyz` (unarmed servers only).
  - stdout/stderr go into a ring buffer, and marker lines are counted.
  - `kill9()` checks that the wait status really is signal 9.
- **REST client:** deploy, start (with an idempotency key), cancel, run, result long-poll, paged history, paged listing. Calls retry connection errors and 5xx across restarts; 4xx answers are final.
- **Child ids:** derived with `ostro_server::children::child_run_id(parent, child_started seq, P)`. `P` (16) is read from the server's start line.
- **Seeds:** each test uses a fresh, printed seed (splitmix64). No new env var was added; `OSTRO_CRASH_CHAIN_ITERS` is the only P9 name used.
- **On failure:** the iteration's data dir is moved to `target/crash-leg/failed-<test>-<iter>`, and the panic reports seed, reason and the server log tail.
- **Inputs:** both specs are loaded from `conformance/spec/crash/`. The input `wf:`/`wf2:` refs are resolved with `server_leg::substitute` for roles TS, Python, Rust, and the iteration number is added as `"v"`.

### C-CRASH-01: `crash_chain_completes`
Each iteration starts `ts_conf_parent_chain {"next": ["py_conf_parent_chain", "rs_conf_leaf_echo"], "v": n}`. Modes rotate every 6 iterations: random, start_child, random, parent_event, random, all.
- **random:** 1 to 3 kill -9 with restart. Each kill comes after a delay measured from the start ack or the previous restart: uniform in [0, 1500] ms, but half of the draws fall in [0, 80] ms.
- **`child::start_child_after_flush`:** the restart before the root starts is armed with `pause`. As soon as the root's journal shows `child_started`, the sink is blocked at the site; the harness then kills after a random 0 to 20 ms and restarts unarmed.
- **`child::parent_event_before_flush`:** armed with `1*print(crash-leg hit <site>)`. The leaf's terminal append logs the marker, and the harness kills as soon as it reads the line.
- **all:** all three sites are armed with `print`; the first marker (the root's child start) triggers the kill.

After the last restart the test checks:
- the root completes within 60 s with `{"child":{"child":{"echo":{"next":[]}}}}`;
- the root and Python histories each have exactly one `child_started` and one `child_completed`;
- the Python and Rust runs at the derived ids are completed, run the expected workflow and return the expected results;
- each of the three runs has exactly one `run_started` and one terminal event;
- `GET /v1/runs?workflow=` lists exactly one `py_conf_parent_chain` and one `rs_conf_leaf_echo` run per root started so far, including the derived ids;
- every listed run of the chain workflows is terminal.

### C-CRASH-02: `crash_cancel_propagates`
Each iteration starts the chain on `rs_conf_leaf_sleep`. The harness follows the derived ids down to the leaf and waits until its journal shows `timer_started`. Modes rotate every 3 iterations:
- kill -9 and restart, then cancel;
- cancel, then kill -9 after 0 to 500 ms (half of the draws in [0, 40] ms) and restart;
- cancel with no kill.

The test then checks:
- root, Python mid and Rust leaf are all `cancelled`;
- the mid's derived child is the leaf that was seen sleeping;
- the leaf journals the `cleanup` step exactly once;
- each run has exactly one `run_started` and exactly one terminal event, which is `run_failed` with type `cancelled`;
- the listing shows one Python and one Rust run per root, and nothing is left non-terminal.

## Gate run (SC3 evidence)

Command (load average about 5 at start; slot target dir; prebuilt failpoint binary via `OSTRO_FAILPOINT_BIN`):

```
OSTRO_FAILPOINT_BIN=<slot>/release/ostro OSTRO_POLYGLOT_CACHE=target/polyglot-guests \
OSTRO_CRASH_CHAIN_ITERS=100 cargo test -p ostro-conformance --features polyglot --locked \
  --test crash_leg -- --ignored --test-threads=1 --nocapture
```

`test result: ok. 2 passed; 0 failed`, 225.0 s in total.

| Test | Iterations | Seed | Injections | Kills / restarts | Median time to final state | Violations |
|---|---|---|---|---|---|---|
| C-CRASH-01 | 100 (50 random, 50 failpoint) | `0x18ff674a53523a79` | failpoint hits start_child_after_flush 17/17, parent_event_before_flush 17/17, all 16/16; 43 random kills landed while the root was still running | 202 kill -9 / 203 restarts | 110.9 ms after the last restart | 0 |
| C-CRASH-02 | 100 (34 kill before cancel, 33 kill after cancel, 33 no kill) | `0x18ff673ac715277f` | 7 of the after-cancel kills landed before the leaf was cancelled | 67 kill -9 / 68 restarts | 43.6 ms after the last restart or cancel | 0 |

Times: C-CRASH-01 158.1 s, C-CRASH-02 66.7 s.

Earlier runs of the final code also passed:
- 10 iterations, C-CRASH-01 alone (Task 1 verify);
- 10 iterations, both tests together (Task 2 verify);
- one 12-iteration trial.

## Task Commits

Each commit was made through the private index `index-09-18` and moved onto main with a CAS update-ref.

1. **Task 1: harness + C-CRASH-01:** `5b20eac` (test). This file version has no C-CRASH-02 section; it passed clippy `-D warnings` and the 10-iteration verify on its own.
2. **Task 2: C-CRASH-02 + kill-window delays:** `c6709cb` (test).
3. **Plan metadata (SUMMARY, ROADMAP, STATE):** the final docs commit.

## Verification

- Task 1 verify: `OSTRO_CRASH_CHAIN_ITERS=10 ... crash_chain_completes` gave `test result: ok. 1 passed`.
- Task 2 verify: `OSTRO_CRASH_CHAIN_ITERS=10 ...` (both tests) gave `test result: ok. 2 passed`.
- `cargo clippy -p ostro-conformance --features polyglot --all-targets --locked -- -D warnings` is clean.
- `cargo test -p ostro-conformance --features polyglot --test spec_catalogue` gave `4 passed`. The crash specs parse against the schema (unknown top-level fields are rejected; the extra keys live in `expect`).
- The failpoint binary was built as `cargo build --release -p ostro --features failpoints --locked` into the agent's slot target dir and passed through `OSTRO_FAILPOINT_BIN`, so `target/failpoints` was not created in the worktree. Without the env var the test builds it there, as in 05-18.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 - Blocking] There is no usable "pause marker" for the `child::` failpoints in a spawned process**
- With `1*return`, the site returns `AppendError::Unavailable("... (injected)")`, but the engine maps it to `Evicted{External}` without logging the message. A first trial showed 0 hits out of 3 armed runs.
- The fix:
  - `start_child_after_flush` uses `pause`, with a journal-visible marker: the flush precedes the site, so `child_started` in the root's history means the sink is blocked at the site, and the kill lands exactly there.
  - `parent_event_before_flush` and the all-three mode use `1*print(...)`. The `fail` crate logs through `log`, and the server's tracing subscriber forwards that. The kill follows within milliseconds of the line.
- No server or checker code was touched.

**2. [Rule 1 - Test weakness] Random kill delays almost never hit a running chain**
- The chain completes in well under 100 ms and the cancel cascade in about 40 ms.
- With the plan's uniform [0, 1500] / [0, 500] ms delays, a 10-iteration trial landed 0 of 21 kills with the root still running, and 0 kills mid-cascade.
- Half of the draws now fall in [0, 80] / [0, 40] ms, and the random mode runs every other iteration (rotation of 6 instead of 4). The reports count kills that landed in flight: 43 of 202 in the chain gate run, and 7 of 33 after-cancel kills.

**3. [Plan input] `"rest": {"v": n}` is not forwarded by the chain guests**
- `conf_parent_chain` passes only `{"next": rest}` to the child in all three languages, and guests could not be edited.
- The root input carries `"v": n` for distinctness, and the expected result stays the spec's fixed nested echo.

**4. [Plan detail] Shared data dir per test instead of per iteration**
- One data dir per test run, `target/crash-leg/<test>-<seed>`, keeps the three modules deployed and compiled across iterations. Crashes therefore accumulate on one node.
- The per-root checks use the deterministic child ids plus "one listed run per root so far".
- A failing iteration keeps the whole dir as `target/crash-leg/failed-<test>-<iter>`.

**5. [Plan detail] `child::detached_after_flush` cannot fire in an attached chain**
- It is armed only in the all-three mode, and the report shows it never fires alone.
- Its behaviour stays covered by 09-08's `children_failpoints` (in FAILPOINT_CRATES since 09-17).

### Observations (not deviations)
- A cancelled root is terminal before its descendants: its journal resolves the CANCEL wait without a `child_completed`, and the mid and leaf follow through the cascade. The checker reads the descendants' histories only after they are terminal. This matches C-CHILD-08's cancel variant.
- No product bug was found. The crash checker (`ostro-crashtest/src/check.rs`) is not used or changed.

## Issues Encountered
- An early draft found the partition count with a probe chain. That was ambiguous: P=2 and P=16 derive the same id whenever the derived partition is below 2. It was replaced by parsing `partitions: N` from the server start line.

## Next Phase Readiness
- SC3 has evidence. 09-19/09-20 can cite the gate table above.
- The crash leg is not in ci-local or ci.yml (it is `#[ignore]` and its command is documented in the test module docs). Adding it to CI is a decision for the phase owner.

## Self-Check: PASSED
- `crates/ostro-conformance/tests/crash_leg.rs`, `conformance/spec/crash/C-CRASH-01.json` and `conformance/spec/crash/C-CRASH-02.json` exist on main.
- Commits `5b20eac` and `c6709cb` are on main.
