---
phase: 09-polyglot-sdks-conformance
plan: 19
subsystem: conformance
tags: [sc5, performance, memory, pooling, eng-2, python, typescript, quickjs, starlingmonkey]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-07 ENG-2 CoreBudget + [engine] class; 09-11/14/15 conformance guests (conf_steps, conf_timer); 09-04 S9 numbers"
provides:
  - "crates/ostro-conformance/tests/resources.rs: #[ignore] SC5 harness (warm_instantiate, idle_rss, pool_mixed), OSTRO_SC5_ENFORCE=1 gate"
  - "docs/data/sdk-resources.jsonl: two release runs (WSL2, loadavg per line)"
  - "docs/sdk-resources.md: per-language/backend resource tables, S2/S9 comparison, pool sizing guidance, SC5 verdict"
affects: [09-20]
tech-stack:
  added: []
  patterns:
    - "start-to-first-step timed by an ActivationSink wrapper that stamps the first StepCompleted append"
    - "idle memory reported as statm RSS plus smaps_rollup PSS/Anonymous (CoW memory-image pages are counted once per pool slot in RSS)"
key-files:
  created:
    - crates/ostro-conformance/tests/resources.rs
    - docs/data/sdk-resources.jsonl
    - docs/sdk-resources.md
  modified: []
key-decisions:
  - "No product code changed. The QuickJS memories/tables pool-sizing gap (09-07 open issue) is measured and documented, not fixed: the plan forbids product changes, and the fix (3x total_memories/tables per class, or memories in the admission budget) trades pool address-space reservation and needs a decision"
  - "Idle memory is published as RSS (statm, the plan's metric) and PSS/Anonymous, because RSS charges about 9 MB of Python's shared copy-on-write image to every instance"
  - "conf_timer parks hot only if the hot-wait window covers its 1 h timer, so the parking runtime uses hot_wait = 2 h with the frozen FakeClock"
requirements-completed: []
duration: ~55min
completed: 2026-11-22
---

# Phase 9 Plan 19: SC5 measurements with real SDKs Summary

**SC5 is met on WSL2 (advisory) by a wide margin: warm instantiation p50 is 0.46 ms for Python and 0.064 ms / 0.011 ms for TS on QuickJS / StarlingMonkey. Idle memory per parked instance is published for every language. Mixed pool sizing passes: 100 parked Python runs under `class = mixed`, and admission makes runs wait under the default class. The QuickJS memories ceiling is now measured.**

## Commits

- `6192b5c` test(09-19): SC5 resource harness with the real SDK conformance guests
- `ba31758` docs(09-19): publish per-language SDK resource numbers and the SC5 verdict

## Environment

- **Machine:** WSL2, AMD Ryzen 9 7950X3D, 12 vCPUs, kernel 6.6.87.2-microsoft-standard-WSL2. Release build, Wasmtime 50.0.2, pooling allocator.
- **Load:** other agents were building throughout, and the 1-minute load never fell below 3 in a 20-minute wait. Both runs started just under 4 (3.99 and 3.98) and rose to 5–7.6 while they ran. Every JSONL line records its own `loadavg`.

## Results (run 1; run 2 agrees within noise)

| | Rust | Python | TS QuickJS | TS StarlingMonkey |
|---|---|---|---|---|
| `.wasm` / `.cwasm` | 0.29 / 1.01 MB | 22.04 / 43.66 MB | 2.31 / 6.34 MB | 13.21 / 43.33 MB |
| core instances (static / weight) | 3 / 3 | 38 / 38 | 11 / 13 | 3 / 3 |
| memories / tables, floor | 1 / 2, 1.06 MiB | 1 / 2, 15.94 MiB | 3 / 3, 2.13 MiB | 1 / 2, 9.06 MiB |
| **warm instantiate p50 / p90** | 22 / 22 µs | **460 / 490 µs** | **64 / 73 µs** | **11 / 17 µs** |
| first call (`describe`, incl. instantiate) p50 | 35 µs | 2,253 µs | 476 µs | 1,448 µs |
| start-to-first-step p50 / p90 | 24 / 25 µs | 3,176 / 3,523 µs | 621 / 717 µs | 1,452 / 1,644 µs |
| **idle RSS per parked instance** (n = 50) | 61 KB | **10,936 KB** | 1,145 KB | 2,877 KB |
| idle PSS / private (Anonymous) | 55 / 55 KB | 1,876 / 1,691 KB | 442 / 427 KB | 734 / 689 KB |
| node-budget charge per instance | 1.06 MiB | 15.94 MiB | 2.13 MiB | 9.06 MiB |

Run 2 for comparison:
- Instantiate p50: 22 / 448 / 66 / 11 µs.
- Start-to-first-step p50: 26 / 3,020 / 697 / 1,517 µs.
- RSS: 66 / 10,915 / 1,139 / 2,880 KB.

## SC5 verdict

- **Met on WSL2 (advisory, D15).** Even start-to-first-step is well under the targets: Python 3.2 ms against 10 ms, QuickJS 0.62 ms and StarlingMonkey 1.45 ms against 5 ms.
- **human_needed:** a quiet native perf machine should confirm with `OSTRO_SC5_ENFORCE=1 cargo test --release -p ostro-conformance --features polyglot --test resources -- --ignored --test-threads=1`.

## Pool sizing (ENG-2)

- **`class = mixed`:** 100 Python + 50 Rust + 50 QuickJS + 50 StarlingMonkey runs parked at once, in 97 ms. 4,750 of 38,000 slots in use, 0 failures.
- **Default class (3,000 core instances, 78 Python at once):** 100 Python runs reached a peak of 3,000 / 3,000 slots. The rest waited for permits; all 100 completed in 630 ms with 0 `AttemptFailed`.
- **QuickJS ceiling** (`class = mixed`, `total_instances = 60`): the budget admits 175, but only 19 / 20 parked (run 1 / run 2). The rest got `AttemptFailed(Trap("instance pool exhausted ... memory stripe 0"))`.

## Verification

- Task 1 verify command, run by `09-19-measure.sh`:
  - `cargo test --release -p ostro-conformance --features polyglot --locked --test resources -- --ignored --test-threads=1 --nocapture`
  - Result: `test result: ok. 3 passed`, and the JSONL holds `warm_instantiate`, `idle_rss` and `pool_mixed` records.
  - Run twice, both green.
- Task 2 verify: 225 lines, all keywords present.
- `cargo clippy -p ostro-conformance --features polyglot --test resources --locked -- -D warnings` is clean, and `cargo fmt` is applied.

## Deviations from Plan

1. **[Rule 1 - Bug, harness] `conf_timer` did not park.** It evicted with `WaitUnsatisfied`, because a timer parks hot only when it is due within `engine.hot_wait`, and the guest sleeps 1 h. The parking runtime now uses `hot_wait = 2 h`; the frozen FakeClock never fires the timer.
2. **[Rule 2] Extra metrics beyond the plan's schema:**
   - PSS / Anonymous / Pss_Shmem per instance and the node-budget charge, because the plan's RSS metric overstates Python by about 9 MB per instance (shared CoW image pages);
   - `.cwasm` size and compile time, memories/tables and memory floor;
   - the QuickJS memory-ceiling probe in `pool_mixed`.
3. **Mixed case extended:** besides the 100 Python runs, 50 each of Rust, QuickJS and StarlingMonkey are parked at the same time, so the case really mixes languages.

## Open issues

- **ENG-2: QuickJS memories and tables are not budgeted** (09-07 open issue, now measured). Under `class = python|mixed`, or with a large `pool_total_core_instances`, more than about `total_instances / 3` live QuickJS instances fail with `AttemptFailed` pool exhaustion instead of waiting. At defaults that is 333 per node, while the budget admits 2,923. Two possible fixes, both changing pool reservation and the engine-config snapshot, so a decision is needed:
  - size `total_memories` / `total_tables` per class: 3 per instance for typescript / mixed, which reserves 3× the memory address space;
  - or add memories and tables to the admission weight.
  - This plan changed no product code.
- **`LanguageClass::Python::min_memory_bytes` is stale.** It is 13,172,736 B (from S2), but the SDK guest declares 16,711,680 B. That only matters if an operator lowers `pool.max_memory_bytes` below 16 MiB.
- **`class = typescript` uses the StarlingMonkey figure** (3 core instances), so it admits only 230 QuickJS instances. The guidance in `docs/sdk-resources.md` is to override `pool_total_core_instances`.
- **The default `node_memory_budget` (1 GiB) keeps only about 64 parked Python instances hot.** The rest are LRU-evicted (`BudgetPressure`) and re-activated cold. This is documented as tuning guidance.
- **SC5 on native hardware is human_needed** (D15).

## Self-Check: PASSED

- FOUND: crates/ostro-conformance/tests/resources.rs, docs/data/sdk-resources.jsonl, docs/sdk-resources.md (on main)
- FOUND: 6192b5c, ba31758 in `git log main`
