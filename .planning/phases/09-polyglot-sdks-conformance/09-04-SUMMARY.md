---
phase: 09-polyglot-sdks-conformance
plan: 04
subsystem: spikes
tags: [spike, s9, polyglot, quickjs, starlingmonkey, componentize-py, asyncio, reproducibility, pool-sizing]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-01 PHASE-9 GATE, D-P9-1..D-P9-30 in 09-DECISIONS.md"
provides:
  - "docs/spikes/S9-polyglot.md: measured inputs for 09-07 (pool/admission), 09-09/09-10 (loops, export, result mapping), 09-13 (fuel), 09-17 (repro ladder)"
  - "docs/spikes/data/S9/measurements.jsonl + repro.txt (raw data)"
  - "spikes/s9-polyglot: standalone harness (real ostro-engine testkit leg), TS SdkPromise prototype, Python OstroLoop prototype + pytest"
affects: [09-07, 09-09, 09-10, 09-13, 09-16, 09-17, 09-20]
tech-stack:
  added: []
  patterns:
    - "spike harness as a standalone crate under spikes/ path-depending on ostro-engine (testkit); Cargo.lock seeded from the root lock, never committed"
    - "jco --bundle-config Rolldown plugin (renderChunk) to dump the final bundle for lowering checks"
key-files:
  created:
    - docs/spikes/S9-polyglot.md
    - docs/spikes/data/S9/measurements.jsonl
    - docs/spikes/data/S9/repro.txt
    - spikes/s9-polyglot/README.md
    - spikes/s9-polyglot/run.sh
    - spikes/s9-polyglot/harness/Cargo.toml
    - spikes/s9-polyglot/harness/src/main.rs
    - spikes/s9-polyglot/py/app_asyncio.py
    - spikes/s9-polyglot/py/loop_proto.py
    - spikes/s9-polyglot/py/test_loop_proto.py
    - spikes/s9-polyglot/ts/package.json
    - spikes/s9-polyglot/ts/package-lock.json
    - spikes/s9-polyglot/ts/probe.ts
    - spikes/s9-polyglot/ts/lowering_fixture.ts
  modified: []
key-decisions:
  - "D-P9-1 confirmed: QuickJS stays the TS default (1.70 MB, 0.61 MB RSS, 1.56 MiB floor, 0.39 s AOT vs SM 12.36 MB / 2.67 MB / 9.06 MiB / 1.46 s); costs 13 core instances, 3 memories, 3 tables per instance"
  - "D-P9-2 confirmed: durable asyncio timers (call_later/call_at -> sleep-until, TimerHandle.cancel -> cancel-op) work on CPython 3.12 and the embedded 3.14.0"
  - "D-P9-3 step 1 (wrapper) is not sufficient for Python (random snapshot hash seed: host PYTHONHASHSEED never reaches pre-init; stub-adapter HashMap order; generated-file mtimes) nor for QJS (133-189 B entropy residual): 09-17 needs step 2"
  - "Python hello activation = 185k-417k fuel = 0.002-0.004% of engine.activation_fuel: no [USER REVIEW] todo"
  - "ENG-2 must count core-instance Instantiate entries only (pool minimum py 38 / QJS 13 / SM 3), not FromExports bags (09-02 inspector's 111/53/8)"
requirements-completed: []
duration: ~85 min
completed: 2026-11-21
---

# Phase 9 Plan 04: Spike S9 (polyglot toolchains) Summary

**Every 09-RESEARCH open question from 5 to 9 now has a measured answer. QuickJS stays the TS default, and durable asyncio timers work. The TS SdkPromise loop ran on the real ostro-engine on both JS backends. The D-P9-3 wrapper alone does not make Python or QuickJS builds reproducible.**

## Performance

- Started 2026-11-21 ~20:05Z, finished ~21:30Z (about 85 min).
- 3 tasks, 3 task commits, plus this docs commit.
- Measurements ran at a 1-minute load average of 9–18, because other Phase 9 executors were building. Treat every timing as advisory (D15). Each JSON line records its own load average.

## Summary table (final run, `measurements.jsonl`)

| Metric | py-asyncio | py-base | ts-qjs | ts-sm |
|---|---|---|---|---|
| `.wasm` | 21.82 MB | 18.36 MB | 1.70 MB | 12.36 MB |
| core instances (static / pool minimum) | 38 / 38 | 38 / 38 | 11 / 13 | 3 / 3 |
| memories / tables per instance | 1 / 2 | 1 / 2 | 3 / 3 | 1 / 2 |
| memory floor | 15.69 MiB | 12.31 MiB | 1.56 MiB | 9.06 MiB |
| RSS per instance (n=50) | 3.78 MB | 3.40 MB | 0.61 MB | 2.67 MB |
| instantiate p50 | 451.5 µs | 466.3 µs | 66.2 µs | 12.3 µs |
| first `describe` p50 | 397.2 µs | 482.9 µs | 211.5 µs | 750.1 µs |
| AOT compile / `.cwasm` | 1.18 s / 43.4 MB | 1.28 s / 39.9 MB | 0.39 s / 5.7 MB | 1.46 s / 43.3 MB |
| hello fuel | 185k | 417k | 778k | 2.54M |

## Answers to questions 5–9

5. **QuickJS core instances.** The static count is 11 `Instantiate` entries across 24 nested components. Wasmtime's pool needs **13** per instance, plus **3 memories and 3 tables**. The memory floor is 1.56 MiB. The default Rust-class pool therefore fits only 230 live QJS instances (Python: 78).
6. **Python snapshot growth with asyncio.** The full SDK import set adds +18.8% to `.wasm` size, +27.5% to the memory floor and +11–30% to RSS. Instantiate time does not change, nor do the 38 core instances. Both fallback thresholds (+50% size, 2× instantiate) are far off, so the SDK keeps asyncio.
7. **`result<..>` export mapping.** On both backends, returning the plain `run-result` gives ok, and throwing the WIT `failure` variant value gives `Err(failure)`. Throwing a JS `Error` or a `{tag:"err"}` wrapper traps, which shows up as `AttemptFailed`. Returning a `{tag:"ok"|"err"}` wrapper works on SM only and traps on QJS. Rule for 09-10: return the plain value, throw only the `failure` variant, and catch everything else at the export.
8. **Lone surrogates.** `String.prototype.isWellFormed` and `toWellFormed` exist on QJS and SM; a lone surrogate gives false and a pair gives true. When a lone surrogate crosses the boundary, QJS traps and SM silently replaces it with U+FFFD.
9. **Lowering coverage.** All of these lower under es2016 and run on the engine on both backends, with zero leftover tokens in the bundle: async functions, arrows, methods and generators, `for await`, `allSettled`/`any`, and try/catch around an awaited throw. Top-level await is not lowered: Rolldown reports `TOLERATED_TRANSFORM`, the build still succeeds, and the TLA runs at pre-init on the native Promise.

Also recorded:
- **componentize-py export naming:**
  - The app module needs `app.Workflows(wit_world.exports.Workflows)`; any other name fails with `AttributeError: module 'app' has no attribute 'Workflows'`. A re-export (`from ostrolib import Workflows`) also works.
  - A library's WIT is discovered through its `componentize-py.toml` (`wit_directory = "wit"`), with no `-d` flag.
- **ENG-1 confirmed** on the real engine: `export exports is not allowed`.
- **Python OstroLoop prototype:** 10 pytest cases pass on 3.12, and `selfcheck()` gives identical results inside the embedded 3.14.0.

## Recommended amendments to 09-DECISIONS (for 09-20; 09-DECISIONS.md not edited)

1. **D-P9-17 / ENG-2:**
   - Count core-instance `Instantiate` entries only (09-02's inspector figures of 111/53/8 include `FromExports` bags). The real pool minimum is 38/13/3.
   - Keep a margin: QJS needs 13 against a static count of 11.
   - Weigh or size memories and tables too, since QJS has 3 of each.
   - The class-table "3 for TS" and `min_memory_bytes` (9,502,720) are SM figures.
2. **D-P9-3:**
   - Step 1 is measured and not sufficient for Python, so 09-17 should plan for the fork from the start.
   - The patch must also cover `src/stubwasi.rs` (HashMap adapter order) and the mtimes of generated files and tempdirs, beyond the WasiCtx env/random/clocks changes.
   - QJS has an entropy residual of 133–189 B: file the componentize-qjs issue.
3. **SDK rules:**
   - Python: no lazy imports. A first import inside `run` traps, because `--stub-wasi` leaves no filesystem.
   - Python loop: track fired timers so `asyncio.sleep`'s `finally: h.cancel()` does not journal a `cancel-op`. Add a `TimerHandle` subclass slot for the op id. `create_task` must accept `**kwargs`.
   - TS: catch everything at the export boundary, ship UTF-8, sha256 and console shims for QJS, and fail the build on leftover non-TLA async.

## Repro verdict

**componentize-py 0.25.1 under the wrapper alone: not reproducible.** All 3 shas differ, and about 1,500 sections and data segments differ per pair. The causes:
- Each build bakes a random str-hash seed into the snapshot. `hash("ostro")` differs in all 6 builds, because `PYTHONHASHSEED` never reaches the pre-init WasiCtx.
- The core-module order shuffles.
- Six 4-byte and up to four f64 mtimes of generated or rewritten files differ by the gap between builds.

**QuickJS under the wrapper: not reproducible either.** All three builds have the same length, but 9–18 snapshot data segments (133–189 B) differ and contain no timestamps.

**Fork estimate (09-17):**
- Patch about 40–80 lines in `src/lib.rs` (pre-init WasiCtxBuilder at 674–691 and 811–816; world-dir and helper mtimes at 729–777), `src/prelink.rs:31–57` and `src/stubwasi.rs:15,32`, plus optional tar-header mtime normalisation in `build.rs:294–306`.
- Build requirements: Rust ≥ 1.94 + `wasm32-wasip2`, wasi-sdk 33, the `dicej/cpython` tarball `v3.14.0-wasi-sdk-30` (native and WASI builds, with zlib and sqlite), and a host C toolchain.
- Estimated 4–6 GB disk and 15–30 min cold (not measured).

## Task Commits

1. **Task 1: S9 harness and resource measurements**: `7773b9f` (chore)
2. **Task 2: behaviour probes and loop prototypes**: `1ecdcec` (chore)
3. **Task 3: reproducibility + S9 report**: `216f8cb` (docs)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] `sdks/scripts/build-guest.sh` was missing when the run started**
- **Found during:** Task 1.
- **Issue:** the plan says to use 09-02's `build-guest.sh` if it has been committed.
- **Fix:** it had not been committed when the spike started, so `run.sh` inlines the same pinned commands, as the plan allows.
- **Files modified:** `spikes/s9-polyglot/run.sh`.

**2. [Rule 1 - Bug] The positional byte diff was meaningless for builds of different lengths**
- **Found during:** Task 3.
- **Issue:** a whole-file `cmp -l` reported about 1.2M differing bytes for QJS builds, because a few-byte length change shifts every later offset.
- **Fix:** `run.sh` gained a section and data-segment walk (common prefix/suffix, equal-length diff counts, u32 and f64 timestamp scan) and a `repro-analyze` mode. The `cmp -l` counts are still reported.
- **Files modified:** `spikes/s9-polyglot/run.sh`.

**3. [Rule 2 - Missing evidence] Extra Python guest workflows**
- **Found during:** Tasks 2 and 3.
- **What was added:** `loop-check` runs `selfcheck()` inside the embedded CPython 3.14.0. `lazy-import` shows the trap. `hash-probe` shows the snapshot hash seed per build.
- **Why:** these turn assumptions into evidence. Running `selfcheck()` in the guest is how the plan's own 3.14 check is done.
- **Files modified:** `spikes/s9-polyglot/py/app_asyncio.py`.

**4. Measurements were regenerated in one consistent run before the Task 3 commit.** The guest source gained the probe workflows after Task 1. Earlier runs (load 10–18) agreed within noise; their ranges are noted in the report.

## Issues Encountered

- **Engine observation, not fixed (out of scope):** on StarlingMonkey, a `run` that begins a step, never completes it and returns completes live with journal `[RunStarted, RunCompleted]`. Replay of that journal then fails with `NonDeterminism { seq: 1, expected: "run completed", actual: "step \"bad�\"" }`. Flagged for 09-07/09-13 in the report.
- The `.planning/phases/*` main checkout is stale. All work was done in a private worktree, and commits went through a private index with CAS `update-ref`.

## Self-Check: PASSED

- All created files listed above exist on main (verified with `git ls-tree`).
- Commits 7773b9f, 1ecdcec and 216f8cb are on main.
- Plan verification: Task 1 grep checks OK; Task 2 pytest `10 passed` plus all five probe tags present; Task 3 report ≥ 120 lines (now about 305) with every required keyword.
- Harness: `cargo clippy --release -- -D warnings` is clean and the build is warning-free. The spike crate is outside the workspace and ci-local.
