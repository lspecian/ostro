---
phase: 01-foundation-vendoring-spikes
plan: 05
subsystem: spikes
tags: [wasm, wasmtime, component-model, componentize-py, jco, wit-bindgen, spike-s2, nfr-2.3]
requires:
  - phase: 01-03
    provides: "S2 apparatus (spike.wit, build.sh, s2-harness)"
provides:
  - "spikes/s2-guests/measure.sh: repeatable S2 protocol (env|pool|perf|probes|repro|all)"
  - "spikes/s2-guests/wasm_sections.py: dependency-free component section diff"
  - "docs/spikes/data/S2/*: raw JSON lines (perf, pool-limits, probes, imports) + env.txt, repro.txt"
  - "docs/spikes/S2-guest-toolchains.md: S2 report with step-handle recommendation"
affects: [01-08, phase-3-engine (ENG-02, ENG-08), phase-4-deploy, phase-9-10-sdks, phase-10-cli (CLI-04)]
tech-stack:
  added: []
  patterns: ["one harness process per measurement, every line tagged guest/allocator/kind/loadavg", "harness allocator object kept as allocator_config"]
key-files:
  created:
    - spikes/s2-guests/measure.sh
    - spikes/s2-guests/wasm_sections.py
    - docs/spikes/S2-guest-toolchains.md
    - docs/spikes/data/S2/env.txt
    - docs/spikes/data/S2/perf.jsonl
    - docs/spikes/data/S2/pool-limits.jsonl
    - docs/spikes/data/S2/probes.jsonl
    - docs/spikes/data/S2/imports.jsonl
    - docs/spikes/data/S2/repro.txt
  modified: []
key-decisions:
  - "S2 step handle: Recommendation resource (round-trips in py/ts/rust; drop observable only with SDK RAII discipline; engine must not rely on HostStep::drop)"
  - "S2: Python does not need to default to workers (6.5 MB RSS/instance); recommendation, final call in 01-08/PROJECT.md"
  - "S2: componentize-py/js do NOT reseed after pre-init; host-driven reseed works in all three languages"
  - "S2: Python component = 38 core instances; wasmtime default pool fits only 26 live Python instances (ENG-08 input)"
metrics:
  duration: ~25min
  completed: 2026-11-18
---

# Phase 1 Plan 05: S2 Measurements and Report Summary

The full S2 protocol ran for Python, TS and Rust guests on wasmtime 50.0.2. NFR-2.3 is met by a wide margin everywhere: steady instantiate p50 is about 450 µs for Python and 10–20 µs for TS and Rust. Python costs 6.5 MB RSS per instance. No toolchain reseeds randomness after pre-init, but host-driven reseeding works in all three. `resource step` round-trips in all three languages, and the report ends with **Recommendation: resource**.

## Tasks

| # | Task | Commit |
|---|------|--------|
| 1 | measure.sh + perf/memory/cold-start runs | a3bd57f, fix ef82798 |
| 2 | Probes, reproducibility, imports, S2 report | 6b00739 |

## Key results (details and traceability in docs/spikes/S2-guest-toolchains.md)

- **Instantiate p50, on-demand / pooling:** py 440 / 454 µs, ts 19.5 / 10.2 µs, rust 15.7 / 9.2 µs. NFR-2.3 is met in all six cells.
- **First call per fresh instance:** py about 0.9 ms, ts about 0.6 ms, rust about 6 µs.
- **Cold start, in-process median:** py 5.1 ms, ts 2.5 ms, rust 0.7 ms.
- **Memory:** py 6.5 MB, ts 2.4–2.5 MB and rust about 40 KB RSS per live instance. Declared linear memory is 13.2 / 9.5 / 1.1 MB.
- **Pooling limits per component instance:** py needs 38 core instances, 1 memory and 2 tables; ts and rust need 3 core instances, 1 memory and 2 tables.
- **Determinism:**
  - py: random state and hash are baked per build; time and uuid trap.
  - ts: random and crypto are fixed even across builds; `Date.now` is the build timestamp.
  - rust: std `HashMap` and `SystemTime` trap on the undefined WASI imports.
  - Reseeding with A, A, B behaves correctly in all three.
- **Rust wasip2 imports:** 16 `wasi:*@0.2.12` interfaces (research expected 17).
- **Reproducibility:** only Rust is reproducible. Python (even with `PYTHONHASHSEED=0`) and TS differ in the snapshot data section. Python also shuffles the order of its shim modules.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Pooling memory run for Python failed with the default pool.**
- **Found during:** Task 1.
- **Issue:** `memory --pooling --n 50` failed with `maximum concurrent limit of 1000 for core instances`, because the Python component has 38 core instances.
- **Fix:** `measure.sh` passes `--pool-total-core-instances $((N_MEM*38))` for Python pooling. The new `pool` phase writes the minimal per-component limits to `pool-limits.jsonl`, as the plan asks for ENG-08.
- **Commit:** a3bd57f

**2. [Rule 1 - Bug] The tag step dropped the harness allocator config.**
- **Found during:** Task 2 preparation.
- **Issue:** jq tagging overwrote the harness `allocator` object (strategy plus pooling overrides) with the string tag.
- **Fix:** the object is kept as `allocator_config`. perf was re-run at lower load (1.07 vs 3.4); the first pass was discarded and is noted in the report.
- **Commit:** ef82798

**3. [Rule 1 - Bug] Rust "independent" builds were cargo cache copies.**
- **Issue:** the `-b` and r1–r3 Rust builds reused the cached artefact.
- **Fix:** added `clean_rust` (`cargo clean --release --target wasm32-wasip2`) before each Rust build in the probes and repro phases. Rust remained byte-identical after clean builds.
- **Commit:** ef82798

**4. [Rule 2 - Missing functionality] Section attribution without wasm-tools.**
- **Fix:** added `spikes/s2-guests/wasm_sections.py`, a small component/core-module section walker. `repro.txt` attributes the differing bytes to sections instead of reporting "not determined".
- **Commit:** 6b00739

## Notes

- Loadavg during the final perf run was 1.06–1.07 with nproc 12, so no latency repeats were needed. Plan 01-08 still re-measures instantiate p50 and cold start on a quiet machine.
- FOUND-06 is not marked complete; 01-08 completes it.

## Self-Check: PASSED
