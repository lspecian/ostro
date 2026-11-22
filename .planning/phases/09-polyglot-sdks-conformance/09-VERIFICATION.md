---
phase: 09-polyglot-sdks-conformance
verified: 2026-11-22T17:15:00Z
status: human_needed
score: 5/5 success criteria verified in code (SC2 rebuild-sha half for Python/TS and SC5 native confirmation are by-design human_needed)
human_verification:
  - test: "Publish the `ostro` Python package to PyPI and the `ostro` TS package to npm (D-P9-19); add `readme` to sdks/python/pyproject.toml first"
    expected: "Packages install from the public registries; the packaging dry runs (09-17) already pass"
    why_human: "No registry credentials or remotes in the agent environment"
  - test: "Run the `polyglot` job of .github/workflows/ci.yml on remote CI (FOUND-05 pattern)"
    expected: "Python/TS SDK tests, polyglot conformance suites and repro-check green on GitHub runners"
    why_human: "Agents never push; the job is only proven via scripts/ci-local.sh"
  - test: "SC2 Python/TS rebuild sha (D-P9-3 step 3): file the componentize-py determinism PR and the componentize-qjs residual issue (texts in sdks/python/toolchain/README.md); USER REVIEW of the D-P9-3 outcome"
    expected: "Once upstream lands, `bash sdks/scripts/repro-check.sh py|ts` reports identical shas and the RESIDUAL-ACCEPTED markers in docs/sdk-builds.md are removed; then tick SDK-04"
    why_human: "Upstream toolchain fixes; the residual (Python 73-76 B with the vendored fork, QuickJS layout drift) is outside the repo. Pre-planned ladder outcome in D-P9-3"
  - test: "SC5 on the native perf machine: run the resources suite with OSTRO_SC5_ENFORCE=1 (quiet, native Linux, NVMe, 8 vCPU)"
    expected: "Warm instantiate p50 Python < 10 ms, TS < 5 ms (WSL2 measured 0.46 ms / 0.064 ms QuickJS / 0.011 ms StarlingMonkey)"
    why_human: "D15: perf gate is a human-run native machine; WSL2 numbers are advisory"
---

# Phase 9: Polyglot SDKs & Conformance Verification Report

**Phase Goal:** Python and TypeScript are first-class. Workflows in all three languages have identical semantics, proven by one language-neutral conformance suite.
**Verified:** 2026-11-22
**Status:** human_needed
**Re-verification:** No (initial verification)

Verification ran against `main` at `15e2d6d` (38 commits after the gate's code commit `762bbc5`; those later commits are Phase 11/12 server work plus Phase 9 docs, they change no SDK code, nothing under conformance/ or crates/ostro-conformance (only the SDK READMEs), but they do touch ostro-server and ostro-engine replay_check). The full three-language table was run again on current main: see "Independent re-run" below.

## Goal Achievement

### Observable Truths (ROADMAP Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | The conformance suite passes 100% for Rust, Python and TS. It covers steps, retries, timers, signals, concurrency/race, cancellation, children, CAN, await tokens, patching and NDET | VERIFIED | `conformance/spec/catalogue.json`: 27 engine scenarios (C-STEP-01..04, C-RETRY-01/02, C-TIMER-01/02, C-SIG-01/02, C-RACE-01/02, C-CANCEL-01..03, C-PATCH-01/02, C-NDET-01, C-CAN-01, C-TOKEN-01, C-RAND-01/02, C-VAL-01, C-NATIVE-01, C-STATUS-01, C-SAGA-01/02) + 16 server scenarios (C-CHILD-01..11, C-CAN-02, C-SIG-API-01, C-TOKEN-API-01, C-FX-01, C-SAGA-03) + 2 crash + C-REBUILD-01, each listing rust/py/ts. Every category is there. `polyglot_cross::full_table` runs every engine/server scenario in every catalogue language and diffs it against the Rust reference: canonical projection, two-execution byte identity and a strict replay that must append 0 events (`engine_leg.rs` strict_replay). It asserts `passed == cells` and has no failures. Re-run on current main: PASS (see below) |
| 2 | Two runs of the same Py/TS workflow get different `ctx.random`, and replay reproduces them. Rebuilding the same source gives the same module sha | VERIFIED (random/replay, all languages; rebuild sha for Rust) + HUMAN (rebuild sha for Py/TS, D-P9-3 residual) | C-RAND-01/02 use `differs_across_seeds`: `engine_leg.rs:750` re-executes with seed+1 and fails if the canonical results are equal. Replay identity comes from the strict replay. C-VAL-01 is equal across Rust/Py/QuickJS/StarlingMonkey (`values_cross_language`). C-REBUILD-01 (`tests/rebuild.rs`) records on the cached build and replays on a fresh build. `repro-check.sh`: Rust 3 identical builds. Py/TS DIFF, gated by `RESIDUAL-ACCEPTED: py/ts` in docs/sdk-builds.md, with measured bytes and causes documented. That is the pre-planned D-P9-3 step 3 |
| 3 | A TS→Python→Rust child chain completes under crash injection. Cancelling the parent reaches every descendant | VERIFIED | `tests/crash_leg.rs` (C-CRASH-01/02, `#[ignore]`, failpoint binary + kill -9). `check_cancelled` asserts root, Python mid and Rust leaf all end `cancelled`, the shielded leaf cleanup is journaled exactly once, there is exactly 1 run_started/1 terminal per run and exactly one child run per root, and nothing is left non-terminal. Evidence: 09-18 ran 100+100 iterations with 0 violations, and the 09-20 gate re-ran 20+20 green. The chain without crashes is also C-CHILD-08 in `full_table` (TS→Py→Rust column) and `cross_chain` |
| 4 | A saga's registered compensations run in reverse when a later step fails | VERIFIED | Python `saga.py:115` `reversed(comps)`, TS `saga.ts:110` `.reverse()`, plus the Rust SDK saga. C-SAGA-01 projection: book_a, book_b, book_c, **cancel_b, cancel_a**, terminal failed. C-SAGA-03 (`saga_checks`) asserts the journal order of HTTP effect idempotency keys is a, b, refund-b, refund-a, with exactly 1 request and 1 charge per key, in all three languages |
| 5 | Warm instantiation p50 < 10 ms Python, < 5 ms TS; idle resident memory published per language | VERIFIED (WSL2, advisory) + HUMAN (native confirmation) | docs/sdk-resources.md: warm p50 Python 460 µs, QuickJS 64 µs, StarlingMonkey 11 µs, Rust 22 µs. Idle RSS/PSS per parked instance is published for all four (Python 10.9 MB RSS / 1.9 MB PSS, QuickJS 1.15/0.44 MB, SM 2.88/0.73 MB, Rust 61/55 KB). Measured by `tests/resources.rs`. Native enforcement (`OSTRO_SC5_ENFORCE=1`) is D15 human-run |

**Score:** 5/5 truths supported in code. The SC2 Py/TS rebuild sha and the SC5 native run remain by-design human_needed.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `sdks/python/ostro/` | Python SDK: workflow/step/ctx/effects/saga, SDK-owned asyncio loop, determinism | VERIFIED | 13 modules, ~3.3k lines (loop.py 434, effects.py 450, _determinism.py 400, saga.py 144). Guests use `asyncio.gather` / `asyncio.wait(FIRST_COMPLETED)` (conformance/guests/python-v2/app.py). The only `NotImplementedError`s are intentional (unsupported loop methods; a host-less fallback outside a guest) |
| `sdks/ts/src/` | TS SDK: SdkPromise job loop, steps, effects, saga, determinism, sha256/utf8 | VERIFIED | 16 modules, ~3.3k lines. `ts_sdk_build.rs:48` asserts zero `wasi:` imports on QuickJS (SDK-03) |
| `sdks/rust` additions | start_child/ChildOptions, saga, random_f64 | VERIFIED | Exercised by every Rust reference cell in full_table; README section "Children, saga, random_f64" |
| `sdks/scripts/build-guest.sh` | Pinned componentize-py `--stub-wasi`, jco/QuickJS stub-wasi builds | VERIFIED | Lines 110 and 128 |
| `crates/ostro-conformance` | Spec model, canonical JSON, projection, comparator, engine/server/crash legs | VERIFIED | 16 test targets, ~6.2k test lines. Catalogue-driven (`spec::scenarios`) |
| `conformance/spec/**` | 46 catalogue specs | VERIFIED | 28 engine (incl. rebuild), 16 server, 2 crash. `spec_catalogue all_specs_present` green at the gate |
| `sdks/scripts/repro-check.sh`, `docs/sdk-builds.md` | Reproducibility check + documented residual | VERIFIED | Machine-checked RESIDUAL-ACCEPTED markers (lines 121, 136) |
| `docs/sdk-resources.md` | SC5 measurements | VERIFIED | See SC5 |
| `docs/conformance.md`, `docs/children.md`, SDK READMEs | Docs | VERIFIED | Present on main (96e9cee) |
| ci-local polyglot block + `ci.yml` polyglot job | CI wiring | VERIFIED (local) | `scripts/ci-local.sh` polyglot block (`--no-polyglot` opt-out). `ci.yml:518` job `polyglot`. Remote run is human_needed |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| catalogue.json | full_table / engine_leg / server_leg | `spec::scenarios(leg)` iteration | WIRED | `assert_eq!(rows.len(), expected)`: no catalogue scenario can be silently skipped |
| SDK-built guests (py/ts) | real engine/server | `polyglot.rs` build cache → `Runtime::activate` / ServerHarness | WIRED | Same runtime as production; ENG-1/ENG-2 admission (09-07, eng2-mem) |
| Python/TS runs | Rust reference | `engine_leg::cross_compare`, `server_cross_compare` | WIRED | Failures are collected and asserted empty |
| parent cancel | descendants | cancel-op → CancelRun cascade (09-08) | WIRED | crash_leg `check_cancelled`; C-CHILD-11 cascade vs shielding |
| saga helper | compensation order | reversed registration list | WIRED | C-SAGA-01/02/03 |

### Requirements Coverage

| Requirement | Source Plans | Description | Status | Evidence |
|-------------|--------------|-------------|--------|----------|
| SDK-02 | 09-03/09/12/14/17/19 | Python SDK with pinned componentize-py `--stub-wasi`, own asyncio loop, gather/race | SATISFIED (publishing human_needed) | SDK code; polyglot_python; py_sdk_build; full_table py column |
| SDK-03 | 09-03/10/12/15/17/19 | TS SDK with pinned jco, no WASI imports, Promise.all/race | SATISFIED (publishing human_needed) | SDK code; ts_sdk_build zero-wasi assertion; full_table ts column |
| SDK-04 | 09-04/06/09/10/12/14/15/17 | Reseeding (differ per run, replay same) + reproducible builds | PARTIAL, by design: reseeding SATISFIED; rebuild sha Rust only, Py/TS residual human_needed (D-P9-3) | C-RAND-01/02, C-VAL-01, C-REBUILD-01, repro-check. REQUIREMENTS.md correctly leaves it unticked ("Partial") |
| SDK-05 | 09-02/11/13/14/15/16/18 | Language-neutral conformance suite passes for Rust/Py/TS | SATISFIED | 135/135 cells (re-run on current main below) |
| WF-06 | 09-05/06/08/13/16/18 | Start/await/detach children in any language; cancel propagates | SATISFIED | C-CHILD-01..11, C-CRASH-01/02 |
| WF-07 (Py/TS) | 09-09/10/11/14/15 | Native time/random deterministic under replay | SATISFIED | C-NATIVE-01 + C-RAND-02 in py/ts with strict replay |
| WF-10 | 09-06/12/16 | Saga runs compensations in reverse on failure | SATISFIED | C-SAGA-01..03 in all languages |

No orphaned requirements: every Phase 9 row in REQUIREMENTS.md (SDK-02..05, WF-06, WF-07, WF-10) is claimed by at least one plan.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| crates/ostro-conformance/tests/crash_leg.rs | 814, 1063 | `#[ignore]`; not wired into ci-local or ci.yml (A30) | Warning | SC3 has evidence from explicit runs (100+100, then 20+20) but no automated regression guard. Recorded as a STATE follow-up |
| docs/data/sdk-resources.jsonl | - | Still holds pre-eng2-mem QuickJS records | Info | Stale raw data only; the docs table is current |
| sdks/python/pyproject.toml | - | No `readme` field | Info | Blocks a clean PyPI publish (part of the human publishing item) |
| `class = "typescript"` core budget | - | Sized with the StarlingMonkey figure (A12) | Info | Conservative over-reservation for QuickJS; no correctness impact |

No TODO/FIXME/placeholder stubs in sdks/python/ostro, sdks/ts/src or crates/ostro-conformance/src.

### Independent re-run (this verification)

`CARGO_TARGET_DIR=target/slot-1 OSTRO_POLYGLOT_CACHE=target/polyglot-guests nice -n 10 cargo test -p ostro-conformance --features polyglot --locked --test polyglot_cross -- full_table` on main `15e2d6d`: **EXIT 0, `test result: ok. 1 passed`**. Output: `scenario x language table (43 scenarios, 135/135 cells pass, 56.2s)` and `server leg: exactly one child run per (parent, op) for 58 child starts`. So SC1/SDK-05 (and the C-CHILD-08 TS→Py→Rust chain, C-SAGA-03, C-NATIVE-01, C-RAND-01/02) still hold after the 38 post-gate commits (Phase 11/12 server changes). The 09-20 gate evidence (ci-local all steps, repro-check, crash leg 20+20, spec catalogue, deny) was not re-run here; it is taken from 09-20-SUMMARY / 09-VALIDATION, and the test code backing it was inspected.

### Human Verification Required

1. **PyPI/npm publishing** (D-P9-19): add `readme` to pyproject, then publish. Expected: installable packages.
2. **Remote CI polyglot job**: run on GitHub. Expected: green, matching ci-local.
3. **Upstream reproducibility fixes / D-P9-3 user review**: file the componentize-py PR and the componentize-qjs issue (texts in sdks/python/toolchain/README.md). When fixed, repro-check py/ts should be identical; then drop the RESIDUAL-ACCEPTED markers and tick SDK-04.
4. **SC5 native confirmation**: `OSTRO_SC5_ENFORCE=1` on the native perf machine.

All four are by design: each was planned as human_needed in 09-DECISIONS (D-P9-3, D-P9-19, D15, FOUND-05) before execution.

### Gaps Summary

No blocking gaps. The implementation, tests and evidence support every success criterion. Two items stay open by plan. First, the SC2 rebuild-sha half for Python/TS depends on upstream toolchain determinism; the D-P9-3 ladder was executed through step 2 (a vendored componentize-py fork patch) and ended at the documented residual. Second, SC5 native-machine confirmation is outstanding. Non-blocking follow-up: wire the crash leg (C-CRASH-01/02) into ci-local, at least at a low iteration count, so SC3 has a regression guard.

---

_Verified: 2026-11-22_
_Verifier: Claude (gsd-verifier)_
