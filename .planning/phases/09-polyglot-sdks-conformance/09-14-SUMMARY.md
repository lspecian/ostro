---
phase: 09-polyglot-sdks-conformance
plan: 14
subsystem: conformance
tags: [conformance, python-sdk, engine-leg, server-leg, wf-07, sdk-05, wave-5]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-11 engine-leg runner + conf-rust contract; 09-12 Python effects/saga + engine smoke; 09-13 server-leg harness + 14 server specs"
provides:
  - "conformance/guests/python (py_conf_*: 26 engine + 15 server-leg workflows) and python-v2 (v2 conf_patch/conf_ndet)"
  - "crates/ostro-conformance/tests/polyglot_python.rs: engine_leg_python (27/27 vs Rust), server_leg_python_* (14/14 vs Rust), v1/v2 drift check"
affects: [09-16, 09-17, 09-18, 09-20]
tech-stack:
  added: []
  patterns:
    - "python-v2 = python/app.py with only a marked v1/v2 section replaced; a test pins the drift"
    - "Ops created in argument order where several ops' journal order matters (ctx.sleep/ctx.wait_signal in gather/race); asyncio.sleep for single durable sleeps"
key-files:
  created:
    - conformance/guests/python/app.py
    - conformance/guests/python-v2/app.py
    - crates/ostro-conformance/tests/polyglot_python.rs
  modified: []
key-decisions:
  - "python-v2/app.py duplicates the module (componentize-py stages one guest dir with one app.py entry module); only the conf_patch/conf_ndet section differs, enforced by engine_leg_python_v2_differs_only_in_patch_and_ndet"
  - "C-NATIVE-01 Python probe: time.time_ns(), time.time(), dict iteration of 16 keys, random.random(), uuid.uuid4(), in the body and in step observe"
  - "conf_rand_native uses the native os.urandom(8) and random.random(); they equal Rust's random_bytes(8)/random_f64 values exactly (C-RAND-02 cross-compare)"
  - "No SDK, engine or server deviation found; no spec change"
requirements-completed: [SDK-05, SDK-04, WF-06, WF-07, WF-10, SDK-02]
duration: ~45 min
completed: 2026-11-22
---

# Phase 9 Plan 14: Python conformance (engine + server legs vs Rust) Summary

**The Python SDK passes the whole language-neutral suite, cross-compared with the Rust reference: all 27 engine-leg scenarios and all 14 server-leg scenarios. Strict checks held for each: two runs give byte-identical journals and replay appends 0 events. Status, canonical outcome and projection all equal Rust's. WF-07 has Python evidence (C-NATIVE-01: native time, `random`, `uuid4` and dict iteration are replay-identical and change with the seed). Every scenario passed on the first run; no SDK fix was needed.**

## Commits

| Task | Commit | Content |
|------|--------|---------|
| 1 | `e359d1d` | Python guests `python/` + `python-v2/`, `polyglot_python.rs` engine leg + v1/v2 drift check |
| 2 | `94b1e70` | `polyglot_python.rs` server leg (5 family tests + coverage test) |

## Guests

`conformance/guests/python/app.py` exports every base name in the 09-11 and 09-13 contract tables as `py_<base>`. The code is plain asyncio:
- Joins use `asyncio.gather`. Races use `asyncio.wait(..., FIRST_COMPLETED)` followed by an explicit `ctx.cancel(loser)`.
- Cleanup runs under `async with ctx.shielded()`, and sagas use `ctx.saga()`. Sleeps use durable `asyncio.sleep` (timer, cancel, patch, leaf sleep, parent delays).
- `conf_step_sibling` gathers an async step `s`, whose body does `await asyncio.sleep(0)`, with a sibling task that draws `ctx.random_u64()` and sleeps 1 s. Step exclusivity defers the sibling, so its commands land after `step:s`, exactly as in Rust.
- `conf_native` reads `time.time_ns()`, `time.time()`, `random.random()` and `uuid.uuid4()`, plus the order of a 16-key dict, both in the body and in step `observe`.

Ops are allocated when the call is made, as in Rust. Where several ops are created together (gather, race), the guest uses `ctx.sleep` / `ctx.wait_signal` objects so the op order follows the argument order. A coroutine `asyncio.sleep` inside `gather` would allocate its timer only when its task first runs, after any eager op.

**v2 layout:** `python-v2/app.py` is a full copy of v1. componentize-py stages one guest dir with a single `app.py` entry module, so v2 cannot import from `python/`. The copy differs only in the marked `# --- v1/v2 bodies` section: `conf_patch` uses `ctx.patched("x")` to choose step `a2` or `a`, and `conf_ndet` swaps the order to `b` then `a`. The first docstring line also differs. The test `engine_leg_python_v2_differs_only_in_patch_and_ndet` fails on any other drift.

## Scenario table (Python, debug build, cached component)

**Engine leg** (`engine_leg_python`: Rust then Python per scenario, `engine_leg::cross_compare`). Every row is a pass with status, result and projection equal to Rust.

| Scenario | Status | Activations | Python time | Rust time |
|----------|--------|-------------|-------------|-----------|
| C-STEP-01 / 02 / 03 / 04 | completed | 1 / 1 / 1 / 2 | 2.05 s (module prepare) / 41 / 42 / 71 ms | 118 (Rust compile) / 1.8 / 2.0 / 3.6 ms |
| C-NATIVE-01 | completed (per-language values; differs across seeds) | 1 | 59 ms | 2.8 ms |
| C-RETRY-01 / 02 | completed / failed | 3 / 2 | 98 / 70 ms | 4.0 / 2.9 ms |
| C-TIMER-01 / 02 | completed | 2 / 2 | 69 / 75 ms | 3.0 / 3.2 ms |
| C-SIG-01 / 02 | completed | 3 / 2 | 94 / 68 ms | 4.6 / 3.0 ms |
| C-RACE-01 / 02 | completed | 4 / 2 | 130 / 71 ms | 5.9 / 3.2 ms |
| C-CANCEL-01 / 02 / 03 | cancelled / cancelled / completed | 2 / 2 / 1 | 67 / 73 / 44 ms | 3.0 / 3.1 / 2.0 ms |
| C-PATCH-01 / 02 | completed | 3 / 2 | 2.22 s (v2 prepare) / 77 ms | 75 / 3.3 ms |
| C-NDET-01 | non_determinism | 2 | 65 ms | 3.1 ms |
| C-RAND-01 / 02 | completed (differ across seeds) | 1 / 1 | 60 / 62 ms | 2.9 / 3.8 ms |
| C-VAL-01 | completed (pinned values equal) | 1 | 42 ms | 2.2 ms |
| C-CAN-01 | completed (3-run chain) | 3 | 126 ms | 4.5 ms |
| C-TOKEN-01 | completed | 2 | 68 ms | 3.2 ms |
| C-STATUS-01 | completed | 1 | 42 ms | 2.2 ms |
| C-SAGA-01 / 02 | failed / cancelled | 1 / 2 | 45 / 77 ms | 2.4 / 3.4 ms |

**Server leg** (`server_leg_python_<family>`: one in-process server per family, all roles in Python, compared with Rust on status and root facts). Every family's check found exactly one child run per (parent, op).

| Scenario | Status | Python time | Rust time |
|----------|--------|-------------|-----------|
| C-CHILD-01 | completed | 5.03 s (incl. Python deploy) | 0.23 s |
| C-CHILD-02 | completed | 0.05 s | 0.01 s |
| C-CHILD-03 | completed (detached child alive 2 s after parent) | 2.11 s | 2.03 s |
| C-CHILD-04 | completed (cancel-op) | 1.05 s | 1.04 s |
| C-CHILD-05 | completed (parent_closed) | 1.07 s | 1.05 s |
| C-CHILD-06 | completed (child_terminated + timeout variant) | 5.56 s (incl. deploy) | 2.16 s |
| C-CHILD-07 | completed (CAN child) | 0.15 s | 0.03 s |
| C-CHILD-09 | cancelled (3 levels) | 0.13 s | 0.04 s |
| C-CHILD-10 | completed (resume after close; child_terminated variant) | 5.80 s (incl. deploy) | 2.51 s |
| C-CHILD-11 | completed (cascade beats shielding: `{"first":"cancelled","cleanup_child":true}`) | 1.11 s | 1.04 s |
| C-SIG-API-01 | completed | 5.00 s (incl. deploy) | 0.24 s |
| C-TOKEN-API-01 | completed | 0.04 s | 0.03 s |
| C-CAN-02 | completed (3-run chain, long-poll on the last) | 0.05 s | 0.02 s |
| C-FX-01 | completed (one charge under `idempotency_key("pay")`) | 3.88 s (incl. deploy) | 0.39 s |

**Leg runtimes:** the whole `polyglot_python` binary (8 tests) takes 35–40 s with `--test-threads=2`, guest components cached. The engine leg alone takes 6.3 s once the components are cached; on a cold Python component cache it took 25 s, including the two componentize-py builds. The server families take 5–12 s each.

## SDK deviation log

None. Every engine and server scenario passed on its first run against the unmodified 09-09/09-12 SDK. No file under `sdks/python/**` changed, so `tests/test_regressions.py` was not needed and was not created.

## Spec changes

None.

## Verification

- `cargo test -p ostro-conformance --features polyglot --locked --test polyglot_python -- --test-threads=2`: **test result: ok. 8 passed** (engine_leg_python, engine_leg_python_v2_differs_only_in_patch_and_ndet, server_leg_python_covers_every_09_13_scenario, 5 × server_leg_python_*). Run before and after rebasing onto main `8cf9bc9` (docs-only commits since).
- Filtered `-- engine_leg`: `ok. 2 passed`. Filtered `-- server_leg`: `ok. 6 passed`.
- `cargo clippy -p ostro-conformance --features polyglot --all-targets --locked -- -D warnings`: clean. `cargo fmt -p ostro-conformance -- --check`: clean.
- Rust leg regression: `cargo test -p ostro-conformance --locked --test engine_leg -- --test-threads=2`: `ok. 16 passed`.
- Python SDK host suite (`uv run --no-project --python 3.12 --with pytest==8.4.2 python -m pytest tests -q`): **148 passed**.
- All builds and tests ran under `nice -n 19` with `CARGO_BUILD_JOBS=4`, because a crash campaign was running on the same machine.
- **Not run:** `scripts/ci-local.sh` and the full workspace suite (load rule, as in 09-11/09-13). This plan changes only new guest files and a new test file.

## Deviations from Plan

1. **No `sdks/python/**` or `tests/test_regressions.py` changes.** No deviation was found, so the files that `files_modified` listed for possible fixes stayed untouched.
2. **Server leg as five family tests** (`server_leg_python_<family>`, same split as `tests/server_leg.rs`) plus a coverage test, rather than one `server_leg_python` test. Each family gets its own server, as in 09-13, and the plan's `server_leg` filter still matches them all.
3. **`OSTRO_POLYGLOT_CACHE`** pointed at the main checkout's `target/polyglot-guests` during the runs, to reuse cached components across worktrees. The test does not depend on it.

## Open issues

- None blocking. ci-local (09-17) should run `--test polyglot_python` next to `polyglot_ts`.

## Self-Check: PASSED

- FOUND: conformance/guests/python/app.py, conformance/guests/python-v2/app.py, crates/ostro-conformance/tests/polyglot_python.rs.
- FOUND: e359d1d, 94b1e70 on the rebased branch (landed on main by the docs commit's CAS update).
