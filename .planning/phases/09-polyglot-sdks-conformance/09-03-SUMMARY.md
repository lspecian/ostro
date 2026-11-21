---
phase: 09-polyglot-sdks-conformance
plan: 03
subsystem: sdks
tags: [python, typescript, sdk, host-contract, fake-host, wave-0]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-01 PHASE-9 GATE (names sdks/python, sdks/ts, package ostro; toolchain pins)"
provides:
  - "sdks/python skeleton: ostro._host.Host protocol + install/current, FakeHost + fake_host fixture, pytest scaffold"
  - "sdks/ts skeleton: Host interface (bigint op ids) + setHost/host, FakeHost helpers, node:test scaffold"
  - "sdks/ts/rolldown.ostro.mjs (es2016 async lowering bundle config)"
  - "`node --test sdks/ts/test/` works (test/index.js imports every *.test.ts)"
affects: [09-02, 09-08, 09-09, 09-10, 09-12, 09-15, 09-17, 09-20]
tech-stack:
  added: []
  patterns:
    - "Host isolation: only ostro/_host.py (Python) and src/bindings.ts (TS, 09-09) touch generated bindings"
    - "Recording + scripted fake hosts; op ids from 16 upward, cancellation op 2^64-1"
key-files:
  created:
    - sdks/python/pyproject.toml
    - sdks/python/ostro/__init__.py
    - sdks/python/ostro/_host.py
    - sdks/python/tests/conftest.py
    - sdks/python/tests/test_scaffold.py
    - sdks/ts/package.json
    - sdks/ts/package-lock.json
    - sdks/ts/rolldown.ostro.mjs
    - sdks/ts/src/host.ts
    - sdks/ts/src/index.ts
    - sdks/ts/test/helpers.ts
    - sdks/ts/test/index.js
    - sdks/ts/test/scaffold.test.ts
  modified: []
key-decisions:
  - "Python WIT datetime is integer nanoseconds (now_nanos, sleep_until(at_nanos), RunInfo.deadline); TS keeps the WIT shape {seconds: bigint, nanoseconds: number}"
  - "WIT variants are tagged tuples (Python) / {tag, val} objects (TS, jco's shape) with WIT case names; result<T, failure> is ('ok'|'err', v) / {tag:'ok'|'err', val}"
  - "The step resource's methods are flattened into Host (step_seq/step_complete, stepSeq/stepComplete); log.emit is log_emit/logEmit"
  - "sdks/ts/test/index.js added so the plans' `node --test sdks/ts/test/` runs the suite (Node 24 loads a directory argument as a single module)"
requirements-completed: []
duration: ~35 min
completed: 2026-11-21
---

# Phase 9 Plan 03: Python and TS SDK skeletons Summary

**Two dependency-free SDK skeletons with fixed `Host` contracts that mirror the frozen WIT imports 1:1 (Python `int` op ids, TS `bigint` op ids), recording/scripted fake hosts, and passing scaffold tests on CPython 3.12 (pytest 8.4.2) and Node 24.13.0.**

## Performance

- 1 task, 1 task commit (e742c6f), plus the docs commit.
- Started ~19:35Z, finished ~20:15Z on 2026-11-21.

## Host signatures

### Python (`sdks/python/ostro/_host.py`, `class Host(Protocol)`, runtime_checkable)

Type mapping: op id `int`; `Payload = tuple[str, bytes]`; records are frozen dataclasses (`ErrorInfo`, `RetryPolicy`, `StepOptions`, `RunInfo`, `Header`, `HttpRequest`, `HttpResponse`, `SqlRequest`, `SqlResult`, `ObjectRequest`, `ChildOptions`, `AwaitToken`); variants are tagged tuples `(case, value)` with WIT case names (`Failure`, `StepStart`, `OpOutput`, `SqlValue`); `Outcome = ("ok", v) | ("err", Failure)`; `CompletionAck = "recorded" | "retry-now"`; `CANCELLATION_OP = 2**64 - 1`.

```python
info() -> RunInfo;  now_nanos() -> int;  random_u64() -> int;  random_bytes(n) -> bytes
uuid_v7() -> str;  is_replaying() -> bool;  patched(change_id) -> bool
set_status(json) -> None;  continue_as_new_suggested() -> bool
begin_step(name, opts: StepOptions) -> ("execute", StepHandle) | ("replayed", Outcome)
step_seq(handle) -> int;  step_complete(handle, outcome) -> CompletionAck
http_fetch(req) -> int;  sql_query(req) -> int;  object_store(req) -> int
sleep(duration_ms) -> int;  sleep_until(at_nanos) -> int
wait_signal(name, timeout_ms | None) -> int;  start_child(workflow, input: Payload, opts) -> int
create_await_token(timeout_ms | None) -> AwaitToken;  cancellation() -> int;  cancel_op(op) -> None
wait(ops: list[int], any: bool) -> list[(op, Outcome)]
log_emit(level, message, fields: list[tuple[str, str]]) -> None
```

`install(host | None) -> previous`, `current()` (falls back to `_bindings_host()`, which raises `NotImplementedError("guest bindings land in 09-08")` until 09-08). `ostro/__init__.py` exports only `__version__ = "0.1.0"`.

### TypeScript (`sdks/ts/src/host.ts`, `export interface Host`)

Op ids and every u64/s64 are `bigint`; `Payload = { contentType, data: Uint8Array }`; `Datetime = { seconds: bigint; nanoseconds: number }`; variants are `{ tag, val? }` (jco's shape); `Outcome<T>`; `CANCELLATION_OP = 2n ** 64n - 1n`. Methods: `info, now, randomU64, randomBytes, uuidV7, isReplaying, patched, setStatus, continueAsNewSuggested, beginStep, stepSeq, stepComplete, httpFetch, sqlQuery, objectStore, sleep, sleepUntil, waitSignal, startChild, createAwaitToken, cancellation, cancelOp, wait(ops: bigint[], mode: "all" | "any"), logEmit`. `setHost(h | undefined) -> previous`, `host()` throws `ostro: no host installed ...` when none is set. No `ostro:runtime/*` specifiers; erasable syntax only; `.ts` import extensions. `src/index.ts`: `VERSION = "0.1.0"`, `export type { Host }`.

## FakeHost features (both languages)

- Records every call as `(method, args)` / `[method, args]` in `calls`.
- Allocates op ids from 16 (`FIRST_OP`, `16n`) upward, one per allocating call; `cancellation()` returns `CANCELLATION_OP`; `ops_issued()` / `opsIssued()`.
- `script_begin_step(...)` / `scriptBeginStep(...)` (default: `execute` with a `FakeStep{name, seq}` from seq 1), `script_step_ack` / `scriptStepAck` (default `recorded`), `script_wait(...)` / `scriptWait(...)` (one list per `wait`; an unscripted wait throws).
- Inspectable state: `statuses`, `logs`, `cancelled_ops`/`cancelledOps`, `patches` (default true, memoised), `replaying`, `run_info`/`runInfo`, `now`/`clock`; deterministic splitmix64 `random_u64`, patterned `random_bytes`, counter-based `uuid_v7`.
- Python: `fake_host` fixture installs it via `install()` and restores the previous host.
- Both scaffold suites assert that every WIT import function (parsed from `wit/ostro-runtime-0.1.0/runtime.wit`) has a Host method, so a contract drift fails the tests.

## Verification

- `cd sdks/python && uv run --no-project --python 3.12 --with pytest==8.4.2 python -m pytest tests -q`: `10 passed, 1 skipped` (the skip: `ostro/wit/runtime.wit is copied by 09-02 and is not committed yet`).
- `cd sdks/ts && npm ci --no-audit --no-fund --silent && node --test test/`: `ℹ tests 9, pass 8, fail 0, skipped 1` (same 09-02 skip). It also works from the repo root as `node --test sdks/ts/test/`.
- `sdks/ts/package.json` has neither `dependencies` nor `devDependencies`; `sdks/python/pyproject.toml` has `dependencies = []`.
- No Rust, Cargo, WIT or CI files touched.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] `node --test test/` fails on Node 24.13.0 with a directory argument**
- **Found during:** Task 1
- **Issue:** Node 24 loads a directory passed to `--test` as one module (`Cannot find module '.../sdks/ts/test'`). The plan's verify, the package `test` script, and 09-10/09-12/09-15/09-17 all use `node --test sdks/ts/test/`.
- **Fix:** added `sdks/ts/test/index.js` (not in files_modified), which imports every `*.test.ts` in the directory in name order. Directory resolution lands on it, so both `node --test test/` and `node --test sdks/ts/test/` run all suites. Later plans just add `*.test.ts` files.
- **Commit:** e742c6f

**2. [Plan defect, verify only] `^# pass` / `^# fail 0` greps never match**
- **Issue:** Node 24's default `--test` reporter is `spec` even when piped (`ℹ pass 8`). The TAP lines `# pass N` appear only with `--test-reporter=tap`.
- **Done instead:** ran the plan's command unchanged and grepped `^ℹ pass [1-9]` / `^ℹ fail 0`. Also ran `node --test --test-reporter=tap test/` and grepped `^# pass [1-9]` / `^# fail 0`. Both passed. 09-10, 09-12, 09-15 and 09-17 should add `--test-reporter=tap` or grep the `ℹ` lines.

### Notes

- `rolldown.ostro.mjs`: 09-02 had not committed `sdks/scripts/toolchain/rolldown.ostro.mjs` when this landed. The SDK copy is a 5-line comment (the Pattern 2 async-to-generator lowering) followed by `export default { transform: { target: "es2016" } };`. **09-02 must copy `sdks/ts/rolldown.ostro.mjs` byte-for-byte** so that 09-17's identity check passes.
- The SDK WIT copies (`sdks/python/ostro/wit/runtime.wit`, `sdks/ts/wit/runtime.wit`) belong to 09-02. Until they land, the byte-identity tests skip with a reason that names 09-02. After that they run automatically.
- `pyproject.toml` has no `readme` field yet, because no README exists before 09-20 and hatchling would fail the build. `addopts = "-p no:cacheprovider"` keeps `.pytest_cache` out of the tree.

## Self-Check: PASSED

- All 13 created files are present in `main` at e742c6f (`git show --stat`).
- Commit e742c6f exists on main.
