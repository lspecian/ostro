---
phase: 09-polyglot-sdks-conformance
plan: 12
subsystem: sdks
tags: [python-sdk, ts-sdk, effects, idempotency-key, saga, engine-smoke, wave-4]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-06 Rust Saga/idempotency_key_for/vectors; 09-07 ENG-1/ENG-2 + abandoned-step replay fix; 09-09 Python core; 09-10 TS core; 09-11 harness"
  - phase: 06-effects-bridge-connectors-secrets
    provides: "06-09 Rust effect builders (reference records)"
provides:
  - "Python ostro.effects (HttpBuilder/HttpClient, SqlBuilder + sql_value, ObjectClient), idempotency_key_for, Ctx.http/sql/object/idempotency_key/saga"
  - "Python ostro.saga (Saga, SagaReport, CompensationFailure)"
  - "TS effects.ts (HttpBuilder/HttpClient, SqlBuilder + sqlValue, ObjectClient), idempotencyKeyFor, ctx.http/sql/object/idempotencyKey/saga"
  - "TS saga.ts (Saga, SagaReport)"
  - "conformance/fixtures/{py-smoke,ts-smoke} + crates/ostro-conformance/tests/sdk_engine_smoke.rs (first real-engine runs of SDK-built guests)"
affects: [09-13, 09-14, 09-15, 09-16, 09-17, 09-20]
tech-stack:
  added: []
  patterns:
    - "Effect builders fill the unchanged WIT request records; per-op decoders check the op-output case (Rust protocol error parity)"
    - "Python compensations are zero-arg callables (shielding via contextvar); TS compensations get the explicit shielded ctx"
key-files:
  created:
    - sdks/python/ostro/effects.py
    - sdks/python/ostro/saga.py
    - sdks/python/tests/test_effects.py
    - sdks/python/tests/test_saga.py
    - sdks/python/tests/test_vectors.py
    - sdks/ts/src/effects.ts
    - sdks/ts/src/saga.ts
    - sdks/ts/test/effects.test.ts
    - sdks/ts/test/saga.test.ts
    - sdks/ts/test/vectors.test.ts
    - conformance/fixtures/py-smoke/app.py
    - conformance/fixtures/ts-smoke/index.ts
    - crates/ostro-conformance/tests/sdk_engine_smoke.rs
  modified:
    - sdks/python/ostro/ctx.py
    - sdks/python/ostro/__init__.py
    - sdks/ts/src/ctx.ts
    - sdks/ts/src/index.ts
key-decisions:
  - "SQL params mirror Rust's sql-value records (not JSON): Python/TS map plain values (None/null, bool, int/bigint/integral number -> int8, float -> float8, str -> text, bytes/Uint8Array -> bytea, datetime/Date -> timestamptz); tagged values pass through"
  - "Durations follow each SDK's convention: Python seconds/timedelta, TS milliseconds"
  - "Saga.fail_with/failWith re-raise an OstroError itself (earlier details under \"details\"); any other exception becomes non-retryable python_exception/js_exception; continue-as-new passes through unchanged"
  - "No SDK core fixes were needed: the engine smoke found no deviation"
requirements-completed: [WF-10, SDK-02, SDK-03, SDK-04]
duration: ~1 h 15 min
completed: 2026-11-22
---

# Phase 9 Plan 12: Python/TS effects, idempotency key, saga and first real-engine runs Summary

**Python and TypeScript now have Rust-parity HTTP/SQL/object builders, the P-11 idempotency key and the D-P9-18 saga helper, all checked against the cross-SDK vectors. SDK-built Python, QuickJS and StarlingMonkey guests ran on the real `ostro-engine` for the first time: 9 smoke workflows each, strict replay (0 appended), byte-identical journals across two runs, and identical host-drawn values across the three builds. No SDK deviation showed up.**

## Commits

- `08a8440` feat(09-12): Python effect builders, idempotency_key and saga helper (7 files)
- `421051d` feat(09-12): TS effect builders, idempotencyKey and saga helper (7 files)
- `20d9f8d` feat(09-12): first real-engine runs of SDK-built Python/TS guests (3 files)

## Builder APIs

**Python** (`ostro.effects`, durations in seconds or `timedelta`):

```python
resp = await (ctx.http.post(url)                 # get/post/put/delete/request(method, url)
    .header(name, value)
    .secret_header("Authorization", "Bearer {{secret:stripe}}")   # ValueError without {{secret:
    .json(value)                                 # replaces content-type with application/json
    .body(bytes_or_str)
    .idempotency_key(ctx.idempotency_key("charge"))
    .timeout(10).retry(ostro.Retry(max_attempts=5)).redact("Authorization")
    .send())                                     # -> HttpResponse; .build() -> WIT HttpRequest
res = await ctx.sql("db").exactly_once().query(stmt, [7, "x", None])    # -> SqlResult; .request() -> SqlRequest
o = ctx.object("files")                          # get -> bytes, put -> ObjectPutInfo(e_tag),
                                                 # head -> ObjectHead(size, last_modified, e_tag, version),
                                                 # delete -> None, presign_get/presign_put -> str, raw(op, key, body) -> bytes
ostro.idempotency_key_for(run_id, name); ctx.idempotency_key(name)
saga = ctx.saga()  # or ostro.Saga(ctx); saga.add(name, fn); await saga.compensate() -> SagaReport; await saga.fail_with(exc)
```

**TypeScript** (`effects.ts`, durations in ms, u64 fields as bigint):

```ts
const resp = await ctx.http.post(url).header(n, v).secretHeader("Authorization", "Bearer {{secret:stripe}}")
  .json(value).body(bytesOrString).idempotencyKey(ctx.idempotencyKey("charge"))
  .timeout(10_000).retry({ maxAttempts: 5 }).redact("Authorization").send();   // .build() -> HttpRequest
const res = await ctx.sql("db").exactlyOnce().query(stmt, [7, "x", null]);    // .request() -> SqlRequest
const o = ctx.object("files");   // get/put/head/delete/presignGet/presignPut/raw; ObjectPutInfo {eTag}, ObjectHead {size, lastModified, eTag, version}
idempotencyKeyFor(runId, name); ctx.idempotencyKey(name)   // sync SDK sha256, no crypto.subtle
const saga = ctx.saga();  saga.add(name, (s) => step(...)); await saga.compensate(); await saga.failWith(e)
```

- **Record parity:** every builder test writes the record the Rust builder produces for the same inputs (06-09 / `effects.rs`) and compares field for field. The engine smoke also checks that all three builds journal the same `EffectScheduled` requests.
- **Saga details** match Rust exactly: `{"compensated": [...], "compensation_failures": [{"compensation", "error": {kind, message, retryable}}]}`, plus earlier details under `"details"`.
- **Shielding:**
  - Python: compensations run inside `with ctx.shielded()`, and the contextvar carries the shield into steps and tasks.
  - TS: compensations receive the shielded ctx `s` (there is no AsyncContext), so ops must be created through `s`.

## Engine smoke (Task 3)

`crates/ostro-conformance/tests/sdk_engine_smoke.rs` (`--features polyglot`):
- **Setup:** `Runtime` with `class = python|typescript` and `hot_wait = 0`, a frozen `FakeClock`. The testkit's `drive_to_completion` is the scheduler. 09-11's `engine_leg` cannot load arbitrary fixtures (its module source is `langs::module_for`), and its `src/` is not in this plan's files.
- **Per workflow, each run checks:**
  - the outcome and the journal shape;
  - a second run gives a byte-identical journal (`compare::journals_identical`);
  - replay returns the same outcome and appends 0 events.

| Workflow | Checked |
|----------|---------|
| `smoke_steps` | output `["a","b","c","d"]`; step order a, b, c, d (gather/`Promise.all` serialised) |
| `smoke_race` | signal wins, loser timer `OpCancelled`, its await raises Cancelled |
| `smoke_sleep` | `asyncio.sleep(3600)` / `ctx.sleep(3_600_000)` gives one `TimerStarted` at accepted_at + 1 h |
| `smoke_child` | `ChildStarted{leaf_echo, {"x":1}, attached}`; output from the fed `ChildCompleted` |
| `smoke_saga` | steps book_a, book_b, book_c, cancel_b, cancel_a; run fails `book_c_failed` with the saga details |
| `smoke_saga_cancel` | cancel during a 1 h sleep; cancel_b and cancel_a still run; run ends `Failed{Cancelled}` |
| `smoke_rand` | key = P-11 of the run id; f64/native in [0,1); native time = logical accepted_at |
| `smoke_print` | print/stderr and console.log/error go to the log only (journal = RunStarted, RunCompleted) |
| `smoke_effects` | HTTP (secret template journaled `[redacted]` per W3), SQL (int8/text/null/float8, exactly_once), object put; decoded responses |

`host_values_agree_across_languages`: for the same seed and run id, `random_f64`, `random_u64`, `uuid7` and `idempotency_key("x")` are equal in Py, QJS and SM.

**Timings** (debug build, cached components, `--test-threads=1`):

| Backend | Component | prepare | First run per workflow | Replay | All 9 workflows (2 runs + replay each) |
|---------|-----------|---------|------------------------|--------|-----------------------------------------|
| Python (componentize-py) | 21.88 MB | 2.7–3.0 s | 19–98 ms (40 ms typical) | 17–21 ms | 0.71 s |
| TS QuickJS | 2.16 MB | 0.97–1.1 s | 3–22 ms | 3–6 ms | 0.17 s |
| TS StarlingMonkey | 13.0 MB | 3.1–3.5 s | 3–22 ms | 3–7 ms | 0.18 s |

Fresh builds take about 4 s (py), 6.7 s (ts) and 4 s (ts-sm). The whole binary takes 16–27 s.

## Deviation log from the engine smoke

**No deviation found.** Every workflow passed on the first engine run in all three builds. This covers steps and gather, race and cancel, durable sleep, child start, saga, saga under cancel, randomness and time, print, and effects. The only assertion that needed changing was in the test itself: the redacted `Authorization` header is journaled as `[redacted]`, as W3 specifies. No SDK core file (`loop`, `step`, `_bindings`, `_determinism`, `bindings.ts`, `determinism.ts`, ...) was changed, so there are no regression tests to add. 09-07's abandoned-step replay fix was not exercised: the SDKs always close their steps.

## Verification

- `uv run --no-project --python 3.12 --with pytest==8.4.2 python -m pytest tests -q`: **148 passed**, no skips. That is 118 existing plus test_effects 20 (including 4 parametrised method cases), test_saga 7 and test_vectors 3.
- `node --test --test-reporter=tap sdks/ts/test/`: **# pass 99, # fail 0, # skipped 0**. That is 76 existing plus effects 13, saga 7 and vectors 3.
  - Note: Node 24's default reporter prints `ℹ pass N`. The plan's `^# pass` grep needs `--test-reporter=tap`.
- `cargo test -p ostro-conformance --features polyglot --locked --test sdk_engine_smoke -- --test-threads=1`: **test result: ok. 4 passed**.
- Regression of neighbouring polyglot tests with the new SDK sources: `--test ts_sdk_build` 3 passed, including the acorn lowering check (0 unlowered async with effects/saga in the bundle); `--test py_sdk_build` 1 passed; `--test wit_copies` 4 passed.
- `cargo clippy -p ostro-conformance --features polyglot --all-targets --locked -- -D warnings`: clean. `cargo fmt -p ostro-conformance -- --check`: clean.

## Deviations from Plan

1. **[Plan wording] SQL params.** The behaviour line says "JSON-encoded params as Rust does". Rust actually sends WIT `sql-value` records (06-09 `SqlBuilder::request`), so both SDKs build `sql-value` records too. They auto-map plain values the same way fx-sql does.
2. **Extra smoke workflows.** I added `smoke_saga_cancel` (saga under run cancellation on the real engine) and `smoke_effects` (builders through the real bindings and host). Neither was required by the plan.
3. **Scheduler.** The smoke uses the ostro-engine testkit scheduler, not `ostro_conformance::engine_leg`, because `engine_leg` only loads `conformance/guests/*` modules. The plan says "where possible".
4. **Python compensations take no arguments.** The context carries the shield. TS compensations take the shielded ctx, because TS has no AsyncContext. Rust passes `&Ctx`.

## Open issues

- None blocking.
- The plan's TS verify grep (`^# pass`) needs `--test-reporter=tap` on Node 24. ci-local (09-17) should use the TAP reporter or grep for `ℹ pass`.

## Self-Check: PASSED

- FOUND at main: every created/modified key file above.
- FOUND: 08a8440, 421051d, 20d9f8d in `git log main`.
