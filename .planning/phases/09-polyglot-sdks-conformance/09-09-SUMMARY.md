---
phase: 09-polyglot-sdks-conformance
plan: 09
subsystem: python-sdk
tags: [python, sdk, asyncio, componentize-py, determinism, steps, cancellation, wf-07]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-02 build-guest.sh py, ostro-conformance inspect/polyglot, SDK WIT copy"
  - phase: 09-polyglot-sdks-conformance
    provides: "09-03 ostro._host.Host protocol + FakeHost fixture"
  - phase: 09-polyglot-sdks-conformance
    provides: "09-04 S9: export class naming, OstroLoop prototype, no-lazy-import rule"
provides:
  - "sdks/python/ostro: OstroLoop, Op/ChildRun, errors, json, Ctx, @step/Retry, @workflow/export, _determinism, _bindings (wit_world adapter)"
  - "118 host tests (CPython 3.12): B1 exclusivity live == replay, D11 shielding, closed-after-settle, determinism patches, bindings conversions checked against generated componentize-py 0.25.1 bindings"
  - "sdks/python/examples/hello + crates/ostro-conformance/tests/py_sdk_build.rs (guest build + import/export shape)"
affects: [09-12, 09-14, 09-16, 09-17, 09-20]
tech-stack:
  added: []
  patterns:
    - "SDK-owned asyncio loop: drain -> one effects.wait(sorted ops [+ CANCELLATION], any) -> resolve in journal order"
    - "step exclusivity via a contextvar carried in Handle._context; deferred handles restored to the front in order; done always resolved via call_soon"
    - "determinism patches route to the host only while an activation runs (originals otherwise, so pre-init and pytest are unaffected)"
key-files:
  created:
    - sdks/python/ostro/loop.py
    - sdks/python/ostro/ops.py
    - sdks/python/ostro/errors.py
    - sdks/python/ostro/json.py
    - sdks/python/ostro/ctx.py
    - sdks/python/ostro/step.py
    - sdks/python/ostro/_determinism.py
    - sdks/python/ostro/export.py
    - sdks/python/ostro/_bindings.py
    - sdks/python/ostro/py.typed
    - sdks/python/tests/test_loop.py
    - sdks/python/tests/test_ops_errors.py
    - sdks/python/tests/test_step.py
    - sdks/python/tests/test_ctx.py
    - sdks/python/tests/test_determinism.py
    - sdks/python/tests/test_export.py
    - sdks/python/examples/hello/app.py
    - crates/ostro-conformance/tests/py_sdk_build.rs
  modified:
    - sdks/python/ostro/__init__.py
    - sdks/python/ostro/_host.py
    - sdks/python/tests/conftest.py
key-decisions:
  - "ostro.export() binds Workflows (a wit_world.exports.Workflows subclass from ostro._bindings) into the calling module and returns it; outside a guest it returns a plain HostWorkflows over the tuple types"
  - "Determinism patches are host-routed only while an activation runs; outside, they call the originals (no host call at pre-init, PF-W8)"
  - "Journaled calls raise ProtocolError inside an open step instead of queueing on the step lock; with B1 exclusivity no sibling can run during a step, so the lock only serialises steps"
  - "Timer deadlines are now + round(delay in ms): exact for asyncio.sleep/call_later, millisecond-snapped for call_at (wait_for/timeout)"
  - "Unshielded asyncio.sleep timers fail with ostro.Cancelled on CANCELLATION like ctx.sleep ops; other timer callbacks keep their durable timer"
  - "componentize-py.toml is not shipped: it conflicts with build-guest.sh's explicit -d (09-17 decides)"
requirements-completed: []
duration: ~45 min
completed: 2026-11-21
---

# Phase 9 Plan 09: Python SDK core Summary

**The Python workflow SDK: an SDK-owned asyncio loop over `effects.wait`, `@workflow`/`@step`/`Ctx` with serialised and exclusive steps that interleave identically live and on replay, D11 cancellation and shielding, the WF-07 determinism surface installed at import, and the componentize-py `Workflows` export. 118 host tests pass on CPython 3.12. The SDK-built hello guest has only `ostro:runtime/*` imports and also runs to completion inside the embedded CPython 3.14 on S9's fake host.**

## Commits
- `62f1d19` feat(09-09): OstroLoop, ops, errors and JSON codec (6 files)
- `482af03` feat(09-09): Ctx, steps with exclusivity, determinism surface and export (14 files)
- `671d21c` feat(09-09): Python SDK hello example guest and build/inspect test (2 files)

## Public API (`import ostro`)
| Name | What |
|------|------|
| `workflow(fn=None, *, name=None, version=1, queue=None, run_timeout=None)` | Registers `async def fn(ctx, input)` (sync functions also work). Names must be unique (a duplicate raises at import). The name is checked for lone surrogates. |
| `step(fn=None, *, name=None, retry=None, timeout=None, fuel=None)` | Durable step over a sync or async function; call it with `await charge(x)`. |
| `Retry(max_attempts, initial, max, backoff, non_retryable=())` | Unset fields default to 3 / 1 s / 60 s / 2.0. `retry=None` means no host retries (PF-W6). |
| `Ctx` | Context reads: `info()`, `now()` (aware UTC), `now_nanos()`, `random_u64()`, `random_f64()`, `random_bytes(n)`, `uuid7()`, `is_replaying()`, `continue_as_new_suggested()`, `log(level, msg, **fields)`.<br>Journaled calls: `patched(id)`, `set_status(json_value)`.<br>`continue_as_new(input, *, workflow=None, version=None)`.<br>Effects, each returning an `Op`: `sleep(d)`, `sleep_until(datetime or epoch s)`, `wait_signal(name, timeout=None)`, `start_child(workflow, input, *, queue, version, detached, run_timeout)`.<br>`create_await_token(timeout=None) -> (Op, token)`, `cancel(op)`, `cancellation() -> Op`, `shielded()` (usable with `with` or `async with`), `cancelled` property.<br>Durations are seconds or a `timedelta`. |
| `Op`, `ChildRun(run_id)` | `Op` is eager and awaitable. Decoded results: timer → `None`; signal, token and attached child → JSON value; detached child → `ChildRun`; http, sql and object → raw records (09-12 adds builders). |
| Errors | `OstroError` → `NonRetryable(kind, message, details)`, `Retryable(...)`, `Timeout`, `Cancelled`, `ProtocolError` / `Deadlock` (`sdk_protocol`), `InvalidString` (`invalid_string`). All subclass `Exception`, never `asyncio.CancelledError`. `errors.to_failure` / `from_failure` round-trip every WIT failure variant. |
| `export()` | Freezes the registry and binds `Workflows` in `app`. Runs `gc.collect()` + `gc.freeze()`. |
| `OstroLoop`, `f64_from_u64`, `WorkflowMeta`, `RunInfo`, `__version__` | Also exported. |

**`run` result mapping (`ostro.export.run`):**

| Workflow outcome | WIT result |
|------------------|------------|
| returned value | `completed(application/json)` |
| `continue_as_new` | `continue-as-new` (the signal is a `BaseException`, so `except Exception` does not swallow it) |
| `Cancelled` | `failure::cancelled` |
| `Timeout` | `failure::timeout` |
| `OstroError` | its own kind |
| any other exception | non-retryable `python_exception` with message `"Type: message"` |
| unknown workflow name | non-retryable `unknown_workflow` |
| invalid input JSON | non-retryable `invalid_json` |

**Inside step bodies:**
- A body exception that is not an `OstroError` becomes a **retryable** `python_exception`.
- A `retry-now` ack re-runs the body on the same handle.
- The returned value is always decoded from the journaled JSON, so live equals replay.

## Step exclusivity (B1) as built
- `run_step` takes the per-activation `asyncio.Lock` (FIFO) and calls `begin_step`.
  - **Replayed:** `loop.call_soon(done.set_result, outcome)`.
  - **Execute:** a driver task runs the body in a copied context with `_current_step = id`. `loop._open_step` is set.
  - While a step is open, `OstroLoop._drain` defers every handle whose `h._context.get(_current_step)` is not the open id.
- On close, the deferred handles go back to the front of the ready queue in their original order (`extendleft(reversed(deferred))`), and only then is `call_soon(done.set_result, outcome)` queued.
- The caller releases the lock after `await done` on both paths, so lock hand-off order is also identical.
- **Quiescence with a step open:** the step completes `non-retryable sdk_protocol "step bodies cannot await durable operations"` and the body task is cancelled.
- **Loop timers inside a step** (`call_later`, `call_at`, so also `asyncio.sleep(d > 0)`, `timeout` and `wait_for`) raise `ProtocolError`. `asyncio.sleep(0)` is a plain yield.
- **Host tests proving B1:**
  - A sibling's `random_u64` and `sleep` happen only after `step_complete`.
  - Siblings A and B run in order before the caller, and live and replay produce equal call lists apart from `step_complete`.
  - A 3-hop sibling produces the same marker/value trace live and on replay (`caller-before, sib, caller-after, sib, sib`).

## asyncio internals relied on (3.12 / 3.14)
- `asyncio.events._get_running_loop` / `_set_running_loop`.
- `asyncio.Handle` (`_run`, `_cancelled`, `_context`) and `asyncio.TimerHandle` (subclassed with a `__slots__ = ("_ostro_op",)` slot; `cancel()` calls `loop._timer_handle_cancelled`).
- `asyncio.futures._set_result_unless_cancelled`: identifies `asyncio.sleep` timers for D11.
- `asyncio.Task(coro, loop=, name=, context=, **kwargs)`. 3.14 forwards `eager_start` through `create_task`, and `create_task` accepts `**kwargs`.
- `asyncio.Lock`: lazily binds to the running loop.
- `asyncio.sleep` calls `h.cancel()` in `finally` even after the timer fired. Fired timers are removed from `_timers`, so no spurious `cancel-op` is journaled (S9 finding).
- `inspect.isawaitable` / `iscoroutinefunction` are used instead of the deprecated `asyncio.iscoroutinefunction` (3.14).
- The SDK imports everything at `import ostro` (a first import inside `run` traps under `--stub-wasi`, S9). `_bindings` is imported by `export()` during pre-init.
- **Evidence:**
  - Full host suite: 118 passed on CPython 3.12.4.
  - The same suite passed on a uv-managed 3.14.0b2 (informational).
  - In-guest run on the real embedded CPython 3.14.0: see below.

## Example guest (Task 3)
- `py_sdk_build`, `hello` built by `build-guest.sh py`:
  - **21,888,914 bytes**, which is +0.3% vs S9 `py-asyncio` (21,820,473), well under the 25% note threshold;
  - **core_instances 111** (09-02's inspector, Instantiate + FromExports; identical to the raw `py-min` fixture);
  - core_modules 38;
  - imports `ostro:runtime/{types,context,journal,effects,log}@0.1.0`;
  - exports `{exports, ostro:runtime/workflows@0.1.0}`.
- **Bonus in-guest smoke** (not a plan requirement): S9's harness `fuel` subcommand (wasmtime 50.0.2, fake host, production-equivalent fuel config) ran `hello` on the SDK-built component:
  - `completed {"greeting":"hello, {}","lucky":0.8833108082136426}`;
  - steps_executed 1, waits 1 (the two gathered sleeps share one wait);
  - fuel 1,558,969, which is 0.016% of `activation_fuel`; describe 56,746;
  - fuel identical on a fresh instance.
  - `lucky` equals `f64_from_u64` of the same splitmix64 draw on the host, so the cross-SDK random_f64 mapping holds in the guest.
- **The first real-engine run of SDK-built Python guests is 09-12 Task 3.** It needs 09-07 ENG-1, the `exports` export allowance.

## Test results
- `uv run --no-project --python 3.12 --with pytest==8.4.2 python -m pytest tests -q`: **118 passed**.
  - Files: test_loop 22, test_ops_errors 17, test_step 19, test_ctx 14, test_determinism 16, test_export 19, test_scaffold 11.
  - The 09-06 JSON and random_f64 vectors are loaded and pass.
  - The `_bindings` tests generate bindings with `uv tool run componentize-py==0.25.1 ... bindings`. They skip with a reason if that fails.
- Task 1 verify: 39 passed. Task 2 verify: 118 passed.
- `cargo test -p ostro-conformance --features polyglot --locked --test py_sdk_build -- --nocapture`: `test result: ok. 1 passed`.
- `cmp wit/ostro-runtime-0.1.0/runtime.wit sdks/python/ostro/wit/runtime.wit`: identical.
- `cargo clippy -p ostro-conformance --features polyglot --all-targets --locked -- -D warnings`: clean. `cargo fmt -p ostro-conformance -- --check`: clean. The test build has 0 warnings.

## Deviations from Plan
1. **[Rule 3 - build conflict] No `componentize-py.toml`.** The brief asked for `sdks/python/ostro/componentize-py.toml` with `wit_directory = "wit"`. With it staged, `build-guest.sh py` fails, because the script also passes `-d wit/ostro-runtime-0.1.0`:
   `AssertionError: failed to resolve directory while parsing WIT for path [.../wit/ostro-runtime-0.1.0] Caused by: attempting to re-add package 'ostro:runtime@0.1.0' when it's already present in this 'Resolve'`.
   I left the file out. 09-17 owns the build script and can either drop `-d` and ship the toml (S9 showed that works), or keep things as they are.
2. **[Plan defect] The allowed imports include `ostro:runtime/types@0.1.0`.** The plan lists imports ⊆ {context, journal, effects, log}, but componentize-py always imports the type-only `types` interface, as do 09-02's raw fixtures. The test allows `types` and requires the other four.
3. **`_host._bindings_host()` now delegates to `ostro._bindings`.** It raises `NotImplementedError` outside a guest. The message keeps the "09-08" reference so 09-02's scaffold test stays green unedited.
4. **Journaled calls inside a step raise; they do not queue on the step lock.** The plan's "every journaled entry point acquires the step lock queue" is covered by B1 exclusivity: no non-step code can run while a step is open, so the lock only serialises steps. Outside a step every journaled call checks `closed`, the open step and the gc guard.
5. **D11 extension for asyncio timers.** An unshielded `asyncio.sleep` fails with `ostro.Cancelled` on CANCELLATION and is remembered afterwards (no new timer is journaled). `wait_for`/`timeout` timers keep their durable timer; their inner op fails instead.
6. **Differences from Rust semantics, with reasons:**
   - `decode_payload` returns raw `bytes` for a non-JSON content type, where Rust would try to parse it as JSON. Empty data is `None` in both.
   - Unexpected step-body exceptions are retryable `python_exception` (the plan's rule). Rust has no uncaught-exception case.
   - Remembered cancellation means Python does not re-issue `wait([op, CANCEL])`. The 09-11 projection normalises this.
   - `random.getrandbits(k > 64)` concatenates ⌈k/64⌉ host draws (Python-specific, so per-language values).
7. **Activation error policy.** Only exceptions escaping a scheduled callback fail the activation. GC-timed reports, such as "Task exception was never retrieved" or a destroyed pending task, are ignored so finalizer timing cannot change behaviour.
8. **The in-guest harness smoke is extra.** It used the throwaway S9 harness, with its lock seeded locally and not committed.

## Notes for later plans
- **09-12:**
  - The http, sql and object `Op`s return raw `HttpResponse` / `SqlResult` / `bytes`. Add the builders, `idempotency_key` (P-11, vectors in `conformance/vectors/idempotency.json`) and saga on top. `ctx.shielded()` is the D-P9-18 hook.
  - `ostro._bindings` already converts every effect record and `SqlValue` variant.
- **09-14:**
  - Conformance guests should end `app.py` with `ostro.export()`.
  - Iterate sets of str through `sorted()`: the hash seed differs per build (S9).
- **09-17:** the componentize-py.toml decision (deviation 1). The hello example is 21.9 MB.

## Self-Check: PASSED
- FOUND at main: every key-files path above.
- FOUND: 62f1d19, 482af03 and 671d21c in `git log main`.
