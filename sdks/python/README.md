# Ostro Python SDK

Write durable Ostro workflows in plain `asyncio` Python. The SDK targets the frozen WIT
contract `ostro:runtime@0.1.0` (`wit/ostro-runtime-0.1.0/runtime.wit`; a byte-identical
copy ships as `ostro/wit/runtime.wit`). A workflow module is a WebAssembly component built
with componentize-py 0.25.1. It imports only `ostro:runtime/*` and exports `workflows`,
plus componentize-py's `exports` init hook, which the engine accepts by exact shape
(ENG-1).

- Package: `ostro` 0.1.0, pure Python, no dependencies, `requires-python >= 3.12`. Host
  unit tests run on CPython 3.12; guest code runs on componentize-py's embedded CPython 3.14.
- Status: the whole three-language conformance suite passes in Python (27 engine-leg and
  16 server-leg scenarios, cross-compared with Rust; see [docs/conformance.md](../../docs/conformance.md)).
- Not yet published to PyPI (see [Publishing](#publishing)).

## Build a module

Every polyglot build goes through one pinned script. Phase 10 adds `ostro build`, which
wraps the same recipe.

```bash
# Needs uv 0.7.15 exactly (the script refuses other versions) and a Python 3.12 that
# `uv python find 3.12` sees. componentize-py 0.25.1 is fetched through `uv tool run`.
sdks/scripts/build-guest.sh py sdks/python/examples/hello hello.wasm

ostro dev &
ostro deploy hello.wasm --activate
ostro run hello --input '"world"'
ostro result <run-id> --wait
```

The guest directory holds an `app.py` entry module. The script stages it into a fresh
directory next to this SDK's `ostro/` package, sets every mtime to `SOURCE_DATE_EPOCH`,
exports `PYTHONHASHSEED=0 PYTHONDONTWRITEBYTECODE=1 TZ=UTC LC_ALL=C`, and runs:

```bash
componentize-py -d wit/ostro-runtime-0.1.0 -w ostro:runtime/workflow@0.1.0 \
  componentize --stub-wasi -p <stage> app -o <out.wasm>
```

`OSTRO_COMPONENTIZE_PY=<path>` selects the deterministic componentize-py fork instead of
the released wheel (see [Reproducibility](#reproducibility)). The exact recipe and pins
are in [docs/sdk-builds.md](../../docs/sdk-builds.md).

## A complete example

```python
import asyncio
import ostro


@ostro.step(retry=ostro.Retry(max_attempts=5), timeout=30)
async def charge(ctx_key: str, amount: int) -> dict:
    # Step bodies run once per attempt; their result is journaled.
    return {"charged": amount, "key": ctx_key}


@ostro.step
def ship(order_id: str) -> str:
    return f"shipment-{order_id}"


@ostro.workflow(name="order", version=1, queue="orders", run_timeout=24 * 3600)
async def order(ctx: ostro.Ctx, inp: dict) -> dict:
    saga = ctx.saga()
    payment = await charge(ctx.idempotency_key("charge"), inp["amount"])
    saga.add("refund", lambda: ctx.http.post("https://pay.example/refund")
             .json({"key": payment["key"]}).send())

    # Wait for approval or give up after one day (both durable).
    sleep, approve = ctx.sleep(24 * 3600), ctx.wait_signal("approve")
    t_approve = asyncio.ensure_future(approve)
    done, _ = await asyncio.wait([t_approve, asyncio.ensure_future(sleep)],
                                 return_when=asyncio.FIRST_COMPLETED)
    if t_approve in done:
        ctx.cancel(sleep)                 # losers are never cancelled implicitly
    else:
        ctx.cancel(approve)
        await saga.fail_with(ostro.NonRetryable("not_approved", "no approval in 24 h"))

    try:
        tracking = await ship(inp["order_id"])
    except ostro.OstroError as e:
        await saga.fail_with(e)           # runs "refund", then re-raises e
    ctx.set_status({"stage": "shipped"})
    return {"tracking": tracking, "child": await ctx.start_child("notify", {"id": inp["order_id"]})}


ostro.export()                            # last line of app.py: binds the `workflows` export
```

## API reference

Everything below is importable from `ostro`.

| Name | What it does |
|------|--------------|
| `@workflow(name=None, version=1, queue=None, run_timeout=None)` | Registers `async def fn(ctx, input)`. Sync functions also work. A duplicate name raises at import. |
| `@step(name=None, retry=None, timeout=None, fuel=None)` | Declares a durable step over a sync or async function. Call it as `await fn(args)`. |
| `Retry(max_attempts, initial, max, backoff, non_retryable=())` | Unset fields default to 3 / 1 s / 60 s / 2.0. `retry=None` means no host retries. |
| `export()` | Freezes the registry, binds the `Workflows` class in `app`, runs `gc.collect()` and `gc.freeze()`. Call it once, at the end of `app.py`. |
| `Ctx` reads | `info()`, `now()` (aware UTC `datetime`), `now_nanos()`, `random_u64()`, `random_f64()`, `random_bytes(n)`, `uuid7()`, `is_replaying()`, `continue_as_new_suggested()`, `cancelled`, `log(level, msg, **fields)`. |
| `Ctx` journaled calls | `patched(change_id)`, `set_status(value)`, `continue_as_new(input, *, workflow=None, version=None)`. |
| `Ctx` ops (each returns an awaitable `Op`) | `sleep(seconds or timedelta)`, `sleep_until(datetime or epoch s)`, `wait_signal(name, timeout=None)`, `start_child(workflow, input, *, queue, version, detached, run_timeout)`, `create_await_token(timeout=None) -> (op, token)`, `cancellation()`. |
| `ctx.cancel(op)` | `cancel-op` (D5). The op then fails with `Cancelled` unless it completed earlier in journal order. On a child op it also cancels the child run. |
| `ctx.shielded()` | `with` / `async with` scope whose ops are not failed by run cancellation (D11). |
| Effects | `ctx.http.get/post/put/delete/request(...)` builder (`.header`, `.secret_header`, `.json`, `.body`, `.idempotency_key`, `.timeout`, `.retry`, `.redact`, `.send()`), `ctx.sql(conn).exactly_once().query(stmt, params)`, `ctx.object(conn).get/put/head/delete/presign_get/presign_put/raw`. |
| Idempotency | `ctx.idempotency_key(name)`, `idempotency_key_for(run_id, name)`: the P-11 key, byte-equal to the Rust and TS SDKs. |
| Saga | `ctx.saga()` or `Saga(ctx)`: `add(name, fn)`, `await compensate() -> SagaReport`, `await fail_with(exc)`. |
| Children | attached `start_child` resolves to the child's JSON output; detached resolves to `ChildRun(run_id)`. |
| Errors | `OstroError` → `NonRetryable(kind, message, details)`, `Retryable(...)`, `Timeout`, `Cancelled`, `ProtocolError` / `Deadlock` (`sdk_protocol`), `InvalidString` (`invalid_string`). None of them is an `asyncio.CancelledError`. |

**How a workflow ends.** A returned value → `completed` (JSON). `continue_as_new` →
`continue-as-new` (a `BaseException`, so `except Exception` cannot swallow it).
`Cancelled` / `Timeout` → the matching WIT failure. Any `OstroError` → its own kind. Any
other uncaught exception → non-retryable `python_exception` (`"Type: message"`). Inside a
step body, an exception that is not an `OstroError` is a **retryable** `python_exception`.

## The event-loop model

The SDK runs its own `asyncio` event loop (`OstroLoop`) on top of `effects.wait`. Nothing
blocks on real I/O. When every task is waiting on durable ops, the loop calls `wait` and
the host either completes ops or suspends the activation.

What works:
- `asyncio.gather`, `asyncio.wait(..., FIRST_COMPLETED)`, `asyncio.create_task`, `Lock`,
  `Event`, `Queue`, contextvars.
- `asyncio.sleep(d)`, `asyncio.timeout`, `asyncio.wait_for`, `loop.call_later` and
  `loop.call_at` map to **durable** `sleep-until` timers (D-P9-2). `TimerHandle.cancel()`
  maps to `cancel-op`. `loop.time()` is logical time. `asyncio.sleep(0)` is a plain yield.

What does not work: sockets, subprocesses, executors (`run_in_executor`), signal handlers
and other OS I/O. These loop methods raise `NotImplementedError`. Use effects instead.

**Op order.** An op id is allocated when the op is created. `ctx.sleep(...)` and
`ctx.wait_signal(...)` allocate at once. A coroutine such as `asyncio.sleep(...)` passed to
`gather` allocates its timer only when its task first runs. Changing which form you use
changes the journal, so treat it like any other code change (`patched`).

## Determinism rules (WF-07)

Importing `ostro` installs the determinism surface, so ordinary Python stays
replay-identical (D-P9-15):

- **Randomness is host-routed.** `random.random()` and the module-level `random`
  functions, `os.urandom`, `secrets`, `random.SystemRandom` and `uuid.uuid4` draw from the
  host stream. Explicitly seeded `random.Random(seed)` instances are untouched.
- **Time is logical.** `time.time()`, `time.time_ns()`, `monotonic`, `perf_counter`, the
  argument-less `localtime`/`gmtime`/`strftime`, and `datetime.now()`/`utcnow()`/`today()`
  return the activation's logical time (`context.now`), identical on replay.
- **Maps.** `dict` iteration is insertion-ordered and deterministic. `set` iteration over
  `str` depends on the build's hash seed, which differs per build, so iterate such sets
  through `sorted()`.
- **stdout/stderr → `log.emit`.** `print` goes to the run's log (`info`, `error` for
  stderr). Logs are never journaled and are suppressed on replay, so never base control
  flow on them.
- **Finalizers.** A host call from a finalizer raises `ProtocolError` (GC timing differs
  live vs replay). `export()` calls `gc.freeze()`.

WF-07 evidence: scenario C-NATIVE-01 (native `time`, `random`, `uuid4` and dict order are
replay-identical and change with the seed) and C-RAND-02 pass in Python.

## Frozen semantics the SDK relies on

These rules are frozen in the WIT since Phase 5. The SDK implements them; workflow code
must respect them.

- **D5 cancel-op.** `ctx.cancel(op)` completes the op with `Cancelled` unless it already
  completed earlier in journal order (first completion wins). Losers of a race are never
  cancelled implicitly.
- **D6 patched.** `ctx.patched("change-id")` is journaled and memoised per run: new runs
  get `True`, replays of old runs get the recorded answer. Remove the branch once no old
  run needs it.
- **D7 step-local RNG.** Inside a step body, `random.random()`, `ctx.random_u64()` and the
  other draws use a per-attempt step-local stream. Draws inside a step never consume the
  workflow's stream.
- **D11 cancellation and shielding.** A graceful cancel (`POST /v1/runs/{id}/cancel`) makes
  every unshielded await raise `ostro.Cancelled`. Once delivered, the cancellation is
  remembered. Cleanup belongs in `async with ctx.shielded():`, where awaits run to
  completion. Terminate and run timeouts are hard stops: no guest code runs.
- **F4 attempt is for logging.** `ctx.info().attempt` grows on every re-dispatch. Use it
  in logs only; branching on it breaks replay.
- **PF-W1 open-step rule and step serialisation (step exclusivity).** No other task runs
  while a step is open. Steps are serialised by an SDK FIFO lock in call order. Handles of
  other tasks are deferred and go back to the front of the queue when the step closes.
  The caller then resumes after the same sibling work on live runs and on replay. A step
  body cannot await durable operations. Loop timers inside an open step (`asyncio.sleep(d > 0)`,
  `asyncio.timeout`, `wait_for`, `call_later`) raise `ProtocolError` (`sdk_protocol`, D-P9-16),
  and a step whose body waits on anything durable completes non-retryable
  `sdk_protocol` ("step bodies cannot await durable operations").
- **PF-W2 wait.** Completions come back in journal order, identically on every replay.
- **PF-W3 op ids** are Python `int`s (u64; the cancellation op is `2**64 - 1`).
- **PF-W4 lone surrogates.** A string containing a lone surrogate (workflow, step, signal,
  queue names, JSON payloads) is rejected before any host call with non-retryable
  `invalid_string`.
- **PF-W5 JSON payloads.** Values travel as `application/json`, compact separators,
  `ensure_ascii=False`. NaN and ±inf encode as `null`. Empty payload data decodes as
  `None`. A non-JSON content type decodes as raw `bytes`.
- **PF-W6 retries and fuel portability.** `retry=None` means no host retries. Fuel counts
  interpreter instructions, so a Python step burns far more fuel than the same Rust step:
  size `fuel=` per language (a hello activation uses ~0.02% of the default
  `activation_fuel`).
- **PF-W7 child outcomes.** See [Children](#children).
- **PF-W8 eviction is a trap; no module-level host calls.** The host ends an activation by
  trapping inside an import, so `finally` blocks and finalizers may not run then. Imports
  trap at pre-init, so module-level code must not call `ctx` or effects. Import everything
  at module top level: a module imported for the first time inside `run` traps under
  `--stub-wasi`.
- **PF-W9 random and uuid streams.** `uuid7()` has its own stream, separate from
  `random_u64()`. Values for the same seed and run id are byte-identical across the Rust,
  Python and TS SDKs (C-VAL-01).

## Children

`await ctx.start_child("wf", input)` starts a child with a deterministic run id derived
from (parent run, op), so a re-delivery never creates a second child. Outcome mapping
(PF-W7): Completed → its output; Failed → the child's failure re-raised; Cancelled →
`Cancelled`; TimedOut → `Timeout`; Terminated → `NonRetryable` with kind
`child_terminated`. A child that continues-as-new is followed to the last run of its chain.

- `detached=True`: the op resolves to `ChildRun(run_id)` as soon as the start is flushed.
  The child has no parent link and outlives the parent.
- `ctx.cancel(child_op)` completes the op with `Cancelled` and gracefully cancels the child
  (reason `parent_cancel_op`).
- When the parent reaches any terminal status, every attached child still running receives
  a graceful cancel (`parent_closed`).
- **The cancel cascade beats shielding.** A shielded await on an in-flight child still
  raises `Cancelled` when the parent is cancelled, because the child run itself is
  cancelled. A child started inside shielded cleanup runs until the parent closes, then
  receives the parent-close cancel (D-P9-26, C-CHILD-11).

Full semantics: [docs/children.md](../../docs/children.md).

## Saga (WF-10)

`saga.add(name, fn)` registers a compensation (any callable returning a value or an
awaitable, called with no arguments). `await saga.compensate()` runs them sequentially,
last registered first (LIFO), inside `ctx.shielded()`, so a cancelled workflow still
compensates. A failing compensation does not stop the others. `await saga.fail_with(exc)`
compensates and re-raises `exc` with
`{"compensated": [...], "compensation_failures": [...]}` in its details, the same shape as
the Rust and TS SDKs.

## Resources

Measured in Phase 9 on WSL2 (advisory; full tables in
[docs/sdk-resources.md](../../docs/sdk-resources.md)): component 22 MB, 38 core instances per
instance, 1 memory and 2 tables, a 15.94 MiB memory floor, warm instantiation p50 0.46 ms,
start to first step p50 3.2 ms, idle RSS per parked instance 10.9 MB (PSS 1.9 MB).
Run Python modules with `[engine] class = "python"` or `"mixed"`; admission waits when the
pool is full instead of failing (ENG-2).

## Reproducibility

Replay never depends on rebuilding: the deployed artefact is the module identity.
Rebuilding the same sources with the released componentize-py 0.25.1 gives a different
module sha (snapshot layout and the str hash secret change per build). The deterministic
fork (`sdks/python/toolchain/`) cuts the difference to 73–76 bytes but does not close it.
SC2 for Python is therefore an accepted residual (D-P9-3, human_needed until an upstream
fix lands). C-REBUILD-01 shows that journals recorded on one build replay on another.
Details: [docs/sdk-builds.md](../../docs/sdk-builds.md).

## Publishing

`uv build` produces `ostro-0.1.0-py3-none-any.whl` and an sdist as a dry run. Publishing
to PyPI is a human step (D-P9-19). Until then, `build-guest.sh` stages this directory into
every build, so no install is needed.

## Tests

```bash
cd sdks/python && uv run --no-project --python 3.12 --with pytest==8.4.2 python -m pytest tests -q
cargo test -p ostro-conformance --features polyglot --locked --test polyglot_python -- --test-threads=2
```
