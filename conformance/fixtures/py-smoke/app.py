"""SDK smoke guest (plan 09-12): the first real-engine runs of an SDK-built Python module.

Same workflow names and outputs as `conformance/fixtures/ts-smoke` (the engine test
`crates/ostro-conformance/tests/sdk_engine_smoke.rs` checks both):

- `smoke_steps`: steps a, b, then gather(c, d) -> ``["a", "b", "c", "d"]``
- `smoke_race`: race a 1 h sleep against signal "go"; the losing timer is cancelled
  explicitly -> ``{"winner": "signal", "payload": <p>, "loser_cancelled": true}``
- `smoke_sleep`: ``asyncio.sleep(3600)`` (durable) -> ``{"slept": true}``
- `smoke_child`: start child "leaf_echo" with ``{"x": 1}`` and await it -> ``{"child": <out>}``
- `smoke_saga`: book_a, book_b, failing book_c -> compensations cancel_b, cancel_a; the run
  fails ``book_c_failed`` with the saga report in its details
- `smoke_saga_cancel`: book_a, book_b, a 1 h sleep; the run is cancelled during the sleep
  and the saga compensates (cancel_b, cancel_a) before the run ends cancelled
- `smoke_rand`: ctx and native randomness, time and the idempotency key (replay-stable)
- `smoke_print`: print/stderr go to the log, no trap -> ``{"printed": true}``
- `smoke_effects`: HTTP, SQL and object builders -> the decoded responses
"""

import asyncio
import random
import sys
import time

import ostro


@ostro.step
def a() -> str:
    return "a"


@ostro.step
def b() -> str:
    return "b"


@ostro.step
def c() -> str:
    return "c"


@ostro.step
def d() -> str:
    return "d"


@ostro.workflow(name="smoke_steps")
async def smoke_steps(ctx: ostro.Ctx, input: object) -> list:
    first = await a()
    second = await b()
    third, fourth = await asyncio.gather(c(), d())
    return [first, second, third, fourth]


@ostro.workflow(name="smoke_race")
async def smoke_race(ctx: ostro.Ctx, input: object) -> dict:
    timer = ctx.sleep(3600)
    signal = ctx.wait_signal("go")
    t_timer = asyncio.ensure_future(timer)
    t_signal = asyncio.ensure_future(signal)
    done, _ = await asyncio.wait([t_timer, t_signal], return_when=asyncio.FIRST_COMPLETED)
    if t_signal not in done:
        return {"winner": "timer"}
    ctx.cancel(timer)
    loser_cancelled = False
    try:
        await t_timer
    except ostro.Cancelled:
        loser_cancelled = True
    return {"winner": "signal", "payload": t_signal.result(), "loser_cancelled": loser_cancelled}


@ostro.workflow(name="smoke_sleep")
async def smoke_sleep(ctx: ostro.Ctx, input: object) -> dict:
    await asyncio.sleep(3600)
    return {"slept": True}


@ostro.workflow(name="smoke_child")
async def smoke_child(ctx: ostro.Ctx, input: object) -> dict:
    out = await ctx.start_child("leaf_echo", {"x": 1})
    return {"child": out}


@ostro.step
def book_a() -> str:
    return "a"


@ostro.step
def book_b() -> str:
    return "b"


@ostro.step
def book_c() -> str:
    raise ostro.NonRetryable("book_c_failed", "no rooms left")


@ostro.step
def cancel_a() -> None:
    return None


@ostro.step
def cancel_b() -> None:
    return None


@ostro.workflow(name="smoke_saga")
async def smoke_saga(ctx: ostro.Ctx, input: object) -> dict:
    saga = ctx.saga()
    await book_a()
    saga.add("cancel_a", cancel_a)
    await book_b()
    saga.add("cancel_b", cancel_b)
    try:
        await book_c()
    except ostro.OstroError as e:
        await saga.fail_with(e)
    return {"unexpected": True}


@ostro.workflow(name="smoke_saga_cancel")
async def smoke_saga_cancel(ctx: ostro.Ctx, input: object) -> dict:
    saga = ctx.saga()
    await book_a()
    saga.add("cancel_a", cancel_a)
    await book_b()
    saga.add("cancel_b", cancel_b)
    try:
        await ctx.sleep(3600)
    except ostro.Cancelled as e:
        await saga.fail_with(e)
    return {"unexpected": True}


@ostro.workflow(name="smoke_rand")
async def smoke_rand(ctx: ostro.Ctx, input: object) -> dict:
    return {
        "f64": ctx.random_f64(),
        "u64": str(ctx.random_u64()),
        "native": random.random(),
        "uuid": ctx.uuid7(),
        "key": ctx.idempotency_key("x"),
        "time_ms": int(time.time() * 1000),
    }


@ostro.workflow(name="smoke_print")
async def smoke_print(ctx: ostro.Ctx, input: object) -> dict:
    print("hello from the python smoke guest")
    print("to stderr", file=sys.stderr)
    return {"printed": True}


@ostro.workflow(name="smoke_effects")
async def smoke_effects(ctx: ostro.Ctx, input: object) -> dict:
    resp = await (
        ctx.http.post("https://api.example.com/v1/charges")
        .secret_header("Authorization", "Bearer {{secret:stripe}}")
        .json({"amount": 100})
        .idempotency_key(ctx.idempotency_key("charge"))
        .timeout(10)
        .retry(ostro.Retry(max_attempts=2))
        .redact("Authorization")
        .send()
    )
    rows = await ctx.sql("db").exactly_once().query("UPDATE c SET n = n + $1 RETURNING n", [1, "t", None, 1.5])
    put = await ctx.object("files").put("k", b"body")
    return {
        "status": resp.status,
        "body": resp.body.decode(),
        "rows_affected": rows.rows_affected,
        "columns": rows.columns,
        "e_tag": put.e_tag,
    }


ostro.export()
