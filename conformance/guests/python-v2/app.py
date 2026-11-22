"""Python conformance workflows, v2 (plan 09-14, SDK-05 / WF-07).

Every workflow mirrors the Rust reference (`guests/conf-rust/src/lib.rs`, contract table
there and in the 09-11 / 09-13 SUMMARYs): same base names, inputs and outputs, exported as
``py_<base>`` (D-P9-23). Bodies are idiomatic Python on the SDK's asyncio loop:
``asyncio.gather`` joins, ``asyncio.wait(..., return_when=FIRST_COMPLETED)`` races with an
explicit ``ctx.cancel`` of the loser, ``async with ctx.shielded()`` cleanup, ``ctx.saga()``,
durable ``asyncio.sleep``, and the native ``time`` / ``random`` / ``os.urandom`` APIs that
the determinism surface routes to the host (WF-07).

`conformance/guests/python-v2/app.py` is a copy of this file in which only
``conf_patch`` and ``conf_ndet`` differ (componentize-py builds one staged guest dir with
one ``app.py`` entry module, so v2 cannot import from this directory). The engine-leg test
(`crates/ostro-conformance/tests/polyglot_python.rs`) checks that the two files are
identical outside the marked v1/v2 bodies section.

Ops are allocated when the effect call is made (as in Rust), so joins and races create
their ops in argument order: ``ctx.sleep``/``ctx.wait_signal`` are used where the
journal order of several ops matters, ``asyncio.sleep`` where a single durable sleep is
awaited directly.
"""

import asyncio
import json
import os
import random
import time
import uuid

import ostro

HOUR = 3600


def unexpected(what: str, detail: object) -> ostro.NonRetryable:
    return ostro.NonRetryable("unexpected", f"{what}: {detail!r}")


async def named(name: str, value: str) -> str:
    """A step `name` returning `value` (default options), like Rust's ``named``."""
    return await ostro.step(name=name)(lambda: value)()


# --- steps -----------------------------------------------------------------------


@ostro.step
def a() -> str:
    return "a"


@ostro.step
def b() -> str:
    return "b"


@ostro.step
def c() -> str:
    return "c"


@ostro.workflow(name="py_conf_steps")
async def conf_steps(ctx: ostro.Ctx, input: object) -> dict:
    return {"a": await a(), "b": await b(), "c": await c()}


@ostro.step
def boom() -> str:
    raise ostro.NonRetryable("boom", "always fails")


@ostro.workflow(name="py_conf_step_fail")
async def conf_step_fail(ctx: ostro.Ctx, input: object) -> dict:
    try:
        v = await boom()
    except ostro.OstroError as e:
        return {"caught": e.kind}
    raise unexpected("boom succeeded", v)


@ostro.step
def draw(ctx: ostro.Ctx) -> str:
    # Inside a step body the ctx stream is the step-local stream (D7).
    return str(ctx.random_u64())


@ostro.workflow(name="py_conf_step_rng")
async def conf_step_rng(ctx: ostro.Ctx, input: object) -> dict:
    return {"value": await draw(ctx)}


@ostro.step(name="s")
async def step_s() -> str:
    # The body yields; the sibling task is ready but must not run while the step is open
    # (step exclusivity, D-P9-16), so its commands land after the step in the journal.
    await asyncio.sleep(0)
    return "s"


async def sibling(ctx: ostro.Ctx) -> str:
    r = str(ctx.random_u64())
    await asyncio.sleep(1)
    return r


@ostro.workflow(name="py_conf_step_sibling")
async def conf_step_sibling(ctx: ostro.Ctx, input: object) -> dict:
    step, sibling_rand = await asyncio.gather(step_s(), sibling(ctx))
    return {"step": step, "sibling_rand": sibling_rand}


def observe_native() -> dict:
    """Native time, randomness and dict iteration (WF-07) through the determinism surface."""
    m = {f"k{i}": i for i in range(16)}
    return {
        "time_ns": time.time_ns(),
        "time": time.time(),
        "map_order": list(m),
        "random": random.random(),
        "uuid4": str(uuid.uuid4()),
    }


@ostro.step
def observe() -> dict:
    return observe_native()


@ostro.workflow(name="py_conf_native")
async def conf_native(ctx: ostro.Ctx, input: object) -> dict:
    outside = observe_native()
    inside = await observe()
    return {"outside": outside, "inside": inside}


# --- retries ---------------------------------------------------------------------


@ostro.step(retry=ostro.Retry(max_attempts=3, initial=1, max=60, backoff=2.0))
def flaky(ctx: ostro.Ctx, t0: int, succeed_after_ms: int) -> int:
    elapsed = max(0, (ctx.now_nanos() - t0) // 1_000_000)
    if elapsed < succeed_after_ms:
        raise ostro.Retryable("transient", f"{elapsed} ms < {succeed_after_ms} ms")
    return elapsed


@ostro.workflow(name="py_conf_retry")
async def conf_retry(ctx: ostro.Ctx, input: dict) -> dict:
    # Run start in logical time; each attempt observes max(last ts, retry_at) (D22).
    t0 = ctx.now_nanos()
    return {"elapsed_ms": await flaky(ctx, t0, input["succeed_after_ms"])}


@ostro.step(retry=ostro.Retry(max_attempts=2, initial=1, max=60, backoff=2.0))
def always() -> str:
    raise ostro.Retryable("transient", "always fails")


@ostro.workflow(name="py_conf_retry_exhaust")
async def conf_retry_exhaust(ctx: ostro.Ctx, input: object) -> dict:
    await always()
    raise unexpected("always", "succeeded")


# --- timers and signals ----------------------------------------------------------


@ostro.workflow(name="py_conf_timer")
async def conf_timer(ctx: ostro.Ctx, input: object) -> dict:
    await asyncio.sleep(HOUR)
    return {"slept": True}


async def race(timer: ostro.Op, signal: ostro.Op) -> tuple[asyncio.Future, asyncio.Future, bool]:
    """``asyncio.wait`` on both ops; returns the two tasks and whether the signal won."""
    t_timer = asyncio.ensure_future(timer)
    t_signal = asyncio.ensure_future(signal)
    done, _ = await asyncio.wait([t_timer, t_signal], return_when=asyncio.FIRST_COMPLETED)
    return t_timer, t_signal, t_signal in done


@ostro.workflow(name="py_conf_timer_vs_signal")
async def conf_timer_vs_signal(ctx: ostro.Ctx, input: object) -> dict:
    t_timer, t_signal, signal_won = await race(ctx.sleep(HOUR), ctx.wait_signal("go"))
    if signal_won:
        return {"winner": "signal", "payload": t_signal.result()}
    t_timer.result()
    return {"winner": "timer"}


@ostro.workflow(name="py_conf_signals")
async def conf_signals(ctx: ostro.Ctx, input: object) -> dict:
    first = await ctx.wait_signal("go")
    second = await ctx.wait_signal("go")
    return {"got": [first, second]}


@ostro.workflow(name="py_conf_signal_timeout")
async def conf_signal_timeout(ctx: ostro.Ctx, input: object) -> dict:
    try:
        v = await ctx.wait_signal("never", timeout=10)
    except ostro.Timeout:
        return {"timeout": True}
    return {"timeout": False, "payload": v}


@ostro.workflow(name="py_conf_gather")
async def conf_gather(ctx: ostro.Ctx, input: object) -> dict:
    _, _, go = await asyncio.gather(ctx.sleep(1), ctx.sleep(2), ctx.wait_signal("go"))
    return {"signal": go}


@ostro.workflow(name="py_conf_race_cancel")
async def conf_race_cancel(ctx: ostro.Ctx, input: object) -> dict:
    timer = ctx.sleep(HOUR)
    go = ctx.wait_signal("go")
    t_timer, t_signal, signal_won = await race(timer, go)
    winner, loser, loser_task = ("signal", timer, t_timer) if signal_won else ("timer", go, t_signal)
    (t_signal if signal_won else t_timer).result()
    ctx.cancel(loser)
    try:
        other = await loser_task
    except ostro.Cancelled:
        return {"winner": winner, "loser_cancelled": True}
    raise unexpected("loser wait", other)


# --- cancellation ----------------------------------------------------------------


@ostro.workflow(name="py_conf_cancel_wait")
async def conf_cancel_wait(ctx: ostro.Ctx, input: object) -> dict:
    await asyncio.sleep(HOUR)
    return {"slept": True}


@ostro.step
def cleanup() -> str:
    return "cleaned"


@ostro.workflow(name="py_conf_cancel_shielded")
async def conf_cancel_shielded(ctx: ostro.Ctx, input: object) -> dict:
    try:
        await asyncio.sleep(HOUR)
    except ostro.Cancelled:
        async with ctx.shielded():
            await cleanup()
        raise
    return {"slept": True}


@ostro.workflow(name="py_conf_cancel_timer")
async def conf_cancel_timer(ctx: ostro.Ctx, input: object) -> dict:
    timer = ctx.sleep(HOUR)
    ctx.cancel(timer)
    try:
        await timer
    except ostro.Cancelled:
        return {"cancelled_op": True}
    raise unexpected("cancelled timer wait", "completed")


# --- v1/v2 bodies (the only section python-v2/app.py changes) ----------------------


@ostro.workflow(name="py_conf_patch")
async def conf_patch(ctx: ostro.Ctx, input: object) -> list:
    # v2: the new branch behind patched("x"); a v1 journal (no marker) takes the old one.
    first = await named("a2", "a2") if ctx.patched("x") else await a()
    await asyncio.sleep(1)
    return [first, await b()]


@ostro.workflow(name="py_conf_ndet")
async def conf_ndet(ctx: ostro.Ctx, input: object) -> list:
    # v2: the steps are swapped without a patch marker (non-determinism on a v1 journal).
    first = await b()
    await asyncio.sleep(1)
    return [first, await a()]


# --- end of v1/v2 bodies -------------------------------------------------------------

# --- randomness and values -------------------------------------------------------


@ostro.workflow(name="py_conf_rand")
async def conf_rand(ctx: ostro.Ctx, input: object) -> dict:
    u = [str(ctx.random_u64()) for _ in range(3)]
    return {"u64": u, "uuid": ctx.uuid7()}


@ostro.workflow(name="py_conf_rand_native")
async def conf_rand_native(ctx: ostro.Ctx, input: object) -> dict:
    # Native APIs (WF-07): os.urandom -> random-bytes, random.random -> random_f64.
    raw = os.urandom(8)
    return {"bytes": raw.hex(), "f64": random.random()}


@ostro.workflow(name="py_conf_values")
async def conf_values(ctx: ostro.Ctx, input: object) -> dict:
    # The order is part of the contract (PF-W9: random and uuid streams are separate).
    random_u64 = str(ctx.random_u64())
    random_f64 = ctx.random_f64()
    uuid_v7 = ctx.uuid7()
    idempotency_key = ctx.idempotency_key("x")
    return {
        "random_u64": random_u64,
        "random_f64": random_f64,
        "uuid_v7": uuid_v7,
        "idempotency_key": idempotency_key,
    }


# --- continue-as-new, tokens, status ---------------------------------------------


@ostro.workflow(name="py_conf_can")
async def conf_can(ctx: ostro.Ctx, input: dict) -> dict:
    n, generation = input["n"], input.get("generation", 0)
    if n == 0:
        return {"generation": generation}
    ctx.continue_as_new({"n": n - 1, "generation": generation + 1})


@ostro.workflow(name="py_conf_token")
async def conf_token(ctx: ostro.Ctx, input: object) -> dict:
    op, token = ctx.create_await_token()
    if not token:
        raise unexpected("token", "empty")
    return {"token_payload": await op}


@ostro.workflow(name="py_conf_status")
async def conf_status(ctx: ostro.Ctx, input: object) -> dict:
    ctx.set_status({"stage": "one"})
    ctx.set_status({"stage": "two", "n": 2})
    return {"done": True}


# --- sagas -----------------------------------------------------------------------


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
def cancel_a() -> str:
    return "a"


@ostro.step
def cancel_b() -> str:
    return "b"


async def book_two(ctx: ostro.Ctx) -> ostro.Saga:
    saga = ctx.saga()
    await book_a()
    saga.add("cancel_a", cancel_a)
    await book_b()
    saga.add("cancel_b", cancel_b)
    return saga


@ostro.workflow(name="py_conf_saga_three")
async def conf_saga_three(ctx: ostro.Ctx, input: object) -> dict:
    saga = await book_two(ctx)
    try:
        v = await book_c()
    except ostro.OstroError as e:
        await saga.fail_with(e)
    raise unexpected("book_c succeeded", v)


@ostro.workflow(name="py_conf_saga_cancel")
async def conf_saga_cancel(ctx: ostro.Ctx, input: object) -> dict:
    saga = await book_two(ctx)
    try:
        await asyncio.sleep(HOUR)
    except ostro.OstroError as e:
        await saga.fail_with(e)
    return {"slept": True}


# --- server leg (plan 09-13 workflows) -----------------------------------------------
#
# Child workflow names arrive in the input as full prefixed names, so a parent in one
# language can start a child in another. Parent input:
# ``{"child": "<name>", "input"?: any, "run_timeout_ms"?: N, "delay_ms"?: N}``.


def start(ctx: ostro.Ctx, p: dict, input: object = None, *, detached: bool = False) -> ostro.Op:
    timeout_ms = p.get("run_timeout_ms")
    return ctx.start_child(
        p["child"],
        p.get("input") if input is None else input,
        detached=detached,
        run_timeout=None if timeout_ms is None else timeout_ms / 1000,
    )


def delay(p: dict) -> float:
    return p.get("delay_ms", 1000) / 1000


async def child_or_caught(op: ostro.Op) -> dict:
    try:
        return {"child": await op}
    except ostro.OstroError as e:
        return {"caught": e.kind}


@ostro.workflow(name="py_conf_parent_attached")
async def conf_parent_attached(ctx: ostro.Ctx, input: dict) -> dict:
    return await child_or_caught(start(ctx, input))


@ostro.workflow(name="py_conf_parent_fail_child")
async def conf_parent_fail_child(ctx: ostro.Ctx, input: dict) -> dict:
    return await child_or_caught(start(ctx, input))


@ostro.workflow(name="py_conf_parent_detached")
async def conf_parent_detached(ctx: ostro.Ctx, input: dict) -> dict:
    run = await start(ctx, input, detached=True)
    return {"run_id": run.run_id}


@ostro.workflow(name="py_conf_parent_cancel_child")
async def conf_parent_cancel_child(ctx: ostro.Ctx, input: dict) -> dict:
    op = start(ctx, input)
    await asyncio.sleep(delay(input))
    ctx.cancel(op)
    return await child_or_caught(op)


@ostro.workflow(name="py_conf_parent_close")
async def conf_parent_close(ctx: ostro.Ctx, input: dict) -> dict:
    start(ctx, input)
    await asyncio.sleep(delay(input))
    return {"started": True}


@ostro.workflow(name="py_conf_parent_chain")
async def conf_parent_chain(ctx: ostro.Ctx, input: dict) -> dict:
    nxt = input.get("next") or []
    if not nxt:
        return {"leaf": True}
    out = await ctx.start_child(nxt[0], {"next": nxt[1:]})
    return {"child": out}


@ostro.workflow(name="py_conf_parent_shielded_child")
async def conf_parent_shielded_child(ctx: ostro.Ctx, input: dict) -> dict:
    op = start(ctx, input)
    try:
        async with ctx.shielded():
            v = await op
    except ostro.Cancelled:
        # The cancel cascade reached the child although the await was shielded (W2).
        async with ctx.shielded():
            start(ctx, input, {})
            await asyncio.sleep(1)
        return {"first": "cancelled", "cleanup_child": True}
    return {"first": v}


@ostro.workflow(name="py_conf_leaf_echo")
async def conf_leaf_echo(ctx: ostro.Ctx, input: object) -> dict:
    if isinstance(input, dict) and isinstance(input.get("delay_ms"), int):
        async with ctx.shielded():
            await asyncio.sleep(input["delay_ms"] / 1000)
    return {"echo": input}


@ostro.workflow(name="py_conf_leaf_fail")
async def conf_leaf_fail(ctx: ostro.Ctx, input: object) -> dict:
    raise ostro.NonRetryable("boom", "conf_leaf_fail always fails")


@ostro.workflow(name="py_conf_leaf_sleep")
async def conf_leaf_sleep(ctx: ostro.Ctx, input: object) -> dict:
    ms = input.get("ms", 3_600_000) if isinstance(input, dict) else 3_600_000
    try:
        await asyncio.sleep(ms / 1000)
    except ostro.Cancelled:
        async with ctx.shielded():
            await cleanup()
        raise
    return {"slept": True}


@ostro.workflow(name="py_conf_leaf_can")
async def conf_leaf_can(ctx: ostro.Ctx, input: dict) -> dict:
    n, generation = input["n"], input.get("generation", 0)
    if n == 0:
        return {"done": True, "generation": generation}
    ctx.continue_as_new({"n": n - 1, "generation": generation + 1})


@ostro.workflow(name="py_conf_sig_api")
async def conf_sig_api(ctx: ostro.Ctx, input: object) -> dict:
    return {"approved": await ctx.wait_signal("approve")}


@ostro.workflow(name="py_conf_token_api")
async def conf_token_api(ctx: ostro.Ctx, input: object) -> dict:
    op, token = ctx.create_await_token()
    ctx.set_status({"token": token})
    return {"token_payload": await op}


async def pay(ctx: ostro.Ctx, url: str, key_name: str) -> dict:
    """``POST url`` with JSON body ``{}`` and idempotency key ``ctx.idempotency_key(key_name)``."""
    resp = await ctx.http.post(url).idempotency_key(ctx.idempotency_key(key_name)).json({}).send()
    try:
        body = json.loads(resp.body)
    except ValueError:
        body = None
    return {"status": resp.status, "body": body}


@ostro.workflow(name="py_conf_fx_http")
async def conf_fx_http(ctx: ostro.Ctx, input: dict) -> dict:
    return await pay(ctx, input["url"], "pay")


@ostro.workflow(name="py_conf_saga_http")
async def conf_saga_http(ctx: ostro.Ctx, input: dict) -> dict:
    url = f"{input['url']}/pay"
    saga = ctx.saga()
    await pay(ctx, url, "a")
    saga.add("refund_a", lambda: pay(ctx, url, "refund-a"))
    await pay(ctx, url, "b")
    saga.add("refund_b", lambda: pay(ctx, url, "refund-b"))
    try:
        v = await book_c()
    except ostro.OstroError as e:
        await saga.fail_with(e)
    raise unexpected("book_c succeeded", v)


ostro.export()
