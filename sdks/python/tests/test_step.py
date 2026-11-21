"""Steps: replay/execute, retry-now, serialisation and exclusivity (D-P9-16, plan check B1)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from ostro import NonRetryable, ProtocolError, Retry, Retryable, errors, step
from ostro._host import ErrorInfo, RetryPolicy, StepOptions
from ostro.loop import STEP_DURABLE_MESSAGE

from conftest import FakeHost, methods, run_in_loop

JSON = "application/json"


def completes(host: FakeHost) -> list[Any]:
    return [args[1] for m, args in host.calls if m == "step_complete"]


def protocol_failure(message: str = STEP_DURABLE_MESSAGE):
    return ("err", ("non-retryable", ErrorInfo("sdk_protocol", message)))


def without_complete(host: FakeHost) -> list[tuple[str, tuple]]:
    """Calls with `step_complete` removed and step handles normalised away."""
    out = []
    for m, args in host.calls:
        if m == "step_complete":
            continue
        out.append((m, args))
    return out


# --- replay / execute ----------------------------------------------------------------


def test_replayed_step_returns_the_journaled_value_without_running(fake_host: FakeHost) -> None:
    runs: list[int] = []

    @step
    def charge(x: int) -> int:
        runs.append(x)
        return x * 2

    fake_host.script_begin_step(("replayed", ("ok", (JSON, b"42"))))

    async def wf(ctx):
        return await charge(5)

    assert run_in_loop(fake_host, wf) == 42
    assert runs == []
    assert methods(fake_host) == ["begin_step"]
    assert fake_host.calls[0] == ("begin_step", ("charge", StepOptions()))


def test_replayed_failure_raises(fake_host: FakeHost) -> None:
    @step
    def s() -> None:
        raise AssertionError("not run")

    fake_host.script_begin_step(("replayed", ("err", ("non-retryable", ErrorInfo("declined", "card")))))

    async def wf(ctx):
        await s()

    with pytest.raises(NonRetryable) as e:
        run_in_loop(fake_host, wf)
    assert e.value.kind == "declined"


@pytest.mark.parametrize("is_async", [False, True])
def test_executed_step_journals_json_and_returns_the_decoded_value(fake_host: FakeHost, is_async: bool) -> None:
    if is_async:

        @step(name="pair")
        async def pair(a: int, b: int) -> tuple[int, int]:
            await asyncio.sleep(0)
            return (a, b)

    else:

        @step(name="pair")
        def pair(a: int, b: int) -> tuple[int, int]:
            return (a, b)

    async def wf(ctx):
        return await pair(1, b=2)

    # Live returns what replay would: the value decoded from the journaled JSON.
    assert run_in_loop(fake_host, wf) == [1, 2]
    assert methods(fake_host) == ["begin_step", "step_complete"]
    assert completes(fake_host) == [("ok", (JSON, b"[1,2]"))]


def test_non_retryable_body_error(fake_host: FakeHost) -> None:
    @step
    def s() -> None:
        raise NonRetryable("declined", "card declined", {"code": 51})

    async def wf(ctx):
        await s()

    with pytest.raises(NonRetryable) as e:
        run_in_loop(fake_host, wf)
    assert (e.value.kind, e.value.details) == ("declined", {"code": 51})
    assert completes(fake_host) == [
        ("err", ("non-retryable", ErrorInfo("declined", "card declined", (JSON, b'{"code":51}'))))
    ]


def test_other_exceptions_are_retryable_python_exceptions(fake_host: FakeHost) -> None:
    @step
    def s() -> None:
        raise ValueError("bad thing")

    async def wf(ctx):
        await s()

    with pytest.raises(Retryable) as e:
        run_in_loop(fake_host, wf)
    assert e.value.kind == "python_exception"
    assert completes(fake_host) == [("err", ("retryable", ErrorInfo("python_exception", "ValueError: bad thing")))]


def test_retry_now_reruns_the_body_on_the_same_handle(fake_host: FakeHost) -> None:
    attempts: list[int] = []

    @step(retry=Retry(max_attempts=5))
    def flaky() -> str:
        attempts.append(len(attempts))
        if len(attempts) < 3:
            raise Retryable("flaky", "again")
        return "ok"

    fake_host.script_step_ack("retry-now", "retry-now", "recorded")

    async def wf(ctx):
        return await flaky()

    assert run_in_loop(fake_host, wf) == "ok"
    assert attempts == [0, 1, 2]
    handles = {args[0] for m, args in fake_host.calls if m == "step_complete"}
    assert len(handles) == 1
    assert methods(fake_host) == ["begin_step"] + ["step_complete"] * 3


def test_step_options() -> None:
    @step(retry=Retry(max_attempts=5, non_retryable=["fatal"]), timeout=2.5, fuel=1000)
    def a() -> None: ...

    @step
    def b() -> None: ...

    assert a.__ostro_step_options__ == StepOptions(
        retry=RetryPolicy(5, 1000, 60000, 2.0, ["fatal"]), timeout_ms=2500, fuel=1000
    )
    assert b.__ostro_step_options__ == StepOptions()
    assert Retry().to_policy() == RetryPolicy(3, 1000, 60000, 2.0, [])
    assert Retry(initial=0.5, max=10, backoff=1.5).to_policy() == RetryPolicy(3, 500, 10000, 1.5, [])


def test_step_name_with_lone_surrogate(fake_host: FakeHost) -> None:
    @step(name="bad\ud800")
    def s() -> None: ...

    async def wf(ctx):
        await s()

    with pytest.raises(errors.InvalidString):
        run_in_loop(fake_host, wf)
    assert fake_host.calls == []


def test_step_returning_a_lone_surrogate_fails_invalid_string(fake_host: FakeHost) -> None:
    @step
    def s() -> str:
        return "x\udfff"

    async def wf(ctx):
        await s()

    with pytest.raises(errors.InvalidString):
        run_in_loop(fake_host, wf)
    assert completes(fake_host)[0][1][1].kind == "invalid_string"


# --- serialisation --------------------------------------------------------------------


def test_gathered_steps_are_serialised_in_call_order(fake_host: FakeHost) -> None:
    @step
    async def a() -> str:
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return "a"

    @step
    async def b() -> str:
        await asyncio.sleep(0)
        return "b"

    async def wf(ctx):
        return await asyncio.gather(a(), b())

    assert run_in_loop(fake_host, wf) == ["a", "b"]
    assert methods(fake_host) == ["begin_step", "step_complete", "begin_step", "step_complete"]
    assert [args[0] for m, args in fake_host.calls if m == "begin_step"] == ["a", "b"]


# --- exclusivity (B1) -----------------------------------------------------------------


def test_siblings_never_run_inside_an_open_step(fake_host: FakeHost) -> None:
    @step
    async def draw(ctx) -> int:
        await asyncio.sleep(0)
        v = ctx.random_u64()  # step-local stream
        await asyncio.sleep(0)
        return v % 1000

    async def sibling(ctx):
        ctx.random_u64()
        ctx.sleep(1)

    async def wf(ctx):
        t = asyncio.ensure_future(sibling(ctx))
        v = await draw(ctx)
        await t
        return v

    run_in_loop(fake_host, wf)
    assert methods(fake_host) == ["begin_step", "random_u64", "step_complete", "random_u64", "sleep"]


def _ordering_workflow(trace: list[str]):
    @step
    async def s() -> str:
        trace.append("body")
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return "s"

    async def sib(ctx, name: str):
        trace.append(name)
        ctx.random_u64()

    async def wf(ctx):
        ta = asyncio.ensure_future(sib(ctx, "A"))
        tb = asyncio.ensure_future(sib(ctx, "B"))
        v = await s()
        trace.append("caller")
        await asyncio.gather(ta, tb)
        return v

    return wf


def test_deferred_siblings_run_in_order_before_the_caller_live_and_replay() -> None:
    live, live_trace = FakeHost(), []
    assert run_in_loop(live, _ordering_workflow(live_trace)) == "s"
    assert live_trace == ["body", "A", "B", "caller"]
    outcome = completes(live)[0]

    replay, replay_trace = FakeHost(), []
    replay.script_begin_step(("replayed", outcome))
    assert run_in_loop(replay, _ordering_workflow(replay_trace)) == "s"
    assert replay_trace == ["A", "B", "caller"]
    assert without_complete(live) == without_complete(replay)


def _multi_hop_workflow(trace: list[Any]):
    @step
    async def s() -> int:
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return 7

    async def sibling(ctx):
        for hop in range(3):
            await asyncio.sleep(0)
            trace.append(("sib", hop, ctx.random_u64()))

    async def wf(ctx):
        t = asyncio.ensure_future(sibling(ctx))
        trace.append(("caller-before", ctx.random_u64()))
        v = await s()
        trace.append(("caller-after", v, ctx.random_u64()))
        await t
        return v

    return wf


def test_multi_hop_sibling_interleaves_identically_live_and_replay() -> None:
    live, live_trace = FakeHost(), []
    run_in_loop(live, _multi_hop_workflow(live_trace))
    replay, replay_trace = FakeHost(), []
    replay.script_begin_step(("replayed", completes(live)[0]))
    run_in_loop(replay, _multi_hop_workflow(replay_trace))
    assert live_trace == replay_trace
    # The sibling's first hop runs before the caller resumes, the rest interleave after.
    assert [t[0] for t in live_trace] == ["caller-before", "sib", "caller-after", "sib", "sib"]
    assert without_complete(live) == without_complete(replay)


def test_loop_timers_inside_a_step_are_protocol_errors(fake_host: FakeHost) -> None:
    seen: list[str] = []

    @step
    async def timers() -> str:
        loop = asyncio.get_running_loop()
        with pytest.raises(ProtocolError):
            loop.call_later(1, print)
        seen.append("call_later")

        async def inner():
            return 1

        coro = inner()
        with pytest.raises(ProtocolError):
            await asyncio.wait_for(coro, 5)
        coro.close()
        seen.append("wait_for")
        await asyncio.sleep(0)  # a plain yield is fine
        await asyncio.sleep(1)  # raises: completes the step sdk_protocol
        return "unreachable"

    async def wf(ctx):
        await timers()

    with pytest.raises(ProtocolError):
        run_in_loop(fake_host, wf)
    assert seen == ["call_later", "wait_for"]
    (outcome,) = completes(fake_host)
    assert outcome[0] == "err" and outcome[1][0] == "non-retryable" and outcome[1][1].kind == "sdk_protocol"
    assert "sleep_until" not in methods(fake_host)


def test_step_body_awaiting_ctx_sleep_fails_the_step(fake_host: FakeHost) -> None:
    @step
    async def s(ctx) -> None:
        await ctx.sleep(1)

    async def wf(ctx):
        await s(ctx)

    with pytest.raises(ProtocolError, match=STEP_DURABLE_MESSAGE):
        run_in_loop(fake_host, wf)
    assert completes(fake_host) == [protocol_failure()]
    assert methods(fake_host) == ["begin_step", "step_complete"]


def test_step_body_awaiting_an_outside_op_is_aborted_at_quiescence(fake_host: FakeHost) -> None:
    @step
    async def s(op) -> None:
        await op

    async def wf(ctx):
        op = ctx.wait_signal("go")
        await s(op)

    with pytest.raises(ProtocolError, match=STEP_DURABLE_MESSAGE):
        run_in_loop(fake_host, wf)
    assert completes(fake_host) == [protocol_failure()]
    assert "wait" not in methods(fake_host)


def test_nested_steps_are_rejected(fake_host: FakeHost) -> None:
    @step
    async def inner() -> int:
        return 1

    @step
    async def outer() -> int:
        return await inner()

    async def wf(ctx):
        await outer()

    with pytest.raises(ProtocolError):
        run_in_loop(fake_host, wf)
    assert methods(fake_host) == ["begin_step", "step_complete"]


def test_time_inside_and_outside_steps_is_logical(fake_host: FakeHost) -> None:
    import time

    @step
    def stamp() -> float:
        return time.time()

    async def wf(ctx):
        return time.time(), await stamp()

    outside, inside = run_in_loop(fake_host, wf)
    assert outside == inside == fake_host.now / 1e9
