"""OstroLoop semantics on the fake host (SDK-02, D-P9-2)."""

from __future__ import annotations

import asyncio

import pytest

from ostro import errors
from ostro.loop import OstroLoop, _shielded
from ostro.ops import Op

from conftest import CANCELLATION_OP, FakeHost

C = CANCELLATION_OP


def timer_ok(op: int):
    return (op, ("ok", ("timer", None)))


def signal_ok(op: int, data: bytes = b'"v"'):
    return (op, ("ok", ("signal", ("application/json", data))))


def waits(host: FakeHost) -> list[list[int]]:
    return [args[0] for m, args in host.calls if m == "wait"]


def methods(host: FakeHost) -> list[str]:
    return [m for m, _ in host.calls]


def test_gather_resolves_in_gather_order_with_sorted_wait_lists(fake_host: FakeHost) -> None:
    loop = OstroLoop(fake_host)
    a = Op(loop, fake_host.wait_signal("a", None), "signal")
    b = Op(loop, fake_host.wait_signal("b", None), "signal")
    fake_host.script_wait([signal_ok(b.id, b'"B"')], [signal_ok(a.id, b'"A"')])

    async def main():
        return await asyncio.gather(a, b)

    assert loop.run_until_complete(main()) == ["A", "B"]
    assert waits(fake_host) == [[a.id, b.id, C], [a.id, C]]
    assert all(args[1] is True for m, args in fake_host.calls if m == "wait")


def test_race_first_completed_leaves_the_loser_pending(fake_host: FakeHost) -> None:
    loop = OstroLoop(fake_host)
    slow = Op(loop, fake_host.sleep(60_000), "timer")
    fast = Op(loop, fake_host.wait_signal("go", None), "signal")
    fake_host.script_wait([signal_ok(fast.id, b"7")])

    async def main():
        t_slow = asyncio.ensure_future(slow)
        t_fast = asyncio.ensure_future(fast)
        done, pending = await asyncio.wait([t_slow, t_fast], return_when=asyncio.FIRST_COMPLETED)
        assert done == {t_fast} and pending == {t_slow}
        return t_fast.result()

    assert loop.run_until_complete(main()) == 7
    assert waits(fake_host) == [[slow.id, fast.id, C]]
    assert "cancel_op" not in methods(fake_host)
    assert not slow.done()


def test_several_completions_resolve_in_returned_order(fake_host: FakeHost) -> None:
    loop = OstroLoop(fake_host)
    ops = [Op(loop, fake_host.wait_signal(n, None), "signal") for n in ("x", "y", "z")]
    x, y, z = ops
    fake_host.script_wait([signal_ok(z.id, b'"z"'), signal_ok(x.id, b'"x"'), signal_ok(y.id, b'"y"')])
    seen: list[str] = []

    async def watch(op: Op) -> None:
        seen.append(await op)

    async def main():
        await asyncio.gather(*(watch(o) for o in ops))

    loop.run_until_complete(main())
    assert seen == ["z", "x", "y"]
    assert waits(fake_host) == [[x.id, y.id, z.id, C]]


def test_reawaiting_a_done_op_issues_no_host_call(fake_host: FakeHost) -> None:
    loop = OstroLoop(fake_host)
    op = Op(loop, fake_host.wait_signal("s", None), "signal")
    fake_host.script_wait([signal_ok(op.id, b"1")])

    async def main():
        first = await op
        n = len(fake_host.calls)
        second = await op
        assert len(fake_host.calls) == n
        return first, second

    assert loop.run_until_complete(main()) == (1, 1)
    assert len(waits(fake_host)) == 1


def test_bare_future_deadlocks_without_an_empty_wait(fake_host: FakeHost) -> None:
    loop = OstroLoop(fake_host)

    async def main():
        await loop.create_future()

    with pytest.raises(errors.Deadlock) as e:
        loop.run_until_complete(main())
    assert e.value.kind == "sdk_protocol"
    assert isinstance(e.value, errors.ProtocolError)
    assert "wait" not in methods(fake_host)


def test_asyncio_sleep_is_one_durable_timer(fake_host: FakeHost) -> None:
    loop = OstroLoop(fake_host)
    op = fake_host.next_op
    fake_host.script_wait([timer_ok(op)])
    loop.run_until_complete(asyncio.sleep(3600))
    assert ("sleep_until", (fake_host.now + 3600 * 10**9,)) in fake_host.calls
    assert methods(fake_host).count("sleep_until") == 1
    assert waits(fake_host) == [[op, C]]
    assert "cancel_op" not in methods(fake_host)


def test_asyncio_sleep_zero_is_not_journaled(fake_host: FakeHost) -> None:
    loop = OstroLoop(fake_host)

    async def main():
        await asyncio.sleep(0)
        return "ok"

    assert loop.run_until_complete(main()) == "ok"
    assert "sleep_until" not in methods(fake_host)
    assert "wait" not in methods(fake_host)


def test_wait_for_cancels_its_timer_when_the_op_wins(fake_host: FakeHost) -> None:
    loop = OstroLoop(fake_host)
    sig = Op(loop, fake_host.wait_signal("go", None), "signal")
    timer = fake_host.next_op
    fake_host.script_wait([signal_ok(sig.id, b'"s"')])

    async def main():
        return await asyncio.wait_for(sig, 5)

    assert loop.run_until_complete(main()) == "s"
    assert ("sleep_until", (fake_host.now + 5 * 10**9,)) in fake_host.calls
    assert methods(fake_host).count("sleep_until") == 1
    assert waits(fake_host) == [[sig.id, timer, C]]
    assert fake_host.cancelled_ops == [timer]


def test_wait_for_times_out_on_the_durable_timer(fake_host: FakeHost) -> None:
    loop = OstroLoop(fake_host)
    sig = Op(loop, fake_host.wait_signal("go", None), "signal")
    timer = fake_host.next_op
    fake_host.script_wait([timer_ok(timer)])

    async def main():
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(sig, 5)
        return "timed out"

    assert loop.run_until_complete(main()) == "timed out"
    assert timer not in fake_host.cancelled_ops


def test_loop_time_is_logical(fake_host: FakeHost) -> None:
    loop = OstroLoop(fake_host)

    async def main():
        return asyncio.get_running_loop().time()

    assert loop.run_until_complete(main()) == fake_host.now / 1e9


def test_tasks_left_after_the_root_never_run(fake_host: FakeHost) -> None:
    loop = OstroLoop(fake_host)
    ran: list[str] = []

    async def leftover():
        ran.append("leftover")
        fake_host.sleep(1)

    tasks: list[asyncio.Task] = []

    async def main():
        tasks.append(loop.create_task(leftover()))
        return "root"

    assert loop.run_until_complete(main()) == "root"
    tasks[0].get_coro().close()
    assert ran == []
    assert fake_host.calls == []
    with pytest.raises(errors.ProtocolError):
        loop.time()
    assert fake_host.calls == []


def test_cancellation_fails_unshielded_ops_and_is_remembered(fake_host: FakeHost) -> None:
    loop = OstroLoop(fake_host)
    a = Op(loop, fake_host.wait_signal("a", None), "signal")
    later = Op(loop, fake_host.wait_signal("b", None), "signal")
    fake_host.script_wait([(C, ("ok", ("cancel-requested", None)))])

    async def main():
        with pytest.raises(errors.Cancelled):
            await a
        n = len(fake_host.calls)
        with pytest.raises(errors.Cancelled):
            await later
        assert len(fake_host.calls) == n
        return "cleaned up"

    assert loop.run_until_complete(main()) == "cleaned up"
    assert waits(fake_host) == [[a.id, C]]


def test_cancelled_is_not_asyncio_cancelled_error() -> None:
    assert issubclass(errors.Cancelled, Exception)
    assert not issubclass(errors.Cancelled, asyncio.CancelledError)


def test_shielded_only_waits_omit_cancellation(fake_host: FakeHost) -> None:
    loop = OstroLoop(fake_host)
    op = Op(loop, fake_host.wait_signal("a", None), "signal")
    fake_host.script_wait([signal_ok(op.id)])

    async def main():
        token = _shielded.set(True)
        try:
            return await op
        finally:
            _shielded.reset(token)

    assert loop.run_until_complete(main()) == "v"
    assert waits(fake_host) == [[op.id]]


def test_op_failure_raises_the_mapped_error(fake_host: FakeHost) -> None:
    loop = OstroLoop(fake_host)
    op = Op(loop, fake_host.wait_signal("a", None), "signal")
    fake_host.script_wait([(op.id, ("err", ("timeout", None)))])

    async def main():
        await op

    with pytest.raises(errors.Timeout):
        loop.run_until_complete(main())


@pytest.mark.parametrize(
    "name, args",
    [
        ("create_connection", (None, "h", 1)),
        ("run_in_executor", (None, print)),
        ("subprocess_exec", (None, "ls")),
        ("add_signal_handler", (2, print)),
        ("call_soon_threadsafe", (print,)),
        ("getaddrinfo", ("h", 80)),
    ],
)
def test_unsupported_loop_methods(fake_host: FakeHost, name: str, args: tuple) -> None:
    loop = OstroLoop(fake_host)
    with pytest.raises(NotImplementedError, match="Ostro"):
        getattr(loop, name)(*args)


def test_callback_exception_fails_the_activation(fake_host: FakeHost) -> None:
    loop = OstroLoop(fake_host)

    def boom() -> None:
        raise ValueError("boom")

    async def main():
        asyncio.get_running_loop().call_soon(boom)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    with pytest.raises(ValueError, match="boom"):
        loop.run_until_complete(main())
