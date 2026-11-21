"""Host-side tests for the S9 OstroLoop prototype (CPython 3.12, pytest)."""

import asyncio
import sys
from asyncio import events

import pytest

from loop_proto import CANCELLATION, Cancelled, Deadlock, FakeHost, Op, OstroLoop, selfcheck


def kinds(host):
    return [c[0] for c in host.commands]


def test_asyncio_apis_used_exist():
    assert hasattr(events, "_set_running_loop")
    assert hasattr(asyncio.Handle, "_run") and hasattr(asyncio.Handle, "cancel")
    assert hasattr(asyncio, "TimerHandle") and hasattr(asyncio, "Task")
    # TimerHandle.cancel() must route through loop._timer_handle_cancelled (cancel-op hook).
    host = FakeHost()
    loop = OstroLoop(host)
    h = loop.call_later(10, lambda: None)
    h.cancel()
    assert kinds(host) == ["sleep_until", "cancel_op"]


def test_gather_resolves_in_returned_order():
    # The host returns the later op first: resolution must follow the returned order,
    # while gather still returns results positionally.
    host = FakeHost(order=lambda ops, h: [(op, ("ok", f"v{op}")) for op in reversed(ops) if op != CANCELLATION])
    loop = OstroLoop(host)
    seen = []

    async def wf():
        a = Op(loop, host.effect("a"))
        b = Op(loop, host.effect("b"))

        async def watch(o):
            v = await o
            seen.append(v)
            return v

        return await asyncio.gather(watch(a), watch(b))

    assert loop.run_until_complete(wf()) == ["v1", "v2"]
    assert seen == ["v2", "v1"]
    # one wait over both ops (sorted) + the cancellation op
    assert host.commands[-1] == ("wait", (1, 2, CANCELLATION))
    assert loop.wait_calls == 1


def test_race_returns_first_returned_completion():
    # Only the signal completes; the race (FIRST_COMPLETED) returns it, the timer stays open.
    host = FakeHost(order=lambda ops, h: [(op, ("ok", "go")) for op in ops if h.kinds.get(op) == "wait_signal"])
    loop = OstroLoop(host)

    async def wf():
        sig = asyncio.ensure_future(Op(loop, host.wait_signal("go")))
        nap = asyncio.ensure_future(asyncio.sleep(3600, "timer"))
        done, pending = await asyncio.wait({sig, nap}, return_when=asyncio.FIRST_COMPLETED)
        for p in pending:
            p.cancel()
        await asyncio.sleep(0)
        return [d.result() for d in done]

    assert loop.run_until_complete(wf()) == ["go"]
    # the losing durable sleep was cancelled through cancel-op
    assert kinds(host).count("sleep_until") == 1
    assert kinds(host).count("cancel_op") == 1


def test_sleep_issues_exactly_one_durable_sleep():
    host = FakeHost(now=1000.0)
    loop = OstroLoop(host)
    assert loop.run_until_complete(asyncio.sleep(3600, "woke")) == "woke"
    assert kinds(host) == ["sleep_until", "wait"]
    assert host.commands[0][2] == 1000.0 + 3600  # absolute logical deadline
    assert loop.time() == 1000.0


def test_wait_for_issues_one_sleep_and_cancels_it_on_success():
    host = FakeHost(order=lambda ops, h: [(op, ("ok", "sig")) for op in ops if h.kinds.get(op) == "wait_signal"])
    loop = OstroLoop(host)

    async def wf():
        return await asyncio.wait_for(Op(loop, host.wait_signal("go")), 5)

    assert loop.run_until_complete(wf()) == "sig"
    assert kinds(host) == ["wait_signal", "sleep_until", "wait", "cancel_op"]


def test_wait_for_times_out_durably():
    host = FakeHost(order=lambda ops, h: [(op, ("ok", None)) for op in ops if h.kinds.get(op) == "sleep_until"])
    loop = OstroLoop(host)

    async def wf():
        try:
            await asyncio.wait_for(Op(loop, host.wait_signal("go")), 5)
        except TimeoutError:
            return "timed out"

    assert loop.run_until_complete(wf()) == "timed out"
    assert kinds(host).count("sleep_until") == 1
    assert "cancel_op" not in kinds(host)  # the timer fired; nothing to cancel


def test_deadlock_on_bare_future():
    loop = OstroLoop(FakeHost())

    async def wf():
        await loop.create_future()

    with pytest.raises(Deadlock, match="no Ostro op"):
        loop.run_until_complete(wf())


def test_cancellation_rejects_unshielded_waits():
    host = FakeHost(order=lambda ops, h: [(CANCELLATION, ("ok", "cancel-requested"))])
    loop = OstroLoop(host)

    async def wf():
        try:
            await Op(loop, host.wait_signal("go"))
        except Cancelled:
            return "cancelled"

    assert loop.run_until_complete(wf()) == "cancelled"


def test_unsupported_methods_raise():
    loop = OstroLoop(FakeHost())
    with pytest.raises(NotImplementedError, match="Ostro"):
        loop.run_in_executor(None, print)


def test_selfcheck_matches_tests():
    r = selfcheck()
    assert r["python"] == sys.version.split()[0]
    assert all(r["api"].values())
    assert r["gather"] == {"result": [1, 2], "resolution_order": [2, 1]}
    assert r["sleep"] == ["sleep_until", "wait"]
    assert r["wait_for"]["commands"] == ["wait_signal", "sleep_until", "wait", "cancel_op"]
    assert "no Ostro op" in r["deadlock"]
