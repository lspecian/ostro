"""`Ctx`: context reads, effects, children, tokens, cancellation and shielding (D11)."""

from __future__ import annotations

import asyncio
import datetime as dt
import json as stdjson
import struct
from pathlib import Path

import pytest

from ostro import Cancelled, ChildRun, InvalidString, ProtocolError, f64_from_u64
from ostro._host import CANCELLATION_OP, ChildOptions
from ostro.ctx import Ctx, duration_ms
from ostro.loop import OstroLoop

from conftest import FakeHost, methods, run_in_loop

C = CANCELLATION_OP
JSON = "application/json"
VECTORS = Path(__file__).resolve().parents[3] / "conformance" / "vectors"


def waits(host: FakeHost) -> list[list[int]]:
    return [args[0] for m, args in host.calls if m == "wait"]


def cancel_requested():
    return (C, ("ok", ("cancel-requested", None)))


# --- context reads --------------------------------------------------------------------


def test_random_f64_is_the_cross_sdk_mapping(fake_host: FakeHost) -> None:
    async def wf(ctx):
        return ctx.random_f64()

    expected = f64_from_u64(FakeHost().random_u64())
    assert run_in_loop(fake_host, wf) == expected
    assert f64_from_u64(2**64 - 1) < 1.0
    assert f64_from_u64(1 << 11) == 2.0**-53


def test_random_f64_vectors() -> None:
    path = VECTORS / "random_f64.json"
    if not path.exists():
        pytest.skip("conformance/vectors/random_f64.json is committed by 09-06 and is not present yet")
    cases = stdjson.loads(path.read_text(encoding="utf-8"))["cases"]
    assert cases
    for case in cases:
        f = f64_from_u64(int(case["input"]["u64"]))
        assert struct.pack(">d", f).hex() == case["expected"]["f64_bits_hex"], case


def test_context_reads(fake_host: FakeHost) -> None:
    async def wf(ctx):
        return (
            ctx.info().workflow,
            ctx.now(),
            ctx.uuid7(),
            ctx.random_bytes(3),
            ctx.is_replaying(),
            ctx.continue_as_new_suggested(),
        )

    workflow, now, uuid7, rnd, replaying, suggested = run_in_loop(fake_host, wf)
    assert workflow == "test"
    assert now == dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(microseconds=fake_host.now // 1000)
    assert uuid7.startswith("018f0000-")
    assert rnd == bytes([7, 38, 69])
    assert (replaying, suggested) == (False, False)
    assert methods(fake_host) == [
        "info", "now_nanos", "uuid_v7", "random_bytes", "is_replaying", "continue_as_new_suggested"
    ]


def test_journaled_context_calls(fake_host: FakeHost) -> None:
    async def wf(ctx):
        p = ctx.patched("v2")
        ctx.set_status({"stage": "ü", "n": 1})
        ctx.log("info", "hello", order=7, who="me")
        return p

    assert run_in_loop(fake_host, wf) is True
    assert fake_host.statuses == ['{"stage":"ü","n":1}']
    assert fake_host.logs == [("info", "hello", [("order", "7"), ("who", "me")])]


def test_durations() -> None:
    assert duration_ms(1.5) == 1500
    assert duration_ms(dt.timedelta(minutes=1)) == 60000
    assert duration_ms(None) is None
    with pytest.raises(ValueError):
        duration_ms(-1)
    with pytest.raises(TypeError):
        duration_ms("1s")  # type: ignore[arg-type]


# --- effects --------------------------------------------------------------------------


def test_sleep_and_sleep_until(fake_host: FakeHost) -> None:
    async def wf(ctx):
        a = ctx.sleep(dt.timedelta(seconds=2))
        at = dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc)
        b = ctx.sleep_until(at)
        fake_host.script_wait([(a.id, ("ok", ("timer", None))), (b.id, ("ok", ("timer", None)))])
        return await asyncio.gather(a, b)

    assert run_in_loop(fake_host, wf) == [None, None]
    assert fake_host.calls[0] == ("sleep", (2000,))
    assert fake_host.calls[1] == ("sleep_until", (1893456000 * 10**9,))


def test_wait_signal_and_children(fake_host: FakeHost) -> None:
    async def wf(ctx):
        sig = ctx.wait_signal("go", timeout=5)
        child = ctx.start_child("leaf", {"n": 1}, queue="q", version=2)
        det = ctx.start_child("leaf", None, detached=True, run_timeout=60)
        fake_host.script_wait(
            [
                (det.id, ("ok", ("child", (JSON, b'{"run_id":"r-9"}')))),
                (sig.id, ("ok", ("signal", (JSON, b'{"ok":true}')))),
                (child.id, ("ok", ("child", (JSON, b'{"echo":{"n":1}}')))),
            ]
        )
        return await asyncio.gather(sig, child, det)

    assert run_in_loop(fake_host, wf) == [{"ok": True}, {"echo": {"n": 1}}, ChildRun("r-9")]
    assert fake_host.calls[0] == ("wait_signal", ("go", 5000))
    assert fake_host.calls[1] == ("start_child", ("leaf", (JSON, b'{"n":1}'), ChildOptions("q", 2, False, None)))
    assert fake_host.calls[2] == ("start_child", ("leaf", (JSON, b"null"), ChildOptions(None, None, True, 60000)))


def test_await_token(fake_host: FakeHost) -> None:
    async def wf(ctx):
        op, token = ctx.create_await_token(timeout=30)
        fake_host.script_wait([(op.id, ("ok", ("token", (JSON, b'"approved"'))))])
        return token, await op

    token, value = run_in_loop(fake_host, wf)
    assert token.startswith("tok-") and value == "approved"
    assert fake_host.calls[0] == ("create_await_token", (30000,))


def test_cancel_op_completes_with_cancelled(fake_host: FakeHost) -> None:
    async def wf(ctx):
        op = ctx.wait_signal("never")
        ctx.cancel(op)
        fake_host.script_wait([(op.id, ("err", ("cancelled", None)))])
        with pytest.raises(Cancelled):
            await op
        return "ok"

    assert run_in_loop(fake_host, wf) == "ok"
    assert fake_host.cancelled_ops == [16]


def test_lone_surrogates_rejected_before_any_host_call(fake_host: FakeHost) -> None:
    async def wf(ctx):
        for call in (
            lambda: ctx.wait_signal("s\ud800"),
            lambda: ctx.start_child("w\udc00"),
            lambda: ctx.start_child("w", {"k": "\ud800"}),
            lambda: ctx.patched("p\ud800"),
            lambda: ctx.set_status("\ud800"),
            lambda: ctx.log("info", "\ud800"),
        ):
            with pytest.raises(InvalidString):
                call()
        return "ok"

    assert run_in_loop(fake_host, wf) == "ok"
    assert fake_host.calls == []


# --- cancellation and shielding (D11) -------------------------------------------------


def test_cancellation_spares_shielded_ops_and_their_tasks(fake_host: FakeHost) -> None:
    async def wf(ctx):
        plain = ctx.wait_signal("plain")
        guarded = ctx.wait_signal("guarded")
        in_task = ctx.wait_signal("in_task")
        fake_host.script_wait(
            [cancel_requested()],
            [(guarded.id, ("ok", ("signal", (JSON, b"1")))), (in_task.id, ("ok", ("signal", (JSON, b"2"))))],
        )
        results: dict[str, object] = {}

        async def shielded_part():
            async with ctx.shielded():
                child = asyncio.ensure_future(in_task)  # task created inside: shielded
                results["guarded"] = await guarded
                results["in_task"] = await child

        part = asyncio.ensure_future(shielded_part())
        with pytest.raises(Cancelled):
            await plain
        await part
        # Remembered: a later unshielded wait fails without a host call.
        n = len(fake_host.calls)
        with pytest.raises(Cancelled):
            await ctx.wait_signal("after")
        assert methods(fake_host)[n:] == ["wait_signal"]
        return results

    assert run_in_loop(fake_host, wf) == {"guarded": 1, "in_task": 2}
    assert waits(fake_host) == [[16, 17, 18, C], [17, 18]]


def test_cancellation_op_resolves_when_cancel_is_requested(fake_host: FakeHost) -> None:
    async def wf(ctx):
        c = ctx.cancellation()
        fake_host.script_wait([cancel_requested()])
        await c
        return ctx.cancelled

    assert run_in_loop(fake_host, wf) is True
    assert waits(fake_host) == [[C]]


def test_unshielded_asyncio_sleep_fails_on_cancellation(fake_host: FakeHost) -> None:
    async def wf(ctx):
        fake_host.script_wait([cancel_requested()])
        with pytest.raises(Cancelled):
            await asyncio.sleep(10)
        with pytest.raises(Cancelled):
            await asyncio.sleep(10)  # remembered: no new timer
        return "ok"

    assert run_in_loop(fake_host, wf) == "ok"
    assert methods(fake_host).count("sleep_until") == 1
    assert "cancel_op" not in methods(fake_host)


# --- closed after the root settles ----------------------------------------------------


def test_closed_after_the_root_settles(fake_host: FakeHost) -> None:
    loop = OstroLoop(fake_host)
    ctx = Ctx(loop)

    async def wf():
        return "done"

    assert loop.run_until_complete(wf()) == "done"
    for call in (lambda: ctx.sleep(1), lambda: ctx.random_u64(), lambda: ctx.wait_signal("s")):
        with pytest.raises(ProtocolError):
            call()
    assert fake_host.calls == []
