"""The determinism surface (D-P9-15, WF-07): host-routed randomness, logical time, stdio,
gc guard. The patches are installed by ``import ostro`` and fall back to the originals
outside an activation."""

from __future__ import annotations

import datetime
import gc
import os
import random
import secrets
import sys
import time
import uuid

import pytest

import ostro
from ostro import _determinism, f64_from_u64, step
from ostro import loop as ostro_loop

from conftest import FakeHost, methods, run_in_loop


def test_installed_at_import() -> None:
    assert ostro._determinism.installed()
    assert isinstance(random._inst, _determinism.HostRandom)  # type: ignore[attr-defined]
    assert os.urandom is _determinism.urandom


def test_randomness_goes_to_the_host(fake_host: FakeHost) -> None:
    async def wf(ctx):
        return (
            random.random(),
            random.randint(1, 6),
            uuid.uuid4(),
            os.urandom(4),
            secrets.token_hex(2),
            random.getrandbits(100),
        )

    rnd, die, u4, ur, tok, bits = run_in_loop(fake_host, wf)
    expected = FakeHost()
    assert rnd == f64_from_u64(expected.random_u64())
    assert 1 <= die <= 6
    assert u4 == uuid.UUID(bytes=expected.random_bytes(16), version=4)
    assert ur == bytes([7, 38, 69, 100])
    assert tok == bytes([7, 38]).hex()
    assert 0 <= bits < 2**100
    m = methods(fake_host)
    assert m[0] == "random_u64"  # random.random
    assert "random_bytes" in m
    assert ("random_bytes", (16,)) in fake_host.calls  # uuid4
    assert ("random_bytes", (4,)) in fake_host.calls  # os.urandom
    assert ("random_bytes", (2,)) in fake_host.calls  # secrets.token_hex(2)
    assert m[-2:] == ["random_u64", "random_u64"]  # getrandbits(100): two words


def test_patches_fall_back_outside_an_activation() -> None:
    assert ostro_loop.active() is None
    assert abs(time.time() - _determinism._orig_time()) < 5
    assert 0.0 <= random.random() < 1.0
    assert len(os.urandom(8)) == 8
    assert uuid.uuid4().version == 4
    assert abs(datetime.datetime.now().timestamp() - _determinism._orig_time()) < 5


def test_time_is_logical(fake_host: FakeHost) -> None:
    async def wf(ctx):
        return (
            time.time(),
            time.time_ns(),
            time.monotonic(),
            time.perf_counter_ns(),
            datetime.datetime.now(datetime.timezone.utc),
            datetime.datetime.utcnow(),
            datetime.date.today(),
            time.gmtime(),
        )

    t, t_ns, mono, perf_ns, now_utc, utcnow, today, gm = run_in_loop(fake_host, wf)
    ns = fake_host.now
    expected = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(microseconds=ns // 1000)
    assert t == mono == ns / 1e9
    assert t_ns == perf_ns == ns
    assert now_utc == expected
    assert utcnow == expected.replace(tzinfo=None)
    assert isinstance(today, datetime.date)
    assert gm == time.gmtime(ns // 10**9)
    assert set(methods(fake_host)) == {"now_nanos"}


def test_replaced_datetime_classes_stay_compatible() -> None:
    orig = _determinism._OrigDatetime(2024, 1, 2, 3, 4, 5)
    assert isinstance(orig, datetime.datetime)
    assert isinstance(orig, datetime.date)
    assert isinstance(_determinism._OrigDate(2024, 1, 2), datetime.date)
    d = datetime.datetime(2024, 1, 2, 3, 4, 5)
    assert repr(d) == "datetime.datetime(2024, 1, 2, 3, 4, 5)"
    assert repr(datetime.date(2024, 1, 2)) == "datetime.date(2024, 1, 2)"
    assert d == orig
    assert issubclass(datetime.datetime, datetime.date)


def test_print_goes_to_log_emit(fake_host: FakeHost) -> None:
    async def wf(ctx):
        print("x")
        print("two", "words")
        print("err line", file=sys.stderr)
        sys.stdout.write("partial")
        return "ok"

    assert run_in_loop(fake_host, wf) == "ok"
    assert fake_host.logs == [
        ("info", "x", []),
        ("info", "two words", []),
        ("error", "err line", []),
        ("info", "partial", []),
    ]


def test_dict_iteration_and_values_replay_identically() -> None:
    def build(host: FakeHost):
        @step
        def fetch(n: int) -> dict:
            return {f"k{(i * 7) % n}": i for i in range(n)}

        async def wf(ctx):
            data = await fetch(9)
            data["extra"] = ctx.random_u64()
            return [list(data), sorted({"b", "a", "c"}), time.time()]

        return run_in_loop(host, wf)

    live = FakeHost()
    first = build(live)
    outcome = next(args[1] for m, args in live.calls if m == "step_complete")
    replay = FakeHost()
    replay.script_begin_step(("replayed", outcome))
    assert build(replay) == first


def test_gc_guard_blocks_host_calls_from_finalizers(fake_host: FakeHost) -> None:
    seen: list[str] = []

    class Finalized:
        def __init__(self, ctx):
            self.ctx = ctx
            self.me = self  # a cycle: only the cyclic GC frees it

        def __del__(self):
            try:
                self.ctx.random_u64()
                seen.append("called host")
            except ostro.ProtocolError:
                seen.append("blocked")

    async def wf(ctx):
        Finalized(ctx)
        gc.collect()
        return "ok"

    assert run_in_loop(fake_host, wf) == "ok"
    assert seen == ["blocked"]
    assert "random_u64" not in methods(fake_host)


def test_gc_flag_is_cleared_after_collection() -> None:
    gc.collect()
    assert ostro_loop._gc_active is False


@pytest.mark.parametrize("fn", ["random", "uniform", "choice", "shuffle", "sample", "randbytes", "gauss"])
def test_module_functions_are_rebound(fn: str) -> None:
    assert getattr(getattr(random, fn), "__self__", None) is random._inst  # type: ignore[attr-defined]
