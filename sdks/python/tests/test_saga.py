"""Saga helper (09-12, D-P9-18): LIFO, sequential, shielded, continues past failures and
attaches every failure to the re-raised error's details (Rust `Saga` parity)."""

from __future__ import annotations

from typing import Any

import pytest

from conftest import CANCELLATION_OP, FIRST_OP, FakeHost, run_in_loop
from ostro import Cancelled, NonRetryable, Saga, step
from ostro.saga import SagaReport


def _step_names(host: FakeHost) -> list[str]:
    return [args[0] for m, args in host.calls if m == "begin_step"]


def _completed(host: FakeHost) -> list[tuple[str, str]]:
    return [(args[0].name, args[1][0]) for m, args in host.calls if m == "step_complete"]


@step
def book_a() -> str:
    return "a"


@step
def book_b() -> str:
    return "b"


@step
def book_c(fail: bool) -> str:
    if fail:
        raise NonRetryable("book_c_failed", "no rooms left")
    return "c"


@step
def cancel_a() -> None:
    return None


@step
def cancel_b(fail: bool = False) -> None:
    if fail:
        raise NonRetryable("cancel_b_failed", "refund rejected")


async def _three(ctx: Any, cancel_b_fails: bool = False) -> Any:
    saga = ctx.saga()
    await book_a()
    saga.add("cancel_a", cancel_a)
    await book_b()
    saga.add("cancel_b", lambda: cancel_b(cancel_b_fails))
    try:
        await book_c(True)
    except NonRetryable as e:
        await saga.fail_with(e)
    raise AssertionError("unreachable")


def test_three_compensates_in_reverse_order(fake_host: FakeHost) -> None:
    async def body(ctx: Any) -> Any:
        try:
            await _three(ctx)
        except NonRetryable as e:
            return e

    err = run_in_loop(fake_host, body)
    assert _step_names(fake_host) == ["book_a", "book_b", "book_c", "cancel_b", "cancel_a"]
    assert err.kind == "book_c_failed"
    assert err.details == {"compensated": ["cancel_b", "cancel_a"], "compensation_failures": []}


def test_failing_compensation_does_not_stop_the_rest(fake_host: FakeHost) -> None:
    async def body(ctx: Any) -> Any:
        try:
            await _three(ctx, cancel_b_fails=True)
        except NonRetryable as e:
            return e

    err = run_in_loop(fake_host, body)
    assert _step_names(fake_host) == ["book_a", "book_b", "book_c", "cancel_b", "cancel_a"]
    assert err.kind == "book_c_failed"
    assert err.details == {
        "compensated": ["cancel_a"],
        "compensation_failures": [
            {
                "compensation": "cancel_b",
                "error": {"kind": "cancel_b_failed", "message": "refund rejected", "retryable": False},
            }
        ],
    }


def test_no_failure_no_compensation(fake_host: FakeHost) -> None:
    async def body(ctx: Any) -> Any:
        saga = Saga(ctx)
        await book_a()
        saga.add("cancel_a", cancel_a)
        await book_b()
        saga.add("cancel_b", cancel_b)
        await book_c(False)
        return len(saga)

    assert run_in_loop(fake_host, body) == 2
    assert _step_names(fake_host) == ["book_a", "book_b", "book_c"]


def test_compensations_are_shielded_after_cancellation(fake_host: FakeHost) -> None:
    # saga_cancelled: book_a, book_b, a 1 h sleep; the run is cancelled during the sleep;
    # each compensation awaits a durable sleep, which must not fail with Cancelled.
    fake_host.script_wait(
        [(CANCELLATION_OP, ("ok", ("cancel-requested", None)))],
        [(FIRST_OP + 1, ("ok", ("timer", None)))],
        [(FIRST_OP + 2, ("ok", ("timer", None)))],
    )

    async def body(ctx: Any) -> Any:
        saga = ctx.saga()
        await book_a()

        async def comp_a() -> None:
            await ctx.sleep(1)
            await cancel_a()

        async def comp_b() -> None:
            await ctx.sleep(1)
            await cancel_b()

        saga.add("cancel_a", comp_a)
        await book_b()
        saga.add("cancel_b", comp_b)
        try:
            await ctx.sleep(3600)
        except Cancelled as e:
            await saga.fail_with(e)

    with pytest.raises(Cancelled) as info:
        run_in_loop(fake_host, body)
    assert _step_names(fake_host) == ["book_a", "book_b", "cancel_b", "cancel_a"]
    assert info.value.details == {"compensated": ["cancel_b", "cancel_a"], "compensation_failures": []}
    waits = [args[0] for m, args in fake_host.calls if m == "wait"]
    # The compensation sleeps are shielded: no CANCELLATION in their waits.
    assert waits == [[FIRST_OP, CANCELLATION_OP], [FIRST_OP + 1], [FIRST_OP + 2]]


def test_cancellation_arriving_during_compensation_does_not_interrupt_it(fake_host: FakeHost) -> None:
    # An unshielded sibling keeps CANCELLATION in the wait set; the cancel arrives while
    # cancel_b's shielded sleep is pending and the compensation still runs to the end.
    fake_host.script_wait(
        [(CANCELLATION_OP, ("ok", ("cancel-requested", None)))],
        [(FIRST_OP + 1, ("ok", ("timer", None)))],
    )

    async def body(ctx: Any) -> Any:
        import asyncio

        saga = ctx.saga()
        sibling = asyncio.ensure_future(ctx.sleep(3600))

        async def comp_b() -> None:
            await ctx.sleep(1)
            await cancel_b()

        saga.add("cancel_a", cancel_a)
        saga.add("cancel_b", comp_b)
        report = await saga.compensate()
        with pytest.raises(Cancelled):
            await sibling
        return report

    report = run_in_loop(fake_host, body)
    assert report == SagaReport(compensated=["cancel_b", "cancel_a"], failures=[])
    assert _step_names(fake_host) == ["cancel_b", "cancel_a"]


def test_fail_with_keeps_earlier_details_and_wraps_plain_exceptions(fake_host: FakeHost) -> None:
    async def body(ctx: Any) -> Any:
        out = []
        saga = ctx.saga()
        saga.add("cancel_a", cancel_a)
        try:
            await saga.fail_with(NonRetryable("k", "m", {"order": 7}))
        except NonRetryable as e:
            out.append(e.details)
        saga2 = ctx.saga()

        def boom() -> None:
            raise ValueError("bad")

        saga2.add("boom", boom)
        try:
            await saga2.fail_with(KeyError("x"))
        except NonRetryable as e:
            out.append((e.kind, e.message, e.details, type(e.__cause__).__name__))
        return out

    first, second = run_in_loop(fake_host, body)
    assert first == {"compensated": ["cancel_a"], "compensation_failures": [], "details": {"order": 7}}
    assert second == (
        "python_exception",
        "KeyError: 'x'",
        {
            "compensated": [],
            "compensation_failures": [
                {"compensation": "boom", "error": {"kind": "python_exception", "message": "ValueError: bad", "retryable": False}}
            ],
        },
        "KeyError",
    )


def test_details_reach_the_wit_failure() -> None:
    from ostro.errors import to_failure

    e = NonRetryable("book_c_failed", "no rooms left", {"compensated": ["cancel_a"], "compensation_failures": []})
    case, info = to_failure(e)
    assert case == "non-retryable"
    assert info.details == ("application/json", b'{"compensated":["cancel_a"],"compensation_failures":[]}')
