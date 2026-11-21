"""Durable steps: ``@step``, `Retry`, serialisation and step exclusivity (D-P9-16).

A step body runs at most once to a recorded completion; on replay the recorded outcome
is returned without running the body. Values are journaled as ``application/json`` and
the value returned to the caller is always decoded from the journaled bytes, so a live
run and its replay see identical values (as in the Rust SDK's ``run_step``).

Rules:

- **Serialised:** steps take an SDK FIFO lock (`asyncio.Lock`, one per activation) around
  ``begin-step``..``complete``, so step order = call order and no ``begin-step`` is
  issued while another step is open (PF-W1).
- **Exclusive:** while a step is open the `OstroLoop` runs only handles whose context
  carries the open step's id (the body runs in a copied context with `_current_step`
  set); everything else is deferred and restored, in order, when the step closes.
- **One resolution rule:** the caller awaits one future ``done`` that is never resolved
  synchronously: ``loop.call_soon(done.set_result, outcome)`` right after ``begin-step``
  answered ``replayed``, or (live) after the body settled, the step was completed and the
  deferred handles were put back at the front of the ready queue. Siblings that were
  ready when the step began therefore run before the caller resumes, live and on replay.
- A body that raises `NonRetryable` (or any `OstroError`) is completed with that failure;
  any other exception is a *retryable* ``python_exception`` (``"Type: message"``). The host
  answers ``retry-now`` to re-run the body in this activation.
- A body that needs a durable operation (an effect call, a loop timer, or awaiting an op)
  fails the step ``non-retryable sdk_protocol``.
- No ``retry=`` means no host retries (PF-W6); ``retry=Retry(...)`` fills the parts not
  given with the cross-SDK defaults 3 attempts / 1 s / 60 s / 2.0.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

from ._host import ErrorInfo, Outcome, RetryPolicy, StepOptions
from .ctx import ContinueAsNew, Duration, duration_ms
from .errors import SDK_PROTOCOL, ProtocolError, from_failure, to_failure
from .json import check_str, decode_payload, encode_payload
from .loop import OstroLoop, _current_step

__all__ = ["Retry", "run_step", "step"]

F = TypeVar("F", bound=Callable[..., Any])

DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_INITIAL = 1.0
DEFAULT_MAX = 60.0
DEFAULT_BACKOFF = 2.0


@dataclass(frozen=True)
class Retry:
    """Exponential-backoff retry policy executed by the host. Unset fields take the
    cross-SDK defaults (3 attempts including the first, 1 s, 60 s, coefficient 2.0)."""

    max_attempts: int | None = None
    initial: Duration | None = None
    max: Duration | None = None
    backoff: float | None = None
    non_retryable: tuple[str, ...] | list[str] = field(default_factory=tuple)

    def to_policy(self) -> RetryPolicy:
        kinds = [check_str(k, "non-retryable kind") for k in self.non_retryable]
        return RetryPolicy(
            max_attempts=DEFAULT_MAX_ATTEMPTS if self.max_attempts is None else int(self.max_attempts),
            initial_interval_ms=duration_ms(DEFAULT_INITIAL if self.initial is None else self.initial, "initial") or 0,
            max_interval_ms=duration_ms(DEFAULT_MAX if self.max is None else self.max, "max") or 0,
            backoff_coefficient=DEFAULT_BACKOFF if self.backoff is None else float(self.backoff),
            non_retryable_kinds=kinds,
        )


def _resolve(done: asyncio.Future[Any], outcome: Outcome) -> None:
    if not done.done():
        done.set_result(outcome)


def _reject(done: asyncio.Future[Any], exc: BaseException) -> None:
    if not done.done():
        done.set_exception(exc)


def _protocol_failure(message: str) -> Outcome:
    return ("err", ("non-retryable", ErrorInfo(kind=SDK_PROTOCOL, message=message)))


class _StepRun:
    """One executing (live) step: its handle, body task and the caller's ``done``."""

    __slots__ = ("done", "finished", "handle", "id", "loop", "name", "task")

    def __init__(self, loop: OstroLoop, step_id: int, name: str, handle: Any, done: asyncio.Future[Any]):
        self.loop = loop
        self.id = step_id
        self.name = name
        self.handle = handle
        self.done = done
        self.finished = False
        self.task: asyncio.Task[Any] | None = None

    def _close(self, outcome: Outcome | None, exc: BaseException | None = None) -> None:
        loop = self.loop
        if loop._open_step is self:
            loop._open_step = None
            # Deferred handles go back to the front, in their original order.
            loop._ready.extendleft(reversed(loop._deferred))
            loop._deferred.clear()
        if exc is not None:
            loop.call_soon(_reject, self.done, exc)
        else:
            loop.call_soon(_resolve, self.done, outcome)

    def abort(self, message: str) -> None:
        """The loop went quiescent with this step open: fail the step (sdk_protocol)."""
        if self.finished:
            return
        self.finished = True
        outcome = _protocol_failure(message)
        try:
            self.loop._host.step_complete(self.handle, outcome)
        except BaseException as e:
            self._close(None, e)
            raise
        else:
            self._close(outcome)
        finally:
            if self.task is not None:
                self.task.cancel()

    async def drive(self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        loop = self.loop
        try:
            while True:
                try:
                    value = fn(*args, **kwargs)
                    if inspect.isawaitable(value):
                        value = await value
                    outcome: Outcome = ("ok", encode_payload(value))
                except asyncio.CancelledError:
                    if self.finished:
                        return
                    outcome = _protocol_failure("step body was cancelled")
                except ContinueAsNew:
                    outcome = _protocol_failure("continue_as_new cannot be called inside a step")
                except Exception as e:
                    outcome = ("err", to_failure(e, retryable_default=True))
                if self.finished:
                    return
                ack = loop._h(False).step_complete(self.handle, outcome)
                if ack == "recorded":
                    break
                if ack != "retry-now":
                    raise ProtocolError(f"unknown step completion ack {ack!r}")
            self.finished = True
            self._close(outcome)
        except BaseException as e:
            if not self.finished:
                self.finished = True
                self._close(None, e)
            if not isinstance(e, Exception):
                raise


def _decode(outcome: Outcome) -> Any:
    tag, value = outcome
    if tag == "ok":
        return decode_payload(value)
    raise from_failure(value)


async def run_step(
    name: str,
    opts: StepOptions,
    fn: Callable[..., Any],
    args: tuple[Any, ...] = (),
    kwargs: dict[str, Any] | None = None,
) -> Any:
    """Run (or replay) the durable step `name` with body ``fn(*args, **kwargs)``."""
    loop = asyncio.get_running_loop()
    if not isinstance(loop, OstroLoop):
        raise ProtocolError("steps can only run inside an Ostro workflow")
    check_str(name, "step name")
    if loop._step_open():
        raise ProtocolError(f"step {name!r} called inside another step (steps cannot be nested)")
    loop._h(True)  # closed / gc checks before queueing on the lock
    if loop._step_lock is None:
        loop._step_lock = asyncio.Lock()
    lock = loop._step_lock
    await lock.acquire()
    try:
        start = loop._h(True).begin_step(name, opts)
        done: asyncio.Future[Any] = loop.create_future()
        tag, value = start
        if tag == "replayed":
            loop.call_soon(_resolve, done, value)
        elif tag == "execute":
            loop._next_step_id += 1
            run = _StepRun(loop, loop._next_step_id, name, value, done)
            context = contextvars.copy_context()
            context.run(_current_step.set, run.id)
            loop._open_step = run
            run.task = loop.create_task(run.drive(fn, args, kwargs or {}), context=context)
        else:
            raise ProtocolError(f"unknown step start {tag!r}")
        outcome = await done
    finally:
        lock.release()
    return _decode(outcome)


def step(
    fn: F | None = None,
    *,
    name: str | None = None,
    retry: Retry | None = None,
    timeout: Duration | None = None,
    fuel: int | None = None,
) -> Any:
    """Declare a durable step. Works on sync and async functions; calling the decorated
    function returns an awaitable: ``await charge(order)``.

    ``@step`` or ``@step(name="charge", retry=Retry(max_attempts=5), timeout=30, fuel=10**9)``.
    """

    def deco(f: F) -> F:
        step_name = name if name is not None else f.__name__
        opts = StepOptions(
            retry=retry.to_policy() if retry is not None else None,
            timeout_ms=duration_ms(timeout, "timeout"),
            fuel=fuel,
        )

        @functools.wraps(f)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await run_step(step_name, opts, f, args, kwargs)

        wrapper.__ostro_step__ = step_name  # type: ignore[attr-defined]
        wrapper.__ostro_step_options__ = opts  # type: ignore[attr-defined]
        return wrapper  # type: ignore[return-value]

    if fn is not None:
        return deco(fn)
    return deco
