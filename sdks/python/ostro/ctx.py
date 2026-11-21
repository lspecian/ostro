"""`Ctx`: the workflow context (SDK-02 surface, Rust-identical semantics).

Context reads (`info`, `now`, `random_*`, `uuid7`, `is_replaying`,
`continue_as_new_suggested`, `log`) are allowed everywhere, including step bodies, where
randomness comes from the step-local stream (PF-W1, D7). Journaled calls (effects,
`patched`, `set_status`, `cancel`) raise `ProtocolError` inside a step body
("step bodies cannot await durable operations") and after the workflow settled.

Effect calls are eager: the op id is allocated when the method is called and the
returned `Op` is awaited lazily (gather/race work as in Rust).
"""

from __future__ import annotations

import datetime as _dt
from typing import Any, NoReturn

from ._host import CANCELLATION_OP, ChildOptions, LogLevel, RunInfo
from .errors import ProtocolError
from .json import check_str, dumps, encode_payload
from .loop import OstroLoop, _shielded
from .ops import Op

__all__ = ["ContinueAsNew", "Ctx", "Duration", "duration_ms", "f64_from_u64"]

#: Seconds (int/float) or a `datetime.timedelta`.
Duration = int | float | _dt.timedelta

_LEVELS = ("trace", "debug", "info", "warn", "error")
_UTC = _dt.timezone.utc
# Captured at import: `ostro._determinism` replaces `datetime.datetime` with a subclass.
_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_UTC)
_MAX_RANDOM_BYTES = 65536


def f64_from_u64(u: int) -> float:
    """``(u >> 11) * 2^-53``: the cross-SDK `random_f64` mapping (in ``[0, 1)``, exact)."""
    return (u >> 11) * 2.0**-53


def duration_ms(d: Duration | None, what: str = "duration") -> int | None:
    """Milliseconds of a duration in seconds or a `timedelta` (rounded, never negative)."""
    if d is None:
        return None
    if isinstance(d, _dt.timedelta):
        ms = d // _dt.timedelta(microseconds=1) / 1000
    elif isinstance(d, (int, float)) and not isinstance(d, bool):
        ms = d * 1000
    else:
        raise TypeError(f"{what} must be seconds (int/float) or a timedelta, not {type(d).__name__}")
    if ms != ms or ms < 0:
        raise ValueError(f"{what} must be a non-negative number of seconds")
    return round(ms)


def _nanos_of(at: Any) -> int:
    if isinstance(at, _dt.datetime):
        if at.tzinfo is None:
            at = at.replace(tzinfo=_UTC)
        return (at - _EPOCH) // _dt.timedelta(microseconds=1) * 1000
    if isinstance(at, (int, float)) and not isinstance(at, bool):
        return round(at * 1e9)
    raise TypeError("sleep_until takes a datetime or epoch seconds")


class ContinueAsNew(BaseException):
    """Internal signal raised by `Ctx.continue_as_new`; `run` maps it to the WIT
    ``run-result::continue-as-new``. A `BaseException`, so ``except Exception`` in user
    code does not swallow it."""

    def __init__(self, input: Any, workflow: str | None, version: int | None):
        super().__init__("continue-as-new")
        self.input = encode_payload(input)
        self.workflow = workflow
        self.version = version


class _Shield:
    """``with`` / ``async with`` scope in which ops are shielded from run cancellation."""

    __slots__ = ("_tokens",)

    def __init__(self) -> None:
        self._tokens: list[Any] = []

    def __enter__(self) -> _Shield:
        self._tokens.append(_shielded.set(True))
        return self

    def __exit__(self, *exc: object) -> None:
        _shielded.reset(self._tokens.pop())

    async def __aenter__(self) -> _Shield:
        return self.__enter__()

    async def __aexit__(self, *exc: object) -> None:
        self.__exit__(*exc)


class Ctx:
    """The context handed to a workflow function as its first argument."""

    __slots__ = ("_info", "_loop")

    def __init__(self, loop: OstroLoop):
        self._loop = loop
        self._info: RunInfo | None = None

    # -- context reads -----------------------------------------------------------------

    def info(self) -> RunInfo:
        """Run metadata (`attempt` is for logging only, F4)."""
        if self._info is None:
            self._info = self._loop._h(False).info()
        return self._info

    def now_nanos(self) -> int:
        """Logical time in nanoseconds since the Unix epoch."""
        return self._loop._h(False).now_nanos()

    def now(self) -> _dt.datetime:
        """Logical time as an aware UTC datetime (microsecond precision)."""
        return _EPOCH + _dt.timedelta(microseconds=self.now_nanos() // 1000)

    def random_u64(self) -> int:
        return self._loop._h(False).random_u64()

    def random_f64(self) -> float:
        return f64_from_u64(self.random_u64())

    def random_bytes(self, n: int) -> bytes:
        if n < 0:
            raise ValueError("random_bytes: n must be non-negative")
        h = self._loop._h(False)
        out = bytearray()
        while n > 0:
            k = min(n, _MAX_RANDOM_BYTES)
            out += h.random_bytes(k)
            n -= k
        return bytes(out)

    def uuid7(self) -> str:
        """A UUIDv7 string from the host's uuid stream (PF-W9)."""
        return self._loop._h(False).uuid_v7()

    def is_replaying(self) -> bool:
        return self._loop._h(False).is_replaying()

    def continue_as_new_suggested(self) -> bool:
        return self._loop._h(False).continue_as_new_suggested()

    def log(self, level: LogLevel, message: str, **fields: Any) -> None:
        """Emit a (non-durable) log line through ``log.emit``."""
        if level not in _LEVELS:
            raise ValueError(f"log level must be one of {_LEVELS}")
        check_str(message, "log message")
        pairs = [(check_str(k, "log field"), check_str(str(v), "log field")) for k, v in fields.items()]
        self._loop._h(False).log_emit(level, message, pairs)

    # -- journaled calls ---------------------------------------------------------------

    def patched(self, change_id: str) -> bool:
        """Versioning marker (D6): True on new code paths, memoised per change id by the host."""
        check_str(change_id, "change id")
        return self._loop._h(True).patched(change_id)

    def set_status(self, value: Any) -> None:
        """Publish a JSON status value for the run."""
        text = dumps(value).decode("utf-8")
        self._loop._h(True).set_status(text)

    def continue_as_new(self, input: Any = None, *, workflow: str | None = None, version: int | None = None) -> NoReturn:
        """End this run and start a successor with `input` (optionally another workflow/version)."""
        if self._loop._step_open():
            raise ProtocolError("continue_as_new cannot be called inside a step")
        if workflow is not None:
            check_str(workflow, "workflow name")
        raise ContinueAsNew(input, workflow, version)

    def sleep(self, duration: Duration) -> Op:
        """A durable timer op that fires after `duration`."""
        ms = duration_ms(duration)
        return Op(self._loop, self._loop._h(True).sleep(ms), "timer")

    def sleep_until(self, at: _dt.datetime | int | float) -> Op:
        """A durable timer op that fires at `at` (a datetime, naive = UTC, or epoch seconds)."""
        ns = _nanos_of(at)
        return Op(self._loop, self._loop._h(True).sleep_until(ns), "timer")

    def wait_signal(self, name: str, timeout: Duration | None = None) -> Op:
        """An op completing with the next signal `name` (JSON-decoded), or `Timeout`."""
        check_str(name, "signal name")
        ms = duration_ms(timeout, "timeout")
        return Op(self._loop, self._loop._h(True).wait_signal(name, ms), "signal")

    def start_child(
        self,
        workflow: str,
        input: Any = None,
        *,
        queue: str | None = None,
        version: int | None = None,
        detached: bool = False,
        run_timeout: Duration | None = None,
    ) -> Op:
        """Start a child workflow. Attached: the op completes with the child's output.
        Detached: the op completes with `ChildRun(run_id)` once the child is started."""
        check_str(workflow, "workflow name")
        if queue is not None:
            check_str(queue, "queue name")
        payload = encode_payload(input)
        opts = ChildOptions(
            queue=queue,
            version=version,
            detached=detached,
            run_timeout_ms=duration_ms(run_timeout, "run_timeout"),
        )
        op = self._loop._h(True).start_child(workflow, payload, opts)
        return Op(self._loop, op, "child", detached=detached)

    def create_await_token(self, timeout: Duration | None = None) -> tuple[Op, str]:
        """An await token: ``(op, token)``. Hand `token` to an external system; the op
        completes with the JSON payload it resolves the token with."""
        tok = self._loop._h(True).create_await_token(duration_ms(timeout, "timeout"))
        return Op(self._loop, tok.op, "token"), tok.token

    def cancel(self, op: Op | int) -> None:
        """Cancel a pending op (``cancel-op``); it then completes with `Cancelled` (D5)."""
        op_id = op.id if isinstance(op, Op) else op
        self._loop._h(True).cancel_op(op_id)

    def cancellation(self) -> Op:
        """An op that completes (with ``None``) when the run is asked to cancel."""
        op = self._loop._h(True).cancellation()
        if op != CANCELLATION_OP:
            raise ProtocolError(f"host cancellation op {op} is not the reserved id")
        return Op(self._loop, op, "cancellation")

    def shielded(self) -> _Shield:
        """``async with ctx.shielded():`` ops awaited inside (and in tasks created inside)
        are not failed by run cancellation (D11)."""
        return _Shield()

    @property
    def cancelled(self) -> bool:
        """Whether the run's cancellation has been observed."""
        return self._loop._cancel_seen
