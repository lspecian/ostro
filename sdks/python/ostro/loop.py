"""`OstroLoop`: the SDK-owned asyncio event loop over ``effects.wait`` (SDK-02, D-P9-2).

The structure follows componentize-py 0.25.1's bundled ``poll_loop.PollLoop`` (an
`asyncio.AbstractEventLoop` that drains its ready queue and then blocks in one host
call); no source is copied beyond that shape. ``poll.poll(pollables)`` is replaced by
one ``effects.wait(sorted(interested ops) [+ CANCELLATION], any)``:

1. run ready handles until the ready queue is empty (exact quiescence), stopping as soon
   as the root task settles (remaining tasks never run);
2. if the root is done, return its result;
3. if a step is open, the step body is blocked on something only the host could
   complete: the step is completed ``non-retryable sdk_protocol`` (D-P9-16);
4. collect the interested ops: op futures that are awaited and not done, plus live
   durable timers; none means `Deadlock`;
5. ``wait`` and resolve the futures in the returned (journal) order, then loop.

Other rules:

- `call_at`/`call_later` map to durable ``sleep-until`` ops and `TimerHandle.cancel()`
  maps to ``cancel-op`` (only while the timer is pending), so ``asyncio.sleep``,
  ``asyncio.timeout`` and ``wait_for`` are durable. `time()` is logical seconds from
  ``context.now``. Timer deadlines are relative to ``now`` at millisecond resolution.
- ``CANCELLATION`` is appended unless every interested op is shielded (D11); once it
  completed, pending unshielded op futures fail with `ostro.Cancelled` and later
  unshielded awaits fail at once, without a host call.
- **Step exclusivity (D-P9-16):** while a step is open only ready handles whose
  ``Handle._context`` carries the open step's id in `_current_step` run; every other
  handle is deferred and put back at the front of the ready queue, in its original
  order, when the step closes.
- After the root settles the loop is *closed* for the SDK: every host call through it
  raises `ProtocolError` without touching the host (Pitfall 1).
- Socket, subprocess, executor and signal methods raise `NotImplementedError`.

asyncio internals relied on (present in CPython 3.12 and 3.14): `asyncio.Handle` /
`asyncio.TimerHandle` (``_run``, ``_cancelled``, ``_context``, ``_callback``,
``_args``), ``asyncio.events._get_running_loop`` / ``_set_running_loop``,
``asyncio.futures._set_result_unless_cancelled`` (the ``asyncio.sleep`` callback), and
`asyncio.Task` accepting ``context=`` plus extra keyword arguments (3.14's
``eager_start``).
"""

from __future__ import annotations

import asyncio
import collections
import contextvars
from asyncio import events, futures
from collections.abc import Callable
from typing import Any

from . import _host
from ._host import CANCELLATION_OP, Host
from .errors import Cancelled, Deadlock, ProtocolError

__all__ = [
    "STEP_DURABLE_MESSAGE",
    "OstroLoop",
    "active",
]

#: Message of the ``sdk_protocol`` failure of a step whose body needs a durable operation.
STEP_DURABLE_MESSAGE = "step bodies cannot await durable operations"

#: Id of the open step whose body runs in this context (D-P9-16).
_current_step: contextvars.ContextVar[int | None] = contextvars.ContextVar("ostro_current_step", default=None)
#: True inside ``ctx.shielded()`` (D11); copied into tasks created there.
_shielded: contextvars.ContextVar[bool] = contextvars.ContextVar("ostro_shielded", default=False)

#: The loop of the running activation (None outside `run_until_complete`).
_active: OstroLoop | None = None
#: Set by the gc callback guard (`ostro._determinism`) while a collection runs.
_gc_active: bool = False
#: ``(enter, exit)`` pairs called around every activation (stdio redirection).
_activation_hooks: list[tuple[Callable[[], Any], Callable[[Any], None]]] = []


def active() -> OstroLoop | None:
    """The loop of the running activation, if any and not yet settled."""
    loop = _active
    if loop is None or loop._sdk_closed:
        return None
    return loop


class _OstroTimer(asyncio.TimerHandle):
    """`TimerHandle` has ``__slots__``: the op id needs a subclass slot."""

    __slots__ = ("_ostro_op",)


def _fail_cancelled(fut: asyncio.Future[Any]) -> None:
    if not fut.done():
        fut.set_exception(Cancelled("run cancelled"))


def _unsupported(name: str) -> Callable[..., Any]:
    def method(self: OstroLoop, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError(
            f"loop.{name}() is not available inside an Ostro workflow: workflows are "
            "deterministic and talk to the outside world only through Ostro effects "
            "(ctx.sleep, steps, HTTP/SQL/object effects)"
        )

    method.__name__ = name
    return method


class OstroLoop(asyncio.AbstractEventLoop):
    """One activation's event loop. Create a fresh loop per ``run``."""

    def __init__(self, host: Host | None = None):
        self._host: Host = host if host is not None else _host.current()
        self._ready: collections.deque[asyncio.Handle] = collections.deque()
        self._deferred: collections.deque[asyncio.Handle] = collections.deque()
        self._ops: dict[int, asyncio.Future[Any]] = {}
        self._shielded_ops: set[int] = set()
        self._timers: dict[int, _OstroTimer] = {}
        self._running = False
        self._closed = False
        self._sdk_closed = False
        self._cancel_seen = False
        self._exception: BaseException | None = None
        self._exception_handler: Any = None
        # Step machinery (driven by ostro.step).
        self._open_step: Any = None
        self._next_step_id = 0
        self._step_lock: asyncio.Lock | None = None
        self.wait_calls = 0

    # -- host access -------------------------------------------------------------------

    def _h(self, journaled: bool) -> Host:
        """The host, after the SDK checks (closed, open step for journaled calls, gc)."""
        if self._sdk_closed:
            raise ProtocolError("the workflow has already settled; no further Ostro calls are allowed")
        if journaled and self._open_step is not None:
            raise ProtocolError(STEP_DURABLE_MESSAGE)
        if _gc_active:
            raise ProtocolError("Ostro host call from a finalizer during garbage collection")
        return self._host

    def _step_open(self) -> bool:
        return self._open_step is not None

    # -- core --------------------------------------------------------------------------

    def run_until_complete(self, future: Any) -> Any:
        global _active
        if self._running:
            raise RuntimeError("OstroLoop is already running")
        if self._sdk_closed:
            raise RuntimeError("OstroLoop already ran an activation; create a new loop")
        root = asyncio.ensure_future(future, loop=self)
        previous_loop = events._get_running_loop()
        previous_active = _active
        self._running = True
        events._set_running_loop(self)
        _active = self
        hook_state = [(exit_, enter()) for enter, exit_ in _activation_hooks]
        try:
            while True:
                self._drain(root)
                if self._exception is not None:
                    exc, self._exception = self._exception, None
                    raise exc
                if root.done():
                    break
                step = self._open_step
                if step is not None:
                    step.abort(STEP_DURABLE_MESSAGE)
                    continue
                ops = self._interested()
                if not ops:
                    raise Deadlock(
                        "the workflow awaits something no Ostro operation can complete "
                        "(a bare future, a lock or event nobody releases)"
                    )
                self.wait_calls += 1
                completions = self._h(True).wait(ops, True)
                if not completions:
                    raise ProtocolError(f"host wait({ops}) returned no completion")
                for op, outcome in completions:
                    self._complete(op, outcome)
            return root.result()
        finally:
            for exit_, state in reversed(hook_state):
                exit_(state)
            self._sdk_closed = True
            self._running = False
            _active = previous_active
            events._set_running_loop(previous_loop)

    def _drain(self, root: asyncio.Future[Any]) -> None:
        ready = self._ready
        while ready:
            if root.done():
                return
            h = ready.popleft()
            if h._cancelled:
                continue
            step = self._open_step
            if step is not None and h._context.get(_current_step) != step.id:
                self._deferred.append(h)
                continue
            h._run()

    def _interested(self) -> list[int]:
        ops: list[int] = []
        unshielded = False
        for op, f in self._ops.items():
            if f.done():
                continue
            ops.append(op)
            if op not in self._shielded_ops:
                unshielded = True
        for op, h in self._timers.items():
            ops.append(op)
            if not h._context.get(_shielded, False):
                unshielded = True
        ops.sort()
        if unshielded and not self._cancel_seen and CANCELLATION_OP not in ops:
            ops.append(CANCELLATION_OP)
        return ops

    def _op_future(self, op: int) -> asyncio.Future[Any]:
        """The future of op `op`, created on first await (registers interest)."""
        f = self._ops.get(op)
        if f is None:
            f = self.create_future()
            if op == CANCELLATION_OP:
                self._shielded_ops.add(op)
                if self._cancel_seen:
                    f.set_result(("ok", ("cancel-requested", None)))
            elif _shielded.get():
                self._shielded_ops.add(op)
            elif self._cancel_seen:
                f.set_exception(Cancelled("run cancelled"))
            self._ops[op] = f
        return f

    def _complete(self, op: int, outcome: Any) -> None:
        if op == CANCELLATION_OP:
            self._on_cancellation(outcome)
            return
        h = self._timers.pop(op, None)
        if h is not None:
            if not h._cancelled:
                self._ready.append(h)
            return
        f = self._ops.get(op)
        if f is None or f.done():
            return
        f.set_result(outcome)

    def _on_cancellation(self, outcome: Any) -> None:
        if self._cancel_seen:
            return
        self._cancel_seen = True
        cf = self._ops.get(CANCELLATION_OP)
        if cf is not None and not cf.done():
            cf.set_result(outcome)
        for op, f in self._ops.items():
            if op not in self._shielded_ops and not f.done():
                f.set_exception(Cancelled("run cancelled"))
        # Unshielded `asyncio.sleep` timers fail like ctx.sleep ops (other timer
        # callbacks, e.g. wait_for timeouts, keep their durable timer).
        for op, h in list(self._timers.items()):
            fut = self._sleep_future(h)
            if fut is not None and not h._context.get(_shielded, False):
                del self._timers[op]
                _fail_cancelled(fut)

    @staticmethod
    def _sleep_future(h: asyncio.TimerHandle) -> asyncio.Future[Any] | None:
        if h._callback is futures._set_result_unless_cancelled and h._args:
            fut = h._args[0]
            if isinstance(fut, asyncio.Future):
                return fut
        return None

    # -- scheduling --------------------------------------------------------------------

    def time(self) -> float:
        """Logical seconds since the epoch (``context.now``)."""
        return self._h(False).now_nanos() / 1e9

    def call_soon(self, callback: Callable[..., Any], *args: Any, context: contextvars.Context | None = None) -> asyncio.Handle:
        h = asyncio.Handle(callback, args, self, context)
        self._ready.append(h)
        return h

    def _check_timer_allowed(self) -> None:
        if self._open_step is not None:
            raise ProtocolError(
                "loop timers (asyncio.sleep(d > 0), asyncio.timeout, asyncio.wait_for, "
                "call_later/call_at) are not allowed inside a step"
            )

    def _schedule_timer(
        self,
        when: float,
        at_nanos: Callable[[], int],
        callback: Callable[..., Any],
        args: tuple[Any, ...],
        context: contextvars.Context | None,
    ) -> _OstroTimer:
        handle = _OstroTimer(when, callback, args, self, context)
        handle._ostro_op = None
        fut = self._sleep_future(handle)
        if self._cancel_seen and fut is not None and not handle._context.get(_shielded, False):
            # Remembered cancellation: an unshielded sleep fails without a host call.
            self.call_soon(_fail_cancelled, fut)
            return handle
        host = self._h(True)
        op = host.sleep_until(at_nanos())
        handle._ostro_op = op
        self._timers[op] = handle
        return handle

    def call_at(self, when: float, callback: Callable[..., Any], *args: Any, context: contextvars.Context | None = None) -> asyncio.TimerHandle:
        self._check_timer_allowed()

        def at() -> int:
            now = self._h(False).now_nanos()
            delta_ms = round((when - now / 1e9) * 1000)
            return now + max(0, delta_ms) * 1_000_000

        return self._schedule_timer(when, at, callback, args, context)

    def call_later(self, delay: float, callback: Callable[..., Any], *args: Any, context: contextvars.Context | None = None) -> asyncio.TimerHandle:
        self._check_timer_allowed()
        now = self._h(False).now_nanos()
        delay_ns = max(0, round(delay * 1000)) * 1_000_000
        return self._schedule_timer((now + delay_ns) / 1e9, lambda: now + delay_ns, callback, args, context)

    def _timer_handle_cancelled(self, handle: asyncio.TimerHandle) -> None:
        op = getattr(handle, "_ostro_op", None)
        if op is None or op not in self._timers:
            return  # already fired, already cancelled, or never journaled
        del self._timers[op]
        if self._sdk_closed:
            return
        self._h(True).cancel_op(op)

    def create_future(self) -> asyncio.Future[Any]:
        return asyncio.Future(loop=self)

    def create_task(self, coro: Any, *, name: str | None = None, context: contextvars.Context | None = None, **kwargs: Any) -> asyncio.Task[Any]:
        # 3.14 forwards extra keyword arguments (eager_start) through create_task.
        return asyncio.Task(coro, loop=self, name=name, context=context, **kwargs)

    # -- misc protocol -----------------------------------------------------------------

    def get_debug(self) -> bool:
        return False

    def set_debug(self, enabled: bool) -> None:
        pass

    def is_running(self) -> bool:
        return self._running

    def is_closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        self._closed = True
        self._ready.clear()
        self._deferred.clear()

    def stop(self) -> None:
        pass

    async def shutdown_asyncgens(self) -> None:
        pass

    async def shutdown_default_executor(self, timeout: float | None = None) -> None:
        pass

    def get_exception_handler(self) -> Any:
        return self._exception_handler

    def set_exception_handler(self, handler: Any) -> None:
        self._exception_handler = handler

    def default_exception_handler(self, context: dict[str, Any]) -> None:
        # Only exceptions escaping a scheduled callback fail the activation. Reports from
        # destructors ("Task exception was never retrieved", pending tasks destroyed) are
        # GC-timed, so they are ignored: they must not change behaviour or call the host.
        exc = context.get("exception")
        if "handle" in context and "task" not in context and "future" not in context and exc is not None:
            if self._exception is None:
                self._exception = exc

    def call_exception_handler(self, context: dict[str, Any]) -> None:
        if self._exception_handler is not None:
            self._exception_handler(self, context)
        else:
            self.default_exception_handler(context)

    # Everything that would touch the outside world.
    run_forever = _unsupported("run_forever")
    call_soon_threadsafe = _unsupported("call_soon_threadsafe")
    run_in_executor = _unsupported("run_in_executor")
    set_default_executor = _unsupported("set_default_executor")
    getaddrinfo = _unsupported("getaddrinfo")
    getnameinfo = _unsupported("getnameinfo")
    create_connection = _unsupported("create_connection")
    create_server = _unsupported("create_server")
    sendfile = _unsupported("sendfile")
    start_tls = _unsupported("start_tls")
    create_unix_connection = _unsupported("create_unix_connection")
    create_unix_server = _unsupported("create_unix_server")
    connect_accepted_socket = _unsupported("connect_accepted_socket")
    create_datagram_endpoint = _unsupported("create_datagram_endpoint")
    connect_read_pipe = _unsupported("connect_read_pipe")
    connect_write_pipe = _unsupported("connect_write_pipe")
    subprocess_exec = _unsupported("subprocess_exec")
    subprocess_shell = _unsupported("subprocess_shell")
    add_reader = _unsupported("add_reader")
    remove_reader = _unsupported("remove_reader")
    add_writer = _unsupported("add_writer")
    remove_writer = _unsupported("remove_writer")
    sock_recv = _unsupported("sock_recv")
    sock_recv_into = _unsupported("sock_recv_into")
    sock_recvfrom = _unsupported("sock_recvfrom")
    sock_recvfrom_into = _unsupported("sock_recvfrom_into")
    sock_sendall = _unsupported("sock_sendall")
    sock_sendto = _unsupported("sock_sendto")
    sock_connect = _unsupported("sock_connect")
    sock_accept = _unsupported("sock_accept")
    sock_sendfile = _unsupported("sock_sendfile")
    add_signal_handler = _unsupported("add_signal_handler")
    remove_signal_handler = _unsupported("remove_signal_handler")
