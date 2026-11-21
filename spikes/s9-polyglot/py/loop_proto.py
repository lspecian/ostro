"""Spike S9: OstroLoop prototype (09-RESEARCH Pattern 1, D-P9-2). Throwaway.

An `asyncio.AbstractEventLoop` shaped like componentize-py 0.25.1's bundled
`poll_loop.PollLoop` (written next to the generated bindings by
`componentize-py bindings <dir>`), with `poll.poll(pollables)` replaced by ONE
`host.wait(sorted(interested ops) + [CANCELLATION], any)` per quiescent point:

1. run ready handles until the ready queue is empty (exact quiescence);
2. if the root future is done, return;
3. collect interested ops (awaited op futures that are not done, plus live timers);
4. none -> `Deadlock` (the root awaits something no Ostro op can complete);
5. otherwise wait, resolve completions in returned order, loop.

`call_at`/`call_later` become durable `sleep-until` ops (D-P9-2), `TimerHandle.cancel()`
becomes `cancel-op` while the timer is still pending, `time()` is logical (`host.now()`).
The host is duck-typed (`now`, `sleep_until`, `cancel_op`, `wait`, `cancellation`), so the
same code runs on host CPython 3.12 with `FakeHost` (pytest) and inside a componentize-py
guest (CPython 3.14.0) via `selfcheck()`.
"""

from __future__ import annotations

import asyncio
import collections
from asyncio import events

CANCELLATION = 2**64 - 1


class Deadlock(RuntimeError):
    """Non-retryable `sdk_protocol`: the workflow awaits a non-Ostro future."""


class Cancelled(Exception):
    """Graceful cancel observed by an unshielded wait (not asyncio.CancelledError)."""


class OpFailed(Exception):
    def __init__(self, failure):
        super().__init__(f"op failed: {failure!r}")
        self.failure = failure


class _OstroTimer(asyncio.TimerHandle):
    """TimerHandle uses __slots__, so the op id needs a subclass slot."""

    __slots__ = ("_ostro_op",)


class Op:
    """An eagerly started durable op; awaiting it registers interest (lazy wait)."""

    __slots__ = ("id", "loop")

    def __init__(self, loop: "OstroLoop", op_id: int):
        self.id = op_id
        self.loop = loop

    def __await__(self):
        return self.loop._op_future(self.id).__await__()


class OstroLoop(asyncio.AbstractEventLoop):
    def __init__(self, host):
        self._host = host
        self._ready: collections.deque = collections.deque()
        self._ops: dict[int, asyncio.Future] = {}
        self._timers: dict[int, asyncio.TimerHandle] = {}
        self._fired: set[int] = set()
        self._running = False
        self._closed = False
        self._exception = None
        self._cancel_seen = False
        self.wait_calls = 0

    # -- core -------------------------------------------------------------------------

    def run_until_complete(self, future):
        root = asyncio.ensure_future(future, loop=self)
        self._running = True
        events._set_running_loop(self)
        try:
            while True:
                while self._ready:
                    h = self._ready.popleft()
                    if not h._cancelled:
                        h._run()
                if self._exception is not None:
                    raise self._exception
                if root.done():
                    break
                ops = sorted(
                    [op for op, f in self._ops.items() if not f.done()]
                    + [op for op, h in self._timers.items() if not h._cancelled and op not in self._fired]
                )
                if not ops:
                    raise Deadlock("workflow awaits something no Ostro op can complete")
                if not self._cancel_seen:
                    ops.append(self._host.cancellation())
                self.wait_calls += 1
                for op, outcome in self._host.wait(ops, "any"):
                    self._complete(op, outcome)
            return root.result()
        finally:
            self._running = False
            events._set_running_loop(None)

    def _op_future(self, op: int) -> asyncio.Future:
        f = self._ops.get(op)
        if f is None:
            f = self.create_future()
            if self._cancel_seen:
                f.set_exception(Cancelled("run cancelled"))
            self._ops[op] = f
        return f

    def _complete(self, op: int, outcome) -> None:
        kind, value = outcome
        if op == CANCELLATION:
            self._cancel_seen = True
            for f in self._ops.values():
                if not f.done():
                    f.set_exception(Cancelled("run cancelled"))
            return
        h = self._timers.get(op)
        if h is not None:
            self._fired.add(op)
            if not h._cancelled:
                self._ready.append(h)
            return
        f = self._ops.get(op)
        if f is None or f.done():
            return
        if kind == "ok":
            f.set_result(value)
        else:
            f.set_exception(OpFailed(value))

    # -- scheduling -------------------------------------------------------------------

    def time(self) -> float:
        return self._host.now()

    def call_soon(self, callback, *args, context=None):
        h = asyncio.Handle(callback, args, self, context)
        self._ready.append(h)
        return h

    def call_at(self, when, callback, *args, context=None):
        op = self._host.sleep_until(when)
        h = _OstroTimer(when, callback, args, self, context)
        h._ostro_op = op
        self._timers[op] = h
        return h

    def call_later(self, delay, callback, *args, context=None):
        return self.call_at(self.time() + delay, callback, *args, context=context)

    def _timer_handle_cancelled(self, handle):
        op = getattr(handle, "_ostro_op", None)
        if op is not None and op not in self._fired:
            self._fired.add(op)
            self._host.cancel_op(op)

    def create_future(self):
        return asyncio.Future(loop=self)

    def create_task(self, coro, *, name=None, context=None, **kwargs):
        # 3.14 passes extra kwargs (eager_start) through create_task.
        if context is None:
            return asyncio.Task(coro, loop=self, name=name, **kwargs)
        return asyncio.Task(coro, loop=self, name=name, context=context, **kwargs)

    # -- misc protocol ----------------------------------------------------------------

    def get_debug(self):
        return False

    def is_running(self):
        return self._running

    def is_closed(self):
        return self._closed

    def close(self):
        self._closed = True

    def stop(self):
        pass

    async def shutdown_asyncgens(self):
        pass

    def call_exception_handler(self, context):
        self._exception = context.get("exception") or RuntimeError(context.get("message"))

    def default_exception_handler(self, context):
        self.call_exception_handler(context)

    def _unsupported(self, *args, **kwargs):
        raise NotImplementedError("not available inside an Ostro workflow (use ostro effects)")

    run_forever = call_soon_threadsafe = run_in_executor = set_default_executor = _unsupported
    getaddrinfo = getnameinfo = create_connection = create_server = _unsupported
    subprocess_exec = subprocess_shell = add_reader = add_writer = add_signal_handler = _unsupported


# ---------------------------------------------------------------------------------
# Fake host (tests and the in-guest self check)
# ---------------------------------------------------------------------------------


class FakeHost:
    """Records commands; `wait` completes ops according to `order` (op kinds)."""

    def __init__(self, order=None, now=1_700_000_000.0):
        self._now = now
        self._next = 1
        self.commands: list[tuple] = []
        self.kinds: dict[int, str] = {}
        self.order = order or (lambda ops, host: [(op, ("ok", host.kinds.get(op))) for op in ops if op != CANCELLATION])

    def _op(self, kind, *args):
        op = self._next
        self._next += 1
        self.kinds[op] = kind
        self.commands.append((kind, op) + args)
        return op

    def now(self):
        return self._now

    def sleep_until(self, at):
        return self._op("sleep_until", at)

    def wait_signal(self, name):
        return self._op("wait_signal", name)

    def effect(self, name):
        return self._op("effect", name)

    def cancel_op(self, op):
        self.commands.append(("cancel_op", op))

    def cancellation(self):
        return CANCELLATION

    def wait(self, ops, mode):
        assert mode == "any" and ops == sorted(ops), ops
        self.commands.append(("wait", tuple(ops)))
        out = self.order(ops, self)
        assert out, f"fake host: nothing completes among {ops}"
        return out


def selfcheck() -> dict:
    """The pytest scenarios in one function, for running inside a guest."""
    import sys

    res = {"python": sys.version.split()[0]}
    api = {
        "events._set_running_loop": hasattr(events, "_set_running_loop"),
        "Handle._run": hasattr(asyncio.Handle, "_run"),
        "TimerHandle": hasattr(asyncio, "TimerHandle"),
        "Task": hasattr(asyncio, "Task"),
        "timeout": hasattr(asyncio, "timeout"),
    }
    res["api"] = api

    # gather in returned order (host returns the later op first)
    host = FakeHost(order=lambda ops, h: [(op, ("ok", op)) for op in reversed(ops) if op != CANCELLATION])
    loop = OstroLoop(host)
    seen = []

    async def g():
        a, b = Op(loop, host.effect("a")), Op(loop, host.effect("b"))

        async def w(o):
            v = await o
            seen.append(v)
            return v

        return await asyncio.gather(w(a), w(b))

    res["gather"] = {"result": loop.run_until_complete(g()), "resolution_order": seen}

    # sleep(3600) -> one durable sleep, no cancel
    host = FakeHost()
    loop = OstroLoop(host)
    loop.run_until_complete(asyncio.sleep(3600))
    res["sleep"] = [c[0] for c in host.commands]

    # wait_for(signal, 5): signal wins -> one sleep, cancelled
    host = FakeHost(order=lambda ops, h: [(op, ("ok", "sig")) for op in ops if h.kinds.get(op) == "wait_signal"])
    loop = OstroLoop(host)

    async def wf():
        return await asyncio.wait_for(Op(loop, host.wait_signal("go")), 5)

    res["wait_for"] = {"result": loop.run_until_complete(wf()), "commands": [c[0] for c in host.commands]}

    # deadlock
    loop = OstroLoop(FakeHost())
    try:
        loop.run_until_complete(loop.create_future())
        res["deadlock"] = "no error"
    except Deadlock as e:
        res["deadlock"] = str(e)
    return res
