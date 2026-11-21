"""Shared fixtures: a scripted, recording fake `Host` for host-side SDK tests.

Run from `sdks/python` with `python -m pytest` so `ostro` is importable.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest

from ostro import _host
from ostro._host import (
    CANCELLATION_OP,
    AwaitToken,
    ChildOptions,
    Completion,
    CompletionAck,
    HttpRequest,
    LogLevel,
    ObjectRequest,
    Outcome,
    Payload,
    RunInfo,
    SqlRequest,
    StepOptions,
    StepStart,
)

__all__ = ["CANCELLATION_OP", "FIRST_OP", "FakeHost", "FakeStep"]

#: Op ids handed out by the fake host start here and grow by one per allocating call,
#: so they never collide with small list indices in assertions.
FIRST_OP = 16


@dataclass(frozen=True)
class FakeStep:
    """The fake host's `journal.step` handle."""

    name: str
    seq: int


class FakeHost:
    """A recording, scripted implementation of `ostro._host.Host`.

    - Every call is appended to `calls` as ``(method, args)``.
    - Op-allocating calls return fresh ids from `FIRST_OP` upward, in call order;
      `cancellation()` returns `CANCELLATION_OP`.
    - `begin_step` pops the next scripted start (`script_begin_step`), and by default
      returns ``("execute", FakeStep)``.
    - `wait` pops the next scripted completion list (`script_wait`), and by default
      fails the test loudly: an unscripted wait is a test bug.
    """

    def __init__(self, info: RunInfo | None = None, now_nanos: int = 1_700_000_000_000_000_000):
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.run_info = info or RunInfo(
            run_id="00000000-0000-8000-8000-000000000001",
            workflow="test",
            workflow_version=1,
            attempt=1,
            module_hash="0" * 64,
        )
        self.now = now_nanos
        self.replaying = False
        self.patches: dict[str, bool] = {}
        self.statuses: list[str] = []
        self.logs: list[tuple[LogLevel, str, list[tuple[str, str]]]] = []
        self.cancelled_ops: list[int] = []
        self.next_op = FIRST_OP
        self.next_seq = 1
        self._u64 = 0
        self._uuids = 0
        self._begin_steps: deque[StepStart] = deque()
        self._step_acks: deque[CompletionAck] = deque()
        self._waits: deque[list[Completion]] = deque()

    # --- scripting ------------------------------------------------------------

    def script_begin_step(self, *starts: StepStart) -> None:
        """Queue `begin_step` results, e.g. ``("replayed", ("ok", ("application/json", b"1")))``."""
        self._begin_steps.extend(starts)

    def script_step_ack(self, *acks: CompletionAck) -> None:
        """Queue `step_complete` acknowledgements (default ``"recorded"``)."""
        self._step_acks.extend(acks)

    def script_wait(self, *completion_lists: list[Completion]) -> None:
        """Queue `wait` results; each argument is the list one `wait` call returns."""
        self._waits.extend(completion_lists)

    def ops_issued(self) -> list[int]:
        """Every op id allocated so far, in order."""
        return list(range(FIRST_OP, self.next_op))

    def _record(self, method: str, *args: Any) -> None:
        self.calls.append((method, args))

    def _op(self) -> int:
        op = self.next_op
        self.next_op += 1
        return op

    # --- context --------------------------------------------------------------

    def info(self) -> RunInfo:
        self._record("info")
        return self.run_info

    def now_nanos(self) -> int:
        self._record("now_nanos")
        return self.now

    def random_u64(self) -> int:
        self._record("random_u64")
        # Deterministic, distinct values (splitmix64 over a counter).
        self._u64 = (self._u64 + 0x9E3779B97F4A7C15) % 2**64
        z = self._u64
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) % 2**64
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) % 2**64
        return z ^ (z >> 31)

    def random_bytes(self, n: int) -> bytes:
        self._record("random_bytes", n)
        return bytes((i * 31 + 7) % 256 for i in range(n))

    def uuid_v7(self) -> str:
        self._record("uuid_v7")
        self._uuids += 1
        return "018f0000-0000-7000-8000-%012x" % self._uuids

    def is_replaying(self) -> bool:
        self._record("is_replaying")
        return self.replaying

    def patched(self, change_id: str) -> bool:
        self._record("patched", change_id)
        return self.patches.setdefault(change_id, True)

    def set_status(self, json: str) -> None:
        self._record("set_status", json)
        self.statuses.append(json)

    def continue_as_new_suggested(self) -> bool:
        self._record("continue_as_new_suggested")
        return False

    # --- journal --------------------------------------------------------------

    def begin_step(self, name: str, opts: StepOptions) -> StepStart:
        self._record("begin_step", name, opts)
        if self._begin_steps:
            return self._begin_steps.popleft()
        step = FakeStep(name=name, seq=self.next_seq)
        self.next_seq += 1
        return ("execute", step)

    def step_seq(self, handle: FakeStep) -> int:  # type: ignore[override]
        self._record("step_seq", handle)
        return handle.seq

    def step_complete(self, handle: FakeStep, outcome: Outcome) -> CompletionAck:  # type: ignore[override]
        self._record("step_complete", handle, outcome)
        return self._step_acks.popleft() if self._step_acks else "recorded"

    # --- effects --------------------------------------------------------------

    def http_fetch(self, req: HttpRequest) -> int:
        self._record("http_fetch", req)
        return self._op()

    def sql_query(self, req: SqlRequest) -> int:
        self._record("sql_query", req)
        return self._op()

    def object_store(self, req: ObjectRequest) -> int:
        self._record("object_store", req)
        return self._op()

    def sleep(self, duration_ms: int) -> int:
        self._record("sleep", duration_ms)
        return self._op()

    def sleep_until(self, at_nanos: int) -> int:
        self._record("sleep_until", at_nanos)
        return self._op()

    def wait_signal(self, name: str, timeout_ms: int | None) -> int:
        self._record("wait_signal", name, timeout_ms)
        return self._op()

    def start_child(self, workflow: str, input: Payload, opts: ChildOptions) -> int:
        self._record("start_child", workflow, input, opts)
        return self._op()

    def create_await_token(self, timeout_ms: int | None) -> AwaitToken:
        self._record("create_await_token", timeout_ms)
        op = self._op()
        return AwaitToken(op=op, token=f"tok-{op}")

    def cancellation(self) -> int:
        self._record("cancellation")
        return CANCELLATION_OP

    def cancel_op(self, op: int) -> None:
        self._record("cancel_op", op)
        self.cancelled_ops.append(op)

    def wait(self, ops: list[int], any: bool) -> list[Completion]:
        self._record("wait", list(ops), any)
        if not self._waits:
            raise AssertionError(f"FakeHost: unscripted wait(ops={ops!r}, any={any!r})")
        return self._waits.popleft()

    # --- log ------------------------------------------------------------------

    def log_emit(self, level: LogLevel, message: str, fields: list[tuple[str, str]]) -> None:
        self._record("log_emit", level, message, list(fields))
        self.logs.append((level, message, list(fields)))


@pytest.fixture
def fake_host() -> Iterator[FakeHost]:
    """A fresh `FakeHost` installed as the SDK host; the previous host is restored after."""
    host = FakeHost()
    previous = _host.install(host)
    try:
        yield host
    finally:
        _host.install(previous)
