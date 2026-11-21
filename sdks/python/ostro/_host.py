"""Host contract of the Ostro Python SDK.

This is the **only** module that touches componentize-py's generated bindings
(`wit_world`). Everything else in the SDK talks to the host through the `Host`
protocol below, so host-side unit tests can install a fake host and the SDK core
never depends on the bindings' shape.

`Host` mirrors the frozen WIT world `ostro:runtime@0.1.0` (`ostro/wit/runtime.wit`)
1:1, with Python types:

- op ids are plain `int` (u64; the cancellation op is ``2**64 - 1``);
- a payload is ``(content_type: str, data: bytes)``;
- WIT records are the frozen dataclasses below; WIT variants are tagged tuples
  ``(case, value)`` with the WIT case name (``"non-retryable"``, ``"retry-now"``);
- ``result<T, failure>`` is ``("ok", T)`` or ``("err", Failure)``;
- WIT ``datetime`` is passed as integer nanoseconds since the Unix epoch
  (``seconds * 10**9 + nanoseconds``); the adapter converts both ways.

Imports trap during component initialisation (the componentize-py snapshot), so
nothing here calls the host at import time; `_bindings_host()` imports
`wit_world` lazily, inside the guest only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

__all__ = [
    "CANCELLATION_OP",
    "AwaitToken",
    "ChildOptions",
    "Completion",
    "ErrorInfo",
    "Failure",
    "Header",
    "Host",
    "HttpRequest",
    "HttpResponse",
    "LogLevel",
    "ObjectRequest",
    "OpOutput",
    "Outcome",
    "Payload",
    "RetryPolicy",
    "RunInfo",
    "SqlRequest",
    "SqlResult",
    "SqlValue",
    "StepHandle",
    "StepOptions",
    "StepStart",
    "current",
    "install",
]

#: Op id of `effects.cancellation` (2^64-1); `u64::MAX-15..=u64::MAX` is host-reserved.
CANCELLATION_OP: int = 2**64 - 1

#: WIT `types.payload`: ``(content_type, data)``.
Payload = tuple[str, bytes]


# --- types: records -------------------------------------------------------------


@dataclass(frozen=True)
class ErrorInfo:
    """WIT `types.error-info`."""

    kind: str
    message: str
    details: Payload | None = None


#: WIT `types.failure`: ``("retryable", ErrorInfo)``, ``("non-retryable", ErrorInfo)``,
#: ``("timeout", None)`` or ``("cancelled", None)``.
Failure = tuple[Literal["retryable", "non-retryable", "timeout", "cancelled"], ErrorInfo | None]

#: ``result<T, failure>``: ``("ok", value)`` or ``("err", Failure)``.
Outcome = tuple[Literal["ok", "err"], Any]


@dataclass(frozen=True)
class RetryPolicy:
    """WIT `types.retry-policy`."""

    max_attempts: int
    initial_interval_ms: int
    max_interval_ms: int
    backoff_coefficient: float
    non_retryable_kinds: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class StepOptions:
    """WIT `types.step-options`."""

    retry: RetryPolicy | None = None
    timeout_ms: int | None = None
    fuel: int | None = None


@dataclass(frozen=True)
class RunInfo:
    """WIT `types.run-info` (`deadline` in nanoseconds since the epoch)."""

    run_id: str
    workflow: str
    workflow_version: int
    attempt: int
    module_hash: str
    tenant: str | None = None
    parent_run_id: str | None = None
    deadline: int | None = None
    continued_from: str | None = None


# --- journal ----------------------------------------------------------------------


class StepHandle(Protocol):
    """Opaque handle of an executing step (WIT `journal.step`).

    Only the host that issued it interprets it; the SDK passes it back to
    `Host.step_seq` and `Host.step_complete`.
    """


#: WIT `journal.step-start`: ``("execute", StepHandle)`` or ``("replayed", Outcome)``.
StepStart = tuple[Literal["execute", "replayed"], Any]

#: WIT `journal.completion-ack`.
CompletionAck = Literal["recorded", "retry-now"]


# --- effects ----------------------------------------------------------------------


@dataclass(frozen=True)
class Header:
    """WIT `effects.header`."""

    name: str
    value: str


@dataclass(frozen=True)
class HttpRequest:
    """WIT `effects.http-request`."""

    method: str
    url: str
    headers: list[Header] = field(default_factory=list)
    body: bytes | None = None
    timeout_ms: int | None = None
    idempotency_key: str | None = None
    retry: RetryPolicy | None = None
    redact: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class HttpResponse:
    """WIT `effects.http-response`."""

    status: int
    headers: list[Header]
    body: bytes


#: WIT `effects.sql-value`: ``("null", None)``, ``("boolean", bool)``, ``("int8", int)``,
#: ``("float8", float)``, ``("numeric", str)``, ``("text", str)``, ``("bytea", bytes)``,
#: ``("json", str)`` or ``("timestamptz", int nanoseconds)``.
SqlValue = tuple[str, Any]


@dataclass(frozen=True)
class SqlRequest:
    """WIT `effects.sql-request`."""

    connection: str
    statement: str
    params: list[SqlValue] = field(default_factory=list)
    exactly_once: bool = False


@dataclass(frozen=True)
class SqlResult:
    """WIT `effects.sql-result`."""

    columns: list[str]
    rows: list[list[SqlValue]]
    rows_affected: int


@dataclass(frozen=True)
class ObjectRequest:
    """WIT `effects.object-request`."""

    connection: str
    op: str
    key: str
    body: bytes | None = None


@dataclass(frozen=True)
class ChildOptions:
    """WIT `effects.child-options`."""

    queue: str | None = None
    version: int | None = None
    detached: bool = False
    run_timeout_ms: int | None = None


@dataclass(frozen=True)
class AwaitToken:
    """WIT `effects.await-token`."""

    op: int
    token: str


#: WIT `effects.op-output`: ``("http", HttpResponse)``, ``("sql", SqlResult)``,
#: ``("object", bytes)``, ``("timer", None)``, ``("signal", Payload)``,
#: ``("child", Payload)``, ``("token", Payload)`` or ``("cancel-requested", None)``.
OpOutput = tuple[str, Any]

#: WIT `effects.completion`: ``(op, Outcome)`` where the ok value is an `OpOutput`.
Completion = tuple[int, Outcome]

#: WIT `log.level`.
LogLevel = Literal["trace", "debug", "info", "warn", "error"]


# --- the contract -----------------------------------------------------------------


@runtime_checkable
class Host(Protocol):
    """The WIT world's imports, 1:1 (see the module docstring for type mapping)."""

    # context
    def info(self) -> RunInfo: ...
    def now_nanos(self) -> int: ...
    def random_u64(self) -> int: ...
    def random_bytes(self, n: int) -> bytes: ...
    def uuid_v7(self) -> str: ...
    def is_replaying(self) -> bool: ...
    def patched(self, change_id: str) -> bool: ...
    def set_status(self, json: str) -> None: ...
    def continue_as_new_suggested(self) -> bool: ...

    # journal
    def begin_step(self, name: str, opts: StepOptions) -> StepStart: ...
    def step_seq(self, handle: StepHandle) -> int: ...
    def step_complete(self, handle: StepHandle, outcome: Outcome) -> CompletionAck: ...

    # effects
    def http_fetch(self, req: HttpRequest) -> int: ...
    def sql_query(self, req: SqlRequest) -> int: ...
    def object_store(self, req: ObjectRequest) -> int: ...
    def sleep(self, duration_ms: int) -> int: ...
    def sleep_until(self, at_nanos: int) -> int: ...
    def wait_signal(self, name: str, timeout_ms: int | None) -> int: ...
    def start_child(self, workflow: str, input: Payload, opts: ChildOptions) -> int: ...
    def create_await_token(self, timeout_ms: int | None) -> AwaitToken: ...
    def cancellation(self) -> int: ...
    def cancel_op(self, op: int) -> None: ...
    def wait(self, ops: list[int], any: bool) -> list[Completion]: ...

    # log
    def log_emit(self, level: LogLevel, message: str, fields: list[tuple[str, str]]) -> None: ...


_current: Host | None = None


def install(host: Host | None) -> Host | None:
    """Install `host` as the SDK's host and return the previously installed one.

    Passing ``None`` uninstalls; the next `current()` then falls back to the guest
    bindings.
    """
    global _current
    previous = _current
    _current = host
    return previous


def current() -> Host:
    """The installed host, or the guest bindings adapter if none was installed."""
    global _current
    if _current is None:
        _current = _bindings_host()
    return _current


def _bindings_host() -> Host:
    """Adapt componentize-py's generated `wit_world` bindings to `Host` (guest only)."""
    raise NotImplementedError("guest bindings land in 09-08")
