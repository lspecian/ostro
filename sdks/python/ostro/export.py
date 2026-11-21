"""Workflow registry and the componentize-py ``workflows`` export (describe/run).

Apps declare workflows with ``@ostro.workflow`` and end ``app.py`` with
``ostro.export()``. That call (at import time, i.e. during componentize-py's pre-init):

- freezes the registry (later registrations raise);
- inside the guest, installs the real host adapter (`ostro._bindings`) and builds
  ``class Workflows(wit_world.exports.Workflows)``; outside a guest it builds a plain
  class with the same methods over the SDK's tuple types (host-side tests);
- binds that class as ``Workflows`` in the calling module, which is the name
  componentize-py 0.25.1 looks up for the exported interface ``workflows`` (S9), and
  returns it (``Workflows = ostro.export()`` works too);
- runs ``gc.collect()`` and ``gc.freeze()`` so the snapshot's objects are never revisited.

`run` creates a fresh `OstroLoop` per activation and maps the outcome to the WIT
``run-result``: a return value → ``completed`` (``application/json``); ``continue_as_new``
→ ``continue-as-new``; `ostro.Cancelled` → ``failure::cancelled``; an `OstroError` → its
failure; any other exception → non-retryable ``python_exception`` ``"Type: message"``; an
unknown name → non-retryable ``unknown_workflow``.
"""

from __future__ import annotations

import gc
import inspect
import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from . import _host
from ._host import ErrorInfo, Failure, Host, Payload
from .ctx import ContinueAsNew, Ctx, Duration, duration_ms
from .errors import to_failure
from .json import check_str, decode_payload, encode_payload
from .loop import OstroLoop

__all__ = [
    "Continuation",
    "Registry",
    "RunResult",
    "WorkflowDef",
    "WorkflowMeta",
    "describe",
    "export",
    "run",
    "workflow",
]

UNKNOWN_WORKFLOW = "unknown_workflow"


@dataclass(frozen=True)
class WorkflowMeta:
    """WIT ``workflows.workflow-meta``."""

    name: str
    version: int
    default_queue: str | None = None
    run_timeout_ms: int | None = None


@dataclass(frozen=True)
class Continuation:
    """WIT ``workflows.continuation``."""

    input: Payload
    workflow: str | None = None
    version: int | None = None


#: ``("completed", Payload)``, ``("continue-as-new", Continuation)`` or ``("err", Failure)``.
RunResult = tuple[str, Any]


@dataclass(frozen=True)
class WorkflowDef:
    meta: WorkflowMeta
    fn: Callable[..., Any]


class Registry:
    """Workflow definitions by name (unique names; frozen by `export`)."""

    def __init__(self) -> None:
        self._defs: dict[str, WorkflowDef] = {}
        self.frozen = False

    def add(self, d: WorkflowDef) -> None:
        if self.frozen:
            raise RuntimeError(f"workflow {d.meta.name!r} registered after ostro.export()")
        if d.meta.name in self._defs:
            raise ValueError(f"duplicate workflow name {d.meta.name!r}")
        self._defs[d.meta.name] = d

    def get(self, name: str) -> WorkflowDef | None:
        return self._defs.get(name)

    def describe(self) -> list[WorkflowMeta]:
        return [self._defs[n].meta for n in sorted(self._defs)]

    def freeze(self) -> None:
        self.frozen = True


_registry = Registry()


def workflow(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    version: int = 1,
    queue: str | None = None,
    run_timeout: Duration | None = None,
) -> Any:
    """Register a workflow ``async def fn(ctx: Ctx, input) -> result``.

    ``@workflow`` or ``@workflow(name="order", version=2, queue="orders", run_timeout=3600)``.
    """

    def deco(f: Callable[..., Any]) -> Callable[..., Any]:
        wf_name = check_str(name if name is not None else f.__name__, "workflow name")
        if queue is not None:
            check_str(queue, "queue name")
        if not isinstance(version, int) or isinstance(version, bool) or not 0 <= version < 2**32:
            raise ValueError("workflow version must be a u32")
        meta = WorkflowMeta(
            name=wf_name,
            version=version,
            default_queue=queue,
            run_timeout_ms=duration_ms(run_timeout, "run_timeout"),
        )
        _registry.add(WorkflowDef(meta=meta, fn=f))
        f.__ostro_workflow__ = meta  # type: ignore[attr-defined]
        return f

    if fn is not None:
        return deco(fn)
    return deco


def describe() -> list[WorkflowMeta]:
    """Every registered workflow, sorted by name."""
    return _registry.describe()


def run(name: str, input: Payload, host: Host | None = None) -> RunResult:
    """Run one activation of workflow `name` on `host` (default: the installed host)."""
    d = _registry.get(name)
    if d is None:
        info = ErrorInfo(kind=UNKNOWN_WORKFLOW, message=f"unknown workflow {name!r}")
        return ("err", ("non-retryable", info))
    loop = OstroLoop(host if host is not None else _host.current())
    ctx = Ctx(loop)
    try:
        value = decode_payload(input)

        async def root() -> Any:
            out = d.fn(ctx, value)
            if inspect.isawaitable(out):
                out = await out
            return out

        result = loop.run_until_complete(root())
        return ("completed", encode_payload(result))
    except ContinueAsNew as c:
        return ("continue-as-new", Continuation(input=c.input, workflow=c.workflow, version=c.version))
    except Exception as e:
        failure: Failure = to_failure(e)
        return ("err", failure)
    finally:
        loop.close()


class HostWorkflows:
    """The ``workflows`` export over the SDK's tuple types (used outside a guest)."""

    def describe(self) -> list[WorkflowMeta]:
        return describe()

    def run(self, name: str, input: Payload) -> RunResult:
        return run(name, input)


def _workflows_class() -> type:
    try:
        from . import _bindings
    except ImportError:
        return HostWorkflows
    if _host._current is None:
        _host.install(_bindings.BindingsHost())
    return _bindings.make_workflows_class(describe, run)


def export() -> type:
    """Freeze the registry and expose ``Workflows`` in the calling module (see module doc)."""
    _registry.freeze()
    cls = _workflows_class()
    caller = sys._getframe(1)
    caller.f_globals["Workflows"] = cls
    gc.collect()
    gc.freeze()
    return cls
