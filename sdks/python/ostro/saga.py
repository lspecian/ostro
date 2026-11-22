"""The saga helper (WF-10, D-P9-18), identical in behaviour to the Rust SDK's `Saga`.

Register a compensation after each completed step; compensate in reverse order when a
later step fails:

```python
saga = ctx.saga()                      # or ostro.Saga(ctx)
await book_a()
saga.add("cancel_a", cancel_a)         # any callable returning a value or an awaitable
await book_b()
saga.add("cancel_b", lambda: cancel_b(order))
try:
    await book_c()
except ostro.OstroError as e:
    await saga.fail_with(e)            # runs cancel_b, then cancel_a; re-raises e
```

`Saga.compensate` runs the compensations **sequentially, last registered first (LIFO)**,
inside ``ctx.shielded()``, so a cancelled workflow still compensates (the ops a
compensation awaits are not failed by the run's cancellation). A failing compensation
does not stop the others; every failure is reported. `Saga.fail_with` compensates and
re-raises the original error with the report in its ``details``:

```json
{"compensated": ["cancel_b", "cancel_a"],
 "compensation_failures": [{"compensation": "cancel_x",
                            "error": {"kind": "...", "message": "...", "retryable": false}}]}
```

Earlier details of the error are kept under ``"details"``. A compensation is called with
no arguments (shielding is carried by the context, unlike the TypeScript SDK, which passes
an explicit shielded ctx). Running compensations in parallel is out of scope.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, NoReturn

from .errors import PYTHON_EXCEPTION, NonRetryable, OstroError, exception_message
from .json import check_str

if TYPE_CHECKING:
    from .ctx import Ctx

__all__ = ["CompensationFailure", "Saga", "SagaReport"]


@dataclass(frozen=True)
class CompensationFailure:
    """One compensation that failed during `Saga.compensate`."""

    compensation: str
    error: BaseException

    def to_json(self) -> dict[str, Any]:
        e = self.error
        if isinstance(e, OstroError):
            err = {"kind": e.kind, "message": e.message, "retryable": e.retryable}
        else:
            err = {"kind": PYTHON_EXCEPTION, "message": exception_message(e), "retryable": False}
        return {"compensation": self.compensation, "error": err}


@dataclass
class SagaReport:
    """What `Saga.compensate` did."""

    #: Compensations that succeeded, in the order they ran (reverse registration order).
    compensated: list[str] = field(default_factory=list)
    #: Compensations that failed, in the order they ran.
    failures: list[CompensationFailure] = field(default_factory=list)

    def is_clean(self) -> bool:
        return not self.failures

    def to_json(self) -> dict[str, Any]:
        return {
            "compensated": list(self.compensated),
            "compensation_failures": [f.to_json() for f in self.failures],
        }


class Saga:
    """Compensations registered while a workflow makes progress (see the module docs)."""

    __slots__ = ("_comps", "_ctx")

    def __init__(self, ctx: Ctx):
        self._ctx = ctx
        self._comps: list[tuple[str, Callable[[], Any]]] = []

    def add(self, name: str, compensation: Callable[[], Any]) -> None:
        """Register compensation `name`; it runs before every compensation registered earlier."""
        check_str(name, "compensation name")
        if not callable(compensation):
            raise TypeError("compensation must be callable")
        self._comps.append((name, compensation))

    def __len__(self) -> int:
        return len(self._comps)

    def __repr__(self) -> str:
        return f"Saga(compensations={[n for n, _ in self._comps]!r})"

    async def compensate(self) -> SagaReport:
        """Run every compensation, last registered first, inside ``ctx.shielded()``; a
        failure is recorded and the remaining compensations still run. The saga is empty
        afterwards."""
        comps, self._comps = self._comps, []
        report = SagaReport()
        with self._ctx.shielded():
            for name, fn in reversed(comps):
                try:
                    result = fn()
                    if inspect.isawaitable(result):
                        await result
                except Exception as e:
                    report.failures.append(CompensationFailure(name, e))
                else:
                    report.compensated.append(name)
        return report

    async def fail_with(self, error: BaseException) -> NoReturn:
        """Compensate, then raise `error` with the report merged into its ``details``.

        An `OstroError` is re-raised itself (its earlier details move under
        ``"details"``); any other exception is raised as non-retryable
        ``python_exception`` (the kind an uncaught exception gets) chained from it. A
        non-`Exception` (e.g. continue-as-new) is re-raised unchanged after compensating.
        """
        report = await self.compensate()
        if not isinstance(error, Exception):
            raise error
        details = report.to_json()
        if isinstance(error, OstroError):
            if error._has_details:
                details["details"] = error.details
            error.details = details
            error._has_details = True
            raise error
        raise NonRetryable(PYTHON_EXCEPTION, exception_message(error), details) from error
