"""Durable operations: eagerly started, lazily awaited.

An effect call (`ctx.sleep`, `ctx.wait_signal`, `ctx.start_child`, ...) allocates its op
id at call time, exactly like the Rust SDK; awaiting the returned `Op` registers interest
with the `OstroLoop`, which includes the op in its next ``effects.wait``.

Decoders per op kind (the ``ok`` arm of the completion):

| kind | value |
|------|-------|
| ``timer`` | ``None`` |
| ``signal`` / ``token`` | the JSON-decoded payload |
| ``child`` (attached) | the JSON-decoded child output |
| ``child`` (detached) | `ChildRun(run_id)` |
| ``http`` / ``sql`` / ``object`` | the raw `HttpResponse` / `SqlResult` / ``bytes`` (09-12 builders wrap them) |
| ``cancellation`` | ``None`` (the run was asked to cancel) |

An ``err`` completion raises the matching `ostro.errors` exception (`Cancelled` for a
cancelled op, D5).
"""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ._host import Outcome
from .errors import ProtocolError, from_failure
from .json import decode_payload

if TYPE_CHECKING:
    from .loop import OstroLoop

__all__ = ["ChildRun", "Op", "decode_outcome"]

_KINDS = frozenset({"timer", "signal", "token", "child", "http", "sql", "object", "cancellation"})


@dataclass(frozen=True)
class ChildRun:
    """A started detached child run (D-P9-10): only its run id is known to the parent."""

    run_id: str


def decode_outcome(kind: str, outcome: Outcome, *, detached: bool = False) -> Any:
    """Decode a raw completion outcome for an op of `kind`, raising on ``err``."""
    tag, value = outcome
    if tag != "ok":
        raise from_failure(value)
    case, data = value
    if kind in ("timer", "cancellation"):
        return None
    if kind in ("signal", "token"):
        return decode_payload(data)
    if kind == "child":
        decoded = decode_payload(data)
        if detached:
            if not isinstance(decoded, dict) or not isinstance(decoded.get("run_id"), str):
                raise ProtocolError(f"detached child completion without a run_id: {decoded!r}")
            return ChildRun(run_id=decoded["run_id"])
        return decoded
    # http / sql / object: raw records for the 09-12 builders.
    return data


class Op:
    """An eagerly started durable operation. Await it for its result."""

    __slots__ = ("_loop", "detached", "id", "kind")

    def __init__(self, loop: OstroLoop, op_id: int, kind: str, *, detached: bool = False):
        if kind not in _KINDS:
            raise ValueError(f"unknown op kind {kind!r}")
        self._loop = loop
        self.id = op_id
        self.kind = kind
        self.detached = detached

    def __await__(self) -> Generator[Any, None, Any]:
        outcome = yield from self._loop._op_future(self.id).__await__()
        return decode_outcome(self.kind, outcome, detached=self.detached)

    def done(self) -> bool:
        """Whether the op's completion has already been observed by the loop."""
        f = self._loop._ops.get(self.id)
        return f is not None and f.done()

    def __repr__(self) -> str:
        return f"Op(id={self.id}, kind={self.kind!r})"
