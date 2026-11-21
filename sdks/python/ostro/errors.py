"""Workflow and step errors, and their mapping to the WIT `failure` variant.

Mirrors the Rust SDK (`ostro_sdk::error`):

- `NonRetryable(kind, message, details)` / `Retryable(kind, message, details)` map to
  `failure::non-retryable` / `failure::retryable` (``details`` is any JSON value and is sent
  as an ``application/json`` payload);
- `Timeout` and `Cancelled` are the reserved kinds ``timeout`` / ``cancelled`` and map to
  the payload-less `failure::timeout` / `failure::cancelled`;
- `ProtocolError` (and its subclass `Deadlock`) is non-retryable ``sdk_protocol``: the
  workflow broke an SDK rule (a durable call inside a step, awaiting a non-Ostro future);
- `InvalidString` is non-retryable ``invalid_string``: a string with a lone surrogate
  (PF-W4) was about to cross the host boundary.

Every class here derives from `Exception`, never from `asyncio.CancelledError`, so
`asyncio.gather` and task machinery treat them as ordinary errors (D11).
"""

from __future__ import annotations

from typing import Any

from ._host import ErrorInfo, Failure

__all__ = [
    "CANCELLED",
    "INVALID_JSON",
    "INVALID_STRING",
    "PYTHON_EXCEPTION",
    "SDK_PROTOCOL",
    "TIMEOUT",
    "Cancelled",
    "Deadlock",
    "InvalidString",
    "NonRetryable",
    "OstroError",
    "ProtocolError",
    "Retryable",
    "Timeout",
    "exception_message",
    "from_failure",
    "to_failure",
]

TIMEOUT = "timeout"
CANCELLED = "cancelled"
SDK_PROTOCOL = "sdk_protocol"
INVALID_STRING = "invalid_string"
INVALID_JSON = "invalid_json"
#: Kind of an uncaught Python exception (D-P9-20, W5).
PYTHON_EXCEPTION = "python_exception"

_NO_DETAILS = object()


class OstroError(Exception):
    """Base class of every SDK error that maps to a WIT `failure`."""

    retryable: bool = False

    def __init__(self, kind: str, message: str = "", details: Any = _NO_DETAILS):
        super().__init__(f"{kind}: {message}" if message else kind)
        self.kind = kind
        self.message = message
        self.details = None if details is _NO_DETAILS else details
        self._has_details = details is not _NO_DETAILS

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.kind!r}, {self.message!r})"


class NonRetryable(OstroError):
    """A failure the host must not retry (`failure::non-retryable`)."""

    retryable = False


class Retryable(OstroError):
    """A failure the host may retry under the step's retry policy (`failure::retryable`)."""

    retryable = True


class Timeout(NonRetryable):
    """The reserved `failure::timeout` (a step, signal, token or child timed out)."""

    def __init__(self, message: str = "timed out"):
        super().__init__(TIMEOUT, message)


class Cancelled(NonRetryable):
    """The reserved `failure::cancelled`: the run (or a cancelled op) was cancelled.

    A plain `Exception` (not `asyncio.CancelledError`): unshielded op futures fail with it
    when the run's cancellation completes (D11), and `cancel-op` completes an op with it (D5).
    """

    def __init__(self, message: str = "cancelled"):
        super().__init__(CANCELLED, message)


class ProtocolError(NonRetryable):
    """Non-retryable ``sdk_protocol``: the workflow broke an SDK rule."""

    def __init__(self, message: str):
        super().__init__(SDK_PROTOCOL, message)


class Deadlock(ProtocolError):
    """The workflow awaits something no Ostro operation can complete."""


class InvalidString(NonRetryable):
    """Non-retryable ``invalid_string``: a lone surrogate would cross the host boundary."""

    def __init__(self, message: str):
        super().__init__(INVALID_STRING, message)


def exception_message(exc: BaseException) -> str:
    """``"Type: message"`` for an uncaught exception (the ``python_exception`` message)."""
    text = str(exc)
    name = type(exc).__name__
    return f"{name}: {text}" if text else name


def to_failure(exc: BaseException, *, retryable_default: bool = False) -> Failure:
    """Convert an exception to a WIT failure tuple.

    `OstroError`s map by kind; any other exception becomes ``python_exception`` with the
    message ``"Type: message"`` (retryable when `retryable_default`, which is how step bodies
    report unexpected errors; workflow bodies report them non-retryable).
    """
    # Local import: json imports errors.
    from .json import encode_payload

    if isinstance(exc, OstroError):
        if not exc.retryable and exc.kind == TIMEOUT:
            return ("timeout", None)
        if not exc.retryable and exc.kind == CANCELLED:
            return ("cancelled", None)
        details = None
        if exc._has_details:
            details = encode_payload(exc.details)
        info = ErrorInfo(kind=exc.kind, message=exc.message, details=details)
        return ("retryable" if exc.retryable else "non-retryable", info)
    info = ErrorInfo(kind=PYTHON_EXCEPTION, message=exception_message(exc))
    return ("retryable" if retryable_default else "non-retryable", info)


def from_failure(failure: Failure) -> OstroError:
    """Convert a WIT failure tuple to the matching exception (not raised)."""
    from .json import decode_payload

    case, info = failure
    if case == "timeout":
        return Timeout()
    if case == "cancelled":
        return Cancelled()
    if not isinstance(info, ErrorInfo):
        return ProtocolError(f"malformed failure from the host: {failure!r}")
    details: Any = _NO_DETAILS
    if info.details is not None:
        try:
            details = decode_payload(info.details)
        except OstroError:
            details = info.details
    if case == "retryable":
        return Retryable(info.kind, info.message, details)
    if info.kind == SDK_PROTOCOL:
        err: OstroError = ProtocolError(info.message)
    elif info.kind == INVALID_STRING:
        err = InvalidString(info.message)
    elif info.kind == TIMEOUT:
        # A non-retryable error-info with the reserved kind (other SDKs never send it).
        err = Timeout(info.message)
    elif info.kind == CANCELLED:
        err = Cancelled(info.message)
    else:
        return NonRetryable(info.kind, info.message, details)
    if details is not _NO_DETAILS:
        err.details = details
        err._has_details = True
    return err
