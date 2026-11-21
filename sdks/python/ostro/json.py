"""The SDK's JSON codec for ``application/json`` payloads (PF-W5, D-P9-15).

Rules (pinned by ``conformance/vectors/json.json``, shared with Rust and TS):

- compact separators, non-ASCII kept as UTF-8 (``ensure_ascii=False``), no BOM;
- NaN and +/-inf are serialised as ``null`` (like serde_json and ``JSON.stringify``);
- an empty payload decodes as ``None`` (JSON ``null``);
- tuples encode as arrays; dict keys must be strings (ints/floats/bools/None are
  converted by the stdlib rules);
- strings with lone surrogates (PF-W4) are rejected with `InvalidString`, on both encode
  and decode, before anything reaches the host.
"""

from __future__ import annotations

import json as _json
import math
from typing import Any

from ._host import Payload
from .errors import INVALID_JSON, InvalidString, NonRetryable

__all__ = [
    "JSON",
    "check_str",
    "decode_payload",
    "dumps",
    "encode_payload",
    "loads",
]

#: Content type of every SDK-encoded payload.
JSON = "application/json"

_ENCODER = _json.JSONEncoder(
    ensure_ascii=False,
    separators=(",", ":"),
    allow_nan=False,
    check_circular=True,
)


def check_str(value: str, what: str = "string") -> str:
    """Return `value` unchanged, or raise `InvalidString` if it has a lone surrogate."""
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise InvalidString(f"{what} contains a lone surrogate (not valid UTF-8)") from None
    return value


def _sanitize(value: Any) -> Any:
    """Copy `value` with non-finite floats replaced by ``None`` (only where needed)."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: _sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize(v) for v in value]
    return value


def dumps(value: Any) -> bytes:
    """Encode `value` as canonical SDK JSON bytes."""
    try:
        text = _ENCODER.encode(_sanitize(value))
    except (TypeError, ValueError) as e:
        raise NonRetryable(INVALID_JSON, f"cannot encode as JSON: {e}") from None
    try:
        return text.encode("utf-8")
    except UnicodeEncodeError:
        raise InvalidString("JSON value contains a lone surrogate (not valid UTF-8)") from None


def _has_lone_surrogate(value: Any) -> bool:
    if isinstance(value, str):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            return True
        return False
    if isinstance(value, dict):
        return any(_has_lone_surrogate(k) or _has_lone_surrogate(v) for k, v in value.items())
    if isinstance(value, list):
        return any(_has_lone_surrogate(v) for v in value)
    return False


def loads(data: bytes | bytearray | memoryview | str) -> Any:
    """Decode SDK JSON; empty input is ``None``."""
    if isinstance(data, str):
        text = data
    else:
        if len(data) == 0:
            return None
        try:
            text = bytes(data).decode("utf-8")
        except UnicodeDecodeError as e:
            raise NonRetryable(INVALID_JSON, f"payload is not UTF-8: {e}") from None
    if text == "":
        return None
    if text.startswith("﻿"):
        raise NonRetryable(INVALID_JSON, "payload starts with a BOM")
    try:
        value = _json.loads(text)
    except ValueError as e:
        raise NonRetryable(INVALID_JSON, f"invalid JSON payload: {e}") from None
    # `\ud800`-style escapes can produce lone surrogates; only scan when one is possible.
    if ("\\ud" in text or "\\uD" in text) and _has_lone_surrogate(value):
        raise InvalidString("JSON payload contains a lone surrogate escape")
    return value


def encode_payload(value: Any) -> Payload:
    """Encode `value` as an ``application/json`` payload."""
    return (JSON, dumps(value))


def decode_payload(payload: Payload) -> Any:
    """Decode a payload: JSON (or empty data) to a Python value, other content types raw.

    Like the Rust SDK, an empty body is JSON ``null`` whatever the content type; a payload
    with a non-JSON content type is returned as ``bytes``.
    """
    content_type, data = payload
    if len(data) == 0:
        return None
    if content_type and content_type.split(";", 1)[0].strip().lower() != JSON:
        return bytes(data)
    return loads(data)
