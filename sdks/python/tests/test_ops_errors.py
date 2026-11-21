"""Errors, the failure mapping, op decoding and the JSON codec (with the 09-06 vectors)."""

from __future__ import annotations

import asyncio
import json as stdjson
import math
import struct
from pathlib import Path

import pytest

from ostro import errors
from ostro import json as ojson
from ostro._host import ErrorInfo, HttpResponse
from ostro.ops import ChildRun, decode_outcome

VECTORS = Path(__file__).resolve().parents[3] / "conformance" / "vectors"


# --- errors -------------------------------------------------------------------------


def test_error_kinds() -> None:
    assert errors.NonRetryable("k", "m").kind == "k"
    assert errors.Retryable("k", "m").retryable is True
    assert errors.NonRetryable("k", "m").retryable is False
    assert errors.Timeout().kind == "timeout"
    assert errors.Cancelled().kind == "cancelled"
    assert errors.ProtocolError("x").kind == "sdk_protocol"
    assert errors.Deadlock("x").kind == "sdk_protocol"
    assert errors.InvalidString("x").kind == "invalid_string"
    for cls in (errors.Cancelled, errors.Timeout, errors.Deadlock, errors.InvalidString):
        assert issubclass(cls, Exception)
        assert not issubclass(cls, asyncio.CancelledError)


@pytest.mark.parametrize(
    "failure",
    [
        ("retryable", ErrorInfo("flaky", "try again")),
        ("non-retryable", ErrorInfo("bad_input", "no")),
        ("non-retryable", ErrorInfo("bad_input", "no", ("application/json", b'{"field":"x"}'))),
        ("retryable", ErrorInfo("flaky", "", ("application/json", b"[1,2]"))),
        ("non-retryable", ErrorInfo("sdk_protocol", "rule")),
        ("non-retryable", ErrorInfo("invalid_string", "surrogate")),
        ("timeout", None),
        ("cancelled", None),
    ],
)
def test_failure_round_trip(failure) -> None:
    exc = errors.from_failure(failure)
    assert errors.to_failure(exc) == failure


def test_from_failure_classes() -> None:
    assert isinstance(errors.from_failure(("timeout", None)), errors.Timeout)
    assert isinstance(errors.from_failure(("cancelled", None)), errors.Cancelled)
    assert isinstance(errors.from_failure(("non-retryable", ErrorInfo("sdk_protocol", "m"))), errors.ProtocolError)
    assert isinstance(errors.from_failure(("non-retryable", ErrorInfo("invalid_string", "m"))), errors.InvalidString)
    e = errors.from_failure(("non-retryable", ErrorInfo("k", "m", ("application/json", b'{"a":1}'))))
    assert type(e) is errors.NonRetryable and e.details == {"a": 1}
    assert type(errors.from_failure(("retryable", ErrorInfo("k", "m")))) is errors.Retryable


def test_details_are_json_payloads() -> None:
    f = errors.to_failure(errors.NonRetryable("k", "m", {"b": [1, None]}))
    assert f == ("non-retryable", ErrorInfo("k", "m", ("application/json", b'{"b":[1,null]}')))
    assert errors.to_failure(errors.NonRetryable("k", "m", None))[1].details == ("application/json", b"null")
    assert errors.to_failure(errors.NonRetryable("k", "m"))[1].details is None


def test_unexpected_exception_is_python_exception() -> None:
    assert errors.to_failure(ValueError("bad value")) == (
        "non-retryable",
        ErrorInfo("python_exception", "ValueError: bad value"),
    )
    assert errors.to_failure(KeyError("k"), retryable_default=True) == (
        "retryable",
        ErrorInfo("python_exception", "KeyError: 'k'"),
    )
    assert errors.exception_message(RuntimeError()) == "RuntimeError"


# --- op decoding --------------------------------------------------------------------


def test_decode_outcome_per_kind() -> None:
    p = ("application/json", b'{"x":1}')
    assert decode_outcome("timer", ("ok", ("timer", None))) is None
    assert decode_outcome("signal", ("ok", ("signal", p))) == {"x": 1}
    assert decode_outcome("token", ("ok", ("token", ("application/json", b"")))) is None
    assert decode_outcome("child", ("ok", ("child", p))) == {"x": 1}
    detached = ("ok", ("child", ("application/json", b'{"run_id":"r1"}')))
    assert decode_outcome("child", detached, detached=True) == ChildRun("r1")
    resp = HttpResponse(200, [], b"hi")
    assert decode_outcome("http", ("ok", ("http", resp))) is resp
    assert decode_outcome("object", ("ok", ("object", b"raw"))) == b"raw"
    with pytest.raises(errors.Cancelled):
        decode_outcome("timer", ("err", ("cancelled", None)))
    with pytest.raises(errors.NonRetryable) as e:
        decode_outcome("child", ("err", ("non-retryable", ErrorInfo("child_terminated", "gone"))))
    assert e.value.kind == "child_terminated"


# --- JSON ---------------------------------------------------------------------------


def test_json_rules() -> None:
    assert ojson.dumps({"a": 1, "b": [1, 2]}) == b'{"a":1,"b":[1,2]}'
    assert ojson.dumps("über") == '"über"'.encode()
    assert ojson.dumps([math.nan, math.inf, -math.inf, 1.5]) == b"[null,null,null,1.5]"
    assert ojson.dumps((1, 2)) == b"[1,2]"
    assert ojson.decode_payload(("application/json", b"")) is None
    assert ojson.decode_payload(("text/plain", b"")) is None
    assert ojson.decode_payload(("text/plain", b"raw")) == b"raw"
    assert ojson.encode_payload({"k": "v"}) == ("application/json", b'{"k":"v"}')


def test_json_rejects_lone_surrogates() -> None:
    with pytest.raises(errors.InvalidString):
        ojson.dumps({"k": "a\ud800"})
    with pytest.raises(errors.InvalidString):
        ojson.dumps({"\udfff": 1})
    with pytest.raises(errors.InvalidString):
        ojson.loads(b'"\\ud800"')
    assert ojson.loads(b'"\\ud83d\\ude00"') == "\U0001f600"
    with pytest.raises(errors.InvalidString):
        ojson.check_str("x\ud800")


def test_json_invalid_input() -> None:
    with pytest.raises(errors.NonRetryable) as e:
        ojson.loads(b"{nope")
    assert e.value.kind == "invalid_json"
    with pytest.raises(errors.NonRetryable) as e:
        ojson.dumps(object())
    assert e.value.kind == "invalid_json"


def _vectors(name: str) -> list[dict]:
    path = VECTORS / name
    if not path.exists():
        pytest.skip(f"conformance/vectors/{name} is committed by 09-06 and is not present yet")
    doc = stdjson.loads(path.read_text(encoding="utf-8"))
    assert doc["version"] == 1
    assert doc["cases"]
    return doc["cases"]


def test_json_vectors() -> None:
    for case in _vectors("json.json"):
        inp, exp = case["input"], case["expected"]
        if inp["op"] == "encode":
            assert ojson.dumps(inp["value"]).hex() == exp["hex"], inp
        elif inp["op"] == "encode_f64":
            (f,) = struct.unpack(">d", bytes.fromhex(inp["f64_bits_hex"]))
            assert ojson.dumps(f).hex() == exp["hex"], inp
        elif inp["op"] == "decode":
            assert ojson.loads(bytes.fromhex(inp["hex"])) == exp["value"], inp
        else:
            raise AssertionError(f"unknown op {inp['op']}")

