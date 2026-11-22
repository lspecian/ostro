"""Cross-SDK vectors (09-06, `conformance/vectors/`): idempotency key (P-11), random_f64
and JSON. The files must exist; a missing file is a failure, not a skip."""

from __future__ import annotations

import json
import struct
from pathlib import Path
from typing import Any

from ostro import f64_from_u64, idempotency_key_for
from ostro.json import dumps, loads

VECTORS = Path(__file__).resolve().parents[3] / "conformance" / "vectors"


def _cases(name: str) -> list[dict[str, Any]]:
    doc = json.loads((VECTORS / name).read_text(encoding="utf-8"))
    assert doc["version"] == 1
    assert doc["cases"], f"{name} has no cases"
    return doc["cases"]


def test_idempotency_vectors() -> None:
    cases = _cases("idempotency.json")
    for case in cases:
        got = idempotency_key_for(case["input"]["run_id"], case["input"]["name"])
        assert got == case["expected"], case
        assert len(got) == 38
    assert len(cases) >= 12


def test_random_f64_vectors() -> None:
    for case in _cases("random_f64.json"):
        u = int(case["input"]["u64"])
        assert u == int(case["input"]["u64_hex"], 16)
        f = f64_from_u64(u)
        bits = struct.pack(">d", f).hex()
        assert bits == case["expected"]["f64_bits_hex"], case


def test_json_vectors() -> None:
    n = 0
    for case in _cases("json.json"):
        inp, exp = case["input"], case["expected"]
        if inp["op"] == "encode":
            assert dumps(inp["value"]).hex() == exp["hex"], case
        elif inp["op"] == "encode_f64":
            (f,) = struct.unpack(">d", bytes.fromhex(inp["f64_bits_hex"]))
            assert dumps(f).hex() == exp["hex"], case
        elif inp["op"] == "decode":
            assert loads(bytes.fromhex(inp["hex"])) == exp["value"], case
        else:
            raise AssertionError(f"unknown op {inp['op']!r}")
        n += 1
    assert n >= 26
