# Cross-SDK vectors

Shared test vectors for the Rust, Python and TypeScript guest SDKs (plan 09-06). Every SDK
loads these files in its own test suite and must match them exactly. The Rust reference
SDK checks them in `sdks/rust/ostro-sdk/tests/vectors.rs`.

The expected values come from an independent generator that uses only the Python standard
library (below), not from any SDK.

| File | Contract | SDK function (Rust) |
|------|----------|---------------------|
| `idempotency.json` | P-11 semantic idempotency key: `"ostro-" + hex(sha256("ostro/idem/v1/name" ‖ utf8(run_id) ‖ 0x00 ‖ utf8(name)))[..32]`, always 38 chars | `ostro_sdk::idempotency_key_for(run_id, name)`; `ctx.idempotency_key(name)` |
| `random_f64.json` | `random_f64 = (random_u64 >> 11) * 2^-53`, in `[0, 1)`, exact | `ostro_sdk::f64_from_u64(u)`; `ctx.random_f64()` |
| `json.json` | Canonical `application/json` payload bytes: compact separators, non-ASCII kept as UTF-8, NaN/±inf → `null`, empty data decodes as `null`, no BOM | `ostro_sdk::codec::{encode_json, decode_json}` |

## Format

```json
{"version": 1, "description": "...", "cases": [{"input": {...}, "expected": ...}, ...]}
```

- `idempotency.json`: `input = {run_id, name}`, `expected` = the key string.
- `random_f64.json`: `input = {u64, u64_hex}` and `expected = {f64_bits_hex, f64}`. The u64
  and the f64 bit pattern are strings, because JS numbers cannot hold them. Compare
  `f64_bits_hex` bit for bit. `f64` is Python's shortest repr and is informative only.
- `json.json`: each case has `input.op` set to one of:
  - `encode`: serialise `input.value` and compare the bytes with `expected.hex`.
  - `encode_f64`: serialise the float `f64::from_bits(input.f64_bits_hex)` and compare the
    bytes with `expected.hex`.
  - `decode`: parse the bytes `input.hex` and compare with `expected.value`.

  `hex` is authoritative and `text` is informative. Objects in `encode` cases have their
  keys in sorted order, so an SDK that keeps insertion order and one that sorts keys
  produce the same bytes. Floats are limited to values that every runtime prints the same
  way: no `-0.0`, no exponents, no integers above 2^53.

## Regenerating

The generator below is the whole source of truth. Save it as `gen_vectors.py` outside the
repository, then run it from the repo root:

```sh
uv run --no-project --python 3.12 python -I /path/to/gen_vectors.py conformance/vectors
```

Run it again and check that `git diff` is empty. Adding cases is fine. Changing an
existing expected value is a breaking change for every SDK.

```python
"""Generate conformance/vectors/{idempotency,random_f64,json}.json (plan 09-06).

Independent of every SDK: only the Python standard library. Run from the repo root:
    uv run --no-project --python 3.12 python conformance/vectors/... (see README)
"""
import base64, hashlib, json, math, struct, sys

OUT = sys.argv[1] if len(sys.argv) > 1 else "conformance/vectors"
CROCKFORD = "0123456789abcdefghjkmnpqrstvwxyz"


def run_id(partition: int, uuid_hex: str) -> str:
    """Ostro run id string: Crockford base32 (lowercase, no padding) of partition u16 BE || uuid."""
    raw = struct.pack(">H", partition) + bytes.fromhex(uuid_hex)
    acc, bits, out = 0, 0, []
    for b in raw:
        acc = (acc << 8) | b
        bits += 8
        while bits >= 5:
            bits -= 5
            out.append(CROCKFORD[(acc >> bits) & 31])
    if bits:
        out.append(CROCKFORD[(acc << (5 - bits)) & 31])
    return "".join(out)


def idem(run: str, name: str) -> str:
    h = hashlib.sha256(b"ostro/idem/v1/name" + run.encode() + b"\x00" + name.encode()).hexdigest()
    return "ostro-" + h[:32]


def dump(name, doc):
    with open(f"{OUT}/{name}.json", "w", encoding="utf-8", newline="\n") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)
        f.write("\n")


# --- idempotency.json -------------------------------------------------------------------
r0 = run_id(0, "019a8f6e7c2b7d3e8f10a1b2c3d4e5f6")
r5 = run_id(5, "019a8f6e7c2b7d3e8f10a1b2c3d4e5f6")
r1023 = run_id(1023, "019a8f6e7c2b7d3e8f10a1b2c3d4e5f7")
r65535 = run_id(65535, "ffffffffffffffffffffffffffffffff")
long_name = "abcdefghijklmnop" * 64  # 1 KiB
pairs = [
    (r0, "charge"),
    (r5, "charge"),  # same uuid, other partition -> different key
    (r1023, "charge"),
    (r65535, "charge"),
    (r5, ""),
    (r5, "über/€"),
    (r5, "😀 emoji"),
    (r5, long_name),
    (r5, "charge-1"),
    (r5, "a\u0000b"),  # NUL inside the name is hashed as-is
    ("run-sdk-cancel-1", "charge"),
    ("", "charge"),
]
cases = []
for run, name in pairs:
    k = idem(run, name)
    assert len(k) == 38 and k.startswith("ostro-")
    cases.append({"input": {"run_id": run, "name": name}, "expected": k})
assert len({c["expected"] for c in cases}) == len(cases)
dump("idempotency", {
    "version": 1,
    "description": "P-11 semantic idempotency key: \"ostro-\" + hex(sha256(\"ostro/idem/v1/name\" || utf8(run_id) || 0x00 || utf8(name)))[..32]",
    "cases": cases,
})

# --- random_f64.json --------------------------------------------------------------------
U64_MAX = (1 << 64) - 1
inputs = [0, 1, 1 << 11, (1 << 11) - 1, ((1 << 53) - 1) << 11, U64_MAX, 1 << 63]
for i in range(8):
    d = hashlib.sha256(b"ostro/vectors/random_f64/" + str(i).encode()).digest()
    inputs.append(int.from_bytes(d[:8], "big"))
cases = []
for u in inputs:
    f = (u >> 11) * 2.0 ** -53  # exact: (u >> 11) < 2^53 and the scale is a power of two
    assert 0.0 <= f < 1.0
    bits = struct.unpack(">Q", struct.pack(">d", f))[0]
    cases.append({
        "input": {"u64": str(u), "u64_hex": f"{u:016x}"},
        "expected": {"f64_bits_hex": f"{bits:016x}", "f64": repr(f)},
    })
dump("random_f64", {
    "version": 1,
    "description": "random_f64 = (random_u64 >> 11) * 2^-53, in [0, 1). u64 inputs and f64 bit patterns are strings (JS numbers cannot hold them); compare f64 bit-exactly via f64_bits_hex.",
    "cases": cases,
})

# --- json.json --------------------------------------------------------------------------


def canon(v) -> bytes:
    """Canonical payload bytes: compact separators, UTF-8 (non-ASCII kept), no BOM."""
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def enc(value, note):
    b = canon(value)
    return {"input": {"op": "encode", "value": value, "note": note},
            "expected": {"hex": b.hex(), "text": b.decode("utf-8")}}


def enc_f64(bits_hex, note):
    f = struct.unpack(">d", bytes.fromhex(bits_hex))[0]
    b = b"null" if not math.isfinite(f) else canon(f)
    return {"input": {"op": "encode_f64", "f64_bits_hex": bits_hex, "note": note},
            "expected": {"hex": b.hex(), "text": b.decode("utf-8")}}


def dec(data: bytes, note):
    v = None if data == b"" else json.loads(data.decode("utf-8"))
    return {"input": {"op": "decode", "hex": data.hex(), "note": note}, "expected": {"value": v}}


cases = [
    enc(None, "null"),
    enc(True, "bool"),
    enc(0, "zero"),
    enc(-1, "negative int"),
    enc(9007199254740991, "2^53-1 (largest JS-safe int)"),
    enc(1.5, "float"),
    enc(0.1, "shortest round-trip float"),
    enc("", "empty string"),
    enc("über/€ 😀", "non-ASCII kept as UTF-8, '/' not escaped"),
    enc("line\nbreak\t\"q\"\\", "short escapes"),
    enc("\u0001\u001f", "control chars as lowercase \\u00XX"),
    enc("\u007f ", "DEL and U+2028 kept raw"),
    enc([], "empty array"),
    enc({}, "empty object"),
    enc([1, "a", None, [True]], "nested array"),
    enc({"a": 1, "b": [1, 2], "c": {"d": "e"}}, "object (keys already sorted; no whitespace)"),
    enc({"echo": {"n": 1}}, "child output shape"),
    enc_f64("7ff8000000000000", "NaN -> null"),
    enc_f64("7ff0000000000000", "+inf -> null"),
    enc_f64("fff0000000000000", "-inf -> null"),
    enc_f64("3ff8000000000000", "1.5 stays a number"),
    dec(b"", "empty data decodes as null"),
    dec(b"null", "null"),
    dec(b'{ "a" : 1 ,\n "b": [ true ] }', "whitespace is accepted on decode"),
    dec("\"über\"".encode(), "UTF-8 string"),
    dec(b'"\\u00fc\\ud83d\\ude00"', "\\u escapes incl. a surrogate pair"),
]
dump("json", {
    "version": 1,
    "description": "Canonical JSON payload encoding (application/json): compact separators, non-ASCII kept as UTF-8, NaN/+-inf serialised as null, empty data decodes as null, no BOM. 'hex' is authoritative; 'text' is informative.",
    "cases": cases,
})
print("ok")
```
