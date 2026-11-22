// Cross-SDK vectors (09-06, conformance/vectors/): idempotency key (P-11), random_f64 and
// JSON. The files must exist; a missing file fails (no skip).

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { f64FromU64 } from "../src/ctx.ts";
import { idempotencyKeyFor } from "../src/effects.ts";
import { decodeJson, encodeJson } from "../src/json.ts";

const vectorsDir = fileURLToPath(new URL("../../../conformance/vectors/", import.meta.url));

interface Case {
  input: Record<string, unknown>;
  expected: unknown;
}

function cases(name: string): Case[] {
  const doc = JSON.parse(readFileSync(`${vectorsDir}${name}`, "utf8")) as { version: number; cases: Case[] };
  assert.equal(doc.version, 1);
  assert.ok(doc.cases.length > 0, `${name} has no cases`);
  return doc.cases;
}

const hex = (b: Uint8Array): string => Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
const unhex = (s: string): Uint8Array => Uint8Array.from(s.match(/../g) ?? [], (h) => parseInt(h, 16));

function f64Bits(f: number): string {
  const v = new DataView(new ArrayBuffer(8));
  v.setFloat64(0, f);
  return hex(new Uint8Array(v.buffer));
}

test("vectors: idempotency.json", () => {
  const cs = cases("idempotency.json");
  assert.ok(cs.length >= 12);
  for (const c of cs) {
    const got = idempotencyKeyFor(c.input.run_id as string, c.input.name as string);
    assert.equal(got, c.expected, JSON.stringify(c.input));
    assert.equal(got.length, 38);
  }
});

test("vectors: random_f64.json", () => {
  for (const c of cases("random_f64.json")) {
    const u = BigInt(c.input.u64 as string);
    assert.equal(u, BigInt(`0x${c.input.u64_hex as string}`));
    assert.equal(f64Bits(f64FromU64(u)), (c.expected as { f64_bits_hex: string }).f64_bits_hex, String(u));
  }
});

test("vectors: json.json", () => {
  let n = 0;
  for (const c of cases("json.json")) {
    const exp = c.expected as { hex?: string; value?: unknown };
    switch (c.input.op) {
      case "encode":
        assert.equal(hex(encodeJson(c.input.value)), exp.hex, JSON.stringify(c.input));
        break;
      case "encode_f64": {
        const v = new DataView(unhex(c.input.f64_bits_hex as string).buffer);
        assert.equal(hex(encodeJson(v.getFloat64(0))), exp.hex, JSON.stringify(c.input));
        break;
      }
      case "decode":
        assert.deepEqual(decodeJson(unhex(c.input.hex as string)), exp.value, JSON.stringify(c.input));
        break;
      default:
        assert.fail(`unknown op ${String(c.input.op)}`);
    }
    n++;
  }
  assert.ok(n >= 26);
});
