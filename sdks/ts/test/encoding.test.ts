// UTF-8 codec, lone-surrogate detection, sync sha256, JSON payload codec and the
// cross-SDK JSON vectors (conformance/vectors/json.json, 09-06).

import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { INVALID_JSON, INVALID_STRING, InvalidStringError, NonRetryableError } from "../src/errors.ts";
import { decodeJson, encodeJson, fromPayload, stringifyJson, toPayload } from "../src/json.ts";
import { sha256, sha256Hex, toHex } from "../src/sha256.ts";
import { checkString, decodeUtf8, encodeUtf8, isWellFormed, scanWellFormed } from "../src/utf8.ts";

const vectorsDir = fileURLToPath(new URL("../../../conformance/vectors/", import.meta.url));

function hex(b: Uint8Array): string {
  return Buffer.from(b).toString("hex");
}

function fromHex(h: string): Uint8Array {
  return new Uint8Array(Buffer.from(h, "hex"));
}

test("utf8 round-trips 1- to 4-byte code points and matches TextEncoder", () => {
  const samples = ["", "ascii", "über", "€ uro", "😀 emoji", "\u0000\u007f\u0080߿ࠀ￿", "\u{10000}\u{10ffff}", "a b"];
  for (const s of samples) {
    const bytes = encodeUtf8(s);
    assert.deepEqual(bytes, new TextEncoder().encode(s), s);
    assert.equal(decodeUtf8(bytes), s);
  }
  const big = "x😀ü".repeat(20000);
  assert.equal(decodeUtf8(encodeUtf8(big)), big);
});

test("utf8 decoding replaces malformed sequences like TextDecoder", () => {
  const cases = [
    [0xff],
    [0xc0, 0x80],
    [0xed, 0xa0, 0x80],
    [0xe2, 0x82],
    [0xf4, 0x90, 0x80, 0x80],
    [0x61, 0x80, 0x62],
  ];
  for (const c of cases) {
    const bytes = new Uint8Array(c);
    const ours = decodeUtf8(bytes);
    assert.ok(ours.includes("�"), `${hex(bytes)} -> ${JSON.stringify(ours)}`);
    assert.ok(isWellFormed(ours));
  }
  assert.equal(decodeUtf8(new Uint8Array([0x61, 0x80, 0x62])), new TextDecoder().decode(new Uint8Array([0x61, 0x80, 0x62])));
});

test("lone surrogates are detected (isWellFormed and the manual scan agree)", () => {
  const cases: [string, boolean][] = [
    ["plain", true],
    ["pair 😀", true],
    ["lead \uD800", false],
    ["\uD800 lead first", false],
    ["trail \uDC00", false],
    ["\uDC00\uD800", false],
    ["😀\uD800", false],
  ];
  for (const [s, ok] of cases) {
    assert.equal(scanWellFormed(s), ok, JSON.stringify(s));
    assert.equal(isWellFormed(s), ok, JSON.stringify(s));
  }
  assert.throws(() => encodeUtf8("bad\uD800"), (e: unknown) => e instanceof InvalidStringError && e.kind === INVALID_STRING);
  assert.throws(() => checkString("x\uDFFF", "step name"), /step name contains a lone surrogate/);
  assert.equal(checkString("fine", "name"), "fine");
});

test("sha256 matches NIST vectors and node:crypto", () => {
  assert.equal(sha256Hex(new Uint8Array()), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  assert.equal(sha256Hex(encodeUtf8("abc")), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  assert.equal(
    sha256Hex(encodeUtf8("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")),
    "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
  );
  const mib = new Uint8Array(1024 * 1024).fill(0x61);
  const ours = sha256Hex(mib);
  assert.equal(ours, createHash("sha256").update(mib).digest("hex"));
  for (const len of [55, 56, 63, 64, 65, 119, 120, 1000]) {
    const data = Uint8Array.from({ length: len }, (_, i) => (i * 7 + 3) & 0xff);
    assert.equal(toHex(sha256(data)), createHash("sha256").update(data).digest("hex"), `len ${len}`);
  }
});

test("json: NaN/Infinity/undefined encode as null, empty data decodes as null", () => {
  assert.equal(stringifyJson(Number.NaN), "null");
  assert.equal(stringifyJson(Number.POSITIVE_INFINITY), "null");
  assert.equal(stringifyJson(undefined), "null");
  assert.equal(stringifyJson({ a: undefined, b: [undefined, Number.NaN] }), '{"b":[null,null]}');
  assert.equal(decodeJson(new Uint8Array()), null);
  assert.deepEqual(fromPayload(toPayload({ x: "ü" })), { x: "ü" });
  assert.equal(toPayload(1).contentType, "application/json");
});

test("json: unencodable values are invalid_json, lone surrogates invalid_string", () => {
  assert.throws(() => encodeJson({ n: BigInt(1) }), (e: unknown) => e instanceof NonRetryableError && e.kind === INVALID_JSON);
  const cyc: Record<string, unknown> = {};
  cyc.self = cyc;
  assert.throws(() => encodeJson(cyc), (e: unknown) => e instanceof NonRetryableError && e.kind === INVALID_JSON);
  assert.throws(() => encodeJson({ s: "bad\uD800" }), (e: unknown) => e instanceof InvalidStringError);
  assert.throws(() => encodeJson(["\uDC00"]), (e: unknown) => e instanceof InvalidStringError);
  // A literal backslash followed by "ud800" is fine (the backslash is escaped).
  assert.equal(stringifyJson("\\ud800"), '"\\\\ud800"');
  assert.throws(() => decodeJson(encodeUtf8("{nope")), (e: unknown) => e instanceof NonRetryableError && e.kind === INVALID_JSON);
});

interface JsonCase {
  input: { op: "encode" | "encode_f64" | "decode"; value?: unknown; f64_bits_hex?: string; hex?: string; note?: string };
  expected: { hex?: string; value?: unknown };
}

test("json: conformance/vectors/json.json (cross-SDK vectors, 09-06)", (t) => {
  const file = `${vectorsDir}json.json`;
  if (!existsSync(file)) {
    t.skip("conformance/vectors/json.json is written by 09-06 and is not present");
    return;
  }
  const doc = JSON.parse(readFileSync(file, "utf8")) as { version: number; cases: JsonCase[] };
  assert.equal(doc.version, 1);
  assert.ok(doc.cases.length >= 20);
  let encode = 0;
  let decode = 0;
  for (const c of doc.cases) {
    const what = c.input.note ?? c.input.op;
    if (c.input.op === "encode") {
      assert.equal(hex(encodeJson(c.input.value)), c.expected.hex, what);
      encode += 1;
    } else if (c.input.op === "encode_f64") {
      const view = new DataView(fromHex(c.input.f64_bits_hex as string).buffer);
      assert.equal(hex(encodeJson(view.getFloat64(0, false))), c.expected.hex, what);
      encode += 1;
    } else {
      assert.deepEqual(decodeJson(fromHex(c.input.hex as string)), c.expected.value, what);
      decode += 1;
    }
  }
  assert.ok(encode > 0 && decode > 0);
});
