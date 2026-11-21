// Raw-binding TypeScript smoke fixture (plan 09-02): no SDK, jco 1.42.0 bindings only.
//
// Workflow `hello` v1: step `greet` records {"hello": <input>} and the run completes with
// that payload. Built by sdks/scripts/build-guest.sh in `ts` (QuickJS, sync export) and
// `ts-sm` (StarlingMonkey) modes, so everything here is synchronous and avoids
// TextEncoder/TextDecoder (UTF-8 is coded by hand).
import { beginStep } from "ostro:runtime/journal@0.1.0";

type Payload = { contentType: string; data: Uint8Array };

const JSON_CT = "application/json";

function utf8Encode(s: string): Uint8Array {
  const out: number[] = [];
  for (const ch of s) {
    const c = ch.codePointAt(0) as number;
    if (c < 0x80) out.push(c);
    else if (c < 0x800) out.push(0xc0 | (c >> 6), 0x80 | (c & 63));
    else if (c < 0x10000) out.push(0xe0 | (c >> 12), 0x80 | ((c >> 6) & 63), 0x80 | (c & 63));
    else out.push(0xf0 | (c >> 18), 0x80 | ((c >> 12) & 63), 0x80 | ((c >> 6) & 63), 0x80 | (c & 63));
  }
  return new Uint8Array(out);
}

function utf8Decode(b: Uint8Array): string {
  let s = "";
  for (let i = 0; i < b.length; ) {
    const x = b[i];
    let c: number;
    if (x < 0x80) { c = x; i += 1; }
    else if (x < 0xe0) { c = ((x & 31) << 6) | (b[i + 1] & 63); i += 2; }
    else if (x < 0xf0) { c = ((x & 15) << 12) | ((b[i + 1] & 63) << 6) | (b[i + 2] & 63); i += 3; }
    else { c = ((x & 7) << 18) | ((b[i + 1] & 63) << 12) | ((b[i + 2] & 63) << 6) | (b[i + 3] & 63); i += 4; }
    s += String.fromCodePoint(c);
  }
  return s;
}

function decode(p: Payload): unknown {
  return p.data.length === 0 ? null : JSON.parse(utf8Decode(p.data));
}

function encode(v: unknown): Payload {
  return { contentType: JSON_CT, data: utf8Encode(JSON.stringify(v)) };
}

export const workflows = {
  describe() {
    return [{ name: "hello", version: 1, defaultQueue: undefined, runTimeoutMs: undefined }];
  },
  run(name: string, input: Payload) {
    if (name !== "hello") {
      throw { tag: "non-retryable", val: { kind: "unknown_workflow", message: name, details: undefined } };
    }
    const start = beginStep("greet", { retry: undefined, timeoutMs: undefined, fuel: undefined });
    if (start.tag === "replayed") {
      const outcome = start.val;
      if (outcome.tag === "err") throw outcome.val;
      return { tag: "completed", val: outcome.val };
    }
    const out = encode({ hello: decode(input) });
    start.val.complete({ tag: "ok", val: out });
    return { tag: "completed", val: out };
  },
};
