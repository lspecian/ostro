// UTF-8 codec and well-formedness checks. QuickJS has no TextEncoder/TextDecoder
// (measured in S9), so the SDK ships its own.
//
// WIT strings are Unicode scalar values (PF-W4): QuickJS traps on a lone surrogate and
// StarlingMonkey silently replaces it, so every string is checked before it crosses the
// boundary and a lone surrogate raises a non-retryable `invalid_string`.

import { InvalidStringError } from "./errors.ts";

type WellFormed = { isWellFormed?: () => boolean };

/** True if `s` has no lone surrogate. Uses `String.prototype.isWellFormed` when present. */
export function isWellFormed(s: string): boolean {
  const native = (String.prototype as WellFormed).isWellFormed;
  if (typeof native === "function") return native.call(s);
  return scanWellFormed(s);
}

/** The manual surrogate scan used when `isWellFormed` is missing (exported for tests). */
export function scanWellFormed(s: string): boolean {
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c < 0xd800 || c > 0xdfff) continue;
    if (c >= 0xdc00) return false;
    const d = i + 1 < s.length ? s.charCodeAt(i + 1) : 0;
    if (d < 0xdc00 || d > 0xdfff) return false;
    i++;
  }
  return true;
}

/** Throw `InvalidStringError` if `s` is not a well-formed string; `what` names it. */
export function checkString(s: string, what: string): string {
  if (typeof s !== "string") throw new TypeError(`${what} must be a string`);
  if (!isWellFormed(s)) throw new InvalidStringError(`${what} contains a lone surrogate (strings must be Unicode scalar values)`);
  return s;
}

/** Encode `s` as UTF-8. A lone surrogate raises `InvalidStringError`. */
export function encodeUtf8(s: string): Uint8Array {
  const out = new Uint8Array(s.length * 3);
  let n = 0;
  for (let i = 0; i < s.length; i++) {
    let c = s.charCodeAt(i);
    if (c >= 0xd800 && c <= 0xdfff) {
      const d = i + 1 < s.length ? s.charCodeAt(i + 1) : 0;
      if (c >= 0xdc00 || d < 0xdc00 || d > 0xdfff) {
        throw new InvalidStringError("string contains a lone surrogate (strings must be Unicode scalar values)");
      }
      c = 0x10000 + ((c - 0xd800) << 10) + (d - 0xdc00);
      i++;
    }
    if (c < 0x80) {
      out[n++] = c;
    } else if (c < 0x800) {
      out[n++] = 0xc0 | (c >> 6);
      out[n++] = 0x80 | (c & 63);
    } else if (c < 0x10000) {
      out[n++] = 0xe0 | (c >> 12);
      out[n++] = 0x80 | ((c >> 6) & 63);
      out[n++] = 0x80 | (c & 63);
    } else {
      out[n++] = 0xf0 | (c >> 18);
      out[n++] = 0x80 | ((c >> 12) & 63);
      out[n++] = 0x80 | ((c >> 6) & 63);
      out[n++] = 0x80 | (c & 63);
    }
  }
  return out.slice(0, n);
}

const CHUNK = 4096;

/**
 * Decode UTF-8. Malformed sequences (bad continuation bytes, overlong forms, surrogate
 * code points, values above U+10FFFF, truncation) decode as U+FFFD, like TextDecoder.
 */
export function decodeUtf8(b: Uint8Array): string {
  const parts: string[] = [];
  const units: number[] = [];
  const flush = (): void => {
    parts.push(String.fromCharCode.apply(null, units));
    units.length = 0;
  };
  let i = 0;
  while (i < b.length) {
    const x = b[i];
    let cp = 0xfffd;
    let len = 1;
    if (x < 0x80) {
      cp = x;
    } else if (x >= 0xc2 && x < 0xe0) {
      if (i + 1 < b.length && (b[i + 1] & 0xc0) === 0x80) {
        cp = ((x & 31) << 6) | (b[i + 1] & 63);
        len = 2;
      }
    } else if (x >= 0xe0 && x < 0xf0) {
      if (i + 2 < b.length && (b[i + 1] & 0xc0) === 0x80 && (b[i + 2] & 0xc0) === 0x80) {
        const v = ((x & 15) << 12) | ((b[i + 1] & 63) << 6) | (b[i + 2] & 63);
        if (v >= 0x800 && (v < 0xd800 || v > 0xdfff)) {
          cp = v;
          len = 3;
        }
      }
    } else if (x >= 0xf0 && x < 0xf5) {
      if (
        i + 3 < b.length &&
        (b[i + 1] & 0xc0) === 0x80 &&
        (b[i + 2] & 0xc0) === 0x80 &&
        (b[i + 3] & 0xc0) === 0x80
      ) {
        const v = ((x & 7) << 18) | ((b[i + 1] & 63) << 12) | ((b[i + 2] & 63) << 6) | (b[i + 3] & 63);
        if (v >= 0x10000 && v <= 0x10ffff) {
          cp = v;
          len = 4;
        }
      }
    }
    i += len;
    if (cp >= 0x10000) {
      const v = cp - 0x10000;
      units.push(0xd800 + (v >> 10), 0xdc00 + (v & 0x3ff));
    } else {
      units.push(cp);
    }
    if (units.length >= CHUNK) flush();
  }
  if (units.length > 0 || parts.length === 0) flush();
  return parts.join("");
}
