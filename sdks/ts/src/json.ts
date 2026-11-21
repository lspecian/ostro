// `application/json` payload codec (PF-W5, D-P9-15).
//
// - Encoding is `JSON.stringify` (compact, non-ASCII kept as UTF-8, NaN/±Infinity → null,
//   undefined → null) followed by the SDK UTF-8 encoder; this matches serde_json and the
//   Python SDK byte for byte on conformance/vectors/json.json.
// - A value JSON cannot represent (a bigint, a cycle) is a non-retryable `invalid_json`.
// - A lone surrogate anywhere in the value is a non-retryable `invalid_string` (PF-W4):
//   well-formed JSON.stringify escapes it as `\udXXX`, which serde_json rejects.
// - Empty data decodes as `null`.

import {
  INVALID_JSON,
  InvalidStringError,
  NonRetryableError,
  OstroError,
  setDetailsCodec,
} from "./errors.ts";
import type { Payload } from "./host.ts";
import { decodeUtf8, encodeUtf8, isWellFormed } from "./utf8.ts";

/** Content type of every SDK-encoded payload. */
export const JSON_CONTENT_TYPE = "application/json";

// An odd number of backslashes followed by a surrogate escape: only a lone surrogate is
// ever escaped this way by a well-formed JSON.stringify.
const ESCAPED_SURROGATE = /(?:^|[^\\])(?:\\\\)*\\u[dD][89a-fA-F][0-9a-fA-F]{2}/;

/** Serialise `value` as compact JSON text. */
export function stringifyJson(value: unknown): string {
  let text: string | undefined;
  try {
    text = JSON.stringify(value === undefined ? null : value);
  } catch (e) {
    if (e instanceof OstroError) throw e;
    const msg = e instanceof Error ? e.message : String(e);
    throw new NonRetryableError(INVALID_JSON, `value is not JSON-encodable: ${msg}`);
  }
  // A function or symbol at the top level stringifies to undefined.
  if (text === undefined) text = "null";
  if (!isWellFormed(text) || ESCAPED_SURROGATE.test(text)) {
    throw new InvalidStringError("JSON value contains a string with a lone surrogate");
  }
  return text;
}

/** Encode `value` as JSON bytes. */
export function encodeJson(value: unknown): Uint8Array {
  return encodeUtf8(stringifyJson(value));
}

/** Decode JSON bytes; empty data is `null`. Malformed JSON is a non-retryable `invalid_json`. */
export function decodeJson(data: Uint8Array): unknown {
  if (data.length === 0) return null;
  const text = decodeUtf8(data);
  try {
    return JSON.parse(text);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    throw new NonRetryableError(INVALID_JSON, `payload is not valid JSON: ${msg}`);
  }
}

/** An `application/json` payload holding `value`. */
export function toPayload(value: unknown): Payload {
  return { contentType: JSON_CONTENT_TYPE, data: encodeJson(value) };
}

/** The JSON value of a payload (the content type is not checked, like the Rust SDK). */
export function fromPayload(p: Payload): unknown {
  return decodeJson(p.data);
}

setDetailsCodec(
  (v) => {
    try {
      return toPayload(v);
    } catch {
      return undefined;
    }
  },
  (p) => fromPayload(p),
);
