// Effect builders (HTTP, SQL, object store) and the semantic idempotency key (P-11).
//
// Parity with the Rust SDK (`ostro_sdk::effects`, 06-09): each builder fills in the
// unchanged WIT `ostro:runtime@0.1.0` request record (`HttpRequest`, `SqlRequest`,
// `ObjectRequest` in host.ts) and issues the existing `http-fetch` / `sql-query` /
// `object-store` call. The same inputs produce the same records as Rust.
//
//   const key = ctx.idempotencyKey("charge");
//   const resp = await ctx.http
//     .post("https://api.example.com/v1/charges")
//     .secretHeader("Authorization", "Bearer {{secret:stripe}}")
//     .json(body)
//     .idempotencyKey(key)
//     .timeout(10_000)
//     .redact("Authorization")
//     .send();
//   const rows = await ctx.sql("orders").exactlyOnce().query("UPDATE t SET n = n + 1 WHERE id = $1", [7]);
//   const data = await ctx.object("files").get("a.txt");
//
// Semantics (as in Rust): W1 `{{secret:NAME}}` is expanded by the host in header values
// only, for granted origins; W2 templated requests never follow redirects; W3 `redact`
// names are replaced in the journal/logs and in response headers; W4 any final status is a
// success; W5 without an override the host derives the key from (run, op); W6
// `exactlyOnce` = one transactional statement with a token row; W7 object `get` → bytes,
// `put`/`head` → JSON metadata, `delete` → nothing, `presign-*` → URL; W8 connector
// failures reject the op. Durations are milliseconds, like everywhere in the TS SDK.
//
// No `async` functions here (the guest bundle lowers them; host tests run the sources
// directly), and no bigint literals (see host.ts).

import type { Ctx } from "./ctx.ts";
import { NonRetryableError } from "./errors.ts";
import type {
  Header,
  HttpRequest,
  HttpResponse,
  ObjectRequest,
  OpOutput,
  RetryPolicy as WitRetryPolicy,
  SqlRequest,
  SqlResult,
  SqlValue,
} from "./host.ts";
import { decodeJson, encodeJson } from "./json.ts";
import type { Op } from "./ops.ts";
import { sha256, toHex } from "./sha256.ts";
import { RetryPolicy, u64 } from "./step.ts";
import type { RetryPolicyInit } from "./step.ts";
import { checkString, decodeUtf8, encodeUtf8 } from "./utf8.ts";

/** Domain separator of the semantic idempotency key (P-11). */
export const IDEMPOTENCY_KEY_DOMAIN = "ostro/idem/v1/name";

/**
 * The semantic idempotency key for `name` in run `runId` (P-11):
 * `"ostro-" + hex(sha256("ostro/idem/v1/name" || runId || 0x00 || name))[..32]`.
 * 38 characters, identical to the Rust and Python SDKs (conformance/vectors/idempotency.json).
 * Uses the SDK's synchronous sha256 (crypto.subtle's promise would never settle inside the
 * synchronous `run` export).
 */
export function idempotencyKeyFor(runId: string, name: string): string {
  const domain = encodeUtf8(IDEMPOTENCY_KEY_DOMAIN);
  const run = encodeUtf8(checkString(runId, "run id"));
  const nm = encodeUtf8(checkString(name, "idempotency key name"));
  const buf = new Uint8Array(domain.length + run.length + 1 + nm.length);
  buf.set(domain, 0);
  buf.set(run, domain.length);
  buf[domain.length + run.length] = 0;
  buf.set(nm, domain.length + run.length + 1);
  return "ostro-" + toHex(sha256(buf).subarray(0, 16));
}

function unexpected(what: string, out: OpOutput): never {
  throw new NonRetryableError("protocol", `expected ${what} output, got ${out.tag}`);
}

function retryPolicy(p: RetryPolicy | RetryPolicyInit): WitRetryPolicy {
  return (p instanceof RetryPolicy ? p : new RetryPolicy(p)).toWit();
}

// --- HTTP -----------------------------------------------------------------------------

/** Decode an `op-output.http`. */
export function decodeHttpResponse(out: OpOutput): HttpResponse {
  if (out.tag === "http") return out.val;
  return unexpected("http", out);
}

/** Builder for one outbound HTTP request (WIT `http-request`). */
export class HttpBuilder {
  _ctx: Ctx | undefined;
  _req: HttpRequest;

  constructor(method: string, url: string, ctx?: Ctx) {
    this._ctx = ctx;
    this._req = {
      method: checkString(method, "HTTP method"),
      url: checkString(url, "URL"),
      headers: [],
      body: undefined,
      timeoutMs: undefined,
      idempotencyKey: undefined,
      retry: undefined,
      redact: [],
    };
  }

  /** Append a header (sent verbatim). */
  header(name: string, value: string): this {
    this._req.headers.push({ name: checkString(name, "header name"), value: checkString(value, "header value") });
    return this;
  }

  /**
   * Append a header whose value carries a `{{secret:NAME}}` template, expanded by the host
   * for granted origins only (W1). Throws if `template` has no `{{secret:` marker.
   */
  secretHeader(name: string, template: string): this {
    if (typeof template !== "string" || !template.includes("{{secret:")) {
      throw new TypeError("secretHeader value must contain a {{secret:NAME}} template");
    }
    return this.header(name, template);
  }

  /** Set the raw body (a string is UTF-8 encoded; never templated, W1). */
  body(body: Uint8Array | string): this {
    this._req.body = typeof body === "string" ? encodeUtf8(checkString(body, "body")) : body.slice();
    return this;
  }

  /** JSON-encode `value` as the body and set `content-type: application/json` (replacing any). */
  json(value: unknown): this {
    const data = encodeJson(value);
    const kept: Header[] = this._req.headers.filter((h) => h.name.toLowerCase() !== "content-type");
    kept.push({ name: "content-type", value: "application/json" });
    this._req.headers = kept;
    this._req.body = data;
    return this;
  }

  /** Override the idempotency key the host sends (W5). */
  idempotencyKey(key: string): this {
    this._req.idempotencyKey = checkString(key, "idempotency key");
    return this;
  }

  /** Timeout of one attempt, in ms. */
  timeout(ms: number | bigint): this {
    this._req.timeoutMs = u64(ms, "timeout");
    return this;
  }

  /** Host retry policy (a `RetryPolicy` or partial init; defaults fill the rest). */
  retry(policy: RetryPolicy | RetryPolicyInit): this {
    this._req.retry = retryPolicy(policy);
    return this;
  }

  /** Header names to redact in the journal and logs (W3); names or arrays of names. */
  redact(...names: (string | string[])[]): this {
    for (const n of names) {
      for (const x of Array.isArray(n) ? n : [n]) this._req.redact.push(checkString(x, "redact name"));
    }
    return this;
  }

  /** The WIT request record this builder describes. */
  build(): HttpRequest {
    const r = this._req;
    return {
      method: r.method,
      url: r.url,
      headers: r.headers.map((h) => ({ name: h.name, value: h.value })),
      body: r.body,
      timeoutMs: r.timeoutMs,
      idempotencyKey: r.idempotencyKey,
      retry: r.retry,
      redact: r.redact.slice(),
    };
  }

  /** Issue the request (`http-fetch`); the op resolves to the `HttpResponse`. */
  send(): Op<HttpResponse> {
    const ctx = requireCtx(this._ctx);
    ctx._act.checkJournaled("http.send");
    return ctx._op(ctx._act.host.httpFetch(this.build()), decodeHttpResponse);
  }
}

/** `ctx.http`: pick the method and URL. */
export class HttpClient {
  _ctx: Ctx | undefined;
  constructor(ctx?: Ctx) {
    this._ctx = ctx;
  }
  get(url: string): HttpBuilder {
    return new HttpBuilder("GET", url, this._ctx);
  }
  post(url: string): HttpBuilder {
    return new HttpBuilder("POST", url, this._ctx);
  }
  put(url: string): HttpBuilder {
    return new HttpBuilder("PUT", url, this._ctx);
  }
  delete(url: string): HttpBuilder {
    return new HttpBuilder("DELETE", url, this._ctx);
  }
  request(method: string, url: string): HttpBuilder {
    return new HttpBuilder(method, url, this._ctx);
  }
}

function requireCtx(ctx: Ctx | undefined): Ctx {
  if (ctx === undefined) {
    throw new TypeError("this builder was created without a workflow ctx; use ctx.http / ctx.sql / ctx.object");
  }
  return ctx;
}

// --- SQL ------------------------------------------------------------------------------

const SQL_TAGS = ["null", "boolean", "int8", "float8", "numeric", "text", "bytea", "json", "timestamptz"];
const INT8_MIN = BigInt("-9223372036854775808");
const INT8_MAX = BigInt("9223372036854775807");

function int8(v: bigint): SqlValue {
  if (v < INT8_MIN || v > INT8_MAX) throw new RangeError("SQL integer parameter does not fit in int8");
  return { tag: "int8", val: v };
}

/**
 * A WIT `sql-value` for a JS value: a tagged `{tag, val}` with a known tag is kept;
 * `null`/`undefined` → null, boolean → boolean, bigint and integral numbers → int8, other
 * numbers → float8, string → text, Uint8Array → bytea, Date → timestamptz.
 */
export function sqlValue(v: unknown): SqlValue {
  if (v === null || v === undefined) return { tag: "null" };
  if (typeof v === "boolean") return { tag: "boolean", val: v };
  if (typeof v === "bigint") return int8(v);
  if (typeof v === "number") return Number.isInteger(v) ? int8(BigInt(v)) : { tag: "float8", val: v };
  if (typeof v === "string") return { tag: "text", val: checkString(v, "SQL text parameter") };
  if (v instanceof Uint8Array) return { tag: "bytea", val: v.slice() };
  if (v instanceof Date) {
    const ms = v.getTime();
    const seconds = Math.floor(ms / 1000);
    return { tag: "timestamptz", val: { seconds: BigInt(seconds), nanoseconds: (ms - seconds * 1000) * 1e6 } };
  }
  if (typeof v === "object" && typeof (v as { tag?: unknown }).tag === "string" && SQL_TAGS.includes((v as { tag: string }).tag)) {
    return v as SqlValue;
  }
  throw new TypeError("no SQL value mapping for this parameter; pass a tagged {tag, val} sql-value");
}

/** Decode an `op-output.sql`. */
export function decodeSqlResult(out: OpOutput): SqlResult {
  if (out.tag === "sql") return out.val;
  return unexpected("sql", out);
}

/** `ctx.sql(connection)`: statements against a named connection, at-least-once by default. */
export class SqlBuilder {
  _ctx: Ctx | undefined;
  _connection: string;
  _exactlyOnce: boolean;

  constructor(connection: string, ctx?: Ctx) {
    this._ctx = ctx;
    this._connection = checkString(connection, "connection name");
    this._exactlyOnce = false;
  }

  /** Exactly-once (W6): one statement in a transaction with a token row. */
  exactlyOnce(): this {
    this._exactlyOnce = true;
    return this;
  }

  /** The WIT request record for `statement` with positional `params`. */
  request(statement: string, params: Iterable<unknown> = []): SqlRequest {
    const ps: SqlValue[] = [];
    for (const p of params) ps.push(sqlValue(p));
    return {
      connection: this._connection,
      statement: checkString(statement, "SQL statement"),
      params: ps,
      exactlyOnce: this._exactlyOnce,
    };
  }

  /** Run `statement` (`sql-query`); the op resolves to the `SqlResult`. */
  query(statement: string, params: Iterable<unknown> = []): Op<SqlResult> {
    const ctx = requireCtx(this._ctx);
    ctx._act.checkJournaled("sql.query");
    return ctx._op(ctx._act.host.sqlQuery(this.request(statement, params)), decodeSqlResult);
  }
}

// --- object store ---------------------------------------------------------------------

/** `put` result metadata. */
export interface ObjectPutInfo {
  eTag: string | null;
}

/** `head` result metadata. */
export interface ObjectHead {
  size: number;
  lastModified: string;
  eTag: string | null;
  version: string | null;
}

function objectBytes(out: OpOutput): Uint8Array {
  if (out.tag === "object") return out.val;
  return unexpected("object", out);
}

function objectJson(out: OpOutput): Record<string, unknown> {
  const v = decodeJson(objectBytes(out));
  if (v === null) return {};
  if (typeof v !== "object" || Array.isArray(v)) throw new NonRetryableError("invalid_json", "expected a JSON object");
  return v as Record<string, unknown>;
}

function optStr(v: unknown): string | null {
  return typeof v === "string" ? v : null;
}

function decodePut(out: OpOutput): ObjectPutInfo {
  return { eTag: optStr(objectJson(out).e_tag) };
}

function decodeHead(out: OpOutput): ObjectHead {
  const v = objectJson(out);
  return {
    size: typeof v.size === "number" ? v.size : 0,
    lastModified: typeof v.last_modified === "string" ? v.last_modified : "",
    eTag: optStr(v.e_tag),
    version: optStr(v.version),
  };
}

function decodeUnitObject(out: OpOutput): undefined {
  objectBytes(out);
  return undefined;
}

function decodeUrl(out: OpOutput): string {
  const bytes = objectBytes(out);
  const text = decodeUtf8(bytes);
  // decodeUtf8 substitutes U+FFFD; a strict decode (Rust's String::from_utf8) must fail.
  const back = encodeUtf8(text);
  let same = back.length === bytes.length;
  for (let i = 0; same && i < bytes.length; i++) same = back[i] === bytes[i];
  if (!same) throw new NonRetryableError("protocol", "object URL not UTF-8");
  return text;
}

/** `ctx.object(connection)`: object-store operations (WIT `object-request`). */
export class ObjectClient {
  _ctx: Ctx | undefined;
  _connection: string;

  constructor(connection: string, ctx?: Ctx) {
    this._ctx = ctx;
    this._connection = checkString(connection, "connection name");
  }

  /** The WIT request record for `op` on `key`. */
  request(op: string, key: string, body?: Uint8Array): ObjectRequest {
    return {
      connection: this._connection,
      op: checkString(op, "object op"),
      key: checkString(key, "object key"),
      body: body === undefined ? undefined : body.slice(),
    };
  }

  _issue<T>(req: ObjectRequest, decode: (out: OpOutput) => T): Op<T> {
    const ctx = requireCtx(this._ctx);
    ctx._act.checkJournaled("object." + req.op);
    return ctx._op(ctx._act.host.objectStore(req), decode);
  }

  /** Any op string; resolves to the raw `op-output.object` bytes. */
  raw(op: string, key: string, body?: Uint8Array): Op<Uint8Array> {
    return this._issue(this.request(op, key, body), objectBytes);
  }

  /** `get`: the object bytes. */
  get(key: string): Op<Uint8Array> {
    return this._issue(this.request("get", key), objectBytes);
  }

  /** `put`: store `body` (a string is UTF-8 encoded). */
  put(key: string, body: Uint8Array | string): Op<ObjectPutInfo> {
    const data = typeof body === "string" ? encodeUtf8(checkString(body, "object body")) : body;
    return this._issue(this.request("put", key, data), decodePut);
  }

  /** `head`: object metadata. */
  head(key: string): Op<ObjectHead> {
    return this._issue(this.request("head", key), decodeHead);
  }

  /** `delete`. */
  delete(key: string): Op<undefined> {
    return this._issue(this.request("delete", key), decodeUnitObject);
  }

  /** `presign-get`: a URL (expiry from the connection config). */
  presignGet(key: string): Op<string> {
    return this._issue(this.request("presign-get", key), decodeUrl);
  }

  /** `presign-put`: a URL (expiry from the connection config). */
  presignPut(key: string): Op<string> {
    return this._issue(this.request("presign-put", key), decodeUrl);
  }
}
