// Effect builders (09-12): the WIT request records equal the Rust builders' (06-09) for the
// same inputs, and responses decode like Rust's per-builder decoders.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Ctx } from "../src/ctx.ts";
import { HttpBuilder, ObjectClient, SqlBuilder, idempotencyKeyFor, sqlValue } from "../src/effects.ts";
import { InvalidStringError, NonRetryableError, ProtocolError } from "../src/errors.ts";
import type { HttpRequest, HttpResponse, OpOutput, SqlResult } from "../src/host.ts";
import { beginActivation, driveToCompletion } from "../src/loop.ts";
import { SdkPromise } from "../src/promise.ts";
import { RetryPolicy, step } from "../src/step.ts";
import { FakeHost, FIRST_OP, done, lowered } from "./helpers.ts";

interface Run {
  root: SdkPromise<unknown>;
  value: unknown;
  error: unknown;
}

function runWorkflow(fake: FakeHost, body: (ctx: Ctx) => unknown): Run {
  const act = beginActivation(fake);
  const ctx = new Ctx(act, false);
  const root = SdkPromise.resolve().then(() => body(ctx));
  let error: unknown;
  try {
    driveToCompletion(act, root);
  } catch (e) {
    error = e;
  }
  let value: unknown;
  if (root.fulfilled) value = root.result;
  else if (root.rejected) error = root.result;
  return { root, value, error };
}

function recorded(fake: FakeHost, method: string): unknown[] {
  return fake.calls.filter(([m]) => m === method).map(([, a]) => a[0]);
}

const enc = (s: string): Uint8Array => new TextEncoder().encode(s);

// --- HTTP -------------------------------------------------------------------------------

test("http record matches the Rust builder", () => {
  const req = new HttpBuilder("POST", "https://api.example.com/v1/charges")
    .header("x-trace", "t1")
    .secretHeader("Authorization", "Bearer {{secret:stripe}}")
    .header("Content-Type", "text/plain")
    .json({ amount: 100, currency: "usd" })
    .idempotencyKey("k-1")
    .timeout(2500)
    .retry({ maxAttempts: 5, initialIntervalMs: 200, maxIntervalMs: 5000, backoffCoefficient: 1.5, nonRetryableKinds: ["bad"] })
    .redact("Authorization")
    .build();
  const expected: HttpRequest = {
    method: "POST",
    url: "https://api.example.com/v1/charges",
    headers: [
      { name: "x-trace", value: "t1" },
      { name: "Authorization", value: "Bearer {{secret:stripe}}" },
      { name: "content-type", value: "application/json" },
    ],
    body: enc('{"amount":100,"currency":"usd"}'),
    timeoutMs: 2500n,
    idempotencyKey: "k-1",
    retry: { maxAttempts: 5, initialIntervalMs: 200n, maxIntervalMs: 5000n, backoffCoefficient: 1.5, nonRetryableKinds: ["bad"] },
    redact: ["Authorization"],
  };
  assert.deepEqual(req, expected);
});

test("http defaults are empty like Rust HttpBuilder::new; retry defaults fill", () => {
  assert.deepEqual(new HttpBuilder("GET", "https://x/").build(), {
    method: "GET",
    url: "https://x/",
    headers: [],
    body: undefined,
    timeoutMs: undefined,
    idempotencyKey: undefined,
    retry: undefined,
    redact: [],
  });
  assert.deepEqual(new HttpBuilder("GET", "https://x/").retry(new RetryPolicy()).build().retry, {
    maxAttempts: 3,
    initialIntervalMs: 1000n,
    maxIntervalMs: 60000n,
    backoffCoefficient: 2.0,
    nonRetryableKinds: [],
  });
  const r = new HttpBuilder("PUT", "https://x/").body("héllo").timeout(3000n).redact(["a", "b"], "c").build();
  assert.deepEqual(r.body, enc("héllo"));
  assert.equal(r.timeoutMs, 3000n);
  assert.deepEqual(r.redact, ["a", "b", "c"]);
});

test("secretHeader requires a template; lone surrogates are rejected", () => {
  assert.throws(() => new HttpBuilder("GET", "https://x/").secretHeader("Authorization", "Bearer sk_live"), /secret/);
  assert.throws(() => new HttpBuilder("GET", "https://x/").header("x", "\ud800"), InvalidStringError);
});

test("ctx.http sends the record and resolves to the response", () => {
  const fake = new FakeHost();
  const resp: HttpResponse = { status: 201, headers: [{ name: "x", value: "y" }], body: enc("{}") };
  fake.scriptWait([done(FIRST_OP, { tag: "http", val: resp })]);
  const run = runWorkflow(fake, (ctx) => ctx.http.get("https://x/a").header("accept", "application/json").send());
  assert.equal(run.error, undefined);
  assert.deepEqual(run.value, resp);
  assert.deepEqual(recorded(fake, "httpFetch"), [
    {
      method: "GET",
      url: "https://x/a",
      headers: [{ name: "accept", value: "application/json" }],
      body: undefined,
      timeoutMs: undefined,
      idempotencyKey: undefined,
      retry: undefined,
      redact: [],
    },
  ]);
});

test("ctx.http methods and failure / wrong-output decoding", () => {
  for (const m of ["get", "post", "put", "delete"] as const) {
    const fake = new FakeHost();
    fake.scriptWait([done(FIRST_OP, { tag: "http", val: { status: 200, headers: [], body: new Uint8Array() } })]);
    runWorkflow(fake, (ctx) => ctx.http[m]("https://x/").send());
    assert.equal((recorded(fake, "httpFetch")[0] as HttpRequest).method, m.toUpperCase());
  }
  const fake = new FakeHost();
  fake.scriptWait(
    [{ op: FIRST_OP, outcome: { tag: "err", val: { tag: "non-retryable", val: { kind: "egress_denied", message: "no" } } } }],
    [done(FIRST_OP + 1n, { tag: "timer" })],
  );
  const run = runWorkflow(
    fake,
    lowered(function* (ctx: Ctx) {
      const kinds: string[] = [];
      for (let i = 0; i < 2; i++) {
        try {
          yield ctx.http.get("https://x/").send();
        } catch (e) {
          kinds.push((e as NonRetryableError).kind);
        }
      }
      return kinds;
    }),
  );
  assert.deepEqual(run.value, ["egress_denied", "protocol"]);
});

test("an effect inside a step body is a protocol error before any host call", () => {
  const fake = new FakeHost();
  const run = runWorkflow(
    fake,
    lowered(function* (ctx: Ctx) {
      try {
        yield step("s", () => {
          ctx.http.get("https://x/").send();
        });
      } catch (e) {
        return (e as ProtocolError).kind;
      }
      return "no error";
    }),
  );
  assert.equal(run.value, "sdk_protocol");
  assert.deepEqual(recorded(fake, "httpFetch"), []);
});

// --- SQL --------------------------------------------------------------------------------

test("sql record matches Rust (exactly-once flag, sql-value params)", () => {
  const req = new SqlBuilder("orders").exactlyOnce().request("UPDATE t SET a = $2 WHERE id = $1", [7, "x", null, true, 1.5, 9n]);
  assert.deepEqual(req, {
    connection: "orders",
    statement: "UPDATE t SET a = $2 WHERE id = $1",
    params: [
      { tag: "int8", val: 7n },
      { tag: "text", val: "x" },
      { tag: "null" },
      { tag: "boolean", val: true },
      { tag: "float8", val: 1.5 },
      { tag: "int8", val: 9n },
    ],
    exactlyOnce: true,
  });
  assert.deepEqual(new SqlBuilder("db").request("SELECT 1"), { connection: "db", statement: "SELECT 1", params: [], exactlyOnce: false });
});

test("sqlValue mapping", () => {
  assert.deepEqual(sqlValue({ tag: "numeric", val: "1.50" }), { tag: "numeric", val: "1.50" });
  assert.deepEqual(sqlValue(new Uint8Array([0, 1])), { tag: "bytea", val: new Uint8Array([0, 1]) });
  assert.deepEqual(sqlValue(new Date(Date.UTC(2026, 0, 1, 0, 0, 0, 250))), {
    tag: "timestamptz",
    val: { seconds: 1767225600n, nanoseconds: 250000000 },
  });
  assert.deepEqual(sqlValue(undefined), { tag: "null" });
  assert.throws(() => sqlValue(2n ** 63n), RangeError);
  assert.throws(() => sqlValue({}), TypeError);
});

test("ctx.sql query", () => {
  const fake = new FakeHost();
  const result: SqlResult = { columns: ["n"], rows: [[{ tag: "int8", val: 1n }]], rowsAffected: 1n };
  fake.scriptWait([done(FIRST_OP, { tag: "sql", val: result })]);
  const run = runWorkflow(fake, (ctx) => ctx.sql("db").exactlyOnce().query("UPDATE c SET n = n + 1 RETURNING n"));
  assert.deepEqual(run.value, result);
  assert.deepEqual(recorded(fake, "sqlQuery"), [
    { connection: "db", statement: "UPDATE c SET n = n + 1 RETURNING n", params: [], exactlyOnce: true },
  ]);
});

// --- object store -----------------------------------------------------------------------

test("object records match Rust", () => {
  const c = new ObjectClient("files");
  assert.deepEqual(c.request("get", "a.txt"), { connection: "files", op: "get", key: "a.txt", body: undefined });
  assert.deepEqual(c.request("put", "a.txt", enc("x")), { connection: "files", op: "put", key: "a.txt", body: enc("x") });
});

test("ctx.object ops and decoders", () => {
  const fake = new FakeHost();
  const outs = [
    "data",
    '{"e_tag":"\\"x\\""}',
    '{"size":3,"last_modified":"2026-01-01T00:00:00Z","e_tag":null,"version":null}',
    "",
    "https://x/y?sig=1",
    "https://x/p?sig=2",
    "raw",
  ];
  outs.forEach((o, i) => fake.scriptWait([done(FIRST_OP + BigInt(i), { tag: "object", val: enc(o) } as OpOutput)]));
  const run = runWorkflow(
    fake,
    lowered(function* (ctx: Ctx) {
      const o = ctx.object("files");
      return [
        yield o.get("k"),
        yield o.put("k", "body"),
        yield o.head("k"),
        yield o.delete("k"),
        yield o.presignGet("k"),
        yield o.presignPut("k"),
        yield o.raw("list", "k", enc("b")),
      ];
    }),
  );
  assert.equal(run.error, undefined);
  assert.deepEqual(run.value, [
    enc("data"),
    { eTag: '"x"' },
    { size: 3, lastModified: "2026-01-01T00:00:00Z", eTag: null, version: null },
    undefined,
    "https://x/y?sig=1",
    "https://x/p?sig=2",
    enc("raw"),
  ]);
  assert.deepEqual(
    recorded(fake, "objectStore").map((r) => {
      const q = r as { op: string; key: string; body?: Uint8Array };
      return [q.op, q.key, q.body === undefined ? null : new TextDecoder().decode(q.body)];
    }),
    [
      ["get", "k", null],
      ["put", "k", "body"],
      ["head", "k", null],
      ["delete", "k", null],
      ["presign-get", "k", null],
      ["presign-put", "k", null],
      ["list", "k", "b"],
    ],
  );
});

test("object decode errors: invalid JSON head, non-UTF-8 URL", () => {
  const fake = new FakeHost();
  fake.scriptWait([done(FIRST_OP, { tag: "object", val: enc("nope") })], [done(FIRST_OP + 1n, { tag: "object", val: new Uint8Array([0xff]) })]);
  const run = runWorkflow(
    fake,
    lowered(function* (ctx: Ctx) {
      const kinds: string[] = [];
      for (const f of [() => ctx.object("f").head("k"), () => ctx.object("f").presignGet("k")]) {
        try {
          yield f();
        } catch (e) {
          kinds.push((e as NonRetryableError).kind);
        }
      }
      return kinds;
    }),
  );
  assert.deepEqual(run.value, ["invalid_json", "protocol"]);
});

// --- idempotency key ----------------------------------------------------------------------

test("ctx.idempotencyKey uses the run id", () => {
  const fake = new FakeHost();
  const run = runWorkflow(fake, (ctx) => ctx.idempotencyKey("charge"));
  assert.equal(run.value, idempotencyKeyFor(fake.runInfo.runId, "charge"));
  assert.equal((run.value as string).length, 38);
});
