// Unit tests against a local node:http stub (no Ostro server needed).
import { test, before, after, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { once } from "node:events";

import * as pkg from "../src/index.js";
import { Client, OstroError, ROUTES, parseDuration } from "../src/index.js";

// --- stub -------------------------------------------------------------------------------

const stub = { requests: [], responses: [], delayMs: 0 };
let server;
let base;

before(async () => {
  server = createServer((req, res) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      stub.requests.push({ method: req.method, url: req.url, headers: req.headers, body: Buffer.concat(chunks) });
      const [status, body] = stub.responses.shift() ?? [200, {}];
      const data = Buffer.isBuffer(body) ? body : Buffer.from(JSON.stringify(body));
      setTimeout(() => {
        res.writeHead(status, { "content-type": "application/json", "content-length": data.length });
        res.end(data);
      }, stub.delayMs);
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  base = `http://127.0.0.1:${server.address().port}`;
});

after(() => {
  server.closeAllConnections();
  server.close();
});

beforeEach(() => {
  stub.requests = [];
  stub.responses = [];
  stub.delayMs = 0;
});

const last = () => stub.requests.at(-1);
const json = (r) => (r.body.length ? JSON.parse(r.body.toString()) : undefined);
const split = (r) => {
  const u = new URL(r.url, "http://x");
  return [u.pathname, Object.fromEntries([...u.searchParams.keys()].map((k) => [k, u.searchParams.getAll(k)]))];
};

// --- tests ------------------------------------------------------------------------------

test("startRun posts JSON to the workflow runs path", async () => {
  stub.responses.push([201, { run_id: "r1", created: true }]);
  const c = new Client({ server: base });
  const out = await c.startRun("order", { id: 7 }, { idempotencyKey: "k1", tags: ["a=b"], extra: { priority: 3 } });
  assert.deepEqual(out, { run_id: "r1", created: true });
  assert.equal(last().method, "POST");
  assert.equal(split(last())[0], "/v1/workflows/order/runs");
  assert.equal(last().headers["content-type"], "application/json");
  assert.deepEqual(json(last()), { input: { id: 7 }, idempotency_key: "k1", tags: ["a=b"], priority: 3 });
});

test("a tenant-qualified workflow is one encoded segment", async () => {
  const c = new Client({ server: base });
  await c.startRun("acme/order");
  assert.equal(split(last())[0], "/v1/workflows/acme%2Forder/runs");
  await c.signal("r 1", "go/now", { ok: true }, { requestId: "q" });
  assert.equal(split(last())[0], "/v1/runs/r%201/signals/go%2Fnow");
  assert.deepEqual(json(last()), { payload: { ok: true }, request_id: "q" });
});

test("result sends wait and allows wait + timeout before aborting", async () => {
  // The server answers after 400 ms; the client timeout is 200 ms but the wait (1 s) is
  // added on top, so the long-poll is not aborted.
  stub.delayMs = 400;
  stub.responses.push([200, { run_id: "r1", completed: true, status: "completed", result: 1 }]);
  const c = new Client({ server: base, timeoutMs: 200 });
  const r = await c.result("r1", { wait: "1s" });
  assert.equal(r.result, 1);
  const [path, q] = split(last());
  assert.equal(path, "/v1/runs/r1/result");
  assert.deepEqual(q.wait, ["1000ms"]);
});

test("result without wait polls once", async () => {
  stub.responses.push([200, { run_id: "r1", completed: false }]);
  const r = await new Client({ server: base }).result("r1");
  assert.equal(r.completed, false);
  assert.deepEqual(split(last())[1].wait, ["0ms"]);
  assert.equal(stub.requests.length, 1);
});

test("an error body maps to OstroError", async () => {
  stub.responses.push([409, { error: { code: "run_terminal", message: "run is completed", details: { status: "completed" } } }]);
  await assert.rejects(new Client({ server: base }).cancel("r1", "x"), (e) => {
    assert.ok(e instanceof OstroError);
    assert.equal(e.status, 409);
    assert.equal(e.code, "run_terminal");
    assert.equal(e.message, "run is completed");
    assert.deepEqual(e.details, { status: "completed" });
    return true;
  });
  assert.deepEqual(json(last()), { reason: "x" });
});

test("a non-JSON error is http_<status>", async () => {
  stub.responses.push([502, Buffer.from("bad gateway")]);
  await assert.rejects(new Client({ server: base }).getRun("r1"), (e) => {
    assert.equal(e.status, 502);
    assert.equal(e.code, "http_502");
    assert.equal(e.message, "bad gateway");
    return true;
  });
});

test("details.reason", async () => {
  stub.responses.push([409, { error: { code: "failed_precondition", message: "off", details: { reason: "time_skipping_disabled" } } }]);
  await assert.rejects(new Client({ server: base }).devTime(), (e) => e.reason === "time_skipping_disabled");
});

test("connection refused is unreachable", async () => {
  const s = createServer();
  s.listen(0, "127.0.0.1");
  await once(s, "listening");
  const port = s.address().port;
  s.close();
  await once(s, "close");
  await assert.rejects(new Client({ server: `http://127.0.0.1:${port}`, timeoutMs: 2000 }).getRun("r1"), (e) => {
    assert.ok(e instanceof OstroError);
    assert.equal(e.code, "unreachable");
    assert.equal(e.status, undefined);
    assert.match(e.message, /ECONNREFUSED/);
    return true;
  });
});

test("a slow server is timeout", async () => {
  stub.delayMs = 500;
  await assert.rejects(new Client({ server: base, timeoutMs: 100 }).getRun("r1"), (e) => e.code === "timeout");
});

test("uploadModule PUTs the Wasm under its SHA-256", async () => {
  const wasm = new Uint8Array([0, 0x61, 0x73, 0x6d, 1, 0, 0, 0]);
  stub.responses.push([201, { created: true, module: { sha256: "x", workflows: [] } }]);
  await new Client({ server: base }).uploadModule(wasm, { name: "m", tenant: "acme" });
  const [path, q] = split(last());
  assert.equal(last().method, "PUT");
  assert.equal(path, `/v1/modules/${createHash("sha256").update(wasm).digest("hex")}`);
  assert.deepEqual(q, { name: ["m"], tenant: ["acme"] });
  assert.equal(last().headers["content-type"], "application/wasm");
  assert.deepEqual(new Uint8Array(last().body), wasm);
});

test("listRuns and history query strings", async () => {
  const c = new Client({ server: base });
  await c.listRuns({ workflow: "w", tags: ["a", "b"], limit: 5 });
  assert.deepEqual(split(last()), ["/v1/runs", { workflow: ["w"], tag: ["a", "b"], limit: ["5"] }]);
  await c.history("r1", { fromSeq: 3, limit: 10 });
  assert.deepEqual(split(last()), ["/v1/runs/r1/history", { from_seq: ["3"], limit: ["10"] }]);
});

test("devSkip and replay bodies", async () => {
  const c = new Client({ server: base });
  await c.devSkip({ by: "72h" });
  assert.deepEqual(json(last()), { by: "72h" });
  await c.devSkip({ by: 90_000 });
  assert.deepEqual(json(last()), { by: "90000ms" });
  await c.devSkip({ toMs: 5 });
  assert.deepEqual(json(last()), { to_ms: 5 });
  await assert.rejects(c.devSkip({}), TypeError);
  await c.replay("abc", { runs: ["r1"], allowMissing: true });
  assert.equal(split(last())[0], "/v1/modules/abc/replay");
  assert.deepEqual(json(last()), { runs: ["r1"], allow_missing: true });
  await c.replay("abc", { sample: 5, workflow: "w" });
  assert.deepEqual(json(last()), { sample: 5, workflow: "w" });
});

test("parseDuration", () => {
  assert.equal(parseDuration("30s"), 30_000);
  assert.equal(parseDuration("500ms"), 500);
  assert.equal(parseDuration("72h"), 72 * 3_600_000);
  assert.equal(parseDuration(15), 15);
  assert.throws(() => parseDuration("soon"), TypeError);
});

test("server defaults to OSTRO_SERVER, then 127.0.0.1:7700", () => {
  const saved = process.env.OSTRO_SERVER;
  try {
    process.env.OSTRO_SERVER = "127.0.0.1:9";
    assert.equal(new Client().server, "http://127.0.0.1:9");
    delete process.env.OSTRO_SERVER;
    assert.equal(new Client().server, "http://127.0.0.1:7700");
  } finally {
    if (saved === undefined) delete process.env.OSTRO_SERVER;
    else process.env.OSTRO_SERVER = saved;
  }
});

// Every public method with sample arguments: its route must be in ROUTES.
const CALLS = {
  uploadModule: [new Uint8Array([1])],
  workflows: [],
  activate: ["w", 1],
  startRun: ["w"],
  getRun: ["r"],
  listRuns: [],
  history: ["r"],
  result: ["r"],
  signal: ["r", "s"],
  cancel: ["r"],
  terminate: ["r"],
  retry: ["r"],
  completeToken: ["t"],
  devTime: [],
  devSkip: [{ by: "1h" }],
  replay: ["sha", { runs: ["r"] }],
};

const methods = () =>
  Object.getOwnPropertyNames(Client.prototype).filter((n) => n !== "constructor" && !n.startsWith("_"));

test("every client method's route is declared in ROUTES", async () => {
  assert.deepEqual(methods().sort(), Object.keys(CALLS).sort(), "update CALLS for new methods");
  const used = [];
  const c = new Client({ server: "http://127.0.0.1:1" });
  c._request = async (route) => {
    used.push(route.join(" "));
    return { workflows: [], completed: true };
  };
  for (const [name, args] of Object.entries(CALLS)) {
    const n = used.length;
    await c[name](...args);
    assert.ok(used.length > n, name);
  }
  const declared = new Set(ROUTES.map((r) => r.join(" ")));
  for (const r of used) assert.ok(declared.has(r), r);
  assert.deepEqual(new Set(used), declared, "ROUTES lists a route no method calls");
});

test("index.d.ts declares every export and every Client method", () => {
  const dts = readFileSync(new URL("../src/index.d.ts", import.meta.url), "utf8");
  for (const name of Object.keys(pkg)) {
    assert.match(dts, new RegExp(`export declare (const|function|class) ${name}\\b`), `export ${name}`);
  }
  const body = dts.slice(dts.indexOf("export declare class Client"));
  for (const m of methods()) {
    assert.match(body, new RegExp(`\\n\\s+${m}\\(`), `Client.${m}`);
  }
});
