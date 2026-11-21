// The registry and the jco `workflows` export: describe, run, result mapping (S9 rule:
// return the plain run-result, throw exactly the WIT failure variant).

import assert from "node:assert/strict";
import { test } from "node:test";

import type { Ctx } from "../src/ctx.ts";
import { CancelledError, NonRetryableError } from "../src/errors.ts";
import { _clearRegistry, workflow, workflows } from "../src/export.ts";
import type { RunResult } from "../src/export.ts";
import type { Failure, Payload } from "../src/host.ts";
import { setHost } from "../src/host.ts";
import { SdkPromise } from "../src/promise.ts";
import { step } from "../src/step.ts";
import { FakeHost, FIRST_OP, cancelRequested, json, lowered, methods, timerFired, unjson } from "./helpers.ts";

function withFake<T>(fake: FakeHost, f: () => T): T {
  const prev = setHost(fake);
  try {
    return f();
  } finally {
    setHost(prev);
  }
}

/** Call the export like jco does; a throw must be exactly a WIT failure variant. */
function call(fake: FakeHost, name: string, input: Payload): { ok: RunResult } | { err: Failure } {
  return withFake(fake, () => {
    try {
      return { ok: workflows.run(name, input) };
    } catch (thrown) {
      assert.ok(!(thrown instanceof Error), `the export threw an Error, not a failure variant: ${String(thrown)}`);
      const f = thrown as Failure;
      assert.ok(["retryable", "non-retryable", "timeout", "cancelled"].includes(f.tag), JSON.stringify(f));
      return { err: f };
    }
  });
}

function completed(r: { ok: RunResult } | { err: Failure }): unknown {
  assert.ok("ok" in r, JSON.stringify(r));
  assert.equal(r.ok.tag, "completed");
  const p = r.ok.val as Payload;
  assert.equal(p.contentType, "application/json");
  return unjson(p);
}

const empty: Payload = { contentType: "application/json", data: new Uint8Array() };

test("workflow() registers; describe() is sorted; duplicate names throw at registration", () => {
  _clearRegistry();
  workflow({ name: "zeta", version: 3, queue: "q", runTimeoutMs: 60_000 }, () => 1);
  const alpha = workflow("alpha", () => 2);
  assert.equal(alpha.name, "alpha");
  assert.equal(alpha.version, 1);
  assert.deepEqual(workflows.describe(), [
    { name: "alpha", version: 1, defaultQueue: undefined, runTimeoutMs: undefined },
    { name: "zeta", version: 3, defaultQueue: "q", runTimeoutMs: 60_000n },
  ]);
  assert.throws(() => workflow("alpha", () => 3), /registered twice/);
  assert.throws(() => workflow({ name: "bad\uD800" }, () => 3), /lone surrogate/);
  assert.throws(() => workflow({ name: "v", version: -1 }, () => 3), RangeError);
  _clearRegistry();
});

test("run(unknown) throws non-retryable unknown_workflow without touching the host", () => {
  _clearRegistry();
  const fake = new FakeHost();
  const r = call(fake, "nope", empty);
  assert.ok("err" in r);
  assert.equal(r.err.tag, "non-retryable");
  assert.equal(r.err.tag === "non-retryable" && r.err.val.kind, "unknown_workflow");
  assert.deepEqual(methods(fake), []);
});

test("a returned value completes with application/json; input decodes (empty = null)", () => {
  _clearRegistry();
  workflow<{ name: string } | null>(
    "hello",
    lowered(function* (_ctx: Ctx, input: { name: string } | null) {
      const greeting = yield step("greet", () => `hello, ${input === null ? "nobody" : input.name}`);
      return { greeting };
    }),
  );
  const fake = new FakeHost();
  assert.deepEqual(completed(call(fake, "hello", json({ name: "ostro" }))), { greeting: "hello, ostro" });
  assert.deepEqual(completed(call(new FakeHost(), "hello", empty)), { greeting: "hello, nobody" });
  assert.deepEqual(methods(fake), ["beginStep", "stepComplete"]);
  _clearRegistry();
});

test("continueAsNew maps to the continue-as-new run result", () => {
  _clearRegistry();
  workflow("looper", (ctx: Ctx, n: number) => {
    if (n < 3) return ctx.continueAsNew(n + 1, { workflow: "looper2", version: 2 });
    return n;
  });
  const r = call(new FakeHost(), "looper", json(1));
  assert.ok("ok" in r && r.ok.tag === "continue-as-new");
  const v = r.ok.val as { input: Payload; workflow: string; version: number };
  assert.deepEqual(unjson(v.input), 2);
  assert.equal(v.workflow, "looper2");
  assert.equal(v.version, 2);
  assert.equal(completed(call(new FakeHost(), "looper", json(3))), 3);
  _clearRegistry();
});

test("errors map to the WIT failure: cancelled, NonRetryable kind, uncaught js_exception, deadlock, invalid input", () => {
  _clearRegistry();
  workflow(
    "cancel-me",
    lowered(function* (ctx: Ctx) {
      yield ctx.sleep(3_600_000);
      return "slept";
    }),
  );
  workflow("fails", () => {
    throw new NonRetryableError("bad_input", "nope", { field: "x" });
  });
  workflow("throws", () => {
    throw new TypeError("boom");
  });
  workflow("deadlocks", () => new SdkPromise(() => {}));
  workflow("rethrows-cancel", () => SdkPromise.reject(new CancelledError()));

  const fake = new FakeHost();
  fake.scriptWait([cancelRequested()]);
  assert.deepEqual(call(fake, "cancel-me", empty), { err: { tag: "cancelled" } });

  const fails = call(new FakeHost(), "fails", empty);
  assert.ok("err" in fails && fails.err.tag === "non-retryable");
  assert.equal(fails.err.val.kind, "bad_input");
  assert.deepEqual(unjson(fails.err.val.details as Payload), { field: "x" });

  assert.deepEqual(call(new FakeHost(), "throws", empty), {
    err: { tag: "non-retryable", val: { kind: "js_exception", message: "TypeError: boom", details: undefined } },
  });

  const dl = call(new FakeHost(), "deadlocks", empty);
  assert.ok("err" in dl && dl.err.tag === "non-retryable" && dl.err.val.kind === "sdk_protocol");

  assert.deepEqual(call(new FakeHost(), "rethrows-cancel", empty), { err: { tag: "cancelled" } });

  const bad = call(new FakeHost(), "throws", { contentType: "application/json", data: new TextEncoder().encode("{nope") });
  assert.ok("err" in bad && bad.err.tag === "non-retryable" && bad.err.val.kind === "invalid_json");

  const unencodable = workflow("big", () => ({ n: BigInt(1) }));
  assert.equal(unencodable.name, "big");
  const big = call(new FakeHost(), "big", empty);
  assert.ok("err" in big && big.err.tag === "non-retryable" && big.err.val.kind === "invalid_json");
  _clearRegistry();
});

test("each run is a fresh activation (op table, step lock, queue, closed flag)", () => {
  _clearRegistry();
  workflow(
    "timer",
    lowered(function* (ctx: Ctx) {
      yield ctx.sleep(1000);
      return yield step("after", () => "done");
    }),
  );
  for (let i = 0; i < 2; i++) {
    const fake = new FakeHost();
    fake.scriptWait([timerFired(FIRST_OP)]);
    assert.equal(completed(call(fake, "timer", empty)), "done");
    assert.deepEqual(methods(fake), ["sleep", "wait", "beginStep", "stepComplete"]);
  }
  _clearRegistry();
});

test("the bundle config's inject targets are the SDK's installed shims", async () => {
  const sdk = await import("../src/index.ts");
  const { install, uninstall } = await import("../src/determinism.ts");
  const { installGlobals, uninstallGlobals } = await import("../src/promise.ts");
  const g = globalThis as Record<string, unknown>;
  install();
  installGlobals();
  try {
    assert.equal(g.console, sdk.__ostroConsole);
    assert.equal(g.setTimeout, sdk.__ostroSetTimeout);
    assert.equal(g.clearTimeout, sdk.__ostroClearTimeout);
    assert.equal(g.setInterval, sdk.__ostroSetInterval);
    assert.equal(g.clearInterval, sdk.__ostroClearInterval);
    assert.equal(g.queueMicrotask, sdk.__ostroQueueMicrotask);
  } finally {
    uninstallGlobals();
    uninstall();
  }
  const { readFileSync } = await import("node:fs");
  const config = readFileSync(new URL("../rolldown.ostro.mjs", import.meta.url), "utf8");
  for (const name of ["__ostroConsole", "__ostroSetTimeout", "__ostroClearTimeout", "__ostroSetInterval", "__ostroClearInterval", "__ostroQueueMicrotask"]) {
    assert.ok(config.includes(`["ostro", "${name}"]`), name);
    assert.equal(typeof (sdk as Record<string, unknown>)[name] === "function" || typeof (sdk as Record<string, unknown>)[name] === "object", true, name);
  }
});
