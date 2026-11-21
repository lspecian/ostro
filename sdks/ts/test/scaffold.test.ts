// Wave-0 scaffold tests: the host contract, the fake host and the vendored WIT copy.

import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { VERSION } from "../src/index.ts";
import { host, setHost } from "../src/host.ts";
import { CANCELLATION_OP, FakeHost, FIRST_OP } from "./helpers.ts";

const witSource = fileURLToPath(new URL("../../../wit/ostro-runtime-0.1.0/runtime.wit", import.meta.url));
const witCopy = fileURLToPath(new URL("../wit/runtime.wit", import.meta.url));

function withHost<T>(h: FakeHost | undefined, body: () => T): T {
  const previous = setHost(h);
  try {
    return body();
  } finally {
    setHost(previous);
  }
}

test("version", () => {
  assert.equal(VERSION, "0.1.0");
});

test("setHost/host round-trip", () => {
  const first = new FakeHost();
  const second = new FakeHost();
  const previous = setHost(first);
  try {
    assert.equal(host(), first);
    assert.equal(setHost(second), first);
    assert.equal(host(), second);
  } finally {
    setHost(previous);
  }
});

test("host() without an installed host throws", () => {
  withHost(undefined, () => {
    assert.throws(() => host(), /no host installed/);
  });
});

test("the fake host covers every WIT import function", () => {
  const text = readFileSync(witSource, "utf8");
  const renames: Record<string, string> = { seq: "stepSeq", complete: "stepComplete", emit: "logEmit" };
  const expected = new Set<string>();
  for (const iface of ["context", "journal", "effects", "log"]) {
    const body = new RegExp(`^interface ${iface} \\{([\\s\\S]*?)^\\}`, "m").exec(text);
    assert.ok(body, iface);
    for (const m of body[1].matchAll(/^\s+([a-z][a-z0-9-]*): func\(/gm)) {
      const camel = m[1].replace(/-([a-z0-9])/g, (_, c: string) => c.toUpperCase());
      expected.add(renames[m[1]] ?? camel);
    }
  }
  const proto = FakeHost.prototype as unknown as Record<string, unknown>;
  const scripting = new Set(["constructor", "scriptBeginStep", "scriptStepAck", "scriptWait", "opsIssued"]);
  const methods = new Set(
    Object.getOwnPropertyNames(proto).filter((n) => typeof proto[n] === "function" && !scripting.has(n)),
  );
  assert.deepEqual([...methods].sort(), [...expected].sort());
});

test("the fake host records calls and allocates bigint op ids", () => {
  const fake = new FakeHost();
  withHost(fake, () => {
    const h = host();
    const a = h.sleep(1000n);
    const b = h.waitSignal("go", undefined);
    const c = h.startChild("leaf", { contentType: "application/json", data: new Uint8Array() }, { detached: false });
    const token = h.createAwaitToken(5000n);
    for (const op of [a, b, c, token.op]) assert.equal(typeof op, "bigint");
    assert.deepEqual([a, b, c, token.op], [FIRST_OP, FIRST_OP + 1n, FIRST_OP + 2n, FIRST_OP + 3n]);
    assert.equal(token.token, `tok-${FIRST_OP + 3n}`);
    assert.equal(h.cancellation(), CANCELLATION_OP);
    assert.equal(CANCELLATION_OP, 18446744073709551615n);
    // 2^64-1 is not representable as a Number: the reason op ids are bigint (PF-W3).
    assert.notEqual(BigInt(Number(CANCELLATION_OP)), CANCELLATION_OP);
    h.cancelOp(b);
    h.logEmit("info", "hello", [["k", "v"]]);
    assert.deepEqual(
      fake.calls.map(([m]) => m),
      ["sleep", "waitSignal", "startChild", "createAwaitToken", "cancellation", "cancelOp", "logEmit"],
    );
    assert.deepEqual(fake.calls[0], ["sleep", [1000n]]);
    assert.deepEqual(fake.cancelledOps, [b]);
    assert.deepEqual(fake.logs, [["info", "hello", [["k", "v"]]]]);
    assert.deepEqual(fake.opsIssued(), [a, b, c, token.op]);
  });
});

test("the fake host returns scripted wait completions", () => {
  const fake = new FakeHost();
  const op = fake.sleep(10n);
  fake.scriptWait([{ op, outcome: { tag: "ok", val: { tag: "timer" } } }]);
  assert.deepEqual(fake.wait([op, CANCELLATION_OP], "any"), [{ op, outcome: { tag: "ok", val: { tag: "timer" } } }]);
  assert.deepEqual(fake.calls.at(-1), ["wait", [[op, CANCELLATION_OP], "any"]]);
  assert.throws(() => fake.wait([op], "all"), /unscripted wait/);
});

test("the fake host scripts steps", () => {
  const fake = new FakeHost();
  const payload = { contentType: "application/json", data: new TextEncoder().encode("42") };
  fake.scriptBeginStep({ tag: "replayed", val: { tag: "ok", val: payload } });
  assert.deepEqual(fake.beginStep("a", {}), { tag: "replayed", val: { tag: "ok", val: payload } });
  const start = fake.beginStep("b", { timeoutMs: 5n });
  assert.equal(start.tag, "execute");
  if (start.tag !== "execute") return;
  assert.equal(fake.stepSeq(start.val), 1n);
  fake.scriptStepAck("retry-now");
  assert.equal(fake.stepComplete(start.val, { tag: "ok", val: payload }), "retry-now");
  assert.equal(fake.stepComplete(start.val, { tag: "ok", val: payload }), "recorded");
});

test("the fake host context reads", () => {
  const fake = new FakeHost();
  assert.equal(fake.info().workflow, "test");
  assert.notEqual(fake.randomU64(), fake.randomU64());
  assert.equal(fake.randomBytes(16).length, 16);
  assert.equal(fake.patched("p1"), true);
  fake.patches.set("p2", false);
  assert.equal(fake.patched("p2"), false);
  fake.setStatus('{"s":1}');
  assert.deepEqual(fake.statuses, ['{"s":1}']);
});

test("wit/runtime.wit is byte-identical to the frozen source", (t) => {
  if (!existsSync(witCopy)) {
    t.skip("sdks/ts/wit/runtime.wit is copied by 09-02 and is not committed yet");
    return;
  }
  assert.deepEqual(readFileSync(witCopy), readFileSync(witSource));
});
