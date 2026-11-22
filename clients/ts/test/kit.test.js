// The TS test kit (`ostro-client/testing`, 10-17) against a spawned
// `ostro dev --ephemeral --time-skipping` (OSTRO_BIN, else <repo>/target/debug/ostro).
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { createServer } from "node:http";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { READY_LINE, READY_LINE_PATTERN, TestEnv, TestEnvError, ostroBin as kitBin } from "../src/testing.js";

const REPO = fileURLToPath(new URL("../../../", import.meta.url));
const DAY_MS = 24 * 3600 * 1000;

function ostroBin() {
  const bin = process.env.OSTRO_BIN || join(REPO, "target", "debug", "ostro");
  if (!existsSync(bin)) {
    throw new Error(`the ostro binary ${bin} does not exist: build it with \`cargo build -p ostro\` or point OSTRO_BIN at it`);
  }
  return bin;
}

function guestPath(name) {
  if (name === "p10-time" && process.env.OSTRO_E2E_WASM) return process.env.OSTRO_E2E_WASM;
  const target = join(REPO, "target", "guests");
  const env = { ...process.env };
  for (const k of ["RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_TARGET", "CARGO_TARGET_DIR", "RUSTC_WRAPPER"]) {
    delete env[k];
  }
  const r = spawnSync(
    process.env.CARGO || "cargo",
    ["build", "--manifest-path", join(REPO, "guests", "Cargo.toml"), "-p", name, "--target", "wasm32-wasip2", "--release", "--target-dir", target],
    { env, encoding: "utf8" },
  );
  if (r.status !== 0) throw new Error(`building guests/${name} failed:\n${r.stderr ?? r.error}`);
  return join(target, "wasm32-wasip2", "release", `${name.replaceAll("-", "_")}.wasm`);
}

let env;

before(async () => {
  env = await TestEnv.start({ binary: ostroBin() });
  await env.deployWasm(guestPath("p10-time"), { name: "p10-time" });
});

after(async () => {
  await env?.close();
});

test("the ready-line pattern is the frozen one", () => {
  assert.equal(READY_LINE_PATTERN, String.raw`^(.+?) listening on http://(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$`);
  const m = READY_LINE.exec("ostro dev listening on http://127.0.0.1:1 (data: /tmp/a b, ephemeral) ready in 5 ms");
  assert.ok(m);
  assert.equal(m[2], "127.0.0.1:1");
  assert.equal(m[3], "/tmp/a b");
  assert.equal(m[4], ", ephemeral");
});

test("a 3-day sleep completes in seconds", async () => {
  assert.ok(env.ephemeral);
  const t0 = Date.now();
  const run = await env.startRun("sleep_for", { ms: 3 * DAY_MS });
  const res = await env.result(run, { wait: "30s" });
  const wall = Date.now() - t0;
  assert.equal(res.status, "completed");
  assert.deepEqual(res.output, { slept: 3 * DAY_MS });
  assert.ok(wall < 10_000, `wall ${wall} ms`);
  assert.ok((await env.nowMs()) - run.acceptedAtMs >= 3 * DAY_MS);
  const timers = await env.timers(run);
  assert.equal(timers.length, 1);
  assert.ok(timers[0].fired);
  assert.equal(timers[0].fireAtMs, run.acceptedAtMs + 3 * DAY_MS);
  const kinds = (await env.history(run)).map((e) => e.kind);
  assert.equal(kinds[0], "run_started");
  assert.equal(kinds.at(-1), "run_completed");
  assert.deepEqual(await env.assertCompleted(run, { slept: 3 * DAY_MS }), { slept: 3 * DAY_MS });
  console.log(`kit wall time: 3-day sleep_for ${wall} ms`);
});

test("signal_then_sleep returns the payload", async () => {
  const run = await env.startRun("signal_then_sleep", {});
  await env.signal(run, "go", { ok: 7 });
  assert.deepEqual(await env.assertCompleted(run.runId), { ok: 7 });
  assert.ok((await env.nowMs()) - run.acceptedAtMs >= DAY_MS);
  assert.deepEqual(await env.steps(run), []);
});

test("skip advances the clock", async () => {
  const before = await env.nowMs();
  const s = await env.skip("72h");
  assert.ok(s.skipped_ms >= 72 * 3_600_000);
  assert.ok((await env.nowMs()) - before >= 72 * 3_600_000);
});

test("loopback HTTP mock with allowEgress", async () => {
  const srv = createServer((req, res) => {
    const ok = req.url === "/hello";
    res.writeHead(ok ? 200 : 404);
    res.end(ok ? "hello from the mock" : "nope");
  });
  await new Promise((r) => srv.listen(0, "127.0.0.1", r));
  const origin = `http://127.0.0.1:${srv.address().port}`;
  const e = await TestEnv.start({ binary: ostroBin(), egressLoopback: true });
  try {
    await e.deployWasm(guestPath("fx-http")); // module "fx-http" (file stem)
    await e.allowEgress("fx-http", origin);
    const run = await e.startRun("fx-http", { requests: [{ method: "GET", url: `${origin}/hello` }], parallel: false });
    const out = await e.assertCompleted(run);
    assert.equal(out.results[0].status, 200, JSON.stringify(out));
    assert.equal(Buffer.from(out.results[0].body_b64, "base64").toString(), "hello from the mock");
  } finally {
    await e.close();
    srv.close();
  }
});

test("close kills the server", async () => {
  const e = await TestEnv.start({ binary: ostroBin() });
  const pid = e.pid;
  await e.close();
  await e.close(); // idempotent
  const deadline = Date.now() + 2000;
  for (;;) {
    let alive = true;
    try {
      process.kill(pid, 0);
    } catch {
      alive = false;
    }
    if (!alive) break;
    assert.ok(Date.now() < deadline, `pid ${pid} still alive`);
    await new Promise((r) => setTimeout(r, 20));
  }
});

test("a missing OSTRO_BIN is a clear error", async () => {
  const saved = process.env.OSTRO_BIN;
  process.env.OSTRO_BIN = "/nonexistent/ostro-kit-test";
  try {
    assert.throws(() => kitBin(), (e) => e instanceof TestEnvError && e.message.includes("OSTRO_BIN=/nonexistent/ostro-kit-test"));
    await assert.rejects(TestEnv.start(), /OSTRO_BIN/);
  } finally {
    if (saved === undefined) delete process.env.OSTRO_BIN;
    else process.env.OSTRO_BIN = saved;
  }
});

test("package.json exports ./testing with types", () => {
  const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
  assert.deepEqual(pkg.exports["./testing"], { types: "./src/testing.d.ts", default: "./src/testing.js" });
});
