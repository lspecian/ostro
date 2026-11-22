// The shared client e2e scenario (SDK-07) against a spawned
// `ostro dev --ephemeral --time-skipping=manual` (OSTRO_BIN, else <repo>/target/debug/ostro).
import { test, after } from "node:test";
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

import { Client, OstroError } from "../src/index.js";

const REPO = fileURLToPath(new URL("../../../", import.meta.url));
const DAY_MS = 24 * 3600 * 1000;

/** The frozen ready line (D-P10-9), printed on stdout. */
const READY = /^(.+?) listening on http:\/\/(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$/;

function ostroBin() {
  const bin = process.env.OSTRO_BIN || join(REPO, "target", "debug", "ostro");
  if (!existsSync(bin)) {
    throw new Error(
      `the ostro binary ${bin} does not exist: build it with \`cargo build -p ostro\` and/or ` +
        "point OSTRO_BIN at it (scripts/ci-local.sh sets OSTRO_BIN)",
    );
  }
  return bin;
}

function guestWasm(name) {
  if (process.env.OSTRO_E2E_WASM) return readFileSync(process.env.OSTRO_E2E_WASM);
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
  return readFileSync(join(target, "wasm32-wasip2", "release", `${name.replaceAll("-", "_")}.wasm`));
}

const cleanups = [];
after(() => {
  for (const f of cleanups.reverse()) f();
});

/** Spawn the server, resolve with {url, ephemeral} once the ready line is printed. */
function spawnServer(extra) {
  const cwd = mkdtempSync(join(tmpdir(), "ostro-client-ts-"));
  const env = Object.fromEntries(Object.entries(process.env).filter(([k]) => !k.startsWith("OSTRO_") && k !== "RUST_LOG"));
  const child = spawn(ostroBin(), ["dev", "--ephemeral", "--listen", "127.0.0.1:0", ...extra], {
    cwd,
    env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  cleanups.push(() => {
    child.kill("SIGKILL");
    rmSync(cwd, { recursive: true, force: true });
  });
  let stderr = "";
  child.stderr.on("data", (d) => (stderr += d));
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`no ready line from ostro dev within 60 s; stderr: ${stderr}`)), 60_000);
    child.on("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`ostro dev exited (${code}) before its ready line; stderr: ${stderr}`));
    });
    createInterface({ input: child.stdout }).on("line", (line) => {
      const m = READY.exec(line);
      if (m) {
        clearTimeout(timer);
        resolve({ url: `http://${m[2]}`, ephemeral: m[4] !== undefined, child });
      }
    });
  });
}

function unknownId(runId) {
  const c = [...runId];
  c[10] = c[10] === "0" ? "1" : "0";
  return c.join("");
}

const hasKind = (h, kind) => h.events.some((e) => e.kind === kind);
const kinds = (h) => h.events.map((e) => e.kind);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

test("shared scenario against ostro dev --time-skipping=manual", { timeout: 300_000 }, async () => {
  const wasm = guestWasm("p10-time");
  const t0 = Date.now();
  const srv = await spawnServer(["--time-skipping=manual"]);
  assert.ok(srv.ephemeral);
  const c = new Client({ server: srv.url, timeoutMs: 30_000 });

  const up = await c.uploadModule(wasm, { name: "p10-time" });
  assert.ok(up.module.workflows.some((w) => w.name === "signal_then_sleep"), JSON.stringify(up));
  const wf = await c.activate("signal_then_sleep", 1);
  assert.equal(wf.active_version, 1);

  const started = await c.startRun("signal_then_sleep", {});
  const runId = started.run_id;
  assert.equal(started.created, true);
  // The run is live: `running`, or `scheduled`/`ready` between activations (a run parked
  // on its signal wait is reported either way).
  const run = await c.getRun(runId);
  assert.ok(["running", "scheduled", "ready"].includes(run.status), JSON.stringify(run));
  assert.equal(run.workflow, "signal_then_sleep");

  const sig = await c.signal(runId, "go", { ok: true });
  assert.equal(sig.accepted, true);

  // Wait until the run is parked on its 1-day timer.
  const deadline = Date.now() + 30_000;
  for (;;) {
    const h = await c.history(runId);
    if (hasKind(h, "timer_started")) break;
    assert.ok(Date.now() < deadline, `never parked: ${kinds(h)}`);
    await sleep(50);
  }

  const skip = await c.devSkip({ by: "24h" });
  assert.ok(skip.skipped_ms >= DAY_MS, JSON.stringify(skip));

  const res = await c.result(runId, { wait: "30s" });
  assert.equal(res.completed, true, JSON.stringify(res));
  assert.equal(res.status, "completed");
  assert.deepEqual(res.result, { ok: true });

  const h = await c.history(runId);
  assert.ok(hasKind(h, "timer_fired"), `${kinds(h)}`);
  assert.ok((await c.devTime()).offset_ms >= DAY_MS);

  await assert.rejects(c.getRun(unknownId(runId)), (e) => e instanceof OstroError && e.status === 404);

  console.log(`[wall] ts e2e scenario: ${((Date.now() - t0) / 1000).toFixed(2)}s`);
});
