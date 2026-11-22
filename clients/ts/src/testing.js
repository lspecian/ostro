// The spawned-server test kit (SDK-06): `TestEnv` starts a throwaway
// `ostro dev --ephemeral --time-skipping` and drives it through the REST `Client`.
//
// The server skips its dev clock forward whenever it is idle and a result long-poll is
// waiting, so `await env.result(run, { wait: "30s" })` on a workflow that sleeps for days
// returns in milliseconds of wall time.
//
//   import { TestEnv } from "ostro-client/testing";
//   const env = await TestEnv.start();
//   await env.buildAndDeploy(".");
//   const run = await env.startRun("hello", { name: "ada" });
//   await env.signal(run, "approve", { ok: true });
//   const res = await env.result(run, { wait: "30s" });
//   await env.close();
//
// Node built-ins only. The binary is OSTRO_BIN, else `ostro` on PATH; its
// `ostro build-info --json` version must be at least this package's version.

import { execFile, spawn, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, delimiter, extname, join } from "node:path";
import { createInterface } from "node:readline";

import { Client, OstroError } from "./client.js";

/** The frozen ready line (D-P10-9, amended); the same pattern in every kit. */
export const READY_LINE_PATTERN = String.raw`^(.+?) listening on http://(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$`;
export const READY_LINE = new RegExp(READY_LINE_PATTERN);

/** Run retention given to the server: skipped days must not expire runs under test. */
export const RETENTION = "87600h";

const KIT_VERSION = "0.1.0";
const READY_TIMEOUT_MS = 30_000;
const STDERR_KEEP = 64 * 1024;

/** A missing/old binary, a server that did not start, or a failed deploy. */
export class TestEnvError extends Error {
  constructor(message) {
    super(message);
    this.name = "TestEnvError";
  }
}

const isFile = (p) => {
  try {
    return statSync(p).isFile();
  } catch {
    return false;
  }
};

/** `binary`, else `$OSTRO_BIN`, else `ostro` on PATH. */
export function ostroBin(binary) {
  if (binary) {
    if (!isFile(binary)) throw new TestEnvError(`the ostro binary ${binary} does not exist`);
    return binary;
  }
  const env = process.env.OSTRO_BIN;
  if (env) {
    if (!isFile(env)) {
      throw new TestEnvError(
        `OSTRO_BIN=${env} does not exist: point OSTRO_BIN at a built ostro binary (cargo build -p ostro) ` +
          "or unset it to use `ostro` on PATH",
      );
    }
    return env;
  }
  const name = process.platform === "win32" ? "ostro.exe" : "ostro";
  for (const dir of (process.env.PATH || "").split(delimiter)) {
    if (dir && isFile(join(dir, name))) return join(dir, name);
  }
  throw new TestEnvError("no `ostro` on PATH and OSTRO_BIN is not set: install ostro or point OSTRO_BIN at the binary");
}

function triple(v) {
  const core = String(v).split(/[-+]/)[0].split(".");
  if (core.length < 2 || core.some((p) => !/^\d+$/.test(p))) return null;
  return [...core.map(Number), 0, 0].slice(0, 3);
}

function older(a, b) {
  for (let i = 0; i < 3; i++) if (a[i] !== b[i]) return a[i] < b[i];
  return false;
}

/** `ostro build-info --json` -> its version; throws when older than the kit. */
export function checkVersion(binary) {
  const r = spawnSync(binary, ["build-info", "--json"], { encoding: "utf8" });
  if (r.error) throw new TestEnvError(`cannot run ${binary}: ${r.error.message}`);
  if (r.status !== 0) throw new TestEnvError(`\`${binary} build-info --json\` failed: ${r.stderr.trim()}`);
  let version;
  try {
    version = String(JSON.parse(r.stdout).version ?? "");
  } catch {
    throw new TestEnvError(`\`${binary} build-info --json\` printed no JSON`);
  }
  const have = triple(version);
  if (!have || older(have, triple(KIT_VERSION))) {
    throw new TestEnvError(`${binary} is version ${JSON.stringify(version)}; this test kit needs ostro >= ${KIT_VERSION}`);
  }
  return version;
}

function serverEnv(extra, egressLoopback) {
  const env = {};
  for (const [k, v] of Object.entries(process.env)) {
    // Inherited server configuration (OSTRO_<SECTION>__*) and RUST_LOG are dropped.
    if (k === "RUST_LOG" || k === "OSTRO_LISTEN" || (k.startsWith("OSTRO_") && k.includes("__"))) continue;
    env[k] = v;
  }
  env.OSTRO_RUNS__RETENTION = RETENTION;
  if (egressLoopback) env.OSTRO_EGRESS__ALLOW_PRIVATE = "127.0.0.0/8";
  return { ...env, ...(extra || {}) };
}

const rid = (run) => (typeof run === "string" ? run : run.runId);

/** A throwaway `ostro dev --ephemeral --time-skipping` server. Use `TestEnv.start()`. */
export class TestEnv {
  /** @private */
  constructor(child, m, binary, cwd, stderr, timeoutMs) {
    this._child = child;
    this.pid = child.pid;
    this.binary = binary;
    this.addr = m[2];
    this.url = `http://${m[2]}`;
    this.dataDir = m[3];
    this.ephemeral = m[4] !== undefined;
    this.readyMs = Number(m[5]);
    this.client = new Client({ server: this.url, timeoutMs });
    this._cwd = cwd;
    this._stderr = stderr;
    this._modules = new Map();
    this._onExit = () => {
      try {
        child.kill("SIGKILL");
      } catch {
        /* already gone */
      }
    };
    process.once("exit", this._onExit);
  }

  /**
   * Spawn the server and wait (30 s) for its ready line.
   * @param {{binary?: string, egressLoopback?: boolean, env?: Record<string,string>,
   *          timeSkipping?: "auto"|"manual", timeoutMs?: number}} [opts]
   */
  static async start({ binary, egressLoopback = false, env, timeSkipping = "auto", timeoutMs = 30_000 } = {}) {
    const exe = ostroBin(binary);
    checkVersion(exe);
    const cwd = mkdtempSync(join(tmpdir(), "ostro-testenv-"));
    const child = spawn(exe, ["dev", "--ephemeral", "--listen", "127.0.0.1:0", `--time-skipping=${timeSkipping}`], {
      cwd,
      env: serverEnv(env, egressLoopback),
      stdio: ["ignore", "pipe", "pipe"],
    });
    const stderr = { text: "" };
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (d) => {
      stderr.text += d;
      if (stderr.text.length > 2 * STDERR_KEEP) stderr.text = stderr.text.slice(-STDERR_KEEP);
    });
    // stdout is drained to the end: the server logs there.
    const lines = createInterface({ input: child.stdout });
    let m;
    try {
      m = await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("printed no ready line within 30 s")), READY_TIMEOUT_MS);
        const done = (f) => (v) => {
          clearTimeout(timer);
          f(v);
        };
        const ok = done(resolve);
        const fail = done(reject);
        lines.on("line", (line) => {
          const r = READY_LINE.exec(line.replace(/\r$/, ""));
          if (r) ok(r);
        });
        child.once("error", (e) => fail(new Error(`could not be spawned: ${e.message}`)));
        child.once("exit", (code) => fail(new Error(`exited (${code}) before printing the ready line`)));
      });
    } catch (e) {
      child.kill("SIGKILL");
      await new Promise((r) => setTimeout(r, 50));
      rmSync(cwd, { recursive: true, force: true });
      throw new TestEnvError(`ostro dev ${e.message} (${exe}); stderr:\n${stderr.text.slice(-4096)}`);
    }
    child.removeAllListeners("exit");
    return new TestEnv(child, m, exe, cwd, stderr, timeoutMs);
  }

  /** SIGTERM, then SIGKILL after 2 s; resolves once the server exited. Idempotent. */
  async close() {
    const child = this._child;
    this._child = null;
    if (child) {
      process.removeListener("exit", this._onExit);
      if (child.exitCode === null && child.signalCode === null) {
        await new Promise((resolve) => {
          const k = setTimeout(() => child.kill("SIGKILL"), 2000);
          child.once("exit", () => {
            clearTimeout(k);
            resolve();
          });
          child.kill("SIGTERM");
        });
      }
    }
    rmSync(this._cwd, { recursive: true, force: true });
  }

  /** The tail of the server's stderr. */
  get stderr() {
    return this._stderr.text;
  }

  _remember(module, workflows) {
    if (!module) return;
    for (const w of workflows) this._modules.set(w, module);
    this._modules.set(module, module);
  }

  // --- deploy ------------------------------------------------------------------------

  /**
   * `ostro deploy <dir> --activate --json --server <url>`: build the project if stale,
   * upload it (module name = the project name) and activate its new versions.
   * The server refuses a changed module under an existing workflow name and version:
   * bump the version in the source to redeploy a change into the same server.
   */
  async buildAndDeploy(projectDir = ".") {
    const out = await new Promise((resolve, reject) => {
      execFile(
        this.binary,
        ["deploy", String(projectDir), "--activate", "--json", "--server", this.url],
        { maxBuffer: 64 * 1024 * 1024, encoding: "utf8" },
        (err, stdout, stderr) => {
          if (err) {
            reject(new TestEnvError(`\`ostro deploy ${projectDir}\` failed (${err.code ?? err.signal}):\n${stderr}${stdout}`));
          } else {
            resolve(stdout);
          }
        },
      );
    });
    const v = JSON.parse(out);
    const b = v.build ?? {};
    this._remember(b.name, (b.workflows ?? []).map((w) => w.name));
    return v;
  }

  /** Upload a prebuilt component as module `name` (default: file stem, `_` -> `-`) and
   * activate every workflow version it declares. */
  async deployWasm(path, { name } = {}) {
    const n = name ?? basename(path, extname(path)).replaceAll("_", "-");
    return this.deployWasmBytes(readFileSync(path), n);
  }

  async deployWasmBytes(bytes, name) {
    const up = await this.client.uploadModule(bytes, { name });
    const wfs = up?.module?.workflows ?? [];
    for (const w of wfs) await this.client.activate(w.name, w.version);
    this._remember(name, wfs.map((w) => w.name));
    return up;
  }

  // --- runs --------------------------------------------------------------------------

  /** Start a run; `opts` are the `Client.startRun` options. Resolves to
   * `{runId, workflow, acceptedAtMs}` (server time). */
  async startRun(workflow, input = null, opts = {}) {
    const started = await this.client.startRun(workflow, input, opts);
    const info = await this.client.getRun(started.run_id);
    return { runId: started.run_id, workflow: info.workflow ?? workflow, acceptedAtMs: Number(info.created_at_ms ?? 0) };
  }

  async signal(run, name, payload = null) {
    return this.client.signal(rid(run), name, payload);
  }

  /** The result (`{run_id, completed, status, result, failure}`) plus `output` (the same
   * value as the wire field `result`), waiting up to `wait` ("30s" or ms; default 30 s);
   * the wait lets the server skip idle time. */
  async result(run, { wait = "30s" } = {}) {
    const r = await this.client.result(rid(run), { wait });
    return { ...r, output: r.result ?? null };
  }

  /** Skip the dev clock forward ("72h" or ms). 503 busy while activations are in flight. */
  async skip(by) {
    return this.client.devSkip({ by });
  }

  /** Server (dev clock) time in ms since the epoch. */
  async nowMs() {
    return Number((await this.client.devTime()).now_ms);
  }

  /** Every history event (all pages): `{seq, kind, ts_ms, summary, details}`. */
  async history(run) {
    const events = [];
    let fromSeq;
    for (;;) {
      const page = await this.client.history(rid(run), { fromSeq });
      events.push(...(page.events ?? []));
      const next = page.next_from_seq;
      if (next === null || next === undefined || next === fromSeq) return events;
      fromSeq = next;
    }
  }

  /** Names of the completed steps (`step_completed` with an `ok` outcome). */
  async steps(run) {
    return (await this.history(run))
      .filter((e) => e.kind === "step_completed" && e.details?.outcome && "ok" in e.details.outcome)
      .map((e) => e.details.name);
  }

  /** Each `timer_started` with its `timer_fired`:
   * `{seq, startedMs, fireAtMs, fired, firedSeq, firedMs}`. */
  async timers(run) {
    const events = await this.history(run);
    const timers = events
      .filter((e) => e.kind === "timer_started")
      .map((e) => ({ seq: e.seq, startedMs: e.ts_ms, fireAtMs: e.details?.fire_at_ms, fired: false, firedSeq: null, firedMs: null }));
    for (const e of events) {
      if (e.kind !== "timer_fired") continue;
      const t = timers.find((t) => t.seq === e.details?.op);
      if (t) Object.assign(t, { fired: true, firedSeq: e.seq, firedMs: e.ts_ms });
    }
    return timers;
  }

  /** Wait for `run` and assert it completed (deep-equal to `output` when given); resolves
   * to the output. */
  async assertCompleted(run, output, { wait = "30s" } = {}) {
    const r = await this.result(run, { wait });
    if (!r.completed) throw new TestEnvError(`run ${rid(run)} did not finish within ${wait} (status ${r.status})`);
    if (r.status !== "completed") {
      throw new TestEnvError(`run ${rid(run)} ended ${r.status}: ${JSON.stringify(r.failure)}`);
    }
    if (output !== undefined) {
      const { deepStrictEqual } = await import("node:assert");
      deepStrictEqual(r.output, output, `output of run ${rid(run)}`);
    }
    return r.output;
  }

  /** Allow a module (or a workflow deployed through this kit) to call `urlPrefix` (an
   * egress rule such as `http://127.0.0.1:8080`), keeping its existing rules. Loopback
   * targets also need `TestEnv.start({ egressLoopback: true })`. */
  async allowEgress(moduleOrWorkflow, urlPrefix) {
    const module = this._modules.get(moduleOrWorkflow) ?? moduleOrWorkflow;
    let allow = [];
    try {
      const cur = await this.client._request(["GET", "/v1/egress/{module}"], ["v1", "egress", module]);
      allow = [...(cur?.allow ?? [])];
    } catch (e) {
      if (!(e instanceof OstroError) || e.status !== 404) throw e;
    }
    const rule = urlPrefix.replace(/\/+$/, "");
    if (!allow.includes(rule)) allow.push(rule);
    return this.client._request(["PUT", "/v1/egress/{module}"], ["v1", "egress", module], { body: { allow } });
  }
}
