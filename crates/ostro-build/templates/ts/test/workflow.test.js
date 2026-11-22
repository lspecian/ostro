// Runs `hello` against a throwaway `ostro dev --ephemeral --time-skipping` server: the
// three-day sleep is skipped automatically, so the test finishes in seconds.
//
// Needs the `ostro` binary (OSTRO_BIN or PATH). Run with `npm test` (node --test).
// Until `ostro-client` is published on npm the kit is imported from the SDK bundle that
// `ostro init` / `ostro build` materialise; afterwards: `from "ostro-client/testing"`.

import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";

import { TestEnv } from "../target/ostro/sdk/clients/ts/src/testing.js";

const PROJECT_DIR = fileURLToPath(new URL("..", import.meta.url));
const THREE_DAYS_MS = 3 * 24 * 60 * 60 * 1000;

let env;

before(async () => {
  env = await TestEnv.start();
  await env.buildAndDeploy(PROJECT_DIR);
});

after(async () => {
  await env?.close();
});

test("hello completes after three days and an approval", async () => {
  const run = await env.startRun("hello", { name: "ada" });
  await env.signal(run, "approve", { ok: true });
  // `result` waits for the run; idle time is skipped (the 3-day sleep).
  const res = await env.result(run, { wait: "30s" });
  assert.equal(res.status, "completed");
  assert.deepEqual(res.output, { greeting: "hello ada", approved: true });
  assert.ok((await env.nowMs()) - run.acceptedAtMs >= THREE_DAYS_MS);
});
