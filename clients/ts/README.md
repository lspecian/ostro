# ostro-client (TypeScript / JavaScript)

A hand-written client for the Ostro REST API: ESM JavaScript with hand-written `.d.ts`
types. Node 18+ built-ins only (`fetch`, `AbortController`, `node:crypto`): no runtime
dependencies.

## Install

Once published: `npm install ostro-client`. Until then install from a checkout
(`npm install ./clients/ts`) or from the path inside the SDK bundle.

## Quick start

```js
import { readFileSync } from "node:fs";
import { Client, OstroError } from "ostro-client";

const c = new Client({ server: "http://127.0.0.1:7700" }); // default: $OSTRO_SERVER, else this

await c.uploadModule(readFileSync("module.wasm"), { name: "orders" });
await c.activate("order", 1);

const run = await c.startRun("order", { id: 7 }, { idempotencyKey: "order-7", tags: ["team=shop"] });
await c.signal(run.run_id, "approved", { by: "ana" });
console.log(await c.result(run.run_id, { wait: "30s" })); // { completed: true, status: "completed", result: ... }
```

Responses are the server's JSON objects (field names as in the API, snake_case); see
`src/index.d.ts` and `ostro openapi`.

## Methods

| Method | Route |
| --- | --- |
| `uploadModule(bytes, {name, tenant})` | `PUT /v1/modules/{sha256}` (the SHA-256 is computed for you) |
| `workflows()` | `GET /v1/workflows` |
| `activate(workflow, version)` | `PUT /v1/workflows/{name}/active` |
| `startRun(workflow, input, {idempotencyKey, tags, conflictPolicy, version, queue, extra})` | `POST /v1/workflows/{name}/runs` |
| `getRun(runId)` | `GET /v1/runs/{run_id}` |
| `listRuns({workflow, status, queue, tags, limit, pageToken})` | `GET /v1/runs` |
| `history(runId, {fromSeq, limit})` | `GET /v1/runs/{run_id}/history` |
| `result(runId, {wait})` | `GET /v1/runs/{run_id}/result` (long-poll; `wait` as `"30s"` or ms) |
| `signal(runId, name, payload, {requestId})` | `POST /v1/runs/{run_id}/signals/{name}` |
| `cancel(runId, reason)` | `POST /v1/runs/{run_id}/cancel` |
| `terminate(runId, reason)` | `POST /v1/runs/{run_id}/terminate` |
| `retry(runId)` | `POST /v1/runs/{run_id}/retry` |
| `completeToken(token, payload)` | `POST /v1/tokens/complete` |
| `devTime()` | `GET /v1/dev/time` |
| `devSkip({by})` / `devSkip({toMs})` | `POST /v1/dev/time/skip` |
| `replay(sha, {runs} \| {sample, workflow, status}, allowMissing)` | `POST /v1/modules/{sha}/replay` |

Path values are percent-encoded as one segment, so a tenant-qualified workflow name
(`acme/order`) works as is. `ROUTES` lists every route; a contract test checks it against
the server's OpenAPI document.

## Errors

Every failure rejects with `OstroError`: `status` (HTTP status, `undefined` without a
response), `code`, `message` and `details` from the REST error body
`{"error": {"code", "message", "details"}}`; `e.reason` is `details.reason` when present.
A server that cannot be reached gives `code: "unreachable"`, a timed-out request
`code: "timeout"`, a non-JSON error response `code: "http_<status>"`.

```js
try {
  await c.getRun(runId);
} catch (e) {
  if (e instanceof OstroError && e.status === 404) { /* ... */ }
}
```

## Time skipping (tests)

Against `ostro dev --ephemeral --time-skipping[=manual]` the dev clock can be moved forward
so long sleeps, timers and backoffs finish at once:

```js
await c.devSkip({ by: "72h" });   // or { by: ms } or { toMs }
console.log(await c.devTime());   // { now_ms, offset_ms, mode, idle, next_due_ms }
```

Without time skipping these reject with status 409 and `reason === "time_skipping_disabled"`.
