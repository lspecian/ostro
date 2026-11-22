# Ostro TypeScript SDK

Write durable Ostro workflows in TypeScript with ordinary `async`/`await`. The SDK targets
the frozen WIT contract `ostro:runtime@0.1.0` (`wit/ostro-runtime-0.1.0/runtime.wit`; a
byte-identical copy ships as `wit/runtime.wit`). A workflow module is a WebAssembly
component built by jco 1.42.0. The default backend is **QuickJS** (bundled
componentize-qjs 0.5.0). **StarlingMonkey** (componentize-js 0.23.0) is a supported
fallback (D-P9-1). A QuickJS module imports only `ostro:runtime/*`, with zero `wasi:`
imports (SDK-03), and exports only `ostro:runtime/workflows@0.1.0`.

- Package: `ostro` 0.1.0, ESM, ships `.ts` sources (`src/`), no dependencies, Node 24 for
  host tests.
- Status: the whole three-language conformance suite passes in TypeScript (27 engine-leg
  and 16 server-leg scenarios on QuickJS, cross-compared with Rust; the StarlingMonkey
  smoke subset passes too; see [docs/conformance.md](../../docs/conformance.md)).
- Not yet published to npm (see [Publishing](#publishing)).

## Build a module

```bash
# Needs Node 24 + npm and uv 0.7.15 (the script checks both). jco and componentize-js
# come from the pinned lock in sdks/scripts/toolchain/ (installed by `npm ci` on first use).
sdks/scripts/build-guest.sh ts    sdks/ts/examples/hello hello.wasm   # QuickJS (default)
sdks/scripts/build-guest.sh ts-sm sdks/ts/examples/hello hello-sm.wasm # StarlingMonkey

ostro dev &
ostro deploy hello.wasm --activate
ostro run hello --input '{"name":"world"}'
ostro result <run-id> --wait
```

The guest directory holds an `index.ts` entry module. The script stages it into a fresh
directory with this SDK as `node_modules/ostro`, with every mtime set to `SOURCE_DATE_EPOCH`,
and runs:

```bash
jco componentize index.ts --wit wit/ostro-runtime-0.1.0 --world-name workflow \
  --backend quickjs --backend-qjs-disable-async --backend-qjs-stub-wasi \
  --bundle-config sdks/scripts/toolchain/rolldown.ostro.mjs -o <out.wasm>
```

`ts-sm` replaces the three QuickJS flags with `--disable all`. The bundle config
(`rolldown.ostro.mjs`) lowers `async` code to ES2016 generators, injects the SDK's
deterministic `console`, timer and `queueMicrotask` globals, and keeps `ostro:*` external.
Phase 10 adds `ostro build`, which wraps the same recipe. Pins and details:
[docs/sdk-builds.md](../../docs/sdk-builds.md).

## A complete example

```ts
import { workflow, step, RetryPolicy, NonRetryableError, OstroError } from "ostro";
import type { Ctx } from "ostro";

// jco takes the component's exports from the entry module: always re-export `workflows`.
export { workflows } from "ostro";

interface Order { orderId: string; amount: number }

workflow({ name: "order", version: 1, queue: "orders" }, async (ctx: Ctx, inp: Order) => {
  const saga = ctx.saga();
  const payment = await step("charge", () => ({ charged: inp.amount, key: ctx.idempotencyKey("charge") }),
    { retry: new RetryPolicy({ maxAttempts: 5 }), timeoutMs: 30_000 });
  saga.add("refund", (s) => s.http.post("https://pay.example/refund").json({ key: payment.key }).send());

  // Wait for approval or give up after one day (both durable).
  const timer = ctx.sleep(24 * 3600 * 1000);
  const approve = ctx.waitSignal<boolean>("approve");
  const winner = await Promise.race([timer.then(() => "timeout" as const), approve.then(() => "approved" as const)]);
  if (winner === "approved") {
    ctx.cancel(timer);                     // losers are never cancelled implicitly
  } else {
    ctx.cancel(approve);
    await saga.failWith(new NonRetryableError("not_approved", "no approval in 24 h"));
  }

  let tracking: string;
  try {
    tracking = await step("ship", () => `shipment-${inp.orderId}`);
  } catch (e) {
    if (e instanceof OstroError) await saga.failWith(e); // runs "refund", then rethrows
    throw e;
  }
  ctx.setStatus({ stage: "shipped" });
  return { tracking, child: await ctx.startChild("notify", { id: inp.orderId }) };
});
```

## API reference

Everything below is exported from `"ostro"`.

| Name | What it does |
|------|--------------|
| `workflow(opts \| name, fn)` | `opts = {name, version?, queue?, runTimeoutMs?}`, `fn(ctx, input)` (usually `async`). A duplicate name throws at module evaluation. |
| `workflows` | The export object jco binds. Re-export it from the entry module. |
| `step(name, fn, opts?)` / `ctx.step(...)` | Durable step. `opts = {retry?, timeoutMs?, fuel?}`. Returns an SDK promise. |
| `RetryPolicy({maxAttempts, initialIntervalMs, maxIntervalMs, backoffCoefficient, nonRetryableKinds})` | Unset fields default to 3 / 1000 / 60000 / 2.0. No `retry` means no host retries. |
| `Ctx` reads | `info()`, `now()` (`Date`), `nowMs()`, `randomU64()` (`bigint`), `randomF64()`, `randomBytes(n)`, `uuid7()`, `isReplaying()`, `continueAsNewSuggested()`, `log(level, msg, fields?)`. Snake-case aliases `random_u64`, `random_f64`, `random_bytes`. |
| `Ctx` journaled calls | `patched(id)`, `setStatus(json)`, `continueAsNew(input, {workflow?, version?})`. |
| `Ctx` ops (each returns a thenable `Op` with a `bigint` `id`) | `sleep(ms)`, `sleepUntil(Date \| epochMs)`, `waitSignal(name, {timeoutMs?})`, `startChild(workflow, input, {queue?, version?, detached?, runTimeoutMs?})`, `createAwaitToken({timeoutMs?}) -> {token, op}`, `cancellation()`. |
| `ctx.cancel(op)` | `cancel-op` (D5). Waiting on the op then rejects with `CancelledError`, unless it completed earlier in journal order. On a child op it also cancels the child run. |
| `ctx.shielded((s) => ...)` | Runs `fn` with a shielded ctx `s` (D11). Ops created **through `s`** are not rejected by run cancellation. There is no AsyncContext on QuickJS or StarlingMonkey, so shielding is explicit. |
| Effects | `ctx.http.get/post/put/delete/request(...)` builder (`.header`, `.secretHeader`, `.json`, `.body`, `.idempotencyKey`, `.timeout`, `.retry`, `.redact`, `.send()`), `ctx.sql(conn).exactlyOnce().query(stmt, params)`, `ctx.object(conn).get/put/head/delete/presignGet/presignPut/raw`. u64 fields are `bigint`. |
| Idempotency | `ctx.idempotencyKey(name)`, `idempotencyKeyFor(runId, name)`: the P-11 key, byte-equal to the Rust and Python SDKs (the SDK's own synchronous sha256, no `crypto.subtle`). |
| Saga | `ctx.saga()` or `new Saga(ctx)`: `add(name, (s) => ...)`, `compensate() -> SagaReport`, `failWith(e)`. |
| Errors | `OstroError` → `NonRetryableError`, `RetryableError`, `TimeoutError`, `CancelledError`, `ProtocolError` / `DeadlockError` (`sdk_protocol`), `InvalidStringError` (`invalid_string`). `NotSupportedError` for unsupported globals. `toFailure` / `fromFailure`. |
| Misc | `SdkPromise`, `CANCELLATION_OP` (`2n ** 64n - 1n`), `encodeUtf8` / `decodeUtf8`, `sha256` / `sha256Hex`, `f64FromU64`, `VERSION`. `__ostro*` exports are bundle-inject targets, not user API. |

**How a workflow ends.** A returned value → `completed` (JSON). `ctx.continueAsNew(...)` →
`continue-as-new`. `CancelledError` / `TimeoutError` / any other `OstroError` → the WIT
failure with that kind. Any other exception → non-retryable `js_exception`. Inside a step
body, a thrown non-`OstroError` is a **retryable** `js_exception`. The SDK catches
everything at the `run` export; anything escaping it would trap the activation.

## The event-loop model

The `run` export is synchronous, so native promises cannot drive a workflow. At module
evaluation the SDK installs its own `SdkPromise` as `globalThis.Promise` and runs a job
queue it drains synchronously. When the queue is empty and the workflow is waiting on
durable ops, it calls `effects.wait`. Reaction ordering follows ECMA-262, so the same
program orders jobs the same way as native promises.

What works: `async`/`await`, `Promise.all`, `Promise.race`, `Promise.allSettled`,
`Promise.any`, `for await`, async generators (all lowered to ES2016 by the bundle config).
`setTimeout` outside a step is one durable sleep op (`clearTimeout` → `cancel-op`).

What does not work:
- sockets, fetch, Node or browser I/O (use effects);
- `setInterval` (throws `NotSupportedError`);
- native engine promises such as `crypto.subtle` or other host-async APIs: they resolve on
  the engine's own microtask queue, which never runs;
- **top-level `await`**: Rolldown does not lower it. It runs at pre-init on the native
  promise, before the SDK exists, so keep module-level code synchronous (spike S9).

The SDK sources avoid `#private` fields and bigint literals because the ES2016 lowering
target handles neither well. Prefer `BigInt(...)` in workflow code too.

## Determinism rules (WF-07)

The SDK installs its determinism surface at module evaluation (D-P9-15):

| Global | Behaviour |
|--------|-----------|
| `Math.random()` | `(host random-u64 >> 11) * 2^-53`, step-local inside steps |
| `crypto.getRandomValues` | host `random-bytes` (≤ 65536 bytes) |
| `crypto.randomUUID` | UUIDv4 from 16 host random bytes |
| `Date.now()`, `new Date()`, `performance.now()` | logical time (`context.now`), identical on replay |
| `setTimeout` / `clearTimeout` | durable sleep / `cancel-op`; `sdk_protocol` inside a step |
| `console.*` | `log.emit` (never journaled, suppressed on replay) |
| `WeakRef` | strong reference (GC timing differs live vs replay) |
| `FinalizationRegistry` | no-op |

`Map`, `Set` and object key iteration are insertion-ordered and deterministic. WF-07
evidence: C-NATIVE-01 (native `Date.now()`, `Math.random()` and `Map` order are
replay-identical and change with the seed) and C-RAND-02 pass on QuickJS.

## Frozen semantics the SDK relies on

- **D5 cancel-op.** `ctx.cancel(op)` completes the op with `CancelledError` unless it
  completed earlier in journal order (first completion wins). `Promise.race` losers are
  never cancelled implicitly.
- **D6 patched.** `ctx.patched("change-id")` is journaled and memoised per run: new runs get
  `true`, replays of old runs get the recorded answer.
- **D7 step-local RNG.** Inside a step body, `Math.random()`, `ctx.randomU64()` and the
  other draws use a per-attempt step-local stream. They never consume the workflow's
  stream.
- **D11 cancellation and shielding.** A graceful cancel makes every unshielded op reject
  with `CancelledError`. Once delivered, the cancellation is remembered. Cleanup goes
  through `ctx.shielded((s) => ...)` with ops created through `s`. Terminate and run
  timeouts are hard stops: no guest code runs.
- **F4 attempt is for logging.** `ctx.info().attempt` grows on every re-dispatch. Log it,
  never branch on it.
- **PF-W1 open-step rule and step serialisation (step exclusivity).** No other task runs
  while a step is open. Steps take an SDK FIFO lock in call order. When a step opens, the
  jobs already queued are set aside, and only jobs the body enqueues run until it settles.
  The set-aside jobs then go back to the front in order, and only then does the caller
  resume. So live runs and replays interleave identically. A step body cannot await
  durable ops. `setTimeout` inside an open step throws `ProtocolError` (`sdk_protocol`,
  D-P9-16). A step whose body waits on anything durable completes non-retryable
  `sdk_protocol`.
- **PF-W2 wait.** Completions come back in journal order, identically on every replay.
- **PF-W3 op ids** are `bigint`, never `Number` (u64 does not fit a double).
- **PF-W4 lone surrogates.** QuickJS traps and StarlingMonkey silently replaces lone
  surrogates at the boundary. The SDK checks every string it sends (names, JSON payloads)
  with `isWellFormed` and rejects it first with non-retryable `invalid_string`.
- **PF-W5 JSON payloads.** Values travel as `application/json`. Empty payload data decodes
  as `null`, and non-finite numbers encode as `null`.
- **PF-W6 retries and fuel portability.** No `retry` means no host retries. Fuel counts
  interpreter instructions: a QuickJS hello activation uses ~0.78M fuel and StarlingMonkey
  ~2.54M, far more than Rust. Size `fuel` per language.
- **PF-W7 child outcomes.** See [Children](#children).
- **PF-W8 eviction is a trap; no module-level host calls.** The host ends an activation by
  trapping inside an import, so `finally` blocks may not run then. Imports trap at
  pre-init, so module-level code must not call `ctx` or effects. Outside an activation,
  randomness and timers throw.
- **PF-W9 random and uuid streams.** `uuid7()` has its own stream, separate from
  `randomU64()`. Values for the same seed and run id are byte-identical across the Rust,
  Python and TS SDKs, on both backends (C-VAL-01).

## Children

`ctx.startChild("wf", input)` starts a child with a deterministic run id derived from
(parent run, op). Outcome mapping (PF-W7): Completed → its output; Failed → the child's
failure rethrown; Cancelled → `CancelledError`; TimedOut → `TimeoutError`; Terminated →
`NonRetryableError` with kind `child_terminated`. A child that continues-as-new is
followed to the last run of its chain.

- `{detached: true}`: the op resolves to `ChildRun` (`runId`) once the start is flushed.
  The child has no parent link and outlives the parent.
- `ctx.cancel(childOp)` rejects the op with `CancelledError` and gracefully cancels the
  child (reason `parent_cancel_op`).
- At any terminal status of the parent, every attached child still running gets a graceful
  cancel (`parent_closed`).
- **The cancel cascade beats shielding.** A shielded await (`s.startChild(...)` inside
  `ctx.shielded`) on an in-flight child still rejects with `CancelledError` when the parent
  is cancelled, because the child run itself is cancelled. A child started inside shielded
  cleanup runs until the parent closes, then receives the parent-close cancel (D-P9-26,
  C-CHILD-11).

Full semantics: [docs/children.md](../../docs/children.md).

## Saga (WF-10)

`saga.add(name, (s) => ...)` registers a compensation that receives the shielded ctx `s`.
`await saga.compensate()` runs the compensations sequentially, last registered first
(LIFO), inside `ctx.shielded`, so a cancelled workflow still compensates. A failing
compensation does not stop the others. `await saga.failWith(e)` compensates and rethrows
`e` with `{"compensated": [...], "compensation_failures": [...]}` in its details, the same
shape as the Rust and Python SDKs.

## Backends

| | QuickJS (default, `ts`) | StarlingMonkey (`ts-sm`) |
|---|---|---|
| Component | 2.3 MB | 13.2 MB |
| Core instances (pool weight) / memories / tables | 13 / 3 / 3 | 3 / 1 / 2 |
| Warm instantiate p50 | 64 µs | 11 µs |
| Idle RSS per parked instance | 1.1 MB | 2.9 MB |
| Conformance | full suite | smoke subset (C-STEP-01, C-RACE-01, C-SIG-01, C-RAND-01, C-CANCEL-01) |
| Reproducible build | residual (see below) | out of scope (D-P9-1) |

Numbers are WSL2 measurements (advisory, D15); full tables and pool-sizing guidance are in
[docs/sdk-resources.md](../../docs/sdk-resources.md). Run QuickJS modules with
`[engine] class = "mixed"` or a `pool_total_core_instances` override. `class = "typescript"`
sizes the pool with the StarlingMonkey figure. Admission charges core instances,
memories and tables, so a full pool makes runs wait instead of failing (ENG-2).

## Reproducibility

Replay never depends on rebuilding: the deployed artefact is the module identity.
QuickJS builds of the same sources differ by a few snapshot bytes per build (entropy in
componentize-qjs), so SC2 for TypeScript is an accepted residual (D-P9-3, human_needed;
the issue text is in `sdks/python/toolchain/README.md`). C-REBUILD-01 shows that journals
recorded on one build replay on another. Details:
[docs/sdk-builds.md](../../docs/sdk-builds.md).

## Publishing

`npm pack --dry-run --json` lists `package.json`, `README.md`, `rolldown.ostro.mjs`,
`src/*.ts` and `wit/runtime.wit`. Publishing to npm is a human step (D-P9-19). Until
then, `build-guest.sh` stages this directory as `node_modules/ostro` in every build.

## Tests

```bash
node --test --test-reporter=tap sdks/ts/test/
cargo test -p ostro-conformance --features polyglot --locked --test polyglot_ts -- --test-threads=2
```
