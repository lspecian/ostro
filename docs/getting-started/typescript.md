# Getting started: TypeScript

This guide takes a TypeScript workflow from an empty directory to a deployed, running and
tested workflow. Every command here is one that `scripts/clean-machine.sh` runs on a machine
with no language toolchain installed, or that the generated project's own test runs. For the
overview of all three languages, see [getting-started.md](../getting-started.md). Rust and
Python have their own guides: [rust.md](rust.md), [python.md](python.md).

## 1. Prerequisites

You need only the `ostro` binary ([install.md](../install.md)). `ostro build` downloads what
it needs on first use (Node 24.13.0 and the pinned jco 1.42.0 lockfile, about 750 MB), pins it
and checks its sha256. Your own Node is not used for the build. The module runs on QuickJS
inside the component. See [toolchains.md](../toolchains.md).

```sh
ostro --version
ostro doctor                    # what is cached and what will be downloaded
```

## 2. Create a project

```sh
ostro init hello --lang ts
cd hello
```

The project has one workflow, `hello`, in `src/index.ts`. It runs a step, sleeps for three
days on a durable timer, then waits for the signal `approve`:

<!-- ostro:snippet path=crates/ostro-build/templates/ts/src/index.ts lines=6-30 -->
```typescript
import { workflow, step } from "ostro";
import type { Ctx } from "ostro";

// jco takes the component's exports from the entry module: always re-export `workflows`.
export { workflows } from "ostro";

interface HelloInput {
  name: string;
}

interface Approval {
  ok?: boolean;
}

const THREE_DAYS_MS = 3 * 24 * 60 * 60 * 1000;

// Greets, sleeps for three days (a durable timer: it survives restarts and deploys),
// then waits for the `approve` signal (no timeout).
workflow({ name: "hello" }, async (ctx: Ctx, input: HelloInput) => {
  // A durable step: it runs at most once to a recorded outcome; replays return it.
  const greeting = await step("greet", () => `hello ${input.name}`);
  await ctx.sleep(THREE_DAYS_MS);
  const approval = await ctx.waitSignal<Approval>("approve");
  return { greeting, approved: approval?.ok === true };
});
```

The entry module must re-export `workflows`. `ostro.toml` is the project manifest; besides the
project name and language it holds:

<!-- ostro:snippet path=crates/ostro-build/templates/ts/ostro.toml lines=6-11 -->
```toml
[build]
# auto | hermetic | system | docker
toolchain = "auto"

[deploy]
activate = true
```

## 3. Start a dev server

In a first terminal:

```sh
ostro dev --ephemeral --time-skipping
```

This serves the API, gRPC and the UI on `http://127.0.0.1:7700`. `--ephemeral` keeps all state
in a throwaway directory, and `--time-skipping` jumps the clock to the next due timer whenever
the server is idle, so the three-day sleep takes no time. A plain `ostro dev` keeps its data in
`./.ostro` and really sleeps. `ostro dev` runs with `[auth] mode = "none"`: no token is needed
on your own machine. `ostro dev --watch` also rebuilds and redeploys the project on every save.

The other commands find the server through `--server` / `OSTRO_SERVER` (default
`http://127.0.0.1:7700`).

## 4. Build and deploy

In a second terminal, inside `hello/`:

```sh
ostro build                     # -> target/ostro/hello.wasm (a Wasm component)
ostro deploy                    # builds if stale, uploads, activates
```

The workflow declares no version, so it is version 1. Versions are write-once on a persistent
server: declare `workflow({ name: "hello", version: 2 }, …)` when you change the code
([getting-started.md](../getting-started.md#changing-a-workflow)).

## 5. Trigger a run

From the CLI:

```sh
ostro run hello --input '{"name":"ada"}'            # prints the run id
ostro signal <run-id> approve --payload '{"ok":true}'
ostro result <run-id> --wait                        # prints the output once the run completes
```

`ostro run hello --input '{"name":"ada"}' --wait` starts the run and waits for its result in
one command (send the signal from another terminal meanwhile).

Over HTTP, the same run is:

```sh
curl -sS -X POST http://127.0.0.1:7700/v1/workflows/hello/runs \
  -H 'content-type: application/json' -d '{"input":{"name":"ada"}}'
# {"run_id":"…","created":true}
curl -sS -X POST "http://127.0.0.1:7700/v1/runs/<run-id>/signals/approve" \
  -H 'content-type: application/json' -d '{"payload":{"ok":true}}'
curl -sS "http://127.0.0.1:7700/v1/runs/<run-id>/result?wait=30s"
```

Against a server with authentication on (`ostro server` defaults to `[auth] mode = "token"`),
pass a bearer: the CLI reads it from `OSTRO_TOKEN`, and curl needs
`-H "Authorization: Bearer $OSTRO_TOKEN"`. A token for this needs the `trigger` scope (and
`read` to fetch the result); see [tenancy-and-auth.md](../tenancy-and-auth.md).

## 6. Observe

```sh
ostro ls                        # recent runs
ostro inspect <run-id>          # one run: status, workflow, version, output
ostro history <run-id>          # the run's journal, event by event
```

The embedded UI is at <http://127.0.0.1:7700/ui/>: runs, histories, the dead-letter queue
and workflows ([ui.md](../ui.md)). `console.*` output inside the workflow goes to the run's log
(the run page in the UI and `GET /v1/runs/{id}/logs`, see [observability.md](../observability.md)).

## 7. Test with time skipping

The project comes with a `node --test` test. `TestEnv.start()` from the TypeScript test kit
starts a throwaway `ostro dev --ephemeral --time-skipping` server, and `buildAndDeploy` deploys
the project:

<!-- ostro:snippet path=crates/ostro-build/templates/ts/test/workflow.test.js lines=17-36 -->
```js
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
```

```sh
npm test                        # needs the ostro binary on PATH (or OSTRO_BIN) and a Node
```

Any Node on your PATH runs the test (the hermetic Node in the Ostro cache works too). Until
`ostro-client` is on npm, the test imports the kit from the SDK bundle that `ostro build` writes
under `target/ostro/sdk/clients/ts`. The kit API is described in [testing.md](../testing.md).

## 8. Before you change the code: replay

Replay the new build against recorded runs before you deploy it. A divergence exits with
status 4:

```sh
ostro build && ostro replay --sample 20 --module target/ostro/hello.wasm
```

The SDK makes `Date.now()`, `Math.random()` and `crypto.getRandomValues` deterministic;
[determinism.md](../determinism.md) lists the rules and shows how `ctx.patched` makes an
intentional change safe.

## Next steps

- [sdks/ts/README.md](../../sdks/ts/README.md): the TypeScript SDK reference and its event-loop
  model.
- [workflow-semantics.md](../workflow-semantics.md), [connectors.md](../connectors.md),
  [webhooks.md](../webhooks.md), [cron.md](../cron.md), [kafka-triggers.md](../kafka-triggers.md).
- [install.md](../install.md) and [operations-sizing.md](../operations-sizing.md): run a real
  server.
