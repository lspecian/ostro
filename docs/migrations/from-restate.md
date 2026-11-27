# Migrating from Restate

This guide is for teams running Restate services, virtual objects and workflows. It
contrasts the two execution models, maps Restate's building blocks to Ostro's (and says
where there is no equivalent), and ends with a cut-over plan.

The Restate side is described from its public documentation, in general terms. Check the
details for your SDK and server version. See the [index](index.md) for prerequisites.

## The model difference

In Restate your code stays in **your services**. You write handlers with a Restate SDK,
deploy the service wherever you like (a container, a serverless function), register the
deployment with the Restate server, and the server invokes your handlers, journals their
progress and replays the journal when a handler has to resume. Your handlers have whatever
network access, libraries and credentials your service has.

In Ostro the server **hosts** your workflow code. You upload a WebAssembly component
(Rust, Python or TypeScript, built by `ostro deploy`); the server, or an `ostro worker`
process, runs it. Workflow code has no direct network or file access. Computation goes into
**steps** (journaled, returned on replay) and I/O into **effects**: HTTP, SQL and
object-store operations that the server performs with a stable idempotency key per effect,
egress rules and secret templating ([Connectors](../connectors.md)).

What this means in practice:

- There is no service of yours for Ostro to call into; the workflow is the deployable.
  Business logic that must stay in a service (a library or protocol beyond HTTP, SQL and
  object storage) is called from the workflow over HTTP.
- Ostro has no per-key state store. A run's state is its local variables, rebuilt by
  replay. Data that outlives runs belongs in your database, written with SQL effects.
- Versions are explicit: a module's workflow versions are write-once, a run stays pinned
  to the module it started on, and `ostro replay` / `ostro migrate-runs` handle code changes
  under live runs ([Workflow semantics §10](../workflow-semantics.md#10-changing-workflow-code-versions-and-patched)).

## Concept mapping

| Restate | Ostro | Reference |
|---|---|---|
| Service (stateless handlers) | Workflows in an uploaded module; each invocation is a **run** | [Workflow semantics §1](../workflow-semantics.md#1-starting-a-run-and-the-journal) |
| Deployment registration | `ostro deploy` (upload, optionally signed, then activate) | [Getting started](../getting-started.md) |
| Handler invocation over HTTP ingress | `POST /v1/workflows/{name}/runs` (REST), gRPC, `ostro run`, signed [webhooks](../webhooks.md), [Kafka triggers](../kafka-triggers.md), [cron schedules](../cron.md) | [§1](../workflow-semantics.md#1-starting-a-run-and-the-journal) |
| Idempotency key on an invocation | `idempotency_key` on the start (`return_existing` or `reject`); held until the run expires by retention | [§1](../workflow-semantics.md#1-starting-a-run-and-the-journal), [§12](../workflow-semantics.md#12-retention) |
| Awaiting an invocation's result | `GET /v1/runs/{id}/result?wait=30s`, `ostro run --wait`, `ostro result --wait` | [§11](../workflow-semantics.md#11-continue-as-new) |
| `ctx.run` (journaled side effect) | A step for computation; an effect (`ctx.http()`, `ctx.sql(...)`, `ctx.object(...)`) for I/O | [Connectors](../connectors.md) |
| Retries of failed side effects | Step retry policies and HTTP effect retry policies, with non-retryable error kinds | [examples/order-saga](../../examples/order-saga/README.md), [Connectors §2.2](../connectors.md#22-retries) |
| Durable timers, `ctx.sleep` | `ctx.sleep` / `ctx.sleep_until`; a sleeping run holds no memory | [§13](../workflow-semantics.md#13-sleeping-runs-hold-no-memory) |
| Virtual object (keyed, exclusive handlers, K/V state) | No direct equivalent. Either one long-lived **entity run per key** (idempotent start keyed by the object key, method calls as signals, state in variables, periodic continue-as-new), or one run per call with a **concurrency key** (see below) | [§2](../workflow-semantics.md#2-signals), [§11](../workflow-semantics.md#11-continue-as-new), [Flow control §3](../flow-control.md#3-fairness-weights-priority-bands-keys-d-w10) |
| Virtual object shared (read-only) handlers | `GET /v1/runs/{id}`: the JSON status the run last published with `ctx.set_status` | [Awakeables](#awakeables-and-durable-promises) (the `tokens` example) |
| `ctx.get` / `ctx.set` keyed state | No state API. Variables of the run; your database through SQL effects (optionally exactly-once) | [Connectors §3](../connectors.md#3-sql-sql-execute) |
| Workflow (`run` handler, once per workflow id) | A run started with the workflow id as its idempotency key | [§1](../workflow-semantics.md#1-starting-a-run-and-the-journal) |
| Workflow shared handlers resolving **durable promises** | **Signals**: named, FIFO per name, buffered until the workflow waits, deduplicated by `request_id`, optional wait timeout | [§2](../workflow-semantics.md#2-signals), [§3](../workflow-semantics.md#3-wait-timeouts) |
| **Awakeable** (id handed to an external system, resolved later) | **Await token**: `ctx.create_await_token`, completed once with `POST /v1/tokens/complete`; optional timeout. There is no reject call: put an error in the payload | [§9](../workflow-semantics.md#9-await-tokens) |
| Request-response call to another service | A child run (`ctx.start_child`, op completes with the child's outcome) for Ostro workflows; an HTTP effect for anything else | [Child workflows](../children.md) |
| One-way send | A detached child run (`ChildOptions::detached()`), or an HTTP effect | [Child workflows: Detached children](../children.md#detached-children-d-p9-10) |
| Delayed send | A start with `delay_ms`; or a child that sleeps first | [§1](../workflow-semantics.md#1-starting-a-run-and-the-journal) |
| Cancelling an invocation | Cooperative cancel (`ostro cancel`), `ctx.shielded` for cleanup; terminate for a hard stop | [§5](../workflow-semantics.md#5-cancel-graceful), [§6](../workflow-semantics.md#6-terminate-hard-stop) |
| Introspection, admin UI | `ostro ls`, `ostro inspect`, `ostro history`, tags, the DLQ and the embedded UI | [Run operations](../operations.md), [UI](../ui.md) |
| Running your service on more machines | `ostro worker` processes run the same modules; queues place work on them | [Workers](../workers.md) |

## Virtual objects: two Ostro shapes

A virtual object gives you, per key, serialized handler calls and durable state. Ostro
offers two shapes; pick by what you need from the object.

**Entity run per key** (serialization and state). Start one run per object key with the
key as its idempotency key; a second start returns the same run, so every caller can
"start or find" it. Each method call is a signal; the run processes signals one at a time
in arrival order and keeps its state in variables. Reads go through the status it
publishes. When the history grows, it continues as new with its state as input. A run that
waits for signals in a loop, from the `signals` test guest:

<!-- ostro:snippet path=guests/signals/src/lib.rs lines=36-42 -->
```rust
        "fifo" => {
            let mut got = Vec::with_capacity(3);
            for _ in 0..3 {
                got.push(ctx.wait_signal::<Value>("x", None).wait()?);
            }
            Ok(Value::Array(got))
        }
```

Continuing as new with the state as the next input, from the `can-chain` test guest:

<!-- ostro:snippet path=guests/can-chain/src/lib.rs lines=17-26 -->
```rust
#[workflow(name = "can-chain", version = 1)]
pub fn can_chain(ctx: &Ctx, input: Input) -> Result<u64, WorkflowError> {
    if input.n < input.max {
        return Err(ctx.continue_as_new(&Input {
            n: input.n + 1,
            max: input.max,
        }));
    }
    Ok(input.n)
}
```

A caller that needs a reply sends an await token with its signal payload, and the entity
completes it (or publishes the answer with `ctx.set_status`). Signals to a run that has
ended are refused with 409, so callers can tell when an entity is gone.

**Run per call with a concurrency key** (throughput, no shared state). Start a run per
call with `concurrency_key` set to the object key (a field of the REST and gRPC start
request) on a queue with `key_concurrency = 1`. Be aware of what this does and does not
give you: the limit counts **leased activations**, so a run that sleeps or waits releases
its slot, and another run for the same key can execute in between. It is not mutual
exclusion across a whole call, and the counters are kept per node
([Flow control §2](../flow-control.md#2-the-concurrency-unit-d-w13)). Use it to keep a hot
key from hogging capacity, not to protect shared state; keep that state in your database
with SQL effects.

## Awakeables and durable promises

An awakeable maps to an await token. The workflow creates the token, hands it to the
external system (here it publishes it as status; usually an HTTP effect sends it), and
waits, with an optional timeout:

<!-- ostro:snippet path=guests/tokens/src/lib.rs lines=22-32 -->
```rust
#[workflow(name = "tokens", version = 1)]
pub fn tokens(ctx: &Ctx, input: Input) -> Result<serde_json::Value, WorkflowError> {
    let t =
        ctx.create_await_token::<serde_json::Value>(input.timeout_ms.map(Duration::from_millis));
    ctx.set_status(&json!({ "token": t.token() }))?;
    match t.op().wait() {
        Ok(v) => Ok(v),
        Err(e) if e.is_timeout() => Ok(json!("timeout")),
        Err(e) => Err(e.into()),
    }
}
```

The external system completes it once:

```sh
curl -X POST "$OSTRO_SERVER/v1/tokens/complete" \
  -H "authorization: Bearer $OSTRO_TOKEN" -H 'content-type: application/json' \
  -d '{"token": "ot1_...", "payload": {"approved": true}}'
```

The token is a bearer credential: send it only in request bodies, never in URLs. A second
completion answers 409.

A durable promise that a shared handler resolves by name maps to a signal: the workflow
waits on `ctx.wait_signal("name", timeout)` and the client sends
`ostro signal <run> <name> --payload ...`. Unlike a durable promise, a signal name can be
used more than once: each signal satisfies exactly one wait, oldest first.

## A side-by-side sketch

A Restate cart object in the TypeScript SDK (illustrative, not tested here):

<!-- ostro:illustrative -->
```typescript
import * as restate from "@restatedev/restate-sdk";

export const cart = restate.object({
  name: "cart",
  handlers: {
    add: async (ctx: restate.ObjectContext, item: string) => {
      const items = (await ctx.get<string[]>("items")) ?? [];
      items.push(item);
      ctx.set("items", items);
    },
    checkout: async (ctx: restate.ObjectContext) => {
      const items = (await ctx.get<string[]>("items")) ?? [];
      const { id, promise } = ctx.awakeable<boolean>();
      await ctx.run(() => requestApproval(ctx.key, items, id));
      const ok = await promise;
      if (ok) ctx.clear("items");
      return ok;
    },
  },
});
```

In Ostro this is an entity run per cart: started with idempotency key `cart/<id>`, `add`
and `checkout` arrive as signals, `items` is a variable, `requestApproval` is an HTTP
effect that carries an await token, and the approval wait is the token's op. The pieces are
the snippets above; there is no cart example in this repository.

For a run-to-completion Restate workflow, the shape is closer to `examples/order-saga`:
steps with retry policies, a durable timer, a signal raced with cancellation and shielded
compensations ([Migrating from Temporal: Converting a saga](from-temporal.md#converting-a-saga)
walks through it).

## Cut-over

Restate journals cannot be replayed by Ostro. Migrate by routing new work and letting
in-flight invocations finish.

1. **Port** each service's handlers to workflows. Move `ctx.run` blocks that do I/O to
   effects; register HTTP destinations, SQL connections, egress rules and secrets
   ([Connectors](../connectors.md), [Secrets](../secrets.md)).
2. **Choose identities**: the idempotency key per workflow (the Restate workflow id or the
   object key) and the signal names that replace handler calls and durable promises.
3. **Set up tenancy and tokens**: a tenant for the workload, `trigger` tokens for callers,
   `deploy` for CI, `operate` for on-call; tenant quotas if the cluster is shared.
4. **Deploy and gate.** `ostro deploy`; add `ostro replay --sample N` to CI before each
   later activation.
5. **Route new invocations** of stateless services and workflows to Ostro (switch the
   caller's endpoint or feature flag). Callers that waited on the Restate response wait on
   `GET /v1/runs/{id}/result?wait=...` instead.
6. **Move virtual objects key by key.** Export each object's state through a handler you
   add for that purpose, start the Ostro entity run with that state as input and the object
   key as idempotency key (rerunning the export is harmless), then route that key's calls
   to Ostro. Do it while no exclusive call for the key is in flight.
7. **Drain Restate**: wait for in-flight invocations and pending awakeables to finish (or
   cancel them), then remove the deployments.

## Limits and non-goals

- No hosting of handlers in your own services; I/O is limited to the HTTP, SQL and
  object-store effects.
- No keyed state API and no exclusive per-key handler lock; use an entity run or your
  database.
- Await tokens cannot be rejected; encode failures in the payload.
- No journal import from Restate.
- SDKs exist for Rust, Python and TypeScript.
