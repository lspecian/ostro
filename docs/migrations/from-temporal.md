# Migrating from Temporal

This guide is for teams running Temporal workflows and activities in their own worker
processes. It maps Temporal's concepts to Ostro's, lists what has no equivalent, converts a
small saga side by side and describes a cut-over in which runs already started finish on
Temporal while new runs start on Ostro.

The Temporal side is described from its public documentation, in general terms. Check the
details for your SDK and server version. See the [index](index.md) for prerequisites.

## The model difference

In Temporal you run **workers**: processes that poll task queues on the Temporal Service
and execute your workflow code and your activity code. Activities are ordinary functions in
your language, with any library, network access and credentials your worker has.

In Ostro you **upload** a workflow module (a WebAssembly component built from Rust, Python
or TypeScript) and the server executes it. Everything a Temporal activity does splits into
two kinds of Ostro code, both inside the uploaded module:

- **steps**: computation whose result is journaled and returned on replay instead of
  re-executed (a step body cannot do I/O, it has no network or file access);
- **effects**: HTTP requests, SQL statements and object-store operations that the host
  performs on the workflow's behalf, with a stable idempotency key per effect, egress
  rules and secret templating ([Connectors](../connectors.md)).

There are no user-operated activity workers. `ostro worker` processes exist, but they run
the same uploaded modules for capacity; they never perform effects or see secrets
([Workers §6](../workers.md#6-effects-stay-on-the-leader-d-w3-user-review)). If an activity
needs a library or protocol that is not HTTP, SQL or object storage, keep that code in a
service you operate and call it over HTTP as an effect.

## Concept mapping

| Temporal | Ostro | Reference |
|---|---|---|
| Workflow definition | A function exported from a module: `#[workflow(name = "order", version = 1)]` (Rust), `@ostro.workflow(...)` (Python), `workflow({...}, ...)` (TypeScript) | [Getting started](../getting-started.md) |
| Workflow ID, ID reuse and conflict policy | Ostro generates the run id; your business id goes into the start's `idempotency_key`, with `conflict_policy` `return_existing` (default) or `reject` (409). The key is free again once the run is deleted by retention | [Workflow semantics §1](../workflow-semantics.md#1-starting-a-run-and-the-journal), [§12](../workflow-semantics.md#12-retention) |
| Activity | A **step** (pure computation, journaled) or an **effect** (`ctx.http()`, `ctx.sql(...)`, `ctx.object(...)`) executed by the host, not by a user worker | [Connectors](../connectors.md) |
| Local activity, side effect | A step | [examples/order-saga](../../examples/order-saga/README.md) |
| Activity retry policy | Step retry policy `#[step(retry(max_attempts, initial, max, backoff, non_retryable))]`; HTTP effects take a `RetryPolicy`; set them explicitly | [Connectors §2.2](../connectors.md#22-retries) |
| Activity StartToClose timeout | Step timeout (`StepOptions::timeout`); HTTP effect timeout | [Connectors §2.3](../connectors.md#23-timeouts-and-size-cap) |
| Activity heartbeats, long-running activities | No equivalent. Long work runs outside: the workflow hands an **await token** to the external system and waits for its completion | [Workflow semantics §9](../workflow-semantics.md#9-await-tokens) |
| Asynchronous activity completion | Await tokens: `ctx.create_await_token`, completed with `POST /v1/tokens/complete` | [Workflow semantics §9](../workflow-semantics.md#9-await-tokens) |
| Signal | Signal: `POST /v1/runs/{id}/signals/{name}`, `ctx.wait_signal::<T>(name, timeout)`; FIFO per name, buffered until waited for, dedupe by `request_id` | [Workflow semantics §2](../workflow-semantics.md#2-signals) |
| Signal-With-Start | An idempotent start followed by the signal: the start with your business key returns the existing run if there is one, and a signal sent before the first wait is buffered | [§1](../workflow-semantics.md#1-starting-a-run-and-the-journal), [§2](../workflow-semantics.md#2-signals) |
| Query | No in-workflow query handlers. Clients read `GET /v1/runs/{id}` (status, and the JSON document the workflow last published with `ctx.set_status`), the result (`GET /v1/runs/{id}/result`, long poll) and the history (`ostro history`) | [§11](../workflow-semantics.md#11-continue-as-new) (long poll) |
| Update | No equivalent. Send a signal and wait for the published status, or give the caller an await token for the reply | [§2](../workflow-semantics.md#2-signals), [§9](../workflow-semantics.md#9-await-tokens) |
| Timer, `sleep` | `ctx.sleep(d)` / `ctx.sleep_until(t)`; durable, a sleeping run holds no memory | [§13](../workflow-semantics.md#13-sleeping-runs-hold-no-memory) |
| `condition` with timeout, `Promise.race`, selectors | `wait_signal` with a timeout; `ostro_sdk::select!` (race) and `join!` (gather); the winner is the earliest completion in the journal | [§3](../workflow-semantics.md#3-wait-timeouts), [§4](../workflow-semantics.md#4-concurrency-gather-and-race) |
| Cancellation, non-cancellable scopes | Cooperative cancel: waits return `Err(cancelled)`; `ctx.shielded(...)` runs cleanup to completion; `ctx.cancellation()` as an op | [§5](../workflow-semantics.md#5-cancel-graceful) |
| Terminate | Terminate: immediate, no guest code runs | [§6](../workflow-semantics.md#6-terminate-hard-stop) |
| Workflow execution timeout / run timeout | `run_timeout` in the workflow attribute; the run ends `timed_out`, resumable | [§8](../workflow-semantics.md#8-run-timeout-and-schedule-to-start-timeout) |
| Workflow task / schedule-to-start timeouts | `schedule_to_start_timeout` bounds the wait for the first activation | [§8](../workflow-semantics.md#8-run-timeout-and-schedule-to-start-timeout) |
| Workflow retry policy, reset | No automatic workflow-level retry. `ostro retry` resumes (same run, replayed) or restarts; `ostro rerun --from-step` starts a new run from a journal point; the DLQ lists failed runs | [§7](../workflow-semantics.md#7-retry-resumed-vs-restarted), [Run operations](../operations.md) |
| Child workflow | Child run: `ctx.start_child(workflow, input, ChildOptions)`; the op completes with the child's outcome | [Child workflows](../children.md) |
| Parent Close Policy | Attached children get a graceful cancel when the parent closes (closest to `REQUEST_CANCEL`); `ChildOptions::detached()` is closest to `ABANDON`; there is no terminate-on-close option | [Child workflows: cancel-op and parent close](../children.md#cancel-op-and-parent-close-d-p9-11-pf-f3) |
| Continue-As-New | `return Err(ctx.continue_as_new(&next))`; `ctx.continue_as_new_suggested()`; lineage `continued_from` / `continued_to`; `--follow` follows the chain | [§11](../workflow-semantics.md#11-continue-as-new) |
| Task queue | Queue, with placement on the embedded executor or on `ostro worker` processes, concurrency, keyed fairness, priority, throttles, rate limits and pause | [Flow control](../flow-control.md), [Workers §3](../workers.md#3-placement-work-03-work-04) |
| Worker | The server's embedded executor, or `ostro worker` (stateless, runs uploaded modules) | [Workers](../workers.md) |
| Versioning: `patched` / `GetVersion` | `ctx.patched("change-id")`, same idea: `false` while replaying a journal from before the change | [§10](../workflow-semantics.md#10-changing-workflow-code-versions-and-patched) |
| Versioning: Worker Versioning, task-queue routing per build | Immutable workflow versions; a run stays pinned to the module it started on, new runs use the active version; `ostro replay` checks a module against recorded journals in CI; `ostro migrate-runs` moves live runs to a new version only after a dry-run replay of each run's journal | [Replay](../replay.md), [Upgrade runbook §3](../upgrade-runbook.md#3-ostro-migrate-runs-move-live-runs-to-a-new-version) |
| Replay tests (`WorkflowReplayer` and friends) | `ostro replay --module ... --sample N` (server-side, exit 4 on divergence) or `--corpus` offline | [Replay](../replay.md) |
| Search attributes, list filters | Tags set at start (`--tag k=v`), searched with `ostro ls --tag ...` (AND, at most 8 tags); filters by workflow, queue, status, DLQ | [Run operations: Search](../operations.md#search) |
| Memo | Tags or the run input; published status for values that change | [Run operations: Search](../operations.md#search) |
| Namespace | Tenant: names, idempotency keys and schedules are tenant-scoped; tokens carry a tenant and scopes; per-tenant quotas | [§14](../workflow-semantics.md#14-tenancy-and-names-d23), [Tenancy and authentication](../tenancy-and-auth.md) |
| Schedules | Schedules (cron, time zone, overlap and catch-up policies) | [Cron](../cron.md) |
| Retention period | Per-run retention resolved at start (request, workflow, queue, server default) | [§12](../workflow-semantics.md#12-retention) |
| Test server with time skipping | `ostro dev --ephemeral --time-skipping` | [Getting started](../getting-started.md) |
| Web UI | The embedded UI | [UI](../ui.md) |

## Determinism rules compared

Both systems rebuild a workflow by replaying its history, so both need deterministic
workflow code. The rules map closely:

| Rule | Temporal | Ostro |
|---|---|---|
| Current time | The SDK's workflow time | `ctx.now()`: journaled workflow time, identical on replay |
| Randomness, UUIDs | The SDK's deterministic helpers | `ctx.random_u64()`, `ctx.random_bytes(n)`, `ctx.uuid_v7()`: seeded from the journal |
| I/O | Only in activities | Only through steps and effects. The sandbox has no sockets or files: a module that imports them is rejected at deploy |
| Threads, global mutable state | Not allowed (SDK-specific sandboxing) | No threads in the component; concurrency only through `select!` / `join!` over ops |
| Changing code under live runs | `patched` / `GetVersion`, or route new runs to new workers | `ctx.patched`, plus pinned runs: a version is write-once and a live run keeps its module |
| Catching a mistake before it ships | Replay tests | `ostro replay` against recorded runs on the server |

A divergence at run time fails the run with `nondeterminism` (resumable after a fix, and
listed in the DLQ), rather than blocking a workflow task.

## Converting a saga

A small order saga in Temporal's TypeScript SDK (illustrative, not tested here):

<!-- ostro:illustrative -->
```typescript
import { proxyActivities, defineSignal, setHandler, condition, sleep, CancellationScope } from '@temporalio/workflow';
import type * as activities from './activities';

const { reserveInventory, chargePayment, refundPayment, releaseInventory, createShipment } =
  proxyActivities<typeof activities>({
    startToCloseTimeout: '30s',
    retry: { maximumAttempts: 3, initialInterval: '200ms', backoffCoefficient: 2,
             maximumInterval: '2s', nonRetryableErrorTypes: ['CardDeclined'] },
  });

export const approve = defineSignal<[boolean]>('approve');

export async function orderSaga(order: Order): Promise<string> {
  let approved: boolean | undefined;
  setHandler(approve, (ok) => { approved = ok; });
  const reservation = await reserveInventory(order);
  const payment = await chargePayment(order);
  try {
    await sleep('1s');
    if (!(await condition(() => approved !== undefined, '24h')) || !approved) {
      throw new Error('not approved');
    }
    return await createShipment(order);
  } catch (err) {
    await CancellationScope.nonCancellable(async () => {
      await refundPayment(payment);
      await releaseInventory(reservation);
    });
    throw err;
  }
}
```

The same saga is `examples/order-saga` in this repository (Rust SDK, tested end to end
through the CLI by `crates/ostro/tests/order_saga.rs`).

**Activities with a retry policy become steps with a retry policy.** The policy sits on the
step:

<!-- ostro:snippet path=examples/order-saga/src/lib.rs lines=104-123 -->
```rust
#[step(retry(
    max_attempts = 3,
    initial = "200ms",
    max = "2s",
    backoff = 2.0,
    non_retryable = ["card_declined"]
))]
fn charge_payment(order: &Order) -> Result<String, StepError> {
    if order.fail_at.as_deref() == Some("charge_payment") {
        // Transient: retried by the policy until the attempts run out.
        return Err(StepError::retryable(
            "gateway_unavailable",
            "payment gateway unavailable (simulated)",
        ));
    }
    if order.amount == 0 {
        return Err(StepError::non_retryable("card_declined", "zero amount"));
    }
    Ok(format!("pay-{}-{}", order.order_id, order.amount))
}
```

In the example the steps are simulations. In a real migration, a step that called a
payment API becomes an HTTP effect issued from the workflow (`ctx.http().post(...)`, with
`ctx.idempotency_key("charge")` when the provider wants the key in the body; see
[Connectors §2.1](../connectors.md#21-idempotency-key)), and the step keeps only the
computation around it.

**The timer and the signal wait.** Instead of a signal handler that sets a variable and a
`condition`, the workflow waits on the signal directly, with a timeout, and races it with
the run's cancellation:

<!-- ostro:snippet path=examples/order-saga/src/lib.rs lines=199-222 -->
```rust
    let settle = order.settle_ms.unwrap_or(DEFAULT_SETTLE_MS);
    ctx.sleep(Duration::from_millis(settle)).wait()?;

    let timeout = order
        .approval_timeout_ms
        .unwrap_or(DEFAULT_APPROVAL_TIMEOUT_MS);
    let approval = ctx.wait_signal::<bool>("approve", Some(Duration::from_millis(timeout)));
    let cancellation = ctx.cancellation();
    match ostro_sdk::select!(approval, cancellation) {
        (0, c) => match approval.decode(c) {
            Ok(true) => {}
            Ok(false) => return Err(Abort::Compensate("rejected".into())),
            Err(e) if e.is_timeout() => {
                return Err(Abort::Compensate("approval_timeout".into()));
            }
            Err(e) => return Err(e.into()),
        },
        // The run was cancelled first: close the approval wait so a late signal is not
        // consumed by this run.
        _ => {
            ctx.cancel(&approval);
            return Err(Abort::Cancelled);
        }
    }
```

**Compensations in a non-cancellable scope become `ctx.shielded`:**

<!-- ostro:snippet path=examples/order-saga/src/lib.rs lines=178-192 -->
```rust
/// Reverse compensations, shielded from cancellation so they always run to completion.
fn compensate(ctx: &Ctx, progress: &Progress) -> Result<Vec<String>, WorkflowError> {
    ctx.shielded(|| {
        let mut done = Vec::new();
        if let Some(payment) = &progress.payment {
            refund_payment(payment)?;
            done.push("refund_payment".to_string());
        }
        if let Some(reservation) = &progress.reservation {
            release_inventory(reservation)?;
            done.push("release_inventory".to_string());
        }
        Ok(done)
    })
}
```

**Starting and signalling** move from the Temporal client to the Ostro API or CLI:

```sh
ostro run order-saga --idempotency-key order/o-1 \
  --input '{"order_id":"o-1","amount":4999,"sku":"BOOK-42","qty":1}'
ostro signal <run> approve --payload true
ostro result <run> --wait
```

### Other patterns

A wait for an external system that replies later, with the token published for clients to
read (`published_status` in `GET /v1/runs/{id}`), from the `tokens` test guest:

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

An attached child (its outcome completes the op) and a detached child (the op completes at
once with the child's run id), from the `child-parent` test guest:

<!-- ostro:snippet path=guests/child-parent/src/lib.rs lines=62-68 -->
```rust
#[workflow(name = "parent_attached", version = 1)]
pub fn parent_attached(ctx: &Ctx, input: ParentInput) -> Result<Value, WorkflowError> {
    let out: Value = ctx
        .start_child(&input.workflow, &input.input, input.options())
        .wait()?;
    Ok(json!({ "child": out }))
}
```

<!-- ostro:snippet path=guests/child-parent/src/lib.rs lines=76-82 -->
```rust
#[workflow(name = "parent_detached", version = 1)]
pub fn parent_detached(ctx: &Ctx, input: ParentInput) -> Result<Value, WorkflowError> {
    let run: ChildRun = ctx
        .start_child(&input.workflow, &input.input, input.options().detached())
        .wait()?;
    Ok(json!({ "detached": run.run_id }))
}
```

Continue-as-new, from the `can-chain` test guest:

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

A patch gate, from the `patch-v2` test guest (runs recorded before the change replay the
old path):

<!-- ostro:snippet path=guests/patch-v2/src/lib.rs lines=27-31 -->
```rust
    let mut steps = vec![step_a()?];
    let add_c = ctx.patched("add-c");
    if add_c {
        steps.push(step_c()?);
    }
```

## Cut-over

Temporal histories cannot be imported into Ostro, and an Ostro module cannot replay a
Temporal history. Migrate by **routing new runs** and letting old ones finish.

1. **Port the workflow** and its tests. Use `ostro dev --ephemeral --time-skipping` for
   tests with long timers.
2. **Map identities.** Decide the idempotency key for each workflow (usually the Temporal
   workflow id). Starting the same business id twice must return the same Ostro run.
3. **Move the activities' side effects** to effects: register HTTP destinations and SQL
   connections, egress rules and secrets ([Connectors](../connectors.md),
   [Secrets](../secrets.md)). Keep services the workflow must still call.
4. **Set up tenancy and tokens**: a tenant per namespace you migrate, `deploy` tokens for
   CI, `trigger` tokens for starters, `operate` tokens for on-call
   ([Tenancy and authentication](../tenancy-and-auth.md)). Admin actions land in the
   [audit log](../audit.md); require [signed modules](../signed-modules.md) if CI signs builds.
5. **Deploy and gate.** Deploy the module; add `ostro replay --sample N` to CI so that later
   changes are checked against recorded runs.
6. **Route new starts** for one workflow type to Ostro (a feature flag in the code that
   starts workflows). Signals for a run go to the system that owns it: keep a lookup by
   business id (Ostro runs can be found by tag or by an idempotent start that returns the
   existing run).
7. **Drain Temporal.** Let open Temporal workflows complete. Long-lived ones (entity
   workflows that continue-as-new forever) need an explicit hand-off: at a continue-as-new
   point, have the Temporal workflow start the Ostro run with its state as input (an HTTP
   call from an activity, with the idempotency key) and complete instead of continuing.
8. **Retire** the Temporal workers for that workflow type once no open executions remain.

## Limits and non-goals

- No user-hosted activity workers and no arbitrary code in activities; I/O is limited to
  the HTTP, SQL and object-store effects. Other protocols go through a service you operate.
- No queries, updates or activity heartbeats (see the mapping table for the replacements).
- No history import from Temporal.
- No terminate-on-close option for child runs.
- Search uses tags set at start, not attributes upserted during the run.
- SDKs exist for Rust, Python and TypeScript; there is no Go, Java or .NET SDK.
