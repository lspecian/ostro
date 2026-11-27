# Migration guides

These guides help you move existing background work to Ostro. Each one maps the concepts
of the system you run today to Ostro's, says plainly where Ostro has no equivalent, walks
through a small conversion and ends with a cut-over plan that keeps both systems running
until the old one is drained.

| Guide | For you if you run today |
|---|---|
| [From cron jobs and queue tables](from-cron-and-queue-tables.md) | crontab entries or Kubernetes CronJobs, a jobs table polled with `SELECT ... FOR UPDATE SKIP LOCKED`, `attempts` / `run_at` retry columns, a dead-letter table, an outbox table |
| [From Temporal](from-temporal.md) | Temporal workflows and activities in your own worker processes: signals, queries, timers, child workflows, continue-as-new, task queues, patching |
| [From Restate](from-restate.md) | Restate services, virtual objects and workflows as handlers in your own services: keyed state, awakeables, durable promises, service-to-service calls |

## How Ostro differs, in one paragraph

Ostro is one binary that stores, schedules and **executes** your workflows. You upload a
workflow as a WebAssembly component (Rust, Python or TypeScript, built by `ostro deploy`),
and the server runs it, either in its embedded executor or on stateless `ostro worker`
processes that run the same modules. Every run keeps a journal; after a crash or an
eviction the run is rebuilt by replaying that journal. Workflow code has no direct network
or file access: I/O goes through host **effects** (HTTP, SQL, object store) that the
server performs with idempotency keys. This is the main difference to the Temporal and
Restate models, where your own processes run the business code.

## Before you start

- Work through [Getting started](../getting-started.md) once: `ostro init`, `ostro dev`,
  `ostro deploy`, `ostro run`, `ostro signal`.
- Read [Workflow semantics](../workflow-semantics.md). Every mapping in these guides
  cites the section that defines the Ostro behaviour.
- Know which of your jobs call external systems. They become effects
  ([Connectors](../connectors.md)), and their destinations need egress rules
  ([Security and egress](../security-egress.md)) and secrets ([Secrets](../secrets.md)).
- In a server deployment, create a tenant for the migrated workload (`ostro tenant create`)
  and API tokens with only the scopes each client needs (`ostro auth token create --scope
  trigger` for producers, `deploy` for CI, `operate` for on-call). Starts, signals and
  token completions count against the tenant's `event_rate` quota and are refused with 429
  and `Retry-After` when it is exhausted, so every producer must retry with the same
  idempotency key.

## Common building blocks

All three guides lean on the same Ostro features:

| Feature | Reference |
|---|---|
| Idempotent starts (`idempotency_key`, `conflict_policy`) | [Workflow semantics §1](../workflow-semantics.md#1-starting-a-run-and-the-journal) |
| Durable steps with retry policies | [examples/order-saga](../../examples/order-saga/README.md) |
| Signals, wait timeouts, await tokens | [§2](../workflow-semantics.md#2-signals), [§3](../workflow-semantics.md#3-wait-timeouts), [§9](../workflow-semantics.md#9-await-tokens) |
| Child runs and continue-as-new | [Child workflows](../children.md), [§11](../workflow-semantics.md#11-continue-as-new) |
| Code changes: pinned runs, `patched`, `ostro replay`, `ostro migrate-runs` | [§10](../workflow-semantics.md#10-changing-workflow-code-versions-and-patched), [Replay](../replay.md), [Upgrade runbook §3](../upgrade-runbook.md#3-ostro-migrate-runs-move-live-runs-to-a-new-version) |
| Queues: concurrency, keys, priority, throttles, rate limits, pause | [Flow control](../flow-control.md) |
| Cron schedules | [Cron](../cron.md) |
| Webhooks and Kafka triggers | [Webhooks](../webhooks.md), [Kafka triggers](../kafka-triggers.md) |
| DLQ, rerun, bulk operations, tag search | [Run operations](../operations.md) |
| Remote execution on workers | [Workers](../workers.md) |

Code blocks marked `ostro:snippet` are copied byte for byte from files in this repository
that CI builds and tests. Blocks marked `ostro:illustrative` show the system you migrate
from, or pseudo-code; they are not compiled.
