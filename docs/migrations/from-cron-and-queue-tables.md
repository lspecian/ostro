# Migrating from cron jobs and queue tables

This guide is for teams whose background work lives in crontab entries (or Kubernetes
CronJobs) and in a jobs table that workers poll with `SELECT ... FOR UPDATE SKIP LOCKED`,
with `attempts` and `run_at` columns for retries, a dead-letter table for jobs that gave up
and perhaps an outbox table for messages to other systems. It maps each of those pieces to
Ostro, converts two typical jobs, and describes a cut-over that never runs a job twice and
never drops one.

See the [index](index.md) for prerequisites.

## What changes

A cron job or a job-table worker is a process you operate: it holds the retry logic, the
locking, the bookkeeping columns and the crash recovery. In Ostro the server owns all of
that. A job becomes a **run** of a workflow; the run's journal records every completed step,
so a crash re-executes only the step that was in progress, never the ones before it. You
keep writing the business logic, and you delete the plumbing.

## Concept mapping

| You have today | Ostro | Reference |
|---|---|---|
| A crontab line (`30 2 * * * report.sh`) | A **schedule**: `ostro schedules create --name ... --workflow ... --cron ...`. 5 fields, or 6 with seconds; `L`, `#`, `W` | [Cron: Syntax](../cron.md#syntax) |
| `CRON_TZ=` / the host's time zone | `--timezone` with an IANA name; DST gaps and overlaps have pinned behaviour | [Cron: Time zones and DST](../cron.md#time-zones-and-dst) |
| `flock -n` around the job, `concurrencyPolicy: Forbid` | Overlap policy: `skip` (default), `buffer_one`, `allow_all`, `cancel_other`, `terminate_other` | [Cron: Overlap policy](../cron.md#overlap-policy) |
| anacron, `startingDeadlineSeconds`, "did last night's run happen?" | Catch-up policy: `skip` (default), `one` (latest missed fire), `all` (within `--catchup-window`) | [Cron: Catch-up policy](../cron.md#catch-up-policy) |
| Running the script by hand | `ostro schedules trigger <name> --request-id ...` (deduplicated per request id) | [Cron: Pause, resume, trigger-now](../cron.md#pause-resume-trigger-now) |
| Commenting out the crontab line | `ostro schedules pause` / `resume` | [Cron: Pause, resume, trigger-now](../cron.md#pause-resume-trigger-now) |
| One row in the jobs table | One **run**; the row's payload column is the run input | [Workflow semantics §1](../workflow-semantics.md#1-starting-a-run-and-the-journal) |
| The row's primary key / a unique constraint against double enqueue | The start's **idempotency key**: a second start with the same key returns the existing run (`conflict_policy` `return_existing`, or `reject` for 409) | [Workflow semantics §1](../workflow-semantics.md#1-starting-a-run-and-the-journal) |
| `status` column (`pending`, `running`, `done`, `failed`) | Run status: `scheduled`, `ready`, `running`, then `completed`, `failed`, `cancelled`, `terminated`, `timed_out` | [Workflow semantics](../workflow-semantics.md) (conventions) |
| `run_at` for delayed jobs | `delay_ms` on the start request (`ostro run --delay 30s`); inside a run, a durable `ctx.sleep` | [Workflow semantics §1](../workflow-semantics.md#1-starting-a-run-and-the-journal) |
| `attempts`, `max_attempts`, `next_attempt_at` with backoff | A **retry policy** per step (`#[step(retry(...))]`) or per HTTP effect: attempts, initial and max interval, backoff, non-retryable error kinds | [examples/order-saga](../../examples/order-saga/README.md), [Connectors §2.2](../connectors.md#22-retries) |
| Worker pool size, one poller per job type | A **queue** with `concurrency`; `key_concurrency` per `concurrency_key`; `priority` bands; fair-share `weight`s across queues | [Flow control §1-3](../flow-control.md#1-configuring-a-queue) |
| "No more than N per second to the mail provider" | `throttle` (first activations per period) or `rate_limit` (run starts per period, 429 beyond it) | [Flow control §4](../flow-control.md#4-throttle-versus-rate-limit-d-w21-d-w24) |
| `locked_by` / `locked_until`, a reaper for stuck rows | Activation **leases** held by the server; a crashed executor or worker loses its lease and the run is re-dispatched and replayed from its journal | [Workers §7](../workers.md#7-failure-semantics) |
| Stopping a job type during an incident | `ostro queue pause` / `ostro workflow pause` / `ostro run pause` | [Flow control §5](../flow-control.md#5-pause-and-resume) |
| Dead-letter table, a script that re-inserts rows | The **DLQ** view: `ostro dlq ls`, `ostro dlq requeue` (resume, restart or rerun from a step), `ostro dlq dismiss`; optional `on_failure` hook workflow | [Run operations: Dead-letter queue](../operations.md#dead-letter-queue) |
| Pausing a job type after repeated failures | `on_retries_exhausted = "pause"` on the queue or workflow: the run is paused instead of failed, and resumed after the fix | [Flow control §6](../flow-control.md#6-auto-pause-on-exhausted-retries-ops-01) |
| Outbox table drained to HTTP or a message broker | **Effects** (`ctx.http()`, `ctx.sql(...)`, object store) with a stable idempotency key per effect; SQL effects can be exactly-once | [Connectors](../connectors.md) |
| `DELETE FROM jobs WHERE finished_at < now() - interval '7 days'` | **Retention**: every terminal run is deleted after its retention period (default `168h` on `ostro server`) | [Workflow semantics §12](../workflow-semantics.md#12-retention) |
| `SELECT count(*) ... WHERE status = 'pending'` | `GET /v1/queues/{q}/demand` (backlog, running), the `ostro_queue_*` metrics, `ostro ls --tag ...` | [Flow control §7](../flow-control.md#7-observability), [Run operations: Search](../operations.md#search) |
| Bulk "UPDATE jobs SET status = 'pending' WHERE ..." | `ostro bulk retry|requeue|cancel|...` over a filter, paced and resumable | [Run operations: Bulk operations](../operations.md#bulk-operations) |

### What has no direct equivalent

- **Enqueueing in the same database transaction as your business write.** A jobs table
  lets you insert the job row in the transaction that creates the order. An Ostro start is
  a separate API call. Start the run **after** the commit with an idempotency key derived
  from the business row, and retry the call until it succeeds; or keep a small outbox
  table in your database and drain it into idempotent Ostro starts. Either way the key
  makes repeated starts harmless.
- **Arbitrary SQL over the queue.** Runs are listed by workflow, queue, status, tags and
  DLQ membership, not queried with SQL. Put the attributes you filter on into tags at start
  (`--tag customer=c-42`, at most 64 per run).
- **Running a shell script or a binary.** Ostro runs WebAssembly workflow modules with no
  direct file or network access. A job that shells out must be rewritten as a workflow, or
  the script must sit behind an HTTP endpoint you operate that the workflow calls as an
  effect.
- **Unbounded idempotency.** An idempotency key is held until the run it created is
  deleted by retention; after that the same key starts a new run
  ([§12](../workflow-semantics.md#12-retention)). Choose a retention longer than the window
  in which a producer may re-send.

## Worked example 1: a nightly report

Today:

<!-- ostro:illustrative -->
```crontab
CRON_TZ=Europe/Berlin
30 2 * * * flock -n /tmp/report.lock /usr/local/bin/nightly-report --date yesterday
```

The report logic becomes a workflow (any workflow; its steps run once each and are replayed
after a crash). Its entry point has the same shape as every Ostro workflow, for example:

<!-- ostro:snippet path=examples/order-saga/src/lib.rs lines=230-232 -->
```rust
/// The order saga.
#[workflow(name = "order-saga", version = 1)]
pub fn order_saga(ctx: &Ctx, order: Order) -> Result<OrderResult, WorkflowError> {
```

Deploy it and create the schedule. `skip` overlap matches `flock -n`; catch-up `one`
reproduces "after an outage, run the latest missed night once":

```sh
ostro deploy                                  # in the project: build, upload, activate
ostro schedules create --name nightly-report --workflow nightly-report \
  --cron '0 30 2 * * *' --timezone Europe/Berlin \
  --overlap skip --catchup one --input-json '{"date": "yesterday"}'
ostro schedules get nightly-report            # next_fire_at_ms, last_fire_at_ms
ostro ls --tag ostro.schedule=nightly-report  # the runs it started
```

Each nominal fire time starts at most one run, across restarts, leader changes and
crashes: a durable watermark and the idempotency key `__cron/<schedule>/<fire_time_ms>`
guard it ([Cron: Exactly-once](../cron.md#exactly-once)). If a run fails, it shows up in
`ostro dlq ls`, and `ostro dlq requeue <run>` resumes or restarts it. There is no lock file
to clean up.

## Worked example 2: an email job table

Today:

<!-- ostro:illustrative -->
```sql
CREATE TABLE email_jobs (
  id           bigserial PRIMARY KEY,
  recipient    text NOT NULL,
  template     text NOT NULL,
  status       text NOT NULL DEFAULT 'pending',
  attempts     int  NOT NULL DEFAULT 0,
  run_at       timestamptz NOT NULL DEFAULT now(),
  last_error   text
);

-- each worker, in a loop
SELECT id, recipient, template FROM email_jobs
 WHERE status = 'pending' AND run_at <= now()
 ORDER BY run_at
 FOR UPDATE SKIP LOCKED
 LIMIT 1;
-- send, then UPDATE ... SET status = 'done', or attempts = attempts + 1,
-- run_at = now() + backoff, and status = 'dead' after max_attempts
```

### The workflow

Sending becomes an HTTP effect. The server performs it, sends a stable `Idempotency-Key`
header that is the same on every retry, and records the response in the journal. When the
provider expects the key in the body, derive a semantic key in the workflow and pass it in
both places (from the `fx-http` test guest):

<!-- ostro:snippet path=guests/fx-http/src/lib.rs lines=77-91 -->
```rust
    if let Some(sk) = r.semantic_key {
        let key = ctx.idempotency_key(&sk);
        let mut obj = body
            .as_deref()
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(b).ok())
            .and_then(|v| match v {
                serde_json::Value::Object(m) => Some(m),
                _ => None,
            })
            .unwrap_or_default();
        obj.insert(
            "idempotency_key".into(),
            serde_json::Value::String(key.clone()),
        );
        b = b.json(&obj)?.idempotency_key(key);
```

Work that is pure computation (rendering the template, choosing a sender) goes into steps.
A step with a retry policy replaces the `attempts` / `run_at` columns; this one is from
`examples/order-saga`:

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

`StepError::retryable` is retried with backoff; `non_retryable` kinds (here
`card_declined`) fail at once. HTTP effects take their own `RetryPolicy`
([Connectors §2.2](../connectors.md#22-retries)).

### The queue

The worker pool size and the provider's rate limit move to the queue:

```sh
ostro queue flow email --set concurrency=20 \
  --set throttle.limit=50 --set throttle.period_ms=1000 \
  --set on_retries_exhausted=pause
```

`concurrency` bounds activations executing at once (a sleeping run, or one waiting for a
signal, holds no slot), `throttle` paces first activations, and `on_retries_exhausted=pause` parks
runs whose retries ran out until an operator resumes them, instead of failing them into the
DLQ ([Flow control §6](../flow-control.md#6-auto-pause-on-exhausted-retries-ops-01)). Leave
it at `fail` if you prefer the DLQ.

### Enqueueing

Where the producer inserted a row, it starts a run. The idempotency key derives from what
made the job unique (here the old row id or a business id):

```sh
curl -X POST "$OSTRO_SERVER/v1/workflows/send-email/runs" \
  -H "authorization: Bearer $OSTRO_TOKEN" -H 'content-type: application/json' \
  -d '{"input": {"recipient": "a@example.com", "template": "welcome"},
       "idempotency_key": "email-jobs/81723", "queue": "email",
       "delay_ms": 0, "tags": ["source=email-jobs"]}'
```

A repeated call with the same key returns the same run (`created: false`). A 429 carries
`Retry-After`: wait and retry with the same key. Producers that already emit events can use
a [webhook](../webhooks.md) (`start` action, delivery-key dedupe) or a
[Kafka trigger](../kafka-triggers.md) (one run per record, keyed by offset or message key)
instead of calling the API.

## Cut-over

The rule for every step: **the idempotency key is derived from the old job's identity**, so
a job that reaches Ostro twice (from the dual-run, from a backfill script run twice, from a
producer retry) still runs once.

### Cron jobs

1. Deploy the workflow and run it by hand against a test target:
   `ostro run nightly-report --input '...' --wait`.
2. Create the schedule **paused** (`ostro schedules create ... --paused`). Check
   `ostro schedules get` for the next fire time in the right zone.
3. Shadow it: `ostro schedules trigger nightly-report --request-id shadow-1` runs it once
   now, without moving the schedule's watermark. Compare its output with the cron job's.
4. Switch: remove the crontab line (or suspend the CronJob), then
   `ostro schedules resume nightly-report`. With catch-up `skip` (the default) the fires
   missed while paused are not started; the next one is.
5. Watch `ostro_cron_fires_total{schedule="nightly-report"}` and `ostro dlq ls` for a few
   cycles, then delete the old script.

### Job tables

1. **Deploy and size.** Deploy the workflow, set the queue's flow limits, and run a few
   jobs started by hand.
2. **Pick a cut line.** Choose a row id (or a time) `N`. Old workers keep processing rows
   with `id <= N`; add `AND id <= N` to their poll query.
3. **Switch producers.** New jobs start Ostro runs with the key `email-jobs/<id>` (keep
   writing the row if other code reads it, or stop writing it). From here on, no new work
   reaches the old workers.
4. **Drain.** Let the old workers finish the rows up to `N`. To finish sooner, stop them and
   start a run for every remaining `pending` row with the same key format; rerunning that
   script is harmless. Rows a worker had locked when you stopped it may have been half
   sent: their run's HTTP effect carries a fresh key, so check the provider for those few
   or give the workflow the old row's id as a business idempotency key.
5. **Move the dead letters.** Decide per dead row: start a run (it will retry from scratch)
   or archive it. Ostro's DLQ starts empty.
6. **Retire.** When `SELECT count(*) FROM email_jobs WHERE status IN ('pending','running')`
   is 0, delete the workers, the poll query, the reaper and the cleanup job.

If something goes wrong after step 3, pause the queue (`ostro queue pause email`): runs are
accepted and held, nothing is sent, and resume continues where they stopped.

## Operational notes

- **Tenancy.** In a shared cluster, put the migrated jobs in their own tenant. Workflow,
  queue and schedule names and idempotency keys are scoped to the tenant, so two teams can
  both use `email-jobs/81723` ([§14](../workflow-semantics.md#14-tenancy-and-names-d23)).
  Tenant quotas (`ostro tenant set --max-concurrency ... --event-rate 100/1s`) cap the whole
  tenant on top of the queue limits; a tenant over its `event_rate` gets 429
  ([Tenancy and authentication](../tenancy-and-auth.md#5-quotas)).
- **Tokens and scopes.** Give producers a `trigger` token, CI a `deploy` token and on-call a
  `operate` token (`ostro auth token create --name ... --scope ...`). Tokens can be rotated
  (`ostro auth token rotate`) and revoked ([Tokens](../tenancy-and-auth.md#4-tokens)).
- **Audit.** Administrative and security-relevant actions (module uploads, schedule
  changes, run cancels and terminates, token, tenant and secret changes, authentication
  failures) are recorded in the cluster audit log: `ostro audit ls --since 24h`
  ([Audit log](../audit.md)).
- **Signed modules.** If the cluster policy requires signatures, deploy with
  `ostro deploy --signature <file>` for a key trusted with `ostro security trust add`
  ([Signed modules](../signed-modules.md)).
- **Retention.** Pick `retention` per workflow or queue
  (`PUT /v1/workflows/{name}/policy`) longer than both your audit needs and your producers'
  re-send window ([§12](../workflow-semantics.md#12-retention)).
