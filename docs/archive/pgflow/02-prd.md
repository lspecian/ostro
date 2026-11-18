# 02 — Product Requirements Document (PRD)

| | |
|---|---|
| **Product** | pgflow (codename): Postgres-native, polyglot Wasm durable execution engine |
| **Status** | Draft v0.1 |
| **Date** | 2026-11-18 |
| **Related** | [01-research](01-research.md) · [03-architecture](03-architecture.md) · [04-roadmap](04-roadmap.md) |

Requirement IDs: `FR-x.y` (functional), `NFR-x` (non-functional). Priority: **P0** = MVP-blocking, **P1** = GA, **P2** = post-GA.

---

## 1. Executive summary

### 1.1 Problem

1. **The Day-1 infrastructure tax.** Durable execution (Temporal, Inngest, Hatchet) means new clusters, brokers, dashboards, upgrades and on-call. For a team of 1–10 engineers, that operating cost exceeds the value until workflows become critical, and by then the code is written against ad-hoc cron + queue tables.
2. **The "just use Postgres" ceiling.** Postgres-backed libraries (DBOS, River, Graphile Worker, pg-boss) avoid the extra cluster but (a) bind workflows to one language and one app process, (b) offer no isolation, and (c) leave no path for moving CPU-heavy work off the database server short of rewriting.
3. **Determinism is developer discipline.** In replay-based engines, a stray `datetime.now()` or `Math.random()` corrupts a workflow. Teams find out in production.

### 1.2 Solution

Workflows are compiled to **Wasm Components** against one WIT contract and stored in Postgres. They run in **Postgres background workers** by default, and the **same binary** runs in **stateless K8s workers** when needed. The sandbox enforces determinism, resource limits and secret isolation.

### 1.3 Value proposition

| For | Value |
|---|---|
| Developers | Write `@workflow` / `@step` in their language, run `pgflow deploy`, and call `SELECT pgflow.start(...)`. No new services to learn or run. |
| Ops/DBAs | One database to back up, monitor and secure. Hard caps on what workflows can consume. |
| Platform teams | A multi-tenant workflow substrate with per-tenant quotas, plus an elastic K8s plane with no code changes. |

### 1.4 Personas

| Persona | Context | Jobs-to-be-done | Success looks like |
|---|---|---|---|
| **P1: Solo dev / indie hacker** | One VPS or Docker Compose, self-hosted Postgres, ships Python or TS | "Send the welcome email sequence, retry webhooks, nightly reports, without running Redis or a worker." | First durable workflow running in **under 15 minutes** from `pgflow init` |
| **P2: Lean startup (5–30 eng)** | Managed or self-hosted PG, monolith + a few services, mixed Python/TS | Payments sagas, onboarding flows, LLM pipelines with retries; transactional enqueue from the app | Replaces cron + queue tables + bespoke retry code. Zero new on-call surface. |
| **P3: Platform team** | K8s, CloudNativePG/Crunchy, many product teams, compliance | Offer "workflows as a service" internally; isolate tenants; autoscale; observe | Teams self-serve. CPU-heavy queues move to K8s through a SQL config change. OTel traces in the existing stack. |
| **P4 (secondary): SaaS builder** | Wants to run *customer-supplied* automation logic | Safe execution of untrusted code with quotas | Customer code cannot read other tenants' data or secrets, or exhaust the DB |

### 1.5 Goals & non-goals

**Goals (v1.0)**
- G1: Durable workflows in Rust, Python and TS with identical semantics.
- G2: Zero extra services in DB mode. One extra stateless deployment in hybrid mode.
- G3: Determinism guaranteed by the runtime for all ambient sources (clock, RNG, FS, network).
- G4: Exactly-once SQL steps; effectively-once HTTP via idempotency keys.
- G5: Per-queue choice of execution plane at runtime (`local` / `k8s` / `auto`).

**Non-goals (v1.0)**
- A general-purpose FaaS or HTTP serving layer.
- Cross-database / multi-region workflow coordination.
- A visual workflow designer.
- Supporting non-Postgres storage backends.
- Running on Postgres < 15.

---

## 2. Functional requirements

### FR-1 Polyglot workflow SDKs

All three SDKs are thin, idiomatic layers over the **same WIT world** (`pgflow:runtime/workflow`, see [03 §2](03-architecture.md#2-unified-wit-interface-specification)). Semantics must be identical: a conformance test suite (FR-1.9) runs the same scenarios against every SDK.

| ID | Requirement | Pri |
|---|---|---|
| FR-1.1 | **Workflow definition**: mark a function as a workflow with a stable name and version (`@workflow`, `#[workflow]`, `workflow()`). Typed input/output, serialised as JSON by default. | P0 |
| FR-1.2 | **Steps**: wrap arbitrary guest code as a journaled step with name, retry policy, timeout and fuel budget. On replay, the recorded result is returned without re-executing. | P0 |
| FR-1.3 | **Async primitives**: `await` on effects; concurrency via `gather` / `Promise.all` / `join!`; `select`/`race` with deterministic resolution. | P0 |
| FR-1.4 | **Durable timers**: `sleep(duration)`, `sleep_until(ts)`. A run sleeping for days holds no memory (it is evicted and replayed on wake). | P0 |
| FR-1.5 | **Signals & queries**: `wait_for_signal(name, timeout)`; external `pgflow.signal(run_id, name, payload)`; read-only `query` handlers (P1). | P0 / P1 |
| FR-1.6 | **Child workflows**: start, await or detach a child (any language). Cancellation propagates. | P1 |
| FR-1.7 | **Deterministic utilities**: `ctx.now()`, `ctx.random()`, `ctx.uuid()`. The language's native equivalents also work, through WASI virtualisation. | P0 |
| FR-1.8 | **Versioning helpers**: `ctx.patched("change-id")` for in-flight code evolution; `continue_as_new(input)`. | P1 |
| FR-1.9 | **Conformance suite**: a language-neutral scenario spec (YAML) plus harness; every SDK must pass before release. | P0 |
| FR-1.10 | **Local test harness**: run workflows in-process against an ephemeral Postgres (testcontainers) or an in-memory journal; time-skipping for sleeps. | P1 |
| FR-1.11 | **Sagas/compensation** helper: register a compensating step; run compensations in reverse on failure. | P2 |

**SDK ergonomics (illustrative)**

```python
# Python
from pgflow import workflow, step, ctx, http

@step(retry=dict(max_attempts=5, backoff="exponential"))
async def charge(order) -> dict:
    r = await http.post("https://api.stripe.com/v1/payment_intents",
                        headers={"Authorization": "Bearer {{secret:stripe}}"},
                        form={"amount": order["total"], "currency": "usd"})
    return r.json()

@workflow(name="checkout", version=1)
async def checkout(order: dict) -> dict:
    payment = await charge(order)
    await ctx.sql("UPDATE orders SET status='paid' WHERE id=$1", [order["id"]])
    await ctx.sleep(days=3)
    await http.post("https://hooks.example.com/review-request", json={"order": order["id"]})
    return {"payment_id": payment["id"]}
```

```ts
// TypeScript
import { workflow, step, ctx, http } from "@pgflow/sdk";

const charge = step("charge", { retry: { maxAttempts: 5 } }, async (order: Order) =>
  (await http.post("https://api.stripe.com/v1/payment_intents", {
    headers: { Authorization: "Bearer {{secret:stripe}}" },
    form: { amount: order.total, currency: "usd" },
  })).json());

export const checkout = workflow("checkout", { version: 1 }, async (order: Order) => {
  const payment = await charge(order);
  await ctx.sql("UPDATE orders SET status='paid' WHERE id=$1", [order.id]);
  await ctx.sleep({ days: 3 });
  return { paymentId: payment.id };
});
```

```rust
// Rust
use pgflow::{workflow, step, ctx, http, Result};

#[step(retry(max_attempts = 5))]
async fn charge(order: &Order) -> Result<Payment> {
    http::post("https://api.stripe.com/v1/payment_intents")
        .header("Authorization", "Bearer {{secret:stripe}}")
        .form(&[("amount", order.total.to_string()), ("currency", "usd".into())])
        .send().await?.json()
}

#[workflow(name = "checkout", version = 1)]
async fn checkout(order: Order) -> Result<Receipt> {
    let payment = charge(&order).await?;
    ctx::sql("UPDATE orders SET status='paid' WHERE id=$1", &[&order.id]).await?;
    ctx::sleep(Duration::from_secs(3 * 86400)).await;
    Ok(Receipt { payment_id: payment.id })
}
```

### FR-2 CLI & build pipeline (`pgflow-cli`)

| ID | Requirement | Pri |
|---|---|---|
| FR-2.1 | `pgflow init --lang {rust,python,ts}`: scaffold a project with the SDK, WIT and example workflow pinned to known-good toolchain versions. | P0 |
| FR-2.2 | `pgflow build`: detect the language and produce an optimised component. **Rust**: `cargo build --target wasm32-wasip2` → `wasm-opt`. **Python**: resolve deps (uv) → `componentize-py` with pre-init. **TS**: bundle (esbuild) → `componentize-js`/`jco` with pre-init. Then validate the component against the pgflow world (no disallowed imports) and emit `manifest.json` (workflows, versions, required secrets, sha256, SDK version). | P0 (Rust) / P0-Phase3 (Py/TS) |
| FR-2.3 | **Hermetic toolchains**: the CLI downloads and pins `componentize-py`, `jco`, `wasm-tools`, `wasm-opt` into a cache, or runs builds in an official container image (`--docker`). | P1 |
| FR-2.4 | `pgflow deploy [--db URL] [--activate]`: upload the component (zstd-compressed) through `pgflow.register_module()`, register its workflows, optionally make them active. Idempotent by sha256. | P0 |
| FR-2.5 | `pgflow run <workflow> --input '{...}' [--follow]`: start a run and stream its events. | P0 |
| FR-2.6 | `pgflow inspect <run_id>`, `pgflow history <run_id>`, `pgflow ls [--status failed]`, `pgflow retry`, `pgflow cancel`, `pgflow signal`. | P1 |
| FR-2.7 | `pgflow replay <run_id> --module ./new.wasm`: replay the recorded history against a candidate build to detect non-determinism **before** deploying (CI gate). | P1 |
| FR-2.8 | `pgflow dev`: watch mode with rebuild + redeploy to a local DB; local time-skipping. | P2 |
| FR-2.9 | `pgflow worker`: run the standalone worker binary (same binary as the K8s image). | P0-Phase4 (P1 earlier for the plain-SQL mode) |

### FR-3 SQL control plane API

Every operation is a SQL function in schema `pgflow` and is callable from any driver, trigger or `pg_cron` job. All mutating functions are **transactional with the caller**: a `pgflow.start()` inside a rolled-back transaction never happened.

| ID | Function (signature abbreviated) | Pri |
|---|---|---|
| FR-3.1 | `pgflow.register_module(name text, component bytea, opts jsonb) → uuid` | P0 |
| FR-3.2 | `pgflow.activate(workflow text, version int)` / `pgflow.deactivate(...)` | P0 |
| FR-3.3 | `pgflow.start(workflow text, input jsonb, opts jsonb DEFAULT '{}') → uuid`. `opts`: `queue`, `priority`, `delay`, `idempotency_key` (dedupe), `version`, `parent_run_id`, `traceparent`, `tenant`. | P0 |
| FR-3.4 | `pgflow.status(run_id) → record`; view `pgflow.runs` (+ RLS-friendly per-tenant views) | P0 |
| FR-3.5 | `pgflow.history(run_id) → SETOF pgflow.workflow_events` | P0 |
| FR-3.6 | `pgflow.signal(run_id, name text, payload jsonb)` | P0 |
| FR-3.7 | `pgflow.cancel(run_id, reason text, mode text DEFAULT 'graceful')` (graceful = run compensations; `terminate` = stop now) | P0 |
| FR-3.8 | `pgflow.retry(run_id, from_step text DEFAULT NULL)`; `pgflow.dlq_requeue(filter)` | P1 |
| FR-3.9 | `pgflow.schedule(name, cron text, workflow, input jsonb, opts)` / `unschedule(name)`. Built-in scheduler (no `pg_cron` dependency); cron run identity = `(schedule, fire_time)` for exactly-once firing. | P1 |
| FR-3.10 | `pgflow.await_result(run_id, timeout interval) → jsonb` (blocks the caller; for scripts/tests) | P2 |
| FR-3.11 | `pgflow.configure_queue(name, policy jsonb)`: concurrency, rate limits, executor routing (FR-5). | P0 (basic) / P1 (routing) |
| FR-3.12 | `pgflow.stats() → jsonb`: queue depths, lag, active instances, executor load | P1 |
| FR-3.13 | Grant model: roles `pgflow_admin`, `pgflow_deployer`, `pgflow_operator`, `pgflow_client` | P1 |

### FR-4 Connector architecture

| ID | Requirement | Pri |
|---|---|---|
| FR-4.1 | **HTTP connector** (host, Tokio + hyper/reqwest + rustls): methods, headers, body, timeouts, redirects off by default, response size cap, retries per step policy, automatic `Idempotency-Key`. | P0 (Phase 2) |
| FR-4.2 | **SQL connector**: parameterised queries against the home DB, exactly-once in the same transaction as the journal (see [01 §3.3](01-research.md#33-transactional-sql-steps-exactly-once)). Executes as a per-module role (`SET LOCAL ROLE`) so RLS applies. | P0 |
| FR-4.3 | **Secrets**: `{{secret:name}}` templating resolved in the host. Pluggable providers. Per-module grants. Redaction in journal and logs. | P0 (Phase 2) |
| FR-4.4 | **Egress policy**: per-module allow-list of hosts/CIDRs. SSRF protection (blocks link-local/metadata IPs unless explicitly allowed). | P0 (Phase 2) |
| FR-4.5 | **gRPC connector** (tonic, dynamic via reflection or descriptor sets) | P2 |
| FR-4.6 | **Object store connector** (S3-compatible, presigned uploads; payload offload for large step results) | P1 |
| FR-4.7 | **Polyglot connector components**: connector libraries written as components (Rust recommended) exporting typed WIT APIs (`stripe:api`, `slack:api`) and composed into the workflow at build time; they may only import primitive host connectors. | P2 |
| FR-4.8 | **Rate limiting** per connector target (token bucket shared across executors through Postgres advisory locks or a rate table) | P1 |

### FR-5 Hybrid routing rules

| ID | Requirement | Pri |
|---|---|---|
| FR-5.1 | Each queue has an `executor` policy: `local` (DB bgworkers only), `remote` (external workers only), `auto` (both, with spillover). | P1 (Phase 4) |
| FR-5.2 | **Spillover in `auto`**: local executors stop claiming from a queue when any threshold is exceeded: host CPU load (from `/proc/loadavg` or cgroup `cpu.stat`), local active instances ≥ cap, queue lag (oldest `visible_at`) > X, or configured DB-protection signals (e.g. `pg_stat_activity` active backends > Y). Remote workers then claim the backlog. Hysteresis prevents flapping. | P1 |
| FR-5.3 | **Per-workflow overrides**: `opts.queue` at start; workflow-level default queue in the manifest; step-level hint `@step(executor="remote")` for heavy steps (the run migrates between planes at a step boundary, which is safe because state lives in the journal). | P2 |
| FR-5.4 | **KEDA integration**: documented `ScaledObject` using the `postgresql` scaler over `pgflow.remote_demand(queue)` (a function returning the backlog that local executors will not take). Scale-to-zero supported. | P1 |
| FR-5.5 | **Module distribution to remote workers**: workers pull components from Postgres by sha256 and cache the AOT artefact on local disk (or an init container warms the cache). No registry needed; OCI registry optional. | P1 |

### FR-6 Deployment modes

| Mode | Install | Executes in | Target |
|---|---|---|---|
| **A. Embedded (DB mode)** | `CREATE EXTENSION pgflow` + `shared_preload_libraries` | Postgres bgworkers | Self-hosted / container / CNPG |
| **B. Plain-SQL + worker** | `pgflow migrate` (schema + PL/pgSQL API, no C extension) | `pgflow-worker` (sidecar, VM, container) | **Managed Postgres** (RDS, Cloud SQL, Supabase, Neon) |
| **C. Hybrid** | A + workers on K8s | Both, per-queue routing | Scale-out |

FR-6.1 (P0): the schema and SQL API are **identical** in A and B; mode A adds the executor and faster wake-ups. FR-6.2 (P1): an official Docker image `postgres + pgflow` and a CloudNativePG image catalogue entry.

---

## 3. Non-functional requirements

### NFR-1 Sandbox isolation & resource control

| ID | Requirement | Target |
|---|---|---|
| NFR-1.1 | Per-instance linear-memory cap (default 64 MiB Rust/TS, 256 MiB Python; configurable per module) | Enforced; an allocation beyond the cap traps → step failure, never process OOM |
| NFR-1.2 | CPU limits: **fuel** budget per step and per run activation (deterministic) + **epoch** wall-clock deadline per activation | A runaway loop is stopped within 1 epoch tick (default 10 ms) after its deadline |
| NFR-1.3 | Global executor budgets: max concurrent instances, max total reserved memory, max executor threads per DB | Never exceeds GUC values |
| NFR-1.4 | No ambient authority: no FS, sockets or env vars; WASI clocks and random virtualised | Verified by import validation at deploy and in tests |
| NFR-1.5 | A guest trap or host panic **fails the run attempt only**. It never crashes the bgworker or Postgres. | Fuzzed (FR conformance + `wasm-smith` fuzz corpus) |

### NFR-2 Performance & cold start

| ID | Requirement | Target (validated in Phase 3/4 benchmarks) |
|---|---|---|
| NFR-2.1 | AOT compile at deploy; no compilation on the hot path | 100% |
| NFR-2.2 | Instantiation from a warm `InstancePre` (pooling allocator) | Rust/TS p50 < 1 ms; Python p50 < 10 ms |
| NFR-2.3 | Cold load (artefact on disk, not in process) | Rust < 20 ms; Python/TS < 100 ms |
| NFR-2.4 | Start-to-first-step latency (DB mode, idle system) | p50 < 10 ms, p99 < 50 ms |
| NFR-2.5 | Throughput (DB mode, 8 vCPU, trivial 3-step Rust workflow, `synchronous_commit=on`) | ≥ 2,000 steps/s (I/O-bound on WAL); documented curve for Python/TS |
| NFR-2.6 | Replay overhead | < 5% of first-execution CPU for histories ≤ 1,000 events |

### NFR-3 Durability & crash recovery

| ID | Requirement |
|---|---|
| NFR-3.1 | **No lost runs**: a committed `start()` will eventually run to completion, fail into the DLQ, or be cancelled, under any combination of worker or Postgres crashes (verified with fault injection: kill -9 at every instrumented point). |
| NFR-3.2 | **Step results are durable before observation**: a step's result is committed before any later step can depend on it. |
| NFR-3.3 | **Lease-based recovery**: an orphaned run is reclaimed within `lease_ttl + sweep_interval` (default 30 s + 5 s). |
| NFR-3.4 | **Fencing**: a worker whose lease was stolen can never write a checkpoint (`lease_owner` + `attempt` predicate). |
| NFR-3.5 | **Idempotent effects**: HTTP effects carry stable idempotency keys; SQL effects are exactly-once. |
| NFR-3.6 | Works with physical replication / failover: no state outside Postgres except caches. Replicas can serve read-only inspection. |
| NFR-3.7 | **Retention**: completed runs archived/dropped by partition after a configurable TTL. |

### NFR-4 Observability

| ID | Requirement |
|---|---|
| NFR-4.1 | **OpenTelemetry traces**: one span per run activation and per step/effect, W3C `traceparent` accepted at `start()` and propagated to outbound HTTP; OTLP export from the bridge (Tokio) thread. |
| NFR-4.2 | **Metrics** (OTel / Prometheus endpoint on the worker; `pgflow.stats()` SQL in DB mode): queue depth, lag, claim rate, step latency histograms, fuel used, memory high-water, replay counts, DLQ size, spillover state. |
| NFR-4.3 | **Structured logs** from guests (`ctx.log`), rate-limited, stored with the run (bounded) and forwarded to Postgres log/OTLP. |
| NFR-4.4 | Journal is human-readable (JSONB payloads by default) and queryable with SQL. |

### NFR-5 Compatibility

| ID | Requirement |
|---|---|
| NFR-5.1 | PostgreSQL **15, 16, 17, 18** on Linux x86_64 and aarch64 (DB mode). macOS for dev only. |
| NFR-5.2 | Plain-SQL mode works on any PG ≥ 15 that allows `CREATE SCHEMA` + PL/pgSQL. |
| NFR-5.3 | The WIT contract is semver-versioned (`pgflow:runtime@0.x`). The host supports N and N-1 minor versions simultaneously. |
| NFR-5.4 | Journal format is versioned; upgrades never require draining in-flight runs. |
| NFR-5.5 | Extension upgrade scripts (`ALTER EXTENSION pgflow UPDATE`) for every release. |

### NFR-6 Security

See [03 §7](03-architecture.md#7-security--multi-tenancy). Summary: modules are signed by sha256 identity with optional signature verification (Sigstore/cosign); secrets never enter guests; egress allow-lists; per-module DB roles with RLS; per-tenant quotas; audit log of deploy, activate, secret access and cancel.

### NFR-7 Developer experience

- Time to first workflow < 15 min (P1 persona) using the Docker image.
- Error messages for non-determinism name the event sequence number, expected vs actual effect, and source location (where the SDK can capture it).
- Docs: concepts, per-language guides, operations guide (sizing DB mode, when to offload), migration guides from cron/queue tables, Temporal and DBOS.

---

## 4. Success metrics

| Metric | 6 months post-GA target |
|---|---|
| Time-to-first-workflow (measured in usability tests) | Median < 15 min |
| Conformance suite pass rate across SDKs | 100% |
| Crash-recovery fault-injection suite | 0 lost runs, 0 duplicate SQL effects over 10⁶ injected faults |
| GitHub stars / monthly active installs (telemetry opt-in) | Track; adoption signal only |
| Share of users on hybrid mode | Signals Day-100 story validity |

## 5. Open questions

1. Default payload encoding: JSON (inspectable) vs CBOR/MessagePack (compact, binary-safe). **Proposal:** JSON default, with `bytes` support through base64 + content type, and CBOR opt-in per workflow.
2. Should DB mode default to the **supervised-process** isolation tier (safer) or **in-process** (faster)? **Proposal:** in-process for dev images, supervised for the production Helm chart and CNPG images. Revisit after Phase 1 benchmarks.
3. Licence: Apache-2.0 (maximises adoption, including by cloud providers) vs a source-available licence for the K8s operator. **Proposal:** Apache-2.0 for everything in v1.
4. Do we expose raw `wasi:http` *outgoing-handler* as an alternative effect API (journaled by the host) to reuse existing WASI-HTTP client libraries? Attractive for ecosystem reuse; evaluate in Phase 2.
