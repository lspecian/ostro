# 02 — Product Requirements Document

| | |
|---|---|
| **Product** | Ostro (codename): single-binary, polyglot Wasm durable execution engine |
| **Status** | Draft v0.2 (supersedes pgflow PRD) |
| **Date** | 2026-11-18 |

IDs: `FR-x.y` functional, `NFR-x` non-functional. Priority: **P0** MVP, **P1** GA (1.0), **P2** post-GA.

---

## 1. Summary

### 1.1 Problem

Durable execution solves retries, long sleeps, sagas and human-in-the-loop flows, but every option charges an infrastructure tax up front: a cluster (Temporal), a database (Hatchet, Windmill, DBOS), or a fleet of processes that host your code and must stay up (all of them except Cloudflare). Small teams fall back to cron + queue tables + hand-rolled retries.

### 1.2 Solution

A single Rust binary that **stores, schedules and executes** durable workflows compiled to Wasm components. Developers write Rust, Python or TypeScript against one SDK contract, run `ostro deploy`, and trigger runs via HTTP, webhook, cron or the CLI. The same binary runs as a stateless worker for more compute or as a replica for high availability.

### 1.3 Personas

| Persona | Context | Success looks like |
|---|---|---|
| **P1 Solo dev** | One VPS / Fly / Railway / Docker host | First durable workflow in **< 10 min**; one process and one data directory to care about |
| **P2 Lean startup** | 5–30 engineers, mixed Python/TS, some K8s | Replaces cron + queue tables + retry code; webhooks (Stripe, GitHub) go straight into workflows; S3 backups; zero new on-call surface beyond one service |
| **P3 Platform team** | K8s, many product teams | Multi-tenant "workflows as a service"; quotas; workers autoscale with KEDA; 3-node HA; OTel |
| **P4 SaaS builder** | Wants to run customer-authored automations | Untrusted code isolated by fuel/memory/egress limits; per-tenant quotas |

### 1.4 Goals / non-goals

**Goals (1.0):** G1 one binary, zero dependencies · G2 server-hosted Wasm execution in Rust/Python/TS with identical semantics · G3 sandbox-enforced determinism · G4 effectively-once effects (outbox + idempotency) · G5 grow from laptop to HA cluster without code changes.

**Non-goals (1.0):** general-purpose FaaS/HTTP serving · visual designer · multi-region active-active · native (non-Wasm) code execution (post-1.0 escape hatch) · partition splitting/re-sharding of a live cluster.

---

## 2. Operating modes (FR-0)

All modes are the same binary.

| Mode | Command | Storage | Executes workflows | Target |
|---|---|---|---|---|
| **Dev** | `ostro dev` | `./.ostro` (or `--ephemeral` in-memory) | Yes, embedded | Laptop; hot reload, UI, time-skipping |
| **Server (single node)** | `ostro server --data /var/lib/ostro` | Local disk, optional S3 bottomless | Yes, embedded | Day 1 production |
| **Worker** | `ostro worker --join https://ostro:7700 --token …` | None (AOT cache only) | Yes, assigned by partition leaders | Add compute; KEDA autoscaling |
| **Cluster** | `ostro server --config cluster.toml` (3 or 5 nodes with `[[peers]]`) | Local disk replicated (RF=3), optional S3 tiering | Yes, embedded + workers | HA |
| **Client** | `ostro deploy/run/ls/inspect/signal/cancel/replay/backup` | — | — | CLI against any of the above |

| ID | Requirement | Pri |
|---|---|---|
| FR-0.1 | Single static binary for linux x86_64/aarch64 and macOS (arm64/x86_64); official container image `FROM scratch` + binary | P0 |
| FR-0.2 | Zero-config start: every mode works with defaults; config file (TOML) and env vars override | P0 |
| FR-0.3 | One client listener (HTTP/1.1, HTTP/2, gRPC multiplexed; default `:7700`) and one internal cluster listener (Raft + replication; optional separate replication address) | P0 |
| FR-0.4 | Single node → cluster migration: `ostro cluster init` converts a single node into node 1 of a cluster without data export | P1 |
| FR-0.5 | Embedded web UI (runs, history timeline, DLQ, modules, queues, cluster status) served from the binary | P1 |

---

## 3. Functional requirements

### FR-1 Polyglot workflow SDKs

| ID | Requirement | Pri |
|---|---|---|
| FR-1.1 | `@workflow` / `#[workflow]` / `workflow()` with stable name + version; typed JSON input/output | P0 |
| FR-1.2 | Steps: journaled guest computation with retry policy, timeout, fuel budget | P0 |
| FR-1.3 | Async: `await` effects; `gather`/`Promise.all`/`join!`; deterministic `race`/`select` | P0 |
| FR-1.4 | Durable timers: `sleep`, `sleep_until`; sleeping runs hold no memory | P0 |
| FR-1.5 | Signals (`wait_for_signal`, external `signal`), queries (read-only handlers) | P0 / P1 |
| FR-1.6 | Child workflows across languages; cancellation propagation | P1 |
| FR-1.7 | Deterministic `ctx.now/random/uuid`; native equivalents also deterministic via WASI virtualisation | P0 |
| FR-1.8 | Versioning: `ctx.patched(id)`, `continue_as_new` | P1 |
| FR-1.9 | Language-neutral conformance suite; all SDKs must pass | P0 |
| FR-1.10 | Local test harness with time-skipping (runs against `ostro dev --ephemeral` or in-process engine) | P1 |
| FR-1.11 | Saga/compensation helper | P2 |

```ts
import { workflow, step, ctx, http } from "@ostro/sdk";

const charge = step("charge", { retry: { maxAttempts: 5 } }, async (o: Order) =>
  (await http.post("https://api.stripe.com/v1/payment_intents", {
    headers: { Authorization: "Bearer {{secret:stripe}}" },
    form: { amount: o.total, currency: "usd" },
  })).json());

export const checkout = workflow("checkout", { version: 1 }, async (o: Order) => {
  const payment = await charge(o);
  await ctx.sql("app_db", "UPDATE orders SET status='paid' WHERE id=$1", [o.id]);
  const approved = await ctx.waitForSignal("review", { timeout: { days: 3 } });
  return { paymentId: payment.id, approved };
});
```

### FR-2 CLI & build pipeline

| ID | Requirement | Pri |
|---|---|---|
| FR-2.1 | `ostro init --lang {rust,python,ts}` scaffolds a project with pinned toolchain versions | P0 |
| FR-2.2 | `ostro build`: Rust (`cargo` → `wasm32-wasip2` → `wasm-opt`), Python (uv resolve → `componentize-py` + pre-init), TS (esbuild → `componentize-js`/`jco` + pre-init); import validation against the Ostro world; emits `manifest.json` (workflows, secrets, egress hosts, limits, toolchain versions) | P0 Rust / P0-Phase 3 Py/TS |
| FR-2.3 | Hermetic toolchains: downloaded and pinned into a cache, or `--docker` build image | P1 |
| FR-2.4 | `ostro deploy [--activate]`: build if needed, upload component (content-addressed, idempotent by sha256), register workflows | P0 |
| FR-2.5 | `ostro run <wf> --input … [--follow]`, `ls`, `inspect`, `history`, `signal`, `cancel`, `retry` | P0 / P1 |
| FR-2.6 | `ostro replay <run-id|--sample N> --module new.wasm`: non-determinism gate for CI | P1 |
| FR-2.7 | `ostro dev` watch mode: rebuild + redeploy on save | P1 |
| FR-2.8 | `ostro backup` / `ostro restore` (local file or S3) | P1 |

### FR-3 Control-plane API

HTTP/JSON (OpenAPI) and gRPC with identical semantics; thin client libraries for TS, Python, Go, Rust.

| ID | Endpoint (HTTP form) | Pri |
|---|---|---|
| FR-3.1 | `PUT /v1/modules/{sha256}` (upload), `POST /v1/workflows/{name}/versions/{v}:activate` | P0 |
| FR-3.2 | `POST /v1/workflows/{name}/runs` → `{run_id}`; options: `queue`, `delay`, `priority`, `idempotency_key` (dedupe), `version`, `traceparent`, `tenant` | P0 |
| FR-3.3 | `GET /v1/runs/{id}`, `GET /v1/runs/{id}/history`, `GET /v1/runs?status=…` | P0 |
| FR-3.4 | `POST /v1/runs/{id}/signals/{name}`, `POST /v1/runs/{id}:cancel`, `:retry` | P0 / P1 |
| FR-3.5 | `GET /v1/runs/{id}/result?wait=30s` (long-poll for synchronous callers) | P1 |
| FR-3.6 | **Webhook ingress**: `POST /hooks/{name}` mapped to a workflow with optional signature verification (Stripe, GitHub, Slack, generic HMAC) and `idempotency_key` from a header/body path | P1 |
| FR-3.7 | **Cron**: `PUT /v1/schedules/{name}` (cron expr, timezone, workflow, input); exactly-once firing per `(schedule, fire_time)` | P1 |
| FR-3.8 | Queues: `PUT /v1/queues/{name}` — concurrency, rate limits, executor policy (FR-5) | P0 basic / P1 full |
| FR-3.9 | Auth: API tokens with scopes (`admin`, `deploy`, `operate`, `trigger`, `read`), per-tenant tokens | P0 basic / P1 scopes |
| FR-3.10 | Event triggers: consume a Kafka/Chronik topic and start or signal workflows per record | P2 |

### FR-4 Connectors

| ID | Requirement | Pri |
|---|---|---|
| FR-4.1 | HTTP: timeouts, size caps, redirect policy, automatic `Idempotency-Key`, retries per policy, `{{secret:…}}` templating | P0 (Phase 2) |
| FR-4.2 | SQL (Postgres, MySQL): named connections, parameterised statements, **transactional idempotency token** for exactly-once writes ([01 §5.2](01-research.md#52-what-changes-without-a-host-database-sql-effects)) | P1 |
| FR-4.3 | Object store (S3/GCS/Azure/local): get/put/presign; large-payload offload | P1 |
| FR-4.4 | Egress allow-list per module; SSRF protection (metadata/link-local/private ranges blocked unless allowed; DNS-rebinding-safe connect) | P0 (Phase 2) |
| FR-4.5 | Per-target rate limits (token buckets owned by a metadata-assigned partition) | P1 |
| FR-4.6 | Kafka/Chronik produce connector | P2 |
| FR-4.7 | Polyglot connector components composed at build time | P2 |

### FR-5 Execution placement & spillover

| ID | Requirement | Pri |
|---|---|---|
| FR-5.1 | Queue executor policy: `embedded` (server nodes only), `workers` (workers only), `auto` (embedded first, spill to workers) | P1 |
| FR-5.2 | Spillover: the partition leader's dispatcher sends activations to workers when the local executor exceeds thresholds (CPU, resident memory, instance count, queue lag), with hysteresis | P1 |
| FR-5.3 | Worker affinity labels (`--labels gpu=true,region=eu`) and queue selectors | P1 |
| FR-5.4 | KEDA: documented `ScaledObject` using the `metrics-api` or `prometheus` scaler on `ostro_queue_worker_demand{queue}` | P1 |
| FR-5.5 | Workers fetch modules by sha256 from the server and cache AOT artefacts locally | P1 |
| FR-5.6 | Step-level placement hint `@step(executor="workers")` | P2 |

### FR-6 Storage, backup & cluster operations

| ID | Requirement | Pri |
|---|---|---|
| FR-6.1 | Embedded storage only; one data directory; no external dependencies | P0 |
| FR-6.2 | Bottomless mode: sealed journal segments + state snapshots streamed to object storage; `restore` rebuilds a node from the bucket | P1 |
| FR-6.3 | Retention: completed runs and their journals deleted/archived after TTL per workflow/queue | P0 |
| FR-6.4 | Cluster: 3/5 nodes, RF=3, `min_insync_replicas=2`; automatic partition leader failover; node add/remove | P1 |
| FR-6.5 | Rolling upgrades across one minor version | P1 |

---

## 4. Non-functional requirements

### NFR-1 Sandbox & resources

| ID | Requirement | Target |
|---|---|---|
| NFR-1.1 | Per-instance memory cap (default 64 MiB Rust/TS, 256 MiB Python) | Breach → trap → step failure, never node OOM |
| NFR-1.2 | Fuel per step/run + epoch deadline per activation slice | Runaway loop stopped within one epoch tick (10 ms) of its deadline |
| NFR-1.3 | Global budgets: instances, resident Wasm memory, executor threads | Never exceeded; excess work queued or spilled |
| NFR-1.4 | No ambient authority (FS, sockets, env) | Enforced by import validation + host linker |
| NFR-1.5 | Guest trap or host panic fails one attempt only | Fuzzed continuously |

### NFR-2 Performance (targets, validated by benchmarks per phase)

| ID | Metric | Target |
|---|---|---|
| NFR-2.1 | Binary size | < 60 MB (incl. Wasmtime/Cranelift and UI) |
| NFR-2.2 | `ostro dev` startup | < 1 s |
| NFR-2.3 | Instantiation from warm `InstancePre` | Rust/TS p50 < 1 ms; Python p50 < 10 ms |
| NFR-2.4 | Start → first step (single node, idle) | p50 < 5 ms |
| NFR-2.5 | Throughput, single node, 8 vCPU NVMe, trivial Rust steps, fsync on | ≥ 10,000 steps/s (group commit) |
| NFR-2.6 | Throughput, 3-node RF=3, `min_insync=2` | ≥ 5,000 steps/s; p99 commit latency < 20 ms on a LAN |
| NFR-2.7 | Replay overhead (≤ 1,000 events) | < 5% of first-execution CPU |

### NFR-3 Durability & recovery

| ID | Requirement |
|---|---|
| NFR-3.1 | An accepted run (API returned `run_id`) is never lost: it completes, fails into the DLQ, or is cancelled — under process crashes (single node) and any minority failure (cluster) |
| NFR-3.2 | Results are committed (fsync on single node; HW past them in a cluster) before any later step observes them |
| NFR-3.3 | No acknowledged write is lost on leader failover (leader epochs + truncation; elect only from ISR) |
| NFR-3.4 | Effects: HTTP effectively-once via stable idempotency keys; SQL exactly-once via transactional tokens |
| NFR-3.5 | Executor failure: activation reassigned within `lease_ttl` (default 15 s) |
| NFR-3.6 | Single node restart to serving: < 5 s for 1 M stored runs (snapshot + log tail replay) |
| NFR-3.7 | Bottomless restore RPO ≤ segment seal interval (default 1 s / 1 MiB) |

### NFR-4 Observability

OpenTelemetry traces (run → activation → step/effect; `traceparent` in, child `traceparent` out on HTTP), Prometheus `/metrics` on every node, structured logs (guest logs bounded per run), UI timeline. Replayed spans are never re-emitted.

### NFR-5 Compatibility

WIT contract semver'd (`ostro:runtime@0.x`); host serves N and N-1. Journal and state formats versioned; upgrades never require draining runs. Linux kernels ≥ 5.10 (io_uring optional, fallback to std I/O).

### NFR-6 Security

TLS on both listeners (auto self-signed in dev; ACME optional); mTLS between nodes and workers; scoped API tokens; signed modules optional; secrets sealed at rest and never sent to guests; per-module egress allow-lists; audit log. Details: [03 §9](03-architecture.md#9-security--multi-tenancy).

### NFR-7 Developer experience

Time-to-first-workflow < 10 minutes; non-determinism errors name the sequence number, expected vs actual command, and source location where available; docs include migration guides from cron/queue tables, Temporal and Restate.

---

## 5. Success metrics

| Metric | Target 6 months after 1.0 |
|---|---|
| Median time-to-first-workflow (usability tests) | < 10 min |
| SDK conformance | 100% for Rust, Python, TS |
| Replication conformance + fault-injection | 0 lost acknowledged runs over 10⁶ injected faults |
| Mode adoption mix (opt-in telemetry) | Validates single-node → cluster growth path |

## 6. Open questions

1. **Licence:** Apache-2.0 everywhere (adoption, differentiator vs source-available competitors) vs source-available cluster features. Proposal: Apache-2.0.
2. **Default partitions:** fixed count at creation (16 single-node, 64 cluster) vs splittable. Proposal: fixed in 1.0; splitting post-1.0.
3. **Native worker protocol** (non-Wasm SDKs in user processes) — when? Proposal: post-1.0, driven by Python native-dependency demand.
4. ~~Product relationship with Chronik~~ **Decided 2026-11-18:** separate product on shared Chronik crates. See [04 §5](04-chronik-reuse.md#5-relationship-options-separate-product-vs-chronik-capability).
