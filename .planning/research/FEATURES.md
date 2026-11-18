# Feature Research

**Domain:** Self-hosted durable execution / workflow engine (single Rust binary hosting uploaded Wasm workflows)
**Researched:** 2026-11-18
**Confidence:** MEDIUM-HIGH overall. Licences and release notes were verified from primary sources (GitHub LICENSE files, release notes, official docs). Some per-cell competitor details in the matrix come from training data and are marked (T).

Competitor versions checked: Restate v1.8.1 (2026-10-21), Inngest v1.53.0 (2026-11-12), Hatchet v0.114.2 (2026-10-26), Temporal server v1.32.2 (2026-11-02), Windmill v1.876.1 (2026-11-17), DBOS Transact py 3.6.0 / ts v5.6 (2026-11-17), Cloudflare Workflows (changelog up to 2026-04-01).

---

## 0. Headline findings (read first)

1. **Nobody else ships "self-hosted single binary + hosts your code".** This was verified again against current docs:
   - **Restate** still invokes *your* deployments (HTTP endpoints, Lambda, Cloud Run, Vercel, Cloudflare). Restate Cloud hosts the *server*, not your code. **Licence: BSL 1.1** (verified from the LICENSE file). Additional Use Grant: anything except a "Public Restate Platform Service". Change date: 4 years after release, then Apache-2.0. A SaaS that lets third parties register their own Restate deployments is forbidden. That is exactly Ostro persona P4.
   - **Inngest** self-host is one binary (`inngest start`). It defaults to SQLite plus in-process Redis. HA needs external Postgres + Redis, plus a Helm chart with KEDA. Code runs in your app (serve/connect). **Licence: SSPL with delayed Apache-2.0 (3 years)** (verified from LICENSE.md). Self-host gaps from the docs: no automatic retention on Postgres, metrics behind an experimental flag, no auth on `/metrics`.
   - **Hatchet** (MIT) needs Postgres (`hatchet-lite`), optionally plus RabbitMQ. It now has an "embedded mode" that boots the engine with *embedded-postgres* as a sidecar, but that is for tests and CI only. "Sandboxes" are Cloud-only and delegate to third-party vendors (e2b etc.).
   - **DBOS** (MIT library) needs Postgres. Its UI and ops plane (**Conductor**) is proprietary, and self-hosting it in production needs a paid licence key.
   - **Windmill** hosts code but needs Postgres. It is AGPLv3 + proprietary EE. Concurrency limits, step priority, audit logs, autoscaling and SSO beyond 10 users are **Enterprise-only**.
   - **Cloudflare Workflows** hosts code but only on Cloudflare. Limits: 1 MiB step results/events, 10k (max 25k) steps, sleep ≤ 365 days, completed-instance retention ≤ 30 days. Python is still beta.
   - **Temporal** (MIT) is a multi-service server plus a DB. Your workers host the code. "Serverless Workers" (Lambda) is only pre-release.
2. **Self-hosted Restate has no authentication on ingress or admin, by design.** Its docs say "put a reverse proxy in front". Ostro's scoped tokens and tenants (FR-3.9, NFR-6) are a real differentiator for P3/P4, not just a checkbox.
3. **Flow control has become table stakes and is now *keyed*.** In 2026 every serious competitor ships per-key concurrency and fairness: Inngest concurrency keys, throttle, rate-limit, debounce, singleton, batching and priority; Hatchet concurrency strategies (GROUP_ROUND_ROBIN etc.), dynamic rate limits and priority; Restate v1.7–1.8 scopes plus L1/L2 limit keys; DBOS partitioned queues, priority, dedupe and debouncer; Temporal Priority & Fairness, GA May 2026. The PRD's FR-3.8 (per-queue concurrency + rate limit) is **below the bar**.
4. **"Rerun from step" and "pause/resume" are now standard ops features.** Restate v1.6 has restart-from-journal-prefix, pause/resume, and *pause by default on retry exhaustion*. Inngest has rerun-from-step and bulk replay by time range and status. DBOS has fork-from-step on a chosen code version. Hatchet has bulk retries/cancellations and pausing workflows. Cloudflare has pause/resume/restart. The PRD only has `:retry` and `:cancel`. **Gap.**
5. **Streaming out of running workflows is the 2026 trend, driven by AI agents.** Inngest has Realtime, Hatchet has Durable Streams, Temporal has Workflow Streams (public preview), DBOS has streams, and Cloudflare has streamed step output. The PRD has nothing here. **Gap**, and P2 is acceptable for 1.0.
6. **Versioning is where Ostro's architecture wins for free.** Restate pins invocations to immutable deployments but tells users to avoid long-running handlers, because old deployments must be kept running. Temporal needed Worker Versioning (GA March 2026) plus a Worker Controller. Ostro stores every module by sha256, so pinning runs to their module costs nothing and needs no fleet. This should be marketed as a headline differentiator.

---

## 1. Competitor feature matrix (current docs, late 2026)

Legend: Y = shipped · P = preview/beta · EE = paid/enterprise only · Cloud = managed only · — = not offered · (T) = from training data, not re-verified this session.

| Capability | Temporal | Restate | Inngest | Hatchet | Windmill | DBOS | CF Workflows | **Ostro PRD** |
|---|---|---|---|---|---|---|---|---|
| Step primitive | Activities | `ctx.run` | `step.run` | tasks/durable tasks, DAGs | flow steps | `@step` | `step.do` | FR-1.2 |
| Parallel / fan-out | Y | Y | `group.parallel` | Y (DAG, children) | for-loops/branches | Y | Y (Promise.all) | FR-1.3 |
| Durable sleep / sleep-until | Y | Y | Y | Y | Y | Y | Y (≤365 d) | FR-1.4 |
| Wait for external input | signals | awakeables, durable promises | `waitForEvent` (match expr), `waitForSignal` (by name) | durable event waits | suspend/approval | `recv`, events | `waitForEvent` | FR-1.5 signals (by run id only) |
| Read-only query | queries | shared handlers | — | — | — | `get_event` | — | FR-1.5 queries (P1) |
| Synchronous update (signal + response) | Updates, Update-with-Start (T) | handler call to workflow | — | — | — | — | — | **Gap** |
| Child workflows | Y (parent-close policy) | service calls | `step.invoke` | child spawning | sub-flows | child wf | — | FR-1.6 |
| Saga helper | Java SDK only (T); docs pattern | guide | "rollbacks" docs | — | error handler | — | — | FR-1.11 (P2) |
| Versioning | patching + Worker Versioning GA 2026 | immutable deployments (pin) | step-ID memoization, no pin | — | script versions/hash (T) | `app_version` + patching | — (T) | FR-1.8 patched + pin to module hash |
| Retry policy / backoff | Y | Y, pause on exhaustion | Y + `NonRetriableError` | Y | Y | Y | Y (limit, delay, backoff) | FR-1.2 (backoff unspecified) |
| Run-level timeout | Y | Y | Y (start/finish timeouts) | Y | Y | `SetWorkflowTimeout` | — (T) | **Gap** (step timeout only) |
| Concurrency (global) | Y (task queues) | Y | Y | Y | EE | Y | per-workflow cap | FR-3.8 |
| **Keyed concurrency / fairness** | Fairness GA 2026 | scope + L1/L2 limit keys | concurrency keys | GROUP_ROUND_ROBIN (T) | — | partitioned queues | — | **Gap** |
| Rate limit (start rate) | Y (T) | planned | rate-limit (drop) + throttle (queue) | static + dynamic keyed | — | `limiter` | — | FR-3.8 (per queue), FR-4.5 (outbound) |
| Debounce | — | — | Y | — (T) | — | Debouncer | — | **Gap** |
| Singleton per key | wf-id reuse policy | wf key once | Y (skip/replace) | — | — | dedupe id | instance id | **Gap** (only idempotency_key) |
| Batching (events into one run) | — | — | Y | batch tasks | — | — | — | not needed 1.0 |
| Priority | GA 2026 | planned | Y | Y | EE | Y | — | FR-3.2 |
| Cron | Schedules (overlap, backfill, pause) | delayed send pattern | Y | Y | Y | `@scheduled` | Cron Triggers (T) | FR-3.7 (no overlap/backfill) |
| Delayed start | Y | delayed send | Y | scheduled runs | Y | `delay_seconds` | — | FR-3.2 `delay` |
| Webhook ingress w/ signature verify | — | Cloud only | Y (transforms) | Y (Stripe/GitHub/Slack cookbooks) | Y | — | via Worker code | FR-3.6 |
| Event bus (1 event → N workflows) | — | Kafka subscriptions | core model | events + filters (CEL) | triggers | — | — | FR-3.10 Kafka only (P2) |
| Idempotent start / dedupe | wf-id | idempotency key | idempotency expr (24 h) | Y | — | wf-id / dedupe id | instance id | FR-3.2 |
| Cancel (graceful) vs terminate | cancel + terminate | cancel + kill | cancel, cancelOn event | Y | Y | cancel | terminate | FR-3.4 cancel only |
| Pause / resume run | Y (2026, T) | Y (v1.6) | pause *functions* | pause workflows | — | — | Y | **Gap** |
| Rerun / restart from step | reset to event | restart from journal prefix | rerun from step | replay | restart from step (T) | fork from step | restart (from start) | **Gap** |
| Bulk ops (cancel/retry/replay by filter) | batch ops | batch ops (v1.6) | bulk replay/cancel | bulk retries/cancels | — | bulk fork (UI) | — | **Gap** |
| DLQ / failure handling | failed list | paused list | failure handlers | on-failure task | error handlers (some EE) | ERROR status | — | DLQ in UI (FR-0.5); no FR |
| Search by custom attributes | search attributes | SQL introspection | Insights (SQL) | additional metadata | Y | list filters | — | **Gap** (status filter only) |
| Streaming progress out | Workflow Streams (P) | — | Realtime | Durable Streams | — | streams | streamed step output | **Gap** |
| Web UI | Y | UI 1.0 (bundled) | Dev Server UI | Y | Y | Conductor (proprietary) | dashboard | FR-0.5 |
| Local dev | CLI dev server | `restate up` | Dev Server | CLI / lite / embedded | docker | lib + PG | `wrangler dev --local` | FR-0 `ostro dev` |
| Time-skipping tests | Y (test server) | — (testcontainers) | mocked steps | embedded engine | — | — (T) | `cloudflare:test` | FR-1.10 (P1) |
| Replay-from-history CI test | Y | — | — | — | — | — | — | FR-2.6 |
| OTel / Prometheus | Y | Y (OTel) | traces; Prom experimental (self-host) | Y | Y (T) | Y (T) | — | NFR-4 |
| AuthN on API (self-host) | plugin (JWT authorizer) (T) | **none by design** | event + signing keys | API tokens, roles, SSO | users/groups, SSO EE | — | CF account | FR-3.9 scoped tokens |
| Multi-tenancy | namespaces | scopes | env + tenant keys (EE key-queues) | tenants | workspaces (CE max 3) | — | — | NFR-6 tenants + quotas |
| Self-host footprint | server + DB (+ES opt.) | 1 binary (RocksDB) | 1 binary; HA = +PG +Redis | engine + PG (+RabbitMQ) | server + PG + workers | your app + PG | n/a | **1 binary, embedded** |
| HA | Y | 3+ nodes, replicated log, S3 snapshots | via PG/Redis replicas | via PG | via PG | via PG | vendor | FR-6.4 (Phase 4) |
| Hosts your code | — (Serverless Workers pre-release) | — | — | — (sandboxes Cloud only) | Y | — | Y (CF only) | **Y (Wasm)** |
| Licence | MIT | BSL 1.1 → Apache after 4 y | SSPL → Apache after 3 y | MIT | AGPLv3 + proprietary EE | MIT (Conductor proprietary) | proprietary | Apache-2.0 |

---

## Feature Landscape

### Table Stakes (Users Expect These)

Missing any of these makes Ostro feel like a toy next to Inngest, Hatchet and Restate. "PRD" maps to existing IDs. **GAP** means it is missing from the PRD or under-specified there.

| # | Feature | Why Expected | Complexity | PRD | Notes |
|---|---|---|---|---|---|
| T1 | Workflow + step primitives with typed JSON I/O | Universal | MEDIUM | FR-1.1, FR-1.2 | Steps are journaled guest computation. Effects (http/sql) are the "activities". |
| T2 | Parallelism (gather/all/join) + deterministic race/select | Universal | MEDIUM | FR-1.3 | — |
| T3 | Durable sleep / sleep_until (no memory held) | Universal | MEDIUM | FR-1.4 | Document the max sleep. CF caps at 365 d; Ostro should have no cap. |
| T4 | Wait for signal with timeout | Human-in-the-loop is the #1 marketed use case everywhere | MEDIUM | FR-1.5 | — |
| T5 | **Callback/await tokens addressable without knowing the run id** | Restate awakeables, Inngest `waitForSignal(name)` / `waitForEvent(match)`, CF `waitForEvent(type)`. Webhook callbacks (e.g. "payment confirmed for order 42") need this | MEDIUM | **GAP** | Recommend `ctx.await_token(key?)` returning a token/URL, plus `POST /v1/signals/{key}` that resolves *by key*. This removes the "store run_id somewhere" step. Builds on T4. |
| T6 | Retry policy with exponential backoff, max interval, jitter, max attempts, **non-retryable error type** | Every competitor | LOW | FR-1.2 (partial) | **GAP:** specify backoff fields and a `NonRetryable`/`Terminal` error in all three SDKs. |
| T7 | Step timeout **and run-level (execution) timeout** | Temporal, Inngest, DBOS, Hatchet, Windmill | LOW | FR-1.2 step only | **GAP:** add a run timeout (from start) and a schedule-to-start timeout (max queue wait). |
| T8 | Child workflows + cancellation propagation | Temporal, Inngest `step.invoke`, Hatchet, DBOS | MEDIUM | FR-1.6 (P1) | Add a fire-and-forget/detached start and a parent-close policy (cancel / abandon). |
| T9 | Idempotent start: user-supplied run id or idempotency key **with conflict policy** (reject / return-existing) and dedupe window | DBOS `workflow_id_reuse_policy` / `duplication_policy`, Temporal id-reuse, Inngest 24 h idempotency | LOW | FR-3.2 (partial) | **GAP:** specify conflict policy and window semantics. |
| T10 | Cancel (graceful, guest observes cancellation and can compensate) **vs** terminate (hard stop) | Temporal, Restate (cancel vs kill) | MEDIUM | FR-3.4 cancel only | **GAP:** add terminate. Cancel must surface as an exception/error in the guest so saga code runs. |
| T11 | Cron schedules: timezone, **overlap policy (skip/buffer/allow), catch-up after downtime, pause/unpause, trigger-now** | Temporal Schedules set the bar; Hatchet/Inngest/DBOS all have cron | MEDIUM | FR-3.7 (partial) | **GAP:** overlap + catch-up policy. Exactly-once per `(schedule, fire_time)` is already specified, which is good. |
| T12 | Delayed start | Universal | LOW | FR-3.2 `delay` | — |
| T13 | Webhook ingress with signature verification (Stripe/GitHub/Slack/HMAC) | Inngest, Hatchet (cookbooks), Windmill. P2 persona requirement | MEDIUM | FR-3.6 (P1) | Inngest also offers payload *transforms*. Consider a JSON path / tiny mapping. |
| T14 | Global + per-queue concurrency limits | Universal | MEDIUM | FR-3.8 | — |
| T15 | **Keyed concurrency (per tenant/user/key) with fair scheduling** | Inngest concurrency keys, Hatchet group strategies, Restate limit keys, DBOS partitioned queues, Temporal Fairness GA 2026 | HIGH | **GAP** | Needed by P3/P4 and any SaaS. Key derived from input (JSON path / expression) at enqueue. The partition-owned dispatcher makes this cheaper than on PG. See ARCH. |
| T16 | Start-rate limits (throttle = queue, rate-limit = drop) | Inngest, Hatchet, DBOS | MEDIUM | FR-3.8 (queue), FR-4.5 (outbound) | Distinguish *start throttling* from *outbound* token buckets. Both are needed. |
| T17 | Priority | Inngest, Hatchet, DBOS, Temporal | LOW | FR-3.2 | — |
| T18 | Run list/inspect/history + filter by status, workflow, time, **custom tags/metadata** | Temporal search attributes, Hatchet additional metadata, Restate SQL | MEDIUM | FR-3.3 (status only) | **GAP:** user-set run tags at start, plus `ctx.set_tags`. Index a bounded set. |
| T19 | **Failed-run handling: DLQ view + retry; pause-on-exhaustion option; on-failure hook** | Restate pauses by default; Inngest failure handlers; Hatchet on-failure | MEDIUM | DLQ only mentioned in FR-0.5 | **GAP:** define a DLQ FR. Add `on_failure` (run a workflow/step with the error) and queue-level "pause instead of fail". |
| T20 | **Rerun from step / restart from journal prefix** | Restate v1.6, Inngest, DBOS fork, Hatchet replay, Temporal reset | MEDIUM | **GAP** | Natural for a journal engine: copy the journal prefix up to seq N into a new run, optionally on a new module version. Strong UX moment in the UI. |
| T21 | **Bulk operations** (cancel/retry/rerun by filter: workflow, status, time range, tag) | Restate, Inngest, Hatchet, Temporal batch | MEDIUM | **GAP** | Must be rate-spread (Inngest spreads replays over time). |
| T22 | **Pause/resume** of a run and of a workflow/queue (stop starting new runs, hold backlog) | Restate, CF, Inngest (functions), Hatchet | LOW-MEDIUM | **GAP** | Queue pause is the incident lever operators reach for first. |
| T23 | Embedded web UI: runs, timeline, inputs/outputs per step, errors with stack, **actions** (cancel, retry, signal, rerun-from-step, pause) | Restate UI 1.0 bundled in binary; Inngest dev server UI | HIGH | FR-0.5 (P1) | Spec the UI *actions*, not just views. Read-only UI is below the bar. |
| T24 | Local dev server = the real thing, with hot reload | Inngest Dev Server, `restate up`, Hatchet embedded, `wrangler dev --local` | MEDIUM | FR-0, FR-2.7 | Ostro's `ostro dev` is structurally better (same binary). |
| T25 | Test harness: run a workflow in-process, mock effects, **time-skipping** | Temporal (gold standard), CF `cloudflare:test`, Inngest `InngestTestEngine` (step mocks) | MEDIUM | FR-1.10 (P1) | Restate has *no* time-skipping (testcontainers only). Ostro's virtualised clock makes this easy. Ship at 1.0. |
| T26 | Retention/TTL of completed runs | Inngest self-host *lacks* this (complaint in docs); Restate configurable | LOW | FR-6.3 | — |
| T27 | OTel traces + Prometheus metrics + structured logs | Universal | MEDIUM | NFR-4 | Metrics on by default with auth (Inngest's are experimental and unauthenticated). |
| T28 | API auth with tokens | Every product except self-hosted Restate | LOW | FR-3.9 | — |
| T29 | HTTP + gRPC API, OpenAPI spec, thin clients for TS/Py/Go/Rust | Universal | MEDIUM | FR-3 | Restate 1.8 added an OpenAPI dump plus a `--json` CLI for scripts/agents. Match it. |
| T30 | Versioning story that keeps in-flight runs safe | Universal pain point | MEDIUM | FR-1.8 + pin to module hash | — |
| T31 | Backups / restore for single node | Litestream-era expectation; Restate S3 snapshots | MEDIUM | FR-2.8, FR-6.2 | — |
| T32 | Documented limits (payload size, steps per run, history length, signal size) | CF and Inngest publish limits pages | LOW | **GAP** | Decide defaults: e.g. 4 MiB step result, 50k journal entries before `continue_as_new` is recommended. Publish them. |

### Differentiators (Competitive Advantage)

Ordered by strength. Each one aligns with the Core Value ("download one binary, deploy a Wasm workflow, it runs durably").

| # | Feature | Value Proposition | Complexity | PRD | Notes |
|---|---|---|---|---|---|
| D1 | **Server hosts the code (upload Wasm), self-hosted, single binary** | No worker fleet, no app to keep alive. No competitor offers this self-hosted (verified above) | HIGH | FR-0, FR-2.4, ARCH §5 | The moat. Everything else is secondary. |
| D2 | **Free, perfect version pinning**: every run is pinned to its module sha256, old modules are retained automatically, plus a `replay` CI gate | Restate makes you keep old deployments *running*. Temporal needed Worker Versioning + Controller (GA 2026). Ostro just keeps a blob | LOW (given D1) | FR-1.8, FR-2.6 | Add operator action "migrate run(s) to version X", guarded by automatic replay verification (Restate offers resume-on-different-deployment *unguarded*). |
| D3 | **Sandbox-enforced determinism** with precise divergence errors | No "stray Date.now()" incidents. Competitors rely on discipline (Temporal has a TS sandbox only; DBOS/Inngest/Restate none) | HIGH | FR-1.7, NFR-7 | — |
| D4 | **Safe for untrusted/multi-tenant code** (fuel, memory caps, egress allow-lists, no ambient authority) | Restate's BSL forbids exactly this use (third parties registering their code). Hatchet sandboxes are Cloud-only and delegate to vendors | HIGH | NFR-1, FR-4.4, NFR-6 | P4 persona. Pair with tenant quotas and keyed fairness (T15). |
| D5 | **Apache-2.0 everything**, including cluster/HA, UI, auth, multi-tenancy | Restate BSL, Inngest SSPL, Windmill AGPL+EE (concurrency limits are EE!), DBOS Conductor proprietary | LOW | PRD §6 Q1 | Resolve Q1 = Apache-2.0. The licence is a feature here. |
| D6 | **Built-in auth + tenants on self-host** | Self-hosted Restate has none; Inngest only has event/signing keys | MEDIUM | FR-3.9, NFR-6 | — |
| D7 | **Exactly-once SQL effects + effectively-once HTTP (auto Idempotency-Key)**, secrets never in guest memory | Others leave idempotency to the user | MEDIUM | FR-4.1, FR-4.2, NFR-3.4 | Document clearly, with a demo against Stripe. |
| D8 | **Bottomless single node** (S3 segments + snapshots) | HA-lite without a cluster. Inngest/Hatchet need PG replication for this | MEDIUM | FR-6.2 | — |
| D9 | **Time-skipping test harness + replay-from-production histories, identical across 3 languages** | Only Temporal has both. Restate has neither | MEDIUM | FR-1.9, FR-1.10, FR-2.6 | Promote FR-1.10 to 1.0 must-have (it already is P1). |
| D10 | Same binary scales laptop → workers → HA cluster, with spillover | k3s/NATS-style growth story | HIGH | FR-0.4, FR-5, FR-6.4 | — |
| D11 | Queries without running guest code: `ctx.set_status(json)` / published state readable via API | Temporal queries need a live worker. Ostro queries would need instantiate + replay (expensive, esp. Python) | LOW | FR-1.5 queries (P1) | **Recommendation:** ship DBOS-style `set_event`/status publishing at 1.0 and keep true query handlers P2. |
| D12 | Agent/automation-friendly CLI (`--json`, `--dry-run`) + MCP server | Restate 1.8, Inngest MCP, Hatchet agent tools: an emerging expectation | LOW | **GAP** | Cheap. `--json` belongs at 1.0. An MCP server can come in 1.x. |

### Anti-Features (Commonly Requested, Often Problematic)

| Feature | Why Requested | Why Problematic | Alternative |
|---|---|---|---|
| Visual workflow designer / low-code flows (Windmill-style) | Demos well | Huge surface. Fights code-first replay semantics. Already a PRD non-goal | Read-only timeline/graph of executed steps in the UI |
| General FaaS / HTTP serving (Inngest "durable endpoints", Restate as API) | "Why not serve my API too?" | Different latency/scaling profile. PRD non-goal | FR-3.5 long-poll result. Webhook ingress |
| Raw `wasi:http` / sockets / filesystem in guests | Use existing libraries unchanged | Breaks determinism, egress control and idempotency | Host effects (http/sql/object store) with SDK adapters that look like fetch/requests |
| Native-code workers at 1.0 | numpy, native drivers | Splits semantics, reintroduces worker fleet (kills D1) | Post-1.0 native worker protocol (PRD Q3) |
| Restate-style durable K/V state per key ("virtual objects") at 1.0 | Restate users like it | Second programming model, new storage surface, consistency questions | Keyed singleton/concurrency (T15) + SQL connector. Revisit post-1.0 |
| Activity heartbeats | Temporal users expect them | Steps run in the host sandbox (fuel/epoch). Long I/O is host-side with timeouts. Heartbeats solve a worker-liveness problem Ostro doesn't have | Leases + epoch deadlines (NFR-1.2, NFR-3.5) |
| Event batching (N events → 1 run) | Inngest has it | Niche. Complicates exactly-once and dedupe | Debounce + fan-in via a parent workflow waiting on signals |
| Linear-memory snapshots instead of replay | "Just checkpoint the VM" | Rejected in research (size, portability, upgrades) | Replay + pre-init at build time |
| Multi-region active-active | Enterprise checklists | Out of 1.0 scope by far | Bottomless restore in another region |
| Hosted sandbox VMs for agents (Inngest/Hatchet "Sandboxes") | AI agent hype | Different product (VM orchestration) | HTTP effect to sandbox vendors. Wasm isolation for in-workflow code |
| Workflow-defined DAG YAML / CEL trigger language | Hatchet/Inngest expressions | A second language to maintain | JSON-path keys for concurrency/idempotency/webhook mapping only |
| Unlimited history per run | Long-lived entities | Replay cost grows, recovery slows | Enforced history limit + `continue_as_new` (FR-1.8) with a warning threshold |

---

## Feature Dependencies

```
Journal + replay engine (ARCH §2-5)
    ├──requires──> Deterministic WASI virtualisation (FR-1.7)
    │                  └──enables──> Time-skipping test harness (FR-1.10, T25)
    │                  └──enables──> Replay CI gate (FR-2.6) ──enables──> Guarded "migrate run to version" (D2)
    ├──enables──> Rerun from step / fork journal prefix (T20)
    │                  └──enables──> Bulk rerun (T21)
    └──enables──> Module pinning by sha256 (D2) ──requires──> content-addressed module store (FR-2.4)

Dispatcher (ready queues + timer wheel, per partition)
    ├──requires──> Durable timers (FR-1.4)
    │                  └──enables──> Cron (FR-3.7) ──enhanced by──> overlap/catch-up policy (T11)
    │                  └──enables──> Delayed start, debounce (T12, debounce)
    ├──enables──> Queue concurrency + priority (FR-3.8, T17)
    │                  └──enables──> Keyed concurrency / fairness (T15)
    │                                     └──enables──> Singleton per key, per-tenant quotas (NFR-6)
    └──enables──> Pause/resume queue & run (T22) ──enables──> Pause-on-retry-exhaustion (T19)

Signals by run id (FR-1.5)
    └──enables──> Await tokens / signal-by-key (T5) ──enables──> Webhook → resume waiting run (FR-3.6)
    └──enables──> Updates (signal + response) (P2)

Run tags/metadata (T18) ──enables──> Search, bulk ops by filter (T21), UI filters (T23)
Control-plane API (FR-3) ──requires before──> UI actions (T23), CLI (FR-2.5)
Auth tokens (FR-3.9) ──requires before──> Tenants/quotas (NFR-6) ──requires──> Keyed fairness (T15)
Cancellation-as-exception in SDKs (T10) ──requires before──> Saga helper (FR-1.11)
Child workflows (FR-1.6) ──requires──> cancellation propagation + parent-close policy (T8)

Query handlers (FR-1.5 P1) ──conflicts──> cheap eviction / Python memory budget
    (requires instantiate+replay per query) → prefer published status (D11)
```

### Dependency Notes

- **T15 (keyed concurrency) needs a data-model decision early.** The concurrency key must be computed at enqueue time and stored with the ready-queue entry. Retrofitting it after the dispatcher is built means reworking the queues. Design it in Phase 1–2, even if the API ships in Phase 3.
- **T20 (rerun from step) is cheap only if journal entries are self-contained.** It needs entries with step results plus a seq-number cut and a "fork" record. Decide the journal format with this in mind (it also serves D2).
- **T5 (await tokens) depends on a key → (run, wait-op) index** in partition state. If keys must route to a partition, either hash the key to the run's partition (embed the partition in the token) or keep a metadata-level lookup. Embed partition + run id in an opaque token. A user-chosen key needs a global index (harder; P2).
- **T10 (graceful cancel) must be in the WIT contract from day one** (`cancelled` result on any await). Adding it later changes SDK semantics in all three languages.
- **D11 vs FR-1.5 queries:** queries must replay the guest, which conflicts with evict-on-sleep for Python (10–50 MB per instance). Published status avoids that.

---

## MVP Definition

### Launch With (1.0: credible against Inngest/Hatchet/Restate)

Primitives
- [ ] T1–T4, T6, T7, T8 (steps, parallel, sleep, signals, retry/backoff + non-retryable, step + run timeouts, child workflows). *Core semantics: users leave without these.*
- [ ] T5 await tokens / signal-by-token: *webhook callbacks are the #1 human-in-the-loop pattern.*
- [ ] T9 idempotent start with conflict policy, T10 cancel vs terminate (cancel visible to guest).
- [ ] FR-1.8 patched + continue_as_new, plus D2 pinning (free).
- [ ] D11 published status (in place of query handlers).

Triggers & flow control
- [ ] T11 cron with overlap/catch-up, T12 delayed start, T13 webhooks with signature verification.
- [ ] T14 queue concurrency, T15 **keyed concurrency** (at least concurrency-per-key; fairness round-robin can be simple), T16 start throttle + outbound limits, T17 priority.

Operations
- [ ] T18 tags + filtering, T19 DLQ + pause-on-exhaustion option + on_failure hook, T20 rerun from step, T21 bulk cancel/retry, T22 pause/resume queue and run.
- [ ] T23 UI with actions, T26 retention, T27 OTel/Prometheus, T28 tokens, T29 OpenAPI + `--json` CLI, T31 backup/restore, T32 published limits.

Developer experience
- [ ] T24 `ostro dev` hot reload, T25 time-skipping test harness, FR-2.6 replay gate, FR-1.9 conformance.

### Add After Validation (1.x)

- [ ] Debounce and singleton modes (skip/replace). *Trigger: users request them for sync/webhook storms.* (LOW complexity once T15 exists. Could ship at 1.0 if cheap.)
- [ ] Streaming progress out of runs (`ctx.stream`, SSE endpoint). *Trigger: AI/agent use cases. Every competitor shipped this in 2025–26.*
- [ ] Synchronous Updates / signal-with-response, signal-with-start.
- [ ] True query handlers (FR-1.5).
- [ ] Event bus: `ctx.emit(event)` + event-triggered workflows (1 → N), then the Kafka/Chronik trigger (FR-3.10).
- [ ] Saga/compensation helper (FR-1.11). Trivial once T10 exists. Could be pulled into 1.0 as docs + a 30-line helper.
- [ ] MCP server for agents. Payload encryption at rest (Temporal codec / Inngest encryption middleware equivalent).
- [ ] UI SSO/OIDC login.

### Future Consideration (2.0+)

- [ ] Durable K/V state per key (virtual objects). *Second programming model.*
- [ ] Native worker protocol (PRD Q3).
- [ ] Event batching, workflow DSL/CEL expressions.
- [ ] Multi-region.

---

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---|---|---|---|
| Server-hosted Wasm, single binary (D1) | HIGH | HIGH | P1 |
| Version pinning + replay gate (D2) | HIGH | LOW | P1 |
| Keyed concurrency/fairness (T15) | HIGH | HIGH | P1 |
| Await tokens / signal-by-token (T5) | HIGH | MEDIUM | P1 |
| Rerun from step (T20) | HIGH | MEDIUM | P1 |
| Pause/resume queue/run, pause-on-exhaustion (T19, T22) | HIGH | LOW | P1 |
| Run timeout, non-retryable errors (T6, T7) | HIGH | LOW | P1 |
| Cancel vs terminate (T10) | MEDIUM | MEDIUM | P1 |
| Cron overlap/catch-up (T11) | MEDIUM | LOW | P1 |
| Tags + search (T18) | MEDIUM | MEDIUM | P1 |
| Bulk ops (T21) | MEDIUM | MEDIUM | P1 |
| UI actions (T23) | HIGH | HIGH | P1 |
| Time-skipping tests (T25) | HIGH | MEDIUM | P1 |
| Published status instead of queries (D11) | MEDIUM | LOW | P1 |
| `--json` CLI (D12) | MEDIUM | LOW | P1 |
| Debounce / singleton | MEDIUM | LOW | P2 |
| Streaming out | MEDIUM-HIGH (AI) | MEDIUM | P2 |
| Updates / signal-with-start | MEDIUM | MEDIUM | P2 |
| Query handlers | LOW-MEDIUM | MEDIUM | P2 |
| Event bus 1→N | MEDIUM | MEDIUM | P2 |
| Saga helper | MEDIUM | LOW | P2 |
| MCP server, payload encryption, SSO | MEDIUM | LOW-MEDIUM | P2 |
| Virtual objects / K/V state | MEDIUM | HIGH | P3 |
| Batching | LOW | MEDIUM | P3 |

---

## PRD Gap List (for requirements definition)

Proposed new or amended requirements. IDs are suggestions.

| Proposed | Change | Pri | Basis |
|---|---|---|---|
| FR-1.2 (amend) | Retry policy fields: `initial_interval`, `backoff_coefficient`, `max_interval`, `max_attempts`, `jitter`; `NonRetryableError` in all SDKs | P0 | All competitors |
| FR-1.12 (new) | Run execution timeout + max queue-wait (schedule-to-start) timeout | P0 | Temporal, Inngest, DBOS |
| FR-1.13 (new) | Await tokens: `ctx.create_token()` → opaque token/URL; `POST /v1/tokens/{token}:resolve` completes the wait with payload; timeout | P0/P1 | Restate awakeables, Inngest waitForSignal, CF waitForEvent |
| FR-1.14 (new) | Cancellation is observable in the guest (error on await) with a cleanup window; separate `:terminate` | P0 (WIT), P1 (API) | Temporal, Restate cancel vs kill |
| FR-1.5 (amend) | Replace P1 queries with `ctx.set_status(json)` readable via `GET /v1/runs/{id}/status`; queries → P2 | P1 | DBOS events; Python memory cost |
| FR-1.6 (amend) | Detached child start, parent-close policy | P1 | Temporal |
| FR-3.2 (amend) | Conflict policy for `idempotency_key`/user run id: `reject` / `return_existing`; dedupe window; `tags` map | P0 | DBOS, Temporal, Inngest |
| FR-3.7 (amend) | Cron overlap policy (skip/buffer-one/allow-all), catch-up window after downtime, pause/unpause, trigger-now, jitter | P1 | Temporal Schedules |
| FR-3.8 (amend) | Concurrency keys (JSON path of input or explicit `concurrency_key` at start) with per-key limit + round-robin fairness across keys; start throttle (queue) vs rate limit (drop) | P1 | Inngest, Hatchet, Restate v1.7–1.8, DBOS, Temporal 2026 |
| FR-3.11 (new) | Pause/resume a run; pause/resume a queue or workflow (accept but don't start); queue option `on_exhausted: fail | pause` | P1 | Restate v1.6, CF, Inngest, Hatchet |
| FR-3.12 (new) | Rerun: from start, or from step seq N, optionally on a different module version (replay-verified); creates a new run linked to the original | P1 | Restate, Inngest, DBOS fork |
| FR-3.13 (new) | Bulk cancel/terminate/retry/rerun by filter (workflow, status, time range, tags), rate-spread, with progress | P1 | Restate, Inngest, Hatchet, Temporal |
| FR-3.14 (new) | DLQ definition: failed runs retained with error, last attempt, journal; list/retry/purge; `on_failure` workflow hook | P1 | Inngest failure handlers, Hatchet on-failure |
| FR-3.15 (new) | Run search by tags/metadata (bounded indexed keys) | P1 | Temporal search attributes, Hatchet metadata |
| FR-0.5 (amend) | UI must support actions: cancel, terminate, signal/resolve token, retry, rerun-from-step, pause/resume, bulk ops | P1 | Restate UI 1.0, Inngest |
| FR-2.9 (new) | CLI `--json` output and `--dry-run` on mutating commands; OpenAPI dump | P1 | Restate 1.8 |
| NFR-8 (new) | Published limits: max input/step result/signal payload, journal entries per run (with continue_as_new warning), schedules, tokens | P0 | CF, Inngest limits pages |
| FR-1.15 (new, P2) | Streaming: `ctx.stream(topic, chunk)` → SSE/gRPC stream for subscribers; not journaled as replay input | P2 | Inngest Realtime, Hatchet Durable Streams, Temporal Workflow Streams, DBOS streams |
| FR-3.16 (new, P2) | Debounce and singleton (skip/replace) per key | P2 | Inngest, DBOS |
| FR-1.16 (new, P2) | Update (signal with synchronous response) and signal-with-start | P2 | Temporal |
| FR-1.11 | Saga helper: consider moving to P1 (cheap once FR-1.14 exists) | P1? | Market pattern |

---

## Competitor Notes (concise, verified where marked)

- **Temporal** (MIT, server v1.32.2): 2026 GA releases were Worker Versioning (Mar), Task Queue Priority & Fairness (May), Worker Controller (May) and Standalone Activities (Sep, per changelog). Workflow Streams and External Storage (large payloads) are public preview. Serverless Workers (Lambda) are pre-release. The Rust SDK is public preview. Testing has time-skipping test servers and `runReplayHistories` for CI. *Verified: Replay 2026 announcement, changelog, TS testing docs.*
- **Restate** (BSL 1.1, v1.8.1): v1.6 added pause/resume, pause-on-retry-exhaustion, restart from journal prefix, batch invocation ops, `restate up`, and Azure/GCS snapshots. v1.7 added flow control (concurrency per scope), UI 1.0, dynamic Kafka. v1.8 turned VQueues on by default, made the CLI `--json`/`--dry-run`, and added OpenAPI. Planned: throttling/rate limits, priorities, backlog limits. Clustering: Raft metadata, replicated log, S3/GCS/Azure snapshots, default 24 partitions (fixed after provisioning). Self-host security: none built in. Webhooks are Cloud-only. Testing uses testcontainers (no time-skipping). *Verified: LICENSE, release notes, docs.*
- **Inngest** (SSPL → Apache after 3 y, v1.53.0): one binary with SQLite + in-memory Redis by default. Scaling needs PG + Redis. Flow control: concurrency (keys), throttling, rate limiting, batching, singleton, debounce, priority, multi-tenancy (key-queues are Enterprise). Ops: rerun from step, bulk replay by time range and status, bulk cancel, pause functions. Primitives: `waitForEvent`, `waitForSignal`, `step.invoke`, `group.parallel`, `defer`, Realtime streaming, checkpointing. Versioning works by step-ID memoization (no pinning). Testing: `@inngest/test` with mocked steps. *Verified: docs, LICENSE.*
- **Hatchet** (MIT, v0.114.2): engine + Postgres (optional RabbitMQ). Embedded mode (embedded-postgres sidecar) for tests/CI. Durable tasks, DAGs, child spawning, sleeps, event waits, task eviction, cron, scheduled, events with CEL filters, webhooks, concurrency, rate limits, priority, idempotency, sticky assignment, worker affinity, slot cost, bulk retries/cancels, pausing workflows, durable streams, SSO, roles, audit logs, Prometheus, OTel. Sandboxes and serverless endpoints are Cloud features. *Verified: docs index, embedded/lite/sandboxes pages.*
- **Windmill** (AGPLv3 + proprietary EE, v1.876.1): hosts scripts and flows. Suspend/approval, sleep, retries, error handlers, branches, loops, caching, timeouts. Concurrency limits, step priority, audit logs, autoscaling, critical alerts and SAML are EE. CE caps: 50 users, 10 SSO users, 3 workspaces, 30-day job retention. *Verified: pricing page, LICENSE.*
- **DBOS** (MIT library, Conductor proprietary): Postgres-backed. Queues with global/worker concurrency, limiter, priority, dedupe (`reject` / `return-existing`), partitioned queues. Debouncer, durable sleep, send/recv, suspend mode, workflow timeouts, fork from step, patching. Self-hosted Conductor needs a paid licence key for production. *Verified: docs, DBOS blog (June–Aug 2026).*
- **Cloudflare Workflows** (proprietary): `step.do`, sleep, `waitForEvent`, pause/resume/restart/terminate, `cloudflare:test`, `--local` wrangler commands (Apr 2026), streamed step output (Mar 2026). Python beta. Limits verified from the limits page.

---

## Sources

- Restate LICENSE (BSL 1.1), read via `gh api repos/restatedev/restate/contents/LICENSE`: HIGH
- Restate release notes v1.6.0 / v1.7.0 / v1.8.0, via `gh api .../releases`: HIGH
- https://docs.restate.dev/services/flow-control: HIGH
- https://docs.restate.dev/operate/clusters: HIGH
- https://docs.restate.dev/server/security.md (no built-in auth): HIGH
- https://docs.restate.dev/services/versioning.md, https://docs.restate.dev/develop/ts/testing.md: HIGH
- https://docs.restate.dev/hosting/overview.md (code runs on your compute): MEDIUM
- Inngest LICENSE.md (SSPL + Apache-2.0 future licence), via GitHub API: HIGH
- https://www.inngest.com/docs/self-hosting: HIGH
- https://www.inngest.com/llms.txt and docs-markdown pages (flow control, singleton, rerun, bulk replay, testing, versioning, waitForSignal): HIGH
- https://www.inngest.com/blog/inngest-1-0-announcing-self-hosting-support: MEDIUM
- https://docs.hatchet.run/llms.txt, /v1/embedded, /self-hosting/hatchet-lite, /v1/sandboxes/index: HIGH
- https://www.windmill.dev/pricing, https://www.windmill.dev/docs/flows/flow_editor, Windmill LICENSE: HIGH
- https://docs.dbos.dev/python/reference/queues, https://docs.dbos.dev/python/tutorials/workflow-tutorial: HIGH
- https://dbos.dev/blog/new-in-dbos-july-2026, https://dbos.dev/blog/what-is-dbos-conductor: MEDIUM
- https://developers.cloudflare.com/workflows/reference/limits/, https://developers.cloudflare.com/workflows/reference/changelog/: HIGH
- https://temporal.io/blog/replay-2026-product-announcements, https://temporal.io/changelog: HIGH
- https://docs.temporal.io/develop/typescript/testing-suite, https://docs.temporal.io/develop/python/best-practices/testing-suite: HIGH
- Temporal Updates, Schedules overlap/backfill, search attributes, Windmill restart-from-step, CF versioning behaviour: training data (T), LOW-MEDIUM, verify if load-bearing

---
*Feature research for: self-hosted durable execution engine (Ostro)*
*Researched: 2026-11-18*
