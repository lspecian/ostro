# 01 — Technical Research

This document gathers the constraints and prior art that shape the PRD and architecture. Every section ends with **design implications** that later documents cite.

- [1. Postgres internals & pgrx](#1-postgres-internals--pgrx)
- [2. Polyglot Wasm Component Model & toolchains](#2-polyglot-wasm-component-model--toolchains)
- [3. Async connector & I/O bridge](#3-async-connector--io-bridge)
- [4. Competitive landscape & positioning](#4-competitive-landscape--positioning)

---

## 1. Postgres internals & pgrx

### 1.1 Background workers (`bgworker`)

| Fact | Consequence for pgflow |
|---|---|
| Static bgworkers are registered in `_PG_init` and require the library in `shared_preload_libraries`, which means a server restart. | Installing in-DB mode always needs a restart. Document this loudly and offer the external-only mode (D3). |
| Dynamic bgworkers (`RegisterDynamicBackgroundWorker`) can start at runtime but count against `max_worker_processes`, the same pool that parallel query and logical replication use. | Ship a **supervisor** static worker that launches N dynamic executor workers, bounded by a GUC (`pgflow.max_executors`). Warn when the pool is nearly full. |
| Each bgworker is a full OS process forked from the postmaster. `_PG_init` runs in the postmaster *before* the fork. | **Never** start Tokio, Wasmtime or any thread in `_PG_init`. Threads do not survive `fork()`, and a lock held mid-fork deadlocks the child. Initialise everything in `bgworker_main`. |
| A bgworker that exits with code 1 is restarted after `bgw_restart_time`. A worker that dies **from a signal** (SIGSEGV, SIGABRT) or exits uncleanly makes the postmaster assume shared memory is corrupt, and it **restarts every backend** (crash-recovery cycle). | One native crash takes down every connection to the database, so this is the single most important risk. It drives the isolation tiers (D8), conservative Wasmtime config, and `panic = "unwind"` with catch boundaries. |
| bgworkers that use `BGWORKER_BACKEND_DATABASE_CONNECTION` can run transactions and SPI against **one** database. | One supervisor per database that has pgflow installed (`pgflow.databases` GUC), or a single "home" database. |
| Signal handling: the worker must honour SIGTERM (shutdown) and SIGHUP (config reload) and must wait on its **latch**, not sleep. | pgrx's `BackgroundWorker::attach_signal_handlers` + `wait_latch(timeout)` form the core loop of the main thread. |

### 1.2 Async runtimes inside a bgworker

Postgres is a process-per-connection, **single-threaded** program. Its memory contexts, `elog`/`ereport` (implemented with `longjmp`), SPI, the catalog caches and the latch machinery all assume one thread. Recent pgrx releases guard against calling Postgres from a non-main thread, but the guard catches mistakes; it does not make such calls safe.

**Safe pattern: "Postgres reactor + Tokio sidecar threads"**

```
 bgworker process
 ┌────────────────────────────────────────────────────────────────┐
 │ main thread (the ONLY thread that touches Postgres)            │
 │   loop {                                                       │
 │     wait_latch(WL_LATCH_SET | WL_SOCKET_READABLE(eventfd) | T) │
 │     drain cmd_rx: SPI txns (claim, checkpoint, enqueue outbox) │
 │     reply via oneshot                                          │
 │   }                                                            │
 ├────────────────────────────────────────────────────────────────┤
 │ Tokio multi-thread runtime (HTTP, timers, OTLP export)         │
 │ Wasm executor threads (Wasmtime stores, fibers)                │
 │   -> send DbCommand over mpsc, write eventfd to wake main      │
 └────────────────────────────────────────────────────────────────┘
```

- Wake the main thread with an `eventfd` registered as a socket wait event, or poll with a short-timeout `wait_latch`. Do **not** call `SetLatch` from a foreign thread unless you have verified it is safe on every supported version.
- Rust panics on worker threads must be caught (`catch_unwind`) and turned into a run failure, never allowed to unwind into FFI. On the main thread, pgrx's `#[pg_guard]` converts panics into `ERROR`s.
- A Postgres `ERROR` inside an SPI call `longjmp`s. pgrx catches it at the guard boundary, but any Rust state held across the call must be consistent (no half-updated in-memory maps). Use `PgTryBuilder` or subtransactions around per-run work so that one bad run cannot abort the whole batch.
- Memory: Tokio and Wasmtime allocate with the system allocator, outside Postgres memory contexts. `work_mem`/`maintenance_work_mem` therefore do not bound them, so pgflow needs its own budgets (§2.6, NFRs).

### 1.3 Shared memory (`shmem`)

- Fixed-size shared memory must be requested in `shmem_request_hook` (PG15+), which needs `shared_preload_libraries`. pgrx exposes `PgLwLock<T>` / `PgAtomic<T>` / `pg_shmem_init!`.
- Uses in pgflow: (a) a per-database **wake-up slot table** (latch pointers / PIDs of executor workers); (b) **load counters** (active instances, fuel consumed per second, bridge queue depth) for spillover decisions and `pgflow.stats()`; (c) a cross-worker **module cache index** (hash → on-disk path of the precompiled artifact). The compiled code itself lives on disk and is `mmap`ed. It is never stored in shmem.
- Shared memory cannot grow, so size everything with GUCs read at startup.

### 1.4 SPI (Server Programming Interface)

- SPI runs inside the worker's current transaction. A bgworker opens its own transactions (`StartTransactionCommand` / `CommitTransactionCommand`, wrapped by pgrx's `BackgroundWorker::transaction`).
- **Batch** claims and checkpoints: one transaction per loop iteration covers many runs, which amortises WAL flushes. Use subtransactions per run so one failure doesn't roll back the others.
- SPI results are copied into the SPI memory context. Copy them out into Rust-owned buffers before `SPI_finish`.
- Prepared plans (`SPI_prepare` + `SPI_keepplan`) for the hot claim and checkpoint statements.

### 1.5 `LISTEN/NOTIFY`

- **bgworkers do not receive NOTIFY the way a client does.** Notifications go out through the frontend protocol when a client backend goes idle, and a bgworker has no frontend. Do not build the in-DB wake-up path on NOTIFY.
- `NOTIFY` takes a global lock at commit, which serialises committing transactions that notify. Under high enqueue rates this becomes a well-known bottleneck.
- **Design implications:**
  - **In-DB wake-up**: `pgflow.start()` (C/Rust function from the extension) writes to the shmem wake slot and sets the executor's latch directly. No NOTIFY involved.
  - **External workers** may optionally `LISTEN pgflow_wakeup` for low latency, with debounced, coalesced notifications (one NOTIFY per transaction, issued only when the queue was empty). They must always fall back to polling with jitter.
  - The plain-SQL (extension-less) install uses NOTIFY for wake-ups and polling as the correctness backstop.

### 1.6 `SKIP LOCKED` queues

The canonical claim:

```sql
WITH c AS (
  SELECT id FROM pgflow.workflow_runs
  WHERE queue = $1 AND status IN ('pending','ready') AND visible_at <= now()
  ORDER BY priority DESC, visible_at
  FOR UPDATE SKIP LOCKED
  LIMIT $2
)
UPDATE pgflow.workflow_runs r
SET status = 'running', lease_owner = $3, lease_expires_at = now() + $4, attempt = attempt + 1
FROM c WHERE r.id = c.id
RETURNING r.*;
```

Known sharp edges and mitigations:

| Issue | Mitigation |
|---|---|
| Table/index **bloat** from constant `UPDATE`s on hot rows | Keep the hot "queue" columns in a narrow table separate from large payloads (`input`/`output` live in a side table or TOAST). Set `fillfactor≈70` to enable HOT updates. Tune autovacuum per table (`autovacuum_vacuum_scale_factor=0.01`). |
| Partial-index scans degrade when dead tuples pile up at the head of the index | Use partial indexes `WHERE status IN (...)`, a periodic `REINDEX CONCURRENTLY` job, and keep the queue table small by moving finished runs to an archive partition. |
| Leases expire while work is still going on (GC pause, slow I/O) | Fencing: every checkpoint write includes `WHERE lease_owner = $me AND attempt = $n`. A writer whose lease was stolen gets 0 rows and aborts. |
| Thundering herd on poll | Jittered backoff, per-queue wake slots, batch claims. |
| Long-running transactions block vacuum | Never hold a transaction open across Wasm execution or I/O. Claim → commit → execute → open a new transaction to checkpoint. |

### 1.7 Version & platform matrix

- Target **PG 15, 16, 17, 18**. PG15 is the floor because of `shmem_request_hook`, `MERGE`, and improved logical replication of partitioned tables. pgrx supports all four in current releases.
- PG18 ships native `uuidv7()`. For 15–17 the extension provides `pgflow.uuidv7()` (the plain-SQL install uses a PL/pgSQL implementation).
- **Managed Postgres reality check:** RDS, Aurora, Cloud SQL, AlloyDB, Azure Flexible Server, Neon and Supabase only allow allow-listed extensions. A brand-new pgrx extension that needs `shared_preload_libraries` will not be available on them for years, if ever (the Trusted Language Extensions route `pg_tle` does not allow bgworkers). This is why **D3** exists: the in-DB executor is an *accelerator*, not a requirement.

---

## 2. Polyglot Wasm Component Model & toolchains

### 2.1 Component Model primer (what matters to pgflow)

- A **component** is a Wasm binary with typed imports and exports described in **WIT**. The host (Wasmtime) links imports to Rust implementations through `wasmtime::component::bindgen!`, which generates type-safe host traits.
- **WASI 0.2 (`wasm32-wasip2`)** is the stable baseline: `wasi:clocks`, `wasi:random`, `wasi:io`, `wasi:cli`, `wasi:filesystem`, `wasi:sockets`, `wasi:http`.
- **WASI 0.3** adds native async to the component model (`future<T>`, `stream<T>`, async functions). Wasmtime has been implementing it behind configuration flags. pgflow's WIT is **designed so it can move to 0.3 async without changing the SDK surface** (D5).
- A component is **shared-nothing**: its linear memory and tables belong to it. Instances are cheap compared with processes, but each one carries its full heap (e.g. CPython's).

### 2.2 Rust guests

- Toolchain: `rustup target add wasm32-wasip2`, `wit-bindgen` (guest bindings via `wit_bindgen::generate!`), `cargo build --target wasm32-wasip2 --release`. `cargo-component` is an alternative workflow.
- Output size: typically **100 KB – 2 MB** after `opt-level="s"`, `lto=true`, `strip=true`, `wasm-opt -Oz`.
- Async: guest-side Rust `async fn` can be driven by a tiny single-threaded executor in the SDK, polling op handles through `effects.await` (see the WIT in 03 §2). There is no Tokio in the guest.
- Determinism hazards: `std::collections::HashMap` seeds `RandomState` from `wasi:random`. Host virtualisation (§2.5) makes this deterministic, and the SDK lints still recommend `BTreeMap` for clarity. `std::time::SystemTime::now()` goes through `wasi:clocks`, which is also virtualised.

### 2.3 Python guests (`componentize-py`)

- `componentize-py` bundles CPython compiled to WASI, the stdlib, and the user's pure-Python packages into a component. It **pre-initialises** the interpreter with a Wizer-style snapshot at build time: imports run once, and the heap is captured into the data segments.
- **Size:** expect tens of MB per component (CPython + stdlib). Mitigations: strip the stdlib to what is needed, share one base runtime across workflows *per module* (put many workflows in one component), and compress at rest in Postgres (`bytea` TOAST is pglz/lz4; storing pre-compressed zstd works better).
- **Cold start:** without pre-init, CPython startup takes hundreds of ms. With pre-init + Wasmtime AOT (`Engine::precompile_component` → `.cwasm` → `Component::deserialize_file`, mmap) + `InstancePre` + the pooling allocator, instantiation drops to a few milliseconds plus copy-on-write page faults. **This is mandatory for Python to be viable inside Postgres.**
- **Memory:** each live instance holds a CPython heap (often 10–50 MB resident). This cost decides how many concurrent Python runs one executor can hold, and is the main reason runs **evict and replay** instead of sitting resident (§2.5).
- **Native extensions:** C-extension wheels need WASI builds. Coverage is limited and improving. Document it as a limitation: "pure-Python dependencies only in v1; selected WASI wheels later".
- Async: the Python SDK ships a custom `asyncio` event loop whose "I/O" is pgflow op handles. `asyncio.gather()` maps to `effects.await(ops, all)`.

### 2.4 TypeScript/JavaScript guests

| Option | Engine | Output | Notes |
|---|---|---|---|
| **`componentize-js` (via `jco`)** | StarlingMonkey (SpiderMonkey compiled to WASI) | Component, roughly 8–12 MB | Bytecode Alliance-maintained, full ES20xx, pre-initialised (Wizer). **Recommended default.** |
| Javy | QuickJS | Core module (dynamic linking to a shared QuickJS "plugin") | Very small per-script output if the engine is shared, but it is not natively a component. Needs adaptation. |
| Porffor / AOT JS | AOT compilation | Experimental | Watch, don't depend on it. |

- TS is transpiled (esbuild/swc) and bundled into a single ES module before componentisation. `jco` generates guest typings from WIT.
- Hazards: `Date.now()`, `Math.random()`, `crypto.getRandomValues` → route them through virtualised WASI. Microtask ordering in a single-threaded engine is deterministic **as long as** every host completion is delivered in journaled order. The SDK has to own that delivery, and must not leave it to the engine's event loop with its own timers.

### 2.5 Determinism: replay vs snapshotting

| Criterion | **Event-sourcing replay** (Temporal, DBOS, Restate, Cloudflare Workflows) | **Linear-memory snapshot** (checkpoint the instance) |
|---|---|---|
| State size per suspension | Small: the journal of step results (KB) | Whole linear memory (MB → tens of MB for CPython/SpiderMonkey) plus globals and tables |
| Native stack / fibers | Not needed | Wasmtime async fibers live on the **native** stack, so you cannot snapshot mid-host-call without stack-switching support or an Asyncify-style transform. Asyncify bloats code and is not available for componentize-py/js output. |
| Host resources (handles) | Recreated on replay | Not capturable; dangling after restore |
| Module upgrades | Possible with versioning/patch markers | Impossible: the memory layout is bound to the exact binary |
| Portability (DB ↔ K8s, CPU features) | Full | Only if both hosts run identical engine versions |
| Cost | CPU to re-execute the prefix (bounded by `continue-as-new`) | Storage + I/O per suspension |
| Language-agnostic | Yes | Yes, but impractical for big runtimes |

**Decision (D1): replay.** Snapshotting is used only at **build time** (Wizer pre-init for cold start), never for durability.

**Determinism by construction (D2).** In Wasm, all ambient non-determinism has to come through imports. pgflow controls those imports:

| Source | Control |
|---|---|
| `wasi:clocks/wall-clock`, `monotonic-clock` | Return the **journaled** time: the logical "now" is recorded when each step starts and replayed exactly. |
| `wasi:random/random`, `insecure`, `insecure-seed` | A ChaCha20 stream seeded by `hash(run_id)`. Identical on replay. CPython's hash-seed randomisation and Rust's `RandomState` become deterministic as a side effect. |
| `wasi:filesystem` | No host paths. A read-only, virtual, in-memory FS only if the runtime needs one. |
| `wasi:sockets`, `wasi:http` (raw) | **Not linked.** All I/O goes through journaled pgflow effects. |
| Floating point | `Config::cranelift_nan_canonicalization(true)`, deterministic relaxed-SIMD (`relaxed_simd_deterministic(true)`) |
| Threads | Disabled (no `wasi-threads` / shared memory) |
| Fuel | Deterministic, so it doubles as a *replay divergence detector* (fuel used per step is recorded in debug mode) |

What remains for SDKs: **code changes between versions** (handled by module pinning plus patch markers, 03 §5.4) and **iteration order over data that comes from host results** (already deterministic, because the results are journaled).

### 2.6 Wasmtime runtime configuration (sandbox)

| Knob | Setting |
|---|---|
| Interruption | **Epoch interruption** (cheap) for wall-clock time slices and timeouts, ticked by one Tokio timer. **Fuel** for deterministic CPU budgets per step and per run. |
| Memory | `StoreLimits` / `ResourceLimiter`: max linear memory per instance, max tables and instances. Pooling allocator with fixed slot sizes gives predictable reservation and fast instantiation. |
| Virtual address space | Default 32-bit memories reserve about 4 GiB of virtual space plus guard regions *per memory*. Inside Postgres, with possible `ulimit -v`/cgroup accounting, tune `memory_reservation`/`memory_guard_size` or use bounds checks. |
| Traps | Signal-based traps install SIGSEGV/SIGBUS/SIGILL handlers. Postgres leaves these at their defaults, so Wasmtime's handlers chain fine, but **verify on every supported platform**. Offer `signals_based_traps(false)` (explicit bounds checks, about 10–20% slower) as a safe-mode GUC. |
| Compilation | Cranelift AOT at `deploy` time, so no JIT work happens on the hot path. Cache `.cwasm` keyed by `(component_sha256, wasmtime_version, target, cpu_features, config_hash)`. `Component::deserialize` is `unsafe`: only load artifacts pgflow produced itself, and keep a MAC on them. |
| Async | `async_support(true)` so host functions can be `async` (fiber-based), letting the executor multiplex many instances per thread. |

---

## 3. Async connector & I/O bridge

### 3.1 The problem

A workflow step that calls Stripe cannot share a transaction with Postgres. The classic failure windows:

1. Effect done, checkpoint not committed (crash) → on retry the **effect repeats** (double charge).
2. Checkpoint committed, effect never done → **lost effect**.

### 3.2 Pattern: transactional outbox + idempotency keys

```
guest: http.fetch(req)  ──►  host:
  TX1: INSERT outbox_requests(run_id, seq, req, idem_key, status='pending')
       + journal event 'effect_scheduled'(seq)          -- COMMIT
  Tokio: perform request with header Idempotency-Key: idem_key
  TX2: UPDATE outbox_requests SET status='done', response=...
       + journal event 'effect_completed'(seq, response) -- COMMIT
  resume guest with response
```

- **Crash before TX1**: nothing happened. Replay re-issues the call and gets the same `seq`.
- **Crash between TX1 and the request, or mid-request**: the outbox row is `pending`/`in_flight` with an expired lease. The recovery sweeper re-sends **with the same idempotency key**, so the provider deduplicates.
- **Crash after the request, before TX2**: same as above. Re-sent with the same key, and providers that support idempotency (Stripe, Adyen, most payment and email APIs) return the original response.
- **Idempotency key** = `base32(sha256(run_id ‖ seq ‖ effect_name))`. It is **stable across attempts** and deliberately excludes the attempt number. Users can override it (`idempotency_key=...`) when the business key matters (e.g. `order-123-capture`).
- For APIs **without** idempotency support, the effect is marked `at_least_once`. The SDK exposes a "check-then-act" helper (query before mutate) and warns in docs.

### 3.3 Transactional SQL steps (exactly-once)

The one effect that *can* be exactly-once is SQL against the same Postgres. The `sql-query` effect runs **in the same transaction** as its journal write: the result row and the `effect_completed` event commit atomically (DBOS's core trick). In DB mode that transaction runs through SPI; in K8s mode it runs on the worker's pooled connection. **Implication:** SQL steps should be short, and SQL effects must not be mixed with HTTP inside one step.

### 3.4 Secrets

- Guests **never** receive secret values (D7). Request templates reference secrets as `{{secret:stripe_live}}` in headers or bodies. The bridge resolves the reference **after** the request leaves the guest, right before sending, and **redacts** it in journaled requests.
- Providers (pluggable trait):
  - **Supabase Vault** (`vault.decrypted_secrets`) where it exists.
  - **pgsodium**: check deprecation status. Supabase has deprecated parts of it.
  - **Built-in**: an `pgflow.secrets` table encrypted with a key outside the DB (KMS/env/file), decrypted only in the bridge process.
  - **External**: K8s Secrets / Vault / AWS Secrets Manager for K8s workers.
- Access control: `pgflow.secret_grants(module_name, secret_name)`. The bridge refuses unknown pairs, and each resolution is audit-logged.
- Responses are journaled. Users may mark fields `redact` so the journal stores a hash, not the value (e.g. tokens returned by OAuth).

### 3.5 Connector architecture options

| Option | Pros | Cons | Verdict |
|---|---|---|---|
| **Primitive host connectors** in Rust/Tokio (`http`, `grpc`, `sql`, `kv`, `object-store`) | Fast, one implementation, secrets/idempotency/retry/rate-limit centralised, auditable egress | A new protocol needs a host release | **Core (v1)** |
| **Polyglot connector libraries** as Wasm components (e.g. `stripe.wasm` exporting `stripe:api/charges`) composed with the workflow through `wac`/`wasm-tools compose`, using only primitive host imports | Community-extensible, language-agnostic, versioned independently, still sandboxed | Composition tooling is young. Size overhead per language runtime (write connectors in Rust for size). | **Phase 3+**: connector SDK in Rust, components composed at build time |
| Raw `wasi:http` / `wasi:sockets` to guests | Zero work | Breaks determinism, idempotency and egress control | **Rejected** |

---

## 4. Competitive landscape & positioning

### 4.1 Comparison matrix

| | **pgflow** | **DBOS** | **Temporal** | **Hatchet** | **River** | **pg_cron** | **pgmq** | **Cloudflare Workflows** |
|---|---|---|---|---|---|---|---|---|
| Category | Durable execution engine | Durable execution library | Durable execution platform | Task queue + workflow engine | Job queue library | Scheduler | Message queue | Durable execution (serverless) |
| Infra beyond Postgres | **None** (DB mode) / stateless workers (K8s mode) | None (library inside your app; app process required) | Temporal cluster (frontend/history/matching/worker services) + DB + your workers | Hatchet engine (Go service) + Postgres (+ optional broker) + workers | None (library in a Go app) | None (extension) | None (extension) | Cloudflare platform (vendor-locked) |
| Where code runs | Inside Postgres bgworkers **or** K8s, chosen per queue | Your app process | Your worker fleet | Your workers | Your Go app | Postgres (SQL only) | n/a | Cloudflare edge |
| Languages | Rust, Python, TS (any component-producing language later) | Python, TS, Go, Java (per-language libs) | Go, Java, TS, Python, .NET, PHP, Ruby | Python, TS, Go | Go | SQL | SQL (+ client libs) | TS/JS, Python |
| Durability model | Journal + deterministic replay; **sandbox-enforced** determinism | Step checkpoints in Postgres; workflow fn re-executed with recorded step outputs (developer discipline) | Event history + deterministic replay (developer discipline, SDK sandbox in some langs) | Step/DAG checkpointing | At-least-once jobs, no replay | None | Visibility timeout | Step results persisted, replay |
| Transactional enqueue (with app data) | **Yes** (`SELECT pgflow.start(...)` inside your TX) | Yes | No (separate system) | No | **Yes** | n/a | Yes | No |
| Exactly-once DB steps | Yes (same-TX journal) | Yes | No | No | Yes (job + work in same TX) | n/a | n/a | No |
| Sandbox / multi-tenant isolation | **Wasm sandbox, fuel, memory caps** | None (app process) | None (your workers) | None | None | SQL perms | SQL perms | V8 isolates |
| Managed-PG friendly | Plain-SQL mode + external workers: yes. DB mode: no (self-hosted PG only) | Yes | n/a | Yes | Yes | Widely available | Some providers | n/a |
| Ops UI | Phase 4 (+ SQL views on day 1) | DBOS Conductor/console | Rich Web UI | Rich dashboard | River UI | none | none | Dashboard |

### 4.2 Differentiators (what only pgflow does)

1. **Zero-process day 1.** No app process even has to be up for workflows to make progress (cron, webhooks via `pg_net`-style ingestion, chained jobs). DBOS and River need *your* service running.
2. **One artefact, two execution planes.** The same `.wasm` hash runs inside Postgres or in K8s, switchable per queue at runtime. Temporal/Hatchet need a worker fleet from the start; DBOS can't move workloads out of the app process.
3. **Determinism enforced by the sandbox**, not by developer discipline or linters. Temporal and DBOS replay bugs ("non-deterministic error") are a top support issue for them.
4. **Polyglot under a single contract.** One WIT → three idiomatic SDKs with identical semantics, and one control plane for workflows written in different languages. Components can even call each other (child workflows across languages).
5. **Multi-tenant safe.** Fuel and memory caps let platform teams run *other teams'* workflows (or customer-supplied code) safely next to each other. No competitor in the Postgres-native space offers this.
6. **SQL as the control plane.** Start, inspect, signal, cancel, retry and schedule are all SQL functions, so they are composable with triggers, RLS, `pg_cron`, and any language's Postgres driver.

### 4.3 Sharp edges (be honest in docs)

| Sharp edge | Mitigation / messaging |
|---|---|
| DB mode needs `shared_preload_libraries` + restart, so it is **not available on most managed Postgres** | Plain-SQL install + `pgflow-worker` sidecar/container is the "Day-1 on RDS" story. DB mode targets self-hosted, Docker, Kubernetes operators (CloudNativePG, Crunchy) and dedicated hosts. |
| Running user code inside the DB process is something DBAs instinctively distrust | Isolation tiers (D8), hard resource caps, a per-database kill switch (`pgflow.enabled = off` on SIGHUP), and a clear capacity guide ("in-DB mode is for ≤ N% of cores"). |
| Python/JS component sizes and per-instance memory | Pre-init + AOT + pooling + eviction-and-replay. Publish a benchmark table. Recommend Rust or TS for high-fan-out workloads. |
| Replay cost for long histories | `continue_as_new`, history size limits with warnings, and step result size limits (payloads > 256 KB offloaded to an object store or large table). |
| Ecosystem maturity of Component Model async and componentize tooling | Pin toolchain versions in `pgflow-cli`. The WIT design works on WASI 0.2 today (D5). |
| Throughput ceiling of a single Postgres primary | Honest numbers. The K8s plane offloads CPU, but the journal still writes to the primary. Batch writes, `synchronous_commit = off` opt-in per queue for low-value jobs, and archive partitions. |
| Not a general compute platform | Long CPU-heavy work (ML inference, video) belongs in K8s queues or external services called through connectors. |

### 4.4 Positioning statement

> **For** teams that already run on Postgres and need durable workflows, **pgflow** is a durable execution engine that **lives inside the database you already operate**. Unlike Temporal, it needs no new cluster. Unlike DBOS or River, it is not tied to one language or to your app process. Unlike everything else, the sandbox guarantees determinism and isolation, and the same workflow binary can move to Kubernetes when you outgrow the database.
