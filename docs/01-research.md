# 01 — Technical Research

- [1. What makes infrastructure "simple to start"](#1-what-makes-infrastructure-simple-to-start)
- [2. Embedded storage options](#2-embedded-storage-options)
- [3. Wasm Component Model & polyglot toolchains](#3-wasm-component-model--polyglot-toolchains)
- [4. Determinism: replay vs snapshotting](#4-determinism-replay-vs-snapshotting)
- [5. Async I/O bridge: side effects, secrets, idempotency](#5-async-io-bridge-side-effects-secrets-idempotency)
- [6. Competitive landscape & positioning](#6-competitive-landscape--positioning)

---

## 1. What makes infrastructure "simple to start"

Tools developers describe as painless share a recognisable shape:

| Tool | Day 1 | Day 100 | Pattern |
|---|---|---|---|
| **NATS** | `nats-server` — one Go binary, no config | Same binary, `--cluster`, JetStream with embedded storage | Same binary, flags change role |
| **Caddy** | `caddy run`, automatic TLS | Same binary behind a load balancer | Batteries included, sane defaults |
| **k3s** | `curl | sh`, a cluster in 30 s | `k3s agent --server …` adds nodes | Server/agent modes of one binary, embedded datastore (SQLite → etcd) |
| **SQLite / Litestream** | A file | Stream WAL to S3 for DR | Durability without a cluster |
| **Consul / Nomad** | `-dev` mode | `-server` / `-client` modes | Dev mode is the full product, in memory |

**Design rules extracted:**
1. **One artefact.** Server, worker, CLI and UI ship in the same binary. No version-matching between components.
2. **Zero required configuration.** Defaults that work on a laptop; config file only to change them.
3. **A dev mode that is the real thing**, not a mock (contrast: Temporal's dev server is a different deployment shape from production).
4. **Embedded storage first**, external storage optional.
5. **Grow by adding the same binary**, not new kinds of components.
6. **One client port** (multiplex HTTP/1.1, HTTP/2, gRPC on one listener); internal cluster traffic on one more.
7. **A credible durability story for one node** (backups to object storage) so "single node" is a choice, not a liability.

---

## 2. Embedded storage options

Ostro's storage is **log-first**: an append-only journal per partition is the source of truth, and a materialized view (runs, timers, inbox, outbox) is derived from it. The question is what to use for each half.

### 2.1 Journal (the log)

| Option | Notes | Verdict |
|---|---|---|
| **Chronik WAL** (`chronik-wal`) | Segmented, CRC'd records, PostgreSQL-style **group commit** (one fsync per batch), torn-tail truncation on open, suffix truncation for divergence, sparse offset index, optional io_uring thread. Already tuned and battle-tested on Kafka workloads | **Use** (see [04](04-chronik-reuse.md)) |
| Custom minimal log | Full control, small | Reinvents what Chronik already debugged |
| RocksDB WAL | Not exposed as a replicated log primitive | No |

### 2.2 Materialized state (per-partition state machine)

| Option | Type | Pros | Cons | Verdict |
|---|---|---|---|---|
| **redb** | Pure-Rust copy-on-write B-tree, ACID, MVCC readers | No C/C++ toolchain → trivially static, cross-compiled binary; simple API; crash-safe | Write throughput below an LSM for huge write rates | **Default** — state writes are batched per applied log chunk, so throughput is fine |
| fjall | Pure-Rust LSM | Higher write throughput, compression | Younger | Keep as an alternative behind the `StateStore` trait |
| RocksDB | C++ LSM | Proven at scale (Restate, TiKV use it) | C++ build, larger binary, tuning surface | No (hurts D1) |
| SQLite | B-tree, SQL | Inspectable, familiar | Second WAL on top of our journal; awkward as a derived store | Use only for the *dev-mode inspection export* (`ostro export --sqlite`) |

**Recovery model:** state store holds `applied_offset`. On start: open state store → replay journal from `applied_offset` to the log end. Periodic **state snapshots** (redb file copy at a consistent point) allow journal prefix truncation and fast replica bootstrap; snapshots and sealed journal segments go to object storage when configured (D9).

### 2.3 Why log-first beats "a database with a queue table"

| Concern | Queue table in a DB (pgflow design) | Partitioned log + single-writer state machine |
|---|---|---|
| Claiming work | `SELECT … FOR UPDATE SKIP LOCKED`, contention, polling | Partition leader owns an in-memory ready queue + timer wheel; dispatch is a function call |
| Timers | Indexed `visible_at` scan | Hierarchical timer wheel rebuilt from state on failover |
| Bloat / vacuum | Constant UPDATE churn on hot rows | Append-only; compaction of sealed segments |
| Write amplification | Row updates + index updates + WAL | One log append (+ batched state apply) |
| Replication | The DB's (or none) | Log replication — exactly what Chronik does |

---

## 3. Wasm Component Model & polyglot toolchains

*(Carried over from the pgflow research; the conclusions are unchanged.)*

### 3.1 Component Model essentials

- A **component** is a Wasm binary with typed imports/exports described in **WIT**. The host links imports to Rust implementations via `wasmtime::component::bindgen!`.
- **WASI 0.2 (`wasm32-wasip2`)** is the stable baseline. **WASI 0.3** adds native async (`future<T>`, `stream<T>`); Ostro's WIT is designed to migrate without SDK surface changes ([03 §6](03-architecture.md#6-unified-wit-interface-specification)).
- Components are **shared-nothing**: each instance owns its linear memory — the cost per live run equals the language runtime's heap.

### 3.2 Per-language toolchains

| | Rust | Python | TypeScript |
|---|---|---|---|
| Toolchain | `wasm32-wasip2` target + `wit-bindgen` (or `cargo-component`) | `componentize-py` (CPython compiled to WASI + stdlib + pure-Python deps) | esbuild bundle → `componentize-js` via `jco` (StarlingMonkey / SpiderMonkey) |
| Typical size | 0.1 – 2 MB | tens of MB | ~8 – 12 MB |
| Cold start (with AOT + pre-init + pooling) | < 1 ms | a few ms (pre-initialised interpreter heap) | ~1–5 ms |
| Resident memory per instance | KBs–MBs | 10–50 MB | several MB |
| Limitations | — | C-extension wheels need WASI builds; v1 = pure-Python deps | Node built-ins limited to what the SDK provides |
| Async mapping | Tiny single-threaded executor polling op handles | Custom `asyncio` event loop | Custom job loop resolving promises in journaled order |

**Mandatory cold-start techniques:** build-time pre-initialisation (Wizer-style snapshot of the interpreter heap — componentize-py/js already do this), Wasmtime **AOT** (`Engine::precompile_component` at deploy time → `.cwasm` → `mmap` with `Component::deserialize_file`), `InstancePre` (pre-linked imports), and the **pooling allocator** (copy-on-write memory images).

### 3.3 Wasmtime sandbox configuration

| Knob | Setting |
|---|---|
| CPU | **Fuel** (deterministic budgets per step/run) + **epoch interruption** (cheap wall-clock slicing and timeouts) |
| Memory | `StoreLimits` / `ResourceLimiter` per instance; pooling allocator slots; global resident-memory budget with eviction |
| Async | `async_support(true)` — host functions are async; guests run on fibers, multiplexed on a small thread pool |
| Floating point / SIMD | `cranelift_nan_canonicalization(true)`, deterministic relaxed-SIMD |
| Threads | Disabled |
| AOT artefacts | Cache keyed by `(component sha256, wasmtime version, target, CPU features, config hash)`; `deserialize` is `unsafe`, so only load self-produced artefacts, HMAC-protected |

Running in **our own process** (instead of inside Postgres) removes the worst risk of the pgflow design: a native crash restarts one Ostro node (and replicas take over), not every connection to a shared database.

---

## 4. Determinism: replay vs snapshotting

| Criterion | **Event-sourcing replay** | **Linear-memory snapshot** |
|---|---|---|
| State per suspension | Journal of results (KB) | Whole linear memory (MB → tens of MB for CPython) |
| Mid-call suspension | Not needed | Fibers live on the native stack; can't be serialised without Asyncify-style transforms (unavailable for componentize-py/js) |
| Host resources | Recreated on replay | Not capturable |
| Code upgrades | Pinning + patch markers | Impossible (memory layout bound to binary) |
| Portability between nodes/workers | Full | Only identical engine/CPU |

**Decision: replay.** Snapshots are used only at build time (pre-init).

**Determinism by construction** — all ambient non-determinism enters through imports that Ostro controls:

| Source | Control |
|---|---|
| `wasi:clocks/*` | Journaled logical time |
| `wasi:random/*` | ChaCha20 seeded from the run id (also makes CPython hash seeds and Rust `RandomState` deterministic) |
| `wasi:filesystem` | None, or read-only in-memory FS |
| `wasi:sockets`, `wasi:http` | Not linked; all I/O is journaled Ostro effects |
| Float/SIMD/threads | Canonicalised / deterministic / disabled |
| Concurrency | `wait()` completion order is journaled and replayed |

Remaining developer responsibility: code changes between versions (runs are pinned to a module hash; `ctx.patched()` markers; `ostro replay` CI gate).

---

## 5. Async I/O bridge: side effects, secrets, idempotency

### 5.1 Outbox + idempotency keys (unchanged in principle)

```
guest: http.fetch(req)
  1. leader appends EffectScheduled{run, op, req(redacted), idem_key}   ── committed (HW)
  2. bridge performs request with Idempotency-Key: idem_key
  3. leader appends EffectCompleted{run, op, response}                  ── committed
  4. guest resumes with response
crash between 1 and 3 → new leader / executor re-sends with the SAME key
```

`idem_key = base32(sha256(run_id ‖ op_id ‖ effect_name))` — stable across attempts; user-overridable with a business key.

### 5.2 What changes without a host database: SQL effects

In pgflow, SQL against the home Postgres could commit atomically with the journal (exactly-once). Ostro's journal lives in Ostro, so **SQL against the user's database is an external effect like HTTP**. Mitigations:

1. **Transactional idempotency token** (SDK helper): the SQL connector wraps the user's statement in a transaction that also inserts the effect's key into an `ostro_effects(key text primary key, result jsonb)` table in *the user's* database. A retry hits the primary key, the connector reads the stored result instead of re-executing. → exactly-once for SQL effects against databases the user controls.
2. Connection definitions (DSN + secret refs) are registered in Ostro; guests reference connections by name.

### 5.3 Secrets

- Guests reference `{{secret:name}}`; values are resolved inside the bridge just before sending, zeroized after, never journaled.
- Built-in encrypted secret store (sealed with a key from env/file/KMS), replicated as metadata; per-module grants; audit log.
- Pluggable providers: HashiCorp Vault, AWS/GCP secret managers, Kubernetes Secrets.

### 5.4 Connector tiers

| Tier | Examples | Where |
|---|---|---|
| Primitive host connectors (Rust/Tokio) | HTTP, SQL (Postgres/MySQL), object store (S3), gRPC (later), Kafka produce (Chronik/Kafka) | Built into the binary |
| Polyglot connector components | `stripe`, `slack`, `openai` typed APIs composed at build time (`wac`), importing only primitive connectors | Phase 3+ |
| Raw `wasi:http`/sockets | — | Rejected (breaks determinism, egress control, idempotency) |

---

## 6. Competitive landscape & positioning

### 6.1 Matrix

| | **Ostro** | **Restate** | **Temporal** | **Inngest** | **Windmill** | **Hatchet** | **DBOS** | **Cloudflare Workflows** |
|---|---|---|---|---|---|---|---|---|
| Server footprint | **1 binary, embedded storage** | 1 Rust binary, embedded storage (RocksDB + replicated log) | Multi-service + DB (single-binary dev server only) | 1 binary self-host (embedded or Redis/Postgres) | Server + **Postgres** | Server + **Postgres** | None (library) + **Postgres** | Hosted |
| Where your code runs | **Inside the server (Wasm)**, or Ostro workers | **Your** services (HTTP/Lambda endpoints) | **Your** worker fleet | **Your** app (HTTP) | Server-managed workers run uploaded scripts | Your workers | Your app process | Cloudflare (hosted) |
| What you must keep running | Ostro | Restate + your services | Temporal + DB + workers | Inngest + your app | Windmill + PG + workers | Hatchet + PG + workers | Your app + PG | Nothing (vendor) |
| Durability model | Journal + replay, **sandbox-enforced determinism** | Journal + replay | History + replay (discipline) | Step memoisation | Script/flow state in PG | Step checkpointing | Step checkpoints in PG | Step results + replay |
| Languages | Rust, Python, TS (any component language later) | TS, Java/Kotlin, Python, Go, Rust | Go, Java, TS, Python, .NET, PHP, Ruby | TS, Python, Go | Many script languages | Python, TS, Go | Python, TS, Go, Java | TS/JS, Python |
| Isolation / multi-tenant code | **Wasm: fuel, memory caps, no ambient authority** | Process (yours) | Process (yours) | Process (yours) | Process/nsjail | Process | Process | V8 isolates |
| HA | 3/5-node Raft + ISR (Phase 4) | Clustered | Yes | Depends on backing stores | Via PG | Via PG | Via PG | Vendor |
| Licence | Apache-2.0 (proposed) | Server source-available (verify) | MIT | Server source-available (verify) | AGPL / commercial | MIT | MIT | Proprietary |

### 6.2 The gap Ostro fills

**Self-hosted, single binary, *and* it hosts the code.** Restate and Inngest nailed the single binary but still execute code in your processes — you still deploy, scale and keep alive an app. Cloudflare hosts the code but only on Cloudflare. Windmill hosts code but needs Postgres, is script-centric rather than replay-based durable execution, and is AGPL.

### 6.3 Differentiators

1. **No worker fleet, no app process** — `ostro deploy` and it runs.
2. **One binary for everything**, including the CLI and UI; grows by adding copies of itself.
3. **Determinism enforced by the sandbox** — no "non-deterministic workflow" incidents from a stray `Date.now()`.
4. **Safe for untrusted code** — platform teams and SaaS builders can run other teams' or customers' workflows.
5. **Bottomless single node** — S3 backups before you ever need a cluster.
6. **Apache-2.0** server, if confirmed — a differentiator against source-available competitors.

### 6.4 Sharp edges (be honest)

| Sharp edge | Mitigation |
|---|---|
| Wasm-only code can't use native deps (numpy, native drivers) | Connectors for common needs; **native worker protocol** post-1.0 (SDK in a normal process, same journal semantics) as an escape hatch |
| Python components are large and memory-hungry | AOT + pre-init + eviction/replay; recommend Rust/TS for high fan-out |
| Single node is a SPOF until Phase 4 | Bottomless mode (fast restore), honest docs, Phase 4 HA |
| Another stateful system to back up | One data directory + built-in S3 streaming; `ostro backup` / `ostro restore` |
| Crowded, well-funded market | Compete on time-to-first-workflow and "nothing else to run"; don't chase feature parity |
| Distributed log correctness is genuinely hard | Reuse Chronik's tested core + its replication conformance suite approach ([04](04-chronik-reuse.md)) |
