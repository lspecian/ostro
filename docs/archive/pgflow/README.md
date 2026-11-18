# pgflow — Design Documentation

> **Postgres-native, polyglot Wasm durable execution engine, written in Rust.**
> Local-first (runs inside Postgres), offload-ready (same Wasm runs on Kubernetes).

| Doc | Contents |
|---|---|
| [01-research.md](01-research.md) | Technical research: Postgres/pgrx internals, Wasm Component Model toolchains, determinism and replay, the I/O bridge, competitive landscape |
| [02-prd.md](02-prd.md) | Product Requirements Document: personas, functional and non-functional requirements |
| [03-architecture.md](03-architecture.md) | System architecture: topology, WIT contract, schema, suspend/resume lifecycle, security |
| [04-roadmap.md](04-roadmap.md) | Four-phase implementation roadmap with deliverables, risks and exit criteria |

---

## The pitch in one paragraph

Teams want durable workflows (retries, sleeps, sagas, human-in-the-loop) without having to run Temporal. Postgres-backed libraries like DBOS and River remove the extra cluster, but they tie workflows to one language runtime and still need an app process to run them. `pgflow` compiles workflows written in **Rust, Python or TypeScript** to **Wasm Components** that share one WIT contract. It stores them **in Postgres** and runs them in **Postgres background workers** on day 1. On day 100, the **same binary** runs in a stateless worker fleet on Kubernetes, autoscaled by KEDA from queue depth, with no code changes.

## Key architectural decisions (summary)

| # | Decision | Rationale | Where |
|---|---|---|---|
| D1 | **Deterministic replay over an event journal**, not linear-memory snapshots | Portable across hosts and module patches, small state, works for every language; snapshots are huge for CPython and cannot capture host resources | [01 §2.5](01-research.md#25-determinism-replay-vs-snapshotting), [03 §5](03-architecture.md#5-the-suspendresume-lifecycle) |
| D2 | **Determinism by construction**: the host virtualises every `wasi:clocks` / `wasi:random` import and canonicalises NaNs | The guest's built-in `time.time()`, `Math.random()` and `HashMap` seeds become deterministic without SDK discipline | [01 §2.5](01-research.md#25-determinism-replay-vs-snapshotting) |
| D3 | **The extension is optional.** The schema and SQL API are plain SQL/PL/pgSQL; the `pgrx` extension only adds the in-DB executor | Most managed Postgres (RDS, Cloud SQL, Supabase, Neon) will not load arbitrary `shared_preload_libraries`. Without this, Day 1 is impossible for most users | [02 FR-6](02-prd.md#fr-6-deployment-modes), [03 §1](03-architecture.md#1-system-topology) |
| D4 | **Postgres is touched only from the bgworker main thread**; Tokio and Wasmtime run on their own threads and reach Postgres through a command channel | Postgres is not thread-safe; calling SPI or `palloc` from another thread corrupts the backend | [01 §1.2](01-research.md#12-async-runtimes-inside-a-bgworker) |
| D5 | **Effects are non-blocking ops plus a deterministic `await`** in WIT, mapped to native `future<T>` when WASI 0.3 async is stable | Lets every SDK express `gather`/`Promise.all` concurrency while completion order stays journaled and replayable | [03 §2](03-architecture.md#2-unified-wit-interface-specification) |
| D6 | **Spillover is pull-based work stealing**, not push routing | No coordinator; local workers back off under pressure, K8s workers claim what is left; KEDA scales from the same SQL metric | [03 §6](03-architecture.md#6-hybrid-routing--spillover) |
| D7 | **Secrets never enter guest memory.** Guests reference secrets by name; the host substitutes them at the edge of the I/O bridge | A malicious or buggy module cannot exfiltrate what it never sees | [03 §7](03-architecture.md#7-security--multi-tenancy) |
| D8 | **Two isolation tiers in DB mode**: in-process (lowest latency) and supervised child process (a crash cannot trigger a postmaster crash-restart) | A segfault in a bgworker forces Postgres to restart *every* connection | [03 §7.1](03-architecture.md#71-protecting-the-postgres-process) |

## ⚠️ Name collision

There is already an open-source project called **pgflow** (pgflow.dev): a Postgres-native workflow engine for Supabase, with a TypeScript DSL on top of pgmq. That makes the name ambiguous in the same niche. Pick a distinct name before public release, especially for crates.io, PyPI, npm and the Postgres extension name. This doc set keeps `pgflow` as the codename.

## Assumptions to re-verify before Phase 1 kick-off

These facts move quickly. They were true as of the knowledge behind this doc set, but check them against current upstream:

1. How stable Wasmtime's Component Model async support is, and the state of WASI 0.3 (`future`/`stream`). This decides whether D5 ships natively or polyfilled.
2. `componentize-py` / `componentize-js` output sizes, supported WIT features, and native-extension (e.g. numpy) support.
3. The pgrx versions that support PG 15–18, plus its current bgworker and thread-check APIs.
4. Whether Supabase still supports `pgsodium`. It has deprecated parts of it, but Supabase Vault remains. The secrets provider must be pluggable either way.
5. KEDA `postgresql` scaler configuration semantics.
