# Ostro

## What This Is

Ostro is a single Rust binary that **stores, schedules and executes durable workflows** written in Rust, Python or TypeScript and compiled to Wasm components against one WIT contract. Developers `ostro deploy` a component and trigger runs via HTTP, webhook, cron or CLI. There are no worker fleets, no app processes to keep alive, and no external database. The same binary runs as `dev`, single-node `server`, stateless `worker`, or a member of a 3/5-node replicated cluster. It targets solo developers, lean startups, platform teams and SaaS builders who find existing durable-execution stacks (Temporal, Restate, Hatchet, Windmill, DBOS) too much to run.

Full design (source of truth): `docs/README.md`, `docs/01-research.md`, `docs/02-prd.md`, `docs/03-architecture.md`, `docs/04-chronik-reuse.md`, `docs/05-roadmap.md`.

## Core Value

**Download one binary, deploy a Wasm workflow, and it runs durably.** Accepted runs are never lost, survive crashes, and replay deterministically, with nothing else to install or operate.

## Requirements

### Validated

(None yet — ship to validate)

### Active

- [ ] Single static binary with `dev`, `server`, `worker`, cluster and client subcommands, zero-config defaults (PRD FR-0)
- [ ] Log-first embedded storage: partitioned journal on `chronik-wal` with group commit + per-partition state machine on ~~redb~~ fjall (Phase 1 S3 decision); snapshot + log-tail recovery (ARCH §2–3)
- [ ] Metadata state machine on `chronik-raft` (single member on day 1, 3/5 in cluster) (ARCH §4.1)
- [ ] Wasmtime component host with virtualised WASI (deterministic clocks/random, no FS/sockets), replay with divergence detection, fuel/epoch/memory limits, AOT cache + InstancePre + pooling (ARCH §5)
- [ ] `ostro:runtime@0.1.0` WIT contract: context, journal (steps), effects (http, sql, object store, sleep, signals, children, wait), log, workflows export (ARCH §6)
- [ ] Per-partition dispatcher with ready queues, timer wheel, soft leases, attempt fencing; hot-wait vs evict activation model
- [ ] HTTP/JSON + gRPC control-plane API: modules, workflows, runs, history, signals, cancel/retry, webhooks, cron, queues, scoped tokens (FR-3)
- [ ] I/O bridge: HTTP effect with outbox semantics and stable idempotency keys; SQL connector with transactional idempotency token (exactly-once); object store; secrets that never enter guests; egress allow-lists + SSRF guard (FR-4)
- [ ] Stateless workers over a gRPC Execute stream; queue placement `embedded`/`workers`/`auto` with spillover and hysteresis; KEDA demand metric (FR-5)
- [ ] Bottomless single node: sealed segments + snapshots to S3-compatible storage; `ostro backup/restore` (FR-6.2)
- [ ] Rust, Python (componentize-py) and TypeScript (componentize-js/jco) SDKs with identical semantics, verified by a language-neutral conformance suite (FR-1)
- [ ] `ostro init/build/deploy/run/replay/dev --watch` CLI with hermetic toolchains (FR-2)
- [ ] Embedded web UI: runs, history timeline, DLQ, modules, queues, cluster health
- [ ] Replicated cluster: follower-pull ISR with leader epochs and truncation (reused from Chronik), elect only from ISR, hot-standby followers; replication conformance suite written first (ARCH §4.2, 04 §4)
- [ ] Observability: OpenTelemetry traces, Prometheus metrics, bounded guest logs (NFR-4)
- [ ] Security: TLS/mTLS, scoped tokens, tenants + quotas, signed modules optional, audit log (NFR-6)

### Out of Scope

- Running inside Postgres / Postgres as the required store: superseded pgflow design (managed PG can't load it; crash blast radius). See `docs/archive/pgflow/`
- General-purpose FaaS / HTTP serving: not a durable-execution concern
- Visual workflow designer: code-first product
- Multi-region active-active: complexity far beyond v1 needs
- Native (non-Wasm) code execution in user processes: post-1.0 escape hatch for native dependencies
- Live partition splitting / re-sharding: fixed partition count in 1.0
- Raw `wasi:http` / `wasi:sockets` for guests: breaks determinism, egress control and idempotency

## Context

- **Prior work:** the author's Chronik Stream (github.com/lspecian/chronik-stream, Apache-2.0, Rust, v2.18) supplies reusable crates: `chronik-raft` (RaftDriver + `StateMachine` trait + durable RaftLogStore, tikv raft-rs 0.7 vendored; nearly standalone), `chronik-wal` (segments, GroupCommitWal, truncation, sparse index; Kafka-shaped `WalRecord`), replication inside `chronik-server` (follower-pull over Kafka Fetch, leader epochs, ISR, failover; ~8.6k lines, needs extraction), and object-store backends inside `chronik-storage` (needs extraction).
- **Chronik lessons (docs/04 §4):** replication silently disabled for user data for ~9 months; "ISR is fiction"; the Raft data path failed on batching. Rules: one replication path including RF=1, ISR from measured LEOs only, conformance suite first, benchmarks verify acknowledged bytes on disk.
- **Competition:** Restate is closest (Rust single binary, embedded storage) but runs code in the user's services. Ostro's moat is that the server hosts the code (upload Wasm) while still being a self-hosted single binary.
- **Ecosystem uncertainty:** Wasmtime Component Model async / WASI 0.3 maturity; componentize-py/js sizes and features; Python per-instance memory (10–50 MB).
- **Repository:** greenfield, at `~/Development/ostro` (renamed from `pgflow` on 2026-11-18).

## Constraints

- **Tech stack**: Rust (stable), Tokio, Wasmtime (Component Model), fjall (partition state store; replaced redb after spike S3), chronik-wal, chronik-raft, tonic/axum: as decided in docs/03
- **Packaging**: Single static binary (no C++ deps such as RocksDB) for linux x86_64/aarch64 + macOS. The single-binary moat (D1) depends on it
- **Dependencies**: Needed Chronik code (chronik-raft driver/log store, chronik-wal pieces, replication algorithms) is **vendored into the Ostro workspace** (Apache-2.0, with attribution and source commit recorded) and modernised there. The Chronik repo is not modified
- **Correctness**: zero lost accepted runs under crash injection; single node uses the replication code path with RF=1 (D5)
- **Determinism**: all ambient non-determinism virtualised by the host; replay divergence is detected, never silently tolerated
- **Security**: secrets never enter guest memory; guests have no ambient authority
- **Licence**: Apache-2.0

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Name: Ostro | Short, free on crates.io/PyPI/npm, brandable | — Pending |
| Separate product on shared Chronik crates (not a Chronik capability) | Clean positioning, small binary, independent cadence | — Pending |
| Single binary, server hosts the code (Wasm upload) | The moat vs Restate/Temporal/Inngest | — Pending |
| Log-first storage: chronik-wal journal + redb state (state engine superseded: fjall, see "State store: redb vs fjall" below) | No external DB, single writer per partition, no queue contention | — Pending |
| Raft for metadata, follower-pull ISR for partition logs (Chronik model) | Proven in Chronik; avoids multi-raft batching pitfalls | — Pending |
| Deterministic replay, not memory snapshots | Portable, small, works for every language | — Pending |
| Single-node product first (Phases 1–3), cluster last (Phase 4) | Usable product early; RF=1 path keeps cluster additive | — Pending |
| ~~Reuse Chronik's Kafka-Fetch replication (Option A)~~ → port Chronik's pure replication algorithms behind an Ostro `ReplicatedLog` trait (exists from Phase 1 at RF=1) | Research: Option A drags in ~10k lines of Kafka handlers and can't run at RF=1 | — Pending |
| Vendor Chronik code into Ostro (no upstream changes, no git deps) | Chronik pins older tonic/prost/axum/OTel; raft patch ignored as a git dep; avoids touching Chronik | — Pending |
| Step handle: resource vs u64 → **resource** (`resource step` in WIT 0.1; no `begin-step-u64`) | S2: `resource step` round-trips in Python (componentize-py), TS (jco) and Rust (wit-bindgen) at no measurable cost (Rust steady `run` incl. both round trips 0.9 µs). It gives typed, unforgeable, ownership-checked handles; u64 needs extra host-side validation for no ergonomic gain. `HostStep::drop` fires for an uncompleted step in all three only with SDK RAII discipline (Python `with`, TS `using`/`Symbol.dispose`); TS never drops completed steps, so the engine must not rely on drop for correctness (drop is an observability signal only) | Decided (Phase 1, S2) — see docs/spikes/S2-guest-toolchains.md |
| State store: redb vs fjall → **fjall 3.1** (`=3.1.13`) for STOR-05. Durability design: one atomic `WriteBatch` per apply batch with `applied_offset` in the batch, default buffered journal flush per commit, `persist(SyncAll)` checkpoint every 100 ms or 256 batches recording `checkpointed_offset`; recovery replays the WAL from the state's own `applied_offset` | Locked rule (≥ 90k cmds/s ≈ 30k steps/s in the planned durability mode). Quiet re-run (01-08, load 0.2–1.3): redb 4.6 T2 67.5k cmds/s insert-only and 30.9k with read-modify-write, so FAIL (CPU-bound: even no-fsync T3 is ~90k; Immediate checkpoints cost 53–80 ms). fjall T2 233.8k insert-only and **106.9k RMW** with 1M preloaded runs, so PASS (3.45x redb). fjall kill -9 crash test 5/5: reopens to a batch-consistent prefix at `applied_offset ≥ last fsynced checkpoint`, with rows matching `applied_offset` exactly (no torn batch). Costs carried into Phase 2: journal-replay reopen of ~3.3 s for ~540k un-flushed commands (bound journal/memtable size), one ~120 ms write stall per RMW run, a smaller RMW margin (1.19x), and background flush/compaction threads | Decided (Phase 1, S3) — see docs/spikes/S3-redb-durability.md |
| Wasmtime pin policy → **track the latest Wasmtime major monthly until 1.0, pinned exactly** (`=50.0.2` today), one bump PR per release with the replay corpus green; ship 1.0 on the 60 LTS (~Aug 2027); fallback: freeze on 48 LTS (supported to ~Aug 2028) if monthly bumps cost more than ~1 day each; bump `rust-toolchain.toml` together with Wasmtime's MSRV | Wasmtime releases a major on the 20th of each month (51 due ~2026-12-20); every 12th major is an LTS supported 24 months, other releases 2 months, with security fixes backported to all supported releases. Tracking keeps Component Model/async fixes flowing while WIT 0.1 is pre-freeze, and landing 1.0 on an LTS gives users a 2-year supported runtime. S2 found nothing on 50.0.2 that contradicts this | Decided (Phase 1) — see .planning/phases/01-foundation-vendoring-spikes/01-RESEARCH.md#decisions-to-log-in-projectmd-found-06 |

---
*Last updated: 2026-11-18 after plan 01-08 (Phase 1 decisions: step handle, state store, Wasmtime pin policy)*
