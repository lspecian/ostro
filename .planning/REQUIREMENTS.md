# Requirements: Ostro

**Defined:** 2026-11-18
**Core Value:** Download one binary, deploy a Wasm workflow, and it runs durably. Accepted runs are never lost, survive crashes, and replay deterministically, with nothing else to install or operate.

Sources: `docs/02-prd.md` (FR/NFR IDs in brackets) amended by `.planning/research/SUMMARY.md`. **v1 = everything required for Ostro 1.0.**

## v1 Requirements

### Foundation & Toolchain

- [x] **FOUND-01**: Cargo workspace builds on pinned Rust 1.100 with crate layout per research (ostro-types, ostro-log, ostro-partition, ostro-meta, ostro-engine, ostro-bridge, ostro-proto, ostro-api, ostro-blob, ostro, ostro-cli)
- [x] **FOUND-02**: Needed Chronik code (raft driver + log store + vendored raft/raft-proto, WAL group commit/segment/index/truncate) is vendored with Apache-2.0 attribution, source commit and a `VENDORED.md` listing local modifications; no dependency on the Chronik repo
- [x] **FOUND-03**: Vendored code is modernised: no unused chronik-common/monitoring deps, `metrics` facade instead of MetricsRecorder, no tokio-uring, raft crates renamed so no `[patch]` is required
- [x] **FOUND-04**: cargo-deny bans duplicate tonic/prost/hyper/axum/opentelemetry versions and disallowed licences in CI
- [ ] **FOUND-05**: CI matrix builds and tests on linux x86_64 + aarch64 (musl via cargo-zigbuild, mimalloc) and macOS
- [ ] **FOUND-06**: Spike results recorded: guest toolchain size/memory/cold-start + pre-init randomness behaviour (S2), redb durability-tier throughput (S3); decisions (resource vs u64 step handle, redb vs fjall, Wasmtime pin policy) logged in PROJECT.md

### Storage & Log

- [ ] **STOR-01**: Each partition has an append-only journal on the vendored WAL with group commit; every batch carries the envelope `{epoch, codec_ver, crc32c, cmds}`
- [ ] **STOR-02**: Every journal read verifies CRC; a corrupt record outside a torn tail fails the partition open instead of being skipped
- [ ] **STOR-03**: On restart the log end is recovered by scanning segments; a torn tail is truncated
- [ ] **STOR-04**: An Ostro-owned `ReplicatedLog` trait (append, HW, epoch cache, ISR) exists from day one; single node runs it with RF=1, ISR={self}
- [ ] **STOR-05**: Partition state machine applies committed commands deterministically into redb (`Durability::None` + periodic `Immediate` checkpoint of `applied_offset`); apply never passes HW
- [ ] **STOR-06**: Deleting a partition's state store and restarting rebuilds byte-identical state (rolling state hash matches)
- [ ] **STOR-07**: fsync/write errors (EIO, ENOSPC) are fatal for the affected partition and are never retried or reported as success
- [ ] **STOR-08**: WAL segments are deleted only below the last durable state checkpoint, and no GC/retention task runs before startup recovery has completed (readiness barrier)
- [ ] **STOR-09**: Fixed partition count configured at creation (default 16 single-node, 64 cluster); run ids embed their partition so any node routes without lookup
- [ ] **STOR-10**: Each new leadership term begins with a `LeaderAnnounced{epoch}` log entry; executors receive `(epoch, attempt)` fencing tokens validated at apply time
- [ ] **STOR-11**: Validation of appended commands happens at apply time; `AppendAck` is sent only after apply
- [ ] **STOR-12**: Cross-partition messages (child start/complete, signals to other runs, cancel cascades, cron starts) go through a per-partition outbox and shuffler with `(src_partition, seq)` deduplication
- [ ] **STOR-13**: Timers are loaded lazily in a time window; `TimerFired` is idempotent
- [ ] **STOR-14**: Large payloads (> 256 KiB inline limit) are stored in the blob store and referenced from the journal

### Metadata

- [ ] **META-01**: Metadata state machine runs on the vendored Raft driver with Ostro's own transport; single voter on a single node
- [ ] **META-02**: Metadata holds membership, partition map (leader, epoch, ISR), modules, workflows and active versions, queues, schedules, tokens, tenants, sealed secrets, grants, connections, egress policies
- [ ] **META-03**: Every replicated metadata record type carries a version field so stale records never overwrite newer state

### Engine, Determinism & Sandbox

- [ ] **ENG-01**: Wasmtime 50.x component host with NaN canonicalisation, deterministic relaxed SIMD, threads/GC disabled
- [ ] **ENG-02**: Host provides deterministic implementations of WASI imports runtimes pull in (journaled clocks, ChaCha20 random seeded per run, empty env, stdout/stderr → log, no FS/sockets/http); deploy-time validation rejects components importing anything else
- [ ] **ENG-03**: Replay compares every guest command (kind + canonical command hash) with the journal; a mismatch fails the run into the DLQ with reason `nondeterminism`, the sequence number, expected vs actual
- [ ] **ENG-04**: Recorded results are returned to the guest as the exact canonical bytes that were journaled
- [ ] **ENG-05**: Per-instance memory caps, fuel budgets per step/run, and epoch-based time slicing and deadlines; a breach fails only that attempt
- [ ] **ENG-06**: A guest at maximum Wasm stack depth calling a host function cannot overflow the native stack or abort the node
- [ ] **ENG-07**: Modules are AOT-compiled at deploy in a bounded compile pool; artefacts cached on disk keyed by sha256 + Wasmtime compatibility hash + Ostro version, HMAC-verified before load
- [ ] **ENG-08**: `InstancePre` + pooling allocator instantiate warm modules within NFR targets; pool sizing is configurable and fails clearly under address-space limits
- [ ] **ENG-09**: Activation model: hot-wait for short effects, otherwise evict (drop Store) and replay on re-dispatch; resident Wasm memory is bounded by a node budget with LRU eviction
- [ ] **ENG-10**: The host links WIT `0.1.x` (and later `0.2.x`) side by side, selected by the component's imports

### WIT Contract (ostro:runtime@0.1.0)

- [ ] **WIT-01**: Synchronous WIT world with context (info, now, random, uuid, is-replaying, patched), journal (begin-step / complete), effects (op handles + deterministic `wait`), log, and `workflows` export (describe, run)
- [ ] **WIT-02**: Guest-visible cancellation: a workflow can observe a cancel request and run cleanup; hard terminate is a separate operation
- [ ] **WIT-03**: First-class `continue-as-new` operation
- [ ] **WIT-04**: Callback/await tokens: a workflow obtains an opaque token (embedding partition and run) that an external caller completes without knowing the run id
- [ ] **WIT-05**: `set-status` publishes a JSON status readable via the API (instead of query handlers)
- [ ] **WIT-06**: Non-retryable error kind and per-run timeouts are expressible in the contract
- [ ] **WIT-07**: WIT 0.1 is frozen at the end of the single-node core phase; later changes are additive only

### Workflow Semantics

- [ ] **WF-01**: Developer can define a workflow with stable name + version and typed JSON input/output [FR-1.1]
- [ ] **WF-02**: Developer can define steps with retry policy (max attempts, initial/max interval, backoff coefficient, non-retryable kinds), timeout and fuel budget; replay returns recorded results without re-executing [FR-1.2]
- [ ] **WF-03**: Developer can await effects concurrently (gather/all/join) and race them, with completion order journaled and replayed identically [FR-1.3]
- [ ] **WF-04**: Developer can sleep for a duration or until a time; a sleeping run holds no memory [FR-1.4]
- [ ] **WF-05**: Developer can wait for a named signal with timeout; callers can send signals via API [FR-1.5]
- [ ] **WF-06**: Developer can start, await or detach child workflows in any language; cancellation propagates [FR-1.6]
- [ ] **WF-07**: Native time/random APIs (`time.time()`, `Date.now()`, `Math.random()`, `HashMap`) behave deterministically under replay [FR-1.7]
- [ ] **WF-08**: Developer can evolve in-flight code with `patched(id)` markers; runs stay pinned to their module sha [FR-1.8]
- [ ] **WF-09**: Run timeout and schedule-to-start (max queue wait) timeout are enforced
- [ ] **WF-10**: Saga/compensation helper runs registered compensations in reverse on failure [FR-1.11]

### Dispatch & Flow Control

- [ ] **DISP-01**: Per-partition leader dispatcher with in-memory ready queues rebuilt from state on leadership; no polling
- [ ] **DISP-02**: Ready-queue entries carry a concurrency key from day one
- [ ] **DISP-03**: Operator can configure per-queue concurrency, priority and rate limits [FR-3.8]
- [ ] **DISP-04**: Operator can configure keyed concurrency (e.g. per customer) with fair scheduling across keys
- [ ] **DISP-05**: Throttle (queue the excess) and rate limit (reject the excess) are distinct policies
- [ ] **DISP-06**: Per-tenant concurrency, fuel/s, memory and event-rate quotas with weighted fair dispatch
- [ ] **DISP-07**: Soft leases with re-dispatch after `lease_ttl`; zombie executors are fenced by `(epoch, attempt)` and self-fence after losing their lease

### Control-Plane API

- [ ] **API-01**: One client port serves HTTP/1.1, HTTP/2 and gRPC (tonic Routes into axum) with identical semantics; OpenAPI published [FR-0.3]
- [ ] **API-02**: Client can upload a module by sha256 (idempotent) and activate a workflow version [FR-3.1]
- [ ] **API-03**: Client can start a run with queue, delay, priority, tags, version, traceparent, tenant and idempotency key with a conflict policy (return existing run or reject) [FR-3.2]
- [ ] **API-04**: Client can get run status (incl. published status), history, and list/filter runs [FR-3.3]
- [ ] **API-05**: Client can signal, cancel (graceful), terminate, and retry runs [FR-3.4]
- [ ] **API-06**: Client can long-poll a run result [FR-3.5]
- [ ] **API-07**: Client can complete a callback/await token [WIT-04]
- [ ] **API-08**: Any node forwards requests to the owning partition leader; reads observe the caller's prior writes

### Triggers

- [ ] **TRIG-01**: Operator can map `POST /hooks/{name}` to a workflow with signature verification (Stripe, GitHub, Slack, generic HMAC) and idempotency key extraction [FR-3.6]
- [ ] **TRIG-02**: Operator can create cron schedules with timezone; each `(schedule, fire_time)` starts exactly once [FR-3.7]
- [ ] **TRIG-03**: Cron schedules support overlap policy, catch-up after downtime, pause and trigger-now

### Effects, Connectors & Secrets

- [ ] **FX-01**: HTTP effect: request intent committed before I/O; timeouts, response size cap, redirect policy, retries per policy [FR-4.1]
- [ ] **FX-02**: Every HTTP effect carries a stable idempotency key (run, op, name), overridable by the user
- [ ] **FX-03**: In-flight effects are re-driven by replay with the same key; if the provider's idempotency window has elapsed, the effect fails with `effect_outcome_unknown` instead of resending
- [ ] **FX-04**: SQL connector (Postgres, MySQL) with named connections; `exactly-once` mode wraps statements in a transaction with an `ostro_effects` idempotency token [FR-4.2]
- [ ] **FX-05**: Object store connector (S3/GCS/Azure/local via `object_store`): get/put/presign [FR-4.3]
- [ ] **FX-06**: Secrets are referenced as `{{secret:name}}`, resolved only in the host bridge, zeroized after use, never journaled or logged, and never visible to guests
- [ ] **FX-07**: Secret grants are bound to (module, secret, destination origins); templated requests do not auto-follow redirects; responses are scrubbed of secret values
- [ ] **FX-08**: Per-module egress allow-lists; SSRF guard blocks private/link-local/metadata ranges incl. IPv4-mapped IPv6, NAT64, decimal/octal literals, cloud metadata IPv6, redirects and env proxies (resolve once, pin address, no proxy) [FR-4.4]
- [ ] **FX-09**: Per-target rate limits for connectors [FR-4.5]
- [ ] **FX-10**: Pluggable secret providers: built-in sealed store, env/file, HashiCorp Vault, Kubernetes Secrets

### Workers & Placement

- [ ] **WORK-01**: `ostro worker --join` connects over an mTLS gRPC Execute stream with credits, heartbeats, blob fetch and per-(run, attempt) secret resolution
- [ ] **WORK-02**: Journals are paged to workers (no single message exceeds gRPC limits)
- [ ] **WORK-03**: Queue placement policies `embedded`, `workers`, `auto`; `auto` spills to workers on CPU/slots/memory/lag thresholds with hysteresis [FR-5.1/5.2]
- [ ] **WORK-04**: Worker labels and queue selectors control placement [FR-5.3]
- [ ] **WORK-05**: Each node exposes `ostro_queue_worker_demand{queue}` and a demand endpoint usable by KEDA; workers scale to zero [FR-5.4]
- [ ] **WORK-06**: Workers fetch modules by sha256 and cache AOT artefacts locally [FR-5.5]

### Backup & Bottomless

- [ ] **BACK-01**: Sealed segments (leader-only, below HW, keyed by partition/epoch/base offset), state snapshots, metadata snapshots and blobs stream to object storage [FR-6.2]
- [ ] **BACK-02**: `ostro restore --from <bucket>` rebuilds a node with RPO ≤ segment seal interval; restored state hash matches
- [ ] **BACK-03**: `ostro backup` / `ostro restore` to/from a local file [FR-2.8]
- [ ] **BACK-04**: One snapshot format serves restore and (later) follower installation; journal trim rule coordinates with snapshots

### Run Operations & Retention

- [ ] **OPS-01**: Operator can pause and resume a run, a workflow or a queue; runs can auto-pause when retries are exhausted
- [ ] **OPS-02**: Operator can rerun a run from a chosen step (journal prefix copied into a new run)
- [ ] **OPS-03**: Operator can bulk cancel/retry/rerun runs matching a filter, rate-spread
- [ ] **OPS-04**: Failed runs land in a DLQ with reason; an optional `on_failure` workflow hook runs; DLQ entries can be requeued
- [ ] **OPS-05**: Runs carry tags with bounded indexed search
- [ ] **OPS-06**: Terminal runs and journals are deleted after a per-workflow/queue TTL; dedupe entries have retention [FR-6.3]
- [ ] **OPS-07**: Published limits (payload sizes, history length, children per run) are documented and enforced with clear errors

### SDKs & Conformance

- [ ] **SDK-01**: Rust SDK: `#[workflow]`, `#[step]`, ctx, effects, `join!`/`select!`, built with `cargo build --target wasm32-wasip2` + wit-bindgen
- [ ] **SDK-02**: Python SDK: `@workflow`, `@step`, ctx, effects, SDK-owned asyncio loop with gather/race; built with pinned componentize-py (`--stub-wasi`)
- [ ] **SDK-03**: TypeScript SDK: `workflow()`, `step()`, ctx, effects, SDK-owned job loop with Promise.all/race; built with pinned jco (no WASI imports)
- [ ] **SDK-04**: SDKs reseed randomness after pre-init: two runs get different `ctx.random`, a replay reproduces the same values; builds are reproducible (same source → same sha)
- [ ] **SDK-05**: Language-neutral conformance suite (steps, retries, timers, signals, concurrency/race, cancellation, children, continue-as-new, await tokens, patching, non-determinism detection) passes for Rust, Python and TS
- [ ] **SDK-06**: Per-language test harness with time-skipping against `ostro dev --ephemeral` [FR-1.10]
- [ ] **SDK-07**: Thin client libraries (TS, Python, Go, Rust) for the control-plane API

### CLI

- [ ] **CLI-01**: `ostro dev` starts server + executor + UI with zero config in < 1 s; `--ephemeral` keeps state in memory [FR-0]
- [ ] **CLI-02**: `ostro server` runs a single node with zero-config defaults; TOML + env overrides [FR-0.2]
- [ ] **CLI-03**: `ostro init --lang {rust,python,ts}` scaffolds a project with pinned toolchains [FR-2.1]
- [ ] **CLI-04**: `ostro build` produces a validated component + manifest for Rust, Python and TS with hermetic toolchains (cached downloads or `--docker`) [FR-2.2/2.3]
- [ ] **CLI-05**: `ostro deploy [--activate]` builds if needed and uploads idempotently [FR-2.4]
- [ ] **CLI-06**: `ostro run/ls/inspect/history/signal/cancel/retry` with `--json` and `--dry-run` where applicable [FR-2.5]
- [ ] **CLI-07**: `ostro replay <run|--sample N> --module new.wasm` detects non-determinism before deploy (CI gate) [FR-2.6]
- [ ] **CLI-08**: `ostro dev --watch` rebuilds and redeploys on save [FR-2.7]

### UI

- [ ] **UI-01**: Embedded web UI served from the binary: run list/filter/tags, history timeline with replay markers, published status [FR-0.5]
- [ ] **UI-02**: UI actions: cancel, terminate, retry, rerun-from-step, pause/resume, DLQ requeue, bulk operations
- [ ] **UI-03**: UI views for modules, workflows/versions, queues, schedules, workers and cluster/partition health

### Observability

- [ ] **OBS-01**: OpenTelemetry traces: run → activation → step/effect spans, `traceparent` accepted at start and propagated on outbound HTTP; replayed spans not re-emitted [NFR-4]
- [ ] **OBS-02**: Prometheus `/metrics` on every node via the `metrics` facade (queues, lag, steps, replay, fuel, memory, DLQ, HW lag, ISR size, Raft)
- [ ] **OBS-03**: Guest logs are structured, rate-limited, bounded per run and forwarded to node logs/OTLP

### Security & Multi-tenancy

- [ ] **SEC-01**: TLS on the client listener (self-signed in dev, provided certs or ACME in production); mTLS on cluster and worker connections with a cluster CA
- [ ] **SEC-02**: Scoped API tokens (`admin`, `deploy`, `operate`, `trigger`, `read`) and per-tenant tokens; tenants see only their own resources [FR-3.9]
- [ ] **SEC-03**: Optional enforcement of signed modules (minisign/cosign)
- [ ] **SEC-04**: Audit log of deploy, activate, secret access, cancel/terminate, token changes
- [ ] **SEC-05**: Fuzzing (wasm-smith components, malformed WIT values) runs continuously; external security review has no open critical/high findings before 1.0

### Cluster

- [ ] **CLUS-01**: Replication conformance suite (placement, ISR honesty from measured LEOs, divergence/truncation, failover, min-insync) exists and fails on a deliberately broken build before replication is integrated
- [ ] **CLUS-02**: Partition logs replicate by follower-pull Fetch over the cluster listener; HW = min(LEO over ISR); leader epochs validated on every fetch; divergent tails truncated (incl. in object storage)
- [ ] **CLUS-03**: Partition leaders are elected only from the published ISR; writes stop when ISR < `min_insync_replicas`
- [ ] **CLUS-04**: Followers apply committed entries (hot standby) and can bootstrap from a snapshot; leader failover p99 < 10 s
- [ ] **CLUS-05**: Multi-voter metadata Raft (3/5 nodes) with membership changes; `ostro cluster init/join/remove`; a single node is promoted to a cluster without export [FR-0.4]
- [ ] **CLUS-06**: Rolling upgrade across one minor version without draining runs; workers advertise WIT/engine versions and receive compatible activations only [FR-6.5]
- [ ] **CLUS-07**: Module GC is reference-counted against non-terminal runs; `ostro migrate-runs` re-pins runs only after a passing dry-run replay

### Distribution

- [ ] **DIST-01**: Release artefacts: static linux x86_64/aarch64 binaries, signed and notarized macOS binaries, `FROM scratch` container image, install script [FR-0.1]
- [ ] **DIST-02**: Helm chart (StatefulSet servers, Deployment workers), KEDA ScaledObject examples, NetworkPolicies
- [ ] **DIST-03**: Docs: getting started per language, determinism guide, connectors, operations/sizing, migrations from cron/queue tables, Temporal and Restate

### Quality & Verification

- [ ] **QA-01**: Crash-injection harness (failpoints + `kill -9` + EIO/ENOSPC) proves 0 lost accepted runs and 0 duplicate journal entries over ≥ 10,000 injected faults per phase gate
- [ ] **QA-02**: Replay corpus replayed in CI across Ostro versions and across x86_64/aarch64
- [ ] **QA-03**: Fencing chaos test: external mock observes exactly one call per idempotency key despite zombie executors and failovers
- [ ] **QA-04**: Benchmarks verify acknowledged work and bytes on disk (not just rates); single node ≥ 10,000 steps/s, 3-node RF=3 ≥ 5,000 steps/s with p99 commit < 20 ms on LAN [NFR-2]
- [ ] **QA-05**: Jepsen-style test (partitions, clock skew, kill -9, disk stalls) on 3 nodes: 0 lost acknowledged runs; 7-day soak at 70% rated load is stable

## v2 Requirements

Deferred to 1.x / later. Tracked, not in the current roadmap.

- **V2-01**: Debounce and singleton runs
- **V2-02**: Streaming progress output from runs
- **V2-03**: Temporal-style updates and signal-with-start
- **V2-04**: Query handlers (re-activating replay)
- **V2-05**: Event bus / Kafka-Chronik triggers and produce connector [FR-3.10, FR-4.6]
- **V2-06**: Polyglot connector components composed at build time [FR-4.7]
- **V2-07**: Step-level placement hints [FR-5.6]
- **V2-08**: WIT 0.2 on WASI 0.3 native async
- **V2-09**: MCP server, SSO
- **V2-10**: gRPC connector
- **V2-11**: Kubernetes operator
- **V2-12**: Native worker protocol (non-Wasm SDKs) for native dependencies
- **V2-13**: Partition splitting / re-sharding

## Out of Scope

| Feature | Reason |
|---------|--------|
| Running inside Postgres / Postgres as required store | Superseded pgflow design: managed PG can't load it; crash blast radius |
| Option A: reusing Chronik's Kafka-Fetch fetcher | Drags in ~10k lines of Kafka handlers and can't be the RF=1 path (research) |
| Raw `wasi:http` / `wasi:sockets` for guests | Breaks determinism, egress control and idempotency |
| Linear-memory snapshots for durability | Huge for CPython, not portable across module versions |
| Visual workflow designer | Code-first product |
| General-purpose FaaS / HTTP serving | Not durable execution |
| Activity heartbeats, event batching, DSL/CEL trigger language | Anti-features per research |
| Restate-style virtual objects (per-key state) | Different product; 2.0+ at the earliest |
| Multi-region active-active | Far beyond 1.0 needs |
| Modifying the Chronik Stream repo | User decision: vendor instead |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| FOUND-01 | Phase 1 | Complete |
| FOUND-02 | Phase 1 | Complete |
| FOUND-03 | Phase 1 | Complete |
| FOUND-04 | Phase 1 | Complete |
| FOUND-05 | Phase 1 | Pending |
| FOUND-06 | Phase 1 | Pending |
| STOR-01 | Phase 2 | Pending |
| STOR-02 | Phase 2 | Pending |
| STOR-03 | Phase 2 | Pending |
| STOR-04 | Phase 2 | Pending |
| STOR-05 | Phase 2 | Pending |
| STOR-06 | Phase 2 | Pending |
| STOR-07 | Phase 2 | Pending |
| STOR-08 | Phase 2 | Pending |
| STOR-09 | Phase 2 | Pending |
| STOR-10 | Phase 2 | Pending |
| STOR-11 | Phase 2 | Pending |
| STOR-12 | Phase 2 | Pending |
| STOR-13 | Phase 2 | Pending |
| STOR-14 | Phase 2 | Pending |
| META-01 | Phase 4 | Pending |
| META-02 | Phase 4 | Pending |
| META-03 | Phase 4 | Pending |
| ENG-01 | Phase 3 | Pending |
| ENG-02 | Phase 3 | Pending |
| ENG-03 | Phase 3 | Pending |
| ENG-04 | Phase 3 | Pending |
| ENG-05 | Phase 3 | Pending |
| ENG-06 | Phase 3 | Pending |
| ENG-07 | Phase 3 | Pending |
| ENG-08 | Phase 3 | Pending |
| ENG-09 | Phase 3 | Pending |
| ENG-10 | Phase 13 | Pending |
| WIT-01 | Phase 3 | Pending |
| WIT-02 | Phase 3 | Pending |
| WIT-03 | Phase 3 | Pending |
| WIT-04 | Phase 3 | Pending |
| WIT-05 | Phase 3 | Pending |
| WIT-06 | Phase 3 | Pending |
| WIT-07 | Phase 5 | Pending |
| WF-01 | Phase 4 | Pending |
| WF-02 | Phase 4 | Pending |
| WF-03 | Phase 5 | Pending |
| WF-04 | Phase 5 | Pending |
| WF-05 | Phase 5 | Pending |
| WF-06 | Phase 9 | Pending |
| WF-07 | Phase 4 | Pending |
| WF-08 | Phase 5 | Pending |
| WF-09 | Phase 5 | Pending |
| WF-10 | Phase 9 | Pending |
| DISP-01 | Phase 4 | Pending |
| DISP-02 | Phase 4 | Pending |
| DISP-03 | Phase 7 | Pending |
| DISP-04 | Phase 7 | Pending |
| DISP-05 | Phase 7 | Pending |
| DISP-06 | Phase 14 | Pending |
| DISP-07 | Phase 4 | Pending |
| API-01 | Phase 4 | Pending |
| API-02 | Phase 4 | Pending |
| API-03 | Phase 4 | Pending |
| API-04 | Phase 4 | Pending |
| API-05 | Phase 5 | Pending |
| API-06 | Phase 5 | Pending |
| API-07 | Phase 5 | Pending |
| API-08 | Phase 13 | Pending |
| TRIG-01 | Phase 8 | Pending |
| TRIG-02 | Phase 8 | Pending |
| TRIG-03 | Phase 8 | Pending |
| FX-01 | Phase 6 | Pending |
| FX-02 | Phase 6 | Pending |
| FX-03 | Phase 6 | Pending |
| FX-04 | Phase 6 | Pending |
| FX-05 | Phase 6 | Pending |
| FX-06 | Phase 6 | Pending |
| FX-07 | Phase 6 | Pending |
| FX-08 | Phase 6 | Pending |
| FX-09 | Phase 6 | Pending |
| FX-10 | Phase 6 | Pending |
| WORK-01 | Phase 7 | Pending |
| WORK-02 | Phase 7 | Pending |
| WORK-03 | Phase 7 | Pending |
| WORK-04 | Phase 7 | Pending |
| WORK-05 | Phase 7 | Pending |
| WORK-06 | Phase 7 | Pending |
| BACK-01 | Phase 8 | Pending |
| BACK-02 | Phase 8 | Pending |
| BACK-03 | Phase 8 | Pending |
| BACK-04 | Phase 8 | Pending |
| OPS-01 | Phase 7 | Pending |
| OPS-02 | Phase 11 | Pending |
| OPS-03 | Phase 11 | Pending |
| OPS-04 | Phase 11 | Pending |
| OPS-05 | Phase 11 | Pending |
| OPS-06 | Phase 5 | Pending |
| OPS-07 | Phase 11 | Pending |
| SDK-01 | Phase 4 | Pending |
| SDK-02 | Phase 9 | Pending |
| SDK-03 | Phase 9 | Pending |
| SDK-04 | Phase 9 | Pending |
| SDK-05 | Phase 9 | Pending |
| SDK-06 | Phase 10 | Pending |
| SDK-07 | Phase 10 | Pending |
| CLI-01 | Phase 4 | Pending |
| CLI-02 | Phase 4 | Pending |
| CLI-03 | Phase 10 | Pending |
| CLI-04 | Phase 10 | Pending |
| CLI-05 | Phase 10 | Pending |
| CLI-06 | Phase 5 | Pending |
| CLI-07 | Phase 10 | Pending |
| CLI-08 | Phase 10 | Pending |
| UI-01 | Phase 11 | Pending |
| UI-02 | Phase 11 | Pending |
| UI-03 | Phase 11 | Pending |
| OBS-01 | Phase 8 | Pending |
| OBS-02 | Phase 8 | Pending |
| OBS-03 | Phase 8 | Pending |
| SEC-01 | Phase 7 | Pending |
| SEC-02 | Phase 14 | Pending |
| SEC-03 | Phase 14 | Pending |
| SEC-04 | Phase 14 | Pending |
| SEC-05 | Phase 14 | Pending |
| CLUS-01 | Phase 12 | Pending |
| CLUS-02 | Phase 12 | Pending |
| CLUS-03 | Phase 12 | Pending |
| CLUS-04 | Phase 12 | Pending |
| CLUS-05 | Phase 13 | Pending |
| CLUS-06 | Phase 13 | Pending |
| CLUS-07 | Phase 13 | Pending |
| DIST-01 | Phase 14 | Pending |
| DIST-02 | Phase 14 | Pending |
| DIST-03 | Phase 14 | Pending |
| QA-01 | Phase 5 | Pending |
| QA-02 | Phase 5 | Pending |
| QA-03 | Phase 7 | Pending |
| QA-04 | Phase 14 | Pending |
| QA-05 | Phase 14 | Pending |

**Coverage:**
- v1 requirements: 136 total
- Mapped to phases: 136
- Unmapped: 0 ✓

---
*Requirements defined: 2026-11-18*
*Last updated: 2026-11-18 after roadmap creation (14 phases)*
