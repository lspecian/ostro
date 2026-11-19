# Roadmap: Ostro

## Overview

Ostro is built in this order: single-node product first, cluster last. Each step builds on the one before.

1. **Gate phase.** Vendor the Chronik code, bootstrap the toolchain and run the spikes.
2. **Two parallel tracks.**
   - The durable partition log and state machine, running at RF=1 behind `ReplicatedLog`.
   - The deterministic Wasm engine and the `ostro:runtime@0.1.0` contract.
3. **Single-node core.** The tracks join here. Rust workflows run end to end, survive `kill -9` and replay deterministically. The on-disk format, the command set and the WIT 0.1 contract are frozen at the end of this stage.
4. **Outside world and scale-out.** Effects with exactly-once / idempotent semantics and secrets that never enter guests. Stateless workers with fencing and flow control. Triggers, bottomless backup and observability.
5. **Product surface.** Python and TypeScript become first-class, verified by one conformance suite. Then the hermetic CLI, run operations and the embedded UI.
6. **Cluster, added behind `ReplicatedLog`.** The replication conformance suite comes first, then follower-pull ISR, multi-voter metadata and rolling upgrades.
7. **1.0 gate.** Tenancy, security review, Jepsen/soak/benchmarks and distribution.

Mapping to `docs/05-roadmap.md`:

| docs/05 phase | GSD phases |
|---------------|------------|
| Phase 0 | 1 |
| Phase 1 | 2–5 |
| Phase 2 | 6–8 |
| Phase 3 | 9–11 |
| Phase 4 | 12–14 |

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Foundation, Vendoring & Spikes** - Workspace, vendored and modernised Chronik code, CI matrix, and S2/S3 spike decisions (completed 2026-11-18)
- [x] **Phase 2: Durable Partition Log & State Machine** - CRC-verified WAL journal behind an RF=1 `ReplicatedLog`, deterministic fjall apply, outbox/shuffler, timers, fencing epochs (completed 2026-11-19)
- [x] **Phase 3: Deterministic Wasm Engine & WIT 0.1** - Wasmtime component host with virtualised WASI, replay divergence detection, sandbox limits, AOT cache/pooling, full WIT 0.1 surface (completed 2026-11-18)
- [x] **Phase 4: Single-Node Server & Rust MVP** - Metadata Raft, dispatcher, unified HTTP/gRPC API, `ostro dev`/`server`, and Rust SDK running workflows with steps end to end (completed 2026-11-19)
- [ ] **Phase 5: Workflow Semantics & Core Hardening** - Timers, signals, concurrency, cancel, patching, timeouts; crash-injection harness, replay corpus, retention, WIT 0.1 freeze
- [ ] **Phase 6: Effects Bridge, Connectors & Secrets** - HTTP/SQL/object-store effects with idempotency semantics, origin-bound secrets, SSRF-hardened egress
- [ ] **Phase 7: Workers, Placement & Flow Control** - mTLS stateless workers, embedded/workers/auto placement, KEDA demand, keyed concurrency/throttle/rate limits, pause, fencing chaos
- [ ] **Phase 8: Triggers, Bottomless & Observability** - Webhooks and cron, object-storage backup/restore with a single snapshot format, OTel traces, Prometheus metrics, guest logs
- [ ] **Phase 9: Polyglot SDKs & Conformance** - Python and TypeScript SDKs with SDK-owned event loops and reseeding, cross-language children, saga helper, three-SDK conformance
- [ ] **Phase 10: Developer CLI & Test Kits** - `ostro init/build/deploy/replay/dev --watch` with hermetic toolchains, time-skipping test harnesses, thin client libraries
- [ ] **Phase 11: Run Operations & Embedded UI** - Rerun-from-step, bulk ops, DLQ with `on_failure`, tags/search, published limits, and an embedded UI with actions
- [ ] **Phase 12: Replicated Partition Logs** - Replication conformance suite first, then follower-pull ISR, HW, epoch truncation, ISR-only election, hot-standby followers
- [ ] **Phase 13: Cluster Membership, Routing & Upgrades** - Multi-voter metadata, `ostro cluster init/join/remove`, request forwarding, rolling upgrades with N/N-1 WIT linkers, module GC and `migrate-runs`
- [ ] **Phase 14: Multi-Tenancy, Security & 1.0 Release** - Scoped/tenant tokens and quotas, signed modules, audit log, fuzzing + external review, Jepsen/soak/benchmarks, release artefacts, Helm, docs

## Phase Details

### Phase 1: Foundation, Vendoring & Spikes
**Goal**: The workspace builds on pinned tooling with Chronik code vendored and modernised, CI enforces the dependency and platform rules, and spike measurements have settled the open stack decisions
**Depends on**: Nothing (first phase)
**Requirements**: FOUND-01, FOUND-02, FOUND-03, FOUND-04, FOUND-05, FOUND-06
**Success Criteria** (what must be TRUE):
  1. `cargo build --workspace` succeeds on Rust 1.100 with the research crate layout. The vendored raft/WAL crates compile without a `[patch]` section and without chronik-common/monitoring or tokio-uring.
  2. `VENDORED.md` lists every vendored file with its source path, Chronik commit `08715f4` and local modifications. Nothing in the workspace references the Chronik repo.
  3. CI runs and passes on linux x86_64 and aarch64 (musl via cargo-zigbuild with mimalloc) and on macOS. cargo-deny fails the build when a duplicate tonic/prost/hyper/axum/opentelemetry version or a disallowed licence is introduced.
  4. The S2 report gives Python/TS guest `.wasm`/AOT size, instance memory and cold start, and the pre-init randomness behaviour. The S3 report gives redb throughput per durability tier. Three decisions are logged in PROJECT.md: step handle (resource vs u64), redb vs fjall, and the Wasmtime pin policy.
**Plans**: 8 plans (5 waves)

Plans:
- [x] 01-01-PLAN.md — Workspace skeleton (11 crates with real deps) on Rust 1.100.0 + cargo-deny policy and negative fixtures (W1)
- [x] 01-02-PLAN.md — Spike S3: redb durability-tier benchmark, crash sanity, report vs 90k cmds/s rule (W1)
- [x] 01-03-PLAN.md — Spike S2 apparatus: shared WIT, py/ts/rust guests, Wasmtime 50.0.2 harness (W1)
- [x] 01-04-PLAN.md — Vendor + modernise raft stack (ostro-raft-proto/-rs/-driver), wire ostro-meta, start VENDORED.md (W2)
- [x] 01-05-PLAN.md — Spike S2 measurements and report (sizes, instantiate, memory, cold start, randomness, step handle) (W2)
- [x] 01-06-PLAN.md — Vendor + modernise WAL (ostro-wal), wire ostro-log, complete VENDORED.md, check-vendored.sh (W3)
- [x] 01-07-PLAN.md — Local CI gate (incl. musl zigbuild) + GitHub Actions matrix; remote legs human_needed (W4)
- [x] 01-08-PLAN.md — Quiet-machine confirmation runs + log the three decisions in PROJECT.md (W5)
**Research flag**: Guest toolchain behaviour under `--stub-wasi` (measure, don't assume)

### Phase 2: Durable Partition Log & State Machine
**Goal**: Each partition is a crash-safe, CRC-verified journal behind an RF=1 `ReplicatedLog`, with a deterministic redb state machine that can always be rebuilt from the log. The on-disk format and command envelope are fixed here.
**Depends on**: Phase 1 (can run in parallel with Phase 3)
**Requirements**: STOR-01, STOR-02, STOR-03, STOR-04, STOR-05, STOR-06, STOR-07, STOR-08, STOR-09, STOR-10, STOR-11, STOR-12, STOR-13, STOR-14
**Success Criteria** (what must be TRUE):
  1. Torn tails are truncated on restart and the log end is found by scanning the segments. A flipped bit outside the torn tail makes partition open fail loudly, never skip silently. Injected EIO/ENOSPC on write or fsync is fatal for that partition and is never reported as success.
  2. Deleting a partition's redb state and restarting rebuilds byte-identical state: the rolling state hash matches. Apply never passes HW, and `AppendAck` is returned only after apply-time validation.
  3. Every leadership term begins with `LeaderAnnounced{epoch}`. A command carrying a stale `(epoch, attempt)` token is rejected at apply.
  4. Cross-partition messages (child start/complete, signals, cancels, cron starts) are delivered exactly once across P=16 partitions through the outbox and shuffler, even when replayed after a crash. Duplicate `TimerFired` entries have no effect.
  5. A payload over 256 KiB is stored in the blob store and referenced from the journal. WAL segments are never deleted above the last durable checkpoint or before the startup readiness barrier.
**Plans**: 16 plans (9 waves)

Plans:
- [x] 02-01-PLAN.md — Wave 0: fjall replaces redb, all Phase 2 workspace deps, cargo-deny with fjall tree, nextest, ostro-testkit corruption helpers, determinism clippy config (W1)
- [x] 02-02-PLAN.md — Vendored WAL patches P1 poison / P2 dir fsync / P3 failpoints + VENDORED.md + CI failpoint step (W1)
- [x] 02-03-PLAN.md — ostro-types core: ids/RunId, xxh3 routing golden vectors, PayloadRef, 32-byte envelope + CRC32C (W2)
- [x] 02-04-PLAN.md — ostro-blob content-addressed durable blob store (W2)
- [x] 02-05-PLAN.md — fjall state store layer: per-partition DB, keyspaces, key/value codecs, ApplyBatch overlay, checkpoint, logical digest (W3)
- [x] 02-06-PLAN.md — Partition command set (prost) with golden encodings, opaque journal events + Derived intents (W3)
- [x] 02-07-PLAN.md — Log recovery: verified full scan, torn-tail vs corruption rule (applied_offset floor), physical cut (W3)
- [x] 02-08-PLAN.md — ReplicatedLog trait + RF=1 Rf1Log/LogNode, HW from commit callback, epoch cache, fatal I/O, trim/barrier (W4)
- [x] 02-09-PLAN.md — Pure apply core: epoch/attempt fencing, StartRun dedupe, EventsAppended + Derived, payload limit, rolling hash, read budget (W4)
- [x] 02-10-PLAN.md — Apply cross-partition: inbound (src, seq) dedupe, child/signal/cancel cascade/cron, outbox truncation, idempotent TimerFired (W5)
- [x] 02-11-PLAN.md — Apply machine thread (≤ HW, chunked WriteBatch, 100 ms/256-batch checkpoints), ack rendezvous, PartitionHandle, blob offload, fail-stop (W5)
- [x] 02-12-PLAN.md — Node: ordered startup, node.toml P check, LeaderAnnounced per term, ReadyBarrier, router, leader skeleton, rebuild (W6)
- [x] 02-13-PLAN.md — Outbox shuffler + exactly-once property over P=16 with restarts and flaky acks (W7)
- [x] 02-14-PLAN.md — Lazy windowed timer service + barrier-gated, inert-by-default retention (W7)
- [x] 02-15-PLAN.md — Rebuild-digest proptest + ostro-crashtest kill -9 / torn / corruption / failpoint loops (W8)
- [x] 02-16-PLAN.md — fjall reopen/throughput/stall measurement + tuning, docs/storage-format.md, docs/03, CI, phase gate (W9)
**Research flag**: redb durability tiers; WAL V2 recovery paths (confirm none decode `canonical_data` as Kafka)

### Phase 3: Deterministic Wasm Engine & WIT 0.1
**Goal**: Wasm components run in a deterministic, sandboxed host that replays them exactly and detects divergence. It is built against a fake `ActivationSink`, in parallel with storage. The full `ostro:runtime@0.1.0` surface is defined, including the research additions.
**Depends on**: Phase 1 (can run in parallel with Phase 2)
**Requirements**: ENG-01, ENG-02, ENG-03, ENG-04, ENG-05, ENG-06, ENG-07, ENG-08, ENG-09, WIT-01, WIT-02, WIT-03, WIT-04, WIT-05, WIT-06
**Success Criteria** (what must be TRUE):
  1. A determinism test guest using `SystemTime::now`, `HashMap` iteration and `rand` produces identical journals across 1,000 replays. A modified module fails with `nondeterminism` at the correct sequence number, with expected vs actual in the DLQ reason.
  2. Infinite-loop, memory-bomb and deep-recursion guests are stopped without affecting other runs. A guest at maximum Wasm stack depth calling a host function does not abort the node.
  3. Deploy-time validation rejects a component that imports anything outside the allowed WIT/WASI set. Accepted modules are AOT-compiled in a bounded pool and cached by `sha256 + compat hash + Ostro version`. A tampered cache artefact fails HMAC verification and is recompiled.
  4. Warm modules instantiate through `InstancePre` + pooling within the NFR targets (Rust < 1 ms p50). Pool misconfiguration under address-space limits fails with a clear error. Evicted activations replay correctly within a bounded node memory budget.
  5. The WIT world compiles for guests with context, journal, effects/`wait`, log and the workflows export. It also covers cancel-vs-terminate, `continue-as-new`, await tokens, `set-status`, run timeouts and the non-retryable error kind.
**Plans**: 12 plans (6 waves)

Plans:
- [x] 03-01-PLAN.md — Contracts + scaffold: full WIT 0.1, guests workspace + build helper, engine module skeleton/deps, error/sink/env contracts, ostro-types journal types (W1)
- [x] 03-02-PLAN.md — TDD: journal event/result codec + per-kind command hash v1 with golden vectors (W2)
- [x] 03-03-PLAN.md — Engine config (NaN canon, det. relaxed SIMD, 4 MiB fiber stack), pooling per language class + wrapped failure/fallback, limiter + node budget, epoch ticker (W2)
- [x] 03-04-PLAN.md — Ostro-owned deterministic WASI 0.2.12 shim + deploy-time import/export validation + import-probe snapshot (W2)
- [x] 03-05-PLAN.md — TDD: pure replay core (cursor/divergence, time rule, wait ordering, patch/status/cancel, ChaCha streams, await tokens) (W2)
- [x] 03-06-PLAN.md — AOT cache: bounded single-flight compile pool, sha256+compat-hash+version key, HMAC-verified load (W2)
- [x] 03-07-PLAN.md — Host adapter for every ostro:runtime import over the replay core + production linker + det-basic/wit-surface guests (W3)
- [x] 03-08-PLAN.md — Activation driver + Runtime facade + failure classification + FakeSink testkit (W4)
- [x] 03-09-PLAN.md — Determinism suite: 1,000 records/replays, divergence at exact seq, golden journal (W5)
- [x] 03-10-PLAN.md — Sandbox (fuel/epoch/memory/recursion/isolation) + max-depth host-call stack test (W5)
- [x] 03-12-PLAN.md — WIT-01..06 behaviour tests: every import, cancel/cleanup, continue-as-new, await tokens, set-status, failures/timeouts (W5)
- [x] 03-11-PLAN.md — Hot-wait vs evict, LRU node-budget eviction, Runtime AOT/InstancePre cache, warm p50; full ci-local gate (W6)
**Research flag**: Wasmtime 50.0.2 pooling allocator and fiber-stack sizing

### Phase 4: Single-Node Server & Rust MVP
**Goal**: A developer runs `ostro dev`, uploads a Rust workflow component through the API, starts runs and sees them complete with steps, retries and recorded results. Storage, engine, metadata and dispatch are joined in one binary.
**Depends on**: Phase 2, Phase 3
**Requirements**: META-01, META-02, META-03, DISP-01, DISP-02, DISP-07, API-01, API-02, API-03, API-04, CLI-01, CLI-02, SDK-01, WF-01, WF-02, WF-07
**Success Criteria** (what must be TRUE):
  1. `ostro dev` starts server, executor and UI shell with zero config in under 1 s, and `--ephemeral` keeps state in memory. `ostro server` starts a single node from defaults, with TOML and env overrides applied.
  2. A Rust workflow built with `cargo build --target wasm32-wasip2` using `#[workflow]`/`#[step]` is uploaded by sha256 (re-upload is a no-op), activated, and started over both HTTP/JSON and gRPC on the same port. The OpenAPI document is served.
  3. A step that fails is retried per its policy (backoff, max attempts, non-retryable kinds). After eviction or restart, replay returns recorded results without re-executing completed steps. Native time/random calls in the guest replay identically.
  4. Starting a run twice with the same idempotency key returns the existing run or rejects, depending on the conflict policy. Run status, history and filtered run lists come back through the API.
  5. Metadata (modules, workflows, queues, tokens) persists across restart through the single-voter Raft group. A stale metadata record never overwrites a newer one. When an executor loses its lease, the run is re-dispatched after `lease_ttl` and the zombie's late completion is fenced out.
**Plans**: 20 plans (8 waves; plans editing ostro-partition apply/store/handle or ostro-types command.rs are gated behind 04-09, which requires Phase 2 complete)

Plans:
- [x] 04-01-PLAN.md — ostro-meta: records, versioned CAS apply, MetaSm snapshot, MetaHandle with immediate campaign (META-01/02/03)
- [x] 04-02-PLAN.md — Workspace pins + ostro-server skeleton with layered ServerConfig + one-port smoke test (CLI-02)
- [x] 04-03-PLAN.md — ostro.api.v1 protos (Modules, Workflows, Runs) via protox (API-01)
- [x] 04-04-PLAN.md — StepAttemptFailed (kind 29, additive) + pure retry decision + replay attempt chains (WF-02)
- [x] 04-05-PLAN.md — Rust SDK runtime crate (sdks/rust/ostro-sdk): bindings, Ctx, step runner, ops, join!/select!, export! (SDK-01)
- [x] 04-06-PLAN.md — #[workflow]/#[step] macros + trybuild + SDK test guests validated on the engine (SDK-01, WF-01, WF-07)
- [x] 04-07-PLAN.md — Host step retry: retry-now, StepBackoff eviction, Runtime::prepared(sha) (WF-02)
- [x] 04-08-PLAN.md — ControlPlane trait, domain types, ApiError mapping, REST + utoipa OpenAPI + UI shell (API-01..04)
- [x] 04-09-PLAN.md — PHASE-4 GATE (Phase 2 complete) + StartRun fields 14..18 + Derived::Suspend (API-03)
- [x] 04-10-PLAN.md — Step timeout + per-step fuel breach attribution (WF-02)
- [x] 04-11-PLAN.md — gRPC adapters + one-port REST/gRPC parity suite (API-01)
- [x] 04-12-PLAN.md — Apply gaps: RunRecord fields, Running keyspace, Suspend CAS, InputAppended, query helpers (API-03, DISP-01/02)
- [x] 04-13-PLAN.md — TimerFired journaled as input + wake (Gap 3) (DISP-01, WF-07)
- [x] 04-14-PLAN.md — Dispatcher leader service: Executor trait, rebuild, no polling, soft leases (DISP-01/02/07)
- [x] 04-15-PLAN.md — derive (journal -> Derived), PartitionSink (ActivationSink), RunStarted synthesis, unsupported effects (WF-01/02)
- [x] 04-16-PLAN.md — EngineExecutor + lease heartbeat/self-fence + DISP-07 zombie fencing suite (DISP-07, WF-02)
- [x] 04-17-PLAN.md — Server bootstrap (meta -> runtime -> node -> API), MetaLeadership, ephemeral, modules/workflows ControlPlane (META-01/03, API-02, CLI-01)
- [x] 04-18-PLAN.md — Runs ControlPlane: start (idempotency), get, history, list (API-03/04, WF-01)
- [x] 04-19-PLAN.md — `ostro dev` / `ostro server` / `ostro openapi` binary + startup/config tests (CLI-01/02)
- [x] 04-20-PLAN.md — End-to-end success-criteria tests (spawned `ostro dev` + full server) + ci-local gate

### Phase 5: Workflow Semantics & Core Hardening
**Goal**: The single-node core is feature-complete for durable workflows and proven crash-safe. The docs/05 Phase 1 exit gate passes, and WIT 0.1, the command set and the on-disk format are frozen.
**Depends on**: Phase 4
**Requirements**: WF-03, WF-04, WF-05, WF-08, WF-09, API-05, API-06, API-07, CLI-06, OPS-06, WIT-07, QA-01, QA-02
**Success Criteria** (what must be TRUE):
  1. The `order-saga` example (5 steps, a timer, a signal) runs from `ostro dev` on a clean machine in under 10 minutes. Concurrent gather/race completion order replays identically. A sleeping run holds no memory.
  2. Users can signal, gracefully cancel (the guest runs cleanup), terminate, retry, long-poll results and complete await tokens via the API and via `ostro run/ls/inspect/history/signal/cancel/retry` (`--json`, `--dry-run`). Run timeouts and schedule-to-start timeouts fire, and `patched(id)` lets in-flight runs continue on old code paths.
  3. The crash-injection harness runs ≥ 10,000 faults (`kill -9`, failpoints, EIO/ENOSPC) under load. Result: 0 lost accepted runs, 0 duplicate journal entries, and every run is terminal or correctly resumed.
  4. The replay corpus is green in CI on x86_64 and aarch64. Terminal runs and journals are deleted after their TTL. A single node sustains ≥ 5,000 trivial Rust steps/s with fsync on, and start → first step is < 5 ms p50.
  5. WIT `ostro:runtime@0.1.0` is tagged frozen. A CI check fails any non-additive change to it.
**Plans**: 24 plans (10 waves; 05-01 is the PHASE-5 GATE (Phase 4 complete; code vs .planning/RESERVATIONS.md); decisions D1–D23 adopted in 05-DECISIONS.md, user may override; 05-21..05-24 were split off by the final plan check without renumbering; 05-20 runs the sharded >= 10,000-kill campaign and flips the WIT/command/on-disk/meta freeze)

Plans:
- [ ] 05-01-PLAN.md — PHASE-5 GATE (Phase 4 complete, code vs RESERVATIONS.md) + adopted decisions to STATE + ostro-types contract (Cmd 8..12, StartRun 19..23, intents, statuses, kinds 70/71/96/97, message fields) + status parity (wave 1)
- [ ] 05-02-PLAN.md — Partition store schema (waits, signal_seen, SignalSlot inbox, RunRecord fields, Default) + apply seams/stubs/hooks + journal_input/journal_encoded + storage-format doc (wave 2; WF-05, WF-09, OPS-06)
- [ ] 05-03-PLAN.md — Engine semantics: cancel-op completion, WaitTimedOut, SignalMatched, patched memo/skip, op index, live_activations, gather/race proptest (wave 2; WF-03, WF-04, WF-08)
- [ ] 05-04-PLAN.md — API surface: signal/cancel/terminate/retry/result/tokens/policies over REST + gRPC against FakeControlPlane (wave 2; API-05..07)
- [ ] 05-05-PLAN.md — WIT freeze tooling + pins + pre-freeze WIT review and doc comments (Phase 6/7/9/14 findings) (wave 2; WIT-07)
- [ ] 05-06-PLAN.md — Rust SDK: await tokens, continue-as-new, D11 cancellation + shielded (wave 2; WF-03, WF-05, API-07)
- [ ] 05-07-PLAN.md — Meta pre-freeze encoding: policies, Schedule/Webhook, ext TLV on every record, Tenant quotas, WorkflowVersion.module, P12 replica fields, META_FORMAT bump, golden (wave 2; OPS-06, WF-09)
- [ ] 05-08-PLAN.md — Apply: waits registry, signals (incl. pending-before-start), wait timeouts, await tokens, graceful cancel, CAN cancel forwarding (wave 3; WF-05, API-05, API-07)
- [ ] 05-09-PLAN.md — Apply: terminate, retry-resume, timeouts, continue-as-new, retention expiry (wave 3; WF-09, API-05, OPS-06)
- [ ] 05-10-PLAN.md — Test guests + examples/order-saga (wave 3; WF-03, WF-05, WF-08, WF-09)
- [ ] 05-11-PLAN.md — Server seam: derive mappings, resume re-arm, executor lifecycle, pause hook, module_override, policy resolution + [runs] config (wave 4)
- [ ] 05-12-PLAN.md — Replay corpus tooling + seed entries + format corpus tooling (wave 4; QA-02)
- [ ] 05-13-PLAN.md — Server ControlPlane: lifecycle ops, dry_run, token verify, start fields + policies, long-poll (wave 5; API-05..07)
- [ ] 05-14-PLAN.md — CLI verbs with --json/--dry-run + minimal deploy (wave 6; CLI-06)
- [ ] 05-15-PLAN.md — Workflow semantics e2e (incl. signal right after start, resume after timeout, 1 MiB payloads, sleepers) (wave 6)
- [ ] 05-16-PLAN.md — Perf harness + start-path optimisations; WSL2 advisory, native NVMe human_needed (wave 7)
- [ ] 05-17-PLAN.md — order-saga via CLI on the final start path, quickstart timing, workflow-semantics docs (wave 8; SC-1, CLI-06)
- [ ] 05-18-PLAN.md — Failpoint feature chain + new sites + FAILPOINT_CRATES/ci.yml sync (wave 8; QA-01)
- [ ] 05-19-PLAN.md — Whole-server kill campaign (sharded, every fault ends in kill -9) + CI smoke (wave 9; QA-01)
- [ ] 05-20-PLAN.md — PHASE GATE + FREEZE FLIP: >= 10,000 kills, corpora, FROZEN.toml + golden/meta-golden re-pin + RESERVATIONS flip + local tag, full ci-local (wave 10)
- [ ] 05-21-PLAN.md — Empty-log anchoring at applied_offset (Phase 12 F3) in ostro-log recovery + partition startup (wave 2)
- [ ] 05-22-PLAN.md — Ready entries (workflow/tenant/first), durable attempt_failures, status_seq, parent_event forwarding (wave 3)
- [ ] 05-23-PLAN.md — D22 retry logical clock + long_backoff expectation fixes (wave 3)
- [ ] 05-24-PLAN.md — D23 tenant-qualified names + upload tenant/name (WorkflowVersion.module) + two-tenant test (wave 3)

### Phase 6: Effects Bridge, Connectors & Secrets
**Goal**: Workflows safely call the outside world. HTTP effects are idempotent and never blindly resent. SQL effects are exactly-once. Secrets never enter guests, and egress cannot reach internal networks.
**Depends on**: Phase 5
**Requirements**: FX-01, FX-02, FX-03, FX-04, FX-05, FX-06, FX-07, FX-08, FX-09, FX-10
**Success Criteria** (what must be TRUE):
  1. An HTTP effect's intent is committed before any I/O and carries a stable idempotency key. In-flight effects are re-driven by replay with the same key. Past the provider's idempotency window, the effect fails with `effect_outcome_unknown` instead of resending.
  2. With a crash injected between the user DB commit and `effect_completed`, the SQL exactly-once mode produces exactly one row change per run on both Postgres and MySQL.
  3. A canary secret referenced as `{{secret:name}}` never appears on disk, in the journal, in logs, in OTLP or in object storage, and is never readable by the guest. Grants bound to other origins are refused, templated requests don't follow redirects, and echoed values are scrubbed from responses.
  4. The SSRF bypass table is blocked: private, link-local and metadata ranges, IPv4-mapped IPv6, NAT64, decimal/octal literals, `fd00:ec2::254`, redirects and env proxies. Per-module allow-lists and per-target rate limits are enforced.
  5. Object-store get/put/presign works against S3-compatible and local backends. Secrets resolve from the sealed store, env/file, Vault and Kubernetes providers.
**Plans**: 20 plans (8 waves; 06-01 is the PHASE-6 GATE requiring Phase 5 complete (all 05-*-SUMMARY + 05-VERIFICATION passed, or human_needed with by-design items only, + WIT freeze in place) and checking the code against .planning/RESERVATIONS.md (P6 block: Cmd 13, Intent 9/10, InvalidCode 15, effects rows); 06-02 lands deps/skeletons/contract; decisions D-FX1–D-FX9 + P-1–P-21 adopted in 06-DECISIONS.md, user may override; MySQL/MinIO legs env-gated → CI service containers / human_needed; the OTLP canary leg closes at the Phase 8 gate)

Plans:
- [ ] 06-01-PLAN.md — PHASE-6 GATE (Phase 5 + WIT freeze, code vs RESERVATIONS) + decisions to STATE + FX-05 wording + RESERVATIONS names and canonical InvalidCode placeholder names (docs only) (all FX)
- [ ] 06-02-PLAN.md — All Phase 6 deps + ostro-bridge contract skeleton + ostro-objstore skeleton (08-03 signatures, if absent) + ostro-types contract (CompleteEffect 13 = encoded EffectCompleted payload, RegisterEffect/CancelEffect 9/10, BadEffectCompletion 15) (all FX)
- [ ] 06-03-PLAN.md — Partition apply: effects rows (#12), RegisterEffect/CancelEffect, first-wins CompleteEffect stored verbatim, resumable-aware cleanup, LeaderAnnounced wake (FX-01, FX-03)
- [ ] 06-04-PLAN.md — Egress core: URL gate, IANA classifier + mapped/NAT64/6to4 unwrap, allow-lists, hickory resolver with pinning, guarded client, capped reader, full [egress] config, SSRF table (FX-08)
- [ ] 06-05-PLAN.md — Secrets core: template parser, origin-bound grants, multi-encoding scrubber, AES-256-GCM sealed store, env/file/sealed providers + chain, resolver (FX-06, FX-07, FX-10)
- [ ] 06-06-PLAN.md — Engine seam: EffectDriver + ActivationSink::effect_driver, start only after the HostState::flush commit, replay re-drive, redacted journal copy (FX-01, FX-03, FX-06)
- [ ] 06-07-PLAN.md — Bridge driver core: Bridge/EffectRegistry, gate → rate limit → execute → completion sink, panic → internal_error, per-target rate limits (FX-09, FX-01, FX-03)
- [ ] 06-08-PLAN.md — Admin API surface: bridge.proto (Secrets, Connectors), REST/gRPC/fake/parity/OpenAPI, unauthenticated local presign route (FX-05..FX-08, FX-10)
- [ ] 06-09-PLAN.md — Rust SDK effect builders + ctx.idempotency_key + guests fx-http/fx-sql/fx-object/fx-canary/fx-ssrf (FX-02, FX-04, FX-05)
- [ ] 06-10-PLAN.md — HTTP connector: idempotency key, unknown-outcome window, retries/timeouts/size cap, templating + no-redirect + identity encoding + scrub/redact, manual redirects, mock provider (FX-01, FX-02, FX-03, FX-07)
- [ ] 06-11-PLAN.md — SQL connector: sqlx PG/MySQL pools (schema option), statement allow-list, token-first protocol with post-COMMIT reconcile (never Retryable), retention horizon, GC (FX-04)
- [ ] 06-12-PLAN.md — Object store: shared ostro-objstore LocalFs + S3 (rusty-s3) + presign, bridge object adapter + local HMAC presign (FX-05)
- [ ] 06-13-PLAN.md — Vault (vaultrs) + Kubernetes (kube) secret providers with zeroizing TTL cache (FX-10)
- [ ] 06-14-PLAN.md — Server seam: config sections, BridgeServices + PresignKeyStore, EffectDriver adapter, CompleteEffect submission, SendGate, derive mappings, no unsupported repair for effects (B1), test helpers (FX-01..FX-06, FX-08)
- [ ] 06-15-PLAN.md — Seam e2e (in-process): intent before I/O, journal delivery, crash re-drive instead of unsupported repair, 1 MiB blob outcome, evicted-run wake, children regression (FX-01, FX-03, FX-06)
- [ ] 06-16-PLAN.md — Server admin impl (sealed secrets, grants, connections, egress, presign route) + CLI verbs + API-based e2e helpers (FX-05..FX-08, FX-10)
- [ ] 06-17-PLAN.md — E2E SC1/SC4: kill -9 re-drive with same key, window → effect_outcome_unknown, SSRF table via guest (classifier reasons asserted), env proxy, allow-lists, rate limits (FX-01, FX-02, FX-03, FX-08, FX-09)
- [ ] 06-18-PLAN.md — E2E SC2: kill -9 between COMMIT and effect_completed on PG (+ MySQL in CI), CI service containers (shared MinIO spec), ci-local env gating (FX-04)
- [ ] 06-19-PLAN.md — E2E SC3/SC5: canary harness (OTLP hook for Phase 8; mocks isolated from capture), grant/redirect/echo, object via guest (local + MinIO), five secret providers (FX-05, FX-06, FX-07, FX-10)
- [ ] 06-20-PLAN.md — PHASE GATE: docs (connectors/egress/secrets incl. normative error table, docs/03 + docs/02 fixes), effect replay-corpus entries, RESERVATIONS flip, freeze/deny re-check, full ci-local, VALIDATION (all)
**Research flag**: SSRF and secret binding; MySQL implicit-commit list; provider idempotency windows

### Phase 7: Workers, Placement & Flow Control
**Goal**: Compute scales out to stateless workers without duplicates or lost work. Operators control where and how fast work runs.
**Depends on**: Phase 6
**Requirements**: WORK-01, WORK-02, WORK-03, WORK-04, WORK-05, WORK-06, DISP-03, DISP-04, DISP-05, OPS-01, SEC-01, QA-03
**Success Criteria** (what must be TRUE):
  1. `ostro worker --join` connects over mTLS (cluster CA) and executes runs. Long journals are paged within gRPC limits, modules are fetched by sha256 with AOT cached locally, and secrets resolve per (run, attempt). The client listener serves TLS (self-signed in dev, provided certs or ACME in prod).
  2. Under a load spike an `auto` queue spills to workers, and returns to embedded after the cool-down without flapping. Label selectors route work. Killing a worker re-dispatches its runs within `lease_ttl`.
  3. Fencing chaos test: random executor, worker and node crashes run against a mock payment API across 10,000 runs. Result: exactly one call per idempotency key, with 0 duplicate charges and 0 lost effects.
  4. Per-queue concurrency, priority and rate limits apply. Keyed concurrency schedules fairly across keys. Throttle queues the excess while rate limit rejects it. Runs, workflows and queues can be paused and resumed, and a run auto-pauses when retries are exhausted.
  5. `ostro_queue_worker_demand{queue}` and the demand endpoint drive a KEDA ScaledObject so workers scale from 0 → N → 0.
**Plans**: 22 plans (8 waves; 07-01 is the PHASE-7 GATE requiring Phase 6 complete (all 06-*-SUMMARY + 06-VERIFICATION passed, or human_needed with by-design items only) and the Phase 5 pre-freeze findings F1–F4, F7 (ReadyValue.first) and F8 (ApiToken.expires_at_ms) in code; revised after the plan check (7 blockers, 14 warnings); runs in parallel with Phase 8 under the 07-DECISIONS shared-file rules: dispatcher.rs 07-05 vs 08-16 (Phase 7 owns the demand gauge value), server/config/lib.rs only 07-02, control/mod.rs 07-02 (field) + 07-14 vs 08-10, ostro-api/proto 07-11 vs 08-10, main.rs 07-15 vs 08-12/08-18, Cargo files only 07-02)

Plans:
- [ ] 07-01-PLAN.md — PHASE-7 GATE (Phase 6 complete, Phase 5 F1–F4/F7/F8 landed, code vs RESERVATIONS.md) + Phase 7 ordinals/names reserved + decisions D-W1..D-W25 and coordination rules to STATE (W1)
- [ ] 07-02-PLAN.md — Every Phase 7 dependency (no governor), ostro-worker-proto + ostro-worker skeleton, partition Placement/FlowControl/QueueGauges contracts, p7.rs seam (P7Shared with engine/bridge/join, attach_node, DispatcherHooks), ControlPlane p7 field, config sections, ErrorCode::RateLimited (W2)
- [ ] 07-03-PLAN.md — Format contract: Cmd 20 PauseRun / 21 ResumeRun, RejectReason 15 RunPaused, Action::RunPaused/RunResumed, apply routing stubs (OPS-01) (W2)
- [ ] 07-04-PLAN.md — Run pause/resume apply: enqueue_ready gate, RunPaused rejects, pause cleared on every terminal transition, sink RunPaused → Fenced, rebuild_digest gen_pause with RetryRun (OPS-01) (W3)
- [ ] 07-05-PLAN.md — Dispatcher generalisation: fair.rs, admission + reservation before grant (placement-blocked queues parked, others served), F7 first_activation, FlowPermit commit after Granted, Abandoned, pause actions, gauges (DISP-03/04/05, WORK-03) (W3)
- [ ] 07-06-PLAN.md — TLS foundation: cluster CA, node/worker/dev certs, reloadable resolver, listener modes off/auto/self-signed/files, ALPN incl. acme-tls/1 in tls-alpn mode (SEC-01) (W3)
- [ ] 07-07-PLAN.md — Worker crate: join with CA-hash pinning, mTLS session, re-join fallback, self-fence, drain, runner + RemoteSink + verified fetch + EffectProxy + AOT cache; guests long-journal/pay-flow/cpu-burn (WORK-01/02/06) (W3)
- [ ] 07-11-PLAN.md — API surface: Queues/Workers services, pause RPCs, REST paths, ControlPlane defaults, FakeControlPlane, parity, OpenAPI semantic-superset check (OPS-01, WORK-05, DISP-03) (W3)
- [ ] 07-08-PLAN.md — Node flow control: concurrency, hand-rolled GCRA throttle on first activations, StartRun rate limit (429), pause, weights, auto-pause (DISP-03/04/05, OPS-01) (W4)
- [ ] 07-09-PLAN.md — Worker service: join tokens with F8 expiry, mTLS cluster listener, sessions, registry, peer-identity-checked bulk RPCs, external-CA identity rule, revocation (WORK-01, SEC-01) (W4)
- [ ] 07-10-PLAN.md — ACME with instant-acme (TLS-ALPN-01, HTTP-01, renewal, cache) + env-gated Pebble (SEC-01) (W4)
- [ ] 07-15-PLAN.md — CLI: worker token/queue/run/workflow verbs, `ostro worker` subcommand with drain, TLS-aware ready line (WORK-01, OPS-01) (W4)
- [ ] 07-12-PLAN.md — RemoteRunner via EngineExecutor::sink_for + PlacementExecutor + NodePlacement (embedded-first overflow) + selectors; StartEffect verified, redacted values literal (WORK-01/02/04/06) (W5)
- [ ] 07-13-PLAN.md — Spill controller: sampler, embedded-first exit rule (no runs on workers, or empty backlog + cool signals), sustained overload = 1 transition (WORK-03) (W5)
- [ ] 07-14-PLAN.md — Control-plane implementation via the p7 field: flow config, queue/workflow/run pause+resume, /retry alias, worker admin, tokens, join, demand (OPS-01, WORK-01, WORK-05) (W5)
- [ ] 07-16-PLAN.md — Demand metric + endpoint (D-W12), KEDA/k8s manifests with offline checks, spawned harness with pinned ports + restart-resume smoke (WORK-05) (W6)
- [ ] 07-17-PLAN.md — SC1 e2e: join/pinning/revocation, paging, module cache, secrets never on workers, listener TLS modes, Pebble env-gated (WORK-01/02/06, SEC-01) (W7)
- [ ] 07-18-PLAN.md — SC2 e2e: auto spill/return, oscillation, sustained overload, selectors, kill -9 re-dispatch (log-timed), forced-late-append SIGSTOP zombie (WORK-03/04) (W7)
- [ ] 07-19-PLAN.md — SC4 e2e: concurrency, weights, priority, keyed fairness, throttle vs rate limit, pause/auto-pause, timeout while paused then /retry (DISP-03/04/05, OPS-01) (W7)
- [ ] 07-20-PLAN.md — SC5 e2e: KEDA metrics-api simulator drives real workers 0→N→0; real KEDA human_needed (WORK-05) (W7)
- [ ] 07-21-PLAN.md — SC3 fencing chaos: 10,000 runs, sharded/resumable, one charge per key, strict duplicate attribution, fenced_sends defined (QA-03) (W7)
- [ ] 07-22-PLAN.md — PHASE-7 GATE: full ci-local, CI sync + optional Pebble/KEDA dispatch jobs, RESERVATIONS flip, docs, VALIDATION + human_needed, STATE (W8)

### Phase 8: Triggers, Bottomless & Observability
**Goal**: Workflows start from webhooks and cron. A single node has real disaster recovery from object storage, and operators can trace and measure everything.
**Depends on**: Phase 5 (can run in parallel with Phases 6–7)
**Requirements**: TRIG-01, TRIG-02, TRIG-03, BACK-01, BACK-02, BACK-03, BACK-04, OBS-01, OBS-02, OBS-03
**Success Criteria** (what must be TRUE):
  1. Stripe webhook → workflow → signal round trip works. Signatures (Stripe, GitHub, Slack, generic HMAC) are verified, and a duplicate delivery with the same idempotency key starts only one run.
  2. Each `(schedule, fire_time)` of a timezone-aware cron schedule starts exactly once across restarts. Overlap policy, catch-up after downtime, pause and trigger-now behave as configured.
  3. `ostro restore --from <bucket>` on a fresh machine reproduces the state hash with RPO ≤ the segment seal interval (target ≤ 1 s). `ostro backup`/`restore` to a local file also round-trips. Only the leader uploads, and only sealed segments below HW.
  4. One documented snapshot format serves restore and future follower install. Journal trimming never removes entries that no snapshot covers.
  5. A run produces run → activation → step/effect spans linked to the caller's `traceparent` and propagated on outbound HTTP, and replayed spans are not re-emitted. `/metrics` exposes queue, lag, step, replay, fuel, memory and DLQ series. Guest logs are structured, rate-limited and bounded per run.
**Plans**: 22 plans (7 waves; 08-01 is the PHASE-8 GATE requiring Phase 5 complete (all 05-*-SUMMARY + 05-VERIFICATION passed, or human_needed with by-design items only; freeze + 05-07 Schedule fields/Webhook landed), checks code vs .planning/RESERVATIONS.md and allocates only from the Phase 8 block (Cmd 27 ScheduleFire, keyspace 16 schedule_state, InvalidCode 22/23); decisions P8-D1–D24 adopted in 08-DECISIONS.md, user may override; runs in parallel with Phases 6–7, shared files coordinated in 08-01; revised after plan check)

Plans:
- [ ] 08-01-PLAN.md — PHASE-8 GATE (Phase 5 complete, code vs RESERVATIONS.md) + decisions to STATE + contract (Cmd 27 ScheduleFire, keyspace 16 schedule_state, InvalidCode 22/23, config/trait stubs) + every Phase 8 dependency + crate skeletons (ostro-objstore only if absent) + anchored forbidden-crate check (all)
- [ ] 08-02-PLAN.md — OSNP v1 snapshot format: batch-consistent export, staged verified install (ingestion spike), blob-ref visitor, docs/snapshot-format.md (BACK-04)
- [ ] 08-03-PLAN.md — ostro-objstore frozen object-safe trait + LocalFs + S3 (rusty-s3) + conformance; s3s-fs/deny spike; MinIO CI leg (BACK-01)
- [ ] 08-04-PLAN.md — Telemetry foundation: Phase 8 config contract, OTLP traces/logs, Prometheus /metrics, JSON logs, OstroIdGenerator, services seam, testkit OTLP collector (OBS-01/02/03)
- [ ] 08-05-PLAN.md — Cron engine (TDD): croner behind jiff-0.2 adapter, shared TZif, DST vectors, latest-before + catch-up planning keeping newest fires (TRIG-02/03)
- [ ] 08-06-PLAN.md — Webhook verifiers (TDD): Stripe/GitHub/Slack/Standard/generic HMAC, idempotency extractors (TRIG-01)
- [ ] 08-07-PLAN.md — ScheduleFire apply: watermark exactly-once, overlap policies, buffered fire on terminal, rebuild digest (TRIG-02/03)
- [ ] 08-08-PLAN.md — Bottomless primitives: HwVector consistent cut (update-before-publish), raw envelope reads, blob put observer, node.toml incarnation (BACK-01)
- [ ] 08-09-PLAN.md — Snapshot service + WAL trimming enabled (dynamic floors, upload floor init), open-path snapshot install, SC4 proptest/failpoints (BACK-04)
- [ ] 08-10-PLAN.md — Phase 8 API surface: schedules/webhooks/hooks/run logs/admin backup over REST+gRPC+OpenAPI, traceparent in, server stubs (TRIG-01/02/03, OBS-01/03, BACK-03)
- [ ] 08-11-PLAN.md — Traces: deterministic ids, live-only activation/step spans, run span on RunCompleted, effect-span + traceparent hooks (OBS-01)
- [ ] 08-12-PLAN.md — CLI verbs: ostro schedules / ostro webhooks (TRIG-01/02/03)
- [ ] 08-13-PLAN.md — Bottomless uploader: OSEG chain with start rule, validated manifest + snapshot hold-back, LOCK fencing, immutable puts, upload floor, remote retention (BACK-01)
- [ ] 08-14-PLAN.md — Cron driver (owner partition) + schedules controller: catch-up, pause/resume, trigger-now (TRIG-02/03)
- [ ] 08-15-PLAN.md — Webhooks controller: POST /hooks/{name} start/signal/complete_token, dedupe, CRUD (TRIG-01)
- [ ] 08-16-PLAN.md — Metrics coverage: queues, lag, steps, replay, fuel, memory, DLQ, HW lag, ISR, Raft + docs/metrics.md (OBS-02)
- [ ] 08-17-PLAN.md — Guest logs: rate limits, per-run caps, ring buffer + /v1/runs/{id}/logs, JSON + OTLP LogRecords (OBS-03)
- [ ] 08-18-PLAN.md — Restore (manifest validation, two-step in-files verified replay, meta bootstrap spike, WAL import) + tar backup online/offline + CLI (BACK-02/03)
- [ ] 08-19-PLAN.md — Blob GC: mtime touch, GC lock, mark/sweep with grace, delayed remote deletion ledger (BACK-01)
- [ ] 08-20-PLAN.md — End-to-end proofs SC1–SC4: Stripe round trip, cron kill loop, restore RPO loop, MinIO leg, Phase 8 campaign faults (TRIG, BACK)
- [ ] 08-21-PLAN.md — SC5 observability e2e + outbound traceparent via ostro-bridge (B2) + OTLP canary (D-FX6) (OBS-01/02/03)
- [ ] 08-22-PLAN.md — PHASE GATE: full ci-local, >= 10,000-fault campaign, RESERVATIONS flip, docs, VALIDATION (all)
**Research flag**: Snapshot format (designed once for restore + follower install)

### Phase 9: Polyglot SDKs & Conformance
**Goal**: Python and TypeScript are first-class. Workflows in all three languages have identical semantics, proven by one language-neutral conformance suite.
**Depends on**: Phase 6 (effects available to SDKs; WIT frozen in Phase 5)
**Requirements**: SDK-02, SDK-03, SDK-04, SDK-05, WF-06, WF-10, WF-07 (Py/TS part)
**Success Criteria** (what must be TRUE):
  1. The conformance suite passes 100% for Rust, Python and TypeScript. It covers steps, retries, timers, signals, concurrency/race, cancellation, children, continue-as-new, await tokens, patching and non-determinism detection.
  2. Two runs of the same Python or TS workflow get different `ctx.random`, and replaying either reproduces its values. Rebuilding the same source yields the same module sha.
  3. A TS → Python → Rust child chain completes under crash injection. Cancelling the parent propagates to every descendant.
  4. A saga's registered compensations run in reverse order when a later step fails.
  5. Warm instantiation p50 is < 10 ms for Python and < 5 ms for TS. Resident memory per idle instance is published for each language.
**Plans**: 20 plans (10 waves; revised after the plan check (3 blockers, 9 warnings) and the WF-07 remap; 09-01 is the PHASE-9 GATE requiring Phase 6 complete (all 06-*-SUMMARY + 06-VERIFICATION passed, or human_needed with by-design items only), the Phase 5 freeze (wit/FROZEN.toml + local tag) and PF-F1/PF-F2 in code, and checks code vs .planning/RESERVATIONS.md (Phase 9 needs no ordinal; the P9 block stays reserved-unused); decisions D-P9-1..D-P9-27 adopted in 09-DECISIONS.md (D-P9-3 Python reproducible-build ladder flagged USER REVIEW), user may override; runs in parallel with Phases 7 and 8 under the 09-DECISIONS shared-file rules: Cargo files only 09-02, ci-local/ci.yml 09-02 (FAILPOINT_CRATES only) + 09-17, engine runtime.rs/lib.rs + server config.rs 09-07 (activation.rs never), server derive/sink 09-05/09-08, apply inbound/terminal/run/retention 09-05/09-08; polyglot legs behind the ostro-conformance `polyglot` feature; PyPI/npm publishing human_needed)

Plans:
- [ ] 09-01-PLAN.md — PHASE-9 GATE (Phase 6 complete, Phase 5 freeze, PF-F1/F2 in code, code vs RESERVATIONS.md) + Phase 9 names + decisions D-P9-1..27 and coordination rules to STATE (all) (W1)
- [ ] 09-02-PLAN.md — Contract + Wave 0 (Rust/build): every Phase 9 dep (wasmparser), ostro-conformance skeleton (spec model, inspector, polyglot build cache), scenario catalogue + schema (incl. C-STEP-04, C-NATIVE-01, C-CHILD-10/11), SDK WIT copies, pinned build-guest.sh + npm toolchain package, raw fixtures + polyglot smoke (all) (W2)
- [ ] 09-03-PLAN.md — Python/TS SDK skeletons with Host contracts and fake hosts (SDK-02/03) (W2)
- [ ] 09-04-PLAN.md — Spike S9: QJS vs SM vs Python-with-asyncio measurements, lowering coverage, result mapping, surrogates, export naming, loop prototypes, wrapper-only reproducibility (SDK-02/03/04) (W2)
- [ ] 09-05-PLAN.md — Children 1/2: derive StartChild (deterministic id, target resolution), no unsupported child path, repair rules that never re-decide delivered starts, child parent_event (none on CAN), parent apply journals ChildCompleted verbatim (WF-06) (W2)
- [ ] 09-06-PLAN.md — Rust SDK additions: start_child/ChildOptions, saga, random_f64, cross-SDK vectors + child/saga guests (WF-06, WF-10, SDK-04) (W2)
- [ ] 09-07-PLAN.md — Engine: componentize-py `exports` exact-shape allowance (ENG-1); core-instance count + weighted admission in Runtime::activate (describe/probe too), [engine] class + pool_total_core_instances (ENG-2) (SDK-02/03) (W3)
- [ ] 09-08-PLAN.md — Children 2/2: cancel-op → CancelRun, detached completion, parent-close cascade, resume-after-close re-send + re-completion (B3), row-guarded completions, CAN chain cleanup + deferred expiry, child:: failpoints (WF-06) (W3)
- [ ] 09-09-PLAN.md — Python SDK core: OstroLoop, exclusive steps (B1), timers raise in steps, D11 shielding, determinism surface, componentize-py export (SDK-02/04, WF-07) (W3)
- [ ] 09-10-PLAN.md — TS SDK core: SdkPromise + sync job loop, exclusive steps (B1), setTimeout mapping, bigint ops, shielding, determinism, UTF-8/sha256, jco export (SDK-03/04, WF-07) (W3)
- [ ] 09-11-PLAN.md — Conformance harness: canonical JSON, projection, comparator, engine-leg scheduler (scenario kind only), conf-rust(+v2), engine-leg specs, 100% Rust pass (SDK-05, WF-07) (W3)
- [ ] 09-12-PLAN.md — Python/TS effects builders + idempotency key + saga (vector-checked), first real-engine runs of SDK-built guests + fixes (WF-10, SDK-02/03/04) (W4)
- [ ] 09-13-PLAN.md — Server leg harness + Rust pass: children e2e incl. resume after close (C-CHILD-10) and cascade-vs-shielding (C-CHILD-11), API signals/tokens, CAN chain, HTTP effect (WF-06, SDK-05) (W4)
- [ ] 09-14-PLAN.md — Python conformance: engine + server legs 100% vs Rust, C-NATIVE-01 (WF-07) (SDK-05/04/02, WF-06/07/10) (W5)
- [ ] 09-15-PLAN.md — TS conformance: engine + server legs 100% vs Rust, C-NATIVE-01 (WF-07), StarlingMonkey smoke (SDK-05/04/03, WF-06/07/10) (W5)
- [ ] 09-16-PLAN.md — Cross-language: TS→Py→Rust chain (C-CHILD-08), HTTP saga in all languages (C-SAGA-03), value probes (C-VAL-01), full scenario × language table (SDK-05, WF-06, WF-10) (W6)
- [ ] 09-17-PLAN.md — Reproducible builds per D-P9-3 (wrapper → pinned fork → documented residual), repro-check + C-REBUILD-01, packaging dry runs, ci-local polyglot block + SHA-pinned ci.yml job (SDK-04, SDK-02/03) (W7)
- [ ] 09-18-PLAN.md — Crash leg SC3: TS→Python→Rust chain under kill -9 + child failpoints, cancel reaches every descendant, 100-iteration gate run (WF-06, SDK-05) (W8)
- [ ] 09-19-PLAN.md — SC5 measurements with real SDKs: warm instantiate, first call, start-to-first-step, idle RSS, mixed pool sizing; docs/sdk-resources.md (SDK-02/03) (W9)
- [ ] 09-20-PLAN.md — PHASE-9 GATE: full gate + evidence, SDK READMEs, docs/conformance.md + docs/children.md, RESERVATIONS flip, REQUIREMENTS ticks (WF-07 only with C-NATIVE-01 evidence), VALIDATION, DECISIONS fold-in, STATE (all) (W10)
**Research flag**: componentize-py/jco event-loop internals; QuickJS / `--script-only` maturity

### Phase 10: Developer CLI & Test Kits
**Goal**: A developer goes from an empty directory to a deployed, running workflow in any of the three languages with one toolchain-free CLI, and can test workflows locally with time skipping.
**Depends on**: Phase 9
**Requirements**: CLI-03, CLI-04, CLI-05, CLI-07, CLI-08, SDK-06, SDK-07
**Success Criteria** (what must be TRUE):
  1. In Rust, Python and TS, `ostro init → ostro deploy → ostro run` works on a clean machine with no pre-installed language toolchain (hermetic downloads or `--docker`) in under 10 minutes.
  2. `ostro deploy` builds when needed, validates imports, and uploads idempotently. `ostro dev --watch` rebuilds and redeploys on save.
  3. `ostro replay <run|--sample N> --module new.wasm` catches a deliberate non-deterministic change in each language and exits non-zero for use as a CI gate.
  4. Each language's test harness runs a workflow with a multi-day sleep against `ostro dev --ephemeral` in seconds using time skipping. TS, Python, Go and Rust client libraries can start, query and signal runs.
**Plans**: TBD

### Phase 11: Run Operations & Embedded UI
**Goal**: Operators triage and repair runs from the CLI/API or from an embedded UI that has real actions, within documented and enforced limits.
**Depends on**: Phase 8 (can run in parallel with Phases 9–10)
**Requirements**: OPS-02, OPS-03, OPS-04, OPS-05, OPS-07, UI-01, UI-02, UI-03
**Success Criteria** (what must be TRUE):
  1. A failed run lands in the DLQ with a reason and triggers the optional `on_failure` workflow. It can be requeued, or rerun from a chosen step: the journal prefix is copied into a new run that skips completed steps.
  2. Bulk cancel/retry/rerun over a tag or status filter runs spread over time without overloading the node. Tag search returns bounded, indexed results.
  3. Exceeding a published limit (payload size, history length, children per run) produces a clear, documented error rather than undefined behaviour.
  4. The embedded UI, served from the binary, shows the run list with filters/tags, a history timeline with replay markers and published status. It also shows modules, versions, queues, schedules, workers and partition health.
  5. From the UI an operator can cancel, terminate, retry, rerun-from-step, pause/resume, requeue from the DLQ, and run bulk operations.
**Plans**: TBD

### Phase 12: Replicated Partition Logs
**Goal**: Partition logs replicate across nodes with no acknowledged-write loss. Chronik's pure algorithms are ported behind the existing `ReplicatedLog`, and the conformance suite gates everything.
**Depends on**: Phase 8 (snapshot format), Phase 5 (`ReplicatedLog` at RF=1)
**Requirements**: CLUS-01, CLUS-02, CLUS-03, CLUS-04
**Success Criteria** (what must be TRUE):
  1. The replication conformance suite exists before integration and is shown to fail on a deliberately broken build. It covers placement, ISR honesty from measured LEOs, divergence/truncation, failover and min-insync.
  2. With RF=3, HW = min(LEO over ISR). Fetches with a stale epoch are rejected. After a leader dies holding unreplicated writes, its divergent tail is truncated on rejoin, including in object storage, and the suite stays 100% green.
  3. Leaders are elected only from the published ISR. Writes stop when the ISR drops below `min_insync_replicas`.
  4. Followers apply committed entries as hot standbys and bootstrap from a snapshot. Leader failover p99 is < 10 s and runs resume automatically. RF=1 still uses exactly the same code path.
**Plans**: TBD
**Research flag**: Snapshot install; failover liveness detection

### Phase 13: Cluster Membership, Routing & Upgrades
**Goal**: Operators form, grow and upgrade a 3/5-node cluster, or promote an existing single node, without draining runs. Any node serves any request.
**Depends on**: Phase 12
**Requirements**: CLUS-05, CLUS-06, CLUS-07, ENG-10, API-08
**Success Criteria** (what must be TRUE):
  1. `ostro cluster init` promotes a running single node to a cluster without export. `ostro cluster join/remove` changes membership of the multi-voter metadata Raft (3/5 nodes) safely.
  2. A request sent to any node is forwarded to the owning partition leader. A client always reads its own prior writes.
  3. A rolling upgrade across one minor version completes with runs in flight and none drained. The host links WIT 0.1.x and 0.2.x side by side, and workers receive only activations compatible with their advertised WIT/engine versions.
  4. Module GC never deletes a module referenced by a non-terminal run. `ostro migrate-runs` re-pins runs only after a passing dry-run replay.
**Plans**: TBD
**Research flag**: Raft transport on tonic 0.14

### Phase 14: Multi-Tenancy, Security & 1.0 Release
**Goal**: Ostro 1.0 ships. It is tenant-isolated, security-reviewed, proven under Jepsen-style faults and sustained load, and installable from signed artefacts, Helm and docs.
**Depends on**: Phase 13
**Requirements**: SEC-02, SEC-03, SEC-04, SEC-05, DISP-06, QA-04, QA-05, DIST-01, DIST-02, DIST-03
**Success Criteria** (what must be TRUE):
  1. Scoped tokens (`admin`, `deploy`, `operate`, `trigger`, `read`) and per-tenant tokens are enforced, and a tenant sees only its own resources. Per-tenant concurrency, fuel/s, memory and event-rate quotas are enforced with weighted fair dispatch. Optional signed-module enforcement rejects unsigned uploads. Deploys, activations, secret access, cancels/terminates and token changes appear in the audit log.
  2. Fuzzing (wasm-smith components, malformed WIT values) runs continuously. The external security review closes with no open critical/high findings.
  3. A Jepsen-style run on 3 nodes (partitions, clock skew, `kill -9`, disk stalls) shows 0 lost acknowledged runs, 0 duplicate journal entries, and fencing holding. A 7-day soak at 70% rated load is stable.
  4. Benchmarks verify acknowledged work and bytes on disk: single node ≥ 10,000 steps/s, and 3-node RF=3 ≥ 5,000 steps/s with p99 commit < 20 ms on LAN.
  5. Users can install from static linux x86_64/aarch64 binaries, signed and notarized macOS binaries, a `FROM scratch` image, the install script, or the Helm chart (with KEDA examples and NetworkPolicies). Docs cover getting started per language, determinism, connectors, operations/sizing, and migrations from cron/queue tables, Temporal and Restate.
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → ... → 14. Allowed parallelism:
- 2 ∥ 3
- 8 alongside 6–7
- 11 alongside 9–10

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Foundation, Vendoring & Spikes | 8/8 | Complete (FOUND-05 remote CI legs human_needed after first push) | 2026-11-18 |
| 2. Durable Partition Log & State Machine | 16/16 | Complete | 2026-11-19 |
| 3. Deterministic Wasm Engine & WIT 0.1 | 12/12 | Complete | 2026-11-18 |
| 4. Single-Node Server & Rust MVP | 20/20 | Complete | 2026-11-19 |
| 5. Workflow Semantics & Core Hardening | 1/20 | In progress (05-01 gate + contract done) | - |
| 6. Effects Bridge, Connectors & Secrets | 0/TBD | Not started | - |
| 7. Workers, Placement & Flow Control | 0/TBD | Not started | - |
| 8. Triggers, Bottomless & Observability | 0/TBD | Not started | - |
| 9. Polyglot SDKs & Conformance | 0/TBD | Not started | - |
| 10. Developer CLI & Test Kits | 0/TBD | Not started | - |
| 11. Run Operations & Embedded UI | 0/TBD | Not started | - |
| 12. Replicated Partition Logs | 0/TBD | Not started | - |
| 13. Cluster Membership, Routing & Upgrades | 0/TBD | Not started | - |
| 14. Multi-Tenancy, Security & 1.0 Release | 0/TBD | Not started | - |

---
*Roadmap created: 2026-11-18*
