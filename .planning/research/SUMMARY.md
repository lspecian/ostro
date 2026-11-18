# Project Research Summary

**Project:** Ostro | **Domain:** self-hosted single-binary polyglot Wasm durable-execution engine (Rust) | **Researched:** 2026-11-18 | **Confidence:** MEDIUM-HIGH

Sources: STACK.md, FEATURES.md, ARCHITECTURE.md, PITFALLS.md (this directory). **Where research and `docs/01–05` disagree, research wins.**

## Executive Summary

Ostro is a log-first durable-execution engine in the Restate/Temporal family. Each partition has a single-writer leader and a WAL. A deterministic state machine on redb derives state from that log. Wasm components run under Wasmtime and recover by deterministic replay.

**The moat is verified:** no competitor offers a self-hosted single binary that hosts your uploaded code.
- Restate is BSL and runs code in your services.
- Inngest is SSPL.
- Hatchet, Windmill and DBOS need Postgres.
- Temporal needs worker fleets.

Content-addressed module storage gives version pinning almost for free. The docs/03 shape is right, but research found structural gaps and over-optimistic Chronik-reuse claims.

## Where research CHANGES docs/01–05

1. **Replication: reject Option A** (reuse Chronik's Kafka-Fetch fetcher). It would embed a Kafka broker's fetch/produce path: about 9k lines, plus `fetch_handler.rs` at 3.7k and `produce_handler.rs` at 6.1k. It also cannot be the RF=1 path, so it would recreate the two-path bug class.
   - **Instead:** define an Ostro-owned `ReplicatedLog` trait in Phase 1 (HW, epoch cache, ISR={self}).
   - In Phase 4, port Chronik's pure algorithms behind it: epoch cache, batch planning, ISR math, failover planner. Add a small tonic `Fetch`. About 4–6 weeks inside Phase 4.
2. **Phase 1 structural additions**
   - `LeaderAnnounced{epoch}` as the first entry of every leadership term, with `(epoch, attempt)` fencing tokens.
   - Cross-partition outbox, an `inbound_seen` dedupe and a shuffler. Children, signals, cron and cancel cross partitions even on one node with P=16.
   - Validation at **apply** time, with `AppendAck` after apply. **Apply ≤ HW.**
   - Lazy windowed timers with idempotent `TimerFired`.
   - Trim and snapshot coordination: the trim rule in Phase 2 and snapshot install in Phase 4, with the snapshot format designed once.
   - The batch envelope `{epoch, codec_ver, crc32c, cmds}` from day one.
3. **WIT 0.1 additions before freeze**
   - Guest-visible cancellation (cancel vs terminate).
   - A first-class `continue-as-new` op.
   - Callback/await tokens.
   - `set-status` (published status instead of query handlers).
   - Run timeouts.
   - A `NonRetryable` error.

   Keep the interface **synchronous** (WASI-0.2 style, no `wasi:*` imports). WASI 0.3 was ratified on 2026-06-11, but guest toolchains aren't ready: Rust `wasm32-wasip3` is Tier 3 and jco's p3 support is experimental.
4. **Stack corrections**
   - Wasmtime 50.0.2. `async_support` was removed in 42, so docs/01 is stale. Ship 1.0 on the 60 LTS (about Aug 2027).
   - Rust **1.100** toolchain. Wasmtime 50 needs 1.97 or newer; the local machine has 1.94.1.
   - redb **4.6** with `Durability::None` plus periodic `Immediate`. Not 5.0, which changes the on-disk encoding.
   - tonic/prost **0.14** and axum **0.8**, on one port via `Routes::into_axum_router()`. Use `protox` instead of `protoc`.
   - The `object_store` 0.14 crate instead of extracting Chronik's object-store code.
   - jco 1.42 bundles TS, so drop esbuild. Drop `cargo-component` (stale) in favour of `cargo build --target wasm32-wasip2` with wit-bindgen 0.63.
   - cargo-zigbuild plus **mimalloc** for musl builds; rustls with aws-lc-rs only; the `metrics` facade; `postcard` for new formats (bincode is archived).
5. **WAL format:** use V2 only, with Ostro's envelope in `canonical_data`. Chronik's `verify_checksum()` does nothing for V2, so **Ostro must verify CRCs on every read**. After a restart the WAL does not know where the log ends, so scan for it. Disable io_uring (Chronik measured it 33% slower).
6. **Features to add** (PRD gaps)
   - Keyed concurrency and fairness: store the key in ready-queue entries from Phase 1; the API ships in Phase 2.
   - Rerun-from-step, which needs a self-contained journal format.
   - Pause/resume, including pausing automatically when retries run out.
   - Bulk operations, spread over time to limit load.
   - A DLQ definition with an `on_failure` hook.
   - Run tags with bounded indexed search.
   - Cron overlap, catch-up and trigger-now.
   - An idempotent-start conflict policy.
   - Run timeouts and schedule-to-start timeouts.
   - Published limits.
   - UI with actions, not just views.
   - `--json`, `--dry-run` and OpenAPI output.
7. **Pitfall-driven requirements**
   - **Idempotency window:** journal when each effect was scheduled. Past the provider's key-retention window, return `effect_outcome_unknown` instead of resending.
   - **Secrets:** grants bound to destination origins, no auto-redirect for templated requests, and response scrubbing.
   - **SSRF bypass table:**
     - IPv4-mapped IPv6 and NAT64 addresses
     - decimal/octal IP literals
     - `fd00:ec2::254`
     - redirects
     - env-var proxies
     - The fix: resolve once, pin the address, use `.no_proxy()`.
   - **fsync and write errors are fatal for the partition.** Add EIO/ENOSPC failpoints, not just `kill -9`.
   - **Replay corpus in CI from Phase 1,** run across Ostro versions and across x86_64 and aarch64.
   - **Snapshot-reseed conformance test (Python/JS):** two runs get different `ctx.random`, and the same run replays identically. Build with `PYTHONHASHSEED=0` and verify reproducible shas.
   - **Fencing chaos test:** the external mock sees exactly one call per idempotency key.

## Chronik code: DECIDED. Vendor into Ostro (user decision, 2026-11-18)

The needed Chronik code is copied into the Ostro workspace with Apache-2.0 attribution and the source commit (`08715f4867c9f40e409c68d8e212bd9479d620c0`), then modernised there. The Chronik repo is **not** modified.

| Vendored | Modernisation in Ostro |
|---|---|
| `chronik-raft` `driver.rs`, `log_store.rs` (+ the `StateMachine`/`Outbox` traits) and the vendored `raft`/`raft-proto` crates | Drop the unused `chronik-common`/`chronik-monitoring` deps. Rename the vendored raft crates so no `[patch]` is needed. Write Ostro's own `Outbox` transport (~300–400 LOC) on tonic 0.14 |
| `chronik-wal` group commit, segment, segment index, truncate (`plan_segment_truncation`) | Replace `MetricsRecorder` with the `metrics` facade. Remove `tokio-uring`. Use V2 only plus the Ostro envelope. Verify CRCs on read |
| Replication algorithms: `LeaderEpochCache`/`LeaderEpochStore`, `plan_batches` pattern, ISR tracker math, watermark acks, `plan_failover`, `FollowerState` shape | Port in Phase 4 behind `ReplicatedLog`. Drop the "unknown = full replica set" fallback. Validate the epoch on every fetch |
| Object store | **Not vendored.** Use the `object_store` crate behind a ~200 LOC adapter |

Keep a `VENDORED.md` with the source path, commit and local modifications for each file.

## Key Findings

**Stack** (verified 2026-11-18; see STACK.md):
- **Guests.** Python (componentize-py 0.25.1, `--stub-wasi`) is 18.4 MB `.wasm` and 32.6 MB AOT, with about 460 MB peak RSS while compiling. TS (jco 1.42, `--disable all`) is 12 MB with zero WASI imports; the QuickJS backend is 1.47 MB, and `--script-only` is about 1.5 KB per script on a shared engine.
- **Wasmtime config.** NaN canonicalisation, deterministic relaxed SIMD, threads and GC off, `default-features = false`.
- **AOT cache.** Keyed by `sha256 + precompile_compatibility_hash + Ostro version`, compiled in a bounded pool.
- **Duplicate versions.** cargo-deny bans duplicate tonic, prost, hyper, axum and opentelemetry.

**Features** (see FEATURES.md for T1–T32 and D1–D12):
- **Table stakes:** as listed above.
- **Differentiators:**
  - server-hosted Wasm
  - version pinning with a replay gate
  - sandbox-enforced determinism
  - Apache-2.0 throughout
  - built-in auth (self-hosted Restate has none)
  - exactly-once SQL
- **Defer to 1.x:** debounce/singleton, streaming output, updates, query handlers, event bus, saga helper, MCP, SSO.
- **Defer to 2.0+:** virtual objects, native workers, batching, multi-region.
- **Anti-features:** raw wasi:http, heartbeats, a visual designer, memory snapshots.

**Architecture** (see ARCHITECTURE.md):
- **Per partition:** Sequencer → WAL group commit → `ReplicatedLog` (HW) → pure `apply_batch` into redb, which returns `Action`s.
- **Leader-only services:** Dispatcher, TimerService, Shuffler, Retention.
- **Metadata:** a small Raft group on the vendored driver with Ostro's transport.
- **Crates:** ostro-types, ostro-log, ostro-partition, ostro-meta, ostro-engine, ostro-bridge, ostro-proto, ostro-api, ostro-blob, ostro, ostro-cli. `ReplicatedLog` is the crate boundary between ostro-log and ostro-partition.
- **Uploads:** only from the leader, only below HW, keyed by `(partition, epoch, base_offset)`.

**Top pitfalls** (20 in PITFALLS.md):
- host-side non-determinism after upgrades
- a second replication code path, plus Chronik's RP-3/5/7/9 lessons: versioned metadata records, `log_end ≥ HW`, epoch validation on every fetch, liveness detection
- state store running ahead of the durable log, and fsyncgate
- zombie executors and the idempotency window
- secret exfiltration and SSRF bypass
- randomness baked in at pre-init, and event-loop determinism
- a fiber-stack overflow aborting the node
- pooling allocator exhausting address space
- quadratic replay cost
- GC races (Chronik deleted live WALs 2 ms after recovery)
- AOT compile storms

## Implications for Roadmap

Keep docs/05's structure and add a Phase 0 gate. The project uses **fine** granularity, so split each doc phase into 2–3 GSD phases.

- **Phase 0: spikes and toolchain (2–3 weeks)**
  - Vendor the Chronik code and record it in `VENDORED.md`.
  - S2: measure the pinned guest toolchains, test pre-init randomness, and decide `resource step` vs a `u64` handle.
  - S3: measure redb durability-tier throughput, with fjall as the fallback.
  - Bootstrap the toolchain: Rust 1.100, cargo-deny bans, a musl + mimalloc throughput gate, and a macOS notarization smoke test.
- **Phase 1: single-node core, WIT 0.1, Rust MVP (~14 weeks realistic)**
  - Build order:
    1. ostro-types envelope
    2. ostro-log: RF=1 `ReplicatedLog`, CRC-verified reads, log-end scan, epoch cache
    3. ostro-partition: apply validation, `LeaderAnnounced`, outbox and shuffler, apply ≤ HW, rebuild-hash test
    4. lazy timers and a dispatcher that carries the concurrency key
    5. ostro-engine, built in parallel against a fake `ActivationSink`
    6. ostro-meta (single voter), the API, the binary, and Rust `ostro build`
    7. the crash/failpoint harness and the replay corpus
  - Added exit criteria:
    - 10k `kill -9` plus EIO failpoints with 0 lost runs
    - a max-depth guest plus a host call does not abort the node
    - the replay corpus is green
- **Phase 2: I/O bridge, workers, bottomless, flow control (~10–12 weeks)**
  - HTTP/SQL effects with the idempotency window, and origin-bound secrets.
  - The SSRF guard with its bypass table.
  - Workers with paged journals (tonic's 4 MiB limit) and self-fencing.
  - Bottomless storage on `object_store`, the trim rule, and backup/restore.
  - Keyed concurrency, throttle and quotas; queue pause; cron overlap and catch-up.
  - Webhooks, the idempotent-start policy, and a bounded compile pool.
- **Phase 3: polyglot SDKs, CLI, UI, ops features (~12–14 weeks)**
  - Python and TS SDKs, with the SDK owning the event loop and reseeding.
  - Hermetic CLI using uv and pinned jco.
  - Three-SDK conformance including the reseed test, and a stratified `ostro replay`.
  - Rerun-from-step, bulk ops, the DLQ with `on_failure`, tags and search.
  - UI with actions, time-skipping test kits, published limits, and `--json`/OpenAPI.
- **Phase 4: cluster and 1.0 (~12 weeks)**
  - Week 1: the replication conformance suite, with each test proven to fail on a broken build.
  - Option B Fetch and ISR/HW, epoch truncation, snapshot install.
  - Multi-voter metadata.
  - Rolling upgrades with N/N-1 linkers, module GC refcounted against runs, and `migrate-runs` with a dry-run.
  - K8s/KEDA, mTLS, tenants and fair dispatch, and the security review.

**Ordering rationale:**
- Anything that fixes the on-disk format or the command set lands in Phase 1.
- The engine is built in parallel with storage.
- The bridge needs fencing tokens, so it comes before the SDKs.
- The cluster is additive behind `ReplicatedLog`.
- The snapshot format is designed once, in Phase 2.

**Research flags (`/gsd:research-phase`):**
- **Phase 0:** guest toolchain behaviour under `--stub-wasi` (measure it).
- **Phase 1:** redb durability tiers, the WAL V2 recovery paths, and Wasmtime pooling and fiber-stack sizing.
- **Phase 2:** SSRF and secret binding, the MySQL implicit-commit list, the snapshot format, and provider idempotency windows.
- **Phase 3:** componentize event-loop internals, the maturity of QuickJS and `--script-only`, and WASI 0.3 guest migration.
- **Phase 4:** the Raft transport, snapshot install, and failover liveness.
- **Standard patterns (skip research):** API/CLI plumbing, cron/webhooks/observability, and UI/docs.

## Confidence

| Area | Confidence | Notes |
|---|---|---|
| Stack | HIGH | Registry-verified on 2026-11-18. Guest toolchain figures are hello-world only (MEDIUM) |
| Features | MEDIUM-HIGH | Licences, release notes and docs verified; some matrix cells are training data |
| Architecture | HIGH on Chronik, MEDIUM on prior art | Chronik source read at `08715f4`; Restate and Temporal from their docs |
| Pitfalls | MEDIUM | Chronik, fsyncgate and Wasmtime docs HIGH; Python/JS production figures and macOS entitlements LOW |

**Open gaps:**
- Whether componentize-py/js reseed after pre-init.
- Python per-instance memory measured against NFR-2, which decides whether Python defaults to workers.
- The macOS notarization entitlement for Cranelift.
- Whether any WAL V2 recovery path decodes `canonical_data` as Kafka.
- Wasmtime default stack and pool sizes on 50.0.2.
- Whether to pin Wasmtime 48 LTS during pre-release or track monthly releases up to 60.
- The MVP is larger than docs/02, so durations need re-estimating. Streaming and debounce/singleton are the likeliest cuts.
- The fixed partition count (P=16 single node, 64 cluster) must be decided before Phase 1 ships.
- Confirm the Apache-2.0 licence.
