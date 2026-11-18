---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: executing
stopped_at: Completed 01-08-PLAN.md
last_updated: "2026-11-18T18:00:00.000Z"
last_activity: "2026-11-18 — Completed 01-08-PLAN.md (quiet-machine S2/S3 confirmation; decisions: resource, fjall, Wasmtime pin policy)"
progress:
  total_phases: 14
  completed_phases: 0
  total_plans: 8
  completed_plans: 8
  percent: 100
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-11-18)

**Core value:** Download one binary, deploy a Wasm workflow, and it runs durably. Accepted runs are never lost, survive crashes, and replay deterministically, with nothing else to install or operate.
**Current focus:** Phase 1: Foundation, Vendoring & Spikes

## Current Position

Phase: 1 of 14 (Foundation, Vendoring & Spikes)
Plan: 8 of 8 in current phase (01-01 to 01-08 complete)
Status: Plans complete; phase verification pending (FOUND-05 remote CI human_needed)
Last activity: 2026-11-18 — Completed 01-08-PLAN.md (quiet-machine confirmation; fjall crash test 5/5 + RMW 106.9k cmds/s; three Phase 1 decisions logged in PROJECT.md; FOUND-06 complete)

Progress: [██████████] 100%

## Performance Metrics

**Velocity:**
- Total plans completed: 0
- Average duration: -
- Total execution time: 0.0 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| - | - | - | - |

**Recent Trend:**
- Last 5 plans: -
- Trend: -

*Updated after each plan completion*
| Phase 01 P01 | 4min | 3 tasks | 37 files |
| Phase 01 P03 | 12min | 2 tasks | 12 files |
| Phase 01 P02 | 22min | 2 tasks | 10 files |
| Phase 01 P04 | 8min | 3 tasks | 41 files |
| Phase 01 P05 | 25min | 2 tasks | 9 files |
| Phase 01 P06 | 8min | 2 tasks | 21 files |
| Phase 01 P07 | 15min | 2 tasks | 3 files |
| Phase 01 P08 | 14min | 2 tasks | 11 files |

## Accumulated Context

### Decisions

Decisions are logged in the PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- [Roadmap]: Phases 2 (storage) and 3 (engine) can run in parallel after Phase 1. The engine is built against a fake `ActivationSink`.
- [Roadmap]: The on-disk format, command set and WIT 0.1 are frozen at the end of Phase 5. Changes after that are additive only.
- [Roadmap]: The snapshot format is designed once, in Phase 8, and serves both restore and follower install (Phase 12).
- [Roadmap]: Cluster work (Phases 12–13) is added behind `ReplicatedLog`. The replication conformance suite is written and proven failing before integration.
- [Research 2/3]: `ostro-types` ownership split. Phase 2 owns the log envelope (32-byte LE header + CRC32C), partition `Command` framing, fencing (`LeaderAnnounced`, `AttemptGranted`) and routing. Phase 3 owns `journal::Event` payload semantics, the per-kind command-hash spec and `RunStarted.rng_seed`. Whichever phase executes first creates the crate; the other phase extends it.
- [Research 3]: Wasmtime host imports are async (`imports: { default: async | trappable }`) with sync WIT, `async_stack_size` = 4 MiB, and an Ostro-owned deterministic WASI 0.2 shim (Rust wasip2 guests import 16 WASI 0.2.12 interfaces per S2 measurement). `wasmtime-wasi` is not used.
- [Roadmap]: Rust `ostro build` exists only as a direct cargo build until Phase 10. CLI-04 (hermetic three-language build) is mapped to Phase 10.
- [Phase 01]: rustls kept as a normal dep of ostro-api (pub tls_provider) so aws-lc-rs stays visible to cargo-deny with exclude-dev
- [Phase 01]: Unused workspace-dependency warnings (metrics, thiserror, tracing, prost-build) accepted until plans 01-04/01-06 consume them
- [Phase 01]: S2 harness defines unknown imports as traps (no wasmtime-wasi); WASI-touching probes report traps as findings
- [Phase 01]: S2 smoke: TS completed resource steps are never dropped without explicit Symbol.dispose; Rust std HashMap traps without a WASI random shim
- [Phase 01]: S3: redb 4.6 FAILS the 90k cmds/s rule (T2 median 62,404 cmds/s; T3 no-fsync only 90.6k, so CPU-bound). fjall 3.1.13 T2 analogue reaches 242k cmds/s. Recommend fjall for STOR-05 pending fjall crash test + read-path bench and user confirmation
- [Phase 01]: S3: redb T2 crash sanity 5/5. After kill -9 the db reopens at exactly the last durable checkpoint; None commits are discarded, so recovery replays the WAL from checkpointed_offset
- [Phase 01]: Vendored crates: empty [lints.rust] table, rustfmt disable_all_formatting, crate-level #![allow(clippy::all)] (upstream deny(clippy::all) broke workspace clippy)
- [Phase 01]: raft failpoints cfg blocks deleted (no fail dep); ostro-meta reaches raft via ostro_raft_driver::raft re-export
- [Phase 01]: S2: step handle Recommendation resource (round-trips py/ts/rust; HostStep::drop observable only with SDK RAII/with/using; engine must not rely on drop)
- [Phase 01]: S2: NFR-2.3 met (instantiate p50 py ~450 us, ts/rust 10-20 us); Python 6.5 MB RSS/instance -> recommend Python embedded by default under NFR-1.3 budgets, not workers
- [Phase 01]: S2: componentize-py/js do NOT reseed after pre-init; host-driven reseed works in all three; Rust wasip2 imports 16 wasi:*@0.2.12 interfaces (not 17)
- [Phase 01]: S2: Python component = 38 core instances; wasmtime default pool fits only 26 live Python instances (ENG-08); only Rust builds reproducible
- [Phase 01]: ostro-wal: CHRONIK_* env knobs replaced by config (new GroupCommitConfig::tail_cache_bytes; Default = low_resource; 250MB rotation const); io_uring/AsyncIoConfig/bincode removed; metrics facade ostro_wal_batches_total + ostro_wal_batch_records
- [Phase 01]: ostro-wal is_raft_topic backpressure exemption uses reserved prefix `__ostro_` (was __chronik_metadata); V2 WalRecord::verify_checksum is a no-op, so the Phase 2 envelope must carry its own CRC32C
- [Phase 01]: scripts/check-vendored.sh is the provenance gate (needs jq); 01-07 CI should run it
- [Phase 01]: CI: zig 0.17.1 + cargo-zigbuild 0.24.1 build a static x86_64 musl ostro (mimalloc, aws-lc-rs) with no fallback needed; test runner is `cargo-zigbuild test` (not `cargo zigbuild test`)
- [Phase 01]: Unused workspace pins removed (raft, raft-proto, metrics, tracing, thiserror, prost-build); vendored crates pin their own deps; build is now warning-free and ci-local.sh enforces it
- [Phase 01]: FOUND-05 PARTIAL until the user pushes and the Actions run (lint, x86_64/aarch64 musl, macos-aarch64, x86_64-gnu) is green
- [Phase 01 / 01-08]: DECIDED Step handle = `resource step` (S2). The engine must not rely on `HostStep::drop` (TS never drops completed steps); SDKs wrap steps in with/using/RAII. See docs/spikes/S2-guest-toolchains.md
- [Phase 01 / 01-08]: DECIDED State store = fjall 3.1.13 (redb vs fjall → fjall). Quiet re-run: redb T2 67.5k plain / 30.9k RMW (FAIL); fjall T2 233.8k plain / 106.9k RMW (PASS); fjall kill -9 crash 5/5. Design: WriteBatch per apply batch + applied_offset, buffered commit, persist(SyncAll) every 100 ms/256 batches; recovery replays the WAL from the state's applied_offset (fjall keeps OS-buffered commits after a process kill, so applied may be > checkpointed). See docs/spikes/S3-redb-durability.md
- [Phase 01 / 01-08]: Phase 2 research items tagged [S3] (durability tiers, Durability::None + Immediate checkpoint, 2PC/quick-repair, repair time) now apply to fjall: PersistMode/persist cadence, journal-replay reopen time, memtable/journal sizing, flush/compaction workers. STOR-05's "redb" wording is superseded by the fjall decision
- [Phase 01 / 01-08]: DECIDED Wasmtime pin policy = track latest major monthly, pinned exactly (=50.0.2 now; 51 due ~2026-12-20), one bump PR per release with replay corpus green; ship 1.0 on 60 LTS (~Aug 2027); fallback freeze on 48 LTS if bumps cost > ~1 day each; bump rust-toolchain.toml with Wasmtime's MSRV
- [Phase 01 / 01-08]: Python embedded by default (6.5 MB RSS/instance, NFR-2.3 met with >20x margin, confirmed on a quiet machine); pools sized per language class (Python = 38 core instances/instance; default pool fits 26 Python instances), workers remain the scaling path. Revisit memory in Phase 9 with a realistic stdlib footprint

### Pending Todos

None yet.

### Blockers/Concerns

Open:
- macOS notarization entitlement for Cranelift (`allow-jit`): a **Phase 14** release-signing concern; unsigned test binaries JIT fine. The macOS CI leg itself awaits the user's first push (human_needed, see FOUND-05).
- FOUND-05 remote CI legs (aarch64 musl, macOS, the Actions run itself) are human_needed: no git remote exists yet.
- Wasmtime 50.0.2 default stack and pool sizes. (Phase 3; S2 input: Python needs 38 core instances per instance, `max_memory_size` floor 13.2 MB Python / 9.5 MB TS / 1.1 MB Rust)
- Fixed partition count (P=16 single node / 64 cluster) must be confirmed before Phase 5 freezes the format.
- The MVP is larger than docs/02. Streaming and debounce/singleton are already v2. The saga helper (WF-10) is a candidate cut if scope pressure appears.
- NEW (S3, **Phase 2**): fjall reopen replays the un-flushed journal (3.2–3.4 s for ~540k commands). Bound journal/memtable size and measure reopen time in the recovery test.
- NEW (S3, **Phase 2**): fjall RMW runs show one ~120 ms write stall per 200k commands (p99 < 0.85 ms); tune memtable/flush workers. RMW margin over the 90k rule is only 1.19x, so keep the apply path to one point read per command plus an in-batch overlay.
- NEW (S3, **Phase 2**): recovery must replay the WAL from the state's own `applied_offset` (not `checkpointed_offset`); apply only WAL-durable commands. Add fjall's dependency tree (lsm-tree, lz4) to the cargo-deny/licence check when it enters the workspace.
- NEW (S2, **Phase 3/4**, ENG-02): Rust wasm32-wasip2 guests import 16 `wasi:*@0.2.12` interfaces even without WASI calls; the deterministic WASI shim must cover them (std `HashMap` needs random).
- NEW (S2, **Phase 10**, CLI-04): Python and TS builds are not reproducible (pre-init snapshot data differs; Python also shuffles shim-module order). Needs deterministic pre-init or a canonicaliser.
- NEW (S2, **Phase 4**): Python/TS AOT compile costs ~1 s and ~0.5 GB peak RSS each; bound concurrent compiles.

Resolved in Phase 1:
- ~~Do componentize-py/js reseed randomness after pre-init?~~ **Resolved** (S2, 01-05): no. Host-driven reseed works in all three languages; SDKs must reseed/patch from ctx. See docs/spikes/S2-guest-toolchains.md#determinism-probes
- ~~Python per-instance memory vs NFR-2.~~ **Resolved** (S2, 01-05/01-08): ~6.5 MB RSS per live instance (TS ~2.5 MB, Rust ~40 KB); recommendation adopted: Python runs embedded by default under NFR-1.3 budgets, workers are the scaling path. See docs/spikes/S2-guest-toolchains.md#memory-per-instance
- ~~Does any WAL V2 recovery path decode `canonical_data` as Kafka?~~ **Resolved** (01-06): no. The only decoder (`extract_offset_range`) had zero callers and was deleted.
- ~~Pin policy: Wasmtime 48 LTS vs tracking monthly releases to 60 LTS.~~ **Resolved** (01-08): track monthly, pinned exactly, ship 1.0 on 60 LTS; 48 LTS fallback. See PROJECT.md Key Decisions and 01-RESEARCH.md#decisions-to-log-in-projectmd-found-06
- ~~Storage engine choice for STOR-05 (redb failed the S3 rule).~~ **Resolved** (01-08): fjall 3.1.13; both preconditions (fjall crash test, RMW benchmark) met. See docs/spikes/S3-redb-durability.md#confirmation-run-quiet-machine-plan-01-08

## Session Continuity

Last session: 2026-11-18T18:00:00.000Z
Stopped at: Completed 01-08-PLAN.md
Resume file: None
