---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: executing
stopped_at: Completed 01-02-PLAN.md
last_updated: "2026-11-18T17:28:09.076Z"
last_activity: "2026-11-18 — Completed 01-02-PLAN.md (S3 redb durability spike: redb FAILS 90k rule; fjall recommended)"
progress:
  total_phases: 14
  completed_phases: 0
  total_plans: 8
  completed_plans: 3
  percent: 38
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-11-18)

**Core value:** Download one binary, deploy a Wasm workflow, and it runs durably. Accepted runs are never lost, survive crashes, and replay deterministically, with nothing else to install or operate.
**Current focus:** Phase 1: Foundation, Vendoring & Spikes

## Current Position

Phase: 1 of 14 (Foundation, Vendoring & Spikes)
Plan: 3 of 8 in current phase (01-01, 01-02, 01-03 complete)
Status: In progress
Last activity: 2026-11-18 — Completed 01-02-PLAN.md (S3 redb durability spike: redb FAILS 90k rule at 62k cmds/s; fjall 242k)

Progress: [████░░░░░░] 38%

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

## Accumulated Context

### Decisions

Decisions are logged in the PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- [Roadmap]: Phases 2 (storage) and 3 (engine) can run in parallel after Phase 1. The engine is built against a fake `ActivationSink`.
- [Roadmap]: The on-disk format, command set and WIT 0.1 are frozen at the end of Phase 5. Changes after that are additive only.
- [Roadmap]: The snapshot format is designed once, in Phase 8, and serves both restore and follower install (Phase 12).
- [Roadmap]: Cluster work (Phases 12–13) is added behind `ReplicatedLog`. The replication conformance suite is written and proven failing before integration.
- [Research 2/3]: `ostro-types` ownership split. Phase 2 owns the log envelope (32-byte LE header + CRC32C), partition `Command` framing, fencing (`LeaderAnnounced`, `AttemptGranted`) and routing. Phase 3 owns `journal::Event` payload semantics, the per-kind command-hash spec and `RunStarted.rng_seed`. Whichever phase executes first creates the crate; the other phase extends it.
- [Research 3]: Wasmtime host imports are async (`imports: { default: async | trappable }`) with sync WIT, `async_stack_size` = 4 MiB, and an Ostro-owned deterministic WASI 0.2 shim (Rust wasip2 guests always import 17 WASI interfaces). `wasmtime-wasi` is not used.
- [Roadmap]: Rust `ostro build` exists only as a direct cargo build until Phase 10. CLI-04 (hermetic three-language build) is mapped to Phase 10.
- [Phase 01]: rustls kept as a normal dep of ostro-api (pub tls_provider) so aws-lc-rs stays visible to cargo-deny with exclude-dev
- [Phase 01]: Unused workspace-dependency warnings (metrics, thiserror, tracing, prost-build) accepted until plans 01-04/01-06 consume them
- [Phase 01]: S2 harness defines unknown imports as traps (no wasmtime-wasi); WASI-touching probes report traps as findings
- [Phase 01]: S2 smoke: TS completed resource steps are never dropped without explicit Symbol.dispose; Rust std HashMap traps without a WASI random shim
- [Phase 01]: S3: redb 4.6 FAILS the 90k cmds/s rule (T2 median 62,404 cmds/s; T3 no-fsync only 90.6k, so CPU-bound). fjall 3.1.13 T2 analogue reaches 242k cmds/s. Recommend fjall for STOR-05 pending fjall crash test + read-path bench and user confirmation
- [Phase 01]: S3: redb T2 crash sanity 5/5. After kill -9 the db reopens at exactly the last durable checkpoint; None commits are discarded, so recovery replays the WAL from checkpointed_offset

### Pending Todos

None yet.

### Blockers/Concerns

Open research gaps, from research/SUMMARY.md:
- Do componentize-py/js reseed randomness after pre-init? (S2, Phase 1)
- Python per-instance memory vs NFR-2. This decides whether Python defaults to workers. (S2, Phase 1)
- macOS notarization entitlement for Cranelift. (Phase 1 smoke test)
- Does any WAL V2 recovery path decode `canonical_data` as Kafka? (Phase 2)
- Wasmtime 50.0.2 default stack and pool sizes. (Phase 3)
- Pin policy: Wasmtime 48 LTS vs tracking monthly releases to 60 LTS. (Phase 1 decision)
- Fixed partition count (P=16 single node / 64 cluster) must be confirmed before Phase 5 freezes the format.
- The MVP is larger than docs/02. Streaming and debounce/singleton are already v2. The saga helper (WF-10) is a candidate cut if scope pressure appears.
- Storage engine choice for STOR-05: redb failed S3 rule, fjall recommended; needs user confirmation in FOUND-06 decision logging (01-08)

## Session Continuity

Last session: 2026-11-18T17:28:06.010Z
Stopped at: Completed 01-02-PLAN.md
Resume file: None
