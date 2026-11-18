# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-11-18)

**Core value:** Download one binary, deploy a Wasm workflow, and it runs durably. Accepted runs are never lost, survive crashes, and replay deterministically, with nothing else to install or operate.
**Current focus:** Phase 1: Foundation, Vendoring & Spikes

## Current Position

Phase: 1 of 14 (Foundation, Vendoring & Spikes)
Plan: 0 of TBD in current phase
Status: Ready to plan
Last activity: 2026-11-18 — Roadmap created (14 phases, 136/136 v1 requirements mapped)

Progress: [░░░░░░░░░░] 0%

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

## Accumulated Context

### Decisions

Decisions are logged in the PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- [Roadmap]: Phases 2 (storage) and 3 (engine) can run in parallel after Phase 1. The engine is built against a fake `ActivationSink`.
- [Roadmap]: The on-disk format, command set and WIT 0.1 are frozen at the end of Phase 5. Changes after that are additive only.
- [Roadmap]: The snapshot format is designed once, in Phase 8, and serves both restore and follower install (Phase 12).
- [Roadmap]: Cluster work (Phases 12–13) is added behind `ReplicatedLog`. The replication conformance suite is written and proven failing before integration.
- [Roadmap]: Rust `ostro build` exists only as a direct cargo build until Phase 10. CLI-04 (hermetic three-language build) is mapped to Phase 10.

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

## Session Continuity

Last session: 2026-11-18
Stopped at: Roadmap and state initialized; ready for `/gsd:plan-phase 1`
Resume file: None
