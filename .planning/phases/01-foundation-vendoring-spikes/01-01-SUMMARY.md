---
phase: 01-foundation-vendoring-spikes
plan: 01
subsystem: infra
tags: [cargo-workspace, rust-1.100, wasmtime, redb, tonic, protox, axum, rustls, aws-lc-rs, mimalloc, cargo-deny]

requires: []
provides:
  - "Virtual Cargo workspace (resolver 3, edition 2024, publish=false) pinned to Rust 1.100.0"
  - "11-crate research layout under crates/ with workspace.dependencies pins"
  - "Skeleton crates linking wasmtime 50.0.2, redb 4.6.0, tonic 0.14.9 + protox, axum 0.8.9, rustls/aws-lc-rs, mimalloc"
  - "deny.toml policy (duplicate bans, outright bans, licence allow-list, crates.io-only sources)"
  - "scripts/check-deny-fixtures.sh negative test with dup-prost and bad-licence fixtures"
  - "LICENSE (Apache-2.0) and NOTICE (Chronik Stream + TiKV raft-rs attribution)"
affects: [01-04, 01-06, 01-07, 01-08, phase-02, phase-03, phase-04]

tech-stack:
  added: [wasmtime 50.0.2, redb 4.6.0, tonic 0.14.9, tonic-prost 0.14.9, tonic-prost-build 0.14.9, prost 0.14.6, protox 0.10.0, axum 0.8.9, rustls 0.23 (aws_lc_rs), mimalloc 0.1.52, clap 4.6.8, tokio 1.54, tempfile 3, cargo-deny 0.21.0 policy]
  patterns:
    - "Every crate inherits version/edition/rust-version/license/publish and [lints] from the workspace"
    - "Skeleton crates hold one small real use of their hard dependency so CI and cargo-deny see the real graph"
    - "Proto codegen via protox::compile + tonic_prost_build::configure().compile_fds (no protoc)"
    - "Engine boundary uses wasmtime::Result, not anyhow"
    - "cargo-deny negative fixtures are standalone workspaces checked with --config deny.toml"

key-files:
  created:
    - Cargo.toml
    - Cargo.lock
    - rust-toolchain.toml
    - LICENSE
    - NOTICE
    - deny.toml
    - crates/ostro-engine/src/lib.rs
    - crates/ostro-partition/src/lib.rs
    - crates/ostro-proto/build.rs
    - crates/ostro-proto/proto/ostro/health/v1/health.proto
    - crates/ostro-api/src/lib.rs
    - crates/ostro/src/main.rs
    - crates/ostro/build.rs
    - crates/ostro/tests/cli.rs
    - ci/deny-fixtures/dup-prost/Cargo.toml
    - ci/deny-fixtures/bad-licence/Cargo.toml
    - scripts/check-deny-fixtures.sh
  modified:
    - .gitignore

key-decisions:
  - "LICENSE copied offline from the canonical 201-line Apache-2.0 text (most common LICENSE-APACHE variant in the cargo registry, e.g. addr2line-0.26.1)"
  - "rustls kept as a normal (non-dev) dep of ostro-api via a pub tls_provider() so aws-lc-rs stays visible to cargo-deny (exclude-dev = true)"
  - "No licence additions were needed: the real graph passes the research allow-list as-is"
  - "Unused workspace dependency warnings (metrics, thiserror, tracing, prost-build) are accepted until plans 01-04/01-06 consume them"

patterns-established:
  - "Workspace crate manifest template: *.workspace = true for package metadata + [lints] workspace = true"
  - "TDD for skeleton crates: failing test commit, then implementation commit"

requirements-completed: [FOUND-01, FOUND-04]

duration: 4min
completed: 2026-11-18
---

# Phase 1 Plan 01: Workspace Bootstrap and cargo-deny Policy Summary

**Rust 1.100.0 virtual workspace with 11 crates. Skeletons link wasmtime 50.0.2, redb 4.6.0, tonic+protox (no protoc), axum, rustls/aws-lc-rs and mimalloc. A cargo-deny policy passes on the workspace, and two fixtures show it rejects a duplicate prost and a GPL-3.0-only licence.**

## Performance

- **Duration:** about 4 min
- **Started:** 2026-11-18T17:19:22Z
- **Completed:** 2026-11-18T17:23:13Z
- **Tasks:** 3
- **Files modified:** 37

## Accomplishments
- `rustc --version` from the repo root reports 1.100.0. `cargo build --workspace` and `cargo test --workspace` pass (9 tests). `cargo metadata` lists exactly the 11 research crates.
- The skeletons really link their dependencies:
  - a wasmtime `Engine` is built from the deterministic `engine_config()`
  - a redb transaction commits with `Durability::None` and then `Immediate`, and both keys are read back
  - protox/tonic codegen produces `ostro.health.v1`, and a `CheckRequest` survives a prost round-trip
  - tonic `Routes::into_axum_router()` is merged with `GET /healthz`
  - the aws-lc-rs provider has cipher suites
  - `ostro build-info` prints `ostro 0.1.0 target=x86_64-unknown-linux-gnu allocator=mimalloc`
- `cargo deny check` passes: advisories, bans, licences and sources are all ok, with no ignores and no licence additions.
- `scripts/check-deny-fixtures.sh` confirms both fixtures are rejected for the expected reason:
  - dup-prost: exit 2, `duplicate` / `prost`
  - bad-licence: exit 4, `GPL-3.0-only` / `rejected: license is not explicitly allowed`
- chrono, bincode, openssl-sys, zstd-sys and protoc are all absent from the dependency graph.

## Task Commits

1. **Task 1: Root workspace files, toolchain pin, licence/notice, dependency-free skeletons** - `257b935` (feat)
2. **Task 2: Dependency-carrying skeleton crates** (TDD)
   - RED: `e65cf5e` (test)
   - GREEN: `494ac52` (feat)
3. **Task 3: cargo-deny policy with negative fixtures** - `6e20420` (feat)

## Files Created/Modified
- `Cargo.toml`: virtual workspace with resolver 3, workspace.package, workspace.dependencies pins, lints and release profile
- `rust-toolchain.toml`: 1.100.0 with clippy and rustfmt, plus the musl and wasm32-wasip2 targets
- `LICENSE` / `NOTICE`: Apache-2.0 text, plus attribution to Chronik Stream (commit 08715f4) and TiKV raft-rs v0.7.0
- `.gitignore`: spike outputs/targets/wasm and the fixture targets
- `crates/ostro-types`: `CODEC_VERSION = 1` with a test
- `crates/ostro-{log,meta,bridge,blob,cli}`: doc-only placeholders
- `crates/ostro-engine/src/lib.rs`: `engine_config()` with component model, NaN canonicalisation, deterministic relaxed SIMD, fuel and epochs
- `crates/ostro-partition/src/lib.rs`: `open()` redb helper and a durability test
- `crates/ostro-proto/{build.rs,proto/...,src/lib.rs}`: protox codegen for the health service
- `crates/ostro-api/src/lib.rs`: `HealthSvc`, `router()` and `tls_provider()`
- `crates/ostro/{build.rs,src/main.rs,tests/cli.rs}`: binary with mimalloc, clap `--version` and `build-info`
- `deny.toml`: the policy from the research verbatim, plus the bincode ban
- `ci/deny-fixtures/{dup-prost,bad-licence}`: standalone fixture workspaces with committed lockfiles
- `scripts/check-deny-fixtures.sh`: the negative test

## Decisions Made
- All pinned versions resolved as written. No patch bumps were needed.
- `ostro-api` exposes `pub fn tls_provider()`. This gives rustls a real non-test use, so it stays in the normal dependency graph that cargo-deny inspects (`exclude-dev = true`).
- The `ostro` binary links ostro-engine, ostro-partition and ostro-api through `use ... as _;`.
- With no subcommand, `ostro` prints help.

## Deviations from Plan

None. The plan was executed as written.

Minor notes:
- In Task 1, the task-2 crates got temporary stub manifests in the working tree only, so `cargo build --workspace` could run. They were first committed in Task 2.
- The CLI test file `crates/ostro/tests/cli.rs` also tests `--version`, which covers the version item in the behaviour spec.

## Issues Encountered
- Cargo 1.100 has a new lint, `cargo::unused_workspace_dependencies`, which warns on every command about `metrics`, `thiserror`, `tracing` and `prost-build`. The plan asked for these pins, but no crate uses them yet. The warning is not an error, and clippy `-D warnings` passes. Plans 01-04 and 01-06 consume these pins, which will clear the warning.
- `cargo deny check` warns (it does not fail) about duplicates of foldhash, itertools, prettyplease, syn and hashbrown. The policy expects these under `multiple-versions = "warn"`.

## User Setup Required
None.

## Next Phase Readiness
- Plans 01-04 and 01-06 can add `"vendor/*"` to `members`, declare the `raft`, `ostro-raft-driver` and `ostro-wal` workspace deps, and wire ostro-meta and ostro-log.
- CI (plan 01-07 or later) can run `cargo deny check && bash scripts/check-deny-fixtures.sh` directly.

---
*Phase: 01-foundation-vendoring-spikes*
*Completed: 2026-11-18*

## Self-Check: PASSED

All key files exist; commits 257b935, e65cf5e, 494ac52, 6e20420 found in git log.
