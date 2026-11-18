---
phase: 01-foundation-vendoring-spikes
verified: 2026-11-18T00:00:00Z
status: human_needed
score: 4/4 success criteria verified locally (remote CI legs of criterion 3 pending)
human_verification:
  - test: "Run the GitHub Actions workflow on a hosted remote"
    expected: "linux x86_64 musl, linux aarch64 musl (cargo-zigbuild + mimalloc) and macos-15 aarch64 legs all pass; deny and lint jobs pass"
    why_human: "No git remote by design. Only the x86_64 musl leg ran locally (scripts/ci-local.sh)."
---

# Phase 1: Foundation, Vendoring & Spikes Verification Report

**Phase Goal:** The workspace builds on pinned tooling with Chronik code vendored and modernised, CI enforces the dependency and platform rules, and spike measurements have settled the open stack decisions.
**Status:** human_needed. Everything verifiable locally passes. The remote CI legs cannot be verified here.
**Re-verification:** No, initial verification.

## Observable Truths

| # | Truth (ROADMAP success criterion) | Status | Evidence |
|---|---|---|---|
| 1 | `cargo build --workspace` succeeds on Rust 1.100 with the research crate layout. Vendored raft/WAL compile with no `[patch]`, no chronik-common/monitoring, no tokio-uring | VERIFIED | `rustc 1.100.0`. Build finished OK. `cargo test --workspace` all `test result: ok`, no failures. All 11 crates are in `crates/`. Four vendored crates are in `vendor/`. No `[patch]` in Cargo.toml. No tokio-uring or chronik-common/monitoring in any Cargo.toml. |
| 2 | `VENDORED.md` lists every vendored file with source path, commit `08715f4` and modifications. Nothing references the Chronik repo | VERIFIED | `scripts/check-vendored.sh` passes ("all checks passed"). It covers no git sources, no upstream build references, and vendored manifests on edition 2021 with no `[workspace]`. VENDORED.md records commit 08715f4867c9... |
| 3 | CI passes on x86_64/aarch64 musl (zigbuild, mimalloc) and macOS. cargo-deny fails on duplicate tonic/prost/hyper/axum/opentelemetry or a disallowed licence | PARTIAL (local part verified) | `cargo deny check`: advisories, bans, licenses and sources ok. `scripts/check-deny-fixtures.sh`: dup-prost rejected (exit 2), bad-licence rejected (exit 4), all fixtures rejected as expected. The workflow has linux-aarch64-musl and macos-aarch64 legs, and mimalloc is the global allocator in `crates/ostro/src/main.rs`. A real run on the aarch64 and macOS legs is not possible locally, so it is listed under human verification. |
| 4 | S2 report (size, memory, cold start, randomness), S3 report (throughput per tier), three decisions logged in PROJECT.md | VERIFIED | `docs/spikes/S2-guest-toolchains.md` has `.wasm` and `.cwasm` sizes, per-instance memory (Python ~6.5 MB, TS ~2.4 MB, Rust ~40 KB), a cold-start table and randomness findings. `docs/spikes/S3-redb-durability.md` has redb T2/T3 throughput. PROJECT.md Key Decisions has the step handle (resource), redb vs fjall (fjall 3.1.13, redb FAIL at about 67k vs the 90k rule) and the Wasmtime pin policy (exact `=50.0.2`, monthly tracking). |

**Score:** 3/4 fully verified, and the fourth is verified except for its remote legs. No failures.

## Requirements Coverage

| Requirement | Source Plan | Status | Evidence |
|---|---|---|---|
| FOUND-01 | 01-01, 01-04, 01-06 | SATISFIED | Workspace builds and tests pass on Rust 1.100 with the 11 required crates. |
| FOUND-02 | 01-04, 01-06 | SATISFIED | VENDORED.md, NOTICE and LICENSE exist. check-vendored passes. |
| FOUND-03 | 01-04, 01-06 | SATISFIED | No `[patch]`, tokio-uring or chronik-common/monitoring. The raft crates are renamed to ostro-raft-*. |
| FOUND-04 | 01-01, 01-07 | SATISFIED | cargo deny passes and the negative fixtures are rejected. |
| FOUND-05 | 01-07 | NEEDS HUMAN | The local x86_64 musl leg ran through ci-local.sh. The workflow defines the aarch64 and macOS legs, but they have not run. REQUIREMENTS.md already marks this item unchecked and "Partial". |
| FOUND-06 | 01-02, 01-03, 01-05, 01-08 | SATISFIED | S2 and S3 reports exist and the three decisions are in PROJECT.md. |

No orphaned requirements. FOUND-01 to FOUND-06 are all claimed by plans.

## Anti-Patterns

None found that block the goal. I did not do a full TODO scan of the vendored upstream code.

## Human Verification Required

### 1. Remote CI matrix

**Test:** Push to a remote that runs Actions, or run the workflow with `act`.
**Expected:** The linux-x86_64-musl, linux-aarch64-musl and macos-aarch64 legs pass, along with the deny and lint jobs.
**Why human:** There is no git remote by design, so the aarch64 and macOS legs cannot be run locally.

## Gaps Summary

No gaps. Phase 1 is achieved locally. The only open item is the remote execution of the aarch64 musl and macOS CI legs (FOUND-05).

_Verified: 2026-11-18_
_Verifier: Claude (gsd-verifier)_
