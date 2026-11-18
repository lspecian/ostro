---
phase: 01-foundation-vendoring-spikes
plan: 07
subsystem: infra
tags: [ci, github-actions, cargo-zigbuild, zig, musl, mimalloc, cargo-deny]

requires:
  - phase: 01-01
    provides: "Workspace, deny.toml, check-deny-fixtures.sh, ostro build-info"
  - phase: 01-06
    provides: "scripts/check-vendored.sh"
provides:
  - "scripts/ci-local.sh: local CI gate (13 steps), mirrors the lint job and the linux-x86_64-musl leg"
  - ".github/workflows/ci.yml: lint job + test matrix (linux x86_64/aarch64 musl via zigbuild, macos-15 arm64, linux x86_64 gnu), SHA-pinned actions"
  - "Warning-free workspace build (unused workspace pins removed)"
affects: [phase-02, phase-03, release]

tech-stack:
  added: [cargo-zigbuild 0.24.1, zig 0.17.1, GitHub Actions]
  patterns:
    - "The zigbuild test runner is `cargo-zigbuild test`. `cargo zigbuild` only builds"
    - "ci-local.sh fails the build step on any `warning` line, including workspace-manifest warnings"
    - "Workflow validation: actionlint (binary or docker), falling back to a PyYAML parse that also asserts every `uses:` is pinned to a 40-hex SHA"

key-files:
  created:
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
  modified:
    - Cargo.toml

key-decisions:
  - "zig 0.17.1 works with cargo-zigbuild 0.24.1 for the static x86_64 musl build (aws-lc-sys, mimalloc and wasmtime included). No fallback version was needed, and CI pins ZIG_VERSION 0.17.1"
  - "cargo-deny is installed in CI with taiki-e/install-action (cargo-deny@0.21.0), not cargo-deny-action, so CI runs the same binary and the same `cargo deny check` as local, and check-deny-fixtures.sh finds it on PATH"
  - "Unused workspace pins (raft, raft-proto, metrics, tracing, thiserror, prost-build) were removed. Vendored crates pin their own deps. Re-add a pin when an Ostro crate consumes it"
  - "Actions pinned: actions/checkout v7.0.1, Swatinem/rust-cache v2.9.3 (peeled SHA), mlugg/setup-zig v2.2.1, taiki-e/install-action v2.90.6"

requirements-completed: [FOUND-04]
requirements-partial: [FOUND-05]

duration: 15min
completed: 2026-11-18
---

# Phase 1 Plan 07: Local CI Gate + GitHub Actions Matrix Summary

**`scripts/ci-local.sh` passes all 13 steps on this machine.** That includes a static x86_64 musl `ostro` built with cargo-zigbuild 0.24.1 and zig 0.17.1, whose `build-info` reports `allocator=mimalloc`. `.github/workflows/ci.yml` has a lint job and a 4-leg matrix (linux x86_64/aarch64 musl, macos-15 arm64, linux x86_64 gnu), with every action pinned to a commit SHA. The remote legs have not run because there is no git remote. They are human_needed.

## Performance

- **Duration:** about 15 min
- **Completed:** 2026-11-18
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- The final `bash scripts/ci-local.sh` run exited 0, with every step PASS: toolchain 1.100.0, fmt, clippy -D warnings (Ostro crates), warning-free build, test, cargo deny check, check-vendored.sh, check-deny-fixtures.sh, the chronik/tokio-uring/prometheus tree check, workflow validation, zig/zigbuild presence, musl release, and musl tests.
- `file target/x86_64-unknown-linux-musl/release/ostro` gives `ELF 64-bit LSB executable, x86-64, ... statically linked, stripped`.
- `build-info` prints `ostro 0.1.0 target=x86_64-unknown-linux-musl allocator=mimalloc`.
- Timings: the cold musl release build (fat LTO) took about 85 s and the cold `cargo-zigbuild test --workspace --target x86_64-unknown-linux-musl` about 67 s. That is well under the 10-minute budget, so `--no-musl-test` is optional and not the default.
- `cargo build --workspace` now emits zero warnings, and ci-local.sh enforces that.
- `git remote` is empty. Nothing was pushed.

## Task Commits

1. **Task 1: Local CI gate script with musl zigbuild leg** - `5d46c2d` (feat)
2. **Task 2: GitHub Actions workflow with SHA-pinned actions** - `d119488` (feat)

## Files Created/Modified
- `scripts/ci-local.sh`: the local gate. It accepts `--no-musl` and `--no-musl-test`, prints a PASS/SKIP/FAIL table and exits non-zero on any FAIL.
- `.github/workflows/ci.yml`: the lint job and the test matrix. It uses `permissions: contents: read`, cancels superseded runs, and pins versions through env (`CARGO_DENY_VERSION`, `CARGO_ZIGBUILD_VERSION`, `ZIG_VERSION`).
- `Cargo.toml`: the 6 unused workspace dependency pins are removed.

## Decisions Made
See key-decisions in the frontmatter.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `cargo zigbuild test` does not exist**
- **Found during:** Task 1 (first ci-local run)
- **Issue:** The plan and research wrote `cargo zigbuild test`. cargo-zigbuild 0.24.1 rejects it ("unexpected argument 'test'"). The test runner is a separate subcommand, `cargo-zigbuild test`, and the version flag works the same way: `cargo-zigbuild --version`.
- **Fix:** The script and the workflow both use `cargo-zigbuild test --workspace --locked --target $T`.
- **Commits:** 5d46c2d, d119488

**2. [Rule 3 - Per orchestrator note] Removed unused workspace pins**
- **Issue:** Cargo printed 6 "unused workspace dependency" warnings.
- **Fix:** Removed raft, raft-proto, metrics, tracing, thiserror and prost-build from `[workspace.dependencies]`. Vendored crates declare their own versions. cargo-deny's duplicate bans are unaffected (`cargo deny check` and the deny fixtures still pass), and Cargo.lock is unchanged (`--locked` passes).
- **Commit:** 5d46c2d

**3. Additions beyond the plan text**
- The ci-local build step fails on warnings.
- The workflow validation fallback also asserts SHA pinning.
- The lint job prints `jq --version` before check-vendored.sh.

These tighten the gate and do not change scope.

## Issues Encountered
- Docker was not running, so the workflow was validated with the PyYAML fallback (YAML parse plus a SHA-pin assertion), not actionlint. Running actionlint (`docker run --rm -v "$PWD":/repo -w /repo rhysd/actionlint:latest .github/workflows/ci.yml`) with Docker up is a recommended extra check.

## human_needed

Push to GitHub (the user creates the repo and remote), then run `gh run watch`. Confirm that **lint**, **linux-x86_64-musl**, **linux-aarch64-musl**, **macos-aarch64** and **linux-x86_64-gnu** are all green. Until then the FOUND-05 remote legs are PARTIAL, not FAIL. Locally, only the x86_64 musl leg and the lint job are verified.

Watch for these on the first run:
- aarch64 musl on `ubuntu-24.04-arm` with zig 0.17.1 (aws-lc-sys) has not been tested locally.
- `mlugg/setup-zig` must be able to fetch 0.17.1.
- `rustup toolchain install` with no arguments has to read rust-toolchain.toml. This needs rustup 1.28 or later, which the runners ship.

## User Setup Required
None for local use. For remote CI, create a GitHub repo and push (see human_needed above).

## Next Phase Readiness
- Every later plan can run `bash scripts/ci-local.sh` (or `--no-musl` for speed) as its gate.
- Plan 01-08 (decision logging) is the last plan in Phase 1.

---
*Phase: 01-foundation-vendoring-spikes*
*Completed: 2026-11-18*

## Self-Check: PASSED

scripts/ci-local.sh and .github/workflows/ci.yml exist. Commits 5d46c2d and d119488 are in git log. `git remote` is empty.
