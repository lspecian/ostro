---
phase: 1
slug: foundation-vendoring-spikes
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-11-18
---

# Phase 1 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | cargo test (built-in) + cargo-deny 0.21.0 + shell checks |
| **Config file** | none — Wave 0 creates `rust-toolchain.toml`, `deny.toml`, `scripts/check-vendored.sh`, `scripts/check-deny-fixtures.sh`, `scripts/ci-local.sh` |
| **Quick run command** | `cargo build --workspace && cargo test -p <touched crates>` |
| **Full suite command** | `cargo build --workspace && cargo test --workspace && cargo deny check && scripts/check-vendored.sh && scripts/check-deny-fixtures.sh` |
| **Estimated runtime** | ~120 seconds (warm cache) |

---

## Sampling Rate

- **After every task commit:** Run the quick run command
- **After every plan wave:** Run the full suite command
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 180 seconds

---

## Per-Task Verification Map

| Req | Behavior | Test Type | Automated Command | File Exists | Status |
|-----|----------|-----------|-------------------|-------------|--------|
| FOUND-01 | Workspace builds on 1.100 with research crate layout | build | `rustc --version \| grep 1.100.0 && cargo build --workspace` | ❌ W0 | ⬜ pending |
| FOUND-01 | Skeleton deps link (wasmtime Engine, redb txn, tonic codegen, raft single voter) | unit | `cargo test -p ostro-engine -p ostro-partition -p ostro-proto -p ostro-meta -p ostro-log` | ❌ | ⬜ pending |
| FOUND-02 | Every vendored file listed with commit; no Chronik references | script | `scripts/check-vendored.sh` | ❌ W0 | ⬜ pending |
| FOUND-03 | Vendored code compiles and tests pass without chronik-common/monitoring, tokio-uring, `[patch]` | unit + grep | `cargo test -p ostro-raft-rs -p ostro-raft-proto -p ostro-raft-driver -p ostro-wal && ! cargo tree -e normal \| grep -E 'chronik\|tokio-uring'` | ❌ | ⬜ pending |
| FOUND-04 | deny passes on repo; fails on duplicate prost and bad licence | script | `cargo deny check && scripts/check-deny-fixtures.sh` | ❌ W0 | ⬜ pending |
| FOUND-05 (local) | x86_64 build/test, fmt, clippy, YAML valid, local musl zigbuild | smoke | `scripts/ci-local.sh` | ❌ | ⬜ pending |
| FOUND-06 | S2/S3 reports with required metrics; 3 decisions in PROJECT.md | doc check | `test -f docs/spikes/S2-guest-toolchains.md && test -f docs/spikes/S3-redb-durability.md && grep -E 'Step handle\|redb vs fjall\|Wasmtime pin' .planning/PROJECT.md` | ❌ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `rust-toolchain.toml`, root `Cargo.toml`, `deny.toml`, `LICENSE`, `NOTICE`
- [ ] `scripts/check-vendored.sh`, `scripts/check-deny-fixtures.sh` with `ci/deny-fixtures/{dup-prost,bad-licence}`
- [ ] `scripts/ci-local.sh`
- [x] Tools: cargo-deny 0.21.0, cargo-zigbuild 0.24.1, zig 0.17.1 (pip `ziglang`), Rust 1.100.0 — already installed

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| aarch64-musl, x86_64-musl and macOS CI legs green | FOUND-05 (remote) | No git remote; macOS/arm runners only on GitHub | Push to GitHub, `gh run watch`; record as human_needed until then |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 180s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
