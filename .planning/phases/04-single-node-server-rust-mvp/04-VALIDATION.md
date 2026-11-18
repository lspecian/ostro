---
phase: 4
slug: single-node-server-rust-mvp
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-11-18
---

# Phase 4 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Source: 04-RESEARCH.md § Validation Architecture, mapped onto plans 04-01..04-20.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test harness (`cargo test`, Rust 1.100.0) + insta 1 (OpenAPI snapshot) + proptest 1.12 (meta versions, rebuild digest) + trybuild 1 (SDK macro diagnostics); spawned-binary tests via `CARGO_BIN_EXE_ostro`; HTTP via reqwest (no TLS), gRPC via generated tonic clients |
| **Config file** | none beyond `Cargo.toml`; SDK has its own workspace `sdks/rust/Cargo.toml` (04-05); guests workspace `guests/Cargo.toml` (`members = ["*"]`) |
| **Guest build** | Automatic: `tests/common::guest_wasm(name)` runs `cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml --target-dir target/guests` once per test process (ostro-engine, ostro-server, ostro tests each carry a copy of the helper) |
| **Quick run command** | `cargo test -p ostro-meta -p ostro-api -p ostro-server --locked` (< 60 s warm) |
| **Full suite command** | `bash scripts/ci-local.sh --no-musl` (fmt, clippy -D warnings, warning-free build, workspace tests, failpoints, deny, vendored, + SDK workspace after 04-20) |
| **Phase gate command** | `bash scripts/ci-local.sh` (including the musl leg) |
| **Estimated runtime** | quick < 60 s; full ~5–8 min debug (guest builds + engine determinism + server e2e) |

---

## Sampling Rate

- **After every task commit:** `cargo test -p <touched crate> --locked` plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings` (SDK: `cargo test --manifest-path sdks/rust/Cargo.toml`).
- **After every plan wave:** `cargo test --workspace --locked` plus the SDK workspace tests.
- **Before `/gsd:verify-work`:** `bash scripts/ci-local.sh` green (04-20 Task 3).
- **Max feedback latency:** 60 s for task-level checks (spawned-binary and e2e suites may take longer; they run at plan end).

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 4-01-01 | 01 | 1 | META-03 | unit (table-driven) + proptest | `cargo test -p ostro-meta --test meta_versions` | ❌ W0 (created by task) | ⬜ pending |
| 4-01-02 | 01 | 1 | META-02 | integration + proptest | `cargo test -p ostro-meta --test meta_records` | ❌ W0 | ⬜ pending |
| 4-01-03 | 01 | 1 | META-01 | integration | `cargo test -p ostro-meta --test meta_persist` | ❌ W0 | ⬜ pending |
| 4-02-01 | 02 | 1 | API-01 (mux smoke) | integration | `cargo test -p ostro-api --test http_smoke` | ❌ W0 | ⬜ pending |
| 4-02-02 | 02 | 1 | CLI-02 | integration | `cargo test -p ostro-server --test config` | ❌ W0 | ⬜ pending |
| 4-03-01/02 | 03 | 1 | API-01 | unit | `cargo test -p ostro-proto` | ✅ (extended) | ⬜ pending |
| 4-04-01 | 04 | 1 | WF-02 | tdd unit + golden | `cargo test -p ostro-types && cargo test -p ostro-engine --lib` | ✅ (extended) | ⬜ pending |
| 4-05-01/02 | 05 | 1 | SDK-01 | build + unit | `cargo build --target wasm32-wasip2 --manifest-path sdks/rust/Cargo.toml && cargo test --manifest-path sdks/rust/Cargo.toml -p ostro-sdk` | ❌ W0 | ⬜ pending |
| 4-06-01 | 06 | 2 | SDK-01 | trybuild | `cargo test --manifest-path sdks/rust/Cargo.toml` | ❌ W0 | ⬜ pending |
| 4-06-02 | 06 | 2 | SDK-01, WF-01, WF-07 | integration (engine testkit) | `cargo test -p ostro-engine --test sdk_guests` | ❌ W0 | ⬜ pending |
| 4-07-01/02 | 07 | 2 | WF-02 | integration (FakeSink) | `cargo test -p ostro-engine --test step_retry` | ❌ W0 | ⬜ pending |
| 4-08-01 | 08 | 2 | API-01..04 | unit | `cargo test -p ostro-api --lib` | ✅ | ⬜ pending |
| 4-08-02 | 08 | 2 | API-01..04 | integration + insta | `cargo test -p ostro-api --test rest --test openapi` | ❌ W0 | ⬜ pending |
| 4-09-01 | 09 | 2 | (gate) | check | Phase 2 SUMMARY files + clean tree + `bash scripts/ci-local.sh --no-musl` | n/a | ⬜ pending |
| 4-09-02 | 09 | 2 | API-03 | unit + golden | `cargo test -p ostro-types` | ✅ (extended) | ⬜ pending |
| 4-10-01 | 10 | 3 | WF-02 | integration | `cargo test -p ostro-engine --test step_timeout` | ❌ W0 | ⬜ pending |
| 4-11-01/02 | 11 | 3 | API-01 | integration (one port) | `cargo test -p ostro-api --test parity` | ❌ W0 | ⬜ pending |
| 4-12-01 | 12 | 3 | API-03, DISP-01 | integration (apply) | `cargo test -p ostro-partition --test apply_phase4` | ❌ W0 | ⬜ pending |
| 4-12-02 | 12 | 3 | DISP-01 | unit + proptest | `cargo test -p ostro-partition --lib query && cargo test -p ostro-partition --test rebuild_digest` | ✅ (extended) | ⬜ pending |
| 4-13-01 | 13 | 4 | DISP-01, WF-07 | integration (apply) | `cargo test -p ostro-partition --test apply_timer_input --test timers` | ❌ W0 | ⬜ pending |
| 4-14-01 | 14 | 4 | DISP-01, DISP-02 | integration (fake Executor) | `cargo test -p ostro-partition --test dispatcher` | ❌ W0 | ⬜ pending |
| 4-14-02 | 14 | 4 | DISP-07 | integration (fake Executor) | `cargo test -p ostro-partition --test dispatcher` | ❌ W0 | ⬜ pending |
| 4-15-01 | 15 | 4 | WF-01, WF-02 | unit | `cargo test -p ostro-server --lib` | ❌ W0 | ⬜ pending |
| 4-15-02 | 15 | 4 | WF-02 | integration (real Node) | `cargo test -p ostro-server --test sink` | ❌ W0 | ⬜ pending |
| 4-16-01 | 16 | 5 | WF-02, DISP-01 | integration (in-process) | `cargo test -p ostro-server --test executor` | ❌ W0 | ⬜ pending |
| 4-16-02 | 16 | 5 | DISP-07 | integration (in-process) | `cargo test -p ostro-server --test leases` | ❌ W0 | ⬜ pending |
| 4-17-01 | 17 | 6 | META-01, META-03, CLI-01 | integration (full server) | `cargo test -p ostro-server --test bootstrap` | ❌ W0 | ⬜ pending |
| 4-17-02 | 17 | 6 | API-02 | integration (REST + gRPC) | `cargo test -p ostro-server --test modules_api` | ❌ W0 | ⬜ pending |
| 4-18-01 | 18 | 7 | API-03, WF-01 | integration (REST + gRPC) | `cargo test -p ostro-server --test start_run_api` | ❌ W0 | ⬜ pending |
| 4-18-02 | 18 | 7 | API-04 | integration (REST + gRPC) | `cargo test -p ostro-server --test runs_api` | ❌ W0 | ⬜ pending |
| 4-19-01 | 19 | 7 | CLI-01, CLI-02 | e2e (binary) | `cargo test -p ostro --test cli` | ✅ (extended) | ⬜ pending |
| 4-19-02 | 19 | 7 | CLI-01, CLI-02 | e2e (spawn binary) | `cargo test -p ostro --test dev_startup --test server_config` | ❌ W0 | ⬜ pending |
| 4-20-01 | 20 | 8 | SDK-01, WF-01, API-01..04, META-01, CLI-01 | e2e (spawn `ostro dev`) | `cargo test -p ostro --test e2e_dev` | ❌ W0 | ⬜ pending |
| 4-20-02 | 20 | 8 | WF-02, WF-07, DISP-07, META-03 | e2e (full in-process server) | `cargo test -p ostro-server --test sdk_e2e` | ❌ W0 | ⬜ pending |
| 4-20-03 | 20 | 8 | all | phase gate | `bash scripts/ci-local.sh` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Test scaffolds are created by the task that first needs them (every task's `<verify>` names its own test file), so there is no separate Wave 0 plan:

- [ ] `crates/ostro-server/` crate skeleton + `tests/config.rs` — 04-02
- [ ] `crates/ostro-meta/tests/{meta_persist,meta_records,meta_versions}.rs` — 04-01
- [ ] `crates/ostro-api/tests/{http_smoke,rest,openapi,parity}.rs` + insta snapshot + `FakeControlPlane` — 04-02 / 04-08 / 04-11
- [ ] `sdks/rust/` workspace (ostro-sdk, ostro-sdk-macros, trybuild) — 04-05 / 04-06
- [ ] `guests/sdk-{basic,basic-v2,retry,native}` (04-06) and `guests/retry-probe` (04-07)
- [ ] `crates/ostro-engine/tests/{step_retry,step_timeout,sdk_guests}.rs` — 04-07 / 04-10 / 04-06
- [ ] `crates/ostro-partition/tests/{apply_phase4,apply_timer_input,dispatcher}.rs` — 04-12 / 04-13 / 04-14
- [ ] `crates/ostro-server/tests/{common/mod.rs,sink,executor,leases,bootstrap,modules_api,start_run_api,runs_api,sdk_e2e}.rs` — 04-15..04-20
- [ ] `crates/ostro/tests/{common/mod.rs,dev_startup,server_config,e2e_dev}.rs` — 04-19 / 04-20
- [ ] Framework installs: `trybuild` (SDK macros dev-dep, 04-06); `reqwest` without TLS (workspace pin, 04-02); tonic clients (existing dependency)
- [ ] ci-local: SDK workspace steps and serial `dev_startup` step — 04-20

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Cold first-run `ostro dev` on a slow-fsync disk (≈1.5 s on WSL2) | CLI-01 | Locked decision: the < 1 s criterion applies to warm restart and `--ephemeral`; the cold number is measured and reported, not asserted | `cargo test -p ostro --test dev_startup -- --ignored cold_first_run_measurement --nocapture`; record min/median in 04-19/04-20 SUMMARY |
| Remote CI legs (aarch64 musl, macOS) | all | No git remote yet (FOUND-05 human_needed) | Push and confirm the Actions run once a remote exists |

*All other phase behaviors have automated verification.*

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references (each test file is created by the task that verifies with it)
- [x] No watch-mode flags
- [ ] Feedback latency < 60 s (spawned-binary/e2e suites exceed it by design; they run at plan end)
- [ ] `nyquist_compliant: true` set in frontmatter (04-20 Task 3 flips it when every row is green)

**Approval:** pending
