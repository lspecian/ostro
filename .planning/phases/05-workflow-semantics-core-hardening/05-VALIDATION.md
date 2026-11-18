---
phase: 5
slug: workflow-semantics-core-hardening
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-11-19
---

# Phase 5 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Source: 05-RESEARCH.md § Validation Architecture, mapped onto plans 05-01..05-17. Adopted decisions: 05-DECISIONS.md.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test harness (`cargo test`, Rust 1.100.0), proptest 1.12, insta 1.49 and trybuild (SDK). Spawned-binary tests use `CARGO_BIN_EXE_ostro`. Clients are reqwest (REST, no TLS) and generated tonic clients (gRPC). Failpoints use the `fail` crate and run in single-threaded suites. |
| **Config file** | None beyond Cargo.toml. Separate workspaces: `guests/Cargo.toml` (members glob), `sdks/rust/Cargo.toml` and `examples/order-saga/Cargo.toml` (standalone, 05-08). `scripts/ci-local.sh` holds `FAILPOINT_CRATES`, which must match the ci.yml "Failpoint suites" step. |
| **Guest build** | `tests/common::guest_wasm(name)` runs `cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml --target-dir target/guests` once per test process. |
| **Quick run command** | `cargo test -p <touched crate> --locked`, for example `cargo test -p ostro-partition --test apply_waits --locked` |
| **Full suite command** | `bash scripts/ci-local.sh --no-musl` |
| **Phase gate commands** | `bash scripts/ci-local.sh` (with musl). Then the sharded campaign: `OSTRO_CAMPAIGN_FAULTS=10000 OSTRO_CAMPAIGN_SHARDS=20 [OSTRO_CAMPAIGN_SHARD_IDS=a-b] OSTRO_CAMPAIGN_PARALLEL=6 cargo test --release -p ostro-crashtest --features failpoints --test server_campaign -- --ignored --nocapture`, followed by the report command. Then `cargo test -p ostro-engine --test replay_corpus`, which runs on every CI leg. Perf: `OSTRO_BENCH_PROFILE=a cargo test --release -p ostro-server --test perf -- --ignored --nocapture`, where the native NVMe run is human_needed. |
| **Estimated runtime** | Quick: < 60 s. Full: ~8–12 min debug (guest builds, engine determinism, server e2e, corpus, smoke campaign). 10k campaign: projected in 05-15 (≈ 1–2 h with 6 parallel shards; measured value recorded). |

---

## Sampling Rate

- **After every task commit:** `cargo test -p <crate> --locked` plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings`. For apply changes, also run `cargo test -p ostro-partition --features failpoints --locked -- --test-threads=1`. For SDK changes, run `cargo test --manifest-path sdks/rust/Cargo.toml`.
- **After every plan wave:** `cargo test --workspace --locked`, the SDK workspace tests and, from 05-10 on, `cargo test -p ostro-engine --test replay_corpus --locked`.
- **Before `/gsd:verify-work`:** the 05-17 gate. That means the full `bash scripts/ci-local.sh` green, the 10k campaign green, the corpus green locally (x86_64 gnu and musl), and the freeze enforced.
- **Max feedback latency:** 60 s for task-level checks. Spawned-binary, e2e and sleeper suites may take longer and run at plan end.

---

## Wave Structure

| Wave | Plans | Notes |
|------|-------|-------|
| 1 | 05-01 | Gate: Phase 4 complete and verified, 02-16 committed, clean tree. Adopted decisions go into STATE.md. ostro-types contract and status parity. |
| 2 | 05-02, 05-03, 05-04, 05-05 | Disjoint files: partition store and apply seams / engine / API adapters / WIT tooling. Only 05-05 touches Cargo.toml, Cargo.lock and deny.toml. |
| 3 | 05-06, 05-07, 05-08 | apply/waits.rs / apply/lifecycle.rs + retention.rs / SDK + guests + order-saga. Only 05-08 touches ci-local.sh and ci.yml. |
| 4 | 05-09, 05-10 | Server seam / replay corpus. Only 05-10 touches Cargo.lock. |
| 5 | 05-11 | ControlPlane lifecycle and long-poll |
| 6 | 05-12, 05-13, 05-14 | CLI / semantics e2e / perf. Only 05-12 touches Cargo.lock and crates/ostro. Only 05-13 touches ostro-server tests/common. |
| 7 | 05-15, 05-16 | Crash campaign / quickstart and docs. Only 05-15 touches crates/ostro Cargo and main, ci-local.sh and ci.yml. |
| 8 | 05-17 | Phase gate and freeze flip |

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 5-01-01 | 01 | 1 | (gate) | check | 04-{01..20}-SUMMARY + `grep '^status: passed' 04-VERIFICATION.md` + 02-16-SUMMARY + clean tree + `bash scripts/ci-local.sh --no-musl` | n/a | ⬜ pending |
| 5-01-02 | 01 | 1 | WIT-07 (contract), WF-05, WF-09, API-05, API-07, OPS-06 | unit + golden (append-only) | `cargo test -p ostro-types --locked && test -z "$(git diff crates/ostro-types/tests/golden | grep '^-[^-]')"` | ✅ (extended) | ⬜ pending |
| 5-01-03 | 01 | 1 | API-05 (status parity) | unit + insta | `cargo test -p ostro-api -p ostro-partition --locked` | ✅ (extended) | ⬜ pending |
| 5-02-01 | 02 | 2 | WF-05, OPS-06 (schema) | unit | `cargo test -p ostro-partition --test store --test store_digest` | ✅ (extended) | ⬜ pending |
| 5-02-02 | 02 | 2 | WF-09, OPS-06 (hooks) | integration | `cargo test -p ostro-partition --test timers --test apply_timer_input --test apply_core` | ✅ | ⬜ pending |
| 5-02-03 | 02 | 2 | (seams) | integration + proptest | `cargo test -p ostro-partition --test apply_seams --test rebuild_digest` | ❌ W0 | ⬜ pending |
| 5-03-01 | 03 | 2 | WF-03 | unit + proptest | `cargo test -p ostro-engine --lib` (incl. `replay::tests::gather_race_prop`) | ❌ W0 | ⬜ pending |
| 5-03-02 | 03 | 2 | WF-08 | integration (engine) | `cargo test -p ostro-engine --test patching` | ❌ W0 | ⬜ pending |
| 5-03-03 | 03 | 2 | WF-04, API-07 | integration | `cargo test -p ostro-engine --test live_activations && cargo test -p ostro-engine --lib token` | ❌ W0 | ⬜ pending |
| 5-04-01 | 04 | 2 | API-05/06/07 | unit | `cargo test -p ostro-proto -p ostro-api --lib && cargo build -p ostro-server` | ✅ (extended) | ⬜ pending |
| 5-04-02 | 04 | 2 | API-05/06/07 | integration + parity + insta | `cargo test -p ostro-api --test lifecycle_surface --test parity --test openapi` | ❌ W0 / ✅ parity | ⬜ pending |
| 5-05-01 | 05 | 2 | WIT-07 | unit (fixtures) | `cargo test -p ostro-witcheck --test semver_fixtures && cargo deny check` | ❌ W0 | ⬜ pending |
| 5-05-02 | 05 | 2 | WIT-07 | unit | `cargo test -p ostro-witcheck --test wit_freeze && cargo test -p ostro-types --test golden_frozen` | ❌ W0 | ⬜ pending |
| 5-05-03 | 05 | 2 | WIT-07 (review) | check (comment-only diff) | stripped-comment diff of runtime.wit vs HEAD is empty; engine + SDK build | n/a | ⬜ pending |
| 5-06-01/02 | 06 | 3 | WF-05, API-05 (signal/cancel), API-07 | integration (apply) | `cargo test -p ostro-partition --test apply_waits --test apply_cross` | ❌ W0 | ⬜ pending |
| 5-06-03 | 06 | 3 | WF-05 | proptest | `cargo test -p ostro-partition --test rebuild_digest` + failpoint suite | ✅ (extended) | ⬜ pending |
| 5-07-01/02 | 07 | 3 | WF-09, API-05 (terminate/retry), OPS-06 | integration (apply) | `cargo test -p ostro-partition --test apply_lifecycle --test apply_retention` | ❌ W0 | ⬜ pending |
| 5-07-03 | 07 | 3 | OPS-06, WF-09 | proptest | `cargo test -p ostro-partition --test rebuild_digest` + failpoint suite | ✅ (extended) | ⬜ pending |
| 5-08-01 | 08 | 3 | WF-03, WF-05 (SDK) | unit + engine | `cargo test --manifest-path sdks/rust/Cargo.toml && cargo test -p ostro-engine --test sdk_guests --test step_retry` | ✅ (extended) | ⬜ pending |
| 5-08-02 | 08 | 3 | WF-03/05/09, API-07 | integration (engine) | `cargo test -p ostro-engine --test phase5_guests` | ❌ W0 | ⬜ pending |
| 5-08-03 | 08 | 3 | SC-1 (example) | build | `cargo build --release --target wasm32-wasip2 --manifest-path examples/order-saga/Cargo.toml` + clippy | ❌ W0 | ⬜ pending |
| 5-09-01 | 09 | 4 | WF-05, API-07, WF-09 | unit (table) | `cargo test -p ostro-server --test derive_phase5 --test sink` | ❌ W0 | ⬜ pending |
| 5-09-02 | 09 | 4 | WF-08, WF-09, API-05 | integration | `cargo test -p ostro-partition --test dispatcher && cargo test -p ostro-server --test executor_phase5 --test executor --test leases` | ❌ W0 | ⬜ pending |
| 5-09-03 | 09 | 4 | OPS-06, WF-09 | unit + integration | `cargo test -p ostro-meta && cargo test -p ostro-server --test config` | ❌ W0 (meta_policies) | ⬜ pending |
| 5-10-01/02 | 10 | 4 | QA-02 | integration (default suite, all CI legs) | `cargo test -p ostro-engine --test replay_corpus` | ❌ W0 | ⬜ pending |
| 5-10-03 | 10 | 4 | QA-02 (format) | integration | `cargo test -p ostro-partition --test format_corpus` | ❌ W0 | ⬜ pending |
| 5-11-01 | 11 | 5 | WF-09, OPS-06 (start fields, policies) | integration | `cargo test -p ostro-api && cargo test -p ostro-server --test start_run_api --test runs_api` | ✅ (extended) | ⬜ pending |
| 5-11-02 | 11 | 5 | API-05, API-07, WF-05 | e2e (REST + gRPC) | `cargo test -p ostro-server --test lifecycle_api` | ❌ W0 | ⬜ pending |
| 5-11-03 | 11 | 5 | API-06 | e2e | `cargo test -p ostro-server --test lifecycle_api long_poll` | ❌ W0 | ⬜ pending |
| 5-12-01/02 | 12 | 6 | CLI-06 | unit | `cargo test -p ostro-cli` | ❌ W0 | ⬜ pending |
| 5-12-03 | 12 | 6 | CLI-06 | e2e (spawned binary) + insta | `cargo test -p ostro --test cli_verbs` | ❌ W0 | ⬜ pending |
| 5-13-01 | 13 | 6 | WF-03, WF-04 | e2e | `cargo test -p ostro-server --test semantics_e2e gather_race && cargo test -p ostro-server --test semantics_sleepers -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 5-13-02 | 13 | 6 | WF-05, WF-08, WF-09, API-05, API-07 | e2e | `cargo test -p ostro-server --test semantics_e2e` | ❌ W0 | ⬜ pending |
| 5-13-03 | 13 | 6 | OPS-06, API-06 | e2e | `cargo test -p ostro-server --test semantics_e2e -- continue_as_new retention` | ❌ W0 | ⬜ pending |
| 5-14-01/03 | 14 | 6 | SC-4 perf (QA-04 methodology) | benchmark (`#[ignore]`) | `OSTRO_BENCH_PROFILE=a cargo test --release -p ostro-server --test perf -- --ignored --nocapture` (WSL2 advisory; native NVMe human_needed) | ❌ W0 | ⬜ pending |
| 5-14-02 | 14 | 6 | SC-4 (start path) | integration | `cargo test -p ostro-partition --test apply_run_started --test dispatcher && cargo test -p ostro-server` | ❌ W0 | ⬜ pending |
| 5-15-01 | 15 | 7 | QA-01 (sites) | failpoint suites | `cargo build --release -p ostro --features failpoints` + per-crate failpoint tests (FAILPOINT_CRATES = ci.yml list) | ❌ W0 | ⬜ pending |
| 5-15-02 | 15 | 7 | QA-01 (checker) | unit (negative controls) | `cargo test -p ostro-crashtest --test check_controls` | ❌ W0 | ⬜ pending |
| 5-15-03 | 15 | 7 | QA-01 (smoke) | integration (`#[ignore]`, run by ci-local) | `OSTRO_CAMPAIGN_FAULTS=20 cargo test --release -p ostro-crashtest --features failpoints --test server_campaign -- --ignored` | ❌ W0 | ⬜ pending |
| 5-16-01 | 16 | 7 | SC-1, CLI-06 | e2e (CLI) | `cargo test -p ostro --test order_saga` | ❌ W0 | ⬜ pending |
| 5-16-02 | 16 | 7 | SC-1 (< 10 min) | containerised | `bash examples/order-saga/quickstart.sh` (docker; otherwise HUMAN_NEEDED + local cold-clone time) | ❌ W0 | ⬜ pending |
| 5-16-03 | 16 | 7 | docs | check | grep checks on docs/workflow-semantics.md and docs/03 | n/a | ⬜ pending |
| 5-17-01 | 17 | 8 | QA-01 | gate (long-running, sharded) | campaign 20 × 500 + report: total ≥ 10,000, lost = 0, duplicates = 0 | n/a | ⬜ pending |
| 5-17-02 | 17 | 8 | WIT-07, QA-02 | gate | `cargo test -p ostro-witcheck -p ostro-types --test golden_frozen && cargo test -p ostro-engine --test replay_corpus && cargo test -p ostro-partition --test format_corpus` + local tag | n/a | ⬜ pending |
| 5-17-03 | 17 | 8 | all | gate | `bash scripts/ci-local.sh` | n/a | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠ flaky / human_needed*

---

## Wave 0 Requirements

Test files and scaffolds are created by the task that first needs them (TDD tasks write tests first):

- [ ] `crates/ostro-partition/tests/{apply_seams,apply_waits,apply_lifecycle,apply_retention,apply_run_started,format_corpus}.rs` and `tests/common/{gen_waits,gen_lifecycle}.rs` (05-02, 05-06, 05-07, 05-10, 05-14)
- [ ] `crates/ostro-engine/tests/{patching,live_activations,phase5_guests,replay_corpus}.rs` and `replay::tests::gather_race_prop` (05-03, 05-08, 05-10)
- [ ] `crates/ostro-api/tests/lifecycle_surface.rs` (05-04)
- [ ] `crates/ostro-witcheck` crate, fixtures under `tests/wit-fixtures/`, and `crates/ostro-types/tests/golden_frozen.rs` (05-05)
- [ ] `crates/ostro-server/tests/{derive_phase5,executor_phase5,lifecycle_api,semantics_e2e,semantics_sleepers,perf}.rs` and `crates/ostro-meta/tests/meta_policies.rs` (05-09, 05-11, 05-13, 05-14)
- [ ] `crates/ostro/tests/{cli_verbs,order_saga}.rs` (05-12, 05-16)
- [ ] `crates/ostro-crashtest/{src/{server_target,workload,check,campaign}.rs, tests/{server_campaign,check_controls}.rs}` (05-15)
- [ ] Guests patch-v1/v2/v3 (05-03); race-fuzz, signals, timeouts, tokens, can-chain, crash-saga (05-08); `examples/order-saga` (05-08)
- [ ] Dev-deps `wit-component =0.261.0` (feature semver-check) and `wit-parser =0.261.0`, with transitive wat/wast checked by cargo-deny (05-05). reqwest promoted in ostro-cli (05-12).
- [ ] `failpoints` feature chain on `ostro`, `ostro-server`, `ostro-meta` and `ostro-engine`, with FAILPOINT_CRATES and ci.yml kept in sync (05-15)

---

## Manual-Only / Human-Needed Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| ≥ 5,000 trivial Rust steps/s (profile A), start → first step p50 < 5 ms | SC-4 (D15/D16) | This machine is WSL2 (≈ 6.8 ms per fsync). The gate needs native Linux NVMe with 8 vCPU. | On native Linux NVMe, run `OSTRO_BENCH_PROFILE=a cargo test --release -p ostro-server --test perf -- --ignored --nocapture`, then profile b. Record hardware and results in docs/benchmarks.md and here. WSL2 values are recorded as advisory by 05-14/05-17. |
| Replay corpus green on linux-aarch64-musl and macos-aarch64 | QA-02 | There is no git remote, so the CI legs cannot run (FOUND-05). | After the first push, confirm the CI legs are green. The corpus test is in the default `cargo test --workspace`. |
| order-saga quickstart < 10 min on a clean machine | SC-1 | It needs docker or a clean machine. | `bash examples/order-saga/quickstart.sh` with docker available. 05-16 records the result, or the local cold-clone equivalent plus HUMAN_NEEDED. |
| Real ENOSPC leg (size-limited tmpfs) | QA-01 (optional) | It needs root to mount. | `sudo mount -t tmpfs -o size=64m tmpfs /mnt/ostro-enospc`, then run the campaign's ENOSPC leg against that dir. Failpoint ENOSPC variants are the primary mechanism. |
| Review of adopted decisions D7 (step-local RNG), D8 (relaxed cold start, P=16) and D14 (WIT review: doc-only), and the WIT freeze flip | WIT-07 | The user was unavailable (YOLO/auto_advance), so research defaults were adopted. | Read 05-DECISIONS.md and docs/wit-0.1-freeze-review.md. Overrides after 05-17 are bound by the additive-only rule. |

---

## Gate results

(Filled by 05-17 Task 3: campaign report, corpus, freeze, ci-local step table, perf and cold-start measurements.)

## Human-needed

(Filled by 05-17 Task 3 with the outstanding items above and their status.)

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60 s for task-level checks
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
