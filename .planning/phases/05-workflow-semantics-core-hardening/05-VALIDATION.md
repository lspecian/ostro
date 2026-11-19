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
> Source: 05-RESEARCH.md § Validation Architecture, mapped onto plans 05-01..05-20. The plans were revised after the plan check, with 2 blockers and 15 warnings addressed. Adopted decisions: 05-DECISIONS.md.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test harness (`cargo test`, Rust 1.100.0), plus proptest 1.12, insta 1.49 and trybuild (SDK). Spawned-binary tests use `CARGO_BIN_EXE_ostro`. REST tests use reqwest without TLS, gRPC tests use the generated tonic clients, and failpoint tests use the `fail` crate in single-threaded suites. |
| **Config file** | None beyond Cargo.toml. Separate workspaces: `guests/Cargo.toml` (members glob), `sdks/rust/Cargo.toml`, and `examples/order-saga/Cargo.toml` (standalone, 05-10). `scripts/ci-local.sh` `FAILPOINT_CRATES` must equal the crates in the ci.yml "Failpoint suites" step. |
| **Guest build** | `tests/common::guest_wasm(name)` runs `cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml --target-dir target/guests` once per test process. |
| **Failpoint binary** | `cargo build --release -p ostro --features failpoints --target-dir target/failpoints` (05-18) |
| **Quick run command** | `cargo test -p <touched crate> --locked` |
| **Full suite command** | `bash scripts/ci-local.sh --no-musl` |
| **Phase gate commands** | 1. `bash scripts/ci-local.sh` (with musl).<br>2. The sharded kill campaign, in batches or in the background: `rm -rf target/campaign-gate`, then `OSTRO_CAMPAIGN_STATE_DIR=target/campaign-gate OSTRO_CAMPAIGN_FAULTS=10000 OSTRO_CAMPAIGN_SHARDS=20 [OSTRO_CAMPAIGN_SHARD_IDS=a-b] OSTRO_CAMPAIGN_PARALLEL=6 cargo test --release -p ostro-crashtest --features failpoints --test server_campaign -- --ignored --nocapture campaign`, then the same command with `report` (gate: `kills ≥ 10000`, `lost_acked_runs=0`, `duplicate_journal_entries=0`, `non_terminal_runs=0`).<br>3. `cargo test -p ostro-engine --test replay_corpus` (all CI legs).<br>4. Perf: `OSTRO_BENCH_PROFILE=a cargo test --release -p ostro-server --test perf -- --ignored --nocapture` (the native NVMe run is human_needed). |
| **Estimated runtime** | Quick: < 60 s. Full: ~8–12 min debug. Kill campaign: projected in 05-19 from a measured 200-kill run (≈ 1–2 h with 6 parallel shards expected). |

---

## Sampling Rate

- **After every task commit:** `cargo test -p <crate> --locked` plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings`.
  - Apply changes also run `cargo test -p ostro-partition --features failpoints --locked -- --test-threads=1`.
  - SDK changes also run `cargo test --manifest-path sdks/rust/Cargo.toml`.
- **After every plan wave:** `cargo test --workspace --locked`, the SDK workspace tests and, from 05-12 on, `cargo test -p ostro-engine --test replay_corpus --locked`.
- **Before `/gsd:verify-work`:** the 05-20 gate.
- **Max feedback latency:** 60 s for task-level checks.
- **Piped verify commands** use `set -o pipefail`, or grep for an explicit result line.

---

## Wave Structure

| Wave | Plans | Exclusive shared files |
|------|-------|------------------------|
| 1 | 05-01 | Gate. Phase 4 must be complete and verified (`passed`, or `human_needed` with by-design items only). |
| 2 | 05-02 store/apply seams, 05-03 engine, 05-04 API surface, 05-05 WIT tooling (+ W1–W8 doc comments), 05-06 SDK + D11, 05-07 meta pre-freeze encoding | Only 05-05 edits Cargo.toml, Cargo.lock and deny.toml. Only 05-07 edits ostro-meta. |
| 3 | 05-08 apply waits, 05-09 apply lifecycle/retention, 05-10 guests + order-saga | Only 05-10 edits ci-local.sh and ci.yml. |
| 4 | 05-11 server seam (+ resume re-arm, pause hook), 05-12 replay corpus | Only 05-12 edits Cargo.lock. |
| 5 | 05-13 ControlPlane lifecycle + start fields + long-poll | — |
| 6 | 05-14 CLI, 05-15 semantics e2e | 05-14 owns Cargo.lock, Cargo.toml and crates/ostro. 05-15 owns ostro-server tests/common, ci-local.sh and ci.yml (serial sleepers step). |
| 7 | 05-16 perf + start-path optimisations | — |
| 8 | 05-17 order-saga CLI + quickstart + docs (after 05-16), 05-18 failpoint chain + sites | 05-18 owns crates/ostro Cargo/main, ci-local.sh, ci.yml and Cargo.lock. 05-17 edits only crates/ostro/tests, examples/order-saga and docs, so there is no overlap. |
| 9 | 05-19 whole-server kill campaign | Owns ostro-crashtest, ci-local.sh, ci.yml and Cargo.lock. |
| 10 | 05-20 phase gate + freeze flip | — |

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 5-01-01 | 01 | 1 | (gate) | check (+ code vs `.planning/RESERVATIONS.md` script) | 04-{01..20}-SUMMARY present. `04-VERIFICATION.md` matches `^status: (passed\|human_needed)$`; under human_needed every human item must be by-design (remote aarch64/macOS CI legs, native NVMe perf, docker clean-machine timing), and gaps_found fails. 02-16-SUMMARY present, clean tree, `bash scripts/ci-local.sh --no-musl` green. | n/a | ⬜ pending |
| 5-01-02 | 01 | 1 | WIT-07 (contract), WF-05, WF-09, API-05, API-07, OPS-06 | unit + golden (append-only) | `cargo test -p ostro-types --locked && test -z "$(git diff crates/ostro-types/tests/golden \| grep '^-[^-]')"` | ✅ (extended) | ⬜ pending |
| 5-01-03 | 01 | 1 | API-05 (status parity) | unit + insta | `cargo test -p ostro-api -p ostro-partition --locked` | ✅ (extended) | ⬜ pending |
| 5-02-01 | 02 | 2 | WF-05, OPS-06 (schema incl. resumed_count, open_waits, s2s_due_ms) | unit | `cargo test -p ostro-partition --test store --test store_digest` | ✅ (extended) | ⬜ pending |
| 5-02-02 | 02 | 2 | WF-09, OPS-06, API-05 (terminal cleanup of waits + wait timers) | integration | `cargo test -p ostro-partition --test timers --test apply_timer_input --test apply_core` | ✅ | ⬜ pending |
| 5-02-03 | 02 | 2 | (seams, outcome codes) | integration + proptest | `cargo test -p ostro-partition --test apply_seams --test rebuild_digest` | ❌ W0 | ⬜ pending |
| 5-03-01 | 03 | 2 | WF-03 | unit + proptest | `cargo test -p ostro-engine --lib` (incl. `replay::tests::gather_race_prop`, hot_waitable false for signal/token) | ❌ W0 | ⬜ pending |
| 5-03-02 | 03 | 2 | WF-08 | integration (engine) | `cargo test -p ostro-engine --test patching --test determinism` | ❌ W0 | ⬜ pending |
| 5-03-03 | 03 | 2 | WF-04, API-07 | integration | `cargo test -p ostro-engine --test live_activations && cargo test -p ostro-engine --lib token` | ❌ W0 | ⬜ pending |
| 5-04-01 | 04 | 2 | API-05/06/07 | unit | `cargo test -p ostro-proto -p ostro-api --lib && cargo build --workspace --all-targets` | ✅ (extended) | ⬜ pending |
| 5-04-02 | 04 | 2 | API-05/06/07 | integration + parity + insta | `cargo test -p ostro-api --test lifecycle_surface --test parity --test openapi` | ❌ W0 / ✅ parity | ⬜ pending |
| 5-05-01 | 05 | 2 | WIT-07 | unit (fixtures) | `cargo test -p ostro-witcheck --test semver_fixtures && cargo deny check` | ❌ W0 | ⬜ pending |
| 5-05-02 | 05 | 2 | WIT-07 | unit | `cargo test -p ostro-witcheck --test wit_freeze && cargo test -p ostro-types --test golden_frozen` | ❌ W0 | ⬜ pending |
| 5-05-03 | 05 | 2 | WIT-07 (review + Phase 6 W1–W8 doc comments) | check | stripped-comment diff of runtime.wit vs the plan base commit (`target/05-05-base.sha`) is empty; W1–W8 markers grep (`effect_outcome_unknown`, `presign-get`, `sql_not_transactional`, `Idempotency-Key`); engine + SDK build | n/a | ⬜ pending |
| 5-06-01 | 06 | 2 | WF-03, WF-05, API-07 (SDK) | unit + build | `cargo test --manifest-path sdks/rust/Cargo.toml && cargo clippy --manifest-path sdks/rust/Cargo.toml --target wasm32-wasip2 -- -D warnings` | ✅ (extended) | ⬜ pending |
| 5-06-02 | 06 | 2 | D11 journal shape | integration | `cargo test -p ostro-engine --test sdk_cancellation --test sdk_guests --test step_retry && cargo test -p ostro-server --test sdk_e2e && cargo test -p ostro --test e2e_dev` | ❌ W0 | ⬜ pending |
| 5-07-01 | 07 | 2 | OPS-06, WF-09 (meta run policies; P8 Schedule fields + Webhook per D20) | unit (CAS) | `cargo test -p ostro-meta --locked && cargo build --workspace --all-targets` | ❌ W0 (meta_policies) | ⬜ pending |
| 5-07-02 | 07 | 2 | D20 (META_FORMAT bump, golden) | golden + negative | `cargo test -p ostro-meta --test meta_golden` | ❌ W0 | ⬜ pending |
| 5-08-01/02 | 08 | 3 | WF-05, API-05 (signal/cancel), API-07 | integration (apply; idempotent RegisterWait, open_waits, asserted read counts, D21 verbatim events + 1 MiB signal/token) | `cargo test -p ostro-partition --test apply_waits --test apply_cross` | ❌ W0 | ⬜ pending |
| 5-08-03 | 08 | 3 | WF-05 | proptest | `cargo test -p ostro-partition --test rebuild_digest` + failpoint suite | ✅ (extended) | ⬜ pending |
| 5-09-01/02 | 09 | 3 | WF-09, API-05 (terminate/retry), OPS-06 | integration (apply; RunCompleted actions, resumed_count, s2s_due_ms) | `cargo test -p ostro-partition --test apply_lifecycle --test apply_retention` | ❌ W0 | ⬜ pending |
| 5-09-03 | 09 | 3 | OPS-06, WF-09 | proptest | `cargo test -p ostro-partition --test rebuild_digest` + failpoint suite | ✅ (extended) | ⬜ pending |
| 5-10-01 | 10 | 3 | WF-03/05/08/09, API-07 | integration (engine) | `cargo test -p ostro-engine --test phase5_guests --test sdk_cancellation` | ❌ W0 | ⬜ pending |
| 5-10-02 | 10 | 3 | SC-1 (example) | build | `cargo build --release --target wasm32-wasip2 --manifest-path examples/order-saga/Cargo.toml` + clippy | ❌ W0 | ⬜ pending |
| 5-11-01 | 11 | 4 | WF-05, API-07, WF-09 (derive + rearm_unresolved) | unit (table) | `cargo test -p ostro-server --test derive_phase5 --test sink` | ❌ W0 | ⬜ pending |
| 5-11-02 | 11 | 4 | WF-08, WF-09, API-05 (revoke, resume re-arm, pause hook) | integration | `cargo test -p ostro-partition --test dispatcher && cargo test -p ostro-server --test executor_phase5 --test resume_rearm --test executor --test leases` | ❌ W0 | ⬜ pending |
| 5-11-03 | 11 | 4 | OPS-06, WF-09 | unit + integration | `cargo test -p ostro-meta && cargo test -p ostro-server --test config && cargo test -p ostro-server --lib retention` | ❌ W0 (meta_policies) | ⬜ pending |
| 5-12-01/02 | 12 | 4 | QA-02 | integration (default suite, all CI legs; terminal payload bytes asserted) | `cargo test -p ostro-engine --test replay_corpus` | ❌ W0 | ⬜ pending |
| 5-12-03 | 12 | 4 | QA-02 (format) | integration | `cargo test -p ostro-partition --test format_corpus` | ❌ W0 | ⬜ pending |
| 5-13-01 | 13 | 5 | WF-09, OPS-06 (start fields incl. dry_run, policies) | integration | `cargo build --workspace --all-targets && cargo test -p ostro-api && cargo test -p ostro-server --test start_run_api --test runs_api` | ✅ (extended) | ⬜ pending |
| 5-13-02 | 13 | 5 | API-05, API-07, WF-05 | e2e (REST + gRPC) | `cargo test -p ostro-server --test lifecycle_api` | ❌ W0 | ⬜ pending |
| 5-13-03 | 13 | 5 | API-06 | e2e | `cargo test -p ostro-server --test lifecycle_api long_poll` | ❌ W0 | ⬜ pending |
| 5-14-01/02 | 14 | 6 | CLI-06 | unit | `cargo test -p ostro-cli && cargo deny check` | ❌ W0 | ⬜ pending |
| 5-14-03 | 14 | 6 | CLI-06 | e2e (spawned binary) + insta | `cargo test -p ostro --test cli_verbs` | ❌ W0 | ⬜ pending |
| 5-15-01 | 15 | 6 | WF-03, WF-04 | e2e (sleepers in a serial CI step) | `cargo test -p ostro-server --test semantics_e2e gather_race && cargo test -p ostro-server --test semantics_sleepers -- --ignored --test-threads=1` | ❌ W0 | ⬜ pending |
| 5-15-02 | 15 | 6 | WF-05, WF-08, WF-09, API-05, API-07 (incl. resume after signal-wait / sleep timeout, D21 1 MiB payloads) | e2e | `cargo test -p ostro-server --test semantics_e2e` | ❌ W0 | ⬜ pending |
| 5-15-03 | 15 | 6 | OPS-06, API-06 | e2e | `cargo test -p ostro-server --test semantics_e2e -- continue_as_new retention` | ❌ W0 | ⬜ pending |
| 5-16-01/03 | 16 | 7 | SC-4 perf (D15/D16), OPS-06 (expiry cost) | benchmark (`#[ignore]`) | `OSTRO_BENCH_PROFILE=a cargo test --release -p ostro-server --test perf -- --ignored --nocapture` (WSL2 advisory; native NVMe human_needed) | ❌ W0 | ⬜ pending |
| 5-16-02 | 16 | 7 | SC-4 (start path) | integration | `cargo test -p ostro-partition && cargo test -p ostro-server && cargo test -p ostro-engine --test replay_corpus` | ❌ W0 (apply_run_started) | ⬜ pending |
| 5-17-01 | 17 | 8 | SC-1, CLI-06 | e2e (CLI) | `cargo test -p ostro --test order_saga` | ❌ W0 | ⬜ pending |
| 5-17-02 | 17 | 8 | SC-1 (< 10 min) | containerised | `bash examples/order-saga/quickstart.sh` prints `QUICKSTART_RESULT: within_budget` (docker) or `HUMAN_NEEDED:` + local cold-clone time | ❌ W0 | ⬜ pending |
| 5-17-03 | 17 | 8 | docs | check | grep checks on docs/workflow-semantics.md and docs/03 | n/a | ⬜ pending |
| 5-18-01 | 18 | 8 | QA-01 (sites) | failpoint suites | `cargo build --release -p ostro --features failpoints --target-dir target/failpoints` + meta/aot/executor/dispatcher failpoint tests | ❌ W0 | ⬜ pending |
| 5-18-02 | 18 | 8 | QA-01 (CI sync) | check + gate | FAILPOINT_CRATES ⊇ {ostro-server, ostro-meta, ostro-engine}, mirrored in ci.yml; `bash scripts/ci-local.sh --no-musl` | n/a | ⬜ pending |
| 5-19-01 | 19 | 9 | QA-01 (checker) | unit (negative controls) | `cargo test -p ostro-crashtest --test check_controls` | ❌ W0 | ⬜ pending |
| 5-19-02 | 19 | 9 | QA-01 (smoke) | integration (`#[ignore]`, run by ci-local) | `rm -rf target/campaign-smoke`; `OSTRO_CAMPAIGN_STATE_DIR=target/campaign-smoke OSTRO_CAMPAIGN_FAULTS=20 … --test server_campaign -- --ignored campaign` then `report` (kills ≥ 20, 0 violations) | ❌ W0 | ⬜ pending |
| 5-20-01 | 20 | 10 | QA-01 | gate (long-running, sharded) | Clean `target/campaign-gate`, fixed seed. Report: `kills ≥ 10000`, `lost_acked_runs=0`, `duplicate_journal_entries=0`, `non_terminal_runs=0` | n/a | ⬜ pending |
| 5-20-02 | 20 | 10 | WIT-07, QA-02 (+ meta golden pinned, RESERVATIONS rows flipped to exists) | gate | `cargo test -p ostro-witcheck && cargo test -p ostro-types --test golden_frozen && cargo test -p ostro-engine --test replay_corpus && cargo test -p ostro-partition --test format_corpus` + local tag, no remote | n/a | ⬜ pending |
| 5-20-03 | 20 | 10 | all | gate | `bash scripts/ci-local.sh` | n/a | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠ flaky / human_needed*

---

## Wave 0 Requirements

Test files are created by the task that first needs them (TDD tasks write tests first).

- [ ] **ostro-partition**: `crates/ostro-partition/tests/{apply_seams,apply_waits,apply_lifecycle,apply_retention,apply_rearm,apply_run_started,format_corpus,dispatcher_failpoints}.rs` and `tests/common/{gen_waits,gen_lifecycle}.rs`
- [ ] **ostro-engine**: `crates/ostro-engine/tests/{patching,live_activations,sdk_cancellation,phase5_guests,replay_corpus,aot_failpoints}.rs` and `replay::tests::gather_race_prop`
- [ ] **ostro-api**: `crates/ostro-api/tests/lifecycle_surface.rs`
- [ ] **WIT tooling**: the `crates/ostro-witcheck` crate with its fixtures, and `crates/ostro-types/tests/golden_frozen.rs`
- [ ] **ostro-server**: `crates/ostro-server/tests/{derive_phase5,executor_phase5,resume_rearm,lifecycle_api,semantics_e2e,semantics_sleepers,perf,failpoints_phase5}.rs`
- [ ] **ostro-meta**: `crates/ostro-meta/tests/{meta_policies,meta_golden,meta_failpoints}.rs` and `tests/golden/meta_v2.txt` (05-07, 05-18)
- [ ] **ostro binary**: `crates/ostro/tests/{cli_verbs,order_saga}.rs`
- [ ] **ostro-crashtest**: `src/{server_target,workload,check,campaign}.rs` and `tests/{server_campaign,check_controls}.rs`
- [ ] **Guests**:
  - patch-v1/v2/v3 (05-03);
  - sdk-cancel (05-06);
  - race-fuzz, signals, timeouts, tokens, can-chain, crash-saga, trivial-steps (05-10);
  - `examples/order-saga` (05-10).
- [ ] **Dev-deps**: `wit-component =0.261.0` (feature semver-check) and `wit-parser =0.261.0` (05-05); reqwest promoted to a normal dependency in ostro-cli (05-14).
- [ ] **Failpoints**: the `failpoints` feature chain on ostro, ostro-server, ostro-meta and ostro-engine, with FAILPOINT_CRATES and ci.yml kept in sync (05-18).

---

## Manual-Only / Human-Needed Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| ≥ 5,000 trivial Rust steps/s (profile A); start → first step p50 < 5 ms | SC-4 (D15/D16) | This machine is WSL2 (≈ 6.8 ms per fsync). The gate number needs native Linux NVMe with 8 vCPU. | On native Linux NVMe run `OSTRO_BENCH_PROFILE=a cargo test --release -p ostro-server --test perf -- --ignored --nocapture`, then profile b. Record the results in docs/benchmarks.md and here. 05-16 and 05-20 record the WSL2 values as advisory. |
| Replay corpus green on linux-aarch64-musl and macos-aarch64 | QA-02 | There is no git remote yet (FOUND-05). | After the first push, confirm the CI legs are green. The corpus test runs in the default `cargo test --workspace`. |
| order-saga quickstart < 10 min on a clean machine | SC-1 | Needs docker or a clean machine. | Run `bash examples/order-saga/quickstart.sh` with docker available. |
| Real ENOSPC on a size-limited tmpfs (optional) | QA-01 | Needs root to mount. | `sudo mount -t tmpfs -o size=64m tmpfs /mnt/ostro-enospc`, then run the campaign ENOSPC leg against it. The primary mechanism is the failpoint ENOSPC variants. |
| Review of adopted D7, D8 and D14, and of the WIT freeze flip | WIT-07 | The user was unavailable (YOLO/auto_advance). | Read 05-DECISIONS.md and docs/wit-0.1-freeze-review.md. |

---

## Gate results

(Filled in by 05-20 Task 3.)

## Human-needed

(Filled in by 05-20 Task 3.)

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60 s for task-level checks
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
