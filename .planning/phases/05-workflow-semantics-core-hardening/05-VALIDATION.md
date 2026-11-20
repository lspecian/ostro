---
phase: 5
slug: workflow-semantics-core-hardening
status: gate_red_check_vendored
nyquist_compliant: false
wave_0_complete: true
updated: 2026-11-20
created: 2026-11-19
---

# Phase 5 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Source: 05-RESEARCH.md § Validation Architecture, mapped onto plans 05-01..05-24 (05-21..05-24 split off by the final plan check, without renumbering). The plans were revised after the plan check, with 2 blockers and 15 warnings addressed. Adopted decisions: 05-DECISIONS.md.

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
| 1 | 05-01 | Gate (executing). |
| 2 | 05-02 store/apply seams, 05-03 engine, 05-04 API surface, 05-05 WIT tooling + doc comments, 05-06 SDK + D11, 05-07 meta pre-freeze encoding, **05-21** empty-log anchoring | Only 05-05 edits Cargo.toml, Cargo.lock and deny.toml. Only 05-07 edits ostro-meta and STATE.md. Only 05-21 edits ostro-log and partition node.rs. B2 carve-out: 05-06 and 05-07 share `ostro-server/tests/sdk_e2e.rs` (05-07 owns exactly the two ActiveVersion literals) and commit their own hunks through private indexes. |
| 3 | 05-08 apply waits, 05-09 apply lifecycle/retention, 05-10 guests + order-saga, **05-22** ready/attempt apply, **05-23** D22 clock, **05-24** D23 tenancy + upload | Only 05-10 edits ci-local.sh/ci.yml. 05-22 owns run.rs/values.rs/outcome.rs/query.rs/dispatcher.rs. 05-23 owns the engine replay sources and the three e2e test files. 05-24 owns ostro-api, modules.proto and control/{modules,runs,names}.rs. |
| 4 | 05-11 server seam, 05-12 replay corpus | Only 05-12 edits Cargo.lock. |
| 5 | 05-13 ControlPlane lifecycle + start fields + long-poll | — |
| 6 | 05-14 CLI, 05-15 semantics e2e | 05-14 owns Cargo.lock, Cargo.toml and crates/ostro. 05-15 owns ostro-server tests/common, ci-local.sh and ci.yml (serial sleepers step). |
| 7 | 05-16 perf + start-path optimisations | — |
| 8 | 05-17 order-saga CLI + quickstart + docs, 05-18 failpoint chain + sites | disjoint |
| 9 | 05-19 whole-server kill campaign | — |
| 10 | 05-20 phase gate + freeze flip | — |

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 5-01-01 | 01 | 1 | (gate) | check (+ code vs `.planning/RESERVATIONS.md` script) | 04-{01..20}-SUMMARY present. `04-VERIFICATION.md` matches `^status: (passed\|human_needed)$`; under human_needed every human item must be by-design (remote aarch64/macOS CI legs, native NVMe perf, docker clean-machine timing), and gaps_found fails. 02-16-SUMMARY present, clean tree, `bash scripts/ci-local.sh --no-musl` green. | n/a | ✅ green |
| 5-01-02 | 01 | 1 | WIT-07 (contract), WF-05, WF-09, API-05, API-07, OPS-06 | unit + golden (append-only) | `cargo test -p ostro-types --locked && test -z "$(git diff crates/ostro-types/tests/golden \| grep '^-[^-]')"` | ✅ (extended) | ✅ green |
| 5-01-03 | 01 | 1 | API-05 (status parity) | unit + insta | `cargo test -p ostro-api -p ostro-partition --locked` | ✅ (extended) | ✅ green |
| 5-02-01 | 02 | 2 | WF-05, OPS-06 (schema incl. resumed_count, open_waits, s2s_due_ms) | unit | `cargo test -p ostro-partition --test store --test store_digest` | ✅ (extended) | ✅ green |
| 5-02-02 | 02 | 2 | WF-09, OPS-06, API-05 (terminal cleanup of waits + wait timers) | integration | `cargo test -p ostro-partition --test timers --test apply_timer_input --test apply_core` | ✅ | ✅ green |
| 5-02-03 | 02 | 2 | (seams, outcome codes) | integration + proptest | `cargo test -p ostro-partition --test apply_seams --test rebuild_digest` | ❌ W0 | ✅ green |
| 5-03-01 | 03 | 2 | WF-03 | unit + proptest | `cargo test -p ostro-engine --lib` (incl. `replay::tests::gather_race_prop`, hot_waitable false for signal/token) | ❌ W0 | ✅ green |
| 5-03-02 | 03 | 2 | WF-08 | integration (engine) | `cargo test -p ostro-engine --test patching --test determinism` | ❌ W0 | ✅ green |
| 5-03-03 | 03 | 2 | WF-04, API-07 | integration | `cargo test -p ostro-engine --test live_activations && cargo test -p ostro-engine --lib token` | ❌ W0 | ✅ green |
| 5-04-01 | 04 | 2 | API-05/06/07 | unit | `cargo test -p ostro-proto -p ostro-api --lib && cargo build --workspace --all-targets` | ✅ (extended) | ✅ green |
| 5-04-02 | 04 | 2 | API-05/06/07 | integration + parity + insta | `cargo test -p ostro-api --test lifecycle_surface --test parity --test openapi` | ❌ W0 / ✅ parity | ✅ green |
| 5-05-01 | 05 | 2 | WIT-07 | unit (fixtures) | `cargo test -p ostro-witcheck --test semver_fixtures && cargo deny check` | ❌ W0 | ✅ green |
| 5-05-02 | 05 | 2 | WIT-07 | unit | `cargo test -p ostro-witcheck --test wit_freeze && cargo test -p ostro-types --test golden_frozen` | ❌ W0 | ✅ green |
| 5-05-03 | 05 | 2 | WIT-07 (review + Phase 6 W1–W8 doc comments) | check | stripped-comment diff of runtime.wit vs the plan base commit (`target/05-05-base.sha`) is empty; W1–W8 markers grep (`effect_outcome_unknown`, `presign-get`, `sql_not_transactional`, `Idempotency-Key`); engine + SDK build | n/a | ✅ green |
| 5-06-01 | 06 | 2 | WF-03, WF-05, API-07 (SDK) | unit + build | `cargo test --manifest-path sdks/rust/Cargo.toml && cargo clippy --manifest-path sdks/rust/Cargo.toml --target wasm32-wasip2 -- -D warnings` | ✅ (extended) | ✅ green |
| 5-06-02 | 06 | 2 | D11 journal shape | integration | `cargo test -p ostro-engine --test sdk_cancellation --test sdk_guests --test step_retry && cargo test -p ostro-server --test sdk_e2e && cargo test -p ostro --test e2e_dev` | ❌ W0 | ✅ green |
| 5-07-01 | 07 | 2 | OPS-06, WF-09 (meta run policies; P8 Schedule fields + Webhook per D20) | unit (CAS) | `cargo test -p ostro-meta --locked && cargo build --workspace --all-targets` | ❌ W0 (meta_policies) | ✅ green |
| 5-07-02 | 07 | 2 | D20 (META_FORMAT bump, golden) | golden + negative | `cargo test -p ostro-meta --test meta_golden` | ❌ W0 | ✅ green |
| 5-08-01/02 | 08 | 3 | WF-05, API-05 (signal/cancel), API-07 | integration (apply; idempotent RegisterWait, open_waits, asserted read counts, D21 verbatim events + 1 MiB signal/token, B1 signal-before-first-activation, W5 size limits) | `cargo test -p ostro-partition --test apply_waits --test apply_cross` | ❌ W0 | ✅ green |
| 5-08-03 | 08 | 3 | WF-05 | proptest | `cargo test -p ostro-partition --test rebuild_digest` + failpoint suite | ✅ (extended) | ✅ green |
| 5-09-01/02 | 09 | 3 | WF-09, API-05 (terminate/retry), OPS-06 | integration (apply; RunCompleted actions, resumed_count, s2s_due_ms) | `cargo test -p ostro-partition --test apply_lifecycle --test apply_retention` | ❌ W0 | ✅ green |
| 5-09-03 | 09 | 3 | OPS-06, WF-09 | proptest | `cargo test -p ostro-partition --test rebuild_digest` + failpoint suite | ✅ (extended) | ✅ green |
| 5-10-01 | 10 | 3 | WF-03/05/08/09, API-07 | integration (engine) | `cargo test -p ostro-engine --test phase5_guests --test sdk_cancellation` | ❌ W0 | ✅ green |
| 5-10-02 | 10 | 3 | SC-1 (example) | build | `cargo build --release --target wasm32-wasip2 --manifest-path examples/order-saga/Cargo.toml` + clippy | ❌ W0 | ✅ green |
| 5-11-01 | 11 | 4 | WF-05, API-07, WF-09 (derive + rearm_unresolved) | unit (table) | `cargo test -p ostro-server --test derive_phase5 --test sink` | ❌ W0 | ✅ green |
| 5-11-02 | 11 | 4 | WF-08, WF-09, API-05 (revoke, resume re-arm, pause hook) | integration | `cargo test -p ostro-partition --test dispatcher && cargo test -p ostro-server --test executor_phase5 --test resume_rearm --test executor --test leases` | ❌ W0 | ✅ green |
| 5-11-03 | 11 | 4 | OPS-06, WF-09 | unit + integration | `cargo test -p ostro-meta && cargo test -p ostro-server --test config && cargo test -p ostro-server --lib retention` | ❌ W0 (meta_policies) | ✅ green |
| 5-12-01/02 | 12 | 4 | QA-02 | integration (default suite, all CI legs; terminal payload bytes asserted) | `cargo test -p ostro-engine --test replay_corpus` | ❌ W0 | ✅ green |
| 5-12-03 | 12 | 4 | QA-02 (format) | integration | `cargo test -p ostro-partition --test format_corpus` | ❌ W0 | ✅ green |
| 5-13-01 | 13 | 5 | WF-09, OPS-06 (start fields incl. dry_run, policies) | integration | `cargo build --workspace --all-targets && cargo test -p ostro-api && cargo test -p ostro-server --test start_run_api --test runs_api` | ✅ (extended) | ✅ green |
| 5-13-02 | 13 | 5 | API-05, API-07, WF-05 | e2e (REST + gRPC) | `cargo test -p ostro-server --test lifecycle_api` | ❌ W0 | ✅ green |
| 5-13-03 | 13 | 5 | API-06 | e2e | `cargo test -p ostro-server --test lifecycle_api long_poll` | ❌ W0 | ✅ green |
| 5-14-01/02 | 14 | 6 | CLI-06 | unit | `cargo test -p ostro-cli && cargo deny check` | ❌ W0 | ✅ green |
| 5-14-03 | 14 | 6 | CLI-06 | e2e (spawned binary) + insta | `cargo test -p ostro --test cli_verbs` | ❌ W0 | ✅ green |
| 5-15-01 | 15 | 6 | WF-03, WF-04 | e2e (sleepers in a serial CI step) | `cargo test -p ostro-server --test semantics_e2e gather_race && cargo test -p ostro-server --test semantics_sleepers -- --ignored --test-threads=1` | ❌ W0 | ✅ green |
| 5-15-02 | 15 | 6 | WF-05, WF-08, WF-09, API-05, API-07 (incl. resume after signal-wait / sleep timeout, D21 1 MiB payloads) | e2e | `cargo test -p ostro-server --test semantics_e2e` | ❌ W0 | ✅ green |
| 5-15-03 | 15 | 6 | OPS-06, API-06 | e2e | `cargo test -p ostro-server --test semantics_e2e -- continue_as_new retention` | ❌ W0 | ✅ green |
| 5-16-01/03 | 16 | 7 | SC-4 perf (D15/D16), OPS-06 (expiry cost) | benchmark (`#[ignore]`) | `OSTRO_BENCH_PROFILE=a cargo test --release -p ostro-server --test perf -- --ignored --nocapture` (WSL2 advisory; native NVMe human_needed) | ❌ W0 | ⚠ human_needed (WSL2 advisory measured 2026-11-20; native NVMe gate pending) |
| 5-16-02 | 16 | 7 | SC-4 (start path) | integration | `cargo test -p ostro-partition && cargo test -p ostro-server && cargo test -p ostro-engine --test replay_corpus` | ❌ W0 (apply_run_started) | ✅ green |
| 5-17-01 | 17 | 8 | SC-1, CLI-06 | e2e (CLI) | `cargo test -p ostro --test order_saga` | ❌ W0 | ✅ green |
| 5-17-02 | 17 | 8 | SC-1 (< 10 min) | containerised | `bash examples/order-saga/quickstart.sh` prints `QUICKSTART_RESULT: within_budget` (docker) or `HUMAN_NEEDED:` + local cold-clone time | ❌ W0 | ⚠ human_needed (docker unusable on this host; local cold clone 254 s in 05-17) |
| 5-17-03 | 17 | 8 | docs | check | grep checks on docs/workflow-semantics.md and docs/03 | n/a | ✅ green |
| 5-18-01 | 18 | 8 | QA-01 (sites) | failpoint suites | `cargo build --release -p ostro --features failpoints --target-dir target/failpoints` + meta/aot/executor/dispatcher failpoint tests | ❌ W0 | ✅ green |
| 5-18-02 | 18 | 8 | QA-01 (CI sync) | check + gate | FAILPOINT_CRATES ⊇ {ostro-server, ostro-meta, ostro-engine}, mirrored in ci.yml; `bash scripts/ci-local.sh --no-musl` | n/a | ✅ green |
| 5-19-01 | 19 | 9 | QA-01 (checker) | unit (negative controls) | `cargo test -p ostro-crashtest --test check_controls` | ❌ W0 | ✅ green |
| 5-19-02 | 19 | 9 | QA-01 (smoke) | integration (`#[ignore]`, run by ci-local) | `rm -rf target/campaign-smoke`; `OSTRO_CAMPAIGN_STATE_DIR=target/campaign-smoke OSTRO_CAMPAIGN_FAULTS=20 … --test server_campaign -- --ignored campaign` then `report` (kills ≥ 20, 0 violations) | ❌ W0 | ✅ green |
| 5-20-01 | 20 | 10 | QA-01 | gate (long-running, sharded) | Clean `target/campaign-gate`, fixed seed. Report: `kills ≥ 10000`, `lost_acked_runs=0`, `duplicate_journal_entries=0`, `non_terminal_runs=0` | n/a | ✅ green (checker v2 / D26, 2026-11-20) |
| 5-20-02 | 20 | 10 | WIT-07, QA-02 (+ meta golden pinned, RESERVATIONS rows flipped to exists) | gate | `cargo test -p ostro-witcheck && cargo test -p ostro-types --test golden_frozen && cargo test -p ostro-engine --test replay_corpus && cargo test -p ostro-partition --test format_corpus` + local tag, no remote | n/a | ✅ green |
| 5-20-03 | 20 | 10 | all | gate | `bash scripts/ci-local.sh` | n/a | ❌ red (check-vendored.sh check 4, see Gate results) |

| 5-21-01/02 | 21 | 2 | QA-01 (P12 F3 empty-log anchoring) | integration | `cargo test -p ostro-log --test recovery_empty_anchor && cargo test -p ostro-partition --test startup_order` + both failpoint suites | ❌ W0 | ✅ green |
| 5-22-01/02 | 22 | 3 | API-05, WF-09 (P7 F3/F7, F9/F10, PF-F1) | integration | `cargo test -p ostro-partition --test apply_ready_attempts --test dispatcher --test apply_phase4` | ❌ W0 | ✅ green |
| 5-23-01/02 | 23 | 3 | WF-03 (D22 retry clock) | integration + e2e | `cargo test -p ostro-engine --test step_retry_clock --test sdk_guests && cargo test -p ostro-server --test sdk_e2e && cargo test -p ostro --test e2e_dev` | ❌ W0 | ✅ green |
| 5-24-01/02 | 24 | 3 | API-05 (D23 tenancy, P14 F4 upload name) | integration | `cargo test -p ostro-server --test tenancy_api --test modules_api --test start_run_api && cargo test -p ostro-api` | ❌ W0 | ✅ green |
| 5-25 | 25 | gap | API-05 (D24 RunStarted at start) | integration | `cargo test -p ostro-partition && cargo test -p ostro-server` (ci-local `cargo test`) | ✅ | ✅ green |
| 5-26 | 26 | gap | QA-01 (exclusive `<data>/LOCK`) | integration | `cargo test -p ostro-server` (ci-local `cargo test`) | ✅ | ✅ green |
| 5-27 | 27 | gap | WF-03 (D25 step retry is a cancellation point) | integration + e2e | `cargo test -p ostro-engine --test step_retry_cancel && cargo test -p ostro-server --test semantics_e2e` | ✅ | ✅ green |
| 5-20-D26 | 20 | 10 | QA-01 (checker: rejected terminate, D26) | unit (negative controls) | `cargo test -p ostro-crashtest --test check_controls` (`control_d26_rejected_terminate`, fails on the previous checker) | ✅ | ✅ green |
*Status: ⬜ pending · ✅ green · ❌ red · ⚠ flaky / human_needed. Statuses set by 05-20 on 2026-11-20 from the ci-local run on `3a6c592` (every automated row's command runs inside a green ci-local step) and the final gate campaign.*

---

## Wave 0 Requirements

Test files are created by the task that first needs them (TDD tasks write tests first).

- [x] **ostro-partition**: `crates/ostro-partition/tests/{apply_seams,apply_waits,apply_lifecycle,apply_retention,apply_rearm,apply_run_started,format_corpus,dispatcher_failpoints}.rs` and `tests/common/{gen_waits,gen_lifecycle}.rs`
- [x] **ostro-engine**: `crates/ostro-engine/tests/{patching,live_activations,sdk_cancellation,phase5_guests,replay_corpus,aot_failpoints}.rs` and `replay::tests::gather_race_prop`
- [x] **ostro-api**: `crates/ostro-api/tests/lifecycle_surface.rs`
- [x] **WIT tooling**: the `crates/ostro-witcheck` crate with its fixtures, and `crates/ostro-types/tests/golden_frozen.rs`
- [x] **ostro-server**: `crates/ostro-server/tests/{derive_phase5,executor_phase5,resume_rearm,lifecycle_api,semantics_e2e,semantics_sleepers,perf,failpoints_phase5}.rs`
- [x] **ostro-meta**: `crates/ostro-meta/tests/{meta_policies,meta_golden,meta_failpoints}.rs` and `tests/golden/meta_v2.txt` (05-07, 05-18)
- [x] **ostro binary**: `crates/ostro/tests/{cli_verbs,order_saga}.rs`
- [x] **ostro-crashtest**: `src/{server_target,workload,check,campaign}.rs` and `tests/{server_campaign,check_controls}.rs`
- [x] **Guests**:
  - patch-v1/v2/v3 (05-03);
  - sdk-cancel (05-06);
  - race-fuzz, signals, timeouts, tokens, can-chain, crash-saga, trivial-steps (05-10);
  - `examples/order-saga` (05-10).
- [x] **Dev-deps**: `wit-component =0.261.0` (feature semver-check) and `wit-parser =0.261.0` (05-05); reqwest promoted to a normal dependency in ostro-cli (05-14).
- [x] **Failpoints**: the `failpoints` feature chain on ostro, ostro-server, ostro-meta and ostro-engine, with FAILPOINT_CRATES and ci.yml kept in sync (05-18).

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

Recorded by 05-20 on 2026-11-20. Tree: `3a6c592` (= freeze commit `fc47569` + the D26
checker fix). Worktree build, `CARGO_TARGET_DIR=target/wt-05-20c`.

**Order of events (stated plainly).** The freeze commit `fc47569` (WIT `ostro:runtime@0.1.0`
FROZEN.toml, golden re-pin, storage format FROZEN v0.1, RESERVATIONS flip) and its local tag
`wit/ostro-runtime-v0.1.0` **landed before the final crash campaign had passed**. The
single-binary campaign re-run that followed flagged one violation (shard 00, see D26). The
user chose to **keep the freeze commit and its tag unchanged** ("Decision 2 do A") and to
narrowly fix the checker ("Decision 1 do A"). The checker was changed under **D26 with the
user's approval**; all 20 shards were then re-run from a clean state dir under the new checker.

### 1. Crash campaign (QA-01): PASS under checker v2 (D26)

- Settings: `OSTRO_CAMPAIGN_SEED=0x0520`, `FAULTS=10000`, `SHARDS=20`, `PARALLEL=6`,
  batches 0-5 / 6-11 / 12-17 / 18-19 (1671 / 1721 / 2017 / 1444 s; wall 04:14:32 → 06:08:46,
  6834 s).
- Failpoint server: `target/failpoints/release/ostro`, sha256
  `ddbd2bbb01f0f928ec6c0b1a76da8285ca5f3246115c8e78352c88055ecaabe6` (built at `c6b1c09`;
  no engine/server source change since, only WIT doc comments, so not rebuilt).
- Checker: `crates/ostro-crashtest/src/check.rs` git blob
  `bebc425b94c167fdb79c5c76fa31754dd9b69403` (commit `3a6c592`; the previous blob was
  `c7e2c3d5dc99c9873be4bc0a5124a9670ec5c1fc`).
- Report (`target/campaign-gate/report.txt`, provenance in `report-provenance.txt`):

```text
kills=10000 lost_acked_runs=0 duplicate_journal_entries=0 non_terminal_runs=0 lost_acked_signals=0 wrong_results=0 other_violations=0
faults_target=10000 shards=20/20 incomplete=[] by_variant=plain:5865 failpoint:3048 enospc:1087 failpoint_hits=4582 extra_kills=4155 rebuilds=420 acked_runs=120888
RESULT: PASS
```

- Every shard: 500 kills, 21 stateless rebuild-and-compare checks, final `ok`. 846
  `stop_rejected terminate` records across the shards' ack logs.
- Superseded state (checker v1, incl. the shard-00 violation history) is kept in
  `target/campaign-gate/superseded-checker-v1/`; earlier attempts in `superseded/`,
  `superseded-prefix/` and `first-attempt-08-10/`.

### 2. Replay corpus, format corpus, freeze: PASS

`cargo test -p ostro-witcheck`, `-p ostro-types --test golden_frozen`,
`-p ostro-engine --test replay_corpus` and `-p ostro-partition --test format_corpus` pass
inside ci-local's `cargo test` step (x86_64 gnu) and `musl zigbuild test` (x86_64 musl).
FROZEN.toml sha256 `611d2982b88e0884d55f0acb8f47691edbcc4c4b2a8918efaad4a7092d6bd98e`; the
local tag `wit/ostro-runtime-v0.1.0` points at `fc47569`; no git remote.

### 3. ci-local (full, with musl): 1 step FAILED

```text
================ ci-local summary ================
toolchain 1.100.0                    PASS  0s
cargo fmt --check                    PASS  2s
cargo clippy -D warnings             PASS  54s
cargo build (warning-free)           PASS  90s
sdk workspace fmt + clippy           PASS  33s
sdk workspace test + wasm build      PASS  38s
guests build (wasm32-wasip2)         PASS  0s
order-saga example                   PASS  36s
cargo test                           PASS  622s
semantics sleepers (serial)          PASS  50s
failpoint list sync (ci.yml)         PASS  0s
failpoint suites                     PASS  774s
server campaign smoke (20 kills)     PASS  261s
cargo deny check                     PASS  1s
check-vendored.sh                    FAIL  1s
check-deny-fixtures.sh               PASS  0s
no chronik/tokio-uring/prometheus    PASS  0s
workflow validation                  PASS  0s
zig + cargo-zigbuild present         PASS  1s
musl release (static, mimalloc)      PASS  193s
musl zigbuild test                   PASS  604s
==================================================
1 step(s) FAILED
```

The failure, verbatim:

```text
FAIL  4 session-path leaks or upstream build references
      tests/replay-corpus/crash-campaign-s00-a/module.wasm
      tests/replay-corpus/crash-campaign-s00-b/module.wasm
      tests/replay-corpus/crash-campaign-s00-c/module.wasm
      tests/replay-corpus/order-saga-cancelled/module.wasm
      tests/replay-corpus/order-saga-compensated/module.wasm
      tests/replay-corpus/order-saga-happy/module.wasm
```

The six corpus entries added by `82c9333` (05-20 Task 2) were exported from guests built
inside a session scratchpad worktree, so the wasm panic-location strings embed that local
path. 05-19's ci-local (before `82c9333`) passed this check; this is the first ci-local run
since. It is not a product or durability failure (the entries replay green on gnu and
musl), but the gate is red. Fixing it needs a decision: re-record the six entries from a
build in a neutral path (they are new in this plan, but their `module_sha256` changes) or
exempt corpus `module.wasm` files from check 4. Not fixed here; listed in deferred-items.md.

### 4. Performance (WSL2 advisory, D15)

Profile A, inproc, P=16, release, fsync on; quiet machine (load 1.2 at start), raw fsync p50
2.56-2.71 ms. Command as in docs/benchmarks.md.

| clients | runs | steps/s | runs/s | start→first step p50 / p99 | flushes/s | CPU (cores) | bytes on disk | target |
|---|---|---|---|---|---|---|---|---|
| 64 | 2000 | **30,517 / 30,168 / 30,116** | 3,012-3,052 | 9.76-10.22 / 18.1-21.3 ms | 742-748 | 3.3-3.5 | 8.17-8.18 MB | ≥ 5,000 steps/s (Phase 5 exit), NFR-2.5 10,000: **met on WSL2** |
| 1 | 300 | 936 / 975 / 959 | 94-97 | **3.52 / 6.16, 3.50 / 5.62, 3.59 / 6.22 ms** | 697-705 | 0.34 | 2.18 MB | p50 < 5 ms (NFR-2.4): **met on WSL2** |

Every run verified all steps on disk after a restart (`steps_verified` = `steps_acked`).
The gate itself is native Linux NVMe (human_needed below).

### 5. `ostro dev` startup (D8, debug binary)

`cargo test -p ostro --test dev_startup -- --include-ignored --nocapture`: `--ephemeral`
100 ms, warm restart 164 ms (both < 1 s, asserted), cold first run 1,407-1,474 ms (median
1,459 ms, measured only).

## Human-needed

a. **Native Linux NVMe perf gate (D15/D16).** On native Linux, NVMe, 8 vCPU, nothing else
   running, run each three times and keep the median:
   `OSTRO_BENCH_PROFILE=a cargo test --release -p ostro-server --test perf --locked -- --ignored --nocapture`,
   `OSTRO_BENCH_PROFILE=a OSTRO_BENCH_CONCURRENCY=1 OSTRO_BENCH_RUNS=300 cargo test --release -p ostro-server --test perf --locked -- --ignored --nocapture`,
   then profiles `b` and `expiry`. Pass: steps/s ≥ 5,000 and idle p50 < 5 ms.
b. **aarch64-musl and macOS-aarch64 CI legs** replaying the corpus after the first push
   (QA-02, FOUND-05): confirm `cargo test -p ostro-engine --test replay_corpus --locked` is
   green on both legs of `.github/workflows/ci.yml`.
c. **Clean-machine quickstart in docker** (SC-1): `bash examples/order-saga/quickstart.sh`
   on a machine where `docker info` succeeds; expect `QUICKSTART_RESULT: within_budget`.
d. **Optional real-ENOSPC leg:** `sudo mount -t tmpfs -o size=64m tmpfs /mnt/ostro-enospc`,
   then run the campaign with `OSTRO_CAMPAIGN_STATE_DIR` on that mount.
e. **User review** of the adopted decisions D7, D8 and D14 and of the freeze flip
   (`05-DECISIONS.md`, `docs/wit-0.1-freeze-review.md`). The user has already chosen to keep
   the freeze commit `fc47569` and tag as they are, and approved D26.
f. **Decision on the check-vendored failure** (Gate results §3): re-record the six corpus
   entries from a neutral build path, or exempt corpus `module.wasm` from check 4.

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 60 s for task-level checks
- [ ] `nyquist_compliant: true` set in frontmatter (not set: row 5-20-03, ci-local, is red on check-vendored.sh)

**Approval:** pending (gate red on check-vendored.sh; human_needed items a-f)
