---
phase: 7
slug: workers-placement-flow-control
status: complete
nyquist_compliant: true
wave_0_complete: true
created: 2026-11-19
---

# Phase 7: Validation Strategy

> This is the per-phase validation contract for feedback sampling during execution.
> Source: 07-RESEARCH.md § Validation Architecture, mapped onto plans 07-01..07-22 (revised after the plan check: 7 blockers, 14 warnings). Adopted decisions: 07-DECISIONS.md (D-W1..D-W25).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test harness (`cargo test`), proptest 1.12 (`rebuild_digest` + `gen_pause`, `fair.rs`), insta (OpenAPI snapshot), `fail` 0.5.1 failpoints (`worker::`, `placement::`, `flow::` plus Phase 5 `executor::`/`dispatcher::`), spawned binaries via `CARGO_BIN_EXE_ostro` (failpoint build in `target/failpoints`), ostro-testkit `mock-http` (`MockProvider /pay`) and `canary`, the in-process fake Execute server (07-07), the in-process server + worker harness (`crates/ostro-server/tests/common/p7_workers.rs`, 07-09), the spawned harness (`crates/ostro/tests/common/p7.rs`, 07-16). |
| **Config** | `scripts/ci-local.sh`. `FAILPOINT_CRATES` gains `ostro-worker` (07-02) and must equal the ci.yml "Failpoint suites" step. 07-22 adds the Phase 7 spawned suites and the optional `workflow_dispatch` jobs `acme-pebble` and `keda-kind`. |
| **Guest build** | `cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml --target-dir target/guests` (new guests from 07-07: `long-journal`, `pay-flow`, `cpu-burn`; `retry-probe` and `sdk-basic` reused). |
| **External services** | None locally required. Pebble ACME is env-gated (`OSTRO_TEST_ACME_DIRECTORY`, `OSTRO_TEST_ACME_CA`, `OSTRO_TEST_ACME_HTTP_PORT`). Real KEDA is human_needed (D-W22). **The microk8s cluster reachable via kubectl on this machine is never used.** |
| **Quick run command** | `cargo test -p <crate> --test <suite> --locked` (each < 60 s), e.g. `cargo test -p ostro-partition --test dispatcher --test dispatcher_flow --locked`, `cargo test -p ostro-worker --locked` |
| **Full suite command** | `bash scripts/ci-local.sh --no-musl` |
| **Phase gate commands** | 1. `bash scripts/ci-local.sh` (with musl).<br>2. `cargo test -p ostro --test worker_smoke --test worker_e2e --test worker_failover --test flow_e2e --test keda_sim --locked -- --test-threads=1` and `cargo test -p ostro --features failpoints --test worker_failover --locked -- --test-threads=1 sigstop_zombie`.<br>3. `OSTRO_CHAOS_RUNS=10000 OSTRO_CHAOS_SHARDS=20 bash scripts/fencing-chaos.sh` (sharded, resumable).<br>4. `cargo test -p ostro-engine --test replay_corpus --locked`.<br>5. `bash -c 'set -euo pipefail; cargo deny check; out=$(cargo tree --workspace -e normal --target all --locked --prefix none); ! grep -Eq "^(chrono|ring|openssl-sys|webpki-roots|native-tls) v" <<<"$out"'` |
| **Estimated runtime** | Quick: < 60 s. Full: ~12–18 min debug. Spawned e2e suites: a few minutes each. Chaos campaign: 1–3 h (sharded, resumable). |

---

## Sampling Rate

- **After every task commit:** the task's `<automated>` command, plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings`.
  - Dispatcher tasks also run the 04-14 `tests/dispatcher.rs` suite (it must stay green with the default seams).
  - executor.rs edits (07-08, 07-12) also run `cargo test -p ostro-server --test executor --test leases --locked`.
- **After every plan wave:** `cargo test --workspace --locked` + `cargo test -p ostro-engine --test replay_corpus --locked` + the failpoint suites.
- **Before `/gsd:verify-work`:** the 07-22 gate.
- **Max feedback latency:** 60 s for task-level checks. Spawned-binary suites and the chaos campaign are wave/gate level.
- Piped verify commands use `set -o pipefail`; cargo test filters go after `--`; no `|| true`; filtered runs must report ≥ 1 passed test (W7).

---

## Wave Structure

| Wave | Plans | Exclusive shared files |
|------|-------|------------------------|
| 1 | 07-01 gate (Phase 6 + F1–F4/F7/F8) + reservations + decisions | RESERVATIONS.md, STATE.md |
| 2 | 07-02 deps + partition contracts + seam; 07-03 format contract (parallel, disjoint files) | 07-02: Cargo files, deny.toml, leader/{mod,placement,flow,fair}.rs, partition lib.rs, ostro-server {server,config,lib,p7}.rs, control/mod.rs (p7 field), ostro-api error.rs, ci-local.sh, ci.yml. 07-03: command.rs + golden, apply/{mod,outcome,pause}.rs, docs/storage-format.md. **Cross-phase coordination point with Phase 8.** |
| 3 | 07-04 pause apply, 07-05 dispatcher, 07-06 TLS, 07-07 worker crate + guests, 07-11 API surface | 07-04: apply/{pause,run,terminal}.rs, sink.rs (one arm), partition tests/common + rebuild_digest. 07-05: **dispatcher.rs (vs 08-16)**, fair.rs, ostro-server tests/common/mod.rs (literal fix only). 07-06: tls/*. 07-07: crates/ostro-worker/**, execute.proto, guests. 07-11: **ostro-api/**, ostro-proto/** (vs 08-10)**. |
| 4 | 07-08 flow + rate limit + auto-pause, 07-09 worker service, 07-10 ACME, 07-15 CLI | 07-08: flow/*, control/runs.rs, **executor.rs**, p7.rs. 07-09: workers/{registry,service,join}.rs, tests/common/p7_workers.rs, execute.proto. 07-10: tls/acme.rs. 07-15: **main.rs, ostro-cli commands/mod.rs (vs 08-12/08-18)**. |
| 5 | 07-12 remote runner + placement, 07-13 spill, 07-14 control-plane impl | 07-12: workers/{remote_executor,placement,selector}.rs, **executor.rs** (`sink_for`), p7_workers.rs, execute.proto. 07-13: workers/spill.rs. 07-14: **control/mod.rs (vs 08-10)**, control/p7_*.rs, control/runs.rs. |
| 6 | 07-16 demand + manifests + spawned harness | workers/demand.rs, deploy/**, crates/ostro/tests/common/p7.rs |
| 7 | 07-17 SC1, 07-18 SC2, 07-19 SC4, 07-20 SC5, 07-21 SC3 | one new test file each (+ scripts/fencing-chaos.sh for 07-21) |
| 8 | 07-22 phase gate | docs, ci-local.sh, ci.yml, RESERVATIONS flip, VALIDATION, DECISIONS, STATE |

---

## Per-Task Verification Map

Every filtered `cargo test` in a plan's verify captures its output and requires `test result: ok. N passed` with N ≥ 1 (W7).

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 7-01-01 | 01 | 1 | (gate) | check + reservations script | 06-{01..20}-SUMMARY; `06-VERIFICATION.md` by-design only; F1–F4, F7 (`first_activation`), F8 (`expires_at_ms`) greps; RESERVATIONS greps | n/a | ✅ |
| 7-01-02 | 01 | 1 | (decisions) | grep | STATE.md: Phase 07 decisions D-W1..D-W25, `[USER REVIEW] D-W3/D-W8/D-W11`, coordination concern | n/a | ✅ |
| 7-02-01 | 02 | 2 | SEC-01/WORK-01 (deps, proto) | deny + tree checks + round-trip | `cargo deny check` + no ring/chrono/openssl/webpki-roots + `cargo tree -d --depth 0 -e normal --workspace --locked` no banned dups + `cargo test -p ostro-worker-proto --locked` | ✅ W0 (wire.rs) | ✅ |
| 7-02-02 | 02 | 2 | DISP-03..05, WORK-03 (contracts) | unit | `cargo test -p ostro-partition --test p7_contract --test dispatcher --locked` | ✅ W0 (p7_contract) | ✅ |
| 7-02-03 | 02 | 2 | all (seam, config) | integration | `cargo test -p ostro-server --test config --test executor --test leases --locked && cargo test -p ostro-api --locked && bash scripts/ci-local.sh --no-musl` | ✅ (extended) | ✅ |
| 7-03-01 | 03 | 2 | OPS-01 (format contract) | unit + golden (append-only) | `cargo test -p ostro-types --locked` + golden diff vs `target/07-03-base.sha` + `cargo test -p ostro-partition --test p7_format --test apply_core --locked` | ✅ W0 (p7_format) | ✅ |
| 7-04-01 | 04 | 3 | OPS-01 | unit (apply) incl. pause → timeout → RetryRun | `cargo test -p ostro-partition --test pause --test apply_core --test apply_phase4 --test timers --test apply_timer_input --locked && cargo build -p ostro-server --locked` | ✅ W0 (pause.rs) | ✅ |
| 7-04-02 | 04 | 3 | OPS-01 | proptest (rebuild digest, RetryRun in gen_pause) + failpoints | `PROPTEST_CASES=256 cargo test -p ostro-partition --test rebuild_digest --locked` + failpoint run | ✅ W0 (gen_pause.rs) | ✅ |
| 7-05-01 | 05 | 3 | DISP-04, DISP-03 | unit + proptest | `cargo test -p ostro-partition --lib --locked -- leader::fair` (non-vacuous) | ✅ W0 | ✅ |
| 7-05-02 | 05 | 3 | DISP-03/04/05, WORK-03, OPS-01 | integration (fakes) incl. blocked-queue-does-not-starve and re-activation-not-throttled | `cargo test -p ostro-partition --test dispatcher --test dispatcher_flow --locked` + server executor/leases + `grep interval(` empty | ✅ W0 (dispatcher_flow.rs) | ✅ |
| 7-06-01 | 06 | 3 | SEC-01 | unit/integration | `cargo test -p ostro-server --test tls_ca --locked` + no ring | ✅ W0 | ✅ |
| 7-06-02 | 06 | 3 | SEC-01 | integration (ALPN per mode) | `cargo test -p ostro-server --test tls_listener --test tls_ca --locked` | ✅ W0 | ✅ |
| 7-07-01 | 07 | 3 | WORK-01 | integration (fake server) incl. re-join fallback + failpoints | `cargo test -p ostro-worker --test protocol --locked` + `--features failpoints` | ✅ W0 | ✅ |
| 7-07-02 | 07 | 3 | WORK-02, WORK-06 | integration (real guests) | guests build + `cargo test -p ostro-worker --test protocol --test module_cache --locked` | ✅ W0 | ✅ |
| 7-11-01 | 11 | 3 | OPS-01, WORK-05, DISP-03, WORK-01 (surface) | unit | `cargo test -p ostro-proto -p ostro-api --locked -- phase7` (non-vacuous) | ✅ W0 (phase7_surface.rs) | ✅ |
| 7-11-02 | 11 | 3 | (surface) | parity + OpenAPI semantic superset | `cargo test -p ostro-api --locked` + `openapi_superset` vs the base snapshot + server suites | ✅ (extended) | ✅ |
| 7-08-01 | 08 | 4 | DISP-03, DISP-04, DISP-05 (throttle), OPS-01 | integration + GCRA unit tests | `cargo test -p ostro-server --test flow_control --locked` + `--lib -- flow::` | ✅ W0 | ✅ |
| 7-08-02 | 08 | 4 | DISP-05 (rate limit) | integration | `cargo test -p ostro-server --test flow_control --locked -- rate_limit` + start/runs API suites | ✅ W0 | ✅ |
| 7-08-03 | 08 | 4 | OPS-01 (auto-pause) | integration | `cargo test -p ostro-server --test auto_pause --test executor --test leases --locked` | ✅ W0 | ✅ |
| 7-09-01 | 09 | 4 | WORK-01 | unit/integration incl. token expiry (F8) | `cargo test -p ostro-server --test worker_session --locked -- registry join_service` | ✅ W0 | ✅ |
| 7-09-02 | 09 | 4 | WORK-01, SEC-01 (mTLS, peer identity, external CA rule) | integration (in-process worker) | `cargo test -p ostro-server --test worker_session --locked` | ✅ W0 | ✅ |
| 7-10-01 | 10 | 4 | SEC-01 (ACME) | unit (fake ACME) | `cargo test -p ostro-server --test acme --locked` | ✅ W0 | ✅ |
| 7-10-02 | 10 | 4 | SEC-01 (ACME) | env-gated (Pebble) | `cargo test -p ostro-server --test acme --locked -- pebble` (SKIP without env) | ✅ W0 | ✅ (Pebble v2.10.3 locally in 07-10: order + real HTTP-01/TLS-ALPN-01; SKIP without env in the gate) |
| 7-15-01 | 15 | 4 | OPS-01, WORK-05, DISP-03 (CLI) | integration (fake control plane) | `cargo test -p ostro --test cli_p7 --locked -- verbs` | ✅ W0 | ✅ |
| 7-15-02 | 15 | 4 | WORK-01 (worker process), SEC-01 (ready line) | spawned binary | `cargo test -p ostro --test cli_p7 --test cli --test dev_startup --locked` | ✅ W0 | ✅ |
| 7-12-01 | 12 | 5 | WORK-04, WORK-03 (embedded-first overflow) | unit | `cargo test -p ostro-server --lib --locked -- workers::selector workers::placement` | ✅ W0 | ✅ |
| 7-12-02 | 12 | 5 | WORK-01, WORK-02, WORK-06 (+ secrets, literal redacted values, sink_for) | integration | `cargo test -p ostro-server --test worker_remote --test worker_paging --test executor --test leases --locked` | ✅ W0 | ✅ |
| 7-13-01 | 13 | 5 | WORK-03 (exit rule, sustained overload = 1 transition) | unit (paused time) | `cargo test -p ostro-server --test placement_spill --locked` + `--lib -- workers::spill` | ✅ W0 | ✅ |
| 7-13-02 | 13 | 5 | WORK-03 | integration | `cargo test -p ostro-server --test placement_spill --locked` | ✅ W0 | ✅ |
| 7-14-01 | 14 | 5 | OPS-01, DISP-03 | integration (REST + gRPC) | `cargo test -p ostro-server --test p7_control --locked -- flow pause retry cancel terminate` | ✅ W0 | ✅ |
| 7-14-02 | 14 | 5 | WORK-01, WORK-05 | integration | `cargo test -p ostro-server --test p7_control --locked` | ✅ W0 | ✅ |
| 7-16-01 | 16 | 6 | WORK-05 | integration + ownership grep | `cargo test -p ostro-server --test demand --locked` + no foreign demand setter | ✅ W0 | ✅ |
| 7-16-02 | 16 | 6 | WORK-05 (manifests) | offline structural check | `bash deploy/check-manifests.sh` | ✅ W0 | ✅ |
| 7-16-03 | 16 | 6 | WORK-01 (binary smoke, restart resume) | spawned e2e | `cargo test -p ostro --test worker_smoke --locked -- --test-threads=1` + `restart_resume` | ✅ W0 | ✅ |
| 7-17-01 | 17 | 7 | WORK-01, WORK-02, WORK-06 (SC1) | spawned e2e | `cargo test -p ostro --test worker_e2e --locked -- --test-threads=1 join paging module_cache secret_never_on_worker` | ✅ W0 | ✅ |
| 7-17-02 | 17 | 7 | SEC-01 (SC1) | spawned e2e (+ env-gated ACME) | `cargo test -p ostro --test worker_e2e --locked -- --test-threads=1 listener` | ✅ W0 | ✅ |
| 7-18-01 | 18 | 7 | WORK-03, WORK-04 (SC2) | spawned e2e | `cargo test -p ostro --test worker_failover --locked -- --test-threads=1 auto_spill oscillation sustained_overload selectors` | ✅ W0 | ✅ |
| 7-18-02 | 18 | 7 | WORK-01 failover (SC2) | spawned e2e + failpoints | `cargo test -p ostro --test worker_failover --locked -- --test-threads=1 kill_redispatch` + `--features failpoints … sigstop_zombie` | ✅ W0 | ✅ |
| 7-19-01 | 19 | 7 | DISP-03, DISP-04, DISP-05 (SC4) | spawned e2e | `cargo test -p ostro --test flow_e2e --locked -- --test-threads=1 queue_limits weights priority keyed_fairness throttle_vs_rate_limit` | ✅ W0 | ✅ |
| 7-19-02 | 19 | 7 | OPS-01 (SC4) | spawned e2e incl. timeout → /retry | `cargo test -p ostro --test flow_e2e --locked -- --test-threads=1 pause timeout_while_paused` | ✅ W0 | ✅ |
| 7-20-01 | 20 | 7 | WORK-05 (SC5) | unit (scaler math) | `cargo test -p ostro --test keda_sim --locked -- scaler` | ✅ W0 | ✅ |
| 7-20-02 | 20 | 7 | WORK-05 (SC5) | spawned e2e (simulated KEDA) | `cargo test -p ostro --test keda_sim --locked -- --test-threads=1 zero_to_n_to_zero` | ✅ W0 | ✅ simulated KEDA (real ScaledObject ⚠ human_needed) |
| 7-21-01 | 21 | 7 | QA-03 (SC3) | chaos smoke (failpoints) | `cargo test -p ostro --features failpoints --test fencing_chaos --locked -- --test-threads=1 chaos_smoke` | ✅ W0 | ✅ |
| 7-21-02 | 21 | 7 | QA-03 (SC3) | chaos campaign (10,000 runs, ignored) | `bash scripts/fencing-chaos.sh` → report.json with double_charges 0, lost 0, fenced_sends 0, raw_duplicates_without_leader_kill 0, unattributed_duplicates 0, runs 10000 | ✅ W0 | ✅ (fix-0721 re-run: 10,000 runs PASS, lost 0; first run FAIL lost 7 → D-W28) |
| 7-22-01 | 22 | 8 | all | full gate | `bash scripts/ci-local.sh` + deny + tree/dup checks + CI greps + RESERVATIONS flip | ✅ | ❌ (1 red: flow_e2e throttle_vs_rate_limit, G1) |
| 7-23-01 | 23 | gap | DISP-05 | G1 fix | `cargo test -p ostro-partition --test dispatcher_flow -- --test-threads=1 throttle_on_first` + `cargo test -p ostro --test flow_e2e -- throttle_vs_rate_limit` (15 sequential runs) | ✅ | ✅ |
| 7-22-02 | 22 | 8 | all | docs + records | docs exist; VALIDATION "Gate results"/"Human-needed"; DECISIONS "Spike outcomes"; STATE `[Phase 07 / 07-22]` | n/a | ✅ |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠ flaky / human_needed*

---

## Wave 0 Requirements

Test files and fixtures created inside the plans that first need them (no framework install needed):

- [x] `crates/ostro-partition/tests/p7_contract.rs`, `crates/ostro-worker-proto/tests/wire.rs`, `crates/ostro-worker/tests/failpoints.rs` (07-02)
- [x] `crates/ostro-partition/tests/p7_format.rs` (07-03)
- [x] `crates/ostro-partition/tests/pause.rs`, `tests/common/gen_pause.rs` (07-04)
- [x] `crates/ostro-partition/tests/dispatcher_flow.rs` + fake FlowControl/Placement (07-05)
- [x] `crates/ostro-server/tests/{tls_ca,tls_listener}.rs` (07-06)
- [x] `crates/ostro-worker/tests/{protocol,module_cache}.rs` + `tests/common/fake_server.rs`; guests `long-journal`, `pay-flow`, `cpu-burn` (07-07)
- [x] `crates/ostro-server/tests/{flow_control,auto_pause}.rs` (07-08)
- [x] `crates/ostro-server/tests/worker_session.rs` + `tests/common/p7_workers.rs` (07-09)
- [x] `crates/ostro-server/tests/acme.rs` (07-10)
- [x] `crates/ostro-api/tests/phase7_surface.rs` (incl. `openapi_superset`) (07-11)
- [x] `crates/ostro-server/tests/{worker_remote,worker_paging}.rs` (07-12), `placement_spill.rs` (07-13), `p7_control.rs` (07-14), `demand.rs` (07-16)
- [x] `crates/ostro/tests/cli_p7.rs` (07-15); `common/p7.rs`, `worker_smoke.rs` (07-16); `worker_e2e.rs` (07-17), `worker_failover.rs` (07-18), `flow_e2e.rs` (07-19), `keda_sim.rs` (07-20), `fencing_chaos.rs` + `scripts/fencing-chaos.sh` (07-21)
- [x] `deploy/keda/*.yaml`, `deploy/k8s/*.yaml`, `deploy/check-manifests.sh` (07-16)

---

## Manual-Only / Human-Needed Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Real KEDA ScaledObject drives a worker Deployment 0 → N → 0 | WORK-05 (SC5) | No Docker/kind/k3d in WSL; the reachable microk8s cluster is off-limits to plans (D-W22) | Either run the `keda-kind` `workflow_dispatch` job (07-22) after the first push, or, on a cluster the user explicitly approves: install KEDA (helm), deploy `deploy/k8s/*.yaml` and `deploy/keda/scaledobject-metrics-api.yaml`, start a burst on queue `heavy`, observe replicas 0 → N → 0. |
| ACME against Pebble (if Pebble did not build locally) | SEC-01 | Go toolchain / network dependent (D-W23) | Build Pebble (`go install github.com/letsencrypt/pebble/v2/cmd/pebble@<tag>`), start it, then `OSTRO_TEST_ACME_DIRECTORY=https://127.0.0.1:14000/dir OSTRO_TEST_ACME_CA=<pebble.minica.pem> OSTRO_TEST_ACME_HTTP_PORT=<port> cargo test -p ostro-server --test acme --locked -- pebble`, or the `acme-pebble` workflow_dispatch job. |
| ACME against a public CA (Let's Encrypt staging) | SEC-01 | Needs a publicly reachable host on :443 or :80 and a domain | Run `ostro server` with `[api.tls] mode = "acme"`, `directory = "https://acme-staging-v02.api.letsencrypt.org/directory"`, real `domains`; confirm issuance and renewal scheduling in the logs. |
| Remote CI legs (aarch64-musl, macOS) | all | No git remote yet (FOUND-05) | Confirm green after the first push. |
| Review of D-W3 (effects on the leader), D-W8 (TLS auto default), D-W11 (QA-03 interpretation) | WORK-01, SEC-01, QA-03 | User unavailable (YOLO) | Read 07-DECISIONS.md; STATE.md Pending Todos list each item. |

---

## Gate results

Full `bash scripts/ci-local.sh` (with musl) ran once at `309e7db` (07-22 fixes + CI sync + reservations flip), 2026-11-21 13:20-15:25 UTC (2 h 05 min), `CARGO_TARGET_DIR=target/wt-07-22`, load 7-17 (a Phase 8 campaign was finishing on the machine).

| Step | Result | Time | Notes |
|------|--------|------|-------|
| toolchain 1.100.0 | PASS | 0 s | |
| cargo fmt --check | PASS | 2 s | |
| cargo clippy -D warnings | PASS | 6 s | |
| cargo build (warning-free) | PASS | 77 s | |
| sdk workspace fmt + clippy | PASS | 20 s | |
| sdk workspace test + wasm build | PASS | 24 s | |
| guests build (wasm32-wasip2) | PASS | 0 s | |
| order-saga example | PASS | 21 s | |
| external: PG / MySQL / S3 | SKIP x3 | - | human_needed (no `.ostro-test-env`), H6 |
| cargo test | **FAIL** | 514 s | `ostro --test flow_e2e` `throttle_vs_rate_limit`: `run 000038fy9k0cgw2kkxqkpkaqr0shy not terminal within 90s` (G1). cargo stopped at that binary, so the rest of the workspace was re-run below |
| semantics sleepers (serial) | PASS | 22 s | |
| failpoint list sync (ci.yml) | PASS | 0 s | |
| failpoint suites | PASS | 4670 s | 1,053 tests, 0 failed |
| server campaign smoke (20 kills) | PASS | 786 s | lost_acked_runs=0, duplicate_journal_entries=0, wrong_results=0 |
| phase 7 failpoint legs + manifests (new) | PASS | 60 s | `chaos_smoke` 1 passed (37.9 s, 100 runs), `sigstop_zombie` 1 passed (16.0 s), check-manifests OK (5 files) |
| cargo deny check | PASS | 2 s | |
| check-vendored.sh / check-deny-fixtures.sh | PASS | 2 s / 1 s | |
| no chronik/tokio-uring/prometheus | PASS | 0 s | |
| workflow validation | **FAIL** | 8 s | actionlint SC2034 `i appears unused` in the new keda-kind job; fixed in `9212095` (loop var `_`; YAML parse + SHA-pin check re-run; actionlint itself not re-run, see SUMMARY) |
| zig + cargo-zigbuild present | PASS | 1 s | |
| musl release (static, mimalloc) | PASS | 636 s | |
| musl zigbuild test | **FAIL** | 639 s | `ostro --test worker_e2e` `module_cache`: `the artefact was not rewritten after the tamper` (left 418170, right 418170): ext4 inode reuse in the test's check, deterministic; fixed in `45d73c6`. musl `flow_e2e` passed in this run |

Follow-up runs (same tree, after the fixes):

| Command | Result |
|---------|--------|
| `cargo test --workspace --locked --no-fail-fast` (at `9212095`) | 2,287 passed, 1 failed (`worker_e2e::module_cache`, the inode check above; `flow_e2e` passed this time) |
| `cargo test -p ostro --test worker_e2e -- --test-threads=1` (at `45d73c6`) | 8 passed; `module_cache` alone 3/3 |
| `cargo test -p ostro --test flow_e2e -- --test-threads=1 throttle_vs_rate_limit`, alone, 7 runs | 4 passed, 3 failed (`not terminal within 90s`): a real, partition-dependent product bug (G1), not load |
| `cargo tree --workspace -e normal --target all --locked --prefix none` | no `chrono`, `ring`, `openssl-sys`, `webpki-roots`, `native-tls` |
| `cargo tree -d --depth 0 -e normal --workspace --locked` | no duplicate tonic, tonic-prost, prost, prost-types, hyper, http, axum, rustls, opentelemetry(_sdk) |
| `PROPTEST_CASES=256 cargo test -p ostro-partition --test rebuild_digest` | ok, 7 passed (1,542 s) |
| `cargo test -p ostro-engine --test replay_corpus` | ok, 5 passed |
| `git diff 000dced HEAD -- wit/` | empty (WIT `ostro:runtime@0.1.0` unchanged) |
| goldens since 000dced | `crates/ostro-types/tests/golden/commands.txt` +3 / -0 (append-only, 07-03) |
| reservations (`07-22-rescheck.py`, the 07-01 script expecting Cmd 20/21 + RejectReason 15) | RESERVATIONS CHECK: PASS (147 OK, 0 FAIL) |
| demand ownership (`grep -rn ostro_queue_worker_demand crates`) | value set only in `workers/demand.rs`; dispatcher.rs describes it (`describe_gauge!`); no foreign setter |
| clock seam grep (p7.rs, workers/, flow/, tls/, control/p7_*.rs, leader/{fair,flow,placement}.rs) | 5 hits, all exempt or non-reads: `tls/ca.rs:53` X.509 validity (clock-exempt), `workers/spill.rs:369` getrusage wall denominator (clock-exempt), `tls/acme.rs:99` SystemClock fallback (clock-exempt, injection root; `p7::init` now injects), `tls/acme.rs:46` import, `tls/acme.rs:92` doc comment. The `remote_executor.rs` `tokio::time::Instant::now` hit was replaced by a pinned sleep (b43f5e5) |
| QA-03 chaos report (`target/fencing-chaos-fix0721/report.json`, 07-21 re-run at e82d99e) | pass true: runs 10,000, completed 10,000, charges_eq_1 10,000, double_charges 0, lost 0, fenced_sends 0, raw_duplicates 170 (all leader_kill), raw_duplicates_without_leader_kill 0, unattributed_duplicates 0, provider_409 10 (all recovered), start_effect_rejections 3 |

### Gaps found by the gate

- **G1 — RESOLVED in 07-23 (`272ff34`)** (was BLOCKING; real product bug, DISP-05 / D-W13): a queue throttle that blocks a *first* activation makes the dispatcher `skip_queue` for the round (`crates/ostro-partition/src/leader/dispatcher.rs:1144`, `Admit::QueueBlocked { until_ms }` from `Block::QueueThrottle` in `crates/ostro-server/src/flow/mod.rs`). A re-activation queued behind it on the same partition (FIFO by ready_at, same key) is therefore never considered until the throttle's `until_ms`, although re-activations are never throttled. `flow_e2e::throttle_vs_rate_limit` sets a 1-per-10-min throttle while a sleeper's 2.5 s timer is pending: when a held probe lands on the sleeper's partition, the sleeper waits ~10 min (`run ... not terminal within 90s`), otherwise it passes. Verbatim: `thread 'throttle_vs_rate_limit' panicked at crates/ostro/tests/flow_e2e.rs:195:9: run 000038fy9k0cgw2kkxqkpkaqr0shy not terminal within 90s`. Fix direction (gap-closure plan, dispatcher + fair.rs, 07-05 owner): for a throttle `QueueBlocked` on a first activation, hold or skip only first activations of the queue (not re-activations) and release them at `until_ms`; add a dispatcher_flow test with a throttled first activation ahead of a re-activation on one partition. **Resolution (07-23):** a `QueueBlocked{Some(until)}` / `KeyBlocked{Some(until)}` on a first activation now calls `FairQueues::gate_first` / `gate_first_key`: only first activations of that queue (key) are held back, until `until_ms` (existing deadline arm via `next_unpark_at`) or a flow `changed()` bump; re-activations are still granted in the same round with DRR/band/key fairness intact; concurrency blocks skip/park as before. `dispatcher_flow::{queue,key}_throttle_on_first_activation_does_not_hold_reactivations` pass and fail on the old code (`timed out waiting for 2 jobs; saw 1`); `flow_e2e throttle_vs_rate_limit` 15 of 15 sequential runs passed.

## Human-needed

| # | Item | Requirement | Exact steps |
|---|------|-------------|-------------|
| H1 | Real KEDA ScaledObject drives a worker Deployment 0 → N → 0 | WORK-05 (SC5) | Either run the optional `keda-kind` job (GitHub Actions, `workflow_dispatch`, kind + KEDA inside the runner) after the first push, or on a cluster the user explicitly approves (**never the local microk8s cluster without explicit approval**): `helm install keda kedacore/keda -n keda --create-namespace`; apply `deploy/k8s/server-statefulset.yaml` (Secret `ostro-api-tls` for `ostro.ostro.svc`); `ostro worker token create` (over `--server https://... --ca-file ...`), put the token and `--ca-hash` into `deploy/k8s/worker-deployment.yaml` and apply it (replicas 0); put the client-listener CA into `deploy/keda/trigger-auth.yaml`, apply it and `deploy/keda/scaledobject-metrics-api.yaml`; `ostro queue flow heavy --set placement=workers --set selector=pool=heavy`; start ~40-100 `cpu-burn` runs on `heavy`; `kubectl -n ostro get deploy ostro-worker-heavy -w` shows 0 → ceil(demand/8) → 0 (after `cooldownPeriod` 120 s); every run `completed`. Check that KEDA accepts `authMode: tls` with an empty client `cert`/`key` (CA only); if KEDA rejects it, drop the `cert`/`key` parameters from the TriggerAuthentication or use the prometheus form (`deploy/keda/scaledobject-prometheus.yaml`). |
| H2 | ACME against Pebble in CI | SEC-01 | Built and passed locally in 07-10 (Pebble v2.10.3, real HTTP-01 + TLS-ALPN-01). Repeat in CI with the `acme-pebble` `workflow_dispatch` job, or locally with the commands in docs/tls.md §3. The gate run itself SKIPs the Pebble tests (env unset). |
| H3 | ACME against a public CA (Let's Encrypt staging) | SEC-01 | On a host reachable from the internet on :443 (TLS-ALPN-01) or :80 (HTTP-01) with a DNS name: `ostro server` with `[api.tls] mode = "acme"`, `[api.tls.acme] domains = ["<name>"]`, `email`, `directory = "https://acme-staging-v02.api.letsencrypt.org/directory"`; confirm issuance (ready line fingerprint, `curl -v --cacert <LE staging root> https://<name>/readyz`), the cached files under `<data_dir>/tls/acme/` (0600) and the renewal schedule in the logs. |
| H4 | Remote CI legs (linux-aarch64-musl, macos-aarch64, linux-x86_64-gnu with PG/MySQL/SeaweedFS) | all (FOUND-05) | No git remote yet. After the first push: CI green on every leg, including the new "Phase 7 failpoint legs + manifests" step. |
| H5 | User review of D-W3 (effects on the leader), D-W8 (TLS `auto` default), D-W11 + D-W28 (QA-03 interpretation, 409 re-drive retry) | WORK-01, SEC-01, QA-03 | Read 07-DECISIONS.md; STATE.md Pending Todos list each `[USER REVIEW]` item. Also: 07-12's adaptation of W6 (well-formed `{{secret:..}}` placeholders accepted in redacted values from workers) and the resume/`attempt_failures` question (F4 below). |
| H6 | Local external-service legs | FX (Phase 6 legs in the gate) | `.ostro-test-env` absent in the 07-22 run: PG/MySQL/S3 legs SKIPped (human_needed, unchanged from Phase 6). |

## Follow-ups (07-22 triage of every Phase 7 SUMMARY)

Fixed in 07-22:

| Follow-up (source) | Fix | Commit |
|--------------------|-----|--------|
| `tls::acme::set_clock(ctx.clock)` never called (07-10, 07-14) | `p7::init` calls it | b43f5e5 |
| `tls::init` used the `auto_sealing_key` dev heuristic (07-06, 07-15) | `ServerOptions::with_tls_profile`; the binary passes Dev/Server to `init_with_profile` (heuristic kept only as the library fallback) | b43f5e5 |
| join command printed `:0` for an ephemeral listen (07-14) | bound address in `P7Init`/`P7Shared::client_addr`, used by `join_url` (e2e: `cli_p7` asserts the real port) | b43f5e5, 80fade4 |
| clock-seam grep: `tokio::time::Instant::now` in remote_executor (07-22) | pinned `tokio::time::sleep` for the post-Cancel bound; acme fallback marked clock-exempt | b43f5e5 |
| revoked worker retried forever (07-17) | a 4xx re-join refusal ends the worker (exit 1); `refused_rejoin_ends_the_worker` | ba5da99 |
| `--ca-hash` with a files/acme listener failed the pinned handshake (found by 07-22 while documenting the deploy/k8s manifests) | one retry with the platform verifier before any request byte; response CA must match the pin | ba5da99 |
| Wasmtime version duplicated (07-07, 07-09) | `ostro_engine::cache::WASMTIME_VERSION`, re-exported by worker and server (cache key bytes unchanged) | ba5da99 |
| ANSI colours when piped (07-17) | `with_ansi(stdout.is_terminal())` | ba5da99 |
| zombie's refused StartEffect not logged (07-18) | warn on the leader with worker, run, token, op | ba5da99 |
| `GET /v1/runs/{id}` had no pause state (07-19) | `pause {reason, detail, at_ms}` on REST, gRPC field 21, `ostro inspect` | 454bbe0 |
| CLI rejected `https://` (07-15, 07-16, 07-17, 07-19) | https with platform roots or `--ca-file`/`OSTRO_CA_FILE` (also `ostro backup`) | 80fade4 |
| `common::is_terminal` lacked timed_out/terminated/continued_as_new (07-19) | added | 323f4e8 |
| `flow_control::queue_concurrency_caps_leased_activations` flake (07-12) | flow counters stay the exact assertion; the lagging shared lease gauge is held to cap x partitions | 323f4e8 |

Recorded (owner):

| # | Follow-up (source) | Owner | Notes |
|---|--------------------|-------|-------|
| F1 | No server→worker `Drain` message: `POST /v1/workers/{id}/drain` stops assignment only (07-09, 07-14) | Phase 13 (worker fleet / rolling upgrades), or a Phase 7 gap-closure plan if wanted earlier | Needs `ServerMsg.drain` (additive proto field) + a worker arm that drains like SIGTERM |
| F2 | Workers send a long activation's appends only when it ends; a lease revoke discards that work (07-19) | Phase 13 (streamed appends / periodic flush) | Design limitation, documented in docs/workers.md §7; correctness unaffected |
| F3 | Auto-pause "resume → completes" leg needs a guest whose trap depends on state, e.g. "panic until signal `fix`" (07-19) | Phase 7 gap closure (guests/ + flow_e2e) | `/retry`-completes is covered by `timeout_while_paused` |
| F4 | A resume keeps `attempt_failures`, so one more failure re-pauses an auto-paused run (07-19) | **USER REVIEW**, then a partition apply plan (Phase 9 or gap closure) | Resetting on resume changes apply semantics/state hash for new logs only; needs a format note |
| F5 | Worker holds one session to the current endpoint; no connection to every `Welcome.members` entry (07-07) | Phase 12/13 | Single node in Phase 7, so no effect yet |
| F6 | No CLI verbs for `GET /v1/workers` / `POST /v1/workers/{id}/drain` (07-15) | Phase 11 (operator UX) | REST + gRPC exist |
| F7 | Demand ticker publishes only queues with dispatcher gauges (policy-only queues are not enumerated; MetaState has no queue-policy iterator) (07-16) | Phase 11 | The endpoint answers for any queue name |
| F8 | No join counter / module-cache hit metrics on the worker (07-17) | Phase 11 (observability) | Tests use logs and file identity |
| F9 | Second-signal exit 130 during a worker drain has no test (07-15) | Phase 10 (test harness) | Behaviour implemented |
| F10 | KEDA `authMode: tls` with empty client `cert`/`key` in `deploy/keda/trigger-auth.yaml` is unverified against a real KEDA (07-16) | H1 (keda-kind job) | Adjust the manifest if KEDA requires a client pair |
| F11 | Rate limit: two concurrent first starts of one new idempotency key may both be charged (07-08) | documented, no owner | Benign over-charge |
| F12 | Pre-Phase-8 direct time reads outside Phase 7 files (executor.rs, dispatcher, node.rs …) | Phase 10 F5 (existing STATE TODO) | Phase 7 files are clean (see Gate results) |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 60 s for task-level checks
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
