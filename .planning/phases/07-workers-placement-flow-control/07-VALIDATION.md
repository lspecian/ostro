---
phase: 7
slug: workers-placement-flow-control
status: draft
nyquist_compliant: false
wave_0_complete: false
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
| 7-01-01 | 01 | 1 | (gate) | check + reservations script | 06-{01..20}-SUMMARY; `06-VERIFICATION.md` by-design only; F1–F4, F7 (`first_activation`), F8 (`expires_at_ms`) greps; RESERVATIONS greps | n/a | ⬜ pending |
| 7-01-02 | 01 | 1 | (decisions) | grep | STATE.md: Phase 07 decisions D-W1..D-W25, `[USER REVIEW] D-W3/D-W8/D-W11`, coordination concern | n/a | ⬜ pending |
| 7-02-01 | 02 | 2 | SEC-01/WORK-01 (deps, proto) | deny + tree checks + round-trip | `cargo deny check` + no ring/chrono/openssl/webpki-roots + `cargo tree -d --depth 0 -e normal --workspace --locked` no banned dups + `cargo test -p ostro-worker-proto --locked` | ❌ W0 (wire.rs) | ⬜ pending |
| 7-02-02 | 02 | 2 | DISP-03..05, WORK-03 (contracts) | unit | `cargo test -p ostro-partition --test p7_contract --test dispatcher --locked` | ❌ W0 (p7_contract) | ⬜ pending |
| 7-02-03 | 02 | 2 | all (seam, config) | integration | `cargo test -p ostro-server --test config --test executor --test leases --locked && cargo test -p ostro-api --locked && bash scripts/ci-local.sh --no-musl` | ✅ (extended) | ⬜ pending |
| 7-03-01 | 03 | 2 | OPS-01 (format contract) | unit + golden (append-only) | `cargo test -p ostro-types --locked` + golden diff vs `target/07-03-base.sha` + `cargo test -p ostro-partition --test p7_format --test apply_core --locked` | ❌ W0 (p7_format) | ⬜ pending |
| 7-04-01 | 04 | 3 | OPS-01 | unit (apply) incl. pause → timeout → RetryRun | `cargo test -p ostro-partition --test pause --test apply_core --test apply_phase4 --test timers --test apply_timer_input --locked && cargo build -p ostro-server --locked` | ❌ W0 (pause.rs) | ⬜ pending |
| 7-04-02 | 04 | 3 | OPS-01 | proptest (rebuild digest, RetryRun in gen_pause) + failpoints | `PROPTEST_CASES=256 cargo test -p ostro-partition --test rebuild_digest --locked` + failpoint run | ❌ W0 (gen_pause.rs) | ⬜ pending |
| 7-05-01 | 05 | 3 | DISP-04, DISP-03 | unit + proptest | `cargo test -p ostro-partition --lib --locked -- leader::fair` (non-vacuous) | ❌ W0 | ⬜ pending |
| 7-05-02 | 05 | 3 | DISP-03/04/05, WORK-03, OPS-01 | integration (fakes) incl. blocked-queue-does-not-starve and re-activation-not-throttled | `cargo test -p ostro-partition --test dispatcher --test dispatcher_flow --locked` + server executor/leases + `grep interval(` empty | ❌ W0 (dispatcher_flow.rs) | ⬜ pending |
| 7-06-01 | 06 | 3 | SEC-01 | unit/integration | `cargo test -p ostro-server --test tls_ca --locked` + no ring | ❌ W0 | ⬜ pending |
| 7-06-02 | 06 | 3 | SEC-01 | integration (ALPN per mode) | `cargo test -p ostro-server --test tls_listener --test tls_ca --locked` | ❌ W0 | ⬜ pending |
| 7-07-01 | 07 | 3 | WORK-01 | integration (fake server) incl. re-join fallback + failpoints | `cargo test -p ostro-worker --test protocol --locked` + `--features failpoints` | ❌ W0 | ⬜ pending |
| 7-07-02 | 07 | 3 | WORK-02, WORK-06 | integration (real guests) | guests build + `cargo test -p ostro-worker --test protocol --test module_cache --locked` | ❌ W0 | ⬜ pending |
| 7-11-01 | 11 | 3 | OPS-01, WORK-05, DISP-03, WORK-01 (surface) | unit | `cargo test -p ostro-proto -p ostro-api --locked -- phase7` (non-vacuous) | ❌ W0 (phase7_surface.rs) | ⬜ pending |
| 7-11-02 | 11 | 3 | (surface) | parity + OpenAPI semantic superset | `cargo test -p ostro-api --locked` + `openapi_superset` vs the base snapshot + server suites | ✅ (extended) | ⬜ pending |
| 7-08-01 | 08 | 4 | DISP-03, DISP-04, DISP-05 (throttle), OPS-01 | integration + GCRA unit tests | `cargo test -p ostro-server --test flow_control --locked` + `--lib -- flow::` | ❌ W0 | ⬜ pending |
| 7-08-02 | 08 | 4 | DISP-05 (rate limit) | integration | `cargo test -p ostro-server --test flow_control --locked -- rate_limit` + start/runs API suites | ❌ W0 | ⬜ pending |
| 7-08-03 | 08 | 4 | OPS-01 (auto-pause) | integration | `cargo test -p ostro-server --test auto_pause --test executor --test leases --locked` | ❌ W0 | ⬜ pending |
| 7-09-01 | 09 | 4 | WORK-01 | unit/integration incl. token expiry (F8) | `cargo test -p ostro-server --test worker_session --locked -- registry join_service` | ❌ W0 | ⬜ pending |
| 7-09-02 | 09 | 4 | WORK-01, SEC-01 (mTLS, peer identity, external CA rule) | integration (in-process worker) | `cargo test -p ostro-server --test worker_session --locked` | ❌ W0 | ⬜ pending |
| 7-10-01 | 10 | 4 | SEC-01 (ACME) | unit (fake ACME) | `cargo test -p ostro-server --test acme --locked` | ❌ W0 | ⬜ pending |
| 7-10-02 | 10 | 4 | SEC-01 (ACME) | env-gated (Pebble) | `cargo test -p ostro-server --test acme --locked -- pebble` (SKIP without env) | ❌ W0 | ⬜ pending |
| 7-15-01 | 15 | 4 | OPS-01, WORK-05, DISP-03 (CLI) | integration (fake control plane) | `cargo test -p ostro --test cli_p7 --locked -- verbs` | ❌ W0 | ⬜ pending |
| 7-15-02 | 15 | 4 | WORK-01 (worker process), SEC-01 (ready line) | spawned binary | `cargo test -p ostro --test cli_p7 --test cli --test dev_startup --locked` | ❌ W0 | ⬜ pending |
| 7-12-01 | 12 | 5 | WORK-04, WORK-03 (embedded-first overflow) | unit | `cargo test -p ostro-server --lib --locked -- workers::selector workers::placement` | ❌ W0 | ⬜ pending |
| 7-12-02 | 12 | 5 | WORK-01, WORK-02, WORK-06 (+ secrets, literal redacted values, sink_for) | integration | `cargo test -p ostro-server --test worker_remote --test worker_paging --test executor --test leases --locked` | ❌ W0 | ⬜ pending |
| 7-13-01 | 13 | 5 | WORK-03 (exit rule, sustained overload = 1 transition) | unit (paused time) | `cargo test -p ostro-server --test placement_spill --locked` + `--lib -- workers::spill` | ❌ W0 | ⬜ pending |
| 7-13-02 | 13 | 5 | WORK-03 | integration | `cargo test -p ostro-server --test placement_spill --locked` | ❌ W0 | ⬜ pending |
| 7-14-01 | 14 | 5 | OPS-01, DISP-03 | integration (REST + gRPC) | `cargo test -p ostro-server --test p7_control --locked -- flow pause retry cancel terminate` | ❌ W0 | ⬜ pending |
| 7-14-02 | 14 | 5 | WORK-01, WORK-05 | integration | `cargo test -p ostro-server --test p7_control --locked` | ❌ W0 | ⬜ pending |
| 7-16-01 | 16 | 6 | WORK-05 | integration + ownership grep | `cargo test -p ostro-server --test demand --locked` + no foreign demand setter | ❌ W0 | ⬜ pending |
| 7-16-02 | 16 | 6 | WORK-05 (manifests) | offline structural check | `bash deploy/check-manifests.sh` | ❌ W0 | ⬜ pending |
| 7-16-03 | 16 | 6 | WORK-01 (binary smoke, restart resume) | spawned e2e | `cargo test -p ostro --test worker_smoke --locked -- --test-threads=1` + `restart_resume` | ❌ W0 | ⬜ pending |
| 7-17-01 | 17 | 7 | WORK-01, WORK-02, WORK-06 (SC1) | spawned e2e | `cargo test -p ostro --test worker_e2e --locked -- --test-threads=1 join paging module_cache secret_never_on_worker` | ❌ W0 | ⬜ pending |
| 7-17-02 | 17 | 7 | SEC-01 (SC1) | spawned e2e (+ env-gated ACME) | `cargo test -p ostro --test worker_e2e --locked -- --test-threads=1 listener` | ❌ W0 | ⬜ pending |
| 7-18-01 | 18 | 7 | WORK-03, WORK-04 (SC2) | spawned e2e | `cargo test -p ostro --test worker_failover --locked -- --test-threads=1 auto_spill oscillation sustained_overload selectors` | ❌ W0 | ⬜ pending |
| 7-18-02 | 18 | 7 | WORK-01 failover (SC2) | spawned e2e + failpoints | `cargo test -p ostro --test worker_failover --locked -- --test-threads=1 kill_redispatch` + `--features failpoints … sigstop_zombie` | ❌ W0 | ⬜ pending |
| 7-19-01 | 19 | 7 | DISP-03, DISP-04, DISP-05 (SC4) | spawned e2e | `cargo test -p ostro --test flow_e2e --locked -- --test-threads=1 queue_limits weights priority keyed_fairness throttle_vs_rate_limit` | ❌ W0 | ⬜ pending |
| 7-19-02 | 19 | 7 | OPS-01 (SC4) | spawned e2e incl. timeout → /retry | `cargo test -p ostro --test flow_e2e --locked -- --test-threads=1 pause timeout_while_paused` | ❌ W0 | ⬜ pending |
| 7-20-01 | 20 | 7 | WORK-05 (SC5) | unit (scaler math) | `cargo test -p ostro --test keda_sim --locked -- scaler` | ❌ W0 | ⬜ pending |
| 7-20-02 | 20 | 7 | WORK-05 (SC5) | spawned e2e (simulated KEDA) | `cargo test -p ostro --test keda_sim --locked -- --test-threads=1 zero_to_n_to_zero` | ❌ W0 | ⬜ pending |
| 7-21-01 | 21 | 7 | QA-03 (SC3) | chaos smoke (failpoints) | `cargo test -p ostro --features failpoints --test fencing_chaos --locked -- --test-threads=1 chaos_smoke` | ❌ W0 | ⬜ pending |
| 7-21-02 | 21 | 7 | QA-03 (SC3) | chaos campaign (10,000 runs, ignored) | `bash scripts/fencing-chaos.sh` → report.json with double_charges 0, lost 0, fenced_sends 0, raw_duplicates_without_leader_kill 0, unattributed_duplicates 0, runs 10000 | ❌ W0 | ⬜ pending |
| 7-22-01 | 22 | 8 | all | full gate | `bash scripts/ci-local.sh` + deny + tree/dup checks + CI greps + RESERVATIONS flip | ✅ | ⬜ pending |
| 7-22-02 | 22 | 8 | all | docs + records | docs exist; VALIDATION "Gate results"/"Human-needed"; DECISIONS "Spike outcomes"; STATE `[Phase 07 / 07-22]` | n/a | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠ flaky / human_needed*

---

## Wave 0 Requirements

Test files and fixtures created inside the plans that first need them (no framework install needed):

- [ ] `crates/ostro-partition/tests/p7_contract.rs`, `crates/ostro-worker-proto/tests/wire.rs`, `crates/ostro-worker/tests/failpoints.rs` (07-02)
- [ ] `crates/ostro-partition/tests/p7_format.rs` (07-03)
- [ ] `crates/ostro-partition/tests/pause.rs`, `tests/common/gen_pause.rs` (07-04)
- [ ] `crates/ostro-partition/tests/dispatcher_flow.rs` + fake FlowControl/Placement (07-05)
- [ ] `crates/ostro-server/tests/{tls_ca,tls_listener}.rs` (07-06)
- [ ] `crates/ostro-worker/tests/{protocol,module_cache}.rs` + `tests/common/fake_server.rs`; guests `long-journal`, `pay-flow`, `cpu-burn` (07-07)
- [ ] `crates/ostro-server/tests/{flow_control,auto_pause}.rs` (07-08)
- [ ] `crates/ostro-server/tests/worker_session.rs` + `tests/common/p7_workers.rs` (07-09)
- [ ] `crates/ostro-server/tests/acme.rs` (07-10)
- [ ] `crates/ostro-api/tests/phase7_surface.rs` (incl. `openapi_superset`) (07-11)
- [ ] `crates/ostro-server/tests/{worker_remote,worker_paging}.rs` (07-12), `placement_spill.rs` (07-13), `p7_control.rs` (07-14), `demand.rs` (07-16)
- [ ] `crates/ostro/tests/cli_p7.rs` (07-15); `common/p7.rs`, `worker_smoke.rs` (07-16); `worker_e2e.rs` (07-17), `worker_failover.rs` (07-18), `flow_e2e.rs` (07-19), `keda_sim.rs` (07-20), `fencing_chaos.rs` + `scripts/fencing-chaos.sh` (07-21)
- [ ] `deploy/keda/*.yaml`, `deploy/k8s/*.yaml`, `deploy/check-manifests.sh` (07-16)

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

(Filled in by 07-22 Task 2.)

## Human-needed

(Filled in by 07-22 Task 2.)

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60 s for task-level checks
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
