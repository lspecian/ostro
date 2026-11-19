---
phase: 14
slug: multi-tenancy-security-1-0-release
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-11-19
---

# Phase 14: Validation Strategy

> This is the per-phase validation contract for feedback sampling during execution.
> Source: 14-RESEARCH.md § Validation Architecture, mapped onto plans 14-01..14-27. Adopted decisions: 14-DECISIONS.md (D-P14-1..D-P14-36).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test harness (`cargo test`, Rust 1.100.0), proptest 1.12 (token codec, fair scheduler model, journal tamper, signature parsing), `fail` 0.5.1 failpoints (`auth::`, `audit::` + Phase 2/5 `wal::`/`state::` sites used by the disk nemesis), insta snapshots (CLI), the Phase 12/13 `ostro-repl-conformance` harness (tier B in-process clusters; tier C `ProcCluster` + Rust `ChaosProxy` on `127.r.r.n`), the new `ostro-jepsen` crate (seeded nemeses, history, lost/duplicate/fencing checkers, bench, soak), `ostro-fuzz-targets` (stable corpus replay) + the separate `fuzz/` workspace (cargo-fuzz 0.13.2, libfuzzer-sys 0.4.13, arbitrary 1.5.0, wasm-smith =0.261.0, dated nightly pinned in `fuzz/rust-toolchain.toml`). Shell/tools: helm 3.17 (`lint`/`template` only, `KUBECONFIG=/dev/null`), kubeconform v0.8.0, crane v0.22.1, shellcheck v0.11.0, rsign2 0.6.7 — all via `scripts/fetch-tools.sh` into `target/tools/`. |
| **Config** | No new config file. `FAILPOINT_CRATES` unchanged unless a Phase 14 failpoint suite lands in a crate not yet listed (ostro-server is already listed since Phase 5/11); kept equal to the ci.yml "Failpoint suites" step. 14-27 adds fuzz smoke, helm-check, release dry run + install tests, image verify, docs-check, Jepsen smoke and `check_p14_gates` to ci-local and ci.yml; `.github/workflows/fuzz.yml` (scheduled) and `.github/workflows/release.yml` (tags/dispatch) are new workflows. |
| **External services** | None. No Docker. **The microk8s cluster reachable via kubectl on this machine is never used** (no kubectl; helm offline only). No registry pushes (a loopback `crane registry serve` is local). Apple notarization, registry pushes, remote CI, the external security review, the 7-day soak and native/LAN benchmarks are human_needed. |
| **Quick run command** | `cargo test -p ostro-server --locked --test <plan test> -- --test-threads=1` · `cargo test -p ostro-fuzz-targets --locked --test corpus_replay` · `cargo test -p ostro-jepsen --locked --test checker_selftest` · `bash scripts/helm-check.sh` · `bash scripts/docs-check.sh <paths>` |
| **Full suite command** | `bash scripts/ci-local.sh --no-musl` (from 14-27 also fuzz smoke, helm-check, docs-check, Jepsen smoke; the musl leg adds release dry run + install tests + image verify) |
| **Phase gate commands** | 1. `bash scripts/ci-local.sh` (with musl).<br>2. `OSTRO_FUZZ_SECS=60 bash scripts/fuzz-smoke.sh` (15 targets, 0 crashes).<br>3. `bash scripts/jepsen-run.sh gate` (2 seeds × 30 min, all four fault classes, auth + audit on; all checkers pass).<br>4. `cargo test --release -p ostro-jepsen --locked --test bench_single --test bench_rf3 -- --ignored --nocapture --test-threads=1` (acked == on disk; thresholds advisory without `OSTRO_BENCH_ENFORCE=1`).<br>5. `OSTRO_SOAK_SECS=1800 cargo test --release -p ostro-jepsen --locked --test soak_smoke -- --ignored --test-threads=1` (verdict PASS).<br>6. `bash scripts/release-dry-run.sh && bash scripts/test-install.sh`.<br>7. `bash scripts/build-image.sh …` (`verify: OK`).<br>8. `bash scripts/helm-check.sh` (0 invalid, 0 skipped).<br>9. `bash scripts/docs-check.sh` (full tree).<br>10. `bash scripts/security-packet.sh` (packet + sha256, secret scan clean).<br>11. Strict CLOCK_GATE (scope incl. `crates/ostro-bridge/src`). |
| **Estimated runtime** | Quick: < 60 s per command. Tier-B Phase 14 groups: 30–120 s. Full ci-local: ~40–55 min debug (+ fuzz/Jepsen smokes ~10 min). Jepsen gate: ~70 min. Soak smoke: 30 min. Fuzz smoke: ~15 min. |

---

## Sampling Rate

- **After every task commit:** the task's `<automated>` command, plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings` for touched crates (with `--features failpoints` where failpoint tests exist) and CLOCK_GATE_SCOPED for server/partition/bridge diffs.
  - Tasks touching `ostro-partition` also run `PROPTEST_CASES=128 cargo test -p ostro-partition --locked --test rebuild_digest` and `--test format_corpus`.
  - Tasks touching `ostro-meta` also run `cargo test -p ostro-meta --locked` (goldens append-only).
  - Tasks touching `ostro-api`/`ostro-proto` also run `cargo test -p ostro-api --locked --test parity --test openapi`.
  - Tasks touching decoders/apply (fuzz crash fixes) also run `cargo test -p ostro-fuzz-targets --locked --test corpus_replay`.
- **After every plan / wave:** `bash scripts/ci-local.sh --no-musl` in the plan's `/tmp` worktree at its last commit; from wave 3 on also the plan's tier-B group plus `cargo test -p ostro-repl-conformance --locked -- --test-threads=1`.
- **Before `/gsd:verify-work`:** the 14-27 gate.
- **Max feedback latency:** 60 s for task-level unit checks; tier-B groups ≤ 2 min; Jepsen/bench/soak/fuzz campaigns are gate level (task-level smokes ≤ 5 min).
- Piped verify commands use `set -euo pipefail`; cargo test filters go after `--`; no `|| true` masking; negations are `if …; then exit 1; fi`; filtered runs must report ≥ 1 passed test (`test result: ok\. [1-9][0-9]* passed`).

---

## Wave Structure

| Wave | Plans | Exclusive shared files |
|------|-------|------------------------|
| 1 | 14-01 gate (Phase 13 + 5–12 complete) + contracts + seam + deps + tool pins | **Cargo files**, `fuzz/Cargo.toml`/toolchain, RESERVATIONS, STATE, every `lib.rs`/`mod.rs` line, `ostro-types` audit/compat, meta `ext.rs`, ostro-api `auth`/`phase14`/`error`, **server.rs / config.rs / main.rs / p14/mod.rs**, hook call sites in `control/*`, bridge + worker sites, CLI variants, client stubs, `scripts/fetch-tools.sh` |
| 2 | 14-02 API surface; 14-03 tokens/authn/bootstrap/guard; 14-04 audit journal; 14-05 signature verifiers; 14-06 dispatch quotas; 14-07 fuzz infra + 13 targets; 14-08 Wasm fuzz targets; 14-09 Jepsen core; 14-11 release binaries + install + release.yml; 14-12 image; 14-13 Helm; 14-14 migration guides | 14-02: **ostro-api / ostro-proto / OpenAPI**. 14-03: `p14/{tokens,authn,bootstrap,guard}`. 14-04: `audit/*`. 14-05: `p14/signing.rs`. 14-06: `leader/fair.rs`, `flow/tenant.rs`, `p14/quota_view.rs`, **`executor.rs`**. 14-07: fuzz target modules, `corpus_replay.rs`, non-Wasm corpora/regressions, `fuzz-smoke.sh`, `fuzz.yml`, `fuzz/Cargo.lock`. 14-08: `evil_guest.rs`, `smith.rs`, Wasm corpora/regressions. 14-09: `ostro-jepsen` workload/history/run/report/checkers/partition+kill nemeses. 14-11: `install.sh`, release scripts, `release.yml`. 14-12: `deploy/image`, `build-image.sh`, NOTICE. 14-13: `deploy/helm/**`, `helm-check.sh`, `crd2jsonschema.py`. 14-14: `docs/migrations/**` |
| 3 | 14-10 clock/disk fault knobs; 14-15 scopes + principal propagation; 14-16 tenant scoping + index + triggers; 14-17 tenant/token admin + CLI; 14-18 audit capture + query + CLI; 14-19 signed-module enforcement + policy + reseal; 14-20 ingress quotas | 14-10: `ostro-partition/src/clock.rs`, `p14/test_hooks.rs`, nemesis clock/disk. 14-15: `p14/{authz,principal_fwd}`, `route/peer.rs`, node-API ingest. 14-16: `p14/{tenant_scope,upload_tenant}`, `control/list.rs`, partition index file, trigger driver files. 14-17: `p14/{tenants_api,tokens_api}`, client tenants/api_tokens, CLI tenant/auth. 14-18: `p14/{audit_hooks,audit_api}`, bridge `audited.rs`, client/CLI audit. 14-19: `p14/{module_sign,security_api,reseal}`, client/CLI security, CLI `deploy.rs`. 14-20: `p14/ingress.rs` |
| 4 | 14-21 UI auth; 14-22 Jepsen runs; 14-23 benchmarks + soak; 14-24 threat model + review packet; 14-25 docs set + docs-check | 14-21: `ostro-ui/**`, `p14/ui_auth.rs`. 14-22: Jepsen smoke/gate tests, `jepsen-run.sh`. 14-23: `bench.rs`, `soak.rs`, `ostro-soak`, bench/soak tests, `docs/soak.md`, `docs/benchmarks.md`. 14-24: `docs/security/**`, `docs/security-model.md`, `security-packet.sh`. 14-25: `README.md`, `docs/README.md`, guides, `docs-check.sh`, `check-links.py` |
| 5 | 14-26 cross-feature integration + suite green (sole) | `scenarios/p14.rs`, `p14_integration.rs`, `p14_regressions.rs`, fixes across Phase 14 product files and docs |
| 6 | 14-27 gate + 1.0.0-rc.1 | ci files, version fields + Cargo.lock, Chart version, `docs/release-checklist.md`, `CHANGELOG.md`, RESERVATIONS, REQUIREMENTS, STATE, VALIDATION, DECISIONS |

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirements | Task | Automated command(s) | Status |
|---------|------|------|--------------|------|----------------------|--------|
| 14-01-01 | 01 | 1 | all | Phase 13 (+5–12) gate, seam + method inventory, RESERVATIONS rows, STATE decisions | `grep / doc checks` (SUMMARYs, VERIFICATION status, RoutedControlPlane, STATE/RESERVATIONS rows) | ⬜ pending |
| 14-01-02 | 01 | 1 | all | Contracts: audit sink, P14 features, ext 112–117, ostro_api::auth + phase14 + auth codes, audited resolver stub | `cargo build --workspace --all-targets` warning-free; `cargo test -p ostro-types / -p ostro-meta / -p ostro-api --lib / -p ostro-bridge --lib`; goldens append-only | ⬜ pending |
| 14-01-03 | 01 | 1 | all | Seam + call sites, config, CLI/client stubs, ostro-fuzz-targets + fuzz/ + ostro-jepsen skeletons, deps, tool pins, CI sync | `cargo deny check`; failpoints build; forbidden-crate check; list diff; nightly fuzz build; `fetch-tools.sh kubeconform crane`; `--test config`, `--test p14_skeleton`, `-p ostro-jepsen --test skeleton`, `-p ostro-fuzz-targets`, `-p ostro-cli` | ⬜ pending |
| 14-02-01 | 02 | 2 | SEC-02..04, DISP-06 | Protos + wire tests | `cargo test -p ostro-proto --test p14_wire` | ⬜ pending |
| 14-02-02 | 02 | 2 | SEC-02 | Authentication layer + interceptor | `cargo test -p ostro-api --test p14_authn` | ⬜ pending |
| 14-02-03 | 02 | 2 | SEC-02..04 | Adapters, signature carriage, tenant hooks, fakes, OpenAPI, parity | `cargo test -p ostro-api --test p14_routes --test parity --test openapi`; OpenAPI diff additive | ⬜ pending |
| 14-03-01 | 03 | 2 | SEC-02 | Token codec | `cargo test -p ostro-server --test p14_tokens` | ⬜ pending |
| 14-03-02 | 03 | 2 | SEC-02 | MetaAuthenticator + bootstrap (online/offline) | `cargo test -p ostro-server --test p14_authn` (+ failpoints) ; CLOCK_GATE_SCOPED | ⬜ pending |
| 14-03-03 | 03 | 2 | SEC-02 | Guard decorator (final) | `cargo test -p ostro-server --test p14_guard`; `cargo test -p ostro-server` | ⬜ pending |
| 14-04-01 | 04 | 2 | SEC-04 | Record/chain/segment/verify/retention (TDD) | `cargo test -p ostro-server --test audit_journal -- chain tamper rotate torn retention ignored_by` | ⬜ pending |
| 14-04-02 | 04 | 2 | SEC-04 | Writer, fail closed, coalescing, OTLP export, crash test | `--test audit_journal` (+ failpoints); `OSTRO_AUDIT_CRASH_ITERS=10 --release --test audit_crash -- --ignored` | ⬜ pending |
| 14-05-01 | 05 | 2 | SEC-03 | minisign + cosign keyed verification, interop vectors (TDD) | `cargo test -p ostro-server --test p14_signing`; no secret key fixture; no sigstore | ⬜ pending |
| 14-06-01 | 06 | 2 | DISP-06 | Tenant level in fair.rs + model tests | `cargo test -p ostro-partition --test fair_tenant`; `--lib -- fair`; `rebuild_digest` | ⬜ pending |
| 14-06-02 | 06 | 2 | DISP-06 | Quota view + TenantFlow + executor charge | `cargo test -p ostro-server --lib -- flow::tenant p14::quota_view`; CLOCK_GATE_SCOPED | ⬜ pending |
| 14-06-03 | 06 | 2 | DISP-06 | Server quota + fairness tests (×3) | `cargo test -p ostro-server --test tenant_quotas` ×3 | ⬜ pending |
| 14-07-01 | 07 | 2 | SEC-05 | 13 target bodies + seeds + stable replay | `cargo test -p ostro-fuzz-targets --test corpus_replay`; ≥ 65 seeds; clippy | ⬜ pending |
| 14-07-02 | 07 | 2 | SEC-05 | fuzz-smoke, nightly lockfile, scheduled workflow, triage | `OSTRO_FUZZ_SECS=20 bash scripts/fuzz-smoke.sh …`; fuzz.yml parse + SHA pins | ⬜ pending |
| 14-08-01 | 08 | 2 | SEC-05 | evil_guest | `cargo test -p ostro-fuzz-targets --test wasm_targets -- evil` | ⬜ pending |
| 14-08-02 | 08 | 2 | SEC-05 | smith_component + smoke | `--test wasm_targets`; `cargo +nightly fuzz run smith_component -- -max_total_time=20` | ⬜ pending |
| 14-09-01 | 09 | 2 | QA-05 | History + checkers + self-tests | `cargo test -p ostro-jepsen --test checker_selftest` | ⬜ pending |
| 14-09-02 | 09 | 2 | QA-05 | Workload, partition/kill nemeses, seeded runs, smoke | `OSTRO_JEPSEN_SECS=180 --release --test harness_smoke -- --ignored` | ⬜ pending |
| 14-10-01 | 10 | 3 | QA-05 | SkewClock + test endpoints (failpoints only) | `-p ostro-partition --features failpoints --test skew_clock`; `-p ostro-server --test p14_test_hooks` (both builds); clippy both feature sets | ⬜ pending |
| 14-10-02 | 10 | 3 | QA-05 | Clock + disk nemeses | `OSTRO_JEPSEN_SECS=120 --release --test nemesis_faults -- --ignored` | ⬜ pending |
| 14-11-01 | 11 | 2 | DIST-01 | Release dry run | `bash scripts/release-dry-run.sh`; `sha256sum -c` | ⬜ pending |
| 14-11-02 | 11 | 2 | DIST-01 | install.sh + local install tests + shellcheck | `shellcheck …`; `bash scripts/test-install.sh` | ⬜ pending |
| 14-11-03 | 11 | 2 | DIST-01 | release.yml + validation | YAML/job/secret-scope/SHA-pin checks | ⬜ pending |
| 14-12-01 | 12 | 2 | DIST-01 | crane spike, CA pin, build-image.sh | `bash scripts/build-image.sh …`; OCI layout present; `--push` refused outside CI | ⬜ pending |
| 14-12-02 | 12 | 2 | DIST-01 | Image verification + README | `build-image.sh` prints `verify: OK` | ⬜ pending |
| 14-13-01 | 13 | 2 | DIST-02 | Chart | `KUBECONFIG=/dev/null helm lint --strict` ×4; 5 NetworkPolicies; ScaledObject renders | ⬜ pending |
| 14-13-02 | 13 | 2 | DIST-02 | KEDA schemas + helm-check (kubeconform) | `bash scripts/helm-check.sh` (0 invalid/skipped) | ⬜ pending |
| 14-14-01 | 14 | 2 | DIST-03 | Cron/queue-table guide + index | keyword + marker checks | ⬜ pending |
| 14-14-02 | 14 | 2 | DIST-03 | Temporal + Restate guides | keyword + marker checks | ⬜ pending |
| 14-15-01 | 15 | 3 | SEC-02 | Scope table + route × scope matrix + export | `OSTRO_EXPORT_AUTH_MATRIX=… cargo test -p ostro-server --test auth_scopes` | ⬜ pending |
| 14-15-02 | 15 | 3 | SEC-02 | Principal propagation over forwarding | `cargo test -p ostro-server --test auth_forwarding`; CLOCK_GATE_SCOPED | ⬜ pending |
| 14-16-01 | 16 | 3 | SEC-02 | Tenant scoping + per-tenant module registration | `cargo test -p ostro-server --test tenant_isolation` | ⬜ pending |
| 14-16-02 | 16 | 3 | SEC-02 | Tenant index + tenant-correct triggers | `--test tenant_triggers`; `rebuild_digest`, `format_corpus` | ⬜ pending |
| 14-17-01 | 17 | 3 | SEC-02, DISP-06 | Tenants + tokens admin services | `cargo test -p ostro-server --test p14_admin_api` | ⬜ pending |
| 14-17-02 | 17 | 3 | SEC-02, DISP-06 | Client + CLI verbs | `cargo test -p ostro-cli --test p14_cli`; `--help` checks | ⬜ pending |
| 14-18-01 | 18 | 3 | SEC-04 | Guard audit hooks, fail closed, worker/auth events | `cargo test -p ostro-server --test audit_log` (+ failpoints) | ⬜ pending |
| 14-18-02 | 18 | 3 | SEC-04 | Coalesced secret-access audit | `--test audit_log -- secret_access`; CLOCK_GATE_SCOPED | ⬜ pending |
| 14-18-03 | 18 | 3 | SEC-04 | Cluster audit query + CLI | `--test audit_cluster`; `-p ostro-cli --test p14_audit_cli` | ⬜ pending |
| 14-19-01 | 19 | 3 | SEC-03 | Upload/activation gates, cluster-wide policy | `cargo test -p ostro-server --test signed_modules` | ⬜ pending |
| 14-19-02 | 19 | 3 | SEC-03 | Security API, config seed, reseal | `--test p14_reseal`; CLOCK_GATE_SCOPED | ⬜ pending |
| 14-19-03 | 19 | 3 | SEC-03 | Client + CLI + deploy --signature | `cargo test -p ostro-cli --test p14_security_cli`; `deploy --help` | ⬜ pending |
| 14-20-01 | 20 | 3 | DISP-06 | admit_ingress unit tests | `cargo test -p ostro-server --lib -- p14::ingress` | ⬜ pending |
| 14-20-02 | 20 | 3 | DISP-06 | Server ingress tests | `--test tenant_ingress`; CLOCK_GATE_SCOPED | ⬜ pending |
| 14-21-01 | 21 | 4 | SEC-02 | TokenUiAuth + session codec | `cargo test -p ostro-server --lib -- p14::ui_auth` | ⬜ pending |
| 14-21-02 | 21 | 4 | SEC-02 | UI login/logout, gating, e2e | `-p ostro-ui --test p14_ui_auth`; `-p ostro-server --test p14_ui_e2e`; template grep | ⬜ pending |
| 14-22-01 | 22 | 4 | QA-05 | Jepsen smoke (all faults, auth + audit) | `OSTRO_JEPSEN_SECS=180 OSTRO_JEPSEN_SEED=1401 --release --test jepsen_smoke -- --ignored` | ⬜ pending |
| 14-22-02 | 22 | 4 | QA-05 | Jepsen gate + runner | `OSTRO_JEPSEN_SECS=300 --test jepsen_gate -- --ignored` (task); `jepsen-run.sh gate` (2 × 30 min, recorded) | ⬜ pending |
| 14-23-01 | 23 | 4 | QA-04 | Bytes-on-disk benchmarks | `--release --test bench_single --test bench_rf3 -- --ignored` | ⬜ pending |
| 14-23-02 | 23 | 4 | QA-05 | Soak criteria (pre-registered), driver, smoke | criteria commit precedes driver commit; `OSTRO_SOAK_SECS=300 --test soak_smoke -- --ignored` | ⬜ pending |
| 14-24-01 | 24 | 4 | SEC-05 | Threat model, security model, fuzzing doc, findings tracker | STRIDE keyword + evidence-reference checks | ⬜ pending |
| 14-24-02 | 24 | 4 | SEC-05 | Auth matrix + review packet | `bash scripts/security-packet.sh --update-docs`; secret scan | ⬜ pending |
| 14-25-01 | 25 | 4 | DIST-03 | docs-check + getting started ×3 | `bash scripts/docs-check.sh docs/getting-started docs/migrations` | ⬜ pending |
| 14-25-02 | 25 | 4 | DIST-03 | Remaining guides + README + index | `bash scripts/docs-check.sh <own files + migrations>` | ⬜ pending |
| 14-26-01 | 26 | 5 | SEC-02..04, DISP-06 | Cross-feature + race scenarios (tier B) + fixes | `cargo test -p ostro-repl-conformance --test p14_integration` (+ failpoints `audit_stall`) | ⬜ pending |
| 14-26-02 | 26 | 5 | all | Full suite ×3, full-tree docs-check, smoke re-runs | `cargo test --workspace` ×3; `docs-check.sh`; `helm-check.sh` | ⬜ pending |
| 14-27-01 | 27 | 6 | all | Permanent CI steps + gate run + final measurements | step greps; list diff; SHA pins; deploy/scripts kubectl grep; gate reports present | ⬜ pending |
| 14-27-02 | 27 | 6 | all | 1.0.0-rc.1 bump, local tag, checklist, CHANGELOG | version + `--version`; compat tests; deny; tag exists; checklist keywords | ⬜ pending |
| 14-27-03 | 27 | 6 | all | RESERVATIONS flip, REQUIREMENTS ticks, VALIDATION, STATE, DECISIONS | `grep / doc checks` | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Success Criteria → Evidence

| SC | Statement | Evidence | Plans |
|----|-----------|----------|-------|
| SC1 / SEC-02, DISP-06, SEC-03, SEC-04 | Scoped and per-tenant tokens enforced; tenants see only their own resources; per-tenant concurrency, fuel/s, memory and event-rate quotas with weighted fair dispatch; optional signed-module enforcement rejects unsigned uploads; deploys, activations, secret access, cancels/terminates and token changes in the audit log | `auth_scopes` (route × scope matrix + exported `docs/security/auth-matrix.md`), `auth_forwarding`, `tenant_isolation`, `tenant_triggers`, `p14_admin_api`, `p14_ui_e2e`; `fair_tenant`, `tenant_quotas` (Jain ≥ 0.9, noisy neighbour), `tenant_ingress`; `signed_modules` (incl. cluster-wide policy), `p14_signing`; `audit_journal`, `audit_crash`, `audit_log` (one record per action, fail closed, secret access coalesced), `audit_cluster`; cross-feature `p14_integration` | 14-02..06, 14-15..21, 14-26 |
| SC2 / SEC-05 | Fuzzing (wasm-smith components, malformed WIT values) runs continuously; external review closes with no open critical/high | `corpus_replay` on every ci-local; `fuzz-smoke.sh` 60 s × 15 targets at the gate; `wasm_targets` (evil_guest, smith_component); `.github/workflows/fuzz.yml` scheduled (remote: human_needed); threat model + review packet + findings tracker (review: human_needed) | 14-07, 14-08, 14-24, 14-27 |
| SC3 / QA-05 | Jepsen-style 3-node run (partitions, clock skew, kill -9, disk stalls): 0 lost acked runs, 0 duplicate journal entries, fencing holds; 7-day soak at 70% stable | `checker_selftest`, `harness_smoke`, `nemesis_faults`, `jepsen_smoke`, `jepsen_gate` (2 seeds × 30 min, auth + audit, audit chains verified); `soak_smoke` 30 min at 70% with pre-registered `docs/soak.md` criteria; 7-day soak human_needed | 14-09, 14-10, 14-22, 14-23 |
| SC4 / QA-04 | Benchmarks verify acked work and bytes on disk: single node ≥ 10,000 steps/s; 3-node RF=3 ≥ 5,000 steps/s with p99 commit < 20 ms on LAN | `bench_single`, `bench_rf3` (acked == on disk always asserted; bytes/step reported); thresholds asserted with `OSTRO_BENCH_ENFORCE=1` on native/LAN (human_needed); WSL numbers advisory | 14-23 |
| SC5 / DIST-01..03 | Static linux binaries, signed + notarized macOS binaries, `FROM scratch` image, install script, Helm chart (KEDA examples, NetworkPolicies); docs for getting started per language, determinism, connectors, operations/sizing, migrations | `release-dry-run.sh` (static, size, reproducible, SHA256SUMS + minisig), `test-install.sh` (positive/tampered/bad signature), `release.yml` (macOS notarized + stapled pkg: human_needed), `build-image.sh` (`verify: OK`; push human_needed), `helm-check.sh` (lint + kubeconform 1.34 + KEDA 2.21; live install human_needed), docs set + `docs-check.sh` | 14-11..14, 14-25, 14-27 |

---

## Wave 0 Requirements

- [ ] `ostro_api::auth` (Principal, scopes, Method inventory, Authenticator, PRINCIPAL), `ostro_api::phase14` traits, ext structs 112–117, `ostro_types::audit`, the `p14/` seam with call sites, `crates/ostro-server/tests/p14_skeleton.rs` — 14-01
- [ ] `crates/ostro-fuzz-targets` (dispatch table + stubs), `fuzz/` workspace with 15 target bins on the pinned nightly — 14-01
- [ ] `crates/ostro-jepsen` skeleton (`tests/skeleton.rs`, `ostro-soak` bin stub) — 14-01
- [ ] `scripts/fetch-tools.sh` (kubeconform, crane, shellcheck, rsign2, cargo-fuzz, rcodesign) — 14-01
- [ ] Test files created by their feature plans before implementation (TDD order): `p14_tokens`, `p14_authn`, `p14_guard` (14-03); `audit_journal`, `audit_crash` (14-04); `p14_signing` (14-05); `fair_tenant`, `tenant_quotas` (14-06); `corpus_replay` (14-07); `wasm_targets` (14-08); `checker_selftest` (14-09); `auth_scopes` (14-15); `tenant_isolation` (14-16); `audit_log` (14-18); `signed_modules` (14-19)

---

## Manual-Only / Human-Needed

| Behavior | Requirement | Why Manual | Acceptance path (exact) |
|----------|-------------|------------|-------------------------|
| External security review | SEC-05 | Booking an external reviewer is the user's action | Hand over `target/security-review/<date>/ostro-security-review-<commit>.tar.gz` (from `bash scripts/security-packet.sh`); reviewer findings entered in `docs/security/review-findings.md`; **accepted when** no critical/high finding is in a status other than `fixed-verified` or `accepted-by-user` (with justification) and the reviewer sign-off line (name, date, scope, commit) is filled |
| macOS signing + notarization | DIST-01 | Needs an Apple Developer ID and App Store Connect API key | Add the `APPLE_*` secrets (14-11 `user_setup`), run `release.yml` (`workflow_dispatch`, `dry_run=false`); **accepted when** `spctl -a -vv -t install ostro.pkg` reports accepted (Notarized Developer ID), `xcrun stapler validate ostro.pkg` succeeds and `codesign --verify --strict --verbose=2 ostro` passes on a clean macOS machine |
| Container image + Helm chart push | DIST-01, DIST-02 | Registry credentials; no pushes from this environment | `release.yml` `image` + `chart` jobs (or `bash scripts/build-image.sh --push <ref>` in CI); **accepted when** `crane digest <ref>` equals the index digest of the locally built `target/image/oci` for the same commit and `helm pull oci://…/ostro --version 1.0.0-rc.1 && helm lint` passes; optional live install on kind/k3d or a cluster the user explicitly approves (never the local microk8s) |
| 7-day soak at 70% rated load | QA-05 | Duration | `ostro-soak --nodes 3 --duration 168h --load 0.7 --rated <steps/s from bench on that machine> --out <dir>`; **accepted when** `verdict.json` is PASS for every criterion in `docs/soak.md` |
| 3-node LAN benchmarks + native perf | QA-04 | Needs 3 machines on a LAN and a native NVMe 8 vCPU box (D15); WSL is advisory | `OSTRO_BENCH_ENFORCE=1 cargo test --release -p ostro-jepsen --locked --test bench_single -- --ignored` on the native box and `… --test bench_rf3` with `--hosts` per `scripts/jepsen-run.sh --hosts` instructions; **accepted when** single node ≥ 10,000 verified steps/s and RF=3 ≥ 5,000 verified steps/s with p99 commit < 20 ms |
| Multi-hour / multi-machine Jepsen run | QA-05 | Duration and hardware | `bash scripts/jepsen-run.sh long` (4 h) and the `--hosts` procedure; **accepted when** all checkers pass |
| Remote CI | all | No git remote yet (FOUND-05) | Push; **accepted when** ci.yml is green on the rc commit (lint, x86_64/aarch64 musl, macOS, gnu + failpoints, fuzz smoke job, helm-check, release dry run, Jepsen smoke, clean-machine job) and `fuzz.yml` has ≥ 1 green scheduled run |
| Publishing | DIST-01 | Release credentials and public URLs | Generate the real minisign key offline (`rsign generate`), put the public key in `docs/install.md` and `install.sh`, run `release.yml` with `dry_run=false`; **accepted when** the GitHub release has all artefacts + `SHA256SUMS` + `SHA256SUMS.minisig`, `curl -fsSL <install.sh URL> \| OSTRO_REQUIRE_SIGNATURE=1 sh` works on a clean Linux and macOS machine, and SDK packages are published per Phase 9 |
| User review of D-P14-3/4, D-P14-12, D-P14-10 | SEC-02..04 | Adopted defaults, user may override | Read 14-DECISIONS.md; overrides folded into any plan not yet started |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60 s at task level
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
