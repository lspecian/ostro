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
> Source: 14-RESEARCH.md § Validation Architecture, mapped onto plans 14-01, 14-01b, 14-02..14-34 (35 plans, 8 waves; revised after two plan checks and by the 2026-11-26 user decisions and their round-1 check: 14-28..14-34 added, the gate stays 14-27). Adopted decisions: 14-DECISIONS.md (D-P14-1..D-P14-53). Execution follows the durable executor brief (D-P14-41).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test harness (`cargo test`, Rust 1.100.0), proptest 1.12 (token codec, fair scheduler model, journal tamper, signature parsing), `fail` 0.5.1 failpoints (`auth::`, `audit::` + Phase 2/5 `wal::`/`state::` sites used by the disk nemesis), insta snapshots (CLI), the Phase 12/13 `ostro-repl-conformance` harness (tier B in-process clusters; tier C `ProcCluster` + Rust `ChaosProxy` on `127.r.r.n`), the new `ostro-jepsen` crate (seeded nemeses, history, lost/duplicate/fencing checkers, bench, soak), `ostro-fuzz-targets` (stable corpus replay) + the separate `fuzz/` workspace (cargo-fuzz 0.13.2, libfuzzer-sys 0.4.13, arbitrary 1.5.0, wasm-smith =0.261.0, dated nightly pinned in `fuzz/rust-toolchain.toml`). Shell/tools: helm 3.17 (`lint`/`template` only, `KUBECONFIG=/dev/null`), kubeconform v0.8.0, crane v0.22.1, shellcheck v0.11.0, rsign2 0.6.7, and the test services SeaweedFS `weed` 4.54 + Apache Kafka 4.1.x with a pinned Temurin 21 JRE (`scripts/test-services.sh`) — all via `scripts/fetch-tools.sh` into `target/tools/`. Kafka client tests: `ostro-kafka` (`testkit` feature, codec fixtures, broker suite). |
| **Config** | No new config file. `FAILPOINT_CRATES` unchanged unless a Phase 14 failpoint suite lands in a crate not yet listed (ostro-server is already listed since Phase 5/11); kept equal to the ci.yml "Failpoint suites" step. 14-27 adds fuzz smoke, helm-check, release dry run + install tests, image verify, docs-check, Jepsen smoke and `check_p14_gates` to ci-local and ci.yml; `.github/workflows/fuzz.yml` (scheduled) and `.github/workflows/release.yml` (tags/dispatch) are new workflows. |
| **External services** | Loopback test services only, started by `scripts/test-services.sh` (SeaweedFS `weed mini`, never MinIO; Apache Kafka 4.1 KRaft) locally or inside `ostro-k8s-ci` pods; GitHub ci.yml uses the SeaweedFS and `apache/kafka` docker steps. No Docker locally. **The microk8s cluster is used for CI/tests/measurements only** through `ostro-k8s-ci` (namespace `ostro-ci`, user approval 2026-11-24; never slot 2; no kubectl): never for product deploys, and the Helm chart is only linted/templated, never installed. No registry pushes (a loopback `crane registry serve` is local). Executors never push; the orchestrator fast-forwards main to github.com/lspecian/ostro. Apple notarization, registry pushes, remote CI acceptance, the external security review, the 7-day soak, native/LAN benchmarks and the optional Chronik check are human_needed. |
| **Quick run command** | `cargo test -p ostro-server --locked --test <plan test> -- --test-threads=1` · `cargo test -p ostro-fuzz-targets --locked --test corpus_replay` · `cargo test -p ostro-jepsen --locked --test checker_selftest` · `cargo test -p ostro-backup --locked --test cluster_cut` · `cargo test -p ostro-triggers --locked --test kafka` · `cargo test -p ostro-kafka --locked --features testkit --test codec` · `bash scripts/helm-check.sh` · `bash scripts/docs-check.sh <paths>` |
| **Full suite command** | `ostro-k8s-ci --parallel <worktree>` = `scripts/ci-local.sh` on the `ostro-ci` cluster, Bash `run_in_background: true` (from 14-27 also test services, fuzz smoke, helm-check, docs-check, Jepsen smoke, Kafka and cluster-restore legs; the musl groups add release dry run + install tests + image verify) |
| **Phase gate commands** | 1. `ostro-k8s-ci --parallel <worktree>` (ci-local with musl, test services, revision legs).<br>2. `OSTRO_FUZZ_SECS=60 bash scripts/fuzz-smoke.sh` (15 targets, 0 crashes).<br>3. `bash scripts/jepsen-run.sh gate` (2 seeds × 30 min, all four fault classes, auth + audit on; all checkers pass).<br>4. `cargo test --release -p ostro-jepsen --locked --test bench_single --test bench_rf3 -- --ignored --nocapture --test-threads=1` (acked == on disk; thresholds advisory without `OSTRO_BENCH_ENFORCE=1`).<br>5. `OSTRO_SOAK_SECS=1800 cargo test --release -p ostro-jepsen --locked --test soak_smoke -- --ignored --test-threads=1` (verdict PASS).<br>6. `bash scripts/release-dry-run.sh && bash scripts/test-install.sh`.<br>7. `bash scripts/build-image.sh …` (`verify: OK`).<br>8. `bash scripts/helm-check.sh` (0 invalid, 0 skipped).<br>9. `bash scripts/docs-check.sh` (full tree).<br>10. `bash scripts/security-packet.sh` (packet + sha256, secret scan clean).<br>11. Strict CLOCK_GATE (scope incl. `crates/ostro-bridge/src`).<br>12. `OSTRO_KAFKA_CRASH_ITERS=20 cargo test -p ostro --features failpoints --locked --test kafka_crash -- --ignored --test-threads=1` with the Kafka test service (exactly one run per offset).<br>13. `cargo test -p ostro-repl-conformance --locked --test process_cluster_restore -- --ignored --test-threads=1` (file:// and SeaweedFS variants; Pitfall 1 + acked-at-or-below-cut checks).<br>14. `process_shutdown controlled_shutdown_followed_partition_stall` (p99 < 1 s) and the P13 mutant matrix incl. `readd_stale_progress`. |
| **Estimated runtime** | Quick: < 60 s per command. Tier-B Phase 14 groups: 30–120 s. Full ci-local: ~40–55 min debug (+ fuzz/Jepsen smokes ~10 min). Jepsen gate: ~70 min. Soak smoke: 30 min. Fuzz smoke: ~15 min. |

---

## Sampling Rate

- **After every task commit:** the task's `<automated>` command, plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings` for touched crates (with `--features failpoints` where failpoint tests exist) and CLOCK_GATE_SCOPED for server/partition/bridge diffs.
  - Tasks touching `ostro-partition` also run `PROPTEST_CASES=128 cargo test -p ostro-partition --locked --test rebuild_digest` and `--test format_corpus`.
  - Tasks touching `ostro-meta` also run `cargo test -p ostro-meta --locked` (goldens append-only).
  - Tasks touching `ostro-api`/`ostro-proto` also run `cargo test -p ostro-api --locked --test parity --test openapi`.
  - Tasks touching decoders/apply (fuzz crash fixes) also run `cargo test -p ostro-fuzz-targets --locked --test corpus_replay`.
- **After every plan / wave:** `ostro-k8s-ci --parallel` on the plan's worktree (`/home/lspecian/.local/share/ostro-orchestrator/wt/14-NN`) at its last commit; from wave 3 on also the plan's tier-B group plus the repl-conformance suite (on the cluster via `--cmd` when long); plans with broker/bucket legs run them once via `ostro-k8s-ci --cmd` with `scripts/test-services.sh`.
- **Before `/gsd:verify-work`:** the 14-27 gate.
- **Max feedback latency:** 60 s for task-level unit checks; tier-B groups ≤ 2 min; Jepsen/bench/soak/fuzz campaigns are gate level (task-level smokes ≤ 5 min).
- Piped verify commands use `set -euo pipefail`; cargo test filters go after `--`; no `|| true` masking; negations are `if …; then exit 1; fi`; filtered runs must report ≥ 1 passed test (`test result: ok\. [1-9][0-9]* passed`).

---

## Wave Structure

| Wave | Plans | Exclusive shared files |
|------|-------|------------------------|
| 1 | 14-01 gate + inventories + contracts + **all manifests** | **Cargo files**, `fuzz/Cargo.toml`/toolchain, new-crate skeletons, RESERVATIONS, STATE, `ostro-types` audit/compat, meta `ext.rs`, ostro-api `auth`/`phase14`/`error`, bridge `audited.rs` |
| 2 | 14-01b seam + stubs + call sites + config + UI scope layer + CLI/client stubs + fuzz bins + tools + CI | **server.rs / config.rs / main.rs / p14/mod.rs**, all `p14/*`/`audit/*` stubs, hook call sites in `control/{runs,modules,migrate,signals,tokens,lifecycle}.rs` + `rerun.rs`, worker sites, client/CLI stubs, `fuzz/fuzz_targets/*`, `scripts/fetch-tools.sh`, CI sync |
| 3 | 14-02 API surface; 14-03 tokens/authn/bootstrap/guard; 14-04 audit journal; 14-05 signature verifiers; 14-06 dispatch quotas; 14-08 Wasm fuzz targets; 14-09 Jepsen core; 14-11 release binaries + install + release.yml; 14-12 image; 14-13 Helm; 14-14 migration guides; **14-28 follow-ups; 14-29 cluster-restore primitives; 14-31 Kafka transport/TLS/SASL; 14-33 ISR self-shrink** | 14-02: **ostro-api / ostro-proto / OpenAPI**. 14-03: `p14/{tokens,authn,bootstrap,guard}`. 14-04: `audit/*`. 14-05: `p14/signing.rs`. 14-06: `leader/fair.rs`, `flow/tenant.rs`, `p14/quota_view.rs`, **`executor.rs`**. 14-08: `evil_guest.rs`, `smith.rs`, Wasm corpora. 14-09: `ostro-jepsen` workload/history/run/report/checkers/partition+kill. 14-11: `install.sh`, release scripts, `release.yml`. 14-12: `deploy/image`, `build-image.sh`, NOTICE. 14-13: `deploy/helm/**`, helm scripts. 14-14: `docs/migrations/**`. 14-28: `ostro-cli/src/watch*`, notify removal in manifests + deny.toml (exception to the 14-01 rule), the binding-conflict hunk of `control/modules.rs`, `webhook_verify.rs`, getting-started "Changing a workflow". 14-29: `ostro-backup/src/{meta_head,cluster_cut}.rs`, uploader meta hunk, the partition replay observer. 14-31: `ostro-kafka/src/{conn,versions,sasl}.rs` + transport/auth tests. 14-33: `ostro-log/src/replica/{isr,tracker}.rs`, `ostro-cluster/src/shutdown.rs`, `ostro-server/src/p13/mod.rs` (`stop_following`), handoff scenarios, `process_shutdown.rs`, P13 mutant registry. 14-29 also `ostro-server/src/services/uploader.rs` and `ostro-partition/src/{replay_observer,node}.rs` |
| 4 | 14-07 fuzz infra + 13 targets; 14-10 fault knobs; 14-15 scopes; 14-16 tenant scoping; 14-17 tenant/token admin; 14-18 audit capture; 14-19 signed modules; **14-30 whole-cluster restore; 14-34 Kafka read path + broker suite + size** | 14-07: fuzz target modules, `corpus_replay.rs`, non-Wasm corpora/regressions, `fuzz-smoke.sh`, `fuzz.yml`, `fuzz/Cargo.lock`. 14-10: `clock.rs`, `p14/test_hooks.rs`, nemesis clock/disk. 14-15: `p14/{authz,principal_fwd}`, `route/peer.rs`, node-API ingest. 14-16: `p14/{tenant_scope,upload_tenant}`, `control/list.rs`, index file, trigger files. 14-17: `p14/{tenants_api,tokens_api}`, client/CLI tenant+auth. 14-18: `p14/{audit_hooks,audit_api}`, bridge `audited.rs`, client/CLI audit. 14-19: `p14/{module_sign,security_api,reseal}`, client/CLI security, CLI `deploy.rs`. 14-30: `ostro-backup/src/{restore,cluster_restore}.rs`, `process_cluster_restore.rs`, `docs/cluster-restore.md`, bottomless doc section. 14-34: `ostro-kafka/src/{metadata,fetch,offsets,codec,testkit}.rs`, codec/broker tests, fixtures, the `check_musl_size` line (only if raised to 70 MB) |
| 5 | 14-20 ingress quotas; 14-21 UI auth; 14-22 Jepsen runs; **14-32 Kafka trigger** | 14-20: `p14/ingress.rs`. 14-21: `ostro-ui/**`, `p14/ui_auth.rs`. 14-22: Jepsen smoke/gate, `jepsen-run.sh`. 14-32: `ostro-triggers/src/kafka.rs` (+ lib line), `p14/kafka.rs`, `kafka_trigger.rs`, `kafka_failover.rs`, `kafka_crash.rs`, `docs/kafka-triggers.md` |
| 6 | 14-23 benchmarks + soak (after 14-22); 14-24 threat model + packet; 14-25 docs set | 14-23: `bench.rs`, `soak.rs`, `ostro-soak`, tests, `docs/soak.md`, `docs/benchmarks.md`. 14-24: `docs/security/**`, `docs/security-model.md`, `security-packet.sh`. 14-25: `README.md`, `docs/README.md`, guides, docs-check scripts |
| 7 | 14-26 cross-feature integration (incl. Kafka/restore/self-shrink with auth, audit and quotas) + suite green (sole) | `scenarios/p14.rs`, `p14_integration.rs`, `p14_regressions.rs`, fixes across Phase 14 files |
| 8 | 14-27 gate + 1.0.0-rc.1 | ci files, version fields + Cargo.lock, Chart version, release checklist, CHANGELOG, tracking files |

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirements | Task | Automated command(s) | Status |
|---------|------|------|--------------|------|----------------------|--------|
| 14-01-01 | 01 | 1 | all | Phases 5–13 gate (SUMMARYs + every VERIFICATION), seam/method/tenant-field inventories, lease clock source, RESERVATIONS rows, STATE | `grep / doc checks` over 05–13 SUMMARY/VERIFICATION, STATE/RESERVATIONS rows incl. W15 names | ⬜ pending |
| 14-01-02 | 01 | 1 | all | Contracts: audit sink, P14 features, ext 112–117 (fingerprint), auth (Anonymous default), phase14 traits, auth codes, audited resolver | workspace build warning-free; `-p ostro-types/-p ostro-meta/-p ostro-api --lib/-p ostro-bridge --lib`; goldens append-only | ⬜ pending |
| 14-01-03 | 01 | 1 | all | Dependency matrix + every manifest + new-crate skeletons + fuzz manifest/nightly | `cargo deny check`; forbidden crates; per-manifest dependency greps; `cargo tree` wasm-smith 0.261.0; 15 `[[bin]]`; skeleton tests | ⬜ pending |
| 14-01b-01 | 01b | 2 | all | p14 seam + stubs + config | `--test config`, `--test p14_skeleton`; version_gate/metrics_tenant_labels/head_export_interval greps | ⬜ pending |
| 14-01b-02 | 01b | 2 | all | Call sites (upload before existing-sha, version_gate, ingress, audit, workers), main.rs intercepts, CLI/client stubs | call-site greps + order check; `-p ostro-server/-p ostro-cli/-p ostro-client` tests | ⬜ pending |
| 14-01b-03 | 01b | 2 | all | Fuzz bins, tool pins, test services (S3 + Kafka listeners), shared size limit, CI sync | nightly fuzz build; fetch-tools; FAILPOINT list diff; no manifest diff; CLOCK_GATE_SCOPED | ⬜ pending |
| 14-02-01 | 02 | 3 | SEC-02..04, DISP-06 | Protos + wire tests | `cargo test -p ostro-proto --test p14_wire` | ⬜ pending |
| 14-02-02 | 02 | 3 | SEC-02 | Authentication layer + interceptor | `cargo test -p ostro-api --test p14_authn` | ⬜ pending |
| 14-02-03 | 02 | 3 | SEC-02..04 | Adapters, signature carriage, tenant hooks, fakes, OpenAPI, parity | `cargo test -p ostro-api --test p14_routes --test parity --test openapi`; OpenAPI diff additive | ⬜ pending |
| 14-03-01 | 03 | 3 | SEC-02 | Token codec | `cargo test -p ostro-server --test p14_tokens` | ⬜ pending |
| 14-03-02 | 03 | 3 | SEC-02 | MetaAuthenticator + bootstrap (online/offline) | `cargo test -p ostro-server --test p14_authn` (+ failpoints) ; CLOCK_GATE_SCOPED | ⬜ pending |
| 14-03-03 | 03 | 3 | SEC-02 | Guard decorator (final); call outside any scope denied | `cargo test -p ostro-server --test p14_guard`; `cargo test -p ostro-server` | ⬜ pending |
| 14-04-01 | 04 | 3 | SEC-04 | Record/chain/segment/verify/retention (TDD) | `cargo test -p ostro-server --test audit_journal -- chain tamper rotate torn retention ignored_by` | ⬜ pending |
| 14-04-02 | 04 | 3 | SEC-04 | Writer, fail closed, coalescing, OTLP export, crash test | `--test audit_journal` (+ failpoints); `OSTRO_AUDIT_CRASH_ITERS=10 --release --test audit_crash -- --ignored` | ⬜ pending |
| 14-05-01 | 05 | 3 | SEC-03 | minisign + cosign keyed verification, interop vectors (TDD) | `cargo test -p ostro-server --test p14_signing`; no secret key fixture; no sigstore | ⬜ pending |
| 14-06-01 | 06 | 3 | DISP-06 | Tenant level in fair.rs + model tests | `cargo test -p ostro-partition --test fair_tenant`; `--lib -- fair`; `rebuild_digest` | ⬜ pending |
| 14-06-02 | 06 | 3 | DISP-06 | Quota view + TenantFlow + executor charge | `cargo test -p ostro-server --lib -- flow::tenant p14::quota_view`; CLOCK_GATE_SCOPED | ⬜ pending |
| 14-06-03 | 06 | 3 | DISP-06 | Server quota + fairness tests (×3) | `cargo test -p ostro-server --test tenant_quotas` ×3 | ⬜ pending |
| 14-07-01 | 07 | 4 | SEC-05 | 13 target bodies + seeds + stable replay | `cargo test -p ostro-fuzz-targets --test corpus_replay`; ≥ 65 seeds; clippy | ⬜ pending |
| 14-07-02 | 07 | 4 | SEC-05 | fuzz-smoke, nightly lockfile, scheduled workflow, triage | `OSTRO_FUZZ_SECS=20 bash scripts/fuzz-smoke.sh …`; fuzz.yml parse + SHA pins | ⬜ pending |
| 14-08-01 | 08 | 3 | SEC-05 | evil_guest | `cargo test -p ostro-fuzz-targets --test wasm_targets -- evil` | ⬜ pending |
| 14-08-02 | 08 | 3 | SEC-05 | smith_component + smoke | `--test wasm_targets`; `cargo +nightly fuzz run smith_component -- -max_total_time=20` | ⬜ pending |
| 14-09-01 | 09 | 3 | QA-05 | History + checkers + self-tests | `cargo test -p ostro-jepsen --test checker_selftest` | ⬜ pending |
| 14-09-02 | 09 | 3 | QA-05 | Workload, partition/kill nemeses, seeded runs, smoke | `OSTRO_JEPSEN_SECS=180 --release --test harness_smoke -- --ignored` | ⬜ pending |
| 14-10-01 | 10 | 4 | QA-05 | SkewClock + test endpoints (failpoints only) | `-p ostro-partition --features failpoints --test skew_clock`; `-p ostro-server --test p14_test_hooks` (both builds); clippy both feature sets | ⬜ pending |
| 14-10-02 | 10 | 4 | QA-05 | Clock + disk nemeses | `OSTRO_JEPSEN_SECS=120 --release --test nemesis_faults -- --ignored` | ⬜ pending |
| 14-11-01 | 11 | 3 | DIST-01 | Release dry run | `bash scripts/release-dry-run.sh`; `sha256sum -c` | ⬜ pending |
| 14-11-02 | 11 | 3 | DIST-01 | install.sh + local install tests + shellcheck | `shellcheck …`; `bash scripts/test-install.sh` | ⬜ pending |
| 14-11-03 | 11 | 3 | DIST-01 | release.yml + validation | YAML/job/secret-scope/SHA-pin checks | ⬜ pending |
| 14-12-01 | 12 | 3 | DIST-01 | crane spike, CA pin, build-image.sh | `bash scripts/build-image.sh …`; OCI layout present; `--push` refused outside CI | ⬜ pending |
| 14-12-02 | 12 | 3 | DIST-01 | Image verification + README | `build-image.sh` prints `verify: OK` | ⬜ pending |
| 14-13-01 | 13 | 3 | DIST-02 | Chart | `KUBECONFIG=/dev/null helm lint --strict` ×4; 5 NetworkPolicies; ScaledObject renders | ⬜ pending |
| 14-13-02 | 13 | 3 | DIST-02 | KEDA schemas + helm-check (kubeconform) | `bash scripts/helm-check.sh` (0 invalid/skipped) | ⬜ pending |
| 14-14-01 | 14 | 3 | DIST-03 | Cron/queue-table guide + index | keyword + marker checks | ⬜ pending |
| 14-14-02 | 14 | 3 | DIST-03 | Temporal + Restate guides | keyword + marker checks | ⬜ pending |
| 14-15-01 | 15 | 4 | SEC-02 | Scope table + matrix (fakes in P14 slots; gRPC from descriptor set; exact key set) + export | `OSTRO_EXPORT_AUTH_MATRIX=… cargo test -p ostro-server --test auth_scopes` | ⬜ pending |
| 14-15-02 | 15 | 4 | SEC-02 | Principal propagation over forwarding | `cargo test -p ostro-server --test auth_forwarding`; CLOCK_GATE_SCOPED | ⬜ pending |
| 14-16-01 | 16 | 4 | SEC-02 | Tenant scoping, tenant-field force/reject, foreign-tenant upload leaves no records, W11 cases | `cargo test -p ostro-server --test tenant_isolation` | ⬜ pending |
| 14-16-02 | 16 | 4 | SEC-02 | Tenant index + tenant-correct triggers | `--test tenant_triggers`; `rebuild_digest`, `format_corpus` | ⬜ pending |
| 14-17-01 | 17 | 4 | SEC-02, DISP-06 | Tenants + tokens admin services | `cargo test -p ostro-server --test p14_admin_api` | ⬜ pending |
| 14-17-02 | 17 | 4 | SEC-02, DISP-06 | Client + CLI verbs | `cargo test -p ostro-cli --test p14_cli`; `--help` checks | ⬜ pending |
| 14-18-01 | 18 | 4 | SEC-04 | Guard audit hooks (wave-3 actions), fail closed, audit_commit_failed, worker/auth events | `cargo test -p ostro-server --test audit_log` (+ failpoints) | ⬜ pending |
| 14-18-02 | 18 | 4 | SEC-04 | Coalesced secret-access audit | `--test audit_log -- secret_access`; CLOCK_GATE_SCOPED | ⬜ pending |
| 14-18-03 | 18 | 4 | SEC-04 | Cluster audit query + CLI | `--test audit_cluster`; `-p ostro-cli --test p14_audit_cli` | ⬜ pending |
| 14-19-01 | 19 | 4 | SEC-03 | Upload/activation/version gates (upload before existing-sha; fingerprint re-verify), cluster-wide policy; start-with-version, migrate-to-unsigned, cross-tenant unsigned re-upload → 403 | `cargo test -p ostro-server --test signed_modules` | ⬜ pending |
| 14-19-02 | 19 | 4 | SEC-03 | Security API, config seed, reseal | `--test p14_reseal`; CLOCK_GATE_SCOPED | ⬜ pending |
| 14-19-03 | 19 | 4 | SEC-03 | Client + CLI + deploy --signature | `cargo test -p ostro-cli --test p14_security_cli`; `deploy --help` | ⬜ pending |
| 14-20-01 | 20 | 5 | DISP-06 | admit_ingress unit tests | `cargo test -p ostro-server --lib -- p14::ingress` | ⬜ pending |
| 14-20-02 | 20 | 5 | DISP-06 | Server ingress tests | `--test tenant_ingress`; CLOCK_GATE_SCOPED | ⬜ pending |
| 14-21-01 | 21 | 5 | SEC-02 | TokenUiAuth + session codec | `cargo test -p ostro-server --lib -- p14::ui_auth` | ⬜ pending |
| 14-21-02 | 21 | 5 | SEC-02 | UI login/logout, gating, e2e | `-p ostro-ui --test p14_ui_auth`; `-p ostro-server --test p14_ui_e2e`; template grep | ⬜ pending |
| 14-22-01 | 22 | 5 | QA-05 | Jepsen smoke (all faults, auth + audit) | `OSTRO_JEPSEN_SECS=180 OSTRO_JEPSEN_SEED=1401 --release --test jepsen_smoke -- --ignored` | ⬜ pending |
| 14-22-02 | 22 | 5 | QA-05 | Jepsen gate + runner | `OSTRO_JEPSEN_SECS=300 --test jepsen_gate -- --ignored` (task); `jepsen-run.sh gate` (2 × 30 min, recorded) | ⬜ pending |
| 14-23-01 | 23 | 6 | QA-04 | Bytes-on-disk benchmarks | `--release --test bench_single --test bench_rf3 -- --ignored` | ⬜ pending |
| 14-23-02 | 23 | 6 | QA-05 | Soak criteria (pre-registered), driver, smoke | criteria commit precedes driver commit; `OSTRO_SOAK_SECS=300 --test soak_smoke -- --ignored` | ⬜ pending |
| 14-24-01 | 24 | 6 | SEC-05 | Threat model, security model, fuzzing doc, findings tracker | STRIDE keyword + evidence-reference checks | ⬜ pending |
| 14-24-02 | 24 | 6 | SEC-05 | Auth matrix + review packet | `bash scripts/security-packet.sh --update-docs`; secret scan | ⬜ pending |
| 14-25-01 | 25 | 6 | DIST-03 | docs-check + getting started ×3 | `bash scripts/docs-check.sh docs/getting-started docs/migrations` | ⬜ pending |
| 14-25-02 | 25 | 6 | DIST-03 | Remaining guides + README + index | `bash scripts/docs-check.sh <own files + migrations>` | ⬜ pending |
| 14-26-01 | 26 | 7 | SEC-02..04, DISP-06 | Cross-feature + race scenarios (tier B), full D-P14-13 audit table, fixes | `cargo test -p ostro-repl-conformance --test p14_integration` (+ failpoints `audit_stall`) | ⬜ pending |
| 14-26-02 | 26 | 7 | all | Full suite ×3, full-tree docs-check, smoke re-runs | `cargo test --workspace` ×3; `docs-check.sh`; `helm-check.sh` | ⬜ pending |
| 14-28-01 | 28 | 3 | CLI-08 | Own watcher (inotify/polling/debounce), notify + CC0 exception removed | `cargo test -p ostro-cli --lib -- watch`; `-p ostro --test dev_watch`; `cargo tree -i notify` fails; `cargo deny check` | ⬜ pending |
| 14-28-02 | 28 | 3 | CLI-01, TRIG-01 | Dev-ephemeral module replacement; webhook test-key split; doc | `cargo test -p ostro-server --test dev_replace`; `-p ostro-triggers --test webhook_verify`; modules.rs ordering check | ⬜ pending |
| 14-29-01 | 29 | 3 | BACK-05 | meta/head + multi-member meta snapshots (term-fenced) via `Uploader::with_meta_leader` fed from MetaHandle | `cargo test -p ostro-backup --test meta_head`; `-p ostro-server --test cluster_meta_head` (3 nodes write meta/head); Phase 8/12 backup suites | ⬜ pending |
| 14-29-02 | 29 | 3 | BACK-05 | Replay observer + consistent-cut search (proptest) | `PROPTEST_CASES=256 cargo test -p ostro-backup --test cluster_cut`; `rebuild_digest`, `format_corpus`; p12 grep gates | ⬜ pending |
| 14-30-01 | 30 | 4 | BACK-05 | restore_cluster: cut, partitions, metadata rewrite (epoch = max(meta, head, log)), seed; `repl::mutant::restore_skip_fixpoint` | `cargo test -p ostro-backup --test cluster_restore` (default + failpoints); Phase 8 restore suites | ⬜ pending |
| 14-30-02 | 30 | 4 | BACK-05 | Tier C: 3 nodes → SIGKILL → seed → 2 joins → RF=3 (file:// + SeaweedFS); runbook; replication.md §7 | test-services s3; `OSTRO_FAILPOINT_BIN=… cargo test -p ostro-repl-conformance --test process_cluster_restore -- --ignored` (2 passed, fails on `SKIP:`) | ⬜ pending |
| 14-31-01 | 31 | 3 | TRIG-04 | Transport: framing, ApiVersions negotiation, TLS, SASL PLAIN/SCRAM (fake broker, rcgen CA at test time) | `cargo test -p ostro-kafka --lib`; `--test transport`; no rdkafka/openssl/lz4-sys/zstd-sys; no committed keys | ⬜ pending |
| 14-31-02 | 31 | 3 | TRIG-04 | TLS + SASL against the Kafka 4.1 test broker (SSL + SASL_PLAINTEXT listeners) | test-services start; `--test broker_auth`; fails on `SKIP:` | ⬜ pending |
| 14-34-01 | 34 | 4 | TRIG-04 | Codecs (pure Rust), metadata, coordinator/offsets, fetch, testkit | `cargo test -p ostro-kafka --features testkit --test codec` + `--lib` | ⬜ pending |
| 14-34-02 | 34 | 4 | TRIG-04 | Broker suite, locally captured fixtures, musl size (70 MB rule via the shared limit file) | test-services start; `--test broker` (fails on `SKIP:`); fixtures present; `scripts/binary-size-limit` 60000000 or 70000000 and matching docs | ⬜ pending |
| 14-32-01 | 32 | 5 | TRIG-04 | Pure keys/input/CommitTracker/owner + config validation | `PROPTEST_CASES=512 cargo test -p ostro-triggers --test kafka`; `-p ostro-server --features kafka --lib -- p14::kafka` | ⬜ pending |
| 14-32-02 | 32 | 5 | TRIG-04 | Driver: owner-leader consumers, durable-then-commit, backpressure, leadership move | test-services start; `cargo test -p ostro-server --features kafka --test kafka_trigger`; `-p ostro-repl-conformance --test kafka_failover` (OSTRO_FAILPOINT_BIN); fails on `SKIP:`; `cargo build -p ostro --no-default-features`; CLOCK_GATE_SCOPED | ⬜ pending |
| 14-32-03 | 32 | 5 | TRIG-04 | kill -9 crash test + operator doc | `OSTRO_KAFKA_CRASH_ITERS=20 … --test kafka_crash -- --ignored` (cluster `--cmd`); doc keyword checks | ⬜ pending |
| 14-33-01 | 33 | 3 | CLUS-06 | No re-add from stale progress; controlled-shutdown self-shrink + `stop_following` seam (p13/mod.rs) | `cargo test -p ostro-log --test replica_unit`; `-p ostro-cluster --test handoff_executor`; p12/p13 grep gates | ⬜ pending |
| 14-33-02 | 33 | 3 | CLUS-06 | Tier-B scenarios (stays out of ISR until exit) + mutant bite; tier-C stall measurement | `cargo test -p ostro-repl-conformance --features failpoints --test handoff`; `OSTRO_P13_MUTANT=readd_stale_progress cargo test -p ostro-repl-conformance --features failpoints --test mutants_p13 -- --nocapture` → "FAILED as expected"; `process_shutdown` stall p99 < 1 s (OSTRO_FAILPOINT_BIN) | ⬜ pending |
| 14-27-01 | 27 | 8 | all | Permanent CI steps + gate run + final measurements | step greps; list diff; SHA pins; deploy/scripts kubectl grep; gate reports present | ⬜ pending |
| 14-27-02 | 27 | 8 | all | 1.0.0-rc.1 bump, local tag, checklist, CHANGELOG | version + `--version`; compat tests; deny; tag exists; checklist keywords | ⬜ pending |
| 14-27-03 | 27 | 8 | all | RESERVATIONS flip, REQUIREMENTS ticks, VALIDATION, STATE, DECISIONS | `grep / doc checks` | ⬜ pending |
| 14-27-04 | 27 | 8 | all | CHECKPOINT (user): push the v1.0.0-rc.1 tag? (D-P14-53) | tag exists locally; answer recorded in 14-27-SUMMARY | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Success Criteria → Evidence

| SC | Statement | Evidence | Plans |
|----|-----------|----------|-------|
| SC1 / SEC-02, DISP-06, SEC-03, SEC-04 | Scoped and per-tenant tokens enforced; tenants see only their own resources; per-tenant concurrency, fuel/s, memory and event-rate quotas with weighted fair dispatch; optional signed-module enforcement rejects unsigned uploads; deploys, activations, secret access, cancels/terminates and token changes in the audit log | `auth_scopes` (route × scope matrix + exported `docs/security/auth-matrix.md`), `auth_forwarding`, `tenant_isolation`, `tenant_triggers`, `p14_admin_api`, `p14_ui_e2e`; `fair_tenant`, `tenant_quotas` (Jain ≥ 0.9, noisy neighbour), `tenant_ingress`; `signed_modules` (incl. cluster-wide policy), `p14_signing`; `audit_journal`, `audit_crash`, `audit_log` (one record per action, fail closed, secret access coalesced), `audit_cluster`; cross-feature `p14_integration` | 14-02..06, 14-15..21, 14-26 |
| SC2 / SEC-05 | Fuzzing (wasm-smith components, malformed WIT values) runs continuously; external review closes with no open critical/high | `corpus_replay` on every ci-local; `fuzz-smoke.sh` 60 s × 15 targets at the gate; `wasm_targets` (evil_guest, smith_component); `.github/workflows/fuzz.yml` scheduled (remote: human_needed); threat model + review packet + findings tracker (review: human_needed) | 14-07, 14-08, 14-24, 14-27 |
| SC3 / QA-05 | Jepsen-style 3-node run (partitions, clock skew, kill -9, disk stalls): 0 lost acked runs, 0 duplicate journal entries, fencing holds; 7-day soak at 70% stable | `checker_selftest`, `harness_smoke`, `nemesis_faults`, `jepsen_smoke`, `jepsen_gate` (2 seeds × 30 min, auth + audit, audit chains verified); `soak_smoke` 30 min at 70% with pre-registered `docs/soak.md` criteria; 7-day soak human_needed | 14-09, 14-10, 14-22, 14-23 |
| SC4 / QA-04 | Benchmarks verify acked work and bytes on disk: single node ≥ 10,000 steps/s; 3-node RF=3 ≥ 5,000 steps/s with p99 commit < 20 ms on LAN | `bench_single`, `bench_rf3` (acked == on disk always asserted; bytes/step reported); thresholds asserted with `OSTRO_BENCH_ENFORCE=1` on native/LAN (human_needed); WSL numbers advisory | 14-23 |
| SC6 / TRIG-04, BACK-05 (revision 2026-11-26) | A Kafka-protocol topic-consumer trigger starts runs at-least-once with idempotent starts and commit-after-durable offsets (pure-Rust client); a whole multi-node cluster restores from bottomless backups to a consistent cut and grows back | `transport`, `broker_auth` (TLS + SASL PLAIN/SCRAM against Kafka 4.1), `codec`, `broker` (every codec, offsets), `kafka` proptest, `kafka_trigger`, `kafka_failover`, `kafka_crash` (exactly one run per offset across kill -9 and leadership moves); `meta_head`, `cluster_cut` proptest (consistent + maximal), `cluster_restore` (+ skip-fixpoint mutant), `process_cluster_restore` (file:// + SeaweedFS); Chronik validation optional/human | 14-28..14-34, 14-26, 14-27 |
| SC5 / DIST-01..03 | Static linux binaries, signed + notarized macOS binaries, `FROM scratch` image, install script, Helm chart (KEDA examples, NetworkPolicies); docs for getting started per language, determinism, connectors, operations/sizing, migrations | `release-dry-run.sh` (static, size, reproducible, SHA256SUMS + minisig), `test-install.sh` (positive/tampered/bad signature), `release.yml` (macOS notarized + stapled pkg: human_needed), `build-image.sh` (`verify: OK`; push human_needed), `helm-check.sh` (lint + kubeconform 1.34 + KEDA 2.21; live install human_needed), docs set + `docs-check.sh` | 14-11..14, 14-25, 14-27 |

---

## Wave 0 Requirements

- [ ] `ostro_api::auth` (Principal with Anonymous default, scopes, Method inventory, Authenticator, PRINCIPAL), `ostro_api::phase14` traits, ext structs 112–117, `ostro_types::audit`, all manifests — 14-01
- [ ] the `p14/` seam with call sites, `crates/ostro-server/tests/p14_skeleton.rs` — 14-01b
- [ ] `crates/ostro-fuzz-targets` (dispatch table + stubs) and `fuzz/` manifest + nightly — 14-01; 15 target bins — 14-01b
- [ ] `crates/ostro-jepsen` skeleton (`tests/skeleton.rs`, `ostro-soak` bin stub) — 14-01
- [ ] `scripts/fetch-tools.sh` (kubeconform, crane, shellcheck, rsign2, cargo-fuzz, rcodesign, weed, Kafka, JRE) and `scripts/test-services.sh` — 14-01b
- [ ] `crates/ostro-kafka` skeleton (contract types, `tests/skeleton.rs`) and the `kafka` feature — 14-01; `p14/kafka.rs` stub + `start_triggers` + `TriggerKind::Kafka` + `[triggers] kafka` — 14-01b
- [ ] Test files created by their feature plans before implementation (TDD order): `p14_tokens`, `p14_authn`, `p14_guard` (14-03); `audit_journal`, `audit_crash` (14-04); `p14_signing` (14-05); `fair_tenant`, `tenant_quotas` (14-06); `corpus_replay` (14-07); `wasm_targets` (14-08); `checker_selftest` (14-09); `auth_scopes` (14-15); `tenant_isolation` (14-16); `audit_log` (14-18); `signed_modules` (14-19); `dev_replace` (14-28); `meta_head`, `cluster_meta_head`, `cluster_cut` (14-29); `cluster_restore` (14-30); `transport` (14-31); `codec` (14-34); `kafka` (ostro-triggers), `kafka_trigger` (14-32); `replica_unit` additions (14-33)

---

## Manual-Only / Human-Needed

| Behavior | Requirement | Why Manual | Acceptance path (exact) |
|----------|-------------|------------|-------------------------|
| External security review | SEC-05 | Booking an external reviewer is the user's action | Hand over `target/security-review/<date>/ostro-security-review-<commit>.tar.gz` (from `bash scripts/security-packet.sh`); reviewer findings entered in `docs/security/review-findings.md`; **accepted when** no critical/high finding is in a status other than `fixed-verified` or `accepted-by-user` (with justification) and the reviewer sign-off line (name, date, scope, commit) is filled |
| macOS signing + notarization | DIST-01 | Needs an Apple Developer ID and App Store Connect API key | Add the `APPLE_*` secrets (14-11 `user_setup`), run `release.yml` (`workflow_dispatch`, `dry_run=false`); **accepted when** `spctl -a -vv -t install ostro.pkg` reports accepted (Notarized Developer ID), `xcrun stapler validate ostro.pkg` succeeds and `codesign --verify --strict --verbose=2 ostro` passes on a clean macOS machine |
| Container image + Helm chart push | DIST-01, DIST-02 | Registry credentials; no pushes from this environment | `release.yml` `image` + `chart` jobs (or `bash scripts/build-image.sh --push <ref>` in CI); **accepted when** `crane digest <ref>` equals the index digest of the locally built `target/image/oci` for the same commit and `helm pull oci://…/ostro --version 1.0.0-rc.1 && helm lint` passes; optional live install on kind/k3d or a cluster the user explicitly approves (never the local microk8s: it is approved for CI pods only, no product deploys) |
| 7-day soak at 70% rated load | QA-05 | Duration | `ostro-soak --nodes 3 --duration 168h --load 0.7 --rated <steps/s from bench on that machine> --out <dir>`; **accepted when** `verdict.json` is PASS for every criterion in `docs/soak.md` |
| 3-node LAN benchmarks + native perf | QA-04 | Needs 3 machines on a LAN and a native NVMe 8 vCPU box (D15); WSL is advisory | `OSTRO_BENCH_ENFORCE=1 cargo test --release -p ostro-jepsen --locked --test bench_single -- --ignored` on the native box and `… --test bench_rf3` with `--hosts` per `scripts/jepsen-run.sh --hosts` instructions; **accepted when** single node ≥ 10,000 verified steps/s and RF=3 ≥ 5,000 verified steps/s with p99 commit < 20 ms |
| Multi-hour / multi-machine Jepsen run | QA-05 | Duration and hardware | `bash scripts/jepsen-run.sh long` (4 h) and the `--hosts` procedure; **accepted when** all checkers pass |
| Remote CI | all | GitHub Actions run only after the orchestrator fast-forwards main to github.com/lspecian/ostro (executors never push); workflow files land per D-P14-41 | **accepted when** ci.yml is green on the rc commit (lint, x86_64/aarch64 musl, macOS, gnu + failpoints, fuzz smoke job, helm-check, release dry run, Jepsen smoke, clean-machine job, SeaweedFS + Kafka legs) and `fuzz.yml` has ≥ 1 green scheduled run |
| Publishing | DIST-01 | Release credentials and public URLs | Generate the real minisign key offline (`rsign generate`), put the public key in `docs/install.md` and `install.sh`, run `release.yml` with `dry_run=false`; **accepted when** the GitHub release has all artefacts + `SHA256SUMS` + `SHA256SUMS.minisig`, `curl -fsSL <install.sh URL> \| OSTRO_REQUIRE_SIGNATURE=1 sh` works on a clean Linux and macOS machine, and SDK packages are published per Phase 9 |
| Chronik compatibility (optional) | TRIG-04 | Chronik is the user's system; executors never clone, reference or modify it | Point a `[[triggers.kafka]]` at a Chronik deployment over the Kafka protocol; **accepted when** the 14-32 `kafka_trigger` scenarios (or the doc's manual steps) pass against it, broker version recorded in docs/release-checklist.md |
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
