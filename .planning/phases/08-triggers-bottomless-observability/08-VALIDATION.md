---
phase: 8
slug: triggers-bottomless-observability
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-11-19
---

# Phase 8: Validation Strategy

> This is the per-phase validation contract for feedback sampling during execution.
> Source: 08-RESEARCH.md § Validation Architecture, mapped onto plans 08-01..08-22 (revised after the plan check: 6 blockers, 13 warnings). Adopted decisions: 08-DECISIONS.md (P8-D1..D24).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test harness and cargo-nextest (`.config/nextest.toml`: default, ci and crash profiles), proptest 1.12 + proptest-state-machine, insta, `fail` 0.5.1 failpoints (single-threaded suites), `ostro-crashtest` (kill -9 harness + the 05-19 sharded campaign), and the in-process OTLP collector (`ostro-testkit::otlp`, 08-04). |
| **Config** | `scripts/ci-local.sh`. `FAILPOINT_CRATES` gains `ostro-backup` (08-01) and must equal the ci.yml "Failpoint suites" step. The forbidden-crate check lives in `scripts/forbidden-crates.sh`, which ci-local and ci.yml both call (08-01). The SeaweedFS docker-run step is in ci.yml (canonical spec: 06-DECISIONS § "Shared S3 test spec"; 08-03 reuses or adds it, 08-20 adds the bottomless leg). |
| **Guest build** | `guest_wasm(name)` builds `guests/` (`log-spammer` 08-17, `hook-order` 08-20). |
| **Docker-only legs** | SeaweedFS (S3 conformance, conditional PUT, fencing, `bottomless_s3`) runs only when `OSTRO_TEST_S3_URL`/`_ACCESS_KEY`/`_SECRET_KEY`/`_BUCKET` are set (Phase 6 convention). CI sets them after a `docker run` SeaweedFS step (service containers cannot pass the `mini` command). Locally (WSL, no Docker) the tests print `SKIP` and the item stays **human_needed**. |
| **Quick run command** | `cargo test -p <crate> --test <suite> --locked` (each < 30–60 s) |
| **Full suite command** | `bash scripts/ci-local.sh --no-musl` |
| **Phase gate commands** | 1. `bash scripts/ci-local.sh` (with musl).<br>2. Crash campaign with the Phase 8 fault kinds: `OSTRO_CAMPAIGN_STATE_DIR=target/campaign-p8-gate OSTRO_CAMPAIGN_FAULTS=10000 OSTRO_CAMPAIGN_SHARDS=20 OSTRO_CAMPAIGN_PARALLEL=6 cargo test --release -p ostro-crashtest --features failpoints --test server_campaign -- --ignored --nocapture campaign`, then the same command with `report`.<br>3. `cargo nextest run -P crash -p ostro-crashtest --run-ignored only cron_exactly_once restore_rpo crash_loop`. |
| **Estimated runtime** | Quick: < 60 s. Full: ~10–15 min debug. Crash loops: a few minutes each. Campaign: hours (sharded, resumable). |

---

## Sampling Rate

- **After every task commit:** the task's `<automated>` command, plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings`.
  - ostro-partition / ostro-blob / ostro-backup / ostro-crashtest changes also run `cargo test -p <crate> --features failpoints --locked -- --test-threads=1`.
- **After every plan wave:** `cargo test --workspace --locked` + `cargo deny check` + `bash scripts/forbidden-crates.sh` + the failpoint suites.
- **Before `/gsd:verify-work`:** the 08-22 gate.
- **Max feedback latency:** 60 s for task-level checks. Crash loops and the campaign are `#[ignore]` and run at plan or gate level.
- Piped verify commands use `set -o pipefail` or grep for an explicit result line.

---

## Wave Structure

| Wave | Plans | Exclusive shared files |
|------|-------|------------------------|
| 1 | 08-01 gate + contract + deps + CI grep | Cargo.toml, Cargo.lock, deny.toml, ci-local.sh, ci.yml, command.rs + golden, TABLES, every shared `lib.rs`/`mod.rs` stub declaration, partition `node.rs` (NodeConfig contract), STATE.md, RESERVATIONS.md. **Cross-phase coordination point with Phases 6/7** (full list in 08-01 Task 1). |
| 2 | 08-02 OSNP format, 08-03 ostro-objstore, 08-04 telemetry + config + services seam, 08-05 cron engine, 08-06 webhook verifiers, 08-07 ScheduleFire apply, 08-08 bottomless primitives | 08-03 alone: Cargo.lock (s3s-fs dev-dependency), deny.toml, ci.yml (SeaweedFS). 08-04 alone: ostro-server `server.rs`/`config.rs`/`executor.rs`, testkit lib.rs. 08-07 alone: `ostro-partition/src/apply/*`, `tests/common`, `rebuild_digest.rs`. 08-08 alone: ostro-log, ostro-blob, partition `node.rs`. |
| 3 | 08-09 snapshot service + trim, 08-10 API surface, 08-11 spans | 08-09: partition `leader/*`, `node.rs`, crashtest. 08-10: ostro-api, ostro-proto, server `server.rs` + `control/*` + `tests/common`. 08-11: engine telemetry/activation/journal, server `executor.rs`, `services/run_spans.rs`, `docs/observability.md`. |
| 4 | 08-12 CLI verbs, 08-13 uploader, 08-14 cron driver + schedules, 08-15 webhooks, 08-16 metrics coverage, 08-17 guest logs | 08-12: ostro-cli, root Cargo.lock (only if the dev-dependency is missing). 08-13: ostro-backup uploader modules + tests, `services/uploader.rs`, `docs/bottomless.md`. 08-14: partition `leader/{cron,mod}.rs`, server `server.rs`. 08-16: engine `activation/runtime/replay`, `dispatcher.rs`, `apply/terminal.rs`, meta `handle.rs`, `services/{metrics_poll,run_spans}.rs`. 08-17: engine `host/log.rs` + `sink.rs`, server `sink.rs` + `executor.rs`, guests lock, `docs/observability.md`. |
| 5 | 08-18 restore + backup, 08-19 blob GC | 08-18: ostro-meta, ostro-log import, ostro-cli, `crates/ostro/src/main.rs`, `docs/bottomless.md`. 08-19: ostro-blob. |
| 6 | 08-20 SC1–SC4 proofs, 08-21 SC5 + outbound traceparent + OTLP canary | 08-20: guests lock, ostro-crashtest, ci.yml. 08-21: `crates/ostro-server/src/effect_trace.rs` (implements 06-14 `EffectHooks`), install line in `effects.rs` (cross-phase, append-only), `lib.rs`, STATE.md. |
| 7 | 08-22 phase gate | docs, RESERVATIONS flip, VALIDATION, STATE |

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 8-01-01 | 01 | 1 | (gate) | check + reservations script | 05-{01..20}-SUMMARY present; `05-VERIFICATION.md` `^status: (passed\|human_needed)$` with by-design items only; freeze tag; 05-07 Schedule fields + Webhook; STATE/RESERVATIONS greps | n/a | ⬜ pending |
| 8-01-02 | 01 | 1 | TRIG-02 (contract), BACK-04 (keyspace) | unit + golden (append-only) + InvalidCode index test | `cargo test -p ostro-types --locked && cargo test -p ostro-partition --test store --test store_digest --test rebuild_digest --locked` + golden diff vs `target/08-01-base.sha` | ✅ (extended) | ⬜ pending |
| 8-01-03 | 01 | 1 | OBS-02 (exporter allowed), all deps | script fixture + deny + pipefail tree checks | `bash scripts/forbidden-crates.sh --self-test && bash scripts/forbidden-crates.sh && bash scripts/check-deny-fixtures.sh && cargo deny check` + `bash -c 'set -euo pipefail; …'` no chrono/zstd-sys/openssl-sys | ❌ W0 | ⬜ pending |
| 8-02-01 | 02 | 2 | BACK-04 | unit + proptest (corruption, concurrent export) | `cargo test -p ostro-partition --test snapshot --test blobrefs --test store_digest --locked` | ❌ W0 | ⬜ pending |
| 8-02-02 | 02 | 2 | BACK-04 | integration + failpoints + proptest (snapshot + tail) | `cargo test -p ostro-partition --features failpoints --test snapshot --locked -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 8-03-01 | 03 | 2 | BACK-01 (transport) | spike (deny) | `cargo deny check` + pipefail-safe "no chrono in normal tree" | n/a | ⬜ pending |
| 8-03-02 | 03 | 2 | BACK-01 (transport) | conformance (LocalFs, s3s-fs?, SeaweedFS env-gated) + object-safety compile test | `cargo test -p ostro-objstore --locked` (SeaweedFS: `OSTRO_TEST_S3_*` set) | ❌ W0 | ⬜ pending |
| 8-04-01 | 04 | 2 | OBS-01/02/03 (config contract, seam) | unit | `cargo test -p ostro-server --test config --locked` | ✅ (extended) | ⬜ pending |
| 8-04-02 | 04 | 2 | OBS-02 (endpoint), OBS-01/03 (pipes) | integration (in-process OTLP collector) | `cargo test -p ostro-server --test metrics_endpoint --test otlp_export --locked` + pipefail duplicate check | ❌ W0 | ⬜ pending |
| 8-05 | 05 | 2 | TRIG-02, TRIG-03 (logic) | TDD unit + proptest (DST vectors, downtime ≫ cap × period) | `cargo test -p ostro-triggers --test cron --locked` | ❌ W0 | ⬜ pending |
| 8-06 | 06 | 2 | TRIG-01 (logic) | TDD unit (vendor vectors) | `cargo test -p ostro-triggers --test webhook_verify --locked` | ❌ W0 | ⬜ pending |
| 8-07-01 | 07 | 2 | TRIG-02, TRIG-03 (apply) | unit (apply) | `cargo test -p ostro-partition --test apply_schedule --locked` | ❌ W0 | ⬜ pending |
| 8-07-02 | 07 | 2 | TRIG-02 (exactly-once) | proptest (rebuild digest) | `cargo test -p ostro-partition --test apply_schedule --test rebuild_digest --locked` + failpoint suite | ✅ (extended) | ⬜ pending |
| 8-08-01 | 08 | 2 | BACK-01 (cut, raw reads) | proptest (causal closure, update-before-publish) | `cargo test -p ostro-log --test hw_cut --test read_envelopes --locked` | ❌ W0 | ⬜ pending |
| 8-08-02 | 08 | 2 | BACK-01 (observer, incarnation) | unit | `cargo test -p ostro-blob --test observer --locked && cargo test -p ostro-partition --test node_incarnation --locked` | ❌ W0 | ⬜ pending |
| 8-09-01 | 09 | 3 | BACK-04 (+ B4 floor init) | integration | `cargo test -p ostro-partition --test snapshot_service --test trim_safety --locked` | ❌ W0 / ✅ trim_safety | ⬜ pending |
| 8-09-02 | 09 | 3 | BACK-04 / SC4 | proptest + failpoints (kill loop at plan level) | `cargo test -p ostro-partition --test snapshot_tail --test trim_safety --locked && cargo test -p ostro-partition --features failpoints --test trim_safety -- --test-threads=1 && cargo test -p ostro-crashtest --locked` | ❌ W0 | ⬜ pending |
| 8-10-01 | 10 | 3 | TRIG-01/02/03, OBS-01 (traceparent in), OBS-03, BACK-03 (surfaces) | unit + parity + insta (additive) | `cargo test -p ostro-proto -p ostro-api --locked` | ❌ W0 (phase8_surface) / ✅ parity | ⬜ pending |
| 8-10-02 | 10 | 3 | (wiring) | integration | `cargo test -p ostro-server --locked` | ✅ | ⬜ pending |
| 8-11-01 | 11 | 3 | OBS-01 | integration (engine) + determinism + pipefail "no SDK in engine" | `cargo test -p ostro-engine --test telemetry_spans --test determinism --locked` | ❌ W0 | ⬜ pending |
| 8-11-02 | 11 | 3 | OBS-01 | integration (OTLP collector) | `cargo test -p ostro-server --test tracing_spans --locked` | ❌ W0 | ⬜ pending |
| 8-12-01 | 12 | 4 | TRIG-01/02/03 (CLI) | unit (FakeControlPlane-backed) | `cargo test -p ostro-cli --test phase8_verbs --locked` | ❌ W0 | ⬜ pending |
| 8-12-02 | 12 | 4 | TRIG-01/02/03 (CLI) | e2e smoke (spawned binary) | `cargo test -p ostro --test cli_phase8 --locked` | ❌ W0 | ⬜ pending |
| 8-13-01 | 13 | 4 | BACK-01 (chunks, manifest validate, fencing, immutable puts) | unit + integration | `cargo test -p ostro-backup --lib --locked && cargo test -p ostro-backup --test fencing --locked` | ❌ W0 | ⬜ pending |
| 8-13-02 | 13 | 4 | BACK-01 (chain start, hold-back, floor, cut lag) | integration (LocalFs) + proptest + failpoints | `cargo test -p ostro-backup --test uploader --test chain_start --test cut_consistency --locked && cargo test -p ostro-backup --features failpoints -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 8-13-03 | 13 | 4 | BACK-01 | integration (server) | `cargo test -p ostro-server --test bottomless_service --locked` | ❌ W0 | ⬜ pending |
| 8-14-01 | 14 | 4 | TRIG-02, TRIG-03 (driver) | integration (FakeClock, restarts, long downtime) | `cargo test -p ostro-partition --test cron_driver --locked` | ❌ W0 | ⬜ pending |
| 8-14-02 | 14 | 4 | TRIG-02, TRIG-03 | integration (TestServer) | `cargo test -p ostro-server --test schedules --locked` | ❌ W0 | ⬜ pending |
| 8-15-01 | 15 | 4 | TRIG-01 | integration (TestServer) | `cargo test -p ostro-server --test webhooks --locked` | ❌ W0 | ⬜ pending |
| 8-15-02 | 15 | 4 | TRIG-01 (CRUD, secrets) | integration | `cargo test -p ostro-server --test webhooks --locked` | ❌ W0 | ⬜ pending |
| 8-16-01 | 16 | 4 | OBS-02 | build + unit | `cargo test -p ostro-engine -p ostro-partition -p ostro-meta --locked` | ✅ | ⬜ pending |
| 8-16-02 | 16 | 4 | OBS-02 | integration (scrape + label audit) | `cargo test -p ostro-server --test metrics_endpoint --locked` | ✅ (extended) | ⬜ pending |
| 8-17-01 | 17 | 4 | OBS-03 | unit (buckets, caps, LRU) | `cargo test -p ostro-server --lib guest_logs --locked && cargo test -p ostro-engine --locked` | ❌ W0 | ⬜ pending |
| 8-17-02 | 17 | 4 | OBS-03 | integration (log-spammer, one LogRecord per line) | `cargo test -p ostro-server --test guest_logs --locked` | ❌ W0 | ⬜ pending |
| 8-18-01 | 18 | 5 | BACK-02 (meta bootstrap, WAL import Create/Append) | spike + integration | `cargo test -p ostro-meta --test bootstrap_snapshot --locked && cargo test -p ostro-log --test import --locked` | ❌ W0 | ⬜ pending |
| 8-18-02 | 18 | 5 | BACK-02, BACK-03 (validate, two-step in-files replay) | integration (LocalFs) + failpoints | `cargo test -p ostro-backup --test restore --locked && cargo test -p ostro-backup --features failpoints -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 8-18-03 | 18 | 5 | BACK-02, BACK-03 | e2e (spawned binary) | `cargo test -p ostro --test backup_cli --locked` | ❌ W0 | ⬜ pending |
| 8-19-01 | 19 | 5 | BACK-01 (blobs), D9 follow-up | unit + race + failpoint | `cargo test -p ostro-blob --locked && cargo test -p ostro-blob --features failpoints -- --test-threads=1` | ❌ W0 (gc.rs) | ⬜ pending |
| 8-19-02 | 19 | 5 | BACK-01 (remote blobs) | integration | `cargo test -p ostro-backup --test remote_gc --locked && cargo test -p ostro-server --lib blob_gc --locked` | ❌ W0 | ⬜ pending |
| 8-20-01 | 20 | 6 | TRIG-01/02/03 (SC1, SC2) | e2e (kill loop at plan level) | `cargo test -p ostro-server --test triggers_e2e --locked && cargo build -p ostro-crashtest --tests --locked` | ❌ W0 | ⬜ pending |
| 8-20-02 | 20 | 6 | BACK-01/02/03/04 (SC3, SC4) | SeaweedFS leg + crashtest unit (RPO loop and campaign smoke at plan level) | `cargo test -p ostro --test bottomless_s3 --locked && cargo test -p ostro-crashtest --locked` | ❌ W0 | ⬜ pending |
| 8-21-01 | 21 | 6 | OBS-01 (outbound traceparent via ServerEffectDriver hooks, B2; journal unchanged) | integration (mock HTTP server) or helper-level + BLOCKING follow-up | `cargo test -p ostro-server --test outbound_traceparent --locked` | ❌ W0 | ⬜ pending |
| 8-21-02 | 21 | 6 | OBS-01/02/03 (SC5) | e2e | `cargo test -p ostro-server --test observability_e2e --locked` | ❌ W0 | ⬜ pending |
| 8-21-03 | 21 | 6 | FX-06 OTLP leg (D-FX6) | e2e | `cargo test -p ostro-server --test otlp_canary --locked` | ❌ W0 | ⬜ pending |
| 8-22-01 | 22 | 7 | all | gate (full ci-local + campaign + loops at plan level) | `cargo deny check && bash scripts/forbidden-crates.sh --self-test` + RESERVATIONS flip grep + campaign report file | n/a | ⬜ pending |
| 8-22-02 | 22 | 7 | all (docs; OBS-01 not green without B2 evidence) | check | grep checks on VALIDATION/DECISIONS/docs | n/a | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠ flaky / human_needed*

**Plan-level slow checks (W12):** `crash_loop` (08-09), `cron_exactly_once` and `restore_rpo` + campaign smoke (08-20), full `bash scripts/ci-local.sh` + the ≥ 10,000-fault campaign (08-22).

---

## Wave 0 Requirements

The task that first needs a test file creates it. TDD tasks write their tests first.

- [ ] `scripts/forbidden-crates.sh` (`--self-test`) and `ci/deny-fixtures/ban-prometheus` (08-01)
- [ ] `crates/ostro-triggers/tests/{cron,webhook_verify}.rs` (08-05, 08-06)
- [ ] `crates/ostro-partition/tests/{snapshot,blobrefs,apply_schedule,snapshot_service,snapshot_tail,cron_driver}.rs`, `tests/common/gen_schedule.rs`, and extensions to `trim_safety.rs` and `rebuild_digest.rs` (08-02, 08-07, 08-09, 08-14)
- [ ] `crates/ostro-objstore/tests/{localfs,s3}.rs` + `tests/conformance/mod.rs` + the s3s-fs decision (08-03)
- [ ] `crates/ostro-backup/tests/{uploader,cut_consistency,fencing,chain_start,restore,remote_gc}.rs` + `tests/common/mod.rs` (08-13, 08-18, 08-19)
- [ ] `crates/ostro-log/tests/{hw_cut,read_envelopes,import}.rs`, `crates/ostro-blob/tests/{observer,gc}.rs`, `crates/ostro-partition/tests/node_incarnation.rs`, `crates/ostro-meta/tests/bootstrap_snapshot.rs`
- [ ] `crates/ostro-server/tests/{metrics_endpoint,otlp_export,tracing_spans,bottomless_service,schedules,webhooks,guest_logs,triggers_e2e,observability_e2e,otlp_canary,outbound_traceparent}.rs` + `tests/common/phase8.rs` (08-04, 08-10..08-18)
- [ ] `crates/ostro-testkit/src/otlp.rs`, the in-process collector (08-04)
- [ ] `crates/ostro-engine/tests/telemetry_spans.rs` (08-11)
- [ ] `crates/ostro-api/tests/phase8_surface.rs`, `crates/ostro-cli/tests/phase8_verbs.rs`, `crates/ostro/tests/{cli_phase8,backup_cli,bottomless_s3}.rs`
- [ ] `crates/ostro-crashtest/tests/{cron_exactly_once,restore_rpo}.rs` + Phase 8 campaign fault kinds (08-20)
- [ ] Guests: `guests/log-spammer` (08-17), `guests/hook-order` (08-20)
- [ ] Failpoints: `snapshot::{before_rename,before_dir_fsync,install_before_swap,install_after_old_rename}`, `trim::after_delete`, `upload::before_manifest`, `restore::{after_meta,mid_partition}`, `blobgc::before_unlink`. `FAILPOINT_CRATES += ostro-backup` in ci-local.sh and ci.yml together (08-01).

---

## Manual-Only / Human-Needed Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| S3 conformance, conditional PUT (If-None-Match / If-Match) and fencing against SeaweedFS | BACK-01 | WSL has no Docker. CI runs it through the shared SeaweedFS docker-run step, but there is no remote yet (FOUND-05). | On a Docker machine: `docker run -d -p 8333:8333 -e AWS_ACCESS_KEY_ID=ostro -e AWS_SECRET_ACCESS_KEY=ostroseaweed -e S3_BUCKET=ostro-test chrislusf/seaweedfs:4.54 mini -dir=/data`; create bucket `ostro-test`; then `OSTRO_TEST_S3_URL=http://127.0.0.1:8333 OSTRO_TEST_S3_ACCESS_KEY=ostro OSTRO_TEST_S3_SECRET_KEY=ostroseaweed OSTRO_TEST_S3_BUCKET=ostro-test cargo test -p ostro-objstore --test s3 && cargo test -p ostro-backup --test fencing && cargo test -p ostro --test bottomless_s3`. Or confirm the CI SeaweedFS steps are green after the first push. |
| Conditional PUT on Cloudflare R2 and AWS S3 | BACK-01 (fencing) | Needs real cloud credentials. | Same tests with the `OSTRO_TEST_S3_*` variables pointing at R2/S3. If `If-Match` is unsupported, confirm the LOCK re-read fallback engages (`supports_conditional_put = false`). |
| `ostro restore --from s3://…` on a fresh machine: state hash matches; RPO ≤ seal interval (target ≤ 1 s); timing recorded | BACK-02 / SC3 | Needs a real bucket and a second machine. | Run a server with `[backup] url = s3://…`, apply load, kill -9 it, then on a fresh machine run `ostro restore --from s3://… --data-dir /var/lib/ostro`. Record the report (hashes, cut_at, duration) here. |
| Span tree and metrics in a real backend (Jaeger/Tempo + Grafana/Prometheus) | OBS-01 / OBS-02 | Visual check. | Point `[telemetry] otlp_endpoint` at a collector, scrape `/metrics`, and check that the run → activation → step tree and the dashboards render. |
| TLS roots in a `FROM scratch` image (webpki knob) | BACK-01 | Phase 14 image artefact. | Build the scratch image and restore from HTTPS S3 with `tls_roots = "webpki"`. |
| Phase 6 fx-canary leg with the OTLP sink (if Phase 6 had not landed by 08-21) | FX-06 / D-FX6 | Cross-phase. | After Phase 6 lands: `cargo test -p ostro-server --test otlp_canary` prints no `PHASE6_CANARY_LEG: pending`. |
| Outbound HTTP traceparent + effect span through ServerEffectDriver (if 06-14 had not landed at 08-21) | OBS-01 (**blocking**) | Cross-phase (P8-D24) | After Phase 6 06-14 lands: re-run 08-21 Task 1 (implement `EffectHooks` in effect_trace.rs and install it); `cargo test -p ostro-server --test outbound_traceparent` without `BRIDGE_TRACEPARENT_LEG: pending`. OBS-01 stays incomplete until then. |
| Review of adopted P8-D3 (seal default/cost), P8-D9 (jiff 0.2 adapter), P8-D11 (Kafka out of scope), P8-D16 (in-memory log cap), P8-D21 (22 plans) | all | The user was unavailable (YOLO). | Read 08-DECISIONS.md. |

---

## Gate results

(08-22 Task 2 fills this in.)

## Human-needed

(08-22 Task 2 fills this in.)

---

## Validation Sign-Off

- [ ] All tasks have an `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without an automated verify
- [ ] Wave 0 covers every MISSING reference
- [ ] No watch-mode flags
- [ ] Feedback latency < 60 s for task-level checks
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
