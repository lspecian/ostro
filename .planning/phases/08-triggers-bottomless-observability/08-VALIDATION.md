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
> Source: 08-RESEARCH.md § Validation Architecture, mapped onto plans 08-01..08-19. Adopted decisions: 08-DECISIONS.md (P8-D1..D22).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test harness and cargo-nextest (`.config/nextest.toml`: default, ci and crash profiles), proptest 1.12 + proptest-state-machine, insta, `fail` 0.5.1 failpoints (single-threaded suites), `ostro-crashtest` (kill -9 harness + the 05-19 sharded campaign), and the in-process OTLP collector (`ostro-testkit::otlp`, 08-04). |
| **Config** | `scripts/ci-local.sh`. `FAILPOINT_CRATES` gains `ostro-backup` (08-01) and must equal the ci.yml "Failpoint suites" step. The forbidden-crate check lives in `scripts/forbidden-crates.sh`, which ci-local and ci.yml both call (08-01). The MinIO docker-run step is in ci.yml (shared with Phase 6 06-01; 08-03 reuses or adds it, 08-18 adds the bottomless leg). |
| **Guest build** | `guest_wasm(name)` builds `guests/` (`log-spammer` 08-15, `hook-order` 08-18). |
| **Docker-only legs** | MinIO (S3 conformance, conditional PUT, fencing, `bottomless_s3`) runs only when `OSTRO_TEST_S3_URL`/`_ACCESS_KEY`/`_SECRET_KEY`/`_BUCKET` are set (Phase 6 convention). CI sets them after a `docker run` MinIO step (service containers cannot pass `server /data`). Locally (WSL, no Docker) the tests print `SKIP` and the item stays **human_needed**. |
| **Quick run command** | `cargo test -p <crate> --test <suite> --locked` (each < 30–60 s) |
| **Full suite command** | `bash scripts/ci-local.sh --no-musl` |
| **Phase gate commands** | 1. `bash scripts/ci-local.sh` (with musl).<br>2. Crash campaign with the Phase 8 fault kinds: `OSTRO_CAMPAIGN_STATE_DIR=target/campaign-p8-gate OSTRO_CAMPAIGN_FAULTS=10000 OSTRO_CAMPAIGN_SHARDS=20 OSTRO_CAMPAIGN_PARALLEL=6 cargo test --release -p ostro-crashtest --features failpoints --test server_campaign -- --ignored --nocapture campaign`, then the same command with `report`.<br>3. `cargo nextest run -P crash -p ostro-crashtest --run-ignored only cron_exactly_once restore_rpo crash_loop`. |
| **Estimated runtime** | Quick: < 60 s. Full: ~10–15 min debug. Crash loops: a few minutes each. Campaign: hours (sharded, resumable). |

---

## Sampling Rate

- **After every task commit:** the task's `<automated>` command, plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings`.
  - ostro-partition / ostro-blob / ostro-backup / ostro-crashtest changes also run `cargo test -p <crate> --features failpoints --locked -- --test-threads=1`.
- **After every plan wave:** `cargo test --workspace --locked` + `cargo deny check` + `bash scripts/forbidden-crates.sh` + the failpoint suites.
- **Before `/gsd:verify-work`:** the 08-19 gate.
- **Max feedback latency:** 60 s for task-level checks. Crash loops and the campaign are `#[ignore]` and run at plan or gate level.
- Piped verify commands use `set -o pipefail` or grep for an explicit result line.

---

## Wave Structure

| Wave | Plans | Exclusive shared files |
|------|-------|------------------------|
| 1 | 08-01 gate + contract + deps + CI grep | Cargo.toml, Cargo.lock, deny.toml, ci-local.sh, ci.yml, command.rs + golden, TABLES, every shared `lib.rs`/`mod.rs` stub declaration, STATE.md, RESERVATIONS.md. **Cross-phase coordination point with Phases 6/7.** |
| 2 | 08-02 OSNP format, 08-03 ostro-objstore, 08-04 telemetry + config + services seam, 08-05 cron engine, 08-06 webhook verifiers, 08-07 ScheduleFire apply | 08-03 alone: Cargo.lock (s3s-fs dev dependency), deny.toml, ci.yml (MinIO). 08-04 alone: ostro-server `server.rs`/`config.rs`/`executor.rs`, testkit lib.rs. 08-07 alone: `ostro-partition/src/apply/*`, `tests/common`, `rebuild_digest.rs`. |
| 3 | 08-08 snapshot service + trim, 08-09 API surface, 08-10 spans | 08-08: partition `leader/*`, `node.rs`, crashtest. 08-09: ostro-api, ostro-proto, ostro-cli, server `server.rs` + `control/*` + `tests/common`. 08-10: engine telemetry/activation/journal, server `executor.rs`, `services/run_spans.rs`, `docs/observability.md`. |
| 4 | 08-11 uploader, 08-12 cron driver + schedules, 08-13 webhooks, 08-14 metrics coverage, 08-15 guest logs | 08-11: ostro-log, ostro-blob, partition `node.rs`. 08-12: partition `leader/{cron,mod}.rs`, server `server.rs`. 08-14: engine `activation/runtime/replay`, `dispatcher.rs`, `apply/terminal.rs`, meta `handle.rs`, `services/{metrics_poll,run_spans}.rs`. 08-15: engine `host/log.rs` + `sink.rs`, server `sink.rs` + `executor.rs`, guests lock, `docs/observability.md`. |
| 5 | 08-16 restore + backup, 08-17 blob GC | 08-16: ostro-meta, ostro-log, ostro-cli, `crates/ostro/src/main.rs`, `docs/bottomless.md`. 08-17: ostro-blob. |
| 6 | 08-18 end-to-end proofs | guests lock, ostro-crashtest, ci.yml |
| 7 | 08-19 phase gate | docs, RESERVATIONS flip, VALIDATION, STATE |

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 8-01-01 | 01 | 1 | (gate) | check + reservations script | 05-{01..20}-SUMMARY present; `05-VERIFICATION.md` `^status: (passed\|human_needed)$` with by-design items only; freeze tag present; 05-07 Schedule fields + Webhook present; STATE/RESERVATIONS greps | n/a | ⬜ pending |
| 8-01-02 | 01 | 1 | TRIG-02 (contract), BACK-04 (keyspace) | unit + golden (append-only) | `cargo test -p ostro-types --locked && cargo test -p ostro-partition --test store --test store_digest --test rebuild_digest --locked` + golden diff empty of removals | ✅ (extended) | ⬜ pending |
| 8-01-03 | 01 | 1 | OBS-02 (exporter allowed), all deps | script fixture + deny | `bash scripts/forbidden-crates.sh --self-test && bash scripts/forbidden-crates.sh && bash scripts/check-deny-fixtures.sh && cargo deny check` | ❌ W0 (forbidden-crates.sh, ban-prometheus fixture) | ⬜ pending |
| 8-02-01 | 02 | 2 | BACK-04 | unit + proptest (corruption, concurrent export) | `cargo test -p ostro-partition --test snapshot --test blobrefs --test store_digest --locked` | ❌ W0 | ⬜ pending |
| 8-02-02 | 02 | 2 | BACK-04 | integration + failpoints + proptest (snapshot + tail) | `cargo test -p ostro-partition --features failpoints --test snapshot --locked -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 8-03-01 | 03 | 2 | BACK-01 (transport) | spike (deny) | `cargo deny check` + `cargo tree -e normal -i chrono` empty | n/a | ⬜ pending |
| 8-03-02 | 03 | 2 | BACK-01 (transport) | conformance (LocalFs, s3s-fs?, MinIO env-gated) | `cargo test -p ostro-objstore --locked` (MinIO: `OSTRO_TEST_S3_*` set) | ❌ W0 | ⬜ pending |
| 8-04-01 | 04 | 2 | OBS-01/02/03 (config contract, seam) | unit | `cargo test -p ostro-server --test config --locked` | ✅ (extended) | ⬜ pending |
| 8-04-02 | 04 | 2 | OBS-02 (endpoint), OBS-01/03 (pipes) | integration (in-process OTLP collector) | `cargo test -p ostro-server --test metrics_endpoint --test otlp_export --locked` + `cargo tree -d` duplicate check | ❌ W0 | ⬜ pending |
| 8-05 | 05 | 2 | TRIG-02, TRIG-03 (logic) | TDD unit + proptest (DST vectors) | `cargo test -p ostro-triggers --test cron --locked` | ❌ W0 | ⬜ pending |
| 8-06 | 06 | 2 | TRIG-01 (logic) | TDD unit (vendor vectors) | `cargo test -p ostro-triggers --test webhook_verify --locked` | ❌ W0 | ⬜ pending |
| 8-07-01 | 07 | 2 | TRIG-02, TRIG-03 (apply) | unit (apply) | `cargo test -p ostro-partition --test apply_schedule --locked` | ❌ W0 | ⬜ pending |
| 8-07-02 | 07 | 2 | TRIG-02 (exactly-once) | proptest (rebuild digest) | `cargo test -p ostro-partition --test apply_schedule --test rebuild_digest --locked` + failpoint suite | ✅ (extended) | ⬜ pending |
| 8-08-01 | 08 | 3 | BACK-04 | integration | `cargo test -p ostro-partition --test snapshot_service --test trim_safety --locked` | ❌ W0 / ✅ trim_safety | ⬜ pending |
| 8-08-02 | 08 | 3 | BACK-04 / SC4 | proptest + failpoints + crash loop | `cargo test -p ostro-partition --test snapshot_tail --test trim_safety --locked && cargo test -p ostro-partition --features failpoints --test trim_safety -- --test-threads=1 && cargo test -p ostro-crashtest --features failpoints -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 8-09-01 | 09 | 3 | TRIG-01/02/03, OBS-01 (traceparent in), OBS-03, BACK-03 (surfaces) | unit + parity + insta (additive) | `cargo test -p ostro-proto -p ostro-api --locked` | ❌ W0 (phase8_surface) / ✅ parity | ⬜ pending |
| 8-09-02 | 09 | 3 | (wiring) | integration | `cargo test -p ostro-server --locked` | ✅ | ⬜ pending |
| 8-09-03 | 09 | 3 | TRIG-01/02/03 (CLI) | e2e (spawned binary) | `cargo test -p ostro --test cli_phase8 --locked` | ❌ W0 | ⬜ pending |
| 8-10-01 | 10 | 3 | OBS-01 | integration (engine, in-memory exporter) + determinism | `cargo test -p ostro-engine --test telemetry_spans --test determinism --locked` | ❌ W0 | ⬜ pending |
| 8-10-02 | 10 | 3 | OBS-01 | integration (OTLP collector) | `cargo test -p ostro-server --test tracing_spans --locked` | ❌ W0 | ⬜ pending |
| 8-11-01 | 11 | 4 | BACK-01 (cut) | proptest | `cargo test -p ostro-log --test hw_cut --locked && cargo test -p ostro-blob --test observer --locked` | ❌ W0 | ⬜ pending |
| 8-11-02 | 11 | 4 | BACK-01 | integration (LocalFs) + proptest (cut consistency) + failpoints | `cargo test -p ostro-backup --locked && cargo test -p ostro-backup --features failpoints --locked -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 8-11-03 | 11 | 4 | BACK-01 | integration (server) | `cargo test -p ostro-server --test bottomless_service --locked` | ❌ W0 | ⬜ pending |
| 8-12-01 | 12 | 4 | TRIG-02, TRIG-03 (driver) | integration (FakeClock, restarts) | `cargo test -p ostro-partition --test cron_driver --locked` | ❌ W0 | ⬜ pending |
| 8-12-02 | 12 | 4 | TRIG-02, TRIG-03 | integration (TestServer) | `cargo test -p ostro-server --test schedules --locked` | ❌ W0 | ⬜ pending |
| 8-13-01 | 13 | 4 | TRIG-01 | integration (TestServer) | `cargo test -p ostro-server --test webhooks --locked` | ❌ W0 | ⬜ pending |
| 8-13-02 | 13 | 4 | TRIG-01 (CRUD, secrets) | integration | `cargo test -p ostro-server --test webhooks --locked` | ❌ W0 | ⬜ pending |
| 8-14-01 | 14 | 4 | OBS-02 | build + unit | `cargo test -p ostro-engine -p ostro-partition -p ostro-meta --locked` | ✅ | ⬜ pending |
| 8-14-02 | 14 | 4 | OBS-02 | integration (scrape + label audit) | `cargo test -p ostro-server --test metrics_endpoint --locked` | ✅ (extended) | ⬜ pending |
| 8-15-01 | 15 | 4 | OBS-03 | unit (buckets, caps, LRU) | `cargo test -p ostro-server --lib guest_logs --locked && cargo test -p ostro-engine --locked` | ❌ W0 | ⬜ pending |
| 8-15-02 | 15 | 4 | OBS-03 | integration (log-spammer guest, OTLP) | `cargo test -p ostro-server --test guest_logs --locked` | ❌ W0 | ⬜ pending |
| 8-16-01 | 16 | 5 | BACK-02 (meta bootstrap, WAL import) | spike + integration | `cargo test -p ostro-meta --test bootstrap_snapshot --locked && cargo test -p ostro-log --test import --locked` | ❌ W0 | ⬜ pending |
| 8-16-02 | 16 | 5 | BACK-02, BACK-03 | integration (LocalFs) + failpoints | `cargo test -p ostro-backup --test restore --locked && cargo test -p ostro-backup --features failpoints -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 8-16-03 | 16 | 5 | BACK-02, BACK-03 | e2e (spawned binary) | `cargo test -p ostro --test backup_cli --locked` | ❌ W0 | ⬜ pending |
| 8-17-01 | 17 | 5 | BACK-01 (blobs), D9 follow-up | unit + race + failpoint | `cargo test -p ostro-blob --locked && cargo test -p ostro-blob --features failpoints -- --test-threads=1` | ❌ W0 (gc.rs) | ⬜ pending |
| 8-17-02 | 17 | 5 | BACK-01 (remote blobs) | integration | `cargo test -p ostro-backup --test remote_gc --locked && cargo test -p ostro-server --lib blob_gc --locked` | ❌ W0 | ⬜ pending |
| 8-18-01 | 18 | 6 | TRIG-01/02/03 (SC1, SC2) | e2e + crash loop (`#[ignore]`) | `cargo test -p ostro-server --test triggers_e2e --locked && cargo nextest run -P crash -p ostro-crashtest --run-ignored only cron_exactly_once` | ❌ W0 | ⬜ pending |
| 8-18-02 | 18 | 6 | BACK-01/02/03/04 (SC3, SC4) | crash loop + campaign smoke + MinIO leg | `cargo nextest run -P crash -p ostro-crashtest --run-ignored only restore_rpo && cargo test -p ostro --test bottomless_s3 --locked` | ❌ W0 | ⬜ pending |
| 8-18-03 | 18 | 6 | OBS-01/02/03 (SC5), FX-06 OTLP leg (D-FX6) | e2e | `cargo test -p ostro-server --test observability_e2e --test otlp_canary --locked` | ❌ W0 | ⬜ pending |
| 8-19-01 | 19 | 7 | all | gate | `bash scripts/ci-local.sh` + `cargo deny check` + campaign `report` (kills ≥ 10000, 0 violations) + RESERVATIONS flip grep | n/a | ⬜ pending |
| 8-19-02 | 19 | 7 | all (docs) | check | grep checks on VALIDATION/DECISIONS/docs | n/a | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠ flaky / human_needed*

---

## Wave 0 Requirements

The task that first needs a test file creates it. TDD tasks write their tests first.

- [ ] `scripts/forbidden-crates.sh` (`--self-test`) and `ci/deny-fixtures/ban-prometheus` (08-01)
- [ ] `crates/ostro-triggers/tests/{cron,webhook_verify}.rs` (08-05, 08-06)
- [ ] `crates/ostro-partition/tests/{snapshot,blobrefs,apply_schedule,snapshot_service,snapshot_tail,cron_driver}.rs`, `tests/common/gen_schedule.rs`, and extensions to `trim_safety.rs` and `rebuild_digest.rs` (08-02, 08-07, 08-08, 08-12)
- [ ] `crates/ostro-objstore/tests/{localfs,s3}.rs` + `tests/conformance/mod.rs` + the s3s-fs decision (08-03)
- [ ] `crates/ostro-backup/tests/{uploader,cut_consistency,fencing,restore,remote_gc}.rs` + `tests/common/mod.rs` (08-11, 08-16, 08-17)
- [ ] `crates/ostro-log/tests/{hw_cut,import}.rs`, `crates/ostro-blob/tests/{observer,gc}.rs`, `crates/ostro-meta/tests/bootstrap_snapshot.rs`
- [ ] `crates/ostro-server/tests/{metrics_endpoint,otlp_export,tracing_spans,bottomless_service,schedules,webhooks,guest_logs,triggers_e2e,observability_e2e,otlp_canary}.rs` + `tests/common/phase8.rs` (08-04, 08-09..08-18)
- [ ] `crates/ostro-testkit/src/otlp.rs`, the in-process collector (08-04)
- [ ] `crates/ostro-engine/tests/telemetry_spans.rs` (08-10)
- [ ] `crates/ostro-api/tests/phase8_surface.rs`, `crates/ostro/tests/{cli_phase8,backup_cli,bottomless_s3}.rs`
- [ ] `crates/ostro-crashtest/tests/{cron_exactly_once,restore_rpo}.rs` + Phase 8 campaign fault kinds (08-18)
- [ ] Guests: `guests/log-spammer` (08-15), `guests/hook-order` (08-18)
- [ ] Failpoints: `snapshot::{before_rename,before_dir_fsync,install_before_swap,install_after_old_rename}`, `trim::after_delete`, `upload::before_manifest`, `restore::{after_meta,mid_partition}`, `blobgc::before_unlink`. `FAILPOINT_CRATES += ostro-backup` in ci-local.sh and ci.yml together (08-01).

---

## Manual-Only / Human-Needed Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| S3 conformance, conditional PUT (If-None-Match / If-Match) and fencing against MinIO | BACK-01 | WSL has no Docker. CI runs it through the shared MinIO docker-run step, but there is no remote yet (FOUND-05). | On a Docker machine: `docker run -d -p 9000:9000 -e MINIO_ROOT_USER=ostro -e MINIO_ROOT_PASSWORD=ostrominio quay.io/minio/minio server /data`; create bucket `ostro-test`; then `OSTRO_TEST_S3_URL=http://127.0.0.1:9000 OSTRO_TEST_S3_ACCESS_KEY=ostro OSTRO_TEST_S3_SECRET_KEY=ostrominio OSTRO_TEST_S3_BUCKET=ostro-test cargo test -p ostro-objstore --test s3 && cargo test -p ostro-backup --test fencing && cargo test -p ostro --test bottomless_s3`. Or confirm the CI MinIO steps are green after the first push. |
| Conditional PUT on Cloudflare R2 and AWS S3 | BACK-01 (fencing) | Needs real cloud credentials. | Same tests with the `OSTRO_TEST_S3_*` variables pointing at R2/S3. If `If-Match` is unsupported, confirm the LOCK re-read fallback engages (`supports_conditional_put = false`). |
| `ostro restore --from s3://…` on a fresh machine: state hash matches; RPO ≤ seal interval (target ≤ 1 s); timing recorded | BACK-02 / SC3 | Needs a real bucket and a second machine. | Run a server with `[backup] url = s3://…`, apply load, kill -9 it, then on a fresh machine run `ostro restore --from s3://… --data-dir /var/lib/ostro`. Record the report (hashes, cut_at, duration) here. |
| Span tree and metrics in a real backend (Jaeger/Tempo + Grafana/Prometheus) | OBS-01 / OBS-02 | Visual check. | Point `[telemetry] otlp_endpoint` at a collector, scrape `/metrics`, and check that the run → activation → step tree and the dashboards render. |
| TLS roots in a `FROM scratch` image (webpki knob) | BACK-01 | Phase 14 image artefact. | Build the scratch image and restore from HTTPS S3 with `tls_roots = "webpki"`. |
| Phase 6 fx-canary leg with the OTLP sink (if Phase 6 had not landed by 08-18) | FX-06 / D-FX6 | Cross-phase. | After Phase 6 lands: `cargo test -p ostro-server --test otlp_canary` prints no `PHASE6_CANARY_LEG: pending`. |
| Review of adopted P8-D3 (seal default/cost), P8-D9 (jiff 0.2 adapter), P8-D11 (Kafka out of scope), P8-D16 (in-memory log cap), P8-D21 (19 plans) | all | The user was unavailable (YOLO). | Read 08-DECISIONS.md. |

---

## Gate results

(08-19 Task 2 fills this in.)

## Human-needed

(08-19 Task 2 fills this in.)

---

## Validation Sign-Off

- [ ] All tasks have an `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without an automated verify
- [ ] Wave 0 covers every MISSING reference
- [ ] No watch-mode flags
- [ ] Feedback latency < 60 s for task-level checks
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
