---
phase: 8
slug: triggers-bottomless-observability
status: complete
nyquist_compliant: true
wave_0_complete: true
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
| **Phase gate commands** | 1. `bash scripts/ci-local.sh` (with musl).<br>2. Crash campaign with the Phase 8 fault kinds. `OSTRO_CAMPAIGN_PHASE8=1` is required; without it only the 05-19 mix runs (08-20). `export OSTRO_CAMPAIGN_PHASE8=1 OSTRO_CAMPAIGN_STATE_DIR=target/campaign-p8-gate OSTRO_CAMPAIGN_FAULTS=10000 OSTRO_CAMPAIGN_SHARDS=20 OSTRO_CAMPAIGN_PARALLEL=6; for ids in 0-5 6-11 12-17 18-19; do OSTRO_CAMPAIGN_SHARD_IDS=$ids cargo test --release -p ostro-crashtest --features failpoints --test server_campaign --locked -- --ignored --nocapture campaign; done`, then `cargo test --release -p ostro-crashtest --features failpoints --test server_campaign --locked -- --ignored --nocapture report > target/campaign-p8-gate/report.txt` with the same exports.<br>3. `cargo nextest run -P crash -p ostro-crashtest --run-ignored only cron_exactly_once restore_rpo crash_loop`. |
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
| 8-01-01 | 01 | 1 | (gate) | check + reservations script | 05-{01..20}-SUMMARY present; `05-VERIFICATION.md` `^status: (passed\|human_needed)$` with by-design items only; freeze tag; 05-07 Schedule fields + Webhook; STATE/RESERVATIONS greps | n/a | ✅ green |
| 8-01-02 | 01 | 1 | TRIG-02 (contract), BACK-04 (keyspace) | unit + golden (append-only) + InvalidCode index test | `cargo test -p ostro-types --locked && cargo test -p ostro-partition --test store --test store_digest --test rebuild_digest --locked` + golden diff vs `target/08-01-base.sha` | ✅ (extended) | ✅ green |
| 8-01-03 | 01 | 1 | OBS-02 (exporter allowed), all deps | script fixture + deny + pipefail tree checks | `bash scripts/forbidden-crates.sh --self-test && bash scripts/forbidden-crates.sh && bash scripts/check-deny-fixtures.sh && cargo deny check` + `bash -c 'set -euo pipefail; …'` no chrono/zstd-sys/openssl-sys | ❌ W0 | ✅ green |
| 8-02-01 | 02 | 2 | BACK-04 | unit + proptest (corruption, concurrent export) | `cargo test -p ostro-partition --test snapshot --test blobrefs --test store_digest --locked` | ❌ W0 | ✅ green |
| 8-02-02 | 02 | 2 | BACK-04 | integration + failpoints + proptest (snapshot + tail) | `cargo test -p ostro-partition --features failpoints --test snapshot --locked -- --test-threads=1` | ❌ W0 | ✅ green |
| 8-03-01 | 03 | 2 | BACK-01 (transport) | spike (deny) | `cargo deny check` + pipefail-safe "no chrono in normal tree" | n/a | ✅ green |
| 8-03-02 | 03 | 2 | BACK-01 (transport) | conformance (LocalFs, s3s-fs?, SeaweedFS env-gated) + object-safety compile test | `cargo test -p ostro-objstore --locked` (SeaweedFS: `OSTRO_TEST_S3_*` set) | ❌ W0 | ✅ green (SeaweedFS leg ⚠ human_needed) |
| 8-04-01 | 04 | 2 | OBS-01/02/03 (config contract, seam) | unit | `cargo test -p ostro-server --test config --locked` | ✅ (extended) | ✅ green |
| 8-04-02 | 04 | 2 | OBS-02 (endpoint), OBS-01/03 (pipes) | integration (in-process OTLP collector) | `cargo test -p ostro-server --test metrics_endpoint --test otlp_export --locked` + pipefail duplicate check | ❌ W0 | ✅ green |
| 8-05 | 05 | 2 | TRIG-02, TRIG-03 (logic) | TDD unit + proptest (DST vectors, downtime ≫ cap × period) | `cargo test -p ostro-triggers --test cron --locked` | ❌ W0 | ✅ green |
| 8-06 | 06 | 2 | TRIG-01 (logic) | TDD unit (vendor vectors) | `cargo test -p ostro-triggers --test webhook_verify --locked` | ❌ W0 | ✅ green |
| 8-07-01 | 07 | 2 | TRIG-02, TRIG-03 (apply) | unit (apply) | `cargo test -p ostro-partition --test apply_schedule --locked` | ❌ W0 | ✅ green |
| 8-07-02 | 07 | 2 | TRIG-02 (exactly-once) | proptest (rebuild digest) | `cargo test -p ostro-partition --test apply_schedule --test rebuild_digest --locked` + failpoint suite | ✅ (extended) | ✅ green |
| 8-08-01 | 08 | 2 | BACK-01 (cut, raw reads) | proptest (causal closure, update-before-publish) | `cargo test -p ostro-log --test hw_cut --test read_envelopes --locked` | ❌ W0 | ✅ green |
| 8-08-02 | 08 | 2 | BACK-01 (observer, incarnation) | unit | `cargo test -p ostro-blob --test observer --locked && cargo test -p ostro-partition --test node_incarnation --locked` | ❌ W0 | ✅ green |
| 8-09-01 | 09 | 3 | BACK-04 (+ B4 floor init) | integration | `cargo test -p ostro-partition --test snapshot_service --test trim_safety --locked` | ❌ W0 / ✅ trim_safety | ✅ green |
| 8-09-02 | 09 | 3 | BACK-04 / SC4 | proptest + failpoints (kill loop at plan level) | `cargo test -p ostro-partition --test snapshot_tail --test trim_safety --locked && cargo test -p ostro-partition --features failpoints --test trim_safety -- --test-threads=1 && cargo test -p ostro-crashtest --locked` | ❌ W0 | ✅ green |
| 8-10-01 | 10 | 3 | TRIG-01/02/03, OBS-01 (traceparent in), OBS-03, BACK-03 (surfaces) | unit + parity + insta (additive) | `cargo test -p ostro-proto -p ostro-api --locked` | ❌ W0 (phase8_surface) / ✅ parity | ✅ green |
| 8-10-02 | 10 | 3 | (wiring) | integration | `cargo test -p ostro-server --locked` | ✅ | ✅ green |
| 8-11-01 | 11 | 3 | OBS-01 | integration (engine) + determinism + pipefail "no SDK in engine" | `cargo test -p ostro-engine --test telemetry_spans --test determinism --locked` | ❌ W0 | ✅ green |
| 8-11-02 | 11 | 3 | OBS-01 | integration (OTLP collector) | `cargo test -p ostro-server --test tracing_spans --locked` | ❌ W0 | ✅ green |
| 8-12-01 | 12 | 4 | TRIG-01/02/03 (CLI) | unit (FakeControlPlane-backed) | `cargo test -p ostro-cli --test phase8_verbs --locked` | ❌ W0 | ✅ green |
| 8-12-02 | 12 | 4 | TRIG-01/02/03 (CLI) | e2e smoke (spawned binary) | `cargo test -p ostro --test cli_phase8 --locked` | ❌ W0 | ✅ green |
| 8-13-01 | 13 | 4 | BACK-01 (chunks, manifest validate, fencing, immutable puts) | unit + integration | `cargo test -p ostro-backup --lib --locked && cargo test -p ostro-backup --test fencing --locked` | ❌ W0 | ✅ green (s3_fencing ⚠ human_needed) |
| 8-13-02 | 13 | 4 | BACK-01 (chain start, hold-back, floor, cut lag) | integration (LocalFs) + proptest + failpoints | `cargo test -p ostro-backup --test uploader --test chain_start --test cut_consistency --locked && cargo test -p ostro-backup --features failpoints -- --test-threads=1` | ❌ W0 | ✅ green |
| 8-13-03 | 13 | 4 | BACK-01 | integration (server) | `cargo test -p ostro-server --test bottomless_service --locked` | ❌ W0 | ✅ green |
| 8-14-01 | 14 | 4 | TRIG-02, TRIG-03 (driver) | integration (FakeClock, restarts, long downtime) | `cargo test -p ostro-partition --test cron_driver --locked` | ❌ W0 | ✅ green |
| 8-14-02 | 14 | 4 | TRIG-02, TRIG-03 | integration (TestServer) | `cargo test -p ostro-server --test schedules --locked` | ❌ W0 | ✅ green |
| 8-15-01 | 15 | 4 | TRIG-01 | integration (TestServer) | `cargo test -p ostro-server --test webhooks --locked` | ❌ W0 | ✅ green |
| 8-15-02 | 15 | 4 | TRIG-01 (CRUD, secrets) | integration | `cargo test -p ostro-server --test webhooks --locked` | ❌ W0 | ✅ green |
| 8-16-01 | 16 | 4 | OBS-02 | build + unit | `cargo test -p ostro-engine -p ostro-partition -p ostro-meta --locked` | ✅ | ✅ green |
| 8-16-02 | 16 | 4 | OBS-02 | integration (scrape + label audit) | `cargo test -p ostro-server --test metrics_endpoint --locked` | ✅ (extended) | ✅ green |
| 8-17-01 | 17 | 4 | OBS-03 | unit (buckets, caps, LRU) | `cargo test -p ostro-server --lib guest_logs --locked && cargo test -p ostro-engine --locked` | ❌ W0 | ✅ green |
| 8-17-02 | 17 | 4 | OBS-03 | integration (log-spammer, one LogRecord per line) | `cargo test -p ostro-server --test guest_logs --locked` | ❌ W0 | ✅ green |
| 8-18-01 | 18 | 5 | BACK-02 (meta bootstrap, WAL import Create/Append) | spike + integration | `cargo test -p ostro-meta --test bootstrap_snapshot --locked && cargo test -p ostro-log --test import --locked` | ❌ W0 | ✅ green |
| 8-18-02 | 18 | 5 | BACK-02, BACK-03 (validate, two-step in-files replay) | integration (LocalFs) + failpoints | `cargo test -p ostro-backup --test restore --locked && cargo test -p ostro-backup --features failpoints -- --test-threads=1` | ❌ W0 | ✅ green |
| 8-18-03 | 18 | 5 | BACK-02, BACK-03 | e2e (spawned binary) | `cargo test -p ostro --test backup_cli --locked` | ❌ W0 | ✅ green |
| 8-19-01 | 19 | 5 | BACK-01 (blobs), D9 follow-up | unit + race + failpoint | `cargo test -p ostro-blob --locked && cargo test -p ostro-blob --features failpoints -- --test-threads=1` | ❌ W0 (gc.rs) | ✅ green |
| 8-19-02 | 19 | 5 | BACK-01 (remote blobs) | integration | `cargo test -p ostro-backup --test remote_gc --locked && cargo test -p ostro-server --lib blob_gc --locked` | ❌ W0 | ✅ green |
| 8-20-01 | 20 | 6 | TRIG-01/02/03 (SC1, SC2) | e2e (kill loop at plan level) | `cargo test -p ostro-server --test triggers_e2e --locked && cargo build -p ostro-crashtest --tests --locked` | ❌ W0 | ✅ green |
| 8-20-02 | 20 | 6 | BACK-01/02/03/04 (SC3, SC4) | SeaweedFS leg + crashtest unit (RPO loop and campaign smoke at plan level) | `cargo test -p ostro --test bottomless_s3 --locked && cargo test -p ostro-crashtest --locked` | ❌ W0 | ✅ green (bottomless_s3 SeaweedFS leg ⚠ human_needed) |
| 8-21-01 | 21 | 6 | OBS-01 (outbound traceparent via ServerEffectDriver hooks, B2; journal unchanged) | integration (mock HTTP server) or helper-level + BLOCKING follow-up | `cargo test -p ostro-server --test outbound_traceparent --locked` | ❌ W0 | ✅ green |
| 8-21-02 | 21 | 6 | OBS-01/02/03 (SC5) | e2e | `cargo test -p ostro-server --test observability_e2e --locked` | ❌ W0 | ✅ green |
| 8-21-03 | 21 | 6 | FX-06 OTLP leg (D-FX6) | e2e | `cargo test -p ostro-server --test otlp_canary --locked` | ❌ W0 | ✅ green |
| 8-22-01 | 22 | 7 | all | gate (full ci-local + campaign + loops at plan level) | `cargo deny check && bash scripts/forbidden-crates.sh --self-test` + RESERVATIONS flip grep + campaign report file | n/a | ✅ green (10k campaign ⚠ human_needed, resumable) |
| 8-22-02 | 22 | 7 | all (docs; OBS-01 not green without B2 evidence) | check | grep checks on VALIDATION/DECISIONS/docs | n/a | ✅ green |
| 8-23-01 | 23 | gap | BACK-02/SC3 (RPO = loss window <= seal; fixed seal/2 cadence, parallel uploads, one cut per manifest) | integration + failpoint | `cargo test -p ostro-backup --features failpoints -- --test-threads=1` (uploader incl. parallel_uploads_name_one_consistent_cut + run_starts_rounds_on_a_fixed_cadence, chain_start, cut_consistency, fencing, remote_gc, restore) | ✅ | ✅ green |
| 8-23-02 | 23 | gap | BACK-02/SC3 (loss window asserted <= seal + 500 ms) | crash loop | `cargo nextest run -P crash -p ostro-crashtest --run-ignored only -E 'test(restore_rpo)'` | ✅ | ✅ green at load 19–33 (quiet-machine re-run ⚠ follow-up) |
| 8-23-03 | 23 | gap | TRIG-01 (`[triggers] webhook_tolerance` fallback) | unit + integration | `cargo test -p ostro-server --lib -- webhooks && cargo test -p ostro-server --test webhooks --test config --test bottomless_service` | ✅ | ✅ green |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠ flaky / human_needed*

**Plan-level slow checks (W12):** `crash_loop` (08-09), `cron_exactly_once` and `restore_rpo` + campaign smoke (08-20), full `bash scripts/ci-local.sh` + the ≥ 10,000-fault campaign (08-22).

---

## Wave 0 Requirements

The task that first needs a test file creates it. TDD tasks write their tests first.

- [x] `scripts/forbidden-crates.sh` (`--self-test`) and `ci/deny-fixtures/ban-prometheus` (08-01)
- [x] `crates/ostro-triggers/tests/{cron,webhook_verify}.rs` (08-05, 08-06)
- [x] `crates/ostro-partition/tests/{snapshot,blobrefs,apply_schedule,snapshot_service,snapshot_tail,cron_driver}.rs`, `tests/common/gen_schedule.rs`, and extensions to `trim_safety.rs` and `rebuild_digest.rs` (08-02, 08-07, 08-09, 08-14)
- [x] `crates/ostro-objstore/tests/{localfs,s3}.rs` + `tests/conformance/mod.rs` + the s3s-fs decision (08-03)
- [x] `crates/ostro-backup/tests/{uploader,cut_consistency,fencing,chain_start,restore,remote_gc}.rs` + `tests/common/mod.rs` (08-13, 08-18, 08-19)
- [x] `crates/ostro-log/tests/{hw_cut,read_envelopes,import}.rs`, `crates/ostro-blob/tests/{observer,gc}.rs`, `crates/ostro-partition/tests/node_incarnation.rs`, `crates/ostro-meta/tests/bootstrap_snapshot.rs`
- [x] `crates/ostro-server/tests/{metrics_endpoint,otlp_export,tracing_spans,bottomless_service,schedules,webhooks,guest_logs,triggers_e2e,observability_e2e,otlp_canary,outbound_traceparent}.rs` + `tests/common/phase8.rs` (08-04, 08-10..08-18)
- [x] `crates/ostro-testkit/src/otlp.rs`, the in-process collector (08-04)
- [x] `crates/ostro-engine/tests/telemetry_spans.rs` (08-11)
- [x] `crates/ostro-api/tests/phase8_surface.rs`, `crates/ostro-cli/tests/phase8_verbs.rs`, `crates/ostro/tests/{cli_phase8,backup_cli,bottomless_s3}.rs`
- [x] `crates/ostro-crashtest/tests/{cron_exactly_once,restore_rpo}.rs` + Phase 8 campaign fault kinds (08-20)
- [x] Guests: `guests/log-spammer` (08-17), `guests/hook-order` (08-20)
- [x] Failpoints: `snapshot::{before_rename,before_dir_fsync,install_before_swap,install_after_old_rename}`, `trim::after_delete`, `upload::before_manifest`, `restore::{after_meta,mid_partition}`, `blobgc::before_unlink`. `FAILPOINT_CRATES += ostro-backup` in ci-local.sh and ci.yml together (08-01).

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

Recorded by 08-22 on 2026-11-20. One full ci-local run was shared with the 06-20 gate and ran on the final code. Everything committed after `5faa97c` is docs/planning only.

### ci-local (full, with musl)

| Step | Result | Time |
|------|--------|------|
| toolchain 1.100.0 | PASS | 0 s |
| cargo fmt --check | PASS | 2 s |
| cargo clippy -D warnings | PASS | 55 s |
| cargo build (warning-free) | PASS | 86 s |
| sdk workspace fmt + clippy | PASS | 21 s |
| sdk workspace test + wasm build | PASS | 23 s |
| guests build (wasm32-wasip2) | PASS | 12 s |
| order-saga example | PASS | 21 s |
| external: OSTRO_TEST_MYSQL_URL | SKIP (human_needed) | - |
| external: OSTRO_TEST_S3_URL | SKIP (human_needed) | - |
| cargo test | PASS (1,959 passed) | 2164 s |
| semantics sleepers (serial) | PASS | 39 s |
| failpoint list sync (ci.yml) | PASS | 0 s |
| failpoint suites | PASS (956 passed) | 2093 s |
| server campaign smoke (20 kills) | PASS | 413 s |
| cargo deny check | PASS | 1 s |
| check-vendored.sh | PASS | 1 s |
| check-deny-fixtures.sh | PASS | 1 s |
| no chronik/tokio-uring/prometheus | PASS | 0 s |
| workflow validation | PASS | 2 s |
| zig + cargo-zigbuild present | PASS | 0 s |
| musl release (static, mimalloc) | PASS | 311 s |
| musl zigbuild test | PASS (1,959 passed) | 2146 s |

`all steps passed`, exit 0. Run on main `5faa97c` (worktree, fresh `CARGO_TARGET_DIR`), 2026-11-20 21:50–23:53 CET (about 2 h 03 min). `OSTRO_TEST_PG_URL` was set (banner: `set: OSTRO_TEST_PG_URL (ostro-bridge sql_pg, ostro fx_sql_crash pg_after_commit/pg_before_user_stmt)`), pointing at a private throwaway PostgreSQL 14.24 cluster (127.0.0.1:55468, scram, data dir in the session scratchpad, loaded via the untracked `.ostro-test-env`; nothing committed).

PG legs that really ran:
- `ostro-bridge --test sql_pg`: 10 passed in `cargo test` and 10 passed in `musl zigbuild test`; 13 passed in `failpoint suites` (with the three `failpoints::pg_*` cases).
- `ostro --features failpoints --test fx_sql_crash`: `pg_after_commit` and `pg_before_user_stmt` passed in `failpoint suites`. Re-run alone with `--nocapture`: `N=50 held in 452 ms, restart (readyz) 164 ms, all terminal 471 ms after kill -9` and `N=50 held in 533 ms, restart 170 ms, all terminal 470 ms`.
- The PG server recorded 1,344 committed transactions and 2,506 inserted tuples. The tests drop their own schemas.

The MySQL legs (`sql_mysql`, `fx_sql_crash mysql_*`) and the S3 legs passed as loud SKIPs.

Campaign smoke: `kills=20 lost_acked_runs=0 duplicate_journal_entries=0 non_terminal_runs=0 lost_acked_signals=0 wrong_results=0 other_violations=0`, acked_runs=231, RESULT: PASS.

The musl release binary (`x86_64-unknown-linux-musl/release/ostro`) is 44,033,456 bytes (42.0 MiB), static, mimalloc. No 06-02 size baseline was recorded, so this number is the baseline from now on.

No step failed and no test needed a re-run.

Phase 8 suites in this run, all green:
- objstore `s3`: 14 passed; the SeaweedFS case is a SKIP.
- backup `fencing`: 5 passed; `s3_fencing` is a SKIP.
- `restore`: 5 passed.
- `remote_gc`: 6 passed.
- `backup_cli`: 2 passed.
- `cron_driver`: 8 passed.
- `snapshot_service`: 4 passed.
- `schedules`: 4 passed.
- `webhooks`: 5 passed.
- `metrics_endpoint`: 4 passed.
- `guest_logs`: 4 passed.
- `triggers_e2e`: 3 passed.
- `outbound_traceparent`: 1 passed.
- `observability_e2e`: 1 passed.
- `otlp_canary`: 1 passed.
- `bottomless_s3`: a SKIP.

### Dependency policy, clock gate, reservations

| Check | Result |
|-------|--------|
| `cargo deny check` | PASS (no new chrono/object_store exception; deny.toml unchanged since the 06-02 base) |
| forbidden crates (`scripts/forbidden-crates.sh`, ci-local step) | PASS |
| normal tree | no `chrono`, `zstd-sys`, `openssl-sys` or `object_store`. Duplicates: jiff 0.2/1.0 (P8-D9) plus the warned duplicates that were already there |
| FAILPOINT_CRATES == ci.yml "Failpoint suites" | PASS (`failpoint list sync`); includes `ostro-backup` |
| clock gate (the 08-22 verify grep over the Phase 8 paths) | no hits. Exemptions are listed in 08-22-SUMMARY. The pre-Phase-8 hits found by the wide grep are a Phase 10 follow-up |
| reservations script | `RESERVATIONS CHECK: PASS (66 checks)` (04cedcb). Cmd 27, keyspace 16 and InvalidCode 22/23 are `exists (P8, 08-01)`; the §11 names that shipped are `exists (P8, 08-NN)`; unused slots are `reserved P8` |

### Crash campaign (Phase 8 scenarios, 10,000 faults): ⚠ human_needed

- `target/campaign-p8-gate` was configured with SHARDS=20, FAULTS=10000, seed 1305 and `phase8=true`.
- Only shards 0–5 have run, all partial, because a machine restart interrupted the run. Kills per shard: 73 / 95 / 90 / 94 / 99 / 90, **541 total**. Every shard tally is 0: `lost_acked_runs`, `lost_acked_signals`, `duplicate_journal_entries`, `non_terminal_runs`, `wrong_results`, `other_violations`; `failures=[]`.
- All eight Phase 8 menu sites were hit.
- `target/campaign-p8-gate/HUMAN_NEEDED` holds the state and the resume command (gate command 2 above).
- Smaller-scale evidence:
  - the 08-20 Phase 8 smoke: 200 faults, 4 shards, PASS, 4 bucket restores verified;
  - this run's ci-local campaign smoke: 20 kills, PASS.

### `#[ignore]` loops (re-run at the gate)

`cargo nextest run -P crash -p ostro-crashtest --run-ignored only --locked -E 'test(=cron_exactly_once) | test(=restore_rpo) | test(=crash_loop_snapshots)'` ran on the gate build (release failpoint binary, quiet machine, after ci-local): **3/3 passed in 398.5 s**.

| Loop | Result |
|------|--------|
| `crash_loop_snapshots` (08-09 snapshot mode) | PASS, 127.7 s |
| `cron_exactly_once` (20 kill -9 restarts, every fire_time exactly once) | PASS, 117.7 s |
| `restore_rpo` (10 kill/restore iterations, every pre-cut ack restored) | PASS, 153.1 s |

### Earlier measurements (from the plan SUMMARYs)

- **RPO, from 08-20 `restore_rpo`:** cut lag p50 1,044 ms and max 1,203 ms at seal 1 s; every pre-cut ack was restored over 10 kill/restore iterations. The data-loss window (p50 1,680 ms / max 2,449 ms) did not meet SC3 (08-VERIFICATION gap).
- **RPO after 08-23 (gap closure):** `restore_rpo` now asserts the loss window <= seal + 500 ms on every iteration. Measured at 1-minute load average 19–33: loss window p50 552 ms / max 1,049 ms, cut lag p50 280 ms / max 412 ms, restored-round upload p50 217 ms / max 576 ms (all 98 rounds p50 421 ms / max 1,958 ms), 18,276 acks; PASS in 291 s.
- **Restore, from 08-18:** P=16 with about 10k runs takes 9.0 s in debug. Phase 8 smoke bucket restores take 9.7–14.9 s each.
- **Snapshot/restore timings:** see docs/bottomless.md (measured values from 08-13, 08-18 and 08-20).

## Human-needed

Each item below is ⚠ human_needed.

1. **The rest of the 10,000-fault campaign.** Run gate command 2 above and write the report to `target/campaign-p8-gate/report.txt`. Gate: `kills >= 10000`, `lost_acked_runs=0`, `duplicate_journal_entries=0`, `non_terminal_runs=0`, `snapshot_required_with_valid_snapshot=0`, and `restore_checked` on every shard. Expect about 8–12 h.
2. **SeaweedFS conditional PUT and the S3 legs (BACK-01).** Use a Docker machine, or CI after the first push. Run `docker run -d -p 8333:8333 -e AWS_ACCESS_KEY_ID=ostro -e AWS_SECRET_ACCESS_KEY=ostroseaweed -e S3_BUCKET=ostro-test chrislusf/seaweedfs:4.54 mini -dir=/data`, then `OSTRO_TEST_S3_URL=http://127.0.0.1:8333 OSTRO_TEST_S3_ACCESS_KEY=ostro OSTRO_TEST_S3_SECRET_KEY=ostroseaweed OSTRO_TEST_S3_BUCKET=ostro-test cargo test -p ostro-objstore --test s3 --locked && cargo test -p ostro-backup --test fencing --locked && cargo test -p ostro --test bottomless_s3 --locked`.
3. **R2/AWS conditional PUT.** Run the same tests with the `OSTRO_TEST_S3_*` variables pointed at R2 or S3. If `If-Match` is unsupported, confirm the LOCK re-read fallback engages.
4. **Fresh-machine `ostro restore --from s3://…` with timing (BACK-02/SC3).** Run a server with `[backup] url = s3://…` under load, kill -9 it, then run `ostro restore --from s3://… --data-dir /var/lib/ostro` on another machine. Record the report: hashes, cut_at, duration.
5. **Jaeger/Tempo + Grafana visual check (OBS-01/02).** Set `[telemetry] otlp_endpoint` to a collector and scrape `/metrics`. Check that the run → activation → step tree, the `effect:<kind>` spans and the dashboards render.
6. **TLS roots in a `FROM scratch` image (Phase 14 artefact).** Restore from HTTPS S3 with `tls_roots = "webpki"`.
7. **Quiet-machine re-run of `restore_rpo` (08-23 follow-up, SC3).** 08-23 measured it at load 19–33, and it passed. Re-run `cargo nextest run -P crash -p ostro-crashtest --run-ignored only -E 'test(restore_rpo)'` on a quiet machine and record the loss window p50/max here. Expect it to be at most the loaded numbers.
7. **The Phase 6 canary leg:** **not pending.** 08-21 ran it (`PHASE6_CANARY_LEG: run ... clean`), and `otlp_canary` passed again in this gate.
8. **The outbound traceparent leg (B2, OBS-01):** **not pending.** 08-21 recorded `BRIDGE_TRACEPARENT_LEG: wired`, and `outbound_traceparent` passed in this gate. OBS-01 is complete.
9. **User review** of P8-D3 (seal default and cost), P8-D9 (jiff 0.2 adapter), P8-D11 (Kafka out of scope), P8-D16 (in-memory log cap), P8-D21 (22 plans) and P8-D25 (SeaweedFS instead of MinIO). See 08-DECISIONS.md.

---

## Validation Sign-Off

- [x] All tasks have an `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without an automated verify
- [x] Wave 0 covers every MISSING reference
- [x] No watch-mode flags
- [x] Feedback latency < 60 s for task-level checks
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** gate evidence recorded by 08-22 (2026-11-20); the 10k campaign remainder is human_needed; phase verification pending (`/gsd:verify-work`)
