---
phase: 6
slug: effects-bridge-connectors-secrets
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-11-19
---

# Phase 6 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Source: 06-RESEARCH.md § Validation Architecture, mapped onto plans 06-01..06-18. Adopted decisions: 06-DECISIONS.md (D-FX1..D-FX9, P-1..P-17).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | `cargo test` (libtest), plus proptest 1.12, insta 1 and `fail` 0.5.1 failpoints (via `bridge_fail!`, P-13). Spawned-binary tests use `CARGO_BIN_EXE_ostro` under `--features failpoints`. Mock HTTP, Vault and K8s servers use axum. The canary harness lives in `ostro-testkit` (feature `canary`). |
| **Config file** | None beyond Cargo.toml. `scripts/ci-local.sh` `FAILPOINT_CRATES` must equal the ci.yml "Failpoint suites" step (06-01 adds ostro-bridge). |
| **External services** | PG 14 runs locally (`OSTRO_TEST_PG_URL`, mandatory, from the untracked `.ostro-test-env`). MySQL 8.4 (`OSTRO_TEST_MYSQL_URL`) and MinIO (`OSTRO_TEST_S3_*`) are env-gated: loud SKIP locally, CI service containers (06-16). Vault/K8s use mocks (a real Vault is optional). |
| **Guest build** | `cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml --target-dir target/guests` (fx-http, fx-sql, fx-object, fx-canary, fx-ssrf from 06-08) |
| **Quick run command** | `cargo test -p ostro-bridge --locked` (unit, no DB) |
| **Full suite command** | `bash scripts/ci-local.sh --no-musl` |
| **Phase gate commands** | 1. `bash scripts/ci-local.sh` (with musl).<br>2. `cargo test -p ostro --features failpoints --test fx_redrive --test fx_sql_crash --locked -- --test-threads=1`.<br>3. `cargo test -p ostro-server --test fx_canary --test fx_ssrf_e2e --test fx_object_e2e --test fx_providers_e2e --locked -- --test-threads=1`.<br>4. `cargo test -p ostro-engine --test replay_corpus --locked`.<br>5. `cargo deny check && test "$(cargo tree --workspace -e normal --locked \| grep -c 'chrono v')" = 0`. |
| **Estimated runtime** | Quick: < 30 s. Full ci-local: Phase 5 baseline plus the kube/k8s-openapi/sqlx compile time (measured in 06-01). The e2e crash tests take a few minutes each. |

---

## Sampling Rate

- **After every task commit:** `cargo test -p <crate touched> --locked` plus `cargo clippy -p <crate> --all-targets --all-features --locked -- -D warnings`. Bridge tasks with failpoint sites also run `cargo test -p ostro-bridge --features failpoints --locked -- --test-threads=1`.
- **After every plan wave:** `cargo test --workspace --locked` with `OSTRO_TEST_PG_URL` set, plus `cargo test -p ostro-engine --test replay_corpus --locked`.
- **Before `/gsd:verify-work`:** the 06-18 gate.
- **Max feedback latency:** 60 s for task-level checks (the DB and crash e2e tests are wave-level).
- **Piped verify commands** use `set -o pipefail`.

---

## Wave Structure

| Wave | Plans | Exclusive shared files |
|------|-------|------------------------|
| 1 | 06-01 | Gate: Phase 5 complete (all 05-*-SUMMARY, 05-VERIFICATION passed or by-design human_needed, WIT freeze in place). **Only plan editing** Cargo.toml, Cargo.lock, member dependency tables, ostro-bridge `lib.rs`, command.rs, InvalidCode, STATE.md and REQUIREMENTS.md. FAILPOINT_CRATES sync. |
| 2 | 06-02 partition apply, 06-03 egress, 06-04 secrets core, 06-05 engine seam, 06-06 driver core + rate limits, 06-07 API surface, 06-08 SDK + guests | Disjoint: ostro-partition (+ docs/storage-format.md) / bridge `egress/*` / bridge `secrets/*` (except `providers/{mod,cache,vault,k8s}`) / ostro-engine / bridge `{driver,registry,ratelimit,outcome,config}.rs` / ostro-proto + ostro-api / sdks/rust + guests/fx-* |
| 3 | 06-09 HTTP, 06-10 SQL, 06-11 object + ostro-objstore, 06-12 Vault/K8s | Disjoint: bridge `http/*` + testkit `mock_provider.rs` / bridge `sql/*` + testkit `db.rs` / `crates/ostro-objstore/**` + bridge `object/*` / bridge `providers/{cache,vault,k8s}.rs` |
| 4 | 06-13 server seam | ostro-server src (config, bridge_services, meta_view, effects, sink, derive, server, lib, control/mod) + tests/common/mod.rs |
| 5 | 06-14 server admin impl + CLI + API test helpers | ostro-server control/bridge.rs, tests/common/fx.rs; ostro-cli |
| 6 | 06-15 e2e HTTP/SSRF/rate/re-drive, 06-16 e2e SQL crash + CI services, 06-17 e2e canary/object/providers | Disjoint test files. **06-16 alone** edits ci-local.sh, ci.yml and .gitignore. **06-17 alone** edits testkit `canary.rs`. |
| 7 | 06-18 phase gate | docs/*, tests/replay-corpus/, RESERVATIONS.md flip, 06-VALIDATION.md |

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 6-01-01 | 01 | 1 | (gate) | check + reservations script | 05-{01..20}-SUMMARY present; 05-VERIFICATION `passed\|human_needed` (by-design only); FROZEN.toml + local tag + witcheck; no `planned P5`; STATE/REQUIREMENTS/RESERVATIONS greps | n/a | ⬜ pending |
| 6-01-02 | 01 | 1 | all (deps, skeleton) | build + deny | `cargo build --workspace --all-targets --locked && cargo build -p ostro-bridge --no-default-features --locked && cargo deny check` + no chrono | ❌ W0 | ⬜ pending |
| 6-01-03 | 01 | 1 | FX-01, FX-03 (contract) | unit + golden (append-only) | `cargo test -p ostro-types --locked && cargo test -p ostro-types --test golden_frozen --locked` + golden diff check | ✅ (extended) | ⬜ pending |
| 6-02-01 | 02 | 2 | FX-01, FX-03 | integration | `cargo test -p ostro-partition --test apply_effects --locked` | ❌ W0 | ⬜ pending |
| 6-02-02 | 02 | 2 | FX-03 (P-8/P-9) | integration + corpus | `cargo test -p ostro-partition --test apply_effects --test format_corpus --locked` | ❌ W0 | ⬜ pending |
| 6-02-03 | 02 | 2 | FX-01, FX-03 | proptest | `cargo test -p ostro-partition --test rebuild_digest --locked` | ✅ (extended) | ⬜ pending |
| 6-03-01 | 03 | 2 | FX-08 | unit + proptest | `cargo test -p ostro-bridge --lib egress::classify --locked` | ❌ W0 | ⬜ pending |
| 6-03-02 | 03 | 2 | FX-08 | table test | `cargo test -p ostro-bridge --test ssrf_table --locked` | ❌ W0 | ⬜ pending |
| 6-03-03 | 03 | 2 | FX-08 | integration (loopback) | `cargo test -p ostro-bridge --test egress_client --locked -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 6-04-01 | 04 | 2 | FX-06, FX-07 | unit + proptest | `cargo test -p ostro-bridge --lib secrets::template secrets::grants secrets::value --locked` | ❌ W0 | ⬜ pending |
| 6-04-02 | 04 | 2 | FX-07, FX-10 | unit | `cargo test -p ostro-bridge --lib secrets::scrub secrets::sealed --locked` | ❌ W0 | ⬜ pending |
| 6-04-03 | 04 | 2 | FX-06, FX-10 | integration | `cargo test -p ostro-bridge --test secrets --locked -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 6-05-01 | 05 | 2 | FX-06 (redacted journal copy) | integration (fake driver) | `cargo test -p ostro-engine --test effects_driver --locked` | ❌ W0 | ⬜ pending |
| 6-05-02 | 05 | 2 | FX-01, FX-03 | integration + corpus | `cargo test -p ostro-engine --test effects_driver --test replay_corpus --locked` | ❌ W0 | ⬜ pending |
| 6-06-01 | 06 | 2 | FX-09 | integration | `cargo test -p ostro-bridge --test ratelimit --locked` | ❌ W0 | ⬜ pending |
| 6-06-02 | 06 | 2 | FX-01, FX-03 | integration (fakes) | `cargo test -p ostro-bridge --test driver --locked` (+ `--features failpoints`) | ❌ W0 | ⬜ pending |
| 6-07-01 | 07 | 2 | FX-06, FX-10 (surface) | build + unit | `cargo build --workspace --all-targets --locked && cargo test -p ostro-api --lib --locked` | ✅ (extended) | ⬜ pending |
| 6-07-02 | 07 | 2 | FX-06, FX-07, FX-08, FX-05 | integration + parity + insta | `cargo test -p ostro-api --locked` | ❌ W0 (bridge_surface) | ⬜ pending |
| 6-08-01 | 08 | 2 | FX-02 (SDK key), FX-04, FX-05 | unit | `cargo test --manifest-path sdks/rust/Cargo.toml` + wasm clippy | ✅ (extended) | ⬜ pending |
| 6-08-02 | 08 | 2 | (test enablers) | build | guest build + 5 `.wasm` present + clippy | ❌ W0 | ⬜ pending |
| 6-09-01 | 09 | 3 | FX-02, FX-03 | unit | `cargo test -p ostro-bridge --lib http:: --locked` | ❌ W0 | ⬜ pending |
| 6-09-02 | 09 | 3 | (mock provider) | build | `cargo build -p ostro-testkit --features mock-http --locked` | ❌ W0 | ⬜ pending |
| 6-09-03 | 09 | 3 | FX-01, FX-02, FX-03, FX-07 | integration (mock) | `cargo test -p ostro-bridge --test http_connector --locked -- --test-threads=1` (+ failpoints) | ❌ W0 | ⬜ pending |
| 6-10-01 | 10 | 3 | FX-04 | unit (classifier table) | `cargo test -p ostro-bridge --lib sql:: --locked` | ❌ W0 | ⬜ pending |
| 6-10-02 | 10 | 3 | FX-04 | DB integration | `OSTRO_TEST_PG_URL=… cargo test -p ostro-bridge --test sql_pg --test sql_mysql --locked -- --test-threads=1` | ❌ W0 | ⬜ pending (MySQL ⚠) |
| 6-11-01 | 11 | 3 | FX-05 | integration | `cargo test -p ostro-objstore --locked -- --test-threads=1` | ❌ W0 | ⬜ pending (S3 ⚠) |
| 6-11-02 | 11 | 3 | FX-05 | integration | `cargo test -p ostro-bridge --test object_local --test object_s3 --locked` | ❌ W0 | ⬜ pending (S3 ⚠) |
| 6-12-01 | 12 | 3 | FX-10 | integration (mock Vault) | `cargo test -p ostro-bridge --test providers_vault --locked` | ❌ W0 | ⬜ pending |
| 6-12-02 | 12 | 3 | FX-10 | integration (mock K8s) | `cargo test -p ostro-bridge --test providers_k8s --locked && cargo build -p ostro-bridge --no-default-features --locked` | ❌ W0 | ⬜ pending |
| 6-13-01 | 13 | 4 | (config, services) | integration | `cargo test -p ostro-server --test config --locked` | ✅ (extended) | ⬜ pending |
| 6-13-02 | 13 | 4 | FX-01..FX-05 | unit + integration | `cargo test -p ostro-server --lib --test sink --locked` | ✅ (extended) | ⬜ pending |
| 6-13-03 | 13 | 4 | FX-01, FX-03, FX-06 | in-process e2e | `cargo test -p ostro-server --test effects_seam --locked -- --test-threads=1` (+ failpoints) | ❌ W0 | ⬜ pending |
| 6-14-01 | 14 | 5 | FX-06, FX-07, FX-08, FX-10, FX-05 | in-process e2e | `cargo test -p ostro-server --test bridge_admin --locked -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 6-14-02 | 14 | 5 | FX-06 (CLI no-leak) | spawned binary | `cargo test -p ostro --test cli_bridge --locked -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 6-15-01 | 15 | 6 | FX-01, FX-02, FX-03 (SC1) | e2e + kill -9 | `cargo test -p ostro-server --test fx_http_e2e --locked` and `cargo test -p ostro --features failpoints --test fx_redrive --locked -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 6-15-02 | 15 | 6 | FX-08, FX-09 (SC4) | e2e via guest | `cargo test -p ostro-server --test fx_ssrf_e2e --test fx_rate_e2e --locked -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 6-16-01 | 16 | 6 | FX-04 (SC2) | e2e + kill -9 | `OSTRO_TEST_PG_URL=… cargo test -p ostro --features failpoints --test fx_sql_crash --locked -- --test-threads=1` | ❌ W0 | ⬜ pending (MySQL ⚠) |
| 6-16-02 | 16 | 6 | (CI wiring) | check + gate | ci.yml services grep + FAILPOINT_CRATES parity loop + `bash scripts/ci-local.sh --no-musl` | ✅ (extended) | ⬜ pending |
| 6-17-01 | 17 | 6 | FX-06 (harness) | unit | `cargo test -p ostro-testkit --features canary --locked` | ❌ W0 | ⬜ pending |
| 6-17-02 | 17 | 6 | FX-06, FX-07 (SC3) | e2e harness | `cargo test -p ostro-server --test fx_canary --locked -- --test-threads=1` | ❌ W0 | ⬜ pending (OTLP leg → Phase 8) |
| 6-17-03 | 17 | 6 | FX-05, FX-10 (SC5) | e2e via guest | `cargo test -p ostro-server --test fx_object_e2e --test fx_providers_e2e --locked -- --test-threads=1` | ❌ W0 | ⬜ pending (S3 ⚠) |
| 6-18-01 | 18 | 7 | all (docs) | check | doc greps (effect_outcome_unknown, fd00:ec2::254, `__ostro/`, ostro-objstore, Azure footnote) | n/a | ⬜ pending |
| 6-18-02 | 18 | 7 | all (corpus, reservations, freeze) | corpus + gate | `cargo test -p ostro-engine --test replay_corpus && cargo test -p ostro-witcheck && cargo test -p ostro-types --test golden_frozen && cargo test -p ostro-partition --test format_corpus && cargo deny check` | ✅ (extended) | ⬜ pending |
| 6-18-03 | 18 | 7 | all | gate | `bash scripts/ci-local.sh` | n/a | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠ flaky / human_needed*

---

## Wave 0 Requirements

Test files are created by the task that first needs them (TDD tasks write tests first).

- [ ] **ostro-bridge tests**: `ssrf_table`, `egress_client`, `secrets`, `driver`, `ratelimit`, `http_connector`, `sql_pg`, `sql_mysql`, `object_local`, `object_s3`, `providers_vault`, `providers_k8s`
- [ ] **ostro-objstore tests**: `localfs`, `presign`, `s3` (unless Phase 8 already created them; 06-11 then adds only presign cases)
- [ ] **ostro-partition**: `tests/apply_effects.rs`, `tests/common/gen_effects.rs`
- [ ] **ostro-engine**: `tests/effects_driver.rs`
- [ ] **ostro-api**: `tests/bridge_surface.rs`
- [ ] **ostro-server**: `tests/{effects_seam, bridge_admin, fx_http_e2e, fx_ssrf_e2e, fx_rate_e2e, fx_canary, fx_object_e2e, fx_providers_e2e}.rs`, `tests/common/fx.rs`
- [ ] **ostro binary**: `tests/{cli_bridge, fx_redrive, fx_sql_crash}.rs`
- [ ] **ostro-testkit** (features): `mock_provider.rs` (06-09), `db.rs` (06-10), `canary.rs` (06-17)
- [ ] **Guests**: fx-http, fx-sql, fx-object, fx-canary, fx-ssrf (06-08)
- [ ] **Failpoint sites**: `bridge::http::before_send`, `bridge::http::after_response`, `bridge::sql::before_user_stmt`, `bridge::sql::after_commit`, `bridge::object::before_put`, `bridge::effect::before_complete_submit`
- [ ] **CI**: ci.yml services postgres:17 and mysql:8.4, MinIO via `docker run`, env exports (06-16)

---

## Manual-Only / Human-Needed Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| SC2 on MySQL 8.4 (exactly one row per run after kill -9) and the sql_mysql suite | FX-04 | No mysqld and no Docker in WSL (D-FX5) | After the first push the CI service container runs it. Locally: install mysql-server 8.4, then `OSTRO_TEST_MYSQL_URL='mysql://root:…@127.0.0.1:3306/ostro?ssl-mode=REQUIRED' cargo test -p ostro --features failpoints --test fx_sql_crash -- --test-threads=1` and `cargo test -p ostro-bridge --test sql_mysql`. |
| Object store on S3-compatible storage (MinIO) incl. presign | FX-05 | No Docker in WSL | CI MinIO step. Locally: `OSTRO_TEST_S3_URL=http://127.0.0.1:9000 OSTRO_TEST_S3_ACCESS_KEY=… OSTRO_TEST_S3_SECRET_KEY=… OSTRO_TEST_S3_BUCKET=… cargo test -p ostro-objstore -p ostro-bridge --test object_s3 && cargo test -p ostro-server --test fx_object_e2e`. |
| GCS through S3 interoperability | FX-05 | Needs a GCS bucket and HMAC keys (MEDIUM confidence) | Point the `OSTRO_TEST_S3_*` vars at `https://storage.googleapis.com`, region `auto`, and run the object suites. |
| Canary absent from OTLP export | FX-06 (SC3) | OTLP export lands in Phase 8 (D-FX6) | Phase 8 registers an OTLP mock collector with `ostro_testkit::canary::register_sink` and re-runs `fx_canary`. |
| Remote CI legs (aarch64-musl, macOS) | all | No git remote yet (FOUND-05) | Confirm green after the first push. |
| Review of the FX-05 wording/Azure deferral, D-FX2 (native roots) and D-FX6 | FX-04, FX-05, FX-06 | User unavailable (YOLO) | Read 06-DECISIONS.md. The STATE.md Pending Todos list each item. |
| Real Vault dev server (optional) | FX-10 | The mock covers the suite | `OSTRO_TEST_VAULT_ADDR=… OSTRO_TEST_VAULT_TOKEN=… cargo test -p ostro-bridge --test providers_vault`. |

---

## Gate results

(Filled in by 06-18 Task 3.)

## Human-needed

(Filled in by 06-18 Task 3.)

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60 s for task-level checks
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
